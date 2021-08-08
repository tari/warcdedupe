#[macro_use]
extern crate log;

use std::io::{BufRead, Error, Seek, Write};

use response_log::ResponseLog;
use warcio::record::{Compression, InvalidRecord, Record};

use crate::digest::Digester;
use flate2::bufread::GzDecoder;
use flate2::write::GzEncoder;
use std::marker::PhantomData;
use warcio::header::FieldName;

pub mod digest;
pub mod response_log;

// TODO: builder pattern seems worthwhile
pub struct Deduplicator<D, L, W> {
    // The digester implementation must be stable over the life of
    // a deduplicator, but we don't hold an instance.
    digester: PhantomData<D>,
    log: L,
    output_compression: Compression,
    output: W,
}

impl<D, L, W> Deduplicator<D, L, W>
where
    D: Digester,
    <D as Digester>::Digest: Eq + Clone,
    L: ResponseLog<D::Digest>,
    W: Write,
{
    pub fn new(output: W, log: L, output_compression: Compression) -> Self {
        Self {
            digester: Default::default(),
            log,
            output_compression,
            // TODO: io::copy can reuse a write buffer (from a BufWriter), may be worthwhile
            output,
        }
    }

    fn process_record<R: BufRead>(
        &mut self,
        record: &mut Record<R>,
    ) -> Result<ProcessOutcome, ProcessError> {
        use ProcessOutcome::NeedsCopy;

        // A record ID is required by the spec to be present, and we need to refer
        // to an original record by ID. Noncompliant records will simply be copied.
        let record_id = match record.header.record_id() {
            Some(id) => id,
            None => {
                trace!("Skip record with no ID: impossible to WARC-Refers-To");
                return Ok(NeedsCopy);
            }
        };

        // Only consider responses to be candidates for deduplication.
        // The spec permits use of revisit records for any kind of record (noting it's
        // typical for response and resource records), but it's likely to be most
        // useful only for responses.
        if record.header.warc_type() != Some("response") {
            trace!(
                "Skip non-response record {:?} of type {:?}",
                record_id,
                record.header.warc_type()
            );
            return Ok(NeedsCopy);
        }

        // For HTTP responses, we'll only digest the response body and ignore headers as
        // specified by WARC 1.1 6.3.2 and RFC 2616.
        let content_type: Option<mime::Mime> = record
            .header
            .get_field(FieldName::ContentType)
            .and_then(|s| s.parse().ok());
        let content_is_http_response = content_type.map_or(false, |t| {
            t.essence_str() == "application/http"
                && t.get_param("msgtype").map_or(true, |mt| mt == "response")
        });
        let uri_is_http = record
            .header
            .field_uri("WARC-Target-URI")
            .map_or(false, |uri| {
                uri.starts_with("http:") || uri.starts_with("https:")
            });

        // The data representing HTTP headers which gets included in the revisit record.
        // If non-empty, a WARC-Truncated record header with reason "length" will be output for
        // deduplicated records with the contained data included in the record.
        let mut prefix_data: Vec<u8> = Vec::new();

        if content_is_http_response || uri_is_http {
            let mut consumed_bytes = 0usize;

            loop {
                // Grab some data to try to parse. We could avoid copying some data in the case
                // that the input's buffer entirely contains the response header, but copying the
                // entire buffer and truncating makes it much easier to handle the case where the
                // response header is larger than the input buffer.
                prefix_data.extend_from_slice(record.fill_buf()?);

                // We need a response to parse into, but don't actually care about the contents.
                let mut headers_buf = [httparse::EMPTY_HEADER; 64];
                let mut parsed_response = httparse::Response::new(&mut headers_buf);
                match httparse::Response::parse(&mut parsed_response, &prefix_data) {
                    Ok(httparse::Status::Complete(n)) => {
                        // Advance input past HTTP headers to digest payload only
                        record.consume(n - consumed_bytes);
                        consumed_bytes += n;
                        // Truncate copied data to the same length as consumed data
                        prefix_data.truncate(consumed_bytes);
                        break;
                    }
                    Err(e) if consumed_bytes == 0 => {
                        // Can't parse HTTP headers, so treat entire record as payload
                        trace!(
                            "HTTP parse error in record, will digest entire content: {}",
                            e
                        );
                        break;
                    }
                    Err(e) => {
                        // Consumed data from the input and can't rewind, so will have to simply
                        // copy the record without deduplicating.
                        trace!("unrewindable HTTP parse error in record, will copy: {}", e);
                        return Ok(NeedsCopy);
                    }
                    Ok(httparse::Status::Partial) => {
                        // Consume the entire buffer from the input
                        record.consume(prefix_data.len() - consumed_bytes);
                        consumed_bytes = prefix_data.len();
                    }
                }
            }
        }

        // Digest payload and record the digest, checking for a match
        let digest = match D::digest_record(record)? {
            None => {
                debug!(
                    "Record {} is not eligible for deduplication; skipping",
                    record.header.record_id().unwrap_or("<unknown>")
                );
                return Ok(NeedsCopy);
            }
            Some(d) => d,
        };
        debug!("Digested record to {:?}", digest);

        let record_id = record
            .header
            .record_id()
            .expect("Record-ID presence should have been validated earlier");
        // It is recommended that revisit records refer to the original target URI and date,
        // but not mandatory.
        let timestamp = record.header.warc_date();
        let target = record.header.field_uri("WARC-Target-URI");
        let (original_id, original_uri, original_date) =
            match self.log.add(record_id, target, timestamp, digest.clone()) {
                None => return Ok(NeedsCopy),
                Some(ids) => ids,
            };

        debug!(
            "writing revisit record for {} referring to {} (URI {:?}, Date {:?})",
            record_id, original_id, original_uri, original_date
        );

        // At this point we've found a record with matching digest to deduplicate against.
        // We'll create a new revisit record to emit, starting by copying the fields from the
        // original record then updating various fields.
        // TODO clone header names but convert to WARC 1.1 (including translating URIs)
        let mut dedup_headers = record.header.clone();
        // We're emitting a revisit record according to WARC 1.1, based on an identical
        // payload digest.
        dedup_headers.set_field("WARC-Type", "revisit");
        dedup_headers.set_field(
            "WARC-Profile",
            "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest",
        );
        dedup_headers.set_field("WARC-Refers-To", original_id);
        if let Some(original_uri) = original_uri {
            dedup_headers.set_field("WARC-Refers-To-Target-URI", original_uri);
        }
        if let Some(original_date) = original_date {
            dedup_headers.set_field("WARC-Refers-To-Date", original_date);
        }

        dedup_headers.set_field("WARC-Payload-Digest", D::format_digest(&digest));
        dedup_headers.set_field("Content-Length", format!("{}", prefix_data.len()));
        dedup_headers.set_field("WARC-Truncated", "length");

        // TODO: this seems to be writing extra garbage after the prefix data
        // TODO: WARC-Block-Digest must be updated or removed?
        let mut dedup_body = dedup_headers.write_to(&mut self.output, self.output_compression)?;
        dedup_body.write_all(&prefix_data)?;
        return Ok(ProcessOutcome::Deduplicated);
    }

    /// Read and deduplicate a single record from the provided input.
    ///
    /// Only data up to the end of the record will be consumed from the input.
    ///
    /// Returns true on success if the record was deduplicated, or false if it was simply
    /// copied.
    pub fn read_record<R: BufRead + Seek>(
        &mut self,
        mut input: R,
        compression: Compression,
    ) -> Result<bool, ProcessError> {
        let start_offset = input.stream_position()?;
        trace!(
            "Deduplicator start record read from input offset {}",
            start_offset
        );
        let mut record = Record::read_from(&mut input, compression)?;

        // TODO: if a payload digest is already present in the record we may be able to use that
        // instead of computing a fresh one.
        if self.process_record(&mut record)? == ProcessOutcome::NeedsCopy {
            // Not a duplicate. Drop the record to regain access to the raw input so we can
            // copy the raw record data with std::io::copy, taking advantage of OS-level copy
            // acceleration like copy_file_range(2) or sendfile(2).
            drop(record);
            let end_offset = input.stream_position()?;
            input.seek(std::io::SeekFrom::Start(start_offset))?;
            let mut raw_record = input.take(end_offset - start_offset);

            trace!(
                "Copy {} bytes to output, transform {:?} -> {:?}",
                end_offset - start_offset,
                compression,
                self.output_compression
            );
            match (compression, self.output_compression) {
                (Compression::None, Compression::None) | (Compression::Gzip, Compression::Gzip) => {
                    std::io::copy(&mut raw_record, &mut self.output)?
                }
                (Compression::None, Compression::Gzip) => std::io::copy(
                    &mut raw_record,
                    &mut GzEncoder::new(&mut self.output, flate2::Compression::best()),
                )?,
                (Compression::Gzip, Compression::None) => {
                    std::io::copy(&mut GzDecoder::new(&mut raw_record), &mut self.output)?
                }
            };

            return Ok(false);
        }
        Ok(true)
    }

    pub fn read_stream<R: BufRead + Seek>(
        &mut self,
        mut input: R,
        compression: Compression,
    ) -> Result<(u64, u64), ProcessError> {
        let mut n_copied = 0;
        let mut n_deduped = 0;

        while !input.fill_buf()?.is_empty() {
            let deduped = self.read_record(&mut input, compression)?;
            if deduped {
                n_deduped += 1;
            } else {
                n_copied += 1;
            }
        }
        Ok((n_copied, n_deduped))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ProcessOutcome {
    Deduplicated,
    NeedsCopy,
}

#[derive(Debug)]
pub enum ProcessError {
    InvalidRecord(InvalidRecord),
    IoError(std::io::Error),
}

impl From<std::io::Error> for ProcessError {
    fn from(e: Error) -> Self {
        ProcessError::IoError(e)
    }
}

impl From<InvalidRecord> for ProcessError {
    fn from(e: InvalidRecord) -> Self {
        ProcessError::InvalidRecord(e)
    }
}

#[cfg(test)]
mod test;

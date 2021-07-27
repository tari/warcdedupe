#[macro_use]
extern crate log;

use std::io::{BufRead, Seek};

use response_log::ResponseLog;
use warcio::reader::InvalidRecord;

use crate::digest::Digester;
use std::marker::PhantomData;

pub mod digest;
pub mod response_log;

pub struct Deduplicator<D, L> {
    // The digester implementation must be stable over the life of
    // a deduplicator, but we don't hold an instance.
    digester: PhantomData<D>,
    log: L,
}

trait RandomAccess: BufRead + Seek {}

enum Input {
    Stream(Box<dyn BufRead>),
    Seekable(Box<dyn RandomAccess>),
    // TODO memory-mapping saves some CPU; use memmap::Mmap
    //MemoryMapped(File),
}

impl From<Box<dyn BufRead>> for Input {
    fn from(input: Box<dyn BufRead>) -> Self {
        Input::Stream(input)
    }
}

impl AsMut<dyn BufRead> for Input {
    fn as_mut(&mut self) -> &mut (dyn BufRead + 'static) {
        match self {
            Input::Stream(r) => r,
            Input::Seekable(r) => r as &mut dyn BufRead,
        }
    }
}

impl<D, L> Deduplicator<D, L>
where
    D: Digester,
    <D as Digester>::Digest: Eq,
    L: ResponseLog<D::Digest>,
{
    pub fn new(log: L) -> Self {
        Self {
            digester: Default::default(),
            log,
        }
    }

    pub fn read<T: Into<Input>>(&mut self, input: T) {
        let mut input = input.into();
        loop {
            let mut record = match warcio::reader::Record::read_from(input.as_mut()) {
                Ok(r) => r,
                Err(InvalidRecord::EndOfStream) => {
                    break;
                }
                Err(e) => panic!("Error reading record: {:?}", e),
            };

            let record_id = record.header.record_id().unwrap_or("<missing>");
            if record.header.warc_type() != Some("response") {
                trace!("Skip non-response record {:?}", record_id);
                continue;
            }
            let len = match record.header.content_length() {
                Some(n) => n,
                None => {
                    info!("Record {} has no content-length; skipping", record_id);
                    continue;
                }
            };

            let mut digester = match D::new(&record.header) {
                Some(d) => d,
                None => {
                    debug!(
                        "Record {} is not eligible for deduplication; skipping",
                        record_id
                    );
                    continue;
                }
            };

            loop {
                // Digest chunks of the record based on the underlying buffer.
                // This avoids extra copies from its buffer into one of ours and allows
                // the caller to control how much data gets loaded into memory here by
                // setting the input's buffer size.
                let n = {
                    let buf = match record.fill_buf() {
                        Ok(buf) => buf,
                        Err(e) => panic!(
                            "I/O error reading body of record {:?}: {}",
                            record.header.record_id(),
                            e
                        ),
                    };
                    if buf.is_empty() {
                        // Reached end of record
                        break;
                    }
                    digester.handle_data(&buf);
                    buf.len()
                };
                record.consume(n);
            }
            let digest = digester.finalize();

            let target = match record.header.field_str("warc-target-uri") {
                None => {
                    eprintln!(
                        "Ignoring record without warc-target-uri: {:?}",
                        record.header
                    );
                    continue;
                }
                Some(t) => t,
            };
            let timestamp = match record.header.warc_date() {
                None => {
                    eprintln!("Ignoring record without warc-date: {:?}", record.header);
                    continue;
                }
                Some(t) => t,
            };

            // TODO need to look at HTTP response body.
            // Response headers tend to vary (Date: headers..), so check the response
            // Content-Type (application/http) and try to look at only the response
            // body. In case of match, still write out the HTTP headers since we haven't
            // deduplicated them.
            //
            // Also need to add WARC-Refers-To to the new revisit record with the WARC-Record-ID
            // of the original copy, where the original is always the chronologically earliest.
            //
            // We can parallelize (and possibly lose some savings) by recording the first-seen
            // date and retrieving it back out, updating it if the current record is older than
            // the currently-known oldest record. If the returned timestamp is earlier than
            // the current record, write a revisit pointing at the returned record ID.
            //
            // Parallelism requires either multiple files input, or a work queue that might be
            // derived from a cdx index or another worker just seeking through records and recording
            // their start points in a larger file.
            if let Some((_, first_seen)) =
                self.log
                    .add(target.to_owned(), timestamp.to_owned(), digest)
            {
                println!("{} dup {} size {}", target, first_seen, len);
                // TODO write revisit record
            } else {
                // Not a duplicate
                // TODO write record to output
            }
        }
    }
}

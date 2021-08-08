use std::io::BufRead;

use std::fmt::Debug;
use warcio::header::{FieldName, Header};
use warcio::record::Record;

pub trait Digester: Sized {
    type Digest: Debug + Eq + Clone;

    /// Create a new digester, or return None if the record is not eligible
    /// for deduplication.
    fn new(x: &Header) -> Option<Self>;
    /// Collect data from a record body and accumulate it into the digest.
    fn handle_data(&mut self, data: &[u8]);
    /// Compute the final digest from the initial state and any accumulated
    /// data.
    fn finalize(self) -> Self::Digest;

    /// Write a digest to the provided writer as a `labelled-digest` as specified
    /// by WARC 1.1 section 5.8.
    fn format_digest(digest: &Self::Digest) -> String;

    /// Compute the digest of a record and return the digest, or None if the record is not
    /// eligible for deduplication (according to [`Self::new`]).
    fn digest_record<Input: BufRead>(
        record: &mut Record<Input>,
    ) -> std::io::Result<Option<Self::Digest>> {
        let mut digester = match Self::new(&record.header) {
            Some(d) => d,
            None => return Ok(None),
        };

        loop {
            // Digest chunks of the record based on the underlying buffer.
            // This avoids extra copies from its buffer into one of ours and allows
            // the caller to control how much data gets loaded into memory here by
            // setting the input's buffer size.
            let n = {
                let buf = record.fill_buf()?;
                if buf.is_empty() {
                    // Reached end of record
                    break;
                }
                digester.handle_data(&buf);
                buf.len()
            };
            record.consume(n);
        }
        Ok(Some(digester.finalize()))
    }
}

/// A digester that keys on the source URL, record length and BLAKE3 hash
/// of the record contents.
pub struct UrlLengthBlake3Digester {
    length: u64,
    // TODO probably don't want this to unnecessarily limit matches:
    // WARC-Refers-To-Target-URI is not required for revisit records and
    // "it is not necessary that the URI of the original capture and the revisit be
    // identical" (6.7.2).
    url: Box<str>,
    hasher: blake3::Hasher,
}

impl UrlLengthBlake3Digester {
    const MIN_PARALLEL_LEN: usize = 1 << 20;
}

impl Digester for UrlLengthBlake3Digester {
    type Digest = (Box<str>, u64, [u8; blake3::OUT_LEN]);

    fn new(x: &Header) -> Option<Self> {
        Some(Self {
            length: x.content_length()?,
            url: x
                .get_field(FieldName::TargetURI)?
                .to_owned()
                .into_boxed_str(),
            hasher: blake3::Hasher::new(),
        })
    }

    fn handle_data(&mut self, data: &[u8]) {
        if data.len() < Self::MIN_PARALLEL_LEN {
            self.hasher.update(data);
        } else {
            debug!("using parallel hash for payload of {} bytes!", data.len());
            self.hasher.update_rayon(data);
        }
    }

    fn finalize(self) -> Self::Digest {
        (self.url, self.length, *self.hasher.finalize().as_bytes())
    }

    fn format_digest(digest: &Self::Digest) -> String {
        use std::fmt::Write;

        let expected_len = digest.2.len() * 2 + 7;
        let mut out = String::with_capacity(expected_len);
        let _ = write!(&mut out, "blake3:");
        for byte in digest.2 {
            let _ = write!(&mut out, "{:02x}", byte);
        }

        debug_assert_eq!(
            out.len(),
            expected_len,
            "string should not need reallocation"
        );
        out
    }
}

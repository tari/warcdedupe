use std::io::BufRead;

use data_encoding::BASE32;
use std::fmt::Debug;
use warcio::Header;
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

/// A digester that tracks the length and SHA-1 hash of records.
///
/// The output digests are base32-encoded (RFC 4648) as suggested by sections
/// 5.8 and 5.9 of the WARC 1.1 specification.
pub struct LengthSha1Digester {
    length: u64,
    hasher: sha1::Sha1,
}

impl Digester for LengthSha1Digester {
    type Digest = (u64, [u8; 20]);

    fn new(header: &Header) -> Option<Self> {
        Some(LengthSha1Digester {
            length: header.content_length(),
            hasher: Default::default(),
        })
    }

    fn handle_data(&mut self, data: &[u8]) {
        use sha1::Digest;

        self.hasher.update(data)
    }

    fn finalize(self) -> Self::Digest {
        use sha1::Digest;

        let mut out = [0u8; 20];
        out.copy_from_slice(self.hasher.finalize().as_slice());
        (self.length, out)
    }

    fn format_digest(digest: &Self::Digest) -> String {
        let digest_len = (digest.1.len() as f32 * 8.0 / 5.0).ceil() as usize;
        let mut out = String::with_capacity(5 + digest_len);
        let initial_capacity = out.capacity();

        out += "sha1:";
        BASE32.encode_append(&digest.1[..], &mut out);
        debug_assert_eq!(initial_capacity, out.len());

        out
    }
}

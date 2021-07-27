use sha1::{Digest, Sha1};
use warcio::Header;

pub trait Digester: Sized {
    type Digest;

    /// Create a new digester, or return None if the record is not eligible
    /// for deduplication.
    fn new(x: &Header) -> Option<Self>;
    fn handle_data(&mut self, data: &[u8]);
    fn finalize(self) -> Self::Digest;
}

// TODO consider using BLAKE3: it's super-fast and more secure
pub struct UrlLengthSha1Digester {
    length: u64,
    url: String,
    sha1: Sha1,
}

impl Digester for UrlLengthSha1Digester {
    type Digest = (String, u64, [u8; 20]);

    fn new(x: &Header) -> Option<Self> {
        Some(Self {
            length: x.content_length()?,
            url: x.field_str("warc-target-uri")?.to_owned(),
            sha1: Sha1::new(),
        })
    }

    fn handle_data(&mut self, data: &[u8]) {
        self.sha1.update(data);
    }

    fn finalize(self) -> Self::Digest {
        // Convert GenericArray<20> to an actual array since we can't really write
        // the generic type.
        let mut fixed = [0u8; 20];
        fixed.copy_from_slice(self.sha1.finalize().as_slice());

        (self.url, self.length, fixed)
    }
}

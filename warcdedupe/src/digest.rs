use warcio::Header;

pub trait Digester: Sized {
    type Digest;

    /// Create a new digester, or return None if the record is not eligible
    /// for deduplication.
    fn new(x: &Header) -> Option<Self>;
    fn handle_data(&mut self, data: &[u8]);
    fn finalize(self) -> Self::Digest;
}

pub struct UrlLengthBlake3Digester {
    length: u64,
    url: String,
    hasher: blake3::Hasher,
}

impl Digester for UrlLengthBlake3Digester {
    type Digest = (String, u64, [u8; blake3::OUT_LEN]);

    fn new(x: &Header) -> Option<Self> {
        Some(Self {
            length: x.content_length()?,
            url: x.field_str("warc-target-uri")?.to_owned(),
            hasher: blake3::Hasher::new(),
        })
    }

    fn handle_data(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    fn finalize(self) -> Self::Digest {
        (self.url, self.length, *self.hasher.finalize().as_bytes())
    }
}

//! Handling of record compression.
//!
//! WARC files can be compressed, but the structure of the compressed data must be managed
//! to ensure a record can be accessed without decompressing every previous one in a file
//! (which may contain many records).
//!
//! In general, records are individually compressed to ensure they can be the subject of random
//! access in a file containing many records. Provided the file offset of a compressed record is
//! known, a reading tool can read a record alone. If records were not compressed individually,
//! readers would need to decompress every preceding record in a file in order to reach a desired
//! one.

use std::io::{Result as IoResult, Write};
use std::path::Path;

use flate2::write::GzEncoder;

/// The supported methods of compressing a single [`Record`](crate::Record).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Compression {
    /// Uncompressed data
    None,
    /// `gzip` compression
    ///
    /// gzip uses DEFLATE compression which is relatively simple but doesn't have particularly good
    /// compression. Each record has a gzip header and footer that include some uninteresting
    /// fields but also include a checksum.
    Gzip,
    // TODO zstd mode https://iipc.github.io/warc-specifications/specifications/warc-zstd/
}

impl Compression {
    /// Return the best guess of compression to be used for a file with the given name.
    ///
    /// A file that may be present is not accessed in any way; only the path is used to guess based
    /// on the name.
    ///
    /// ```
    /// # use warcio::Compression;
    /// assert_eq!(Compression::guess_for_filename("test.warc.gz"), Compression::Gzip);
    /// ```
    pub fn guess_for_filename<P: AsRef<Path>>(path: P) -> Compression {
        match path.as_ref().extension() {
            Some(ext) if ext == "gz" => Compression::Gzip,
            _ => Compression::None,
        }
    }
}

/// Writes to an output stream with specified [`Compression`].
pub enum Writer<W: Write> {
    Plain(W),
    Gzip(GzEncoder<W>),
}

impl<W: Write> Writer<W> {
    /// Construct a writer to the given adapter with the given compression mode.
    pub fn new(dest: W, mode: Compression) -> Self {
        match mode {
            Compression::None => Self::Plain(dest),
            Compression::Gzip => Self::Gzip(GzEncoder::new(dest, flate2::Compression::best())),
        }
    }

    /// Gracefully close the writer (terminating a compressed stream) and return the output stream.
    pub fn finish(self) -> IoResult<W> {
        match self {
            Self::Plain(w) => Ok(w),
            Self::Gzip(gz) => gz.finish()
        }
    }
}


impl<W: Write> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        match self {
            Writer::Plain(w) => w.write(buf),
            Writer::Gzip(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match self {
            Writer::Plain(w) => w.flush(),
            Writer::Gzip(w) => w.flush(),
        }
    }
}


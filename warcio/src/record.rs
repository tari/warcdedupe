//! Operations on complete WARC records.
use super::HeaderParseError;
use crate::header::get_record_header;
use buf_redux::BufReader;
use std::cmp;
use std::error::Error as StdError;
use std::fmt;
use std::io::prelude::*;
use std::io::Error as IoError;
use std::ops::Drop;
use std::path::Path;

pub use buf_redux::Buffer;
use thiserror::Error;

/// The number of bytes to skip per read() call when closing a record.
///
/// Larger values require more memory but will reduce overhead.
const SKIP_BUF_LEN: usize = 4096;

/// An error in reading a record from an input stream.
#[derive(Debug, Error)]
pub enum InvalidRecord {
    /// The header of the record was malformed.
    ///
    /// This may mean the input doesn't actually contain WARC records.
    #[error("record header is not valid: {0}")]
    InvalidHeader(#[source] HeaderParseError),
    /// The length of the payload could not be determined.
    ///
    /// Contained value is the contents of the Content-Length header.
    #[error("Content-Length is not a valid integer (contained bytes {0:?})")]
    UnknownLength(Option<Vec<u8>>),
    /// Reached the end of the input stream.
    #[error("unexpected end of input")]
    EndOfStream,
    /// Other I/O error.
    #[error("I/O error")]
    IoError(#[source] IoError),
}

impl From<HeaderParseError> for InvalidRecord {
    fn from(e: HeaderParseError) -> Self {
        match e {
            HeaderParseError::IoError(e) => InvalidRecord::IoError(e),
            HeaderParseError::Truncated => InvalidRecord::EndOfStream,
            e => InvalidRecord::InvalidHeader(e),
        }
    }
}

/// The supported methods of compressing a single [`Record`].
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Compression {
    None,
    Gzip,
    // TODO zstd mode https://iipc.github.io/warc-specifications/specifications/warc-zstd/
}

impl Compression {
    pub fn guess_for_filename<P: AsRef<Path>>(path: P) -> Compression {
        match path.as_ref().extension() {
            Some(ext) if ext == "gz" => Compression::Gzip,
            _ => Compression::None,
        }
    }
}

#[derive(Debug)]
enum Input<R>
where
    R: BufRead,
{
    // The buffer is unused, stored only for uniformity so the input always owns the buffer.
    Plain(R, Buffer),
    Compressed(BufReader<flate2::bufread::GzDecoder<R>>),
}

impl<R> Input<R>
where
    R: BufRead,
{
    fn into_inner(self) -> (R, Buffer) {
        match self {
            Input::Plain(r, buf) => (r, buf),
            Input::Compressed(r) => {
                let (r, buf) = r.into_inner_with_buffer();
                (r.into_inner(), buf)
            }
        }
    }
}

impl<R> Read for Input<R>
where
    R: BufRead,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Input::Plain(r, _) => r.read(buf),
            Input::Compressed(r) => r.read(buf),
        }
    }
}

impl<R> BufRead for Input<R>
where
    R: BufRead,
{
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        match self {
            Input::Plain(r, _) => r.fill_buf(),
            Input::Compressed(r) => r.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Input::Plain(r, _) => r.consume(amt),
            Input::Compressed(r) => r.consume(amt),
        }
    }
}

/// A streaming WARC record.
///
/// The header of the record is accessible via the [`Self::header`] method, and its
/// payload is accessible through the [`Read`] impl.
///
/// When done reading the payload, call [`Self::finish`] to advance the underlying
/// reader past this record. This also automatically happens when the record
/// is dropped, but the [`Drop`] impl will panic on error so you should
/// explicitly call [`Self::finish`] if you wish to handle I/O errors at that point.
///
/// The input stream is guaranteed to have been read to the end of the record, including to
/// the end of the compressed stream if the input is gzipped, when the record is finished.
#[derive(Debug)]
pub struct Record<R>
where
    R: BufRead,
{
    /// The parsed record header.
    pub header: crate::header::Header,
    /// The record Content-Length in bytes
    content_length: u64,
    /// The number of bytes left to read in the record body
    bytes_remaining: u64,
    input: Input<R>,
    debug_info: DebugInfo,
}

#[cfg(debug_assertions)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct DebugInfo {
    /// Whether the record tail has already been consumed.
    ///
    /// This field is used in debug builds to ensure that the Drop logic is
    /// correctly preventing redundant drops which can incorrectly consume
    /// input data.
    consumed_tail: bool,
}

#[cfg(not(debug_assertions))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct DebugInfo;

impl DebugInfo {
    #[cfg(debug_assertions)]
    fn new() -> DebugInfo {
        DebugInfo {
            consumed_tail: false,
        }
    }

    #[cfg(debug_assertions)]
    fn set_consumed_tail(&mut self) {
        debug_assert!(!self.consumed_tail, "Record tail was already consumed!");
        self.consumed_tail = true;
    }

    #[cfg(not(debug_assertions))]
    fn new() -> DebugInfo {
        DebugInfo
    }

    #[cfg(not(debug_assertions))]
    fn set_consumed_tail(&mut self) {}
}

/// Read data from the record body.
impl<R> Read for Record<R>
where
    R: BufRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let constrained = if (buf.len() as u64) > self.bytes_remaining {
            &mut buf[..self.bytes_remaining as usize]
        } else {
            buf
        };

        let n = self.input.read(constrained)?;
        self.bytes_remaining -= n as u64;
        Ok(n)
    }
}

/// Read data from the record body, using the underlying input's buffer.
impl<R> BufRead for Record<R>
where
    R: BufRead,
{
    fn fill_buf(&mut self) -> Result<&[u8], IoError> {
        let buf = self.input.fill_buf()?;
        let remaining = self.bytes_remaining as usize;
        let out = if buf.len() > remaining {
            &buf[..remaining]
        } else {
            buf
        };

        debug_assert!(out.len() <= remaining);
        Ok(out)
    }

    fn consume(&mut self, n: usize) {
        debug_assert!(n <= self.bytes_remaining as usize);
        self.input.consume(n);
        self.bytes_remaining -= n as u64;
    }
}

/// Errors that might occur when closing a record.
#[derive(Debug)]
pub enum FinishError {
    /// The record tail (CRLF CRLF) was not present.
    ///
    /// This may be because the record is malformed and lacks the tail, or the
    /// input is truncated. Lenient applications may wish to ignore this error.
    MissingTail,
    /// An I/O error occurred.
    Io(IoError),
}

impl From<IoError> for FinishError {
    fn from(e: IoError) -> FinishError {
        FinishError::Io(e)
    }
}

impl fmt::Display for FinishError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Error closing WARC record: ")?;
        match self {
            FinishError::Io(ref e) => write!(f, "I/O error: {}", e),
            FinishError::MissingTail => write!(f, "missing record tail"),
        }
    }
}

impl StdError for FinishError {
    fn cause(&self) -> Option<&dyn StdError> {
        if let FinishError::Io(e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl<R> Record<R>
where
    R: BufRead,
{
    /// Read a record from an input stream.
    ///
    /// This allocates a new buffer on every invocation, which can be costly.
    /// If reading many records, prefer to use [`Self::read_buffered_from`] to reuse a
    /// buffer across records.
    pub fn read_from(reader: R, compression: Compression) -> Result<Self, InvalidRecord> {
        let buffer = Buffer::with_capacity(8 << 10);
        Self::read_buffered_from(reader, buffer, compression)
    }

    /// Read a record from an input stream with a user-provided buffer.
    ///
    /// The provided buffer will be returned so it can be reused by the caller, which is useful
    /// for performance because it can be allocated once and used many times. This needs to
    /// (slightly awkwardly) take ownership of the buffer because the current buffering API does
    /// not accept a mutable reference to a buffer.
    pub fn read_buffered_from(
        reader: R,
        mut buffer: Buffer,
        compression: Compression,
    ) -> Result<Self, InvalidRecord> {
        let mut input = match compression {
            Compression::None => Input::Plain(reader, buffer),
            Compression::Gzip => {
                buffer.clear();
                Input::Compressed(BufReader::with_buffer(
                    buffer,
                    flate2::bufread::GzDecoder::new(reader),
                ))
            }
        };

        let header = get_record_header(&mut input)?;
        let len = match header.content_length_lenient() {
            None => {
                return Err(InvalidRecord::UnknownLength(
                    header.get_field_bytes("Content-Length").map(|bytes| bytes.to_vec()),
                ));
            }
            Some(n) => n,
        };

        let record = Record {
            content_length: len,
            bytes_remaining: len,
            header,
            input,
            debug_info: DebugInfo::new(),
        };
        Ok(record)
    }

    /// Get the expected length of the record body.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.content_length
    }

    /// Advance the input reader past this record's payload and return the input.
    ///
    /// This method **must be called** if the caller wants to continue reading from
    /// the input following this record. If not, the input stream may be left somewhere in
    /// the middle of the record, and the exact location is not predictable.
    ///
    /// Expects there to be two newlines following the payload as specified by
    /// the WARC standard, but is tolerant of having none- if missing
    /// `FinishError::MissingTail` will be returned. Regardless of the presence
    /// of a correct tail however, bytes will be consumed which may cause some
    /// of the following data to be lost.
    pub fn finish(mut self) -> Result<(R, Buffer), FinishError> {
        self.finish_internal()?;
        let Record { input, .. } = self;

        Ok(input.into_inner())
    }

    /// Actual drop implementation.
    ///
    /// This is separate from finish() so that can take ownership of self
    /// (which it must in order to prevent a redundant drop after being called)
    /// but the Drop impl can still call this because Drop only takes self by
    /// reference.
    ///
    /// We need to prevent redundant drops because it has side effects
    /// (consuming the tail) and we don't want to need a flag indicating
    /// whether we've already consume the tail (but we do use one to check
    /// correctness of this code in debug builds).
    fn finish_internal(&mut self) -> Result<(), FinishError> {
        let mut buf = [0u8; SKIP_BUF_LEN];
        let mut remaining = self.bytes_remaining;

        while remaining > 0 {
            let n = cmp::min(buf.len(), remaining as usize);
            self.input.read_exact(&mut buf[..n])?;
            remaining -= n as u64;
        }

        self.debug_info.set_consumed_tail();
        {
            let mut buf = [0u8; 4];
            if let Err(e) = self.input.read_exact(&mut buf[..]) {
                if e.kind() == ::std::io::ErrorKind::UnexpectedEof {
                    return Err(FinishError::MissingTail);
                }
                return Err(e.into());
            }

            if &buf[..] != b"\r\n\r\n" {
                return Err(FinishError::MissingTail);
            }
        }

        // Advance the input to the very end of the compressed stream if compressed; this ensures
        // that the reader advances past the gzip trailer so a user won't trip over them when
        // trying to resume reading another record following this one.
        if let Input::Compressed(ref mut input) = self.input {
            loop {
                let n = input.fill_buf()?.len();
                if n == 0 {
                    break;
                }
                trace!(
                    "compressed record finish consuming {} extra bytes",
                    buf.len()
                );
                input.consume(n);
            }
        }

        Ok(())
    }
}

pub(crate) struct RecordWriter<W> {
    limit: u64,
    written: u64,
    writer: W,
    finished: bool,
}

impl<W> RecordWriter<W> {
    pub fn new(writer: W, content_length: u64) -> Self {
        RecordWriter {
            limit: content_length,
            written: 0,
            writer,
            finished: false,
        }
    }
}

impl<W: Write> Write for RecordWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        debug_assert!(self.written <= self.limit);
        let take = std::cmp::min(buf.len() as u64, self.limit - self.written);

        let written = self.writer.write(&buf[..take as usize])?;
        self.written += written as u64;

        // A record is always followed by CRLF2
        if self.written == self.limit && !self.finished {
            self.writer.write_all(b"\r\n\r\n")?;
            self.finished = true;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W> Drop for RecordWriter<W> {
    fn drop(&mut self) {
        if self.written < self.limit {
            error!(
                "record contents wrote only {} bytes but expected {}",
                self.written, self.limit
            );
        }
    }
}

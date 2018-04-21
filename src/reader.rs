use std::cmp;
use std::error::Error as StdError;
use std::fmt;
use std::io::prelude::*;
use std::io::{Error as IoError, Result as IoResult};
use std::marker::PhantomData;
use std::ops::Drop;
use super::ParseError;

/// The number of bytes to skip per read() call when closing a record.
///
/// Larger values require more memory but will reduce overhead.
const SKIP_BUF_LEN: usize = 4096;

/// An error in reading a record from an input stream.
#[derive(Debug)]
pub enum InvalidRecord {
    /// The header of the record was malformed.
    ///
    /// This may mean the input doesn't actually contain WARC records.
    InvalidHeader(ParseError),
    /// The length of the payload could not be determined.
    ///
    /// Contained value is the contents of the Content-Length header.
    UnknownLength(Option<Vec<u8>>),
    /// Reached the end of the input stream.
    EndOfStream,
    /// Other I/O error.
    IoError(IoError),
}

impl From<ParseError> for InvalidRecord {
    fn from(e: ParseError) -> InvalidRecord {
        match e {
            ParseError::IoError(e) => InvalidRecord::IoError(e),
            ParseError::NoMoreData => InvalidRecord::EndOfStream,
            e => InvalidRecord::InvalidHeader(e),
        }
    }
}

impl StdError for InvalidRecord {
    fn description(&self) -> &str {
        use self::InvalidRecord::*;

        match self {
            &InvalidHeader(_) => "invalid or malformed WARC record",
            &UnknownLength(_) => "missing or malformed record Content-Length",
            &EndOfStream => "unexpected end of input",
            &IoError(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match self {
            &InvalidRecord::IoError(ref e) => Some(e),
            &InvalidRecord::InvalidHeader(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for InvalidRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Error reading WARC record: ")?;

        use self::InvalidRecord::*;
        match self {
            &InvalidHeader(ref e) => write!(f, "invalid record header: {}", e),
            &UnknownLength(None) => write!(f, "record missing required Content-Length header"),
            &UnknownLength(Some(ref bytes)) => {
                write!(f,
                       "illegal numeric value for Content-Length: {}",
                       String::from_utf8_lossy(bytes))
            }
            &EndOfStream => write!(f, "unexpected end of input"),
            &IoError(ref e) => write!(f, "I/O error: {}", e),
        }
    }
}

/// A streaming WARC record.
///
/// The header of the record is accessible via the `header` method, and its
/// payload is accessible through the `Read` impl.
///
/// When done reading the payload, call `finish` to advance the underlying
/// reader past this record. This also automatically happens when the record
/// is dropped, but the `Drop` impl will panic on error so you should
/// explicitly call `finish` if you wish to handle I/O errors at that point.
// Efficient seeking if we have an uncompressed file or compressed file with
// CDX index is impossible here. We'll need a BufRead-like trait to cover
// those.
#[derive(Debug)]
pub struct Record<'a, R>
    where R: 'a + BufRead
{
    /// The parsed record header.
    pub header: super::Header,
    bytes_remaining: u64,
    reader: R,
    marker: PhantomData<&'a mut R>,
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
        DebugInfo { consumed_tail: false }
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

impl<'a, R> Read for Record<'a, R>
    where R: 'a + BufRead
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, IoError> {
        let constrained = if (buf.len() as u64) > self.bytes_remaining {
            &mut buf[..self.bytes_remaining as usize]
        } else {
            buf
        };

        let n = self.reader.read(constrained)?;
        self.bytes_remaining -= n as u64;
        Ok(n)
    }
}

impl<'a, R> BufRead for Record<'a, R>
    where R: 'a + BufRead
{
    fn fill_buf(&mut self) -> Result<&[u8], IoError> {
        let buf = self.reader.fill_buf()?;
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
        assert!(n <= self.bytes_remaining as usize);
        self.reader.consume(n);
        self.bytes_remaining -= n as u64;
    }
}

impl<'a, R> Drop for Record<'a, R>
    where R: 'a + BufRead
{
    fn drop(&mut self) {
        if let Err(e) = self.finish_internal() {
            error!("{:?} while closing WARC record", e);
        }
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
        if let &FinishError::Io(ref e) = self {
            write!(f, "I/O error: {}", e)
        } else {
            write!(f, "{}", self.description())
        }
    }
}

impl StdError for FinishError {
    fn description(&self) -> &str {
        use self::FinishError::*;
        match self {
            &MissingTail => "missing record tail",
            &Io(ref e) => e.description(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        if let &FinishError::Io(ref e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl<'a, R> Record<'a, R>
    where R: 'a + BufRead
{
    /// Read a record from an input stream.
    ///
    /// Because the record ensures the input is advanced pass the payload when
    /// it goes out of scope, the reader is inaccessible as long as the record
    /// is live if a reference is provided.
    pub fn read_from(mut reader: R) -> Result<Self, InvalidRecord> {
        let header = super::get_record_header(&mut reader)?;
        let len = match header.content_length() {
            None => {
                return Err(InvalidRecord::UnknownLength(header.field("content-length")
                                                            .map(|bytes| bytes.to_vec())))
            }
            Some(n) => n,
        };

        Ok(Record {
               bytes_remaining: len,
               header: header,
               reader: reader,
               marker: PhantomData,
               debug_info: DebugInfo::new(),
           })
    }

    /// Advance the input reader past this record's payload.
    ///
    /// Unlike the `Drop` impl, this method returns a `Result` so you can
    /// handle I/O errors that may occur while advancing the input.
    ///
    /// Expects there to be two newlines following the payload as specified by
    /// the WARC standard, but is tolerant of having none- if missing
    /// `FinishError::MissingTail` will be returned. Regardless of the presence
    /// of a correct tail however, bytes will be consumed which may cause some
    /// of the following data to be lost.
    pub fn finish(mut self) -> Result<(), FinishError> {
        self.finish_internal()?;
        // Manually deconstruct self to prevent the redundant drop but still
        // ensure members are dropped as necessary.
        let Record { header: _, bytes_remaining: _, reader: _, marker: _, debug_info: _ } = self;

        Ok(())
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
            self.reader.read_exact(&mut buf[..n])?;
            remaining -= n as u64;
        }

        self.debug_info.set_consumed_tail();
        {
            let mut buf = [0u8; 4];
            match self.reader.read_exact(&mut buf[..]) {
                Err(e) => {
                    if e.kind() == ::std::io::ErrorKind::UnexpectedEof {
                        return Err(FinishError::MissingTail);
                    }
                    return Err(e.into());
                }
                Ok(_) => {}
            }
            if &buf[..] != b"\r\n\r\n" {
                return Err(FinishError::MissingTail);
            }
        }

        Ok(())
    }
}

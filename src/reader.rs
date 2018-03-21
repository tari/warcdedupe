use std::cmp;
use std::io::prelude::*;
use std::io::Result as IoResult;
use std::ops::Drop;

/// The number of bytes to skip per read() call when closing a record.
///
/// Larger values require more memory but will reduce overhead.
const SKIP_BUF_LEN: usize = 4096;

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
pub struct Record<'a, R>
    where R: 'a + BufRead
{
    bytes_remaining: u64,
    header: super::Header,
    reader: &'a mut R,
}

impl<'a, R> Drop for Record<'a, R>
    where R: 'a + BufRead
{
    fn drop(&mut self) {
        self.finish().expect("Error while closing Record");
    }
}

impl<'a, R> Record<'a, R>
    where R: 'a + BufRead
{
    /// Read a record from an input stream.
    ///
    /// Because the record ensures the input is advanced pass the payload when
    /// it goes out of scope, the reader is inaccessible as long as the record
    /// is live.
    pub fn read_from(reader: &'a mut R) -> Result<Self, super::ParseError> {
        unimplemented!();
    }

    /// Advance the input reader past this record's payload.
    ///
    /// Unlike the `Drop` impl, this method returns a `Result` so you can
    /// handle I/O errors that may occur while advancing the input.
    pub fn finish(&mut self) -> IoResult<()> {
        let mut buf = [0u8; SKIP_BUF_LEN];
        let mut remaining = self.bytes_remaining;

        while remaining > 0 {
            let n = cmp::min(buf.len(), remaining as usize);
            self.reader.read_exact(&mut buf[..n])?;
            remaining -= n as u64;
        }

        Ok(())
    }
}

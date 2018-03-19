//! Grammar per ISO-28500:2016 (WARC 1.1):
//!
//! ```
//! warc-file   = 1*warc-record
//! warc-record = header CRLF
//!               block CRLF CRLF
//! header      = version warc-fields
//! version     = "WARC/1.1" CRLF
//! warc-fields = *named-field CRLF
//! block       = *OCTET
//!
//! named-field = field-name ":" [ field-value ]
//! field-name  = token
//! field-value = *( field-content | LWS )
//! field-content = ...
//! OCTET = <any bytes>
//! token = 1*<any ASCII, except CTLs or separators>
//! separators = [()<>@,;:\\"/[\]?={} \t]
//! TEXT = <any except CTL>
//! CHAR = <UTF-8 characters>
//! DIGIT = [0-9]
//! CTL = [\x00-\x1F]|\x7F
//! CR = \r
//! LF = \n
//! SP = " "
//! HT = \t
//! CRLF = CR LF
//! LWS [CRLF] 1*( SP | HT )
//! quoted-string = ( <"> * (dqtext | quoted-pair ) <"> )
//! qdtext = <any TEXT except <">>
//! quoted-pair = "\" CHAR
//! uri = <'URI' per RFC3986>
//! ```

extern crate errno;
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate libflate;
#[macro_use]
extern crate log;
extern crate regex;

use failure::Error;
use std::io::BufRead;
use std::str::{self, FromStr};
use regex::bytes::Regex;

//mod copy;
//mod file;

fn main() {
    println!("Hello, world!");
}

#[derive(Debug, PartialEq)]
pub enum InvalidHeader {
    /// The WARC/m.n signature is not present or invalid.
    InvalidSignature,
    /// A header field was malformed or truncated.
    MalformedField,
}

impl std::fmt::Display for InvalidHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        unimplemented!();
    }
}

impl std::error::Error for InvalidHeader {
    fn description(&self) -> &str {
        unimplemented!();
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    version: Version,
    fields: Vec<Field>,
}

impl Header {
    /// Parse a header from bytes, returning the header and the number of bytes consumed.
    pub fn parse(mut bytes: &[u8]) -> Result<Header, InvalidHeader> {
        // version, fields, CRLF
        let (mut bytes_consumed, version) = Version::parse(bytes)?;
        bytes = &bytes[bytes_consumed..];

        let mut fields: Vec<Field> = Vec::new();
        while &bytes[..2] != b"\r\n" {
            let (n, field) = Field::parse(bytes)?;
            bytes_consumed += n;
            bytes = &bytes[n..];
            fields.push(field);
        }

        Ok(Header {
               version: version,
               fields: fields,
           })
    }
}

/// The version of a WARC record.
///
/// Versions 0.9, 1.0 and 1.1 are all well-known, corresponding to the IIPC draft
/// WARC specification, ISO 28500 and ISO 28500:2016, respectively.
///
/// No particular value for the version is assumed, just that one is specified.
/// Users should validate the version number if desired (such as to ignore records
/// with newer versions).
#[derive(Debug, PartialEq, Eq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
}

impl Version {
    /// Parse the version line from a record header, returning the number of bytes
    /// consumed and the version.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Version), InvalidHeader> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^WARC/(\d+)\.(\d+)\r\n")
                .expect("Version regex invalid");
        }
        fn bytes_to_u32(bytes: &[u8]) -> Result<u32, InvalidHeader> {
            match str::from_utf8(bytes).map(u32::from_str) {
                Ok(Ok(x)) => Ok(x),
                Err(_) | Ok(Err(_)) => Err(InvalidHeader::InvalidSignature),
            }
        }

        match RE.captures(bytes) {
            None => Err(InvalidHeader::InvalidSignature),
            Some(m) => {
                let version = Version {
                    major: bytes_to_u32(&m[1])?,
                    minor: bytes_to_u32(&m[2])?,
                };
                let bytes_consumed = m[0].len();

                Ok((bytes_consumed, version))
            }
        }
    }
}

/// A header field.
///
/// The name of a field is case-insensitive, and its value may be any bytes.
#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    name: String,
    value: Vec<u8>,
}

impl Field {
    /// Construct a field with the given name and value.
    pub fn new<S: AsRef<str>>(name: S, value: Vec<u8>) -> Field {
        Field {
            name: name.as_ref().to_lowercase(),
            value: value,
        }
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the field value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Get a mutable reference to the field value.
    pub fn value_mut(&mut self) -> &mut Vec<u8> {
        &mut self.value
    }

    /// Parse a Field from bytes.
    ///
    /// Returns the number of bytes consumed and the parsed field on succes.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Field), InvalidHeader> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^([a-zA-Z_\-]+): *(.*?)\r\n")
                .expect("Field regex invalid");
            static ref CONTINUATION: Regex = Regex::new(r"^[ \t]+(.*?)\r\n")
                .expect("Continuation regex invalid");
        }

        let m = match RE.captures(bytes) {
            None => {
                debug!("Header regex did not match");
                return Err(InvalidHeader::MalformedField);
            }
            Some(c) => c,
        };
        let name = unsafe {
            // RE only matches a subset of ASCII, so we're also guaranteed that
            // the name is valid UTF-8 as long as there was a match.
            debug_assert!(m[1].iter().all(std::ascii::AsciiExt::is_ascii));
            str::from_utf8_unchecked(&m[1])
        };
        let mut bytes_taken = m[0].len();
        let mut value: Vec<u8> = m[2].to_owned();

        // Handle multiline values
        while let Some(m) = CONTINUATION.captures(&bytes[bytes_taken..]) {
            trace!("Multiline header detected, continuing with {:?}", m);
            value.extend(&m[1]);
            bytes_taken += m[0].len();
        }

        trace!("Got header {}: {:?}", name, value);
        Ok((bytes_taken, Field::new(name, value)))
    }
}

#[test]
fn can_read_multi_record_gzip() {
    use libflate::finish::Complete;
    use libflate::gzip;
    use std::io::{self, Read, Seek, SeekFrom, Write};

    const MESSAGES: &[&'static [u8]] = &[b"Hello, world!", b"This is the second message."];

    let compressed: Vec<u8> = vec![];
    let mut cursor = io::Cursor::new(compressed);
    for message in MESSAGES {
        let mut encoder = gzip::Encoder::new(&mut cursor).unwrap();
        encoder.write_all(message).unwrap();
        encoder.complete().unwrap();
    }

    cursor.seek(SeekFrom::Start(0)).unwrap();
    for message in MESSAGES.iter() {
        let mut decoder = gzip::Decoder::new(&mut cursor).unwrap();
        let mut buf: Vec<u8> = vec![];
        decoder.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, message);
    }
}

pub fn get_record_header<R: BufRead>(mut reader: R) -> Result<Header, Error> {
    /// Return the index of the first position in the given buffer following
    /// a b"\r\n\r\n" sequence.
    fn find_crlf2(buf: &[u8]) -> Option<usize> {
        for (i, window) in buf.windows(4).enumerate() {
            if window == b"\r\n\r\n" {
                return Some(i + 4);
            }
        }
        None
    }

    // Read bytes out of the input reader until we find the end of the header
    // (two CRLFs in a row).
    // First-chance: without copying anything
    let mut header: Option<(usize, Header)> = None;
    {
        let buf = reader.fill_buf()?;
        if let Some(i) = find_crlf2(buf) {
            // Weird split of parse and consume here is necessary because buf
            // is borrowed from the reader so we can't consume until we no
            // longer hold a reference to the buffer.
            header = Some((i, Header::parse(&buf[..i])?));
        }
    }
    if let Some((sz, header)) = header {
        reader.consume(sz);
        return Ok(header);
    }

    // Need to start copying out of the reader's buffer. Throughout this loop,
    // we've grabbed some number of bytes and own them with a tail copied out
    // of the reader's buffer but still buffered so we can give bytes back at
    // the end.
    let mut buf: Vec<u8> = Vec::new();
    buf.extend(reader.fill_buf()?);
    reader.consume(buf.len());

    let mut bytes_consumed = buf.len();
    loop {
        // Copy out of the reader
        buf.extend(reader.fill_buf()?);
        if buf.len() == bytes_consumed {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,
                                           "WARC header not terminated")
                               .into());
        }

        if let Some(i) = find_crlf2(&buf) {
            // If we hit, consume up to the hit and done.
            // Our buffer is larger than the reader's: be careful only to
            // consume what the reader gave us most recently, which we haven't
            // taken ownership of yet.
            reader.consume(i - bytes_consumed);
            return Ok(Header::parse(&buf[..i])?);
        }

        // Otherwise keep looking
        reader.consume(buf.len() - bytes_consumed);
        bytes_consumed = buf.len();
        // TODO enforce maximum vec size?
    }
}

#[test]
fn can_read_record_header() {
    let header = b"WARC/1.0\r\n\
                   Warc-Type: testdata\r\n\
                   Content-Length: 6\r\n\
                   X-Multiline-Test:lol \r\n  multiline headers\r\n\
                   \r\n";

    fn field(name: &str, value: &[u8]) -> Field {
        Field::new(name, value.to_owned())
    }
    assert_eq!(get_record_header(&header[..]).expect("Should be valid"),
               Header {
                   version: Version {
                       major: 1,
                       minor: 0,
                   },
                   fields: vec![field("Warc-Type", b"testdata"),
                                field("Content-Length", b"6"),
                                field("X-Multiline-Test", b"lol multiline headers")],
               });
}

#[test]
fn extra_buffering_works() {
    use std::io::{self, Result};
    /// A type to probe the buffering behavior of `get_record_header`.
    ///
    /// On each `fill_buf` call it transitions to the next state, and
    /// after two it is in the terminal state.
    #[derive(Debug, PartialEq)]
    enum DoubleBuffer<'a> {
        /// Nothing read yet.
        Start(&'a [u8], &'a [u8]),
        /// One whole buffer read.
        Second(&'a [u8]),
        /// Both buffers read, with n bytes read from the second.
        Done(usize),
    }
    // Only because BufRead: Read
    impl<'a> io::Read for DoubleBuffer<'a> {
        fn read(&mut self, _: &mut [u8]) -> Result<usize> {
            unimplemented!();
        }
    }
    impl<'a> BufRead for DoubleBuffer<'a> {
        fn fill_buf(&mut self) -> Result<&[u8]> {
            eprintln!("fill_buf {:?}", self);
            match self {
                &mut DoubleBuffer::Start(fst, _) => Ok(fst),
                &mut DoubleBuffer::Second(snd) => Ok(snd),
                &mut DoubleBuffer::Done(_) => panic!("Should not fill after snd"),
            }
        }

        fn consume(&mut self, amt: usize) {
            eprintln!("consume {} {:?}", amt, self);
            let next = match *self {
                DoubleBuffer::Start(fst, snd) => {
                    assert_eq!(amt, fst.len());
                    DoubleBuffer::Second(snd)
                }
                DoubleBuffer::Second(snd) => DoubleBuffer::Done(snd.len() - amt),
                DoubleBuffer::Done(_) => panic!("Should not consume after snd"),
            };
            *self = next;
        }
    }

    let mut reader = DoubleBuffer::Start(b"WARC/1.0\r\n\
                                           X-First-Header: yes\r\n",
                                         b"X-Second-Header:yes\r\n\
                                           \r\n\
                                           IGNORED_DATA");
    get_record_header(&mut reader).expect("failed to parse valid header");
    assert_eq!(reader, DoubleBuffer::Done(12));
}

#[test]
fn incorrect_signature_is_invalid() {
    assert_eq!(Version::parse(b"\x89PNG\r\n\x1a\n"),
               Err(InvalidHeader::InvalidSignature));
    assert_eq!(Version::parse(b"WARC/1.0a\r\n"),
               Err(InvalidHeader::InvalidSignature));
}

#[test]
fn truncated_header_is_invalid() {
    use std::error::Error;
    const BYTES: &[u8] = b"WARC/1.1\r\n
                           Warc-Type: testdata\r\n\r";

    let err = get_record_header(BYTES)
        .expect_err("Record parsing should fail")
        .downcast::<std::io::Error>()
        .expect("should be IoError");

    assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(err.description(), "WARC header not terminated");
}

#[test]
fn invalid_fields_are_invalid() {
    assert_eq!(Field::parse(b"This is not a valid field"),
               Err(InvalidHeader::MalformedField));

    assert_eq!(Field::parse(b"X-Invalid-UTF-8\xFF: yes"),
               Err(InvalidHeader::MalformedField));
}

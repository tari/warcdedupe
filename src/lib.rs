//! Grammar per ISO-28500:2016 (WARC 1.1):
//!
//! ```text
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
#![deny(missing_docs)]

#[cfg(feature = "chrono")]
extern crate chrono;
extern crate errno;
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate libflate;
#[macro_use]
extern crate log;
extern crate regex;
extern crate twoway;

use std::collections::HashMap;
use std::io::BufRead;
use std::str::{self, FromStr};
use regex::bytes::Regex;

//mod copy;
//mod file;
/// Reading WARC records from files or streams.
pub mod reader;
#[cfg(test)]
mod tests;

/// Reasons it may be impossible to parse a WARC header.
#[derive(Debug)]
pub enum ParseError {
    /// The WARC/m.n signature is not present or invalid.
    InvalidSignature,
    /// A header field was malformed or truncated.
    MalformedField,
    /// An I/O error occured while trying to read the input.
    IoError(std::io::Error),
}

impl std::cmp::PartialEq for ParseError {
    fn eq(&self, other: &Self) -> bool {
        use std::error::Error;
        use ParseError::*;

        match (self, other) {
            (&InvalidSignature, &InvalidSignature) |
            (&MalformedField, &MalformedField) => true,
            (&IoError(ref e1), &IoError(ref e2)) => {
                e1.kind() == e2.kind() && e1.description() == e2.description()
            }
            (_, _) => false,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use std::error::Error;
        write!(f, "Invalid WARC header: {}", self.description())
    }
}

impl std::error::Error for ParseError {
    fn description(&self) -> &str {
        use ParseError::*;
        match *self {
            InvalidSignature => "WARC signature is missing or invalid",
            MalformedField => "Header field is malformed or truncated",
            IoError(_) => "I/O error",
        }
    }
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> ParseError {
        ParseError::IoError(e)
    }
}

/// The header of a WARC record.
#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    version: Version,
    fields: HashMap<String, Vec<u8>>,
}

impl Header {
    /// Parse a header from bytes, returning the header and the number of bytes consumed.
    pub fn parse(mut bytes: &[u8]) -> Result<Header, ParseError> {
        // version, fields, CRLF
        let (mut bytes_consumed, version) = Version::parse(bytes)?;
        bytes = &bytes[bytes_consumed..];

        let mut fields = HashMap::new();
        while &bytes[..2] != b"\r\n" {
            let (n, field) = Field::parse(bytes)?;
            bytes_consumed += n;
            bytes = &bytes[n..];
            fields.insert(field.name, field.value);
        }

        Ok(Header {
               version: version,
               fields: fields,
           })
    }

    fn field_str(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(|b| str::from_utf8(b).ok())
    }

    /// Get the WARC-Record-ID field value.
    /// 
    /// Returns `None` if the field is absent or is not a valid `str`.
    /// 
    /// This is a mandatory field, but in the interest of parsing leniency is
    /// is not required to exist or have any particular value in order to parse
    /// a record header.
    /// 
    /// Note that a valid value is assumed to be a valid `str` because the
    /// record ID is specified to be a RFC 3986 URI, which are always valid
    /// ASCII (and therefore UTF-8) strings when well-formed.
    pub fn record_id(&self) -> Option<&str> {
        self.field_str("warc-record-id")
    }

    /// Get the Content-Length field value.
    /// 
    /// Returns `None` if the field is absent or does not represent a valid
    /// content length.
    /// 
    /// This is a mandatory field, but in the interest of parsing leniency it
    /// is not required to exist or have any particular value in order to parse
    /// a record header.
    pub fn content_length(&self) -> Option<u64> {
        self.field_str("content-length")
            .and_then(|s| str::parse::<u64>(s).ok())
    }

    /// Get the WARC-Date field value.
    /// 
    /// Returns `None` if the field is absent or does not represent a valid
    /// `DateTime`. If you prefer to get this value as a string instead,
    /// disable the `chrono` feature for this crate.
    /// 
    /// This is a mandatory field, but in the interest of parsing leniency it
    /// is not required to exist or have any particular value in order to parse
    /// record header.
    #[cfg(feature = "chrono")]
    pub fn warc_date(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        // YYYY-MM-DDThh:mm:ssZ per WARC-1.0. This is valid RFC3339, which is
        // itself valid ISO 8601. We're slightly lenient in accepting non-UTC
        // zone offsets.
        use chrono::{DateTime, Utc};
        self.field_str("warc-date")
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// Get the WARC-Date field value.
    /// 
    /// Returns `None` if the field is absent or is not a valid string. If you
    /// prefer to get this valid as a parsed datetime instead, enable the
    /// `chrono` feature for this crate.
    /// 
    /// This is a mandatory field, but in the interest of parsing leniency it
    /// is not required to exist or have any particular value in order to parse
    /// record header.
    #[cfg(not(feature = "chrono"))]
    pub fn warc_date(&self) -> Option<&str> {
        self.field_str("warc-date")
    }

    /// Get the WARC-Type field value.
    /// 
    /// This is a mandatory field, but in the interest of parsing leniency it
    /// is not required to exist or be a valid string in order to parse a
    /// record header.
    /// 
    /// The WARC specification non-exhausively defines the following record
    /// types:
    /// 
    ///  * warcinfo
    ///  * response
    ///  * resource
    ///  * request
    ///  * metadata
    ///  * revisit
    ///  * conversion
    ///  * continuation
    /// 
    /// Additional types are permitted as core format extensions. Creators of
    /// extensions are encouraged by the standard to discuss their intentions
    /// within the IIPC.
    pub fn warc_type(&self) -> Option<&str> {
        self.field_str("warc-type")
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
    /// The integer part of the version number.
    ///
    /// In '12.345', this is 12.
    pub major: u32,
    /// The fractional part of the version number.
    ///
    /// In '12.345', this is 345.
    pub minor: u32,
}

impl Version {
    /// Parse the version line from a record header, returning the number of bytes
    /// consumed and the version.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Version), ParseError> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^WARC/(\d+)\.(\d+)\r\n")
                .expect("Version regex invalid");
        }
        fn bytes_to_u32(bytes: &[u8]) -> Result<u32, ParseError> {
            match str::from_utf8(bytes).map(u32::from_str) {
                Ok(Ok(x)) => Ok(x),
                Err(_) | Ok(Err(_)) => Err(ParseError::InvalidSignature),
            }
        }

        match RE.captures(bytes) {
            None => Err(ParseError::InvalidSignature),
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
/// 
/// This type is a convenience for parsing; actual header fields are stored in
/// a map inside the record header.
#[derive(Debug, PartialEq, Eq)]
struct Field {
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

    /// Parse a Field from bytes.
    ///
    /// Returns the number of bytes consumed and the parsed field on succes.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Field), ParseError> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"^([a-zA-Z_\-]+): *(.*?)\r\n")
                .expect("Field regex invalid");
            static ref CONTINUATION: Regex = Regex::new(r"^[ \t]+(.*?)\r\n")
                .expect("Continuation regex invalid");
        }

        let m = match RE.captures(bytes) {
            None => {
                debug!("Header regex did not match");
                return Err(ParseError::MalformedField);
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

/// Parse a WARC record header out of the provided `BufRead`.
///
/// Consumes the bytes that are parsed, leaving the reader at the beginning
/// of the record payload. In case of an error in parsing some or all of the
/// input may be consumed.
pub fn get_record_header<R: BufRead>(mut reader: R) -> Result<Header, ParseError> {
    /// Return the index of the first position in the given buffer following
    /// a b"\r\n\r\n" sequence.
    #[inline]
    fn find_crlf2(buf: &[u8]) -> Option<usize> {
        twoway::find_bytes(buf, b"\r\n\r\n").map(|i| i + 4)
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
            // Read returned 0 bytes
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,
                                           "WARC header not terminated")
                               .into());
        }

        // Only search new data; start from the earliest possible location
        // a crlf2 could appear if it spans the boundary between buffers.
        let start_search = if bytes_consumed > 3 {
            bytes_consumed - 3
        } else {
            0
        };
        if let Some(i) = find_crlf2(&buf[start_search..]) {
            // If we hit, consume up to the hit and done.
            // Our buffer is larger than the reader's: be careful only to
            // consume what the reader gave us most recently, which we haven't
            // taken ownership of yet.
            let match_idx = start_search + i;
            reader.consume(match_idx - bytes_consumed);
            return Ok(Header::parse(&buf[..match_idx])?);
        }

        // Otherwise keep looking
        reader.consume(buf.len() - bytes_consumed);
        bytes_consumed = buf.len();
        // TODO enforce maximum vec size?
    }
}

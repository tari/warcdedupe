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

#[cfg(feature = "chrono")]
extern crate chrono;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

pub mod header;
pub mod record;
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
    /// Reached end of input.
    NoMoreData,
}

impl std::cmp::PartialEq for ParseError {
    fn eq(&self, other: &Self) -> bool {
        use ParseError::*;

        match (self, other) {
            (InvalidSignature, InvalidSignature) | (MalformedField, MalformedField) => true,
            (IoError(e1), IoError(e2)) => e1.kind() == e2.kind(),
            (_, _) => false,
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use ParseError::*;

        write!(f, "Invalid WARC header: ")?;
        match self {
            InvalidSignature => write!(f, "WARC signature is missing or invalid"),
            MalformedField => write!(f, "Header field is malformed or truncated"),
            NoMoreData => write!(f, "No more data available"),
            IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for ParseError {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        if let ParseError::IoError(ref e) = self {
            Some(e)
        } else {
            None
        }
    }
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> ParseError {
        ParseError::IoError(e)
    }
}

/// WARC EBNF "separators" class
const SEPARATORS: &[char] = &[
    '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ', '\t',
];

/// WARC EBNF "CTL" class: ASCII chars 0-31 and DEL (127)
const CTL: &[char] = &[
    '\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08', '\x09', '\x0a', '\x0b',
    '\x0c', '\x0d', '\x0e', '\x0f', '\x10', '\x11', '\x12', '\x13', '\x14', '\x15', '\x16', '\x17',
    '\x18', '\x19', '\x1a', '\x1b', '\x1c', '\x1d', '\x1e', '\x1f', '\x7f',
];

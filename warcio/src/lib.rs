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

use thiserror::Error;

pub mod header;
pub mod record;
#[cfg(test)]
mod tests;

/// Reasons it may be impossible to parse a WARC header.
#[derive(Debug, Error)]
pub enum HeaderParseError {
    /// The WARC/m.n signature is not present or invalid.
    #[error("WARC signature missing or invalid (near \"{0}\")")]
    InvalidSignature(String),
    /// A header field was malformed or truncated.
    #[error("header field is malformed or truncated")]
    MalformedField, // TODO more information?
    /// An I/O error occured while trying to read the input.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    /// Reached end of input.
    #[error("input ended before end of header")]
    Truncated,
}

impl std::cmp::PartialEq for HeaderParseError {
    fn eq(&self, other: &Self) -> bool {
        use HeaderParseError::*;

        match (self, other) {
            (MalformedField, MalformedField) | (Truncated, Truncated) => true,
            (InvalidSignature(x), InvalidSignature(y)) => x == y,
            (IoError(e1), IoError(e2)) => e1.kind() == e2.kind(),
            (_, _) => false,
        }
    }
}

impl HeaderParseError {
    fn invalid_signature(sig_bytes: &[u8]) -> Self {
        HeaderParseError::InvalidSignature(String::from_utf8_lossy(sig_bytes).into_owned())
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

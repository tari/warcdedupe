//! Tools for reading, writing, and working with WARC (Web ARChive) files.
//!
//! ## Background
//!
//! WARC files are used to store digital resources and related information, generally for archival
//! storage. They are most commonly used to store the results of web crawls, wherein a crawler
//! requests resources from any desired web server(s) while storing the request that was sent for
//! each resource, the corresponding response, metadata for each and optionally other related
//! information. WARC files are widely used by organizations involved in web archiving, such as the
//! [Internet Archive](https://archive.org) and the [Library of
//! Congress](https://www.loc.gov/preservation/digital/formats/fdd/fdd000236.shtml).
//!
//! The WARC file format is formalized in an international standard, ISO 28500, which to date has
//! two published versions: ISO 28500:2009 (WARC 1.0) and ISO 28500:2017 (WARC 1.1). The standard
//! was largely developed through the [International Internet Preservation
//! Consortium](https://netpreserve.org/) (IIPC); public discussion of further development and
//! freely-available specifications are made available through the IIPC: see
//! <https://iipc.github.io/warc-specifications/>.
//!
//! ## WARC structure
//!
//! A WARC file is a simple concatenation of records. Each record has a format similar to an HTTP
//! message, consisting of a version declaration, a number of header fields, and any number of bytes
//! of data. A simple record representing an HTTP request might look like this:
//!
//! ```text
//! WARC/1.1
//! WARC-Type: request
//! WARC-Target-URI: https://example.com
//! Content-Type: application/http;msgtype=request
//! WARC-Record-ID: <urn:uuid:e061d11b-fb0a-4314-88c5-54e4870be701>
//! WARC-Date: 2021-08-24T23:19:14Z
//! Content-Length: 135
//!
//! GET /image/png HTTP/1.1
//! User-Agent: Wget/1.21.1
//! Accept: */*
//! Accept-Encoding: identity
//! Host: httpbin.org
//! Connection: Keep-Alive
//!
//!
//!
//! ```
//!
//! Collectively the portion of the record before `GET` in this example is the record header,
//! and the remainder is the record block with the exception of two newlines (each of them `\r\n`)
//! at the end of the record. The first line of the header is the version line (allowing the
//! record to be easily identified as a WARC record and indicating what format version it conforms
//! to), and the remainder of the header is a series of fields each of which has a name and a value.
//!
//! ## Library structure
//!
//! In this library, the [`Header`] type contains the record version and fields. To write a record,
//! a header can be constructed and its [`write_to`](Header::write_to) method will yield a write
//! adapter to which the record block can be written. [`Record::read_from`] will do the opposite
//! operation, reading the header of a record from a read adapter and returning an adapter allowing
//! read access to the record block.

#[cfg(feature = "chrono")]
extern crate chrono;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

use thiserror::Error;

mod header;
pub mod record;
#[cfg(test)]
mod tests;
mod version;

pub use header::{RecordType, FieldName, Header};
pub use record::{Record, Compression};
pub use version::Version;

/// Reasons it may be impossible to parse a WARC header.
#[derive(Debug, Error)]
pub enum HeaderParseError {
    /// The WARC/m.n signature marking the start of a record is not present or invalid.
    ///
    /// The contained value is a UTF-8 interpretation of the data that was attempted to be parsed.
    #[error("WARC signature missing or invalid (near \"{0}\")")]
    InvalidSignature(String),
    /// A header field was malformed or truncated.
    #[error("header field is malformed or truncated")]
    MalformedField, // TODO more information?
    /// An I/O error occured while trying to read the input.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    /// The parser reached the end of the input before the end of the WARC header.
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

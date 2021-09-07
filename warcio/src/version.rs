use std::str::{self, FromStr};

use crate::HeaderParseError;
use std::ops::Range;

/// The version of a WARC record.
///
/// Versions 1.0 and 1.1 are well-known, corresponding to, ISO 28500:2009 and ISO 28500:2016,
/// respectively. Well-known versions can be conveniently referred to with associated constants like
/// [`WARC1_0`](Self::WARC1_0) and [`WARC1_1`](Self::WARC1_1).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
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
    // TODO: WARC 0.9? http://archive-access.sourceforge.net/warc/warc_file_format-0.9.html
    /// WARC 1.0, as specified by ISO 28500:2009.
    pub const WARC1_0: Self = Version { major: 1, minor: 0 };
    /// WARC 1.1, as specified by ISO 28500:2017.
    pub const WARC1_1: Self = Version { major: 1, minor: 1 };

    /// Parse the version line of a record from a buffer, returning the number of bytes
    /// consumed and the parsed version.
    ///
    /// ```
    /// # use warcio::Version;
    /// let buf = b"WARC/1.0\r\n\
    ///             <more here>";
    /// assert_eq!(
    ///     Version::parse(&buf[..]),
    ///     Ok((10, Version::WARC1_0))
    /// );
    /// ```
    pub fn parse(bytes: &[u8]) -> Result<(usize, Version), HeaderParseError> {
        use std::cmp::min;
        fn grab_digits(bytes: &[u8], from: usize) -> Result<usize, HeaderParseError> {
            if bytes[from..].is_empty() {
                // Nothing here; it could be digits which would be valid
                return Err(HeaderParseError::Truncated);
            }
            match bytes[from..].iter().copied().take_while(u8::is_ascii_digit).enumerate().last() {
                // Got at least one digit
                Some((idx, _)) => Ok(from + idx + 1),
                // Have something but the first character is not a digit; that's invalid
                None => Err(HeaderParseError::invalid_signature(&bytes[..min(from + 8, bytes.len())]))
            }
        }
        fn bytes_to_u32(bytes: &[u8], range: Range<usize>) -> Result<u32, HeaderParseError> {
            debug_assert!(&bytes[range.clone()].iter().all(u8::is_ascii_digit),
                "Integer to parse should consist only of ASCII digits");
            let s = unsafe {
                str::from_utf8_unchecked(&bytes[range.clone()])
            };

            match u32::from_str(s) {
                Ok(x) => Ok(x),
                Err(_) => Err(HeaderParseError::invalid_signature(&bytes[..range.end])),
            }
        }
        // This parsing could be implemented as a state machine to save duplicated parsing in the
        // case where a single buffer isn't a complete header, but that's a TODO for later.

        // WARC identifier
        let major_start = 5;
        if !bytes.starts_with(b"WARC/") {
            return Err(HeaderParseError::invalid_signature(
                &bytes[..min(major_start, bytes.len())],
            ));
        }

        // Major version
        let major_end = grab_digits(bytes, major_start)?;
        // .. must be followed by decimal point
        match bytes.get(major_end) {
            Some(b'.') => {/* Decimal point is where we expect it */},
            // Something else is invalid
            Some(_) => return Err(HeaderParseError::invalid_signature(&bytes[..major_end])),
            // No more data, could still be valid
            None => return Err(HeaderParseError::Truncated),
        }
        let major = bytes_to_u32(&bytes, major_start..major_end)?;

        // Minor version
        let minor_start = major_end + 1;
        let minor_end = grab_digits(bytes, minor_start)?;
        // .. must be followed by CRLF
        match bytes[minor_end..] {
            [] | [b'\r'] => return Err(HeaderParseError::Truncated),
            [b'\r', b'\n', ..] => {/* got the desired CRLF */},
            _ => return Err(HeaderParseError::invalid_signature(&bytes[..min(minor_end + 2, bytes.len())]))
        }
        let minor = bytes_to_u32(&bytes, minor_start..minor_end)?;

        Ok((minor_end + 2, Version { major, minor }))
    }
}

/// Construct a Version with parts from a tuple of integers.
impl From<(u32, u32)> for Version {
    fn from((major, minor): (u32, u32)) -> Self {
        Version { major, minor }
    }
}

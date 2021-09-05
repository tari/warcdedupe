use std::str::{self, FromStr};

use crate::HeaderParseError;

/// The version of a WARC record.
///
/// Versions 0.9, 1.0 and 1.1 are all well-known, corresponding to the IIPC draft
/// WARC specification, ISO 28500 and ISO 28500:2016, respectively. Those well-known
/// versions can be conveniently referred to with associated constants like
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
        fn bytes_to_u32(bytes: &[u8]) -> Result<u32, HeaderParseError> {
            match str::from_utf8(bytes).map(u32::from_str) {
                Ok(Ok(x)) => Ok(x),
                Err(_) | Ok(Err(_)) => Err(HeaderParseError::invalid_signature(bytes)),
            }
        }

        if !bytes.starts_with(b"WARC/") {
            return Err(HeaderParseError::invalid_signature(&bytes[..5]));
        }
        let major_start = 5;
        let major_end = match bytes[major_start..].iter().position(|&x| x == b'.') {
            None => {
                return Err(HeaderParseError::invalid_signature(
                    &bytes[..major_start + 4],
                ))
            }
            Some(i) => i + major_start,
        };
        let major = bytes_to_u32(&bytes[major_start..major_end])?;

        let minor_start = major_end + 1;
        let minor_end = match bytes[minor_start..].windows(2).position(|x| x == b"\r\n") {
            None => {
                return Err(HeaderParseError::invalid_signature(
                    &bytes[..minor_start + 4],
                ))
            }
            Some(i) => i + minor_start,
        };
        let minor = bytes_to_u32(&bytes[minor_start..minor_end])?;

        Ok((minor_end + 2, Version { major, minor }))
    }
}

/// Construct a Version with parts from a tuple of integers.
impl From<(u32, u32)> for Version {
    fn from((major, minor): (u32, u32)) -> Self {
        Version { major, minor }
    }
}

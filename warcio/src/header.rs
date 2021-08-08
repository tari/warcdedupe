use std::collections::HashMap;
use std::io::BufRead;
use std::str::{self, FromStr};

use regex::bytes::Regex;
use uncased::{AsUncased, UncasedStr};

use super::{CTL, SEPARATORS};
use crate::record::Compression;
use crate::ParseError;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// The name of a WARC header field.
///
/// Field names are case-insensitive, so the [`Eq`], [`Ord`] and [`Hash`] implementations for this
/// type are all case-insensitive.
#[derive(Debug, Eq, Ord, Clone)]
pub enum FieldName {
    /// WARC-Record-ID: mandatory. A globally unique identifier for a record.
    ///
    /// A URI delimited by angle brackets: `"<" uri ">"`.
    RecordId,
    /// Content-Length: mandatory. The number of octets in the record block.
    ///
    /// One or more ASCII digits.
    ContentLength,
    /// WARC-Date: mandatory. The instant that record data capture began.
    ///
    /// This must be a UTC timestamp according to the W3C profile of ISO 8601,
    /// such as YYYY-MM-DDThh:mm:ssZ. Fractional parts of a second may be included,
    /// but must have between 1 and 9 digits (inclusive) if present.
    Date,
    /// WARC-Type: mandatory. The type of record this is.
    Type,
    /// Content-Type: optional. The RFC 2045 MIME type of the record's data block.
    ///
    /// If absent, readers may attempt to identify the resource type by inspecting
    /// its contents and URI and otherwise treat it as application/octet-stream.
    ContentType,
    /// WARC-Concurrent-To: optional, may appear multiple times. The [`RecordId`] of
    /// any records created as part of the same capture event.
    ConcurrentTo,
    /// WARC-Block-Digest: optional. A `labelled-digest` of the full record block.
    BlockDigest,
    /// WARC-Payload-Digest: optional. A `labelled-digest` of the record payload (usually
    /// the RFC 2616 `entity-body` of an HTTP message).
    PayloadDigest,
    /// WARC-IP-Address: optional. An IP address contacted to retrieve record content.
    IpAddress,
    /// WARC-Refers-To: optional. The record ID of a single record for which the present record
    /// holds additional content.
    ///
    /// May be used with `metadata`, `revisit` or `conversion` record types.
    RefersTo,
    /// WARC-Refers-To-Target-URI: optional. The [`TargetURI`] of the record referred to by
    /// [`RefersTo`].
    RefersToTargetURI,
    /// WARC-Refers-To-Date: optional. The [`Date`] of the record referred to by [`RefersTo`].
    RefersToDate,
    /// WARC-Target-URI: optional. The original URI that provided the record content.
    ///
    /// Usually this is the URI requested by a web crawler.
    TargetURI,
    /// WARC-Truncated: optional. The reason that a record contains a truncated version of the
    /// original resource.
    ///
    /// Writers may wish to limit time or other resources used to retrieve resources, and if the
    /// resource is truncated the reason for truncation should be recorded here.
    Truncated,
    /// WARC-Warcinfo-ID: optional. The ID of the warcinfo record associated with this record.
    ///
    /// Typically used when the context of a record is lost such as by splitting a collection
    /// of records into individual files.
    InfoID,
    /// WARC-Filename: optional. The name of the file containing the current warcinfo record.
    ///
    /// Only used in warcinfo records.
    Filename,
    /// WARC-Profile: optional. The kind of analysis and handling applied in a revisit record.
    Profile,
    /// WARC-Identified-Payload-Type: the content-type discovered by inspecting a record payload.
    IdentifiedPayloadType,
    /// WARC-Segment-Number: the current record's ordering in a sequence of segmented record.
    ///
    /// Mandatory for continuation records.
    SegmentNumber,
    /// WARC-Segment-Origin-ID: the ID of the starting record in a series of segmented records.
    ///
    /// Mandatory for continuation records.
    SegmentOriginID,
    /// WARC-Segment-Total-Length: the total length of concatenated segmented content blocks.
    ///
    /// Mandatory for continuation records.
    SegmentTotalLength,
    /// Any unrecognized field name.
    Other(Box<str>),
}

include!(concat!(env!("OUT_DIR"), "/header_field_types.rs"));

impl Borrow<UncasedStr> for FieldName {
    fn borrow(&self) -> &UncasedStr {
        self.as_ref().as_uncased()
    }
}

// Implementing Borrow requires the same semantics between the borrowed and original versions,
// so Eq, Ord and Hash are implemented in terms of the case-insensitive field name.
impl PartialEq for FieldName {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().as_uncased().eq(other.as_ref())
    }
}

impl PartialOrd for FieldName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().as_uncased().partial_cmp(other.as_ref())
    }
}

impl Hash for FieldName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().as_uncased().hash(state)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RecordType {
    Info,
    Response,
    Resource,
    Request,
    Metadata,
    Revisit,
    Conversion,
    Continuation,
    Other(Box<str>),
}

include!(concat!(env!("OUT_DIR"), "/header_record_types.rs"));

/// The header of a WARC record.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Header {
    version: Version,
    fields: HashMap<FieldName, Vec<u8>>,
}

impl Header {
    pub fn new(major_version: u32, minor_version: u32) -> Self {
        Header {
            version: Version {
                major: major_version,
                minor: minor_version,
            },
            fields: HashMap::new(),
        }
    }

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

        Ok(Header { version, fields })
    }

    /// Write the current record to the given output stream.
    ///
    /// The returned [`Write`] adapter will accept only as many bytes as the `Content-Length`
    /// header declares and will panic if that header is missing or does not have a valid
    /// base-10 integer value (consisting only of ASCII digits).
    ///
    /// While the output adapter will only write up to the given number of bytes, it will
    /// not pad the output if dropped before `Content-Length` bytes have been written. Failing
    /// to write enough data will usually result in data corruption, but an error will also
    /// be emitted to the log.
    pub fn write_to<W: std::io::Write>(
        &self,
        mut dest: W,
        compression: Compression,
    ) -> std::io::Result<impl std::io::Write> {
        use flate2::write::GzEncoder;
        use std::io::{Result as IoResult, Write};

        enum Output<W: Write> {
            Plain(W),
            Gzip(GzEncoder<W>),
        }
        impl<W: Write> Write for Output<W> {
            fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
                match self {
                    Output::Plain(w) => w.write(buf),
                    Output::Gzip(w) => w.write(buf),
                }
            }

            fn flush(&mut self) -> IoResult<()> {
                match self {
                    Output::Plain(w) => w.flush(),
                    Output::Gzip(w) => w.flush(),
                }
            }
        }

        let mut dest = match compression {
            Compression::None => Output::Plain(dest),
            Compression::Gzip => Output::Gzip(GzEncoder::new(dest, flate2::Compression::best())),
        };

        // record header: version followed by fields
        write!(
            dest,
            "WARC/{}.{}\r\n",
            self.version.major, self.version.minor
        )?;
        for (key, value) in self.fields.iter() {
            write!(&mut dest, "{}: ", <FieldName as AsRef<str>>::as_ref(key))?;
            dest.write_all(value)?;
            write!(&mut dest, "\r\n")?;
        }
        write!(&mut dest, "\r\n")?;

        // record body: Content-Length octets of arbitrary data
        Ok(crate::record::RecordWriter::new(
            dest,
            self.content_length()
                .expect("Record::write_to requires the record have a valid Content-Length"),
        ))
    }

    /// Get the value of a header field as bytes, or None if no such header exists.
    ///
    /// Although the WARC specification does not permit values that are not also valid Rust strings,
    /// users that wish to be lenient in accepting malformed records may wish to relax that
    /// requirement by using this function.
    pub fn get_field_bytes<F: AsUncased>(&self, field: F) -> Option<&[u8]> {
        self.fields.get(field.as_uncased()).map(Vec::as_slice)
    }

    /// Get the value of a header field, or None if it does not exist or is not a valid Rust string.
    ///
    /// If you want to handle potentially malformed records, [`get_field_bytes`] allows you to
    /// accept values that are not acceptable Rust strings if the actual value is not important.
    /// However, because such a value would be malformed according to the specification, it should
    /// rarely be required.
    pub fn get_field<F: AsUncased>(&self, field: F) -> Option<&str> {
        str::from_utf8(self.get_field_bytes(field)?).ok()
    }

    pub fn field_exists<F: AsUncased>(&self, field: F) -> bool {
        self.get_field_bytes(field).is_some()
    }

    /// Set the value of a header field, returning the old value (if any).
    ///
    /// This function will panic if the provided name contains characters that are not
    /// permitted in `field-name` context.
    pub fn set_field<N: Into<FieldName>, V: Into<Vec<u8>>>(
        &mut self,
        name: N,
        value: V,
    ) -> Option<Vec<u8>> {
        let name = name.into();
        let name_str: &str = name.as_ref();
        assert!(
            !name_str.contains(SEPARATORS) && !name_str.contains(CTL),
            "field name {:?} contains illegal characters",
            name
        );
        self.fields.insert(name, value.into())
    }

    /// Get the value of a header field as bytes.
    ///
    /// Returns None if there is no such field. The field name is
    /// case-insensitive.
    #[deprecated(note = "Use get_field() instead")]
    pub fn field<S: AsRef<str>>(&self, name: S) -> Option<&[u8]> {
        self.fields
            .get(UncasedStr::new(name.as_ref()))
            .map(Vec::as_slice)
    }

    /// Get the value of a header field as a string.
    ///
    /// Returns None if there is no such field or its value is not a valid
    /// string. The field name is case-insensitive.
    #[deprecated(note = "Use get_field_str() instead")]
    pub fn field_str<S: AsRef<str>>(&self, name: S) -> Option<&str> {
        self.field(UncasedStr::new(name.as_ref()))
            .and_then(|b| str::from_utf8(b).ok())
    }

    /// Get the value of a header field that is a URI as a string.
    ///
    /// This handles the difference between the definition of a URI in the WARC 1.0
    /// and WARC 1.1 standards, where the former specifies angle brackets (<>) around
    /// the URI and the latter doesn't, by stripping the brackets if present.
    ///
    /// Returns None if no such header exists or its value is not a valid string.
    pub fn field_uri<S: AsRef<str>>(&self, name: S) -> Option<&str> {
        let s = self.field_str(name)?;
        // Trim brackets if present to return only the URI, but
        // only if both are present- preserve weird (unmatched) brackets.
        Some(
            s.strip_prefix('<')
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or(s),
        )
    }

    pub fn set_version<V: Into<Version>>(&mut self, version: V) {
        // TODO fields need to know their type in order to convert between representations in some
        // cases, particularly URLs for conversion to or from WARC 1.1. Handle this by making
        // FieldName aware of value classes (such as URIs) so we can add or remove surrounding
        // brackets from fields that have URI values on access.
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
        // TODO make this panic if the field is missing; callers that want to be lenient can
        // use get_field (and do the same for other field-specific functions).
        self.field_str("WARC-Record-ID")
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
        self.field_str("Content-Length")
            .and_then(|s| str::parse::<u64>(s).ok())
    }

    /// Get the WARC-Date field value, parsed as a `DateTime`.
    ///
    /// Equivalent to parsing the result of [warc_date] as a datetime in the
    /// format dictated by the WARC specification.
    #[cfg(feature = "chrono")]
    pub fn warc_date_parsed(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        // YYYY-MM-DDThh:mm:ssZ per WARC-1.0. This is valid RFC3339, which is
        // itself valid ISO 8601. We're slightly lenient in accepting non-UTC
        // zone offsets.
        use chrono::{DateTime, Utc};
        self.field_str("WARC-Date")
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// Get the WARC-Date field value.
    ///
    /// Returns `None` if the field is absent or is not a valid string.
    ///
    /// This is a mandatory field, but in the interest of parsing leniency it
    /// is not required to exist or have any particular value in order to parse
    /// record header.
    pub fn warc_date(&self) -> Option<&str> {
        self.field_str("WARC-Date")
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
        self.field_str("WARC-Type")
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
///
/// Versions for which a standards document exist can be conveniently expressed as
/// a [`StandardVersion`] which may be compared with a `Version` and converted to
/// one via the [`Into`] implementation.
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
    /// WARC 1.0: ISO 28500:2009
    pub const WARC1_0: Self = Version { major: 1, minor: 0 };
    /// WARC 1.1: ISO 28500:2017
    pub const WARC1_1: Self = Version { major: 1, minor: 0 };

    /// Parse the version line from a record header, returning the number of bytes
    /// consumed and the version.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Version), ParseError> {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"^WARC/(\d+)\.(\d+)\r\n").expect("Version regex invalid");
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
pub struct Field {
    name: FieldName,
    value: Vec<u8>,
}

impl Field {
    /// Construct a field with the given name and value.
    pub fn new<S: Into<FieldName>>(name: S, value: Vec<u8>) -> Field {
        Field {
            name: name.into(),
            value,
        }
    }

    /// Parse a Field from bytes.
    ///
    /// Returns the number of bytes consumed and the parsed field on succes.
    pub fn parse(bytes: &[u8]) -> Result<(usize, Field), ParseError> {
        // TODO these may not correctly handle `quoted-string` values containing CRLF2
        lazy_static! {
            static ref RE: Regex =
                Regex::new(r"^([a-zA-Z_\-]+): *(.*?)\r\n").expect("Field regex invalid");
            static ref CONTINUATION: Regex =
                Regex::new(r"^[ \t]+(.*?)\r\n").expect("Continuation regex invalid");
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
            debug_assert!(m[1].iter().all(u8::is_ascii));
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
/// of the record payload. In case of an error in parsing, some or all of the
/// input may be consumed.
pub fn get_record_header<R: BufRead>(mut reader: R) -> Result<Header, ParseError> {
    /// Return the index of the first position in the given buffer following
    /// a b"\r\n\r\n" sequence.
    #[inline]
    fn find_crlf2(buf: &[u8]) -> Option<usize> {
        use memchr::memmem::Finder;
        lazy_static! {
            static ref SEARCHER: Finder<'static> = Finder::new(b"\r\n\r\n");
        }

        SEARCHER.find(buf).map(|i| i + 4)
    }

    // Read bytes out of the input reader until we find the end of the header
    // (two CRLFs in a row).
    // First-chance: without copying anything
    let mut header: Option<(usize, Header)> = None;
    {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Err(ParseError::NoMoreData);
        }
        if let Some(i) = find_crlf2(buf) {
            // Weird split of parse and consume here is necessary because buf
            // is borrowed from the reader so we can't consume until we no
            // longer hold a reference to the buffer.
            header = Some((i, Header::parse(&buf[..i])?));
        }
    }
    if let Some((sz, header)) = header {
        trace!("Found complete header in buffer, {} bytes", sz);
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
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "WARC header not terminated",
            )
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
            return Header::parse(&buf[..match_idx]);
        }

        // Otherwise keep looking
        reader.consume(buf.len() - bytes_consumed);
        bytes_consumed = buf.len();
        // TODO enforce maximum vec size?
    }
}

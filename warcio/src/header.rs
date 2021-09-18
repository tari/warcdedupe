//! WARC record header data structures.

use std::borrow::Borrow;
use std::io::BufRead;
use std::str;

use indexmap::map::IndexMap;

pub use fieldkind::FieldKind;
pub use fieldname::FieldName;
pub use recordkind::RecordKind;
pub use recordtype::RecordType;

use crate::compression::Compression;
use crate::HeaderParseError;
use crate::record::RecordWriter;
use crate::version::Version;

use super::{CTL, SEPARATORS};

mod fieldkind;
mod recordkind;
mod recordtype;
mod fieldname;

// We use an IndexMap to preserve the read order of fields when writing them back out;
// std::collections::HashMap randomizes ordering.
type FieldMap = IndexMap<FieldName, Vec<u8>>;

/// The header of a WARC record.
///
/// Field values can be accessed using the [`get_field`](Self::get_field) family of functions, or
/// accessed in parsed form through specific methods such as
/// [`content_length`](Self::content_length). Field values can be modified using the
/// [`set_field`](Self::set_field) method, or fields can be removed entirely with [`remove_field`](
/// Self::remove_field).
///
/// ```
/// # use warcio::{Header, Version, FieldName, FieldKind};
/// // Parse a header from bytes
/// let raw_header = b"\
/// WARC/1.1\r
/// WARC-Record-ID: <urn:uuid:b4beb26f-54c4-4277-8e23-51aa9fc4476d>\r
/// WARC-Date: 2021-08-05T06:22Z\r
/// WARC-Type: resource\r
/// Content-Length: 0\r
/// \r
/// ";
/// let (parsed_header, parsed_size) = Header::parse(raw_header).unwrap();
/// assert_eq!(parsed_size, raw_header.len());
///
/// // Construct a header from nothing
/// let mut synthetic_header = Header::new(Version::WARC1_1);
/// synthetic_header.set_field(FieldKind::RecordId,
///                            "<urn:uuid:b4beb26f-54c4-4277-8e23-51aa9fc4476d>");
/// synthetic_header.set_field(FieldKind::Date, "2021-08-05T06:22Z");
/// synthetic_header.set_field(FieldKind::Type, "resource");
/// synthetic_header.set_field(FieldKind::ContentLength, "0");
///
/// // Headers compare equal because they have the same version and fields
/// assert_eq!(parsed_header, synthetic_header);
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Header {
    version: Version,
    fields: FieldMap,
}

impl Header {
    pub fn new<V: Into<Version>>(version: V) -> Self {
        Header {
            version: version.into(),
            fields: Default::default(),
        }
    }

    /// Parse a header from bytes, returning the header and the number of bytes consumed.
    pub fn parse(mut bytes: &[u8]) -> Result<(Header, usize), HeaderParseError> {
        let (version_consumed, version) = Version::parse(bytes)?;
        bytes = &bytes[version_consumed..];

        let mut fields: FieldMap = Default::default();
        let mut headers_consumed = 0;
        loop {
            match bytes.get(..2) {
                Some([b'\r', b'\n', ..]) => break,
                Some(_) => { /* Not end of headers, so probably a field */ }
                None => return Err(HeaderParseError::Truncated),
            }

            let (name, value, sz) = Self::parse_field(bytes)?;
            let name =
                str::from_utf8(name).expect("parse_field should only accept ASCII field names");
            fields.insert(name.into(), value);
            bytes = &bytes[sz..];
            headers_consumed += sz;
        }

        Ok((
            Header { version, fields },
            version_consumed + headers_consumed + 2,
        ))
    }

    pub(crate) fn parse_field(bytes: &[u8]) -> Result<(&[u8], Vec<u8>, usize), HeaderParseError> {
        if bytes.is_empty() {
            return Err(HeaderParseError::Truncated);
        }

        // field-name: at least one token, which is an ASCII value excluding CTL or SEPARATORS
        let name_end = match bytes
            .iter()
            .position(|b| !b.is_ascii() || CTL.contains(b) || SEPARATORS.contains(b))
        {
            Some(i) => i,
            None => return Err(HeaderParseError::MalformedField),
        };
        // literal colon must follow field-name
        match bytes.get(name_end) {
            None => return Err(HeaderParseError::Truncated),
            Some(b':') => { /* Correctly formed */ }
            Some(_) => return Err(HeaderParseError::MalformedField),
        }

        let mut chunk_start = name_end + 1;
        let mut value: Vec<u8> = vec![];
        let consumed = loop {
            // Trim leading whitespace
            chunk_start += match bytes[chunk_start..]
                .iter()
                .position(|&x| x != b' ' && x != b'\t')
            {
                None => return Err(HeaderParseError::Truncated),
                Some(idx) => idx,
            };

            // Take data until CRLF
            let chunk_end = match bytes[chunk_start..].windows(2).position(|s| s == b"\r\n") {
                Some(idx) => chunk_start + idx,
                None => return Err(HeaderParseError::Truncated),
            };
            value.extend_from_slice(&bytes[chunk_start..chunk_end]);

            // Stop if the following byte after CRLF isn't LWS, otherwise continue since it's a
            // folded line.
            match bytes.get(chunk_end + 2) {
                // LWS follows: this is a folded line. Advance to it.
                Some(b' ') | Some(b'\t') => {
                    chunk_start = chunk_end + 2;
                    continue;
                }
                // Non-LWS: end of this field
                Some(_) => break chunk_end + 2,
                // Absent: can't tell
                None => return Err(HeaderParseError::Truncated),
            }
        };

        Ok((&bytes[..name_end], value, consumed))
    }

    /// Write the current record to the given output stream.
    ///
    /// The returned `Write`r will accept only as many bytes as the [`Content-Length`](FieldName::ContentLength)
    /// header declares and will panic if that field is missing or does not have a valid
    /// base-10 integer value (consisting only of ASCII digits). Further writes after the
    /// specified number of bytes will do nothing.
    ///
    /// While the output adapter will only write up to the given number of bytes, it will
    /// not pad the output if dropped before `Content-Length` bytes have been written. Failing
    /// to write enough data will usually result in data corruption, but an error will also
    /// be emitted to the log.
    pub fn write_to<W: std::io::Write>(
        &self,
        dest: W,
        compression: Compression,
    ) -> std::io::Result<RecordWriter<W>> {
        RecordWriter::new(dest, self, compression)
    }

    /// Get the value of a header field as bytes, or None if no such header exists.
    ///
    /// Although the WARC specification does not permit values that are not also valid Rust strings,
    /// users that wish to be lenient in accepting malformed records may wish to relax that
    /// requirement by using this function.
    ///
    /// ## URL translation
    ///
    /// For fields that are [defined to contain a bare URI by the
    /// specification](FieldName::value_is_bare_uri), this function will strip surrounding angle
    /// brackets from the value if the [WARC version](Version) of the record is less than 1.1
    /// and they are present. This hides the changed definition of a URL in the standard from
    /// version 1.1.
    ///
    /// For example, a `WARC-Record-ID` field in WARC 1.0 might have the raw value
    /// `<urn:uuid:ea07dfdd-9452-4b7d-add0-b2c538af5fa5>` but the same value in WARC 1.1 has the
    /// value `urn:uuid:ea07dfdd-9452-4b7d-add0-b2c538af5fa5` (without angle brackets). This
    /// function would return the value without brackets for all record versions.
    pub fn get_field_bytes<F: Into<FieldName>>(&self, field: F) -> Option<&[u8]> {
        let field = field.into();
        if !field.value_is_bare_uri() {
            return self.get_field_bytes_raw(field);
        }

        let mut value = self.get_field_bytes_raw(field)?;
        if self.version <= Version::WARC1_0 {
            // Strip angle brackets for pre-1.1 WARC versions
            if value.first() == Some(&b'<') && value.last() == Some(&b'>') {
                value = &value[1..value.len() - 1];
            }
        }
        Some(value)
    }

    /// Get the value of a header field as bytes, without URL translation.
    ///
    /// This function works like [`get_field_bytes`], except bare URIs are not translated as
    /// described in that function's documentation.
    pub fn get_field_bytes_raw<F: Into<FieldName>>(&self, field: F) -> Option<&[u8]> {
        self.fields.get(&field.into()).map(Vec::as_slice)
    }

    /// Get the value of a header field, or None if it does not exist or is not a valid Rust string.
    ///
    /// This function transforms fields that are bare URIs in the same way as
    /// [`get_field_bytes`](Header::get_field_bytes), stripping angle brackets from
    /// the value when the record's WARC version is pre-1.1.
    ///
    /// If you want to handle potentially malformed records,
    /// [`get_field_bytes`](Header::get_field_bytes) allows you to
    /// access values that are not acceptable Rust strings (that is, they are not valid UTF-8).
    /// However, because such a value would be malformed according to the specification, it should
    /// rarely be required.
    pub fn get_field<F: Into<FieldName>>(&self, field: F) -> Option<&str> {
        str::from_utf8(self.get_field_bytes(field)?).ok()
    }

    /// Return `true` if a field with the given name currently exists in this header.
    pub fn field_exists<F: Into<FieldName>>(&self, field: F) -> bool {
        self.get_field_bytes(field).is_some()
    }

    /// Set the value of a header field, returning the old value (if any).
    ///
    /// This function will panic if the provided name contains characters that are not
    /// permitted in `field-name` context. The value will have angle brackets added if
    /// the field value is a [bare URI](FieldName::value_is_bare_uri) and the record's WARC
    /// version is pre-1.1, performing the opposite transformation of
    /// [`get_field_bytes`](Header::get_field_bytes).
    pub fn set_field<N: Into<FieldName>, V: Into<Vec<u8>>>(
        &mut self,
        name: N,
        value: V,
    ) -> Option<Vec<u8>> {
        let name = name.into();
        let name_str: &str = name.as_ref();
        assert!(
            name_str
                .chars()
                .any(|c| c.is_ascii()
                    && !(SEPARATORS.contains(&(c as u8)) || CTL.contains(&(c as u8)))),
            "field name {:?} contains illegal characters",
            name
        );

        let mut value = value.into();
        if name.value_is_bare_uri() && self.version < Version::WARC1_1 {
            value.reserve_exact(2);
            value.insert(0, b'<');
            value.push(b'>');
        }
        self.fields.insert(name, value)
    }

    pub fn remove_field<N: Borrow<FieldName>>(&mut self, name: N) -> Option<Vec<u8>> {
        self.fields.shift_remove(name.borrow())
    }

    /// Get an iterator over the fields in this header.
    ///
    /// In comparison to [`get_field_bytes`], the values yielded by this iterator will have the
    /// raw value to be read or written from a serialized record, including angle brackets
    /// or not for values which are [bare URIs](FieldName::value_is_bare_uri) based on
    /// the WARC version.
    pub fn iter_field_bytes(&self) -> impl Iterator<Item = (&FieldName, &[u8])> {
        self.fields.iter().map(|(k, v)| (k, v.as_slice()))
    }

    /// Get an iterator over mutable field values.
    ///
    /// As with [`iter_field_bytes`], the yielded values are not transformed URIs if the field
    /// is a bare URI like they would be if retrieved by [`get_field_bytes`] or modified with
    /// [`set_field`].
    pub fn iter_field_bytes_mut(&mut self) -> impl Iterator<Item = (&FieldName, &mut Vec<u8>)> {
        self.fields.iter_mut()
    }

    /// Get the WARC version of this record.
    pub fn version(&self) -> &Version {
        &self.version
    }

    /// Update the WARC version of this header.
    ///
    /// This will update any bare URIs to account for version differences as described for
    /// [`set_field`], then set the version to the provided one.
    pub fn set_version<V: Into<Version>>(&mut self, version: V) {
        let version = version.into();
        let transform: Option<fn(&mut Vec<u8>)> = if self.version <= Version::WARC1_0 && version > Version::WARC1_0 {
            // Upgrading: remove angle brackets if present. Malformed pre-1.1 records might
            // not have brackets.
            Some(|v| {
                if v.starts_with(b"<") && v.ends_with(b">") {
                    v.pop();
                    v.remove(0);
                }
            })
        } else if version < Version::WARC1_1 && self.version >= Version::WARC1_1 {
            // Downgrading: add angle brackets if not present. Malformed post-1.1 records might
            // already have brackets.
            Some(|v| {
                if !(v.starts_with(b"<") && v.ends_with(b">")) {
                    v.insert(0, b'<');
                    v.push(b'>');
                }
            })
        } else {
            None
        };

        if let Some(transform) = transform {
            for (name, value) in self.iter_field_bytes_mut() {
                if name.value_is_bare_uri() {
                    transform(value);
                }
            }
        }
        self.version = version;
    }

    /// Get the [`WARC-Record-ID`](FieldName::RecordId) field value.
    ///
    /// `WARC-Record-ID` is a mandatory WARC field, so if it is not present this
    /// function will panic. If the caller wishes to be lenient in this situation,
    /// use [`get_field`](Header::get_field) to read the field instead.
    ///
    /// Note that a valid value is assumed to be a valid `str` because the
    /// record ID is specified to be a [RFC 3986](https://dx.doi.org/10.17487/rfc3986) URI,
    /// which are always valid ASCII (and therefore UTF-8) strings when well-formed.
    pub fn record_id(&self) -> &str {
        self.get_field(FieldKind::RecordId)
            .expect("record header does not have a WARC-Record-ID")
    }

    /// Get the record [`Content-Length`](FieldName::ContentLength) or panic.
    ///
    /// `Content-Length` is a mandatory WARC field, so if it is not present or does not
    /// represent a valid length, this function will panic. If the caller wishes to be
    /// lenient and accept records without comprehensible length, use
    /// [`content_length_lenient`](Header::content_length_lenient) or
    /// [`get_field`](Header::get_field) to read as an optional value instead.
    pub fn content_length(&self) -> u64 {
        self.get_field(FieldKind::ContentLength)
            .expect("record header does not have a Content-Length")
            .parse()
            .expect("record Content-Length is not a valid integer")
    }

    /// Get the record `Content-Length`, if valid.
    ///
    /// If not present or not parseable as an integer, returns None. If the None
    /// case is uninteresting, use [`content_length`](Header::content_length) to panic instead
    /// if the value is missing or invalid.
    pub fn content_length_lenient(&self) -> Option<u64> {
        self.get_field(FieldKind::ContentLength)?.parse().ok()
    }

    /// Get the [`WARC-Date`](FieldName::Date) field value, parsed as a `DateTime`.
    ///
    /// Equivalent to parsing the result of [`warc_date`] as a datetime in the
    /// format dictated by the WARC specification, and panics if the field is
    /// malformed or missing. Callers should use [`get_field`] and parse the value
    /// themselves to avoid panicking if required.
    #[cfg(feature = "chrono")]
    pub fn warc_date_parsed(&self) -> chrono::DateTime<chrono::Utc> {
        // YYYY-MM-DDThh:mm:ssZ per WARC-1.0. This is valid RFC3339, which is
        // itself valid ISO 8601. We're slightly lenient in accepting non-UTC
        // zone offsets.
        use chrono::{DateTime, Utc};
        DateTime::parse_from_rfc3339(s)
            .expect("WARC-Date field cannot be parsed as RTC3339 datetime")
            .with_timezone(&Utc)
    }

    /// Get the [`WARC-Date`](FieldName::Date) field value or panic.
    ///
    /// `WARC-Date` is a mandatory field, so this function panics if it is not present.
    /// If the caller wishes to be lenient, use [`get_field`](Self::get_field) to avoid panicking.
    pub fn warc_date(&self) -> &str {
        self.get_field(FieldKind::Date)
            .expect("record header does not have a WARC-Date field")
    }

    /// Get the [`WARC-Type`](FieldName::Type) field value or panic.
    ///
    /// Because `WARC-Type` is a mandatory field, this function will panic if the
    /// field is not present. Callers should use [`get_field`](Self::get_field) to gracefully handle
    /// that case.
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
    // TODO this should return a RecordType instead. That will want a RecordTypeRef that borrows
    // the value and that should impl ToOwned as well as having AsRef and such impls.
    // alternately: RecordType<S> where S: AsRef<str>. That would push allocation choices to
    // the user at creation time.
    pub fn warc_type(&self) -> &str {
        self.get_field(FieldKind::Type)
            .expect("record header does not have a WARC-Type field")
    }
}

/// A header field.
///
/// The name of a field is case-insensitive, and its value may be any bytes.
///
/// This type is a convenience for parsing; actual header fields are stored in
/// a map inside the record header.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Field {
    name: FieldName,
    value: Vec<u8>,
}

/// Parse a WARC record header out of the provided `BufRead`.
///
/// Consumes the bytes that are parsed, leaving the reader at the beginning
/// of the record payload. In case of an error in parsing, some or all of the
/// input may be consumed.
// TODO put this on a RecordReader type or something
pub(crate) fn get_record_header<R: BufRead>(mut reader: R) -> Result<Header, HeaderParseError> {
    enum FirstChanceOutcome {
        Success(usize, Header),
        KeepLooking(Vec<u8>),
    }

    // First-chance: without copying anything, operating only from the input's buffer
    let outcome = {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Err(HeaderParseError::Truncated);
        }
        // Weird split of parse and consume here is necessary because buf
        // is borrowed from the reader so we can't consume until we no
        // longer hold a reference to the buffer.
        match Header::parse(buf) {
            Ok((parsed, n)) => FirstChanceOutcome::Success(n, parsed),
            Err(HeaderParseError::Truncated) => {
                // Copy the input we've already searched into a buffer to continue past it
                let owned_buf: Vec<u8> = buf.into();
                reader.consume(owned_buf.len());
                FirstChanceOutcome::KeepLooking(owned_buf)
            }
            Err(e) => return Err(e),
        }
    };

    let mut owned_buf = match outcome {
        FirstChanceOutcome::Success(sz, header) => {
            trace!("Found complete header in buffer, {} bytes", sz);
            reader.consume(sz);
            return Ok(header);
        }
        FirstChanceOutcome::KeepLooking(buf) => buf,
    };
    trace!(
        "First-chance read unsuccessful yielding {} bytes",
        owned_buf.len()
    );

    // Second chance: need to start copying out of the reader's buffer. Throughout this loop,
    // we've grabbed some number of bytes and own them with a tail copied out of the reader's
    // buffer but still buffered so we can give bytes back at the end.
    loop {
        // Tracks the number of bytes we've read that definitely aren't a complete header
        let bytes_consumed = owned_buf.len();
        // Grab some data out of the reader and copy into the owned buffer
        owned_buf.extend(reader.fill_buf()?);
        if owned_buf.len() == bytes_consumed {
            // Read returned 0 bytes
            return Err(HeaderParseError::Truncated);
        }

        // Try to parse what we have buffered
        match Header::parse(&owned_buf) {
            Ok((parsed, n)) => {
                reader.consume(n - bytes_consumed);
                return Ok(parsed);
            }
            Err(HeaderParseError::Truncated) => {}
            Err(e) => return Err(e),
        }

        // Otherwise keep looking
        reader.consume(owned_buf.len() - bytes_consumed);
        // TODO enforce maximum vec size? (to avoid unbounded memory use on malformed input)
    }
}

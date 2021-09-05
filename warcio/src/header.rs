//! WARC record header data structures.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::str;

use indexmap::map::IndexMap;
use regex::bytes::Regex;
use uncased::{AsUncased, UncasedStr};

use crate::record::Compression;
use crate::version::Version;
use crate::HeaderParseError;

use super::{CTL, SEPARATORS};

/// The name of a WARC header field.
///
/// Field names are case-insensitive strings made up of one or more ASCII characters excluding
/// control characters (values 0-31 and 127) and separators (`()<>@,;:\"/[]?={} \t`). A `FieldName`
/// can be constructed from arbitrary input using the [`From<str>` impl](#impl-From<S>), or a
/// variant may be directly constructed. A string representation of a parsed name can be obtained
/// through [`AsRef<str>`](#impl-AsRef<str>).
///
/// Comparison, ordering, and hashing of field names is always case-insensitive, and the standard
/// variants normalize their case to those used by the standard. Unrecognized values will preserve
/// case when converted to strings but still compare case-insensitively.
///
/// ```
/// # use warcio::FieldName;
/// let id = FieldName::RecordId;
/// // Conversion from string via From
/// let parsed_id: FieldName = "warc-record-id".into();
///
/// assert_eq!(id, parsed_id);
/// // Input was cased differently: standard name has been normalized
/// assert_eq!("WARC-Record-ID", parsed_id.as_ref());
/// // Only comparison of FieldNames is case-insensitive, a string is not
/// assert_eq!(parsed_id, "wArC-ReCoRd-Id".into());
/// assert_ne!(parsed_id.as_ref(), "wArC-ReCoRd-Id");
/// ```
#[derive(Debug, Clone)]
pub enum FieldName {
    /// `WARC-Record-ID`: a globally unique identifier for a record.
    ///
    /// This field is mandatory and must be present in a standards-compliant record.
    /// Values should consist of a URI delimited by angle brackets: `"<" uri ">"`. Often
    /// the URI describes a UUID consistent with [RFC 4122](https://dx.doi.org/10.17487/rfc4122),
    /// such as `urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6`.
    RecordId,
    /// `Content-Length`: The number of octets (bytes) in a record block.
    ///
    /// This field is mandatory and must be present in a standards-compliant record.
    /// Values should consist of one or more [ASCII](https://dx.doi.org/10.17487/rfc0020) digits.
    ContentLength,
    /// `WARC-Date`: the instant that record data capture of a record began.
    ///
    /// This must be a UTC timestamp according to the W3C profile of ISO 8601,
    /// such as `YYYY-MM-DDThh:mm:ssZ`. Fractional parts of a second may be included,
    /// but must have between 1 and 9 digits (inclusive) if present.
    ///
    /// This field is mandatory and must be present in a standards-compliant record.
    Date,
    /// `WARC-Type`: the type of a record, corresponding to a [`RecordType`].
    ///
    /// This field is mandatory and must be present in a standards-compliant record.
    Type,
    /// `Content-Type`: the [RFC 2045](https://dx.doi.org/10.17487/rfc2045) MIME type
    /// of a record's data block.
    ///
    /// If absent, readers may attempt to identify the resource type by inspecting
    /// its contents and URI, or otherwise treat it as `application/octet-stream`.
    ContentType,
    /// `WARC-Concurrent-To`: the [`RecordId`](Self::RecordId) of any records created as part of the same
    /// capture event as a record.
    ///
    /// A capture event includes all information automatically gathered by a retrieval of a single
    /// [`TargetURI`](Self::TargetURI), such as a [`Request`](RecordType::Request) record and its
    /// associated [`Response`](RecordType::Response).
    ///
    /// This field may appear multiple times on a single record, but due to API limitations
    /// this library does not currently support that.
    // TODO support multiple Concurrent-To values per record.
    ConcurrentTo,
    /// `WARC-Block-Digest`: a `labelled-digest` of a complete record block.
    ///
    /// A `labelled-digest` has the form `algorithm ":" digest-value`, where the algorithm and
    /// digest value consist of any number of tokens. Although the specification does not specify
    /// any particular hash algorithm or digest representation, it uses
    /// [SHA-1](https://dx.doi.org/10.17487/rfc3174) with a [RFC 4648
    /// base32](https://dx.doi.org/10.17487/rfc4648)-encoded as an example, exemplified by the
    /// string `sha1:3EF4GH5IJ6KL7MN8OPQAB2CD`; many tools adopt this same format.
    BlockDigest,
    /// `WARC-Payload-Digest`: a `labelled-digest` of a record's payload, in the same format as a
    /// [`BlockDigest`](FieldName::BlockDigest) value.
    ///
    /// The payload of a record is not necessarily equivalent to the record block. In particular,
    /// the payload of a block with [`ContentType`](FieldName::ContentType) `application/http` is the
    /// [RFC 2616](https://dx.doi.org/10.17487/rfc2616) `entity-body`; the HTTP request or response
    /// body, excluding any headers.
    ///
    /// The payload digest of a record may also refer to data that is not present in the record
    /// block at all, such as when a [`Revisit`](RecordType::Revisit) record truncates duplicated
    /// data or in a segmented record.
    PayloadDigest,
    /// `WARC-IP-Address`: an IP address that was contacted to retrieve record content.
    ///
    /// For IPv4 addresses, the permitted format is a dotted quad like `127.0.0.1`. Acceptable IPv6
    /// address formats are as specified by section 2.2 of
    /// [RFC 4291](https://dx.doi.org/10.17487/rfc4291).
    ///
    /// This is most often the IP address from which an HTTP resource was retrieved at capture-time,
    /// corresponding to the address resolved from the block's [`TargetURI`](Self::TargetURI).
    IpAddress,
    /// `WARC-Refers-To`: the record ID of a single record for which the present record
    /// holds additional content.
    ///
    /// The field value is a URI surrounded by angle brackets: `<uri>`.
    ///
    /// [`Revisit`](RecordType::Revisit) or [`Conversion`](RecordType::Conversion) records use this
    /// field to indicate a preceding record which helps determine the record's content. It can also
    /// be used to associate a [`Metadata`](RecordType::Metadata) record with the record it
    /// describes.
    RefersTo,
    /// `WARC-Refers-To-Target-URI`: the [`TargetURI`](Self::TargetURI) of the record referred to by
    /// [`RefersTo`](Self::RefersTo).
    RefersToTargetURI,
    /// `WARC-Refers-To-Date`: the [`Date`](Self::Date) of the record referred to by
    /// [`RefersTo`](Self::RefersTo).
    RefersToDate,
    /// `WARC-Target-URI`: the original URI that provided the record content.
    ///
    /// In the context of web crawling, this is the URI that a crawler sent a request to retrieve.
    /// For indirect records such as [metadata](RecordType::Metadata) or
    /// [conversion](RecordType::Conversion)s, the value is a copy of the target URI from the
    /// original record.
    TargetURI,
    /// `WARC-Truncated`: the reason that a record contains a truncated version of the
    /// original resource.
    ///
    /// Reasons a writer may specify when truncating a record (the value of this field) are
    /// non-exhaustively enumerated by the standard:
    ///  * `length`: the record is too large
    ///  * `time`: the record took too long to capture
    ///  * `disconnect`: the resource was disconnected from a network
    ///  * `unspecified`: some other or unknown reason
    Truncated,
    /// `WARC-Warcinfo-ID`: the [ID](Self::RecordId) of the [warcinfo](RecordType::Info) record
    /// associated with this record.
    ///
    /// This field is typically used when the context of a record is lost, such as when a collection
    /// of records is split into individual files. The value is a URI surrounded by angle brackets:
    /// `<uri>`.
    InfoID,
    /// `WARC-Filename`: the name of the file containing the current
    /// [warcinfo](RecordType::Info) record.
    ///
    /// This field may only be used in info records.
    Filename,
    /// `WARC-Profile`: the kind of analysis and handling applied to create a
    /// [revisit](RecordType::Revisit) record, specified as a URI.
    ///
    /// Readers shall not attempt to interpret records for which the profile is not recognized. The
    /// WARC 1.1 standard defines two profiles and allows for others:
    ///  * `http://netpreserve.org/warc/1.1/revisit/identical-payload-digest`
    ///  * `http://netpreserve.org/warc/1.1/revisit/server-not-modified`
    Profile,
    /// `WARC-Identified-Payload-Type`: the content-type discovered by inspecting a record payload.
    ///
    /// Writers *shall not* blindly promote HTTP `Content-Type` values without inspecting the
    /// payload when creating records- such values may be incorrect. Users should perform an
    /// independent check to arrive at a value for this field.
    IdentifiedPayloadType,
    /// `WARC-Segment-Number`: the current record's ordering in a sequence of segmented records.
    ///
    /// This field is required for [continuation](RecordType::Continuation) records as well as for
    /// any record that has associated continuations. In the first record its value is `1`, and
    /// for subsequent continuations the value is incremented.
    SegmentNumber,
    /// `WARC-Segment-Origin-ID`: the ID of the starting record in a series of segmented records.
    ///
    /// This field if required for [continuation](RecordType::Continuation) records with the value
    /// being a URI surrounded by angle brackets, where the URI is the
    /// [ID of the first record](Self::RecordId) in the continuation sequence.
    SegmentOriginID,
    /// `WARC-Segment-Total-Length`: the total length of concatenated segmented content blocks.
    ///
    /// This field is required for the last [continuation](RecordType::Continuation) record of a
    /// series, and *shall not* be used elsewhere.
    SegmentTotalLength,
    /// Any unrecognized field name.
    ///
    /// The WARC format permits arbitrarily-named extension fields, and specifies that software
    /// *shall* ignore fields with unrecognized names. This variant allows their representation and
    /// processing but does not ascribe any particular meaning to unrecognized names.
    ///
    /// `Other` should generally not be manually constructed, instead using the [`From`]
    /// impl to convert from a string. However, an `Other` value that matches the string
    /// representation of a known field name will behave the same as the variant matching
    /// that field name (but may have worse performance characteristics).
    Other(Box<str>),
}

include!(concat!(env!("OUT_DIR"), "/header_field_types.rs"));

impl FieldName {
    /// Returns `true` if a field's value consists of a bare URI.
    ///
    /// This is useful for handling the difference in grammar between WARC 1.1 and earlier versions:
    /// while earlier versions define a URI as `"<" <'URI' per RFC3986> ">"` (surrounded by angle
    /// brackets), WARC 1.1 removes the angle brackets from the grammar describing a URI and
    /// explicitly adds the angle brackets to some (but not all) fields.
    /// This function will return `true` for those fields that do not have angle brackets in a
    /// WARC 1.1 record but do in earlier versions, and `false` for all other fields.
    ///
    /// Users will generally not need to use this function directly; field values will be
    /// transformed as necessary when accessing values through a [`Header`] (brackets added when
    /// writing a value to a pre-1.1 record, and removed when reading).
    pub fn value_is_bare_uri(&self) -> bool {
        use FieldName::*;
        match self {
            TargetURI | RefersToTargetURI | Profile => true,
            // Types that include angle brackets regardless of WARC version: "<" uri ">"
            RecordId | ConcurrentTo | RefersTo | InfoID | SegmentOriginID => false,
            // Types that don't contain URIs
            ContentLength
            | Date
            | Type
            | ContentType
            | BlockDigest
            | PayloadDigest
            | IpAddress
            | RefersToDate
            | Truncated
            | Filename
            | IdentifiedPayloadType
            | SegmentNumber
            | SegmentTotalLength => false,
            // No particular interpretation
            Other(_) => false,
        }
    }
}

impl Borrow<UncasedStr> for FieldName {
    fn borrow(&self) -> &UncasedStr {
        self.as_ref().as_uncased()
    }
}

// Implementing Borrow requires the same semantics between the borrowed and original versions,
// so Eq, Ord and Hash are implemented in terms of the case-insensitive field name.
impl PartialEq for FieldName {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldName::Other(_), _) | (_, FieldName::Other(_)) => {
                self.as_ref().as_uncased().eq(other.as_ref())
            }
            // If neither operand is Other, we can simply compare the discriminant and avoid
            // doing a string comparison
            (l, r) => std::mem::discriminant(l) == std::mem::discriminant(r),
        }
    }
}

impl Eq for FieldName {}

impl PartialOrd for FieldName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().as_uncased().partial_cmp(other.as_ref())
    }
}

impl Ord for FieldName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().as_uncased().cmp(other.as_ref().as_uncased())
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
/// # use warcio::{Header, Version, FieldName};
/// // Parse a header from bytes
/// let raw_header = b"\
/// WARC/1.1\r
/// WARC-Record-ID: <urn:uuid:b4beb26f-54c4-4277-8e23-51aa9fc4476d>\r
/// WARC-Date: 2021-08-05T06:22Z\r
/// WARC-Type: resource\r
/// Content-Length: 0\r
/// \r
/// ";
/// let parsed_header = Header::parse(raw_header).unwrap();
///
/// // Construct a header from nothing
/// let mut synthetic_header = Header::new(Version::WARC1_1);
/// synthetic_header.set_field(FieldName::RecordId,
///                            "<urn:uuid:b4beb26f-54c4-4277-8e23-51aa9fc4476d>");
/// synthetic_header.set_field(FieldName::Date, "2021-08-05T06:22Z");
/// synthetic_header.set_field(FieldName::Type, "resource");
/// synthetic_header.set_field(FieldName::ContentLength, "0");
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
    pub fn parse(mut bytes: &[u8]) -> Result<Header, HeaderParseError> {
        // version, fields, CRLF
        let (mut bytes_consumed, version) = Version::parse(bytes)?;
        bytes = &bytes[bytes_consumed..];

        let mut fields: FieldMap = Default::default();
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
    /// The returned `Write`r will accept only as many bytes as the [`Content-Length`](FieldName::ContentLength)
    /// header declares and will panic if that field is missing or does not have a valid
    /// base-10 integer value (consisting only of ASCII digits). Further writes after the
    /// specified number of bytes will do nothing.
    ///
    /// While the output adapter will only write up to the given number of bytes, it will
    /// not pad the output if dropped before `Content-Length` bytes have been written. Failing
    /// to write enough data will usually result in data corruption, but an error will also
    /// be emitted to the log.
    // TODO make the concrete type public so it can into_inner() and the like.
    pub fn write_to<W: std::io::Write>(
        &self,
        dest: W,
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
            self.content_length(),
        ))
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
        let field: FieldName = field.into();

        let mut value = self.fields.get(&field).map(Vec::as_slice)?;
        if field.value_is_bare_uri() && self.version <= Version::WARC1_0 {
            if value.first() == Some(&b'<') && value.last() == Some(&b'>') {
                value = &value[1..value.len() - 1];
            }
        }
        Some(value)
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
            !name_str.contains(SEPARATORS) && !name_str.contains(CTL),
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

    pub fn set_version<V: Into<Version>>(&mut self, version: V) {
        // TODO fields need to know their type in order to convert between representations in some
        // cases, particularly URLs for conversion to or from WARC 1.1. Handle this by making
        // FieldName aware of value classes (such as URIs) so we can add or remove surrounding
        // brackets from fields that have URI values on access.
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
        self.get_field(FieldName::RecordId)
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
        self.get_field(FieldName::ContentLength)
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
        self.get_field(FieldName::ContentLength)?.parse().ok()
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
        self.get_field(FieldName::Date)
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
    // TODO this should return a RecordType instead
    pub fn warc_type(&self) -> &str {
        self.get_field(FieldName::Type)
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
    pub fn parse(bytes: &[u8]) -> Result<(usize, Field), HeaderParseError> {
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
                return Err(HeaderParseError::MalformedField);
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

/// Return the index of the first position in the given buffer following
/// a b"\r\n\r\n" sequence.
#[inline]
fn find_crlf2(buf: &[u8]) -> Option<usize> {
    // TODO a naive implementation comparing windows() over buf actually seems slightly faster
    // than an optimized searcher. Seems like headers are short enough and Finder overhead large
    // enough that a simple (possibly autovectorized?) implementation is better. Should examine the
    // generated code.
    use memchr::memmem::Finder;
    lazy_static! {
        static ref SEARCHER: Finder<'static> = Finder::new(b"\r\n\r\n");
    }

    SEARCHER.find(buf).map(|i| i + 4)
}

/// Parse a WARC record header out of the provided `BufRead`.
///
/// Consumes the bytes that are parsed, leaving the reader at the beginning
/// of the record payload. In case of an error in parsing, some or all of the
/// input may be consumed.
// TODO put this on a RecordReader type or something
pub(crate) fn get_record_header<R: BufRead>(mut reader: R) -> Result<Header, HeaderParseError> {
    // Read bytes out of the input reader until we find the end of the header
    // (two CRLFs in a row).
    // First-chance: without copying anything
    let header: Option<(usize, Header)> = {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            return Err(HeaderParseError::Truncated);
        }
        if let Some(i) = find_crlf2(buf) {
            // Weird split of parse and consume here is necessary because buf
            // is borrowed from the reader so we can't consume until we no
            // longer hold a reference to the buffer.
            Some((i, Header::parse(&buf[..i])?))
        } else {
            None
        }
    };
    if let Some((sz, header)) = header {
        trace!("Found complete header in buffer, {} bytes", sz);
        reader.consume(sz);
        return Ok(header);
    }

    // Second chance: need to start copying out of the reader's buffer. Throughout this loop,
    // we've grabbed some number of bytes and own them with a tail copied out of the reader's
    // buffer but still buffered so we can give bytes back at the end.
    let mut buf: Vec<u8> = Vec::new();
    let mut bytes_consumed = 0;
    loop {
        // Copy out of the reader
        buf.extend(reader.fill_buf()?);
        if buf.len() == bytes_consumed {
            // Read returned 0 bytes
            return Err(HeaderParseError::Truncated);
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
        // TODO enforce maximum vec size? (to avoid unbounded memory use on malformed input)
    }
}

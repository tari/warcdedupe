//! WARC record header data structures.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::str;

use indexmap::map::IndexMap;
use uncased::{AsUncased, UncasedStr};

use crate::compression::Compression;
use crate::HeaderParseError;
use crate::record::RecordWriter;
use crate::version::Version;

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

/// The kind of a single WARC record.
///
/// Every record is specified to have a type in its [`WARC-Type`](FieldName::Type) field. This
/// enumeration provides variants for those specified in the WARC standard and allows representation
/// of others as might be used by extensions to the core WARC format or future versions.
///
/// A `RecordType` can be parsed from a string using the [`From<str>`](#impl-From<S>) impl, and
/// retrieved as a string via [`AsRef<str>`](#impl-AsRef<str>). Parsed values are case-insensitive
/// and normalize to the standard capitalization, but [unknown](RecordType::Other) values preserve
/// case when parsed.
///
/// ```
/// # use warcio::RecordType;
/// assert_eq!(<RecordType as From<_>>::from("Response").as_ref(), "response");
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RecordType {
    /// `warcinfo`: describes the records that follow this one.
    ///
    /// An info record describes the records following itself through the end of the current input
    /// or until the next info record. Its [`Content-Type`](FieldName::ContentType) is recommended
    /// to be `application/warc-fields`, and the standard suggests that it contain information about
    /// the tools that wrote the WARC file such as tool operator, software version and IP address.
    Info,
    /// `response`: a complete scheme-specific response to some request.
    ///
    /// The exact contents of a response depend on the URI scheme of the record's
    /// [target URI](FieldName::TargetURI); the WARC 1.1 specification only defines a format for the
    /// http and https schemes, with a `response` record containing a full HTTP response
    /// (including headers) received over the network.
    Response,
    /// `resource`: a resource without full protocol response information.
    ///
    /// While a [`response`](RecordType::Response) record contains a full response as it appears on
    /// the network, a `resource` record discards protocol information- however the exact contents
    /// of the record block still depend on the scheme of the [target URI](FieldName::TargetURI).
    /// WARC 1.1 specifies meanings for the `http`, `https`, `ftp`, and `dns` schemes.
    Resource,
    /// `request`: a complete scheme-specific request.
    ///
    /// A request record includes a complete request including network protocol information in the
    /// same way that a [`response`](RecordType::Response) record does, including dependence on the
    /// target URI scheme.
    Request,
    /// `metadata`: content created to further describe, explain or accompany a resource.
    ///
    /// Metadata fields usually refer to another record of some other type that contains original
    /// content that was harvested or transformed. It is suggested that the record block have the
    /// `application/warc-fields` [content type](FieldName::ContentType) and may have fields
    /// including `via`, `hopsFromSeed`, and `fetchTimeMs`.
    Metadata,
    /// `revisit`: revisitation of content that was already archived.
    ///
    /// A revisit record is typically used instead of a [response](RecordType::Response) or
    /// [resource](RecordType::Resource) record to indicate that the received content was a complete
    /// or substantial duplicate of material that was previously archived, allowing reduced storage
    /// size or improved cross-referencing of material.
    ///
    /// Revisit records *shall* have a [`WARC-Profile`](FieldName::Profile) field describing how
    /// the fields and block should be interpreted. WARC 1.1 defines two profiles, each indicated
    /// by a particular URI:
    ///  * identical payload digest: a strong hash function indicates the payload is identical to
    ///    a previously-archived version.
    ///  * server not modified: a server indicates that content has not changed, such as with a
    ///    HTTP "304 Not Modified" response.
    ///
    /// Other profiles are permitted beyond those defined in the standard, but readers *shall not*
    /// attempt to interpret records with unknown profile.
    Revisit,
    /// `conversion`: an alternative version of another record's content.
    ///
    /// Conversion records typically hold transformed content that has been converted to another
    /// format, such as to ensure the content remains readable with current tools while minimizing
    /// lost information.
    Conversion,
    /// `continuation`: additional data to be appended to a prior block.
    ///
    /// Continuation records are used when records are segmented, allowing large blocks to be split
    /// into multiple records (and also multiple files). Continuations *shall* always have
    /// [`Segment-Origin-ID`](FieldName::SegmentOriginID) and
    /// [`WARC-Segment-Number`](FieldName::SegmentNumber) fields.
    Continuation,
    /// Any unrecognized record type.
    ///
    /// This variant allows unknown record types to be represented, beyond those specified. Software
    /// *shall* skip records of unknown type, which may be defined in future versions of the file
    /// format.
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
/// let (parsed_header, parsed_size) = Header::parse(raw_header).unwrap();
/// assert_eq!(parsed_size, raw_header.len());
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
    // TODO make the concrete type public so it can into_inner() and the like.
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

    /// Get an iterator over the fields
    pub fn iter_field_bytes(&self) -> impl Iterator<Item=(&FieldName, &[u8])> {
        self.fields.iter().map(|(k, v)| (k, v.as_slice()))
    }

    pub fn iter_field_bytes_mut(&mut self) -> impl Iterator<Item=(&FieldName, &mut Vec<u8>)> {
        self.fields.iter_mut()
    }

    /// Get the WARC version of this record.
    pub fn version(&self) -> &Version {
        &self.version
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
            Err(e) => return Err(e)
        }
    };

    let mut owned_buf = match outcome {
        FirstChanceOutcome::Success(sz, header) => {
            trace!("Found complete header in buffer, {} bytes", sz);
            reader.consume(sz);
            return Ok(header);
        },
        FirstChanceOutcome::KeepLooking(buf) => buf,
    };
    trace!("First-chance read unsuccessful yielding {} bytes", owned_buf.len());

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
            Err(HeaderParseError::Truncated) => {},
            Err(e) => return Err(e),
        }

        // Otherwise keep looking
        reader.consume(owned_buf.len() - bytes_consumed);
        // TODO enforce maximum vec size? (to avoid unbounded memory use on malformed input)
    }
}

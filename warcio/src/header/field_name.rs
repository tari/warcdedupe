use std::borrow::Borrow;
use uncased::{AsUncased, UncasedStr};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::fmt::Debug;

pub(crate) trait FieldStr: AsRef<str> + Clone + Debug {}
impl FieldStr for &str {}
impl FieldStr for Box<str> {}

/// Standardized values for [field names](FieldName).
// TODO FieldKind should be Borrow-compatible with FieldName so get/remove_field
// operations can take a FieldKind (or a string..) directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
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
}

impl FieldKind {
    pub fn into_name<S: FieldStr>(self) -> FieldName<S> {
        FieldName::Known(self)
    }
}

include!(concat!(env!("OUT_DIR"), "/field_kind_conversions.rs"));

/// The name of a WARC header field.
///
/// Field names are case-insensitive strings made up of one or more ASCII characters excluding
/// control characters (values 0-31 and 127) and separators (`()<>@,;:\"/[]?={} \t`). A `FieldName`
/// can be constructed from arbitrary input using the [`From<str>` impl](#impl-From<S>), or a
/// variant may be directly constructed. A string representation of a parsed name can be obtained
/// through [`AsRef<str>`](#impl-AsRef<str>).
///
/// Standardized values for field names are enumerated by the [`FieldKind`] type, for which
/// conversions and most operations are conveniently implemented. A `FieldKind` can trivially
/// be converted to or compared with a `FieldName`.
///
/// Comparison, ordering, and hashing of field names is always case-insensitive, and the standard
/// variants normalize their case to those used by the standard. Unrecognized values will preserve
/// case when converted to strings but still compare case-insensitively.
///
/// ```
/// # use warcio::{FieldName, FieldKind};
/// let id = FieldKind::RecordId;
/// // Conversion from string via From
/// let parsed_id: FieldName = "warc-record-id".into();
///
/// assert_eq!(id, parsed_id);
/// // Input was cased differently: standard name has been normalized
/// assert_eq!("WARC-Record-ID", parsed_id.as_ref());
/// // Only comparison of FieldNames is case-insensitive, a string is not
/// assert_eq!(parsed_id, <FieldName as From<_>>::from("wArC-ReCoRd-Id"));
/// assert_ne!(parsed_id.as_ref(), "wArC-ReCoRd-Id");
/// ```
#[derive(Debug, Clone)]
pub enum FieldName<S> {
    Known(FieldKind),
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
    Other(S),
}

impl<S: AsRef<str>> AsRef<str> for FieldName<S> {
    fn as_ref(&self) -> &str {
        match self {
            Self::Known(x) => x.as_ref(),
            Self::Other(s) => s.as_ref(),
        }
    }
}

impl<S: AsRef<str>> From<S> for FieldName<S> {
    fn from(s: S) -> Self {
        match <FieldKind as std::convert::TryFrom<&str>>::try_from(s.as_ref()) {
            Ok(x) => Self::Known(x),
            Err(_) => Self::Other(s.into())
        }
    }
}

impl<S: AsRef<str>> AsRef<FieldName<&str>> for FieldName<S> {
    fn as_ref(&self) -> &FieldName<&str> {
        match self {
            Self::Known(kind) => &FieldName::Known(*kind),
            Self::Other(s) => &FieldName::Other(s.as_ref()),
        }
    }
}

impl From<&str> for FieldName<Box<str>> {
    fn from(_: &str) -> Self {
        todo!()
    }
}

// Would prefer to do something like
// impl<S: AsRef<str>, T: Into<S>> Into<FieldName<S>> for FieldName<T>
// but this conflicts with the trivial `impl<T> From<T> for T` because S and T can be the same
// type. So instead we bless a few concrete types for S and implement conversions from common
// other types for those.

impl From<FieldName<&str>> for FieldName<Box<str>> {
    fn from(borrowed: FieldName<&str>) -> Self {
        todo!()
    }
}

impl<S: AsRef<str>> FieldName<S> {
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
        let kind = match self {
            Self::Known(kind) => kind,
            Self::Other(_) => return false,
        };

        // TODO this should be a method on FieldKind
        use FieldKind::*;
        match kind {
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
        }
    }
}

// TODO impl Borrow for FieldKind and such?
impl<S: AsRef<str>> Borrow<UncasedStr> for FieldName<S> {
    fn borrow(&self) -> &UncasedStr {
        self.as_ref().as_uncased()
    }
}

// Implementing Borrow requires the same semantics between the borrowed and original versions,
// so Eq, Ord and Hash are implemented in terms of the case-insensitive field name.
impl<S: AsRef<str>> PartialEq for FieldName<S> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldName::Other(_), _) | (_, FieldName::Other(_)) => {
                self.as_ref().as_uncased().eq(other.as_ref())
            }
            (FieldName::Known(l), FieldName::Known(r)) => l == r,
        }
    }
}

impl<S: AsRef<str>> Eq for FieldName<S> {}

impl<S: AsRef<str>> PartialOrd for FieldName<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().as_uncased().partial_cmp(other.as_ref())
    }
}

impl<S: AsRef<str>> Ord for FieldName<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().as_uncased().cmp(other.as_ref().as_uncased())
    }
}

impl<S: AsRef<str>> Hash for FieldName<S> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_uncased().hash(state)
    }
}

// FieldKind/FieldName compatibility

/*
impl<S: AsRef<str>> From<FieldKind> for FieldName<S> {
    fn from(_: FieldKind) -> Self {
        todo!()
    }
}
 */

impl<S: AsRef<str>> PartialEq<FieldKind> for FieldName<S> {
    fn eq(&self, other: &FieldKind) -> bool {
        match self {
            FieldName::Known(k) => k == other,
            FieldName::Other(s) => s.as_ref().as_uncased() == other.as_ref(),
        }
    }
}

impl<S: AsRef<str>> PartialEq<FieldName<S>> for FieldKind {
    fn eq(&self, other: &FieldName<S>) -> bool {
        other == self
    }
}

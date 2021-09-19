use uncased::UncasedStr;

use crate::FieldName;

/// Standardized values for [field names](FieldName).
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
    /// `WARC-Type`: the type of a record, corresponding to a [`RecordType`](crate::RecordType).
    ///
    /// This field is mandatory and must be present in a standards-compliant record.
    Type,
    /// `Content-Type`: the [RFC 2045](https://dx.doi.org/10.17487/rfc2045) MIME type
    /// of a record's data block.
    ///
    /// If absent, readers may attempt to identify the resource type by inspecting
    /// its contents and URI, or otherwise treat it as `application/octet-stream`.
    ContentType,
    /// `WARC-Concurrent-To`: the [`RecordId`](Self::RecordId) of any records created as part of
    /// the same capture event as a record.
    ///
    /// A capture event includes all information automatically gathered by a retrieval of a single
    /// [`TargetURI`](Self::TargetURI), such as a [`Request`](crate::RecordKind::Request) record
    /// and its associated [`Response`](crate::RecordKind::Response).
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
    /// [`BlockDigest`](crate::FieldKind::BlockDigest) value.
    ///
    /// The payload of a record is not necessarily equivalent to the record block. In particular,
    /// the payload of a block with [`ContentType`](FieldKind::ContentType) `application/http` is the
    /// [RFC 2616](https://dx.doi.org/10.17487/rfc2616) `entity-body`; the HTTP request or response
    /// body, excluding any headers.
    ///
    /// The payload digest of a record may also refer to data that is not present in the record
    /// block at all, such as when a [`Revisit`](crate::RecordKind::Revisit) record truncates duplicated
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
    /// [`Revisit`](crate::RecordKind::Revisit) or [`Conversion`](crate::RecordKind::Conversion)
    /// records use this field to indicate a preceding record which helps determine the record's
    /// content. It can also be used to associate a [`Metadata`](crate::RecordKind::Metadata)
    /// record with the record it describes.
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
    /// For indirect records such as [metadata](crate::RecordKind::Metadata) or
    /// [conversion](crate::RecordKind::Conversion)s, the value is a copy of the target URI from the
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
    /// `WARC-Warcinfo-ID`: the [ID](Self::RecordId) of the [warcinfo](crate::RecordKind::Info)
    /// record associated with this record.
    ///
    /// This field is typically used when the context of a record is lost, such as when a collection
    /// of records is split into individual files. The value is a URI surrounded by angle brackets:
    /// `<uri>`.
    InfoID,
    /// `WARC-Filename`: the name of the file containing the current
    /// [warcinfo](crate::RecordKind::Info) record.
    ///
    /// This field may only be used in info records.
    Filename,
    /// `WARC-Profile`: the kind of analysis and handling applied to create a
    /// [revisit](crate::RecordKind::Revisit) record, specified as a URI.
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
    /// This field is required for [continuation](crate::RecordKind::Continuation) records as well as for
    /// any record that has associated continuations. In the first record its value is `1`, and
    /// for subsequent continuations the value is incremented.
    SegmentNumber,
    /// `WARC-Segment-Origin-ID`: the ID of the starting record in a series of segmented records.
    ///
    /// This field if required for [continuation](crate::RecordKind::Continuation) records with the
    /// value being a URI surrounded by angle brackets, where the URI is the
    /// [ID of the first record](Self::RecordId) in the continuation sequence.
    SegmentOriginID,
    /// `WARC-Segment-Total-Length`: the total length of concatenated segmented content blocks.
    ///
    /// This field is required for the last [continuation](crate::RecordKind::Continuation) record
    /// of a series, and *shall not* be used elsewhere.
    SegmentTotalLength,
}

impl FieldKind {
    pub fn into_name(self) -> FieldName {
        FieldName::Known(self)
    }
}

include!(concat!(env!("OUT_DIR"), "/field_kind_conversions.rs"));

impl PartialEq<FieldName> for FieldKind {
    fn eq(&self, other: &FieldName) -> bool {
        other == self
    }
}

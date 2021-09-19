use uncased::{AsUncased, UncasedStr};

/// Standardized values for [record types](crate::RecordType).
#[derive(Debug, Clone, Copy, PartialOrd, Ord)]
pub enum RecordKind {
    /// `warcinfo`: describes the records that follow this one.
    ///
    /// An info record describes the records following itself through the end of the current input
    /// or until the next info record. Its [`Content-Type`](crate::FieldKind::ContentType) is recommended
    /// to be `application/warc-fields`, and the standard suggests that it contain information about
    /// the tools that wrote the WARC file such as tool operator, software version and IP address.
    Info,
    /// `response`: a complete scheme-specific response to some request.
    ///
    /// The exact contents of a response depend on the URI scheme of the record's
    /// [target URI](crate::FieldKind::TargetURI); the WARC 1.1 specification only defines a format for the
    /// http and https schemes, with a `response` record containing a full HTTP response
    /// (including headers) received over the network.
    Response,
    /// `resource`: a resource without full protocol response information.
    ///
    /// While a [`response`](RecordKind::Response) record contains a full response as it appears on
    /// the network, a `resource` record discards protocol information- however the exact contents
    /// of the record block still depend on the scheme of the [target URI](crate::FieldKind::TargetURI).
    /// WARC 1.1 specifies meanings for the `http`, `https`, `ftp`, and `dns` schemes.
    Resource,
    /// `request`: a complete scheme-specific request.
    ///
    /// A request record includes a complete request including network protocol information in the
    /// same way that a [`response`](RecordKind::Response) record does, including dependence on the
    /// target URI scheme.
    Request,
    /// `metadata`: content created to further describe, explain or accompany a resource.
    ///
    /// Metadata fields usually refer to another record of some other type that contains original
    /// content that was harvested or transformed. It is suggested that the record block have the
    /// `application/warc-fields` [content type](crate::FieldKind::ContentType) and may have fields
    /// including `via`, `hopsFromSeed`, and `fetchTimeMs`.
    Metadata,
    /// `revisit`: revisitation of content that was already archived.
    ///
    /// A revisit record is typically used instead of a [response](crate::RecordKind::Response) or
    /// [resource](crate::RecordKind::Resource) record to indicate that the received content was a complete
    /// or substantial duplicate of material that was previously archived, allowing reduced storage
    /// size or improved cross-referencing of material.
    ///
    /// Revisit records *shall* have a [`WARC-Profile`](crate::FieldKind::Profile) field describing how
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
    /// [`Segment-Origin-ID`](crate::FieldKind::SegmentOriginID) and
    /// [`WARC-Segment-Number`](crate::FieldKind::SegmentNumber) fields.
    Continuation,
}

include!(concat!(env!("OUT_DIR"), "/record_kind_conversions.rs"));

impl<S: AsRef<str>> PartialEq<S> for RecordKind {
    fn eq(&self, other: &S) -> bool {
        self.as_uncased().eq(other.as_ref())
    }
}

impl Eq for RecordKind {}

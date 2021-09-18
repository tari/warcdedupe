use crate::FieldKind;
use std::borrow::Borrow;
use uncased::{UncasedStr, AsUncased};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

impl From<&FieldKind> for FieldName {
    fn from(kind: &FieldKind) -> FieldName {
        kind.into_name()
    }
}

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
pub enum FieldName {
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
    Other(Box<str>),
}

impl AsRef<str> for FieldName {
    fn as_ref(&self) -> &str {
        use FieldName::*;
        match self {
            Known(x) => x.as_ref(),
            Other(s) => s.as_ref(),
        }
    }
}

impl<S: AsRef<str> + Into<Box<str>>> From<S> for FieldName {
    fn from(s: S) -> Self {
        match <FieldKind as std::convert::TryFrom<&str>>::try_from(s.as_ref()) {
            Ok(x) => FieldName::Known(x),
            Err(_) => FieldName::Other(s.into())
        }
    }
}

impl From<FieldKind> for FieldName {
    fn from(k: FieldKind) -> Self {
        FieldName::Known(k)
    }
}

impl PartialEq<FieldKind> for FieldName {
    fn eq(&self, other: &FieldKind) -> bool {
        match self {
            FieldName::Known(k) => k == other,
            FieldName::Other(s) => s.as_ref().as_uncased() == other.as_ref(),
        }
    }
}


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
        let kind = match self {
            Self::Known(kind) => kind,
            Self::Other(_) => return false,
        };

        use crate::FieldKind::*;
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
            (FieldName::Known(l), FieldName::Known(r)) => l == r,
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

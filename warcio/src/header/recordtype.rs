use crate::RecordKind;
use std::cmp::Ordering;
use uncased::AsUncased;

/// The kind of a single WARC record.
///
/// Every record is specified to have a type in its [`WARC-Type`](crate::FieldKind::Type) field. This
/// enumeration provides variants for those specified in the WARC standard and allows representation
/// of others as might be used by extensions to the core WARC format or future versions.
///
/// A `RecordType` can be parsed from a string using the [`From<str>`](#impl-From<S>) impl, and
/// retrieved as a string via [`AsRef<str>`](#impl-AsRef<str>). Parsed values are case-insensitive
/// and normalize to the standard capitalization, but [unknown](RecordType::Other) values preserve
/// case when parsed. A `RecordType` can also be trivially constructed from a `RecordKind`, meaning
/// most functions that accept a `RecordType` can also accept a `RecordKind`.
///
/// ```
/// # use warcio::{RecordType, RecordKind};
/// let response_type = <RecordType<_> as From<_>>::from("Response");
///
/// assert_eq!(response_type, RecordKind::Response);
/// assert_eq!(response_type, "response");
/// ```
#[derive(Debug, Clone)]
pub enum RecordType<S> {
    /// A known (standardized) record type.
    Known(RecordKind),
    /// Any unrecognized record type.
    ///
    /// This variant allows unknown record types to be represented, beyond those specified. Software
    /// *shall* skip records of unknown type, which may be defined in future versions of the file
    /// format.
    Other(S),
}

impl<S: AsRef<str>> AsRef<str> for RecordType<S> {
    fn as_ref(&self) -> &str {
        match self {
            RecordType::Known(x) => x.as_ref(),
            RecordType::Other(s) => s.as_ref(),
        }
    }
}

// This impl covers RecordKind and hopefully optimization can see through the
// map lookup in TryFrom. An explicit impl depends on specialization.
impl<S: AsRef<str>> From<S> for RecordType<S> {
    fn from(s: S) -> Self {
        match <RecordKind as std::convert::TryFrom<&str>>::try_from(s.as_ref()) {
            Ok(x) => RecordType::Known(x),
            Err(_) => RecordType::Other(s.into()),
        }
    }
}

impl<S: AsRef<str>, T: AsRef<str>> PartialEq<T> for RecordType<S> {
    fn eq(&self, other: &T) -> bool {
        self.as_uncased().eq(other.as_ref())
    }
}

impl<S: AsRef<str>> Eq for RecordType<S> {}

impl<S: AsRef<str>, T: AsRef<str>> PartialOrd<T> for RecordType<S> {
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.as_uncased().partial_cmp(other.as_uncased())
    }
}

impl<S: AsRef<str>> Ord for RecordType<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_uncased().cmp(other.as_uncased())
    }
}

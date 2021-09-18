use crate::RecordKind;

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
    /// A known (standardized) record type.
    Known(RecordKind),
    /// Any unrecognized record type.
    ///
    /// This variant allows unknown record types to be represented, beyond those specified. Software
    /// *shall* skip records of unknown type, which may be defined in future versions of the file
    /// format.
    Other(Box<str>),
}

impl AsRef<str> for RecordType {
    fn as_ref(&self) -> &str {
        match self {
            RecordType::Known(x) => x.as_ref(),
            RecordType::Other(s) => s.as_ref(),
        }
    }
}

impl<S: AsRef<str> + Into<Box<str>>> From<S> for RecordType {
    fn from(s: S) -> Self {
        match <RecordKind as std::convert::TryFrom<&str>>::try_from(s.as_ref()) {
            Ok(x) => RecordType::Known(x),
            Err(_) => RecordType::Other(s.into())
        }
    }
}

impl From<RecordKind> for RecordType {
    fn from(k: RecordKind) -> Self {
        RecordType::Known(k)
    }
}

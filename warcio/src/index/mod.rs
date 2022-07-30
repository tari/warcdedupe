mod cdx;
mod cdxj;

pub enum Field {
    /// A searchable URI, equivalent to the CDJX definition.
    ///
    /// In CDX indexes this is a 'N' field. The value is a canonicalized URI
    /// that is filtered to SURT format and omits the scheme from the final result.
    SearchableUrl,
    /// The WARC-Date of the record.
    ///
    /// 'b' fields in a CDX index.
    Date,
    /// The WARC-Target-URI of the record.
    ///
    /// CDX 'a' field.
    Url,
    /// MIME type of the record.
    ///
    /// This value is warc/revisit for revisit records, the HTTP content type of the
    /// entity body for responses or requests, otherwise the Content-Type header of
    /// the record itself.
    ///
    /// CDX 'm' field.
    MimeType,
    /// HTTP response code (for response and revisit records only)
    ///
    /// CDX 's' field.
    ResponseCode,
    /// WARC-Payload-Digest value.
    ///
    /// CDX 'k' field.
    Digest,
    /// Size of the record data.
    ///
    /// CDX 'S' field.
    Length,
    /// Record offset in the file containing the record.
    ///
    /// CDX 'V' field.
    CompressedFileOffset,
    /// Name of the file containing the described record.
    ///
    /// CDX 'g' field.
    Filename,
}

pub trait Entry {

}

pub trait Index {
    type Entry: Entry;
}

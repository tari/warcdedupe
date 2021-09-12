use crate::{FieldName, Header, Version};

mod read;
mod write;

#[test]
fn header_parse_consumes_full() {
    // "WARC/1.1" CRLF (=version)
    // named-field CRLF (=warc-fields)
    // CRLF
    let text = b"\
        WARC/1.1\r\n\
        Content-Length: 123\r\n\
        \r\n\
    ";

    let (header, sz) = Header::parse(&text[..]).expect("Parse should succeed");
    assert_eq!(sz, text.len());
    let mut test_header = Header::new(Version::WARC1_1);
    test_header.set_field(FieldName::ContentLength, "123");
    assert_eq!(header, test_header);
}

#[test]
fn header_adjusts_bare_uri_brackets() {
    let mut header = Header::new(Version::WARC1_0);
    header.set_field(FieldName::TargetURI, "http://example.com");

    assert_eq!(header.get_field(FieldName::TargetURI), Some("http://example.com"));
    assert_eq!(
        header.get_field_bytes_raw(FieldName::TargetURI),
        Some(&b"<http://example.com>"[..])
    );

    header.set_version(Version::WARC1_1);
    assert_eq!(header.get_field(FieldName::TargetURI), Some("http://example.com"));
    assert_eq!(
        header.get_field_bytes_raw(FieldName::TargetURI),
        Some(&b"http://example.com"[..])
    );
}

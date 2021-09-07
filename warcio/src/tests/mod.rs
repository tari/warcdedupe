use crate::{Header, Version, FieldName};

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

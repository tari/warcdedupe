use crate::header::{FieldName, Header, Version};
use crate::record::{Compression, Record};
use std::io::{Cursor, Read, Write};

#[test]
fn writes_well_formed_warc1_1() {
    let mut header = Header::new(Version::WARC1_1);
    header.set_field(FieldName::ContentLength, &b"8"[..]);

    let mut write_buf = Vec::new();
    let mut body = header
        .write_to(&mut write_buf, Compression::None)
        .expect("failed to write record header");
    body.write_all(b"abcdefgh").unwrap();
    assert_eq!(body.write(b"IGNOREME").unwrap(), 0);
    drop(body);

    assert_eq!(
        String::from_utf8_lossy(&write_buf),
        "WARC/1.1\r
Content-Length: 8\r
\r
abcdefgh\r
\r
"
    );

    let mut reread_cursor = Cursor::new(&write_buf);
    let mut reread_record = Record::read_from(&mut reread_cursor, Compression::None)
        .expect("failed to reread written record");
    let mut reread_body = Vec::new();
    reread_record
        .read_to_end(&mut reread_body)
        .expect("read_to_end() returned an error");
    reread_record.finish().expect("finish() returned an error");

    assert_eq!(
        reread_cursor.position(),
        write_buf.len() as u64,
        "extra data left behind after reading record: {:?}",
        &write_buf[reread_cursor.position() as usize..]
    );
}

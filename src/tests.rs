
/// Verify that it's possible to read multi-record gzip files (like a
/// .warc.gz).
#[test]
fn can_read_multi_record_gzip() {
    use libflate::finish::Complete;
    use libflate::gzip;
    use std::io::{self, Read, Seek, SeekFrom, Write};

    const MESSAGES: &[&'static [u8]] = &[b"Hello, world!", b"This is the second message."];

    let compressed: Vec<u8> = vec![];
    let mut cursor = io::Cursor::new(compressed);
    for message in MESSAGES {
        let mut encoder = gzip::Encoder::new(&mut cursor).unwrap();
        encoder.write_all(message).unwrap();
        encoder.complete().unwrap();
    }

    cursor.seek(SeekFrom::Start(0)).unwrap();
    for message in MESSAGES.iter() {
        let mut decoder = gzip::Decoder::new(&mut cursor).unwrap();
        let mut buf: Vec<u8> = vec![];
        decoder.read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, message);
    }
}

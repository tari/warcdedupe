use std::io::{Cursor, Read};

use pretty_assertions::assert_eq;

use warcio::compression::Compression;
use warcio::record::Record;
use warcio::Header;

use crate::digest::Digester;
use crate::response_log::ResponseLog;
use crate::Deduplicator;

const HTTP_RECORD: &str = "\
WARC/1.0\r
WARC-Type: response\r
WARC-Record-ID: <urn:uuid:409aba28-ce26-43ec-ae90-60dd3de9a60a>\r
WARC-Concurrent-To: <urn:uuid:469a06ca-a78c-4962-8c21-b88c35024d34>\r
WARC-Target-URI: <https://example.com/example.txt>\r
WARC-Date: 2018-01-28T13:33:12Z\r
WARC-IP-Address: 2606:2800:220:1:248:1893:25c8:1946\r
Content-Type: application/http;msgtype=response\r
Content-Length: 132\r
\r
HTTP/1.1 404 Not Found\r
Date: Sun, 28 Jan 2018 13:33:12 GMT\r
Content-Type: text/plain\r
Content-Length: 23\r
\r
There is nothing here.
\r
\r
";

/// A Digester for which records may never be deduplicated.
struct IneligibleDigester;

impl Digester for IneligibleDigester {
    type Digest = ();

    fn new(_: &Header) -> Option<Self> {
        None
    }

    fn handle_data(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn finalize(self) -> Self::Digest {
        unreachable!()
    }
    fn format_digest(_: &Self::Digest) -> String {
        unreachable!()
    }
}

/// A Digester that digests to `()`.
struct UnitDigester;

impl Digester for UnitDigester {
    type Digest = ();

    fn new(_: &Header) -> Option<Self> {
        Some(Self)
    }

    fn handle_data(&mut self, _: &[u8]) {}

    fn finalize(self) -> Self::Digest {
        ()
    }

    fn format_digest(_digest: &Self::Digest) -> String {
        "nop:".into()
    }
}

/// A ResponseLog that panics if a record is checked against it.
struct PanickingLog;

impl<T: Eq> ResponseLog<T> for PanickingLog {
    fn add<Id: Into<String>, Uri: Into<String>, Date: Into<String>, IDigest: Into<T>>(
        &mut self,
        _: Id,
        _: Option<Uri>,
        _: Option<Date>,
        _: IDigest,
    ) -> Option<(&str, Option<&str>, Option<&str>)> {
        unimplemented!()
    }
}

/// A ResponseLog that has always seen a record before with fixed properties.
struct AlwaysSeen;

impl AlwaysSeen {
    const RECORD_ID: &'static str = "urn:fake";
    const URL: Option<&'static str> = Some("http://example.com");
    const DATE: Option<&'static str> = Some("fakedate");
}

impl<T: Eq> ResponseLog<T> for AlwaysSeen {
    fn add<Id: Into<String>, Uri: Into<String>, Date: Into<String>, IDigest: Into<T>>(
        &mut self,
        _: Id,
        _: Option<Uri>,
        _: Option<Date>,
        _: IDigest,
    ) -> Option<(&str, Option<&str>, Option<&str>)> {
        Some((Self::RECORD_ID, Self::URL, Self::DATE))
    }
}

#[test]
fn copies_ineligible() {
    let mut out = Vec::<u8>::new();
    let mut deduplicator =
        Deduplicator::<IneligibleDigester, _, _>::new(&mut out, AlwaysSeen, Compression::None);
    // TODO "failed to fill whole buffer" is a read_exact error from record::finish_internal
    let deduplicated = deduplicator
        .read_record(Cursor::new(HTTP_RECORD.as_bytes()), Compression::None)
        .expect("should read record okay");

    assert!(!deduplicated);
    assert_eq!(
        out,
        HTTP_RECORD.as_bytes(),
        "ineligible record should be copied to output"
    );
}

#[test]
fn deduplicates_duplicate() {
    fn run_with_compression(output_compression: Compression) {
        println!("{:?}", output_compression);

        let mut out = Vec::<u8>::new();
        let mut deduplicator =
            Deduplicator::<UnitDigester, _, _>::new(&mut out, AlwaysSeen, output_compression);
        let deduplicated = deduplicator
            .read_record(Cursor::new(HTTP_RECORD.as_bytes()), Compression::None)
            .expect("should read record okay");
        assert!(deduplicated, "record should have been deduplicated");

        println!("Output record:\n{:?}", &out);
        let mut reparse_cursor = Cursor::new(&out);
        let mut reparsed = Record::read_from(&mut reparse_cursor, output_compression)
            .expect("should be able to parse an emitted revisit record");

        //assert_eq!(reparsed, ...)
        let mut reparsed_contents = Vec::new();
        reparsed.read_to_end(&mut reparsed_contents).unwrap();
        reparsed.finish().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&reparsed_contents),
            "\
HTTP/1.1 404 Not Found\r
Date: Sun, 28 Jan 2018 13:33:12 GMT\r
Content-Type: text/plain\r
Content-Length: 23\r
\r
"
        );
        assert_eq!(
            reparse_cursor.position(),
            out.len() as u64,
            "output record had {} bytes of extra garbage: {:?}",
            out.len() as u64 - reparse_cursor.position(),
            out.as_slice()
        );
    }

    run_with_compression(Compression::None);
    run_with_compression(Compression::Gzip);
}

#[test]
fn deduplicates_compressed() {
    // Two compressed records, both of which are duplicates of a previous one
    let mut compressed_input = Vec::<u8>::new();
    let mut compress_record = |buf| {
        use std::io::Write;

        let mut encoder =
            flate2::write::GzEncoder::new(&mut compressed_input, flate2::Compression::fast());
        encoder.write_all(buf).unwrap();
    };
    compress_record(HTTP_RECORD.as_bytes());
    compress_record(HTTP_RECORD.as_bytes());

    let mut out = Vec::<u8>::new();
    let output_compression = Compression::Gzip;
    let mut deduplicator =
        Deduplicator::<UnitDigester, _, _>::new(&mut out, AlwaysSeen, output_compression);

    let mut cursor = Cursor::new(&compressed_input);
    for _ in 0..2 {
        let deduplicated = deduplicator
            .read_record(&mut cursor, Compression::Gzip)
            .expect("should read record okay");
        assert!(deduplicated, "record should have been deduplicated");
    }

    println!("Output records:\n{:?}", &out);
    let mut reparse_cursor = Cursor::new(&out);

    for _ in 0..2 {
        let mut reparsed = Record::read_from(&mut reparse_cursor, output_compression)
            .expect("should be able to parse an emitted revisit record");

        let mut reparsed_contents = Vec::new();
        reparsed.read_to_end(&mut reparsed_contents).unwrap();
        reparsed.finish().unwrap();

        assert_eq!(
            String::from_utf8_lossy(&reparsed_contents),
            "HTTP/1.1 404 Not Found\r\n\
             Date: Sun, 28 Jan 2018 13:33:12 GMT\r\n\
             Content-Type: text/plain\r\n\
             Content-Length: 23\r\n\
             \r\n"
        );
    }

    assert_eq!(
        reparse_cursor.position(),
        out.len() as u64,
        "output had {} bytes of extra garbage after records: {:?}",
        out.len() as u64 - reparse_cursor.position(),
        out.as_slice()
    );
}

use std::io::Cursor;
use warcdedupe::digest::LengthSha1Digester;
use warcdedupe::response_log::InMemoryResponseLog;
use warcdedupe::Deduplicator;
use warcio::record::RecordReader;
use warcio::{Compression, RecordKind};

#[test]
fn deduplicates_simple_warc() {
    const PNG_WARC_GZ: &[u8] = include_bytes!("png2.warc");
    let mut out: Vec<u8> = vec![];
    let response_log: InMemoryResponseLog<_> = Default::default();

    let mut deduplicator =
        Deduplicator::<LengthSha1Digester, _, _>::new(&mut out, response_log, Compression::Gzip);
    let (n_copied, n_deduplicated) = deduplicator
        .read_stream(Cursor::new(PNG_WARC_GZ), Compression::None)
        .expect("read_stream returned an unexpected error");

    assert_eq!(n_copied, 7);
    assert_eq!(n_deduplicated, 1);

    let mut reader = RecordReader::new(&out[..], Compression::Gzip);
    let mut i = 0;
    const EXPECTED_TYPES: &[RecordKind] = &[
        RecordKind::Info,       // Leading warcinfo
        RecordKind::Request,    // First request for resource
        RecordKind::Response,   // Response for resource
        RecordKind::Request,    // Second request for same resource
        RecordKind::Revisit,    // Same response replaced with revisit
        RecordKind::Metadata,   // "manifest" metadata
        RecordKind::Resource,   // crawler configuration
        RecordKind::Resource,   // crawl log
    ];
    while let Some(r) = reader.next() {
        let record = r.unwrap();
        assert_eq!(
            record.header.warc_type(),
            EXPECTED_TYPES[i],
            "record {} has unexpected type",
            i
        );
        record.finish().unwrap();
        i += 1;
    }
}

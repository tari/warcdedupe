#[macro_use]
extern crate criterion;
extern crate warcio;

use criterion::{Criterion, Fun};
use std::io::BufRead;
use std::time::Duration;
use warcio::{Header, ParseError, get_record_header};

criterion_main!(benches);
criterion_group!{
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(30));
    targets = bench_get_record_header
}

fn get_record_header_windows<R: BufRead>(mut reader: R) -> Result<Header, ParseError> {
    /// Return the index of the first position in the given buffer following
    /// a b"\r\n\r\n" sequence.
    fn find_crlf2(buf: &[u8]) -> Option<usize> {
        for (i, window) in buf.windows(4).enumerate() {
            if window == b"\r\n\r\n" {
                return Some(i + 4);
            }
        }
        None
    }

    // Read bytes out of the input reader until we find the end of the header
    // (two CRLFs in a row).
    // First-chance: without copying anything
    let mut header: Option<(usize, Header)> = None;
    {
        let buf = reader.fill_buf()?;
        if let Some(i) = find_crlf2(buf) {
            // Weird split of parse and consume here is necessary because buf
            // is borrowed from the reader so we can't consume until we no
            // longer hold a reference to the buffer.
            header = Some((i, Header::parse(&buf[..i])?));
        }
    }
    if let Some((sz, header)) = header {
        reader.consume(sz);
        return Ok(header);
    }

    // Need to start copying out of the reader's buffer. Throughout this loop,
    // we've grabbed some number of bytes and own them with a tail copied out
    // of the reader's buffer but still buffered so we can give bytes back at
    // the end.
    let mut buf: Vec<u8> = Vec::new();
    buf.extend(reader.fill_buf()?);
    reader.consume(buf.len());

    let mut bytes_consumed = buf.len();
    loop {
        // Copy out of the reader
        buf.extend(reader.fill_buf()?);
        if buf.len() == bytes_consumed {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof,
                                           "WARC header not terminated")
                               .into());
        }

        if let Some(i) = find_crlf2(&buf) {
            // If we hit, consume up to the hit and done.
            // Our buffer is larger than the reader's: be careful only to
            // consume what the reader gave us most recently, which we haven't
            // taken ownership of yet.
            reader.consume(i - bytes_consumed);
            return Ok(Header::parse(&buf[..i])?);
        }

        // Otherwise keep looking
        reader.consume(buf.len() - bytes_consumed);
        bytes_consumed = buf.len();
        // TODO enforce maximum vec size?
    }
}

fn bench_get_record_header(c: &mut Criterion) {
    const DATA: &[u8] = b"WARC/1.0\r\n\
          WARC-Type: warcinfo\r\n\
          Content-Type: application/warc-fields\r\n\
          WARC-Date: 2018-01-28T09:13:46Z\r\n\
          Content-Length: 464\r\n\
          \r\n\
          software: Wget/1.19.4 (linux-gnu)\r\n\
          format: WARC File Format 1.0\r\n\
          conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n\
          robots: off\r\n\
          wget-arguments: \"--warc-file=2018-01-28/2018-01-28T091346\"\r\n\
           \"--warc-cdx\" \"--warc-max-size=1G\" \"-e\" \"robots=off\"\r\n\
           \"-U\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0\"\r\n\
           \"--timeout\" \"30\" \"--tries\" \"2\" \"--page-requisites\"\r\n\
           \"--load-cookies\" \"cookies.txt\" \"--delete-after\" \"-i\" \"-\"\
          \r\n";

    let windows = Fun::new("Windows",
                           |b, _| b.iter(|| get_record_header_windows(DATA).unwrap()));
    let twoway = Fun::new("Twoway", |b, _| b.iter(|| get_record_header(DATA).unwrap()));

    c.bench_functions("get_record_header", vec![windows, twoway], 0);
}

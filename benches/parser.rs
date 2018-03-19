#[macro_use]
extern crate criterion;
extern crate warcdedupe;

use criterion::Criterion;
use warcdedupe::get_record_header;

criterion_main!(benches);
criterion_group!(benches, bench_get_record_header);

fn bench_get_record_header(c: &mut Criterion) {
    const DATA: &[u8] =
        b"WARC/1.0\r\n\
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

    c.bench_function("parse warcinfo", |b|
                     b.iter(|| get_record_header(DATA).expect("invalid data?!")));
}


extern crate docopt;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate libflate;
#[macro_use]
extern crate serde_derive;
extern crate sha1;
extern crate warcio;

use docopt::Docopt;
use libflate::gzip;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use warcio::reader::InvalidRecord;

const USAGE: &'static str = "
WARC deduplicator.

Usage:
  warcdedupe [options] [<infile>] [<outfile>]

If infile or outfile is not specified or is '-', read from standard input or
write to standard output.

Options:
  -h --help             Show this help.
  --compressed-input    Assume records in non-file input are compressed.
  --compress-output     Write compressed records to non-file output.

When input or output is a file, the --compressed-input and --compress-output
options are ignored each is assumed to be compressed if the file name ends in
'.gz'.
";

#[derive(Debug, Deserialize)]
struct Args {
    arg_infile: Option<PathBuf>,
    arg_outfile: Option<PathBuf>,
    flag_compressed_input: bool,
    flag_compress_output: bool,
}

/// Transform "-" into None to use stdio instead of a file.
fn maybe_file(o: Option<PathBuf>) -> Option<PathBuf> {
    o.and_then(|p| if p.as_os_str() == "-" { None } else { Some(p) })
}

fn file_is_gzip(p: &Path) -> bool {
    p.extension().map(|ext| ext == "gz").unwrap_or(false)
}

// Lazy statics for stdio because we lock them to help ensure no accidental use
// otherwise.
lazy_static!{
    static ref STDIN: std::io::Stdin = std::io::stdin();
    static ref STDOUT: std::io::Stdout = std::io::stdout();
}

fn open_input_stream(p: Option<PathBuf>, compressed_stream: bool) -> Box<BufRead> {
    if let Some(p) = maybe_file(p) {
        // From file
        let file = BufReader::new(File::open(&p).expect("Failed to open input file"));

        if file_is_gzip(&p) {
            Box::new(BufReader::new(gzip::MultiDecoder::new(file).expect(
                        "Failed to initialize gzip decoder")))
        } else {
            Box::new(file)
        }
    } else {
        // From stdin
        if compressed_stream {
            Box::new(BufReader::new(gzip::MultiDecoder::new(STDIN.lock()).expect(
                    "Failed to set up compressed input stream")))
        } else {
            Box::new(STDIN.lock())
        }
    }
}

// TODO we need to write with record granularity too, need a custom trait.
fn open_output_stream(p: Option<PathBuf>, compress_stream: bool) -> Box<Write> {
    if let Some(p) = maybe_file(p) {
        // To file
        let file = File::create(&p).expect("Failed to create output file");

        if file_is_gzip(&p) {
            unimplemented!();
        } else {
            Box::new(file)
        }
    } else {
        // To stdout
        if compress_stream {
            unimplemented!();
        } else {
            Box::new(STDOUT.lock())
        }
    }
}

fn main() {
    let args: Args = Docopt::new(USAGE).and_then(|d| d.deserialize()).unwrap_or_else(|e| e.exit());

    let mut input = open_input_stream(args.arg_infile, args.flag_compressed_input);
    let mut output = open_output_stream(args.arg_outfile, args.flag_compress_output);

    let mut input_size = 0u64;
    let mut output_size = 0u64;
    let mut dedup = InMemoryResponseLog::new();
    loop {
        let mut record = match warcio::reader::Record::read_from(&mut input) {
            Ok(r) => r,
            Err(InvalidRecord::EndOfStream) => {
                break;
            }
            Err(e) => panic!("Error reading record: {:?}", e),
        };

        if record.header.warc_type() != Some("response") {
            debug!("Skip non-response record {:?}", record.header);
            continue;
        }

        let len = match record.header.content_length() {
            Some(n) => n,
            None => {
                eprintln!("Ignoring record without Content-Length: {:?}", record.header);
                continue;
            }
        };

        let mut sha = Sha1::new();
        let mut buf = [0u8; 8 << 10];

        loop {
            let n = match record.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => unimplemented!(),
            };
            sha.input(&buf[..n]);
        }
        let digest = {
            let mut fixed = [0u8; 20];
            fixed.copy_from_slice(&sha.result());
            fixed
        };

        let target = match record.header.field_str("warc-target-uri") {
            None => {
                eprintln!("Ignoring record without warc-target-uri: {:?}", record.header);
                continue;
            },
            Some(t) => t
        };
        let timestamp = match record.header.warc_date() {
            None => {
                eprintln!("Ignoring record without warc-date: {:?}", record.header);
                continue;
            },
            Some(t) => t,
        };

        // TODO need to look at HTTP response body.
        // Response headers tend to vary (Date: headers..), so check the response
        // Content-Type (application/http) and try to look at only the response
        // body. In case of match, still write out the HTTP headers since we haven't
        // deduplicated them.
        //
        // Also need to add WARC-Refers-To to the new revisit record with the WARC-Record-ID
        // of the original copy, where the original is always the chronologically earliest.
        //
        // We can parallelize (and possibly lose some savings) by recording the first-seen
        // date and retrieving it back out, updating it if the current record is older than
        // the currently-known oldest record. If the returned timestamp is earlier than
        // the current record, write a revisit pointing at the returned record ID.
        //
        // Parallelism requires either multiple files input, or a work queue that might be
        // derived from a cdx index or another worker just seeking through records and recording
        // their start points in a larger file.
        input_size += len;
        match dedup.add(target.to_owned(), timestamp.to_owned(), digest) {
            Some((_, first_seen)) => {
                println!("{} dup {} size {}", target, first_seen, len);
            },
            None => {
                // Not a duplicate
                output_size += len;
            }
        }
    }

    eprintln!("{} bytes in, {} out\n{} saved", input_size, output_size, input_size - output_size);
}

/// Generic log for tracking seen responses.
///
/// While the basic version of this is an in-memory mapping, for very large
/// archives an alternate index may be desired.
trait ResponseLog {
    /// Record a response for the specified URI with SHA-1 digest, captured at
    /// the specified date.
    ///
    /// Returns None if that response has not been seen before, otherwise the
    /// URI and date that it was first seen.
    // TODO arbitrary GenericArray digest that could be multiple ones
    // concatenated for better collision resistance?
    fn add<'a, 'b>(&'a mut self, target_uri: String, date: String, digest: [u8; 20])
        -> Option<(String, &'a str)>;
}

/// Basic implementation of a `ResponseLog`.
///
/// This implementation will only deduplicate identical responses from the same
/// URL because it currently lacks strong protection against hash collisions.
struct InMemoryResponseLog(HashMap<([u8; 20], String), String>);

impl InMemoryResponseLog {
    fn new() -> Self {
        InMemoryResponseLog(
            HashMap::new()
        )
    }
}

impl ResponseLog for InMemoryResponseLog {
    fn add<'a, 'b>(&'a mut self, target_uri: String, date: String, digest: [u8; 20]) -> Option<(String, &'a str)> {
        // API limitation: we need to take an owned copy of the target URI to look up, even if we
        // don't need to insert since K: Borrow<Q> doesn't allow us to return a ref to a tuple that
        // the HashMap owns. But we also can't get a ref to the key to return without
        // HashMap::get_key_value (#49347), so we can at least reuse it.
        let key = (digest, target_uri.to_owned());
        if self.0.contains_key(&key) {
            let first_seen = self.0.get(&key).unwrap();
            Some((key.1, first_seen))
        } else {
            self.0.insert(key, date);
            None
        }
    }
}

/// A `ResponseLog` implementation backed by LMDB, a high-performance embedded key-value store.
/// 
/// LMDB should scale better than the simple in-memory response log and is persistent, but like
/// the in-memory log is limited to a single machine.
#[cfg(feature = "lmdb")]
struct LmdbResponseLog {

}

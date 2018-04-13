extern crate docopt;
#[macro_use]
extern crate lazy_static;
extern crate libflate;
#[macro_use]
extern crate serde_derive;
extern crate warcio;

use docopt::Docopt;
use libflate::gzip;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Result as IoResult, Write};
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
        let file = File::open(&p).expect("Failed to open input file");

        if file_is_gzip(&p) {
            Box::new(BufReader::new(gzip::MultiDecoder::new(file).expect(
                        "Failed to initialize gzip decoder")))
        } else {
            Box::new(BufReader::new(file))
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

    loop {
        let record = match warcio::reader::Record::read_from(&mut input) {
            Ok(r) => r,
            Err(InvalidRecord::EndOfStream) => {
                break;
            }
            Err(e) => panic!("Error reading record: {:?}", e),
        };

        match record.header.warc_type() {
            Some("warcinfo") => {
                println!("WARCINFO for {}", record.header.field_str("warc-filename")
                         .unwrap_or("[unknown warc-filename]"));
            }
            Some("request") => {
                println!("REQUEST for {}", record.header.field_str("warc-target-uri")
                         .unwrap_or("[unknown warc-target-uri]"));
            }
            Some("response") => {
                println!("RESPONSE for {} ({} bytes)",
                         record.header.field_str("warc-target-uri")
                                .unwrap_or("[unknown warc-target-uri]"),
                         record.header.content_length()
                                .unwrap_or(0));
            }
            Some(s) => println!("UNKNOWN {}", s),
            None => println!("MISSING_TYPE"),
        }
    }
}

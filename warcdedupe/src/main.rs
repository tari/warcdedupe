#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use docopt::Docopt;
use flate2::read::MultiGzDecoder;

use warcdedupe::digest::UrlLengthBlake3Digester;
use warcdedupe::response_log::InMemoryResponseLog;
use warcdedupe::Deduplicator;

const USAGE: &str = "
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
lazy_static! {
    static ref STDIN: std::io::Stdin = std::io::stdin();
    static ref STDOUT: std::io::Stdout = std::io::stdout();
}

fn open_input_stream(p: Option<PathBuf>, compressed_stream: bool) -> Box<dyn BufRead> {
    if let Some(p) = maybe_file(p) {
        // From file
        let file = BufReader::new(File::open(&p).expect("Failed to open input file"));

        if file_is_gzip(&p) {
            Box::new(BufReader::new(MultiGzDecoder::new(file)))
        } else {
            Box::new(file)
        }
    } else {
        // From stdin
        if compressed_stream {
            Box::new(BufReader::new(MultiGzDecoder::new(STDIN.lock())))
        } else {
            Box::new(STDIN.lock())
        }
    }
}

// TODO we need to write with record granularity too, need a custom trait.
fn open_output_stream(p: Option<PathBuf>, compress_stream: bool) -> Box<dyn Write> {
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
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let input = open_input_stream(args.arg_infile, args.flag_compressed_input);
    let output = open_output_stream(args.arg_outfile, args.flag_compress_output);

    let mut deduplicator =
        Deduplicator::<UrlLengthBlake3Digester, _>::new(InMemoryResponseLog::new());

    deduplicator.read(input);
}

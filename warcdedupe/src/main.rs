#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::{BufReader, Cursor, Write};
use std::path::PathBuf;

use docopt::Docopt;
use warcio::Compression;

use warcdedupe::digest::UrlLengthBlake3Digester;
use warcdedupe::response_log::InMemoryResponseLog;
use warcdedupe::Deduplicator;

const USAGE: &str = "
WARC deduplicator.

Usage:
  warcdedupe [options] <infile> [<outfile>]

If infile or outfile is not specified or is '-', read from standard input or
write to standard output.

Options:
  -h --help             Show this help.
  --compress-output     Write compressed records to non-file output.
  --disable-mmap        Never memory-map the input file, even if possible.

When output is a file, the --compress-output option is ignored. Input and
output files are assumed to be compressed if the file name ends in '.gz'.
";

#[derive(Debug, Deserialize)]
struct Args {
    arg_infile: PathBuf,
    arg_outfile: Option<PathBuf>,
    flag_compress_output: bool,
    flag_disable_mmap: bool,
}

/// Transform "-" into None to use stdio instead of a file.
fn maybe_file(o: Option<PathBuf>) -> Option<PathBuf> {
    o.and_then(|p| if p.as_os_str() == "-" { None } else { Some(p) })
}

fn open_output_stream(p: Option<PathBuf>) -> Box<dyn Write> {
    if let Some(p) = maybe_file(p) {
        Box::new(File::create(&p).expect("Failed to create output file"))
    } else {
        // Locking stdout takes a ref to the instance, so it must be static
        lazy_static! {
            static ref STDOUT: std::io::Stdout = std::io::stdout();
        }
        Box::new(STDOUT.lock())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init_custom_env("WARCDEDUPE_LOG");

    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let input_compression = Compression::guess_for_filename(&args.arg_infile);
    let input_file = std::fs::File::open(args.arg_infile)?;
    let output_compression = args
        .arg_outfile
        .as_ref()
        .map(Compression::guess_for_filename)
        .unwrap_or(Compression::None);
    let output = open_output_stream(args.arg_outfile);

    let mut deduplicator = Deduplicator::<UrlLengthBlake3Digester, _, _>::new(
        output,
        InMemoryResponseLog::new(),
        output_compression,
    );

    if args.flag_disable_mmap {
        deduplicator.read(BufReader::with_capacity(16, input_file), input_compression)
    } else {
        let input_map = unsafe { memmap2::Mmap::map(&input_file)? };
        // We do sequential access, so advise the OS of that where such a mechanism exists.
        #[cfg(unix)]
        unsafe {
            use nix::sys::mman::{madvise, MmapAdvise};
            let _ = madvise(
                input_map.as_ptr() as *mut _,
                input_map.len(),
                MmapAdvise::MADV_SEQUENTIAL,
            );
        }
        deduplicator.read(Cursor::new(&input_map), input_compression)
    }
    .unwrap();

    Ok(())
}

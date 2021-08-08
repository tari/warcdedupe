#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

use std::fs::File;
use std::io::{BufReader, Cursor, Write};
use std::path::PathBuf;

use docopt::Docopt;
use warcio::record::Compression;

use warcdedupe::digest::UrlLengthBlake3Digester;
use warcdedupe::response_log::InMemoryResponseLog;
use warcdedupe::{Deduplicator, ProcessError};

const USAGE: &str = "
WARC deduplicator.

Usage:
  warcdedupe [options] <infile> [<outfile>]

If outfile is not specified or is '-', it will be written to standard output.

Options:
  -h --help             Show this help.
  --compress-output     Write compressed records to non-file output.
  --disable-mmap        Never memory-map the input file, even if possible.
  --disable-progress    Do not display progress, even to a terminal.

When output is a file, the --compress-output option is ignored. Input and
output files are assumed to be compressed if the file name ends in '.gz'.
";

#[derive(Debug, Deserialize)]
struct Args {
    arg_infile: PathBuf,
    arg_outfile: Option<PathBuf>,
    flag_compress_output: bool,
    flag_disable_mmap: bool,
    flag_disable_progress: bool,
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
    let default_output_compression = if args.flag_compress_output {
        Compression::Gzip
    } else {
        Compression::None
    };
    let output_compression = args
        .arg_outfile
        .as_ref()
        .map_or(default_output_compression, Compression::guess_for_filename);
    let output = open_output_stream(args.arg_outfile);

    let deduplicator = Deduplicator::<UrlLengthBlake3Digester, _, _>::new(
        output,
        InMemoryResponseLog::new(),
        output_compression,
    );

    let (n_copied, n_deduped) = if args.flag_disable_mmap {
        run_with_progress(
            !args.flag_disable_progress,
            deduplicator,
            BufReader::with_capacity(1 << 20, input_file),
            input_compression,
        )
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
        run_with_progress(
            !args.flag_disable_progress,
            deduplicator,
            Cursor::new(&input_map),
            input_compression,
        )
    }
    .unwrap();

    eprintln!(
        "Processed {} records, deduplicated {}",
        n_copied + n_deduped,
        n_deduped
    );
    Ok(())
}

fn run_with_progress<R: std::io::BufRead + std::io::Seek, D, L, W>(
    enable: bool,
    mut deduplicator: Deduplicator<D, L, W>,
    mut input: R,
    input_compression: Compression,
) -> Result<(u64, u64), ProcessError>
where
    R: std::io::BufRead + std::io::Seek,
    D: warcdedupe::digest::Digester,
    <D as warcdedupe::digest::Digester>::Digest: Clone + Eq,
    L: warcdedupe::response_log::ResponseLog<D::Digest>,
    W: std::io::Write,
{
    if enable {
        let input_len = {
            use std::io::SeekFrom;

            let n = input.seek(SeekFrom::End(0))?;
            input.seek(SeekFrom::Start(0))?;
            n
        };
        let progress = indicatif::ProgressBar::new(input_len).with_style(
            indicatif::ProgressStyle::default_bar().template(
                "[{elapsed} ETA {eta}] {wide_bar} [{bytes}/{total_bytes} ({bytes_per_sec})]",
            ),
        );
        progress.set_draw_rate(1);

        let out = deduplicator.read_stream(progress.wrap_read(input), input_compression);
        progress.finish();
        out
    } else {
        deduplicator.read_stream(input, input_compression)
    }
}

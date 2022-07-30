#[macro_use]
extern crate lazy_static;

use clap::{command, Arg};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Write};

use warcio::compression::Compression;

use std::ffi::OsStr;
use warcdedupe::digest::LengthSha1Digester;
use warcdedupe::response_log::InMemoryResponseLog;
use warcdedupe::{Deduplicator, ProcessError};

fn open_output_stream(p: Option<&OsStr>) -> Box<dyn Write> {
    if let Some(p) = p {
        Box::new(BufWriter::new(
            File::create(&p).expect("Failed to create output file"),
        ))
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

    let matches = command!()
        .arg(Arg::with_name("compress-output")
            .long("compress-output")
            .help("Write compressed records to non-file output"))
        .arg(Arg::with_name("disable-mmap")
            .long("disable-mmap")
            .help("Never memory-map the input, even if possible"))
        .arg(Arg::with_name("disable-progress")
            .long("disable-progress")
            .help("Do not display progress, even to a terminal"))
        .arg(Arg::with_name("infile")
            .required(true)
            .allow_invalid_utf8(true)
            .help("Name of file to read"))
        .arg(Arg::with_name("outfile")
            .required(false)
            .allow_invalid_utf8(true)
            .help("Name of file to write, stdout if omitted or '-'"))
        .after_help("When output is a file, compression options are ignored. Input and output \n\
                          output files are assumed to be compressed if the file name ends in '.gz'.")
        .get_matches();

    let infile_path = matches.value_of_os("infile").unwrap();
    let input_compression = Compression::guess_for_filename(infile_path);
    let input_file = std::fs::File::open(infile_path)?;

    let outfile_path = matches.value_of_os("outfile").filter(|&p| p != "-");
    let default_output_compression = if matches.is_present("compress-output") {
        Compression::Gzip
    } else {
        Compression::None
    };
    let output_compression = match outfile_path {
        None => default_output_compression,
        Some(p) => Compression::guess_for_filename(p),
    };
    let output = open_output_stream(outfile_path);

    let deduplicator = Deduplicator::<LengthSha1Digester, _, _>::new(
        output,
        InMemoryResponseLog::new(),
        output_compression,
    );

    let enable_progress = !matches.is_present("disable-progress");
    let (n_copied, n_deduped) = if matches.is_present("disable-mmap") {
        run_with_progress(
            enable_progress,
            deduplicator,
            BufReader::with_capacity(1 << 20, input_file),
            input_compression,
        )
    } else {
        // Safety: downstream users of the created Cursor are not prevented from interpreting
        // data in a way that would break if the underlying file were mutated, but as this is
        // a binary target we will assume the user can ensure that doesn't happen.
        let map = unsafe { SequentialMmap::of_file(&input_file)? };

        run_with_progress(
            enable_progress,
            deduplicator,
            Cursor::new(&map),
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
            ).unwrap(),
        );

        let out = deduplicator.read_stream(progress.wrap_read(input), input_compression);
        progress.finish();
        out
    } else {
        deduplicator.read_stream(input, input_compression)
    }
}

struct SequentialMmap {
    map: memmap2::Mmap,
}

impl SequentialMmap {
    /// This is unsafe because some uses can trigger undefined behavior if another mapping of the
    /// same file (perhaps in another process) mutates what appears to Rust to be an immutable
    /// slice. See https://users.rust-lang.org/t/how-unsafe-is-mmap/19635 for detailed discussion.
    unsafe fn of_file(file: &File) -> std::io::Result<SequentialMmap> {
        let map = memmap2::Mmap::map(file)?;
        // We do sequential access, so advise the OS of that where such a mechanism exists.
        #[cfg(unix)]
        {
            use nix::sys::mman::{madvise, MmapAdvise};
            let _ = madvise(
                map.as_ptr() as *mut _,
                map.len(),
                MmapAdvise::MADV_SEQUENTIAL,
            );
        }

        Ok(SequentialMmap { map })
    }
}

impl AsRef<[u8]> for SequentialMmap {
    fn as_ref(&self) -> &[u8] {
        self.map.as_ref()
    }
}

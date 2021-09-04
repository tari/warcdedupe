`warcdedupe`: a tool and libraries for manipulating and deduplicating
[WARC files](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/).

## `warcdedupe`

TODO: how does performance compare to https://github.com/ArchiveTeam/warc-dedup?
(a goal here is speed, so hopefully it's very favorable)
Answer: about 5x faster (12 vs 66 seconds) and requires about 1/3 as much memory (once
warc-dedupe is tweaked to not print lots of stuff to the console or
contact IA). Note that RSS metrics are misleading when memory-mapping the input
file.

Given an input WARC file, output a copy with duplicate response records
rewritten to be revisit records pointing to the first instance of that response
in the same file.

Disclaimer: **this tool's behavior is not yet well-tested**: although it
appears to correctly deduplicate records and write valid WARC files, its
correctness and validity of output files may not be standards-compliant.
Users are strongly advised to test its correctness if the results will be
used to replace the original inputs for long-term storage.

## `warcio`

The `warcio` crate is intended to be suitable for general use in programs that
need to read or write WARC files, similar to the [Python library of the same
name](https://pypi.org/project/warcio/).

At present its API is designed according to the needs of `warcdedupe`, but
suggestions or commentary on ways it may be improved for general use are
welcome.

## Performance

Good performance is a goal of this tool. Within reason, additional
complexity is acceptable where it results in improved runtime performance.

In `warcdedupe`, command line options are offered to disable some ergonomic
features that have runtime cost. In particular, enabling progress reporting
(the default unless a terminal is not available, turned off with
`--disable-progress`) incurs a performance hit of around 5% in tests. In some
situations it may also be useful to avoid memory-mapping an input file
(`--disable-mmap`) and instead use regular `read` operations copying data into
memory, though the performance effects of doing so are strongly dependent
on the underlying storage (however if operating on files that may be
concurrently modified, avoiding `mmap` also ensures concurrent modifications
cannot cause `warcdedupe` to crash as a result).

When possible, `warcdedupe` avoids unnecessary compression or decompression of
records when the input and output use the same compression (if both are
uncompressed or both are compressed with gzip, for instance). This requires
support for random access to inputs (usually implying the input must be a
regular file).

### Optimization

As is typical for Rust programs, performance is not very good in debug builds,
which are the default for Cargo. In my testing, debug builds are around 50
times slower: **remember to build in release mode** for actual use!

When building `warcdedupe` you may select an alternate compression backend for
handling gzipped records via feature flags. Of particular interest, `zlib-ng`
is around 10% faster than the default choice for decompression, although it
does require a C compiler at build-time. You can use `zlib-ng` by turning on
the `flate2/zlib-ng-compat` feature:

    cargo build --release --features flate2/zlib-ng-compat


## Future improvements

 * Parallel processing of records if an index is available
 * Something about interop with warcio the python module.
 * Check behavior around <> surrounding some URIs:
   github.com/iipc/warc-specifications/pull/24
   Handle all of:
    * Correct formed WARC 1.0 (with <>)
    * Malformed WARC 1.1 (with <>)
    * Correctly formed WARC 1.1 (without <>)

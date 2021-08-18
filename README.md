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

Disclaimer: this tool doesn't currently do much that can be considered useful;
it's both a prototype and a work in progress. In particular, while it is currently
able to identify duplicate records in a WARC file it cannot output a new file with
Revisit records replacing the originals.

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

## `warcio`

## Future improvements

 * More speed. Faster decompression (flate2 is faster than libflate for
   decompression), parallel reading/parsing with some kind of exotic buffer
   (or possibly iobuf).

 * Something about interop with warcio the python module.

 * Check behavior around <> surrounding some URIs:
   github.com/iipc/warc-specifications/pull/24

 * Per above, handle all of
   * Correctly formed WARC 1.0
   * Malformed URIs in WARC 1.1
   * Correctly formed WARC 1.1

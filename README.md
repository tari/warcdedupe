`warcdedupe`: a tool for deduplicating WARC files.

Given an input WARC file, output a copy with duplicate response records
rewritten to be revisit records pointing to the first instance of that response
in the same file.

Disclaimer: this tool doesn't currently do much that can be considered useful;
it's both a prototype and a work in progress.

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

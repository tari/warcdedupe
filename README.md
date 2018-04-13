`warcdedupe`: a tool for deduplicating WARC files.

Given an input WARC file, output a copy with duplicate response records
rewritten to be revisit records pointing to the first instance of that response
in the same file.

## Future improvements

 * More speed. Faster decompression (flate2 is faster than libflate for
   decompression), parallel reading/parsing with some kind of exotic buffer
   (or possibly iobuf).

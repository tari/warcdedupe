`warcdedupe`: a tool for deduplicating WARC files.

Given an input WARC file, output a copy with duplicate response records
rewritten to be revisit records pointing to the first instance of that response
in the same file.

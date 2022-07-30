//! Internet Archive
//! [CDX](https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/)

struct CdxIndex {
    fields: Box<[Field]>,
}

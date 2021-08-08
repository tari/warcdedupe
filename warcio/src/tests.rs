use crate::header::{get_record_header, Field, Header, Version};
use crate::ParseError;

#[test]
fn can_read_record_header() {
    let header = b"WARC/1.0\r\n\
                   Warc-Type: testdata\r\n\
                   Content-Length: 6\r\n\
                   X-Multiline-Test:lol \r\n  multiline headers\r\n\
                   \r\n";

    let mut expected = Header::new(1, 0);
    expected.set_field("warc-type", b"testdata".to_vec());
    expected.set_field("content-length", b"6".to_vec());
    expected.set_field("x-multiline-test", b"lol multiline headers".to_vec());

    assert_eq!(
        get_record_header(&header[..]).expect("Should be valid"),
        expected
    );
}

#[test]
fn extra_buffering_works() {
    use std::io::{self, Result};
    /// A type to probe the buffering behavior of `get_record_header`.
    ///
    /// On each `fill_buf` call it transitions to the next state, and
    /// after two it is in the terminal state.
    #[derive(Debug, PartialEq)]
    enum DoubleBuffer<'a> {
        /// Nothing read yet.
        Start(&'a [u8], &'a [u8]),
        /// One whole buffer read.
        Second(&'a [u8]),
        /// Both buffers read, with n bytes read from the second.
        Done(usize),
    }
    // Only because BufRead: Read
    impl<'a> io::Read for DoubleBuffer<'a> {
        fn read(&mut self, _: &mut [u8]) -> Result<usize> {
            unimplemented!();
        }
    }
    impl<'a> io::BufRead for DoubleBuffer<'a> {
        fn fill_buf(&mut self) -> Result<&[u8]> {
            eprintln!("fill_buf {:?}", self);
            match self {
                &mut DoubleBuffer::Start(fst, _) => Ok(fst),
                &mut DoubleBuffer::Second(snd) => Ok(snd),
                &mut DoubleBuffer::Done(_) => panic!("Should not fill after snd"),
            }
        }

        fn consume(&mut self, amt: usize) {
            eprintln!("consume {} {:?}", amt, self);
            let next = match *self {
                DoubleBuffer::Start(fst, snd) => {
                    assert_eq!(amt, fst.len());
                    DoubleBuffer::Second(snd)
                }
                DoubleBuffer::Second(snd) => DoubleBuffer::Done(snd.len() - amt),
                DoubleBuffer::Done(_) => panic!("Should not consume after snd"),
            };
            *self = next;
        }
    }

    let mut reader = DoubleBuffer::Start(
        b"WARC/1.0\r\n\
                                           X-First-Header: yes\r\n\
                                           X-Second-Header:yes\r\n\
                                           \r",
        // Header termination spans two buffers
        // to catch potential errors there.
        b"\nIGNORED_DATA",
    );
    get_record_header(&mut reader).expect("failed to parse valid header");
    assert_eq!(reader, DoubleBuffer::Done(12));
}

#[test]
fn incorrect_signature_is_invalid() {
    assert_eq!(
        Version::parse(b"\x89PNG\r\n\x1a\n"),
        Err(ParseError::InvalidSignature)
    );
    assert_eq!(
        Version::parse(b"WARC/1.0a\r\n"),
        Err(ParseError::InvalidSignature)
    );
}

#[test]
fn truncated_header_is_invalid() {
    use std::io;
    const BYTES: &[u8] = b"WARC/1.1\r\n
                           Warc-Type: testdata\r\n\r";

    assert_eq!(
        get_record_header(BYTES),
        Err(ParseError::IoError(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "WARC header not terminated"
        )))
    );
}

#[test]
fn invalid_fields_are_invalid() {
    assert_eq!(
        Field::parse(b"This is not a valid field"),
        Err(ParseError::MalformedField)
    );

    assert_eq!(
        Field::parse(b"X-Invalid-UTF-8\xFF: yes"),
        Err(ParseError::MalformedField)
    );
}

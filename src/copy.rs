use std::fs::File;
use std::io;
use std::ptr;

/// Copy from the src to dst file, from the current file offsets in each.
// TODO replace with std::io::copy, which already knows how to use OS features
// to do efficient copies.
#[cfg(target_os="linux")]
pub fn copy_file(src: &File, dst: &mut File, mut n_bytes: usize) -> io::Result<()> {
    use errno::errno;
    use libc::{sendfile, size_t};
    use std::os::unix::io::AsRawFd;

    while n_bytes > 0 {
        let res = unsafe {
            sendfile(dst.as_raw_fd(),
                     src.as_raw_fd(),
                     ptr::null_mut(),
                     n_bytes as size_t)
        };

        if res < 0 {
            let err = errno().0;
            debug_assert!(err != 0);
            return Err(io::Error::from_raw_os_error(err));
        }

        n_bytes -= res as usize;
    }

    // TODO Possible future work: ioctl(FIDEDUPERANGE) for the copied data
    Ok(())
}

use std::ffi::CString;
use std::os::fd::RawFd;

use crate::error::FxfspError;
use crate::io::aligned_buf::{AlignedBuf, IO_ALIGN, alloc_aligned};
use crate::io::platform::{configure_direct_io, direct_open_flags};

/// Default buffer size: 16 MiB.
const DEFAULT_BUF_SIZE: usize = 16 * 1024 * 1024;

/// A direct-I/O engine with a single reusable aligned buffer.
pub struct IoEngine {
    fd: RawFd,
    buf: AlignedBuf,
}

impl IoEngine {
    /// Open `path` with direct I/O.
    pub fn open(path: &str) -> Result<Self, FxfspError> {
        let c_path =
            CString::new(path).map_err(|_| FxfspError::Parse("invalid path (contains NUL)"))?;
        let flags = direct_open_flags();
        let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
        if fd < 0 {
            return Err(FxfspError::Io(std::io::Error::last_os_error()));
        }
        configure_direct_io(fd)?;
        Ok(Self {
            fd,
            buf: alloc_aligned(DEFAULT_BUF_SIZE),
        })
    }

    /// Read exactly `len` bytes at byte offset `offset` into the internal buffer.
    /// Returns a slice into the buffer. `len` must be a multiple of `IO_ALIGN`.
    pub fn read_at(&mut self, offset: u64, len: usize) -> Result<&[u8], FxfspError> {
        assert!(
            len % IO_ALIGN == 0,
            "read length {len} not aligned to {IO_ALIGN}"
        );

        // Grow buffer if needed.
        if self.buf.len() < len {
            self.buf = alloc_aligned(len);
        }

        let mut total = 0usize;
        while total < len {
            let ret = unsafe {
                libc::pread(
                    self.fd,
                    self.buf[total..].as_mut_ptr() as *mut libc::c_void,
                    len - total,
                    (offset + total as u64) as libc::off_t,
                )
            };
            if ret < 0 {
                return Err(FxfspError::Io(std::io::Error::last_os_error()));
            }
            if ret == 0 {
                return Err(FxfspError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected EOF during pread",
                )));
            }
            total += ret as usize;
        }

        Ok(&self.buf[..len])
    }
}

impl Drop for IoEngine {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

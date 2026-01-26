use std::ffi::CString;
use std::os::fd::RawFd;

use crate::error::FxfspError;
use crate::io::aligned_buf::{AlignedBuf, IO_ALIGN, alloc_aligned};
use crate::io::platform::{configure_direct_io, direct_open_flags};

/// Default buffer size: 256 MiB (large for batch reads).
const DEFAULT_BUF_SIZE: usize = 256 * 1024 * 1024;

/// A direct-I/O engine with a single reusable aligned buffer.
pub struct IoEngine {
    fd: RawFd,
    buf: AlignedBuf,
    device_size: u64,
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

        // Get device/file size via lseek to end.
        let size = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
        if size < 0 {
            unsafe {
                libc::close(fd);
            }
            return Err(FxfspError::Io(std::io::Error::last_os_error()));
        }

        Ok(Self {
            fd,
            buf: alloc_aligned(DEFAULT_BUF_SIZE),
            device_size: size as u64,
        })
    }

    /// Device/file size in bytes.
    pub fn device_size(&self) -> u64 {
        self.device_size
    }

    /// Read up to `len` bytes at byte offset `offset`.
    /// Automatically clamps to device size and I/O alignment.
    /// Returns a slice into the internal buffer (may be shorter than `len`
    /// if near end of device).
    pub fn read_at(&mut self, offset: u64, len: usize) -> Result<&[u8], FxfspError> {
        // Clamp to device boundary.
        let available = self.device_size.saturating_sub(offset) as usize;
        let clamped = len.min(available);
        // Round down to alignment.
        let clamped = clamped & !(IO_ALIGN - 1);
        if clamped == 0 {
            return Err(FxfspError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read at or beyond device boundary",
            )));
        }

        // Grow buffer if needed.
        if self.buf.len() < clamped {
            self.buf = alloc_aligned(clamped);
        }

        let mut total = 0usize;
        while total < clamped {
            let ret = unsafe {
                libc::pread(
                    self.fd,
                    self.buf[total..].as_mut_ptr() as *mut libc::c_void,
                    clamped - total,
                    (offset + total as u64) as libc::off_t,
                )
            };
            if ret < 0 {
                return Err(FxfspError::Io(std::io::Error::last_os_error()));
            }
            if ret == 0 {
                break; // EOF
            }
            total += ret as usize;
        }

        if total == 0 {
            return Err(FxfspError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected EOF during pread",
            )));
        }

        Ok(&self.buf[..total])
    }
}

impl Drop for IoEngine {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

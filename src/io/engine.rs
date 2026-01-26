use std::ffi::CString;
use std::io::Write;
use std::os::fd::RawFd;

use crate::error::FxfspError;
use crate::io::aligned_buf::{AlignedBuf, IO_ALIGN, alloc_aligned};
use crate::io::platform::{configure_direct_io, direct_open_flags};

/// Physical characteristics of the underlying block device.
pub struct DiskProfile {
    pub is_rotational: bool,
    pub max_io_bytes: usize,
    pub merge_gap: usize,
}

impl Default for DiskProfile {
    fn default() -> Self {
        Self {
            is_rotational: true,
            max_io_bytes: 1024 * 1024,
            merge_gap: 1024 * 1024,
        }
    }
}

impl std::fmt::Display for DiskProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Disk: rotational={} max_io={} merge_gap={}",
            self.is_rotational, self.max_io_bytes, self.merge_gap
        )
    }
}

/// Detect disk profile from an open file descriptor by reading sysfs.
/// Never fails — returns conservative defaults on any error.
#[cfg(target_os = "linux")]
fn detect_disk_profile(fd: RawFd) -> DiskProfile {
    use std::fs;

    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut stat) } != 0 {
        return DiskProfile::default();
    }

    let rdev = stat.st_rdev;
    let major = libc::major(rdev);
    let minor = libc::minor(rdev);

    // Not a block device (regular file, etc.) — use defaults.
    if major == 0 && minor == 0 {
        return DiskProfile::default();
    }

    let base = format!("/sys/dev/block/{}:{}", major, minor);

    // Try direct queue path first, then parent (for partitions).
    let read_queue_file = |name: &str| -> Option<String> {
        let direct = format!("{}/queue/{}", base, name);
        if let Ok(v) = fs::read_to_string(&direct) {
            return Some(v.trim().to_string());
        }
        let parent = format!("{}/../queue/{}", base, name);
        fs::read_to_string(&parent).ok().map(|v| v.trim().to_string())
    };

    let is_rotational = read_queue_file("rotational")
        .and_then(|v| v.parse::<u32>().ok())
        .map(|v| v != 0)
        .unwrap_or(true);

    let max_sectors_kb = read_queue_file("max_sectors_kb")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);

    let max_io_bytes = max_sectors_kb * 1024;

    let merge_gap = if is_rotational {
        max_io_bytes
    } else {
        256 * 1024
    };

    DiskProfile {
        is_rotational,
        max_io_bytes,
        merge_gap,
    }
}

#[cfg(not(target_os = "linux"))]
fn detect_disk_profile(_fd: RawFd) -> DiskProfile {
    DiskProfile::default()
}

/// Detect disk profile for a given device path.
/// Opens the path briefly to stat it, then reads sysfs.
/// Never fails — returns conservative defaults on any error.
pub fn detect_disk_profile_for_path(path: &str) -> DiskProfile {
    let c_path = match CString::new(path) {
        Ok(p) => p,
        Err(_) => return DiskProfile::default(),
    };
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
    if fd < 0 {
        return DiskProfile::default();
    }
    let profile = detect_disk_profile(fd);
    unsafe { libc::close(fd); }
    profile
}

/// Default buffer size: 256 MiB (large for batch reads).
const DEFAULT_BUF_SIZE: usize = 256 * 1024 * 1024;

/// Maximum number of I/O operations in flight at once for `read_batch`.
#[cfg(target_os = "linux")]
const BATCH_QUEUE_DEPTH: usize = 128;

/// A direct-I/O engine with a single reusable aligned buffer.
pub struct IoEngine {
    fd: RawFd,
    buf: AlignedBuf,
    device_size: u64,
    disk_profile: DiskProfile,
    io_log: Option<std::io::BufWriter<std::fs::File>>,
    io_log_remaining: usize,
    phase: &'static str,
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

        let disk_profile = detect_disk_profile(fd);

        // Get device/file size via lseek to end.
        let size = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
        if size < 0 {
            unsafe {
                libc::close(fd);
            }
            return Err(FxfspError::Io(std::io::Error::last_os_error()));
        }

        let (io_log, io_log_remaining) = if let Ok(path) = std::env::var("FXFSP_IO_LOG") {
            let f = std::fs::File::create(&path).map_err(FxfspError::Io)?;
            let mut w = std::io::BufWriter::new(f);
            writeln!(w, "phase,offset,len").map_err(FxfspError::Io)?;
            let limit = std::env::var("FXFSP_IO_LOG_LIMIT")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(usize::MAX);
            (Some(w), limit)
        } else {
            (None, 0)
        };

        Ok(Self {
            fd,
            buf: alloc_aligned(DEFAULT_BUF_SIZE),
            device_size: size as u64,
            disk_profile,
            io_log,
            io_log_remaining,
            phase: "unknown",
        })
    }

    /// Device/file size in bytes.
    pub fn device_size(&self) -> u64 {
        self.device_size
    }

    /// Physical characteristics of the underlying block device.
    pub fn disk_profile(&self) -> &DiskProfile {
        &self.disk_profile
    }

    /// Set the current I/O phase label for logging.
    pub fn set_phase(&mut self, phase: &'static str) {
        self.phase = phase;
    }

    /// Log a single read operation to the CSV file (if enabled).
    fn log_read(&mut self, offset: u64, len: usize) {
        if self.io_log_remaining == 0 {
            return;
        }
        if let Some(log) = &mut self.io_log {
            let _ = writeln!(log, "{},{},{}", self.phase, offset, len);
            self.io_log_remaining -= 1;
        }
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

        self.log_read(offset, clamped);

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

// ---- Batch read: io_uring on Linux, pread fallback elsewhere ----

#[cfg(target_os = "linux")]
impl IoEngine {
    /// Batch-read multiple (offset, len) pairs, calling `on_complete` for each.
    ///
    /// Uses io_uring to submit all reads to the kernel I/O scheduler, which
    /// merges adjacent requests and reorders for optimal disk access.
    ///
    /// - `requests`: (byte_offset, byte_len, tag) triples
    /// - `on_complete`: called once per completed read with the data buffer and tag.
    ///   The buffer slice is only valid for the duration of the callback.
    pub fn read_batch<T: Copy, F>(
        &mut self,
        requests: &[(u64, usize, T)],
        mut on_complete: F,
    ) -> Result<(), FxfspError>
    where
        F: FnMut(&[u8], T) -> Result<(), FxfspError>,
    {
        use io_uring::{IoUring, opcode, types};

        if requests.is_empty() {
            return Ok(());
        }

        let max_len = requests.iter().map(|r| r.1).max().unwrap();
        let aligned_max = align_up(max_len, IO_ALIGN);
        let pool_size = BATCH_QUEUE_DEPTH.min(requests.len());

        // Pre-allocate aligned buffer pool.  Declared before `ring` so that
        // on drop, the ring is destroyed first (cancelling in-flight ops)
        // before the buffers are freed.
        let mut pool: Vec<AlignedBuf> = (0..pool_size)
            .map(|_| alloc_aligned(aligned_max))
            .collect();

        // Grab stable raw pointers — the Vec is never resized, so these
        // remain valid for the lifetime of this function.
        let pool_ptrs: Vec<*mut u8> = pool.iter_mut().map(|b| b.as_mut_ptr()).collect();

        let mut slot_tags: Vec<Option<T>> = vec![None; pool_size];
        let mut slot_lens: Vec<usize> = vec![0; pool_size];
        let mut free_slots: Vec<usize> = (0..pool_size).rev().collect();

        let mut ring: IoUring =
            IoUring::new(BATCH_QUEUE_DEPTH as u32).map_err(FxfspError::Io)?;

        let mut next_req = 0usize;
        let mut in_flight = 0usize;

        while next_req < requests.len() || in_flight > 0 {
            // ---- Submit phase: fill the SQ with new requests ----
            {
                let mut sq = ring.submission();
                while next_req < requests.len() && !free_slots.is_empty() {
                    let (offset, len, tag) = requests[next_req];
                    next_req += 1;

                    let available = self.device_size.saturating_sub(offset) as usize;
                    let clamped = len.min(available) & !(IO_ALIGN - 1);
                    if clamped == 0 {
                        continue;
                    }

                    self.log_read(offset, clamped);

                    let slot = free_slots.pop().unwrap();
                    slot_tags[slot] = Some(tag);
                    slot_lens[slot] = clamped;

                    let sqe = opcode::Read::new(
                        types::Fd(self.fd),
                        pool_ptrs[slot],
                        clamped as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(slot as u64);

                    unsafe {
                        sq.push(&sqe).map_err(|_| {
                            FxfspError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "io_uring submission queue full",
                            ))
                        })?;
                    }
                    in_flight += 1;
                }
            } // sq dropped — releases &mut ring

            if in_flight == 0 {
                break;
            }

            // Submit all queued SQEs and wait for at least 1 completion.
            loop {
                match ring.submit_and_wait(1) {
                    Ok(_) => break,
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(e) => return Err(FxfspError::Io(e)),
                }
            }

            // ---- Completion phase: drain all available CQEs ----
            {
                let cq = ring.completion();
                for cqe in cq {
                    let slot = cqe.user_data() as usize;
                    let result = cqe.result();

                    if result < 0 {
                        return Err(FxfspError::Io(std::io::Error::from_raw_os_error(
                            -result,
                        )));
                    }

                    let tag = slot_tags[slot].take().unwrap();
                    let bytes_read = (result as usize).min(slot_lens[slot]);

                    let buf_slice =
                        unsafe { std::slice::from_raw_parts(pool_ptrs[slot], bytes_read) };
                    on_complete(buf_slice, tag)?;

                    free_slots.push(slot);
                    in_flight -= 1;
                }
            }
        }

        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
impl IoEngine {
    /// Batch-read multiple (offset, len) pairs, calling `on_complete` for each.
    ///
    /// Fallback implementation using sequential pread() calls.  Same API as
    /// the Linux io_uring version so all callers are platform-agnostic.
    pub fn read_batch<T: Copy, F>(
        &mut self,
        requests: &[(u64, usize, T)],
        mut on_complete: F,
    ) -> Result<(), FxfspError>
    where
        F: FnMut(&[u8], T) -> Result<(), FxfspError>,
    {
        if requests.is_empty() {
            return Ok(());
        }

        let max_len = requests.iter().map(|r| r.1).max().unwrap();
        let aligned_max = align_up(max_len, IO_ALIGN);
        let mut buf = alloc_aligned(aligned_max);

        for &(offset, len, tag) in requests {
            let available = self.device_size.saturating_sub(offset) as usize;
            let clamped = len.min(available) & !(IO_ALIGN - 1);
            if clamped == 0 {
                continue;
            }

            self.log_read(offset, clamped);

            let mut total = 0usize;
            while total < clamped {
                let ret = unsafe {
                    libc::pread(
                        self.fd,
                        buf[total..].as_mut_ptr() as *mut libc::c_void,
                        clamped - total,
                        (offset + total as u64) as libc::off_t,
                    )
                };
                if ret < 0 {
                    return Err(FxfspError::Io(std::io::Error::last_os_error()));
                }
                if ret == 0 {
                    break;
                }
                total += ret as usize;
            }

            if total > 0 {
                on_complete(&buf[..total], tag)?;
            }
        }

        Ok(())
    }
}

fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

impl Drop for IoEngine {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

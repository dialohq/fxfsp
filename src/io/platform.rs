use std::os::fd::RawFd;

/// Configure direct I/O on the given file descriptor.
///
/// - Linux: O_DIRECT is set at open time (see engine.rs).
/// - macOS: Uses fcntl(F_NOCACHE) to disable the buffer cache.
#[cfg(target_os = "macos")]
pub fn configure_direct_io(fd: RawFd) -> std::io::Result<()> {
    // F_NOCACHE = 48 on macOS
    let ret = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
    if ret == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn configure_direct_io(_fd: RawFd) -> std::io::Result<()> {
    // On Linux, O_DIRECT is passed at open time. Nothing to do here.
    Ok(())
}

/// Return platform-specific open flags for direct I/O.
#[cfg(target_os = "linux")]
pub fn direct_open_flags() -> libc::c_int {
    libc::O_RDONLY | libc::O_DIRECT
}

#[cfg(target_os = "macos")]
pub fn direct_open_flags() -> libc::c_int {
    libc::O_RDONLY
}

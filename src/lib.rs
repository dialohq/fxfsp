use std::ops::ControlFlow;

pub mod error;
#[cfg(feature = "io")]
pub mod io;
pub mod reader;
mod scan;
pub mod xfs;

pub use error::FxfspError;
pub use reader::{IoPhase, IoReader};
pub use xfs::extent::Extent;

#[cfg(feature = "io")]
pub use io::engine::{DiskProfile, IoEngine, detect_disk_profile_for_path};
#[cfg(feature = "io")]
pub use io::reader::MaybeInstrumented;

/// Events emitted during a filesystem scan.
///
/// `'a` borrows from the I/O buffer (e.g. directory entry names).
pub enum FsEvent<'a> {
    /// Superblock has been parsed.
    Superblock {
        block_size: u32,
        ag_count: u32,
        inode_size: u16,
        root_ino: u64,
    },
    /// An allocated inode was found.
    ///
    /// For regular files with inline extent lists (`FMT_EXTENTS`), `extents`
    /// contains the file's physical extent map at zero extra I/O cost.
    /// For btree-format files, extents arrive later via [`FileExtents`].
    InodeFound {
        ag_number: u32,
        ino: u64,
        mode: u16,
        size: u64,
        uid: u32,
        gid: u32,
        nlink: u32,
        mtime_sec: u32,
        mtime_nsec: u32,
        atime_sec: u32,
        atime_nsec: u32,
        ctime_sec: u32,
        ctime_nsec: u32,
        nblocks: u64,
        /// Physical extent map for regular files with inline extents.
        /// `None` for directories, non-regular files, and btree-format files
        /// (whose extents arrive via [`FileExtents`]).
        extents: Option<Vec<Extent>>,
    },
    /// Physical extent map for a btree-format regular file.
    ///
    /// Emitted after the batched bmbt walk (phase 1.5), separately from
    /// [`InodeFound`] to avoid random disk seeks during the inode scan.
    FileExtents {
        ino: u64,
        extents: Vec<Extent>,
    },
    /// A directory entry.
    DirEntry {
        parent_ino: u64,
        child_ino: u64,
        name: &'a [u8],
        file_type: u8,
    },
}

/// Scan an XFS filesystem using a custom [`IoReader`].
///
/// Calls `callback` for each event discovered. Events are emitted in
/// sequential disk order (AG-by-AG, forward within each AG) for optimal
/// HDD throughput.
///
/// The callback returns [`ControlFlow::Continue(())`] to keep scanning or
/// [`ControlFlow::Break(())`] to stop early. Early stop is not an error.
///
/// All errors are fatal -- any corrupt metadata aborts the scan immediately.
pub fn scan_reader<R: IoReader, F>(reader: &mut R, mut callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent) -> ControlFlow<()>,
{
    match scan::run_scan_inner(reader, &mut callback) {
        Err(FxfspError::Stopped) => Ok(()),
        other => other,
    }
}


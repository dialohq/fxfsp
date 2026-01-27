use crate::error::FxfspError;
use crate::orchestrator;
use crate::tree_scan;

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
    /// Beginning to scan an allocation group.
    AgBegin {
        ag_number: u32,
    },
    /// Finished scanning an allocation group.
    AgEnd {
        ag_number: u32,
    },
    /// An allocated inode was found.
    InodeFound {
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
    },
    /// A directory entry.
    DirEntry {
        parent_ino: u64,
        child_ino: u64,
        name: &'a [u8],
        file_type: u8,
    },
}

/// Scan an XFS filesystem at the given device/image path.
///
/// Calls `callback` for each event discovered. Events are emitted in
/// sequential disk order (AG-by-AG, forward within each AG) for optimal
/// HDD throughput.
///
/// All errors are fatal -- any corrupt metadata aborts the scan immediately.
pub fn scan<F>(device_path: &str, callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    orchestrator::run_scan(device_path, callback)
}

/// Scan an XFS filesystem by walking the directory tree from root.
///
/// Only reads directory inodes and directory data blocks — file inodes
/// are never touched. Much faster than `scan()` when you only need
/// the directory structure.
///
/// Emits: `Superblock`, `InodeFound` (directories only), `DirEntry`.
pub fn scan_tree<F>(device_path: &str, callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    tree_scan::run_tree_scan(device_path, callback)
}

use std::fmt;

use crate::error::FxfspError;

/// I/O phase labels for analytics and diagnostics.
#[derive(Debug, Clone, Copy)]
pub enum IoPhase {
    Superblock,
    Agi,
    InobtWalk,
    InodeChunks,
    BmbtWalk,
    DirExtents,
}

impl fmt::Display for IoPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Superblock => write!(f, "superblock"),
            Self::Agi => write!(f, "agi"),
            Self::InobtWalk => write!(f, "inobt_walk"),
            Self::InodeChunks => write!(f, "inode_chunks"),
            Self::BmbtWalk => write!(f, "bmbt_walk"),
            Self::DirExtents => write!(f, "dir_extents"),
        }
    }
}

/// Trait for reading raw bytes from a block device or image file.
///
/// Implementations must provide `read_at`. The default `coalesced_read_batch`
/// falls back to sequential `read_at` calls; override for performance
/// (e.g. io_uring with coalescing).
pub trait IoReader {
    /// Read `len` bytes at byte offset `offset`.
    /// Returns a slice borrowed from the engine's internal buffer.
    fn read_at(&mut self, offset: u64, len: usize, phase: IoPhase) -> Result<&[u8], FxfspError>;

    /// Batch-read with coalescing. `requests` must be sorted by offset.
    ///
    /// Default implementation calls `read_at` sequentially (no coalescing).
    fn coalesced_read_batch<T: Copy, F>(
        &mut self,
        requests: &[(u64, usize, T)],
        mut on_complete: F,
        phase: IoPhase,
    ) -> Result<(), FxfspError>
    where
        F: FnMut(&[u8], T) -> Result<(), FxfspError>,
    {
        for &(offset, len, tag) in requests {
            let buf = self.read_at(offset, len, phase)?;
            on_complete(buf, tag)?;
        }
        Ok(())
    }
}

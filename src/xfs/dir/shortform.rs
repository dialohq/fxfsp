use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::{U32, U64};

use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::xfs::superblock::FsContext;

/// Shortform directory header (when parent inode fits in 4 bytes).
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDirSfHdr4 {
    pub count: u8,
    pub i8count: u8,
    pub parent: U32,
}

/// Shortform directory header (when parent inode needs 8 bytes).
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDirSfHdr8 {
    pub count: u8,
    pub i8count: u8,
    pub parent: U64,
}

/// Parse a shortform directory from the inode's data fork.
/// Emits DirEntry events for each entry via the callback.
pub fn parse_shortform_dir<F>(
    fork_buf: &[u8],
    parent_ino: u64,
    ctx: &FsContext,
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    if fork_buf.len() < 6 {
        return Err(FxfspError::Parse("shortform dir too small"));
    }

    // Determine if we use 4-byte or 8-byte inode numbers.
    // i8count > 0 means 8-byte inodes are used.
    let i8count = fork_buf[1];
    let use_8byte = i8count > 0;

    let (entry_count, hdr_parent_ino, hdr_size) = if use_8byte {
        let hdr = XfsDirSfHdr8::ref_from_prefix(fork_buf)
            .map_err(|_| FxfspError::Parse("shortform hdr8 parse failed"))?
            .0;
        (hdr.i8count as usize, hdr.parent.get(), 10usize)
    } else {
        let hdr = XfsDirSfHdr4::ref_from_prefix(fork_buf)
            .map_err(|_| FxfspError::Parse("shortform hdr4 parse failed"))?
            .0;
        (hdr.count as usize, hdr.parent.get() as u64, 6usize)
    };

    // Emit "." entry (self).
    callback(&FsEvent::DirEntry {
        parent_ino,
        child_ino: parent_ino,
        name: b".",
        file_type: 0,
    });

    // Emit ".." entry (parent).
    callback(&FsEvent::DirEntry {
        parent_ino,
        child_ino: hdr_parent_ino,
        name: b"..",
        file_type: 0,
    });

    // Parse variable-length entries.
    let ino_size: usize = if use_8byte { 8 } else { 4 };
    let mut offset = hdr_size;

    for _ in 0..entry_count {
        if offset >= fork_buf.len() {
            return Err(FxfspError::Parse("shortform entry past end"));
        }

        let namelen = fork_buf[offset] as usize;
        // Skip the 2-byte offset field.
        let name_start = offset + 1 + 2; // namelen(1) + offset(2)
        let name_end = name_start + namelen;

        if name_end > fork_buf.len() {
            return Err(FxfspError::Parse("shortform entry name out of bounds"));
        }

        let name = &fork_buf[name_start..name_end];

        // ftype byte (only present if filesystem has ftype support).
        let ftype_size = if ctx.has_ftype { 1 } else { 0 };
        let ftype = if ctx.has_ftype {
            fork_buf[name_end]
        } else {
            0
        };

        // Inode number follows name (+ optional ftype).
        let ino_start = name_end + ftype_size;
        let child_ino = if use_8byte {
            if ino_start + 8 > fork_buf.len() {
                return Err(FxfspError::Parse("shortform 8-byte ino out of bounds"));
            }
            u64::from_be_bytes(fork_buf[ino_start..ino_start + 8].try_into().unwrap())
        } else {
            if ino_start + 4 > fork_buf.len() {
                return Err(FxfspError::Parse("shortform 4-byte ino out of bounds"));
            }
            u32::from_be_bytes(fork_buf[ino_start..ino_start + 4].try_into().unwrap()) as u64
        };

        callback(&FsEvent::DirEntry {
            parent_ino,
            child_ino,
            name,
            file_type: ftype,
        });

        offset = ino_start + ino_size;
    }

    Ok(())
}

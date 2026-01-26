use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::{U16, U32, U64};

use crate::error::FxfspError;

/// Inode magic: "IN"
const XFS_DINODE_MAGIC: u16 = 0x494e;

/// Inode data fork format codes.
pub const XFS_DINODE_FMT_DEV: u8 = 0;
pub const XFS_DINODE_FMT_LOCAL: u8 = 1;
pub const XFS_DINODE_FMT_EXTENTS: u8 = 2;
pub const XFS_DINODE_FMT_BTREE: u8 = 3;
pub const XFS_DINODE_FMT_UUID: u8 = 4;

/// S_IFMT mask.
pub const S_IFMT: u16 = 0o170000;
pub const S_IFDIR: u16 = 0o040000;
pub const S_IFREG: u16 = 0o100000;
pub const S_IFLNK: u16 = 0o120000;

/// On-disk XFS dinode core (V4 layout). V5 extends this.
/// The V4 core is 96 bytes; V5 core is 176 bytes.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDinodeCore {
    pub di_magic: U16,
    pub di_mode: U16,
    pub di_version: u8,
    pub di_format: u8,
    pub di_onlink: U16,
    pub di_uid: U32,
    pub di_gid: U32,
    pub di_nlink: U32,
    pub di_projid: U16,
    pub di_projid_hi: U16,
    pub di_pad: [u8; 6],
    pub di_flushiter: U16,
    pub di_atime: XfsTimestamp,
    pub di_mtime: XfsTimestamp,
    pub di_ctime: XfsTimestamp,
    pub di_size: U64,
    pub di_nblocks: U64,
    pub di_extsize: U32,
    pub di_nextents: U32,
    pub di_anextents: U16,
    pub di_forkoff: u8,
    pub di_aformat: u8,
    pub di_dmevmask: U32,
    pub di_dmstate: U16,
    pub di_flags: U16,
    pub di_gen: U32,
}

/// On-disk XFS timestamp.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsTimestamp {
    pub t_sec: U32,
    pub t_nsec: U32,
}

/// Size of the V4 dinode core.
pub const V4_CORE_SIZE: usize = 96;

/// Size of the V5 dinode core.
pub const V5_CORE_SIZE: usize = 176;

/// Parsed inode information.
pub struct InodeInfo {
    pub ino: u64,
    pub mode: u16,
    pub format: u8,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub nextents: u32,
    pub mtime_sec: u32,
    pub mtime_nsec: u32,
    pub atime_sec: u32,
    pub atime_nsec: u32,
    pub ctime_sec: u32,
    pub ctime_nsec: u32,
    pub nblocks: u64,
    /// Byte offset of the data fork within the on-disk inode.
    pub data_fork_offset: usize,
}

impl InodeInfo {
    pub fn is_dir(&self) -> bool {
        (self.mode & S_IFMT) == S_IFDIR
    }

    pub fn is_regular(&self) -> bool {
        (self.mode & S_IFMT) == S_IFREG
    }

    pub fn is_symlink(&self) -> bool {
        (self.mode & S_IFMT) == S_IFLNK
    }
}

/// Parse a dinode core from `buf` starting at byte 0.
/// `ino` is the absolute inode number (for the returned InodeInfo).
/// `is_v5` selects V4 vs V5 core size.
/// `has_nrext64`: if true, extent count is a U64 at inode byte offset 24.
pub fn parse_inode_core(
    buf: &[u8],
    ino: u64,
    is_v5: bool,
    has_nrext64: bool,
) -> Result<InodeInfo, FxfspError> {
    let core = XfsDinodeCore::ref_from_prefix(buf)
        .map_err(|_| FxfspError::Parse("buffer too small for dinode core"))?
        .0;

    if core.di_magic.get() != XFS_DINODE_MAGIC {
        return Err(FxfspError::BadMagic("dinode"));
    }

    let data_fork_offset = if is_v5 { V5_CORE_SIZE } else { V4_CORE_SIZE };

    // With NREXT64, di_nextents (offset 76) is zeroed; the actual data fork
    // extent count is stored as the lower 48 bits of a U64 at inode byte
    // offset 24 (overlapping the old di_pad + di_flushiter fields).
    let nextents = if has_nrext64 {
        if buf.len() < 32 {
            return Err(FxfspError::Parse("buffer too small for nrext64 extent count"));
        }
        let big = u64::from_be_bytes(buf[24..32].try_into().unwrap());
        // Lower 48 bits = data fork extent count.
        (big & 0x0000_FFFF_FFFF_FFFF) as u32
    } else {
        core.di_nextents.get()
    };

    Ok(InodeInfo {
        ino,
        mode: core.di_mode.get(),
        format: core.di_format,
        size: core.di_size.get(),
        uid: core.di_uid.get(),
        gid: core.di_gid.get(),
        nlink: core.di_nlink.get(),
        nextents,
        mtime_sec: core.di_mtime.t_sec.get(),
        mtime_nsec: core.di_mtime.t_nsec.get(),
        atime_sec: core.di_atime.t_sec.get(),
        atime_nsec: core.di_atime.t_nsec.get(),
        ctime_sec: core.di_ctime.t_sec.get(),
        ctime_nsec: core.di_ctime.t_nsec.get(),
        nblocks: core.di_nblocks.get(),
        data_fork_offset,
    })
}

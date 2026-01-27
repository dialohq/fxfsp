use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::{U16, U32, U64};

use crate::error::FxfspError;

/// XFS superblock magic: "XFSB"
const XFS_SB_MAGIC: u32 = 0x58465342;

/// On-disk XFS superblock (first 264 bytes, enough for all fields we need).
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDsb {
    pub sb_magicnum: U32,
    pub sb_blocksize: U32,
    pub sb_dblocks: U64,
    pub sb_rblocks: U64,
    pub sb_rextents: U64,
    pub sb_uuid: [u8; 16],
    pub sb_logstart: U64,
    pub sb_rootino: U64,
    pub sb_rbmino: U64,
    pub sb_rsumino: U64,
    pub sb_rextsize: U32,
    pub sb_agblocks: U32,
    pub sb_agcount: U32,
    pub sb_rbmblocks: U32,
    pub sb_logblocks: U32,
    pub sb_versionnum: U16,
    pub sb_sectsize: U16,
    pub sb_inodesize: U16,
    pub sb_inopblock: U16,
    pub sb_fname: [u8; 12],
    pub sb_blocklog: u8,
    pub sb_sectlog: u8,
    pub sb_inodelog: u8,
    pub sb_inopblog: u8,
    pub sb_agblklog: u8,
    pub sb_rextslog: u8,
    pub sb_inprogress: u8,
    pub sb_imax_pct: u8,
    pub sb_icount: U64,
    pub sb_ifree: U64,
    pub sb_fdblocks: U64,
    pub sb_frextents: U64,
    pub sb_uquotino: U64,
    pub sb_gquotino: U64,
    pub sb_qflags: U16,
    pub sb_flags: u8,
    pub sb_shared_vn: u8,
    pub sb_inoalignmt: U32,
    pub sb_unit: U32,
    pub sb_width: U32,
    pub sb_dirblklog: u8,
    pub sb_logsectlog: u8,
    pub sb_logsectsize: U16,
    pub sb_logsunit: U32,
    pub sb_features2: U32,
    pub sb_bad_features2: U32,
    // V5 fields follow but we parse them separately if needed.
}

/// Which XFS format version we're dealing with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatVersion {
    V4,
    V5,
}

/// Filesystem context extracted from the superblock.
#[derive(Debug, Clone)]
pub struct FsContext {
    pub version: FormatVersion,
    pub block_size: u32,
    pub block_log: u8,
    pub ag_count: u32,
    pub ag_blocks: u32,
    pub ag_blk_log: u8,
    pub inode_size: u16,
    pub inodes_per_block: u16,
    pub inode_log: u8,
    pub inop_blog: u8,
    pub dir_blk_log: u8,
    pub root_ino: u64,
    pub sect_size: u16,
    /// Does the filesystem store ftype in directory entries?
    pub has_ftype: bool,
    /// NREXT64: extent counts stored as 64-bit at inode offset 24.
    pub has_nrext64: bool,
}

impl FsContext {
    /// Parse the superblock from the given buffer and build an FsContext.
    pub fn from_superblock(buf: &[u8]) -> Result<Self, FxfspError> {
        let sb = XfsDsb::ref_from_prefix(buf)
            .map_err(|_| FxfspError::Parse("buffer too small for superblock"))?
            .0;

        if sb.sb_magicnum.get() != XFS_SB_MAGIC {
            return Err(FxfspError::BadMagic("superblock"));
        }

        let versionnum = sb.sb_versionnum.get();
        // V5 superblocks have version number 5 in the low nibble.
        let version = if (versionnum & 0x000f) >= 5 {
            FormatVersion::V5
        } else {
            FormatVersion::V4
        };

        let features2 = sb.sb_features2.get();
        // XFS_SB_VERSION2_FTYPE = 0x00000200
        let has_ftype_v4 = (features2 & 0x0200) != 0;

        // For V5, ftype is always present.
        let has_ftype = version == FormatVersion::V5 || has_ftype_v4;

        // V5: check incompat features for NREXT64 (bit 5).
        // sb_features_incompat is at byte offset 216 in the superblock.
        let has_nrext64 = if version == FormatVersion::V5 && buf.len() >= 220 {
            let incompat = u32::from_be_bytes([buf[216], buf[217], buf[218], buf[219]]);
            (incompat & 0x20) != 0 // XFS_SB_FEAT_INCOMPAT_NREXT64 = 1 << 5
        } else {
            false
        };

        Ok(FsContext {
            version,
            block_size: sb.sb_blocksize.get(),
            block_log: sb.sb_blocklog,
            ag_count: sb.sb_agcount.get(),
            ag_blocks: sb.sb_agblocks.get(),
            ag_blk_log: sb.sb_agblklog,
            inode_size: sb.sb_inodesize.get(),
            inodes_per_block: sb.sb_inopblock.get(),
            inode_log: sb.sb_inodelog,
            inop_blog: sb.sb_inopblog,
            dir_blk_log: sb.sb_dirblklog,
            root_ino: sb.sb_rootino.get(),
            sect_size: sb.sb_sectsize.get(),
            has_ftype,
            has_nrext64,
        })
    }

    /// Convert an absolute inode number to (ag_number, ag_relative_inode).
    pub fn ino_to_agno(&self, ino: u64) -> u32 {
        (ino >> (self.inop_blog as u64 + self.ag_blk_log as u64)) as u32
    }

    pub fn ino_to_agino(&self, ino: u64) -> u32 {
        let mask = (1u64 << (self.inop_blog as u64 + self.ag_blk_log as u64)) - 1;
        (ino & mask) as u32
    }

    /// Convert AG-relative inode to absolute inode number.
    pub fn agino_to_ino(&self, agno: u32, agino: u32) -> u64 {
        ((agno as u64) << (self.inop_blog as u64 + self.ag_blk_log as u64)) | (agino as u64)
    }

    /// Byte offset of an AG-relative block within the filesystem.
    pub fn ag_block_to_byte(&self, agno: u32, agblock: u32) -> u64 {
        let abs_block = (agno as u64) * (self.ag_blocks as u64) + (agblock as u64);
        abs_block << self.block_log as u64
    }

    /// Byte offset of the start of an AG.
    pub fn ag_start_byte(&self, agno: u32) -> u64 {
        (agno as u64) * (self.ag_blocks as u64) * (self.block_size as u64)
    }

    /// Byte offset of the AGI header for a given AG.
    /// AGI is at disk-address sector 2 within the AG (sector = sb_sectsize).
    pub fn agi_byte_offset(&self, agno: u32) -> u64 {
        self.ag_start_byte(agno) + 2 * self.sect_size as u64
    }

    /// Number of filesystem blocks in a directory block.
    pub fn dir_blk_fsblocks(&self) -> u32 {
        1u32 << self.dir_blk_log
    }

    /// Size of a directory block in bytes.
    pub fn dir_blk_size(&self) -> u32 {
        self.block_size * self.dir_blk_fsblocks()
    }

    /// Given an absolute inode number, return the byte offset of the block
    /// containing it and the byte offset of the inode within that block.
    pub fn ino_to_disk_position(&self, ino: u64) -> (u64, usize) {
        let agno = self.ino_to_agno(ino);
        let agino = self.ino_to_agino(ino);
        let ag_block = agino >> self.inop_blog;
        let block_byte = self.ag_block_to_byte(agno, ag_block);
        let within = (agino & ((1u32 << self.inop_blog) - 1)) as usize
            * self.inode_size as usize;
        (block_byte, within)
    }
}

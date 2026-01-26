//! Walk the bmap B-tree (bmbt) in a directory inode's data fork to collect
//! all extent records. Supports both the compact in-inode root format
//! (xfs_bmdr_block) and full on-disk block format (xfs_bmbt_block).

use zerocopy::FromBytes;

use crate::error::FxfspError;
use crate::io::engine::IoEngine;
use crate::xfs::extent::{Extent, XfsBmbtRec};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// V4 bmbt long-form block magic: "BMAP"
const XFS_BMAP_MAGIC: u32 = 0x424d4150;
/// V5 bmbt long-form block magic: "BMA3"
const XFS_BMAP3_MAGIC: u32 = 0x424d4133;

/// Size of the on-disk bmbt long-form header.
fn bmbt_block_hdr_size(version: FormatVersion) -> usize {
    match version {
        FormatVersion::V4 => 24,  // magic(4) + level(2) + numrecs(2) + leftsib(8) + rightsib(8)
        FormatVersion::V5 => 72,  // + blkno(8) + lsn(8) + uuid(16) + owner(8) + crc(4) + pad(4)
    }
}

/// Collect all extent records from a btree-format data fork.
///
/// `fork_data` is a copy of the inode's data fork (starting at the bmdr root).
/// `data_fork_size` is the byte size of the data fork area.
pub fn collect_bmbt_extents(
    engine: &mut IoEngine,
    ctx: &FsContext,
    fork_data: &[u8],
    data_fork_size: usize,
) -> Result<Vec<Extent>, FxfspError> {
    if fork_data.len() < 4 {
        return Err(FxfspError::Parse("bmbt root too small"));
    }

    let level = u16::from_be_bytes([fork_data[0], fork_data[1]]);
    let numrecs = u16::from_be_bytes([fork_data[2], fork_data[3]]) as usize;

    if level == 0 {
        // Leaf-level root: extent records inline in the fork.
        let mut extents = Vec::with_capacity(numrecs);
        for i in 0..numrecs {
            let offset = 4 + i * 16;
            if offset + 16 > fork_data.len() {
                break;
            }
            let rec = <XfsBmbtRec as FromBytes>::ref_from_prefix(&fork_data[offset..])
                .map_err(|_| FxfspError::Parse("bmbt leaf record parse failed"))?
                .0;
            extents.push(rec.unpack());
        }
        Ok(extents)
    } else {
        // Interior root: keys[maxrecs] then ptrs[maxrecs].
        // In-inode root uses compact layout: maxrecs = (fork_size - 4) / 16.
        let maxrecs = (data_fork_size - 4) / (8 + 8);
        let ptr_start = 4 + maxrecs * 8;

        let mut extents = Vec::new();
        let mut child_blocks = Vec::with_capacity(numrecs);
        for i in 0..numrecs {
            let off = ptr_start + i * 8;
            if off + 8 > fork_data.len() {
                break;
            }
            let fsblock = u64::from_be_bytes(fork_data[off..off + 8].try_into().unwrap());
            child_blocks.push(fsblock);
        }

        for fsblock in child_blocks {
            walk_bmbt_block(engine, ctx, fsblock, level as u32 - 1, &mut extents)?;
        }
        Ok(extents)
    }
}

/// Recursively walk an on-disk bmbt block (long-form header).
fn walk_bmbt_block(
    engine: &mut IoEngine,
    ctx: &FsContext,
    fsblock: u64,
    expected_level: u32,
    extents: &mut Vec<Extent>,
) -> Result<(), FxfspError> {
    let byte_offset = fsblock << ctx.block_log as u64;
    let buf = engine.read_at(byte_offset, ctx.block_size as usize)?;

    if buf.len() < 8 {
        return Err(FxfspError::Parse("bmbt block too small"));
    }

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let hdr_size = match ctx.version {
        FormatVersion::V5 => {
            if magic != XFS_BMAP3_MAGIC {
                return Err(FxfspError::BadMagic("bmbt V5 block"));
            }
            bmbt_block_hdr_size(FormatVersion::V5)
        }
        FormatVersion::V4 => {
            if magic != XFS_BMAP_MAGIC {
                return Err(FxfspError::BadMagic("bmbt V4 block"));
            }
            bmbt_block_hdr_size(FormatVersion::V4)
        }
    };

    let level = u16::from_be_bytes([buf[4], buf[5]]);
    let numrecs = u16::from_be_bytes([buf[6], buf[7]]) as usize;

    if level as u32 != expected_level {
        return Err(FxfspError::Parse("bmbt level mismatch"));
    }

    if level == 0 {
        // Leaf: extent records starting at header.
        for i in 0..numrecs {
            let offset = hdr_size + i * 16;
            if offset + 16 > buf.len() {
                break;
            }
            let rec = <XfsBmbtRec as FromBytes>::ref_from_prefix(&buf[offset..])
                .map_err(|_| FxfspError::Parse("bmbt leaf record parse failed"))?
                .0;
            extents.push(rec.unpack());
        }
    } else {
        // Interior: keys[maxrecs] then ptrs[maxrecs].
        let key_size = 8usize;
        let ptr_size = 8usize;
        let maxrecs = (ctx.block_size as usize - hdr_size) / (key_size + ptr_size);
        let ptr_start = hdr_size + maxrecs * key_size;

        let mut child_blocks = Vec::with_capacity(numrecs);
        for i in 0..numrecs {
            let off = ptr_start + i * ptr_size;
            if off + ptr_size > buf.len() {
                break;
            }
            let fsblock = u64::from_be_bytes(buf[off..off + 8].try_into().unwrap());
            child_blocks.push(fsblock);
        }

        for child in child_blocks {
            walk_bmbt_block(engine, ctx, child, expected_level - 1, extents)?;
        }
    }

    Ok(())
}

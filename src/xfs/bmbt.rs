//! Walk the bmap B-tree (bmbt) in directory inodes' data forks to collect
//! all extent records.  Uses level-by-level sorted batch reads across all
//! btree-format directories at once, replacing depth-first per-directory
//! traversal which caused random seeks.

use std::collections::HashMap;

use zerocopy::FromBytes;

use crate::error::FxfspError;
use crate::reader::{IoPhase, IoReader};
use crate::xfs::extent::{Extent, XfsBmbtRec, fsblock_to_byte};
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

/// Input for one btree-format directory whose bmbt needs walking.
pub struct BmbtDirInput<'a> {
    pub ino: u64,
    pub fork_data: &'a [u8],
    pub data_fork_size: usize,
}

/// A pending on-disk bmbt block that needs to be read.
struct PendingBlock {
    fsblock: u64,
    owner_ino: u64,
    expected_level: u32,
}

/// Collect extent records from all btree-format directories in one batched walk.
///
/// Instead of walking each directory's bmbt independently (random seeks), this
/// collects child pointers from all directories, sorts them by disk offset, and
/// reads each tree level in a single sorted coalesced pass.
///
/// Returns `(inode_number, extents)` pairs for each directory that has extents.
pub fn collect_all_bmbt_extents<R: IoReader>(
    engine: &mut R,
    ctx: &FsContext,
    dirs: &[BmbtDirInput],
) -> Result<Vec<(u64, Vec<Extent>)>, FxfspError> {
    let mut results: HashMap<u64, Vec<Extent>> = HashMap::new();
    let mut pending: Vec<PendingBlock> = Vec::new();

    // Parse all inline roots — no I/O needed for this step.
    for dir in dirs {
        if dir.fork_data.len() < 4 {
            return Err(FxfspError::Parse("bmbt root too small"));
        }

        let level = u16::from_be_bytes([dir.fork_data[0], dir.fork_data[1]]);
        let numrecs = u16::from_be_bytes([dir.fork_data[2], dir.fork_data[3]]) as usize;

        if level == 0 {
            // Leaf-level root: extent records inline in the fork.
            let extents = parse_bmbt_leaf_inline(dir.fork_data, numrecs)?;
            if !extents.is_empty() {
                results.entry(dir.ino).or_default().extend(extents);
            }
        } else {
            // Interior root: extract child fsblock pointers.
            let maxrecs = (dir.data_fork_size - 4) / (8 + 8);
            let ptr_start = 4 + maxrecs * 8;

            for i in 0..numrecs {
                let off = ptr_start + i * 8;
                if off + 8 > dir.fork_data.len() {
                    break;
                }
                let fsblock = u64::from_be_bytes(dir.fork_data[off..off + 8].try_into().unwrap());
                pending.push(PendingBlock {
                    fsblock,
                    owner_ino: dir.ino,
                    expected_level: level as u32 - 1,
                });
            }
        }
    }

    let block_size = ctx.block_size as usize;

    // Process pending blocks level by level with sorted batch reads.
    while !pending.is_empty() {
        pending.sort_unstable_by_key(|p| p.fsblock);

        let requests: Vec<(u64, usize, usize)> = pending
            .iter()
            .enumerate()
            .map(|(idx, p)| (fsblock_to_byte(ctx, p.fsblock), block_size, idx))
            .collect();

        let mut next_pending: Vec<PendingBlock> = Vec::new();

        engine.coalesced_read_batch(
            &requests,
            |buf, idx| {
                let owner_ino = pending[idx].owner_ino;
                let expected_level = pending[idx].expected_level;

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
                    // Leaf: extract extent records.
                    for i in 0..numrecs {
                        let offset = hdr_size + i * 16;
                        if offset + 16 > buf.len() {
                            break;
                        }
                        let rec = <XfsBmbtRec as FromBytes>::ref_from_prefix(&buf[offset..])
                            .map_err(|_| FxfspError::Parse("bmbt leaf record parse failed"))?
                            .0;
                        results.entry(owner_ino).or_default().push(rec.unpack());
                    }
                } else {
                    // Interior: extract child fsblock pointers.
                    let key_size = 8usize;
                    let ptr_size = 8usize;
                    let maxrecs = (block_size - hdr_size) / (key_size + ptr_size);
                    let ptr_start = hdr_size + maxrecs * key_size;

                    for i in 0..numrecs {
                        let off = ptr_start + i * ptr_size;
                        if off + ptr_size > buf.len() {
                            break;
                        }
                        let fsblock = u64::from_be_bytes(buf[off..off + 8].try_into().unwrap());
                        next_pending.push(PendingBlock {
                            fsblock,
                            owner_ino,
                            expected_level: expected_level - 1,
                        });
                    }
                }

                Ok(())
            },
            IoPhase::BmbtWalk,
        )?;

        pending = next_pending;
    }

    Ok(results.into_iter().collect())
}

/// Parse extent records from an inline leaf-level bmbt root (in the inode fork).
fn parse_bmbt_leaf_inline(fork_data: &[u8], numrecs: usize) -> Result<Vec<Extent>, FxfspError> {
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
}

use zerocopy::FromBytes;

use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::io::aligned_buf::IO_ALIGN;
use crate::io::engine::IoEngine;
use crate::xfs::ag::AgiInfo;
use crate::xfs::btree::collect_inobt_records;
use crate::xfs::dir::block::parse_dir_data_block;
use crate::xfs::dir::shortform::parse_shortform_dir;
use crate::xfs::extent::{Extent, parse_extent_list};
use crate::xfs::inode::{
    InodeInfo, XFS_DINODE_FMT_BTREE, XFS_DINODE_FMT_EXTENTS, XFS_DINODE_FMT_LOCAL,
    parse_inode_core,
};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// Default gap-fill threshold in filesystem blocks.
/// If two extents are separated by fewer than this many blocks,
/// read through the gap instead of seeking.
/// ~256 blocks at 4K = 1 MiB.
const GAP_FILL_BLOCKS: u64 = 256;

/// A deferred directory work item: inode + its data extents.
struct DirWorkItem {
    ino: u64,
    extents: Vec<Extent>,
}

/// A coalesced read range (in absolute filesystem blocks).
struct ReadRange {
    start_block: u64,
    block_count: u64,
    /// Which directory work items and which extents overlap this range.
    entries: Vec<DirRangeEntry>,
}

/// Tracks a directory extent within a coalesced read range.
struct DirRangeEntry {
    ino: u64,
    /// Offset within the read range buffer where this extent's data starts.
    buf_offset: usize,
    /// Number of bytes for this extent.
    byte_len: usize,
}

pub fn run_scan<F>(device_path: &str, mut callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    let mut engine = IoEngine::open(device_path)?;

    // Read superblock (always at byte offset 0, within first sector).
    let sb_read_size = align_up(4096, IO_ALIGN);
    let sb_buf = engine.read_at(0, sb_read_size)?;
    let ctx = FsContext::from_superblock(sb_buf)?;
    let is_v5 = ctx.version == FormatVersion::V5;

    callback(&FsEvent::Superblock {
        block_size: ctx.block_size,
        ag_count: ctx.ag_count,
        inode_size: ctx.inode_size,
        root_ino: ctx.root_ino,
    });

    for agno in 0..ctx.ag_count {
        callback(&FsEvent::AgBegin { ag_number: agno });
        scan_ag(&mut engine, &ctx, agno, is_v5, &mut callback)?;
        callback(&FsEvent::AgEnd { ag_number: agno });
    }

    Ok(())
}

fn scan_ag<F>(
    engine: &mut IoEngine,
    ctx: &FsContext,
    agno: u32,
    is_v5: bool,
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    // ---- Read AGI header (at sector offset 2 within the AG) ----
    let agi_offset = ctx.agi_byte_offset(agno);
    // AGI might not be block-aligned; read the entire block containing it.
    let agi_block_offset = agi_offset & !(ctx.block_size as u64 - 1);
    let agi_read_size = align_up(ctx.block_size as usize, IO_ALIGN);
    let agi_buf = engine.read_at(agi_block_offset, agi_read_size)?;
    let agi_within_block = (agi_offset - agi_block_offset) as usize;
    let agi = AgiInfo::from_buf(&agi_buf[agi_within_block..], agno, ctx.version)?;

    // ---- Phase 1a: Collect all inobt records ----
    let mut inobt_records = collect_inobt_records(engine, ctx, agno, agi.inobt_root, agi.inobt_level)?;

    // ---- Phase 1b: Sort by physical offset, read sequentially ----
    inobt_records.sort_by_key(|r| r.start_ino());

    let mut dir_work: Vec<DirWorkItem> = Vec::new();

    for rec in &inobt_records {
        let start_agino = rec.start_ino();
        // Inode chunk: up to 64 inodes. Physical location is determined by agino.
        // agino / inodes_per_block = AG block offset of the inode.
        let chunk_ag_block = start_agino / ctx.inodes_per_block as u32;
        let chunk_byte_offset = ctx.ag_block_to_byte(agno, chunk_ag_block);

        // Number of inodes in this chunk.
        let chunk_ino_count = rec.ir_count as u32;
        // Number of blocks to read for this chunk.
        let blocks_needed =
            (chunk_ino_count as usize * ctx.inode_size as usize + ctx.block_size as usize - 1)
                / ctx.block_size as usize;
        let read_size = align_up(blocks_needed * ctx.block_size as usize, IO_ALIGN);
        let chunk_buf = engine.read_at(chunk_byte_offset, read_size)?;

        for i in 0..chunk_ino_count {
            if !rec.is_allocated(i) {
                continue;
            }

            let agino = start_agino + i;
            let abs_ino = ctx.agino_to_ino(agno, agino);
            let inode_offset_in_chunk = i as usize * ctx.inode_size as usize;

            if inode_offset_in_chunk + ctx.inode_size as usize > chunk_buf.len() {
                return Err(FxfspError::Parse("inode offset out of chunk bounds"));
            }

            let inode_buf = &chunk_buf[inode_offset_in_chunk..];
            let info = parse_inode_core(inode_buf, abs_ino, is_v5, ctx.has_nrext64)?;

            callback(&FsEvent::InodeFound {
                ino: info.ino,
                mode: info.mode,
                size: info.size,
                uid: info.uid,
                gid: info.gid,
                nlink: info.nlink,
                mtime_sec: info.mtime_sec,
                mtime_nsec: info.mtime_nsec,
                atime_sec: info.atime_sec,
                atime_nsec: info.atime_nsec,
                ctime_sec: info.ctime_sec,
                ctime_nsec: info.ctime_nsec,
                nblocks: info.nblocks,
            });

            if info.is_dir() {
                handle_directory(
                    inode_buf,
                    &info,
                    ctx,
                    is_v5,
                    callback,
                    &mut dir_work,
                )?;
            }
        }
    }

    // ---- Phase 2: Directory sweep ----
    if !dir_work.is_empty() {
        phase2_dir_sweep(engine, ctx, &dir_work, callback)?;
    }

    Ok(())
}

/// Handle a directory inode: parse shortform inline, or defer extents to Phase 2.
fn handle_directory<F>(
    inode_buf: &[u8],
    info: &InodeInfo,
    ctx: &FsContext,
    _is_v5: bool,
    callback: &mut F,
    dir_work: &mut Vec<DirWorkItem>,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    match info.format {
        XFS_DINODE_FMT_LOCAL => {
            // Shortform directory -- parse inline now.
            let fork_start = info.data_fork_offset;
            let fork_end = fork_start + info.size as usize;
            if fork_end > inode_buf.len() {
                return Err(FxfspError::Parse("shortform dir fork out of bounds"));
            }
            let fork_buf = &inode_buf[fork_start..fork_end];
            parse_shortform_dir(fork_buf, info.ino, ctx, callback)?;
        }
        XFS_DINODE_FMT_EXTENTS => {
            let fork_buf = &inode_buf[info.data_fork_offset..];
            let extents = parse_extent_list(fork_buf, info.nextents)?;
            dir_work.push(DirWorkItem {
                ino: info.ino,
                extents,
            });
        }
        XFS_DINODE_FMT_BTREE => {
            // B-tree format: we need to walk the data fork B-tree to get extents.
            // For now, collect extents from the B-tree root block in the data fork.
            let fork_buf = &inode_buf[info.data_fork_offset..];
            let extents = parse_bmbt_root(fork_buf, ctx, info)?;
            dir_work.push(DirWorkItem {
                ino: info.ino,
                extents,
            });
        }
        _ => {
            // Other formats (DEV, UUID) don't have directory data.
        }
    }
    Ok(())
}

/// Parse B-tree root in the inode data fork to extract extents.
/// The root block is stored inline in the inode's data fork.
fn parse_bmbt_root(
    fork_buf: &[u8],
    _ctx: &FsContext,
    _info: &InodeInfo,
) -> Result<Vec<Extent>, FxfspError> {
    // The data fork contains a bmbt block:
    // - U16 bb_level
    // - U16 bb_numrecs
    // - keys[numrecs] (each 8 bytes: file offset)
    // - ptrs[numrecs] (each 8 bytes: fsblock)
    //
    // For directory inodes with BTREE format, the actual data extents are
    // in leaf nodes. We'd need to read those blocks to get the full extent list.
    // For the first version, we only support extents that fit in the root.
    // A proper implementation would walk the B-tree levels.

    if fork_buf.len() < 4 {
        return Err(FxfspError::Parse("bmbt root too small"));
    }

    let level = u16::from_be_bytes([fork_buf[0], fork_buf[1]]);
    let numrecs = u16::from_be_bytes([fork_buf[2], fork_buf[3]]) as usize;

    if level == 0 {
        // Leaf-level root: records are XfsBmbtRec (16 bytes each).
        let rec_start = 4;
        let rec_size = 16;
        let mut extents = Vec::with_capacity(numrecs);
        for i in 0..numrecs {
            let offset = rec_start + i * rec_size;
            if offset + rec_size > fork_buf.len() {
                break;
            }
            let rec = <crate::xfs::extent::XfsBmbtRec as FromBytes>::ref_from_prefix(&fork_buf[offset..])
                .map_err(|_| FxfspError::Parse("bmbt leaf record parse failed"))?
                .0;
            extents.push(rec.unpack());
        }
        Ok(extents)
    } else {
        // Interior root: keys (8 bytes each) then pointers (8 bytes each).
        // We'd need to read child blocks. For now, return empty -- this means
        // very large directories with deep B-trees won't have their entries
        // emitted in this version. A TODO for proper B-tree descent.
        //
        // TODO: Walk child blocks for full btree directory support.
        Ok(Vec::new())
    }
}

/// Phase 2: Sort directory extents, merge/gap-fill, read sequentially, parse.
fn phase2_dir_sweep<F>(
    engine: &mut IoEngine,
    ctx: &FsContext,
    dir_work: &[DirWorkItem],
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    // Flatten all extents with their owning inode.
    let mut all_extents: Vec<(u64, &Extent)> = Vec::new();
    for item in dir_work {
        for ext in &item.extents {
            if ext.block_count > 0 && !ext.is_unwritten {
                all_extents.push((item.ino, ext));
            }
        }
    }

    // Sort by physical start block.
    all_extents.sort_by_key(|&(_, ext)| ext.start_block);

    // Build coalesced read ranges with gap-filling.
    let ranges = coalesce_extents(&all_extents, ctx);

    // Read each range and parse directory data blocks.
    for range in &ranges {
        let byte_offset = range.start_block << ctx.block_log as u64;
        let byte_len = (range.block_count as usize) << ctx.block_log as usize;
        let read_size = align_up(byte_len, IO_ALIGN);

        let buf = engine.read_at(byte_offset, read_size)?;

        // Parse each directory extent within this range.
        for entry in &range.entries {
            let dir_blk_size = ctx.dir_blk_size() as usize;
            let extent_buf = &buf[entry.buf_offset..entry.buf_offset + entry.byte_len];

            // A directory extent may contain multiple directory blocks.
            let mut off = 0;
            while off + dir_blk_size <= extent_buf.len() {
                let block_buf = &extent_buf[off..off + dir_blk_size];
                parse_dir_data_block(block_buf, entry.ino, ctx, callback)?;
                off += dir_blk_size;
            }
        }
    }

    Ok(())
}

/// Coalesce sorted extents into read ranges with gap-filling.
fn coalesce_extents(extents: &[(u64, &Extent)], ctx: &FsContext) -> Vec<ReadRange> {
    if extents.is_empty() {
        return Vec::new();
    }

    let mut ranges: Vec<ReadRange> = Vec::new();

    for &(ino, ext) in extents {
        let ext_start = ext.start_block;
        let ext_blocks = ext.block_count;

        if let Some(last) = ranges.last_mut() {
            let last_end = last.start_block + last.block_count;
            let gap = ext_start.saturating_sub(last_end);

            if gap <= GAP_FILL_BLOCKS {
                // Extend the current range to include this extent (plus gap).
                let new_end = ext_start + ext_blocks;
                last.block_count = new_end - last.start_block;
                let buf_offset =
                    ((ext_start - last.start_block) as usize) << ctx.block_log as usize;
                last.entries.push(DirRangeEntry {
                    ino,
                    buf_offset,
                    byte_len: (ext_blocks as usize) << ctx.block_log as usize,
                });
                continue;
            }
        }

        // Start a new range.
        ranges.push(ReadRange {
            start_block: ext_start,
            block_count: ext_blocks,
            entries: vec![DirRangeEntry {
                ino,
                buf_offset: 0,
                byte_len: (ext_blocks as usize) << ctx.block_log as usize,
            }],
        });
    }

    ranges
}

fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

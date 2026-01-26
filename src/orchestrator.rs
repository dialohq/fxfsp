use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::io::aligned_buf::IO_ALIGN;
use crate::io::engine::IoEngine;
use crate::xfs::ag::AgiInfo;
use crate::xfs::bmbt::collect_bmbt_extents;
use crate::xfs::btree::collect_inobt_records;
use crate::xfs::dir::block::parse_dir_data_block;
use crate::xfs::dir::shortform::parse_shortform_dir;
use crate::xfs::extent::{Extent, parse_extent_list};
use crate::xfs::inode::{
    InodeInfo, XFS_DINODE_FMT_BTREE, XFS_DINODE_FMT_EXTENTS, XFS_DINODE_FMT_LOCAL,
    parse_inode_core,
};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// Gap-fill threshold in filesystem blocks for directory extent coalescing.
/// With 32 GB of RAM we can afford aggressive gap-fill. 4096 blocks at 4K = 16 MiB.
const GAP_FILL_BLOCKS: u64 = 4096;

/// Maximum gap (bytes) between inode chunks before starting a new batch read.
const INODE_BATCH_GAP: u64 = 16 * 1024 * 1024; // 16 MiB

/// Maximum batch read size for inode chunks.
const INODE_BATCH_MAX: u64 = 256 * 1024 * 1024; // 256 MiB

/// A deferred directory work item: inode + its data extents.
struct DirWorkItem {
    ino: u64,
    extents: Vec<Extent>,
}

/// Deferred btree-format directory: we need the engine to walk the bmbt.
struct BtreeDirItem {
    ino: u64,
    fork_data: Vec<u8>,
    data_fork_size: usize,
}

/// A coalesced read range (in absolute filesystem blocks).
struct ReadRange {
    start_block: u64,
    block_count: u64,
    entries: Vec<DirRangeEntry>,
}

/// Tracks a directory extent within a coalesced read range.
struct DirRangeEntry {
    ino: u64,
    buf_offset: usize,
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
    let agi_block_offset = agi_offset & !(ctx.block_size as u64 - 1);
    let agi_read_size = align_up(ctx.block_size as usize, IO_ALIGN);
    let agi_buf = engine.read_at(agi_block_offset, agi_read_size)?;
    let agi_within_block = (agi_offset - agi_block_offset) as usize;
    let agi = AgiInfo::from_buf(&agi_buf[agi_within_block..], agno, ctx.version)?;

    // ---- Phase 1a: Collect all inobt records ----
    let mut inobt_records =
        collect_inobt_records(engine, ctx, agno, agi.inobt_root, agi.inobt_level)?;

    // ---- Phase 1b: Sort by physical offset ----
    inobt_records.sort_by_key(|r| r.start_ino());

    // ---- Phase 1c: Batched inode reads ----
    // Pre-compute chunk byte ranges.
    let chunk_blocks = 64usize * ctx.inode_size as usize / ctx.block_size as usize;
    let chunk_byte_len = chunk_blocks * ctx.block_size as usize;

    struct ChunkMeta {
        byte_offset: u64,
        rec_idx: usize,
    }

    let chunks: Vec<ChunkMeta> = inobt_records
        .iter()
        .enumerate()
        .map(|(idx, rec)| {
            let chunk_ag_block = rec.start_ino() / ctx.inodes_per_block as u32;
            ChunkMeta {
                byte_offset: ctx.ag_block_to_byte(agno, chunk_ag_block),
                rec_idx: idx,
            }
        })
        .collect();

    let mut dir_work: Vec<DirWorkItem> = Vec::new();
    let mut btree_dirs: Vec<BtreeDirItem> = Vec::new();

    // Process chunks in batches.
    let mut batch_start_idx = 0;
    while batch_start_idx < chunks.len() {
        let batch_start_byte = chunks[batch_start_idx].byte_offset;
        let mut batch_end_byte = batch_start_byte + chunk_byte_len as u64;
        let mut batch_end_idx = batch_start_idx + 1;

        // Extend batch with nearby chunks.
        while batch_end_idx < chunks.len() {
            let next_start = chunks[batch_end_idx].byte_offset;
            let gap = next_start.saturating_sub(batch_end_byte);
            let total_span = next_start + chunk_byte_len as u64 - batch_start_byte;
            if gap <= INODE_BATCH_GAP && total_span <= INODE_BATCH_MAX {
                batch_end_byte = next_start + chunk_byte_len as u64;
                batch_end_idx += 1;
            } else {
                break;
            }
        }

        // Read the entire batch range.
        let read_len = align_up((batch_end_byte - batch_start_byte) as usize, IO_ALIGN);
        let buf = engine.read_at(batch_start_byte, read_len)?;

        // Process each chunk within this batch.
        for chunk in &chunks[batch_start_idx..batch_end_idx] {
            let buf_offset = (chunk.byte_offset - batch_start_byte) as usize;
            if buf_offset + chunk_byte_len > buf.len() {
                // Near end of device, skip partial chunk.
                continue;
            }
            let chunk_buf = &buf[buf_offset..buf_offset + chunk_byte_len];
            let rec = &inobt_records[chunk.rec_idx];

            process_inode_chunk(
                chunk_buf,
                rec,
                agno,
                ctx,
                is_v5,
                callback,
                &mut dir_work,
                &mut btree_dirs,
            )?;
        }

        batch_start_idx = batch_end_idx;
    }

    // ---- Phase 1.5: Walk bmbt trees for btree-format directories ----
    for item in btree_dirs {
        let extents = collect_bmbt_extents(engine, ctx, &item.fork_data, item.data_fork_size)?;
        if !extents.is_empty() {
            dir_work.push(DirWorkItem {
                ino: item.ino,
                extents,
            });
        }
    }

    // ---- Phase 2: Directory sweep ----
    if !dir_work.is_empty() {
        phase2_dir_sweep(engine, ctx, &dir_work, callback)?;
    }

    Ok(())
}

/// Process all allocated inodes in a single inobt chunk.
fn process_inode_chunk<F>(
    chunk_buf: &[u8],
    rec: &crate::xfs::btree::XfsInobtRec,
    agno: u32,
    ctx: &FsContext,
    is_v5: bool,
    callback: &mut F,
    dir_work: &mut Vec<DirWorkItem>,
    btree_dirs: &mut Vec<BtreeDirItem>,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    let start_agino = rec.start_ino();

    for i in 0..64u32 {
        // Skip holes (sparse inode chunks) and free inodes.
        let group = i / 4;
        let is_hole = (rec.ir_holemask.get() & (1u16 << group)) != 0;
        if is_hole || !rec.is_allocated(i) {
            continue;
        }

        let agino = start_agino + i;
        let abs_ino = ctx.agino_to_ino(agno, agino);
        let inode_offset = i as usize * ctx.inode_size as usize;

        if inode_offset + ctx.inode_size as usize > chunk_buf.len() {
            break;
        }

        let inode_buf = &chunk_buf[inode_offset..];
        let info = parse_inode_core(inode_buf, abs_ino, is_v5, ctx.has_nrext64, ctx.inode_size)?;

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
            handle_directory(inode_buf, &info, ctx, callback, dir_work, btree_dirs)?;
        }
    }

    Ok(())
}

/// Handle a directory inode: parse shortform inline, or defer to Phase 2.
fn handle_directory<F>(
    inode_buf: &[u8],
    info: &InodeInfo,
    ctx: &FsContext,
    callback: &mut F,
    dir_work: &mut Vec<DirWorkItem>,
    btree_dirs: &mut Vec<BtreeDirItem>,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    match info.format {
        XFS_DINODE_FMT_LOCAL => {
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
            // Save a copy of the fork data for the bmbt walk in phase 1.5.
            let fork_start = info.data_fork_offset;
            let fork_end = (fork_start + info.data_fork_size).min(inode_buf.len());
            let fork_data = inode_buf[fork_start..fork_end].to_vec();
            btree_dirs.push(BtreeDirItem {
                ino: info.ino,
                fork_data,
                data_fork_size: info.data_fork_size,
            });
        }
        _ => {}
    }
    Ok(())
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
    let mut all_extents: Vec<(u64, &Extent)> = Vec::new();
    for item in dir_work {
        for ext in &item.extents {
            if ext.block_count > 0 && !ext.is_unwritten {
                all_extents.push((item.ino, ext));
            }
        }
    }

    all_extents.sort_by_key(|&(_, ext)| ext.start_block);

    let ranges = coalesce_extents(&all_extents, ctx);
    let device_blocks = engine.device_size() >> ctx.block_log as u64;

    for range in &ranges {
        // Skip ranges that extend past the device.
        if range.start_block >= device_blocks {
            continue;
        }
        let clamped_blocks = range.block_count.min(device_blocks - range.start_block);

        let byte_offset = range.start_block << ctx.block_log as u64;
        let byte_len = (clamped_blocks as usize) << ctx.block_log as usize;
        let read_size = align_up(byte_len, IO_ALIGN);

        let buf = engine.read_at(byte_offset, read_size)?;

        for entry in &range.entries {
            // Clamp entry to what we actually read.
            let entry_end = entry.buf_offset + entry.byte_len;
            if entry.buf_offset >= buf.len() {
                continue;
            }
            let clamped_end = entry_end.min(buf.len());

            let dir_blk_size = ctx.dir_blk_size() as usize;
            let extent_buf = &buf[entry.buf_offset..clamped_end];

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

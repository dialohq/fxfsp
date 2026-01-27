use std::ops::ControlFlow;

use crate::FsEvent;
use crate::error::FxfspError;
use crate::reader::{IoPhase, IoReader};

/// Alignment for direct I/O reads (512 bytes covers all common block devices).
const IO_ALIGN: usize = 512;
use crate::xfs::ag::AgiInfo;
use crate::xfs::bmbt::{BmbtDirInput, collect_all_bmbt_extents};
use crate::xfs::btree::collect_inobt_records;
use crate::xfs::dir::block::parse_dir_data_block;
use crate::xfs::dir::shortform::parse_shortform_dir;
use crate::xfs::extent::{Extent, fsblock_to_byte, parse_extent_list};
use crate::xfs::inode::{
    InodeInfo, XFS_DINODE_FMT_BTREE, XFS_DINODE_FMT_EXTENTS, XFS_DINODE_FMT_LOCAL,
    parse_inode_core,
};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// XFS superblock is always at byte offset 0 and fits within this many bytes.
/// We read this much before the filesystem block size is known.
const SUPERBLOCK_SIZE: usize = 4096;

/// Call the callback, converting `ControlFlow::Break` into `FxfspError::Stopped`.
fn emit<F>(callback: &mut F, event: &FsEvent) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent) -> ControlFlow<()>,
{
    match callback(event) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(()) => Err(FxfspError::Stopped),
    }
}

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

pub(crate) fn run_scan_inner<R: IoReader, F>(engine: &mut R, callback: &mut F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent) -> ControlFlow<()>,
{
    // Read superblock (always at byte offset 0, within first sector).
    let sb_read_size = align_up(SUPERBLOCK_SIZE, IO_ALIGN);
    let sb_buf = engine.read_at(0, sb_read_size, IoPhase::Superblock)?;
    let ctx = FsContext::from_superblock(sb_buf)?;
    let is_v5 = ctx.version == FormatVersion::V5;

    emit(callback, &FsEvent::Superblock {
        block_size: ctx.block_size,
        ag_count: ctx.ag_count,
        inode_size: ctx.inode_size,
        root_ino: ctx.root_ino,
    })?;

    for agno in 0..ctx.ag_count {
        scan_ag(engine, &ctx, agno, is_v5, callback)?;
    }

    Ok(())
}

fn scan_ag<R: IoReader, F>(
    engine: &mut R,
    ctx: &FsContext,
    agno: u32,
    is_v5: bool,
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent) -> ControlFlow<()>,
{
    // ---- Read AGI header (at sector offset 2 within the AG) ----
    let agi_offset = ctx.agi_byte_offset(agno);
    let agi_block_offset = agi_offset & !(ctx.block_size as u64 - 1);
    let agi_read_size = align_up(ctx.block_size as usize, IO_ALIGN);
    let agi_buf = engine.read_at(agi_block_offset, agi_read_size, IoPhase::Agi)?;
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

    // Build one read request per inode chunk — the kernel I/O scheduler
    // merges adjacent requests and reorders for optimal disk access.
    let requests: Vec<(u64, usize, usize)> = chunks
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.byte_offset, chunk_byte_len, idx))
        .collect();

    engine.coalesced_read_batch(&requests, |buf, idx| {
        let rec = &inobt_records[chunks[idx].rec_idx];
        process_inode_chunk(buf, rec, agno, ctx, is_v5, callback, &mut dir_work, &mut btree_dirs)
    }, IoPhase::InodeChunks)?;

    // ---- Phase 1.5: Walk bmbt trees for btree-format directories (batched) ----
    if !btree_dirs.is_empty() {
        let inputs: Vec<BmbtDirInput> = btree_dirs
            .iter()
            .map(|item| BmbtDirInput {
                ino: item.ino,
                fork_data: &item.fork_data,
                data_fork_size: item.data_fork_size,
            })
            .collect();
        let bmbt_results = collect_all_bmbt_extents(engine, ctx, &inputs)?;
        for (ino, extents) in bmbt_results {
            if !extents.is_empty() {
                dir_work.push(DirWorkItem { ino, extents });
            }
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
    F: FnMut(&FsEvent) -> ControlFlow<()>,
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

        emit(callback, &FsEvent::InodeFound {
            ag_number: agno,
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
        })?;

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
    F: FnMut(&FsEvent) -> ControlFlow<()>,
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

/// Phase 2: Read directory extents via batch I/O and parse directory blocks.
fn phase2_dir_sweep<R: IoReader, F>(
    engine: &mut R,
    ctx: &FsContext,
    dir_work: &[DirWorkItem],
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent) -> ControlFlow<()>,
{
    // Build one request per directory extent.
    let mut requests: Vec<(u64, usize, u64)> = Vec::new();
    for item in dir_work {
        for ext in &item.extents {
            if ext.block_count > 0 && !ext.is_unwritten {
                let byte_offset = fsblock_to_byte(ctx, ext.start_block);
                let byte_len = (ext.block_count as usize) << ctx.block_log as usize;
                requests.push((byte_offset, byte_len, item.ino));
            }
        }
    }

    // Sort by disk offset — helps the I/O scheduler.
    requests.sort_by_key(|r| r.0);

    let dir_blk_size = ctx.dir_blk_size() as usize;

    engine.coalesced_read_batch(&requests, |buf, ino| {
        let mut off = 0;
        while off + dir_blk_size <= buf.len() {
            parse_dir_data_block(&buf[off..off + dir_blk_size], ino, ctx, callback)?;
            off += dir_blk_size;
        }
        Ok(())
    }, IoPhase::DirExtents)?;

    Ok(())
}

/// Rounds `value` up to the nearest multiple of `align`. `align` must be a power of two.
fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

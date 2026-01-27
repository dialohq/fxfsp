use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::io::aligned_buf::IO_ALIGN;
use crate::io::engine::IoEngine;
use crate::scan_common::{
    align_up, dir_data_sweep, emit_inode_found, handle_directory, resolve_btree_dirs,
    BtreeDirItem, DirWorkItem,
};
use crate::xfs::ag::AgiInfo;
use crate::xfs::btree::collect_inobt_records;
use crate::xfs::inode::parse_inode_core;
use crate::xfs::superblock::{FormatVersion, FsContext};

pub fn run_scan<F>(device_path: &str, mut callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    let mut engine = IoEngine::open(device_path)?;

    // Read superblock (always at byte offset 0, within first sector).
    engine.set_phase("superblock");
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

    let max_ag = std::env::var("FXFSP_MAX_AG")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(ctx.ag_count);
    let ag_limit = max_ag.min(ctx.ag_count);

    for agno in 0..ag_limit {
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
    engine.set_phase("agi");
    let agi_offset = ctx.agi_byte_offset(agno);
    let agi_block_offset = agi_offset & !(ctx.block_size as u64 - 1);
    let agi_read_size = align_up(ctx.block_size as usize, IO_ALIGN);
    let agi_buf = engine.read_at(agi_block_offset, agi_read_size)?;
    let agi_within_block = (agi_offset - agi_block_offset) as usize;
    let agi = AgiInfo::from_buf(&agi_buf[agi_within_block..], agno, ctx.version)?;

    // ---- Phase 1a: Collect all inobt records ----
    engine.set_phase("inobt_walk");
    let mut inobt_records =
        collect_inobt_records(engine, ctx, agno, agi.inobt_root, agi.inobt_level)?;

    // ---- Phase 1b: Sort by physical offset ----
    inobt_records.sort_by_key(|r| r.start_ino());

    // ---- Phase 1c: Batched inode reads ----
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

    let requests: Vec<(u64, usize, usize)> = chunks
        .iter()
        .enumerate()
        .map(|(idx, c)| (c.byte_offset, chunk_byte_len, idx))
        .collect();

    engine.set_phase("inode_chunks");
    engine.coalesced_read_batch(&requests, |buf, idx| {
        let rec = &inobt_records[chunks[idx].rec_idx];
        process_inode_chunk(buf, rec, agno, ctx, is_v5, callback, &mut dir_work, &mut btree_dirs)
    })?;

    // ---- Phase 1.5: Walk bmbt trees for btree-format directories ----
    engine.set_phase("bmbt_walk");
    resolve_btree_dirs(engine, ctx, btree_dirs, &mut dir_work)?;

    // ---- Phase 2: Directory sweep ----
    if !dir_work.is_empty() {
        engine.set_phase("dir_extents");
        dir_data_sweep(engine, ctx, &dir_work, callback)?;
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

        emit_inode_found(&info, callback);

        if info.is_dir() {
            handle_directory(inode_buf, &info, ctx, callback, dir_work, btree_dirs)?;
        }
    }

    Ok(())
}

use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::io::aligned_buf::IO_ALIGN;
use crate::io::engine::IoEngine;
use crate::scan_common::{
    align_up, dir_data_sweep, emit_inode_found, handle_directory, resolve_btree_dirs,
    BtreeDirItem, DirWorkItem,
};
use crate::xfs::inode::parse_inode_core;
use crate::xfs::superblock::{FormatVersion, FsContext};

/// XFS directory entry file_type value for directories.
const XFS_DIR3_FT_DIR: u8 = 2;

/// Scan an XFS filesystem by walking the directory tree from the root inode.
///
/// Only reads directory inodes and directory data blocks. File inodes are
/// never touched. Emits `DirEntry` events for every directory entry found,
/// and `InodeFound` events only for directory inodes.
pub fn run_tree_scan<F>(device_path: &str, mut callback: F) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    let mut engine = IoEngine::open(device_path)?;

    // Read superblock (always at byte offset 0).
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

    // BFS from root inode.
    let mut current_level: Vec<u64> = vec![ctx.root_ino];

    while !current_level.is_empty() {
        let next_level =
            process_bfs_level(&mut engine, &ctx, is_v5, &current_level, &mut callback)?;
        current_level = next_level;
    }

    Ok(())
}

/// Process one BFS level: read all directory inodes, parse their entries,
/// return child directory inode numbers for the next level.
fn process_bfs_level<F>(
    engine: &mut IoEngine,
    ctx: &FsContext,
    is_v5: bool,
    dir_inos: &[u64],
    callback: &mut F,
) -> Result<Vec<u64>, FxfspError>
where
    F: FnMut(&FsEvent),
{
    // ---- Phase A: Batch-read directory inodes ----
    engine.set_phase("tree_inodes");

    let inode_read_len = align_up(ctx.block_size as usize, IO_ALIGN);

    let mut requests: Vec<(u64, usize, usize)> = dir_inos
        .iter()
        .enumerate()
        .map(|(idx, &ino)| {
            let (block_byte, _within) = ctx.ino_to_disk_position(ino);
            (block_byte, inode_read_len, idx)
        })
        .collect();

    requests.sort_by_key(|r| r.0);

    let mut dir_work: Vec<DirWorkItem> = Vec::new();
    let mut btree_dirs: Vec<BtreeDirItem> = Vec::new();
    let mut child_dir_inos: Vec<u64> = Vec::new();

    engine.coalesced_read_batch(&requests, |buf, idx| {
        let ino = dir_inos[idx];
        let (_block_byte, within_block) = ctx.ino_to_disk_position(ino);

        if within_block + ctx.inode_size as usize > buf.len() {
            return Err(FxfspError::Parse("inode extends past block read"));
        }

        let inode_buf = &buf[within_block..];
        let info = parse_inode_core(inode_buf, ino, is_v5, ctx.has_nrext64, ctx.inode_size)?;

        // On non-ftype filesystems we optimistically enqueue file_type=0
        // entries. Skip non-directories here.
        if !info.is_dir() {
            return Ok(());
        }

        emit_inode_found(&info, callback);

        // Wrap callback to also collect child directory inode numbers.
        let mut interceptor = |event: &FsEvent| {
            collect_child_dir(event, &mut child_dir_inos);
            callback(event);
        };

        handle_directory(
            inode_buf,
            &info,
            ctx,
            &mut interceptor,
            &mut dir_work,
            &mut btree_dirs,
        )?;

        Ok(())
    })?;

    // ---- Phase B: Walk bmbt trees for btree-format directories ----
    engine.set_phase("tree_bmbt");
    resolve_btree_dirs(engine, ctx, btree_dirs, &mut dir_work)?;

    // ---- Phase C: Batch-read directory data blocks ----
    if !dir_work.is_empty() {
        engine.set_phase("tree_dir_data");

        // Wrap callback to also collect child directory inode numbers.
        let mut interceptor = |event: &FsEvent| {
            collect_child_dir(event, &mut child_dir_inos);
            callback(event);
        };

        dir_data_sweep(engine, ctx, &dir_work, &mut interceptor)?;
    }

    Ok(child_dir_inos)
}

/// Inspect a DirEntry event and collect child directory inode numbers.
fn collect_child_dir(event: &FsEvent, child_dirs: &mut Vec<u64>) {
    if let FsEvent::DirEntry {
        child_ino,
        file_type,
        name,
        ..
    } = event
    {
        if is_child_directory(*file_type, name) {
            child_dirs.push(*child_ino);
        }
    }
}

/// Determine whether a directory entry is a child directory to recurse into.
fn is_child_directory(file_type: u8, name: &[u8]) -> bool {
    if name == b"." || name == b".." {
        return false;
    }
    // file_type=2 is DIR. file_type=0 means unknown (no ftype on V4);
    // optimistically enqueue — non-dirs are filtered when we read their inode.
    file_type == XFS_DIR3_FT_DIR || file_type == 0
}

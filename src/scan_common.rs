use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::io::engine::IoEngine;
use crate::xfs::bmbt::collect_bmbt_extents;
use crate::xfs::dir::block::parse_dir_data_block;
use crate::xfs::dir::shortform::parse_shortform_dir;
use crate::xfs::extent::{fsblock_to_byte, parse_extent_list, Extent};
use crate::xfs::inode::{
    InodeInfo, XFS_DINODE_FMT_BTREE, XFS_DINODE_FMT_EXTENTS, XFS_DINODE_FMT_LOCAL,
};
use crate::xfs::superblock::FsContext;

/// A deferred directory work item: inode + its data extents.
pub struct DirWorkItem {
    pub ino: u64,
    pub extents: Vec<Extent>,
}

/// Deferred btree-format directory: we need the engine to walk the bmbt.
pub struct BtreeDirItem {
    pub ino: u64,
    pub fork_data: Vec<u8>,
    pub data_fork_size: usize,
}

/// Handle a directory inode: parse shortform inline, or defer to a later sweep.
pub fn handle_directory<F>(
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

/// Walk bmbt trees for btree-format directories, adding results to dir_work.
pub fn resolve_btree_dirs(
    engine: &mut IoEngine,
    ctx: &FsContext,
    btree_dirs: Vec<BtreeDirItem>,
    dir_work: &mut Vec<DirWorkItem>,
) -> Result<(), FxfspError> {
    for item in btree_dirs {
        let extents = collect_bmbt_extents(engine, ctx, &item.fork_data, item.data_fork_size)?;
        if !extents.is_empty() {
            dir_work.push(DirWorkItem {
                ino: item.ino,
                extents,
            });
        }
    }
    Ok(())
}

/// Read directory data extents via batch I/O and parse directory blocks.
pub fn dir_data_sweep<F>(
    engine: &mut IoEngine,
    ctx: &FsContext,
    dir_work: &[DirWorkItem],
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
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

    requests.sort_by_key(|r| r.0);

    let dir_blk_size = ctx.dir_blk_size() as usize;

    engine.coalesced_read_batch(&requests, |buf, ino| {
        let mut off = 0;
        while off + dir_blk_size <= buf.len() {
            parse_dir_data_block(&buf[off..off + dir_blk_size], ino, ctx, callback)?;
            off += dir_blk_size;
        }
        Ok(())
    })?;

    Ok(())
}

/// Emit an InodeFound event from parsed inode info.
pub fn emit_inode_found<F>(info: &InodeInfo, callback: &mut F)
where
    F: FnMut(&FsEvent),
{
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
}

pub fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

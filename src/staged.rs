//! Phased XFS parser API.
//!
//! This module exposes parsing phases explicitly, allowing users to:
//! 1. Parse superblock first, get metadata upfront
//! 2. Iterate through AGs manually
//! 3. Run phases (inodes → extents → dir entries) in sequence with typed callbacks
//! 4. Avoid enum matching when only one event type is needed
//!
//! The typestate pattern enforces the correct phase order at compile time.

use std::ops::ControlFlow;

use crate::error::FxfspError;
use crate::reader::{IoPhase, IoReader};
use crate::xfs::ag::AgiInfo;
use crate::xfs::bmbt::{BmbtDirInput, collect_all_bmbt_extents};
use crate::xfs::btree::collect_inobt_records;
use crate::xfs::dir::block::parse_dir_data_block_staged;
use crate::xfs::dir::shortform::parse_shortform_dir_staged;
use crate::xfs::extent::{Extent, parse_extent_list};
use crate::xfs::inode::{
    XFS_DINODE_FMT_BTREE, XFS_DINODE_FMT_EXTENTS, XFS_DINODE_FMT_LOCAL,
    parse_inode_core,
};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// Alignment for direct I/O reads.
const IO_ALIGN: usize = 512;

/// XFS superblock is always at byte offset 0.
const SUPERBLOCK_SIZE: usize = 4096;

/// Superblock information returned at scan start.
#[derive(Debug, Clone)]
pub struct SuperblockInfo {
    pub block_size: u32,
    pub ag_count: u32,
    pub ag_blocks: u32,
    pub inode_size: u16,
    pub root_ino: u64,
}

/// Information about a discovered inode.
#[derive(Debug, Clone)]
pub struct InodeInfo {
    pub ag_number: u32,
    pub ino: u64,
    pub mode: u16,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub mtime_sec: u32,
    pub mtime_nsec: u32,
    pub atime_sec: u32,
    pub atime_nsec: u32,
    pub ctime_sec: u32,
    pub ctime_nsec: u32,
    pub nblocks: u64,
    /// Physical extent map for regular files with inline extents.
    /// `None` for directories, non-regular files, and btree-format files
    /// (whose extents arrive via [`FileExtentsInfo`]).
    pub extents: Option<Vec<Extent>>,
}

/// Physical extent map for a btree-format regular file.
#[derive(Debug, Clone)]
pub struct FileExtentsInfo {
    pub ino: u64,
    pub extents: Vec<Extent>,
}

/// A directory entry.
pub struct DirEntryInfo<'a> {
    pub parent_ino: u64,
    pub child_ino: u64,
    pub name: &'a [u8],
    pub file_type: u8,
}

/// Parse the superblock and return filesystem metadata plus a scanner.
///
/// This is the entry point for the phased API.
pub fn parse_superblock<R: IoReader>(mut reader: R) -> Result<(SuperblockInfo, FsScanner<R>), FxfspError> {
    let sb_read_size = align_up(SUPERBLOCK_SIZE, IO_ALIGN);
    let sb_buf = reader.read_at(0, sb_read_size, IoPhase::Superblock)?;
    let ctx = FsContext::from_superblock(sb_buf)?;

    let sb_info = SuperblockInfo {
        block_size: ctx.block_size,
        ag_count: ctx.ag_count,
        ag_blocks: ctx.ag_blocks,
        inode_size: ctx.inode_size,
        root_ino: ctx.root_ino,
    };

    let scanner = FsScanner {
        reader,
        ctx,
        current_ag: 0,
    };

    Ok((sb_info, scanner))
}

/// Filesystem scanner for iterating through AGs.
pub struct FsScanner<R: IoReader> {
    reader: R,
    ctx: FsContext,
    current_ag: u32,
}

impl<R: IoReader> FsScanner<R> {
    /// Get the superblock information.
    pub fn superblock(&self) -> SuperblockInfo {
        SuperblockInfo {
            block_size: self.ctx.block_size,
            ag_count: self.ctx.ag_count,
            ag_blocks: self.ctx.ag_blocks,
            inode_size: self.ctx.inode_size,
            root_ino: self.ctx.root_ino,
        }
    }

    /// Access full filesystem context for advanced use.
    pub fn context(&self) -> &FsContext {
        &self.ctx
    }

    /// Get the next AG scanner, or None if all AGs have been processed.
    pub fn next_ag(&mut self) -> Option<Result<AgScanner<'_, R>, FxfspError>> {
        if self.current_ag >= self.ctx.ag_count {
            return None;
        }

        let agno = self.current_ag;
        self.current_ag += 1;

        Some(self.create_ag_scanner(agno))
    }

    fn create_ag_scanner(&mut self, agno: u32) -> Result<AgScanner<'_, R>, FxfspError> {
        // Read AGI header
        let agi_offset = self.ctx.agi_byte_offset(agno);
        let agi_block_offset = agi_offset & !(self.ctx.block_size as u64 - 1);
        let agi_read_size = align_up(self.ctx.block_size as usize, IO_ALIGN);
        let agi_buf = self.reader.read_at(agi_block_offset, agi_read_size, IoPhase::Agi)?;
        let agi_within_block = (agi_offset - agi_block_offset) as usize;
        let agi = AgiInfo::from_buf(&agi_buf[agi_within_block..], agno, self.ctx.version)?;

        Ok(AgScanner {
            reader: &mut self.reader,
            ctx: &self.ctx,
            agno,
            agi,
        })
    }
}

/// Per-AG scanner for phased processing.
pub struct AgScanner<'a, R: IoReader> {
    reader: &'a mut R,
    ctx: &'a FsContext,
    agno: u32,
    agi: AgiInfo,
}

impl<'a, R: IoReader> AgScanner<'a, R> {
    /// Get the AG number being scanned.
    pub fn ag_number(&self) -> u32 {
        self.agno
    }

    /// Phase 1: Scan inodes, returns scanner for next phase.
    pub fn scan_inodes<F>(self, mut callback: F) -> Result<AgExtentPhase<'a, R>, FxfspError>
    where
        F: FnMut(&InodeInfo) -> ControlFlow<()>,
    {
        let is_v5 = self.ctx.version == FormatVersion::V5;

        // Collect all inobt records
        let mut inobt_records = collect_inobt_records(
            self.reader,
            self.ctx,
            self.agno,
            self.agi.inobt_root,
            self.agi.inobt_level,
        )?;

        // Sort by physical offset
        inobt_records.sort_by_key(|r| r.start_ino());

        // Pre-compute chunk byte ranges
        let chunk_blocks = 64usize * self.ctx.inode_size as usize / self.ctx.block_size as usize;
        let chunk_byte_len = chunk_blocks * self.ctx.block_size as usize;

        struct ChunkMeta {
            byte_offset: u64,
            rec_idx: usize,
        }

        let chunks: Vec<ChunkMeta> = inobt_records
            .iter()
            .enumerate()
            .map(|(idx, rec)| {
                let chunk_ag_block = rec.start_ino() / self.ctx.inodes_per_block as u32;
                ChunkMeta {
                    byte_offset: self.ctx.ag_block_to_byte(self.agno, chunk_ag_block),
                    rec_idx: idx,
                }
            })
            .collect();

        let mut dir_work: Vec<DirWorkItem> = Vec::new();
        let mut shortform_dirs: Vec<ShortformDirItem> = Vec::new();
        let mut btree_dirs: Vec<BtreeItem> = Vec::new();
        let mut btree_files: Vec<BtreeItem> = Vec::new();
        let mut stopped = false;

        let requests: Vec<(u64, usize, usize)> = chunks
            .iter()
            .enumerate()
            .map(|(idx, c)| (c.byte_offset, chunk_byte_len, idx))
            .collect();

        self.reader.coalesced_read_batch(
            &requests,
            |buf, idx| {
                if stopped {
                    return Ok(());
                }
                let rec = &inobt_records[chunks[idx].rec_idx];
                let result = process_inode_chunk_staged(
                    buf,
                    rec,
                    self.agno,
                    self.ctx,
                    is_v5,
                    &mut callback,
                    &mut dir_work,
                    &mut shortform_dirs,
                    &mut btree_dirs,
                    &mut btree_files,
                );
                if let Err(FxfspError::Stopped) = result {
                    stopped = true;
                    return Ok(());
                }
                result
            },
            IoPhase::InodeChunks,
        )?;

        Ok(AgExtentPhase {
            reader: self.reader,
            ctx: self.ctx,
            dir_work,
            shortform_dirs,
            btree_dirs,
            btree_files,
        })
    }
}

/// Phase 1.5: Emit extents for btree-format files.
pub struct AgExtentPhase<'a, R: IoReader> {
    reader: &'a mut R,
    ctx: &'a FsContext,
    dir_work: Vec<DirWorkItem>,
    shortform_dirs: Vec<ShortformDirItem>,
    btree_dirs: Vec<BtreeItem>,
    btree_files: Vec<BtreeItem>,
}

impl<'a, R: IoReader> AgExtentPhase<'a, R> {
    /// Phase 1.5: Emit extents for btree-format files.
    pub fn scan_file_extents<F>(mut self, mut callback: F) -> Result<AgDirPhase<'a, R>, FxfspError>
    where
        F: FnMut(&FileExtentsInfo) -> ControlFlow<()>,
    {
        if !self.btree_dirs.is_empty() || !self.btree_files.is_empty() {
            let inputs: Vec<BmbtDirInput> = self.btree_dirs
                .iter()
                .chain(self.btree_files.iter())
                .map(|item| BmbtDirInput {
                    ino: item.ino,
                    fork_data: &item.fork_data,
                    data_fork_size: item.data_fork_size,
                })
                .collect();

            let bmbt_results = collect_all_bmbt_extents(self.reader, self.ctx, &inputs)?;

            let dir_inos: std::collections::HashSet<u64> =
                self.btree_dirs.iter().map(|d| d.ino).collect();

            for (ino, extents) in bmbt_results {
                if extents.is_empty() {
                    continue;
                }
                if dir_inos.contains(&ino) {
                    self.dir_work.push(DirWorkItem { ino, extents });
                } else {
                    let fe = FileExtentsInfo { ino, extents };
                    if callback(&fe).is_break() {
                        // Early termination requested, but we still return the dir phase
                        break;
                    }
                }
            }
        }

        Ok(AgDirPhase {
            reader: self.reader,
            ctx: self.ctx,
            dir_work: self.dir_work,
            shortform_dirs: self.shortform_dirs,
        })
    }

    /// Skip if file extents are not needed.
    pub fn skip_extents(mut self) -> AgDirPhase<'a, R> {
        // Still need to process btree dirs to get their extents for dir phase
        if !self.btree_dirs.is_empty() {
            let inputs: Vec<BmbtDirInput> = self.btree_dirs
                .iter()
                .map(|item| BmbtDirInput {
                    ino: item.ino,
                    fork_data: &item.fork_data,
                    data_fork_size: item.data_fork_size,
                })
                .collect();

            if let Ok(bmbt_results) = collect_all_bmbt_extents(self.reader, self.ctx, &inputs) {
                for (ino, extents) in bmbt_results {
                    if !extents.is_empty() {
                        self.dir_work.push(DirWorkItem { ino, extents });
                    }
                }
            }
        }

        AgDirPhase {
            reader: self.reader,
            ctx: self.ctx,
            dir_work: self.dir_work,
            shortform_dirs: self.shortform_dirs,
        }
    }
}

/// Phase 2: Scan directory entries.
pub struct AgDirPhase<'a, R: IoReader> {
    reader: &'a mut R,
    ctx: &'a FsContext,
    dir_work: Vec<DirWorkItem>,
    shortform_dirs: Vec<ShortformDirItem>,
}

impl<'a, R: IoReader> AgDirPhase<'a, R> {
    /// Phase 2: Scan directory entries.
    pub fn scan_dir_entries<F>(self, mut callback: F) -> Result<(), FxfspError>
    where
        F: FnMut(&DirEntryInfo) -> ControlFlow<()>,
    {
        // First, process shortform directories (no I/O needed)
        for sf in &self.shortform_dirs {
            let result = parse_shortform_dir_staged(&sf.fork_data, sf.ino, self.ctx, &mut callback);
            if let Err(FxfspError::Stopped) = result {
                return Ok(()); // Early termination is not an error
            }
            result?;
        }

        if self.dir_work.is_empty() {
            return Ok(());
        }

        // Build one request per directory extent
        let mut requests: Vec<(u64, usize, u64)> = Vec::new();
        for item in &self.dir_work {
            for ext in &item.extents {
                if ext.block_count > 0 && !ext.is_unwritten {
                    let byte_offset = ext.start_byte(self.ctx);
                    let byte_len = (ext.block_count as usize) << self.ctx.block_log as usize;
                    requests.push((byte_offset, byte_len, item.ino));
                }
            }
        }

        // Sort by disk offset
        requests.sort_by_key(|r| r.0);

        let dir_blk_size = self.ctx.dir_blk_size() as usize;
        let mut stopped = false;

        self.reader.coalesced_read_batch(
            &requests,
            |buf, ino| {
                if stopped {
                    return Ok(());
                }
                let mut off = 0;
                while off + dir_blk_size <= buf.len() {
                    let result = parse_dir_data_block_staged(
                        &buf[off..off + dir_blk_size],
                        ino,
                        self.ctx,
                        &mut callback,
                    );
                    if let Err(FxfspError::Stopped) = result {
                        stopped = true;
                        return Ok(());
                    }
                    result?;
                    off += dir_blk_size;
                }
                Ok(())
            },
            IoPhase::DirExtents,
        )?;

        Ok(())
    }

    /// Skip if directory entries are not needed.
    pub fn skip_dirs(self) -> Result<(), FxfspError> {
        Ok(())
    }
}

// Internal types

struct DirWorkItem {
    ino: u64,
    extents: Vec<Extent>,
}

/// Shortform directory: inline data in inode fork.
struct ShortformDirItem {
    ino: u64,
    fork_data: Vec<u8>,
}

struct BtreeItem {
    ino: u64,
    fork_data: Vec<u8>,
    data_fork_size: usize,
}

/// Process all allocated inodes in a single inobt chunk.
fn process_inode_chunk_staged<F>(
    chunk_buf: &[u8],
    rec: &crate::xfs::btree::XfsInobtRec,
    agno: u32,
    ctx: &FsContext,
    is_v5: bool,
    callback: &mut F,
    dir_work: &mut Vec<DirWorkItem>,
    shortform_dirs: &mut Vec<ShortformDirItem>,
    btree_dirs: &mut Vec<BtreeItem>,
    btree_files: &mut Vec<BtreeItem>,
) -> Result<(), FxfspError>
where
    F: FnMut(&InodeInfo) -> ControlFlow<()>,
{
    let start_agino = rec.start_ino();

    for i in 0..64u32 {
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

        // Extract inline extents for regular files
        let extents = if info.is_regular() && info.format == XFS_DINODE_FMT_EXTENTS && info.nextents > 0 {
            let fork_buf = &inode_buf[info.data_fork_offset..];
            Some(parse_extent_list(fork_buf, info.nextents, ctx)?)
        } else {
            None
        };

        let inode_info = InodeInfo {
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
            extents,
        };

        if callback(&inode_info).is_break() {
            return Err(FxfspError::Stopped);
        }

        if info.is_dir() {
            handle_directory_staged(inode_buf, &info, ctx, dir_work, shortform_dirs, btree_dirs)?;
        } else if info.is_regular() && info.format == XFS_DINODE_FMT_BTREE {
            let fork_start = info.data_fork_offset;
            let fork_end = (fork_start + info.data_fork_size).min(inode_buf.len());
            let fork_data = inode_buf[fork_start..fork_end].to_vec();
            btree_files.push(BtreeItem {
                ino: info.ino,
                fork_data,
                data_fork_size: info.data_fork_size,
            });
        }
    }

    Ok(())
}

/// Handle a directory inode: store shortform data or defer to Phase 2.
fn handle_directory_staged(
    inode_buf: &[u8],
    info: &crate::xfs::inode::InodeInfo,
    ctx: &FsContext,
    dir_work: &mut Vec<DirWorkItem>,
    shortform_dirs: &mut Vec<ShortformDirItem>,
    btree_dirs: &mut Vec<BtreeItem>,
) -> Result<(), FxfspError> {
    match info.format {
        XFS_DINODE_FMT_LOCAL => {
            // Store shortform directory data for parsing in dir phase
            let fork_start = info.data_fork_offset;
            let fork_end = fork_start + info.size as usize;
            if fork_end > inode_buf.len() {
                return Err(FxfspError::Parse("shortform dir fork out of bounds"));
            }
            let fork_data = inode_buf[fork_start..fork_end].to_vec();
            shortform_dirs.push(ShortformDirItem {
                ino: info.ino,
                fork_data,
            });
        }
        XFS_DINODE_FMT_EXTENTS => {
            let fork_buf = &inode_buf[info.data_fork_offset..];
            let extents = parse_extent_list(fork_buf, info.nextents, ctx)?;
            dir_work.push(DirWorkItem {
                ino: info.ino,
                extents,
            });
        }
        XFS_DINODE_FMT_BTREE => {
            let fork_start = info.data_fork_offset;
            let fork_end = (fork_start + info.data_fork_size).min(inode_buf.len());
            let fork_data = inode_buf[fork_start..fork_end].to_vec();
            btree_dirs.push(BtreeItem {
                ino: info.ino,
                fork_data,
                data_fork_size: info.data_fork_size,
            });
        }
        _ => {}
    }
    Ok(())
}

fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

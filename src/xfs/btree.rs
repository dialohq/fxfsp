use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::{U16, U32, U64};

use crate::error::FxfspError;
use crate::reader::{IoPhase, IoReader};
use crate::xfs::superblock::{FormatVersion, FsContext};

/// Short-form B-tree block magic: "IABT" (V4 inode allocation B-tree).
const XFS_IBT_MAGIC: u32 = 0x49414254;
/// V5 magic: "IAB3"
const XFS_IBT3_MAGIC: u32 = 0x49414233;

/// V4 short-form B-tree block header (16 bytes).
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsBtreeShortBlockV4 {
    pub bb_magic: U32,
    pub bb_level: U16,
    pub bb_numrecs: U16,
    pub bb_leftsib: U32,
    pub bb_rightsib: U32,
}

/// V5 short-form B-tree block header (56 bytes).
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsBtreeShortBlockV5 {
    pub bb_magic: U32,
    pub bb_level: U16,
    pub bb_numrecs: U16,
    pub bb_leftsib: U32,
    pub bb_rightsib: U32,
    pub bb_blkno: U64,
    pub bb_lsn: U64,
    pub bb_uuid: [u8; 16],
    pub bb_owner: U32,
    pub bb_crc: U32,
}

/// Inode B-tree record (16 bytes).
#[derive(FromBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct XfsInobtRec {
    pub ir_startino: U32,
    pub ir_holemask: U16,
    pub ir_count: u8,
    pub ir_freecount: u8,
    pub ir_free: U64,
}

impl XfsInobtRec {
    /// Check if inode at index `i` (0..63) is allocated (not free).
    pub fn is_allocated(&self, i: u32) -> bool {
        let free_mask = self.ir_free.get();
        (free_mask & (1u64 << i)) == 0
    }

    /// Starting AG-relative inode number.
    pub fn start_ino(&self) -> u32 {
        self.ir_startino.get()
    }
}

/// Size of the B-tree block header depending on version.
fn btree_header_size(version: FormatVersion) -> usize {
    match version {
        FormatVersion::V4 => std::mem::size_of::<XfsBtreeShortBlockV4>(),
        FormatVersion::V5 => std::mem::size_of::<XfsBtreeShortBlockV5>(),
    }
}

/// Parse the header from a B-tree block buffer.
fn parse_btree_header(buf: &[u8], version: FormatVersion) -> Result<(u16, u16), FxfspError> {
    match version {
        FormatVersion::V4 => {
            let hdr = XfsBtreeShortBlockV4::ref_from_prefix(buf)
                .map_err(|_| FxfspError::Parse("buffer too small for V4 btree header"))?
                .0;
            let magic = hdr.bb_magic.get();
            if magic != XFS_IBT_MAGIC {
                return Err(FxfspError::BadMagic("inobt V4 block"));
            }
            Ok((hdr.bb_level.get(), hdr.bb_numrecs.get()))
        }
        FormatVersion::V5 => {
            let hdr = XfsBtreeShortBlockV5::ref_from_prefix(buf)
                .map_err(|_| FxfspError::Parse("buffer too small for V5 btree header"))?
                .0;
            let magic = hdr.bb_magic.get();
            if magic != XFS_IBT3_MAGIC {
                return Err(FxfspError::BadMagic("inobt V5 block"));
            }
            Ok((hdr.bb_level.get(), hdr.bb_numrecs.get()))
        }
    }
}

/// Walk the inode B-tree rooted at `root_block` (AG-relative) and collect all inobt records.
///
/// Uses level-by-level sorted batch reads: at each tree level the child block
/// pointers are sorted by disk offset and read in one coalesced forward sweep,
/// replacing the previous depth-first traversal which caused random seeks.
pub fn collect_inobt_records<R: IoReader>(
    engine: &mut R,
    ctx: &FsContext,
    agno: u32,
    root_block: u32,
    level: u32,
) -> Result<Vec<XfsInobtRec>, FxfspError> {
    // AGI level is 1-based (number of levels), but bb_level in blocks is 0-based.
    let root_level = level.saturating_sub(1);
    let hdr_size = btree_header_size(ctx.version);
    let block_size = ctx.block_size as usize;

    // Read root block.
    let offset = ctx.ag_block_to_byte(agno, root_block);
    let buf = engine.read_at(offset, block_size, IoPhase::InobtWalk)?;
    let (blk_level, numrecs) = parse_btree_header(buf, ctx.version)?;
    if blk_level as u32 != root_level {
        return Err(FxfspError::Parse("inobt level mismatch"));
    }

    if root_level == 0 {
        return parse_inobt_leaf(buf, hdr_size, numrecs);
    }

    // Root is interior — extract child pointers for the next level.
    let mut current_blocks = extract_inobt_children(buf, hdr_size, numrecs, block_size)?;

    // Walk down level by level with sorted batch reads.
    for current_level in (0..root_level).rev() {
        current_blocks.sort_unstable();

        let requests: Vec<(u64, usize, usize)> = current_blocks
            .iter()
            .enumerate()
            .map(|(idx, &block)| (ctx.ag_block_to_byte(agno, block), block_size, idx))
            .collect();

        if current_level == 0 {
            // Leaf level — collect records.
            let mut records = Vec::new();
            engine.coalesced_read_batch(
                &requests,
                |buf, _idx| {
                    let (_lvl, numrecs) = parse_btree_header(buf, ctx.version)?;
                    let recs = parse_inobt_leaf(buf, hdr_size, numrecs)?;
                    records.extend(recs);
                    Ok(())
                },
                IoPhase::InobtWalk,
            )?;
            return Ok(records);
        }

        // Interior level — collect next level's block numbers.
        let mut next_blocks = Vec::new();
        engine.coalesced_read_batch(
            &requests,
            |buf, _idx| {
                let (blk_level, numrecs) = parse_btree_header(buf, ctx.version)?;
                if blk_level as u32 != current_level {
                    return Err(FxfspError::Parse("inobt level mismatch"));
                }
                let children = extract_inobt_children(buf, hdr_size, numrecs, block_size)?;
                next_blocks.extend(children);
                Ok(())
            },
            IoPhase::InobtWalk,
        )?;
        current_blocks = next_blocks;
    }

    unreachable!("loop always returns at leaf level")
}

/// Parse inobt leaf records from a block buffer.
fn parse_inobt_leaf(buf: &[u8], hdr_size: usize, numrecs: u16) -> Result<Vec<XfsInobtRec>, FxfspError> {
    let rec_size = std::mem::size_of::<XfsInobtRec>();
    let mut records = Vec::with_capacity(numrecs as usize);
    for i in 0..numrecs as usize {
        let start = hdr_size + i * rec_size;
        let end = start + rec_size;
        if end > buf.len() {
            return Err(FxfspError::Parse("inobt leaf record out of bounds"));
        }
        let rec = XfsInobtRec::ref_from_prefix(&buf[start..])
            .map_err(|_| FxfspError::Parse("failed to parse inobt record"))?
            .0;
        records.push(*rec);
    }
    Ok(records)
}

/// Extract child AG-block pointers from an inobt interior node.
fn extract_inobt_children(buf: &[u8], hdr_size: usize, numrecs: u16, block_size: usize) -> Result<Vec<u32>, FxfspError> {
    // Keys are XfsInobtKey (4 bytes) and pointers are U32 (AG block numbers).
    // XFS lays out keys and pointers based on maxrecs (the maximum that fit
    // in the block), NOT the current numrecs.
    let key_size = 4usize;
    let ptr_size = 4usize;
    let maxrecs = (block_size - hdr_size) / (key_size + ptr_size);
    let ptr_offset = hdr_size + maxrecs * key_size;

    let mut children = Vec::with_capacity(numrecs as usize);
    for i in 0..numrecs as usize {
        let start = ptr_offset + i * ptr_size;
        let ptr = U32::ref_from_prefix(&buf[start..])
            .map_err(|_| FxfspError::Parse("inobt ptr out of bounds"))?
            .0;
        children.push(ptr.get());
    }
    Ok(children)
}

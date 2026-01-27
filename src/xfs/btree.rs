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
/// This reads the entire tree before returning, collecting records into a Vec.
pub fn collect_inobt_records<R: IoReader>(
    engine: &mut R,
    ctx: &FsContext,
    agno: u32,
    root_block: u32,
    level: u32,
) -> Result<Vec<XfsInobtRec>, FxfspError> {
    let mut records = Vec::new();
    // AGI level is 1-based (number of levels), but bb_level in blocks is 0-based.
    // Root node bb_level = level - 1.
    let root_level = level.saturating_sub(1);
    walk_inobt_node(engine, ctx, agno, root_block, root_level, &mut records)?;
    Ok(records)
}

fn walk_inobt_node<R: IoReader>(
    engine: &mut R,
    ctx: &FsContext,
    agno: u32,
    block: u32,
    level: u32,
    records: &mut Vec<XfsInobtRec>,
) -> Result<(), FxfspError> {
    let offset = ctx.ag_block_to_byte(agno, block);
    let buf = engine.read_at(offset, ctx.block_size as usize, IoPhase::InobtWalk)?;

    let (blk_level, numrecs) = parse_btree_header(buf, ctx.version)?;

    if blk_level as u32 != level {
        return Err(FxfspError::Parse("inobt level mismatch"));
    }

    let hdr_size = btree_header_size(ctx.version);

    if level == 0 {
        // Leaf node: records are XfsInobtRec.
        let rec_size = std::mem::size_of::<XfsInobtRec>();
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
    } else {
        // Interior node: keys followed by pointers.
        // Keys are XfsInobtKey (4 bytes: startino) and pointers are U32 (AG block numbers).
        // IMPORTANT: XFS lays out keys and pointers based on maxrecs (the maximum
        // that fit in the block), NOT the current numrecs. The pointer array always
        // starts at hdr_size + maxrecs * key_size.
        let key_size = 4usize;
        let ptr_size = 4usize;
        let maxrecs = (ctx.block_size as usize - hdr_size) / (key_size + ptr_size);
        let ptr_offset = hdr_size + maxrecs * key_size;

        // Collect child block numbers first (before we reuse the engine buffer).
        let mut child_blocks = Vec::with_capacity(numrecs as usize);
        for i in 0..numrecs as usize {
            let start = ptr_offset + i * 4;
            let ptr = U32::ref_from_prefix(&buf[start..])
                .map_err(|_| FxfspError::Parse("inobt ptr out of bounds"))?
                .0;
            child_blocks.push(ptr.get());
        }

        for child_block in child_blocks {
            walk_inobt_node(engine, ctx, agno, child_block, level - 1, records)?;
        }
    }

    Ok(())
}

use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::U64;

use crate::error::FxfspError;
use crate::xfs::superblock::FsContext;

/// On-disk XFS extent record (packed 128-bit / 16-byte).
///
/// Bit layout (big-endian, 128 bits total):
/// - Bit 127:        extent flag (1 = unwritten)
/// - Bits 126..73:   logical file offset (54 bits)
/// - Bits 72..21:    absolute filesystem block number (52 bits)
/// - Bits 20..0:     block count (21 bits)
#[derive(FromBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
pub struct XfsBmbtRec {
    pub l0: U64,
    pub l1: U64,
}

/// Unpacked extent with decomposed AG information.
#[derive(Debug, Clone)]
pub struct Extent {
    pub logical_offset: u64,
    pub ag_number: u32,
    pub ag_block: u32,
    pub block_count: u64,
    pub is_unwritten: bool,
}

impl XfsBmbtRec {
    /// Unpack extent record with filesystem context to decompose fsblock into AG components.
    pub fn unpack_with_context(&self, ctx: &FsContext) -> Extent {
        let l0 = self.l0.get();
        let l1 = self.l1.get();

        let is_unwritten = (l0 >> 63) != 0;
        let logical_offset = (l0 >> 9) & 0x003F_FFFF_FFFF_FFFF; // 54 bits
        let fsblock = ((l0 & 0x1FF) << 43) | (l1 >> 21); // 52 bits
        let block_count = l1 & 0x001F_FFFF; // 21 bits

        let (ag_number, ag_block) = fsblock_to_ag(ctx, fsblock);

        Extent {
            logical_offset,
            ag_number,
            ag_block,
            block_count,
            is_unwritten,
        }
    }
}

/// Extract extent list from an inode's data fork (FMT_EXTENTS format).
/// `fork_buf` is the data fork portion of the inode. `nextents` is the count.
pub fn parse_extent_list(
    fork_buf: &[u8],
    nextents: u32,
    ctx: &FsContext,
) -> Result<Vec<Extent>, FxfspError> {
    let rec_size = std::mem::size_of::<XfsBmbtRec>();
    let mut extents = Vec::with_capacity(nextents as usize);

    for i in 0..nextents as usize {
        let start = i * rec_size;
        if start + rec_size > fork_buf.len() {
            return Err(FxfspError::Parse("extent record out of bounds"));
        }
        let rec = XfsBmbtRec::ref_from_prefix(&fork_buf[start..])
            .map_err(|_| FxfspError::Parse("failed to parse extent record"))?
            .0;
        extents.push(rec.unpack_with_context(ctx));
    }

    Ok(extents)
}

impl Extent {
    /// Compute the starting byte offset of this extent on disk.
    pub fn start_byte(&self, ctx: &FsContext) -> u64 {
        ctx.ag_block_to_byte(self.ag_number, self.ag_block)
    }
}

/// Convert an absolute filesystem block number to a byte offset on disk.
///
/// XFS fsblock numbers are packed: upper bits = AG number, lower
/// `sb_agblklog` bits = AG-relative block.  When `sb_agblocks` is not a
/// power of two the simple shift `fsblock << block_log` gives the wrong
/// result for AGs beyond 0.  We must unpack first.
pub fn fsblock_to_byte(ctx: &FsContext, fsblock: u64) -> u64 {
    let (agno, agblock) = fsblock_to_ag(ctx, fsblock);
    ctx.ag_block_to_byte(agno, agblock)
}

/// Convert an absolute filesystem block number to (agno, agblock).
pub fn fsblock_to_ag(ctx: &FsContext, fsblock: u64) -> (u32, u32) {
    let agno = (fsblock >> ctx.ag_blk_log as u64) as u32;
    let agblock = (fsblock & ((1u64 << ctx.ag_blk_log as u64) - 1)) as u32;
    (agno, agblock)
}

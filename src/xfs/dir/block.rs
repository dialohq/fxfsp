use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::{U16, U32, U64};

use crate::api::FsEvent;
use crate::error::FxfspError;
use crate::xfs::superblock::{FormatVersion, FsContext};

/// V4 data block magic: "XD2D"
const XFS_DIR2_DATA_MAGIC: u32 = 0x58443244;
/// V4 block format magic: "XD2B"
const XFS_DIR2_BLOCK_MAGIC: u32 = 0x58443242;
/// V5 data block magic: "XDD3"
const XFS_DIR3_DATA_MAGIC: u32 = 0x58444433;
/// V5 block format magic: "XDB3"
const XFS_DIR3_BLOCK_MAGIC: u32 = 0x58444233;

/// V4 directory data block header.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDir2DataHdrV4 {
    pub magic: U32,
    pub bestfree: [XfsDir2DataFree; 3],
    // Total: 4 + 3*4 = 16 bytes.
}

/// V5 directory data block header.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDir3DataHdr {
    pub magic: U32,
    pub crc: U32,
    pub blkno: U64,
    pub lsn: U64,
    pub uuid: [u8; 16],
    pub owner: U64,
    pub bestfree: [XfsDir2DataFree; 3],
    pub pad: U32,
}

/// Free space entry in directory data block header.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsDir2DataFree {
    pub offset: U16,
    pub length: U16,
}

/// Size of the data block header.
fn data_hdr_size(version: FormatVersion) -> usize {
    match version {
        FormatVersion::V4 => 16,  // 4 + 3*4
        FormatVersion::V5 => 64,  // full XfsDir3DataHdr
    }
}

/// Unused entry tag value.
const XFS_DIR2_DATA_FREE_TAG: u16 = 0xffff;

/// Check if a directory data block has a valid magic number.
/// Returns true for data or block-format magic numbers.
fn is_data_block_magic(magic: u32, version: FormatVersion) -> bool {
    match version {
        FormatVersion::V4 => magic == XFS_DIR2_DATA_MAGIC || magic == XFS_DIR2_BLOCK_MAGIC,
        FormatVersion::V5 => magic == XFS_DIR3_DATA_MAGIC || magic == XFS_DIR3_BLOCK_MAGIC,
    }
}

fn is_block_format(magic: u32) -> bool {
    magic == XFS_DIR2_BLOCK_MAGIC || magic == XFS_DIR3_BLOCK_MAGIC
}

/// Compute the end-of-data-entries offset for a directory block.
/// For block-format directories (XD2B/XDB3), the block contains:
///   [header][data entries...][leaf entries...][block_tail (8 bytes)]
/// The tail has { u32 count, u32 stale }, and leaf entries (8 bytes each)
/// immediately precede the tail. Data entries end before the leaf section.
/// For data-format directories (XD2D/XDD3), the entire block is data entries.
fn data_end_offset(buf: &[u8], magic: u32) -> usize {
    if is_block_format(magic) && buf.len() >= 8 {
        let tail_offset = buf.len() - 8;
        let leaf_count = u32::from_be_bytes(
            buf[tail_offset..tail_offset + 4].try_into().unwrap(),
        ) as usize;
        // Each leaf entry is 8 bytes (u32 hashval + u32 address).
        tail_offset.saturating_sub(leaf_count * 8)
    } else {
        buf.len()
    }
}

/// Parse directory data entries from a data block.
/// `buf` is a single directory block (block_size or dir_blk_size bytes).
/// `parent_ino` is the inode owning this directory.
/// Calls `callback` for each entry found.
pub fn parse_dir_data_block<F>(
    buf: &[u8],
    parent_ino: u64,
    ctx: &FsContext,
    callback: &mut F,
) -> Result<(), FxfspError>
where
    F: FnMut(&FsEvent),
{
    if buf.len() < 4 {
        return Err(FxfspError::Parse("dir data block too small"));
    }

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if !is_data_block_magic(magic, ctx.version) {
        // Not a data block (could be a leaf/node block or gap filler). Skip.
        return Ok(());
    }

    let hdr_size = data_hdr_size(ctx.version);
    let data_end = data_end_offset(buf, magic);
    let mut offset = hdr_size;

    while offset + 6 <= data_end {
        // Each entry starts with either a used entry or a free (unused) entry.
        // Free entries have a 2-byte freetag (0xffff) + 2-byte length.
        let freetag = u16::from_be_bytes([buf[offset], buf[offset + 1]]);

        if freetag == XFS_DIR2_DATA_FREE_TAG {
            // Unused entry: 2-byte tag + 2-byte length.
            let length = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
            if length == 0 || offset + length > data_end {
                break;
            }
            offset += length;
            continue;
        }

        // Used entry layout:
        // - U64 inumber (8 bytes)
        // - u8 namelen (1 byte)
        // - name[namelen]
        // - optional ftype (1 byte if has_ftype)
        // - padding to 8-byte boundary
        // - U16 tag (2 bytes, offset of this entry from block start)

        if offset + 9 > data_end {
            break;
        }

        let inumber = u64::from_be_bytes(buf[offset..offset + 8].try_into().unwrap());
        let namelen = buf[offset + 8] as usize;

        let name_start = offset + 9;
        let name_end = name_start + namelen;
        if name_end > data_end {
            break;
        }

        let name = &buf[name_start..name_end];

        let ftype = if ctx.has_ftype && name_end < data_end {
            buf[name_end]
        } else {
            0
        };

        let ftype_size: usize = if ctx.has_ftype { 1 } else { 0 };

        callback(&FsEvent::DirEntry {
            parent_ino,
            child_ino: inumber,
            name,
            file_type: ftype,
        });

        // Compute entry size: round up to 8-byte boundary.
        // entry_size = 8 (ino) + 1 (namelen) + namelen + ftype_size + 2 (tag)
        let raw_size = 8 + 1 + namelen + ftype_size + 2;
        let padded_size = (raw_size + 7) & !7;
        offset += padded_size;
    }

    Ok(())
}

use zerocopy::{FromBytes, Immutable, KnownLayout};
use zerocopy::byteorder::big_endian::U32;

use crate::error::FxfspError;
use crate::xfs::superblock::FormatVersion;

/// AGI magic: "XAGI"
const XFS_AGI_MAGIC: u32 = 0x58414749;

/// On-disk AG inode header (AGI). We only need the first portion.
#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct XfsAgi {
    pub agi_magicnum: U32,
    pub agi_versionnum: U32,
    pub agi_seqno: U32,
    pub agi_length: U32,
    pub agi_count: U32,
    pub agi_root: U32,
    pub agi_level: U32,
    pub agi_freecount: U32,
    pub agi_newino: U32,
    pub agi_dirino: U32,
    pub agi_unlinked: [U32; 64],
    // V5 fields (uuid, crc, pad, lsn) follow but we don't need them.
}

/// Parsed AGI information we need for traversal.
pub struct AgiInfo {
    pub ag_number: u32,
    pub inobt_root: u32,
    pub inobt_level: u32,
}

impl AgiInfo {
    /// Parse AGI from buffer. `agno` is used for error context.
    pub fn from_buf(buf: &[u8], agno: u32, _version: FormatVersion) -> Result<Self, FxfspError> {
        let agi = XfsAgi::ref_from_prefix(buf)
            .map_err(|_| FxfspError::Parse("buffer too small for AGI"))?
            .0;

        if agi.agi_magicnum.get() != XFS_AGI_MAGIC {
            return Err(FxfspError::BadMagic("AGI header"));
        }

        let seq = agi.agi_seqno.get();
        if seq != agno {
            return Err(FxfspError::Parse("AGI sequence number mismatch"));
        }

        Ok(AgiInfo {
            ag_number: agno,
            inobt_root: agi.agi_root.get(),
            inobt_level: agi.agi_level.get(),
        })
    }
}


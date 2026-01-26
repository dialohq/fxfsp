/// XFS inode number (absolute, 64-bit).
pub type XfsIno = u64;

/// XFS AG number.
pub type XfsAgnumber = u32;

/// XFS AG-relative block number.
pub type XfsAgblock = u32;

/// XFS AG-relative inode number (within the AG).
pub type XfsAgino = u32;

/// XFS filesystem block number (absolute, 64-bit).
pub type XfsFsblock = u64;

/// XFS file offset in filesystem blocks.
pub type XfsFileoff = u64;

/// XFS block count.
pub type XfsFilblks = u64;

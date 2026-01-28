pub mod error;
#[cfg(feature = "io")]
pub mod io;
pub mod reader;
pub mod staged;
pub mod xfs;

pub use error::FxfspError;
pub use reader::{IoPhase, IoReader};
pub use xfs::extent::Extent;
pub use xfs::superblock::FsContext;

// Phased API exports
pub use staged::{
    parse_superblock,
    SuperblockInfo,
    FsScanner,
    AgScanner,
    AgExtentPhase,
    AgDirPhase,
    InodeInfo,
    FileExtentsInfo,
    DirEntryInfo,
};

#[cfg(feature = "io")]
pub use io::engine::{DiskProfile, IoEngine, detect_disk_profile_for_path};
#[cfg(feature = "io")]
pub use io::reader::MaybeInstrumented;

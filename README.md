# fxfsp

High-performance raw XFS filesystem metadata scanner with phased typestate API and HDD-optimized I/O.

## Features

- **Phased typestate API** enforcing correct phase order at compile time
- **XFS v4 and v5 support** (with ftype, NREXT64)
- **HDD-optimized I/O**: read coalescing, sorted batch reads
- **io_uring on Linux** for async batch I/O
- **Zero-copy parsing** with zerocopy crate
- **Streaming callbacks** with early termination via `ControlFlow`
- **AG-decomposed extents** (ag_number + ag_block)

## Installation

```toml
[dependencies]
fxfsp = { git = "https://github.com/dialohq/fxfsp" }
```

## Quick Start

```rust
use std::ops::ControlFlow;
use fxfsp::{parse_superblock, IoEngine, InodeInfo};

let engine = IoEngine::open("disk.xfs", 256 * 1024, 2 * 1024 * 1024)?;
let (sb, mut scanner) = parse_superblock(engine)?;

println!("Block size: {}, AG count: {}", sb.block_size, sb.ag_count);

while let Some(ag_result) = scanner.next_ag() {
    let ag = ag_result?;

    ag.scan_inodes(|inode: &InodeInfo| {
        println!("inode {} size={}", inode.ino, inode.size);
        ControlFlow::Continue(())
    })?
    .skip_extents()
    .skip_dirs()?;
}
```

## API Overview

### Flow

```
parse_superblock(reader)
    → (SuperblockInfo, FsScanner)
        → FsScanner::next_ag()
            → AgScanner::scan_inodes(callback)
                → AgExtentPhase::scan_file_extents(callback) | skip_extents()
                    → AgDirPhase::scan_dir_entries(callback) | skip_dirs()
```

### Event Types

- `InodeInfo`: inode metadata + optional inline extents
- `FileExtentsInfo`: btree-format file extents
- `DirEntryInfo`: directory entries

## I/O Optimizations

- **Read coalescing**: merge adjacent reads (configurable gap/max size)
- **io_uring**: 128-deep queue for NCQ coordination
- **Direct I/O**: O_DIRECT (Linux) / F_NOCACHE (macOS)
- **Sorted batch reads**: minimize head movement

## Platform Support

| Platform | I/O Backend | Direct I/O |
|----------|-------------|------------|
| Linux    | io_uring    | O_DIRECT   |
| macOS    | pread       | F_NOCACHE  |

## Performance

Benchmarks on rotational media:

- 38,600+ inodes/sec on HDD
- 8% improvement with coalescing (256KB gap, 2MB max)

## License

MIT

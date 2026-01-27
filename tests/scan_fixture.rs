use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::ops::ControlFlow;
use std::os::unix::fs::FileExt;
use std::path::Path;

use fxfsp::{Extent, FsEvent, IoEngine, MaybeInstrumented, scan_reader};
use fxfsp::xfs::extent::fsblock_to_byte;
use fxfsp::xfs::superblock::FsContext;

const FIXTURE_PATH: &str = "tests/fixtures/test_v5.xfs";

/// Collect all scan events into structured data for assertions.
struct ScanResult {
    block_size: u32,
    ag_count: u32,
    inode_size: u16,
    root_ino: u64,
    /// ino -> (mode, size, nlink)
    inodes: HashMap<u64, InodeRecord>,
    /// (parent_ino, name) -> (child_ino, file_type)
    dir_entries: Vec<DirEntryRecord>,
    /// ino -> extents (from InodeFound inline + FileExtents events)
    file_extents: HashMap<u64, Vec<Extent>>,
}

#[allow(dead_code)]
struct InodeRecord {
    mode: u16,
    size: u64,
    nlink: u32,
    uid: u32,
    gid: u32,
    nblocks: u64,
}

#[derive(Clone)]
struct DirEntryRecord {
    parent_ino: u64,
    child_ino: u64,
    name: String,
    file_type: u8,
}

impl ScanResult {
    fn collect() -> Self {
        let mut result = ScanResult {
            block_size: 0,
            ag_count: 0,
            inode_size: 0,
            root_ino: 0,
            inodes: HashMap::new(),
            dir_entries: Vec::new(),
            file_extents: HashMap::new(),
        };

        let engine = IoEngine::open(FIXTURE_PATH, 256 * 1024, 2 * 1024 * 1024).expect("failed to open fixture");
        let mut reader = MaybeInstrumented::from_env(engine).expect("failed to create reader");
        scan_reader(&mut reader, |event| {
            match event {
                FsEvent::Superblock {
                    block_size,
                    ag_count,
                    inode_size,
                    root_ino,
                } => {
                    result.block_size = *block_size;
                    result.ag_count = *ag_count;
                    result.inode_size = *inode_size;
                    result.root_ino = *root_ino;
                }
                FsEvent::InodeFound {
                    ino,
                    mode,
                    size,
                    nlink,
                    uid,
                    gid,
                    nblocks,
                    extents,
                    ..
                } => {
                    result.inodes.insert(
                        *ino,
                        InodeRecord {
                            mode: *mode,
                            size: *size,
                            nlink: *nlink,
                            uid: *uid,
                            gid: *gid,
                            nblocks: *nblocks,
                        },
                    );
                    if let Some(exts) = extents {
                        result.file_extents.insert(*ino, exts.clone());
                    }
                }
                FsEvent::FileExtents { ino, extents } => {
                    result.file_extents.insert(*ino, extents.clone());
                }
                FsEvent::DirEntry {
                    parent_ino,
                    child_ino,
                    name,
                    file_type,
                } => {
                    result.dir_entries.push(DirEntryRecord {
                        parent_ino: *parent_ino,
                        child_ino: *child_ino,
                        name: String::from_utf8_lossy(name).to_string(),
                        file_type: *file_type,
                    });
                }
            }
            ControlFlow::Continue(())
        })
        .expect("scan should succeed");

        result
    }

    /// Get all directory entries for a given parent inode, excluding "." and "..".
    fn children_of(&self, parent_ino: u64) -> Vec<&DirEntryRecord> {
        self.dir_entries
            .iter()
            .filter(|e| e.parent_ino == parent_ino && e.name != "." && e.name != "..")
            .collect()
    }

    /// Look up a specific entry by parent ino and name.
    fn find_entry(&self, parent_ino: u64, name: &str) -> Option<&DirEntryRecord> {
        self.dir_entries
            .iter()
            .find(|e| e.parent_ino == parent_ino && e.name == name)
    }

    /// Get "." and ".." entries for a given parent.
    fn dot_entries_of(&self, parent_ino: u64) -> (Option<&DirEntryRecord>, Option<&DirEntryRecord>) {
        let dot = self.dir_entries.iter().find(|e| e.parent_ino == parent_ino && e.name == ".");
        let dotdot = self.dir_entries.iter().find(|e| e.parent_ino == parent_ino && e.name == "..");
        (dot, dotdot)
    }
}

fn skip_if_missing() -> bool {
    if !Path::new(FIXTURE_PATH).exists() {
        eprintln!("Skipping: fixture not found at {FIXTURE_PATH}");
        return true;
    }
    false
}

// ---------------------------------------------------------------------------
// Superblock
// ---------------------------------------------------------------------------

#[test]
fn superblock_has_valid_parameters() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    assert_eq!(r.block_size, 4096, "expected 4K block size");
    assert_eq!(r.ag_count, 4, "expected 4 AGs");
    assert_eq!(r.inode_size, 512, "expected 512-byte inodes");
    assert!(r.root_ino > 0, "root inode should be nonzero");
}

// ---------------------------------------------------------------------------
// Root directory
// ---------------------------------------------------------------------------

#[test]
fn root_inode_is_a_directory() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let root = r.inodes.get(&r.root_ino).expect("root inode not found");
    // S_IFDIR = 0o040000
    assert_eq!(root.mode & 0o170000, 0o040000, "root inode should be a directory");
}

#[test]
fn root_directory_contains_expected_entries() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let children = r.children_of(r.root_ino);
    let names: HashSet<&str> = children.iter().map(|e| e.name.as_str()).collect();

    assert!(names.contains("empty_file"), "root should contain 'empty_file'");
    assert!(names.contains("hello.txt"), "root should contain 'hello.txt'");
    assert!(names.contains("subdir"), "root should contain 'subdir'");

    // Exactly 3 non-dot entries in root.
    assert_eq!(children.len(), 3, "root should have exactly 3 children (empty_file, hello.txt, subdir)");
}

#[test]
fn root_directory_has_dot_entries() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let (dot, dotdot) = r.dot_entries_of(r.root_ino);

    let dot = dot.expect("root should have '.' entry");
    assert_eq!(dot.child_ino, r.root_ino, "'.' should point to root itself");

    let dotdot = dotdot.expect("root should have '..' entry");
    assert_eq!(dotdot.child_ino, r.root_ino, "root '..' should point to root itself");
}

#[test]
fn root_entry_file_types_are_correct() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // ftype: 1 = regular file, 2 = directory
    let empty_file = r.find_entry(r.root_ino, "empty_file").unwrap();
    assert_eq!(empty_file.file_type, 1, "empty_file should be ftype=1 (regular)");

    let hello = r.find_entry(r.root_ino, "hello.txt").unwrap();
    assert_eq!(hello.file_type, 1, "hello.txt should be ftype=1 (regular)");

    let subdir = r.find_entry(r.root_ino, "subdir").unwrap();
    assert_eq!(subdir.file_type, 2, "subdir should be ftype=2 (directory)");
}

// ---------------------------------------------------------------------------
// Known files
// ---------------------------------------------------------------------------

#[test]
fn empty_file_has_zero_size() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "empty_file").unwrap();
    let inode = r.inodes.get(&entry.child_ino).expect("empty_file inode not found");

    assert_eq!(inode.size, 0, "empty_file should have size 0");
    assert_eq!(inode.mode & 0o170000, 0o100000, "empty_file should be a regular file");
}

#[test]
fn hello_txt_has_expected_size() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "hello.txt").unwrap();
    let inode = r.inodes.get(&entry.child_ino).expect("hello.txt inode not found");

    // "hello\n" = 6 bytes
    assert_eq!(inode.size, 6, "hello.txt should have size 6 (\"hello\\n\")");
    assert_eq!(inode.mode & 0o170000, 0o100000, "hello.txt should be a regular file");
}

// ---------------------------------------------------------------------------
// Subdirectory
// ---------------------------------------------------------------------------

#[test]
fn subdir_inode_is_a_directory() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let inode = r.inodes.get(&entry.child_ino).expect("subdir inode not found");

    assert_eq!(inode.mode & 0o170000, 0o040000, "subdir should be a directory");
    assert_eq!(inode.nlink, 2, "subdir should have nlink=2 (. and parent)");
}

#[test]
fn subdir_has_dot_entries_with_correct_targets() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let subdir_ino = subdir_entry.child_ino;

    let (dot, dotdot) = r.dot_entries_of(subdir_ino);

    let dot = dot.expect("subdir should have '.' entry");
    assert_eq!(dot.child_ino, subdir_ino, "subdir '.' should point to itself");

    let dotdot = dotdot.expect("subdir should have '..' entry");
    assert_eq!(dotdot.child_ino, r.root_ino, "subdir '..' should point to root");
}

#[test]
fn subdir_contains_nested_txt() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let subdir_ino = subdir_entry.child_ino;

    let nested = r.find_entry(subdir_ino, "nested.txt");
    assert!(nested.is_some(), "subdir should contain 'nested.txt'");

    let nested = nested.unwrap();
    assert_eq!(nested.file_type, 1, "nested.txt should be ftype=1 (regular)");

    let inode = r.inodes.get(&nested.child_ino).expect("nested.txt inode not found");
    // "nested\n" = 7 bytes
    assert_eq!(inode.size, 7, "nested.txt should have size 7 (\"nested\\n\")");
}

#[test]
fn subdir_contains_all_200_numbered_files() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let subdir_ino = subdir_entry.child_ino;

    let children = r.children_of(subdir_ino);
    let names: HashSet<&str> = children.iter().map(|e| e.name.as_str()).collect();

    // Check every file_1 through file_200.
    for i in 1..=200 {
        let expected_name = format!("file_{i}");
        assert!(
            names.contains(expected_name.as_str()),
            "subdir should contain '{expected_name}'"
        );
    }
}

#[test]
fn subdir_has_exactly_201_children() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let subdir_ino = subdir_entry.child_ino;

    let children = r.children_of(subdir_ino);
    // nested.txt + file_1..file_200 = 201 entries
    assert_eq!(
        children.len(),
        201,
        "subdir should have 201 children (nested.txt + file_1..file_200)"
    );
}

#[test]
fn all_numbered_files_are_empty_regular_files() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let subdir_ino = subdir_entry.child_ino;

    for i in 1..=200 {
        let name = format!("file_{i}");
        let entry = r.find_entry(subdir_ino, &name)
            .unwrap_or_else(|| panic!("file_{i} not found in subdir"));

        assert_eq!(entry.file_type, 1, "{name} should be ftype=1 (regular file)");

        let inode = r.inodes.get(&entry.child_ino)
            .unwrap_or_else(|| panic!("{name} inode {} not found", entry.child_ino));
        assert_eq!(inode.size, 0, "{name} should be an empty file (size=0)");
        assert_eq!(inode.mode & 0o170000, 0o100000, "{name} should be a regular file");
    }
}

// ---------------------------------------------------------------------------
// File extents
// ---------------------------------------------------------------------------

#[test]
fn hello_txt_has_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "hello.txt").unwrap();
    let extents = r.file_extents.get(&entry.child_ino)
        .expect("hello.txt should have extents");

    assert!(!extents.is_empty(), "hello.txt should have at least one extent");
    assert_eq!(extents[0].logical_offset, 0, "first extent should start at logical offset 0");
    assert!(extents[0].block_count > 0, "extent should have nonzero block count");
    assert!(!extents[0].is_unwritten, "hello.txt extent should not be unwritten");
}

#[test]
fn empty_file_has_no_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "empty_file").unwrap();
    // Empty files have no data blocks, so no extents should be emitted.
    assert!(
        !r.file_extents.contains_key(&entry.child_ino),
        "empty_file should have no extents"
    );
}

#[test]
fn non_empty_regular_files_have_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // Every regular file with size > 0 should have extents.
    for (&ino, rec) in &r.inodes {
        if (rec.mode & 0o170000) == 0o100000 && rec.size > 0 {
            assert!(
                r.file_extents.contains_key(&ino),
                "regular file ino={} size={} should have extents",
                ino, rec.size
            );
        }
    }
}

#[test]
fn directories_have_no_file_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // Directory extents are handled internally; they should NOT appear in file_extents.
    for (&ino, rec) in &r.inodes {
        if (rec.mode & 0o170000) == 0o040000 {
            assert!(
                !r.file_extents.contains_key(&ino),
                "directory ino={} should not have file extents",
                ino
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Extent-based file content verification
// ---------------------------------------------------------------------------

/// Read file content from the raw fixture image using extent information.
///
/// Opens the fixture file, parses the superblock to get an FsContext,
/// then reads data at the byte offsets computed from the extent records.
fn read_file_from_extents(extents: &[Extent], file_size: u64) -> Vec<u8> {
    let f = File::open(FIXTURE_PATH).expect("failed to open fixture for extent read");

    // Parse the superblock to get FsContext (needed for fsblock → byte conversion).
    let mut sb_buf = vec![0u8; 4096];
    f.read_at(&mut sb_buf, 0).expect("failed to read superblock");
    let ctx = FsContext::from_superblock(&sb_buf).expect("failed to parse superblock");

    let block_size = ctx.block_size as u64;
    let mut data = Vec::new();
    let mut remaining = file_size;

    for ext in extents {
        if remaining == 0 {
            break;
        }
        let byte_offset = fsblock_to_byte(&ctx, ext.start_block);
        let extent_bytes = ext.block_count * block_size;
        let to_read = remaining.min(extent_bytes) as usize;

        let mut buf = vec![0u8; to_read];
        f.read_at(&mut buf, byte_offset).expect("failed to read extent data");
        data.extend_from_slice(&buf);
        remaining = remaining.saturating_sub(extent_bytes);
    }

    data
}

#[test]
fn hello_txt_content_matches_via_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let entry = r.find_entry(r.root_ino, "hello.txt").unwrap();
    let inode = r.inodes.get(&entry.child_ino).expect("hello.txt inode not found");
    let extents = r.file_extents.get(&entry.child_ino)
        .expect("hello.txt should have extents");

    let content = read_file_from_extents(extents, inode.size);
    assert_eq!(content, b"hello\n", "hello.txt content should be \"hello\\n\"");
}

#[test]
fn nested_txt_content_matches_via_extents() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    let subdir_entry = r.find_entry(r.root_ino, "subdir").unwrap();
    let nested_entry = r.find_entry(subdir_entry.child_ino, "nested.txt").unwrap();
    let inode = r.inodes.get(&nested_entry.child_ino).expect("nested.txt inode not found");
    let extents = r.file_extents.get(&nested_entry.child_ino)
        .expect("nested.txt should have extents");

    let content = read_file_from_extents(extents, inode.size);
    assert_eq!(content, b"nested\n", "nested.txt content should be \"nested\\n\"");
}

// ---------------------------------------------------------------------------
// Cross-directory consistency
// ---------------------------------------------------------------------------

#[test]
fn every_dir_entry_references_a_discovered_inode() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    for entry in &r.dir_entries {
        assert!(
            r.inodes.contains_key(&entry.child_ino),
            "dir entry '{}' (parent={}) references inode {} which was not found in inode scan",
            entry.name, entry.parent_ino, entry.child_ino
        );
    }
}

#[test]
fn every_directory_inode_has_entries() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // Every inode that is a directory should have at least "." and ".." entries.
    let dir_inodes: Vec<u64> = r
        .inodes
        .iter()
        .filter(|(_, rec)| (rec.mode & 0o170000) == 0o040000)
        .map(|(&ino, _)| ino)
        .collect();

    for dir_ino in &dir_inodes {
        let (dot, dotdot) = r.dot_entries_of(*dir_ino);
        assert!(
            dot.is_some(),
            "directory inode {dir_ino} should have a '.' entry"
        );
        assert!(
            dotdot.is_some(),
            "directory inode {dir_ino} should have a '..' entry"
        );
    }
}

#[test]
fn no_duplicate_entries_within_a_directory() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // Group entries by parent, check for duplicate names.
    let mut by_parent: HashMap<u64, Vec<&str>> = HashMap::new();
    for entry in &r.dir_entries {
        by_parent
            .entry(entry.parent_ino)
            .or_default()
            .push(&entry.name);
    }

    for (parent_ino, names) in &by_parent {
        let unique: HashSet<&&str> = names.iter().collect();
        assert_eq!(
            names.len(),
            unique.len(),
            "directory inode {parent_ino} has duplicate entry names"
        );
    }
}

// ---------------------------------------------------------------------------
// Inode counts and totals
// ---------------------------------------------------------------------------

#[test]
fn total_inode_count_is_plausible() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // We created: root dir, 2 internal inodes (typically ino 129, 130),
    // empty_file, hello.txt, subdir, nested.txt, and file_1..file_200.
    // That's 3 + 3 + 1 + 200 = 207, but internal inodes may vary.
    // At minimum we need 204 user-visible inodes.
    assert!(
        r.inodes.len() >= 204,
        "expected at least 204 inodes, got {}",
        r.inodes.len()
    );
}

#[test]
fn total_dir_entry_count_matches_expected() {
    if skip_if_missing() { return; }
    let r = ScanResult::collect();

    // Root: . + .. + empty_file + hello.txt + subdir = 5
    // Subdir: . + .. + nested.txt + file_1..file_200 = 203
    // Total = 208
    assert_eq!(
        r.dir_entries.len(),
        208,
        "expected exactly 208 directory entries total"
    );
}

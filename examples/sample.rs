use std::env;
use std::ops::ControlFlow;
use std::process;
use std::time::Instant;

use fxfsp::{
    parse_superblock, IoEngine, MaybeInstrumented, detect_disk_profile_for_path,
    InodeInfo, FileExtentsInfo, DirEntryInfo,
};

fn mode_string(mode: u16) -> String {
    let file_type = match mode & 0o170000 {
        0o140000 => 's', // socket
        0o120000 => 'l', // symlink
        0o100000 => '-', // regular
        0o060000 => 'b', // block device
        0o040000 => 'd', // directory
        0o020000 => 'c', // char device
        0o010000 => 'p', // fifo
        _ => '?',
    };
    let perms = mode & 0o7777;
    let r = |bit: u16| if perms & bit != 0 { 'r' } else { '-' };
    let w = |bit: u16| if perms & bit != 0 { 'w' } else { '-' };
    let x = |bit: u16| if perms & bit != 0 { 'x' } else { '-' };
    format!(
        "{}{}{}{}{}{}{}{}{}{}",
        file_type,
        r(0o400), w(0o200), x(0o100),
        r(0o040), w(0o020), x(0o010),
        r(0o004), w(0o002), x(0o001),
    )
}

struct Args {
    path: String,
    max_ag: Option<u32>,
    merge_gap_kb: usize,
    max_merged_kb: usize,
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let mut path = None;
    let mut max_ag = None;
    let mut merge_gap_kb = 256;
    let mut max_merged_kb = 2048;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--max-ag" => {
                i += 1;
                max_ag = args.get(i).and_then(|s| s.parse().ok());
            }
            "--merge-gap" => {
                i += 1;
                merge_gap_kb = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(merge_gap_kb);
            }
            "--max-merged" => {
                i += 1;
                max_merged_kb = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(max_merged_kb);
            }
            _ if !args[i].starts_with('-') && path.is_none() => {
                path = Some(args[i].clone());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                eprintln!("Usage: sample [--max-ag N] [--merge-gap KB] [--max-merged KB] <device-or-image>");
                process::exit(1);
            }
        }
        i += 1;
    }
    let path = match path {
        Some(p) => p,
        None => {
            eprintln!("Usage: sample [--max-ag N] [--merge-gap KB] [--max-merged KB] <device-or-image>");
            process::exit(1);
        }
    };
    Args { path, max_ag, merge_gap_kb, max_merged_kb }
}

fn main() {
    let args = parse_args();

    let profile = detect_disk_profile_for_path(&args.path);
    eprintln!("{}", profile);
    eprintln!("merge_gap={} KB  max_merged={} KB", args.merge_gap_kb, args.max_merged_kb);

    let engine = IoEngine::open(
        &args.path,
        args.merge_gap_kb * 1024,
        args.max_merged_kb * 1024,
    ).unwrap_or_else(|e| {
        eprintln!("Failed to open {}: {e}", args.path);
        process::exit(1);
    });

    let reader = MaybeInstrumented::from_env(engine).unwrap_or_else(|e| {
        eprintln!("Failed to set up I/O reader: {e}", );
        process::exit(1);
    });

    let max_ag = args.max_ag;

    let start = Instant::now();
    let mut inode_count: u64 = 0;
    let mut dir_entry_count: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;

    let result = (|| {
        let (sb, mut scanner) = parse_superblock(reader)?;
        println!(
            "Superblock: block_size={} ag_count={} ag_blocks={} inode_size={} root_ino={}",
            sb.block_size, sb.ag_count, sb.ag_blocks, sb.inode_size, sb.root_ino
        );

        while let Some(ag_result) = scanner.next_ag() {
            let ag = ag_result?;
            let ag_number = ag.ag_number();

            if max_ag.is_some_and(|limit| ag_number >= limit) {
                break;
            }

            // Phase 1: Scan inodes
            let phase2 = ag.scan_inodes(|inode: &InodeInfo| {
                inode_count += 1;
                match inode.mode & 0o170000 {
                    0o040000 => dir_count += 1,
                    0o100000 => file_count += 1,
                    _ => {}
                }
                if inode_count % 1000 == 0 {
                    println!(
                        "[inode #{:>9}] ag={:<4} ino={:<12} {} uid={:<5} gid={:<5} nlink={:<4} size={:<12} blocks={:<8} mtime={}",
                        inode_count, inode.ag_number, inode.ino, mode_string(inode.mode),
                        inode.uid, inode.gid, inode.nlink, inode.size, inode.nblocks, inode.mtime_sec
                    );
                }
                ControlFlow::Continue(())
            })?;

            // Phase 1.5: File extents (btree-format files)
            let phase3 = phase2.scan_file_extents(|_fe: &FileExtentsInfo| {
                // File extent maps available for later batch reads.
                ControlFlow::Continue(())
            })?;

            // Phase 2: Directory entries
            phase3.scan_dir_entries(|de: &DirEntryInfo| {
                dir_entry_count += 1;
                if dir_entry_count % 1000 == 0 {
                    let name_str = String::from_utf8_lossy(de.name);
                    let ft = match de.file_type {
                        1 => "REG",
                        2 => "DIR",
                        3 => "CHR",
                        4 => "BLK",
                        5 => "FIFO",
                        6 => "SOCK",
                        7 => "LNK",
                        _ => "???",
                    };
                    println!(
                        "[entry #{:>9}] parent={:<12} -> {:?} (ino={}, type={})",
                        dir_entry_count, de.parent_ino, name_str, de.child_ino, ft
                    );
                }
                ControlFlow::Continue(())
            })?;
        }

        Ok::<(), fxfsp::FxfspError>(())
    })();

    let elapsed = start.elapsed();

    match result {
        Ok(()) => {
            println!();
            println!("=== Scan complete ===");
            println!("  Inodes:      {}", inode_count);
            println!("    Files:     {}", file_count);
            println!("    Dirs:      {}", dir_count);
            println!("    Other:     {}", inode_count - file_count - dir_count);
            println!("  Dir entries: {}", dir_entry_count);
            println!("  Elapsed:     {:.3}s", elapsed.as_secs_f64());
            if elapsed.as_secs_f64() > 0.0 {
                println!(
                    "  Throughput:  {:.0} inodes/s, {:.0} entries/s",
                    inode_count as f64 / elapsed.as_secs_f64(),
                    dir_entry_count as f64 / elapsed.as_secs_f64()
                );
            }
        }
        Err(e) => {
            eprintln!("Scan failed: {e}");
            process::exit(1);
        }
    }
}

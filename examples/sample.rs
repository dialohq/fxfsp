use std::env;
use std::ops::ControlFlow;
use std::process;
use std::time::Instant;

use fxfsp::{FsEvent, IoEngine, MaybeInstrumented, detect_disk_profile_for_path, scan_reader};

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

    let mut reader = MaybeInstrumented::from_env(engine).unwrap_or_else(|e| {
        eprintln!("Failed to set up I/O reader: {e}", );
        process::exit(1);
    });

    let max_ag = args.max_ag;

    let start = Instant::now();
    let mut inode_count: u64 = 0;
    let mut dir_entry_count: u64 = 0;
    let mut dir_count: u64 = 0;
    let mut file_count: u64 = 0;

    let result = scan_reader(&mut reader, |event| {
        match event {
            FsEvent::Superblock { block_size, ag_count, inode_size, root_ino } => {
                println!(
                    "Superblock: block_size={} ag_count={} inode_size={} root_ino={}",
                    block_size, ag_count, inode_size, root_ino
                );
            }
            FsEvent::InodeFound { ag_number, ino, mode, size, uid, gid, nlink, mtime_sec, nblocks, .. } => {
                if max_ag.is_some_and(|limit| *ag_number >= limit) {
                    return ControlFlow::Break(());
                }
                inode_count += 1;
                match mode & 0o170000 {
                    0o040000 => dir_count += 1,
                    0o100000 => file_count += 1,
                    _ => {}
                }
                if inode_count % 1000 == 0 {
                    println!(
                        "[inode #{:>9}] ag={:<4} ino={:<12} {} uid={:<5} gid={:<5} nlink={:<4} size={:<12} blocks={:<8} mtime={}",
                        inode_count, ag_number, ino, mode_string(*mode), uid, gid, nlink, size, nblocks, mtime_sec
                    );
                }
            }
            FsEvent::FileExtents { .. } => {
                // File extent maps available for later batch reads.
            }
            FsEvent::DirEntry { parent_ino, child_ino, name, file_type } => {
                dir_entry_count += 1;
                if dir_entry_count % 1000 == 0 {
                    let name_str = String::from_utf8_lossy(name);
                    let ft = match file_type {
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
                        dir_entry_count, parent_ino, name_str, child_ino, ft
                    );
                }
            }
        }
        ControlFlow::Continue(())
    });

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

#!/bin/bash
#
# Benchmark different merge_gap / max_merged combinations.
# Runs AG 0 only for each config and records elapsed time + IO stats.
#
# Usage: sudo ./bench_merge.sh /dev/sde1
#
set -euo pipefail

DEV="${1:?Usage: $0 <device>}"
BIN="$(dirname "$0")/target/release/fxfsp-sample"
RESULTS="/tmp/bench_results.csv"
DISKSTATS_DEV="sde1"

if [ ! -x "$BIN" ]; then
  echo "ERROR: $BIN not found. Run: cargo build --release" >&2
  exit 1
fi

echo "merge_gap_kb,max_merged_kb,elapsed_s,inodes,entries,reads,sectors_read,mb_read,mb_per_sec,iops" > "$RESULTS"

# Snapshot /proc/diskstats for a device, returns "reads sectors"
diskstats() {
  awk -v dev="$DISKSTATS_DEV" '$3 == dev {print $4, $6}' /proc/diskstats
}

run_one() {
  local gap_kb=$1
  local max_kb=$2
  local label="gap=${gap_kb}KB_max=${max_kb}KB"
  local logfile="/tmp/bench_${label}.log"

  echo ">>> Running: merge_gap=${gap_kb}KB max_merged=${max_kb}KB"

  # Drop caches so each run starts cold
  sync
  echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

  local ds_before
  ds_before=$(diskstats)
  local reads_before=${ds_before%% *}
  local sectors_before=${ds_before##* }
  local t_start
  t_start=$(date +%s%N)

  FXFSP_MERGE_GAP_KB=$gap_kb \
  FXFSP_MAX_MERGED_KB=$max_kb \
  FXFSP_MAX_AG=1 \
    "$BIN" "$DEV" > "$logfile" 2>&1

  local t_end
  t_end=$(date +%s%N)
  local ds_after
  ds_after=$(diskstats)
  local reads_after=${ds_after%% *}
  local sectors_after=${ds_after##* }

  local elapsed_ns=$((t_end - t_start))
  local elapsed_s=$((elapsed_ns / 1000000000))
  local elapsed_frac=$(( (elapsed_ns % 1000000000) / 1000000 ))
  local elapsed="${elapsed_s}.${elapsed_frac}"

  local delta_reads=$((reads_after - reads_before))
  local delta_sectors=$((sectors_after - sectors_before))
  local delta_mb=$((delta_sectors * 512 / 1048576))

  # Extract counts from log
  local inodes entries
  inodes=$(grep -oP 'Inodes:\s+\K[0-9]+' "$logfile" 2>/dev/null || echo 0)
  entries=$(grep -oP 'Dir entries:\s+\K[0-9]+' "$logfile" 2>/dev/null || echo 0)

  local mb_per_sec=0
  local iops=0
  if [ "$elapsed_s" -gt 0 ]; then
    mb_per_sec=$((delta_mb / elapsed_s))
    iops=$((delta_reads / elapsed_s))
  fi

  echo "${gap_kb},${max_kb},${elapsed},${inodes},${entries},${delta_reads},${delta_sectors},${delta_mb},${mb_per_sec},${iops}" >> "$RESULTS"
  echo "    elapsed=${elapsed}s inodes=${inodes} entries=${entries} reads=${delta_reads} MB=${delta_mb} MB/s=${mb_per_sec} IOPS=${iops}"
  echo ""
}

echo "============================================"
echo " fxfsp merge benchmark (AG 0 only)"
echo " device: $DEV"
echo " results: $RESULTS"
echo " started: $(date)"
echo "============================================"
echo ""

# ---- Test matrix ----
# merge_gap_kb values: 0 (no coalescing), 64, 128, 192, 256, 384, 512, 768, 1024, 1280
# max_merged_kb values: 1024, 2048, 4096, 8192, 16384
#
# First, sweep merge_gap with a generous max_merged (16 MB)
for gap in 0 64 128 192 256 384 512 768 1024 1280; do
  run_one $gap 16384
done

# Then sweep max_merged at the most promising merge_gap values
for max in 256 512 1024 2048 4096 8192 16384 32768 65536; do
  run_one 256 $max
done

for max in 256 512 1024 2048 4096 8192 16384 32768 65536; do
  run_one 512 $max
done

# Repeat the full gap sweep to check variance
echo "=== SECOND PASS (variance check) ==="
for gap in 0 64 128 192 256 384 512 768 1024 1280; do
  run_one $gap 16384
done

echo ""
echo "============================================"
echo " DONE: $(date)"
echo " Results in: $RESULTS"
echo "============================================"
cat "$RESULTS"

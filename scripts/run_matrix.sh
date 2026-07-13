#!/usr/bin/env bash
# S6 benchmark matrix driver. Runs each cell N times against a FRESH data dir on
# the measured disk, teeing raw output to benchmarks/raw/. No numbers are derived
# here; RESULTS.md quotes/medians whatever these raw files contain.
set -u
cd "$(dirname "$0")/.."
B=target/release/accretion-bench
RAW=benchmarks/raw
mkdir -p "$RAW"
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT

MB64=$((64 * 1024 * 1024))

# cell <outfile> <runs> <bench-args...>
cell() {
  out="$RAW/$1"; runs="$2"; shift 2
  : > "$out"
  for r in $(seq 1 "$runs"); do
    d="$SCRATCH/$(echo "$1$*" | tr -c 'a-zA-Z0-9' '_')_$r"
    echo "## run $r  ARGS: $*" >> "$out"
    "$B" "$@" --dir "$d" >> "$out" 2>&1
    rm -rf "$d"
  done
  echo "done $out"
}

echo "=== B1 WAL-commit-bound durability table (64MiB memtable, no flush) ==="
for m in always group osbuffered; do
  for c in 1 8 64; do
    cell "b1_walbound_${m}_c${c}.txt" 5 \
      fill-random --engine accretion --durability "$m" --keys 10000 \
      --concurrency "$c" --memtable-bytes "$MB64"
  done
done

echo "=== B1b full-engine durability table (default memtable, crosses flush+compaction) ==="
cell "b1b_fullengine_osbuffered_c64.txt" 5 \
  fill-random --engine accretion --durability osbuffered --keys 50000 --concurrency 64
cell "b1b_fullengine_group_c64.txt" 3 \
  fill-random --engine accretion --durability group --keys 50000 --concurrency 64
cell "b1b_fullengine_always_c64.txt" 3 \
  fill-random --engine accretion --durability always --keys 50000 --concurrency 64

echo "=== B1 fill-seq (sequential fill, group + osbuffered) ==="
cell "b1_fillseq_group_c8_walbound.txt" 5 \
  fill-seq --engine accretion --durability group --keys 10000 --concurrency 8 --memtable-bytes "$MB64"
cell "b1_fillseq_osbuffered_c8.txt" 5 \
  fill-seq --engine accretion --durability osbuffered --keys 50000 --concurrency 8

echo "=== B2 point-read (cold, post-flush+compaction) ==="
cell "b2_pointread_c8.txt" 5 \
  point-read --engine accretion --durability osbuffered --keys 50000 --reads 50000 --concurrency 8

echo "=== B3 scan ==="
cell "b3_scan.txt" 5 \
  scan --engine accretion --durability osbuffered --keys 50000 --concurrency 1

echo "=== SLED baseline (matched durability) ==="
cell "sled_durable_c1.txt" 5 \
  fill-random --engine sled --durability always --keys 3000 --concurrency 1
cell "sled_buffered_c1.txt" 5 \
  fill-random --engine sled --durability osbuffered --keys 50000 --concurrency 1
cell "sled_buffered_pointread_c8.txt" 5 \
  point-read --engine sled --durability osbuffered --keys 50000 --reads 50000 --concurrency 8

echo "=== accretion matched partners for sled ==="
cell "acc_durable_matched_c1.txt" 5 \
  fill-random --engine accretion --durability always --keys 3000 --concurrency 1
cell "acc_buffered_matched_c1.txt" 5 \
  fill-random --engine accretion --durability osbuffered --keys 50000 --concurrency 1

echo "ALL MATRIX CELLS COMPLETE"

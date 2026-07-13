# Design notes

Rationale behind the non-obvious choices in `accretion-db`. This is a living
document; sections are stubbed now and filled as each stage lands. The
interview-facing goal is that every decision here can be defended from first
principles.

## The `Storage` seam (built, S0)

The engine talks to disk through one object-safe trait, `Storage`, held as
`Arc<dyn Storage>`. Two implementations — `RealFs` and `SimFs` — are
interchangeable, so the entire test suite can run the real engine against a
deterministic power-loss simulator without the engine knowing. The trait is
path-addressed (no per-file handle type) to stay object-safe and to keep the
buffered-vs-durable accounting in one place.

**Durability is explicit.** A mutating call promises visibility to this process
only. Survival across a crash is earned by `sync_file` (file bytes) and
`sync_dir` (directory entries: create/rename/delete). This mirrors real POSIX
semantics and is exactly what makes the manifest's tmp+fsync+rename+dir-fsync
protocol necessary — see below.

## Group-commit math (stub — WAL stage)

On a disk whose 4 KiB fsync costs ~2.79 ms (measured on the build host via
`scripts/fsync_probe.rs`), per-write durability caps throughput at ~1/0.00279 ≈
**350 writes/sec regardless of engine quality**. Group commit batches *N* queued
writers into a single write+fsync, dividing the fsync cost across them:
throughput scales toward *N* × 350 while single-write latency rises toward one
batch interval. This throughput/latency tradeoff is why the headline resume
number names the *mode* (`GroupCommit`), not just a raw figure.
`{MEASURE: Nx multiplier}` to be filled from `benchmarks/RESULTS.md`.

## Torn-tail truncation (stub — WAL stage)

The WAL is a sequence of length-prefixed, CRC32-framed records. Recovery scans
frames and **truncates at the first frame that is short or fails its CRC**. This
cannot lose acknowledged data: in a durable mode a `put` only acks after its
record is `sync_file`d, so any acked record precedes the torn tail and verifies
cleanly. The toy store in `tests/harness.rs` already demonstrates this rule;
the real WAL adopts it verbatim.

## Bloom filter sizing (stub — SSTable stage)

Per-table bloom, own implementation, k hash functions via xxhash64 double
hashing. FPR ≈ (1 − e^(−kn/m))^k, minimised at k = (m/n) ln 2. Target bits/key,
chosen k, and the **measured vs theoretical FPR** get published here and in the
README once the SSTable builder exists.

## Size-tiered vs leveled compaction (stub — compaction stage)

Size-tiered chosen deliberately: **lower write amplification and simpler
invariants**, at the cost of higher space and read amplification. Tier *t*
compacts when it holds ≥ `tier_fanout` tables, merging into one table in tier
*t+1*. Tombstone GC only at the bottom tier — a tombstone may still be masking
live data in a lower tier, so dropping it early would resurrect deleted keys.
Leveled compaction buys RocksDB tighter read/space amp at higher write amp; out
of scope here.

## Manifest atomicity (stub — manifest stage)

A version switch writes a new manifest file, `sync_file`s it, atomically
`rename`s it into place, then `sync_dir`s the parent. Without the final
directory fsync the rename is volatile: a crash could leave the manifest name
pointing at the old inode even though the new file's bytes are durable, so a
reader could load a manifest that references files that were meant to be
obsoleted (or miss files that were meant to be installed). Readers pin an
`Arc<Version>` and never observe a half-installed version.

## Concurrency model (stub — engine stage)

A deliberate simplicity choice: a single logical writer (mutex on the write
path), readers via an `RwLock` memtable snapshot plus a pinned `Arc<Version>`,
and — after the crash suite is green — exactly one background thread for
flush/compaction, joined on `Drop`. Readers pinned to an old `Arc<Version>` stay
correct while compaction replaces files underneath them, because a file is only
deleted once no `Version` references it.

## Why not mmap / io_uring / a block cache (stub)

Out of scope by design: the engine leans on the OS page cache instead of a
custom block cache (documented decision, revisited only if benchmarks demand
it), and avoids io_uring/O_DIRECT to keep the code pure-Rust and portable. The
point of the project is crash-consistency proof, not squeezing the I/O path.

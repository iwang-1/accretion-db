# Benchmark results

All numbers here were produced on the build host described below, by the
committed tooling (`scripts/fsync_probe.rs`, `target/release/accretion-bench`,
`scripts/run_matrix.sh`). Every raw run is preserved under
[`benchmarks/raw/`](raw/); the tables quote the **median of 5 runs** (3 for the
slowest full-engine cells) with min/max noted where the spread matters. Nothing
in this file is hand-tuned — re-running `scripts/run_matrix.sh` regenerates the
raw inputs. Where a result is *worse* than sled it is still published (see
§6): the honesty of the comparison is the point.

## 1. Host + disk disclosure

| | |
|---|---|
| CPU | Intel Xeon Platinum 8488C, 48 vCPU |
| Memory | 92 GiB |
| OS | Linux, kernel 5.10.259 x86_64 |
| libc | glibc 2.26 |
| Filesystem | ext4 on NVMe (`/dev/nvme0n1p1`, mounted `noatime`) |
| Toolchain | rustc 1.95.0, `--release` (opt-level 3, `debug = true` for symbols) |
| Data dir | fresh temp dir per run on the ext4 NVMe volume above |

No page-cache dropping is performed (the host has no root); the read benchmarks
are therefore **warm-page-cache** reads, disclosed as such in §4.

## 2. The fsync probe — the number that frames everything

`scripts/fsync_probe.rs` (standalone, `std`-only; compile with
`rustc -O scripts/fsync_probe.rs`) times *only* the durability call over 2000
iterations of a 4 KiB block, after 16 warmup iterations. Median across three runs
([`raw/fsync_probe_run{1,2,3}.txt`](raw/)):

| operation | p50 | p99 | notes |
|---|---|---|---|
| `sync_data` (fdatasync, 4 KiB) | **878 µs** | **975 µs** | what the WAL commit path pays per fsync |
| `sync_all` (fsync, 4 KiB) | 889 µs | 2.78 ms | data + metadata; the heavy p99 tail |
| dir fsync (rename durability) | 1.97 ms | 2.14 ms | what the manifest tmp+rename+dir-sync protocol pays |

**Framing.** A durable `put` cannot return until its record is on stable storage,
i.e. it pays at least one `fdatasync`. At 878 µs p50 that caps a *bare*
fsync-per-write engine at **≈ 1,140 durable writes/sec regardless of engine
quality** — the disk, not the code, is the ceiling. (An earlier draft of this
repo quoted "2.79 ms"; that figure is really the `sync_all` **p99**, not the
`sync_data` p50 the WAL actually pays. Corrected here to the measured value.)
Group commit's entire job is to amortize one `fdatasync` across many queued
writers; §3 measures how far it gets.

## 3. Headline: the durability-mode table (B1)

`fill-random`, 16-byte keys / 100-byte values, `accretion-bench` closed loop
(each worker issues the next op only after the previous returns, so latency is
real per-op service time and throughput is `ops / wall`).

### 3a. WAL-commit-bound regime (64 MiB memtable — the fill never flushes)

This isolates the **commit pipeline**: the memtable is large enough that all
10,000 writes stay in memory, so what is measured is purely WAL append + the
mode's fsync discipline. This is the regime the group-commit math describes.

| mode | c=1 | c=8 | c=64 | write p50 @ c=64 |
|---|---:|---:|---:|---:|
| `Always` (fsync per put) | 369 | 348 | 276 | 3.7 ms |
| `GroupCommit` (batched fsync) | 274 | 1,093 | **8,082** | 7.5 ms |
| `OsBuffered` (no durability guarantee) | 60,329 | 38,232 | 34,361 | 20 µs |

throughput = writes/sec, median of 5 runs. Raw:
[`raw/b1_walbound_*`](raw/).

**Group-commit multiplier: 8,082 / 276 ≈ 29× at c=64**, bought by trading
same-concurrency p50 latency (3.7 ms → 7.5 ms) for batched fsync amortization —
exactly the throughput-for-latency trade the math predicts. This no-flush setup
means `Always` executes one WAL `sync_data` per put; the observed 3.7 ms also
includes append/file-open, locking, and scheduler overhead. The comparison is one
barrier per `Always` write versus one shared barrier per group. `Always` *loses*
throughput as concurrency rises (369 → 276) because contending writers serialize
while each still pays its own barrier — extra threads add only lock traffic.

### 3b. Full-engine regime (default 4 MiB memtable — crosses flush + compaction)

The same workload at 50,000 keys with the default memtable genuinely freezes,
flushes, and compacts. Here **synchronous compaction under the write lock**
(a deliberate design choice — see DESIGN_NOTES → *Concurrency model*) dominates:

| mode | throughput c=64 | write p50 | write max (stall) |
|---|---:|---:|---:|
| `OsBuffered` | 17,342 | 18 µs | 2.4 s |
| `GroupCommit` | 623 | 7.4 ms | 76 s |
| `Always` | 341 | 5.4 ms | 63 s |

median of 3–5 runs; raw [`raw/b1b_fullengine_*`](raw/). The multi-second `max`
is a single write absorbing a full compaction merge on the writer's thread — the
honest cost of the synchronous-compaction simplicity decision, published rather
than hidden. Moving compaction to a background thread is documented future work.

### 3c. fill-seq (sequential keys)

| workload | mode | throughput | note |
|---|---|---:|---|
| fill-seq, 10k, c=8, WAL-bound | `GroupCommit` | 1,407 | vs 1,093 fill-random — sorted inserts, same commit path |
| fill-seq, 50k, c=8 | `OsBuffered` | 56,291 | vs ~38k fill-random c=8 — sequential build is friendlier |

Raw [`raw/b1_fillseq_*`](raw/).

## 4. Reads (B2) and scans (B3)

**Point reads** (cold = post-flush, served from SSTables via bloom + sparse
index, not the memtable), 50k keys, c=8, median of 5:

| metric | value |
|---|---:|
| throughput | **615,188 reads/sec** |
| p50 latency | 11.7 µs |
| p99 latency | 21.7 µs |
| max latency | 256 µs |

Raw [`raw/b2_pointread_c8.txt`](raw/). **Honest caveat:** the 50k-key data set
(~6 MiB) fits entirely in the 92 GiB page cache, and the host has no root to drop
caches, so these are **warm-page-cache** SSTable reads — they measure the bloom +
sparse-index + block-decode CPU path, *not* cold-disk read amplification. A true
cold-disk read number would require a cache-drop this environment can't perform;
that limitation is disclosed rather than papered over.

**Forward scans** (500-key windows over 50k keys, single-threaded), median of 5:

| metric | value |
|---|---:|
| scans/sec | 63 |
| pairs visited/sec | ≈ 31,500 |
| per-scan p50 | 15.6 ms |

Raw [`raw/b3_scan.txt`](raw/). Each scan is a k-way merge across the memtable and
all SSTable tiers, materializing every pair in the window.

**Bloom filter FPR (measured vs theoretical),** from
`src/sstable/bloom.rs::measured_fpr_near_theoretical` (n = 10,000 keys, 10
bits/key, k = 7, 100,000 disjoint probes):

| | value |
|---|---:|
| theoretical `(1 − e^(−kn/m))^k` | 0.0082 (0.82 %) |
| measured | **0.0077 (0.77 %)** |

Measured FPR sits just under theoretical, as expected for a well-mixed
double-hashing filter.

## 5. Crash-consistency counts (feed resume bullet 2)

From `tests/crash.rs` (run under `SimFs`, the deterministic power-loss simulator):

| count | value | source |
|---|---:|---|
| distinct exhaustive crash points (`N`) | **330** | `exhaustive::reports_crash_point_count` |
| fixed seeds per point (spanning drop/torn/bit-flip) | 4 | `SEEDS` |
| durable modes swept (`Always`, `GroupCommit`) | 2 | — |
| **total exhaustive crash executions** | **2,640** | 330 × 4 × 2 |
| property-based random crash schedules | **160** | `schedules::random_schedule_zero_acked_loss` proptest cases |
| named fixed-seed regression sweeps | 3 | `regressions` module (each sweeps every crash point × 4 seeds × 2 modes) |

Every one recovers with **zero acknowledged-write loss** — the invariant asserted
by `verify()`. The process-kill integration test (`tests/process_kill.rs`,
RealFs + SIGKILL) adds one single-kill run and 3 repeated abrupt-process-death
rounds with a minimum-progress assertion. Acknowledged-key counts are reported
at runtime because they are timing-dependent.

## 6. sled baseline — matched durability (wins AND losses)

Same `KvBench` trait, same driver, same histogram; only the engine differs. sled
0.34, matched per the mapping in `src/bin/accretion-bench/kv.rs`. sled is a
mature beta engine with a different architecture (Bw-tree/lock-free log); this is
context, not a contest. **sled wins every matched comparison on this host** —
reported plainly:

| comparison | accretion-db | sled | winner |
|---|---:|---:|---|
| **Durable** (acc `Always` vs sled `insert`+`flush`), fill-random 3k c=1 | 364 w/s (p50 2.73 ms) | **1,070 w/s** (p50 0.93 ms) | **sled (2.9×)** |
| **Buffered** (acc `OsBuffered` vs sled no-flush), fill-random 50k c=1 | 84,937 w/s | **217,719 w/s** | **sled (2.6×)** |
| **Buffered reads**, point-read 50k c=8 | 615,188 r/s (p50 11.7 µs) | **3,686,026 r/s** (p50 1.3 µs) | **sled (6×)** |

Raw: [`raw/sled_*`](raw/), [`raw/acc_*_matched_*`](raw/).

**Why sled wins the durable row, honestly:** sled's `flush()` pays a single
fdatasync (~915 µs p50, right at the disk's `sync_data` floor), while
accretion-db's `Always` includes WAL append/file-open work, one `sync_data`, and
engine locking around each write. accretion-db's answer to the fsync wall is
**`GroupCommit`**, which sled has no API for — so it is reported as accretion's
own headline mode against its own `Always` baseline (§3a), never as a sled
comparison. On the read/buffered rows sled's lock-free architecture and years of
tuning simply beat this teaching-scale engine, and that is the correct thing to
publish.

## 7. Reproduce

```sh
# fsync probe (frames §2)
rustc -O scripts/fsync_probe.rs -o /tmp/fsync_probe && /tmp/fsync_probe --iters 2000

# full matrix (regenerates benchmarks/raw/*, ~30 min)
cargo build --release --features bench-sled --bins
bash scripts/run_matrix.sh

# crash-sweep counts (§5)
cargo test --release --test crash reports_crash_point_count -- --nocapture

# bloom FPR (§4)
cargo test --release --lib measured_fpr_near_theoretical
```

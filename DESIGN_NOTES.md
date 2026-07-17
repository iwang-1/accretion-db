# Design notes

Rationale behind the non-obvious choices in `accretion-db`. The goal is that
every decision here can be defended from first principles — this is the document
to read before whiteboarding the engine.

**Contents:** [the `Storage` seam](#the-storage-seam) ·
[write path & read path](#write-path-and-read-path) ·
[group-commit math](#group-commit-math) ·
[torn-tail truncation](#torn-tail-truncation) ·
[bloom sizing](#bloom-filter-sizing) ·
[size-tiered vs leveled](#size-tiered-vs-leveled-compaction) ·
[manifest atomicity](#manifest-atomicity) ·
[concurrency model](#concurrency-model) ·
[why not mmap/io_uring](#why-not-mmap--io_uring--a-block-cache) ·
[crash-consistency evidence](#crash-consistency-evidence).

## The `Storage` seam

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

## Write path and read path

**Write.** `Db::put`/`delete` funnels into one `write` method that runs in three
phases so the db-level write mutex is held only long enough to *order* the write,
not across the durable wait (this phasing is the fix for BUGS_FOUND #3):

1. *Locked:* claim the next monotonic sequence number and mark the write
   in-flight. Ordering seq under the lock is what keeps the log's logical order
   total even though the durable acks below can complete out of order.
2. *Unlocked:* encode the record (`[seq u64][tag u8][klen u32][key]([vlen u32][value])`,
   see FORMAT.md) and call `wal.append`. This is exactly where concurrent
   `GroupCommit` writers meet and share one leader `fsync`; because the db mutex
   is released, a second writer can enqueue into the same batch.
3. *Re-locked:* apply to the active memtable via `insert_if_newer` (seq-guarded,
   so an older ack landing late cannot clobber a newer value), clear the in-flight
   mark, and — if the memtable crossed its size threshold — freeze it and run the
   synchronous flush/compaction.

The `put` returns only after the WAL commit contract for the configured mode is
met, so *return implies durable* in `Always`/`GroupCommit`. Freeze is atomic: the
active memtable is swapped for a fresh one and pushed (as an `Arc`) onto a frozen
list; a flush drains a frozen memtable into a new tier-0 SSTable, bumps the
manifest version, then releases the covered WAL segments. A flush cannot race a
still-in-flight write: `flush_locked` gates new writers and waits on a `Condvar`
for `in_flight == 0` before `wal.reset()`, so no acked write is dropped from the
log before it reaches the memtable.

**Read.** `Db::get` checks, newest state first: the active memtable, then each
frozen memtable, then SSTables newest-first (youngest tier first, and within a
tier the highest file id first — `Version::tables_newest_first`). The first hit
wins, because newer state shadows older; a tombstone hit returns `None`
immediately (an acked delete must hide any older value below it). Per table the
probe order is: skip if the sought key is outside the table's `[first_key,
last_key]` range; else consult the in-memory **bloom filter** and skip on a
confident absent (no false negatives, so skipping is always safe); else binary-
search the **sparse index** (first key per block) to the one 4 KiB block that
could hold the key, read and CRC-check that block, and scan it. A `scan(range)`
is the same set of sources fed into a k-way **merge iterator** (`src/iter/`) that
yields keys in order, newest-wins per key, dropping tombstoned keys — verified
against a `BTreeMap` reference model.

## Group-commit math

On a disk whose 4 KiB `fdatasync` costs ~878 µs p50 / ~975 µs p99 (measured on
the build host via `scripts/fsync_probe.rs`; the heavier directory fsync that
backs rename durability is ~1.97 ms), a *bare* per-write fdatasync caps
throughput at ~1/0.000878 ≈ **1140 writes/sec regardless of engine quality**.
The engine's own `Always` path is heavier — measured ~2.7 ms in the WAL-bound
c=1 run, which never flushes. Each commit issues one WAL `sync_data`; append and
file-open work plus engine and scheduler overhead account for the rest of the
observed latency. Group commit batches *N* queued writers into one write+fsync,
dividing the barrier cost across them: throughput scales toward *N* × the
per-fsync ceiling while single-write latency rises toward one batch interval.
This throughput/latency tradeoff is why the headline resume number names the
*mode* (`GroupCommit`), not just a raw figure.

**Measured multiplier (build host, `benchmarks/RESULTS.md`).** In the
WAL-commit-bound regime (a memtable large enough that the fill never crosses a
flush, so the commit pipeline is what is measured), fill-random at 64 workers
sustains a median **8,082 writes/sec** in `GroupCommit` versus **276 writes/sec**
in `Always` at the same concurrency — a **~29× group-commit multiplier**, with
same-concurrency p50 latency rising from ~3.7 ms (`Always`) to ~7.5 ms
(`GroupCommit`), exactly the throughput-for-latency trade the math predicts. Not
all 64 writers occupy one batch, and both paths include file, lock, and scheduler
overhead beyond the bare 878 µs `fdatasync`; the comparison is one WAL barrier per
`Always` write versus one shared barrier per group. Once the workload crosses
flush + compaction boundaries, **synchronous
compaction under the write lock** (see *Concurrency model*) becomes the dominant
cost and collapses `GroupCommit` to ~623 writes/sec with multi-second stall
outliers — the honest full-engine number, reported alongside the WAL-bound figure
in RESULTS.md rather than hidden.

## Torn-tail truncation

The WAL is a sequence of length-prefixed, CRC32-framed records. Recovery scans
frames and **truncates at the first frame that is short or fails its CRC**. This
cannot lose acknowledged data: in a durable mode a `put` only acks after its
record is `sync_file`d, so any acked record precedes the torn tail and verifies
cleanly. The toy store in `tests/harness.rs` already demonstrates this rule;
the real WAL adopts it verbatim.

## Bloom filter sizing

Each SSTable carries its own bloom filter (`src/sstable/bloom.rs`), so a point
read can skip a table's data blocks entirely on a confident "absent". The filter
gives no false negatives, so skipping is always safe; it costs an occasional
wasted block read on a false positive.

**Sizing.** For `m` bits, `n` keys, `k` probes, the fraction of bits still `0`
after all inserts is `≈ e^(−kn/m)`, so a lookup for an absent key reports present
with probability `FPR ≈ (1 − e^(−kn/m))^k`. Holding `m/n` fixed, this is
minimised at `k = (m/n) ln 2`, where `FPR ≈ 0.6185^(m/n)`. We size `m` from a
caller-supplied **bits-per-key** (default `10`), round `m` up to a whole byte,
and round `k` to the nearest integer of `(m/n) ln 2` (default `k = 7`). At 10
bits/key the theoretical FPR is `0.6185^10 ≈ 0.0082` (~0.8%).

**Why byte-aligned `m`, not a power of two.** A power-of-two `m` lets probes use
a bit-mask instead of a modulo, but rounding an arbitrary bits-per-key budget up
to the next power of two wastes up to ~2× memory (10 000 → 16 384 bits). For an
in-memory per-table filter that overhead is not worth saving one `mod` per probe,
so `m` is only byte-aligned.

**Double hashing.** Rather than compute `k` independent hashes, we use the
Kirsch–Mitzenmacher construction `g_i(x) = h1(x) + i·h2(x) (mod m)`, deriving
`h1`/`h2` as the low/high 64-bit halves of one `xxh3_128` of the key. `h2` is
reduced to a non-zero residue mod `m` so the step never degenerates to probing
one bit `k` times. Same asymptotic FPR, one hash computation per key.

**Measured vs theoretical.** `bloom::tests::measured_fpr_near_theoretical`
inserts 10 000 keys and probes 100 000 disjoint keys, asserting the empirical FPR
tracks the theoretical formula. On the build host the measured FPR is **0.77 %**
against the **0.82 %** theoretical (10 bits/key, k=7) — just under, as expected
for a well-mixed double-hashing filter (RESULTS.md §4).

## Size-tiered vs leveled compaction

Size-tiered chosen deliberately: **lower write amplification and simpler
invariants**, at the cost of higher space and read amplification. Tier *t*
compacts when it holds ≥ `tier_fanout` tables, merging into one table in tier
*t+1*. Tombstone GC only at the bottom tier — a tombstone may still be masking
live data in a lower tier, so dropping it early would resurrect deleted keys.
Leveled compaction buys RocksDB tighter read/space amp at higher write amp; out
of scope here.

## Manifest atomicity

A version switch writes a new manifest file, `sync_file`s it, atomically
`rename`s it into place, then `sync_dir`s the parent. Without the final
directory fsync the rename is volatile: a crash could leave the manifest name
pointing at the old inode even though the new file's bytes are durable, so a
reader could load a manifest that references files that were meant to be
obsoleted (or miss files that were meant to be installed). Readers pin an
`Arc<Version>` and never observe a half-installed version.

## Concurrency model

A deliberate simplicity choice: a single logical writer (mutex on the write
path), readers via an `RwLock` memtable snapshot plus a pinned `Arc<Version>`.
Readers pinned to an old `Arc<Version>` stay correct while compaction replaces
files underneath them, because a file is only deleted once no `Version`
references it.

**Compaction runs synchronously by design in this version.** Both the flush
path and each compaction pass derive their output table id from
`next_table_id` on the version snapshot they started from and then call
`Manifest::install`, which unconditionally swaps in the new version. Under a
single logical writer this is race-free and keeps the crash-consistency reasoning
tractable: every durable state transition is totally ordered, so the exhaustive
crash sweep and proptest schedules reason about one linear history of manifest
installs. Moving compaction onto a background thread would let two producers
pick the same `next_table_id` (colliding output files) and let a compaction
`install` clobber a concurrently flushed tier-0 table (acked-write loss) — i.e.
it demands turning `install` into a transactional compare-and-apply and then
re-establishing the crash invariant against interleaved installs. That rework is
deliberately deferred: **background/concurrent compaction is future work.** The
synchronous design costs write-path latency spikes when a large tier compacts,
but buys a correctness story that is simple to inspect and audit, which is the
point of this project.

## Why not mmap / io_uring / a block cache

Out of scope by design: the engine leans on the OS page cache instead of a
custom block cache (documented decision, revisited only if benchmarks demand
it), and avoids io_uring/O_DIRECT to keep the code pure-Rust and portable. The
point of the project is crash-consistency evidence, not squeezing the I/O path.

## Crash-consistency evidence

The recovery invariant, enforced everywhere: **zero acknowledged-write loss**.
Every `put`/`delete` that *returned* under a durable mode (`Always` /
`GroupCommit`) is present with its exact value after recovery; an in-flight op
that never returned is either fully applied or fully absent; there are no phantom
keys, no corruption, and `scan` agrees with `get`. Three independent layers test
it (`tests/crash.rs`, `tests/process_kill.rs`):

1. **Exhaustive deterministic sweep.** A canonical mixed workload (puts,
   overwrites, deletes over a colliding key set, sized against a 128-byte memtable
   to force multiple flushes and at least one compaction) is run once to count
   `N = 330` mutating storage ops. Then for every crash point `i in 1..=N`, across
   four fixed RNG seeds spanning the simulator's three tail outcomes (drop,
   torn-truncate, and bit-flip) and both durable modes, the engine crashes after op `i`,
   reopened, and verified against a model of the acknowledged prefix — 2 640 total
   crash executions. This is the headline distinct-crash-point figure.

2. **Property-based random schedules.** proptest generates random op sequences ×
   random crash fraction × random durable mode and model-verifies after recovery,
   shrinking any failure to a minimal counterexample. A fixed-seed regression
   corpus (`mod regressions`) pins the highest-risk shapes — resurrection across a
   flush, a tombstone shadowing a lower tier through a compaction, a delete-heavy
   schedule — sweeping every crash point of each so CI re-runs them deterministically.

3. **Real process kill.** `accretion-crashtest` writes to a real `RealFs`
   database in `Always` mode and prints each key index only after its `put`
   returns durable. The test sends `SIGKILL`, reopens through real filesystem
   calls, and confirms every acknowledged key survived. The kernel and page
   cache remain alive, so this exercises abrupt process death and reopen
   behavior, not hardware power loss or torn-write behavior.

**Why the SimFs `rename`-durability subtlety matters (BUGS_FOUND.md #2).** The
manifest's atomic switch and the WAL's segment-truncate both `rename` a synced
temp file *over an existing durable name*. Real POSIX guarantees that once the
parent directory is `fsync`'d the destination durably resolves to the new inode,
whether or not the name previously existed. SimFs originally refreshed a file's
durable image only when it was not already durably present, so it under-modeled
this case and reported false acknowledged-write loss on a correct engine. The
final model assigns each file generation an inode identity and stores separate
live and durable inode bindings per path. `rename` moves the live inode identity;
`sync_dir` commits the namespace binding; a crash before it restores the old
binding. The same representation prevents delete/recreate from resurrecting old
bytes or overwriting the rollback generation when only the new file is synced.
This is evidence within the simulator's documented fault model, not an exhaustive
model of every filesystem outcome.

**Harness validation (positive control).** To show the sweep is not vacuously
green, the `Always` fsync-before-ack was deliberately removed once: the sweep
failed instantly at crash point 1 with acknowledged-write loss, then the fsync was
restored. Documented in `BUGS_FOUND.md` as validation, never as a shipped bug.

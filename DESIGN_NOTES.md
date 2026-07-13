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

On a disk whose 4 KiB `fdatasync` costs ~878 µs p50 / ~975 µs p99 (measured on
the build host via `scripts/fsync_probe.rs`; the heavier directory fsync that
backs rename durability is ~1.97 ms), a *bare* per-write fdatasync caps
throughput at ~1/0.000878 ≈ **1140 writes/sec regardless of engine quality**.
The engine's own `Always` per-put is heavier — measured ~2.7 ms — because each
durable put fsyncs the WAL *plus* amortized SSTable and manifest syncs, roughly
3× a bare fdatasync. Group commit batches *N* queued writers into a single
write+fsync, dividing the fsync cost across them: throughput scales toward
*N* × the per-fsync ceiling while single-write latency rises toward one batch
interval. This throughput/latency tradeoff is why the headline resume number
names the *mode* (`GroupCommit`), not just a raw figure.
`{MEASURE: Nx multiplier}` to be filled from `benchmarks/RESULTS.md`.

## Torn-tail truncation (stub — WAL stage)

The WAL is a sequence of length-prefixed, CRC32-framed records. Recovery scans
frames and **truncates at the first frame that is short or fails its CRC**. This
cannot lose acknowledged data: in a durable mode a `put` only acks after its
record is `sync_file`d, so any acked record precedes the torn tail and verifies
cleanly. The toy store in `tests/harness.rs` already demonstrates this rule;
the real WAL adopts it verbatim.

## Bloom filter sizing (built, S1c)

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
tracks the theoretical formula. The exact host-run measured figure is published
in the README once the bench/measure stage runs.

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
path), readers via an `RwLock` memtable snapshot plus a pinned `Arc<Version>`.
Readers pinned to an old `Arc<Version>` stay correct while compaction replaces
files underneath them, because a file is only deleted once no `Version`
references it.

**Compaction runs synchronously by design in this version.** Both the flush
path and each compaction pass derive their output table id from
`next_table_id` on the version snapshot they started from and then call
`Manifest::install`, which unconditionally swaps in the new version. Under a
single logical writer this is race-free and keeps the crash-consistency proof
tractable: every durable state transition is totally ordered, so the exhaustive
crash sweep and proptest schedules reason about one linear history of manifest
installs. Moving compaction onto a background thread would let two producers
pick the same `next_table_id` (colliding output files) and let a compaction
`install` clobber a concurrently flushed tier-0 table (acked-write loss) — i.e.
it demands turning `install` into a transactional compare-and-apply and then
re-establishing the crash invariant against interleaved installs. That rework is
deliberately deferred: **background/concurrent compaction is future work.** The
synchronous design costs write-path latency spikes when a large tier compacts,
but buys a correctness story that is simple to prove and audit, which is the
point of this project.

## Why not mmap / io_uring / a block cache (stub)

Out of scope by design: the engine leans on the OS page cache instead of a
custom block cache (documented decision, revisited only if benchmarks demand
it), and avoids io_uring/O_DIRECT to keep the code pure-Rust and portable. The
point of the project is crash-consistency proof, not squeezing the I/O path.

## Crash-consistency proof (built, S3)

The recovery invariant, enforced everywhere: **zero acknowledged-write loss**.
Every `put`/`delete` that *returned* under a durable mode (`Always` /
`GroupCommit`) is present with its exact value after recovery; an in-flight op
that never returned is either fully applied or fully absent; there are no phantom
keys, no corruption, and `scan` agrees with `get`. Three independent layers prove
it (`tests/crash.rs`, `tests/process_kill.rs`):

1. **Exhaustive deterministic sweep.** A canonical mixed workload (puts,
   overwrites, deletes over a colliding key set, sized against a 128-byte memtable
   to force multiple flushes and at least one compaction) is run once to count
   `N = 330` mutating storage ops. Then for every crash point `i in 1..=N`, across
   four RNG seeds (which steer the tear mode — drop / torn-truncate / bit-flip on
   the crashing op) and both durable modes, the engine is crashed after op `i`,
   reopened, and verified against a model of the acknowledged prefix — 2 640 total
   crash executions. This is the distinct-crash-point figure the résumé cites.

2. **Property-based random schedules.** proptest generates random op sequences ×
   random crash fraction × random durable mode and model-verifies after recovery,
   shrinking any failure to a minimal counterexample. A fixed-seed regression
   corpus (`mod regressions`) pins the highest-risk shapes — resurrection across a
   flush, a tombstone shadowing a lower tier through a compaction, a delete-heavy
   schedule — sweeping every crash point of each so CI re-runs them deterministically.

3. **Real process kill.** `accretion-crashtest` writes to a real `RealFs` database
   in `Always` mode and prints each key index only after its `put` returns durable;
   the test `SIGKILL`s it mid-load (uncatchable, no unwinding, no destructors — a
   true power-loss analogue), reopens on the real kernel, and confirms every acked
   key survived. This exercises real `fsync`, real directory-entry durability, and
   real torn-tail truncation, closing the gap between the simulator and hardware.

**Why the SimFs `rename`-durability subtlety matters (BUGS_FOUND.md #2).** The
manifest's atomic switch and the WAL's segment-truncate both `rename` a synced
temp file *over an existing durable name*. Real POSIX guarantees that once the
parent directory is `fsync`'d the destination durably resolves to the new inode,
whether or not the name previously existed. SimFs originally refreshed a file's
durable image only when it was not already durably present, so it under-modeled
this case and reported false acknowledged-write loss on a correct engine. The fix
stages the source's synced image on `rename` and commits it into the destination's
durable bytes on the covering `sync_dir`; a crash before that `sync_dir` discards
it. This is the fault-model boundary the whole proof rests on, so it is modeled
exactly, not approximately.

**Harness validation (positive control).** To show the sweep is not vacuously
green, the `Always` fsync-before-ack was deliberately removed once: the sweep
failed instantly at crash point 1 with acknowledged-write loss, then the fsync was
restored. Documented in `BUGS_FOUND.md` as validation, never as a shipped bug.

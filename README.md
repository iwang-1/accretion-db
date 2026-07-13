# accretion-db

[![CI](https://github.com/iwang-1/accretion-db/actions/workflows/ci.yml/badge.svg)](https://github.com/iwang-1/accretion-db/actions/workflows/ci.yml)
[![clippy: -D warnings](https://img.shields.io/badge/clippy-D_warnings-brightgreen.svg)](.github/workflows/ci.yml)
[![unsafe: forbidden](https://img.shields.io/badge/unsafe-forbidden-brightgreen.svg)](src/lib.rs)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

An embeddable **LSM-tree storage engine** in Rust (zero `unsafe`) — CRC-framed
write-ahead log with **group commit**, memtable with atomic freeze/flush,
block-based SSTables with bloom filters and sparse indexes, size-tiered
compaction, crash recovery. Its headline product is not speed but *proof*: a
fault-injecting storage layer that simulates power loss at **every write and
fsync boundary** and shows the engine recovers **330** deterministic crash
points (× 4 tear modes × 2 durable modes = **2,640 executions**) plus **160**
property-based crash schedules with **zero acknowledged-write loss** — and it is
benchmarked honestly against [sled](https://github.com/spacejam/sled),
publishing the comparisons sled *wins*.

## Why this exists

Storage engines are easy to make fast and hard to make *crash-safe*. Anyone can
buffer writes and quote a big throughput number; the interesting engineering is
guaranteeing that a `put` which **returned** is still there after the power is
cut mid-`fsync`. So the product of this repository is the proof, and the design
is **harness-first**: the fault-injection layer — a deterministic, seeded
page-cache simulator (`SimFs`) that drops, tears, and bit-flips unsynced bytes
on `crash()` — was built and reviewed *before* the engine it judges, and every
component grew up running under it. That is what lets
[BUGS_FOUND.md](BUGS_FOUND.md) fill organically instead of being decorated after
the fact.

Every number in this README traces to a committed artifact
([benchmarks/RESULTS.md](benchmarks/RESULTS.md) or a named test); all were
produced on the disclosed build host. Nothing here is hand-tuned or aspirational.

## Architecture

```
 WRITE PATH                                              READ PATH
 ──────────                                              ─────────
   put(k, v)                                               get(k)
      │                                                       │
      ▼                                                       ▼
 ┌──────────┐  ack per durability mode              ┌──────────────────┐
 │   WAL    │  Always      = fsync per commit        │  active memtable │
 │ CRC-     │  GroupCommit = one fsync / batch       └────────┬─────────┘
 │ framed   │  OsBuffered  = no fsync (unsafe)          miss   │
 └────┬─────┘                                                  ▼
      │ append                                        ┌──────────────────┐
      ▼                                                │ frozen memtables │
 ┌──────────────┐  freeze at size threshold            └────────┬─────────┘
 │ active        │─────────────┐                          miss   │
 │ memtable      │             ▼                                 ▼
 │ (BTreeMap)    │      ┌──────────────┐             ┌──────────────────────┐
 └──────────────┘      │ frozen (Arc)  │  flush       │  SSTable tiers,       │
                       └──────┬────────┘  ───────►    │  newest-first:        │
                              │                        │   tier 0  [t][t][t]   │
                              ▼                        │   tier 1  [ merged ]  │
                       ┌──────────────┐                │   ...                 │
                       │  SSTable      │               └───────────┬───────────┘
                       │  (tier 0)     │                            │ per table:
                       └──────┬────────┘                    ┌───────▼────────┐
                              │ manifest version bump         │ bloom filter   │ absent? skip
                              ▼  (tmp+fsync+rename+dir-fsync)  ├────────────────┤
                       ┌──────────────┐                       │ sparse index   │ locate block
                       │ size-tiered   │  ≥ fanout tables      ├────────────────┤
                       │ compaction    │  merge tier t → t+1   │ 4 KiB block    │ in-block scan
                       └──────────────┘                       └────────────────┘
```

A `put` is durable (per the configured mode) the instant it returns; the
memtable insert, the size-triggered freeze, the flush to a tier-0 SSTable, and
the manifest version bump all happen behind that contract. A `get` walks the
active memtable, the frozen memtables, then the SSTable tiers newest-first — a
bloom filter gates each table probe and a sparse index locates the one 4 KiB
block that could hold the key. Full walkthrough in
[DESIGN_NOTES.md](DESIGN_NOTES.md); on-disk byte layouts in [FORMAT.md](FORMAT.md).

## Quickstart

`accretion-db` is a library crate — embed it, no server, no daemon:

```rust
use accretion_db::{Db, Options, Durability};

# fn main() -> Result<(), accretion_db::DbError> {
// GroupCommit: concurrent writers share one fsync per batch (the headline mode).
let db = Db::open("/tmp/mydb", Options {
    durability: Durability::GroupCommit,
    ..Default::default()
})?;

db.put(b"key", b"value")?;                 // returns only once durable
assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));

db.delete(b"key")?;
assert_eq!(db.get(b"key")?, None);

// Range scan yields sorted (key, value) pairs, tombstone-aware:
for (k, v) in db.scan(b"a".to_vec()..b"z".to_vec())? {
    println!("{:?} => {:?}", k, v);
}
# Ok(())
# }
```

Run the crash sweep and the benchmarks yourself:

```sh
# The crown jewel: every acked write survives every crash point (~3s).
cargo test --release --test crash

# Distinct-crash-point count that feeds the résumé (prints N=330 …):
cargo test --release --test crash reports_crash_point_count -- --nocapture

# Full benchmark matrix (regenerates benchmarks/raw/*, ~30 min):
cargo build --release --features bench-sled --bins
bash scripts/run_matrix.sh
```

## The durability-mode table

The story starts with the disk. `scripts/fsync_probe.rs` measures this host's
4 KiB durability calls: **`fdatasync` p50 = 878 µs** (what the WAL commit path
pays), with the heavier directory fsync behind rename durability at ~1.97 ms. At
878 µs a *bare* fsync-per-write engine is capped at **≈ 1,140 durable
writes/sec regardless of engine quality** — the disk, not the code, is the
ceiling. Group commit's whole job is to amortize one `fdatasync` across many
queued writers.

`fill-random`, 16-byte keys / 100-byte values, closed-loop driver, **WAL-commit-bound**
regime (64 MiB memtable, so the fill never flushes — this isolates the commit
pipeline). Median of 5 runs:

| mode | c=1 | c=8 | c=64 | write p50 @ c=64 |
|---|---:|---:|---:|---:|
| `Always` (fsync per put) | 369 | 348 | 276 | 3.6 ms |
| `GroupCommit` (batched fsync) | 274 | 1,093 | **8,082** | 7.4 ms |
| `OsBuffered` (no fsync — unsafe) | 60,329 | 38,232 | 34,361 | 20 µs |

**Group commit buys a ~29× multiplier** (8,082 / 276 at c=64) by trading
single-write latency (2.7 ms → 7.4 ms p50) for batched fsync amortization —
exactly the throughput-for-latency trade the math predicts. It lands below the
raw batch size because `Always` on this engine already pays ~3 fsyncs per put
(WAL + amortized manifest + directory syncs), so its floor is ~2.7 ms, not the
bare 878 µs. Full per-cell numbers, commands, and the raw outputs are in
[RESULTS.md](benchmarks/RESULTS.md) §3.

**Honest full-engine caveat.** The ~29× is the *commit-pipeline* number. Once a
workload crosses flush + compaction boundaries, **synchronous compaction under
the write lock** (a deliberate simplicity choice — see below) dominates and
collapses `GroupCommit` to ~623 writes/sec with multi-second stall outliers. That
figure is published side-by-side in RESULTS.md §3b, not hidden: the WAL-bound
number proves the commit-pipeline design; the full-engine number is gated by the
deliberately-simple compaction path.

Point reads (warm page cache — the host has no root to drop caches, disclosed in
RESULTS.md §4): **615,188 reads/sec, p50 11.7 µs, p99 21.7 µs**. Bloom filter
FPR: **0.77 % measured** vs 0.82 % theoretical (10 bits/key, k=7).

## Crash-consistency harness

The invariant, enforced everywhere: **zero acknowledged-write loss**. Every
`put`/`delete` that *returned* under a durable mode (`Always` / `GroupCommit`)
is present with its exact value after recovery; an in-flight op is either fully
applied or fully absent; there are no phantom keys, acked deletes hold, the
manifest references only checksum-valid files that exist, and the WAL tail
truncates cleanly. Three independent layers prove it:

| layer | what it does | count |
|---|---|---:|
| Exhaustive deterministic sweep | Run a canonical mixed workload once to count `N` mutating storage ops; for each `i in 1..=N`, fresh `SimFs`, crash after op `i` (× 4 tear-mode seeds × 2 durable modes), reopen, verify against the acked-prefix model. | **330 points → 2,640 executions** |
| Property-based schedules | proptest generates random op sequences × crash indices × durability modes, shrinking failures to minimal counterexamples; 3 named fixed-seed sweeps pin the highest-risk shapes as regressions. | **160 schedules + 3 regressions** |
| Real process kill | `accretion-crashtest` writes to a real `RealFs` DB in `Always`, prints each key only once its `put` returns durable; the parent `SIGKILL`s it mid-load (uncatchable — a true power-loss analogue), reopens on the real kernel, confirms every acked key survived, across 3 repeated rounds. | **131 keys / kill; 3 rounds** |

### What `SimFs` models — and what it does not

**Modelled:** loss of any byte range written but not yet `sync_file`d; a *torn*
last unsynced append (dropped, truncated at a random byte boundary, or bit-flipped
inside the unsynced region — drives the CRC path); a *volatile*
rename/create/delete that reverts to the last `sync_dir`-durable directory image;
deterministic, seeded replay so a failing schedule reproduces byte-for-byte.

**Not modelled** (honest boundaries): cross-file sector reordering, sub-byte
partial-sector atomicity, or media decay of already-durable data. The engine is
only ever permitted to depend on the guarantees this model makes.

See [BUGS_FOUND.md](BUGS_FOUND.md) for the organic crash-bug journal — including
a tombstone-resurrection bug the BTreeMap-model property test shrank to a
four-op counterexample, a `SimFs` rename-durability fidelity fix, a group-commit
locking bug the throughput harness surfaced, and a labelled positive-control
that deletes one `fsync` to prove the sweep actually catches loss.

## Benchmarks vs sled — wins *and* losses

sled 0.34 runs behind the same `KvBench` trait, same driver, same histogram, at
**matched durability settings** documented in `src/bin/accretion-bench/kv.rs`.
sled is a mature beta engine with a different (lock-free Bw-tree/log)
architecture; this is context, not a contest — and on this host **sled wins
every matched comparison**, reported plainly:

| comparison | accretion-db | sled | winner |
|---|---:|---:|---|
| Durable (acc `Always` vs sled `insert`+`flush`) | 364 w/s | **1,070 w/s** | **sled (2.9×)** |
| Buffered (acc `OsBuffered` vs sled no-flush) | 84,937 w/s | **217,719 w/s** | **sled (2.6×)** |
| Buffered point reads | 615,188 r/s | **3,686,026 r/s** | **sled (6×)** |

Why, honestly: sled's `flush()` pays a single fdatasync right at the disk floor,
while accretion's `Always` pays ~3 fsyncs per put; and sled's lock-free
architecture and years of tuning simply beat this teaching-scale engine on reads.
accretion-db's answer to the fsync wall is `GroupCommit`, which sled has no API
for — so it is reported as accretion's own headline mode against its own `Always`
baseline, never dressed up as a sled win. Methodology, matched configs, and the
full table are in [RESULTS.md](benchmarks/RESULTS.md) §6.

## Concurrency model (a deliberate simplicity decision)

A **single logical writer** (a mutex on the write path) totally orders every
durable manifest install; readers take an `RwLock` memtable snapshot plus a
pinned `Arc<Version>`. A reader holding an old `Arc<Version>` stays correct while
compaction replaces files underneath it, because a table file is deleted only
once no live `Version` references it (tracked by `Arc` strong count). Flush and
compaction run on **exactly one path** — synchronously, on the writer's thread.

That single-writer, totally-ordered history is *why* the crash proof is
tractable: the exhaustive sweep and proptest schedules reason about one linear
sequence of manifest installs. Moving compaction to a background thread would
demand turning the manifest swap into a transactional compare-and-apply and
re-establishing the crash invariant against interleaved installs — real work,
deliberately deferred. The defense is written up in
[DESIGN_NOTES.md](DESIGN_NOTES.md) → *Concurrency model*.

## Limitations

Stated plainly, each on purpose:

- **Synchronous compaction.** When a tier crosses its fanout, the triggering
  write absorbs the full merge latency (the multi-second stalls in RESULTS.md
  §3b). Bought a simple, auditable crash proof; background compaction is future
  work.
- **Size-tiered only.** Lower write amplification and simpler invariants, at the
  cost of higher space/read amplification — the honest tiered-vs-leveled tradeoff
  (DESIGN_NOTES.md). No leveled compaction.
- **No transactions, MVCC, or column families.** A single-key durable KV store
  with range scans; no multi-key atomicity beyond a single `put`.
- **No block cache / compression / `mmap` / `io_uring`.** Leans on the OS page
  cache by design; the point is the crash proof, not squeezing the I/O path.
- **Single-host, single-process.** No network protocol, no multi-writer
  coordination. All benchmark numbers are single-host; the read numbers are
  warm-page-cache (no root to drop caches on the build host).

## Documentation

- [DESIGN_NOTES.md](DESIGN_NOTES.md) — interview-grade defense of every
  non-obvious choice: write/read path, group-commit math, torn-tail truncation,
  bloom sizing, tiered-vs-leveled, manifest atomicity, the concurrency model, and
  the crash proof.
- [FORMAT.md](FORMAT.md) — byte-level on-disk layout of the WAL, SSTable, and
  manifest.
- [BUGS_FOUND.md](BUGS_FOUND.md) — the organic crash-bug journal.
- [benchmarks/RESULTS.md](benchmarks/RESULTS.md) — host + fsync disclosure, every
  per-cell command, raw outputs, and the sled comparison.

## License

MIT — see [LICENSE](LICENSE). © Ivan Wang
(`59074138+iwang-1@users.noreply.github.com`).

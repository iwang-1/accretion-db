# accretion-db

> An embeddable LSM-tree storage engine in Rust — crash consistency proven by
> simulated-power-loss fault injection at every write and fsync boundary,
> benchmarked honestly against sled.

![status](https://img.shields.io/badge/status-under%20construction-orange)
![unsafe](https://img.shields.io/badge/unsafe-forbidden-brightgreen)
![license](https://img.shields.io/badge/license-MIT-blue)

---

## Status

The full engine is built and measured: the [`Storage`](src/storage/mod.rs) seam,
the [`SimFs`](src/storage/sim.rs) deterministic power-loss simulator, the crash
harness ([`src/testkit`](src/testkit/mod.rs)), and the LSM machinery on top of it
(WAL + group commit, memtable freeze/flush, block SSTables with bloom filters and
sparse indexes, size-tiered compaction, crash recovery). Remaining work is
portfolio-voice documentation polish and a final adversarial claim review.

The design is **harness-first** on purpose. Storage engines are easy to make
fast and hard to make *crash-safe*: the product of this repository is the proof
of crash safety, so the fault-injection layer was built and tested before the
engine it judges. Every number in this README traces to
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md), generated on the disclosed
build host — no unmeasured figures appear here. (The `{MEASURE: …}` convention
below marks where host measurements are substituted in.)

## What is here now

| Piece | File | What it does |
|---|---|---|
| `Storage` trait | [`src/storage/mod.rs`](src/storage/mod.rs) | The narrow, object-safe seam (`Arc<dyn Storage>`) the whole engine is written against: path-addressed files with explicit `sync_file` / `sync_dir` durability barriers. |
| `RealFs` | [`src/storage/real.rs`](src/storage/real.rs) | Production backend over `std::fs` — `fdatasync` for files, directory-handle fsync for rename durability. |
| `SimFs` | [`src/storage/sim.rs`](src/storage/sim.rs) | Deterministic, seeded page-cache model: buffered-until-synced bytes, volatile-until-dir-synced renames, and a `crash()` that drops/tears/bit-flips the last unsynced append. |
| Crash harness | [`src/testkit/mod.rs`](src/testkit/mod.rs) | Runs a workload closure against `SimFs`, crashes at op *N*, reopens, and hands the recovered store to a verifier closure. Exhaustive `crash_sweep` over `1..=N`. |

## Crash-consistency harness: the fault model

`SimFs` models power loss precisely and states its own limits.

**Modelled:**
- Loss of any byte range written but not yet `sync_file`d.
- A **torn** last unsynced append: dropped entirely, truncated at a random byte
  boundary, or bit-flipped inside the unsynced region (drives the CRC path).
- A **volatile rename/create/delete**: reverts to the last `sync_dir`-durable
  directory image.
- Deterministic, seeded replay — a failing crash schedule reproduces
  byte-for-byte.

**Not modelled** (honest boundaries): cross-file sector reordering,
sub-byte partial-sector atomicity, or media decay of already-durable data. The
engine is only ever permitted to depend on the guarantees this model makes.

## Roadmap (build stages)

- S0 — scaffold + frozen `Storage`/`SimFs` + crash harness. ✓
- S1: WAL + commit pipeline; SSTable + bloom; memtable + merge iterator. ✓
- S2: manifest/versioning + size-tiered compaction + full engine assembly. ✓
- S3: exhaustive crash sweep + proptest schedules + process-kill binary. ✓
- S4: compaction concurrency evaluation — kept synchronous by design (see Limitations). ✓
- S5: benchmarks vs sled + CI hardening. ✓
- **S6: full benchmark matrix on the build host → [RESULTS.md](benchmarks/RESULTS.md).** ← *you are here*
- S7: portfolio-voice docs polish + adversarial claim review.

## Verified claims (filled at the measurement stage)

- Group-commit durable writes/sec: **8,082/s** at c=64 (WAL-commit-bound), a
  **~29×** multiplier over per-write `fsync` (276/s) on the measured 878 µs-fsync
  disk. Full-engine (crossing compaction) it is 623/s — both published in
  [RESULTS.md](benchmarks/RESULTS.md) §3.
- Warm point reads/sec @ p99: **615,188/s** @ **21.7 µs** (post-flush, served via
  bloom + sparse index; warm-page-cache caveat disclosed in RESULTS.md §4).
- Distinct exhaustive crash points swept: **330** (× 4 tear-mode seeds × 2 durable
  modes = 2,640 crash executions).
- Property-based crash schedules recovered: **160** (plus 3 named fixed-seed
  regression sweeps), zero acknowledged-write loss.
- Bloom filter measured vs theoretical FPR: **0.77 %** measured vs **0.82 %**
  theoretical (10 bits/key, k=7).

Numbers come from `benchmarks/RESULTS.md`, generated on the build host with
hardware and fsync-probe disclosure. See [DESIGN_NOTES.md](DESIGN_NOTES.md) for
the architecture rationale, [FORMAT.md](FORMAT.md) for on-disk layouts, and
[BUGS_FOUND.md](BUGS_FOUND.md) for the crash-bug journal.

## Limitations (by design)

- **Compaction runs synchronously**, on the writer's thread, under the write
  lock. When a tier crosses its fanout the triggering write absorbs the merge
  latency. This is deliberate: a single logical writer totally orders every
  durable manifest install, which is what keeps the crash-consistency proof
  simple to audit. Background/concurrent compaction is **future work** — it
  would require turning the manifest swap into a transactional
  compare-and-apply (to avoid table-id collisions and clobbering a concurrently
  flushed table) and re-establishing the crash invariant against interleaved
  installs. See [DESIGN_NOTES.md](DESIGN_NOTES.md) → *Concurrency model*.
- Single-process embeddable store: no network protocol, no multi-writer
  coordination.

## License

MIT. See [LICENSE](LICENSE).

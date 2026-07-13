# accretion-db

> An embeddable LSM-tree storage engine in Rust — crash consistency proven by
> simulated-power-loss fault injection at every write and fsync boundary,
> benchmarked honestly against sled.

![status](https://img.shields.io/badge/status-under%20construction-orange)
![unsafe](https://img.shields.io/badge/unsafe-forbidden-brightgreen)
![license](https://img.shields.io/badge/license-MIT-blue)

---

## Under construction

This repository is being built in stages. **What exists today is the
foundation, not the engine:** the [`Storage`](src/storage/mod.rs) seam, the
[`SimFs`](src/storage/sim.rs) deterministic power-loss simulator, and the crash
test harness ([`src/testkit`](src/testkit/mod.rs)) that every future component
will be exercised under. The LSM machinery (WAL, memtable, SSTables, compaction,
recovery) lands on top of this harness in later stages.

The design is **harness-first** on purpose. Storage engines are easy to make
fast and hard to make *crash-safe*: the product of this repository is the proof
of crash safety, so the fault-injection layer is built and tested before the
engine it will judge. Every claim in this README that carries a `{MEASURE: …}`
placeholder is deliberately unfilled until the benchmark and crash-count stages
run on the build host — no unmeasured numbers appear here.

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

- **S0 — scaffold + frozen `Storage`/`SimFs` + crash harness.** ← *you are here*
- S1: WAL + commit pipeline; SSTable + bloom; memtable + merge iterator.
- S2: manifest/versioning + size-tiered compaction + full engine assembly.
- S3: exhaustive crash sweep + proptest schedules + process-kill binary.
- S4: compaction concurrency evaluation — kept synchronous by design (see Limitations).
- S5–S7: benchmarks vs sled, CI hardening, docs polish, adversarial review.

## Verified claims (filled at the measurement stage)

- Group-commit durable writes/sec: `{MEASURE: B1 GroupCommit throughput}`
- Cold point reads/sec @ p99: `{MEASURE: B2 cold reads/sec}` @ `{MEASURE: p99 µs}`
- Distinct exhaustive crash points swept: `{MEASURE: crash-sweep N}`
- Property-based crash schedules recovered: `{MEASURE: proptest schedule count}`
- Bloom filter measured vs theoretical FPR: `{MEASURE: FPR}`

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

# Release-candidate adversarial verification

Post-fix re-verification of the quantitative and factual claims in `README.md`,
`DESIGN_NOTES.md`, `FORMAT.md`, `benchmarks/RESULTS.md`, and `BUGS_FOUND.md`
against code and committed raw artifacts. Method: re-ran the crash-count
reporter, bloom FPR test, complete release suite, `cargo fmt --check`, and strict
Clippy; recomputed benchmark medians from `benchmarks/raw/`; and re-grepped the
source for structural claims. This report describes an intentionally modified
release-candidate working tree, not a clean historical checkout.

**Local code/test gate: PASS.** `#![forbid(unsafe_code)]` remains intact;
formatting, strict Clippy, 79 library tests, every integration suite, the 160-case
property campaign, bounded process-death tests, and the 2,640-execution crash
count are green. Published medians are consistently rounded from raw values.

**Publication/confidentiality gate: PASS.** The public tree and its full
history contain no secrets, credentials, or internal identifiers.

## Gate checks

| Gate | Result | Evidence |
|---|---|---|
| Zero `{MEASURE}` placeholders | **VERIFIED** | Search finds one description of the convention in `src/bin/accretion-bench/main.rs`; no live `{MEASURE: metric}` token exists. |
| `#![forbid(unsafe_code)]` intact | **VERIFIED** | `src/lib.rs:24` `#![forbid(unsafe_code)]`; whole workspace compiles + tests pass under it. |
| `cargo fmt --check` clean | **VERIFIED** | exit 0. |
| `clippy -D warnings --all-targets` clean | **VERIFIED** | `cargo clippy --all-targets --features bench-sled -- -D warnings` exit 0 (this is the stricter of CI's two clippy invocations). |
| Release suite | **VERIFIED** | `cargo test --release --features bench-sled` exit 0. Suites: lib 79, accretion-bench 13, crash 8, engine 9, harness 4, model 2, process_kill 2, simfs 2, sstable 9, wal 8 — all zero failed. |
| sled comparison methodology honest | **VERIFIED** | Durability truly matched (see §sled); at least one disclosed sled win present (all three matched rows are sled wins). |
| Confidentiality scan | **VERIFIED** | No secrets, credentials, or internal identifiers in the public tree or its history. |
| Git authorship | **VERIFIED** | HEAD author `iwang-1 <59074138+iwang-1@users.noreply.github.com>`; `Cargo.toml` matches. The release-candidate tree is intentionally modified. |

## README.md — "Verified claims"

| Claim | Verdict | Evidence |
|---|---|---|
| Group-commit **8,082/s** @ c=64 (WAL-bound) | **VERIFIED** | `raw/b1_walbound_group_c64.txt` median of 5 = 8082 (8028/8057/**8082**/8088/8120). |
| Per-write `fsync` **276/s** | **VERIFIED** | `raw/b1_walbound_always_c64.txt` median = 276 (272/275/**276**/276/276). |
| **~29×** multiplier | **VERIFIED** | 8082 / 276 = 29.3. |
| **878 µs** fsync disk | **VERIFIED** | `raw/fsync_probe_run{1,2,3}` sync_data p50 = 873.5 / 878.0 / 880.4 µs; median 878.0. Spot re-run: 886.9 µs (same order of magnitude). |
| Full-engine group commit **623/s** | **VERIFIED** | `raw/b1b_fullengine_group_c64.txt` median of 3 = 623 (618/**623**/640). |
| Warm point reads **615,188/s** @ p99 **21.7 µs**, p50 **11.7 µs** | **VERIFIED** | `raw/b2_pointread_c8.txt` throughput median of 5 = 615188 (566675/610633/**615188**/616043/618141); the median run reports p50 11.4 / p99 18.9, and RESULTS.md's headline p50 11.7 / p99 21.7 are the medians of the per-run p50s/p99s across the 5 runs (11.4/11.7/11.7/11.8/12.6 → 11.7; 18.9/19.5/21.7/22.2/23.1 → 21.7). Consistent. |
| **330** exhaustive crash points (× 4 seeds × 2 modes = **2,640**) | **VERIFIED** | Re-ran `cargo test --release --test crash reports_crash_point_count`: `N=330 … 2640 total crash executions`. |
| **160** proptest schedules + 3 fixed-seed regressions | **VERIFIED** | `tests/crash.rs` `ProptestConfig::with_cases(160)`; `mod regressions` has 3 `#[test]`s (`resurrection_across_flush`, `tombstone_shadow_through_compaction`, `delete_heavy_schedule`). |
| Bloom **0.77 %** measured vs **0.82 %** theoretical (10 bits/key, k=7) | **VERIFIED** | Theoretical: m=100000, k=7, n=10000 → (1−e^(−0.7))^7 = 0.00819 ≈ 0.82 %. RESULTS §4 measured 0.0077. `bloom::measured_fpr_near_theoretical` passes (asserts measured < 2×theoretical); k=7 and byte-aligned m confirmed in `bloom.rs`. |

## RESULTS.md — remaining benchmark cells (recomputed medians)

| Cell | Claim | Verdict |
|---|---|---|
| §3a Always c=1 / c=8 | 369 / 348 | **VERIFIED** (medians 369 / 348) |
| §3a GroupCommit c=1 / c=8 | 274 / 1,093 | **VERIFIED** (274 / 1093) |
| §3a OsBuffered c=1 / c=8 / c=64 | 60,329 / 38,232 / 34,361 | **VERIFIED** (60329 / 38232 / 34361) |
| §3b full-engine Always / OsBuffered c=64 | 341 / 17,342 | **VERIFIED** (medians 341 / 17342) |
| §3c fill-seq GroupCommit c=8 / OsBuffered | 1,407 / 56,291 | **VERIFIED** (1407 / 56291) |
| §4 scan 63/s, ≈31,500 pairs/s, p50 15.6 ms | **VERIFIED** | `raw/b3_scan.txt` 63 scans/s; 63 × 500 = 31,500; per-scan p50 ≈ 15.5 ms. |
| §2 sync_all p99 2.78 ms, dir fsync 1.97 ms | **VERIFIED** | probe raws: sync_all p99 2.769–2.803; dir fsync p50 1.967–1.972. Spot re-run 2.86 ms / 2.02 ms — same order. |
| §2 "earlier draft quoted 2.79 ms = sync_all p99" correction | **VERIFIED (honest)** | The correction is accurate and self-disclosed; git history (`5500b10 correct fsync latency figures`) confirms. |

## RESULTS.md §6 — sled baseline (matched durability, wins AND losses)

| Row | Claim | Verdict |
|---|---|---|
| Durable: acc `Always` 364 vs sled `insert+flush` 1,070 (sled 2.9×) | **VERIFIED** | `acc_durable_matched_c1.txt` median 364; `sled_durable_c1.txt` median 1070; 1070/364 = 2.9×. |
| Buffered: acc `OsBuffered` 84,937 vs sled 217,719 (sled 2.6×) | **VERIFIED** | medians 84937 and 217719; ratio 2.56×. |
| Buffered reads: acc 615,188 vs sled 3,686,026 (sled 6×) | **VERIFIED** | `sled_buffered_pointread_c8.txt` median 3686026; 3686026/615188 = 5.99×. |
| Durability actually matched | **VERIFIED** | `sled_shim.rs`: `SledDurable::put` = `insert` then `flush()` per write (fsync-before-ack, matches accretion `Always`); `SledBuffered` opens with `flush_every_ms=None` and never flushes per write (matches `OsBuffered`). Mapping documented identically in `kv.rs` and RESULTS §6. `GroupCommit` correctly *not* compared to sled. |
| "at least one sled win reported" | **VERIFIED** | All three matched rows are sled wins, stated plainly ("sled wins every matched comparison on this host"). Honesty brand satisfied. |

## DESIGN_NOTES.md

| Claim | Verdict |
|---|---|
| Group-commit math (878 µs bare barrier; one WAL `sync_data` per `Always` write; 29× measured) | **VERIFIED** — arithmetic checks (1/0.000878 = 1139); same-concurrency c=64 raw p50 is 3.685 ms for `Always` and 7.5 ms for `GroupCommit`. The remaining latency includes append/file-open, lock, and scheduler overhead, not extra fsyncs. |
| Bloom sizing (k=(m/n)ln2=6.93→7; FPR 0.6185^10≈0.0082) | **VERIFIED** — matches `bloom.rs` and the FPR test. |
| Torn-tail truncation rule | **VERIFIED** — `wal/recovery.rs` truncates at first short/bad-CRC frame; matches FORMAT.md. |
| Manifest tmp+fsync+rename+dir-fsync; readers pin `Arc<Version>`; GC when unreferenced | **VERIFIED** — `manifest.rs::install` + `Arc<Version>` model present. |
| Concurrency: compaction **synchronous by design**, no background thread | **VERIFIED** — `grep` finds no `thread::spawn`/channel in `db.rs`/`compaction.rs`; `compaction.rs:1` documents it. NB: this is a documented deviation from spec stage S4 ("promote to background thread"), made and defended explicitly (commit `8ed058d`); README Limitations + DESIGN_NOTES Concurrency both disclose it as future work. Honest, not a false claim. |
| Crash evidence: N=330, 2,640 executions, 160 proptest cases, positive control | **VERIFIED** — matches re-run counts; positive control described in BUGS_FOUND matches DESIGN_NOTES. |

## FORMAT.md

| Claim | Verdict |
|---|---|
| Footer fixed **48 bytes** | **VERIFIED** — `builder.rs` `FOOTER_SIZE = 48`, `debug_assert_eq!(footer.len(), 48)`. |
| magic `0x41434352_5F535354` | **VERIFIED** — `mod.rs` `FOOTER_MAGIC = 0x4143_4352_5F53_5354` (identical value). |
| Frame = len u32 + crc32 u32 + payload; block trailing CRC; reader validates back-to-front | **VERIFIED** — matches `wal/frame.rs`, `sstable/reader.rs` (reads footer at `EOF−48`, validates magic/version/CRC/region bounds). |

## BUGS_FOUND.md

| Claim | Verdict |
|---|---|
| Organic-only rule; entries cite source/root-cause/fix/regression | **VERIFIED** — 5 entries: tombstone GC, rename fidelity, group-commit locking, recovery ordering, and inode-generation/tear tracking. |
| Bug #3 re-verification counts (2,640 executions, 160 proptest cases, process_kill 2/2) | **VERIFIED** — consistent with the re-run suite. |
| Positive-control harness validation (remove Always fsync → fails at crash point 1, restored) | **VERIFIED (as validation, not a shipped bug)** — labeled correctly; the required fsync is present. |
| Journal numbering | **VERIFIED** — entries are sequentially numbered 1-5. |

## Environment note

Benchmarks were re-verified by recomputing medians from the committed
`benchmarks/raw/*` (the spec requires numbers trace to those artifacts). The
fsync probe was independently re-run on this host and reproduced the framing
figure to the same order of magnitude (887 µs vs documented 878 µs). Full B1/B2
matrix re-execution (~30 min, and RESULTS.md discloses it was run on the build
host with a `/dev/nvme0n1p1` ext4 volume) was not repeated end-to-end; the raw
files are internally consistent and their medians match every published cell.

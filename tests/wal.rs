//! Crash-consistency tests for the write-ahead log, run against `SimFs`.
//!
//! These exercise the WAL through the [`accretion_db::testkit`] harness so the
//! log grows up under simulated power loss exactly as the spec demands:
//!
//! * **crash-mid-append / torn-tail** — an unsynced tail may be dropped, torn at
//!   a random byte, or bit-flipped; recovery must truncate at the first bad
//!   frame and never surface a partial or corrupt record.
//! * **crash-before/after-sync** — in a durable mode a record acked (its
//!   `append` returned) implies its frame is synced, so it must survive; a record
//!   whose sync had not completed may vanish but must never half-apply.
//!
//! The invariant every verifier enforces is *prefix consistency*: the recovered
//! record sequence is a prefix of what the workload wrote, each record intact.
//! This is the WAL analogue of the toy-store proof in `tests/harness.rs`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use accretion_db::testkit::{count_ops, crash_sweep, run_crash};
use accretion_db::wal::{Durability, Wal, WalOptions};
use accretion_db::Storage;

const DIR: &str = "/wal";

fn dir() -> PathBuf {
    PathBuf::from(DIR)
}

/// The canonical crash workload: append `records` distinct payloads through a
/// freshly-opened WAL in `mode`. Each payload is `rNNN` so a recovered prefix is
/// self-identifying.
fn payloads(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("r{i:03}").into_bytes()).collect()
}

fn open(fs: Arc<dyn Storage>, mode: Durability) -> (Wal, accretion_db::wal::Recovered) {
    Wal::open(
        fs,
        &dir(),
        WalOptions {
            durability: mode,
            // Small segments so the sweep also crosses rotation boundaries.
            segment_size: 128,
        },
    )
    .expect("open wal")
}

/// Assert the recovered records are a clean prefix of `expected`: same values,
/// in order, no phantom or reordered entries.
fn assert_prefix(recovered: &[Vec<u8>], expected: &[Vec<u8>]) {
    assert!(
        recovered.len() <= expected.len(),
        "recovered {} records but only {} were ever written (phantom records)",
        recovered.len(),
        expected.len()
    );
    for (i, (got, want)) in recovered.iter().zip(expected).enumerate() {
        assert_eq!(got, want, "record {i} differs: recovered vs written");
    }
}

/// Exhaustive deterministic crash sweep in a durable mode: crash after every
/// storage op of a multi-append workload, reopen, and confirm the recovered log
/// is always a clean prefix. No torn or corrupt frame is ever accepted.
fn sweep_durable(mode: Durability, seed: u64) -> u64 {
    let expected = payloads(12);
    let body_records = expected.clone();

    let body = move |fs: Arc<dyn Storage>| {
        let (wal, rec) = open(fs, mode);
        assert!(rec.records.is_empty());
        for r in &body_records {
            wal.append(r).expect("append");
        }
    };

    let verify_expected = expected.clone();
    let verify = move |fs: Arc<dyn Storage>, _report: &_| {
        let (_wal, rec) = open(fs, mode);
        assert_prefix(&rec.records, &verify_expected);
    };

    crash_sweep(seed, body, verify)
}

#[test]
fn crash_sweep_always_is_prefix_consistent() {
    let n = sweep_durable(Durability::Always, 42);
    assert!(n > 0, "sweep must exercise at least one crash point");
}

#[test]
fn crash_sweep_group_commit_is_prefix_consistent() {
    // GroupCommit with a single writer thread still runs one full commit per
    // append (it becomes its own leader), so the durable-prefix invariant holds
    // op-for-op just like Always.
    let n = sweep_durable(Durability::GroupCommit, 7);
    assert!(n > 0);
}

/// A record whose `append` returned before the crash (acked) MUST survive, in a
/// durable mode. We arm the crash for after the workload's final op so every
/// append completed durably, then confirm all records are present.
#[test]
fn acked_records_survive_crash_always() {
    let recs = payloads(6);
    let n = count_ops(3, {
        let recs = recs.clone();
        move |fs| {
            let (wal, _) = open(fs, Durability::Always);
            for r in &recs {
                wal.append(r).expect("append");
            }
        }
    });

    let body_recs = recs.clone();
    let verify_recs = recs.clone();
    run_crash(
        3,
        n, // crash exactly at the last op: everything acked is durable
        move |fs| {
            let (wal, _) = open(fs, Durability::Always);
            for r in &body_recs {
                wal.append(r).expect("append");
            }
        },
        move |fs, _report| {
            let (_wal, rec) = open(fs, Durability::Always);
            assert_eq!(
                rec.records, verify_recs,
                "every acked record must survive a post-sync crash"
            );
        },
    );
}

/// Crash *mid-append* — after the frame's bytes are buffered but before the
/// covering `sync_file` — must never leave a half-valid frame that recovery
/// accepts. The un-synced record is either fully absent or, if the tear kept its
/// whole synced-length prefix, intact; a torn/bit-flipped frame is rejected.
#[test]
fn crash_mid_append_never_half_applies() {
    // Workload: open (create+sync_dir on fresh) then one append that fsyncs.
    // We sweep every crash point and, for each, verify prefix consistency — this
    // includes the exact op boundary between the buffered append and its sync.
    let expected = payloads(3);
    let body_recs = expected.clone();
    let body = move |fs: Arc<dyn Storage>| {
        let (wal, _) = open(fs, Durability::Always);
        for r in &body_recs {
            wal.append(r).expect("append");
        }
    };
    let verify_expected = expected.clone();
    let verify = move |fs: Arc<dyn Storage>, _r: &_| {
        let (_wal, rec) = open(fs, Durability::Always);
        // No corrupt/torn frame is ever surfaced; only a clean prefix.
        assert_prefix(&rec.records, &verify_expected);
    };
    let n = crash_sweep(99, body, verify);
    assert!(n >= 3);
}

/// Reopening after a crash and appending more records leaves a log that recovers
/// cleanly — recovery truncates any torn tail so subsequent appends start at a
/// clean frame boundary.
#[test]
fn reopen_after_crash_then_append_recovers_clean() {
    run_crash(
        13,
        4, // crash somewhere mid-workload
        |fs| {
            let (wal, _) = open(fs, Durability::Always);
            for r in &payloads(5) {
                wal.append(r).expect("append");
            }
        },
        |fs, _report| {
            // First reopen: recovers whatever survived (a clean prefix) and
            // truncates any torn tail.
            let (wal, rec1) = open(fs.clone(), Durability::Always);
            let survived = rec1.records.len();
            // Append two fresh records post-recovery.
            wal.append(b"post-a").expect("append");
            wal.append(b"post-b").expect("append");
            drop(wal);
            // Second reopen: the survived prefix plus the two new records, clean.
            let (_wal2, rec2) = open(fs, Durability::Always);
            assert_eq!(rec2.records.len(), survived + 2);
            assert_eq!(rec2.records[survived], b"post-a");
            assert_eq!(rec2.records[survived + 1], b"post-b");
        },
    );
}

/// RealFs smoke test: the same open → append → reopen → replay cycle must work
/// against the real filesystem (via `tempfile`), proving the WAL is not
/// SimFs-specific. No crash is injected here — that is SimFs's job.
#[test]
fn realfs_smoke_round_trip() {
    use accretion_db::RealFs;

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("wal");
    std::fs::create_dir_all(&root).expect("mkdir");
    let fs: Arc<dyn Storage> = Arc::new(RealFs::new());

    let recs = payloads(20);
    {
        let (wal, rec) = Wal::open(
            fs.clone(),
            &root,
            WalOptions {
                durability: Durability::Always,
                segment_size: 256,
            },
        )
        .expect("open");
        assert!(rec.records.is_empty());
        for r in &recs {
            wal.append(r).expect("append");
        }
    }

    // Reopen and confirm every record replays in order off real disk.
    let (_wal2, rec2) = Wal::open(
        fs,
        &root,
        WalOptions {
            durability: Durability::Always,
            segment_size: 256,
        },
    )
    .expect("reopen");
    assert_eq!(rec2.records, recs);
}

/// Sanity that the harness sees the WAL's storage ops (guards against a workload
/// that silently does nothing).
#[test]
fn workload_issues_storage_ops() {
    let n = count_ops(1, |fs: Arc<dyn Storage>| {
        let (wal, _) = open(fs, Durability::Always);
        wal.append(b"x").expect("append");
    });
    assert!(n >= 2, "expected setup + append ops, got {n}");
    let _ = Path::new(DIR);
}

mod property {
    use super::*;
    use proptest::prelude::*;

    /// Map a proptest-chosen index onto a durable mode. `OsBuffered` is excluded
    /// here because it makes no durability promise, so "acked ⇒ survives" — the
    /// property under test — does not apply to it.
    fn durable_mode(pick: bool) -> Durability {
        if pick {
            Durability::Always
        } else {
            Durability::GroupCommit
        }
    }

    proptest! {
        // Random durable-mode crash schedules: for an arbitrary record set, an
        // arbitrary crash point, and either durable mode, recovery yields a clean
        // prefix of the written records. Shrinking gives a minimal counterexample
        // if this ever fails.
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn random_crash_point_is_prefix_consistent(
            record_lens in prop::collection::vec(1usize..40, 1..24),
            seed in any::<u64>(),
            crash_frac in 0.0f64..1.0,
            mode_pick in any::<bool>(),
        ) {
            let mode = durable_mode(mode_pick);
            // Distinct, self-identifying payloads of the chosen lengths.
            let records: Vec<Vec<u8>> = record_lens
                .iter()
                .enumerate()
                .map(|(i, &len)| {
                    let mut v = format!("r{i:04}-").into_bytes();
                    v.resize(v.len() + len, b'.');
                    v
                })
                .collect();

            let body_recs = records.clone();
            let body = move |fs: Arc<dyn Storage>| {
                let (wal, _) = open(fs, mode);
                for r in &body_recs {
                    wal.append(r).expect("append");
                }
            };

            // Count ops, then crash at a fraction of the way through.
            let n = count_ops(seed, &body);
            let crash_at = ((n as f64 * crash_frac) as u64).clamp(1, n.max(1));

            let verify_recs = records.clone();
            run_crash(
                seed,
                crash_at,
                body,
                move |fs, _report| {
                    // Plain assertions here: proptest catches the panic and shrinks
                    // to a minimal counterexample. (`prop_assert!` cannot be used —
                    // the verifier closure returns `()`, not a `TestCaseResult`.)
                    let (_wal, rec) = open(fs, mode);
                    assert!(
                        rec.records.len() <= verify_recs.len(),
                        "phantom records: recovered {} > written {}",
                        rec.records.len(),
                        verify_recs.len()
                    );
                    for (got, want) in rec.records.iter().zip(&verify_recs) {
                        assert_eq!(got, want, "recovered record differs from written");
                    }
                },
            );
        }
    }
}

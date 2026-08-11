//! The crash-consistency stage: the heart of `accretion-db`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use accretion_db::db::{Db, Options};
use accretion_db::storage::{CrashReport, SimFs, Storage};
use accretion_db::testkit::run_crash;
use accretion_db::Durability;

const DIR: &str = "/db";

/// One logical mutation in a workload. Reads are not part of the crash workload
/// (they issue no storage ops and cannot be lost); only mutations matter here.
#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// The engine configuration used by the whole sweep: a tiny memtable and a small
/// fanout so a modest workload genuinely crosses freeze → flush → compaction.
fn opts(durability: Durability) -> Options {
    Options {
        durability,
        memtable_size: 128,
        tier_fanout: 4,
    }
}

/// The reference model: the key→value map a prefix of ops produces.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Fold a prefix of ops into the key/value state it produces — the reference
/// model. A `Put` sets, a `Delete` removes.
fn model_fold(ops: &[Op]) -> Model {
    let mut m = BTreeMap::new();
    for op in ops {
        match op {
            Op::Put(k, v) => {
                m.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                m.remove(k);
            }
        }
    }
    m
}

/// The key a mutation touches (for the "in-flight op may or may not have applied"
/// allowance).
fn op_key(op: &Op) -> &[u8] {
    match op {
        Op::Put(k, _) | Op::Delete(k) => k,
    }
}

/// Every key any op in the workload mentions.
fn key_universe(ops: &[Op]) -> Vec<Vec<u8>> {
    let mut ks: Vec<Vec<u8>> = ops.iter().map(|o| op_key(o).to_vec()).collect();
    ks.sort();
    ks.dedup();
    ks
}

/// Run `ops` against a freshly-opened engine on `fs`, recording into `acked` the number of ops that *fully
/// returned* before any crash halted the workload.
fn run_workload(fs: Arc<dyn Storage>, mode: Durability, ops: &[Op], acked: &AtomicUsize) {
    let db = Db::open_on(fs, Path::new(DIR), opts(mode)).expect("open db");
    for op in ops {
        match op {
            Op::Put(k, v) => db.put(k, v).expect("put"),
            Op::Delete(k) => db.delete(k).expect("delete"),
        }
        acked.fetch_add(1, Ordering::SeqCst);
    }
}

/// Verify the recovery invariant against the recovered filesystem.
fn verify(fs: Arc<dyn Storage>, mode: Durability, ops: &[Op], acked: usize, _r: &CrashReport) {
    // Reopen the engine on the post-crash image (no crash armed now).
    let db = Db::open_on(fs, Path::new(DIR), opts(mode)).expect("reopen after crash");

    let acked_model = model_fold(&ops[..acked]);
    // The in-flight op, if the crash interrupted one, and the state it would
    // produce if it happened to have applied.
    let maybe_model = if acked < ops.len() {
        Some((op_key(&ops[acked]).to_vec(), model_fold(&ops[..=acked])))
    } else {
        None
    };

    // 1. Per-key bound + acked durability: every key resolves to an allowed value.
    for k in key_universe(ops) {
        let got = db.get(&k).expect("get");
        let acked_val = acked_model.get(&k).cloned();
        let allowed_maybe = match &maybe_model {
            Some((ik, mm)) if ik == &k => Some(mm.get(&k).cloned()),
            _ => None,
        };
        let touched = allowed_maybe.is_some();
        let ok = got == acked_val || allowed_maybe.is_some_and(|m| got == m);
        assert!(
            ok,
            "key {k:?} mode={mode:?} acked={acked}: got {got:?}, \
             acked-model says {acked_val:?} (in-flight touches this key: {touched})"
        );
    }

    // 2. Scan/get consistency: the scan is sorted, every pair it yields is a live key in the universe whose
    // value matches a fresh `get`, and the set of keys the scan yields is exactly the set of universe keys `get` finds live.
    let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).expect("scan").collect();
    assert!(
        scanned.windows(2).all(|w| w[0].0 < w[1].0),
        "scan not strictly ascending mode={mode:?}"
    );
    let universe = key_universe(ops);
    for (k, v) in &scanned {
        assert!(
            universe.contains(k),
            "phantom key {k:?} in scan mode={mode:?}"
        );
        assert_eq!(
            db.get(k).expect("get"),
            Some(v.clone()),
            "scan/get disagree on {k:?} mode={mode:?}"
        );
    }
    let scan_keys: std::collections::BTreeSet<&Vec<u8>> = scanned.iter().map(|(k, _)| k).collect();
    for k in &universe {
        let live_by_get = db.get(k).expect("get").is_some();
        assert_eq!(
            live_by_get,
            scan_keys.contains(k),
            "scan vs get liveness disagree on {k:?} mode={mode:?}"
        );
    }
}

/// The canonical crash workload: mixed puts, overwrites, and deletes over a small colliding key set, sized
/// (with the 128-byte memtable) to force several flushes and at least one compaction.
fn canonical_ops() -> Vec<Op> {
    let mut ops = Vec::new();
    // Round 1: seed 12 distinct keys with 12-byte values → forces multiple freezes.
    for i in 0..12u32 {
        ops.push(Op::Put(
            format!("key{i:02}").into_bytes(),
            format!("val-round0-{i:02}").into_bytes(),
        ));
    }
    // Round 2: overwrite half, delete a few → newest-wins + tombstones across tiers.
    for i in 0..12u32 {
        if i % 3 == 0 {
            ops.push(Op::Delete(format!("key{i:02}").into_bytes()));
        } else {
            ops.push(Op::Put(
                format!("key{i:02}").into_bytes(),
                format!("val-round1-{i:02}").into_bytes(),
            ));
        }
    }
    // Round 3: re-put some deleted keys (resurrection) and add fresh ones →
    // drives tier-0 past the fanout so a compaction cascade runs.
    for i in 0..12u32 {
        if i % 3 == 0 {
            ops.push(Op::Put(
                format!("key{i:02}").into_bytes(),
                format!("val-round2-{i:02}").into_bytes(),
            ));
        }
        ops.push(Op::Put(
            format!("new{i:02}").into_bytes(),
            format!("fresh-{i:02}").into_bytes(),
        ));
    }
    ops
}

mod exhaustive {
    use super::*;

    /// The seeds swept at every crash point.
    const SEEDS: [u64; 4] = [1, 7, 42, 1234];

    /// Count `N`: the exact number of mutating storage ops the canonical workload issues. Seed-independent
    /// (the rng only fires at crash time), so any seed gives the same count.
    fn count_workload_ops(mode: Durability) -> u64 {
        let sim = Arc::new(SimFs::with_seed(0));
        let fs: Arc<dyn Storage> = sim.clone();
        let acked = AtomicUsize::new(0);
        run_workload(fs, mode, &canonical_ops(), &acked);
        sim.op_count()
    }

    /// Drive the whole sweep for one mode: for every crash point `i in 1..=N` and
    /// every seed, crash after op `i`, reopen, and verify. Returns `N`.
    fn sweep(mode: Durability) -> u64 {
        let ops = canonical_ops();
        let n = count_workload_ops(mode);
        assert!(n > 0, "workload issued no storage ops");
        for i in 1..=n {
            for &seed in &SEEDS {
                let acked = Arc::new(AtomicUsize::new(0));
                let acked_body = Arc::clone(&acked);
                let ops_body = ops.clone();
                let ops_verify = ops.clone();
                run_crash(
                    seed,
                    i,
                    move |fs| run_workload(fs, mode, &ops_body, &acked_body),
                    move |fs, report| {
                        verify(fs, mode, &ops_verify, acked.load(Ordering::SeqCst), report)
                    },
                );
            }
        }
        n
    }

    #[test]
    fn always_zero_acked_loss_at_every_crash_point() {
        sweep(Durability::Always);
    }

    #[test]
    fn group_commit_zero_acked_loss_at_every_crash_point() {
        sweep(Durability::GroupCommit);
    }

    /// The workload must genuinely cross flush and compaction boundaries, or the sweep would not exercise
    /// the paths it claims to.
    #[test]
    fn workload_forces_flushes_and_compaction() {
        let sim = Arc::new(SimFs::with_seed(0));
        let fs: Arc<dyn Storage> = sim.clone();
        let db = Db::open_on(fs, Path::new(DIR), opts(Durability::Always)).expect("open");
        for op in &canonical_ops() {
            match op {
                Op::Put(k, v) => db.put(k, v).unwrap(),
                Op::Delete(k) => db.delete(k).unwrap(),
            }
        }
        db.flush().unwrap();
        let v = db.debug_version();
        // ≥2 tiers means data was flushed (tier 0) and compacted down (tier 1+).
        assert!(
            v.num_tiers() >= 2,
            "expected a compaction to build ≥2 tiers, got {} tier(s)",
            v.num_tiers()
        );
    }

    /// Emit the distinct-crash-point count and the variant multiplier for
    /// RESULTS.md. Not an assertion — a reporting harness.
    #[test]
    fn reports_crash_point_count() {
        let n = count_workload_ops(Durability::Always);
        let total = n * SEEDS.len() as u64 * 2; // × seeds × durable modes
        println!(
            "CRASH-SWEEP: N={n} distinct crash points; {} seeds (drop/torn/bit-flip); \
             2 durable modes; {total} total crash executions",
            SEEDS.len()
        );
    }
}

/// Property-based random crash schedules: an arbitrary op sequence, crashed at an arbitrary point, in an
/// arbitrary durable mode, must recover with zero acknowledged-write loss. proptest shrinks any failure to a.
mod schedules {
    use super::*;
    use proptest::prelude::*;

    use proptest::test_runner::FileFailurePersistence;

    fn crash_proptest_config() -> ProptestConfig {
        let mut config = ProptestConfig::with_cases(160);
        // Integration tests are outside `src`, so avoid SourceParallel's
        // lib.rs/main.rs ancestor lookup and persist beside this source file.
        config.failure_persistence = Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        )));
        config
    }

    /// A small colliding key alphabet so puts, overwrites, and deletes of the same key interleave often.
    fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(b'a'..=b'd', 1..=2)
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (key_strategy(), proptest::collection::vec(any::<u8>(), 0..=6))
                .prop_map(|(k, v)| Op::Put(k, v)),
            1 => key_strategy().prop_map(Op::Delete),
        ]
    }

    /// Map a bool onto a durable mode. `OsBuffered` is excluded: it makes no crash-durability promise, so
    /// "acked ⇒ survives" — the property under test — does not apply to it.
    fn durable_mode(pick: bool) -> Durability {
        if pick {
            Durability::Always
        } else {
            Durability::GroupCommit
        }
    }

    /// Count the mutating storage ops `ops` issues in `mode`, so a crash fraction
    /// can be mapped onto a concrete op index.
    fn count_ops(mode: Durability, ops: &[Op]) -> u64 {
        let sim = Arc::new(SimFs::with_seed(0));
        let fs: Arc<dyn Storage> = sim.clone();
        let acked = AtomicUsize::new(0);
        run_workload(fs, mode, ops, &acked);
        sim.op_count()
    }

    proptest! {
        // Modest case count keeps the whole file CI-fast while still exploring
        // thousands of distinct (sequence, crash-point, mode) schedules.
        #![proptest_config(crash_proptest_config())]

        #[test]
        fn random_schedule_zero_acked_loss(
            ops in proptest::collection::vec(op_strategy(), 1..80),
            seed in any::<u64>(),
            crash_frac in 0.0f64..1.0,
            mode_pick in any::<bool>(),
        ) {
            let mode = durable_mode(mode_pick);

            // Map the fraction onto a concrete crash op in 1..=N.
            let n = count_ops(mode, &ops);
            let crash_at = ((n as f64 * crash_frac) as u64).clamp(1, n.max(1));

            let acked = Arc::new(AtomicUsize::new(0));
            let acked_body = Arc::clone(&acked);
            let ops_body = ops.clone();
            let ops_verify = ops.clone();
            run_crash(
                seed,
                crash_at,
                move |fs| run_workload(fs, mode, &ops_body, &acked_body),
                // Plain assertions inside the verifier: a panic here is caught by proptest and shrunk.
                // (`prop_assert!` is unavailable — the verifier closure returns `()`, not a `TestCaseResult`.)
                move |fs, report| {
                    verify(fs, mode, &ops_verify, acked.load(Ordering::SeqCst), report)
                },
            );
        }
    }
}

/// Fixed-seed regression corpus: named, deterministic crash schedules pinning the specific boundaries the
/// recovery invariant most depends on.
mod regressions {
    use super::*;

    const TEAR_SEEDS: [u64; 4] = [1, 7, 42, 1234];

    /// Sweep every crash point of `ops` in `mode` across all tear seeds, verifying
    /// the recovery invariant each time.
    fn sweep_ops(mode: Durability, ops: &[Op]) {
        let sim = Arc::new(SimFs::with_seed(0));
        let fs: Arc<dyn Storage> = sim.clone();
        let counter = AtomicUsize::new(0);
        run_workload(fs, mode, ops, &counter);
        let n = sim.op_count();
        assert!(n > 0);
        for i in 1..=n {
            for &seed in &TEAR_SEEDS {
                let acked = Arc::new(AtomicUsize::new(0));
                let acked_body = Arc::clone(&acked);
                let ops_body = ops.to_vec();
                let ops_verify = ops.to_vec();
                run_crash(
                    seed,
                    i,
                    move |fs| run_workload(fs, mode, &ops_body, &acked_body),
                    move |fs, report| {
                        verify(fs, mode, &ops_verify, acked.load(Ordering::SeqCst), report)
                    },
                );
            }
        }
    }

    fn put(k: &str, v: &str) -> Op {
        Op::Put(k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }
    fn del(k: &str) -> Op {
        Op::Delete(k.as_bytes().to_vec())
    }

    fn all_durable_modes() -> [Durability; 2] {
        [Durability::Always, Durability::GroupCommit]
    }

    /// Overwrite-then-delete-then-resurrect a single key with enough volume to force flushes between the
    /// versions: a crash at any op must never resurrect a deleted value nor lose the final live one when it was acked.
    #[test]
    fn resurrection_across_flush() {
        // 20-byte values with a 128-byte memtable flush every ~5 writes.
        let mut ops = vec![put("k", "first-value-padding")];
        for i in 0..8 {
            ops.push(put(&format!("f{i}"), "filler-value-padding"));
        }
        ops.push(del("k"));
        for i in 0..8 {
            ops.push(put(&format!("g{i}"), "filler-value-padding"));
        }
        ops.push(put("k", "final-value-padding"));
        for mode in all_durable_modes() {
            sweep_ops(mode, &ops);
        }
    }

    /// A delete of a key that already lives in a lower tier, with a compaction in between.
    #[test]
    fn tombstone_shadow_through_compaction() {
        let mut ops = Vec::new();
        // Fill enough to build tiers and trigger a compaction (fanout 4).
        for round in 0..6 {
            for i in 0..4 {
                ops.push(put(&format!("k{i}"), &format!("r{round}-value-pad")));
            }
        }
        ops.push(del("k0"));
        for i in 0..4 {
            ops.push(put(&format!("x{i}"), "tail-value-padding"));
        }
        for mode in all_durable_modes() {
            sweep_ops(mode, &ops);
        }
    }

    /// A pure-delete-heavy schedule: many keys created then deleted, crashing at every point.
    /// Deleted-and-acked keys must stay absent; live-and-acked keys must stay present.
    #[test]
    fn delete_heavy_schedule() {
        let mut ops = Vec::new();
        for i in 0..10 {
            ops.push(put(&format!("d{i:02}"), "value-with-padding-x"));
        }
        for i in 0..10 {
            if i % 2 == 0 {
                ops.push(del(&format!("d{i:02}")));
            }
        }
        for mode in all_durable_modes() {
            sweep_ops(mode, &ops);
        }
    }
}

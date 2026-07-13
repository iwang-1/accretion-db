//! Reference-model property tests: the real [`Db`] engine driven with a random
//! sequence of operations must agree, at every observation point, with a plain
//! `BTreeMap` computing the same answer directly.
//!
//! The same op sequence is applied to both the engine and the model, and after
//! every op a point read (and periodically a full scan) is cross-checked. The
//! memtable is sized small and flushes are injected so the sequence genuinely
//! crosses freeze/flush and compaction boundaries rather than staying in one
//! memtable. Every durability mode is exercised — correctness of the read path is
//! mode-independent, so all three must match the model with no crash involved.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use proptest::prelude::*;

use accretion_db::db::{Db, Options};
use accretion_db::storage::{SimFs, Storage};
use accretion_db::Durability;

/// One operation in a generated workload.
#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Flush,
    /// Read back a key (may or may not be present) — checked against the model.
    Get(Vec<u8>),
    /// A bounded forward scan — checked against the model.
    Scan(Vec<u8>, Vec<u8>),
}

/// Keys are drawn from a small alphabet so puts, overwrites, and deletes collide
/// often — that collision is what stresses newest-wins across memtable, tiers,
/// and compaction.
fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(b'a'..=b'e', 1..=3)
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=8)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (key_strategy(), value_strategy()).prop_map(|(k, v)| Op::Put(k, v)),
        1 => key_strategy().prop_map(Op::Delete),
        1 => Just(Op::Flush),
        2 => key_strategy().prop_map(Op::Get),
        1 => (key_strategy(), key_strategy()).prop_map(|(a, b)| {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            Op::Scan(lo, hi)
        }),
    ]
}

fn modes() -> [Durability; 3] {
    [
        Durability::Always,
        Durability::GroupCommit,
        Durability::OsBuffered,
    ]
}

/// Apply `ops` to a fresh engine (in `mode`) and an in-memory `BTreeMap` model,
/// asserting the two agree on every read and scan.
fn run_against_model(ops: &[Op], mode: Durability, seed: u64) {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(seed));
    let opts = Options {
        durability: mode,
        memtable_size: 128, // tiny: force frequent freezes/flushes/compactions
        tier_fanout: 4,
    };
    let db = Db::open_on(fs, std::path::Path::new("/db"), opts).expect("open db");
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    for op in ops {
        match op {
            Op::Put(k, v) => {
                db.put(k, v).expect("put");
                model.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                db.delete(k).expect("delete");
                model.remove(k);
            }
            Op::Flush => {
                db.flush().expect("flush");
            }
            Op::Get(k) => {
                let got = db.get(k).expect("get");
                assert_eq!(got.as_ref(), model.get(k), "get({k:?}) mode={mode:?}");
            }
            Op::Scan(lo, hi) => {
                let got: Vec<(Vec<u8>, Vec<u8>)> =
                    db.scan(lo.clone()..hi.clone()).expect("scan").collect();
                let expected: Vec<(Vec<u8>, Vec<u8>)> = model
                    .range((Bound::Included(lo.clone()), Bound::Excluded(hi.clone())))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                assert_eq!(
                    got, expected,
                    "scan({lo:?}..{hi:?}) mode={mode:?}\n got={got:?}\n exp={expected:?}"
                );
            }
        }
    }

    // Final full-database cross-check: every model key resolves to its value, and
    // a full scan reproduces the model exactly.
    for (k, v) in &model {
        assert_eq!(
            db.get(k).expect("final get").as_ref(),
            Some(v),
            "final {k:?}"
        );
    }
    let full: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).expect("full scan").collect();
    let model_full: Vec<(Vec<u8>, Vec<u8>)> =
        model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(full, model_full, "final full scan mode={mode:?}");
}

proptest! {
    // Kept modest per-mode so the whole matrix (x3 modes) stays CI-fast while
    // still exploring thousands of ops each.
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn engine_matches_btreemap_model(
        ops in proptest::collection::vec(op_strategy(), 1..200),
        seed in any::<u64>(),
    ) {
        for mode in modes() {
            run_against_model(&ops, mode, seed);
        }
    }
}

/// A fixed regression: overwrite then delete then re-put a key across a flush,
/// in every mode — the canonical newest-wins-across-boundaries case, pinned as a
/// named test so a shrink from the proptest has a home.
#[test]
fn resurrection_across_flush_all_modes() {
    let ops = vec![
        Op::Put(b"a".to_vec(), b"1".to_vec()),
        Op::Flush,
        Op::Delete(b"a".to_vec()),
        Op::Flush,
        Op::Put(b"a".to_vec(), b"2".to_vec()),
        Op::Get(b"a".to_vec()),
        Op::Flush,
        Op::Get(b"a".to_vec()),
    ];
    for mode in modes() {
        run_against_model(&ops, mode, 42);
    }
}

//! Property-based merge correctness: random write sequences (puts, overwrites,
//! deletes, resurrections) distributed across a random number of sources must
//! produce exactly the live scan a flat `BTreeMap` reference model computes.

use super::*;
use crate::memtable::{InternalValue, ValueKind};
use proptest::prelude::*;
use std::collections::BTreeMap;

/// One generated write. `seq` is assigned deterministically by position at
/// apply time (writes[i] gets seq = i + 1), so overwrites and deletes to the
/// same key have a well-defined newest.
#[derive(Clone, Debug)]
enum Op {
    Put(u8, Vec<u8>),
    Del(u8),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Small key space (0..16) so keys collide often, exercising newest-wins.
        (0u8..16, prop::collection::vec(any::<u8>(), 0..8)).prop_map(|(k, v)| Op::Put(k, v)),
        (0u8..16).prop_map(Op::Del),
    ]
}

/// Apply ops in order (seq = index + 1) to a flat map keeping the newest version
/// per key, then compute the live (tombstone-free) scan.
fn model_live(ops: &[Op]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut m: BTreeMap<Vec<u8>, InternalValue> = BTreeMap::new();
    for (i, op) in ops.iter().enumerate() {
        let seq = (i + 1) as u64;
        let (key, iv) = match op {
            Op::Put(kb, v) => (vec![*kb], InternalValue::value(seq, v.clone())),
            Op::Del(kb) => (vec![*kb], InternalValue::tombstone(seq)),
        };
        m.insert(key, iv); // seq strictly increases, so always newest
    }
    m.into_iter()
        .filter_map(|(k, v)| match v.kind {
            ValueKind::Value(val) => Some((k, val)),
            ValueKind::Tombstone => None,
        })
        .collect()
}

/// Distribute ops round-robin across `n` sources; within each source keep only
/// the newest version per key and emit key-sorted — the per-source contract.
fn build_sources(ops: &[Op], n: usize) -> Vec<EntrySource> {
    let mut buckets: Vec<BTreeMap<Vec<u8>, InternalValue>> = vec![BTreeMap::new(); n];
    for (i, op) in ops.iter().enumerate() {
        let seq = (i + 1) as u64;
        let (key, iv) = match op {
            Op::Put(kb, v) => (vec![*kb], InternalValue::value(seq, v.clone())),
            Op::Del(kb) => (vec![*kb], InternalValue::tombstone(seq)),
        };
        // Round-robin by op index keeps versions of one key spread across
        // sources, so newest-wins must reconcile across the whole set.
        buckets[i % n].insert(key, iv);
    }
    buckets
        .into_iter()
        .map(|b| -> EntrySource { Box::new(b.into_iter()) })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn merge_live_matches_btreemap_model(
        ops in prop::collection::vec(op_strategy(), 0..80),
        n in 1usize..6,
    ) {
        let expected = model_live(&ops);
        let sources = build_sources(&ops, n);
        let got: Vec<_> = MergeIterator::new(sources).live().collect();
        prop_assert_eq!(got, expected);
    }

    #[test]
    fn merge_output_is_strictly_key_sorted_and_deduped(
        ops in prop::collection::vec(op_strategy(), 0..80),
        n in 1usize..6,
    ) {
        let sources = build_sources(&ops, n);
        let out: Vec<Entry> = MergeIterator::new(sources).collect();
        // Raw merge: strictly ascending keys, exactly one entry per key.
        for w in out.windows(2) {
            prop_assert!(w[0].0 < w[1].0, "keys not strictly ascending");
        }
    }
}

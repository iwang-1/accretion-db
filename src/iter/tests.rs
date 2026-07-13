//! Merge-iterator correctness: newest-wins de-duplication, tombstone handling,
//! forward-scan ordering — all checked against a flat `BTreeMap` reference model
//! that applies the same writes and computes the expected answer directly.

use super::*;
use crate::memtable::{InternalValue, ValueKind};
use std::collections::BTreeMap;

fn k(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Turn a key-sorted list of entries into a boxed source. Panics if unsorted or
/// duplicated, enforcing the per-source contract the merge iterator relies on.
fn source(entries: Vec<Entry>) -> EntrySource {
    for w in entries.windows(2) {
        assert!(w[0].0 < w[1].0, "source must be strictly key-sorted");
    }
    Box::new(entries.into_iter())
}

#[test]
fn empty_merge_yields_nothing() {
    let mut it = MergeIterator::new(vec![]);
    assert!(it.next().is_none());
}

#[test]
fn single_source_passes_through() {
    let src = source(vec![
        (k("a"), InternalValue::value(1, "1")),
        (k("b"), InternalValue::value(2, "2")),
    ]);
    let out: Vec<_> = MergeIterator::new(vec![src]).collect();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, k("a"));
    assert_eq!(out[1].0, k("b"));
}

#[test]
fn merges_disjoint_sources_in_key_order() {
    let a = source(vec![
        (k("a"), InternalValue::value(1, "a")),
        (k("c"), InternalValue::value(1, "c")),
    ]);
    let b = source(vec![
        (k("b"), InternalValue::value(1, "b")),
        (k("d"), InternalValue::value(1, "d")),
    ]);
    let keys: Vec<_> = MergeIterator::new(vec![a, b]).map(|(key, _)| key).collect();
    assert_eq!(keys, vec![k("a"), k("b"), k("c"), k("d")]);
}

#[test]
fn newest_seq_wins_regardless_of_source_order() {
    // Same key in three sources at seq 3, 1, 2 — the seq-3 value must win no
    // matter which source it sits in.
    let older = source(vec![(k("x"), InternalValue::value(1, "one"))]);
    let newest = source(vec![(k("x"), InternalValue::value(3, "three"))]);
    let middle = source(vec![(k("x"), InternalValue::value(2, "two"))]);

    let out: Vec<_> = MergeIterator::new(vec![older, newest, middle]).collect();
    assert_eq!(out.len(), 1, "duplicates collapse to one entry");
    assert_eq!(out[0].1.seq, 3);
    assert_eq!(out[0].1.as_value(), Some(&b"three"[..]));
}

#[test]
fn tombstone_preserved_in_raw_merge_when_newest() {
    let live = source(vec![(k("x"), InternalValue::value(1, "v"))]);
    let dead = source(vec![(k("x"), InternalValue::tombstone(2))]);
    let out: Vec<_> = MergeIterator::new(vec![live, dead]).collect();
    assert_eq!(out.len(), 1);
    assert!(out[0].1.is_tombstone(), "newest is a tombstone: it wins");
}

#[test]
fn live_scan_drops_tombstoned_key() {
    let live = source(vec![
        (k("a"), InternalValue::value(1, "a")),
        (k("x"), InternalValue::value(1, "old")),
    ]);
    let dead = source(vec![(k("x"), InternalValue::tombstone(2))]);
    let out: Vec<_> = MergeIterator::new(vec![live, dead]).live().collect();
    assert_eq!(out, vec![(k("a"), b"a".to_vec())], "x deleted, a survives");
}

#[test]
fn resurrection_after_delete_is_visible() {
    // put(1) -> delete(2) -> put(3): the key is live again at seq 3.
    let s1 = source(vec![(k("x"), InternalValue::value(1, "first"))]);
    let s2 = source(vec![(k("x"), InternalValue::tombstone(2))]);
    let s3 = source(vec![(k("x"), InternalValue::value(3, "third"))]);
    let out: Vec<_> = MergeIterator::new(vec![s1, s2, s3]).live().collect();
    assert_eq!(out, vec![(k("x"), b"third".to_vec())]);
}

/// The reference model: apply a sequence of `(key, seq, kind)` writes to a flat
/// `BTreeMap`, keeping the highest-seq version per key, then compute the live
/// scan result directly.
fn model_live(writes: &[(Vec<u8>, u64, ValueKind)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut m: BTreeMap<Vec<u8>, InternalValue> = BTreeMap::new();
    for (key, seq, kind) in writes {
        let iv = InternalValue {
            seq: *seq,
            kind: kind.clone(),
        };
        match m.get(key) {
            Some(prev) if prev.seq >= *seq => {}
            _ => {
                m.insert(key.clone(), iv);
            }
        }
    }
    m.into_iter()
        .filter_map(|(key, v)| match v.kind {
            ValueKind::Value(val) => Some((key, val)),
            ValueKind::Tombstone => None,
        })
        .collect()
}

/// Distribute the same writes across `n` sources round-robin, keeping only the
/// newest version per key *within each source* (the per-source contract), each
/// source key-sorted — exactly the shape memtables/tiers hand the merger.
fn build_sources(writes: &[(Vec<u8>, u64, ValueKind)], n: usize) -> Vec<EntrySource> {
    let mut buckets: Vec<BTreeMap<Vec<u8>, InternalValue>> = vec![BTreeMap::new(); n];
    for (i, (key, seq, kind)) in writes.iter().enumerate() {
        let b = &mut buckets[i % n];
        let iv = InternalValue {
            seq: *seq,
            kind: kind.clone(),
        };
        match b.get(key) {
            Some(prev) if prev.seq >= *seq => {}
            _ => {
                b.insert(key.clone(), iv);
            }
        }
    }
    buckets
        .into_iter()
        .map(|b| source(b.into_iter().collect()))
        .collect()
}

#[test]
fn matches_model_on_handmade_mixed_workload() {
    let writes = vec![
        (k("a"), 1, ValueKind::Value(b"a1".to_vec())),
        (k("b"), 2, ValueKind::Value(b"b1".to_vec())),
        (k("a"), 5, ValueKind::Value(b"a2".to_vec())), // overwrite a
        (k("c"), 3, ValueKind::Value(b"c1".to_vec())),
        (k("b"), 7, ValueKind::Tombstone), // delete b
        (k("d"), 4, ValueKind::Value(b"d1".to_vec())),
        (k("c"), 9, ValueKind::Tombstone),              // delete c
        (k("c"), 11, ValueKind::Value(b"c2".to_vec())), // resurrect c
    ];
    let expected = model_live(&writes);
    for n in 1..=4 {
        let sources = build_sources(&writes, n);
        let got: Vec<_> = MergeIterator::new(sources).live().collect();
        assert_eq!(got, expected, "merge disagreed with model at n={n}");
    }
}

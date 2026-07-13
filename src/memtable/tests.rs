//! Unit + concurrency tests for the memtable set: byte accounting, overwrite
//! semantics, atomic freeze, the frozen-list read guarantee, and freeze racing
//! concurrent readers.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

fn k(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn insert_then_get_newest_wins_within_active() {
    let set = MemtableSet::new(1 << 20);
    set.insert(k("a"), InternalValue::value(1, "one"));
    set.insert(k("a"), InternalValue::value(5, "five"));
    let got = set.get(b"a").expect("present");
    assert_eq!(got.seq, 5);
    assert_eq!(got.as_value(), Some(&b"five"[..]));
}

#[test]
fn tombstone_is_returned_not_hidden() {
    let set = MemtableSet::new(1 << 20);
    set.insert(k("a"), InternalValue::value(1, "one"));
    set.insert(k("a"), InternalValue::tombstone(2));
    let got = set.get(b"a").expect("tombstone still present as a version");
    assert!(got.is_tombstone());
    assert_eq!(got.as_value(), None);
}

#[test]
fn missing_key_is_none() {
    let set = MemtableSet::new(1 << 20);
    assert!(set.get(b"nope").is_none());
}

#[test]
fn overwrite_does_not_double_count_bytes() {
    let mut mt = Memtable::new();
    mt.insert(k("key"), InternalValue::value(1, vec![0u8; 100]));
    let after_first = mt.approx_bytes();
    mt.insert(k("key"), InternalValue::value(2, vec![0u8; 100]));
    // Same key, same-size value: byte count must be unchanged, not doubled.
    assert_eq!(mt.approx_bytes(), after_first);
    assert_eq!(mt.len(), 1);
}

#[test]
fn approx_bytes_grows_with_distinct_keys() {
    let mut mt = Memtable::new();
    assert_eq!(mt.approx_bytes(), 0);
    mt.insert(k("a"), InternalValue::value(1, vec![0u8; 10]));
    let one = mt.approx_bytes();
    mt.insert(k("b"), InternalValue::value(2, vec![0u8; 10]));
    assert!(mt.approx_bytes() > one);
}

#[test]
fn freeze_moves_active_and_installs_empty() {
    let set = MemtableSet::new(1 << 20);
    set.insert(k("a"), InternalValue::value(1, "one"));
    set.insert(k("b"), InternalValue::value(2, "two"));
    let frozen = set.freeze();

    assert_eq!(frozen.len(), 2);
    assert_eq!(set.frozen_count(), 1);
    // New active table is empty, so its byte accounting resets.
    assert_eq!(set.active_bytes(), 0);
    // But reads still see the frozen data.
    assert_eq!(set.get(b"a").unwrap().as_value(), Some(&b"one"[..]));
}

#[test]
fn reads_span_active_and_frozen_newest_first() {
    let set = MemtableSet::new(1 << 20);
    // Old version goes to a table we then freeze.
    set.insert(k("x"), InternalValue::value(1, "old"));
    set.freeze();
    // New version lands in the fresh active table.
    set.insert(k("x"), InternalValue::value(9, "new"));

    let got = set.get(b"x").expect("present");
    assert_eq!(got.seq, 9, "active table must shadow the frozen one");
    assert_eq!(got.as_value(), Some(&b"new"[..]));
}

#[test]
fn frozen_read_guarantee_survives_across_freeze_boundary() {
    // Models the flush window: a key written before a freeze must remain
    // readable from the frozen list until it is explicitly discarded.
    let set = MemtableSet::new(1 << 20);
    set.insert(k("k"), InternalValue::value(3, "v"));
    let frozen = set.freeze();

    // Mid-flush: still readable.
    assert_eq!(set.get(b"k").unwrap().as_value(), Some(&b"v"[..]));

    // Flush completed -> discard. (In the engine the SSTable now covers it.)
    set.discard_frozen(&frozen);
    assert_eq!(set.frozen_count(), 0);
    assert!(set.get(b"k").is_none(), "discarded frozen data is gone");
}

#[test]
fn discard_frozen_removes_only_the_named_table() {
    let set = MemtableSet::new(1 << 20);
    set.insert(k("a"), InternalValue::value(1, "a"));
    let first = set.freeze();
    set.insert(k("b"), InternalValue::value(2, "b"));
    let _second = set.freeze();
    assert_eq!(set.frozen_count(), 2);

    set.discard_frozen(&first);
    assert_eq!(set.frozen_count(), 1);
    // The other frozen table's data is untouched.
    assert_eq!(set.get(b"b").unwrap().as_value(), Some(&b"b"[..]));
    assert!(set.get(b"a").is_none());
}

#[test]
fn freeze_if_full_respects_threshold() {
    // Threshold small enough that a single fat value trips it.
    let set = MemtableSet::new(64);
    set.insert(k("small"), InternalValue::value(1, vec![0u8; 4]));
    assert!(set.freeze_if_full().is_none(), "not full yet");

    set.insert(k("big"), InternalValue::value(2, vec![0u8; 256]));
    assert!(set.is_full());
    let frozen = set.freeze_if_full().expect("full now, must freeze");
    assert!(!frozen.is_empty());
    assert_eq!(set.active_bytes(), 0);
    assert!(set.freeze_if_full().is_none(), "empty active never freezes");
}

#[test]
fn scan_iters_yields_one_source_per_table() {
    let set = MemtableSet::new(1 << 20);
    set.insert(k("a"), InternalValue::value(1, "a"));
    set.freeze();
    set.insert(k("b"), InternalValue::value(2, "b"));
    // active + 1 frozen = 2 sources.
    assert_eq!(set.full_iters().len(), 2);
}

#[test]
fn range_snapshot_is_bounded_and_sorted() {
    let mut mt = Memtable::new();
    for c in ['a', 'b', 'c', 'd', 'e'] {
        mt.insert(vec![c as u8], InternalValue::value(1, "v"));
    }
    let snap = mt.range_snapshot(k("b")..k("d"));
    let keys: Vec<Vec<u8>> = snap.into_iter().map(|(key, _)| key).collect();
    assert_eq!(keys, vec![k("b"), k("c")]);
}

/// Freeze racing a swarm of concurrent readers: readers that keep querying a
/// key written before any freeze must NEVER observe it as missing, no matter
/// where the freeze lands relative to their reads. This is the core "frozen
/// list keeps reads correct mid-flush" invariant, exercised under real threads.
#[test]
fn concurrent_readers_never_miss_key_across_freeze() {
    let set = Arc::new(MemtableSet::new(1 << 30)); // never auto-freezes
    for i in 0..200u32 {
        set.insert(i.to_be_bytes().to_vec(), InternalValue::value(1, "v"));
    }

    let stop = Arc::new(AtomicBool::new(false));
    let misses = Arc::new(AtomicU64::new(0));

    let mut readers = Vec::new();
    for r in 0..8 {
        let set = Arc::clone(&set);
        let stop = Arc::clone(&stop);
        let misses = Arc::clone(&misses);
        readers.push(thread::spawn(move || {
            let probe = ((r * 17) % 200) as u32;
            let key = probe.to_be_bytes().to_vec();
            while !stop.load(Ordering::Relaxed) {
                if set.get(&key).is_none() {
                    misses.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Freeze repeatedly while readers hammer the set.
    for _ in 0..1000 {
        set.freeze();
    }
    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }

    assert_eq!(
        misses.load(Ordering::Relaxed),
        0,
        "a key present before any freeze went missing during a freeze"
    );
}

/// Concurrent writers plus a freezing thread: after everything joins, every key
/// each writer acknowledged must be resolvable. Guards against a freeze losing
/// concurrently-inserted data.
#[test]
fn concurrent_writers_and_freezer_lose_nothing() {
    let set = Arc::new(MemtableSet::new(1 << 30));
    let writers = 4u32;
    let per_writer = 500u32;

    let mut handles = Vec::new();
    for w in 0..writers {
        let set = Arc::clone(&set);
        handles.push(thread::spawn(move || {
            for i in 0..per_writer {
                let key = (w * per_writer + i).to_be_bytes().to_vec();
                set.insert(key, InternalValue::value(1, "v"));
            }
        }));
    }
    // A freezer thread churns the active/frozen split during the writes.
    let freezer = {
        let set = Arc::clone(&set);
        thread::spawn(move || {
            for _ in 0..300 {
                set.freeze();
            }
        })
    };
    for h in handles {
        h.join().unwrap();
    }
    freezer.join().unwrap();

    for w in 0..writers {
        for i in 0..per_writer {
            let key = (w * per_writer + i).to_be_bytes().to_vec();
            assert!(
                set.get(&key).is_some(),
                "key {w}:{i} lost across concurrent freeze"
            );
        }
    }
}

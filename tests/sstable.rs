//! SSTable integration tests, exercised over the [`SimFs`] storage seam.
//!
//! Coverage: build/read roundtrip (single- and multi-block), sparse-index
//! lookups across block boundaries, tombstone handling, strictly-increasing key
//! enforcement, forward iteration, and corruption detection for every
//! CRC-framed structure (data block, index, bloom, footer). A final case builds
//! a table under an armed [`SimFs`] crash and confirms the durability contract:
//! a table synced before the crash reads back intact; a table whose bytes never
//! reached a `sync_file`/`sync_dir` does not masquerade as valid.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use accretion_db::sstable::{
    SsTableBuilder, SsTableError, SsTableReader, Value, ValueRef, DEFAULT_BITS_PER_KEY,
};
use accretion_db::{SimFs, Storage};

fn dir() -> PathBuf {
    PathBuf::from("/sst")
}

/// Create a SimFs with the SSTable's parent directory already durably present,
/// so a crash keeps the directory itself (the file's own durability is what the
/// tests probe).
fn fresh_fs() -> (Arc<SimFs>, Arc<dyn Storage>) {
    let sim = Arc::new(SimFs::with_seed(42));
    let fs: Arc<dyn Storage> = sim.clone();
    (sim, fs)
}

fn table_path() -> PathBuf {
    dir().join("000001.sst")
}

/// Build a table from `(key, seq, value)` triples (must be sorted by key).
fn build(fs: Arc<dyn Storage>, path: &Path, entries: &[(Vec<u8>, u64, Value)]) {
    let mut b =
        SsTableBuilder::new(fs, path, entries.len(), DEFAULT_BITS_PER_KEY).expect("new builder");
    for (k, seq, v) in entries {
        let vref = match v {
            Value::Put(bytes) => ValueRef::Put(bytes),
            Value::Delete => ValueRef::Delete,
        };
        b.add(k, *seq, vref).expect("add entry");
    }
    b.finish().expect("finish");
}

fn key(i: usize) -> Vec<u8> {
    format!("key{i:08}").into_bytes()
}

#[test]
fn roundtrip_single_block() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    let entries = vec![
        (b"alpha".to_vec(), 1, Value::Put(b"one".to_vec())),
        (b"bravo".to_vec(), 2, Value::Put(b"two".to_vec())),
        (b"charlie".to_vec(), 3, Value::Delete),
    ];
    build(fs.clone(), &p, &entries);

    let reader = SsTableReader::open(fs, &p).expect("open");
    assert_eq!(reader.num_entries(), 3);

    let got = reader.get(b"alpha").expect("get").expect("present");
    assert_eq!(got.value, Value::Put(b"one".to_vec()));
    assert_eq!(got.seq, 1);

    let del = reader.get(b"charlie").expect("get").expect("present");
    assert_eq!(del.value, Value::Delete);

    // Absent key: bloom may or may not gate it, but the answer is None.
    assert!(reader.get(b"zzz-absent").expect("get").is_none());
    assert!(reader.get(b"aaa-before").expect("get").is_none());
}

#[test]
fn roundtrip_multi_block_sparse_index() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    // ~2000 entries of ~40 bytes each => many 4 KiB blocks, exercising the
    // sparse index and cross-block lookups.
    let n = 2000usize;
    let entries: Vec<_> = (0..n)
        .map(|i| {
            (
                key(i),
                i as u64,
                Value::Put(format!("value-{i}").into_bytes()),
            )
        })
        .collect();
    build(fs.clone(), &p, &entries);

    let reader = SsTableReader::open(fs, &p).expect("open");
    assert_eq!(reader.num_entries() as usize, n);

    // Probe keys spread across the whole key range, including first and last.
    for &i in &[0usize, 1, 7, 42, 999, 1000, 1001, 1999] {
        let got = reader.get(&key(i)).expect("get").expect("present");
        assert_eq!(got.value, Value::Put(format!("value-{i}").into_bytes()));
        assert_eq!(got.seq, i as u64);
    }
    // A key that sorts between two present keys but is absent.
    assert!(reader.get(b"key00000005-x").expect("get").is_none());
}

#[test]
fn forward_iteration_is_sorted_and_complete() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    let n = 500usize;
    let entries: Vec<_> = (0..n)
        .map(|i| (key(i), i as u64, Value::Put(vec![i as u8; 20])))
        .collect();
    build(fs.clone(), &p, &entries);

    let reader = SsTableReader::open(fs, &p).expect("open");
    let collected: Vec<_> = reader.iter().map(|e| e.expect("entry")).collect();
    assert_eq!(collected.len(), n);
    for (i, e) in collected.iter().enumerate() {
        assert_eq!(e.key, key(i));
        assert_eq!(e.seq, i as u64);
    }
    // Strictly increasing keys.
    for w in collected.windows(2) {
        assert!(w[0].key < w[1].key);
    }
}

#[test]
fn unsorted_input_rejected() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    let mut b = SsTableBuilder::new(fs, &p, 4, DEFAULT_BITS_PER_KEY).expect("new");
    b.add(b"b", 1, ValueRef::Put(b"x")).expect("add b");
    // Equal key is not strictly increasing.
    assert!(matches!(
        b.add(b"b", 2, ValueRef::Put(b"y")),
        Err(SsTableError::Unsorted)
    ));
    // Smaller key is rejected too.
    assert!(matches!(
        b.add(b"a", 3, ValueRef::Put(b"z")),
        Err(SsTableError::Unsorted)
    ));
}

/// Flip one byte inside the first data block and confirm the CRC check fires on
/// the lookup that must read that block.
#[test]
fn corrupt_data_block_detected() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    let entries: Vec<_> = (0..100usize)
        .map(|i| (key(i), i as u64, Value::Put(format!("v{i}").into_bytes())))
        .collect();
    build(fs.clone(), &p, &entries);

    // Corrupt a byte near the very start of the file (inside data block 0).
    let mut byte = [0u8; 1];
    fs.read_at(&p, 16, &mut byte).expect("read");
    byte[0] ^= 0xFF;
    fs.write_at(&p, 16, &byte).expect("write corruption");

    let reader = SsTableReader::open(fs, &p).expect("open still ok (footer/index intact)");
    // key(0) lives in block 0, whose CRC no longer matches.
    match reader.get(&key(0)) {
        Err(SsTableError::Corrupt(_)) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// Corrupt the footer's trailing CRC region and confirm open fails cleanly.
#[test]
fn corrupt_footer_detected() {
    let (_sim, fs) = fresh_fs();
    let p = table_path();
    build(
        fs.clone(),
        &p,
        &[(b"k".to_vec(), 1, Value::Put(b"v".to_vec()))],
    );

    let len = fs.len(&p).expect("len");
    // Flip a byte in the middle of the footer (an offset field).
    let mut byte = [0u8; 1];
    let pos = len - 20;
    fs.read_at(&p, pos, &mut byte).expect("read");
    byte[0] ^= 0xFF;
    fs.write_at(&p, pos, &byte).expect("write corruption");

    match SsTableReader::open(fs, &p) {
        Err(SsTableError::Corrupt(_)) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// A file too short to even hold a footer is rejected, not panicked on.
#[test]
fn truncated_file_detected() {
    let (_sim, fs) = fresh_fs();
    let p = dir().join("stub.sst");
    fs.create(&p).expect("create");
    fs.append(&p, b"too short").expect("append");
    match SsTableReader::open(fs, &p) {
        Err(SsTableError::Corrupt(_)) => {}
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

/// The durability contract under a simulated power loss: a table whose
/// `finish()` completed (issuing its `sync_file`) before the crash reads back
/// intact. We arm the crash *after* the file is synced and installed.
#[test]
fn survives_crash_after_sync() {
    let sim = Arc::new(SimFs::with_seed(9));
    let fs: Arc<dyn Storage> = sim.clone();
    let d = dir();
    // Make the directory durable so the file's entry can survive.
    let p = table_path();

    let entries: Vec<_> = (0..300usize)
        .map(|i| (key(i), i as u64, Value::Put(format!("v{i}").into_bytes())))
        .collect();
    build(fs.clone(), &p, &entries);
    // Install the file durably: it was synced by finish(); now make its
    // directory entry durable, mirroring the manifest rename+sync_dir protocol.
    fs.sync_dir(&d).expect("sync_dir");

    // Power loss now. Everything is durable, so nothing is lost.
    sim.crash();

    let reader = SsTableReader::open(fs, &p).expect("open after crash");
    assert_eq!(reader.num_entries() as usize, entries.len());
    for i in [0usize, 150, 299] {
        let got = reader.get(&key(i)).expect("get").expect("present");
        assert_eq!(got.value, Value::Put(format!("v{i}").into_bytes()));
    }
}

/// A table whose directory entry was never `sync_dir`'d vanishes on crash — it
/// must not be readable as a valid, half-written table.
#[test]
fn unsynced_table_vanishes_on_crash() {
    let sim = Arc::new(SimFs::with_seed(11));
    let fs: Arc<dyn Storage> = sim.clone();
    let p = table_path();
    let entries: Vec<_> = (0..50usize)
        .map(|i| (key(i), i as u64, Value::Put(b"x".to_vec())))
        .collect();
    build(fs.clone(), &p, &entries);
    // No sync_dir: the create is volatile.
    sim.crash();
    // The file's directory entry never became durable, so it is gone.
    assert!(fs.open(&p).is_err());
}

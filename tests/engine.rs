//! End-to-end engine tests: the public [`Db`] API driven over both the deterministic `SimFs` and the real
//! filesystem, exercising the full write path.

use std::path::PathBuf;
use std::sync::Arc;

use accretion_db::db::{Db, Options};
use accretion_db::storage::{SimFs, Storage};
use accretion_db::Durability;

fn dir() -> PathBuf {
    PathBuf::from("/db")
}

/// Small memtable so a handful of writes forces real flushes and compactions.
fn small_opts(durability: Durability) -> Options {
    Options {
        durability,
        memtable_size: 256,
        tier_fanout: 4,
        ..Default::default()
    }
}

fn open_sim(fs: Arc<dyn Storage>, opts: Options) -> Db {
    Db::open_on(fs, &dir(), opts).expect("open db")
}

#[test]
fn put_get_delete_roundtrip_memtable() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
    let db = open_sim(fs, small_opts(Durability::Always));
    db.put(b"alpha", b"1").unwrap();
    db.put(b"beta", b"2").unwrap();
    assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"beta").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);

    db.delete(b"alpha").unwrap();
    assert_eq!(db.get(b"alpha").unwrap(), None);
    // Overwrite wins.
    db.put(b"beta", b"22").unwrap();
    assert_eq!(db.get(b"beta").unwrap(), Some(b"22".to_vec()));
}

#[test]
fn values_survive_flush_to_sstable() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(2));
    let db = open_sim(fs, small_opts(Durability::Always));
    for i in 0..50u32 {
        db.put(
            format!("key{i:04}").as_bytes(),
            format!("val{i}").as_bytes(),
        )
        .unwrap();
    }
    db.flush().unwrap();
    // After a forced flush the memtable is empty; reads must come from tables.
    for i in 0..50u32 {
        assert_eq!(
            db.get(format!("key{i:04}").as_bytes()).unwrap(),
            Some(format!("val{i}").into_bytes()),
            "key{i:04} lost across flush"
        );
    }
}

#[test]
fn overwrites_across_flushes_take_newest() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(3));
    let db = open_sim(fs, small_opts(Durability::Always));
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v3").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn delete_shadows_older_flushed_value() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(4));
    let db = open_sim(fs, small_opts(Durability::Always));
    db.put(b"k", b"live").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        None,
        "tombstone must shadow old value"
    );
}

#[test]
fn scan_yields_sorted_live_pairs() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(5));
    let db = open_sim(fs, small_opts(Durability::Always));
    for i in 0..30u32 {
        db.put(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    db.delete(b"k005").unwrap();
    db.flush().unwrap();
    db.put(b"k010", b"updated").unwrap();

    let got: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().collect();
    // Ascending keys, k005 gone, k010 reflects the newest write.
    assert!(got.windows(2).all(|w| w[0].0 < w[1].0), "scan not sorted");
    assert!(
        !got.iter().any(|(k, _)| k == b"k005"),
        "deleted key present"
    );
    let k010 = got.iter().find(|(k, _)| k == b"k010").unwrap();
    assert_eq!(k010.1, b"updated".to_vec());
    assert_eq!(got.len(), 29, "30 keys, one deleted");
}

#[test]
fn bounded_scan_respects_range() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(6));
    let db = open_sim(fs, small_opts(Durability::Always));
    for i in 0..20u32 {
        db.put(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();
    let lo = b"k005".to_vec();
    let hi = b"k010".to_vec();
    let got: Vec<Vec<u8>> = db.scan(lo..hi).unwrap().map(|(k, _)| k).collect();
    assert_eq!(got.first().unwrap(), b"k005");
    assert_eq!(got.last().unwrap(), b"k009"); // exclusive upper bound
    assert_eq!(got.len(), 5);
}

#[test]
fn reopen_recovers_flushed_and_walled_writes() {
    // Share one SimFs handle across two Db opens: the second sees exactly what
    // the first made durable (no crash here, so all buffered bytes persist too).
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(7));
    {
        let db = open_sim(Arc::clone(&fs), small_opts(Durability::Always));
        for i in 0..40u32 {
            db.put(format!("k{i:03}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // Some data is flushed to tables, the tail is still only in the WAL.
        db.flush().unwrap();
        db.put(b"tail", b"only-in-wal").unwrap();
    }
    let db2 = open_sim(fs, small_opts(Durability::Always));
    for i in 0..40u32 {
        assert_eq!(
            db2.get(format!("k{i:03}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }
    assert_eq!(db2.get(b"tail").unwrap(), Some(b"only-in-wal".to_vec()));
}

#[test]
fn compaction_triggers_and_preserves_data() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(8));
    let db = open_sim(Arc::clone(&fs), small_opts(Durability::Always));
    // Enough distinct keys with forced flushes to build several tiers and force
    // at least one compaction.
    for round in 0..12u32 {
        for i in 0..10u32 {
            let k = format!("k{i:02}");
            db.put(k.as_bytes(), format!("r{round}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }
    // Newest round wins for every key.
    for i in 0..10u32 {
        assert_eq!(
            db.get(format!("k{i:02}").as_bytes()).unwrap(),
            Some(b"r11".to_vec())
        );
    }
    // Survives a reopen (manifest + any remaining WAL).
    drop(db);
    let db2 = open_sim(fs, small_opts(Durability::Always));
    for i in 0..10u32 {
        assert_eq!(
            db2.get(format!("k{i:02}").as_bytes()).unwrap(),
            Some(b"r11".to_vec())
        );
    }
}

#[test]
fn realfs_smoke_roundtrip_and_reopen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("adb");
    {
        let db = Db::open(&path, small_opts(Durability::Always)).expect("open");
        for i in 0..60u32 {
            db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        db.put(b"late", b"buffered").unwrap();
    }
    let db2 = Db::open(&path, small_opts(Durability::Always)).expect("reopen");
    for i in 0..60u32 {
        assert_eq!(
            db2.get(format!("k{i:04}").as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes())
        );
    }
    assert_eq!(db2.get(b"late").unwrap(), Some(b"buffered".to_vec()));
}

// ---- WriteBatch ----

use accretion_db::WriteBatch;

#[test]
fn write_batch_applies_every_op_in_order() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(11));
    let db = open_sim(fs, small_opts(Durability::Always));
    db.put(b"stale", b"old").unwrap();
    let mut batch = WriteBatch::new();
    batch
        .put(b"a", b"1")
        .put(b"b", b"2")
        .delete(b"stale")
        .put(b"a", b"3"); // a later op on the same key wins
    db.write_batch(&batch).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"3".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"stale").unwrap(), None);
}

#[test]
fn empty_write_batch_is_a_noop() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(12));
    let db = open_sim(fs, small_opts(Durability::Always));
    db.write_batch(&WriteBatch::new()).unwrap();
    assert_eq!(db.scan(..).unwrap().count(), 0);
}

#[test]
fn write_batch_survives_reopen_from_wal() {
    for mode in [Durability::Always, Durability::GroupCommit] {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(13));
        {
            let db = open_sim(Arc::clone(&fs), small_opts(mode));
            let mut batch = WriteBatch::new();
            batch.put(b"x", b"1").put(b"y", b"2").delete(b"x");
            db.write_batch(&batch).unwrap();
        }
        let db = open_sim(fs, small_opts(mode));
        assert_eq!(db.get(b"x").unwrap(), None, "{mode:?}");
        assert_eq!(db.get(b"y").unwrap(), Some(b"2".to_vec()), "{mode:?}");
    }
}

#[test]
fn write_batch_larger_than_memtable_flushes_and_reads_back() {
    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(14));
    let db = open_sim(Arc::clone(&fs), small_opts(Durability::Always));
    let mut batch = WriteBatch::new();
    for i in 0..64u32 {
        batch.put(format!("k{i:03}").as_bytes(), format!("v{i:03}").as_bytes());
    }
    db.write_batch(&batch).unwrap();
    // Single puts after the batch must get sequence numbers above every batch op.
    db.put(b"k000", b"after").unwrap();
    db.flush().unwrap();
    drop(db);
    let db = open_sim(fs, small_opts(Durability::Always));
    assert_eq!(db.get(b"k000").unwrap(), Some(b"after".to_vec()));
    for i in 1..64u32 {
        assert_eq!(
            db.get(format!("k{i:03}").as_bytes()).unwrap(),
            Some(format!("v{i:03}").into_bytes())
        );
    }
}

// ---- Index-seeking range scans ----

/// Every bound kind on both ends, against a reference model, over a DB with many SSTables and data
/// blocks, so a seek can land on a block boundary, inside a block, before the first key, or past the
/// last one.
#[test]
fn scan_matches_model_for_every_bound_kind() {
    use std::collections::BTreeMap;
    use std::ops::Bound;

    // Tiny xorshift so the test is deterministic without extra dependencies.
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let key = |n: u64| format!("key{:05}", n % 4000).into_bytes();

    let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(21));
    let opts = Options {
        durability: Durability::OsBuffered,
        memtable_size: 16 * 1024, // several tables, each with several 4 KiB blocks
        tier_fanout: 1000,        // no compaction, so the tables stay separate and overlap
        ..Default::default()
    };
    let db = open_sim(fs, opts);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..6000u64 {
        let k = key(next());
        if next() % 5 == 0 {
            db.delete(&k).unwrap();
            model.remove(&k);
        } else {
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            model.insert(k, v);
        }
    }
    db.flush().unwrap();
    assert!(
        db.debug_version().num_tiers() >= 1,
        "expected data in SSTables"
    );

    let bound = |r: u64, k: Vec<u8>| match r % 3 {
        0 => Bound::Included(k),
        1 => Bound::Excluded(k),
        _ => Bound::Unbounded,
    };
    for _ in 0..3000 {
        // Keys from a slightly wider space than was written, so bounds also fall before the first
        // key, after the last, and between existing keys.
        let a = format!("key{:05}", next() % 4100).into_bytes();
        let b = format!("key{:05}", next() % 4100).into_bytes();
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let range = (bound(next(), lo), bound(next(), hi));
        if let (Bound::Excluded(l), Bound::Excluded(h)) = (&range.0, &range.1) {
            if l == h {
                continue; // BTreeMap::range panics on (Excluded(x), Excluded(x))
            }
        }
        let got: Vec<(Vec<u8>, Vec<u8>)> = db.scan(range.clone()).unwrap().collect();
        let expected: Vec<(Vec<u8>, Vec<u8>)> = model
            .range(range.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(got, expected, "scan({range:?})");
    }

    // Ranges that start or end exactly on a table's boundary keys, which random bounds rarely hit:
    // these pin the table-skipping check at both edges.
    let version = db.debug_version();
    let tables: Vec<_> = version.all_tables().cloned().collect();
    assert!(
        tables.len() >= 2,
        "expected several SSTables, got {}",
        tables.len()
    );
    for t in &tables {
        for range in [
            (Bound::Included(t.last_key.clone()), Bound::Unbounded),
            (Bound::Excluded(t.last_key.clone()), Bound::Unbounded),
            (Bound::Unbounded, Bound::Included(t.first_key.clone())),
            (Bound::Unbounded, Bound::Excluded(t.first_key.clone())),
            (
                Bound::Included(t.first_key.clone()),
                Bound::Included(t.first_key.clone()),
            ),
            (
                Bound::Included(t.last_key.clone()),
                Bound::Included(t.last_key.clone()),
            ),
        ] {
            let got: Vec<(Vec<u8>, Vec<u8>)> = db.scan(range.clone()).unwrap().collect();
            let expected: Vec<(Vec<u8>, Vec<u8>)> = model
                .range(range.clone())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(got, expected, "boundary scan({range:?})");
        }
    }
}

// ---- Background compaction ----

/// Several writer threads against background compaction: every write lands, compaction keeps up
/// (no tier left at or above the fanout once flushed), and reads match a model throughout.
#[test]
fn background_compaction_with_concurrent_writers_matches_model() {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    for mode in [Durability::OsBuffered, Durability::GroupCommit] {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(31));
        let opts = Options {
            durability: mode,
            memtable_size: 512,
            tier_fanout: 4,
            background_compaction: true,
        };
        let db = Arc::new(open_sim(Arc::clone(&fs), opts.clone()));
        let model = Arc::new(Mutex::new(BTreeMap::new()));
        let writers: Vec<_> = (0..4u32)
            .map(|w| {
                let db = Arc::clone(&db);
                let model = Arc::clone(&model);
                std::thread::spawn(move || {
                    for i in 0..600u32 {
                        // Disjoint key ranges per writer, so the model needs no ordering between threads.
                        let key = format!("w{w}-k{:03}", i % 150).into_bytes();
                        if i % 7 == 3 {
                            db.delete(&key).unwrap();
                            model.lock().unwrap().remove(&key);
                        } else {
                            let val = format!("w{w}-v{i}").into_bytes();
                            db.put(&key, &val).unwrap();
                            model.lock().unwrap().insert(key, val);
                        }
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        db.flush().unwrap();
        let version = db.debug_version();
        assert!(version.num_tiers() >= 2, "{mode:?}: compaction never ran");
        for t in 0..version.num_tiers() {
            assert!(
                version.tier_len(t) < 4,
                "{mode:?}: tier {t} left over the fanout"
            );
        }
        let model = model.lock().unwrap();
        let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().collect();
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(scanned, expected, "{mode:?}");
        drop(db);
        // Reopen (inline mode) and check again: what the background thread installed is durable.
        let db = open_sim(
            fs,
            Options {
                background_compaction: false,
                ..opts
            },
        );
        let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().collect();
        assert_eq!(scanned, expected, "{mode:?} after reopen");
    }
}

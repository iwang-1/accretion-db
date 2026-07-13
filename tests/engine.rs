//! End-to-end engine tests: the public [`Db`] API driven over both the
//! deterministic `SimFs` and the real filesystem, exercising the full write path
//! (WAL → memtable → freeze/flush → SSTable + manifest → compaction) and the read
//! path through the merge iterator.
//!
//! These are correctness (not crash) tests; the exhaustive crash sweep and
//! property-based crash schedules land in the dedicated crash suite.

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

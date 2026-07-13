//! `SimFs` behaviour tests that live at the integration level (they only touch
//! the public [`Storage`] surface). The inline unit tests in `src/storage/sim.rs`
//! cover the fine-grained buffered/durable/torn/rename cases; these focus on the
//! property the crash sweep depends on above all else: **determinism**. Given a
//! seed and an identical op sequence, the post-crash image — and its hash — is
//! byte-for-byte reproducible, so a failing schedule can always be replayed.

use std::path::{Path, PathBuf};

use accretion_db::{SimFs, Storage};

fn root() -> PathBuf {
    PathBuf::from("/d")
}

/// Run a fixed mixed workload against a fresh `SimFs`, crash after `crash_after`
/// ops, and return a hash of the entire recovered namespace (every present
/// file's path + bytes). Two runs with the same seed must return the same hash.
fn post_crash_hash(seed: u64, crash_after: u64) -> u64 {
    let fs = SimFs::with_seed(seed);
    fs.arm_crash_after(crash_after);

    // A deterministic mixed workload: creates, appends, syncs, a rename, and a
    // second file — enough surface for the crash to make interesting choices.
    let a = root().join("a");
    let b = root().join("b");
    let c = root().join("c");
    let _ = fs.create(&a);
    let _ = fs.sync_dir(&root());
    let _ = fs.append(&a, b"alpha-payload");
    let _ = fs.sync_file(&a);
    let _ = fs.append(&a, b"-unsynced-tail");
    let _ = fs.create(&b);
    let _ = fs.sync_dir(&root());
    let _ = fs.append(&b, b"bravo");
    let _ = fs.sync_file(&b);
    let _ = fs.rename(&b, &c);
    let _ = fs.append(&a, b"-more");

    // If the armed crash has not yet fired, force one so both runs converge.
    if fs.last_report().is_none() {
        fs.crash();
    }
    hash_namespace(&fs)
}

/// Hash the full process-visible namespace after a crash: sorted (path, bytes).
fn hash_namespace(fs: &SimFs) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut entries = fs.list(root().as_path()).unwrap_or_default();
    entries.sort();
    for p in entries {
        hasher.update(p.to_string_lossy().as_bytes());
        hasher.update(&read_all(fs, &p));
        hasher.update(&[0xff]); // record separator
    }
    hasher.digest()
}

fn read_all(fs: &SimFs, p: &Path) -> Vec<u8> {
    let len = fs.len(p).unwrap_or(0) as usize;
    let mut buf = vec![0u8; len];
    let n = fs.read_at(p, 0, &mut buf).unwrap_or(0);
    buf.truncate(n);
    buf
}

/// Same seed + same op sequence + same crash point ⇒ identical recovered image.
#[test]
fn crash_is_deterministic_across_runs() {
    for crash_after in 1..=11u64 {
        let h1 = post_crash_hash(1234, crash_after);
        let h2 = post_crash_hash(1234, crash_after);
        assert_eq!(
            h1, h2,
            "post-crash namespace hash differed between identical runs at crash_after={crash_after}"
        );
    }
}

/// Different seeds should (at least sometimes) diverge on a torn tail — proving
/// the RNG genuinely drives the tear decision rather than being ignored. We
/// crash at a point where an unsynced tail exists and assert not all seeds agree.
#[test]
fn seed_influences_torn_tail() {
    // crash_after = 11 crashes with a's "-more" tail unsynced (the last append).
    let hashes: Vec<u64> = (0..64u64).map(|s| post_crash_hash(s, 11)).collect();
    let first = hashes[0];
    assert!(
        hashes.iter().any(|&h| h != first),
        "no seed diverged: the crash RNG is not influencing the torn tail"
    );
}

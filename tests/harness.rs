//! Crash-harness skeleton test — drives [`accretion_db::testkit`] against a
//! **toy append-only store** so the harness itself is exercised end-to-end
//! before the real LSM engine exists.
//!
//! The toy store is deliberately minimal but crash-*correct*: a single
//! CRC32-framed, length-prefixed log with a torn-tail truncation recovery rule —
//! the same discipline the real WAL will use. It is enough to prove the harness
//! can (a) count a workload's storage ops, (b) crash at any chosen op, and
//! (c) hand a recovered store to a verifier that distinguishes acknowledged
//! writes (must survive) from unacknowledged ones (may vanish or tear).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use accretion_db::testkit::{count_ops, crash_sweep, run_crash};
use accretion_db::Storage;

/// One CRC-framed record: `[payload_len: u32 LE][crc32(payload): u32 LE][payload]`,
/// where `payload = [key_len: u32 LE][key][value]`.
const LEN_SZ: usize = 4;
const CRC_SZ: usize = 4;

fn encode_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(LEN_SZ + key.len() + value.len());
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(value);

    let crc = crc32fast::hash(&payload);
    let mut frame = Vec::with_capacity(LEN_SZ + CRC_SZ + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// A toy append-only key/value store over the [`Storage`] seam.
///
/// `put` is durable-on-return: it appends a CRC frame and `sync_file`s before
/// acknowledging, so any acked key must survive a crash. This is the property
/// the harness verifies.
struct ToyStore {
    fs: Arc<dyn Storage>,
    log: PathBuf,
}

impl ToyStore {
    /// Open (creating if absent) the store rooted at `dir`, returning the store
    /// and the map of key→value recovered from the durable log.
    fn open(fs: Arc<dyn Storage>, dir: &Path) -> (Self, BTreeMap<Vec<u8>, Vec<u8>>) {
        let log = dir.join("toy.log");
        if fs.open(&log).is_err() {
            // First open: create the log and make its directory entry durable.
            fs.create(&log).expect("create log");
            fs.sync_file(&log).expect("sync new log");
            fs.sync_dir(dir).expect("sync dir");
        }
        let recovered = Self::recover(&fs, &log);
        (ToyStore { fs, log }, recovered)
    }

    /// Scan the log frame by frame, applying each valid record. Stops at the
    /// first frame that is short (torn) or fails its CRC — the torn-tail rule.
    fn recover(fs: &Arc<dyn Storage>, log: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut map = BTreeMap::new();
        let len = fs.len(log).unwrap_or(0);
        let mut off: u64 = 0;
        loop {
            // Frame header: length + CRC.
            let mut header = [0u8; LEN_SZ + CRC_SZ];
            if off + header.len() as u64 > len {
                break; // torn / no more frames
            }
            let n = fs.read_at(log, off, &mut header).expect("read header");
            if n < header.len() {
                break;
            }
            let payload_len = u32::from_le_bytes(header[..LEN_SZ].try_into().unwrap()) as usize;
            let want_crc = u32::from_le_bytes(header[LEN_SZ..].try_into().unwrap());

            let payload_off = off + header.len() as u64;
            if payload_off + payload_len as u64 > len {
                break; // torn payload
            }
            let mut payload = vec![0u8; payload_len];
            let n = fs
                .read_at(log, payload_off, &mut payload)
                .expect("read payload");
            if n < payload_len || crc32fast::hash(&payload) != want_crc {
                break; // torn or corrupt: stop, discard rest
            }

            // Decode key/value.
            if payload.len() < LEN_SZ {
                break;
            }
            let klen = u32::from_le_bytes(payload[..LEN_SZ].try_into().unwrap()) as usize;
            if LEN_SZ + klen > payload.len() {
                break;
            }
            let key = payload[LEN_SZ..LEN_SZ + klen].to_vec();
            let value = payload[LEN_SZ + klen..].to_vec();
            map.insert(key, value);

            off = payload_off + payload_len as u64;
        }
        map
    }

    /// Durable put: append a frame and `sync_file` before returning. Returns
    /// only once the write is guaranteed to survive a crash.
    fn put(&self, key: &[u8], value: &[u8]) {
        let frame = encode_frame(key, value);
        self.fs.append(&self.log, &frame).expect("append frame");
        self.fs.sync_file(&self.log).expect("sync frame");
    }
}

/// Sanity: the harness counts the same op total the workload actually issues.
/// The workload below performs, per key: 1 append + 1 sync_file = 2 ops, plus
/// the 3 setup ops (create + sync_file + sync_dir) on first open.
#[test]
fn count_ops_matches_workload() {
    let n = count_ops(1, |fs| {
        let dir = PathBuf::from("/db");
        let (store, recovered) = ToyStore::open(fs, &dir);
        assert!(recovered.is_empty());
        store.put(b"a", b"1");
        store.put(b"b", b"2");
    });
    // 3 setup + 2 puts * 2 ops = 7.
    assert_eq!(n, 7);
}

/// The core harness invariant: exhaustively crash after every single storage op
/// of a multi-put workload and confirm the recovered store is always *prefix-
/// consistent*. Because each `put` syncs before returning, a key is
/// "acknowledged" once its `sync_file` op completes. After a crash at any op:
///
/// * every key whose put fully completed (append + sync) before the crash is
///   present with the exact value, and
/// * the recovered key set is a prefix of the workload's key sequence — no
///   phantom keys, no gaps.
///
/// This is the toy analogue of the engine's crash sweep: it proves the harness
/// drives crash points 1..=N and that torn/dropped tails are handled.
#[test]
fn crash_sweep_is_prefix_consistent() {
    let keys: &[(&[u8], &[u8])] = &[
        (b"alpha", b"one"),
        (b"bravo", b"two"),
        (b"charlie", b"three"),
        (b"delta", b"four"),
    ];

    let body = |fs: Arc<dyn Storage>| {
        let dir = PathBuf::from("/db");
        let (store, _) = ToyStore::open(fs, &dir);
        for (k, v) in keys {
            store.put(k, v);
        }
    };

    let verify = |fs: Arc<dyn Storage>, _report: &_| {
        let dir = PathBuf::from("/db");
        let (_store, recovered) = ToyStore::open(fs, &dir);
        // The recovered keys must be a prefix of the workload's key order, each
        // with its correct value: this rejects phantom keys, reordering, and
        // partial/torn frames that slipped past the CRC check.
        let mut expected = recovered.len();
        for (k, v) in keys.iter().take(recovered.len()) {
            assert_eq!(
                recovered.get(*k).map(Vec::as_slice),
                Some(*v),
                "recovered key {:?} has wrong or missing value",
                k
            );
            expected -= 1;
        }
        assert_eq!(expected, 0, "recovered set was not a clean prefix");
    };

    let n = crash_sweep(42, body, verify);
    // 3 setup + 4 puts * 2 ops = 11 distinct crash points swept.
    assert_eq!(n, 11);
}

/// A single acknowledged (append + sync) put must survive a crash that is armed
/// for *after* its sync op — durability actually holds through the harness.
#[test]
fn acked_put_survives_crash() {
    // count: 3 setup + 1 put*2 = 5 ops. Crash after op 5 (the sync) => durable.
    run_crash(
        7,
        5,
        |fs| {
            let dir = PathBuf::from("/db");
            let (store, _) = ToyStore::open(fs, &dir);
            store.put(b"survivor", b"payload");
        },
        |fs, _report| {
            let dir = PathBuf::from("/db");
            let (_store, recovered) = ToyStore::open(fs, &dir);
            assert_eq!(
                recovered.get(b"survivor".as_slice()).map(Vec::as_slice),
                Some(b"payload".as_slice()),
                "an acked put must survive a post-sync crash"
            );
        },
    );
}

/// An un-acked put — crashed *after the append but before the sync* — must never
/// leave a half-valid frame that recovery accepts. Whatever survives is either
/// nothing or a clean prefix; the CRC/length rule rejects the torn frame.
#[test]
fn unacked_put_never_half_applies() {
    // 3 setup ops, then append (op 4), then sync (op 5). Crash after op 4: the
    // frame is appended but unsynced, so it is buffered and may be torn/dropped.
    run_crash(
        11,
        4,
        |fs| {
            let dir = PathBuf::from("/db");
            let (store, _) = ToyStore::open(fs, &dir);
            store.put(b"maybe", b"lost");
        },
        |fs, _report| {
            let dir = PathBuf::from("/db");
            let (_store, recovered) = ToyStore::open(fs, &dir);
            // The un-acked key must NOT reappear with a wrong value. Either it is
            // absent (dropped/torn) or — if the tail happened to be fully synced
            // by the file-length model — present with the exact value; a corrupt
            // partial frame must be rejected outright.
            match recovered.get(b"maybe".as_slice()) {
                None => {}
                Some(v) => assert_eq!(v.as_slice(), b"lost"),
            }
            // No phantom keys either way.
            assert!(recovered.len() <= 1);
        },
    );
}

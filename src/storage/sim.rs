//! [`SimFs`] — the deterministic power-loss simulator.
//!
//! `SimFs` is the heart of the crash-consistency harness. It implements the
//! same [`Storage`](super::Storage) trait as [`RealFs`](super::RealFs), but
//! instead of talking to the kernel it maintains an in-memory *page-cache
//! model*:
//!
//! * Every mutating byte range is **buffered** (visible to this process) until a
//!   covering [`sync_file`](Storage::sync_file) promotes it to **durable**.
//! * A [`rename`](Storage::rename) / [`create`](Storage::create) /
//!   [`delete`](Storage::delete) is likewise **volatile** — visible now, but the
//!   directory-entry change does not survive a crash until a
//!   [`sync_dir`](Storage::sync_dir) on the parent directory.
//! * [`crash`](SimFs::crash) discards everything that has not been made durable
//!   and, per a seeded [`StdRng`], **tears** the most recent unsynced append at
//!   a random byte boundary (or drops it, or flips a bit inside it). Given the
//!   seed the outcome is fully deterministic and reproducible.
//!
//! A monotonic op counter lets the future exhaustive sweep arm a crash *after
//! op #i* deterministically ([`arm_crash_after`](SimFs::arm_crash_after)).
//!
//! # What is modelled — and what is not
//!
//! Modelled: loss of unsynced data, a torn/dropped unsynced append, and a
//! volatile directory entry that reverts to its last durably-synced name. **Not**
//! modelled: cross-file sector reordering, partial-sector atomicity below the
//! byte level, or media decay of already-durable data. The engine is only ever
//! allowed to depend on the guarantees this model makes.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::{Storage, StorageError, StorageResult};

/// How a crash mangles the most recent unsynced append.
///
/// The variant chosen for a given crash is decided by the seeded RNG and
/// reported in the [`CrashReport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TearMode {
    /// The unsynced tail is dropped in full (the file reverts to its last
    /// synced length).
    #[default]
    Drop,
    /// The unsynced tail is truncated at a seeded random byte boundary,
    /// leaving a partial prefix — the classic "torn write" a WAL must survive.
    Truncate,
    /// A single bit inside the unsynced tail is flipped, leaving the length
    /// intact but the bytes corrupt — a CRC check must catch this.
    BitFlip,
}

/// Configuration for a [`SimFs`] instance.
#[derive(Debug, Clone, Default)]
pub struct SimConfig {
    /// Seed for the deterministic RNG that drives every crash decision.
    pub seed: u64,
}

/// A summary of what a simulated [`crash`](SimFs::crash) did.
///
/// Everything here is a deterministic function of the [`SimConfig::seed`] and
/// the exact sequence of operations that preceded the crash, so a failing crash
/// schedule can be replayed byte-for-byte.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrashReport {
    /// Number of mutating storage ops observed before the crash.
    pub ops_before_crash: u64,
    /// The file whose unsynced tail was subjected to tearing, if any.
    pub torn_path: Option<PathBuf>,
    /// How the tail of [`torn_path`](CrashReport::torn_path) was mangled.
    pub tear_mode: TearMode,
    /// Length of the unsynced tail that existed at crash time (bytes).
    pub tail_len: u64,
    /// How many bytes of that tail survived the crash (bytes). Equal to
    /// `tail_len` for [`TearMode::BitFlip`], `0` for [`TearMode::Drop`], and a
    /// random prefix for [`TearMode::Truncate`].
    pub tail_kept: u64,
}

/// The per-file page-cache model: `live` is what the process sees, `durable` is
/// what would survive a crash right now. Appends and overwrites mutate `live`;
/// only [`sync_file`](Storage::sync_file) copies `live` into `durable`.
#[derive(Debug, Clone, Default)]
struct FileState {
    /// Bytes visible to the process (buffered + durable).
    live: Vec<u8>,
    /// Bytes guaranteed to survive a crash (last synced image).
    durable: Vec<u8>,
    /// Whether the path currently exists in the process-visible namespace.
    present_live: bool,
    /// Whether the path exists in the durable namespace (survives a crash).
    present_durable: bool,
}

/// The mutable interior of a [`SimFs`], guarded by a single [`Mutex`].
#[derive(Debug)]
struct SimState {
    /// The flat namespace, keyed by path. Sorted for deterministic `list`.
    files: BTreeMap<PathBuf, FileState>,
    /// Monotonic count of every mutating op (drives crash scheduling).
    op_count: u64,
    /// Path of the most recent append; the tear target on the next crash.
    last_append: Option<PathBuf>,
    /// When `Some(n)`, a crash fires automatically once `op_count` reaches `n`.
    crash_after: Option<u64>,
    /// The report from the most recent crash, if one has occurred.
    last_report: Option<CrashReport>,
    /// Deterministic RNG driving crash tear decisions.
    rng: StdRng,
}

/// A deterministic, seeded power-loss simulator implementing [`Storage`].
///
/// Cheap to construct and `Send + Sync`; share it as `Arc<dyn Storage>` exactly
/// like [`RealFs`](super::RealFs). See the [module docs](self) for the fault
/// model.
#[derive(Debug)]
pub struct SimFs {
    state: Mutex<SimState>,
}

impl SimFs {
    /// Construct a `SimFs` from an explicit [`SimConfig`].
    pub fn new(config: SimConfig) -> Self {
        SimFs {
            state: Mutex::new(SimState {
                files: BTreeMap::new(),
                op_count: 0,
                last_append: None,
                crash_after: None,
                last_report: None,
                rng: StdRng::seed_from_u64(config.seed),
            }),
        }
    }

    /// Construct a `SimFs` with the given RNG seed and otherwise default config.
    pub fn with_seed(seed: u64) -> Self {
        SimFs::new(SimConfig { seed })
    }
}

impl Default for SimFs {
    fn default() -> Self {
        SimFs::new(SimConfig::default())
    }
}

impl SimFs {
    /// Number of mutating ops applied so far. `sync_file`/`sync_dir` count too:
    /// they are durability barriers the sweep may want to crash immediately
    /// after.
    pub fn op_count(&self) -> u64 {
        self.state.lock().expect("simfs poisoned").op_count
    }

    /// Arm an automatic [`crash`](SimFs::crash) to fire the instant `op_count`
    /// reaches `n`. This is the deterministic "crash after op #n" hook the
    /// exhaustive sweep drives. Passing an already-reached `n` fires on the next
    /// mutating op; the arm is one-shot and disarms itself when it fires.
    pub fn arm_crash_after(&self, n: u64) {
        self.state.lock().expect("simfs poisoned").crash_after = Some(n);
    }

    /// The [`CrashReport`] from the most recent [`crash`](SimFs::crash), if one
    /// has occurred (including one fired by [`arm_crash_after`]).
    ///
    /// [`arm_crash_after`]: SimFs::arm_crash_after
    pub fn last_report(&self) -> Option<CrashReport> {
        self.state
            .lock()
            .expect("simfs poisoned")
            .last_report
            .clone()
    }

    /// Simulate a power loss: discard all buffered (unsynced) state, revert the
    /// namespace to its last durably-synced image, and tear the most recent
    /// unsynced append per the seeded RNG. Returns a [`CrashReport`] describing
    /// exactly what happened, which is also stored for [`last_report`].
    ///
    /// After this returns, `SimFs` is in the state a freshly rebooted machine
    /// would see: reads observe only what survived. It is deterministic given
    /// the seed and the preceding op sequence.
    ///
    /// [`last_report`]: SimFs::last_report
    pub fn crash(&self) -> CrashReport {
        let mut st = self.state.lock().expect("simfs poisoned");
        Self::crash_locked(&mut st)
    }

    /// The crash mechanism, operating on already-locked state so it can be
    /// invoked both from [`crash`](SimFs::crash) and from the armed auto-crash
    /// hook without re-locking.
    fn crash_locked(st: &mut SimState) -> CrashReport {
        let ops_before_crash = st.op_count;

        // Identify the tear target: the most recent append, but only if its
        // file will still exist on disk (its directory entry is durable) and it
        // actually carries unsynced tail bytes. A file whose create was never
        // dir-synced vanishes wholesale — there is no torn tail to model.
        let mut report = CrashReport {
            ops_before_crash,
            ..Default::default()
        };
        let target = st.last_append.clone().filter(|p| {
            st.files
                .get(p)
                .map(|f| f.present_durable && f.live.len() > f.durable.len())
                .unwrap_or(false)
        });

        if let Some(path) = target {
            let f = st.files.get(&path).expect("target present");
            let tail_len = (f.live.len() - f.durable.len()) as u64;
            let tail: Vec<u8> = f.live[f.durable.len()..].to_vec();
            // Choose how the tail is mangled. Weighted toward Drop/Truncate,
            // the physically common outcomes; BitFlip exercises the CRC path.
            let mode = match st.rng.gen_range(0u8..3) {
                0 => TearMode::Drop,
                1 => TearMode::Truncate,
                _ => TearMode::BitFlip,
            };
            let mut kept = f.durable.clone();
            let tail_kept: u64 = match mode {
                TearMode::Drop => 0,
                TearMode::Truncate => {
                    // A torn write keeps a prefix of the tail: [0, tail_len].
                    let k = st.rng.gen_range(0..=tail_len as usize);
                    kept.extend_from_slice(&tail[..k]);
                    k as u64
                }
                TearMode::BitFlip => {
                    kept.extend_from_slice(&tail);
                    // Flip one bit somewhere in the unsynced tail region.
                    let bit = st.rng.gen_range(0..(tail_len * 8)) as usize;
                    let byte = f.durable.len() + bit / 8;
                    kept[byte] ^= 1 << (bit % 8);
                    tail_len
                }
            };
            report.torn_path = Some(path.clone());
            report.tear_mode = mode;
            report.tail_len = tail_len;
            report.tail_kept = tail_kept;
            let fs = st.files.get_mut(&path).expect("target present");
            fs.durable = kept;
        }

        // Revert every file to its durable image: buffered data and volatile
        // directory-entry changes are lost.
        for f in st.files.values_mut() {
            f.live = f.durable.clone();
            f.present_live = f.present_durable;
        }
        // Files that never became durably present disappear entirely.
        st.files.retain(|_, f| f.present_durable);

        st.last_append = None;
        st.crash_after = None;
        st.last_report = Some(report.clone());
        report
    }

    /// Count one mutating op and, if an [`arm_crash_after`](SimFs::arm_crash_after)
    /// threshold has now been reached, fire the crash immediately. Called at the
    /// end of every mutating [`Storage`] method after its effect is applied, so
    /// "crash after op #n" means op #n's effect existed in the page cache the
    /// instant power was lost.
    fn bump(st: &mut SimState) {
        st.op_count += 1;
        if matches!(st.crash_after, Some(n) if st.op_count >= n) {
            Self::crash_locked(st);
        }
    }
}

/// Return `true` if `path` is a direct child of `dir` (same parent).
fn is_child_of(path: &Path, dir: &Path) -> bool {
    path.parent() == Some(dir)
}

impl Storage for SimFs {
    fn create(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let entry = st.files.entry(path.to_path_buf()).or_default();
        if entry.present_live {
            return Err(StorageError::AlreadyExists(path.to_path_buf()));
        }
        // A fresh, empty, process-visible file. Its directory entry is not
        // durable until a `sync_dir` on the parent, so `present_durable` and
        // `durable` are left untouched.
        entry.present_live = true;
        entry.live.clear();
        Self::bump(&mut st);
        Ok(())
    }

    fn open(&self, path: &Path) -> StorageResult<()> {
        let st = self.state.lock().expect("simfs poisoned");
        match st.files.get(path) {
            Some(f) if f.present_live => Ok(()),
            _ => Err(StorageError::NotFound(path.to_path_buf())),
        }
    }

    fn append(&self, path: &Path, data: &[u8]) -> StorageResult<u64> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let offset = {
            let f = st
                .files
                .get_mut(path)
                .filter(|f| f.present_live)
                .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
            let offset = f.live.len() as u64;
            f.live.extend_from_slice(data);
            offset
        };
        // The tear target on the next crash is always the most recent append.
        st.last_append = Some(path.to_path_buf());
        Self::bump(&mut st);
        Ok(offset)
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        {
            let f = st
                .files
                .get_mut(path)
                .filter(|f| f.present_live)
                .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
            let len = f.live.len() as u64;
            if offset > len || offset + data.len() as u64 > len {
                return Err(StorageError::OutOfBounds {
                    path: path.to_path_buf(),
                    offset,
                    len,
                });
            }
            let start = offset as usize;
            f.live[start..start + data.len()].copy_from_slice(data);
        }
        Self::bump(&mut st);
        Ok(())
    }

    fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let st = self.state.lock().expect("simfs poisoned");
        let f = st
            .files
            .get(path)
            .filter(|f| f.present_live)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        let len = f.live.len() as u64;
        if offset > len {
            return Err(StorageError::OutOfBounds {
                path: path.to_path_buf(),
                offset,
                len,
            });
        }
        let start = offset as usize;
        let n = buf.len().min(f.live.len() - start);
        buf[..n].copy_from_slice(&f.live[start..start + n]);
        Ok(n)
    }

    fn sync_file(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        {
            let f = st
                .files
                .get_mut(path)
                .filter(|f| f.present_live)
                .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
            // Promote every buffered byte to durable. The directory entry's
            // durability is a separate concern (`sync_dir`), so `present_durable`
            // is intentionally not set here.
            f.durable = f.live.clone();
        }
        // This file's tail is now durable, so it is no longer a tear target.
        if st.last_append.as_deref() == Some(path) {
            st.last_append = None;
        }
        Self::bump(&mut st);
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        for (p, f) in st.files.iter_mut() {
            if !is_child_of(p, dir) {
                continue;
            }
            if f.present_live {
                // A newly-linked entry (create/rename target) adopts its current
                // content as durable. The engine's contract is to `sync_file`
                // before renaming, so `live` is the already-durable image; see
                // the module docs for this modeled boundary.
                if !f.present_durable {
                    f.durable = f.live.clone();
                }
                f.present_durable = true;
            } else {
                // An unlink (delete/rename source) becomes durable.
                f.present_durable = false;
            }
        }
        // Drop entries that are now absent in both namespaces.
        st.files.retain(|_, f| f.present_live || f.present_durable);
        Self::bump(&mut st);
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let src = st
            .files
            .get(from)
            .filter(|f| f.present_live)
            .ok_or_else(|| StorageError::NotFound(from.to_path_buf()))?;
        let bytes = src.live.clone();
        // Link `to` at the source's current content; unlink `from`. Both changes
        // are volatile in the live namespace until a `sync_dir` on the parent.
        let dst = st.files.entry(to.to_path_buf()).or_default();
        dst.present_live = true;
        dst.live = bytes;
        st.files.get_mut(from).expect("source present").present_live = false;
        Self::bump(&mut st);
        Ok(())
    }

    fn delete(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        {
            let f = st
                .files
                .get_mut(path)
                .filter(|f| f.present_live)
                .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
            // Volatile until a `sync_dir`: the entry is gone from the process
            // view but survives a crash unless the unlink was made durable.
            f.present_live = false;
            f.live.clear();
        }
        Self::bump(&mut st);
        Ok(())
    }

    fn list(&self, dir: &Path) -> StorageResult<Vec<PathBuf>> {
        let st = self.state.lock().expect("simfs poisoned");
        let out: Vec<PathBuf> = st
            .files
            .iter()
            .filter(|(p, f)| f.present_live && is_child_of(p, dir))
            .map(|(p, _)| p.clone())
            .collect();
        // `files` is a BTreeMap, so iteration — and thus this list — is sorted.
        Ok(out)
    }

    fn len(&self, path: &Path) -> StorageResult<u64> {
        let st = self.state.lock().expect("simfs poisoned");
        st.files
            .get(path)
            .filter(|f| f.present_live)
            .map(|f| f.live.len() as u64)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> PathBuf {
        PathBuf::from("/d")
    }

    fn setup_synced_file(fs: &SimFs, name: &str) -> PathBuf {
        let p = root().join(name);
        fs.create(&p).expect("create");
        // Make the file's existence durable so a crash keeps the file itself.
        fs.sync_dir(&root()).expect("sync_dir");
        p
    }

    fn read_all(fs: &SimFs, p: &Path) -> Vec<u8> {
        let len = fs.len(p).expect("len") as usize;
        let mut buf = vec![0u8; len];
        let n = fs.read_at(p, 0, &mut buf).expect("read");
        buf.truncate(n);
        buf
    }

    /// Buffered bytes are visible to the process but do not survive a crash
    /// until a covering `sync_file` promotes them to durable.
    #[test]
    fn buffered_vs_durable_promotion() {
        let fs = SimFs::with_seed(1);
        let p = setup_synced_file(&fs, "wal");

        fs.append(&p, b"durable-").expect("append 1");
        fs.sync_file(&p).expect("sync"); // promote "durable-" to durable
        fs.append(&p, b"buffered").expect("append 2"); // stays buffered

        // In-process, both parts are visible.
        assert_eq!(read_all(&fs, &p), b"durable-buffered");

        // Force a full drop of the unsynced tail (Drop is a possible outcome;
        // assert against the reported mode rather than assuming it).
        let report = fs.crash();
        // ops: create, sync_dir, append, sync_file, append.
        assert_eq!(report.ops_before_crash, 5);
        // Only the synced prefix can survive; a torn tail never exceeds it.
        let survived = read_all(&fs, &p);
        assert!(survived.starts_with(b"durable-"));
        assert!(survived.len() >= b"durable-".len());
        // Whatever survived beyond the prefix is a torn fragment of "buffered".
        assert!(survived.len() <= b"durable-buffered".len());
    }

    /// A `Truncate` crash keeps a prefix of the unsynced tail — the torn-write
    /// case a CRC-framed WAL must detect and truncate. We search seeds for a
    /// deterministic Truncate outcome and verify the survivor is a clean prefix.
    #[test]
    fn torn_append_keeps_prefix() {
        // Find a seed that yields a partial (non-empty, non-full) truncation.
        for seed in 0..2000u64 {
            let fs = SimFs::with_seed(seed);
            let p = setup_synced_file(&fs, "wal");
            fs.append(&p, b"0123456789").expect("append");
            let r = fs.crash();
            if r.tear_mode == TearMode::Truncate && r.tail_kept > 0 && r.tail_kept < r.tail_len {
                let survived = read_all(&fs, &p);
                assert_eq!(survived.len() as u64, r.tail_kept);
                assert_eq!(&survived[..], &b"0123456789"[..r.tail_kept as usize]);
                return;
            }
        }
        panic!("no seed produced a partial torn append in range");
    }

    /// A file whose creation was never `sync_dir`'d vanishes entirely on crash:
    /// its directory entry was volatile.
    #[test]
    fn volatile_create_vanishes_on_crash() {
        let fs = SimFs::with_seed(7);
        let p = root().join("ghost");
        fs.create(&p).expect("create");
        fs.append(&p, b"data").expect("append");
        // No sync_dir, so the entry is volatile.
        fs.crash();
        assert!(matches!(fs.open(&p), Err(StorageError::NotFound(_))));
        assert!(fs.list(&root()).expect("list").is_empty());
    }

    /// `arm_crash_after(n)` fires deterministically once the op count reaches n,
    /// leaving a retrievable report — the exhaustive-sweep scheduling hook.
    #[test]
    fn armed_crash_fires_at_op_n() {
        let fs = SimFs::with_seed(3);
        let p = setup_synced_file(&fs, "log"); // 2 ops: create, sync_dir
        assert_eq!(fs.op_count(), 2);
        fs.arm_crash_after(3); // fire on the very next mutating op
        fs.append(&p, b"xyz").expect("append"); // op 3 -> triggers crash
        let report = fs.last_report().expect("armed crash produced a report");
        assert_eq!(report.ops_before_crash, 3);
    }

    /// A rename is volatile until the parent is `sync_dir`'d: a crash reverts to
    /// the last durable directory image.
    #[test]
    fn rename_volatile_until_dir_sync() {
        let fs = SimFs::with_seed(5);
        let a = setup_synced_file(&fs, "a");
        fs.append(&a, b"payload").expect("append");
        fs.sync_file(&a).expect("sync data durable");
        let b = root().join("b");
        fs.rename(&a, &b).expect("rename");
        // Crash before sync_dir: the rename reverts, "a" comes back.
        fs.crash();
        fs.open(&a).expect("a survives the volatile rename");
        assert!(matches!(fs.open(&b), Err(StorageError::NotFound(_))));
        assert_eq!(read_all(&fs, &a), b"payload");
    }
}

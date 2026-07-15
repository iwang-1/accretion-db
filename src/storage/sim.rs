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

use std::collections::{BTreeMap, BTreeSet};
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

/// The page-cache state for one inode generation. Directory entries refer to
/// these generations independently in the live and durable namespaces.
#[derive(Debug, Clone, Default)]
struct InodeState {
    /// Bytes visible to the process (buffered + durable).
    live: Vec<u8>,
    /// Bytes guaranteed to survive a crash (last synced image).
    durable: Vec<u8>,
    /// Order of this inode's newest append not yet covered by `sync_file`.
    last_append_seq: Option<u64>,
}

/// The two namespace images for a path. A delete followed by a recreate can
/// point `live_inode` at a new generation while `durable_inode` still points at
/// the old one until the parent directory is synced.
#[derive(Debug, Clone, Default)]
struct PathState {
    /// Inode generation visible to the running process.
    live_inode: Option<u64>,
    /// Inode generation that the path resolves to after a crash.
    durable_inode: Option<u64>,
}

/// The mutable interior of a [`SimFs`], guarded by a single [`Mutex`].
#[derive(Debug)]
struct SimState {
    /// The flat namespace, keyed by path. Sorted for deterministic `list`.
    files: BTreeMap<PathBuf, PathState>,
    /// Inode generations referenced by the live or durable namespace.
    inodes: BTreeMap<u64, InodeState>,
    /// Monotonic inode-generation allocator.
    next_inode: u64,
    /// Monotonic count of every mutating op (drives crash scheduling).
    op_count: u64,
    /// Monotonic ordering for append tear candidates across inode generations.
    next_append_seq: u64,
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
                inodes: BTreeMap::new(),
                next_inode: 1,
                op_count: 0,
                next_append_seq: 0,
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
        let target = st
            .inodes
            .iter()
            .filter_map(|(&inode_id, inode)| {
                let seq = inode.last_append_seq?;
                let is_durably_referenced = st
                    .files
                    .values()
                    .any(|entry| entry.durable_inode == Some(inode_id));
                (inode.live.len() > inode.durable.len() && is_durably_referenced)
                    .then_some((seq, inode_id))
            })
            .max_by_key(|(seq, _)| *seq)
            .and_then(|(_, inode_id)| {
                st.files.iter().find_map(|(path, entry)| {
                    (entry.durable_inode == Some(inode_id)).then(|| (path.clone(), inode_id))
                })
            });

        if let Some((path, inode_id)) = target {
            let inode = st.inodes.get(&inode_id).expect("target inode present");
            let durable_len = inode.durable.len();
            let tail_len = (inode.live.len() - durable_len) as u64;
            let tail: Vec<u8> = inode.live[durable_len..].to_vec();
            // Choose how the tail is mangled. Weighted toward Drop/Truncate,
            // the physically common outcomes; BitFlip exercises the CRC path.
            let mode = match st.rng.gen_range(0u8..3) {
                0 => TearMode::Drop,
                1 => TearMode::Truncate,
                _ => TearMode::BitFlip,
            };
            let mut kept = inode.durable.clone();
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
                    let byte = durable_len + bit / 8;
                    kept[byte] ^= 1 << (bit % 8);
                    tail_len
                }
            };
            report.torn_path = Some(path);
            report.tear_mode = mode;
            report.tail_len = tail_len;
            report.tail_kept = tail_kept;
            st.inodes
                .get_mut(&inode_id)
                .expect("target inode present")
                .durable = kept;
        }

        // Revert every inode and path to its durable image. A volatile
        // delete/recreate therefore restores the old durable inode generation.
        for inode in st.inodes.values_mut() {
            inode.live = inode.durable.clone();
            inode.last_append_seq = None;
        }
        for entry in st.files.values_mut() {
            entry.live_inode = entry.durable_inode;
        }
        st.files.retain(|_, entry| entry.durable_inode.is_some());
        Self::collect_unreferenced_inodes(st);

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

    fn collect_unreferenced_inodes(st: &mut SimState) {
        let referenced: BTreeSet<u64> = st
            .files
            .values()
            .flat_map(|entry| [entry.live_inode, entry.durable_inode])
            .flatten()
            .collect();
        st.inodes
            .retain(|inode_id, _| referenced.contains(inode_id));
    }
}

/// Return `true` if `path` is a direct child of `dir` (same parent).
fn is_child_of(path: &Path, dir: &Path) -> bool {
    path.parent() == Some(dir)
}

impl Storage for SimFs {
    fn create(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        if st
            .files
            .get(path)
            .is_some_and(|entry| entry.live_inode.is_some())
        {
            return Err(StorageError::AlreadyExists(path.to_path_buf()));
        }

        let inode_id = st.next_inode;
        st.next_inode += 1;
        st.inodes.insert(inode_id, InodeState::default());
        // Keep any old durable directory entry intact. The new generation is
        // process-visible immediately but replaces the old one durably only
        // after a parent-directory sync.
        st.files.entry(path.to_path_buf()).or_default().live_inode = Some(inode_id);
        Self::bump(&mut st);
        Ok(())
    }

    fn open(&self, path: &Path) -> StorageResult<()> {
        let st = self.state.lock().expect("simfs poisoned");
        match st.files.get(path) {
            Some(entry) if entry.live_inode.is_some() => Ok(()),
            _ => Err(StorageError::NotFound(path.to_path_buf())),
        }
    }

    fn append(&self, path: &Path, data: &[u8]) -> StorageResult<u64> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(path)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        let append_seq = st.next_append_seq;
        st.next_append_seq += 1;
        let offset = {
            let inode = st.inodes.get_mut(&inode_id).expect("live inode present");
            let offset = inode.live.len() as u64;
            inode.live.extend_from_slice(data);
            inode.last_append_seq = Some(append_seq);
            offset
        };
        Self::bump(&mut st);
        Ok(offset)
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(path)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        {
            let inode = st.inodes.get_mut(&inode_id).expect("live inode present");
            let len = inode.live.len() as u64;
            if offset > len || offset + data.len() as u64 > len {
                return Err(StorageError::OutOfBounds {
                    path: path.to_path_buf(),
                    offset,
                    len,
                });
            }
            let start = offset as usize;
            inode.live[start..start + data.len()].copy_from_slice(data);
        }
        Self::bump(&mut st);
        Ok(())
    }

    fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(path)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        let inode = st.inodes.get(&inode_id).expect("live inode present");
        let len = inode.live.len() as u64;
        if offset > len {
            return Err(StorageError::OutOfBounds {
                path: path.to_path_buf(),
                offset,
                len,
            });
        }
        let start = offset as usize;
        let n = buf.len().min(inode.live.len() - start);
        buf[..n].copy_from_slice(&inode.live[start..start + n]);
        Ok(n)
    }

    fn sync_file(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(path)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        let inode = st.inodes.get_mut(&inode_id).expect("live inode present");
        inode.durable = inode.live.clone();
        inode.last_append_seq = None;
        Self::bump(&mut st);
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        for (path, entry) in st.files.iter_mut() {
            if !is_child_of(path, dir) {
                continue;
            }
            // Commit the live directory entry exactly. The selected inode's
            // buffered bytes remain governed by `sync_file`.
            entry.durable_inode = entry.live_inode;
        }
        st.files
            .retain(|_, entry| entry.live_inode.is_some() || entry.durable_inode.is_some());
        Self::collect_unreferenced_inodes(&mut st);
        Self::bump(&mut st);
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(from)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(from.to_path_buf()))?;
        if from != to {
            // Move the live directory link to the same inode generation. Both
            // namespace changes remain volatile until `sync_dir`.
            st.files.entry(to.to_path_buf()).or_default().live_inode = Some(inode_id);
            st.files.get_mut(from).expect("source present").live_inode = None;
        }
        Self::collect_unreferenced_inodes(&mut st);
        Self::bump(&mut st);
        Ok(())
    }

    fn delete(&self, path: &Path) -> StorageResult<()> {
        let mut st = self.state.lock().expect("simfs poisoned");
        {
            let entry = st
                .files
                .get_mut(path)
                .filter(|entry| entry.live_inode.is_some())
                .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
            // Volatile until a `sync_dir`: the durable path can still point to
            // the old inode generation after this live link disappears.
            entry.live_inode = None;
        }
        Self::collect_unreferenced_inodes(&mut st);
        Self::bump(&mut st);
        Ok(())
    }

    fn list(&self, dir: &Path) -> StorageResult<Vec<PathBuf>> {
        let st = self.state.lock().expect("simfs poisoned");
        let out: Vec<PathBuf> = st
            .files
            .iter()
            .filter(|(path, entry)| entry.live_inode.is_some() && is_child_of(path, dir))
            .map(|(path, _)| path.clone())
            .collect();
        // `files` is a BTreeMap, so iteration — and thus this list — is sorted.
        Ok(out)
    }

    fn len(&self, path: &Path) -> StorageResult<u64> {
        let st = self.state.lock().expect("simfs poisoned");
        let inode_id = st
            .files
            .get(path)
            .and_then(|entry| entry.live_inode)
            .ok_or_else(|| StorageError::NotFound(path.to_path_buf()))?;
        Ok(st
            .inodes
            .get(&inode_id)
            .expect("live inode present")
            .live
            .len() as u64)
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

    /// An atomic replace over an *already-durable* file (the manifest's
    /// tmp+sync_file+rename-over-`MANIFEST`+sync_dir protocol) must, after a crash
    /// following the `sync_dir`, durably resolve to the NEW content — not revert
    /// to the old durable image. This is the POSIX guarantee the manifest relies
    /// on; SimFs once under-modeled it and the crash sweep exposed the resulting
    /// false acknowledged-write loss (see `BUGS_FOUND.md`).
    #[test]
    fn rename_over_durable_file_commits_new_content_on_dir_sync() {
        let fs = SimFs::with_seed(1);
        let man = root().join("MANIFEST");
        let tmp = root().join("MANIFEST.tmp");

        // Install v1 durably.
        fs.create(&man).expect("create v1");
        fs.append(&man, b"VERSION-1").expect("append v1");
        fs.sync_file(&man).expect("sync v1");
        fs.sync_dir(&root()).expect("dir-sync v1");

        // Atomically replace with v2 over the existing durable MANIFEST.
        fs.create(&tmp).expect("create tmp");
        fs.append(&tmp, b"VERSION-2222").expect("append v2");
        fs.sync_file(&tmp).expect("sync tmp");
        fs.rename(&tmp, &man).expect("rename over MANIFEST");
        fs.sync_dir(&root()).expect("dir-sync v2");

        // A crash after the committing dir-sync keeps the new content.
        fs.crash();
        assert_eq!(read_all(&fs, &man), b"VERSION-2222");
    }

    /// A rename-over-durable that crashes *before* the committing `sync_dir`
    /// reverts the destination to its old durable content — the replace never
    /// happened.
    #[test]
    fn rename_over_durable_reverts_if_crash_before_dir_sync() {
        let fs = SimFs::with_seed(1);
        let man = root().join("MANIFEST");
        let tmp = root().join("MANIFEST.tmp");
        fs.create(&man).expect("create v1");
        fs.append(&man, b"VERSION-1").expect("append v1");
        fs.sync_file(&man).expect("sync v1");
        fs.sync_dir(&root()).expect("dir-sync v1");

        fs.create(&tmp).expect("create tmp");
        fs.append(&tmp, b"VERSION-2222").expect("append v2");
        fs.sync_file(&tmp).expect("sync tmp");
        fs.rename(&tmp, &man).expect("rename over MANIFEST");
        // No committing sync_dir: crash now.
        fs.crash();
        assert_eq!(read_all(&fs, &man), b"VERSION-1");
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

    /// Syncing a directory commits the file name, not its buffered payload.
    /// After a crash the path therefore remains, while the payload is still
    /// treated as an unsynced tail rather than silently promoted to durable.
    #[test]
    fn dir_sync_commits_name_but_not_unsynced_bytes() {
        let fs = SimFs::with_seed(11);
        let p = root().join("named-but-buffered");
        let payload = b"not-yet-durable";

        fs.create(&p).expect("create");
        fs.append(&p, payload).expect("append");
        fs.sync_dir(&root()).expect("sync directory entry");

        let report = fs.crash();
        fs.open(&p).expect("directory-synced name survives");
        assert_eq!(report.torn_path.as_deref(), Some(p.as_path()));
        assert_eq!(report.tail_len, payload.len() as u64);
    }

    #[test]
    fn delete_recreate_before_dir_sync_restores_old_generation() {
        let fs = SimFs::with_seed(19);
        let p = root().join("reused");
        fs.create(&p).expect("create old");
        fs.append(&p, b"old-generation").expect("append old");
        fs.sync_file(&p).expect("sync old data");
        fs.sync_dir(&root()).expect("sync old name");

        fs.delete(&p).expect("delete old");
        fs.create(&p).expect("create new");
        fs.append(&p, b"new-generation").expect("append new");

        fs.crash();
        assert_eq!(read_all(&fs, &p), b"old-generation");
    }

    #[test]
    fn delete_recreate_dir_sync_never_resurrects_old_bytes() {
        let fs = SimFs::with_seed(23);
        let p = root().join("reused");
        fs.create(&p).expect("create old");
        fs.append(&p, b"old-generation").expect("append old");
        fs.sync_file(&p).expect("sync old data");
        fs.sync_dir(&root()).expect("sync old name");

        fs.delete(&p).expect("delete old");
        fs.create(&p).expect("create new");
        fs.append(&p, b"new").expect("append new");
        fs.sync_dir(&root()).expect("commit new name");

        let report = fs.crash();
        assert_eq!(report.torn_path.as_deref(), Some(p.as_path()));
        assert_ne!(read_all(&fs, &p), b"old-generation");
    }

    #[test]
    fn syncing_recreated_file_before_dir_sync_preserves_old_generation() {
        let fs = SimFs::with_seed(29);
        let p = root().join("reused");
        fs.create(&p).expect("create old");
        fs.append(&p, b"old-generation").expect("append old");
        fs.sync_file(&p).expect("sync old data");
        fs.sync_dir(&root()).expect("sync old name");

        fs.delete(&p).expect("delete old");
        fs.create(&p).expect("create new");
        fs.append(&p, b"new-generation").expect("append new");
        fs.sync_file(&p).expect("sync new data only");

        fs.crash();
        assert_eq!(read_all(&fs, &p), b"old-generation");
    }

    #[test]
    fn syncing_recreated_file_and_dir_commits_new_generation() {
        let fs = SimFs::with_seed(31);
        let p = root().join("reused");
        fs.create(&p).expect("create old");
        fs.append(&p, b"old-generation").expect("append old");
        fs.sync_file(&p).expect("sync old data");
        fs.sync_dir(&root()).expect("sync old name");

        fs.delete(&p).expect("delete old");
        fs.create(&p).expect("create new");
        fs.append(&p, b"new-generation").expect("append new");
        fs.sync_file(&p).expect("sync new data");
        fs.sync_dir(&root()).expect("commit new name");

        fs.crash();
        assert_eq!(read_all(&fs, &p), b"new-generation");
    }

    #[test]
    fn rename_to_same_path_is_a_noop() {
        let fs = SimFs::with_seed(37);
        let p = root().join("same");
        fs.create(&p).expect("create");
        fs.append(&p, b"content").expect("append");
        fs.sync_file(&p).expect("sync data");
        fs.sync_dir(&root()).expect("sync name");

        fs.rename(&p, &p).expect("rename same path");
        assert_eq!(read_all(&fs, &p), b"content");
        fs.crash();
        assert_eq!(read_all(&fs, &p), b"content");
    }

    #[test]
    fn syncing_newest_append_reveals_previous_unsynced_tear_candidate() {
        let fs = SimFs::with_seed(41);
        let a = setup_synced_file(&fs, "a");
        let b = setup_synced_file(&fs, "b");

        fs.append(&a, b"older-unsynced").expect("append a");
        fs.append(&b, b"newer-then-synced").expect("append b");
        fs.sync_file(&b).expect("sync b");

        let report = fs.crash();
        assert_eq!(report.torn_path.as_deref(), Some(a.as_path()));
        assert_eq!(report.tail_len, b"older-unsynced".len() as u64);
        assert_eq!(read_all(&fs, &b), b"newer-then-synced");
    }

    #[test]
    fn unsynced_append_follows_durable_source_name_before_rename_commit() {
        let fs = SimFs::with_seed(43);
        let source = setup_synced_file(&fs, "source");
        let destination = root().join("destination");

        fs.append(&source, b"tail").expect("append source");
        fs.rename(&source, &destination).expect("rename");

        let report = fs.crash();
        assert_eq!(report.torn_path.as_deref(), Some(source.as_path()));
        fs.open(&source).expect("durable source name restored");
        assert!(matches!(
            fs.open(&destination),
            Err(StorageError::NotFound(_))
        ));
    }

    #[test]
    fn unsynced_append_follows_destination_after_rename_commit() {
        let fs = SimFs::with_seed(47);
        let source = setup_synced_file(&fs, "source");
        let destination = root().join("destination");

        fs.append(&source, b"tail").expect("append source");
        fs.rename(&source, &destination).expect("rename");
        fs.sync_dir(&root()).expect("commit rename");

        let report = fs.crash();
        assert_eq!(report.torn_path.as_deref(), Some(destination.as_path()));
        assert!(matches!(fs.open(&source), Err(StorageError::NotFound(_))));
        fs.open(&destination)
            .expect("durable destination name survives");
    }

    #[test]
    fn rename_overwrite_collects_unreachable_live_inodes() {
        let fs = SimFs::with_seed(53);
        let a = root().join("a");
        let b = root().join("b");
        let c = root().join("c");
        for path in [&a, &b, &c] {
            fs.create(path).expect("create live generation");
        }
        assert_eq!(fs.state.lock().expect("simfs poisoned").inodes.len(), 3);

        fs.rename(&a, &b).expect("overwrite b");
        assert_eq!(fs.state.lock().expect("simfs poisoned").inodes.len(), 2);
        fs.rename(&b, &c).expect("overwrite c");
        assert_eq!(fs.state.lock().expect("simfs poisoned").inodes.len(), 1);
    }

    #[test]
    fn cross_directory_rename_requires_both_directory_syncs() {
        let fs = SimFs::with_seed(59);
        let source_dir = PathBuf::from("/source-dir");
        let destination_dir = PathBuf::from("/destination-dir");
        let source = source_dir.join("file");
        let destination = destination_dir.join("file");

        fs.create(&source).expect("create source");
        fs.append(&source, b"content").expect("append source");
        fs.sync_file(&source).expect("sync source data");
        fs.sync_dir(&source_dir).expect("sync source name");

        fs.rename(&source, &destination)
            .expect("cross-directory rename");
        fs.sync_dir(&source_dir).expect("commit source unlink");
        fs.sync_dir(&destination_dir)
            .expect("commit destination link");

        fs.crash();
        assert!(matches!(fs.open(&source), Err(StorageError::NotFound(_))));
        fs.open(&destination).expect("destination survives");
        assert_eq!(read_all(&fs, &destination), b"content");
    }
}

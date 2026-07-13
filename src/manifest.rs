//! The manifest: the crash-safe, versioned record of *which SSTables exist and
//! which tier each lives in*.
//!
//! The set of live tables changes every time a memtable is flushed or a
//! compaction merges a tier. Each such change installs a brand-new immutable
//! [`Version`]; readers pin the current version as an `Arc<Version>` and keep
//! reading it, unbothered, even while a newer version is installed and old files
//! are deleted underneath them.
//!
//! # Atomic version switch
//!
//! Installing a version writes the whole manifest snapshot to a scratch file,
//! `sync_file`s it, atomically `rename`s it over `MANIFEST`, then `sync_dir`s the
//! directory. Without that final directory fsync the rename is volatile: a crash
//! could leave the directory entry pointing at the old manifest even though the
//! new bytes are durable (see `DESIGN_NOTES.md`). The order also matters relative
//! to file deletion — obsolete SSTables are removed *only after* the new manifest
//! that stops referencing them is durable, so a crash mid-switch can at worst
//! leave unreferenced garbage (reclaimed by the next GC), never a dangling
//! reference.
//!
//! # File GC only when unreferenced
//!
//! When a version is superseded it is retained until no reader pins it — detected
//! via the `Arc` strong count. A table file is deleted only once it is referenced
//! by neither the current version nor any still-pinned older version. This is the
//! concrete mechanism behind "readers pinned to an old `Arc<Version>` stay correct
//! while compaction replaces files".

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::storage::{Storage, StorageError};

/// Canonical manifest filename within the database directory.
const MANIFEST_NAME: &str = "MANIFEST";
/// Scratch filename a new manifest is written to before being renamed into place.
const MANIFEST_TMP: &str = "MANIFEST.tmp";
/// Magic tag at the head of a manifest file.
const MANIFEST_MAGIC: u64 = 0x4143_4352_5F4D_4652; // "ACCR_MFR"
/// On-disk manifest format version.
const MANIFEST_FORMAT: u32 = 1;

/// Result alias for manifest operations.
pub type Result<T> = std::result::Result<T, ManifestError>;

/// Errors produced while reading or writing the manifest.
#[derive(Debug)]
pub enum ManifestError {
    /// An error from the underlying [`Storage`] backend.
    Storage(StorageError),
    /// The manifest file's bytes are inconsistent with the format: a bad CRC,
    /// bad magic, unsupported version, or a structure that runs past the data.
    Corrupt(String),
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestError::Storage(e) => write!(f, "storage error: {e}"),
            ManifestError::Corrupt(m) => write!(f, "corrupt manifest: {m}"),
        }
    }
}

impl std::error::Error for ManifestError {}

impl From<StorageError> for ManifestError {
    fn from(e: StorageError) -> Self {
        ManifestError::Storage(e)
    }
}

/// Immutable metadata describing one SSTable file on disk.
///
/// The key range lets the read path skip a table whose `[first_key, last_key]`
/// span cannot contain the sought key without opening the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMeta {
    /// Unique file id; the on-disk filename is [`table_path`]`(dir, id)`.
    pub id: u64,
    /// Number of entries the table holds (tombstones included).
    pub num_entries: u64,
    /// Smallest key in the table.
    pub first_key: Vec<u8>,
    /// Largest key in the table.
    pub last_key: Vec<u8>,
}

impl TableMeta {
    /// Whether `key` falls within this table's stored key range. A `false` means
    /// the table definitely does not contain `key` (no file access needed).
    pub fn key_in_range(&self, key: &[u8]) -> bool {
        key >= self.first_key.as_slice() && key <= self.last_key.as_slice()
    }
}

/// An immutable snapshot of the LSM's file layout at one point in time.
///
/// A `Version` is never mutated after it is built; a change produces a fresh
/// `Version` via [`flushed`](Version::flushed) or [`compacted`](Version::compacted).
/// Readers hold an `Arc<Version>` and see a consistent set of tables for as long
/// as they keep it.
///
/// `tiers[0]` is the youngest tier (fresh flushes land here); higher indices hold
/// older, larger, previously-compacted tables. Within a tier, tables are ordered
/// oldest-first (a compaction output or newest flush is pushed at the back), so a
/// larger file id is newer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Version {
    /// Tables grouped by tier; `tiers[0]` youngest.
    pub tiers: Vec<Vec<Arc<TableMeta>>>,
    /// Next sequence number the engine should hand out.
    pub next_seq: u64,
    /// Next table file id to allocate.
    pub next_table_id: u64,
}

impl Version {
    /// An empty version: no tables, sequence and file ids starting at 1.
    pub fn empty() -> Self {
        Version {
            tiers: Vec::new(),
            next_seq: 1,
            next_table_id: 1,
        }
    }

    /// Every table in this version, across all tiers.
    pub fn all_tables(&self) -> impl Iterator<Item = &Arc<TableMeta>> {
        self.tiers.iter().flatten()
    }

    /// The set of table file ids this version references.
    pub fn referenced_ids(&self) -> BTreeSet<u64> {
        self.all_tables().map(|t| t.id).collect()
    }

    /// Number of tables in tier `t` (0 if the tier does not exist).
    pub fn tier_len(&self, t: usize) -> usize {
        self.tiers.get(t).map_or(0, Vec::len)
    }

    /// Total number of tiers currently populated.
    pub fn num_tiers(&self) -> usize {
        self.tiers.len()
    }

    /// Tables ordered newest-first for a point lookup: youngest tier first, and
    /// within a tier the most-recently-added (largest id) first. The first table
    /// in this order that contains a key holds that key's newest version, so a
    /// point read can stop at the first hit.
    pub fn tables_newest_first(&self) -> Vec<Arc<TableMeta>> {
        let mut out = Vec::new();
        for tier in &self.tiers {
            for table in tier.iter().rev() {
                out.push(Arc::clone(table));
            }
        }
        out
    }

    /// Derive a new version with `table` appended to tier 0 (a memtable flush),
    /// carrying `next_seq` forward and consuming `table.id` from the id space.
    pub fn flushed(&self, table: TableMeta, next_seq: u64) -> Version {
        let mut v = self.clone();
        if v.tiers.is_empty() {
            v.tiers.push(Vec::new());
        }
        let id = table.id;
        v.tiers[0].push(Arc::new(table));
        v.next_seq = next_seq.max(v.next_seq);
        v.next_table_id = v.next_table_id.max(id + 1);
        v
    }

    /// Derive a new version in which every table of tier `t` is replaced by the
    /// single merged `output` table placed at tier `t + 1` — the size-tiered
    /// compaction step. Empty tiers are trimmed from the tail.
    pub fn compacted(&self, t: usize, output: TableMeta) -> Version {
        let mut v = self.clone();
        while v.tiers.len() <= t + 1 {
            v.tiers.push(Vec::new());
        }
        v.tiers[t].clear();
        let id = output.id;
        v.tiers[t + 1].push(Arc::new(output));
        v.next_table_id = v.next_table_id.max(id + 1);
        // Trim trailing empty tiers so tier count reflects reality.
        while matches!(v.tiers.last(), Some(tier) if tier.is_empty()) {
            v.tiers.pop();
        }
        v
    }
}

/// The path of the SSTable file for `id` within database directory `dir`.
///
/// The 20-digit zero-padded name sorts lexicographically the same as
/// numerically, matching the WAL segment convention.
pub fn table_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.sst"))
}

/// The crash-safe, versioned manifest handle.
///
/// Holds the current [`Version`] plus every superseded version still pinned by a
/// reader, and owns the atomic file-switch + GC protocol.
#[derive(Debug)]
pub struct Manifest {
    fs: Arc<dyn Storage>,
    dir: PathBuf,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    current: Arc<Version>,
    /// Superseded versions retained until no reader pins them.
    obsolete: Vec<Arc<Version>>,
}

impl Manifest {
    /// Open the manifest in `dir`, creating a fresh empty one if none exists.
    ///
    /// A pre-existing `MANIFEST` is parsed and CRC-verified; its version becomes
    /// current. On a fresh directory an empty manifest is written durably so a
    /// subsequent crash still finds a valid file.
    pub fn open(fs: Arc<dyn Storage>, dir: &Path) -> Result<Manifest> {
        let path = dir.join(MANIFEST_NAME);
        let current = if fs.open(&path).is_ok() {
            let len = fs.len(&path)?;
            let mut buf = vec![0u8; len as usize];
            let n = fs.read_at(&path, 0, &mut buf)?;
            buf.truncate(n);
            decode_version(&buf)?
        } else {
            let v = Version::empty();
            write_manifest(&*fs, dir, &v)?;
            v
        };
        Ok(Manifest {
            fs,
            dir: dir.to_path_buf(),
            inner: Mutex::new(Inner {
                current: Arc::new(current),
                obsolete: Vec::new(),
            }),
        })
    }

    /// The directory this manifest lives in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Pin and return the current version. The returned `Arc` keeps every table
    /// it names alive on disk until it is dropped.
    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.inner.lock().expect("manifest poisoned").current)
    }

    /// Durably install `new` as the current version, then delete any table file
    /// no longer referenced by a live version.
    ///
    /// The new manifest is made durable *before* any file is deleted, so a crash
    /// during the switch can only strand unreferenced garbage — never leave the
    /// manifest pointing at a file that has already been removed.
    pub fn install(&self, new: Version) -> Result<Arc<Version>> {
        // 1. Persist the new manifest durably (tmp + sync_file + rename + sync_dir).
        write_manifest(&*self.fs, &self.dir, &new)?;

        // 2. Swap it in, retiring the previous current for GC bookkeeping.
        let new_arc = Arc::new(new);
        {
            let mut inner = self.inner.lock().expect("manifest poisoned");
            let prev = std::mem::replace(&mut inner.current, Arc::clone(&new_arc));
            inner.obsolete.push(prev);
        }

        // 3. GC: delete files unreferenced by any still-live version.
        self.gc()?;
        Ok(new_arc)
    }

    /// Delete SSTable files that no live version (current or a still-pinned
    /// obsolete one) references. Called after every install; safe to call anytime.
    fn gc(&self) -> Result<()> {
        // Compute the set of ids still referenced, dropping obsolete versions
        // that no reader pins any more (strong_count == 1 means only our vec).
        let live_ids = {
            let mut inner = self.inner.lock().expect("manifest poisoned");
            inner.obsolete.retain(|v| Arc::strong_count(v) > 1);
            let mut ids = inner.current.referenced_ids();
            for v in &inner.obsolete {
                ids.extend(v.referenced_ids());
            }
            ids
        };

        // Any *.sst file on disk not in the live set is garbage.
        let mut deleted_any = false;
        for entry in self.fs.list(&self.dir)? {
            if let Some(id) = parse_table_id(&entry) {
                if !live_ids.contains(&id) {
                    // A crash before the sync_dir simply leaves the file to be
                    // reclaimed by the next GC — deletion is idempotent.
                    self.fs.delete(&entry)?;
                    deleted_any = true;
                }
            }
        }
        if deleted_any {
            self.fs.sync_dir(&self.dir)?;
        }
        Ok(())
    }
}

/// Parse a `*.sst` file id from a full path, or `None` if it is not a table file.
fn parse_table_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_suffix(".sst")?;
    stem.parse::<u64>().ok()
}

/// Write `version` to `dir` durably via tmp + `sync_file` + `rename` + `sync_dir`.
fn write_manifest(fs: &dyn Storage, dir: &Path, version: &Version) -> Result<()> {
    let tmp = dir.join(MANIFEST_TMP);
    let final_path = dir.join(MANIFEST_NAME);
    let bytes = encode_version(version);

    // A stale tmp from an interrupted prior switch must not block the create.
    let _ = fs.delete(&tmp);
    fs.create(&tmp)?;
    fs.append(&tmp, &bytes)?;
    fs.sync_file(&tmp)?;
    fs.rename(&tmp, &final_path)?;
    fs.sync_dir(dir)?;
    Ok(())
}

// --- Encoding -------------------------------------------------------------
//
// Layout (all little-endian):
//   magic u64, format u32, next_seq u64, next_table_id u64, num_tiers u32,
//   per tier: num_tables u32, per table: id u64, num_entries u64,
//             first_len u32, first_key, last_len u32, last_key
//   trailing crc32 over everything above.

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_u32(buf, b.len() as u32);
    buf.extend_from_slice(b);
}

fn encode_version(v: &Version) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u64(&mut buf, MANIFEST_MAGIC);
    put_u32(&mut buf, MANIFEST_FORMAT);
    put_u64(&mut buf, v.next_seq);
    put_u64(&mut buf, v.next_table_id);
    put_u32(&mut buf, v.tiers.len() as u32);
    for tier in &v.tiers {
        put_u32(&mut buf, tier.len() as u32);
        for t in tier {
            put_u64(&mut buf, t.id);
            put_u64(&mut buf, t.num_entries);
            put_bytes(&mut buf, &t.first_key);
            put_bytes(&mut buf, &t.last_key);
        }
    }
    let crc = crc32fast::hash(&buf);
    put_u32(&mut buf, crc);
    buf
}

/// A minimal bounds-checked reader over the manifest bytes.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() - self.pos < n {
            return Err(ManifestError::Corrupt(format!(
                "unexpected end: need {n}, have {}",
                self.buf.len() - self.pos
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4")))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8")))
    }
    fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}

fn decode_version(buf: &[u8]) -> Result<Version> {
    if buf.len() < 4 {
        return Err(ManifestError::Corrupt("shorter than a crc".into()));
    }
    let (body, crc_bytes) = buf.split_at(buf.len() - 4);
    let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4"));
    let actual = crc32fast::hash(body);
    if stored != actual {
        return Err(ManifestError::Corrupt(format!(
            "crc mismatch: stored {stored:#010x}, computed {actual:#010x}"
        )));
    }

    let mut r = Reader::new(body);
    if r.u64()? != MANIFEST_MAGIC {
        return Err(ManifestError::Corrupt("magic mismatch".into()));
    }
    let format = r.u32()?;
    if format != MANIFEST_FORMAT {
        return Err(ManifestError::Corrupt(format!(
            "unsupported format {format}"
        )));
    }
    let next_seq = r.u64()?;
    let next_table_id = r.u64()?;
    let num_tiers = r.u32()? as usize;
    let mut tiers = Vec::with_capacity(num_tiers);
    for _ in 0..num_tiers {
        let num_tables = r.u32()? as usize;
        let mut tier = Vec::with_capacity(num_tables);
        for _ in 0..num_tables {
            let id = r.u64()?;
            let num_entries = r.u64()?;
            let first_key = r.bytes()?;
            let last_key = r.bytes()?;
            tier.push(Arc::new(TableMeta {
                id,
                num_entries,
                first_key,
                last_key,
            }));
        }
        tiers.push(tier);
    }
    Ok(Version {
        tiers,
        next_seq,
        next_table_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimFs;

    fn dir() -> PathBuf {
        PathBuf::from("/db")
    }

    fn meta(id: u64, first: &[u8], last: &[u8]) -> TableMeta {
        TableMeta {
            id,
            num_entries: 1,
            first_key: first.to_vec(),
            last_key: last.to_vec(),
        }
    }

    #[test]
    fn fresh_open_is_empty_and_durable() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let m = Manifest::open(fs.clone(), &dir()).unwrap();
        assert!(m.current().all_tables().next().is_none());
        // The empty manifest was written durably: reopening finds it.
        let m2 = Manifest::open(fs, &dir()).unwrap();
        assert_eq!(m2.current().next_table_id, 1);
    }

    #[test]
    fn install_persists_across_reopen() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let m = Manifest::open(fs.clone(), &dir()).unwrap();
        let v = m.current().flushed(meta(1, b"a", b"m"), 5);
        m.install(v).unwrap();

        let m2 = Manifest::open(fs, &dir()).unwrap();
        let cur = m2.current();
        assert_eq!(cur.tier_len(0), 1);
        assert_eq!(cur.next_seq, 5);
        assert_eq!(cur.next_table_id, 2);
        assert_eq!(cur.tiers[0][0].id, 1);
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut v = Version::empty();
        v = v.flushed(meta(1, b"a", b"c"), 3);
        v = v.flushed(meta(2, b"d", b"f"), 7);
        let bytes = encode_version(&v);
        let back = decode_version(&bytes).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn corrupt_manifest_is_detected() {
        let mut v = Version::empty();
        v = v.flushed(meta(1, b"a", b"c"), 3);
        let mut bytes = encode_version(&v);
        bytes[12] ^= 0xFF; // flip a byte inside next_seq
        assert!(matches!(
            decode_version(&bytes),
            Err(ManifestError::Corrupt(_))
        ));
    }

    #[test]
    fn newest_first_ordering_prefers_young_tier_then_high_id() {
        let mut v = Version::empty();
        // tier 0 gets ids 1 then 2 (2 newer); then compact tier 0 into tier 1.
        v = v.flushed(meta(1, b"a", b"z"), 1);
        v = v.flushed(meta(2, b"a", b"z"), 2);
        v = v.compacted(0, meta(3, b"a", b"z"));
        // Now: tier 0 empty, tier 1 = [3]. Add a fresh flush to tier 0.
        v = v.flushed(meta(4, b"a", b"z"), 4);
        let order: Vec<u64> = v.tables_newest_first().iter().map(|t| t.id).collect();
        assert_eq!(order, vec![4, 3], "young tier first, high id first");
    }
}

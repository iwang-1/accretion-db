//! The public engine: [`Db`], wiring the WAL, memtable, SSTable tiers, manifest,
//! and compaction into one embeddable key/value store.
//!
//! # Write path
//!
//! `put`/`delete` run under a single write mutex (the deliberate single-writer
//! model — see `DESIGN_NOTES.md`):
//!
//! 1. assign the next global sequence number;
//! 2. append a framed `(seq, key, value)` record to the [WAL](crate::wal), which
//!    does not return until the configured [`Durability`] contract is met — so a
//!    durable-mode `put` is crash-safe the instant it returns;
//! 3. insert the value into the active memtable;
//! 4. if the active memtable is now full, *freeze* it and *flush* it: write a new
//!    tier-0 SSTable, install a new manifest version, then release the WAL. A
//!    cascading size-tiered compaction runs synchronously afterwards.
//!
//! The flush ordering — SSTable durable, *then* manifest install, *then* WAL
//! reset — is what makes a crash at any point recoverable: an SSTable not yet
//! referenced by the manifest is GC'd, and WAL records not yet flushed are
//! replayed; nothing acked is ever lost, and a replayed-and-also-flushed record
//! de-duplicates by sequence number on read.
//!
//! # Read path
//!
//! `get` consults the memtable set (active, then frozen, newest-first), and on a
//! miss walks the SSTable tiers newest-first, letting each table's bloom filter
//! gate the probe. The first table that answers holds the key's newest version.
//! `scan` folds the memtables and every table through the
//! [merge iterator](crate::iter), yielding an ascending, tombstone-free stream.

use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::compaction::{self, CompactionError};
use crate::iter::{EntrySource, LiveIter, MergeIterator};
use crate::manifest::{table_path, Manifest, ManifestError, TableMeta};
use crate::memtable::{InternalValue, MemtableSet, Seq, ValueKind};
use crate::sstable::{SsTableError, SsTableReader, Value};
use crate::storage::{RealFs, SimFs, Storage, StorageError};
use crate::wal::{Durability, Wal, WalOptions};

/// Default memtable freeze threshold in bytes (4 MiB).
pub const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;
/// Default number of tables a tier holds before it compacts.
pub const DEFAULT_TIER_FANOUT: usize = 4;

/// Engine configuration passed to [`Db::open`].
#[derive(Debug, Clone)]
pub struct Options {
    /// The WAL durability contract each `put`/`delete` honours before returning.
    pub durability: Durability,
    /// Byte threshold at which the active memtable freezes and flushes.
    pub memtable_size: usize,
    /// Number of tables a tier accumulates before it is compacted down.
    pub tier_fanout: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            durability: Durability::default(),
            memtable_size: DEFAULT_MEMTABLE_SIZE,
            tier_fanout: DEFAULT_TIER_FANOUT,
        }
    }
}

/// Errors surfaced by the engine.
#[derive(Debug)]
pub enum DbError {
    /// An error from the underlying storage backend.
    Storage(StorageError),
    /// An error building or reading an SSTable.
    SsTable(SsTableError),
    /// An error reading or writing the manifest.
    Manifest(ManifestError),
    /// An error during a flush or compaction.
    Compaction(CompactionError),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Storage(e) => write!(f, "storage error: {e}"),
            DbError::SsTable(e) => write!(f, "sstable error: {e}"),
            DbError::Manifest(e) => write!(f, "manifest error: {e}"),
            DbError::Compaction(e) => write!(f, "compaction error: {e}"),
        }
    }
}

impl std::error::Error for DbError {}

impl From<StorageError> for DbError {
    fn from(e: StorageError) -> Self {
        DbError::Storage(e)
    }
}
impl From<SsTableError> for DbError {
    fn from(e: SsTableError) -> Self {
        DbError::SsTable(e)
    }
}
impl From<ManifestError> for DbError {
    fn from(e: ManifestError) -> Self {
        DbError::Manifest(e)
    }
}
impl From<CompactionError> for DbError {
    fn from(e: CompactionError) -> Self {
        DbError::Compaction(e)
    }
}

/// Result alias for engine operations.
pub type Result<T> = std::result::Result<T, DbError>;

/// Write-path state guarded by the single-writer mutex: the next sequence number
/// to hand out. Kept behind its own lock so reads never contend with it.
#[derive(Debug)]
struct WriteState {
    next_seq: Seq,
}

/// An embeddable LSM-tree key/value store.
///
/// Cheaply shareable across threads: reads take no exclusive lock, and the write
/// path is serialized internally. Wrap in an `Arc` to hand to multiple threads.
#[derive(Debug)]
pub struct Db {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    options: Options,
    wal: Wal,
    memtables: MemtableSet,
    manifest: Manifest,
    /// Immutable-per-id SSTable reader cache (a file id is never reused).
    readers: RwLock<HashMap<u64, Arc<SsTableReader>>>,
    write: Mutex<WriteState>,
}

impl Db {
    /// Open (creating if absent) the database rooted at `dir` on the real
    /// filesystem, recovering any WAL and manifest present.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Db> {
        let dir = dir.as_ref();
        // RealFs has no mkdir on the Storage seam; the real-filesystem entry
        // point owns creating the database directory before the engine opens.
        std::fs::create_dir_all(dir).map_err(|e| {
            DbError::Storage(StorageError::Io {
                path: dir.to_path_buf(),
                message: e.to_string(),
            })
        })?;
        let storage: Arc<dyn Storage> = Arc::new(RealFs::new());
        Db::open_on(storage, dir, options)
    }

    /// Open a database backed by a fresh in-memory [`SimFs`] with the given seed —
    /// the harness constructor used by the crash suite and property tests.
    pub fn open_sim(dir: impl AsRef<Path>, seed: u64, options: Options) -> Result<Db> {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(seed));
        Db::open_on(storage, dir.as_ref(), options)
    }

    /// Open a database on a caller-supplied [`Storage`] backend — the seam that
    /// lets the same engine run on `RealFs` or a shared `SimFs` handle (so a test
    /// can crash the backend out from under the engine and reopen it).
    pub fn open_on(storage: Arc<dyn Storage>, dir: &Path, options: Options) -> Result<Db> {
        // 1. Recover the manifest: the durable set of tables and the seq floor.
        let manifest = Manifest::open(Arc::clone(&storage), dir)?;
        let version = manifest.current();
        let mut next_seq = version.next_seq;

        // 2. Recover the WAL into a fresh memtable, replaying records that were
        //    acked but not yet captured in an SSTable.
        let wal_opts = WalOptions {
            durability: options.durability,
            ..Default::default()
        };
        let (wal, recovered) = Wal::open(Arc::clone(&storage), dir, wal_opts)?;
        let memtables = MemtableSet::new(options.memtable_size);
        for record in &recovered.records {
            let rec = decode_record(record)?;
            next_seq = next_seq.max(rec.seq + 1);
            memtables.insert(rec.key, rec.value);
        }

        Ok(Db {
            storage,
            dir: dir.to_path_buf(),
            options,
            wal,
            memtables,
            manifest,
            readers: RwLock::new(HashMap::new()),
            write: Mutex::new(WriteState { next_seq }),
        })
    }

    /// Insert or overwrite `key` with `value`. Returns only once the configured
    /// durability contract is met.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write(key, ValueKind::Value(value.to_vec()))
    }

    /// Delete `key` (writing a tombstone). Returns only once the configured
    /// durability contract is met.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key, ValueKind::Tombstone)
    }

    /// The shared write path for `put` and `delete`.
    fn write(&self, key: &[u8], kind: ValueKind) -> Result<()> {
        let mut ws = self.write.lock().expect("db write lock poisoned");
        let seq = ws.next_seq;

        // 1. Durably log the record first (the WAL enforces the mode's contract).
        let record = encode_record(seq, key, &kind);
        self.wal.append(&record)?;

        // 2. Apply to the active memtable and advance the sequence clock.
        self.memtables
            .insert(key.to_vec(), InternalValue { seq, kind });
        ws.next_seq = seq + 1;

        // 3. Flush if the memtable is now full, then cascade compaction. Both run
        //    synchronously under the write lock in this stage.
        if self.memtables.is_full() {
            self.flush_locked(&mut ws)?;
        }
        Ok(())
    }

    /// Look up `key`, returning its current value, or `None` if it is absent or
    /// deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1. Memtables (active + frozen, newest-first) win over any table.
        if let Some(v) = self.memtables.get(key) {
            return Ok(match v.kind {
                ValueKind::Value(bytes) => Some(bytes),
                ValueKind::Tombstone => None,
            });
        }

        // 2. Tables newest-first; the pinned version keeps their files alive for
        //    the duration of this read even if a compaction GCs concurrently.
        let version = self.manifest.current();
        for meta in version.tables_newest_first() {
            if !meta.key_in_range(key) {
                continue;
            }
            let reader = self.reader_for(&meta)?;
            if let Some(entry) = reader.get(key)? {
                return Ok(match entry.value {
                    Value::Put(bytes) => Some(bytes),
                    Value::Delete => None,
                });
            }
        }
        Ok(None)
    }

    /// A forward scan over `range`, yielding live `(key, value)` pairs in
    /// ascending key order (deletes and shadowed versions removed).
    ///
    /// The scan materialises a consistent snapshot of every source up front, so
    /// the returned iterator is unaffected by concurrent writes or compactions.
    pub fn scan<R>(&self, range: R) -> Result<Scan>
    where
        R: RangeBounds<Vec<u8>> + Clone,
    {
        let mut sources: Vec<EntrySource> = self.memtables.scan_iters(range.clone());

        let version = self.manifest.current();
        for meta in version.all_tables() {
            let reader = self.reader_for(meta)?;
            let mut entries: Vec<(Vec<u8>, InternalValue)> = Vec::new();
            for entry in reader.iter() {
                let e = entry?;
                if range_contains(&range, &e.key) {
                    entries.push((e.key, internal_of(e.seq, e.value)));
                }
            }
            sources.push(Box::new(entries.into_iter()));
        }

        Ok(Scan {
            inner: MergeIterator::new(sources).live(),
        })
    }

    /// Force the active memtable to freeze and flush to a tier-0 SSTable, then
    /// cascade compaction. A no-op if there is nothing buffered.
    pub fn flush(&self) -> Result<()> {
        let mut ws = self.write.lock().expect("db write lock poisoned");
        self.flush_locked(&mut ws)
    }

    /// Freeze the active memtable (if non-empty), flush every frozen table to a
    /// new tier-0 SSTable under the manifest protocol, release the WAL, then run
    /// cascading compaction. Caller holds the write lock.
    fn flush_locked(&self, ws: &mut WriteState) -> Result<()> {
        // Freeze whatever is buffered so the active table is empty while we flush.
        if self.memtables.active_bytes() > 0 {
            self.memtables.freeze();
        }

        // Flush each frozen table oldest-first, installing a manifest version per
        // table. Holding the write lock means no new frozen tables appear here.
        for frozen in self.memtables.frozen() {
            let version = self.manifest.current();
            let id = version.next_table_id;
            let mut entries = frozen.snapshot();
            let n = entries.len();
            // snapshot() is already key-sorted; hand it straight to the writer.
            let table = compaction::write_table(
                Arc::clone(&self.storage),
                &table_path(&self.dir, id),
                id,
                n,
                std::mem::take(&mut entries),
            )?;
            if let Some(meta) = table {
                // Persist the current write clock so recovery resumes seq numbers
                // past every flushed record.
                let new_version = version.flushed(meta, ws.next_seq);
                self.manifest.install(new_version)?;
            }
            // The table's contents are now durable (or it was empty); drop the
            // frozen buffer from the read set.
            self.memtables.discard_frozen(&frozen);
        }

        // Every buffered record is now durable in an SSTable: release the WAL so a
        // future recovery does not replay already-flushed data. Active is empty.
        self.wal.reset()?;

        // Cascade size-tiered compaction until no tier is over the fanout.
        while compaction::maybe_compact(&self.storage, &self.manifest, self.options.tier_fanout)? {}
        Ok(())
    }

    /// Fetch (opening and caching if needed) the reader for table `meta`.
    fn reader_for(&self, meta: &TableMeta) -> Result<Arc<SsTableReader>> {
        if let Some(r) = self
            .readers
            .read()
            .expect("readers lock poisoned")
            .get(&meta.id)
        {
            return Ok(Arc::clone(r));
        }
        let reader = Arc::new(SsTableReader::open(
            Arc::clone(&self.storage),
            &table_path(&self.dir, meta.id),
        )?);
        self.readers
            .write()
            .expect("readers lock poisoned")
            .insert(meta.id, Arc::clone(&reader));
        Ok(reader)
    }
}

/// A forward range scan: live `(key, value)` pairs in ascending key order.
///
/// Backed by an owned snapshot of every source, so it holds no lock and outlives
/// concurrent mutation. Iterating never fails: any I/O error was surfaced when
/// the scan was constructed.
pub struct Scan {
    inner: LiveIter,
}

impl std::fmt::Debug for Scan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scan").finish()
    }
}

impl Iterator for Scan {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Convert a stored SSTable [`Value`] into the in-memory [`InternalValue`].
fn internal_of(seq: Seq, value: Value) -> InternalValue {
    match value {
        Value::Put(v) => InternalValue {
            seq,
            kind: ValueKind::Value(v),
        },
        Value::Delete => InternalValue {
            seq,
            kind: ValueKind::Tombstone,
        },
    }
}

/// Whether `range` contains `key`, for any `RangeBounds<Vec<u8>>`.
fn range_contains<R: RangeBounds<Vec<u8>>>(range: &R, key: &[u8]) -> bool {
    let after_start = match range.start_bound() {
        Bound::Unbounded => true,
        Bound::Included(s) => key >= s.as_slice(),
        Bound::Excluded(s) => key > s.as_slice(),
    };
    let before_end = match range.end_bound() {
        Bound::Unbounded => true,
        Bound::Included(e) => key <= e.as_slice(),
        Bound::Excluded(e) => key < e.as_slice(),
    };
    after_start && before_end
}

// --- WAL record codec -----------------------------------------------------
//
// A WAL payload is one logical mutation:
//   [seq u64][tag u8]([klen u32][key])([vlen u32][value] iff tag == PUT)
// The WAL frames this with its own length + CRC; here we only encode the fields.

const REC_TAG_DELETE: u8 = 0;
const REC_TAG_PUT: u8 = 1;

/// One decoded WAL record.
struct Record {
    seq: Seq,
    key: Vec<u8>,
    value: InternalValue,
}

fn encode_record(seq: Seq, key: &[u8], kind: &ValueKind) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + 1 + 4 + key.len() + 8);
    buf.extend_from_slice(&seq.to_le_bytes());
    match kind {
        ValueKind::Value(v) => {
            buf.push(REC_TAG_PUT);
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        ValueKind::Tombstone => {
            buf.push(REC_TAG_DELETE);
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
        }
    }
    buf
}

fn decode_record(buf: &[u8]) -> Result<Record> {
    let corrupt = |m: &str| DbError::SsTable(SsTableError::Corrupt(format!("wal record: {m}")));
    let mut pos = 0usize;
    let take = |pos: &mut usize, n: usize| -> Result<&[u8]> {
        if buf.len() - *pos < n {
            return Err(DbError::SsTable(SsTableError::Corrupt(
                "wal record: truncated".into(),
            )));
        }
        let out = &buf[*pos..*pos + n];
        *pos += n;
        Ok(out)
    };
    let seq = u64::from_le_bytes(take(&mut pos, 8)?.try_into().expect("8"));
    let tag = take(&mut pos, 1)?[0];
    let klen = u32::from_le_bytes(take(&mut pos, 4)?.try_into().expect("4")) as usize;
    let key = take(&mut pos, klen)?.to_vec();
    let kind = match tag {
        REC_TAG_PUT => {
            let vlen = u32::from_le_bytes(take(&mut pos, 4)?.try_into().expect("4")) as usize;
            let value = take(&mut pos, vlen)?.to_vec();
            ValueKind::Value(value)
        }
        REC_TAG_DELETE => ValueKind::Tombstone,
        other => return Err(corrupt(&format!("unknown tag {other}"))),
    };
    Ok(Record {
        seq,
        key,
        value: InternalValue { seq, kind },
    })
}

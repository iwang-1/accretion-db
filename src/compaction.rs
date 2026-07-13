//! Size-tiered compaction (synchronous in this stage; a background thread is a
//! later stage).
//!
//! # The strategy, and why
//!
//! Tables are grouped into *tiers* of geometrically increasing size. A memtable
//! flush drops a fresh table into tier 0. When a tier accumulates
//! [`tier_fanout`](crate::Options) tables, they are merged into one larger table
//! that moves down to the next tier. This is **size-tiered** compaction: it has
//! low write amplification (each byte is rewritten only when its whole tier is
//! merged) and simple invariants, at the cost of higher read/space amplification
//! than leveled compaction — a deliberate tradeoff defended in `DESIGN_NOTES.md`.
//!
//! # Newest-wins merge
//!
//! The tables of a tier can hold different versions of the same key. The
//! [`MergeIterator`](crate::iter::MergeIterator) folds them into one ascending,
//! de-duplicated stream keeping only the entry with the largest sequence number
//! per key — the newest write wins.
//!
//! # Bottom-tier-only tombstone GC
//!
//! A [`Tombstone`](crate::memtable::ValueKind::Tombstone) must be preserved while
//! any *older* table (a higher-indexed tier) might still hold a live value it is
//! meant to shadow; dropping it early would resurrect a deleted key. It is safe
//! to physically drop a tombstone only when the merge output lands in the
//! bottom-most tier, because then there is nothing older left for it to mask.

use std::path::Path;
use std::sync::Arc;

use crate::iter::{EntrySource, MergeIterator};
use crate::manifest::{table_path, Manifest, ManifestError, TableMeta, Version};
use crate::memtable::{InternalValue, ValueKind};
use crate::sstable::{
    SsTableBuilder, SsTableError, SsTableReader, Value, ValueRef, DEFAULT_BITS_PER_KEY,
};
use crate::storage::{Storage, StorageError};

/// Errors produced during a flush or compaction.
#[derive(Debug)]
pub enum CompactionError {
    /// An error from the underlying storage backend.
    Storage(StorageError),
    /// An error building or reading an SSTable.
    SsTable(SsTableError),
    /// An error installing the new manifest version.
    Manifest(ManifestError),
}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactionError::Storage(e) => write!(f, "storage error: {e}"),
            CompactionError::SsTable(e) => write!(f, "sstable error: {e}"),
            CompactionError::Manifest(e) => write!(f, "manifest error: {e}"),
        }
    }
}

impl std::error::Error for CompactionError {}

impl From<StorageError> for CompactionError {
    fn from(e: StorageError) -> Self {
        CompactionError::Storage(e)
    }
}
impl From<SsTableError> for CompactionError {
    fn from(e: SsTableError) -> Self {
        CompactionError::SsTable(e)
    }
}
impl From<ManifestError> for CompactionError {
    fn from(e: ManifestError) -> Self {
        CompactionError::Manifest(e)
    }
}

/// Result alias for compaction operations.
pub type Result<T> = std::result::Result<T, CompactionError>;

/// Convert a stored SSTable [`Value`] into the in-memory [`InternalValue`] the
/// merge iterator speaks.
fn internal_of(seq: u64, value: Value) -> InternalValue {
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

/// Borrow an [`InternalValue`] as the [`ValueRef`] the builder consumes.
fn value_ref(v: &InternalValue) -> ValueRef<'_> {
    match &v.kind {
        ValueKind::Value(bytes) => ValueRef::Put(bytes),
        ValueKind::Tombstone => ValueRef::Delete,
    }
}

/// Read an entire SSTable into an owned, key-sorted vector of entries.
///
/// Compaction merges whole tiers, so a table is materialised in full here rather
/// than streamed: the [`SsTableIter`](crate::sstable::SsTableIter) borrows its
/// reader, which cannot be co-owned with the reader in one `EntrySource` without
/// `unsafe`. Tier tables are bounded in size, so this is a bounded, honest
/// simplification (noted in `DESIGN_NOTES.md`); streaming compaction is a future
/// optimisation, not a correctness requirement.
fn read_all_entries(reader: &SsTableReader) -> Result<Vec<(Vec<u8>, InternalValue)>> {
    let mut out = Vec::with_capacity(reader.num_entries() as usize);
    for entry in reader.iter() {
        let e = entry?;
        out.push((e.key, internal_of(e.seq, e.value)));
    }
    Ok(out)
}

/// Write a sorted, de-duplicated stream of `(key, value)` entries to a brand-new
/// SSTable at `path`, returning its [`TableMeta`].
///
/// Shared by the memtable flush path and compaction: both produce an ascending,
/// one-version-per-key stream. `entries` **must** be strictly key-increasing;
/// `expected_keys` sizes the Bloom filter. Returns `Ok(None)` if the stream is
/// empty (nothing is written), so callers can skip installing an empty table.
pub fn write_table<I>(
    storage: Arc<dyn Storage>,
    path: &Path,
    id: u64,
    expected_keys: usize,
    entries: I,
) -> Result<Option<TableMeta>>
where
    I: IntoIterator<Item = (Vec<u8>, InternalValue)>,
{
    let mut builder = SsTableBuilder::new(
        Arc::clone(&storage),
        path,
        expected_keys.max(1),
        DEFAULT_BITS_PER_KEY,
    )?;
    let mut first_key: Option<Vec<u8>> = None;
    let mut last_key: Vec<u8> = Vec::new();
    let mut num_entries: u64 = 0;

    for (key, value) in entries {
        builder.add(&key, value.seq, value_ref(&value))?;
        if first_key.is_none() {
            first_key = Some(key.clone());
        }
        last_key = key;
        num_entries += 1;
    }

    let Some(first_key) = first_key else {
        // Empty stream: drop the builder without finishing. The empty file it
        // created is never installed and is reclaimed by manifest GC.
        return Ok(None);
    };
    let summary = builder.finish()?;
    debug_assert_eq!(summary.num_entries, num_entries);

    Ok(Some(TableMeta {
        id,
        num_entries,
        first_key,
        last_key,
    }))
}

/// Whether compacting tier `t` produces the globally-oldest data, so a tombstone
/// has nothing older left to shadow and may be physically dropped.
///
/// This requires every tier *below* the inputs — the destination tier `t + 1`
/// **and** everything beyond it — to be empty before the merge. A subtle case the
/// property tests caught: the destination tier `t + 1` can already hold older
/// tables from prior compactions, and those may contain a live value that a
/// tombstone in tier `t` is meant to shadow. Dropping the tombstone then would
/// resurrect the deleted key, so it is only safe when tier `t + 1` and below are
/// all empty.
fn output_is_bottom(version: &Version, t: usize) -> bool {
    (t + 1..version.num_tiers()).all(|i| version.tier_len(i) == 0)
}

/// Run one compaction pass over the current version: for the youngest tier that
/// has reached `tier_fanout` tables, merge it down. Returns `true` if a
/// compaction happened (the caller loops until it returns `false` to cascade).
///
/// A single pass compacts at most one tier so the caller can re-read the freshly
/// installed version and decide whether a cascading compaction of the next tier
/// is now warranted.
pub fn maybe_compact(
    storage: &Arc<dyn Storage>,
    manifest: &Manifest,
    tier_fanout: usize,
) -> Result<bool> {
    let version = manifest.current();
    let Some(t) = (0..version.num_tiers()).find(|&t| version.tier_len(t) >= tier_fanout) else {
        return Ok(false);
    };
    compact_tier(storage, manifest, &version, t)?;
    Ok(true)
}

/// Merge every table of tier `t` into one new table in tier `t + 1` and install
/// the resulting version.
fn compact_tier(
    storage: &Arc<dyn Storage>,
    manifest: &Manifest,
    version: &Version,
    t: usize,
) -> Result<()> {
    let inputs = &version.tiers[t];

    // Open every input table and materialise it as a merge source.
    let mut expected_keys = 0usize;
    let mut sources: Vec<EntrySource> = Vec::with_capacity(inputs.len());
    for meta in inputs {
        let reader =
            SsTableReader::open(Arc::clone(storage), &table_path(manifest.dir(), meta.id))?;
        expected_keys += reader.num_entries() as usize;
        let entries = read_all_entries(&reader)?;
        sources.push(Box::new(entries.into_iter()));
    }

    let drop_tombstones = output_is_bottom(version, t);
    let out_id = version.next_table_id;
    let out_path = table_path(manifest.dir(), out_id);

    // Newest-wins merge; at the bottom tier, physically drop tombstones.
    let merged =
        MergeIterator::new(sources).filter(|(_, v)| !(drop_tombstones && v.is_tombstone()));

    let output = write_table(
        Arc::clone(storage),
        &out_path,
        out_id,
        expected_keys,
        merged,
    )?;

    // Install the new version. If the merge produced no live entries (everything
    // was tombstoned away at the bottom tier) the output table is dropped and the
    // tier is simply cleared.
    let new_version = match output {
        Some(meta) => version.compacted(t, meta),
        None => {
            let mut v = version.clone();
            v.tiers[t].clear();
            while matches!(v.tiers.last(), Some(tier) if tier.is_empty()) {
                v.tiers.pop();
            }
            v
        }
    };
    manifest.install(new_version)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimFs;
    use std::path::PathBuf;

    fn dir() -> PathBuf {
        PathBuf::from("/db")
    }

    fn iv(seq: u64, v: &str) -> InternalValue {
        InternalValue::value(seq, v)
    }
    fn tomb(seq: u64) -> InternalValue {
        InternalValue::tombstone(seq)
    }

    /// Build a tier-0 table directly through the manifest, returning nothing —
    /// helper for wiring up a compaction scenario.
    fn flush_table(
        storage: &Arc<dyn Storage>,
        manifest: &Manifest,
        entries: Vec<(Vec<u8>, InternalValue)>,
    ) {
        let version = manifest.current();
        let id = version.next_table_id;
        let next_seq = entries.iter().map(|(_, v)| v.seq).max().unwrap_or(0) + 1;
        let meta = write_table(
            Arc::clone(storage),
            &table_path(manifest.dir(), id),
            id,
            entries.len(),
            entries,
        )
        .unwrap()
        .unwrap();
        manifest.install(version.flushed(meta, next_seq)).unwrap();
    }

    fn open_reader(storage: &Arc<dyn Storage>, manifest: &Manifest, id: u64) -> SsTableReader {
        SsTableReader::open(Arc::clone(storage), &table_path(manifest.dir(), id)).unwrap()
    }

    /// Write a table file at `id` (without touching the manifest) and return its
    /// meta, so a test can hand-assemble a multi-tier version.
    fn write_at(
        storage: &Arc<dyn Storage>,
        manifest: &Manifest,
        id: u64,
        entries: Vec<(Vec<u8>, InternalValue)>,
    ) -> TableMeta {
        write_table(
            Arc::clone(storage),
            &table_path(manifest.dir(), id),
            id,
            entries.len(),
            entries,
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn compacts_tier_when_full_and_merges_newest_wins() {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let manifest = Manifest::open(Arc::clone(&storage), &dir()).unwrap();

        // Four tier-0 tables, all writing key "k" at increasing seqs.
        for seq in 1..=4u64 {
            flush_table(
                &storage,
                &manifest,
                vec![(b"k".to_vec(), iv(seq, &format!("v{seq}")))],
            );
        }
        assert_eq!(manifest.current().tier_len(0), 4);

        // One pass compacts tier 0 into tier 1.
        assert!(maybe_compact(&storage, &manifest, 4).unwrap());
        let v = manifest.current();
        assert_eq!(v.tier_len(0), 0);
        assert_eq!(v.tier_len(1), 1);

        // The merged table holds the newest value for "k".
        let out_id = v.tiers[1][0].id;
        let reader = open_reader(&storage, &manifest, out_id);
        let e = reader.get(b"k").unwrap().unwrap();
        assert_eq!(e.value, Value::Put(b"v4".to_vec()));
        assert_eq!(e.seq, 4);
    }

    #[test]
    fn bottom_tier_drops_tombstones_top_tier_keeps_them() {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(7));
        let manifest = Manifest::open(Arc::clone(&storage), &dir()).unwrap();

        // Four tables: key "d" is live then tombstoned; "k" is always live.
        flush_table(&storage, &manifest, vec![(b"d".to_vec(), iv(1, "alive"))]);
        flush_table(&storage, &manifest, vec![(b"k".to_vec(), iv(2, "one"))]);
        flush_table(&storage, &manifest, vec![(b"d".to_vec(), tomb(3))]);
        flush_table(&storage, &manifest, vec![(b"k".to_vec(), iv(4, "two"))]);

        // Compacting tier 0 -> tier 1: tier 1 IS the bottom tier, so the "d"
        // tombstone (and the value it shadows) are dropped entirely.
        assert!(maybe_compact(&storage, &manifest, 4).unwrap());
        let v = manifest.current();
        let out_id = v.tiers[1][0].id;
        let reader = open_reader(&storage, &manifest, out_id);
        assert!(
            reader.get(b"d").unwrap().is_none(),
            "tombstone GC'd at bottom"
        );
        assert_eq!(
            reader.get(b"k").unwrap().unwrap().value,
            Value::Put(b"two".to_vec())
        );
    }

    #[test]
    fn tombstone_preserved_when_output_not_bottom() {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(9));
        let manifest = Manifest::open(Arc::clone(&storage), &dir()).unwrap();

        // Hand-assemble a version with a populated bottom tier 2 holding a live
        // "d", and four tier-0 tables that tombstone "d". Compacting tier 0 ->
        // tier 1 puts the output ABOVE the bottom tier 2, so the tombstone MUST be
        // kept: tier 2 still holds a live "d" it has to shadow.
        let mut v = Version::empty();
        for seq in 1..=4u64 {
            let meta = write_at(
                &storage,
                &manifest,
                seq,
                vec![(b"d".to_vec(), tomb(seq + 10))],
            );
            v = v.flushed(meta, seq + 11);
        }
        // Bottom tier 2 with an older live "d".
        let bottom = write_at(
            &storage,
            &manifest,
            100,
            vec![(b"d".to_vec(), iv(1, "old"))],
        );
        while v.tiers.len() <= 2 {
            v.tiers.push(Vec::new());
        }
        v.tiers[2].push(Arc::new(bottom));
        v.next_table_id = 101;
        manifest.install(v).unwrap();

        assert!(!output_is_bottom(&manifest.current(), 0), "tier 2 is below");
        assert!(maybe_compact(&storage, &manifest, 4).unwrap());

        // The merged tier-1 output must still carry the "d" tombstone.
        let v = manifest.current();
        let out_id = v.tiers[1][0].id;
        let reader = open_reader(&storage, &manifest, out_id);
        let e = reader
            .get(b"d")
            .unwrap()
            .expect("tombstone kept above bottom");
        assert_eq!(e.value, Value::Delete);
    }

    #[test]
    fn no_compaction_below_fanout() {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let manifest = Manifest::open(Arc::clone(&storage), &dir()).unwrap();
        flush_table(&storage, &manifest, vec![(b"a".to_vec(), iv(1, "x"))]);
        assert!(!maybe_compact(&storage, &manifest, 4).unwrap());
        assert_eq!(manifest.current().tier_len(0), 1);
    }
}

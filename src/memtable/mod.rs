//! The in-memory write buffer: a sorted `BTreeMap` behind an `RwLock`, with a
//! size-triggered *atomic freeze* that keeps concurrent reads correct while a
//! frozen buffer is being flushed to an SSTable.
//!
//! # Shape
//!
//! * [`Memtable`] is one immutable-once-frozen sorted table: a
//!   `BTreeMap<Vec<u8>, InternalValue>` plus an approximate byte accounting.
//!   The *active* table is mutated in place; a *frozen* table is wrapped in an
//!   `Arc` and never changes again.
//! * [`MemtableSet`] owns `RwLock<MemState { active, frozen: Vec<Arc<Memtable>> }>`.
//!   Writes go to `active`; when it grows past `max_bytes` a [`freeze`] moves it
//!   (atomically, under the write lock) onto the `frozen` list and installs a
//!   fresh empty `active`.
//!
//! # Why the frozen list exists
//!
//! Flushing a frozen table to disk is not instantaneous. If freezing simply
//! dropped the old buffer, a reader could miss a key during the window between
//! "buffer frozen" and "SSTable durable + visible". Instead the frozen table
//! stays on the `frozen` list and remains fully readable; the flusher removes it
//! (via [`MemtableSet::discard_frozen`]) only *after* the SSTable that supersedes
//! it is installed. During the overlap a key is covered by both the frozen table
//! and the new SSTable — never neither — and newest-wins de-duplication (see the
//! [`crate::iter`] merge iterator) resolves the duplicate correctly.
//!
//! [`freeze`]: MemtableSet::freeze

use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests;

/// A logical timestamp assigned to every write. Monotonically increasing and
/// globally unique across the engine, so "newest-wins" is a total order.
pub type Seq = u64;

/// One (key, value) pair as the merge iterator sees it — the key alongside the
/// versioned value that currently wins for it in a single source.
pub type Entry = (Vec<u8>, InternalValue);

/// What a versioned value *is*: either a live value or a deletion marker.
///
/// Tombstones are first-class so that a delete can shadow an older value that
/// still lives in a lower tier. They are only physically dropped during
/// bottom-tier compaction (see the compaction module), never in the memtable.
#[derive(Clone, PartialEq, Eq)]
pub enum ValueKind {
    /// A live value.
    Value(Vec<u8>),
    /// A deletion marker shadowing any older value for the same key.
    Tombstone,
}

impl fmt::Debug for ValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Print the length rather than the bytes: values can be large and
            // binary, and the length is what matters when eyeballing a dump.
            ValueKind::Value(v) => write!(f, "Value({} bytes)", v.len()),
            ValueKind::Tombstone => f.write_str("Tombstone"),
        }
    }
}

/// A value tagged with the sequence number of the write that produced it.
///
/// Ordering between two `InternalValue`s for the *same key* is by [`seq`](Self::seq):
/// the larger sequence number is newer and wins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalValue {
    /// The sequence number of the write that produced this value.
    pub seq: Seq,
    /// Whether this is a live value or a tombstone.
    pub kind: ValueKind,
}

impl InternalValue {
    /// A live value written at `seq`.
    pub fn value(seq: Seq, value: impl Into<Vec<u8>>) -> Self {
        InternalValue {
            seq,
            kind: ValueKind::Value(value.into()),
        }
    }

    /// A tombstone (deletion marker) written at `seq`.
    pub fn tombstone(seq: Seq) -> Self {
        InternalValue {
            seq,
            kind: ValueKind::Tombstone,
        }
    }

    /// Whether this value is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self.kind, ValueKind::Tombstone)
    }

    /// The live bytes, or `None` if this is a tombstone.
    pub fn as_value(&self) -> Option<&[u8]> {
        match &self.kind {
            ValueKind::Value(v) => Some(v),
            ValueKind::Tombstone => None,
        }
    }

    /// Approximate heap footprint of the payload (used for memtable sizing).
    fn payload_bytes(&self) -> usize {
        match &self.kind {
            ValueKind::Value(v) => v.len(),
            ValueKind::Tombstone => 0,
        }
    }
}

/// A single sorted in-memory table.
///
/// The active table is mutated in place through [`insert`](Self::insert); once
/// [frozen](MemtableSet::freeze) it is shared as `Arc<Memtable>` and never
/// mutated again, so snapshots taken from it are stable.
#[derive(Default)]
pub struct Memtable {
    map: BTreeMap<Vec<u8>, InternalValue>,
    /// Approximate live byte count: sum over entries of key + payload + a fixed
    /// per-entry overhead. Used only to decide when to freeze, so an estimate is
    /// fine.
    approx_bytes: usize,
}

/// Fixed per-entry bookkeeping charge, so a table full of tiny keys still
/// accounts for its `BTreeMap` node overhead and eventually freezes.
const ENTRY_OVERHEAD: usize = 32;

impl Memtable {
    /// A fresh, empty table.
    pub fn new() -> Self {
        Memtable::default()
    }

    /// Insert or overwrite `key` with `val`, keeping the byte accounting current.
    ///
    /// An overwrite replaces the previous value unconditionally: callers assign
    /// monotonically increasing sequence numbers, so the incoming write is by
    /// construction the newest for this key within this (active) table.
    pub fn insert(&mut self, key: Vec<u8>, val: InternalValue) {
        let incoming = key.len() + val.payload_bytes() + ENTRY_OVERHEAD;
        if let Some(prev) = self.map.get(&key) {
            let outgoing = key.len() + prev.payload_bytes() + ENTRY_OVERHEAD;
            self.approx_bytes = self.approx_bytes - outgoing + incoming;
        } else {
            self.approx_bytes += incoming;
        }
        self.map.insert(key, val);
    }

    /// Look up `key` in this table only.
    pub fn get(&self, key: &[u8]) -> Option<&InternalValue> {
        self.map.get(key)
    }

    /// Number of distinct keys held.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the table holds no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate live byte footprint (the freeze trigger reads this).
    pub fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    /// Iterate all entries in ascending key order, borrowing.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &InternalValue)> {
        self.map.iter()
    }

    /// Clone every entry into an owned, key-sorted `Vec` — a stable snapshot
    /// suitable as a merge-iterator source.
    pub fn snapshot(&self) -> Vec<Entry> {
        self.map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Clone the entries whose keys fall in `range` into an owned, key-sorted
    /// `Vec` — a bounded snapshot for a forward range scan.
    pub fn range_snapshot<R>(&self, range: R) -> Vec<Entry>
    where
        R: RangeBounds<Vec<u8>>,
    {
        self.map
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl fmt::Debug for Memtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Memtable")
            .field("entries", &self.map.len())
            .field("approx_bytes", &self.approx_bytes)
            .finish()
    }
}

/// The mutable state guarded by the set's `RwLock`.
#[derive(Debug, Default)]
struct MemState {
    active: Memtable,
    /// Frozen tables awaiting flush, oldest first (newest pushed at the back).
    frozen: Vec<Arc<Memtable>>,
}

/// The engine's in-memory buffer: one mutable *active* table plus a list of
/// *frozen* tables awaiting flush, all behind a single `RwLock`.
///
/// Reads take the read lock and consult `active` first, then frozen tables
/// newest-first; because sequence numbers only ever increase, the first table
/// that contains the key holds its newest version. Writes take the write lock.
#[derive(Debug)]
pub struct MemtableSet {
    state: RwLock<MemState>,
    max_bytes: usize,
}

impl MemtableSet {
    /// Create a set whose active table freezes once it exceeds `max_bytes`.
    pub fn new(max_bytes: usize) -> Self {
        MemtableSet {
            state: RwLock::new(MemState::default()),
            max_bytes,
        }
    }

    /// Insert or overwrite `key` in the active table.
    pub fn insert(&self, key: Vec<u8>, val: InternalValue) {
        let mut st = self.state.write().expect("memtable lock poisoned");
        st.active.insert(key, val);
    }

    /// Resolve `key` to its newest version across active + frozen tables, or
    /// `None` if no table mentions it. A returned [`InternalValue`] may be a
    /// tombstone — callers decide whether that counts as "present".
    pub fn get(&self, key: &[u8]) -> Option<InternalValue> {
        let st = self.state.read().expect("memtable lock poisoned");
        if let Some(v) = st.active.get(key) {
            return Some(v.clone());
        }
        // Newest frozen table first.
        for table in st.frozen.iter().rev() {
            if let Some(v) = table.get(key) {
                return Some(v.clone());
            }
        }
        None
    }

    /// Approximate byte footprint of the active table.
    pub fn active_bytes(&self) -> usize {
        self.state
            .read()
            .expect("memtable lock poisoned")
            .active
            .approx_bytes()
    }

    /// Whether the active table has grown past its freeze threshold.
    pub fn is_full(&self) -> bool {
        self.active_bytes() >= self.max_bytes
    }

    /// Number of frozen tables currently awaiting flush.
    pub fn frozen_count(&self) -> usize {
        self.state
            .read()
            .expect("memtable lock poisoned")
            .frozen
            .len()
    }

    /// Atomically move the active table onto the frozen list and install a fresh
    /// empty active table; returns an `Arc` to the just-frozen table.
    ///
    /// The whole swap happens under one write-lock acquisition, so no reader can
    /// observe a state in which the buffer's contents have gone missing.
    pub fn freeze(&self) -> Arc<Memtable> {
        let mut st = self.state.write().expect("memtable lock poisoned");
        let frozen = Arc::new(std::mem::take(&mut st.active));
        st.frozen.push(Arc::clone(&frozen));
        frozen
    }

    /// Freeze only if the active table is full and non-empty; returns the frozen
    /// table when a freeze happened.
    pub fn freeze_if_full(&self) -> Option<Arc<Memtable>> {
        let mut st = self.state.write().expect("memtable lock poisoned");
        if st.active.is_empty() || st.active.approx_bytes() < self.max_bytes {
            return None;
        }
        let frozen = Arc::new(std::mem::take(&mut st.active));
        st.frozen.push(Arc::clone(&frozen));
        Some(frozen)
    }

    /// Snapshot the current frozen list (cloning the `Arc`s), oldest first.
    ///
    /// The flusher uses this to pick a table to write out; holding an `Arc`
    /// keeps that table readable even after [`discard_frozen`](Self::discard_frozen)
    /// removes it from the set.
    pub fn frozen(&self) -> Vec<Arc<Memtable>> {
        self.state
            .read()
            .expect("memtable lock poisoned")
            .frozen
            .clone()
    }

    /// Remove `table` from the frozen list by pointer identity (called once its
    /// contents are durable in an SSTable). No-op if it is already gone.
    pub fn discard_frozen(&self, table: &Arc<Memtable>) {
        let mut st = self.state.write().expect("memtable lock poisoned");
        st.frozen.retain(|t| !Arc::ptr_eq(t, table));
    }

    /// Build one boxed entry iterator per in-memory table (active + every frozen
    /// table), each pre-filtered to `range`, ready to feed the merge iterator.
    ///
    /// Each iterator owns a stable snapshot, so it is valid even if the set is
    /// mutated afterwards. Source order is irrelevant to correctness: the merge
    /// iterator resolves duplicates by sequence number, not source position.
    pub fn scan_iters<R>(&self, range: R) -> Vec<Box<dyn Iterator<Item = Entry> + Send>>
    where
        R: RangeBounds<Vec<u8>> + Clone,
    {
        let st = self.state.read().expect("memtable lock poisoned");
        let mut iters: Vec<Box<dyn Iterator<Item = Entry> + Send>> =
            Vec::with_capacity(st.frozen.len() + 1);
        iters.push(Box::new(
            st.active.range_snapshot(range.clone()).into_iter(),
        ));
        for table in &st.frozen {
            iters.push(Box::new(table.range_snapshot(range.clone()).into_iter()));
        }
        iters
    }

    /// Convenience: entry iterators over the entire key space of every in-memory
    /// table.
    pub fn full_iters(&self) -> Vec<Box<dyn Iterator<Item = Entry> + Send>> {
        self.scan_iters::<(Bound<Vec<u8>>, Bound<Vec<u8>>)>((Bound::Unbounded, Bound::Unbounded))
    }
}

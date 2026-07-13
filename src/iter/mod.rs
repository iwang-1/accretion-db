//! The read path's k-way merge: fold many key-sorted sources (active + frozen
//! memtables, then SSTable tiers) into one ascending, de-duplicated stream.
//!
//! # What it guarantees
//!
//! Given any number of sources that each yield [`Entry`]s in ascending key
//! order and hold at most one version per key, [`MergeIterator`]:
//!
//! * yields keys in ascending order (a forward scan);
//! * for a key present in several sources, yields exactly **one** entry — the
//!   one with the largest [`Seq`] (*newest-wins*), discarding the rest;
//! * preserves [`Tombstone`](crate::memtable::ValueKind::Tombstone)s, because a
//!   deletion must still shadow older live values in lower tiers. The
//!   compaction merge consumes this raw stream. User-facing scans layer
//!   [`MergeIterator::live`] on top to drop tombstones and expose plain
//!   `(key, value)` pairs.
//!
//! # Why newest-wins is well defined here
//!
//! Every source holds at most one version of any key, so for a given key there
//! is at most one entry per source. When that key is the smallest remaining key
//! across all sources, it sits at the head of *every* source that contains it
//! simultaneously — so all its versions are in the heap at once and the largest
//! sequence number is guaranteed to pop first. See the module tests for the
//! adversarial cases (interleaved overwrites, deletes, resurrections).

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::memtable::{Entry, InternalValue};

#[cfg(test)]
mod proptests;
#[cfg(test)]
mod tests;

/// A boxed, key-sorted source of entries. The memtable set and each SSTable
/// reader produce these; the merge iterator does not care which is which.
pub type EntrySource = Box<dyn Iterator<Item = Entry> + Send>;

/// One source's current head, ordered for the merge heap.
///
/// [`BinaryHeap`] is a max-heap, so [`Ord`] is written "backwards": the element
/// that should come out *first* must compare as the *greatest*. We want the
/// smallest key first, and among equal keys the largest sequence number first.
struct HeapHead {
    key: Vec<u8>,
    value: InternalValue,
    /// Index into the sources vec, so the popped head can be refilled.
    source: usize,
}

impl PartialEq for HeapHead {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapHead {}

impl Ord for HeapHead {
    fn cmp(&self, other: &Self) -> Ordering {
        // Smallest key should be "greatest" so it pops first: reverse the key
        // comparison. Break ties by *largest* seq popping first: forward seq
        // comparison (larger seq = greater = pops first). The source index is a
        // final tiebreaker purely for a total, deterministic order.
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.value.seq.cmp(&other.value.seq))
            .then_with(|| other.source.cmp(&self.source))
    }
}
impl PartialOrd for HeapHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A forward, newest-wins, tombstone-preserving merge over many key-sorted
/// sources. Construct with [`MergeIterator::new`]; iterate for [`Entry`]s, or
/// call [`live`](Self::live) to drop tombstones and expose `(key, value)`.
pub struct MergeIterator {
    sources: Vec<EntrySource>,
    heap: BinaryHeap<HeapHead>,
}

impl std::fmt::Debug for MergeIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergeIterator")
            .field("sources", &self.sources.len())
            .field("heap_len", &self.heap.len())
            .finish()
    }
}

impl MergeIterator {
    /// Build a merge over `sources`. Each source must yield entries in ascending
    /// key order and hold at most one version per key; nothing else is assumed.
    ///
    /// Source order is irrelevant to correctness — duplicates are resolved by
    /// sequence number, not by position — so callers may pass memtables and
    /// tiers in any convenient order.
    pub fn new(sources: Vec<EntrySource>) -> Self {
        let mut sources = sources;
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (source, it) in sources.iter_mut().enumerate() {
            if let Some((key, value)) = it.next() {
                heap.push(HeapHead { key, value, source });
            }
        }
        MergeIterator { sources, heap }
    }

    /// Pull the next entry from source `idx` (if any) back onto the heap.
    fn refill(&mut self, idx: usize) {
        if let Some((key, value)) = self.sources[idx].next() {
            self.heap.push(HeapHead {
                key,
                value,
                source: idx,
            });
        }
    }

    /// Adapt this raw merge into a tombstone-free stream of `(key, value)` pairs
    /// — the user-facing forward scan. Tombstones (and the older values they
    /// shadow) are dropped.
    pub fn live(self) -> LiveIter {
        LiveIter { inner: self }
    }
}

impl Iterator for MergeIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        // The heap head is the smallest remaining key and, among duplicates of
        // that key, the largest sequence number — i.e. the winner.
        let winner = self.heap.pop()?;
        self.refill(winner.source);

        // Every other head equal to this key is an older version of the same
        // key: discard it and advance its source.
        while let Some(peek) = self.heap.peek() {
            if peek.key == winner.key {
                let dup = self.heap.pop().expect("peek just succeeded");
                self.refill(dup.source);
            } else {
                break;
            }
        }

        Some((winner.key, winner.value))
    }
}

/// A [`MergeIterator`] with tombstones filtered out, yielding plain
/// `(key, value)` byte pairs — the shape a user range scan returns.
pub struct LiveIter {
    inner: MergeIterator,
}

impl std::fmt::Debug for LiveIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveIter").finish()
    }
}

impl Iterator for LiveIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        for (key, value) in self.inner.by_ref() {
            match value.kind {
                crate::memtable::ValueKind::Value(v) => return Some((key, v)),
                crate::memtable::ValueKind::Tombstone => continue,
            }
        }
        None
    }
}

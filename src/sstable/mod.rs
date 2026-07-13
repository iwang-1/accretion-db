//! `sstable` — immutable, block-based sorted-string tables.
//!
//! An SSTable is the on-disk form a memtable takes when it is flushed: a file
//! of key/value entries sorted by key, written once and never mutated. Reads
//! locate a key through three layers, cheapest first:
//!
//! 1. a per-table **bloom filter** ([`bloom`]) that answers "definitely absent"
//!    without touching a data block;
//! 2. a **sparse index** (the first key of every data block) that binary-searches
//!    to the one block that could hold the key;
//! 3. an **in-block scan** of that single 4 KiB block.
//!
//! Every structural unit — each data block, the index block, the bloom block,
//! and the footer — carries a trailing CRC32 so a torn or bit-flipped byte is
//! detected rather than silently returned. The exact byte layout is specified in
//! `FORMAT.md`; this module is the authority that document is checked against.
//!
//! # Immutability and the storage seam
//!
//! Both the [`builder`] and the [`reader`] talk to disk only through the
//! [`Storage`](crate::storage::Storage) trait, so they run unchanged against
//! `RealFs` and the power-loss simulator `SimFs`. A builder writes the whole
//! file, then a single `sync_file` makes its bytes durable; the caller renames it
//! into place and `sync_dir`s under the manifest protocol. Because an SSTable is
//! never rewritten, the reader needs no locking: it loads the index and bloom
//! once at open time and fetches data blocks on demand (leaning on the OS page
//! cache — this engine keeps no block cache of its own, a documented scope
//! decision).

use std::fmt;

use crate::storage::StorageError;

mod bloom;
mod builder;
mod reader;

pub use bloom::BloomFilter;
pub use builder::{SsTableBuilder, DEFAULT_BITS_PER_KEY};
pub use reader::{SsTableIter, SsTableReader};

/// Target size of a data block in bytes (4 KiB).
///
/// A block is sealed once appending the next entry would push it past this size;
/// a single entry larger than a block still occupies a block of its own, so the
/// bound is a target, not a hard cap.
pub const BLOCK_SIZE: usize = 4096;

/// Magic number in the footer, identifying an `accretion-db` SSTable and
/// catching a file that is truncated to fewer bytes than a footer.
const FOOTER_MAGIC: u64 = 0x4143_4352_5F53_5354; // "ACCR_SST" (LE-ish tag)

/// On-disk format version. Bumped if the layout changes incompatibly.
const FORMAT_VERSION: u32 = 1;

/// The value bound to a key in an SSTable entry: either live bytes or a
/// tombstone marking the key deleted.
///
/// A tombstone must be stored (not simply omitted) because a lower, older tier
/// may still hold a live value for the same key; the tombstone is what shadows
/// it until compaction reaches the bottom tier and can drop both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// A live value.
    Put(Vec<u8>),
    /// A deletion marker (tombstone).
    Delete,
}

/// A borrowed view of a [`Value`], used on the write path so a flush need not
/// clone every value it hands to the builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRef<'a> {
    /// A live value, borrowed.
    Put(&'a [u8]),
    /// A deletion marker (tombstone).
    Delete,
}

/// One key/value record read back from an SSTable.
///
/// `seq` is the global sequence number the write was assigned; the read path
/// uses it to resolve newest-wins across tables and memtables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The record's key.
    pub key: Vec<u8>,
    /// The sequence number at which the write occurred.
    pub seq: u64,
    /// The value (live or tombstone).
    pub value: Value,
}

/// Errors produced while building or reading an SSTable.
#[derive(Debug)]
pub enum SsTableError {
    /// An error from the underlying [`Storage`](crate::storage::Storage) backend.
    Storage(StorageError),
    /// The file's bytes are inconsistent with the format: a bad CRC, a bad
    /// magic number, or a structure that runs past the data actually present.
    /// The message states which check failed.
    Corrupt(String),
    /// The builder was handed keys that were not strictly increasing (an SSTable
    /// requires sorted, de-duplicated input).
    Unsorted,
}

impl fmt::Display for SsTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SsTableError::Storage(e) => write!(f, "storage error: {e}"),
            SsTableError::Corrupt(msg) => write!(f, "corrupt sstable: {msg}"),
            SsTableError::Unsorted => {
                write!(f, "sstable builder requires strictly increasing keys")
            }
        }
    }
}

impl std::error::Error for SsTableError {}

impl From<StorageError> for SsTableError {
    fn from(e: StorageError) -> Self {
        SsTableError::Storage(e)
    }
}

/// Result alias for SSTable operations.
pub type Result<T> = std::result::Result<T, SsTableError>;

// --- Little-endian encoding helpers ---------------------------------------
//
// The whole file format is little-endian (see FORMAT.md). Writers push onto a
// `Vec<u8>`; readers advance a cursor and bounds-check every field so a
// truncated structure surfaces as `Corrupt` rather than a panic.

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// A bounds-checked cursor over an in-memory byte buffer.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(SsTableError::Corrupt(format!(
                "unexpected end of buffer: need {n} bytes, have {}",
                self.remaining()
            )));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
}

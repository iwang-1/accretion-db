//! `sstable`

use std::fmt;

use crate::storage::StorageError;

mod bloom;
mod builder;
mod reader;

pub use bloom::BloomFilter;
pub use builder::{SsTableBuilder, DEFAULT_BITS_PER_KEY};
pub use reader::{SsTableIter, SsTableReader};

/// Target size of a data block in bytes (4 KiB).
pub const BLOCK_SIZE: usize = 4096;

/// Magic number in the footer, identifying an `accretion-db` SSTable and
/// catching a file that is truncated to fewer bytes than a footer.
const FOOTER_MAGIC: u64 = 0x4143_4352_5F53_5354; // "ACCR_SST" (LE-ish tag)

/// On-disk format version. Bumped if the layout changes incompatibly.
const FORMAT_VERSION: u32 = 1;

/// The value bound to a key in an SSTable entry: either live bytes or a tombstone marking the key deleted.
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

/// One key/value record read back from an SSTable. `seq` is the global sequence number the write was
/// assigned; the read path uses it to resolve newest-wins across tables and memtables.
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
    /// The file's bytes are inconsistent with the format: a bad CRC, a bad magic number, or a structure
    /// that runs past the data actually present.
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

// Little-endian encoding helpers The whole file format is little-endian (see format.md).

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

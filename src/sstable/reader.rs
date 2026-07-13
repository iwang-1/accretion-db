//! [`SsTableReader`] — reads back an immutable SSTable written by
//! [`SsTableBuilder`](super::builder::SsTableBuilder).
//!
//! Opening a table reads and verifies the footer, then loads the sparse index
//! and the Bloom filter (each CRC-checked) into memory; data blocks are fetched
//! from storage on demand. A point lookup is:
//!
//! 1. `bloom.contains(key)` — a confident "absent" returns immediately without
//!    any block I/O;
//! 2. binary search of the sparse index for the one block whose key range could
//!    contain `key`;
//! 3. a linear scan of that single decoded (and CRC-verified) 4 KiB block.
//!
//! Because the file is immutable, the reader holds no locks and can be shared
//! freely. Every read re-validates the CRC of whatever structure it touches, so
//! a torn or bit-flipped byte surfaces as [`SsTableError::Corrupt`] rather than a
//! wrong answer.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::Storage;

use super::builder::{FOOTER_SIZE, TAG_DELETE, TAG_PUT};
use super::{
    BloomFilter, Cursor, Entry, Result, SsTableError, Value, BLOCK_SIZE, FOOTER_MAGIC,
    FORMAT_VERSION,
};

/// One entry of the in-memory sparse index: the first key of a data block and
/// where that block lives in the file.
#[derive(Debug, Clone)]
struct BlockHandle {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

/// A read handle over one immutable SSTable.
#[derive(Debug)]
pub struct SsTableReader {
    storage: Arc<dyn Storage>,
    path: PathBuf,
    index: Vec<BlockHandle>,
    bloom: BloomFilter,
    num_entries: u64,
}

impl SsTableReader {
    /// Open the SSTable at `path`, parsing and verifying its footer, sparse
    /// index, and Bloom filter. Returns [`SsTableError::Corrupt`] if any of
    /// those structures fails its checksum or bounds checks.
    pub fn open(storage: Arc<dyn Storage>, path: &Path) -> Result<Self> {
        let file_len = storage.len(path)?;
        if file_len < FOOTER_SIZE as u64 {
            return Err(SsTableError::Corrupt(format!(
                "file length {file_len} is smaller than a footer ({FOOTER_SIZE} bytes)"
            )));
        }

        // Footer: fixed-size, self-checksummed, at the tail of the file.
        let footer = read_exact(&*storage, path, file_len - FOOTER_SIZE as u64, FOOTER_SIZE)?;
        verify_crc(&footer, "footer")?;
        let mut cur = Cursor::new(&footer);
        if cur.u64()? != FOOTER_MAGIC {
            return Err(SsTableError::Corrupt("footer magic mismatch".into()));
        }
        let version = cur.u32()?;
        if version != FORMAT_VERSION {
            return Err(SsTableError::Corrupt(format!(
                "unsupported format version {version}"
            )));
        }
        let index_off = cur.u64()?;
        let index_len = cur.u32()?;
        let bloom_off = cur.u64()?;
        let bloom_len = cur.u32()?;
        let num_entries = cur.u64()?;

        // Both regions must lie within the file and before the footer.
        let footer_start = file_len - FOOTER_SIZE as u64;
        check_region(index_off, index_len, footer_start, "index")?;
        check_region(bloom_off, bloom_len, footer_start, "bloom")?;

        let index_buf = read_exact(&*storage, path, index_off, index_len as usize)?;
        verify_crc(&index_buf, "index")?;
        let index = decode_index(&index_buf)?;

        let bloom_buf = read_exact(&*storage, path, bloom_off, bloom_len as usize)?;
        verify_crc(&bloom_buf, "bloom")?;
        let mut bcur = Cursor::new(&bloom_buf[..bloom_buf.len() - 4]);
        let bloom = BloomFilter::decode(&mut bcur)?;

        Ok(SsTableReader {
            storage,
            path: path.to_path_buf(),
            index,
            bloom,
            num_entries,
        })
    }

    /// Total number of entries in the table.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    /// Whether the Bloom filter believes `key` may be present (no block I/O).
    /// A `false` here is a definitive absence.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom.contains(key)
    }

    /// Look up `key`, returning its [`Entry`] (value + seq) if present.
    ///
    /// Consults the Bloom filter first, then the single candidate block. Returns
    /// `Ok(None)` for a genuine miss (including a Bloom "definitely absent").
    pub fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }
        let block_idx = match self.find_block(key) {
            Some(i) => i,
            None => return Ok(None),
        };
        let entries = self.load_block(block_idx)?;
        // Blocks are sorted by key; binary-search within.
        match entries.binary_search_by(|e| e.key.as_slice().cmp(key)) {
            Ok(i) => Ok(Some(entries[i].clone())),
            Err(_) => Ok(None),
        }
    }

    /// Index of the block that could contain `key`: the last block whose
    /// `first_key <= key`. `None` if `key` precedes the first block's first key.
    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() || key < self.index[0].first_key.as_slice() {
            return None;
        }
        // partition_point gives the count of blocks with first_key <= key.
        let p = self
            .index
            .partition_point(|h| h.first_key.as_slice() <= key);
        Some(p - 1)
    }

    /// Fetch and decode data block `idx`, verifying its CRC.
    fn load_block(&self, idx: usize) -> Result<Vec<Entry>> {
        let h = &self.index[idx];
        let raw = read_exact(&*self.storage, &self.path, h.offset, h.len as usize)?;
        verify_crc(&raw, "data block")?;
        decode_block(&raw[..raw.len() - 4])
    }

    /// A forward iterator over every entry in key order.
    pub fn iter(&self) -> SsTableIter<'_> {
        SsTableIter {
            reader: self,
            block_idx: 0,
            block: Vec::new(),
            within: 0,
            loaded: false,
        }
    }
}

/// Forward iterator over an SSTable's entries, block by block.
#[derive(Debug)]
pub struct SsTableIter<'a> {
    reader: &'a SsTableReader,
    block_idx: usize,
    block: Vec<Entry>,
    within: usize,
    loaded: bool,
}

impl Iterator for SsTableIter<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.loaded && self.within < self.block.len() {
                let e = self.block[self.within].clone();
                self.within += 1;
                return Some(Ok(e));
            }
            if self.block_idx >= self.reader.index.len() {
                return None;
            }
            match self.reader.load_block(self.block_idx) {
                Ok(entries) => {
                    self.block = entries;
                    self.within = 0;
                    self.loaded = true;
                    self.block_idx += 1;
                }
                Err(e) => {
                    // Stop iteration after surfacing the error once.
                    self.block_idx = self.reader.index.len();
                    self.loaded = false;
                    self.block.clear();
                    return Some(Err(e));
                }
            }
        }
    }
}

/// Read exactly `len` bytes at `offset`, erroring if the file is shorter.
fn read_exact(storage: &dyn Storage, path: &Path, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let n = storage.read_at(path, offset, &mut buf)?;
    if n != len {
        return Err(SsTableError::Corrupt(format!(
            "short read at offset {offset}: wanted {len} bytes, got {n}"
        )));
    }
    Ok(buf)
}

/// Verify the trailing 4-byte CRC32 of a framed structure covers all prior
/// bytes.
fn verify_crc(buf: &[u8], what: &str) -> Result<()> {
    if buf.len() < 4 {
        return Err(SsTableError::Corrupt(format!("{what} too short for a crc")));
    }
    let (body, crc_bytes) = buf.split_at(buf.len() - 4);
    let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
    let actual = crc32fast::hash(body);
    if stored != actual {
        return Err(SsTableError::Corrupt(format!(
            "{what} crc mismatch: stored {stored:#010x}, computed {actual:#010x}"
        )));
    }
    Ok(())
}

/// Bounds-check that `[offset, offset+len)` lies within `[0, limit)`.
fn check_region(offset: u64, len: u32, limit: u64, what: &str) -> Result<()> {
    if offset.saturating_add(len as u64) > limit {
        return Err(SsTableError::Corrupt(format!(
            "{what} region [{offset}, +{len}) runs past data limit {limit}"
        )));
    }
    Ok(())
}

/// Decode the sparse-index block body (CRC already stripped).
fn decode_index(buf: &[u8]) -> Result<Vec<BlockHandle>> {
    let mut cur = Cursor::new(&buf[..buf.len() - 4]);
    let count = cur.u32()? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = cur.u32()? as usize;
        let first_key = cur.take(key_len)?.to_vec();
        let offset = cur.u64()?;
        let len = cur.u32()?;
        out.push(BlockHandle {
            first_key,
            offset,
            len,
        });
    }
    Ok(out)
}

/// Decode a data-block body (CRC already stripped) into its entries.
fn decode_block(buf: &[u8]) -> Result<Vec<Entry>> {
    let mut cur = Cursor::new(buf);
    let mut out = Vec::new();
    while cur.remaining() > 0 {
        let key_len = cur.u32()? as usize;
        let key = cur.take(key_len)?.to_vec();
        let seq = cur.u64()?;
        let tag = cur.u8()?;
        let value = match tag {
            TAG_PUT => {
                let vlen = cur.u32()? as usize;
                Value::Put(cur.take(vlen)?.to_vec())
            }
            TAG_DELETE => Value::Delete,
            other => {
                return Err(SsTableError::Corrupt(format!(
                    "unknown value tag {other} in data block"
                )));
            }
        };
        out.push(Entry { key, seq, value });
    }
    Ok(out)
}

// A tiny compile-time nudge that BLOCK_SIZE stays reasonable relative to reads.
const _: () = assert!(BLOCK_SIZE >= 512);

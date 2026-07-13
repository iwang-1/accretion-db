//! [`SsTableBuilder`] — writes a sorted run of entries into an immutable SSTable.
//!
//! The builder streams entries (which the caller must supply in strictly
//! increasing key order) into 4 KiB data blocks, accumulating a sparse index
//! (the first key of each block) and a Bloom filter over every key. On
//! [`finish`](SsTableBuilder::finish) it appends the index block, the bloom
//! block, and a fixed-size checksummed footer, then makes the file's bytes
//! durable with a single `sync_file`.
//!
//! The full byte layout is specified in `FORMAT.md`; the encoding here is the
//! authority that document mirrors. Every structural unit ends in a CRC32 so the
//! [reader](super::reader) can detect a torn or bit-flipped byte.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::Storage;

use super::{
    put_u32, put_u64, BloomFilter, Result, SsTableError, ValueRef, BLOCK_SIZE, FOOTER_MAGIC,
    FORMAT_VERSION,
};

/// Default Bloom filter budget in bits per key.
///
/// 10 bits/key yields a theoretical false-positive rate of `0.6185^10 ≈ 0.8%`
/// at the optimal `k = 7` (see [`bloom`](super::bloom) for the derivation) — the
/// usual sweet spot between filter size and probe cost.
pub const DEFAULT_BITS_PER_KEY: usize = 10;

/// Value tag byte written ahead of each entry's value in a data block.
pub(crate) const TAG_DELETE: u8 = 0;
pub(crate) const TAG_PUT: u8 = 1;

/// Size in bytes of the fixed-layout footer at the tail of every SSTable.
///
/// `magic(8) + version(4) + index_off(8) + index_len(4) + bloom_off(8) +
/// bloom_len(4) + num_entries(8) + crc(4)`.
pub(crate) const FOOTER_SIZE: usize = 48;

/// Metadata about a finished SSTable, returned by
/// [`finish`](SsTableBuilder::finish) for the caller (e.g. the manifest) to
/// record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsTableSummary {
    /// Total number of entries written.
    pub num_entries: u64,
    /// The smallest key in the table (`None` iff the table is empty).
    pub first_key: Option<Vec<u8>>,
    /// The largest key in the table (`None` iff the table is empty).
    pub last_key: Option<Vec<u8>>,
    /// Total bytes written to the file.
    pub file_len: u64,
}

/// Streams sorted entries into an immutable SSTable file.
#[derive(Debug)]
pub struct SsTableBuilder {
    storage: Arc<dyn Storage>,
    path: PathBuf,
    /// Bytes of the current, not-yet-sealed data block (entries only; the CRC is
    /// appended when the block is sealed).
    block: Vec<u8>,
    /// First key of the current block, captured when its first entry lands.
    block_first_key: Option<Vec<u8>>,
    /// The sparse index: `(first_key, block_offset, block_len_with_crc)` per
    /// sealed block.
    index: Vec<IndexEntry>,
    /// Bloom filter over every key added.
    bloom: BloomFilter,
    /// Running append offset (where the next byte lands in the file).
    offset: u64,
    /// Entries written so far.
    num_entries: u64,
    /// The most recently added key, to enforce strictly-increasing order.
    last_key: Option<Vec<u8>>,
    first_key: Option<Vec<u8>>,
}

#[derive(Debug)]
struct IndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    len: u32,
}

impl SsTableBuilder {
    /// Create a builder that writes to `path` on `storage`.
    ///
    /// `expected_keys` sizes the Bloom filter (see [`BloomFilter::new`]);
    /// supplying the exact count from the flushing memtable makes the filter's
    /// FPR land on target. The file is created empty immediately.
    pub fn new(
        storage: Arc<dyn Storage>,
        path: &Path,
        expected_keys: usize,
        bits_per_key: usize,
    ) -> Result<Self> {
        storage.create(path)?;
        Ok(SsTableBuilder {
            storage,
            path: path.to_path_buf(),
            block: Vec::with_capacity(BLOCK_SIZE + 256),
            block_first_key: None,
            index: Vec::new(),
            bloom: BloomFilter::new(expected_keys, bits_per_key),
            offset: 0,
            num_entries: 0,
            last_key: None,
            first_key: None,
        })
    }

    /// Add one entry. Keys must arrive strictly increasing; a key `<=` the
    /// previous one returns [`SsTableError::Unsorted`].
    pub fn add(&mut self, key: &[u8], seq: u64, value: ValueRef<'_>) -> Result<()> {
        if let Some(prev) = &self.last_key {
            if key <= prev.as_slice() {
                return Err(SsTableError::Unsorted);
            }
        }

        // Seal the current block first if appending this entry would overflow the
        // 4 KiB target — but never seal an empty block (a single oversized entry
        // gets a block to itself).
        let entry_len = encoded_entry_len(key, value);
        if !self.block.is_empty() && self.block.len() + entry_len > BLOCK_SIZE {
            self.seal_block()?;
        }

        if self.block_first_key.is_none() {
            self.block_first_key = Some(key.to_vec());
        }
        encode_entry(&mut self.block, key, seq, value);

        self.bloom.insert(key);
        if self.first_key.is_none() {
            self.first_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
        self.num_entries += 1;
        Ok(())
    }

    /// Seal the current data block: append its CRC, flush it to storage, and
    /// record its sparse-index entry. A no-op if the block is empty.
    fn seal_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let crc = crc32fast::hash(&self.block);
        put_u32(&mut self.block, crc);
        let block_offset = self.offset;
        let block_len = self.block.len() as u32;
        self.storage.append(&self.path, &self.block)?;
        self.offset += block_len as u64;
        self.index.push(IndexEntry {
            first_key: self.block_first_key.take().expect("block has a first key"),
            offset: block_offset,
            len: block_len,
        });
        self.block.clear();
        Ok(())
    }

    /// Finish the table: flush the last block, write the index and bloom blocks
    /// and the footer, then `sync_file` the whole file durable. Returns a
    /// [`SsTableSummary`]. The caller is responsible for the rename+`sync_dir`
    /// that installs the file under the manifest protocol.
    pub fn finish(mut self) -> Result<SsTableSummary> {
        self.seal_block()?;

        // Index block: [num_entries u32] then per entry
        // [key_len u32][key][offset u64][len u32], then a trailing CRC32.
        let mut index_buf = Vec::new();
        put_u32(&mut index_buf, self.index.len() as u32);
        for e in &self.index {
            put_u32(&mut index_buf, e.first_key.len() as u32);
            index_buf.extend_from_slice(&e.first_key);
            put_u64(&mut index_buf, e.offset);
            put_u32(&mut index_buf, e.len);
        }
        let index_crc = crc32fast::hash(&index_buf);
        put_u32(&mut index_buf, index_crc);
        let index_offset = self.offset;
        let index_len = index_buf.len() as u32;
        self.storage.append(&self.path, &index_buf)?;
        self.offset += index_len as u64;

        // Bloom block: the encoded filter followed by a trailing CRC32.
        let mut bloom_buf = Vec::new();
        self.bloom.encode(&mut bloom_buf);
        let bloom_crc = crc32fast::hash(&bloom_buf);
        put_u32(&mut bloom_buf, bloom_crc);
        let bloom_offset = self.offset;
        let bloom_len = bloom_buf.len() as u32;
        self.storage.append(&self.path, &bloom_buf)?;
        self.offset += bloom_len as u64;

        // Footer: fixed FOOTER_SIZE bytes, self-checksummed.
        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        put_u64(&mut footer, FOOTER_MAGIC);
        put_u32(&mut footer, FORMAT_VERSION);
        put_u64(&mut footer, index_offset);
        put_u32(&mut footer, index_len);
        put_u64(&mut footer, bloom_offset);
        put_u32(&mut footer, bloom_len);
        put_u64(&mut footer, self.num_entries);
        let footer_crc = crc32fast::hash(&footer);
        put_u32(&mut footer, footer_crc);
        debug_assert_eq!(footer.len(), FOOTER_SIZE);
        self.storage.append(&self.path, &footer)?;
        self.offset += footer.len() as u64;

        // One durability barrier for the whole file. Directory-entry durability
        // (rename) is the manifest layer's responsibility.
        self.storage.sync_file(&self.path)?;

        Ok(SsTableSummary {
            num_entries: self.num_entries,
            first_key: self.first_key,
            last_key: self.last_key,
            file_len: self.offset,
        })
    }
}

/// Number of bytes [`encode_entry`] will write for this key/value.
fn encoded_entry_len(key: &[u8], value: ValueRef<'_>) -> usize {
    // key_len(4) + key + seq(8) + tag(1) + [value_len(4) + value]
    let value_part = match value {
        ValueRef::Put(v) => 4 + v.len(),
        ValueRef::Delete => 0,
    };
    4 + key.len() + 8 + 1 + value_part
}

/// Append one entry to `buf`:
/// `[key_len u32][key][seq u64][tag u8]([value_len u32][value])`.
fn encode_entry(buf: &mut Vec<u8>, key: &[u8], seq: u64, value: ValueRef<'_>) {
    put_u32(buf, key.len() as u32);
    buf.extend_from_slice(key);
    put_u64(buf, seq);
    match value {
        ValueRef::Put(v) => {
            buf.push(TAG_PUT);
            put_u32(buf, v.len() as u32);
            buf.extend_from_slice(v);
        }
        ValueRef::Delete => {
            buf.push(TAG_DELETE);
        }
    }
}

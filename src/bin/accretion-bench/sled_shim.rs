//! The sled baseline behind the same [`KvBench`] surface, compiled only under
//! `--features bench-sled`. Two configurations, matching accretion-db's two
//! crash-safety contracts (see the durability table in `kv.rs`):
//!
//! * [`SledDurable`] — `insert` then `flush()` per write: ack implies fsync,
//!   the fair match for accretion `Always`.
//! * [`SledBuffered`] — default sled with the 500 ms background flusher
//!   *disabled* (`flush_every_ms = None`) and no explicit `flush`: ack does not
//!   imply durability, the fair match for accretion `OsBuffered`.
//!
//! Both are opened via [`sled::Config`] with equal cache and segment settings so
//! the only variable between them is when (and whether) the fsync barrier runs.

use std::path::Path;

use crate::kv::KvBench;
use crate::BenchResult;

/// Shared sled opener: one config knob (`flush_every_ms`) differs per mode.
fn open_sled(dir: &Path, flush_every_ms: Option<u64>) -> BenchResult<sled::Db> {
    let db = sled::Config::new()
        .path(dir)
        .flush_every_ms(flush_every_ms)
        // Modest fixed cache so results reflect the engine, not a giant RAM
        // cache; documented in RESULTS.md alongside accretion's memtable size.
        .cache_capacity(64 * 1024 * 1024)
        .open()
        .map_err(|e| boxed(format!("sled open: {e}")))?;
    Ok(db)
}

/// sled matched to accretion `Always`: fsync (via `flush`) after every insert.
#[derive(Debug)]
pub struct SledDurable {
    db: sled::Db,
}

impl SledDurable {
    /// Open a durable-mode sled at `dir` (background flusher disabled; every
    /// write is explicitly flushed instead, so the flusher is not a variable).
    pub fn open(dir: &Path) -> BenchResult<Self> {
        Ok(SledDurable {
            db: open_sled(dir, None)?,
        })
    }
}

impl KvBench for SledDurable {
    fn label(&self) -> String {
        "sled/insert+flush(durable)".to_string()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        self.db
            .insert(key, value)
            .map_err(|e| boxed(format!("sled insert: {e}")))?;
        // The durability barrier: matches accretion Always's per-write fsync.
        self.db
            .flush()
            .map_err(|e| boxed(format!("sled flush: {e}")))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
        Ok(self
            .db
            .get(key)
            .map_err(|e| boxed(format!("sled get: {e}")))?
            .map(|ivec| ivec.to_vec()))
    }

    fn scan_count(&self, start: &[u8], end: &[u8]) -> BenchResult<usize> {
        let mut n = 0usize;
        for item in self.db.range(start.to_vec()..end.to_vec()) {
            item.map_err(|e| boxed(format!("sled range: {e}")))?;
            n += 1;
        }
        Ok(n)
    }

    fn flush(&self) -> BenchResult<()> {
        self.db
            .flush()
            .map_err(|e| boxed(format!("sled flush: {e}")))?;
        Ok(())
    }
}

/// sled matched to accretion `OsBuffered`: buffered insert, no synchronous
/// fsync, background flusher disabled so it is not a hidden variable.
#[derive(Debug)]
pub struct SledBuffered {
    db: sled::Db,
}

impl SledBuffered {
    /// Open a buffered-mode sled at `dir` (`flush_every_ms = None`).
    pub fn open(dir: &Path) -> BenchResult<Self> {
        Ok(SledBuffered {
            db: open_sled(dir, None)?,
        })
    }
}

impl KvBench for SledBuffered {
    fn label(&self) -> String {
        "sled/buffered(no-flush)".to_string()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        // No flush: ack after buffered insert, matching accretion OsBuffered.
        self.db
            .insert(key, value)
            .map_err(|e| boxed(format!("sled insert: {e}")))?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
        Ok(self
            .db
            .get(key)
            .map_err(|e| boxed(format!("sled get: {e}")))?
            .map(|ivec| ivec.to_vec()))
    }

    fn scan_count(&self, start: &[u8], end: &[u8]) -> BenchResult<usize> {
        let mut n = 0usize;
        for item in self.db.range(start.to_vec()..end.to_vec()) {
            item.map_err(|e| boxed(format!("sled range: {e}")))?;
            n += 1;
        }
        Ok(n)
    }

    fn flush(&self) -> BenchResult<()> {
        // Only called at phase boundaries (fill -> read); a real flush here makes
        // the fill queryable without changing the per-write buffered timing.
        self.db
            .flush()
            .map_err(|e| boxed(format!("sled flush: {e}")))?;
        Ok(())
    }
}

fn boxed(msg: String) -> Box<dyn std::error::Error + Send + Sync> {
    msg.into()
}

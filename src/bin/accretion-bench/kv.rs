//! The `KvBench` trait: the single narrow surface the throughput driver speaks,
//! implemented by both `accretion-db` and (behind `--features bench-sled`) sled,
//! so every engine runs the *same* driver code, the *same* workload generator,
//! and the *same* histogram. Any throughput or latency difference is then a
//! property of the engine, not of the measurement harness.
//!
//! ## Matched durability — the honesty-critical part
//!
//! A durability comparison is only fair if both engines make the *same promise*
//! about an acked write. accretion-db's three [`Durability`] modes map onto sled
//! configurations as follows; this mapping is reproduced verbatim in
//! `benchmarks/RESULTS.md` and in the README methodology section.
//!
//! | accretion-db mode        | promise on `put` return             | matched sled config |
//! |--------------------------|-------------------------------------|---------------------|
//! | `Always`                 | record fsync'd before ack           | `flush()` after every `insert` |
//! | `GroupCommit`            | record fsync'd (batched) before ack | *no sled equivalent* — sled has no group-commit API; reported as an accretion-only mode, never compared to sled |
//! | `OsBuffered`             | ack after buffered write, no fsync  | default sled with `flush_every_ms = None` (background flusher disabled), never calling `flush()` |
//!
//! ### Why these are the fair matches
//!
//! * **Durable match — accretion `Always` vs sled `insert` + `flush`.** sled's
//!   `Db::flush` fsyncs all dirty IO buffers (its docs), i.e. it is sled's
//!   durability barrier. Calling it after every `insert` gives sled the same
//!   *ack-implies-fsync* contract accretion `Always` gives, so both pay one
//!   fsync per write and both are bounded by the disk's ~2.79 ms fsync (see
//!   DESIGN_NOTES). This is the apples-to-apples durable row.
//!
//! * **Buffered match — accretion `OsBuffered` vs sled default, flusher off.**
//!   sled's `flush_every_ms` (default `Some(500)`) spawns a background thread
//!   that fsyncs every 500 ms; an acked `insert` is *not* fsync'd synchronously
//!   either way, so like accretion `OsBuffered` the ack does not imply
//!   durability. We set `flush_every_ms = None` and never call `flush()` so the
//!   comparison measures pure buffered-insert throughput on both sides with the
//!   background flusher removed as a variable. We disclose that neither buffered
//!   configuration is crash-safe.
//!
//! * **`GroupCommit` is deliberately *not* matched to sled.** sled exposes no
//!   group-commit knob, so any sled config we picked would be an unfair straw
//!   man. `GroupCommit` is reported as accretion-db's own headline mode against
//!   its own `Always` baseline (the group-commit multiplier), never as a sled
//!   win. Stating this explicitly is part of the honesty brand.

use std::path::Path;

use accretion_db::db::{Db, Options};
use accretion_db::Durability;
#[cfg(test)]
use accretion_db::Storage;
#[cfg(test)]
use std::sync::Arc;

use crate::BenchResult;

/// The uniform key/value surface the driver benchmarks against.
///
/// Implementations wrap a concrete engine at a chosen durability configuration.
/// Every method mirrors the durability contract of the engine it wraps: `put`
/// returns only once that engine's ack contract for the configured mode is met,
/// so the driver's per-op timing captures the real cost of the promise.
pub trait KvBench {
    /// Human-readable engine + durability label for the results table.
    fn label(&self) -> String;

    /// Insert or overwrite `key` with `value`, honouring the engine's durability
    /// contract before returning.
    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()>;

    /// Look up `key`.
    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>>;

    /// Count the live pairs in `[start, end)` (a forward scan), returning how many
    /// were visited — enough to exercise the scan path without materialising.
    fn scan_count(&self, start: &[u8], end: &[u8]) -> BenchResult<usize>;

    /// Force any buffered state to a durable, queryable resting point. Used
    /// between the fill and read phases so cold reads hit tables, not memtables.
    fn flush(&self) -> BenchResult<()>;
}

/// The accretion-db shim at a chosen [`Durability`] mode.
#[derive(Debug)]
pub struct AccretionBench {
    db: Db,
    durability: Durability,
}

impl AccretionBench {
    /// Open an accretion-db instance on the real filesystem at `dir`.
    pub fn open(dir: &Path, durability: Durability, memtable_size: usize) -> BenchResult<Self> {
        let opts = Options {
            durability,
            memtable_size,
            tier_fanout: 4,
        };
        let db = Db::open(dir, opts).map_err(|e| boxed(format!("accretion open: {e}")))?;
        Ok(AccretionBench { db, durability })
    }

    /// Open on a caller-supplied [`Storage`] backend (used by smoke tests to run
    /// the driver over an in-memory SimFs without touching the disk).
    #[cfg(test)]
    pub fn open_on(
        storage: Arc<dyn Storage>,
        dir: &Path,
        durability: Durability,
        memtable_size: usize,
    ) -> BenchResult<Self> {
        let opts = Options {
            durability,
            memtable_size,
            tier_fanout: 4,
        };
        let db =
            Db::open_on(storage, dir, opts).map_err(|e| boxed(format!("accretion open: {e}")))?;
        Ok(AccretionBench { db, durability })
    }
}

impl KvBench for AccretionBench {
    fn label(&self) -> String {
        format!("accretion-db/{:?}", self.durability)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> BenchResult<()> {
        self.db
            .put(key, value)
            .map_err(|e| boxed(format!("put: {e}")))
    }

    fn get(&self, key: &[u8]) -> BenchResult<Option<Vec<u8>>> {
        self.db.get(key).map_err(|e| boxed(format!("get: {e}")))
    }

    fn scan_count(&self, start: &[u8], end: &[u8]) -> BenchResult<usize> {
        let scan = self
            .db
            .scan(start.to_vec()..end.to_vec())
            .map_err(|e| boxed(format!("scan: {e}")))?;
        Ok(scan.count())
    }

    fn flush(&self) -> BenchResult<()> {
        self.db.flush().map_err(|e| boxed(format!("flush: {e}")))
    }
}

/// Box a message as a bench error.
fn boxed(msg: String) -> Box<dyn std::error::Error + Send + Sync> {
    msg.into()
}

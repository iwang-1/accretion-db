//! Write-ahead log: CRC-framed, segmented, with a mode-driven commit pipeline.
//!
//! The WAL is the engine's durability anchor. Every mutation is appended here
//! as a [CRC-framed record](frame) *before* it touches the memtable, so a crash
//! can always be recovered by replaying the log ([`recovery`]).
//!
//! # Segments
//!
//! The log is split into monotonically-numbered [`segment`] files. Once a flush
//! has captured every record up to some point, the whole prefix of segments is
//! released in one step rather than rewriting one ever-growing file. A new
//! segment is rolled when the active one crosses [`WalOptions::segment_size`].
//!
//! # The commit pipeline and its three durability modes
//!
//! All three modes share one code path — append the frame, then satisfy the
//! mode's durability contract before [`Wal::append`] returns:
//!
//! * [`Durability::Always`] — `fsync` per commit. Every ack implies the record
//!   is durable. Simplest and safest; throughput is fsync-bound (~1/fsync).
//! * [`Durability::GroupCommit`] — the headline mode. Concurrent writers enqueue
//!   their frame and park; a single *leader* drains the whole queue, writes every
//!   queued frame, and issues **one** `fsync` that makes all of them durable at
//!   once, then wakes the batch. This amortizes the fixed fsync cost across `N`
//!   writers (see the group-commit math in `DESIGN_NOTES.md`): throughput scales
//!   toward `N ×` the per-write ceiling while single-write latency rises toward
//!   one batch interval.
//! * [`Durability::OsBuffered`] — ack without requiring a durability barrier.
//!   Fast but *not crash-safe*: an acked write can be lost on power loss.
//!   Segment rotation may incidentally sync data, but no ack carries a durability
//!   guarantee. Offered only as the no-durability ceiling and labeled unsafe.
//!
//! The leader/follower group-commit design keeps two locks: a `coord` mutex
//! guarding the queue and completion bookkeeping (held only briefly), and a
//! `writer` mutex guarding the segment I/O. The leader releases `coord` while it
//! does the write+fsync, so writers that arrive *during* the fsync accumulate
//! into the next batch — that overlap is what turns many fsyncs into one.

mod frame;
pub mod recovery;
mod segment;

use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

use crate::storage::{Storage, StorageResult};
use frame::encode;
use recovery::{recover, truncate_tail, StopReason};
use segment::Segment;

pub use recovery::Recovered;

/// Default soft size at which the active segment is rolled (4 MiB).
pub const DEFAULT_SEGMENT_SIZE: u64 = 4 * 1024 * 1024;

/// The durability contract [`Wal::append`] honours before returning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// `fsync` on every commit — each ack implies durability.
    #[default]
    Always,
    /// Batch concurrent writers into one `fsync` (the headline mode).
    GroupCommit,
    /// Ack without requiring a durability barrier — fast, not crash-safe.
    /// Segment rotation may sync data, but an ack has no durability guarantee.
    OsBuffered,
}

impl Durability {
    /// Whether this mode guarantees an acked write survives a crash.
    pub fn is_durable(self) -> bool {
        matches!(self, Durability::Always | Durability::GroupCommit)
    }
}

/// Configuration for opening a [`Wal`].
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// The durability contract for commits.
    pub durability: Durability,
    /// Soft threshold at which the active segment is rolled.
    pub segment_size: u64,
}

impl Default for WalOptions {
    fn default() -> Self {
        WalOptions {
            durability: Durability::default(),
            segment_size: DEFAULT_SEGMENT_SIZE,
        }
    }
}

/// The segment-writing half of the WAL, guarded by its own mutex so the
/// group-commit leader can hold it across I/O while the coordinator lock is free
/// for other writers to enqueue.
#[derive(Debug)]
struct Writer {
    active: Segment,
}

/// The commit-coordination half: the pending queue plus the bookkeeping that
/// lets one leader sync a whole batch and wake every writer it covered.
#[derive(Debug, Default)]
struct Coord {
    /// FIFO of `(seq, frame_bytes)` awaiting a durability barrier.
    queue: Vec<(u64, Vec<u8>)>,
    /// Next commit sequence number to hand out (1-based; 0 means "none").
    next_seq: u64,
    /// Highest commit seq that has been made durable.
    synced_through: u64,
    /// Whether a leader is currently draining+syncing a batch.
    leader_active: bool,
}

/// A CRC-framed, segmented write-ahead log with a mode-driven commit pipeline.
///
/// Construct with [`Wal::open`], which recovers any existing log (applying the
/// torn-tail rule) and returns the recovered records alongside the handle.
/// [`Wal::append`] is safe to call concurrently from many threads; in
/// [`Durability::GroupCommit`] that concurrency is exactly what lets one `fsync`
/// serve many writers.
#[derive(Debug)]
pub struct Wal {
    fs: std::sync::Arc<dyn Storage>,
    dir: PathBuf,
    durability: Durability,
    segment_size: u64,
    writer: Mutex<Writer>,
    coord: Mutex<Coord>,
    /// Signaled when a batch completes; parked followers re-check their seq.
    batch_done: Condvar,
}

impl Wal {
    /// Open (recovering, or creating fresh) the WAL rooted at `dir`.
    ///
    /// Recovery replays every segment under the torn-tail rule, and if the tail
    /// segment ends in a torn or corrupt frame it is physically truncated to its
    /// last clean boundary so future appends start clean. Returns the handle and
    /// the [`Recovered`] records (in log order) for the engine to replay into its
    /// memtable.
    pub fn open(
        fs: std::sync::Arc<dyn Storage>,
        dir: &Path,
        options: WalOptions,
    ) -> StorageResult<(Wal, Recovered)> {
        // Recover first so we know the tail segment and its clean length.
        let recovered = recover(&*fs, dir)?;

        // If the tail ended dirty, truncate it to the clean prefix.
        if recovered.stop_reason != StopReason::Clean {
            if let Some(tail) = recovered.tail_segment {
                truncate_tail(&*fs, dir, tail, recovered.tail_valid_len)?;
            }
        }

        // Choose the active segment: reopen the tail if present, else create #1.
        let active = match segment::list_segments(&*fs, dir)?.last() {
            Some((id, _)) => Segment::open(&*fs, dir, *id)?,
            None => {
                let seg = Segment::create(&*fs, dir, 1)?;
                fs.sync_dir(dir)?;
                seg
            }
        };

        let wal = Wal {
            fs,
            dir: dir.to_path_buf(),
            durability: options.durability,
            segment_size: options.segment_size,
            writer: Mutex::new(Writer { active }),
            coord: Mutex::new(Coord::default()),
            batch_done: Condvar::new(),
        };
        Ok((wal, recovered))
    }

    /// The id of the segment currently being appended to.
    pub fn active_segment_id(&self) -> u64 {
        self.writer.lock().expect("wal writer poisoned").active.id()
    }

    /// Release the entire log: delete every segment and start a fresh, empty one.
    ///
    /// Called after a flush has durably captured every buffered record in an
    /// SSTable, so the records those segments hold are no longer needed for
    /// recovery ("segment release" in the data-flow doc). The new segment is
    /// created at `max_existing_id + 1` and its directory entry made durable, so
    /// a crash immediately afterwards recovers an empty log rather than replaying
    /// already-flushed data.
    ///
    /// Crash-safety note: the new empty segment is created and `sync_dir`d
    /// *before* the old segments are deleted, and the fresh id never collides with
    /// a deleted one. A crash between the two steps leaves stale segments that a
    /// subsequent recovery would replay — harmless, because their records are
    /// already in an SSTable and de-duplicate by sequence number on read. This is
    /// noted for the S3 crash sweep.
    pub fn reset(&self) -> StorageResult<()> {
        let mut w = self.writer.lock().expect("wal writer poisoned");
        let existing = segment::list_segments(&*self.fs, &self.dir)?;
        let next_id = existing.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

        // Create the replacement first and make its directory entry durable.
        let fresh = Segment::create(&*self.fs, &self.dir, next_id)?;
        self.fs.sync_dir(&self.dir)?;

        // Now retire the old segments (including the previously-active one).
        for (_, path) in &existing {
            self.fs.delete(path)?;
        }
        self.fs.sync_dir(&self.dir)?;

        w.active = fresh;
        Ok(())
    }

    /// Append one record `payload` and return only once the configured
    /// [`Durability`] contract is met.
    ///
    /// The payload is opaque to the WAL: it is CRC-framed verbatim. On return in
    /// a durable mode the record is guaranteed to survive a crash.
    pub fn append(&self, payload: &[u8]) -> StorageResult<()> {
        let frame = encode(payload);
        match self.durability {
            Durability::Always => self.commit_sync_now(frame),
            Durability::OsBuffered => self.commit_buffered(frame),
            Durability::GroupCommit => self.commit_group(frame),
        }
    }

    /// `Always` / per-write path: write the single frame and `fsync` before ack.
    fn commit_sync_now(&self, frame: Vec<u8>) -> StorageResult<()> {
        let mut w = self.writer.lock().expect("wal writer poisoned");
        w.active.append(&*self.fs, &frame)?;
        w.active.sync(&*self.fs)?;
        self.maybe_rotate(&mut w)?;
        Ok(())
    }

    /// `OsBuffered` path: append without requiring a durability barrier.
    fn commit_buffered(&self, frame: Vec<u8>) -> StorageResult<()> {
        let mut w = self.writer.lock().expect("wal writer poisoned");
        w.active.append(&*self.fs, &frame)?;
        self.maybe_rotate(&mut w)?;
        Ok(())
    }

    /// `GroupCommit` path: enqueue this frame, then either lead a batch (draining
    /// the queue and issuing one `fsync`) or park until a leader syncs our seq.
    fn commit_group(&self, frame: Vec<u8>) -> StorageResult<()> {
        // 1. Enqueue and claim a sequence number.
        let my_seq = {
            let mut c = self.coord.lock().expect("wal coord poisoned");
            c.next_seq += 1;
            let seq = c.next_seq;
            c.queue.push((seq, frame));
            seq
        };

        loop {
            let mut c = self.coord.lock().expect("wal coord poisoned");
            if c.synced_through >= my_seq {
                return Ok(()); // a leader already made our record durable
            }
            if c.leader_active {
                // Another writer is syncing; park until the batch completes.
                let guard = self
                    .batch_done
                    .wait(c)
                    .expect("wal coord poisoned during wait");
                drop(guard); // re-loop to re-check synced_through / leadership
                continue;
            }

            // 2. Become the leader: take the whole queue and release `coord` so
            //    late writers can enqueue into the next batch during our fsync.
            c.leader_active = true;
            let batch: Vec<(u64, Vec<u8>)> = std::mem::take(&mut c.queue);
            drop(c);

            // A leader is chosen only when synced_through < my_seq and my frame
            // was enqueued, so the queue is non-empty here.
            let highest = batch.last().map(|(s, _)| *s).unwrap_or(my_seq);
            let io = self.write_batch(&batch);

            // 3. Publish the outcome and wake every parked follower.
            let mut c = self.coord.lock().expect("wal coord poisoned");
            c.leader_active = false;
            match &io {
                Ok(()) => c.synced_through = c.synced_through.max(highest),
                Err(_) => {
                    // The batch was not made durable. Return the taken frames to
                    // the front of the queue (they hold the lowest seqs, so this
                    // preserves order) so a subsequent leader retries them rather
                    // than silently stranding parked followers. Under this crate's
                    // storage contract a write error is terminal (a crash halts
                    // every op), so those retries will re-observe the failure and
                    // every writer surfaces the error — no false ack occurs.
                    let mut restored = batch;
                    restored.extend(std::mem::take(&mut c.queue));
                    c.queue = restored;
                }
            }
            self.batch_done.notify_all();
            drop(c);

            io?;
            return Ok(());
        }
    }

    /// Write every frame of a batch to the active segment and issue one `fsync`,
    /// rolling the segment afterward if it crossed the size threshold.
    fn write_batch(&self, batch: &[(u64, Vec<u8>)]) -> StorageResult<()> {
        let mut w = self.writer.lock().expect("wal writer poisoned");
        for (_, frame) in batch {
            w.active.append(&*self.fs, frame)?;
        }
        w.active.sync(&*self.fs)?;
        self.maybe_rotate(&mut w)?;
        Ok(())
    }

    /// Roll to a fresh segment if the active one has crossed `segment_size`.
    ///
    /// The old segment's bytes are already durable in the durable modes; we make
    /// the new segment's directory entry durable with a `sync_dir` so a crash
    /// right after rotation still finds it.
    fn maybe_rotate(&self, w: &mut Writer) -> StorageResult<()> {
        if w.active.len() < self.segment_size {
            return Ok(());
        }
        let next_id = w.active.id() + 1;
        // Ensure the segment we are leaving is durable before abandoning it.
        w.active.sync(&*self.fs)?;
        let seg = Segment::create(&*self.fs, &self.dir, next_id)?;
        self.fs.sync_dir(&self.dir)?;
        w.active = seg;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimFs;
    use std::sync::Arc;

    fn dir() -> PathBuf {
        PathBuf::from("/wal")
    }

    fn open(fs: Arc<dyn Storage>, mode: Durability) -> (Wal, Recovered) {
        Wal::open(
            fs,
            &dir(),
            WalOptions {
                durability: mode,
                ..Default::default()
            },
        )
        .expect("open wal")
    }

    /// Round-trip through every durability mode: whatever is appended replays in
    /// order after reopening the log.
    fn round_trip_for(mode: Durability) {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let (wal, rec) = open(fs.clone(), mode);
        assert!(rec.records.is_empty());
        for r in [b"a".as_slice(), b"bb", b"ccc"] {
            wal.append(r).expect("append");
        }
        drop(wal);
        // In OsBuffered nothing is guaranteed durable without an explicit crash,
        // but here there was no crash so the buffered bytes are still readable.
        let (_wal2, rec2) = open(fs, mode);
        assert_eq!(
            rec2.records,
            vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()],
            "mode {mode:?}"
        );
    }

    #[test]
    fn round_trip_always() {
        round_trip_for(Durability::Always);
    }

    #[test]
    fn round_trip_group_commit() {
        round_trip_for(Durability::GroupCommit);
    }

    #[test]
    fn round_trip_os_buffered() {
        round_trip_for(Durability::OsBuffered);
    }

    #[test]
    fn segment_rolls_at_threshold() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let (wal, _) = Wal::open(
            fs.clone(),
            &dir(),
            WalOptions {
                durability: Durability::Always,
                segment_size: 64, // tiny so a few records roll it
            },
        )
        .expect("open");
        assert_eq!(wal.active_segment_id(), 1);
        for _ in 0..20 {
            wal.append(b"0123456789").expect("append");
        }
        // Multiple segments now exist and the active id has advanced.
        assert!(wal.active_segment_id() > 1);
        let segs = segment::list_segments(&*fs, &dir()).unwrap();
        assert!(
            segs.len() > 1,
            "expected multiple segments, got {}",
            segs.len()
        );

        // Records survive reopen across the segment boundary in order.
        drop(wal);
        let (_wal2, rec) = open(fs, Durability::Always);
        assert_eq!(rec.records.len(), 20);
        assert!(rec.records.iter().all(|r| r == b"0123456789"));
    }

    /// Group commit under real thread concurrency: many writers, every acked
    /// record must be present and correct after reopen. The batching is an
    /// optimization — correctness is that no acked write is lost or duplicated.
    #[test]
    fn group_commit_concurrent_writers_all_durable() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let (wal, _) = open(fs.clone(), Durability::GroupCommit);
        let wal = Arc::new(wal);

        let threads = 8;
        let per_thread = 50;
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let wal = wal.clone();
                std::thread::spawn(move || {
                    for i in 0..per_thread {
                        let rec = format!("t{t:02}-r{i:03}");
                        wal.append(rec.as_bytes()).expect("append");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("thread");
        }

        let wal = Arc::try_unwrap(wal).expect("sole owner");
        drop(wal);
        let (_wal2, rec) = open(fs, Durability::GroupCommit);
        // Every one of threads*per_thread acked records is present exactly once.
        assert_eq!(rec.records.len(), threads * per_thread);
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for r in &rec.records {
            assert!(seen.insert(r.clone()), "duplicate record {r:?}");
        }
        for t in 0..threads {
            for i in 0..per_thread {
                let rec = format!("t{t:02}-r{i:03}");
                assert!(seen.contains(rec.as_bytes()), "missing {rec}");
            }
        }
    }
}

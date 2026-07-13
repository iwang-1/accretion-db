//! WAL recovery: replay durable records and locate the clean truncation point.
//!
//! Recovery reads each segment in id order and decodes frames back-to-back.
//! The **torn-tail rule** (see `DESIGN_NOTES.md`) governs where replay stops:
//! the scan halts at the first frame that is short (truncated) or fails its
//! CRC, discarding that frame and everything after it.
//!
//! Why this cannot lose acknowledged data: in a durable mode (`Always` /
//! `GroupCommit`) a write is only acked *after* its frame has been `sync_file`d,
//! so every acked frame precedes the torn tail and decodes cleanly. Only
//! unsynced (therefore never-acked) tail bytes are ever discarded.
//!
//! A crash can only tear the *most recent* unsynced append, which lives in the
//! last segment. Earlier segments are fully durable, so a decode failure there
//! would indicate real corruption; recovery still stops at the first bad frame
//! (conservative and safe) and records where.

use std::path::{Path, PathBuf};

use crate::storage::{Storage, StorageResult};
use crate::wal::frame::{self, FrameError};
use crate::wal::segment::{self, read_all};

/// How recovery stopped scanning a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StopReason {
    /// Every byte of the segment decoded into a valid frame with no leftover.
    #[default]
    Clean,
    /// A frame's header or payload ran past end-of-file: a torn write.
    Truncated,
    /// A frame was fully present but its CRC did not verify: corruption.
    BadCrc,
}

/// The outcome of replaying the whole log.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Recovered {
    /// Every valid record payload, in log order across all segments.
    pub records: Vec<Vec<u8>>,
    /// The id of the segment where scanning stopped (the tail segment), if any
    /// segments existed.
    pub tail_segment: Option<u64>,
    /// The byte offset within [`tail_segment`](Recovered::tail_segment) of the
    /// first byte that is *not* part of a valid record — i.e. the length the
    /// tail segment should be truncated to so it holds only clean frames.
    pub tail_valid_len: u64,
    /// Why the scan of the tail segment stopped.
    pub stop_reason: StopReason,
}

/// Scan a single already-read segment buffer, appending decoded payloads to
/// `out`. Returns `(valid_len, stop_reason)` where `valid_len` is the offset of
/// the first byte past the last cleanly-decoded frame.
fn scan_buffer(buf: &[u8], out: &mut Vec<Vec<u8>>) -> (u64, StopReason) {
    let mut off = 0usize;
    loop {
        if off == buf.len() {
            return (off as u64, StopReason::Clean);
        }
        match frame::decode(buf, off) {
            Ok(f) => {
                out.push(f.payload);
                off += f.total_len;
            }
            Err(FrameError::Truncated) => return (off as u64, StopReason::Truncated),
            Err(FrameError::BadCrc) => return (off as u64, StopReason::BadCrc),
        }
    }
}

/// Replay every WAL segment under `dir` in id order, applying the torn-tail
/// rule. Records from all fully-clean leading segments plus the clean prefix of
/// the final segment are returned in order.
pub fn recover(fs: &dyn Storage, dir: &Path) -> StorageResult<Recovered> {
    let segments = segment::list_segments(fs, dir)?;
    let mut result = Recovered::default();
    if segments.is_empty() {
        result.stop_reason = StopReason::Clean;
        return Ok(result);
    }

    for (idx, (id, path)) in segments.iter().enumerate() {
        let buf = read_all(fs, path)?;
        let before = result.records.len();
        let (valid_len, reason) = scan_buffer(&buf, &mut result.records);
        result.tail_segment = Some(*id);
        result.tail_valid_len = valid_len;
        result.stop_reason = reason;

        let is_last = idx + 1 == segments.len();
        // If a non-final segment stops early, the tail it produced is where the
        // log truly ends — later segments are unreachable, so stop here. (In
        // normal operation only the final segment is ever partial.)
        if reason != StopReason::Clean {
            let _ = before; // records already pushed are the valid prefix
            break;
        }
        // A clean, fully-consumed non-final segment: continue to the next.
        if !is_last {
            continue;
        }
    }
    Ok(result)
}

/// Truncate the tail segment to its valid length, physically discarding a torn
/// or corrupt tail so future appends land at a clean boundary.
///
/// The [`Storage`] seam grows files by `append` only, so "truncation" is
/// modeled by rewriting the segment to its valid prefix via a
/// tmp-write + rename, then syncing the directory. This keeps the operation
/// crash-safe: a crash mid-truncate leaves either the old (torn-tail) segment
/// or the clean rewritten one, and re-running recovery converges either way.
pub fn truncate_tail(
    fs: &dyn Storage,
    dir: &Path,
    tail_segment: u64,
    valid_len: u64,
) -> StorageResult<()> {
    let path = segment::segment_path(dir, tail_segment);
    let current = fs.len(&path)?;
    if valid_len >= current {
        return Ok(()); // nothing torn; already clean
    }
    let buf = read_all(fs, &path)?;
    let keep = &buf[..valid_len as usize];

    let tmp = tmp_path(dir, tail_segment);
    // Replace any stale tmp from a prior interrupted truncate.
    let _ = fs.delete(&tmp);
    fs.create(&tmp)?;
    fs.append(&tmp, keep)?;
    fs.sync_file(&tmp)?;
    fs.rename(&tmp, &path)?;
    fs.sync_dir(dir)?;
    Ok(())
}

/// Path of the scratch file used while rewriting a segment to its valid prefix.
fn tmp_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.wal.tmp"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimFs;
    use crate::wal::frame::encode;
    use crate::wal::segment::Segment;
    use std::sync::Arc;

    fn dir() -> PathBuf {
        PathBuf::from("/wal")
    }

    fn write_segment(fs: &dyn Storage, d: &Path, id: u64, frames: &[&[u8]]) -> Segment {
        let mut seg = Segment::create(fs, d, id).unwrap();
        for f in frames {
            seg.append(fs, &encode(f)).unwrap();
        }
        seg.sync(fs).unwrap();
        seg
    }

    #[test]
    fn empty_dir_recovers_nothing() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let r = recover(&*fs, &dir()).unwrap();
        assert!(r.records.is_empty());
        assert_eq!(r.stop_reason, StopReason::Clean);
        assert_eq!(r.tail_segment, None);
    }

    #[test]
    fn single_clean_segment() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        write_segment(&*fs, &dir(), 1, &[b"a", b"bb", b"ccc"]);
        let r = recover(&*fs, &dir()).unwrap();
        assert_eq!(
            r.records,
            vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]
        );
        assert_eq!(r.stop_reason, StopReason::Clean);
        assert_eq!(r.tail_segment, Some(1));
    }

    #[test]
    fn records_span_multiple_segments_in_order() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        write_segment(&*fs, &dir(), 1, &[b"one", b"two"]);
        write_segment(&*fs, &dir(), 2, &[b"three"]);
        let r = recover(&*fs, &dir()).unwrap();
        assert_eq!(
            r.records,
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]
        );
        assert_eq!(r.tail_segment, Some(2));
    }

    #[test]
    fn torn_tail_frame_is_dropped() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let mut seg = Segment::create(&*fs, &dir(), 1).unwrap();
        seg.append(&*fs, &encode(b"good")).unwrap();
        // A partial frame: write only part of an encoded frame's bytes.
        let partial = encode(b"torn-away");
        seg.append(&*fs, &partial[..5]).unwrap();
        seg.sync(&*fs).unwrap();

        let good_len = encode(b"good").len() as u64;
        let r = recover(&*fs, &dir()).unwrap();
        assert_eq!(r.records, vec![b"good".to_vec()]);
        assert_eq!(r.stop_reason, StopReason::Truncated);
        assert_eq!(r.tail_valid_len, good_len);
    }

    #[test]
    fn bad_crc_frame_stops_replay() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let mut seg = Segment::create(&*fs, &dir(), 1).unwrap();
        seg.append(&*fs, &encode(b"keep")).unwrap();
        let mut corrupt = encode(b"corrupt");
        let hdr = frame::HEADER_SZ;
        corrupt[hdr] ^= 0xFF; // flip a payload bit
        seg.append(&*fs, &corrupt).unwrap();
        seg.sync(&*fs).unwrap();

        let r = recover(&*fs, &dir()).unwrap();
        assert_eq!(r.records, vec![b"keep".to_vec()]);
        assert_eq!(r.stop_reason, StopReason::BadCrc);
    }

    #[test]
    fn truncate_tail_rewrites_to_valid_prefix() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let mut seg = Segment::create(&*fs, &dir(), 1).unwrap();
        seg.append(&*fs, &encode(b"good")).unwrap();
        let partial = encode(b"torn");
        seg.append(&*fs, &partial[..3]).unwrap();
        seg.sync(&*fs).unwrap();
        fs.sync_dir(&dir()).unwrap();

        let r = recover(&*fs, &dir()).unwrap();
        assert_eq!(r.stop_reason, StopReason::Truncated);
        truncate_tail(&*fs, &dir(), r.tail_segment.unwrap(), r.tail_valid_len).unwrap();

        // After truncation the segment holds exactly the valid prefix and a
        // re-recovery is fully clean.
        let r2 = recover(&*fs, &dir()).unwrap();
        assert_eq!(r2.records, vec![b"good".to_vec()]);
        assert_eq!(r2.stop_reason, StopReason::Clean);
    }
}

//! WAL segment files: naming, creation, and the thin per-file append/sync
//! operations the commit pipeline drives.
//!
//! The log is split into monotonically numbered segment files so that once a
//! memtable flush has captured every record up to some point, the whole
//! prefix of segments covering those records can be deleted in one step
//! ("segment release") rather than rewriting a single ever-growing file.
//!
//! A segment is named `<id>.wal` with a zero-padded, lexicographically-sortable
//! id, so a plain sorted `list` of the directory yields segments in write
//! order.

use std::path::{Path, PathBuf};

use crate::storage::{Storage, StorageError, StorageResult};

/// Width of the zero-padded segment id in a file name. 20 digits holds any
/// `u64`, so ids never overflow the fixed width and sort lexicographically.
const ID_WIDTH: usize = 20;
/// File-name suffix identifying a WAL segment.
const SUFFIX: &str = ".wal";

/// Format the on-disk file name for segment `id` within `dir`.
pub(crate) fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:0ID_WIDTH$}{SUFFIX}"))
}

/// Parse a segment id out of a path produced by [`segment_path`], returning
/// `None` for any path that is not a well-formed segment file name.
pub(crate) fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(SUFFIX)?;
    if digits.len() != ID_WIDTH {
        return None;
    }
    digits.parse::<u64>().ok()
}

/// List every WAL segment in `dir`, returning `(id, path)` pairs sorted by id.
///
/// Non-segment files in the directory (e.g. a manifest, or a `.tmp` rewrite in
/// progress) are ignored.
pub(crate) fn list_segments(fs: &dyn Storage, dir: &Path) -> StorageResult<Vec<(u64, PathBuf)>> {
    let mut out: Vec<(u64, PathBuf)> = fs
        .list(dir)?
        .into_iter()
        .filter_map(|p| parse_segment_id(&p).map(|id| (id, p)))
        .collect();
    out.sort_unstable_by_key(|(id, _)| *id);
    Ok(out)
}

/// A handle to one open, writable segment file.
///
/// Tracks the id, path, and the current write offset (the file's length as far
/// as this process is concerned) so appends know where the next frame lands.
#[derive(Debug)]
pub(crate) struct Segment {
    id: u64,
    path: PathBuf,
    /// Byte length written so far — the offset the next append begins at.
    write_offset: u64,
}

impl Segment {
    /// Create a brand-new, empty segment file `<id>.wal` in `dir`.
    ///
    /// The file's directory entry is not durable until the caller `sync_dir`s
    /// the parent (the pipeline does this on open and on rotation).
    pub(crate) fn create(fs: &dyn Storage, dir: &Path, id: u64) -> StorageResult<Segment> {
        let path = segment_path(dir, id);
        fs.create(&path)?;
        Ok(Segment {
            id,
            path,
            write_offset: 0,
        })
    }

    /// Adopt an existing segment file, taking its current length as the write
    /// offset. Used when reopening a log whose tail segment is still writable.
    pub(crate) fn open(fs: &dyn Storage, dir: &Path, id: u64) -> StorageResult<Segment> {
        let path = segment_path(dir, id);
        let write_offset = fs.len(&path)?;
        Ok(Segment {
            id,
            path,
            write_offset,
        })
    }

    /// This segment's id.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// This segment's on-disk path.
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Current byte length written to this segment.
    pub(crate) fn len(&self) -> u64 {
        self.write_offset
    }

    /// Append an already-encoded frame's bytes, advancing the write offset.
    ///
    /// The bytes are buffered (not durable) until [`sync`](Segment::sync).
    pub(crate) fn append(&mut self, fs: &dyn Storage, bytes: &[u8]) -> StorageResult<()> {
        let at = fs.append(&self.path, bytes)?;
        debug_assert_eq!(at, self.write_offset, "append landed at unexpected offset");
        self.write_offset += bytes.len() as u64;
        Ok(())
    }

    /// Make every buffered byte of this segment durable (`fsync`).
    pub(crate) fn sync(&self, fs: &dyn Storage) -> StorageResult<()> {
        fs.sync_file(&self.path)
    }
}

/// Read an entire segment file into memory for recovery scanning.
pub(crate) fn read_all(fs: &dyn Storage, path: &Path) -> StorageResult<Vec<u8>> {
    let len = match fs.len(path) {
        Ok(l) => l as usize,
        Err(StorageError::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut buf = vec![0u8; len];
    let mut read = 0;
    while read < len {
        let n = fs.read_at(path, read as u64, &mut buf[read..])?;
        if n == 0 {
            break;
        }
        read += n;
    }
    buf.truncate(read);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SimFs;
    use std::sync::Arc;

    fn dir() -> PathBuf {
        PathBuf::from("/wal")
    }

    #[test]
    fn path_names_are_sortable_and_reversible() {
        let a = segment_path(&dir(), 1);
        let b = segment_path(&dir(), 2);
        let big = segment_path(&dir(), 12345);
        // Lexicographic order matches numeric order.
        assert!(a < b);
        assert!(b < big);
        assert_eq!(parse_segment_id(&a), Some(1));
        assert_eq!(parse_segment_id(&big), Some(12345));
    }

    #[test]
    fn parse_rejects_non_segments() {
        assert_eq!(parse_segment_id(Path::new("/wal/MANIFEST")), None);
        assert_eq!(parse_segment_id(Path::new("/wal/0001.wal")), None); // wrong width
        assert_eq!(
            parse_segment_id(Path::new("/wal/00000000000000000001.tmp")),
            None
        );
    }

    #[test]
    fn list_sorted_by_id() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let d = dir();
        // Create out of order.
        Segment::create(&*fs, &d, 3).unwrap();
        Segment::create(&*fs, &d, 1).unwrap();
        Segment::create(&*fs, &d, 2).unwrap();
        let ids: Vec<u64> = list_segments(&*fs, &d)
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn append_tracks_offset_and_reads_back() {
        let fs: Arc<dyn Storage> = Arc::new(SimFs::with_seed(1));
        let d = dir();
        let mut seg = Segment::create(&*fs, &d, 1).unwrap();
        seg.append(&*fs, b"hello").unwrap();
        seg.append(&*fs, b"world").unwrap();
        assert_eq!(seg.len(), 10);
        let bytes = read_all(&*fs, seg.path()).unwrap();
        assert_eq!(bytes, b"helloworld");
    }
}

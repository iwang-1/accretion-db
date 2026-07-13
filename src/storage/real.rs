//! [`RealFs`] — the production [`Storage`] backend over `std::fs`.
//!
//! Every durability barrier maps onto a real kernel primitive:
//!
//! * `sync_file` → [`File::sync_data`] (`fdatasync`): flush the file's data
//!   (and the metadata strictly required to read it back) to stable storage.
//! * `sync_dir` → open the directory as a file and [`File::sync_all`] it. On
//!   Unix this fsyncs the directory inode, which is what makes a preceding
//!   `rename`/`create`/`delete` durable. Without it the directory entry can be
//!   lost across a power cut even though the file's data survived.
//!
//! There is deliberately no per-file caching here: each call opens, seeks, acts
//! and closes. The engine batches at a higher level (the WAL commit pipeline),
//! and keeping this layer stateless keeps its behaviour trivially comparable to
//! `SimFs`.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::{Storage, StorageError, StorageResult};

/// A [`Storage`] backend backed by the real filesystem via `std::fs`.
#[derive(Debug, Default, Clone)]
pub struct RealFs;

impl RealFs {
    /// Construct a `RealFs`. It holds no state; all paths are absolute or
    /// relative to the process working directory, exactly as `std::fs` treats
    /// them.
    pub fn new() -> Self {
        RealFs
    }
}

/// Translate a [`std::io::Error`] into a [`StorageError`] for the given path,
/// mapping the common `NotFound` / `AlreadyExists` kinds onto our variants.
fn map_io(path: &Path, err: std::io::Error) -> StorageError {
    match err.kind() {
        std::io::ErrorKind::NotFound => StorageError::NotFound(path.to_path_buf()),
        std::io::ErrorKind::AlreadyExists => StorageError::AlreadyExists(path.to_path_buf()),
        _ => StorageError::Io {
            path: path.to_path_buf(),
            message: err.to_string(),
        },
    }
}

impl Storage for RealFs {
    fn create(&self, path: &Path) -> StorageResult<()> {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map(|_| ())
            .map_err(|e| map_io(path, e))
    }

    fn open(&self, path: &Path) -> StorageResult<()> {
        if path.is_file() {
            Ok(())
        } else if path.exists() {
            Err(StorageError::NotADirectory(path.to_path_buf()))
        } else {
            Err(StorageError::NotFound(path.to_path_buf()))
        }
    }

    fn append(&self, path: &Path, data: &[u8]) -> StorageResult<u64> {
        let mut f = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| map_io(path, e))?;
        // In append mode every write goes to the current end; the offset the
        // data lands at is the length just before we write.
        let offset = f.metadata().map_err(|e| map_io(path, e))?.len();
        f.write_all(data).map_err(|e| map_io(path, e))?;
        Ok(offset)
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> StorageResult<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| map_io(path, e))?;
        let len = f.metadata().map_err(|e| map_io(path, e))?.len();
        if offset > len || offset + data.len() as u64 > len {
            return Err(StorageError::OutOfBounds {
                path: path.to_path_buf(),
                offset,
                len,
            });
        }
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| map_io(path, e))?;
        f.write_all(data).map_err(|e| map_io(path, e))?;
        Ok(())
    }

    fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        let mut f = File::open(path).map_err(|e| map_io(path, e))?;
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| map_io(path, e))?;
        // `read` may return short; loop until buf is full or EOF.
        let mut filled = 0;
        while filled < buf.len() {
            match f.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(map_io(path, e)),
            }
        }
        Ok(filled)
    }

    fn sync_file(&self, path: &Path) -> StorageResult<()> {
        let f = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| map_io(path, e))?;
        f.sync_data().map_err(|e| map_io(path, e))
    }

    fn sync_dir(&self, dir: &Path) -> StorageResult<()> {
        // On Unix, opening a directory read-only and fsyncing its handle makes
        // pending directory-entry changes (rename/create/delete) durable. This
        // is the step that turns a "visible" rename into a "survives-power-loss"
        // rename.
        let f = File::open(dir).map_err(|e| map_io(dir, e))?;
        f.sync_all().map_err(|e| map_io(dir, e))
    }

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        std::fs::rename(from, to).map_err(|e| map_io(from, e))
    }

    fn delete(&self, path: &Path) -> StorageResult<()> {
        std::fs::remove_file(path).map_err(|e| map_io(path, e))
    }

    fn list(&self, dir: &Path) -> StorageResult<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).map_err(|e| map_io(dir, e))? {
            let entry = entry.map_err(|e| map_io(dir, e))?;
            if entry.path().is_file() {
                out.push(entry.path());
            }
        }
        out.sort();
        Ok(out)
    }

    fn len(&self, path: &Path) -> StorageResult<u64> {
        std::fs::metadata(path)
            .map(|m| m.len())
            .map_err(|e| map_io(path, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end round-trip: create → append → read_at, then exercise the
    /// positional-write, rename, list, len, and durability-barrier paths so the
    /// production backend is genuinely driven (not just referenced) even before
    /// the engine exists. All under a `tempfile` dir so nothing leaks.
    #[test]
    fn realfs_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let fs = RealFs::new();
        let a = dir.path().join("a.log");

        fs.create(&a).expect("create");
        assert!(matches!(fs.create(&a), Err(StorageError::AlreadyExists(_))));
        fs.open(&a).expect("open existing");

        let off0 = fs.append(&a, b"hello ").expect("append 1");
        let off1 = fs.append(&a, b"world").expect("append 2");
        assert_eq!(off0, 0);
        assert_eq!(off1, 6);
        assert_eq!(fs.len(&a).expect("len"), 11);

        let mut buf = [0u8; 11];
        let n = fs.read_at(&a, 0, &mut buf).expect("read_at");
        assert_eq!(n, 11);
        assert_eq!(&buf, b"hello world");

        // Positional overwrite within bounds; past-end is rejected.
        fs.write_at(&a, 0, b"HELLO").expect("write_at");
        let mut head = [0u8; 5];
        fs.read_at(&a, 0, &mut head).expect("read head");
        assert_eq!(&head, b"HELLO");
        assert!(matches!(
            fs.write_at(&a, 8, b"xxxx"),
            Err(StorageError::OutOfBounds { .. })
        ));

        // Durability barriers must succeed on a real fs.
        fs.sync_file(&a).expect("sync_file");
        fs.sync_dir(dir.path()).expect("sync_dir");

        // Atomic rename, then confirm the namespace reflects it.
        let b = dir.path().join("b.log");
        fs.rename(&a, &b).expect("rename");
        fs.sync_dir(dir.path()).expect("sync_dir after rename");
        assert!(matches!(fs.open(&a), Err(StorageError::NotFound(_))));
        fs.open(&b).expect("open renamed");
        assert_eq!(fs.list(dir.path()).expect("list"), vec![b.clone()]);

        // Delete and confirm removal.
        fs.delete(&b).expect("delete");
        assert!(fs.list(dir.path()).expect("list empty").is_empty());
        assert!(matches!(fs.len(&b), Err(StorageError::NotFound(_))));
    }
}

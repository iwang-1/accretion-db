//! The storage seam: a single narrow trait ([`Storage`]) that the entire LSM
//! engine is written against, plus two implementations — [`RealFs`] (the real
//! filesystem) and [`SimFs`] (a deterministic power-loss simulator).
//!
//! # Why a path-addressed trait
//!
//! The trait is intentionally *object-safe* (`Arc<dyn Storage>`), so the engine
//! holds one handle and the whole test suite can swap `RealFs` for `SimFs`
//! without touching a line of engine code. To stay object-safe it exposes no
//! per-file handle type: every operation names its target by path. A storage
//! backend behaves like a flat namespace of append-mostly files that also
//! supports positional reads/writes, atomic rename, and explicit durability
//! barriers (`sync_file`, `sync_dir`).
//!
//! # The durability contract
//!
//! A mutating call (`append`, `write_at`, `create`, `delete`, `rename`) only
//! promises that its effect is *visible to subsequent operations in this
//! process*. It makes **no** promise that the effect survives a power loss.
//! Durability is earned separately:
//!
//! * `sync_file(p)` promotes every buffered byte range of `p` to durable.
//! * `sync_dir(d)` promotes directory-entry changes (notably `rename` and
//!   `create`) under `d` to durable.
//!
//! `SimFs::crash` discards everything that has not been made durable by those
//! barriers — and may additionally *tear* the most recent unsynced append.
//! `RealFs` maps the same calls onto `fsync`/`fdatasync` and a directory-handle
//! fsync. `SimFs` deliberately chooses deterministic outcomes within its stated
//! fault model; passing it is strong evidence for those outcomes, not a claim
//! that every filesystem or hardware failure behavior is exhaustively modeled.

use std::fmt;
use std::path::{Path, PathBuf};

mod real;
mod sim;

pub use real::RealFs;
pub use sim::{CrashReport, SimConfig, SimFs, TearMode};

/// Result alias for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors returned by a [`Storage`] backend.
///
/// The variants are deliberately backend-agnostic so that `SimFs` and `RealFs`
/// surface the same failure modes to the engine. The underlying OS error, when
/// there is one, is preserved as a string (rather than a live
/// [`std::io::Error`], which is neither `Clone` nor easy to fabricate in the
/// simulator).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// The named path does not exist.
    NotFound(PathBuf),
    /// A `create` targeted a path that already exists.
    AlreadyExists(PathBuf),
    /// A positional read/write addressed a range past the end of the file.
    OutOfBounds {
        /// The file that was addressed.
        path: PathBuf,
        /// The offset the caller asked for.
        offset: u64,
        /// The current length of the file.
        len: u64,
    },
    /// The path named a directory where a file was expected (or vice-versa).
    NotADirectory(PathBuf),
    /// An underlying I/O error from the real filesystem.
    Io {
        /// The path being operated on when the error occurred.
        path: PathBuf,
        /// A human-readable description of the OS error.
        message: String,
    },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::NotFound(p) => write!(f, "not found: {}", p.display()),
            StorageError::AlreadyExists(p) => write!(f, "already exists: {}", p.display()),
            StorageError::OutOfBounds { path, offset, len } => write!(
                f,
                "out of bounds on {}: offset {offset} exceeds len {len}",
                path.display()
            ),
            StorageError::NotADirectory(p) => write!(f, "not a directory: {}", p.display()),
            StorageError::Io { path, message } => {
                write!(f, "io error on {}: {message}", path.display())
            }
        }
    }
}

impl std::error::Error for StorageError {}

/// A flat, path-addressed, append-mostly file namespace with explicit
/// durability barriers.
///
/// All methods take `&self`; a backend is shared as `Arc<dyn Storage>` and must
/// be `Send + Sync`. Implementations serialize their own internal state.
///
/// See the [module docs](self) for the durability contract that every method
/// participates in.
pub trait Storage: Send + Sync + fmt::Debug {
    /// Create a new, empty file at `path`.
    ///
    /// Returns [`StorageError::AlreadyExists`] if the path is already present.
    /// The new file's *directory entry* is not durable until a `sync_dir` on
    /// the parent directory.
    fn create(&self, path: &Path) -> StorageResult<()>;

    /// Assert that a file exists at `path`, returning
    /// [`StorageError::NotFound`] otherwise.
    ///
    /// Because the trait is handle-free, `open` carries no state; it is the
    /// existence check the engine performs before reading a file it expects to
    /// be present (e.g. an SSTable named in the manifest).
    fn open(&self, path: &Path) -> StorageResult<()>;

    /// Append `data` to the end of `path` and return the byte offset at which
    /// the appended data begins (i.e. the file length prior to the append).
    ///
    /// The appended bytes are *buffered*: they are visible to subsequent reads
    /// in this process but are not durable until a covering `sync_file`. Under
    /// `SimFs` an unsynced append is exactly what a crash may tear or drop.
    fn append(&self, path: &Path, data: &[u8]) -> StorageResult<u64>;

    /// Overwrite `data.len()` bytes at `offset` within `path`.
    ///
    /// The range `[offset, offset + data.len())` must lie within the current
    /// file length; writing past the end returns [`StorageError::OutOfBounds`]
    /// (use `append` to grow a file). Written bytes are buffered until synced.
    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> StorageResult<()>;

    /// Read into `buf` starting at `offset`, returning the number of bytes read
    /// (which may be short at end-of-file).
    ///
    /// Reads observe buffered-but-unsynced bytes: within a live process a
    /// backend behaves like an ordinary filesystem. Durability only matters
    /// across a crash.
    fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> StorageResult<usize>;

    /// Flush and make durable every buffered byte range of `path`.
    ///
    /// After this returns, the current live inode's bytes are durable. A newly
    /// created or renamed path still requires `sync_dir` before its namespace
    /// binding is durable; without that binding the synced inode may be
    /// unreachable after a crash. On `RealFs` this is `fdatasync`.
    fn sync_file(&self, path: &Path) -> StorageResult<()>;

    /// Make durable the directory-entry changes (creates, deletes, renames)
    /// that have occurred under directory `dir`.
    ///
    /// Without this barrier a `rename` is *volatile*: a crash may leave either
    /// the old or the new name in place. On `RealFs` this is an fsync of the
    /// directory handle.
    fn sync_dir(&self, dir: &Path) -> StorageResult<()>;

    /// Atomically rename `from` to `to`, replacing `to` if it exists.
    ///
    /// The rename is visible immediately but *volatile* until the affected
    /// parent directory is synced (see [`sync_dir`](Storage::sync_dir)). For a
    /// cross-directory rename, callers must sync both source and destination
    /// parents; current engine call sites rename within one directory.
    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()>;

    /// Remove the file at `path`. Volatile until a `sync_dir` on the parent.
    fn delete(&self, path: &Path) -> StorageResult<()>;

    /// List the immediate children (files) of directory `dir`.
    ///
    /// Entries are returned as full paths (prefixed by `dir`) in sorted order
    /// for determinism. Includes buffered-but-unsynced creations.
    fn list(&self, dir: &Path) -> StorageResult<Vec<PathBuf>>;

    /// Return the current length of `path` in bytes (including buffered
    /// appends).
    fn len(&self, path: &Path) -> StorageResult<u64>;
}

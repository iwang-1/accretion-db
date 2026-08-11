//! `accretion-db`

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod compaction;
pub mod db;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod storage;
pub mod testkit;
pub mod wal;

pub use db::{Db, DbError, Options, Scan};
pub use storage::{RealFs, SimFs, Storage, StorageError, StorageResult};
pub use wal::Durability;

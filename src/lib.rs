//! `accretion-db` — an embeddable LSM-tree storage engine whose headline
//! product is *proof* of crash consistency, not raw speed.
//!
//! The engine is built on top of a single narrow abstraction, the [`Storage`]
//! trait (see [`storage`]), which stands between the LSM machinery and the
//! filesystem. Two implementations exist:
//!
//! * [`RealFs`](storage::RealFs) — a thin wrapper over `std::fs` that honours
//!   real durability primitives (`fsync`/`fdatasync`, directory-handle fsync
//!   for rename durability).
//! * [`SimFs`](storage::SimFs) — a deterministic, seeded page-cache simulator
//!   that models power loss: every mutating byte range is *buffered* until a
//!   covering `sync_file` promotes it to *durable*, renames are volatile until
//!   the parent directory is synced, and [`crash`](storage::SimFs::crash)
//!   discards buffered state and may *tear* the last unsynced append at a
//!   random byte boundary.
//!
//! **Harness-first:** the fault-injection seam is the foundation the whole
//! engine grows up under. Every component is exercised against `SimFs` from
//! the day it is written; nothing merges that has not been crash-tested.
//!
//! This crate contains no `unsafe` code — it is forbidden at the crate root.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod compaction;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod storage;
pub mod testkit;
pub mod wal;

pub use storage::{RealFs, SimFs, Storage, StorageError, StorageResult};

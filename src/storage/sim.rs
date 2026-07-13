//! [`SimFs`] — the deterministic power-loss simulator (STUB).
//!
//! This module is intentionally a stub in stage S0a. The [`Storage`] trait and
//! [`RealFs`](super::RealFs) land first; the full page-cache fault model —
//! buffered-vs-durable byte ranges, torn/dropped unsynced appends, volatile
//! renames, and a deterministic fault-point registry — is implemented in the
//! next stage on top of the frozen trait.
//!
//! The public types below are declared now (and re-exported from
//! [`super`](super)) so the crate's surface is stable, but they carry no
//! behaviour yet.
//
// TODO(S0b): implement the SimFs page-cache model:
//   * every mutating byte range tracked as BUFFERED until a covering
//     `sync_file` promotes it to DURABLE;
//   * `rename` volatile until `sync_dir` on the parent;
//   * `crash()` discards all buffered state and, per seeded `StdRng`, TEARS the
//     last unsynced append at a random boundary and/or bit-flips an unsynced
//     region;
//   * a fault-point registry that counts every mutating op and can be armed to
//     crash after op #i (deterministic) or at RNG-chosen ops.

/// How a crash mangles the most recent unsynced append (STUB).
///
/// Filled in the next stage; see the module-level TODO.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TearMode {
    /// The unsynced tail is dropped in full.
    #[default]
    Drop,
    /// The unsynced tail is truncated at a seeded random byte boundary.
    Truncate,
    /// A bit inside the unsynced region is flipped.
    BitFlip,
}

/// Configuration for a [`SimFs`] instance (STUB).
///
/// Filled in the next stage; see the module-level TODO.
#[derive(Debug, Clone, Default)]
pub struct SimConfig {
    /// Seed for the deterministic RNG that drives crash decisions.
    pub seed: u64,
}

/// A summary of what a simulated crash did (STUB).
///
/// Filled in the next stage; see the module-level TODO.
#[derive(Debug, Clone, Default)]
pub struct CrashReport {
    /// Number of mutating storage ops observed before the crash.
    pub ops_before_crash: u64,
}

/// A deterministic, seeded power-loss simulator implementing [`Storage`] (STUB).
///
/// Does not yet implement the [`Storage`](super::Storage) trait — that arrives
/// in the next stage. See the module-level TODO for the full fault model.
#[derive(Debug, Default)]
pub struct SimFs {
    _private: (),
}

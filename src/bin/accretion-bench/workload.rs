//! Deterministic key/value generation for the benchmark workloads.
//!
//! Keys are a fixed 16 bytes and values a fixed 100 bytes (the spec's B1 sizes),
//! both a pure function of a `u64` index so any phase can reconstruct the exact
//! bytes for a given index without shared state — the fill phase and the
//! read/verify phase agree by construction. Randomised access orders are driven
//! by a *seeded* `StdRng` so a run is reproducible from its seed.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

/// Fixed key width in bytes (spec B1: 16-byte keys).
pub const KEY_LEN: usize = 16;
/// Fixed value width in bytes (spec B1: 100-byte values).
pub const VALUE_LEN: usize = 100;

/// The 16-byte key for index `i`: the big-endian index in the low 8 bytes so
/// keys sort in index order, zero-padded to [`KEY_LEN`].
pub fn key_for(i: u64) -> Vec<u8> {
    let mut k = vec![0u8; KEY_LEN];
    k[KEY_LEN - 8..].copy_from_slice(&i.to_be_bytes());
    k
}

/// The 100-byte value for index `i`: index-derived, filled deterministically so
/// a reader can recompute and byte-compare the expected value for any index.
pub fn value_for(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; VALUE_LEN];
    // Stamp the index in the first 8 bytes, then fill the rest with an
    // index-derived byte pattern so distinct indices have distinct values.
    v[..8].copy_from_slice(&i.to_le_bytes());
    let seed = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for (j, b) in v[8..].iter_mut().enumerate() {
        *b = (seed.wrapping_add(j as u64) & 0xFF) as u8;
    }
    v
}

/// The order in which the fill phase writes `n` keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillOrder {
    /// Ascending index order (fill-seq): best case for sorted-run building.
    Sequential,
    /// A seeded random permutation of `0..n` (fill-random): stresses the
    /// memtable and compaction merge with out-of-order keys.
    Random,
}

/// Produce the sequence of indices the fill phase writes, in write order.
pub fn fill_indices(order: FillOrder, n: u64, seed: u64) -> Vec<u64> {
    let mut idx: Vec<u64> = (0..n).collect();
    if order == FillOrder::Random {
        let mut rng = StdRng::seed_from_u64(seed);
        idx.shuffle(&mut rng);
    }
    idx
}

/// Produce `count` uniformly-random key indices in `0..n` for the point-read
/// phase (a fresh seeded RNG so the read pattern is reproducible).
pub fn read_indices(n: u64, count: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count).map(|_| rng.gen_range(0..n)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_value_widths_are_fixed() {
        assert_eq!(key_for(0).len(), KEY_LEN);
        assert_eq!(key_for(u64::MAX).len(), KEY_LEN);
        assert_eq!(value_for(0).len(), VALUE_LEN);
        assert_eq!(value_for(12345).len(), VALUE_LEN);
    }

    #[test]
    fn keys_sort_in_index_order() {
        assert!(key_for(1) < key_for(2));
        assert!(key_for(100) < key_for(1000));
    }

    #[test]
    fn values_are_distinct_and_reproducible() {
        assert_eq!(value_for(42), value_for(42));
        assert_ne!(value_for(1), value_for(2));
    }

    #[test]
    fn fill_random_is_a_permutation_and_seed_stable() {
        let a = fill_indices(FillOrder::Random, 1000, 7);
        let b = fill_indices(FillOrder::Random, 1000, 7);
        assert_eq!(a, b, "same seed -> same order");
        let mut sorted = a.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..1000).collect::<Vec<_>>(), "is a permutation");
    }

    #[test]
    fn fill_seq_is_ascending() {
        let s = fill_indices(FillOrder::Sequential, 100, 0);
        assert_eq!(s, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn read_indices_are_in_range_and_seed_stable() {
        let a = read_indices(50, 200, 3);
        let b = read_indices(50, 200, 3);
        assert_eq!(a, b);
        assert!(a.iter().all(|&i| i < 50));
        assert_eq!(a.len(), 200);
    }
}

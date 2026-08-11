//! A per-table Bloom filter with double hashing. A Bloom filter is a compact probabilistic set.

use xxhash_rust::xxh3::xxh3_128;

use super::{put_u32, put_u64, Cursor, Result, SsTableError};

/// A Bloom filter backed by a flat bit array, addressed by double hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    /// The bit array, one bit per slot, `bits.len() * 8` slots total.
    bits: Vec<u8>,
    /// Number of bits (`m`); a whole multiple of 8 (byte-aligned), so probes
    /// reduce modulo `m` rather than masking.
    num_bits: u32,
    /// Number of hash probes per key (`k`).
    num_hashes: u32,
}

impl BloomFilter {
    /// Build a filter sized for `expected_keys` at `bits_per_key` bits/key, choosing `k = round((m/n) ln
    /// 2)` clamped to `[1, 30]`.
    pub fn new(expected_keys: usize, bits_per_key: usize) -> Self {
        let n = expected_keys.max(1);
        let target_bits = n.saturating_mul(bits_per_key).max(64);
        // Round up to a whole number of bytes.
        let num_bytes = target_bits.div_ceil(8);
        let num_bits = (num_bytes * 8) as u32;
        // k = (m/n) ln 2, from the FPR-minimising derivation in the module docs.
        let k = ((num_bits as f64 / n as f64) * std::f64::consts::LN_2).round() as i64;
        let num_hashes = k.clamp(1, 30) as u32;
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    /// Split a key's 128-bit hash into the `(h1, h2)` pair for double hashing. `h2` is reduced to a
    /// non-zero step modulo `num_bits` so the probe sequence never degenerates to a single repeated bit.
    fn hashes(&self, key: &[u8]) -> (u64, u64) {
        let h = xxh3_128(key);
        let h1 = h as u64;
        let m = self.num_bits as u64;
        // Non-zero step in [1, m); a zero step would probe one bit k times.
        let h2 = ((h >> 64) as u64) % (m - 1) + 1;
        (h1, h2)
    }

    /// The `k` bit indices probed for `key`, via `g_i = h1 + i·h2 (mod m)`.
    fn probes(&self, key: &[u8]) -> impl Iterator<Item = u32> + '_ {
        let (h1, h2) = self.hashes(key);
        let m = self.num_bits as u64;
        (0..self.num_hashes).map(move |i| {
            let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
            (combined % m) as u32
        })
    }

    /// Record `key` as present, setting all `k` of its bits.
    pub fn insert(&mut self, key: &[u8]) {
        for idx in self.probes(key).collect::<Vec<_>>() {
            self.bits[(idx / 8) as usize] |= 1 << (idx % 8);
        }
    }

    /// Test whether `key` *may* be present. Returns `false` only if `key` is definitely absent (no false
    /// negatives); `true` means "probably present" — possibly a false positive.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.probes(key)
            .all(|idx| self.bits[(idx / 8) as usize] & (1 << (idx % 8)) != 0)
    }

    /// Number of hash probes per key (`k`). Exposed for the FPR test's
    /// theoretical comparison.
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Number of bits in the filter (`m`).
    pub fn num_bits(&self) -> u32 {
        self.num_bits
    }

    /// Serialize the filter into `buf` (little-endian): `num_bits`,
    /// `num_hashes`, then the raw bit array. The caller frames this with a CRC.
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        put_u32(buf, self.num_bits);
        put_u32(buf, self.num_hashes);
        put_u64(buf, self.bits.len() as u64);
        buf.extend_from_slice(&self.bits);
    }

    /// Parse a filter previously written by [`encode`](BloomFilter::encode), validating internal
    /// consistency (`num_bits` is a non-zero multiple of 8, and the bit-array length matches it).
    pub(crate) fn decode(cur: &mut Cursor<'_>) -> Result<Self> {
        let num_bits = cur.u32()?;
        let num_hashes = cur.u32()?;
        let byte_len = cur.u64()? as usize;
        let bits = cur.take(byte_len)?.to_vec();
        if num_bits == 0 || num_bits % 8 != 0 {
            return Err(SsTableError::Corrupt(format!(
                "bloom num_bits {num_bits} is not a positive multiple of 8"
            )));
        }
        if num_hashes == 0 {
            return Err(SsTableError::Corrupt("bloom num_hashes is zero".into()));
        }
        if (num_bits / 8) as usize != bits.len() {
            return Err(SsTableError::Corrupt(format!(
                "bloom bit-array length {} disagrees with num_bits {num_bits}",
                bits.len()
            )));
        }
        Ok(BloomFilter {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut bloom = BloomFilter::new(1000, 10);
        for i in 0..1000u32 {
            bloom.insert(&i.to_le_bytes());
        }
        // Every inserted key must report present — the one guarantee.
        for i in 0..1000u32 {
            assert!(bloom.contains(&i.to_le_bytes()), "false negative for {i}");
        }
    }

    #[test]
    fn empty_filter_reports_absent() {
        let bloom = BloomFilter::new(0, 10);
        assert!(!bloom.contains(b"anything"));
    }

    #[test]
    fn sizing_matches_derivation() {
        // 10 bits/key should pick k = round(10 ln2) = round(6.93) = 7, and
        // m ≈ 10 000 bits rounded up to a whole byte (not a power of two).
        let bloom = BloomFilter::new(1000, 10);
        assert_eq!(bloom.num_hashes(), 7);
        assert_eq!(bloom.num_bits() % 8, 0);
        assert!(bloom.num_bits() >= 1000 * 10);
        // Byte-aligned, not power-of-two: no more than a byte of slack.
        assert!(bloom.num_bits() < 1000 * 10 + 8);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mut bloom = BloomFilter::new(500, 10);
        for i in 0..500u32 {
            bloom.insert(&i.to_le_bytes());
        }
        let mut buf = Vec::new();
        bloom.encode(&mut buf);
        let mut cur = Cursor::new(&buf);
        let decoded = BloomFilter::decode(&mut cur).expect("decode");
        assert_eq!(decoded, bloom);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn measured_fpr_near_theoretical() {
        // Insert n keys, query a disjoint key set, and confirm the empirical
        // FPR is in the neighbourhood of the theoretical (1 - e^(-kn/m))^k.
        let n = 10_000usize;
        let bits_per_key = 10;
        let mut bloom = BloomFilter::new(n, bits_per_key);
        for i in 0..n as u64 {
            bloom.insert(&i.to_le_bytes());
        }
        let m = bloom.num_bits() as f64;
        let k = bloom.num_hashes() as f64;
        let theoretical = (1.0 - (-k * n as f64 / m).exp()).powf(k);

        let trials = 100_000u64;
        let mut fp = 0u64;
        for i in n as u64..n as u64 + trials {
            if bloom.contains(&i.to_le_bytes()) {
                fp += 1;
            }
        }
        let measured = fp as f64 / trials as f64;
        // Loose bound: measured within 2x of theoretical (and both small).
        assert!(
            measured < theoretical * 2.0 + 0.005,
            "measured FPR {measured} far exceeds theoretical {theoretical}"
        );
    }
}

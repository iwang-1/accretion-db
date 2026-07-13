//! An exact latency histogram: record every sample, sort once, read
//! percentiles off the sorted vector.
//!
//! We keep raw nanosecond samples rather than bucketing. A closed-loop run of
//! 1M operations costs 8 MiB of `u64`s — trivial — and buys *exact* p50/p99
//! rather than a bucket approximation, which matters for an honesty-first
//! portfolio: the number printed is the number measured, not the nearest
//! bucket edge. Percentiles use the nearest-rank method on the sorted samples.

/// Accumulates per-operation latencies (nanoseconds) for one workload phase.
#[derive(Debug, Default)]
pub struct Histogram {
    samples: Vec<u64>,
    sorted: bool,
}

impl Histogram {
    /// A histogram pre-sized for `expected` samples (avoids reallocations on the
    /// hot path — allocation noise would pollute the latency it is measuring).
    pub fn with_capacity(expected: usize) -> Self {
        Histogram {
            samples: Vec::with_capacity(expected),
            sorted: false,
        }
    }

    /// Record one latency sample in nanoseconds.
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        self.samples.push(nanos);
        self.sorted = false;
    }

    /// Number of recorded samples.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// The raw samples (nanoseconds), in insertion order — used to merge
    /// per-worker histograms into one aggregate.
    pub fn samples_slice(&self) -> &[u64] {
        &self.samples
    }

    /// Whether no samples were recorded. (Kept as the conventional companion to
    /// [`len`](Self::len); used by tests and required by `clippy`.)
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.samples.sort_unstable();
            self.sorted = true;
        }
    }

    /// The `q`-quantile (0.0..=1.0) in nanoseconds by nearest-rank, or 0 if empty.
    pub fn quantile(&mut self, q: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        self.ensure_sorted();
        let q = q.clamp(0.0, 1.0);
        // Nearest-rank: rank = ceil(q * n), clamped to [1, n], 1-based.
        let n = self.samples.len();
        let rank = (q * n as f64).ceil() as usize;
        let idx = rank.clamp(1, n) - 1;
        self.samples[idx]
    }

    /// The minimum sample in nanoseconds, or 0 if empty.
    pub fn min(&mut self) -> u64 {
        self.ensure_sorted();
        self.samples.first().copied().unwrap_or(0)
    }

    /// The maximum sample in nanoseconds, or 0 if empty.
    pub fn max(&mut self) -> u64 {
        self.ensure_sorted();
        self.samples.last().copied().unwrap_or(0)
    }

    /// The arithmetic mean in nanoseconds, or 0 if empty.
    pub fn mean(&self) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let sum: u128 = self.samples.iter().map(|&s| s as u128).sum();
        (sum / self.samples.len() as u128) as u64
    }

    /// A one-line latency summary (`min/mean/p50/p99/max`) in microseconds.
    pub fn summary_us(&mut self) -> LatencySummary {
        LatencySummary {
            min_us: self.min() as f64 / 1_000.0,
            p50_us: self.quantile(0.50) as f64 / 1_000.0,
            p99_us: self.quantile(0.99) as f64 / 1_000.0,
            max_us: self.max() as f64 / 1_000.0,
            mean_us: self.mean() as f64 / 1_000.0,
        }
    }
}

/// A rendered latency summary in microseconds (what the driver prints).
#[derive(Debug, Clone, Copy)]
pub struct LatencySummary {
    /// Minimum observed latency.
    pub min_us: f64,
    /// Median (p50) latency.
    pub p50_us: f64,
    /// 99th-percentile latency.
    pub p99_us: f64,
    /// Maximum observed latency.
    pub max_us: f64,
    /// Arithmetic mean latency.
    pub mean_us: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_zero() {
        let mut h = Histogram::default();
        assert!(h.is_empty());
        assert_eq!(h.quantile(0.5), 0);
        assert_eq!(h.min(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.mean(), 0);
    }

    #[test]
    fn nearest_rank_percentiles() {
        let mut h = Histogram::with_capacity(100);
        // 1..=100 nanoseconds.
        for v in 1..=100u64 {
            h.record(v);
        }
        // Nearest-rank: p50 -> rank ceil(0.5*100)=50 -> value 50.
        assert_eq!(h.quantile(0.50), 50);
        // p99 -> rank 99 -> value 99.
        assert_eq!(h.quantile(0.99), 99);
        // p100 -> rank 100 -> value 100 (max).
        assert_eq!(h.quantile(1.0), 100);
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 100);
        assert_eq!(h.mean(), 50); // (1+..+100)/100 = 5050/100 = 50 (floored)
    }

    #[test]
    fn single_sample() {
        let mut h = Histogram::with_capacity(1);
        h.record(42);
        assert_eq!(h.quantile(0.50), 42);
        assert_eq!(h.quantile(0.99), 42);
        assert_eq!(h.min(), 42);
        assert_eq!(h.max(), 42);
    }

    #[test]
    fn record_after_read_resorts() {
        let mut h = Histogram::with_capacity(4);
        h.record(10);
        h.record(5);
        assert_eq!(h.min(), 5);
        // A late, smaller sample must still be reflected.
        h.record(1);
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 10);
    }
}

//! The closed-loop workload driver: given a shared [`KvBench`] engine, run a
//! fill/read/scan phase at a chosen concurrency, timing every operation into a
//! per-thread [`Histogram`] and reporting throughput + latency percentiles.
//!
//! *Closed loop* means each worker issues the next operation only after the
//! previous one returns — so throughput is `ops / wall_time` and latency is the
//! real per-op service time, with no pipelining hiding the fsync cost. Criterion
//! is the tool for single-op latency *distributions*; this driver is the tool
//! for aggregate throughput under load (stated in the methodology).

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use crate::hist::{Histogram, LatencySummary};
use crate::kv::KvBench;
use crate::workload::{self, FillOrder};
use crate::BenchResult;

/// The outcome of one measured phase.
#[derive(Debug, Clone)]
pub struct PhaseReport {
    /// Phase name for the printout (e.g. `fill-random`).
    pub phase: String,
    /// Engine + durability label.
    pub engine: String,
    /// Number of worker threads.
    pub concurrency: usize,
    /// Total operations completed across all workers.
    pub ops: usize,
    /// Wall-clock seconds for the phase.
    pub secs: f64,
    /// Aggregate operations per second (`ops / secs`).
    pub throughput: f64,
    /// Merged latency summary across all workers (microseconds).
    pub latency: LatencySummary,
}

impl PhaseReport {
    /// A single human-readable line for the terminal / smoke output.
    pub fn render(&self) -> String {
        format!(
            "{:<12} {:<28} c={:<3} ops={:<8} {:>8.2}s {:>10.0} ops/s  \
             min={:>7.1}us mean={:>8.1}us p50={:>8.1}us p99={:>8.1}us max={:>9.1}us",
            self.phase,
            self.engine,
            self.concurrency,
            self.ops,
            self.secs,
            self.throughput,
            self.latency.min_us,
            self.latency.mean_us,
            self.latency.p50_us,
            self.latency.p99_us,
            self.latency.max_us,
        )
    }
}

/// Split `n` items into `workers` contiguous, near-equal chunks (as index
/// ranges). The last chunk absorbs the remainder.
fn partition(n: usize, workers: usize) -> Vec<(usize, usize)> {
    let workers = workers.max(1);
    let base = n / workers;
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0;
    for w in 0..workers {
        let len = if w == workers - 1 { n - start } else { base };
        ranges.push((start, start + len));
        start += len;
    }
    ranges
}

/// Merge per-worker histograms into one.
fn merge_hists(hists: Vec<Histogram>) -> Histogram {
    let total: usize = hists.iter().map(|h| h.len()).sum();
    let mut merged = Histogram::with_capacity(total);
    for h in &hists {
        for &s in h.samples_slice() {
            merged.record(s);
        }
    }
    merged
}

/// Run the fill phase: write every index in the generated order, timing each
/// `put`. The generated order is a pure function of `(order, n, seed)`, so
/// concurrency only changes *who* writes an index, never *which* bytes.
pub fn run_fill<K>(
    engine: &Arc<K>,
    order: FillOrder,
    n: u64,
    concurrency: usize,
    seed: u64,
) -> BenchResult<PhaseReport>
where
    K: KvBench + Send + Sync + 'static,
{
    let indices = Arc::new(workload::fill_indices(order, n, seed));
    let ranges = partition(indices.len(), concurrency);

    let start = Instant::now();
    let hists = thread::scope(|scope| -> BenchResult<Vec<Histogram>> {
        let mut handles = Vec::with_capacity(ranges.len());
        for (lo, hi) in ranges {
            let engine = Arc::clone(engine);
            let indices = Arc::clone(&indices);
            handles.push(scope.spawn(move || -> BenchResult<Histogram> {
                let mut hist = Histogram::with_capacity(hi - lo);
                for &i in &indices[lo..hi] {
                    let key = workload::key_for(i);
                    let value = workload::value_for(i);
                    let t = Instant::now();
                    engine.put(&key, &value)?;
                    hist.record(t.elapsed().as_nanos() as u64);
                }
                Ok(hist)
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.join().expect("fill worker panicked")?);
        }
        Ok(out)
    })?;
    let secs = start.elapsed().as_secs_f64();

    let phase = match order {
        FillOrder::Sequential => "fill-seq",
        FillOrder::Random => "fill-random",
    };
    Ok(finish(phase, engine.label(), concurrency, secs, hists))
}

/// Run the point-read phase: look up `count` random keys, timing each `get`.
/// Verifies each present value matches the deterministic expected bytes so a
/// silently-wrong read cannot masquerade as fast.
pub fn run_point_read<K>(
    engine: &Arc<K>,
    n: u64,
    count: usize,
    concurrency: usize,
    seed: u64,
) -> BenchResult<PhaseReport>
where
    K: KvBench + Send + Sync + 'static,
{
    let indices = Arc::new(workload::read_indices(n, count, seed));
    let ranges = partition(indices.len(), concurrency);

    let start = Instant::now();
    let hists = thread::scope(|scope| -> BenchResult<Vec<Histogram>> {
        let mut handles = Vec::with_capacity(ranges.len());
        for (lo, hi) in ranges {
            let engine = Arc::clone(engine);
            let indices = Arc::clone(&indices);
            handles.push(scope.spawn(move || -> BenchResult<Histogram> {
                let mut hist = Histogram::with_capacity(hi - lo);
                for &i in &indices[lo..hi] {
                    let key = workload::key_for(i);
                    let t = Instant::now();
                    let got = engine.get(&key)?;
                    hist.record(t.elapsed().as_nanos() as u64);
                    match got {
                        Some(v) if v == workload::value_for(i) => {}
                        Some(_) => return Err("point-read: value mismatch".into()),
                        None => return Err("point-read: missing key that was filled".into()),
                    }
                }
                Ok(hist)
            }));
        }
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.join().expect("read worker panicked")?);
        }
        Ok(out)
    })?;
    let secs = start.elapsed().as_secs_f64();

    Ok(finish(
        "point-read",
        engine.label(),
        concurrency,
        secs,
        hists,
    ))
}

/// Run the scan phase: `count` forward range scans, each covering a fixed-width
/// window starting at a random key; times the whole scan (visit every pair).
pub fn run_scan<K>(
    engine: &Arc<K>,
    n: u64,
    count: usize,
    window: u64,
    seed: u64,
) -> BenchResult<PhaseReport>
where
    K: KvBench + Send + Sync + 'static,
{
    // Scans are measured single-threaded: a scan is a heavy, long operation and
    // the metric of interest is per-scan latency + pairs/sec, not lock contention.
    let starts = workload::read_indices(n.saturating_sub(window).max(1), count, seed);

    let start = Instant::now();
    let mut hist = Histogram::with_capacity(count);
    let mut total_pairs = 0usize;
    for s in starts {
        let lo = workload::key_for(s);
        let hi = workload::key_for(s + window);
        let t = Instant::now();
        total_pairs += engine.scan_count(&lo, &hi)?;
        hist.record(t.elapsed().as_nanos() as u64);
    }
    let secs = start.elapsed().as_secs_f64();

    let mut report = finish("scan", engine.label(), 1, secs, vec![hist]);
    // For scans, "ops" is more informative as pairs visited; keep ops = scans and
    // let the caller read throughput as scans/sec. Record pairs in the phase name.
    report.phase = format!("scan(≈{} pairs)", total_pairs);
    Ok(report)
}

/// Assemble a [`PhaseReport`] from per-worker histograms and elapsed time.
fn finish(
    phase: &str,
    engine: String,
    concurrency: usize,
    secs: f64,
    hists: Vec<Histogram>,
) -> PhaseReport {
    let mut merged = merge_hists(hists);
    let ops = merged.len();
    let throughput = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
    PhaseReport {
        phase: phase.to_string(),
        engine,
        concurrency,
        ops,
        secs,
        throughput,
        latency: merged.summary_us(),
    }
}

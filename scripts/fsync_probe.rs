//! `fsync_probe`

#![forbid(unsafe_code)]

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BLOCK: usize = 4096;
const DEFAULT_ITERS: usize = 1000;
/// Discard the first few samples: cold-cache / first-allocation effects that
/// would skew the low percentiles and misrepresent steady-state cost.
const WARMUP: usize = 16;

fn main() {
    let cfg = Config::from_args();
    println!("fsync_probe: {} iterations, 4 KiB blocks", cfg.iters);
    println!("  dir: {}", cfg.dir.display());

    match run(&cfg) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("fsync_probe: error: {e}");
            std::process::exit(1);
        }
    }
}

struct Config {
    iters: usize,
    dir: PathBuf,
}

impl Config {
    fn from_args() -> Config {
        let mut iters = DEFAULT_ITERS;
        let mut dir = env::temp_dir();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--iters" => {
                    iters = args
                        .next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| fail("--iters needs a positive integer"));
                }
                "--dir" => {
                    dir = args
                        .next()
                        .map(PathBuf::from)
                        .unwrap_or_else(|| fail("--dir needs a path"));
                }
                "-h" | "--help" => {
                    println!(
                        "usage: fsync_probe [--iters N] [--dir PATH]\n\
                         measures 4 KiB sync_data / sync_all / dir-fsync latency"
                    );
                    std::process::exit(0);
                }
                other => fail(&format!("unknown argument: {other}")),
            }
        }
        if iters == 0 {
            fail("--iters must be > 0");
        }
        Config { iters, dir }
    }
}

fn fail(msg: &str) -> ! {
    eprintln!("fsync_probe: {msg}");
    std::process::exit(2);
}

fn run(cfg: &Config) -> std::io::Result<()> {
    // A unique probe file so concurrent runs / stale files never collide.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = cfg.dir.join(format!("accretion_fsync_probe_{nonce}.dat"));

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open(&path)?;

    let block = vec![0xA5u8; BLOCK];

    let mut data_ns = Vec::with_capacity(cfg.iters);
    let mut all_ns = Vec::with_capacity(cfg.iters);

    let total = cfg.iters + WARMUP;
    for i in 0..total {
        // Overwrite the same 4 KiB in place, then time just the durability call.
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&block)?;

        let t0 = Instant::now();
        file.sync_data()?;
        let d_data = t0.elapsed();

        // Dirty the page again so sync_all has real work (data + metadata).
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&block)?;
        let t1 = Instant::now();
        file.sync_all()?;
        let d_all = t1.elapsed();

        if i >= WARMUP {
            data_ns.push(d_data.as_nanos() as u64);
            all_ns.push(d_all.as_nanos() as u64);
        }
    }

    let dir_ns = probe_dir_fsync(&cfg.dir, cfg.iters)?;

    // Best-effort cleanup; a leftover probe file is harmless.
    drop(file);
    let _ = std::fs::remove_file(&path);

    println!();
    print_header();
    print_row("sync_data (fdatasync, 4 KiB)", &mut data_ns);
    print_row("sync_all  (fsync, 4 KiB)", &mut all_ns);
    if let Some(mut dir_ns) = dir_ns {
        print_row("dir fsync (rename durability)", &mut dir_ns);
    }
    println!();
    println!(
        "Headline for RESULTS.md: 4 KiB fsync p50 = {}, p99 = {} \
         (sync_data). Per-write durability ceiling ≈ {} ops/s.",
        fmt_dur(min_ns(percentile(&mut data_ns.clone(), 50.0))),
        fmt_dur(min_ns(percentile(&mut data_ns.clone(), 99.0))),
        ops_per_sec(percentile(&mut data_ns.clone(), 50.0)),
    );
    Ok(())
}

/// Measure the cost of fsync'ing a *directory* handle.
fn probe_dir_fsync(dir: &Path, iters: usize) -> std::io::Result<Option<Vec<u64>>> {
    let subdir = dir.join(format!(
        "accretion_fsync_probe_dir_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&subdir)?;

    let mut samples = Vec::with_capacity(iters);
    for i in 0..(iters + WARMUP) {
        // Create then rename a file inside the dir so the directory entry is
        // dirty, then time the directory fsync.
        let tmp = subdir.join(format!("t{i}.tmp"));
        let dst = subdir.join(format!("f{i}"));
        std::fs::write(&tmp, b"x")?;
        std::fs::rename(&tmp, &dst)?;

        match File::open(&subdir) {
            Ok(dh) => {
                let t0 = Instant::now();
                match dh.sync_all() {
                    Ok(()) => {
                        if i >= WARMUP {
                            samples.push(t0.elapsed().as_nanos() as u64);
                        }
                    }
                    // Directory fsync unsupported here: give up on this metric.
                    Err(_) => {
                        let _ = std::fs::remove_dir_all(&subdir);
                        return Ok(None);
                    }
                }
            }
            Err(_) => {
                let _ = std::fs::remove_dir_all(&subdir);
                return Ok(None);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&subdir);
    if samples.is_empty() {
        Ok(None)
    } else {
        Ok(Some(samples))
    }
}

fn print_header() {
    println!(
        "{:<32} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "operation", "p50", "p99", "min", "max", "mean"
    );
    println!("{}", "-".repeat(86));
}

fn print_row(label: &str, samples: &mut [u64]) {
    if samples.is_empty() {
        println!("{label:<32} {:>10}", "(n/a)");
        return;
    }
    let p50 = percentile(samples, 50.0);
    let p99 = percentile(samples, 99.0);
    let min = *samples.iter().min().unwrap();
    let max = *samples.iter().max().unwrap();
    let mean = samples.iter().sum::<u64>() / samples.len() as u64;
    println!(
        "{:<32} {:>10} {:>10} {:>10} {:>10} {:>10}",
        label,
        fmt_dur(min_ns(p50)),
        fmt_dur(min_ns(p99)),
        fmt_dur(min_ns(min)),
        fmt_dur(min_ns(max)),
        fmt_dur(min_ns(mean)),
    );
}

/// Wrap a nanosecond count as a `Duration` for uniform formatting.
fn min_ns(ns: u64) -> Duration {
    Duration::from_nanos(ns)
}

/// Nearest-rank percentile over a slice of nanosecond samples. Sorts in place.
fn percentile(samples: &mut [u64], pct: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let rank = (pct / 100.0 * samples.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(samples.len() - 1);
    samples[idx]
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 1000.0 {
        format!("{:.3}ms", us / 1000.0)
    } else {
        format!("{us:.1}µs")
    }
}

/// Durable writes/sec implied by a per-write fsync of `ns` nanoseconds.
fn ops_per_sec(ns: u64) -> u64 {
    if ns == 0 {
        return 0;
    }
    (1_000_000_000f64 / ns as f64) as u64
}

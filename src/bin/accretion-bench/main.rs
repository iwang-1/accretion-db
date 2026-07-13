//! `accretion-bench` — the closed-loop throughput/latency driver.
//!
//! Runs one of the spec's workloads (`fill-random`, `fill-seq`, `point-read`,
//! `scan`) against accretion-db in a chosen [`Durability`] mode — and, when
//! built with `--features bench-sled`, against the matched sled baseline — over
//! a real filesystem directory, reporting aggregate throughput and p50/p99/max
//! latency from an exact histogram.
//!
//! This binary is the *throughput* tool; criterion (see `benches/hot_paths.rs`)
//! is the *single-op latency distribution* tool. The full measurement matrix and
//! the numbers that fill the resume `{MEASURE:}` placeholders are produced in
//! stage S6 by driving this binary; this stage only builds and smoke-tests it.
//!
//! # Usage
//!
//! ```text
//! accretion-bench <workload> [options]
//!   workloads: fill-random | fill-seq | point-read | scan | all
//!   --engine <accretion|sled>     default accretion
//!   --durability <always|group|osbuffered>   default group  (accretion only)
//!   --keys <N>                    number of keys           default 100000
//!   --reads <N>                   point-read / scan count  default = keys
//!   --concurrency <N>             worker threads           default 1
//!   --memtable-bytes <N>          memtable freeze size     default 4194304
//!   --dir <path>                  data dir (default: a fresh temp dir)
//!   --seed <N>                    RNG seed                 default 0x5eed
//! ```
//!
//! The sled engine ignores `--durability`: it always uses its matched config
//! (durable = insert+flush) unless `--durability osbuffered` selects the
//! buffered (no-flush) shim. See `kv.rs` for the matched-durability rationale.

mod hist;
mod kv;
mod runner;
#[cfg(feature = "bench-sled")]
mod sled_shim;
mod workload;

use std::path::PathBuf;
use std::sync::Arc;

use accretion_db::Durability;

use crate::kv::{AccretionBench, KvBench};
use crate::runner::PhaseReport;
use crate::workload::FillOrder;

/// The driver's error type: any boxed, thread-safe error.
pub type BenchError = Box<dyn std::error::Error + Send + Sync>;
/// The driver's result alias.
pub type BenchResult<T> = std::result::Result<T, BenchError>;

/// Which engine to benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Accretion,
    Sled,
}

/// Parsed command-line configuration.
#[derive(Debug, Clone)]
struct Config {
    workload: String,
    engine: Engine,
    durability: Durability,
    keys: u64,
    reads: usize,
    concurrency: usize,
    memtable_bytes: usize,
    dir: Option<PathBuf>,
    seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            workload: "all".to_string(),
            engine: Engine::Accretion,
            durability: Durability::GroupCommit,
            keys: 100_000,
            reads: 0, // 0 => default to keys
            concurrency: 1,
            memtable_bytes: 4 * 1024 * 1024,
            dir: None,
            seed: 0x5eed,
        }
    }
}

fn parse_durability(s: &str) -> BenchResult<Durability> {
    match s {
        "always" => Ok(Durability::Always),
        "group" | "groupcommit" | "group-commit" => Ok(Durability::GroupCommit),
        "osbuffered" | "buffered" | "os" => Ok(Durability::OsBuffered),
        other => Err(format!("unknown durability {other:?}").into()),
    }
}

fn parse_args(args: &[String]) -> BenchResult<Config> {
    let mut cfg = Config::default();
    let mut it = args.iter();
    if let Some(first) = it.next() {
        if !first.starts_with("--") {
            cfg.workload = first.clone();
        } else {
            // First token was a flag: rewind by handling it below.
            return parse_flags(args, cfg);
        }
    }
    let rest: Vec<String> = it.cloned().collect();
    parse_flags(&rest, cfg)
}

fn parse_flags(args: &[String], mut cfg: Config) -> BenchResult<Config> {
    let mut i = 0;
    while i < args.len() {
        let flag = &args[i];
        let mut value = || -> BenchResult<String> {
            i += 1;
            args.get(i)
                .cloned()
                .ok_or_else(|| format!("flag {flag} needs a value").into())
        };
        match flag.as_str() {
            "--engine" => {
                cfg.engine = match value()?.as_str() {
                    "accretion" | "accretion-db" => Engine::Accretion,
                    "sled" => Engine::Sled,
                    o => return Err(format!("unknown engine {o:?}").into()),
                }
            }
            "--durability" => cfg.durability = parse_durability(&value()?)?,
            "--keys" => cfg.keys = value()?.parse().map_err(|e| format!("--keys: {e}"))?,
            "--reads" => cfg.reads = value()?.parse().map_err(|e| format!("--reads: {e}"))?,
            "--concurrency" => {
                cfg.concurrency = value()?
                    .parse()
                    .map_err(|e| format!("--concurrency: {e}"))?
            }
            "--memtable-bytes" => {
                cfg.memtable_bytes = value()?
                    .parse()
                    .map_err(|e| format!("--memtable-bytes: {e}"))?
            }
            "--dir" => cfg.dir = Some(PathBuf::from(value()?)),
            "--seed" => cfg.seed = value()?.parse().map_err(|e| format!("--seed: {e}"))?,
            other => return Err(format!("unknown flag {other:?}").into()),
        }
        i += 1;
    }
    if cfg.reads == 0 {
        cfg.reads = cfg.keys as usize;
    }
    Ok(cfg)
}

fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("accretion-bench: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> BenchResult<()> {
    let cfg = parse_args(args)?;

    // Resolve the data directory. A caller-supplied --dir is used as-is; else a
    // fresh, uniquely-named dir under the system temp root keeps runs isolated.
    // (We avoid the tempfile crate here — it is a dev-dependency and not
    // available to `[[bin]]` targets; a pid+seed name is unique enough for a
    // benchmark scratch dir.)
    let dir = match cfg.dir.clone() {
        Some(d) => d,
        None => {
            let mut d = std::env::temp_dir();
            d.push(format!(
                "accretion-bench-{}-{}",
                std::process::id(),
                cfg.seed
            ));
            std::fs::create_dir_all(&d).map_err(|e| format!("create data dir: {e}"))?;
            d
        }
    };

    println!(
        "# accretion-bench engine={:?} durability={:?} keys={} reads={} concurrency={} \
         memtable={}B seed={} dir={}",
        cfg.engine,
        cfg.durability,
        cfg.keys,
        cfg.reads,
        cfg.concurrency,
        cfg.memtable_bytes,
        cfg.seed,
        dir.display()
    );
    println!(
        "# NOTE: absolute numbers here are NOT resume figures — the committed \
         benchmark matrix (S6) runs on the disclosed build host."
    );

    let reports = match cfg.engine {
        Engine::Accretion => {
            let engine = Arc::new(AccretionBench::open(
                &dir,
                cfg.durability,
                cfg.memtable_bytes,
            )?);
            run_workload(&cfg, &engine)?
        }
        Engine::Sled => run_sled(&cfg, &dir)?,
    };

    for r in &reports {
        println!("{}", r.render());
    }
    Ok(())
}

/// Dispatch to the sled shims (only meaningful under `--features bench-sled`).
#[cfg(feature = "bench-sled")]
fn run_sled(cfg: &Config, dir: &std::path::Path) -> BenchResult<Vec<PhaseReport>> {
    use crate::sled_shim::{SledBuffered, SledDurable};
    if cfg.durability == Durability::OsBuffered {
        let engine = Arc::new(SledBuffered::open(dir)?);
        run_workload(cfg, &engine)
    } else {
        let engine = Arc::new(SledDurable::open(dir)?);
        run_workload(cfg, &engine)
    }
}

#[cfg(not(feature = "bench-sled"))]
fn run_sled(_cfg: &Config, _dir: &std::path::Path) -> BenchResult<Vec<PhaseReport>> {
    Err("sled engine requires building with --features bench-sled".into())
}

/// Run the configured workload(s) against an opened engine.
fn run_workload<K>(cfg: &Config, engine: &Arc<K>) -> BenchResult<Vec<PhaseReport>>
where
    K: KvBench + Send + Sync + 'static,
{
    let mut out = Vec::new();
    let want = |name: &str| cfg.workload == "all" || cfg.workload == name;

    if want("fill-random") || want("fill-seq") {
        let order = if want("fill-seq") && !want("fill-random") {
            FillOrder::Sequential
        } else {
            FillOrder::Random
        };
        // For "all" we run fill-random as the canonical fill so the read/scan
        // phases have data; fill-seq is run separately when requested alone.
        if cfg.workload == "fill-seq" {
            out.push(runner::run_fill(
                engine,
                FillOrder::Sequential,
                cfg.keys,
                cfg.concurrency,
                cfg.seed,
            )?);
            return Ok(out);
        }
        out.push(runner::run_fill(
            engine,
            order,
            cfg.keys,
            cfg.concurrency,
            cfg.seed,
        )?);
        if cfg.workload == "fill-random" {
            return Ok(out);
        }
    }

    // point-read and scan need data present and flushed to tables (cold read).
    if want("point-read") || want("scan") {
        if cfg.workload == "point-read" || cfg.workload == "scan" {
            // Standalone read/scan: fill first so there is something to read.
            out.push(runner::run_fill(
                engine,
                FillOrder::Random,
                cfg.keys,
                cfg.concurrency,
                cfg.seed,
            )?);
        }
        engine.flush()?; // make cold reads hit SSTables, not the memtable
        if want("point-read") {
            out.push(runner::run_point_read(
                engine,
                cfg.keys,
                cfg.reads,
                cfg.concurrency,
                cfg.seed ^ 0xA5A5,
            )?);
        }
        if want("scan") {
            let window = (cfg.keys / 100).clamp(1, 1000);
            out.push(runner::run_scan(
                engine,
                cfg.keys,
                cfg.reads.min(1000),
                window,
                cfg.seed ^ 0x1234,
            )?);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    //! HARNESS-FIRST smoke: drive the full fill -> flush -> point-read -> scan
    //! pipeline over a deterministic in-memory [`SimFs`], so the driver, the
    //! KvBench shim, and every runner phase are exercised under the fault
    //! seam — no disk, fast, and reproducible — before anything relies on it.

    use super::*;
    use accretion_db::{SimFs, Storage};

    fn sim_engine(seed: u64) -> Arc<AccretionBench> {
        let storage: Arc<dyn Storage> = Arc::new(SimFs::with_seed(seed));
        // Small memtable so the tiny workload still crosses a flush boundary.
        Arc::new(
            AccretionBench::open_on(
                storage,
                std::path::Path::new("/db"),
                Durability::Always,
                4 * 1024,
            )
            .expect("open on simfs"),
        )
    }

    #[test]
    fn fill_read_scan_over_simfs() {
        let engine = sim_engine(1);
        let keys = 500u64;

        let fill = runner::run_fill(&engine, FillOrder::Random, keys, 4, 7).unwrap();
        assert_eq!(fill.ops, keys as usize);
        assert!(fill.secs >= 0.0);
        assert!(fill.latency.p99_us >= fill.latency.p50_us);

        engine.flush().unwrap();

        // Every filled key must read back its exact value (run_point_read verifies
        // the bytes internally; a mismatch would return Err here).
        let read = runner::run_point_read(&engine, keys, keys as usize, 4, 9).unwrap();
        assert_eq!(read.ops, keys as usize);
        assert!(read.throughput > 0.0);

        let scan = runner::run_scan(&engine, keys, 20, 50, 3).unwrap();
        assert!(scan.phase.starts_with("scan"));
        assert!(scan.latency.max_us >= scan.latency.min_us);
    }

    #[test]
    fn fill_seq_matches_fill_random_contents() {
        // Both fill orders must leave the same key set present.
        let random = sim_engine(2);
        runner::run_fill(&random, FillOrder::Random, 200, 2, 1).unwrap();
        random.flush().unwrap();

        let seq = sim_engine(3);
        runner::run_fill(&seq, FillOrder::Sequential, 200, 1, 1).unwrap();
        seq.flush().unwrap();

        for i in 0..200u64 {
            let k = workload::key_for(i);
            assert_eq!(
                random.get(&k).unwrap(),
                seq.get(&k).unwrap(),
                "index {i} differs between fill orders"
            );
        }
    }

    #[test]
    fn arg_parsing_defaults_and_overrides() {
        let cfg = parse_args(&[]).unwrap();
        assert_eq!(cfg.workload, "all");
        assert_eq!(cfg.durability, Durability::GroupCommit);
        assert_eq!(cfg.reads, cfg.keys as usize);

        let cfg = parse_args(&[
            "point-read".into(),
            "--durability".into(),
            "always".into(),
            "--keys".into(),
            "1000".into(),
            "--concurrency".into(),
            "8".into(),
        ])
        .unwrap();
        assert_eq!(cfg.workload, "point-read");
        assert_eq!(cfg.durability, Durability::Always);
        assert_eq!(cfg.keys, 1000);
        assert_eq!(cfg.concurrency, 8);

        assert!(parse_args(&["--durability".into(), "bogus".into()]).is_err());
    }
}

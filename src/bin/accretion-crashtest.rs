//! `accretion-crashtest`

use std::io::{BufWriter, Write};
use std::path::Path;
use std::process::ExitCode;

use accretion_db::db::{Db, Options};
use accretion_db::Durability;

/// Deterministic key for record `i` (fixed width so keys sort in index order).
pub fn key_for(i: u64) -> Vec<u8> {
    format!("key{i:012}").into_bytes()
}

/// Deterministic value for record `i`: index-derived with padding so records are
/// large enough to force many flushes and at least one compaction during a run.
pub fn value_for(i: u64) -> Vec<u8> {
    format!("val-{i:012}-payload-padding-to-force-flushes-0123456789").into_bytes()
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let usage = || {
        eprintln!("usage: accretion-crashtest <write|verify> <dir>");
        ExitCode::from(2)
    };
    if args.len() < 3 {
        return usage();
    }
    let cmd = args[1].as_str();
    let dir = args[2].clone();

    match cmd {
        "write" => write_loop(&dir),
        "verify" => verify(&dir),
        _ => usage(),
    }
}

/// Open the database and write acked keys forever, announcing each durable put.
fn write_loop(dir: &str) -> ExitCode {
    let opts = Options {
        durability: Durability::Always,
        memtable_size: 4 * 1024, // small: force flushes + compaction quickly
        tier_fanout: 4,
    };
    let db = match Db::open(Path::new(dir), opts) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Announce acked indices on a buffered stdout that we flush every write, so the parent sees an index
    // only once its put is durable — and never loses a line to buffering when the sigkill lands.
    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut i: u64 = 0;
    loop {
        if let Err(e) = db.put(&key_for(i), &value_for(i)) {
            eprintln!("put {i} failed: {e}");
            return ExitCode::FAILURE;
        }
        // The put returned ⇒ record i is durable. Tell the parent.
        if writeln!(out, "{i}").is_err() || out.flush().is_err() {
            // Parent closed the pipe (it is about to kill us): stop cleanly.
            return ExitCode::SUCCESS;
        }
        i += 1;
    }
}

/// Reopen and print the value of every present key `0..`, stopping at the first
/// gap. Manual-debugging aid; the test verifies in-process instead.
fn verify(dir: &str) -> ExitCode {
    let opts = Options {
        durability: Durability::Always,
        memtable_size: 4 * 1024,
        tier_fanout: 4,
    };
    let db = match Db::open(Path::new(dir), opts) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut i: u64 = 0;
    loop {
        match db.get(&key_for(i)) {
            Ok(Some(_)) => {
                println!("{i}");
                i += 1;
            }
            Ok(None) => return ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("get {i} failed: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
}

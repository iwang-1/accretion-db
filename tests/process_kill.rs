//! Abrupt-process-death integration test (RealFs), complementing the
//! deterministic `SimFs` sweep in `tests/crash.rs`.
//!
//! Unlike `SimFs`, this test does not model hardware power loss or torn writes.
//! It spawns the `accretion-crashtest` child binary writing to a real temp
//! directory with [`Durability::Always`], lets it acknowledge durable writes,
//! then sends it `SIGKILL`: abrupt process death with no destructors, application
//! flush, or unwinding. It reopens the same directory *in this process* and
//! asserts every acknowledged key survived with its exact value.
//!
//! This exercises the durability contract across process death and reopen using
//! the real kernel and `fsync`; it does not test loss of kernel page cache or
//! storage-device persistence.
//!
//! Unix-only (needs `SIGKILL`); a no-op stub elsewhere.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use accretion_db::db::{Db, Options};
use accretion_db::storage::{RealFs, Storage};
use accretion_db::Durability;

const MIN_ACKNOWLEDGED_WRITES: u64 = 8;

/// Must match `accretion-crashtest`'s `key_for` / `value_for` exactly so the
/// parent can reconstruct the expected value for any acked index.
fn key_for(i: u64) -> Vec<u8> {
    format!("key{i:012}").into_bytes()
}
fn value_for(i: u64) -> Vec<u8> {
    format!("val-{i:012}-payload-padding-to-force-flushes-0123456789").into_bytes()
}

fn child_opts() -> Options {
    Options {
        durability: Durability::Always,
        memtable_size: 4 * 1024,
        tier_fanout: 4,
    }
}

/// Path to the freshly-built `accretion-crashtest` binary. Cargo sets
/// `CARGO_BIN_EXE_<name>` for every bin target when building tests.
fn crashtest_bin() -> String {
    env!("CARGO_BIN_EXE_accretion-crashtest").to_string()
}

/// Terminate the writer without unwinding. `Child::kill` sends SIGKILL on Unix
/// and reports delivery failure instead of letting the test wait indefinitely.
fn sigkill(child: &mut Child) {
    child.kill().expect("send SIGKILL to writer child");
}

fn acknowledged_count(highest_acked: Option<u64>) -> u64 {
    highest_acked.map_or(0, |highest| highest.saturating_add(1))
}

/// Spawn the writer, collect the highest acknowledged index it printed before we
/// kill it, then reopen and verify. Returns the number of acked keys verified.
fn run_one_kill(dir: &Path, settle: Duration) -> u64 {
    let mut child = Command::new(crashtest_bin())
        .arg("write")
        .arg(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn accretion-crashtest");

    // Read acknowledged indices on a separate thread so a stalled child cannot
    // block the test forever in `BufRead::lines`.
    let stdout = child.stdout.take().expect("child stdout");
    let (acked_tx, acked_rx) = mpsc::channel();
    let reader_thread = thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Ok(idx) = line.trim().parse::<u64>() {
                if acked_tx.send(idx).is_err() {
                    break;
                }
            }
        }
    });
    let mut highest_acked: Option<u64> = None;

    // Let the child run for the requested window and require a nontrivial durable
    // prefix before killing it. The hard deadline makes a no-progress host fail
    // cleanly after terminating the child instead of hanging the test.
    let started = Instant::now();
    let settle_deadline = started + settle;
    let hard_deadline = started + std::cmp::max(settle.saturating_mul(10), Duration::from_secs(5));
    loop {
        let now = Instant::now();
        if now >= settle_deadline && acknowledged_count(highest_acked) >= MIN_ACKNOWLEDGED_WRITES {
            break;
        }
        if now >= hard_deadline {
            break;
        }
        let wait = (hard_deadline - now).min(Duration::from_millis(50));
        match acked_rx.recv_timeout(wait) {
            Ok(idx) => highest_acked = Some(highest_acked.map_or(idx, |h| h.max(idx))),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    // Abruptly terminate the writer, then reopen from another process.
    sigkill(&mut child);
    let _ = child.wait();
    reader_thread.join().expect("stdout reader thread");
    // Drain any lines already emitted-and-flushed before the kill: those indices
    // were acknowledged too, so count them toward what must survive.
    for idx in acked_rx.try_iter() {
        highest_acked = Some(highest_acked.map_or(idx, |h| h.max(idx)));
    }

    let acked = acknowledged_count(highest_acked);
    assert!(
        acked >= MIN_ACKNOWLEDGED_WRITES,
        "writer acknowledged only {acked} writes; expected at least \
         {MIN_ACKNOWLEDGED_WRITES} before SIGKILL"
    );

    // Reopen in-process on RealFs and verify every acked key survived exactly.
    let fs: Arc<dyn Storage> = Arc::new(RealFs::new());
    let db = Db::open_on(fs, dir, child_opts()).expect("reopen after kill");
    for i in 0..acked {
        assert_eq!(
            db.get(&key_for(i)).expect("get"),
            Some(value_for(i)),
            "acked key {i} lost or corrupted after SIGKILL"
        );
    }
    acked
}

/// One SIGKILL mid-load: every acknowledged key must survive on real disk.
#[test]
fn sigkill_mid_load_preserves_acked_keys() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("adb");
    let acked = run_one_kill(&dir, Duration::from_millis(400));
    eprintln!("process-kill: verified {acked} acknowledged keys survived SIGKILL");
}

/// Kill, reopen, then relaunch and kill again against the SAME directory: the
/// engine must recover cleanly and keep every acked key across repeated abrupt
/// process deaths. This exercises the reopen → append → kill cycle on real disk,
/// including WAL and manifest recovery.
///
/// The child restarts its index at 0 each round and re-puts the same
/// (deterministic) values, and the workload contains no deletes, so the set of
/// present keys only ever grows. We track the high-water mark of acked keys
/// across all rounds and, at the end, reopen once more to confirm every key up to
/// that mark is still present with its exact value — no repeated process death
/// ever lost an acknowledged write.
#[test]
fn repeated_sigkill_recovers_each_time() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("adb");

    let mut high_water = 0u64;
    for _round in 0..3 {
        // run_one_kill already verifies this round's acked prefix survived; here
        // we also accumulate the high-water mark across rounds.
        let acked = run_one_kill(&dir, Duration::from_millis(250));
        high_water = high_water.max(acked);
    }

    // Final reopen: every key ever acknowledged (across all rounds) is present.
    let fs: Arc<dyn Storage> = Arc::new(RealFs::new());
    let db = Db::open_on(fs, &dir, child_opts()).expect("final reopen");
    for i in 0..high_water {
        assert_eq!(
            db.get(&key_for(i)).expect("get"),
            Some(value_for(i)),
            "key {i} (acked in some round) lost across repeated SIGKILLs"
        );
    }
    eprintln!("repeated process-kill: {high_water} keys survived 3 SIGKILL rounds");
}

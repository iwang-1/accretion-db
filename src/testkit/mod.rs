//! `testkit`

use std::cell::Cell;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};

use crate::storage::{CrashReport, SimFs, Storage, StorageError, StorageResult};

thread_local! {
    /// When set on a thread, the installed panic hook stays silent for panics on that thread.
    static SUPPRESS_PANIC: Cell<bool> = const { Cell::new(false) };
}

static HOOK_INIT: Once = Once::new();

/// Install (once, process-wide) a panic hook that suppresses output only on threads that have set
/// [`SUPPRESS_PANIC`].
fn install_quiet_hook() {
    HOOK_INIT.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let suppress = SUPPRESS_PANIC.with(|c| c.get());
            if !suppress {
                prev(info);
            }
        }));
    });
}

/// A [`Storage`] decorator that forwards to an inner [`SimFs`] and *halts*.
#[derive(Debug)]
struct CrashInjector {
    inner: Arc<SimFs>,
    halted: AtomicBool,
}

impl CrashInjector {
    fn new(inner: Arc<SimFs>) -> Self {
        CrashInjector {
            inner,
            halted: AtomicBool::new(false),
        }
    }

    /// Reject a mutating op if the inner filesystem has already crashed.
    fn guard(&self, path: &Path) -> StorageResult<()> {
        if self.halted.load(Ordering::SeqCst) {
            return Err(StorageError::Io {
                path: path.to_path_buf(),
                message: "simulated power loss: process halted after crash".into(),
            });
        }
        Ok(())
    }

    /// After a forwarded mutating op, latch `halted` if the crash has fired.
    fn observe(&self) {
        if self.inner.last_report().is_some() {
            self.halted.store(true, Ordering::SeqCst);
        }
    }
}

impl Storage for CrashInjector {
    fn create(&self, path: &Path) -> StorageResult<()> {
        self.guard(path)?;
        let r = self.inner.create(path);
        self.observe();
        r
    }

    fn open(&self, path: &Path) -> StorageResult<()> {
        self.inner.open(path)
    }

    fn append(&self, path: &Path, data: &[u8]) -> StorageResult<u64> {
        self.guard(path)?;
        let r = self.inner.append(path, data);
        self.observe();
        r
    }

    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> StorageResult<()> {
        self.guard(path)?;
        let r = self.inner.write_at(path, offset, data);
        self.observe();
        r
    }

    fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> StorageResult<usize> {
        self.inner.read_at(path, offset, buf)
    }

    fn sync_file(&self, path: &Path) -> StorageResult<()> {
        self.guard(path)?;
        let r = self.inner.sync_file(path);
        self.observe();
        r
    }

    fn sync_dir(&self, dir: &Path) -> StorageResult<()> {
        self.guard(dir)?;
        let r = self.inner.sync_dir(dir);
        self.observe();
        r
    }

    fn rename(&self, from: &Path, to: &Path) -> StorageResult<()> {
        self.guard(from)?;
        let r = self.inner.rename(from, to);
        self.observe();
        r
    }

    fn delete(&self, path: &Path) -> StorageResult<()> {
        self.guard(path)?;
        let r = self.inner.delete(path);
        self.observe();
        r
    }

    fn list(&self, dir: &Path) -> StorageResult<Vec<std::path::PathBuf>> {
        self.inner.list(dir)
    }

    fn len(&self, path: &Path) -> StorageResult<u64> {
        self.inner.len(path)
    }
}

/// Run `body` against a fresh, seeded [`SimFs`] and return the number of mutating storage ops it performed
/// — the `N` a crash sweep ranges over.
pub fn count_ops(seed: u64, body: impl FnOnce(Arc<dyn Storage>)) -> u64 {
    let sim = Arc::new(SimFs::with_seed(seed));
    let fs: Arc<dyn Storage> = sim.clone();
    body(fs);
    sim.op_count()
}

/// Run `body` against a fresh, seeded [`SimFs`] with a crash armed to fire the instant `crash_after`
/// mutating ops have completed, then hand the recovered filesystem to `verify`. * `body` receives a [`Storage`].
pub fn run_crash(
    seed: u64,
    crash_after: u64,
    body: impl FnOnce(Arc<dyn Storage>),
    verify: impl FnOnce(Arc<dyn Storage>, &CrashReport),
) -> CrashReport {
    let sim = Arc::new(SimFs::with_seed(seed));
    sim.arm_crash_after(crash_after);
    let injector: Arc<dyn Storage> = Arc::new(CrashInjector::new(sim.clone()));

    // A simulated power loss kills the process at an arbitrary op boundary, so the workload closure will
    // hit an error on its next mutating call — and a typical closure surfaces that by panicking (`.expect(...)`).
    install_quiet_hook();
    let run = std::panic::AssertUnwindSafe(move || body(injector));
    SUPPRESS_PANIC.with(|c| c.set(true));
    let outcome = std::panic::catch_unwind(run);
    SUPPRESS_PANIC.with(|c| c.set(false));
    if let Err(panic) = outcome {
        // A panic that fired without any crash is a genuine workload bug: re-raise.
        if sim.last_report().is_none() {
            std::panic::resume_unwind(panic);
        }
    }

    // If the armed crash never fired (workload shorter than `crash_after`),
    // induce a power loss now so the verifier always sees a post-crash image.
    let report = match sim.last_report() {
        Some(r) => r,
        None => sim.crash(),
    };

    let recovered: Arc<dyn Storage> = sim;
    verify(recovered, &report);
    report
}

/// Exhaustive deterministic crash sweep: count the workload's `N` mutating ops, then [`run_crash`] once for
/// every crash point `i in 1..=N`, verifying each recovered image.
pub fn crash_sweep(
    seed: u64,
    body: impl Fn(Arc<dyn Storage>),
    verify: impl Fn(Arc<dyn Storage>, &CrashReport),
) -> u64 {
    let n = count_ops(seed, &body);
    for i in 1..=n {
        run_crash(seed, i, &body, &verify);
    }
    n
}

# Bugs found

A journal of correctness and harness-fidelity bugs found by tests, benchmarks,
and independent review while building this engine.

## The organic-bugs-only rule

Entries here are **real bugs found during development and verification** — never
fabricated, never planted to pad the count. Each entry records the test,
benchmark, or review that exposed it, the root cause, the fix, and a regression.
The point of the harness-first discipline is that this file fills *organically*
as the engine grows up under `SimFs`.

## If the count is genuinely zero at ship

If the engine reaches ship with no organic bug found, this file will not invent
one. Instead it will document a **harness validation**: deliberately remove one
`fsync` (e.g. drop the WAL `sync_file` before ack), show the crash sweep catch
the resulting acknowledged-write loss, then restore it. That entry will be
labelled clearly as *validation of the harness*, never passed off as a bug the
engine had.

## Journal

### 1. Tombstone GC resurrected deleted keys when the destination tier was non-empty (S2)

**Found by:** `tests/model.rs::engine_matches_btreemap_model` — the BTreeMap
reference-model property test, on a random op sequence over a 5-letter key
alphabet with a 128-byte memtable (frequent flushes + compactions).
proptest shrank it to a scan where the engine returned a key (`"cc"`) that the
model had deleted: the merged table still carried a live `"cc"` the tombstone
was supposed to erase.

**Root cause:** size-tiered compaction merges tier `t` into tier `t + 1` and may
physically drop tombstones only when the merge output is the globally-oldest
data. The bottom-tier check was `output_is_bottom = tiers[t+2..] all empty`,
which ignored the **destination** tier `t + 1` itself. When tier `t + 1` already
held older tables from a prior compaction, a tombstone in tier `t` could be
shadowing a live value living in that older tier-`t+1` table; dropping the
tombstone during the `t → t+1` merge (which does not include those older tables)
resurrected the deleted key.

**Fix:** require tier `t + 1` **and** everything below it to be empty before the
merge — `output_is_bottom = tiers[t+1..] all empty` — so a tombstone is dropped
only when there is provably nothing older left for it to mask. `src/compaction.rs`,
same commit as this entry.

**Why the harness caught it and hand-written tests had not:** the unit tests only
ever compacted into an *empty* destination tier, so the buggy and correct
predicates agreed. It takes a multi-round workload that fills tier 0, compacts
into tier 1, then fills and compacts tier 0 *again* — with a delete of a key that
already lives in tier 1 — for the predicates to diverge. Random op sequences
found that shape immediately.

### 2. SimFs under-modeled `rename` over an already-durable file → false acknowledged-write loss (S3)

**Found by:** `tests/crash.rs::exhaustive::{always,group_commit}_zero_acked_loss_at_every_crash_point`
— the exhaustive crash-point sweep over the full engine, on the very first run.
At crash points from op 28 onward (seeds 1/7/42/1234, tear modes drop and
bit-flip), `key00` — a value whose `put` had *fully acknowledged* under a durable
mode — was **absent** after recovery. Reducing to a direct SimFs probe: install
`MANIFEST=v1` durably (create+append+sync_file+sync_dir), then atomically replace
it with `v2` (tmp+append+sync_file+rename-over-`MANIFEST`+sync_dir), then crash.
The recovered `MANIFEST` was `v1`, not `v2`.

**Root cause:** the fault is in the *harness*, not the engine. `SimFs::rename`
linked the destination in the live namespace but recorded nothing about what the
destination should durably resolve to. `sync_dir` then only refreshed a file's
durable image when `!present_durable` — so a rename **over a name that already
existed durably** (exactly the manifest's atomic overwrite of `MANIFEST`, and the
WAL's segment-truncate rewrite) left the destination's `durable` bytes pointing at
the *old* content. A crash after the committing `sync_dir` reverted `MANIFEST` to
the previous version, silently dropping every table the newer version had
installed — and with it, acknowledged writes. Real POSIX makes a `rename` durable
once the parent directory is fsync'd regardless of whether the target previously
existed, so this was SimFs modeling *less* durability than a real disk offers: a
false positive that would have wrongly failed a *correct* engine.

**Fix:** the final model assigns every file generation an inode identity. Each
path stores separate live and durable inode bindings; inode state separately
stores live and synced bytes. `rename` moves the live inode identity and
`sync_dir` commits the namespace binding, while a crash before that barrier
restores the old binding. This also models overwrite and rollback without
copying path-level byte snapshots. `src/storage/sim.rs`; covered by
`rename_over_durable_file_commits_new_content_on_dir_sync` and
`rename_over_durable_reverts_if_crash_before_dir_sync`.

**Honesty note:** this is a harness-fidelity bug, logged here because the crash
sweep is the product and its fidelity *is* the deliverable. It was a
false-positive (the engine's manifest protocol was already correct); fixing it
made SimFs model the POSIX rename-durability guarantee the manifest and WAL
depend on, so the sweep now tests the engine against the simulator's documented
fault model rather than an overly-pessimistic path model.

### 3. Group commit degenerated to per-write fsync — the write lock was held across the append (S5.5)

**Found by:** the benchmark calibration itself (the throughput harness). At every
concurrency the `Durability::GroupCommit` fill matched `Durability::Always`
(~369 ops/s at c=64) instead of beating it — the headline mode delivered no
batching win. The group-commit design was demonstrably correct in isolation:
`wal::tests::group_commit_concurrent_writers_all_durable` passed, proving the WAL
leader/follower batching works when writers reach `append` concurrently.

**Root cause:** `Db::write` held the db-level `write` mutex across
`self.wal.append(&record)`. In `GroupCommit`, `append` enqueues the frame and
then *parks* on the leader's shared fsync (`wal/mod.rs::commit_group`). Because
the db mutex was still held during that park, no second writer could ever reach
`append` to enqueue into the same batch — so every batch contained exactly one
frame and group commit collapsed into one fsync per write, identical to
`Always`. The bug was in the *engine's* write-path locking, not the WAL: the WAL
could batch, but the layer above it serialized the writers before they got there.

**Fix:** restructure `Db::write` into three phases so the db mutex is held only to
order the write, not across the durable wait. Phase 1 (locked) claims the
monotonic seq and marks the write in flight; phase 2 (unlocked) runs
`wal.append`, which is exactly where concurrent `GroupCommit` writers now enqueue
and share one leader fsync; phase 3 (re-locked) applies to the memtable and
clears the in-flight mark. Two invariants are preserved explicitly: (a) seq is
still totally ordered because it is claimed under the lock, and concurrent appends
that ack out of seq order can no longer clobber a newer value because the memtable
insert became seq-guarded (`MemtableSet::insert_if_newer`); (b) a flush cannot
race a still-in-flight (acked-but-not-yet-applied) write — `flush_locked` sets a
`flush_pending` gate that blocks new writers and waits on a `Condvar` for
`in_flight == 0` before `wal.reset()`, so no acked write is ever dropped from the
log before it lands in the memtable. `src/db.rs` + `src/memtable/mod.rs`, same
commit as this entry.

**Measured effect (build host, 10k-key fill-random, c=64):** GroupCommit rose from
~369 ops/s to ~9.0k ops/s while Always stayed at ~367 ops/s — a ~24x group-commit
speedup where before there was none. (Absolute numbers are build-host calibration
figures, not the disclosed S6 resume matrix.)

**Why the harness caught it and unit tests had not:** the WAL unit test drives
`append` directly from many threads, so it observes the batching the WAL is
capable of. Only an end-to-end throughput run *through the engine's write path* at
real concurrency exposes that the layer above the WAL was serializing writers
before they could batch — a genuine integration-level bug the closed-loop
benchmark surfaced.

**Crash-consistency re-verification (post-fix, S5.5c):** because the fix restructured the
write path's locking — exactly what the crash invariant depends on — the full crash
suite was re-run against the new `claim → log (unlocked) → apply` phasing. All
green, zero acknowledged-write loss, no regression introduced:
`crash::exhaustive` 4/4 (330 distinct crash points × 4 seeds × 2 durable modes =
2640 crash executions), `crash::schedules::random_schedule_zero_acked_loss` (160
proptest cases), and `process_kill` 2/2 (one single-kill run plus 3 repeated
SIGKILL/recover rounds). Acknowledged-key counts are printed at runtime and vary
with host timing; the invariant is that every acknowledged key survives.

### 4. WAL replay order could let an older sequence overwrite a newer value

**Found by:** an independent concurrency audit of the three-phase write path.
Writers claim sequence numbers under the DB mutex but append to the WAL after
releasing it, so a writer with sequence 2 can reach the WAL before sequence 1.
A deterministic regression wrote records in `[2, 1]` order for one key through
the real WAL and reopened in both durable modes.

**Root cause:** live writes already used `MemtableSet::insert_if_newer`, but
`Db::open_on` replayed recovered records with unconditional `insert`. WAL replay
therefore treated physical append order as logical version order and could end
with the stale sequence-1 value after a restart.

**Fix:** recovery now uses the same sequence-aware insertion rule as the live
path. `db::tests::recovery_keeps_highest_sequence_when_wal_order_differs`
appends the inverted sequence order, waits for durable acknowledgements,
simulates a crash, reopens, and requires sequence 2 to win under both `Always`
and `GroupCommit`.

### 5. SimFs conflated path reuse and forgot older unsynced append candidates

**Found by:** independent fault-model review plus focused negative controls. The
path-level representation had two related fidelity holes. First, deleting a
durable file and recreating the same path reused the old durable byte image, so
a directory sync could make a crash resurrect bytes from the prior file
generation. Second, one global `last_append` pointer was cleared when the newest
appended file was synced, even if an older file still had an unsynced append
eligible for tearing.

**Root cause:** path namespace state, inode identity, byte durability, and tear
ordering were collapsed into one `FileState`. That cannot represent a live new
inode and an old durable inode at the same pathname simultaneously, nor more
than one outstanding unsynced append.

**Fix:** `SimFs` now stores separate live/durable inode bindings per path and
live/durable bytes per inode generation. `create` allocates a generation;
`rename` moves identity; `sync_file` durably updates only the live inode;
`sync_dir` commits bindings. Each inode also records its newest unsynced append
sequence, so crash selection scans all durably reachable candidates. Regressions
cover crash before/after the delete-recreate directory switch, syncing the new
inode before switching the name, rename identity and tear-path behavior,
same-path rename, and the two-file append/sync ordering case.

**Honesty note:** these were simulator defects, not storage-engine defects. They
are release blockers because the simulator's fidelity determines what the crash
campaign can legitimately claim. The model remains intentionally bounded and
does not claim to enumerate every filesystem or hardware failure outcome.

## Harness validation (positive control)

Bug #2 above was a false-*positive* (the harness reported loss the engine did not
have). To also confirm the harness is not a false-*negative* — that it genuinely
*catches* real acknowledged-write loss and is not vacuously green — a positive
control was run at the close of S3:

* **Sabotage:** in `Wal::commit_sync_now` (the `Durability::Always` path), the
  `w.active.sync(&*self.fs)?` fsync-before-ack was deleted, so an `Always` `put`
  returned before its WAL frame was durable.
* **Result:** `tests/crash.rs::exhaustive::always_zero_acked_loss_at_every_crash_point`
  failed *immediately* — at crash point 1, `acked=1`: `key00` had been
  acknowledged yet was `None` after recovery. Exactly the acknowledged-write loss
  the invariant forbids.
* **Revert:** the fsync was restored (`git`-clean revert, verified by `git diff`),
  and the sweep is green again.

This is labelled *validation of the harness*, not a bug the engine ever shipped:
the engine's code was never wrong here — the fsync was removed on purpose and put
straight back. It demonstrates the sweep has real detection power, closing the
loop on the two possible harness failure modes (false positive: bug #2, fixed;
false negative: this control, disproven).

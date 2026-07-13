# Bugs found

A journal of crash-consistency bugs the harness caught while building this
engine.

## The organic-bugs-only rule

Entries here are **real bugs found by the harness during development** — never
fabricated, never planted to pad the count. Each entry records: the harness
output that exposed it (seed, crash point, failing invariant), the root cause,
and the fix commit. The point of the harness-first discipline is that this file
fills *organically* as the engine grows up under `SimFs`.

## If the count is genuinely zero at ship

If the engine reaches ship with no organic bug found, this file will not invent
one. Instead it will document a **harness validation**: deliberately remove one
`fsync` (e.g. drop the WAL `sync_file` before ack), show the crash sweep catch
the resulting acknowledged-write loss, then restore it. That entry will be
labelled clearly as *validation of the harness*, never passed off as a bug the
engine had — and the resume bullet's wording will match.

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

**Fix:** give `FileState` a `staged_durable: Option<Vec<u8>>` slot. `rename` stages
the source inode's already-synced (`durable`) image into the destination; the
committing `sync_dir` moves that staged image into the destination's `durable`
bytes (superseding an existing durable image), while a crash before that
`sync_dir` discards the staged content and the destination reverts to its old
durable image. `sync_file` clears any staged intent (an explicit file sync makes
the live bytes durable directly). `src/storage/sim.rs`, same commit as this entry;
covered by the new `rename_over_durable_file_commits_new_content_on_dir_sync` and
`rename_over_durable_reverts_if_crash_before_dir_sync` unit tests.

**Honesty note:** this is a harness-fidelity bug, logged here because the crash
sweep is the product and its fidelity *is* the deliverable. It was a
false-positive (the engine's manifest protocol was already correct); fixing it
made SimFs model the exact POSIX rename-durability guarantee the manifest and WAL
depend on, so the sweep now proves the engine against a faithful power-loss model
rather than an overly-pessimistic one.

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

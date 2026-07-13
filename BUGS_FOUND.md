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

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

_No entries yet. The engine does not exist above the storage seam; the crash
sweep currently exercises only the toy store in `tests/harness.rs`, which is
correct by construction. Real entries begin at the WAL stage (S1)._

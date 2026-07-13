# On-disk formats

Byte-level layout of every file `accretion-db` writes. Multi-byte integers are
**little-endian** unless noted. This document is the authority the reader and
writer are checked against; sections are stubbed until each component lands.

## Conventions

- `u32` / `u64`: little-endian unsigned integers.
- CRC32: `crc32fast` (IEEE) over exactly the bytes stated, stored as `u32`.
- "Frame": a length-prefixed, CRC-checked unit that recovery can validate in
  isolation and truncate at on the first bad instance.

## WAL record frame (stub — WAL stage)

The write-ahead log is a flat sequence of frames. The toy harness store already
uses this shape; the production frame will extend it with a sequence number and
an op type (Put / Tombstone):

```
+------------------+------------------+-------------------------+
| payload_len: u32 | crc32(payload):  | payload (payload_len B) |
|                  |   u32            |                         |
+------------------+------------------+-------------------------+
```

Recovery scans frames from offset 0; it stops at the first frame whose header
runs past EOF, whose payload runs past EOF, or whose CRC does not match — the
torn-tail truncation rule (see DESIGN_NOTES.md).

## SSTable (stub — SSTable stage)

Planned: 4 KiB sorted data blocks, a per-table bloom filter, a sparse index
(first key per block), and a CRC-checksummed footer pointing at the index and
bloom. Full layout published when `src/sstable/` lands.

## Manifest (stub — manifest stage)

Planned: versioned metadata (tables per tier, next sequence number), written to
a new file then atomically renamed into place under the tmp+fsync+rename+dir-sync
protocol. Full layout published when `src/manifest.rs` lands.

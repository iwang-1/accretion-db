# On-disk formats

Byte-level layout of every file `accretion-db` writes. Multi-byte integers are
**little-endian** unless noted. This document is the authority the reader and
writer are checked against.

## Conventions

- `u32` / `u64`: little-endian unsigned integers.
- CRC32: `crc32fast` (IEEE) over exactly the bytes stated, stored as `u32`.
- "Frame": a length-prefixed, CRC-checked unit that recovery can validate in
  isolation and truncate at on the first bad instance.

## WAL segments and record frames (`src/wal/`)

The write-ahead log is split into monotonically numbered **segment files** named
`<id>.wal`, where `<id>` is a 20-digit zero-padded, lexicographically-sortable
id, so a plain sorted directory listing yields segments in write order. Splitting
into segments lets the engine release a whole prefix of segments in one step once
their records are safely in an SSTable, instead of rewriting one ever-growing
file.

Each segment is a flat sequence of **frames**. A frame is payload-agnostic — the
WAL never interprets record bytes — with an 8-byte header:

```
+------------------+------------------+-------------------------+
| payload_len: u32 | crc32(payload):  | payload (payload_len B) |
|                  |   u32            |                         |
+------------------+------------------+-------------------------+
```

The CRC32 covers **only the payload**. Recovery scans frames from offset 0 and
stops at the first frame whose header runs past EOF, whose payload runs past EOF,
or whose CRC does not match — the torn-tail truncation rule (see DESIGN_NOTES.md).

### Record payload (one logical mutation)

The engine's WAL payload encodes a single `put` or `delete`:

```
+----------+--------+-------------+---------+---------------+-----------+
| seq: u64 | tag:u8 | key_len:u32 | key     | value_len:u32 | value     |
|          |        |             | (bytes) | (Put only)    | (Put only)|
+----------+--------+-------------+---------+---------------+-----------+
```

`seq` is the monotonic sequence number claimed under the write lock (it totally
orders mutations across recovery). `tag` is `1` for a `Put` (followed by
`value_len` + `value`) or `0` for a `Delete`/tombstone (no value bytes). This
payload is what the frame's `payload_len`/`crc32` wrap.

## SSTable (`src/sstable/`)

An SSTable is an immutable, sorted run of key/value entries. The file is written
front-to-back — data blocks, then the sparse index, then the bloom filter, then
a fixed-size footer — so the reader parses it back-to-front: read the footer at
`EOF − 48`, follow its offsets to the index and bloom blocks.

```
+=====================+
| data block 0        |
+---------------------+
| data block 1        |
+---------------------+
| ...                 |
+---------------------+
| data block D-1      |
+=====================+
| index block         |
+=====================+
| bloom block         |
+=====================+
| footer (48 bytes)   |
+=====================+
```

Every block below ends in a trailing `crc32(preceding bytes of the block): u32`;
the reader recomputes and compares it, so a torn or bit-flipped byte becomes a
`Corrupt` error rather than a wrong answer.

### Data block

A data block is a concatenation of entries in **strictly increasing key order**,
capped at a 4 KiB target (a single entry larger than 4 KiB gets a block to
itself). Each entry:

```
+-------------+---------+--------+-------+--------------+-----------+
| key_len:u32 | key     | seq:u64| tag:u8| value_len:u32| value     |
|             | (bytes) |        |       | (Put only)   | (Put only)|
+-------------+---------+--------+-------+--------------+-----------+
```

`tag` is `1` for a live value (`Put`, followed by `value_len` + `value`) or `0`
for a tombstone (`Delete`, no value bytes). The block ends with `crc32: u32` over
all preceding entry bytes.

### Index block (sparse index)

One entry per data block: the block's **first key** plus a pointer to it. This is
"sparse" — only the first key of each block, not every key, so the index stays
small and is loaded whole at open time.

```
+---------------+  then, repeated `count` times:
| count: u32    |
+---------------+  +-------------+---------+-------------+----------+
                   | key_len:u32 | key     | offset: u64 | len: u32 |
                   +-------------+---------+-------------+----------+
```

`offset`/`len` locate the data block (length **includes** its trailing CRC). The
block ends with `crc32: u32` over all preceding bytes.

### Bloom block

The encoded per-table bloom filter followed by `crc32: u32`:

```
+--------------+----------------+-------------------+-----------+---------+
| num_bits:u32 | num_hashes:u32 | bit_array_len:u64 | bit_array | crc:u32 |
+--------------+----------------+-------------------+-----------+---------+
```

`num_bits` (`m`) is a non-zero multiple of 8 (byte-aligned); `num_hashes` (`k`)
is the probe count. See the bloom sizing note in DESIGN_NOTES.md.

### Footer (fixed 48 bytes)

```
+------------+-------------+-------------+------------+-------------+------------+----------------+---------+
| magic: u64 | version:u32 | index_off:  | index_len: | bloom_off:  | bloom_len: | num_entries:u64| crc:u32 |
|            |             |   u64       |   u32      |   u64       |   u32      |                |         |
+------------+-------------+-------------+------------+-------------+------------+----------------+---------+
```

`magic` = `0x41434352_5F535354` identifies an accretion-db SSTable (and
rejects a file too short to hold a footer). `crc` covers the first 44 bytes. The
reader validates `magic`, `version`, the footer CRC, and that the index/bloom
regions lie within the file before the footer — any failure is `Corrupt`.

## Manifest (`src/manifest.rs`)

The manifest is the crash-safe, versioned record of *which SSTables exist and
which tier each lives in*. It is written to a scratch file `MANIFEST.tmp`, then
`sync_file`d, atomically `rename`d over `MANIFEST`, then the parent directory is
`sync_dir`d — the tmp+fsync+rename+dir-fsync protocol that makes the switch
atomic across a crash (see DESIGN_NOTES.md → *Manifest atomicity*). The file is a
single self-describing snapshot with a trailing CRC:

```
+------------+-------------+--------------+-------------------+---------------+
| magic: u64 | format: u32 | next_seq:u64 | next_table_id:u64 | num_tiers:u32 |
+------------+-------------+--------------+-------------------+---------------+

then, for each of `num_tiers` tiers (tier 0 = youngest):
+----------------+   then, repeated `num_tables` times:
| num_tables:u32 |
+----------------+   +--------+----------------+--------------+-----------+--------------+----------+
                     | id:u64 | num_entries:u64| first_len:u32| first_key | last_len:u32 | last_key |
                     +--------+----------------+--------------+-----------+--------------+----------+

finally:
+------------+
| crc32: u32 |   over every preceding byte
+------------+
```

`magic` = `0x4143_4352_5F4D_4652` ("ACCR_MFR") tags an accretion-db manifest;
`format` is the on-disk version (`1`). Each table entry records its file `id`
(the on-disk file is `<id>.sst`, 20-digit zero-padded), its `num_entries`
(tombstones included), and its `[first_key, last_key]` range so the read path can
skip a table without opening it. Decode verifies the trailing CRC, the magic, and
the format before trusting any offset; any mismatch is a `Corrupt` error.

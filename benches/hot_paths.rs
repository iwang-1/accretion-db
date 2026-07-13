//! Criterion micro-benchmarks for accretion-db's hot paths.
//!
//! Criterion is the *single-op latency distribution* tool (statistical timing of
//! one operation in isolation); the closed-loop `accretion-bench` binary is the
//! *aggregate throughput under load* tool. This file measures the in-memory hot
//! paths that sit under every read and write and never touch the disk, so their
//! cost is not masked by fsync latency:
//!
//! * bloom filter `insert` and `contains` (hit + miss) — gates every table probe;
//! * memtable `insert` and `get` — the front of the write and read paths.
//!
//! CI runs these with `--bench hot_paths -- --test` (a smoke pass that executes
//! each bench once and asserts nothing about absolute numbers). Real numbers, if
//! ever quoted, come from the S6 host run.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};

use accretion_db::memtable::{InternalValue, Memtable};
use accretion_db::sstable::BloomFilter;

/// A fixed 16-byte key for index `i` (matches the bench workload key shape).
fn key_for(i: u64) -> Vec<u8> {
    let mut k = vec![0u8; 16];
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}

fn bench_bloom(c: &mut Criterion) {
    let n = 10_000usize;
    let mut group = c.benchmark_group("bloom");

    group.bench_function("insert_10k", |b| {
        b.iter_batched(
            || BloomFilter::new(n, 10),
            |mut bloom| {
                for i in 0..n as u64 {
                    bloom.insert(black_box(&key_for(i)));
                }
                bloom
            },
            BatchSize::SmallInput,
        );
    });

    // A populated filter for the query benches.
    let mut bloom = BloomFilter::new(n, 10);
    for i in 0..n as u64 {
        bloom.insert(&key_for(i));
    }

    group.bench_function("contains_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let hit = bloom.contains(black_box(&key_for(i % n as u64)));
            i = i.wrapping_add(1);
            hit
        });
    });

    group.bench_function("contains_miss", |b| {
        // Keys well outside the inserted range: exercises the true-negative path.
        let mut i = n as u64;
        b.iter(|| {
            let hit = bloom.contains(black_box(&key_for(i)));
            i = i.wrapping_add(1);
            hit
        });
    });

    group.finish();
}

fn bench_memtable(c: &mut Criterion) {
    let n = 10_000usize;
    let mut group = c.benchmark_group("memtable");

    group.bench_function("insert_10k", |b| {
        b.iter_batched(
            Memtable::new,
            |mut mt| {
                for i in 0..n as u64 {
                    mt.insert(key_for(i), InternalValue::value(i, key_for(i)));
                }
                mt
            },
            BatchSize::SmallInput,
        );
    });

    // A populated memtable for the read bench.
    let mut mt = Memtable::new();
    for i in 0..n as u64 {
        mt.insert(key_for(i), InternalValue::value(i, key_for(i)));
    }
    group.bench_function("get_hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let v = mt.get(black_box(&key_for(i % n as u64)));
            i = i.wrapping_add(1);
            v.is_some()
        });
    });

    group.finish();
}

criterion_group!(benches, bench_bloom, bench_memtable);
criterion_main!(benches);

#![allow(deprecated)]

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lightning_db::{Database, WriteBatch};
use rand::{thread_rng, Rng};
use std::time::Duration;
use tempfile::TempDir;

fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_put");
    group.measurement_time(Duration::from_secs(10));

    for size in &[64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_function(format!("{}B", size), |b| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("bench_db");
            let db = Database::open(db_path, Default::default()).unwrap();
            let value = vec![0u8; *size];
            let mut counter = 0u64;

            b.iter(|| {
                let key = format!("key_{:016}", counter);
                db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                counter += 1;
            });
        });
    }
    group.finish();
}

fn bench_single_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_get");
    group.measurement_time(Duration::from_secs(10));

    for size in &[64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_function(format!("{}B", size), |b| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("bench_db");
            let db = Database::open(db_path, Default::default()).unwrap();

            // Pre-populate database
            let value = vec![0u8; *size];
            for i in 0..1000 {
                let key = format!("key_{:016}", i);
                db.put(key.as_bytes(), &value).unwrap();
            }

            let mut rng = thread_rng();
            b.iter(|| {
                let idx = rng.gen_range(0..1000);
                let key = format!("key_{:016}", idx);
                black_box(db.get(black_box(key.as_bytes())).unwrap());
            });
        });
    }
    group.finish();
}

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_write");
    group.measurement_time(Duration::from_secs(10));

    for batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_function(format!("{}_items", batch_size), |b| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("bench_db");
            let db = Database::open(db_path, Default::default()).unwrap();
            let value = vec![0u8; 256];
            let mut counter = 0u64;

            b.iter(|| {
                let mut batch = WriteBatch::new();
                for _ in 0..*batch_size {
                    let key = format!("key_{:016}", counter);
                    batch.put(key.into_bytes(), value.clone()).unwrap();
                    counter += 1;
                }
                db.write_batch(black_box(&batch)).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");
    group.measurement_time(Duration::from_secs(10));

    for range_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(*range_size as u64));
        group.bench_function(format!("{}_items", range_size), |b| {
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join("bench_db");
            let db = Database::open(db_path, Default::default()).unwrap();

            // Pre-populate database with sorted keys
            let value = vec![0u8; 256];
            for i in 0..10000 {
                let key = format!("key_{:08}", i);
                db.put(key.as_bytes(), &value).unwrap();
            }

            let mut rng = thread_rng();
            b.iter(|| {
                let start_idx = rng.gen_range(0..(10000 - range_size));
                let end_idx = start_idx + range_size;
                let start_key = format!("key_{:08}", start_idx);
                let end_key = format!("key_{:08}", end_idx);

                let iter = db.scan(
                    Some(black_box(start_key.as_bytes())),
                    Some(black_box(end_key.as_bytes()))
                ).unwrap();

                let count = iter.count();
                assert!(count > 0);
            });
        });
    }
    group.finish();
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("4_threads", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_db");
        let db = std::sync::Arc::new(Database::open(db_path, Default::default()).unwrap());

        // Pre-populate database
        let value = vec![0u8; 256];
        for i in 0..1000 {
            let key = format!("key_{:016}", i);
            db.put(key.as_bytes(), &value).unwrap();
        }

        b.iter_batched(
            || db.clone(),
            |db| {
                let handles: Vec<_> = (0..4)
                    .map(|_| {
                        let db_clone = db.clone();
                        std::thread::spawn(move || {
                            let mut rng = thread_rng();
                            for _ in 0..100 {
                                let idx = rng.gen_range(0..1000);
                                let key = format!("key_{:016}", idx);
                                black_box(db_clone.get(key.as_bytes()).unwrap());
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("single_op", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_db");
        let db = Database::open(db_path, Default::default()).unwrap();
        let value = vec![0u8; 256];
        let mut counter = 0u64;

        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            let key = format!("key_{:016}", counter);
            db.put_tx(tx_id, black_box(key.as_bytes()), black_box(&value)).unwrap();
            db.commit_transaction(tx_id).unwrap();
            counter += 1;
        });
    });

    group.bench_function("10_ops", |b| {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("bench_db");
        let db = Database::open(db_path, Default::default()).unwrap();
        let value = vec![0u8; 256];
        let mut counter = 0u64;

        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            for _ in 0..10 {
                let key = format!("key_{:016}", counter);
                db.put_tx(tx_id, key.as_bytes(), &value).unwrap();
                counter += 1;
            }
            db.commit_transaction(tx_id).unwrap();
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_single_put,
    bench_single_get,
    bench_batch_write,
    bench_range_scan,
    bench_concurrent_reads,
    bench_transaction_overhead
);
criterion_main!(benches);
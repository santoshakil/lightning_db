use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lightning_db::{Database, LightningDbConfig};
use rand::{rng, Rng};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn bench_write_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_operations");
    group.measurement_time(Duration::from_secs(10));

    for size in &[100, 1000, 10000] {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_function(BenchmarkId::new("sequential", size), |b| {
            let dir = tempdir().unwrap();
            let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
            let mut i = 0;

            b.iter(|| {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                i += 1;
            });
        });

        group.bench_function(BenchmarkId::new("random", size), |b| {
            let dir = tempdir().unwrap();
            let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
            let mut rng = rng();

            b.iter(|| {
                let key = format!("key_{:08}", rng.random::<u32>());
                let value = vec![0u8; 100];
                db.put(key.as_bytes(), &value).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_read_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_operations");
    group.measurement_time(Duration::from_secs(10));

    for size in &[1000, 10000] {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

        for i in 0..*size {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_function(BenchmarkId::new("sequential", size), |b| {
            let mut i = 0;
            b.iter(|| {
                let key = format!("key_{:08}", i % size);
                black_box(db.get(key.as_bytes()).unwrap());
                i += 1;
            });
        });

        group.bench_function(BenchmarkId::new("random", size), |b| {
            let mut rng = rng();
            b.iter(|| {
                let key = format!("key_{:08}", rng.random::<u32>() % size);
                black_box(db.get(key.as_bytes()).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_range_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_operations");

    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    for i in 0..10000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    for range_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(*range_size as u64));

        group.bench_function(BenchmarkId::new("scan", range_size), |b| {
            let mut start = 0;
            b.iter(|| {
                let start_key = format!("key_{:08}", start);
                let end_key = format!("key_{:08}", start + range_size);
                black_box(
                    db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes()))
                        .unwrap(),
                );
                start = (start + 1) % (10000 - range_size);
            });
        });
    }

    group.finish();
}

fn bench_transaction_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");

    group.bench_function("simple_transaction", |b| {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
        let mut i = 0;

        b.iter(|| {
            let tx = db.begin_transaction().unwrap();
            for j in 0..10 {
                let key = format!("tx_key_{}_{}", i, j);
                let value = format!("tx_value_{}_{}", i, j);
                db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
            }
            db.commit_transaction(tx).unwrap();
            i += 1;
        });
    });

    group.bench_function("read_write_transaction", |b| {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

        for i in 0..100 {
            let key = format!("base_key_{}", i);
            let value = format!("base_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let mut i = 0;
        b.iter(|| {
            let tx = db.begin_transaction().unwrap();

            let read_key = format!("base_key_{}", i % 100);
            black_box(db.get_tx(tx, read_key.as_bytes()).unwrap());

            let write_key = format!("new_key_{}", i);
            let write_value = format!("new_value_{}", i);
            db.put_tx(tx, write_key.as_bytes(), write_value.as_bytes())
                .unwrap();

            db.commit_transaction(tx).unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group.measurement_time(Duration::from_secs(10));

    for num_threads in &[2, 4, 8] {
        group.bench_function(BenchmarkId::new("writes", num_threads), |b| {
            let dir = tempdir().unwrap();
            let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

            b.iter(|| {
                let handles: Vec<_> = (0..*num_threads)
                    .map(|t| {
                        let db_clone = db.clone();
                        std::thread::spawn(move || {
                            for i in 0..100 {
                                let key = format!("t{}_key_{}", t, i);
                                let value = format!("t{}_value_{}", t, i);
                                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    let uncompressed_dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    let db_uncompressed = Database::create(uncompressed_dir.path(), config.clone()).unwrap();

    let compressed_dir = tempdir().unwrap();
    config.compression_enabled = true;
    config.compression_type = 2;
    let db_compressed = Database::create(compressed_dir.path(), config).unwrap();

    let data = vec![42u8; 1000];

    group.bench_function("uncompressed_write", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("key_{}", i);
            db_uncompressed.put(key.as_bytes(), &data).unwrap();
            i += 1;
        });
    });

    group.bench_function("compressed_write", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("key_{}", i);
            db_compressed.put(key.as_bytes(), &data).unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_values");
    group.sample_size(10);

    for size_kb in &[1, 10, 100, 1000] {
        let value_size = size_kb * 1024;
        group.throughput(Throughput::Bytes(value_size as u64));

        group.bench_function(BenchmarkId::new("write", size_kb), |b| {
            let dir = tempdir().unwrap();
            let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
            let value = vec![0u8; value_size];
            let mut i = 0;

            b.iter(|| {
                let key = format!("large_key_{}", i);
                db.put(key.as_bytes(), &value).unwrap();
                i += 1;
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_operations,
    bench_read_operations,
    bench_range_operations,
    bench_transaction_operations,
    bench_concurrent_operations,
    bench_compression,
    bench_large_values
);

criterion_main!(benches);

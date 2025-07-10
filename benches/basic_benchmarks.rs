use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lightning_db::{Database, LightningDbConfig};
use std::hint::black_box;
use std::time::Duration;
use tempfile::tempdir;

fn bench_basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_operations");
    group.measurement_time(Duration::from_secs(10));

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_operations", size),
            size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();

                b.iter(|| {
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        let value = format!("value_{}", i);
                        db.put(black_box(key.as_bytes()), black_box(value.as_bytes()))
                            .unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get_operations", size),
            size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();

                // Pre-populate data
                for i in 0..size {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }

                b.iter(|| {
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        let _result = db.get(black_box(key.as_bytes())).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_transaction_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_operations");
    group.measurement_time(Duration::from_secs(10));

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("transaction_batch", batch_size),
            batch_size,
            |b, &batch_size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();

                b.iter(|| {
                    let tx_id = db.begin_transaction().unwrap();

                    for i in 0..batch_size {
                        let key = format!("tx_key_{}", i);
                        let value = format!("tx_value_{}", i);
                        db.put_tx(
                            black_box(tx_id),
                            black_box(key.as_bytes()),
                            black_box(value.as_bytes()),
                        )
                        .unwrap();
                    }

                    db.commit_transaction(tx_id).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(15));

    let sizes = [1000, 5000, 10000];

    for &size in sizes.iter() {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("mixed_70_read_30_write", size),
            &size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();

                // Pre-populate some data
                for i in 0..size / 2 {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }

                b.iter(|| {
                    for i in 0..size {
                        if i % 10 < 7 {
                            // 70% reads
                            let key = format!("key_{}", i % (size / 2));
                            let _result = db.get(black_box(key.as_bytes())).unwrap();
                        } else {
                            // 30% writes
                            let key = format!("key_{}", i);
                            let value = format!("value_{}", i);
                            db.put(black_box(key.as_bytes()), black_box(value.as_bytes()))
                                .unwrap();
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_different_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_sizes");
    group.measurement_time(Duration::from_secs(10));

    let value_sizes = [64, 1024, 8192, 65536]; // 64B, 1KB, 8KB, 64KB

    for &value_size in value_sizes.iter() {
        group.throughput(Throughput::Bytes(value_size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_variable_size", value_size),
            &value_size,
            |b, &value_size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                let value = vec![0u8; value_size];

                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{}", counter);
                    counter += 1;
                    db.put(black_box(key.as_bytes()), black_box(&value))
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get_variable_size", value_size),
            &value_size,
            |b, &value_size| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                let value = vec![0u8; value_size];

                // Pre-populate
                for i in 0..1000 {
                    let key = format!("key_{}", i);
                    db.put(key.as_bytes(), &value).unwrap();
                }

                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{}", counter % 1000);
                    counter += 1;
                    let _result = db.get(black_box(key.as_bytes())).unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_basic_operations,
    bench_transaction_operations,
    bench_mixed_workload,
    bench_different_value_sizes
);
criterion_main!(benches);

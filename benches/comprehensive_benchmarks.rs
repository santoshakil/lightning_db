use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, BatchSize};
use lightning_db::{Config, Database, TransactionMode};
use tempfile::tempdir;
use rand::{Rng, thread_rng, SeedableRng, rngs::StdRng};
use std::time::Duration;

fn generate_key(idx: usize) -> Vec<u8> {
    format!("benchmark_key_{:010}", idx).into_bytes()
}

fn generate_value(size: usize) -> Vec<u8> {
    vec![0x42; size]
}

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");
    
    for value_size in &[100, 1024, 4096, 16384] {
        group.bench_with_input(
            BenchmarkId::new("value_size", value_size),
            value_size,
            |b, &size| {
                let dir = tempdir().unwrap();
                let config = Config {
                    path: dir.path().to_path_buf(),
                    cache_size: 256 * 1024 * 1024,
                    max_concurrent_transactions: 10,
                    enable_compression: false,
                    compression_level: None,
                    fsync_mode: lightning_db::FsyncMode::Never,
                    page_size: 4096,
                    enable_encryption: false,
                    encryption_key: None,
                };
                
                let db = Database::open(config).unwrap();
                let mut counter = 0;
                
                b.iter(|| {
                    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                    let key = generate_key(counter);
                    let value = generate_value(size);
                    tx.put(&key, &value).unwrap();
                    tx.commit().unwrap();
                    counter += 1;
                });
            }
        );
    }
    
    group.finish();
}

fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads");
    
    for num_keys in &[1000, 10000, 100000] {
        group.bench_with_input(
            BenchmarkId::new("num_keys", num_keys),
            num_keys,
            |b, &num| {
                let dir = tempdir().unwrap();
                let config = Config {
                    path: dir.path().to_path_buf(),
                    cache_size: 256 * 1024 * 1024,
                    max_concurrent_transactions: 10,
                    enable_compression: false,
                    compression_level: None,
                    fsync_mode: lightning_db::FsyncMode::Never,
                    page_size: 4096,
                    enable_encryption: false,
                    encryption_key: None,
                };
                
                let db = Database::open(config).unwrap();
                
                // Pre-populate database
                let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                for i in 0..num {
                    let key = generate_key(i);
                    let value = generate_value(1024);
                    tx.put(&key, &value).unwrap();
                    
                    if i % 1000 == 999 {
                        tx.commit().unwrap();
                        tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                    }
                }
                tx.commit().unwrap();
                
                let mut rng = StdRng::seed_from_u64(42);
                
                b.iter(|| {
                    let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
                    let key_idx = rng.gen_range(0..num);
                    let key = generate_key(key_idx);
                    black_box(tx.get(&key).unwrap());
                });
            }
        );
    }
    
    group.finish();
}

fn bench_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_operations");
    
    for batch_size in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            batch_size,
            |b, &size| {
                let dir = tempdir().unwrap();
                let config = Config {
                    path: dir.path().to_path_buf(),
                    cache_size: 256 * 1024 * 1024,
                    max_concurrent_transactions: 10,
                    enable_compression: false,
                    compression_level: None,
                    fsync_mode: lightning_db::FsyncMode::Never,
                    page_size: 4096,
                    enable_encryption: false,
                    encryption_key: None,
                };
                
                let db = Database::open(config).unwrap();
                let mut counter = 0;
                
                b.iter(|| {
                    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                    for _ in 0..size {
                        let key = generate_key(counter);
                        let value = generate_value(1024);
                        tx.put(&key, &value).unwrap();
                        counter += 1;
                    }
                    tx.commit().unwrap();
                });
            }
        );
    }
    
    group.finish();
}

fn bench_range_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scans");
    
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 256 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    
    // Pre-populate with sorted keys
    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    for i in 0..100000 {
        let key = generate_key(i);
        let value = generate_value(100);
        tx.put(&key, &value).unwrap();
        
        if i % 1000 == 999 {
            tx.commit().unwrap();
            tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        }
    }
    tx.commit().unwrap();
    
    for range_size in &[10, 100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("range_size", range_size),
            range_size,
            |b, &size| {
                let mut rng = StdRng::seed_from_u64(42);
                
                b.iter(|| {
                    let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
                    let start_idx = rng.gen_range(0..100000 - size);
                    let start_key = generate_key(start_idx);
                    let end_key = generate_key(start_idx + size);
                    
                    let mut count = 0;
                    for result in tx.range(&start_key..&end_key) {
                        black_box(result.unwrap());
                        count += 1;
                    }
                    assert!(count > 0);
                });
            }
        );
    }
    
    group.finish();
}

fn bench_concurrent_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_mixed");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    
    for num_threads in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("threads", num_threads),
            num_threads,
            |b, &threads| {
                let dir = tempdir().unwrap();
                let config = Config {
                    path: dir.path().to_path_buf(),
                    cache_size: 512 * 1024 * 1024,
                    max_concurrent_transactions: threads * 2,
                    enable_compression: false,
                    compression_level: None,
                    fsync_mode: lightning_db::FsyncMode::Never,
                    page_size: 4096,
                    enable_encryption: false,
                    encryption_key: None,
                };
                
                let db = std::sync::Arc::new(Database::open(config).unwrap());
                
                // Pre-populate
                let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                for i in 0..10000 {
                    let key = generate_key(i);
                    let value = generate_value(1024);
                    tx.put(&key, &value).unwrap();
                    
                    if i % 1000 == 999 {
                        tx.commit().unwrap();
                        tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                    }
                }
                tx.commit().unwrap();
                
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    
                    let handles: Vec<_> = (0..threads)
                        .map(|thread_id| {
                            let db_clone = std::sync::Arc::clone(&db);
                            std::thread::spawn(move || {
                                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                                
                                for _ in 0..iters / threads {
                                    let op = rng.gen_range(0..100);
                                    
                                    if op < 50 {
                                        // Read
                                        let tx = db_clone.begin_transaction(TransactionMode::ReadOnly).unwrap();
                                        let key_idx = rng.gen_range(0..10000);
                                        let key = generate_key(key_idx);
                                        black_box(tx.get(&key).unwrap());
                                    } else if op < 80 {
                                        // Write
                                        let mut tx = db_clone.begin_transaction(TransactionMode::ReadWrite).unwrap();
                                        let key_idx = rng.gen_range(10000..20000);
                                        let key = generate_key(key_idx);
                                        let value = generate_value(1024);
                                        tx.put(&key, &value).unwrap();
                                        tx.commit().unwrap();
                                    } else {
                                        // Range scan
                                        let tx = db_clone.begin_transaction(TransactionMode::ReadOnly).unwrap();
                                        let start_idx = rng.gen_range(0..9900);
                                        let start_key = generate_key(start_idx);
                                        let end_key = generate_key(start_idx + 100);
                                        
                                        for result in tx.range(&start_key..&end_key).take(10) {
                                            black_box(result.unwrap());
                                        }
                                    }
                                }
                            })
                        })
                        .collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    start.elapsed()
                });
            }
        );
    }
    
    group.finish();
}

fn bench_compression_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    
    for compressed in &[false, true] {
        let label = if *compressed { "with_compression" } else { "without_compression" };
        
        group.bench_with_input(
            BenchmarkId::new("mode", label),
            compressed,
            |b, &use_compression| {
                let dir = tempdir().unwrap();
                let config = Config {
                    path: dir.path().to_path_buf(),
                    cache_size: 256 * 1024 * 1024,
                    max_concurrent_transactions: 10,
                    enable_compression: use_compression,
                    compression_level: if use_compression { Some(3) } else { None },
                    fsync_mode: lightning_db::FsyncMode::Never,
                    page_size: 4096,
                    enable_encryption: false,
                    encryption_key: None,
                };
                
                let db = Database::open(config).unwrap();
                let mut counter = 0;
                
                // Use compressible data
                let value = "Hello World! ".repeat(100).into_bytes();
                
                b.iter(|| {
                    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
                    let key = generate_key(counter);
                    tx.put(&key, &value).unwrap();
                    tx.commit().unwrap();
                    counter += 1;
                });
            }
        );
    }
    
    group.finish();
}

fn bench_transaction_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_overhead");
    
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 256 * 1024 * 1024,
        max_concurrent_transactions: 100,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Never,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Database::open(config).unwrap();
    
    group.bench_function("begin_readonly", |b| {
        b.iter(|| {
            let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
            black_box(tx);
        });
    });
    
    group.bench_function("begin_readwrite", |b| {
        b.iter(|| {
            let tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            black_box(tx);
        });
    });
    
    group.bench_function("commit_empty", |b| {
        b.iter(|| {
            let tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            tx.commit().unwrap();
        });
    });
    
    group.bench_function("rollback", |b| {
        b.iter(|| {
            let tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            drop(tx); // Implicit rollback
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_reads,
    bench_batch_operations,
    bench_range_scans,
    bench_concurrent_mixed_workload,
    bench_compression_impact,
    bench_transaction_overhead
);

criterion_main!(benches);
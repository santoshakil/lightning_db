//! Performance benchmarks for Lightning DB
//! 
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use lightning_db::*;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn create_test_db() -> (Database, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig {
        path: temp_dir.path().to_path_buf(),
        cache_size: 100 * 1024 * 1024, // 100MB cache for benchmarks
        wal_enabled: true,
        compression_enabled: false, // Disable for raw performance
        ..Default::default()
    };
    
    let db = Database::create(temp_dir.path(), config).unwrap();
    (db, temp_dir)
}

fn bench_single_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_writes");
    
    for size in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (db, _dir) = create_test_db();
            let value = vec![0u8; size];
            let mut counter = 0u64;
            
            b.iter(|| {
                let key = format!("key_{}", counter);
                db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                counter += 1;
            });
        });
    }
    
    group.finish();
}

fn bench_batch_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_writes");
    
    for batch_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), batch_size, |b, &batch_size| {
            let (db, _dir) = create_test_db();
            let value = vec![0u8; 100];
            let mut counter = 0u64;
            
            b.iter(|| {
                let mut batch = db.create_batch();
                for _ in 0..batch_size {
                    let key = format!("key_{}", counter);
                    batch.put(key.as_bytes(), &value);
                    counter += 1;
                }
                db.write_batch(black_box(batch)).unwrap();
            });
        });
    }
    
    group.finish();
}

fn bench_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("reads");
    
    // Setup: Populate database
    let (db, _dir) = create_test_db();
    let num_keys = 100000;
    let value = vec![0u8; 100];
    
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Benchmark random reads
    group.bench_function("random", |b| {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        b.iter(|| {
            let idx = rng.gen_range(0..num_keys);
            let key = format!("key_{:08}", idx);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
        });
    });
    
    // Benchmark sequential reads
    group.bench_function("sequential", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{:08}", counter % num_keys);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
            counter += 1;
        });
    });
    
    // Benchmark hot key reads (cache hits)
    group.bench_function("cache_hits", |b| {
        let hot_keys: Vec<String> = (0..100).map(|i| format!("key_{:08}", i)).collect();
        let mut idx = 0;
        
        b.iter(|| {
            let key = &hot_keys[idx % hot_keys.len()];
            black_box(db.get(black_box(key.as_bytes())).unwrap());
            idx += 1;
        });
    });
    
    group.finish();
}

fn bench_range_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scans");
    
    // Setup
    let (db, _dir) = create_test_db();
    let value = vec![0u8; 100];
    
    for i in 0..10000 {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    for range_size in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(*range_size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(range_size), range_size, |b, &range_size| {
            let mut start_idx = 0;
            
            b.iter(|| {
                let start_key = format!("key_{:08}", start_idx);
                let end_key = format!("key_{:08}", start_idx + range_size);
                
                let range = db.range(black_box(start_key.as_bytes()..end_key.as_bytes()));
                let count = range.count();
                black_box(count);
                
                start_idx = (start_idx + 1) % (10000 - range_size);
            });
        });
    }
    
    group.finish();
}

fn bench_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactions");
    
    // Small transactions
    group.bench_function("small_txn", |b| {
        let (db, _dir) = create_test_db();
        let value = vec![0u8; 100];
        let mut counter = 0u64;
        
        b.iter(|| {
            let tx = db.begin_transaction().unwrap();
            
            for i in 0..10 {
                let key = format!("tx_key_{}_{}", counter, i);
                db.put_tx(tx, key.as_bytes(), &value).unwrap();
            }
            
            db.commit_transaction(black_box(tx)).unwrap();
            counter += 1;
        });
    });
    
    // Large transactions
    group.bench_function("large_txn", |b| {
        let (db, _dir) = create_test_db();
        let value = vec![0u8; 100];
        let mut counter = 0u64;
        
        b.iter(|| {
            let tx = db.begin_transaction().unwrap();
            
            for i in 0..1000 {
                let key = format!("tx_key_{}_{}", counter, i);
                db.put_tx(tx, key.as_bytes(), &value).unwrap();
            }
            
            db.commit_transaction(black_box(tx)).unwrap();
            counter += 1;
        });
    });
    
    // Read-only transactions
    group.bench_function("readonly_txn", |b| {
        let (db, _dir) = create_test_db();
        
        // Populate data
        for i in 0..1000 {
            let key = format!("ro_key_{}", i);
            db.put(key.as_bytes(), b"value").unwrap();
        }
        
        let mut counter = 0;
        
        b.iter(|| {
            let tx = db.begin_transaction().unwrap();
            
            for i in 0..100 {
                let key = format!("ro_key_{}", (counter + i) % 1000);
                black_box(db.get_tx(tx, key.as_bytes()).unwrap());
            }
            
            db.commit_transaction(black_box(tx)).unwrap();
            counter += 1;
        });
    });
    
    group.finish();
}

fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    
    group.bench_function("compaction_speed", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            
            for _ in 0..iters {
                let (db, _dir) = create_test_db();
                
                // Create fragmented data
                for i in 0..10000 {
                    let key = format!("compact_key_{}", i);
                    db.put(key.as_bytes(), vec![0u8; 1000].as_slice()).unwrap();
                }
                
                // Delete half to create fragmentation
                for i in (0..10000).step_by(2) {
                    let key = format!("compact_key_{}", i);
                    db.delete(key.as_bytes()).unwrap();
                }
                
                // Measure compaction time
                let start = std::time::Instant::now();
                db.compact().unwrap();
                total_duration += start.elapsed();
            }
            
            total_duration
        });
    });
    
    group.finish();
}

fn bench_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent");
    group.measurement_time(Duration::from_secs(10));
    
    for num_threads in &[1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            num_threads,
            |b, &num_threads| {
                let (db, _dir) = create_test_db();
                let db = Arc::new(db);
                
                // Populate initial data
                for i in 0..10000 {
                    let key = format!("concurrent_{}", i);
                    db.put(key.as_bytes(), b"value").unwrap();
                }
                
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    
                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db = db.clone();
                        let ops_per_thread = iters / num_threads as u64;
                        
                        std::thread::spawn(move || {
                            let mut rng = ChaCha8Rng::seed_from_u64(thread_id as u64);
                            
                            for i in 0..ops_per_thread {
                                let op = rng.gen_range(0..10);
                                let key_idx = rng.gen_range(0..10000);
                                let key = format!("concurrent_{}", key_idx);
                                
                                if op < 7 {
                                    // 70% reads
                                    black_box(db.get(key.as_bytes()).unwrap());
                                } else {
                                    // 30% writes
                                    db.put(key.as_bytes(), format!("v_{}", i).as_bytes()).unwrap();
                                }
                            }
                        })
                    }).collect();
                    
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

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    
    // Benchmark with compression enabled
    group.bench_function("compressed_writes", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            compression_enabled: true,
            compression_type: CompressionType::Zstd,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Highly compressible data
        let value = vec![b'A'; 10000];
        let mut counter = 0u64;
        
        b.iter(|| {
            let key = format!("compressed_{}", counter);
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            counter += 1;
        });
    });
    
    // Benchmark without compression
    group.bench_function("uncompressed_writes", |b| {
        let (db, _dir) = create_test_db();
        let value = vec![b'A'; 10000];
        let mut counter = 0u64;
        
        b.iter(|| {
            let key = format!("uncompressed_{}", counter);
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            counter += 1;
        });
    });
    
    group.finish();
}

fn bench_cache_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache");
    
    // Setup database with limited cache
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig {
        path: temp_dir.path().to_path_buf(),
        cache_size: 1 * 1024 * 1024, // Small 1MB cache
        ..Default::default()
    };
    let db = Database::create(temp_dir.path(), config).unwrap();
    
    // Populate with more data than cache can hold
    let num_keys = 10000;
    let value = vec![0u8; 1000]; // 1KB values = 10MB total
    
    for i in 0..num_keys {
        let key = format!("cache_key_{:08}", i);
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Benchmark cache misses (random access pattern)
    group.bench_function("cache_misses", |b| {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        
        b.iter(|| {
            let idx = rng.gen_range(0..num_keys);
            let key = format!("cache_key_{:08}", idx);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
        });
    });
    
    // Benchmark cache locality (sequential access)
    group.bench_function("cache_locality", |b| {
        let mut counter = 0;
        
        b.iter(|| {
            let key = format!("cache_key_{:08}", counter % num_keys);
            black_box(db.get(black_box(key.as_bytes())).unwrap());
            counter += 1;
        });
    });
    
    group.finish();
}

fn bench_wal_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    
    // WAL enabled (default)
    group.bench_function("wal_enabled", |b| {
        let (db, _dir) = create_test_db();
        let value = vec![0u8; 100];
        let mut counter = 0u64;
        
        b.iter(|| {
            let key = format!("wal_key_{}", counter);
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            counter += 1;
        });
    });
    
    // WAL disabled
    group.bench_function("wal_disabled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            wal_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        let value = vec![0u8; 100];
        let mut counter = 0u64;
        
        b.iter(|| {
            let key = format!("no_wal_key_{}", counter);
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            counter += 1;
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_single_writes,
    bench_batch_writes,
    bench_reads,
    bench_range_scans,
    bench_transactions,
    bench_compaction,
    bench_concurrent_access,
    bench_compression,
    bench_cache_performance,
    bench_wal_performance
);

criterion_main!(benches);
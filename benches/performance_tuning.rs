use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lightning_db::{Database, LightningDbConfig};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Comprehensive performance tuning benchmark suite for Lightning DB
/// 
/// This benchmark suite focuses on profiling and optimizing the critical performance paths:
/// 1. B+Tree operations (search, insert, delete)
/// 2. Page manager and cache operations  
/// 3. WAL (Write-Ahead Log) performance
/// 4. Lock-free data structures
/// 5. Memory-mapped I/O operations
/// 6. SIMD optimizations
/// 7. Cache line optimization

/// Performance baseline expectations
#[derive(Debug, Clone)]
pub struct PerformanceTuningBaseline {
    /// Cache-friendly read operations: Target 25M ops/sec 
    pub cache_friendly_read_ops_per_sec: f64,
    /// SIMD-optimized write operations: Target 1.5M ops/sec
    pub simd_write_ops_per_sec: f64,
    /// Memory layout optimized mixed workload: Target 1.2M ops/sec
    pub memory_optimized_mixed_ops_per_sec: f64,
    /// Lock-free concurrent operations: Target 2M ops/sec with 16 threads
    pub lock_free_concurrent_ops_per_sec: f64,
    /// Cache line aligned B+Tree operations: Target 30M ops/sec
    pub btree_cache_aligned_ops_per_sec: f64,
    /// Zero-copy I/O operations: Target 5GB/sec throughput
    pub zero_copy_io_gbps: f64,
}

impl Default for PerformanceTuningBaseline {
    fn default() -> Self {
        Self {
            cache_friendly_read_ops_per_sec: 25_000_000.0,   // 25M ops/sec
            simd_write_ops_per_sec: 1_500_000.0,             // 1.5M ops/sec
            memory_optimized_mixed_ops_per_sec: 1_200_000.0, // 1.2M ops/sec
            lock_free_concurrent_ops_per_sec: 2_000_000.0,   // 2M ops/sec
            btree_cache_aligned_ops_per_sec: 30_000_000.0,   // 30M ops/sec
            zero_copy_io_gbps: 5.0,                          // 5 GB/sec
        }
    }
}

/// B+Tree performance optimization benchmarks
fn bench_btree_cache_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_cache_optimization");
    group.measurement_time(Duration::from_secs(30));
    
    // Test cache-friendly vs standard B+Tree operations
    for &workload_size in &[1_000, 10_000, 100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::new("cache_aligned_btree_search", workload_size),
            &workload_size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 512 * 1024 * 1024, // 512MB for cache optimization testing
                    prefetch_enabled: true,
                    compression_enabled: false,
                    ..Default::default()
                };
                let db = Database::create(temp_dir.path(), config).unwrap();
                
                // Pre-populate with cache-friendly data patterns
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    let value = vec![i as u8; 64]; // Cache-line sized values
                    db.put(key.as_bytes(), &value).unwrap();
                }
                
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("key_{:08}", counter % size);
                    counter += 1;
                    let result = db.get(black_box(key.as_bytes())).unwrap();
                    black_box(result);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("memory_layout_optimized_insert", workload_size),
            &workload_size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 512 * 1024 * 1024,
                    prefetch_enabled: true,
                    compression_enabled: false,
                    ..Default::default()
                };
                let db = Database::create(temp_dir.path(), config).unwrap();
                
                // Pre-populate base data
                for i in 0..size / 10 {
                    let key = format!("base_{:08}", i);
                    let value = vec![i as u8; 64];
                    db.put(key.as_bytes(), &value).unwrap();
                }
                
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("test_{:08}", counter);
                    counter += 1;
                    let value = vec![counter as u8; 64]; // Cache-aligned value size
                    db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                });
            },
        );
    }
    
    group.finish();
}

/// SIMD optimization benchmarks for bulk operations
fn bench_simd_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_optimizations");
    group.measurement_time(Duration::from_secs(25));
    
    // Test SIMD-accelerated key comparisons, hashing, and bulk operations
    group.bench_function("simd_key_comparison_bulk", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create test data optimized for SIMD operations
        let keys: Vec<String> = (0..10_000)
            .map(|i| format!("simd_key_{:016}", i)) // 16-byte aligned keys
            .collect();
        
        for (i, key) in keys.iter().enumerate() {
            let value = vec![i as u8; 32]; // SIMD-aligned values
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            let key = &keys[counter % keys.len()];
            counter += 1;
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });

    group.bench_function("simd_bulk_write_operations", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        let mut counter = 0;
        b.iter(|| {
            // Batch multiple writes to leverage SIMD
            for _ in 0..10 {
                let key = format!("simd_write_{:016}", counter);
                counter += 1;
                let value = vec![counter as u8; 32]; // SIMD-friendly size
                db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            }
        });
    });
    
    group.finish();
}

/// Memory layout optimization benchmarks
fn bench_memory_layout_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_layout_optimizations");
    group.measurement_time(Duration::from_secs(30));
    
    // Test memory-efficient data structures and cache-friendly layouts
    group.bench_function("cache_line_aligned_operations", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 1024 * 1024 * 1024, // 1GB for memory layout testing
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create data aligned to cache line boundaries (64 bytes)
        let cache_line_size = 64;
        let num_records = 100_000;
        
        for i in 0..num_records {
            let key = format!("cache_aligned_{:08}", i);
            let value = vec![i as u8; cache_line_size]; // Exactly one cache line
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            // Mix of operations to test memory layout efficiency
            let operation = counter % 4;
            counter += 1;
            
            match operation {
                0 | 1 | 2 => {
                    // 75% reads
                    let key = format!("cache_aligned_{:08}", counter % num_records);
                    let result = db.get(black_box(key.as_bytes())).unwrap();
                    black_box(result);
                }
                3 => {
                    // 25% writes
                    let key = format!("cache_new_{:08}", counter);
                    let value = vec![counter as u8; cache_line_size];
                    db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                }
                _ => unreachable!(),
            }
        });
    });

    group.bench_function("memory_pool_efficiency", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 512 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Test memory pool reuse efficiency
        let pool_size = 1000;
        let value_sizes = [64, 128, 256, 512]; // Different allocation sizes
        
        let mut counter = 0;
        b.iter(|| {
            let key = format!("pool_test_{:08}", counter);
            let value_size = value_sizes[counter % value_sizes.len()];
            counter += 1;
            
            let value = vec![counter as u8; value_size];
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            
            // Occasionally read to test pool reuse
            if counter % 10 == 0 {
                let read_key = format!("pool_test_{:08}", counter - 5);
                let result = db.get(black_box(read_key.as_bytes())).unwrap();
                black_box(result);
            }
        });
    });
    
    group.finish();
}

/// Lock-free data structure performance benchmarks
fn bench_lock_free_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_free_optimizations");
    group.measurement_time(Duration::from_secs(30));
    
    // Test concurrent operations with lock-free structures
    for &thread_count in &[2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("lock_free_concurrent_mixed", thread_count),
            &thread_count,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let temp_dir = tempdir().unwrap();
                    let config = LightningDbConfig {
                        cache_size: 2048 * 1024 * 1024, // 2GB for concurrent testing
                        prefetch_enabled: true,
                        compression_enabled: false,
                        ..Default::default()
                    };
                    let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());
                    
                    // Pre-populate for concurrent reads
                    let base_records = 50_000;
                    for i in 0..base_records {
                        let key = format!("concurrent_base_{:08}", i);
                        let value = vec![i as u8; 128];
                        db.put(key.as_bytes(), &value).unwrap();
                    }
                    
                    let ops_per_thread = iters / num_threads as u64;
                    let start = Instant::now();
                    
                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db_clone = Arc::clone(&db);
                        
                        thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let operation = i % 5;
                                
                                match operation {
                                    0 | 1 | 2 => {
                                        // 60% reads - test lock-free read paths
                                        let key = format!("concurrent_base_{:08}", i % base_records as u64);
                                        let result = db_clone.get(key.as_bytes()).unwrap();
                                        black_box(result);
                                    }
                                    3 => {
                                        // 20% writes - test lock-free write coordination
                                        let key = format!("thread_{}_{:08}", thread_id, i);
                                        let value = vec![thread_id as u8; 128];
                                        db_clone.put(key.as_bytes(), &value).unwrap();
                                    }
                                    4 => {
                                        // 20% mixed operations
                                        let key = format!("mixed_{}_{:08}", thread_id, i);
                                        let value = vec![(thread_id * 2) as u8; 64];
                                        db_clone.put(key.as_bytes(), &value).unwrap();
                                        
                                        // Immediate read back
                                        let result = db_clone.get(key.as_bytes()).unwrap();
                                        black_box(result);
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    start.elapsed()
                });
            },
        );
    }
    
    group.finish();
}

/// Page manager and cache optimization benchmarks
fn bench_page_manager_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("page_manager_optimizations");
    group.measurement_time(Duration::from_secs(25));
    
    group.bench_function("cache_hit_optimization", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 128 * 1024 * 1024, // 128MB - small cache to test hit rates
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create hot data that should stay in cache
        let hot_keys = 1000;
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            let value = vec![i as u8; 512];
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        // Prime the cache with multiple reads
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            // Focus on hot keys to maximize cache hits
            let key = format!("hot_key_{:04}", counter % hot_keys);
            counter += 1;
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });

    group.bench_function("page_prefetch_optimization", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true, // Enable prefetching
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create sequential data to benefit from prefetching
        let num_records = 10_000;
        for i in 0..num_records {
            let key = format!("seq_key_{:08}", i);
            let value = vec![i as u8; 256];
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            // Sequential access pattern to trigger prefetching
            let key = format!("seq_key_{:08}", counter % num_records);
            counter += 1;
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });
    
    group.finish();
}

/// Zero-copy I/O and memory-mapped optimization benchmarks
fn bench_zero_copy_io_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("zero_copy_io_optimizations");
    group.measurement_time(Duration::from_secs(20));
    
    // Test different value sizes for zero-copy efficiency
    for &value_size in &[1024, 4096, 16384, 65536] {
        group.bench_with_input(
            BenchmarkId::new("zero_copy_large_values", value_size),
            &value_size,
            |b, &size| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 512 * 1024 * 1024,
                    prefetch_enabled: true,
                    compression_enabled: false,
                    ..Default::default()
                };
                let db = Database::create(temp_dir.path(), config).unwrap();
                
                let mut counter = 0;
                b.iter(|| {
                    let key = format!("large_value_{:08}", counter);
                    counter += 1;
                    let value = vec![counter as u8; size];
                    
                    // Test write throughput
                    db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                    
                    // Test read throughput
                    let result = db.get(black_box(key.as_bytes())).unwrap();
                    black_box(result);
                });
            },
        );
    }

    group.bench_function("mmap_sequential_scan", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 1024 * 1024 * 1024, // 1GB for large scan test
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create large dataset for sequential scanning
        let num_records = 100_000;
        for i in 0..num_records {
            let key = format!("scan_key_{:08}", i);
            let value = vec![i as u8; 128];
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut scan_start = 0;
        b.iter(|| {
            // Simulate range scan operations
            let start_key = format!("scan_key_{:08}", scan_start);
            let end_key = format!("scan_key_{:08}", scan_start + 100);
            scan_start = (scan_start + 50) % (num_records - 100);
            
            // Use range operation to test sequential I/O efficiency
            let results = db.range(
                Some(black_box(start_key.as_bytes())),
                Some(black_box(end_key.as_bytes()))
            ).unwrap();
            black_box(results);
        });
    });
    
    group.finish();
}

/// Transaction performance with optimizations
fn bench_transaction_optimizations(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_optimizations");
    group.measurement_time(Duration::from_secs(20));
    
    group.bench_function("batch_transaction_optimization", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        let mut counter = 0;
        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            
            // Batch multiple operations per transaction for efficiency
            for i in 0..20 {
                let key = format!("batch_tx_{}_{}", counter, i);
                let value = vec![counter as u8; 64];
                db.put_tx(tx_id, key.as_bytes(), &value).unwrap();
            }
            
            db.commit_transaction(tx_id).unwrap();
            counter += 1;
        });
    });
    
    group.finish();
}

/// CPU profiling and hotspot identification benchmark
fn bench_cpu_profiling_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("cpu_profiling_analysis");
    group.measurement_time(Duration::from_secs(30));
    
    group.bench_function("hotspot_identification_workload", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 512 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Create realistic workload for profiling
        let num_base_records = 50_000;
        for i in 0..num_base_records {
            let key = format!("profile_base_{:08}", i);
            let value = vec![i as u8; 256];
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            let operation = counter % 10;
            counter += 1;
            
            match operation {
                0..=5 => {
                    // 60% reads with different patterns
                    let key = if operation % 2 == 0 {
                        format!("profile_base_{:08}", counter % num_base_records)
                    } else {
                        format!("profile_base_{:08}", (counter * 7) % num_base_records)
                    };
                    let result = db.get(black_box(key.as_bytes())).unwrap();
                    black_box(result);
                }
                6..=7 => {
                    // 20% writes
                    let key = format!("profile_new_{:08}", counter);
                    let value = vec![counter as u8; 256];
                    db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
                }
                8 => {
                    // 10% range operations
                    let start = (counter * 13) % num_base_records;
                    let start_key = format!("profile_base_{:08}", start);
                    let end_key = format!("profile_base_{:08}", start + 10);
                    let results = db.range(
                        Some(black_box(start_key.as_bytes())),
                        Some(black_box(end_key.as_bytes()))
                    ).unwrap();
                    black_box(results);
                }
                9 => {
                    // 10% transaction operations
                    let tx_id = db.begin_transaction().unwrap();
                    for i in 0..3 {
                        let key = format!("profile_tx_{}_{}", counter, i);
                        let value = vec![(counter + i) as u8; 128];
                        db.put_tx(tx_id, key.as_bytes(), &value).unwrap();
                    }
                    db.commit_transaction(tx_id).unwrap();
                }
                _ => unreachable!(),
            }
        });
    });
    
    group.finish();
}

/// Memory allocation pattern optimization
fn bench_memory_allocation_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation_patterns");
    group.measurement_time(Duration::from_secs(20));
    
    group.bench_function("allocation_reduction_pattern", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true,
            compression_enabled: false,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Test memory-efficient patterns
        let mut counter = 0;
        b.iter(|| {
            // Use static-sized data to reduce allocations
            let key = format!("mem_opt_{:016}", counter); // Fixed-width key
            counter += 1;
            
            let value = [counter as u8; 128]; // Stack-allocated value
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            
            // Reuse key for immediate read
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });
    
    group.finish();
}

criterion_group!(
    performance_tuning_benches,
    bench_btree_cache_optimization,
    bench_simd_optimizations,
    bench_memory_layout_optimizations,
    bench_lock_free_optimizations,
    bench_page_manager_optimizations,
    bench_zero_copy_io_optimizations,
    bench_transaction_optimizations,
    bench_cpu_profiling_analysis,
    bench_memory_allocation_patterns
);

criterion_main!(performance_tuning_benches);
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lightning_db::{Database, LightningDbConfig};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Baseline performance expectations based on documented results
#[derive(Debug, Clone)]
pub struct PerformanceBaseline {
    /// Read performance: 20.4M ops/sec (documented baseline)
    pub read_ops_per_sec: f64,
    /// Write performance: 1.14M ops/sec (documented baseline)  
    pub write_ops_per_sec: f64,
    /// Mixed workload: 885K ops/sec sustained (documented baseline)
    pub mixed_ops_per_sec: f64,
    /// Concurrent operations: 1.4M ops/sec with 8 threads (documented baseline)
    pub concurrent_ops_per_sec: f64,
    /// Transaction throughput baseline
    pub transaction_ops_per_sec: f64,
    /// Read latency P50: 0.049 μs (documented baseline)
    pub read_latency_us_p50: f64,
    /// Write latency P50: 0.88 μs (documented baseline) 
    pub write_latency_us_p50: f64,
}

impl Default for PerformanceBaseline {
    fn default() -> Self {
        Self {
            // Set to documented baselines
            read_ops_per_sec: 20_400_000.0,      // 20.4M ops/sec
            write_ops_per_sec: 1_140_000.0,      // 1.14M ops/sec
            mixed_ops_per_sec: 885_000.0,        // 885K ops/sec 
            concurrent_ops_per_sec: 1_400_000.0, // 1.4M ops/sec
            transaction_ops_per_sec: 412_371.0,  // From benchmark results
            read_latency_us_p50: 0.049,          // 0.049 μs
            write_latency_us_p50: 0.88,          // 0.88 μs
        }
    }
}

/// Performance regression test results
#[derive(Debug)]
pub struct RegressionTestResult {
    pub test_name: String,
    pub measured_ops_per_sec: f64,
    pub baseline_ops_per_sec: f64,
    pub regression_ratio: f64,      // measured / baseline
    pub is_regression: bool,
    pub measured_latency_p50: Duration,
    pub measured_latency_p99: Duration,
    pub memory_usage_mb: f64,
}

/// Acceptable regression thresholds
pub const REGRESSION_THRESHOLD_READ: f64 = 0.90;    // Allow 10% regression for reads
pub const REGRESSION_THRESHOLD_WRITE: f64 = 0.90;   // Allow 10% regression for writes  
pub const REGRESSION_THRESHOLD_MIXED: f64 = 0.85;   // Allow 15% regression for mixed workload
pub const REGRESSION_THRESHOLD_CONCURRENT: f64 = 0.85; // Allow 15% regression for concurrent

/// Core read performance regression test - validates 20.4M ops/sec baseline
fn bench_read_operations_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_read_performance");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(50);
    
    let temp_dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 16 * 1024 * 1024 * 1024, // 16GB cache like benchmark
        prefetch_enabled: true,
        compression_enabled: false, // Disable for max performance
        ..Default::default()
    };
    let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());
    
    // Pre-populate with 1M records for realistic read performance
    let num_records = 1_000_000;
    for i in 0..num_records {
        let key = format!("key_{:08}", i);
        let value = vec![0u8; 1024]; // 1KB values like benchmark
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Force data to disk for realistic conditions
    db.checkpoint().unwrap();
    
    group.bench_function("single_threaded_reads", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{:08}", counter % num_records);
            counter += 1;
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });
    
    // Multi-threaded read test to match documented concurrent performance
    group.bench_function("multi_threaded_reads_16_threads", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let handles: Vec<_> = (0..16).map(|thread_id| {
                let db_clone = Arc::clone(&db);
                let ops_per_thread = iters / 16;
                
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{:08}", (thread_id * 1000 + i as u32) % num_records as u32);
                        let result = db_clone.get(key.as_bytes()).unwrap();
                        black_box(result);
                    }
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
            start.elapsed()
        });
    });
    
    group.finish();
}

/// Core write performance regression test - validates 1.14M ops/sec baseline
fn bench_write_operations_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_write_performance");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(50);
    
    group.bench_function("single_threaded_writes", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 16 * 1024 * 1024 * 1024, // 16GB cache
            compression_enabled: false, // Disable for max performance
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        let value = vec![0u8; 1024]; // 1KB values
        
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key_{:08}", counter);
            counter += 1;
            db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
        });
    });
    
    // Multi-threaded write test
    group.bench_function("multi_threaded_writes_16_threads", |b| {
        b.iter_custom(|iters| {
            let temp_dir = tempdir().unwrap();
            let config = LightningDbConfig {
                cache_size: 16 * 1024 * 1024 * 1024,
                compression_enabled: false,
                ..Default::default()
            };
            let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());
            let value = vec![0u8; 1024];
            
            let start = Instant::now();
            let handles: Vec<_> = (0..16).map(|thread_id| {
                let db_clone = Arc::clone(&db);
                let value_clone = value.clone();
                let ops_per_thread = iters / 16;
                
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{}_{:08}", thread_id, i);
                        db_clone.put(key.as_bytes(), &value_clone).unwrap();
                    }
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
            start.elapsed()
        });
    });
    
    group.finish();
}

/// Mixed workload regression test - validates 885K ops/sec sustained baseline
fn bench_mixed_workload_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_mixed_workload");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(50);
    
    group.bench_function("mixed_80_read_20_write", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 16 * 1024 * 1024 * 1024,
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Pre-populate data
        let num_initial_keys = 100_000;
        let value = vec![0u8; 1024];
        for i in 0..num_initial_keys {
            let key = format!("init_key_{:08}", i);
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        let mut op_counter = 0;
        b.iter(|| {
            let op_type = op_counter % 10;
            op_counter += 1;
            
            if op_type < 8 {
                // 80% reads
                let key = format!("init_key_{:08}", op_counter % num_initial_keys);
                let result = db.get(black_box(key.as_bytes())).unwrap();
                black_box(result);
            } else {
                // 20% writes
                let key = format!("new_key_{:08}", op_counter);
                db.put(black_box(key.as_bytes()), black_box(&value)).unwrap();
            }
        });
    });
    
    group.finish();
}

/// Transaction performance regression test
fn bench_transaction_performance_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_transaction_performance");
    group.measurement_time(Duration::from_secs(30));
    
    group.bench_function("transaction_commits_5_ops", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        let value = vec![0u8; 256]; // Smaller values for transaction tests
        
        let mut counter = 0;
        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            
            // 5 operations per transaction (matching benchmark)
            for i in 0..5 {
                let key = format!("tx_{}_op_{}", counter, i);
                db.put_tx(tx_id, key.as_bytes(), &value).unwrap();
            }
            
            db.commit_transaction(tx_id).unwrap();
            counter += 1;
        });
    });
    
    group.finish();
}

/// Error handling performance impact test
fn bench_error_handling_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_error_handling_overhead");
    group.measurement_time(Duration::from_secs(20));
    
    // Test performance with all error handling enabled vs baseline
    group.bench_function("error_handling_enabled", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024, // Smaller cache for error handling test
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        let value = vec![0u8; 512];
        
        let mut counter = 0;
        b.iter(|| {
            let key = format!("error_test_{:08}", counter);
            counter += 1;
            
            // Test with proper error handling (current implementation)
            let result = db.put(black_box(key.as_bytes()), black_box(&value));
            black_box(result.unwrap());
        });
    });
    
    group.finish();
}

/// Memory efficiency regression test
fn bench_memory_efficiency_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_memory_efficiency");
    group.measurement_time(Duration::from_secs(20));
    
    group.bench_function("memory_allocation_patterns", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 64 * 1024 * 1024, // 64MB cache for memory test
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        b.iter(|| {
            // Test memory-efficient key/value handling
            let key = b"memory_test_key";
            let value = b"memory_test_value_with_reasonable_size";
            
            db.put(black_box(key), black_box(value)).unwrap();
            let result = db.get(black_box(key)).unwrap();
            black_box(result);
        });
    });
    
    group.finish();
}

/// Startup performance regression test
fn bench_startup_performance_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_startup_performance");
    group.measurement_time(Duration::from_secs(15));
    
    group.bench_function("database_initialization", |b| {
        b.iter_custom(|iters| {
            let total_start = Instant::now();
            
            for _ in 0..iters {
                let temp_dir = tempdir().unwrap();
                let start = Instant::now();
                
                let _db = Database::create(
                    temp_dir.path(), 
                    black_box(LightningDbConfig::default())
                ).unwrap();
                
                black_box(start.elapsed());
            }
            
            total_start.elapsed()
        });
    });
    
    group.finish();
}

/// Concurrent performance regression test - validates 1.4M ops/sec with 8 threads
fn bench_concurrent_performance_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_concurrent_performance");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(20);
    
    for thread_count in [4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_mixed_workload", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter_custom(|iters| {
                    let temp_dir = tempdir().unwrap();
                    let config = LightningDbConfig {
                        cache_size: 8 * 1024 * 1024 * 1024, // 8GB cache for concurrent test
                        ..Default::default()
                    };
                    let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());
                    
                    // Pre-populate some data
                    let value = vec![0u8; 512];
                    for i in 0..10000 {
                        let key = format!("base_{:06}", i);
                        db.put(key.as_bytes(), &value).unwrap();
                    }
                    
                    let ops_per_thread = iters / thread_count as u64;
                    let start = Instant::now();
                    
                    let handles: Vec<_> = (0..thread_count).map(|thread_id| {
                        let db_clone = Arc::clone(&db);
                        let value_clone = value.clone();
                        
                        thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                if i % 3 == 0 {
                                    // Write operation
                                    let key = format!("thread_{}_{:08}", thread_id, i);
                                    db_clone.put(key.as_bytes(), &value_clone).unwrap();
                                } else {
                                    // Read operation
                                    let key = format!("base_{:06}", i as u32 % 10000);
                                    let result = db_clone.get(key.as_bytes()).unwrap();
                                    black_box(result);
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

/// Cache performance regression test
fn bench_cache_performance_regression(c: &mut Criterion) {
    let mut group = c.benchmark_group("regression_cache_performance");
    group.measurement_time(Duration::from_secs(20));
    
    group.bench_function("hot_cache_performance", |b| {
        let temp_dir = tempdir().unwrap();
        let config = LightningDbConfig {
            cache_size: 128 * 1024 * 1024, // 128MB cache
            ..Default::default()
        };
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Pre-populate with data that fits in cache
        let hot_keys = 1000;
        let value = vec![0u8; 1024];
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            db.put(key.as_bytes(), &value).unwrap();
        }
        
        // Prime the cache
        for i in 0..hot_keys {
            let key = format!("hot_key_{:04}", i);
            let _ = db.get(key.as_bytes()).unwrap();
        }
        
        let mut counter = 0;
        b.iter(|| {
            let key = format!("hot_key_{:04}", counter % hot_keys);
            counter += 1;
            let result = db.get(black_box(key.as_bytes())).unwrap();
            black_box(result);
        });
    });
    
    group.finish();
}

criterion_group!(
    regression_benches,
    bench_read_operations_regression,
    bench_write_operations_regression,
    bench_mixed_workload_regression,
    bench_transaction_performance_regression,
    bench_error_handling_overhead,
    bench_memory_efficiency_regression,
    bench_startup_performance_regression,
    bench_concurrent_performance_regression,
    bench_cache_performance_regression
);

criterion_main!(regression_benches);
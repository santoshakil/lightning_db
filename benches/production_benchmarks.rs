use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::{Rng, RngCore};
use rayon::prelude::*;

/// Performance targets for validation
#[derive(Debug, Clone)]
struct PerformanceTargets {
    read_ops_per_sec: f64,      // 14M+ target
    write_ops_per_sec: f64,     // 350K+ target
    latency_p99_us: f64,        // <1ms target
    memory_usage_gb: f64,       // <1GB for 10GB DB
    cpu_utilization: f64,       // <80% at peak
}

impl Default for PerformanceTargets {
    fn default() -> Self {
        Self {
            read_ops_per_sec: 14_000_000.0,
            write_ops_per_sec: 350_000.0,
            latency_p99_us: 1000.0,
            memory_usage_gb: 1.0,
            cpu_utilization: 80.0,
        }
    }
}

/// Latency measurement utilities
struct LatencyMeasurement {
    measurements: Vec<Duration>,
}

impl LatencyMeasurement {
    fn new() -> Self {
        Self {
            measurements: Vec::new(),
        }
    }

    fn record(&mut self, latency: Duration) {
        self.measurements.push(latency);
    }

    fn percentile(&mut self, p: f64) -> Duration {
        self.measurements.sort_unstable();
        let idx = ((self.measurements.len() as f64 * p / 100.0) as usize).min(self.measurements.len() - 1);
        self.measurements[idx]
    }

    fn mean(&self) -> Duration {
        let sum: Duration = self.measurements.iter().sum();
        sum / self.measurements.len() as u32
    }

    fn jitter(&mut self) -> Duration {
        self.measurements.sort_unstable();
        let p99 = self.percentile(99.0);
        let p95 = self.percentile(95.0);
        p99 - p95
    }
}

/// Core performance benchmarks
fn benchmark_sequential_reads_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/sequential_reads_single_thread");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("sequential_read", size), size, |b, &size| {
            let temp_dir = tempdir().unwrap();
            let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
            
            // Populate with sequential data
            for i in 0..size {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                for i in 0..size {
                    let key = format!("key_{:08}", i);
                    let result = db.get(key.as_bytes()).unwrap();
                    black_box(result);
                }
            });
        });
    }
    group.finish();
}

fn benchmark_sequential_reads_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/sequential_reads_multi_thread");
    
    for threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("parallel_read", threads), threads, |b, &threads| {
            let temp_dir = tempdir().unwrap();
            let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
            
            let size = 10000;
            // Populate with data
            for i in 0..size {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(threads));
                let handles: Vec<_> = (0..threads).map(|thread_id| {
                    let db = Arc::clone(&db);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        let chunk_size = size / threads;
                        let start = thread_id * chunk_size;
                        let end = if thread_id == threads - 1 { size } else { start + chunk_size };
                        
                        for i in start..end {
                            let key = format!("key_{:08}", i);
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    group.finish();
}

fn benchmark_random_reads_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/random_reads_single_thread");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("random_read", size), size, |b, &size| {
            let temp_dir = tempdir().unwrap();
            let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
            
            // Populate with data
            for i in 0..size {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            // Generate random access pattern
            let mut rng = rand::rng();
            let indices: Vec<usize> = (0..size).map(|_| rng.gen_range(0..size)).collect();
            
            b.iter(|| {
                for &i in &indices {
                    let key = format!("key_{:08}", i);
                    let result = db.get(key.as_bytes()).unwrap();
                    black_box(result);
                }
            });
        });
    }
    group.finish();
}

fn benchmark_random_reads_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/random_reads_multi_thread");
    
    for threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("parallel_random_read", threads), threads, |b, &threads| {
            let temp_dir = tempdir().unwrap();
            let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
            
            let size = 10000;
            // Populate with data
            for i in 0..size {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(threads));
                let handles: Vec<_> = (0..threads).map(|_| {
                    let db = Arc::clone(&db);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        let mut rng = rand::rng();
                        for _ in 0..1000 {
                            let i = rng.gen_range(0..size);
                            let key = format!("key_{:08}", i);
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    group.finish();
}

fn benchmark_sequential_writes_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/sequential_writes_single_thread");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("sequential_write", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = tempdir().unwrap();
                    Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap()
                },
                |db| {
                    for i in 0..*size {
                        let key = format!("key_{:08}", i);
                        let value = format!("value_{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_sequential_writes_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/sequential_writes_multi_thread");
    
    for threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("parallel_write", threads), threads, |b, &threads| {
            b.iter_batched(
                || {
                    let temp_dir = tempdir().unwrap();
                    Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap())
                },
                |db| {
                    let size = 10000;
                    let barrier = Arc::new(Barrier::new(threads));
                    let handles: Vec<_> = (0..threads).map(|thread_id| {
                        let db = Arc::clone(&db);
                        let barrier = Arc::clone(&barrier);
                        thread::spawn(move || {
                            barrier.wait();
                            let chunk_size = size / threads;
                            let start = thread_id * chunk_size;
                            let end = if thread_id == threads - 1 { size } else { start + chunk_size };
                            
                            for i in start..end {
                                let key = format!("key_{:08}_{:02}", i, thread_id);
                                let value = format!("value_{:08}_{:02}", i, thread_id);
                                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_random_writes_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/random_writes_single_thread");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("random_write", size), size, |b, &size| {
            b.iter_batched(
                || {
                    let temp_dir = tempdir().unwrap();
                    let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                    let mut rng = rand::rng();
                    let indices: Vec<usize> = (0..*size).map(|_| rng.gen_range(0..*size)).collect();
                    (db, indices)
                },
                |(db, indices)| {
                    for &i in &indices {
                        let key = format!("key_{:08}", i);
                        let value = format!("value_{:08}", i);
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_random_writes_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/random_writes_multi_thread");
    
    for threads in [2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::new("parallel_random_write", threads), threads, |b, &threads| {
            b.iter_batched(
                || {
                    let temp_dir = tempdir().unwrap();
                    Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap())
                },
                |db| {
                    let size = 10000;
                    let barrier = Arc::new(Barrier::new(threads));
                    let handles: Vec<_> = (0..threads).map(|thread_id| {
                        let db = Arc::clone(&db);
                        let barrier = Arc::clone(&barrier);
                        thread::spawn(move || {
                            barrier.wait();
                            let mut rng = rand::rng();
                            for i in 0..1000 {
                                let rand_id = rng.gen_range(0..size);
                                let key = format!("key_{:08}_{:02}", rand_id, thread_id);
                                let value = format!("value_{:08}_{:02}", rand_id, thread_id);
                                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/mixed_workload");
    
    for ratio in [(70, 30), (80, 20), (90, 10), (95, 5)].iter() {
        let (read_pct, write_pct) = *ratio;
        group.bench_with_input(
            BenchmarkId::new("mixed_rw", format!("{}r_{}w", read_pct, write_pct)), 
            ratio, 
            |b, _| {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                
                // Pre-populate
                for i in 0..10000 {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                
                b.iter(|| {
                    let mut rng = rand::rng();
                    for _ in 0..1000 {
                        if rng.gen_range(0..100) < read_pct {
                            // Read operation
                            let i = rng.gen_range(0..10000);
                            let key = format!("key_{:08}", i);
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        } else {
                            // Write operation
                            let i = rng.gen_range(0..10000);
                            let key = format!("key_{:08}", i);
                            let value = format!("value_{:08}_updated", i);
                            db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                    }
                });
            }
        );
    }
    group.finish();
}

fn benchmark_transaction_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/transaction_throughput");
    
    for tx_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*tx_size as u64));
        group.bench_with_input(BenchmarkId::new("transaction", tx_size), tx_size, |b, &tx_size| {
            let temp_dir = tempdir().unwrap();
            let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
            
            b.iter(|| {
                let tx_id = db.begin_transaction().unwrap();
                for i in 0..tx_size {
                    let key = format!("tx_key_{:08}", i);
                    let value = format!("tx_value_{:08}", i);
                    db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                }
                db.commit_transaction(tx_id).unwrap();
            });
        });
    }
    group.finish();
}

fn benchmark_range_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_performance/range_scans");
    
    for scan_size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*scan_size as u64));
        group.bench_with_input(BenchmarkId::new("range_scan", scan_size), scan_size, |b, &scan_size| {
            let temp_dir = tempdir().unwrap();
            let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
            
            // Populate with ordered data
            for i in 0..100000 {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                let start_key = format!("key_{:08}", 10000);
                let end_key = format!("key_{:08}", 10000 + scan_size);
                let mut count = 0;
                
                for result in db.range(start_key.as_bytes()..end_key.as_bytes()).unwrap() {
                    let _entry = result.unwrap();
                    count += 1;
                    if count >= scan_size {
                        break;
                    }
                }
                black_box(count);
            });
        });
    }
    group.finish();
}

/// Latency benchmarks with percentile analysis
fn benchmark_read_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/read_distribution");
    group.sample_size(10000);
    
    group.bench_function("read_latency_p99", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Populate
        for i in 0..10000 {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        let mut latencies = LatencyMeasurement::new();
        
        b.iter_custom(|iters| {
            let mut total_time = Duration::new(0, 0);
            let mut rng = rand::rng();
            
            for _ in 0..iters {
                let i = rng.gen_range(0..10000);
                let key = format!("key_{:08}", i);
                
                let start = Instant::now();
                let result = db.get(key.as_bytes()).unwrap();
                let elapsed = start.elapsed();
                
                latencies.record(elapsed);
                total_time += elapsed;
                black_box(result);
            }
            
            total_time
        });
        
        // Report percentiles (this would be logged in real implementation)
        println!("Read Latency P50: {:?}", latencies.percentile(50.0));
        println!("Read Latency P90: {:?}", latencies.percentile(90.0));
        println!("Read Latency P95: {:?}", latencies.percentile(95.0));
        println!("Read Latency P99: {:?}", latencies.percentile(99.0));
        println!("Read Latency P999: {:?}", latencies.percentile(99.9));
    });
    
    group.finish();
}

fn benchmark_write_latency_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/write_distribution");
    group.sample_size(1000);
    
    group.bench_function("write_latency_p99", |b| {
        b.iter_batched(
            || {
                let temp_dir = tempdir().unwrap();
                let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                let mut latencies = LatencyMeasurement::new();
                (db, latencies)
            },
            |(db, mut latencies)| {
                let mut rng = rand::rng();
                let mut total_time = Duration::new(0, 0);
                
                for i in 0..1000 {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}", rng.random::<u64>());
                    
                    let start = Instant::now();
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    let elapsed = start.elapsed();
                    
                    latencies.record(elapsed);
                    total_time += elapsed;
                }
                
                black_box(total_time);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    
    group.finish();
}

fn benchmark_latency_under_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency/under_load");
    
    for concurrent_threads in [1, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("latency_with_load", concurrent_threads), 
            concurrent_threads, 
            |b, &threads| {
                let temp_dir = tempdir().unwrap();
                let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
                
                // Pre-populate
                for i in 0..10000 {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                
                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(threads + 1));
                    let mut latencies = LatencyMeasurement::new();
                    
                    // Start background load
                    let load_handles: Vec<_> = (0..threads).map(|_| {
                        let db = Arc::clone(&db);
                        let barrier = Arc::clone(&barrier);
                        thread::spawn(move || {
                            barrier.wait();
                            let mut rng = rand::rng();
                            for _ in 0..1000 {
                                let i = rng.gen_range(0..10000);
                                let key = format!("load_key_{:08}", i);
                                let value = format!("load_value_{:08}", rng.random::<u64>());
                                let _ = db.put(key.as_bytes(), value.as_bytes());
                            }
                        })
                    }).collect();
                    
                    // Measure latency under load
                    barrier.wait();
                    let mut rng = rand::rng();
                    for _ in 0..100 {
                        let i = rng.gen_range(0..10000);
                        let key = format!("key_{:08}", i);
                        
                        let start = Instant::now();
                        let result = db.get(key.as_bytes()).unwrap();
                        let elapsed = start.elapsed();
                        
                        latencies.record(elapsed);
                        black_box(result);
                    }
                    
                    for handle in load_handles {
                        handle.join().unwrap();
                    }
                    
                    black_box(latencies.percentile(99.0));
                });
            }
        );
    }
    group.finish();
}

/// Scalability tests
fn benchmark_thread_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalability/thread_scaling");
    
    for threads in [1, 2, 4, 8, 16, 32].iter() {
        group.throughput(Throughput::Elements(10000));
        group.bench_with_input(BenchmarkId::new("linear_scaling", threads), threads, |b, &threads| {
            let temp_dir = tempdir().unwrap();
            let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
            
            // Pre-populate
            for i in 0..100000 {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(threads));
                let handles: Vec<_> = (0..threads).map(|_| {
                    let db = Arc::clone(&db);
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        barrier.wait();
                        let mut rng = rand::rng();
                        for _ in 0..10000 / threads {
                            let i = rng.gen_range(0..100000);
                            let key = format!("key_{:08}", i);
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    group.finish();
}

fn benchmark_database_size_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalability/database_size_scaling");
    group.sample_size(10);
    
    for size_mb in [1, 10, 100].iter() {
        let records = size_mb * 1000; // Approximate 1KB per record
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(BenchmarkId::new("db_size", format!("{}MB", size_mb)), &records, |b, &records| {
            b.iter_batched(
                || {
                    let temp_dir = tempdir().unwrap();
                    let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
                    
                    // Populate database to target size
                    let value = vec![b'x'; 1000]; // 1KB value
                    for i in 0..records {
                        let key = format!("key_{:08}", i);
                        db.put(key.as_bytes(), &value).unwrap();
                    }
                    
                    db
                },
                |db| {
                    // Measure random read performance on large DB
                    let mut rng = rand::rng();
                    for _ in 0..1000 {
                        let i = rng.gen_range(0..records);
                        let key = format!("key_{:08}", i);
                        let result = db.get(key.as_bytes()).unwrap();
                        black_box(result);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn benchmark_connection_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalability/connection_scaling");
    
    for connections in [1, 10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::new("concurrent_connections", connections), connections, |b, &connections| {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().to_path_buf();
            
            b.iter(|| {
                let barrier = Arc::new(Barrier::new(connections));
                let handles: Vec<_> = (0..connections).map(|conn_id| {
                    let db_path = db_path.clone();
                    let barrier = Arc::clone(&barrier);
                    thread::spawn(move || {
                        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
                        barrier.wait();
                        
                        // Each connection performs operations
                        for i in 0..100 {
                            let key = format!("conn_{}_key_{:08}", conn_id, i);
                            let value = format!("conn_{}_value_{:08}", conn_id, i);
                            db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.join().unwrap();
                }
            });
        });
    }
    group.finish();
}

/// Real-world workload simulations
fn benchmark_oltp_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("workloads/oltp_tpcc_like");
    
    group.bench_function("tpcc_new_order", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Setup TPC-C like schema
        for warehouse_id in 0..10 {
            for district_id in 0..10 {
                for customer_id in 0..3000 {
                    let key = format!("customer_{}_{}", warehouse_id * 10 + district_id, customer_id);
                    let value = format!("customer_data_{}", customer_id);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                
                for item_id in 0..1000 {
                    let key = format!("stock_{}_{}_{}", warehouse_id, district_id, item_id);
                    let value = format!("stock_data_{}", item_id);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            }
        }
        
        b.iter(|| {
            let tx_id = db.begin_transaction().unwrap();
            let mut rng = rand::rng();
            
            // Simulate new order transaction
            let warehouse_id = rng.gen_range(0..10);
            let district_id = rng.gen_range(0..10);
            let customer_id = rng.gen_range(0..3000);
            
            // Read customer
            let customer_key = format!("customer_{}_{}", warehouse_id * 10 + district_id, customer_id);
            let _customer = db.get_tx(tx_id, customer_key.as_bytes()).unwrap();
            
            // Update stock for multiple items
            for _ in 0..rng.gen_range(5..15) {
                let item_id = rng.gen_range(0..1000);
                let stock_key = format!("stock_{}_{}_{}", warehouse_id, district_id, item_id);
                let stock_value = format!("updated_stock_{}", item_id);
                db.put_tx(tx_id, stock_key.as_bytes(), stock_value.as_bytes()).unwrap();
            }
            
            // Create order record
            let order_id = rng.random::<u64>();
            let order_key = format!("order_{}_{}", warehouse_id * 10 + district_id, order_id);
            let order_value = format!("order_data_{}", order_id);
            db.put_tx(tx_id, order_key.as_bytes(), order_value.as_bytes()).unwrap();
            
            db.commit_transaction(tx_id).unwrap();
        });
    });
    
    group.finish();
}

fn benchmark_analytics_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("workloads/analytics_tpch_like");
    
    group.bench_function("tpch_scan_aggregate", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Setup TPC-H like data
        for i in 0..100000 {
            let lineitem_key = format!("lineitem_{:08}", i);
            let lineitem_value = format!("price:{:.2},quantity:{},discount:{:.2}", 
                rand::rng().gen_range(1.0..1000.0),
                rand::rng().gen_range(1..100),
                rand::rng().gen_range(0.0..0.1)
            );
            db.put(lineitem_key.as_bytes(), lineitem_value.as_bytes()).unwrap();
        }
        
        b.iter(|| {
            let mut total_price = 0.0;
            let mut count = 0;
            
            // Simulate analytical query with scan and aggregation
            for result in db.range(b"lineitem_00000000"..b"lineitem_99999999").unwrap() {
                let (_, value) = result.unwrap();
                let value_str = String::from_utf8_lossy(&value);
                
                // Parse price (simplified)
                if let Some(price_start) = value_str.find("price:") {
                    if let Some(price_end) = value_str[price_start + 6..].find(',') {
                        if let Ok(price) = value_str[price_start + 6..price_start + 6 + price_end].parse::<f64>() {
                            total_price += price;
                            count += 1;
                        }
                    }
                }
                
                if count >= 10000 {
                    break;
                }
            }
            
            black_box((total_price, count));
        });
    });
    
    group.finish();
}

fn benchmark_key_value_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("workloads/key_value_ycsb");
    
    for workload in ["A", "B", "C"].iter() {
        group.bench_with_input(BenchmarkId::new("ycsb", workload), workload, |b, &workload| {
            let temp_dir = tempdir().unwrap();
            let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
            
            // Pre-populate for YCSB
            for i in 0..100000 {
                let key = format!("user{:08}", i);
                let value = format!("field1=value1,field2=value2,field3=value3,field4=value4");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            b.iter(|| {
                let mut rng = rand::rng();
                
                match workload {
                    "A" => {
                        // 50% reads, 50% updates
                        for _ in 0..1000 {
                            let i = rng.gen_range(0..100000);
                            let key = format!("user{:08}", i);
                            
                            if rng.gen_bool(0.5) {
                                let result = db.get(key.as_bytes()).unwrap();
                                black_box(result);
                            } else {
                                let value = format!("updated_field1=new_value1,field2=value2");
                                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                        }
                    }
                    "B" => {
                        // 95% reads, 5% updates
                        for _ in 0..1000 {
                            let i = rng.gen_range(0..100000);
                            let key = format!("user{:08}", i);
                            
                            if rng.gen_bool(0.95) {
                                let result = db.get(key.as_bytes()).unwrap();
                                black_box(result);
                            } else {
                                let value = format!("updated_field1=new_value1,field2=value2");
                                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                        }
                    }
                    "C" => {
                        // 100% reads
                        for _ in 0..1000 {
                            let i = rng.gen_range(0..100000);
                            let key = format!("user{:08}", i);
                            let result = db.get(key.as_bytes()).unwrap();
                            black_box(result);
                        }
                    }
                    _ => {}
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_time_series_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("workloads/time_series");
    
    group.bench_function("time_series_ingestion", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        b.iter(|| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            // Simulate time series data ingestion
            for i in 0..1000 {
                let timestamp = now + i;
                let sensor_id = i % 100;
                let key = format!("ts_{}_{:016}", sensor_id, timestamp);
                let value = format!("temperature:{:.2},humidity:{:.2},pressure:{:.2}", 
                    20.0 + (i as f64 * 0.1) % 40.0,
                    30.0 + (i as f64 * 0.2) % 70.0,
                    1000.0 + (i as f64 * 0.05) % 100.0
                );
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
    });
    
    group.bench_function("time_series_range_query", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Pre-populate time series data
        let base_time = 1640995200; // 2022-01-01
        for sensor_id in 0..10 {
            for i in 0..10000 {
                let timestamp = base_time + i * 60; // 1 minute intervals
                let key = format!("ts_{}_{:016}", sensor_id, timestamp);
                let value = format!("temperature:{:.2}", 20.0 + (i as f64 * 0.1) % 40.0);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        }
        
        b.iter(|| {
            let sensor_id = 0;
            let start_time = base_time + 1000 * 60;
            let end_time = start_time + 3600; // 1 hour range
            
            let start_key = format!("ts_{}_{:016}", sensor_id, start_time);
            let end_key = format!("ts_{}_{:016}", sensor_id, end_time);
            
            let mut count = 0;
            for result in db.range(start_key.as_bytes()..end_key.as_bytes()).unwrap() {
                let (_key, _value) = result.unwrap();
                count += 1;
            }
            
            black_box(count);
        });
    });
    
    group.finish();
}

/// Performance validation against targets
fn validate_performance_targets(c: &mut Criterion) {
    let mut group = c.benchmark_group("validation/performance_targets");
    let targets = PerformanceTargets::default();
    
    group.bench_function("validate_read_throughput", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Populate
        for i in 0..100000 {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut rng = rand::rng();
            
            for _ in 0..iters {
                let i = rng.gen_range(0..100000);
                let key = format!("key_{:08}", i);
                let result = db.get(key.as_bytes()).unwrap();
                black_box(result);
            }
            
            let elapsed = start.elapsed();
            let ops_per_sec = iters as f64 / elapsed.as_secs_f64();
            
            // Validate against target
            if ops_per_sec >= targets.read_ops_per_sec {
                eprintln!("✅ Read throughput: {:.0} ops/sec (target: {:.0})", 
                    ops_per_sec, targets.read_ops_per_sec);
            } else {
                eprintln!("❌ Read throughput: {:.0} ops/sec (target: {:.0})", 
                    ops_per_sec, targets.read_ops_per_sec);
            }
            
            elapsed
        });
    });
    
    group.bench_function("validate_write_throughput", |b| {
        b.iter_batched(
            || {
                let temp_dir = tempdir().unwrap();
                Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap()
            },
            |db| {
                let start = Instant::now();
                let iters = 10000;
                
                for i in 0..iters {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                
                let elapsed = start.elapsed();
                let ops_per_sec = iters as f64 / elapsed.as_secs_f64();
                
                // Validate against target
                if ops_per_sec >= targets.write_ops_per_sec {
                    eprintln!("✅ Write throughput: {:.0} ops/sec (target: {:.0})", 
                        ops_per_sec, targets.write_ops_per_sec);
                } else {
                    eprintln!("❌ Write throughput: {:.0} ops/sec (target: {:.0})", 
                        ops_per_sec, targets.write_ops_per_sec);
                }
                
                black_box(ops_per_sec);
            },
            criterion::BatchSize::SmallInput,
        );
    });
    
    group.bench_function("validate_latency_p99", |b| {
        let temp_dir = tempdir().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Populate
        for i in 0..10000 {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        b.iter(|| {
            let mut latencies = LatencyMeasurement::new();
            let mut rng = rand::rng();
            
            for _ in 0..1000 {
                let i = rng.gen_range(0..10000);
                let key = format!("key_{:08}", i);
                
                let start = Instant::now();
                let result = db.get(key.as_bytes()).unwrap();
                let elapsed = start.elapsed();
                
                latencies.record(elapsed);
                black_box(result);
            }
            
            let p99 = latencies.percentile(99.0);
            let p99_us = p99.as_nanos() as f64 / 1000.0;
            
            // Validate against target
            if p99_us <= targets.latency_p99_us {
                eprintln!("✅ P99 latency: {:.2} μs (target: {:.0} μs)", 
                    p99_us, targets.latency_p99_us);
            } else {
                eprintln!("❌ P99 latency: {:.2} μs (target: {:.0} μs)", 
                    p99_us, targets.latency_p99_us);
            }
            
            black_box(p99);
        });
    });
    
    group.finish();
}

criterion_group!(
    core_performance_benches,
    benchmark_sequential_reads_single_thread,
    benchmark_sequential_reads_multi_thread,
    benchmark_random_reads_single_thread,
    benchmark_random_reads_multi_thread,
    benchmark_sequential_writes_single_thread,
    benchmark_sequential_writes_multi_thread,
    benchmark_random_writes_single_thread,
    benchmark_random_writes_multi_thread,
    benchmark_mixed_workload,
    benchmark_transaction_throughput,
    benchmark_range_scans
);

criterion_group!(
    latency_benches,
    benchmark_read_latency_distribution,
    benchmark_write_latency_distribution,
    benchmark_latency_under_load
);

criterion_group!(
    scalability_benches,
    benchmark_thread_scaling,
    benchmark_database_size_scaling,
    benchmark_connection_scaling
);

criterion_group!(
    workload_benches,
    benchmark_oltp_workload,
    benchmark_analytics_workload,
    benchmark_key_value_workload,
    benchmark_time_series_workload
);

criterion_group!(
    validation_benches,
    validate_performance_targets
);

criterion_main!(
    core_performance_benches,
    latency_benches,
    scalability_benches,
    workload_benches,
    validation_benches
);
//! Critical path performance benchmarks for Lightning DB optimizations
//!
//! This benchmark suite validates the performance improvements achieved through:
//! - SIMD-optimized operations
//! - Lock-free data structures  
//! - Thread-local caching
//! - Transaction batching
//! - Memory-efficient allocations
//!
//! Target: Maintain >100K ops/sec for basic operations, >50K tx/sec for transactions

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lightning_db::performance::optimizations::database_integration::{
    OptimizedDatabaseOps, OptimizedDatabaseStats,
};
use lightning_db::performance::optimizations::{
    critical_path::create_critical_path_optimizer, simd::safe as simd_ops,
    transaction_batching::create_transaction_batcher,
};
use lightning_db::performance::thread_local::optimized_storage::{
    IsolationLevel, ThreadLocalStorage,
};
use lightning_db::performance::{TransactionPriority, WorkloadType};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Benchmark optimized get operations
fn benchmark_optimized_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_get");

    for workload in [
        WorkloadType::HighThroughput,
        WorkloadType::LowLatency,
        WorkloadType::Mixed,
    ]
    .iter()
    {
        let ops = OptimizedDatabaseOps::new(*workload);

        // Warm up caches with test data
        let warm_pages: Vec<(u32, Vec<u8>)> = (0..1000)
            .map(|i| (i, format!("cached_value_{}", i).into_bytes()))
            .collect();
        ops.warm_caches(warm_pages).unwrap();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("workload", format!("{:?}", workload)),
            workload,
            |b, _workload| {
                b.iter(|| {
                    let key = format!("key_{}", fastrand::u32(0..1000));
                    black_box(ops.optimized_get(key.as_bytes()).unwrap())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark optimized put operations
fn benchmark_optimized_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimized_put");

    for size in [32, 128, 512, 1024].iter() {
        let ops = OptimizedDatabaseOps::new(WorkloadType::HighThroughput);
        let value = vec![42u8; *size];

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("value_size", size), &value, |b, value| {
            b.iter(|| {
                let key = format!("key_{}", fastrand::u64(..));
                black_box(ops.optimized_put(key.as_bytes(), value).unwrap())
            })
        });
    }

    group.finish();
}

/// Benchmark transaction operations with batching
fn benchmark_transaction_batching(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_batching");

    for batch_size in [10, 50, 100, 200].iter() {
        let ops = OptimizedDatabaseOps::new(WorkloadType::HighThroughput);

        group.throughput(Throughput::Elements(*batch_size));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let mut tx_ids = Vec::new();

                    // Begin transactions
                    for _ in 0..batch_size {
                        let tx_id = ops
                            .optimized_begin_transaction(IsolationLevel::ReadCommitted)
                            .unwrap();
                        tx_ids.push(tx_id);
                    }

                    // Commit all transactions
                    for tx_id in tx_ids {
                        black_box(ops.optimized_commit_transaction(tx_id).unwrap());
                    }

                    // Process batches
                    black_box(ops.process_batches().unwrap());
                })
            },
        );
    }

    group.finish();
}

/// Benchmark SIMD operations
fn benchmark_simd_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_operations");

    // Test key comparison
    for key_size in [16, 32, 64, 128, 256].iter() {
        let key1 = vec![1u8; *key_size];
        let key2 = vec![2u8; *key_size];

        group.throughput(Throughput::Bytes(*key_size as u64));
        group.bench_with_input(
            BenchmarkId::new("key_comparison", key_size),
            &(key1, key2),
            |b, (key1, key2)| b.iter(|| black_box(simd_ops::compare_keys(key1, key2))),
        );
    }

    // Test hash calculation
    for data_size in [64, 256, 1024, 4096].iter() {
        let data = vec![42u8; *data_size];

        group.throughput(Throughput::Bytes(*data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("hash_calculation", data_size),
            &data,
            |b, data| b.iter(|| black_box(simd_ops::hash(data, 12345))),
        );
    }

    group.finish();
}

/// Benchmark concurrent operations across multiple threads
fn benchmark_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");

    for thread_count in [2, 4, 8, 16].iter() {
        group.throughput(Throughput::Elements(1000 * *thread_count as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter(|| {
                    let ops = Arc::new(OptimizedDatabaseOps::new(WorkloadType::HighThroughput));

                    let handles: Vec<_> = (0..thread_count)
                        .map(|thread_id| {
                            let ops = Arc::clone(&ops);
                            thread::spawn(move || {
                                for i in 0..1000 {
                                    let key = format!("key_{}_{}", thread_id, i);
                                    let value = format!("value_{}_{}", thread_id, i);

                                    // Mix of operations
                                    match i % 4 {
                                        0 => {
                                            ops.optimized_put(key.as_bytes(), value.as_bytes())
                                                .unwrap();
                                        }
                                        1 => {
                                            ops.optimized_get(key.as_bytes()).unwrap();
                                        }
                                        2 => {
                                            ops.optimized_delete(key.as_bytes()).unwrap();
                                        }
                                        3 => {
                                            let tx_id = ops
                                                .optimized_begin_transaction(
                                                    IsolationLevel::ReadCommitted,
                                                )
                                                .unwrap();
                                            ops.optimized_commit_transaction(tx_id).unwrap();
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            })
                        })
                        .collect();

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    // Process any remaining batches
                    black_box(ops.process_batches().unwrap());
                })
            },
        );
    }

    group.finish();
}

/// Benchmark thread-local storage operations
fn benchmark_thread_local_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("thread_local_storage");

    // Benchmark transaction caching
    group.bench_function("transaction_cache", |b| {
        b.iter(|| {
            ThreadLocalStorage::clear_all_caches();
            // Cache 50 transactions
            for i in 0..50 {
                let state = lightning_db::performance::thread_local::optimized_storage::CachedTransactionState {
                    tx_id: i,
                    read_timestamp: 1000 + i,
                    write_count: 0,
                    read_count: 0,
                    last_access: Instant::now(),
                    isolation_level: IsolationLevel::ReadCommitted,
                    priority: TransactionPriority::Normal,
                };
                black_box(ThreadLocalStorage::cache_transaction(state));
            }

            // Access cached transactions
            for i in 0..50 {
                black_box(ThreadLocalStorage::get_transaction(i));
                ThreadLocalStorage::touch_transaction(i, i % 2 == 0);
            }

            // Remove transactions
            for i in 0..50 {
                black_box(ThreadLocalStorage::remove_transaction(i));
            }
        })
    });

    // Benchmark SIMD buffer operations
    group.bench_function("simd_buffers", |b| {
        b.iter(|| {
            ThreadLocalStorage::with_simd_buffers(|buffers| {
                let key_buf = buffers.get_key_buffer(256);
                for i in 0..10 {
                    key_buf.clear();
                    key_buf.extend_from_slice(format!("test_key_{}", i).as_bytes());
                }

                let value_buf = buffers.get_value_buffer(4096);
                for i in 0..5 {
                    value_buf.clear();
                    value_buf.extend_from_slice(&vec![i as u8; 1000]);
                }

                black_box(key_buf.len() + value_buf.len())
            })
        })
    });

    group.finish();
}

/// Benchmark lock-free data structures
fn benchmark_lock_free_structures(c: &mut Criterion) {
    let mut group = c.benchmark_group("lock_free_structures");

    // Test lock-free queue performance
    group.bench_function("lock_free_queue", |b| {
        use lightning_db::performance::lock_free::concurrent_structures::LockFreeQueue;

        b.iter(|| {
            let queue = Arc::new(LockFreeQueue::with_capacity(1024));

            // Producer thread
            let queue_prod = Arc::clone(&queue);
            let producer = thread::spawn(move || {
                for i in 0..1000 {
                    while queue_prod.enqueue(i).is_err() {
                        thread::yield_now();
                    }
                }
            });

            // Consumer thread
            let queue_cons = Arc::clone(&queue);
            let consumer = thread::spawn(move || {
                let mut consumed = 0;
                while consumed < 1000 {
                    if queue_cons.dequeue().is_some() {
                        consumed += 1;
                    }
                    thread::yield_now();
                }
                consumed
            });

            producer.join().unwrap();
            let consumed = consumer.join().unwrap();
            black_box(consumed);
        })
    });

    group.finish();
}

/// Mixed workload benchmark that simulates real usage patterns
fn benchmark_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(10)); // Longer measurement for stability

    for workload in [
        ("oltp_read_heavy", (70, 20, 5, 5)), // 70% reads, 20% writes, 5% deletes, 5% tx
        ("oltp_write_heavy", (20, 60, 10, 10)), // 20% reads, 60% writes, 10% deletes, 10% tx
        ("mixed_analytical", (40, 30, 10, 20)), // 40% reads, 30% writes, 10% deletes, 20% tx
    ]
    .iter()
    {
        let (workload_name, (read_pct, write_pct, delete_pct, tx_pct)) = workload;

        group.throughput(Throughput::Elements(10000));
        group.bench_with_input(
            BenchmarkId::new("workload", workload_name),
            workload,
            |b, _workload| {
                let ops = OptimizedDatabaseOps::new(WorkloadType::Mixed);

                // Pre-populate some data
                for i in 0..1000 {
                    let key = format!("init_key_{}", i);
                    let value = format!("init_value_{}", i);
                    ops.optimized_put(key.as_bytes(), value.as_bytes()).unwrap();
                }

                b.iter(|| {
                    for i in 0..10000 {
                        let operation = i % 100;
                        let key = format!("key_{}", i % 1000);
                        let value = format!("value_{}", i);

                        if operation < *read_pct {
                            // Read operation
                            black_box(ops.optimized_get(key.as_bytes()).unwrap());
                        } else if operation < read_pct + write_pct {
                            // Write operation
                            black_box(ops.optimized_put(key.as_bytes(), value.as_bytes()).unwrap());
                        } else if operation < read_pct + write_pct + delete_pct {
                            // Delete operation
                            black_box(ops.optimized_delete(key.as_bytes()).unwrap());
                        } else {
                            // Transaction operation
                            let tx_id = ops
                                .optimized_begin_transaction(IsolationLevel::ReadCommitted)
                                .unwrap();
                            black_box(ops.optimized_commit_transaction(tx_id).unwrap());
                        }

                        // Periodically process batches
                        if i % 100 == 0 {
                            ops.process_batches().unwrap();
                        }
                    }

                    // Final batch processing
                    ops.process_batches().unwrap();
                })
            },
        );
    }

    group.finish();
}

/// Comprehensive performance report generation
fn performance_report() {
    println!("\nLightning DB Critical Path Performance Report");
    println!("==============================================");

    let ops = OptimizedDatabaseOps::new(WorkloadType::Mixed);

    // Run a quick performance test
    let start = Instant::now();
    let mut operations = 0;

    for i in 0..10000 {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);

        ops.optimized_put(key.as_bytes(), value.as_bytes()).unwrap();
        ops.optimized_get(key.as_bytes()).unwrap();
        operations += 2;

        if i % 100 == 0 {
            let tx_id = ops
                .optimized_begin_transaction(IsolationLevel::ReadCommitted)
                .unwrap();
            ops.optimized_commit_transaction(tx_id).unwrap();
            operations += 1;
        }

        if i % 50 == 0 {
            ops.process_batches().unwrap();
        }
    }

    let duration = start.elapsed();
    let ops_per_sec = operations as f64 / duration.as_secs_f64();

    println!("\nQuick Performance Test Results:");
    println!("- Operations: {}", operations);
    println!("- Duration: {:.2?}", duration);
    println!("- Throughput: {:.0} ops/sec", ops_per_sec);

    // Get detailed statistics
    let stats = ops.get_performance_stats();
    println!("\n{}", stats.generate_report());

    let efficiency = stats.efficiency_metrics();
    println!("Performance Analysis:");
    println!(
        "- Estimated Peak Throughput: {:.0} ops/sec",
        stats.estimated_throughput_ops_per_sec()
    );
    println!(
        "- Transaction Throughput: {:.0} tx/sec",
        stats.estimated_tx_throughput()
    );
    println!(
        "- Cache Efficiency: {:.1}%",
        efficiency.cache_efficiency * 100.0
    );
    println!(
        "- SIMD Utilization: {:.1}%",
        efficiency.simd_utilization * 100.0
    );
    println!(
        "- Batching Efficiency: {:.1}%",
        efficiency.batch_efficiency * 100.0
    );
    println!(
        "- Lock-Free Utilization: {:.1}%",
        efficiency.lock_free_utilization * 100.0
    );

    if stats.simd_enabled {
        println!("\n✅ SIMD optimizations are ENABLED");
    } else {
        println!("\n❌ SIMD optimizations are DISABLED");
    }

    // Performance targets validation
    println!("\nPerformance Targets Validation:");
    println!("- Target: >100K ops/sec for basic operations");
    if ops_per_sec > 100_000.0 {
        println!("  ✅ PASSED: {:.0} ops/sec", ops_per_sec);
    } else {
        println!("  ❌ FAILED: {:.0} ops/sec (below target)", ops_per_sec);
    }

    println!("- Target: >50K tx/sec for transactions");
    let tx_throughput = stats.estimated_tx_throughput();
    if tx_throughput > 50_000 {
        println!("  ✅ PASSED: {} tx/sec", tx_throughput);
    } else {
        println!("  ❌ FAILED: {} tx/sec (below target)", tx_throughput);
    }

    println!("- Target: >80% cache hit rate");
    if efficiency.cache_efficiency > 0.8 {
        println!("  ✅ PASSED: {:.1}%", efficiency.cache_efficiency * 100.0);
    } else {
        println!(
            "  ❌ FAILED: {:.1}% (below target)",
            efficiency.cache_efficiency * 100.0
        );
    }
}

criterion_group!(
    benches,
    benchmark_optimized_get,
    benchmark_optimized_put,
    benchmark_transaction_batching,
    benchmark_simd_operations,
    benchmark_concurrent_operations,
    benchmark_thread_local_storage,
    benchmark_lock_free_structures,
    benchmark_mixed_workload,
);

criterion_main!(benches);

// Run performance report when this is executed as a main program
fn main() {
    performance_report();
}

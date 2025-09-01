use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod regression_suite;

use regression_suite::{
    analysis::{PerformanceAnalyzer, RegressionAnalyzer},
    metrics::{BenchmarkMetrics, MetricsCollector},
    reporting::ReportGenerator,
    workloads::{WorkloadGenerator, WorkloadType},
};

/// High-level benchmark orchestrator that runs the comprehensive regression suite
pub fn run_comprehensive_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("comprehensive_regression_suite");
    group.measurement_time(Duration::from_secs(60));
    group.sample_size(20);

    // Initialize components
    let workload_generator = WorkloadGenerator::new();
    let mut metrics_collector = MetricsCollector::new(true, true, true);
    let report_generator = ReportGenerator::new("./performance_reports")
        .expect("Failed to create report generator");

    // Define comprehensive benchmark scenarios
    let scenarios = vec![
        BenchmarkScenario {
            name: "high_throughput_reads",
            workload: WorkloadType::SequentialRead,
            config: LightningDbConfig {
                cache_size: 2 * 1024 * 1024 * 1024, // 2GB cache
                prefetch_enabled: true,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8, 16],
            value_size: 1024,
            dataset_size: 1_000_000,
            expected_ops_per_sec: Some(20_000_000.0),
        },
        BenchmarkScenario {
            name: "random_read_performance",
            workload: WorkloadType::RandomRead,
            config: LightningDbConfig {
                cache_size: 1 * 1024 * 1024 * 1024, // 1GB cache
                prefetch_enabled: false,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8],
            value_size: 1024,
            dataset_size: 500_000,
            expected_ops_per_sec: Some(15_000_000.0),
        },
        BenchmarkScenario {
            name: "write_throughput",
            workload: WorkloadType::SequentialWrite,
            config: LightningDbConfig {
                cache_size: 512 * 1024 * 1024, // 512MB cache
                prefetch_enabled: false,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8],
            value_size: 1024,
            dataset_size: 0,
            expected_ops_per_sec: Some(1_100_000.0),
        },
        BenchmarkScenario {
            name: "mixed_oltp_workload",
            workload: WorkloadType::Mixed { read_ratio: 0.8 },
            config: LightningDbConfig {
                cache_size: 1 * 1024 * 1024 * 1024, // 1GB cache
                prefetch_enabled: true,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8, 16],
            value_size: 512,
            dataset_size: 100_000,
            expected_ops_per_sec: Some(850_000.0),
        },
        BenchmarkScenario {
            name: "transaction_processing",
            workload: WorkloadType::Transaction { ops_per_tx: 5 },
            config: LightningDbConfig {
                cache_size: 256 * 1024 * 1024, // 256MB cache
                prefetch_enabled: false,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 2, 4, 8],
            value_size: 256,
            dataset_size: 0,
            expected_ops_per_sec: Some(400_000.0),
        },
        BenchmarkScenario {
            name: "concurrent_stress_test",
            workload: WorkloadType::Concurrent,
            config: LightningDbConfig {
                cache_size: 1 * 1024 * 1024 * 1024, // 1GB cache
                prefetch_enabled: true,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![8, 16, 32],
            value_size: 512,
            dataset_size: 50_000,
            expected_ops_per_sec: Some(1_400_000.0),
        },
        BenchmarkScenario {
            name: "range_scan_performance",
            workload: WorkloadType::Scan { 
                key_count: 100_000, 
                scan_length: 1000 
            },
            config: LightningDbConfig {
                cache_size: 1 * 1024 * 1024 * 1024, // 1GB cache
                prefetch_enabled: true,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8],
            value_size: 1024,
            dataset_size: 100_000,
            expected_ops_per_sec: Some(50_000.0),
        },
        BenchmarkScenario {
            name: "cache_efficiency_test",
            workload: WorkloadType::CacheTest,
            config: LightningDbConfig {
                cache_size: 128 * 1024 * 1024, // 128MB cache
                prefetch_enabled: true,
                compression_enabled: false,
                ..Default::default()
            },
            thread_counts: vec![1, 4, 8],
            value_size: 1024,
            dataset_size: 1000, // Small dataset to fit in cache
            expected_ops_per_sec: Some(25_000_000.0),
        },
        BenchmarkScenario {
            name: "compression_impact",
            workload: WorkloadType::Mixed { read_ratio: 0.7 },
            config: LightningDbConfig {
                cache_size: 512 * 1024 * 1024, // 512MB cache
                prefetch_enabled: true,
                compression_enabled: true,
                ..Default::default()
            },
            thread_counts: vec![1, 4],
            value_size: 4096, // Larger values to see compression effect
            dataset_size: 50_000,
            expected_ops_per_sec: Some(600_000.0), // Expect some overhead from compression
        },
    ];

    // Run each benchmark scenario
    for scenario in scenarios {
        for &thread_count in &scenario.thread_counts {
            group.bench_with_input(
                BenchmarkId::new(scenario.name.clone(), thread_count),
                &(&scenario, thread_count),
                |b, &(scenario, threads)| {
                    b.iter_custom(|iters| {
                        run_scenario_benchmark(
                            scenario, 
                            threads, 
                            iters, 
                            &workload_generator,
                            &mut metrics_collector
                        ).unwrap_or(Duration::from_secs(999))
                    });
                },
            );
        }
    }

    group.finish();
}

struct BenchmarkScenario {
    name: &'static str,
    workload: WorkloadType,
    config: LightningDbConfig,
    thread_counts: Vec<usize>,
    value_size: usize,
    dataset_size: usize,
    expected_ops_per_sec: Option<f64>,
}

fn run_scenario_benchmark(
    scenario: &BenchmarkScenario,
    thread_count: usize,
    iterations: u64,
    workload_generator: &WorkloadGenerator,
    metrics_collector: &mut MetricsCollector,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Arc::new(Database::create(temp_dir.path(), scenario.config.clone())?);

    let snapshot_before = metrics_collector.take_snapshot()?;
    let start_time = Instant::now();

    // Calculate appropriate duration based on iterations
    let target_duration = Duration::from_millis((iterations as u64).max(100));

    let _ops_completed = workload_generator.run_workload(
        &scenario.workload,
        db.clone(),
        thread_count,
        scenario.value_size,
        scenario.dataset_size,
        target_duration,
    )?;

    let elapsed = start_time.elapsed();
    let _snapshot_after = metrics_collector.take_snapshot()?;

    Ok(elapsed)
}

/// Specialized benchmark for memory usage patterns
fn benchmark_memory_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_patterns");
    group.measurement_time(Duration::from_secs(30));

    // Test different memory allocation patterns
    let memory_scenarios = vec![
        ("small_values", 64, 1_000_000),
        ("medium_values", 1024, 500_000),
        ("large_values", 8192, 100_000),
        ("huge_values", 65536, 10_000),
    ];

    for (name, value_size, dataset_size) in memory_scenarios {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 1 * 1024 * 1024 * 1024, // 1GB cache
                    ..Default::default()
                };
                let db = Database::create(temp_dir.path(), config).unwrap();
                let value = vec![0u8; value_size];

                let start = Instant::now();
                
                for i in 0..iters.min(dataset_size as u64) {
                    let key = format!("key_{:08}", i);
                    db.put(key.as_bytes(), &value).unwrap();
                }
                
                start.elapsed()
            });
        });
    }

    group.finish();
}

/// Specialized benchmark for I/O patterns
fn benchmark_io_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("io_patterns");
    group.measurement_time(Duration::from_secs(20));

    let io_scenarios = vec![
        ("sequential_io", true),
        ("random_io", false),
    ];

    for (name, sequential) in io_scenarios {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 64 * 1024 * 1024, // Small cache to force I/O
                    prefetch_enabled: sequential,
                    ..Default::default()
                };
                let db = Database::create(temp_dir.path(), config).unwrap();
                let value = vec![0u8; 4096];

                // Pre-populate data
                for i in 0..10000 {
                    let key = format!("key_{:08}", i);
                    db.put(key.as_bytes(), &value).unwrap();
                }
                
                db.checkpoint().unwrap();

                let start = Instant::now();
                
                for i in 0..iters.min(10000) {
                    let key_index = if sequential {
                        i % 10000
                    } else {
                        (i * 7919) % 10000 // Pseudo-random pattern
                    };
                    let key = format!("key_{:08}", key_index);
                    let _result = db.get(key.as_bytes()).unwrap();
                }
                
                start.elapsed()
            });
        });
    }

    group.finish();
}

/// Specialized benchmark for lock contention scenarios
fn benchmark_concurrency_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrency_patterns");
    group.measurement_time(Duration::from_secs(30));

    let concurrency_scenarios = vec![
        ("low_contention", 1000, 8),
        ("medium_contention", 100, 8),
        ("high_contention", 10, 8),
        ("extreme_contention", 1, 16),
    ];

    for (name, key_space, thread_count) in concurrency_scenarios {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let temp_dir = tempdir().unwrap();
                let config = LightningDbConfig {
                    cache_size: 256 * 1024 * 1024, // 256MB cache
                    ..Default::default()
                };
                let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());

                let start = Instant::now();
                let handles: Vec<_> = (0..thread_count)
                    .map(|thread_id| {
                        let db = Arc::clone(&db);
                        let ops_per_thread = iters / thread_count as u64;
                        
                        std::thread::spawn(move || {
                            let value = vec![0u8; 256];
                            for i in 0..ops_per_thread {
                                let key_index = (thread_id as u64 + i) % key_space;
                                let key = format!("key_{:04}", key_index);
                                
                                if i % 2 == 0 {
                                    let _ = db.put(key.as_bytes(), &value);
                                } else {
                                    let _ = db.get(key.as_bytes());
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
        });
    }

    group.finish();
}

/// Recovery and persistence benchmark
fn benchmark_recovery_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery_patterns");
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("write_and_recover", |b| {
        b.iter_custom(|iters| {
            let temp_dir = tempdir().unwrap();
            let config = LightningDbConfig::default();
            
            let start = Instant::now();
            
            // Write phase
            {
                let db = Database::create(temp_dir.path(), config.clone()).unwrap();
                let value = vec![0u8; 512];
                
                for i in 0..iters.min(10000) {
                    let key = format!("key_{:08}", i);
                    db.put(key.as_bytes(), &value).unwrap();
                }
                
                db.checkpoint().unwrap();
                // Database dropped here, simulating shutdown
            }
            
            // Recovery phase
            {
                let db = Database::create(temp_dir.path(), config).unwrap();
                // Verify data exists (simplified recovery test)
                for i in 0..std::cmp::min(iters, 100) {
                    let key = format!("key_{:08}", i);
                    let _result = db.get(key.as_bytes()).unwrap();
                }
            }
            
            start.elapsed()
        });
    });

    group.finish();
}

/// Comprehensive regression suite entry point
criterion_group!(
    comprehensive_benchmarks,
    run_comprehensive_benchmarks,
    benchmark_memory_patterns,
    benchmark_io_patterns,
    benchmark_concurrency_patterns,
    benchmark_recovery_patterns
);

criterion_main!(comprehensive_benchmarks);
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use lightning_db::{Database, LightningDbConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub mod analysis;
pub mod metrics;
pub mod reporting;
pub mod workloads;

use analysis::RegressionAnalyzer;
use metrics::{BenchmarkMetrics, MetricsCollector, PerformanceSnapshot};
use reporting::ReportGenerator;
use workloads::{WorkloadGenerator, WorkloadType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub name: String,
    pub workload_type: WorkloadType,
    pub duration_secs: u64,
    pub sample_size: usize,
    pub thread_counts: Vec<usize>,
    pub value_sizes: Vec<usize>,
    pub dataset_sizes: Vec<usize>,
    pub cache_sizes: Vec<u64>,
    pub compression_enabled: bool,
    pub expected_ops_per_sec: Option<f64>,
    pub max_regression_percent: f64,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            name: "default_benchmark".to_string(),
            workload_type: WorkloadType::ReadHeavy,
            duration_secs: 30,
            sample_size: 20,
            thread_counts: vec![1, 4, 8, 16],
            value_sizes: vec![64, 256, 1024, 4096],
            dataset_sizes: vec![10_000, 100_000, 1_000_000],
            cache_sizes: vec![64 * 1024 * 1024, 256 * 1024 * 1024, 1024 * 1024 * 1024],
            compression_enabled: false,
            expected_ops_per_sec: None,
            max_regression_percent: 15.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSuite {
    pub benchmarks: Vec<BenchmarkConfig>,
    pub global_config: GlobalConfig,
    pub baseline_file: String,
    pub history_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub output_dir: String,
    pub generate_html_report: bool,
    pub generate_json_report: bool,
    pub generate_csv_export: bool,
    pub track_memory_usage: bool,
    pub track_cpu_usage: bool,
    pub track_io_stats: bool,
    pub fail_on_regression: bool,
    pub statistical_significance_threshold: f64,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            output_dir: "performance_reports".to_string(),
            generate_html_report: true,
            generate_json_report: true,
            generate_csv_export: true,
            track_memory_usage: true,
            track_cpu_usage: true,
            track_io_stats: true,
            fail_on_regression: true,
            statistical_significance_threshold: 0.05,
        }
    }
}

pub struct RegressionSuite {
    config: BenchmarkSuite,
    analyzer: RegressionAnalyzer,
    report_generator: ReportGenerator,
    metrics_collector: MetricsCollector,
}

impl RegressionSuite {
    pub fn new(config_path: Option<&str>) -> Result<Self, Box<dyn std::error::Error>> {
        let config = if let Some(path) = config_path {
            let content = fs::read_to_string(path)?;
            serde_json::from_str(&content)?
        } else {
            Self::default_config()
        };

        Ok(Self {
            analyzer: RegressionAnalyzer::new(
                config.global_config.statistical_significance_threshold,
            ),
            report_generator: ReportGenerator::new(&config.global_config.output_dir)?,
            metrics_collector: MetricsCollector::new(
                config.global_config.track_memory_usage,
                config.global_config.track_cpu_usage,
                config.global_config.track_io_stats,
            ),
            config,
        })
    }

    pub fn default_config() -> BenchmarkSuite {
        BenchmarkSuite {
            benchmarks: vec![
                BenchmarkConfig {
                    name: "sequential_reads".to_string(),
                    workload_type: WorkloadType::SequentialRead,
                    expected_ops_per_sec: Some(20_000_000.0),
                    max_regression_percent: 10.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "random_reads".to_string(),
                    workload_type: WorkloadType::RandomRead,
                    expected_ops_per_sec: Some(15_000_000.0),
                    max_regression_percent: 10.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "sequential_writes".to_string(),
                    workload_type: WorkloadType::SequentialWrite,
                    expected_ops_per_sec: Some(1_100_000.0),
                    max_regression_percent: 15.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "random_writes".to_string(),
                    workload_type: WorkloadType::RandomWrite,
                    expected_ops_per_sec: Some(800_000.0),
                    max_regression_percent: 15.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "mixed_workload".to_string(),
                    workload_type: WorkloadType::Mixed { read_ratio: 0.8 },
                    expected_ops_per_sec: Some(885_000.0),
                    max_regression_percent: 20.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "transaction_throughput".to_string(),
                    workload_type: WorkloadType::Transaction { ops_per_tx: 5 },
                    expected_ops_per_sec: Some(400_000.0),
                    max_regression_percent: 20.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "concurrent_operations".to_string(),
                    workload_type: WorkloadType::Concurrent,
                    expected_ops_per_sec: Some(1_400_000.0),
                    max_regression_percent: 15.0,
                    thread_counts: vec![8, 16, 32],
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "range_scan".to_string(),
                    workload_type: WorkloadType::Scan { 
                        key_count: 1000,
                        scan_length: 100 
                    },
                    expected_ops_per_sec: Some(50_000.0),
                    max_regression_percent: 25.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "batch_operations".to_string(),
                    workload_type: WorkloadType::Batch { batch_size: 1000 },
                    expected_ops_per_sec: Some(500_000.0),
                    max_regression_percent: 20.0,
                    ..Default::default()
                },
                BenchmarkConfig {
                    name: "cache_performance".to_string(),
                    workload_type: WorkloadType::CacheTest,
                    expected_ops_per_sec: Some(25_000_000.0),
                    max_regression_percent: 10.0,
                    ..Default::default()
                },
            ],
            global_config: GlobalConfig::default(),
            baseline_file: "performance-baselines.json".to_string(),
            history_file: "performance-history.json".to_string(),
        }
    }

    pub fn run_benchmark(&mut self, benchmark: &BenchmarkConfig) -> Result<BenchmarkMetrics, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        let workload_generator = WorkloadGenerator::new();

        for &thread_count in &benchmark.thread_counts {
            for &value_size in &benchmark.value_sizes {
                for &dataset_size in &benchmark.dataset_sizes {
                    for &cache_size in &benchmark.cache_sizes {
                        let config = LightningDbConfig {
                            cache_size,
                            compression_enabled: benchmark.compression_enabled,
                            prefetch_enabled: true,
                            ..Default::default()
                        };

                        let result = self.run_single_benchmark(
                            benchmark,
                            &workload_generator,
                            thread_count,
                            value_size,
                            dataset_size,
                            config,
                        )?;

                        results.push(result);
                    }
                }
            }
        }

        let aggregated = self.aggregate_results(results);
        Ok(aggregated)
    }

    fn run_single_benchmark(
        &mut self,
        benchmark: &BenchmarkConfig,
        workload_generator: &WorkloadGenerator,
        thread_count: usize,
        value_size: usize,
        dataset_size: usize,
        config: LightningDbConfig,
    ) -> Result<BenchmarkMetrics, Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let db = Arc::new(Database::create(temp_dir.path(), config)?);

        let snapshot_before = self.metrics_collector.take_snapshot()?;
        let start_time = Instant::now();

        let ops_completed = workload_generator.run_workload(
            &benchmark.workload_type,
            db.clone(),
            thread_count,
            value_size,
            dataset_size,
            Duration::from_secs(benchmark.duration_secs),
        )?;

        let elapsed = start_time.elapsed();
        let snapshot_after = self.metrics_collector.take_snapshot()?;

        let ops_per_sec = ops_completed as f64 / elapsed.as_secs_f64();
        let avg_latency = elapsed / ops_completed as u32;

        Ok(BenchmarkMetrics {
            benchmark_name: benchmark.name.clone(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            thread_count,
            value_size,
            dataset_size,
            cache_size: config.cache_size,
            compression_enabled: config.compression_enabled,
            operations_completed: ops_completed,
            duration: elapsed,
            ops_per_sec,
            avg_latency,
            p50_latency: Duration::from_nanos(0), // TODO: Implement histogram
            p95_latency: Duration::from_nanos(0),
            p99_latency: Duration::from_nanos(0),
            memory_before: snapshot_before,
            memory_after: snapshot_after,
            peak_memory_usage: snapshot_after.rss_bytes.max(snapshot_before.rss_bytes),
            cpu_usage_percent: 0.0, // TODO: Implement CPU monitoring
            io_read_bytes: 0,
            io_write_bytes: 0,
            cache_hit_rate: 0.0,
            error_count: 0,
            timeout_count: 0,
            regression_detected: false,
            regression_percent: 0.0,
            statistical_significance: 0.0,
        })
    }

    fn aggregate_results(&self, results: Vec<BenchmarkMetrics>) -> BenchmarkMetrics {
        if results.is_empty() {
            panic!("Cannot aggregate empty results");
        }

        let total_ops: u64 = results.iter().map(|r| r.operations_completed).sum();
        let total_duration: Duration = results.iter().map(|r| r.duration).sum();
        let avg_ops_per_sec: f64 = results.iter().map(|r| r.ops_per_sec).sum::<f64>() / results.len() as f64;

        let mut base = results[0].clone();
        base.operations_completed = total_ops;
        base.duration = total_duration;
        base.ops_per_sec = avg_ops_per_sec;
        base
    }

    pub fn load_baselines(&self) -> Result<HashMap<String, BenchmarkMetrics>, Box<dyn std::error::Error>> {
        let path = Path::new(&self.config.baseline_file);
        if !path.exists() {
            return Ok(HashMap::new());
        }

        let content = fs::read_to_string(path)?;
        let baselines: HashMap<String, BenchmarkMetrics> = serde_json::from_str(&content)?;
        Ok(baselines)
    }

    pub fn save_baselines(&self, baselines: &HashMap<String, BenchmarkMetrics>) -> Result<(), Box<dyn std::error::Error>> {
        let content = serde_json::to_string_pretty(baselines)?;
        fs::write(&self.config.baseline_file, content)?;
        Ok(())
    }

    pub fn detect_regressions(
        &self,
        current: &BenchmarkMetrics,
        baseline: &BenchmarkMetrics,
    ) -> (bool, f64) {
        let regression_percent = ((baseline.ops_per_sec - current.ops_per_sec) / baseline.ops_per_sec) * 100.0;
        let is_regression = self.analyzer.is_significant_regression(
            current.ops_per_sec,
            baseline.ops_per_sec,
            10, // TODO: Make configurable
        );
        (is_regression, regression_percent)
    }

    pub fn run_full_suite(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut baselines = self.load_baselines()?;
        let mut results = Vec::new();
        let mut regressions_detected = false;

        for benchmark in &self.config.benchmarks.clone() {
            println!("Running benchmark: {}", benchmark.name);
            
            let mut metrics = self.run_benchmark(benchmark)?;

            if let Some(baseline) = baselines.get(&benchmark.name) {
                let (is_regression, regression_percent) = self.detect_regressions(&metrics, baseline);
                metrics.regression_detected = is_regression;
                metrics.regression_percent = regression_percent;

                if is_regression && regression_percent > benchmark.max_regression_percent {
                    regressions_detected = true;
                    eprintln!("REGRESSION DETECTED in {}: {:.2}% slower than baseline", 
                             benchmark.name, regression_percent);
                }
            } else {
                baselines.insert(benchmark.name.clone(), metrics.clone());
                println!("Established baseline for {}: {:.0} ops/sec", 
                        benchmark.name, metrics.ops_per_sec);
            }

            results.push(metrics);
        }

        self.save_baselines(&baselines)?;
        self.report_generator.generate_reports(&results, &baselines)?;

        if self.config.global_config.fail_on_regression && regressions_detected {
            return Err("Performance regressions detected!".into());
        }

        Ok(())
    }
}

pub fn run_regression_benchmarks(c: &mut Criterion) {
    let mut suite = RegressionSuite::new(None).expect("Failed to create regression suite");

    for benchmark in &suite.config.benchmarks.clone() {
        let mut group = c.benchmark_group(&format!("regression_{}", benchmark.name));
        group.measurement_time(Duration::from_secs(benchmark.duration_secs));
        group.sample_size(benchmark.sample_size);

        for &thread_count in &benchmark.thread_counts {
            group.bench_with_input(
                BenchmarkId::new("performance", thread_count),
                &thread_count,
                |b, &threads| {
                    let workload_generator = WorkloadGenerator::new();
                    let temp_dir = tempfile::tempdir().unwrap();
                    
                    let config = LightningDbConfig {
                        cache_size: benchmark.cache_sizes[0],
                        compression_enabled: benchmark.compression_enabled,
                        ..Default::default()
                    };
                    
                    let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());

                    b.iter(|| {
                        workload_generator.run_workload(
                            &benchmark.workload_type,
                            db.clone(),
                            threads,
                            benchmark.value_sizes[0],
                            benchmark.dataset_sizes[0],
                            Duration::from_millis(100),
                        ).unwrap();
                    });
                },
            );
        }

        group.finish();
    }
}

criterion_group!(regression_suite_benches, run_regression_benchmarks);
criterion_main!(regression_suite_benches);
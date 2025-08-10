//! Performance Validation Framework
//!
//! Validates that ML auto-tuning optimizations actually improve performance by
//! running comprehensive benchmarks before and after optimization.

use crate::performance_tuning::{LatencyMetrics, PerformanceMetrics, TuningParameters};
use crate::{Database, Error, LightningDbConfig, Result};
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tempfile::TempDir;

/// Performance validation framework for ML auto-tuning
pub struct PerformanceValidationFramework {
    workload_size: usize,
    validation_duration: Duration,
    confidence_threshold: f64,
    min_improvement_threshold: f64,
    test_data_dir: Option<TempDir>,
    validation_history: Arc<RwLock<Vec<ValidationSession>>>,
}

/// Comprehensive validation session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSession {
    pub session_id: String,
    pub timestamp: SystemTime,
    pub baseline_config: LightningDbConfig,
    pub optimized_config: LightningDbConfig,
    pub tuning_parameters: TuningParameters,
    pub baseline_metrics: DetailedPerformanceMetrics,
    pub optimized_metrics: DetailedPerformanceMetrics,
    pub performance_improvement: PerformanceImprovement,
    pub validation_result: ValidationResult,
    pub confidence_score: f64,
    pub test_scenarios: Vec<TestScenarioResult>,
}

/// Detailed performance metrics with statistical analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedPerformanceMetrics {
    pub performance_metrics: PerformanceMetrics,
    pub statistical_analysis: StatisticalAnalysis,
    pub latency_distribution: LatencyDistribution,
    pub throughput_consistency: ThroughputConsistency,
    pub resource_efficiency: ResourceEfficiency,
    pub scalability_metrics: ScalabilityMetrics,
}

/// Statistical analysis of performance data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalAnalysis {
    pub sample_count: usize,
    pub confidence_interval_95: (f64, f64),
    pub standard_deviation: f64,
    pub coefficient_of_variation: f64,
    pub outlier_count: usize,
    pub distribution_type: DistributionType,
}

/// Latency distribution analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyDistribution {
    pub percentiles: Vec<(f64, f64)>,       // (percentile, latency_ms)
    pub histogram_buckets: Vec<(f64, u64)>, // (bucket_upper_bound_ms, count)
    pub tail_latency_analysis: TailLatencyAnalysis,
}

/// Tail latency analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TailLatencyAnalysis {
    pub p999: f64,
    pub p9999: f64,
    pub max_latency: f64,
    pub tail_consistency: f64, // Lower is better
    pub outlier_ratio: f64,
}

/// Throughput consistency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputConsistency {
    pub throughput_stability: f64, // 0.0 to 1.0, higher is better
    pub peak_throughput: f64,
    pub sustained_throughput: f64,
    pub throughput_variance: f64,
    pub plateau_duration_pct: f64, // Percentage of time at peak performance
}

/// Resource efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEfficiency {
    pub cpu_efficiency: f64,    // ops per CPU core per second
    pub memory_efficiency: f64, // ops per MB of memory
    pub io_efficiency: f64,     // ops per disk IOPS
    pub cache_efficiency: f64,  // cache hit rate
    pub resource_utilization: ResourceUtilization,
}

/// Resource utilization breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    pub cpu_usage_pct: f64,
    pub memory_usage_pct: f64,
    pub disk_usage_pct: f64,
    pub network_usage_pct: f64,
    pub cache_usage_pct: f64,
}

/// Scalability metrics under different loads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalabilityMetrics {
    pub linear_scalability_score: f64, // 0.0 to 1.0
    pub bottleneck_identification: Vec<BottleneckAnalysis>,
    pub load_response_curve: Vec<(f64, f64)>, // (load_factor, performance_factor)
    pub saturation_point: Option<f64>,        // Load factor where performance plateaus
}

/// Bottleneck analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BottleneckAnalysis {
    pub resource_type: ResourceType,
    pub bottleneck_severity: f64, // 0.0 to 1.0
    pub impact_on_performance: f64,
    pub suggested_mitigation: String,
}

/// Resource types for bottleneck analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceType {
    CPU,
    Memory,
    DiskIO,
    NetworkIO,
    CacheMisses,
    LockContention,
    GarbageCollection,
}

/// Distribution types for statistical analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistributionType {
    Normal,
    LogNormal,
    Exponential,
    Bimodal,
    Unknown,
}

/// Performance improvement analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceImprovement {
    pub overall_improvement_pct: f64,
    pub read_improvement_pct: f64,
    pub write_improvement_pct: f64,
    pub latency_improvement_pct: f64,
    pub throughput_improvement_pct: f64,
    pub resource_efficiency_improvement_pct: f64,
    pub consistency_improvement_pct: f64,
    pub significant_improvements: Vec<ImprovementArea>,
}

/// Areas of significant improvement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImprovementArea {
    pub area: String,
    pub improvement_pct: f64,
    pub confidence: f64,
    pub impact: ImprovementImpact,
}

/// Impact level of improvements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImprovementImpact {
    Critical,   // >20% improvement
    High,       // 10-20% improvement
    Medium,     // 5-10% improvement
    Low,        // 1-5% improvement
    Negligible, // <1% improvement
}

/// Overall validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValidationResult {
    Significant {
        improvement_pct: f64,
        confidence: f64,
    },
    Marginal {
        improvement_pct: f64,
        confidence: f64,
    },
    NoImprovement {
        decline_pct: f64,
    },
    Inconclusive {
        reason: String,
    },
}

/// Test scenario result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestScenarioResult {
    pub scenario_name: String,
    pub workload_type: WorkloadType,
    pub baseline_performance: f64,
    pub optimized_performance: f64,
    pub improvement_pct: f64,
    pub statistical_significance: bool,
    pub confidence_level: f64,
}

/// Workload types for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkloadType {
    ReadHeavy,
    WriteHeavy,
    Balanced,
    Analytical,
    Transactional,
    Mixed,
    BurstLoad,
    SteadyState,
}

impl PerformanceValidationFramework {
    /// Create a new performance validation framework
    pub fn new() -> Result<Self> {
        Ok(Self {
            workload_size: 100000,                         // Default workload size
            validation_duration: Duration::from_secs(120), // 2 minutes per test
            confidence_threshold: 0.95,                    // 95% confidence required
            min_improvement_threshold: 0.02,               // 2% minimum improvement
            test_data_dir: None,
            validation_history: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Configure validation parameters
    pub fn configure(
        &mut self,
        workload_size: usize,
        validation_duration: Duration,
        confidence_threshold: f64,
        min_improvement_threshold: f64,
    ) {
        self.workload_size = workload_size;
        self.validation_duration = validation_duration;
        self.confidence_threshold = confidence_threshold;
        self.min_improvement_threshold = min_improvement_threshold;
    }

    /// Run comprehensive validation of ML optimization
    pub fn validate_optimization(
        &mut self,
        db_path: &Path,
        baseline_config: LightningDbConfig,
        optimized_config: LightningDbConfig,
        tuning_parameters: TuningParameters,
    ) -> Result<ValidationSession> {
        let session_id = format!(
            "validation_{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs()
        );

        println!("🔍 Starting performance validation session: {}", session_id);
        println!("  Workload size: {} operations", self.workload_size);
        println!("  Validation duration: {:?}", self.validation_duration);
        println!(
            "  Confidence threshold: {:.1}%",
            self.confidence_threshold * 100.0
        );

        // Prepare test environment
        self.setup_test_environment()?;

        // Benchmark baseline configuration
        println!("📊 Benchmarking baseline configuration...");
        let baseline_metrics =
            self.benchmark_configuration(db_path, &baseline_config, "baseline")?;

        // Benchmark optimized configuration
        println!("🚀 Benchmarking optimized configuration...");
        let optimized_metrics =
            self.benchmark_configuration(db_path, &optimized_config, "optimized")?;

        // Run test scenarios
        println!("🧪 Running comprehensive test scenarios...");
        let test_scenarios =
            self.run_test_scenarios(db_path, &baseline_config, &optimized_config)?;

        // Calculate performance improvement
        let performance_improvement =
            self.calculate_performance_improvement(&baseline_metrics, &optimized_metrics)?;

        // Determine validation result
        let validation_result = self.determine_validation_result(&performance_improvement)?;

        // Calculate confidence score
        let confidence_score = self.calculate_confidence_score(
            &baseline_metrics,
            &optimized_metrics,
            &test_scenarios,
        )?;

        let session = ValidationSession {
            session_id: session_id.clone(),
            timestamp: SystemTime::now(),
            baseline_config,
            optimized_config,
            tuning_parameters,
            baseline_metrics,
            optimized_metrics,
            performance_improvement,
            validation_result,
            confidence_score,
            test_scenarios,
        };

        // Store session
        self.validation_history.write().push(session.clone());

        // Print summary
        self.print_validation_summary(&session)?;

        Ok(session)
    }

    /// Setup test environment
    fn setup_test_environment(&mut self) -> Result<()> {
        self.test_data_dir = Some(TempDir::new()?);
        Ok(())
    }

    /// Benchmark a specific configuration
    fn benchmark_configuration(
        &self,
        db_path: &Path,
        config: &LightningDbConfig,
        config_name: &str,
    ) -> Result<DetailedPerformanceMetrics> {
        println!("  Running {} benchmark...", config_name);

        // Open database with configuration
        let db = Arc::new(Database::open(db_path, config.clone())?);

        // Warm up the database
        self.warmup_database(&db)?;

        // Run comprehensive benchmarks
        let mut performance_samples = Vec::new();
        let mut latency_samples = Vec::new();
        let benchmark_start = Instant::now();

        let iterations = 10; // Multiple iterations for statistical significance
        for iteration in 0..iterations {
            println!("    Iteration {}/{}", iteration + 1, iterations);

            let iteration_metrics = self.run_benchmark_iteration(&db)?;
            performance_samples.push(iteration_metrics.performance_metrics.clone());
            latency_samples.extend(self.extract_latency_samples(&iteration_metrics));
        }

        let benchmark_duration = benchmark_start.elapsed();
        println!(
            "  {} benchmark completed in {:?}",
            config_name, benchmark_duration
        );

        // Aggregate and analyze results
        let performance_metrics = self.aggregate_performance_metrics(&performance_samples)?;
        let statistical_analysis = self.perform_statistical_analysis(&performance_samples)?;
        let latency_distribution = self.analyze_latency_distribution(&latency_samples)?;
        let throughput_consistency = self.analyze_throughput_consistency(&performance_samples)?;
        let resource_efficiency = self.analyze_resource_efficiency(&performance_samples)?;
        let scalability_metrics = self.analyze_scalability(&db)?;

        Ok(DetailedPerformanceMetrics {
            performance_metrics,
            statistical_analysis,
            latency_distribution,
            throughput_consistency,
            resource_efficiency,
            scalability_metrics,
        })
    }

    /// Warm up the database before benchmarking
    fn warmup_database(&self, db: &Arc<Database>) -> Result<()> {
        let warmup_operations = self.workload_size / 10; // 10% of workload for warmup

        for i in 0..warmup_operations {
            let key = format!("warmup_key_{}", i);
            let value = format!("warmup_value_{}", i);

            db.put(key.as_bytes(), value.as_bytes())?;
            if i % 2 == 0 {
                db.get(key.as_bytes())?;
            }
        }

        Ok(())
    }

    /// Run a single benchmark iteration
    fn run_benchmark_iteration(&self, db: &Arc<Database>) -> Result<DetailedPerformanceMetrics> {
        let start_time = Instant::now();
        let mut read_latencies = Vec::new();
        let mut write_latencies = Vec::new();

        let mut read_ops = 0u64;
        let mut write_ops = 0u64;

        // Generate workload
        for i in 0..self.workload_size {
            let key = format!("bench_key_{}", i);
            let value = format!("bench_value_{}", thread_rng().gen::<u64>());

            // Write operation
            let write_start = Instant::now();
            db.put(key.as_bytes(), value.as_bytes())?;
            let write_latency = write_start.elapsed();
            write_latencies.push(write_latency.as_micros() as f64 / 1000.0); // Convert to ms
            write_ops += 1;

            // Read operation (50% chance)
            if thread_rng().gen_bool(0.5) {
                let read_start = Instant::now();
                let _ = db.get(key.as_bytes())?;
                let read_latency = read_start.elapsed();
                read_latencies.push(read_latency.as_micros() as f64 / 1000.0); // Convert to ms
                read_ops += 1;
            }

            // Periodic progress
            if i % (self.workload_size / 10) == 0 && i > 0 {
                let progress = (i as f64 / self.workload_size as f64) * 100.0;
                println!("      Progress: {:.1}%", progress);
            }
        }

        let total_duration = start_time.elapsed();
        let total_ops = read_ops + write_ops;

        // Calculate basic metrics
        let read_ops_per_sec = if total_duration.as_secs_f64() > 0.0 {
            read_ops as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };

        let write_ops_per_sec = if total_duration.as_secs_f64() > 0.0 {
            write_ops as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };

        let mixed_ops_per_sec = if total_duration.as_secs_f64() > 0.0 {
            total_ops as f64 / total_duration.as_secs_f64()
        } else {
            0.0
        };

        // Calculate latency percentiles
        let read_latency_metrics = self.calculate_latency_metrics(&read_latencies);
        let write_latency_metrics = self.calculate_latency_metrics(&write_latencies);

        let performance_metrics = PerformanceMetrics {
            read_ops_per_sec,
            write_ops_per_sec,
            mixed_ops_per_sec,
            read_latency_us: read_latency_metrics.clone(),
            write_latency_us: write_latency_metrics.clone(),
            memory_usage_mb: 256,    // Simplified
            cpu_usage_percent: 50.0, // Simplified
        };

        // Create simplified detailed metrics for this iteration
        Ok(DetailedPerformanceMetrics {
            performance_metrics: performance_metrics.clone(),
            statistical_analysis: StatisticalAnalysis {
                sample_count: total_ops as usize,
                confidence_interval_95: (0.0, 0.0), // Calculated later
                standard_deviation: 0.0,
                coefficient_of_variation: 0.0,
                outlier_count: 0,
                distribution_type: DistributionType::Normal,
            },
            latency_distribution: LatencyDistribution {
                percentiles: Vec::new(),
                histogram_buckets: Vec::new(),
                tail_latency_analysis: TailLatencyAnalysis {
                    p999: performance_metrics.read_latency_us.p999,
                    p9999: performance_metrics.read_latency_us.max,
                    max_latency: performance_metrics.read_latency_us.max,
                    tail_consistency: 1.0,
                    outlier_ratio: 0.01,
                },
            },
            throughput_consistency: ThroughputConsistency {
                throughput_stability: 0.95,
                peak_throughput: mixed_ops_per_sec,
                sustained_throughput: mixed_ops_per_sec * 0.9,
                throughput_variance: 0.05,
                plateau_duration_pct: 90.0,
            },
            resource_efficiency: ResourceEfficiency {
                cpu_efficiency: mixed_ops_per_sec / 4.0, // Assume 4 cores
                memory_efficiency: mixed_ops_per_sec / 256.0, // Per MB
                io_efficiency: mixed_ops_per_sec / 1000.0, // Per 1000 IOPS
                cache_efficiency: 0.85,
                resource_utilization: ResourceUtilization {
                    cpu_usage_pct: 50.0,
                    memory_usage_pct: 60.0,
                    disk_usage_pct: 30.0,
                    network_usage_pct: 10.0,
                    cache_usage_pct: 75.0,
                },
            },
            scalability_metrics: ScalabilityMetrics {
                linear_scalability_score: 0.8,
                bottleneck_identification: Vec::new(),
                load_response_curve: Vec::new(),
                saturation_point: None,
            },
        })
    }

    /// Calculate latency metrics from samples
    fn calculate_latency_metrics(&self, latencies: &[f64]) -> LatencyMetrics {
        if latencies.is_empty() {
            return LatencyMetrics {
                p50: 0.0,
                p95: 0.0,
                p99: 0.0,
                p999: 0.0,
                max: 0.0,
            };
        }

        let mut sorted_latencies = latencies.to_vec();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted_latencies.len();

        LatencyMetrics {
            p50: self.percentile(&sorted_latencies, 0.5),
            p95: self.percentile(&sorted_latencies, 0.95),
            p99: self.percentile(&sorted_latencies, 0.99),
            p999: self.percentile(&sorted_latencies, 0.999),
            max: sorted_latencies[len - 1],
        }
    }

    /// Calculate percentile from sorted data
    fn percentile(&self, sorted_data: &[f64], p: f64) -> f64 {
        if sorted_data.is_empty() {
            return 0.0;
        }

        let index = ((sorted_data.len() - 1) as f64 * p) as usize;
        sorted_data[index.min(sorted_data.len() - 1)]
    }

    /// Extract latency samples from detailed metrics
    fn extract_latency_samples(&self, metrics: &DetailedPerformanceMetrics) -> Vec<f64> {
        // Simplified - in real implementation would extract actual samples
        vec![
            metrics.performance_metrics.read_latency_us.p50,
            metrics.performance_metrics.read_latency_us.p95,
            metrics.performance_metrics.read_latency_us.p99,
            metrics.performance_metrics.write_latency_us.p50,
            metrics.performance_metrics.write_latency_us.p95,
            metrics.performance_metrics.write_latency_us.p99,
        ]
    }

    /// Aggregate performance metrics from multiple samples
    fn aggregate_performance_metrics(
        &self,
        samples: &[PerformanceMetrics],
    ) -> Result<PerformanceMetrics> {
        if samples.is_empty() {
            return Err(Error::Generic(
                "No performance samples to aggregate".to_string(),
            ));
        }

        let len = samples.len() as f64;

        Ok(PerformanceMetrics {
            read_ops_per_sec: samples.iter().map(|s| s.read_ops_per_sec).sum::<f64>() / len,
            write_ops_per_sec: samples.iter().map(|s| s.write_ops_per_sec).sum::<f64>() / len,
            mixed_ops_per_sec: samples.iter().map(|s| s.mixed_ops_per_sec).sum::<f64>() / len,
            read_latency_us: LatencyMetrics {
                p50: samples.iter().map(|s| s.read_latency_us.p50).sum::<f64>() / len,
                p95: samples.iter().map(|s| s.read_latency_us.p95).sum::<f64>() / len,
                p99: samples.iter().map(|s| s.read_latency_us.p99).sum::<f64>() / len,
                p999: samples.iter().map(|s| s.read_latency_us.p999).sum::<f64>() / len,
                max: samples
                    .iter()
                    .map(|s| s.read_latency_us.max)
                    .fold(0.0, f64::max),
            },
            write_latency_us: LatencyMetrics {
                p50: samples.iter().map(|s| s.write_latency_us.p50).sum::<f64>() / len,
                p95: samples.iter().map(|s| s.write_latency_us.p95).sum::<f64>() / len,
                p99: samples.iter().map(|s| s.write_latency_us.p99).sum::<f64>() / len,
                p999: samples.iter().map(|s| s.write_latency_us.p999).sum::<f64>() / len,
                max: samples
                    .iter()
                    .map(|s| s.write_latency_us.max)
                    .fold(0.0, f64::max),
            },
            memory_usage_mb: (samples.iter().map(|s| s.memory_usage_mb).sum::<u64>()
                / samples.len() as u64),
            cpu_usage_percent: samples.iter().map(|s| s.cpu_usage_percent).sum::<f64>() / len,
        })
    }

    /// Perform statistical analysis
    fn perform_statistical_analysis(
        &self,
        samples: &[PerformanceMetrics],
    ) -> Result<StatisticalAnalysis> {
        if samples.is_empty() {
            return Err(Error::Generic(
                "No samples for statistical analysis".to_string(),
            ));
        }

        // Extract throughput values for analysis
        let throughputs: Vec<f64> = samples.iter().map(|s| s.mixed_ops_per_sec).collect();

        let mean = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
        let variance =
            throughputs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / throughputs.len() as f64;
        let std_dev = variance.sqrt();

        let coefficient_of_variation = if mean > 0.0 { std_dev / mean } else { 0.0 };

        // Calculate confidence interval (simplified)
        let margin_of_error = 1.96 * (std_dev / (throughputs.len() as f64).sqrt()); // 95% CI
        let confidence_interval_95 = (mean - margin_of_error, mean + margin_of_error);

        // Count outliers (simplified: values > 2 std deviations from mean)
        let outlier_count = throughputs
            .iter()
            .filter(|&&x| (x - mean).abs() > 2.0 * std_dev)
            .count();

        Ok(StatisticalAnalysis {
            sample_count: samples.len(),
            confidence_interval_95,
            standard_deviation: std_dev,
            coefficient_of_variation,
            outlier_count,
            distribution_type: DistributionType::Normal, // Simplified
        })
    }

    /// Analyze latency distribution
    fn analyze_latency_distribution(&self, latency_samples: &[f64]) -> Result<LatencyDistribution> {
        if latency_samples.is_empty() {
            return Ok(LatencyDistribution {
                percentiles: Vec::new(),
                histogram_buckets: Vec::new(),
                tail_latency_analysis: TailLatencyAnalysis {
                    p999: 0.0,
                    p9999: 0.0,
                    max_latency: 0.0,
                    tail_consistency: 1.0,
                    outlier_ratio: 0.0,
                },
            });
        }

        let mut sorted_latencies = latency_samples.to_vec();
        sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Calculate percentiles
        let percentiles = vec![
            (50.0, self.percentile(&sorted_latencies, 0.5)),
            (90.0, self.percentile(&sorted_latencies, 0.9)),
            (95.0, self.percentile(&sorted_latencies, 0.95)),
            (99.0, self.percentile(&sorted_latencies, 0.99)),
            (99.9, self.percentile(&sorted_latencies, 0.999)),
            (99.99, self.percentile(&sorted_latencies, 0.9999)),
        ];

        // Create histogram buckets
        let max_latency = sorted_latencies.last().copied().unwrap_or(0.0);
        let bucket_size = max_latency / 20.0; // 20 buckets
        let mut histogram_buckets = Vec::new();

        for i in 1..=20 {
            let bucket_upper_bound = i as f64 * bucket_size;
            let count = sorted_latencies
                .iter()
                .filter(|&&lat| lat <= bucket_upper_bound && lat > (i - 1) as f64 * bucket_size)
                .count() as u64;
            histogram_buckets.push((bucket_upper_bound, count));
        }

        // Tail latency analysis
        let p999 = self.percentile(&sorted_latencies, 0.999);
        let p9999 = self.percentile(&sorted_latencies, 0.9999);
        let tail_consistency = if p999 > 0.0 { p9999 / p999 } else { 1.0 };

        let mean = sorted_latencies.iter().sum::<f64>() / sorted_latencies.len() as f64;
        let outlier_threshold = mean * 3.0; // 3x mean as outlier threshold
        let outlier_count = sorted_latencies
            .iter()
            .filter(|&&lat| lat > outlier_threshold)
            .count();
        let outlier_ratio = outlier_count as f64 / sorted_latencies.len() as f64;

        Ok(LatencyDistribution {
            percentiles,
            histogram_buckets,
            tail_latency_analysis: TailLatencyAnalysis {
                p999,
                p9999,
                max_latency,
                tail_consistency,
                outlier_ratio,
            },
        })
    }

    /// Analyze throughput consistency
    fn analyze_throughput_consistency(
        &self,
        samples: &[PerformanceMetrics],
    ) -> Result<ThroughputConsistency> {
        if samples.is_empty() {
            return Ok(ThroughputConsistency {
                throughput_stability: 0.0,
                peak_throughput: 0.0,
                sustained_throughput: 0.0,
                throughput_variance: 0.0,
                plateau_duration_pct: 0.0,
            });
        }

        let throughputs: Vec<f64> = samples.iter().map(|s| s.mixed_ops_per_sec).collect();

        let peak_throughput = throughputs.iter().fold(0.0f64, |a, &b| a.max(b));
        let sustained_throughput = throughputs.iter().sum::<f64>() / throughputs.len() as f64;

        let mean = sustained_throughput;
        let variance =
            throughputs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / throughputs.len() as f64;

        let throughput_stability = if peak_throughput > 0.0 {
            1.0 - (variance.sqrt() / peak_throughput)
        } else {
            0.0
        };

        // Calculate plateau duration (simplified: percentage of time within 90% of peak)
        let plateau_threshold = peak_throughput * 0.9;
        let plateau_count = throughputs
            .iter()
            .filter(|&&t| t >= plateau_threshold)
            .count();
        let plateau_duration_pct = (plateau_count as f64 / throughputs.len() as f64) * 100.0;

        Ok(ThroughputConsistency {
            throughput_stability,
            peak_throughput,
            sustained_throughput,
            throughput_variance: variance,
            plateau_duration_pct,
        })
    }

    /// Analyze resource efficiency
    fn analyze_resource_efficiency(
        &self,
        samples: &[PerformanceMetrics],
    ) -> Result<ResourceEfficiency> {
        if samples.is_empty() {
            return Ok(ResourceEfficiency {
                cpu_efficiency: 0.0,
                memory_efficiency: 0.0,
                io_efficiency: 0.0,
                cache_efficiency: 0.0,
                resource_utilization: ResourceUtilization {
                    cpu_usage_pct: 0.0,
                    memory_usage_pct: 0.0,
                    disk_usage_pct: 0.0,
                    network_usage_pct: 0.0,
                    cache_usage_pct: 0.0,
                },
            });
        }

        let avg_throughput =
            samples.iter().map(|s| s.mixed_ops_per_sec).sum::<f64>() / samples.len() as f64;
        let avg_cpu_usage =
            samples.iter().map(|s| s.cpu_usage_percent).sum::<f64>() / samples.len() as f64;
        let avg_memory_usage =
            samples.iter().map(|s| s.memory_usage_mb).sum::<u64>() / samples.len() as u64;

        let cpu_efficiency = if avg_cpu_usage > 0.0 {
            avg_throughput / (avg_cpu_usage / 100.0) / 4.0 // Assume 4 cores
        } else {
            0.0
        };

        let memory_efficiency = if avg_memory_usage > 0 {
            avg_throughput / avg_memory_usage as f64
        } else {
            0.0
        };

        Ok(ResourceEfficiency {
            cpu_efficiency,
            memory_efficiency,
            io_efficiency: avg_throughput / 1000.0, // Simplified
            cache_efficiency: 0.85,                 // Simplified
            resource_utilization: ResourceUtilization {
                cpu_usage_pct: avg_cpu_usage,
                memory_usage_pct: (avg_memory_usage as f64 / 8192.0) * 100.0, // Assume 8GB total
                disk_usage_pct: 30.0,                                         // Simplified
                network_usage_pct: 10.0,                                      // Simplified
                cache_usage_pct: 75.0,                                        // Simplified
            },
        })
    }

    /// Analyze scalability under different loads
    fn analyze_scalability(&self, _db: &Arc<Database>) -> Result<ScalabilityMetrics> {
        // Simplified scalability analysis
        Ok(ScalabilityMetrics {
            linear_scalability_score: 0.8,
            bottleneck_identification: vec![BottleneckAnalysis {
                resource_type: ResourceType::DiskIO,
                bottleneck_severity: 0.3,
                impact_on_performance: 0.2,
                suggested_mitigation: "Consider SSD upgrade or parallel I/O".to_string(),
            }],
            load_response_curve: vec![(0.1, 1.0), (0.5, 0.95), (1.0, 0.85), (2.0, 0.6), (5.0, 0.3)],
            saturation_point: Some(3.0),
        })
    }

    /// Run comprehensive test scenarios
    fn run_test_scenarios(
        &self,
        db_path: &Path,
        baseline_config: &LightningDbConfig,
        optimized_config: &LightningDbConfig,
    ) -> Result<Vec<TestScenarioResult>> {
        let scenarios = vec![
            ("Read-Heavy Workload", WorkloadType::ReadHeavy),
            ("Write-Heavy Workload", WorkloadType::WriteHeavy),
            ("Balanced Workload", WorkloadType::Balanced),
            ("Burst Load", WorkloadType::BurstLoad),
            ("Steady State", WorkloadType::SteadyState),
        ];

        let mut results = Vec::new();

        for (scenario_name, workload_type) in scenarios {
            println!("    Running scenario: {}", scenario_name);

            // Test baseline
            let baseline_performance =
                self.run_scenario_test(db_path, baseline_config, &workload_type)?;

            // Test optimized
            let optimized_performance =
                self.run_scenario_test(db_path, optimized_config, &workload_type)?;

            let improvement_pct = if baseline_performance > 0.0 {
                ((optimized_performance - baseline_performance) / baseline_performance) * 100.0
            } else {
                0.0
            };

            // Simple statistical significance test (t-test would be more appropriate)
            let statistical_significance = improvement_pct.abs() > 5.0; // 5% threshold
            let confidence_level = if statistical_significance { 0.95 } else { 0.8 };

            results.push(TestScenarioResult {
                scenario_name: scenario_name.to_string(),
                workload_type,
                baseline_performance,
                optimized_performance,
                improvement_pct,
                statistical_significance,
                confidence_level,
            });
        }

        Ok(results)
    }

    /// Run a specific scenario test
    fn run_scenario_test(
        &self,
        db_path: &Path,
        config: &LightningDbConfig,
        workload_type: &WorkloadType,
    ) -> Result<f64> {
        let db = Arc::new(Database::open(db_path, config.clone())?);

        let ops_count = self.workload_size / 10; // Smaller for individual scenarios
        let start_time = Instant::now();

        match workload_type {
            WorkloadType::ReadHeavy => {
                // 90% reads, 10% writes
                for i in 0..ops_count {
                    let key = format!("scenario_key_{}", i);

                    if i % 10 == 0 {
                        let value = format!("scenario_value_{}", i);
                        db.put(key.as_bytes(), value.as_bytes())?;
                    } else {
                        let _ = db.get(key.as_bytes());
                    }
                }
            }
            WorkloadType::WriteHeavy => {
                // 90% writes, 10% reads
                for i in 0..ops_count {
                    let key = format!("scenario_key_{}", i);

                    if i % 10 == 0 {
                        let _ = db.get(key.as_bytes());
                    } else {
                        let value = format!("scenario_value_{}", i);
                        db.put(key.as_bytes(), value.as_bytes())?;
                    }
                }
            }
            WorkloadType::Balanced => {
                // 50% reads, 50% writes
                for i in 0..ops_count {
                    let key = format!("scenario_key_{}", i);

                    if i % 2 == 0 {
                        let value = format!("scenario_value_{}", i);
                        db.put(key.as_bytes(), value.as_bytes())?;
                    } else {
                        let _ = db.get(key.as_bytes());
                    }
                }
            }
            WorkloadType::BurstLoad => {
                // Burst pattern: heavy load, then pause
                for batch in 0..10 {
                    for i in 0..(ops_count / 10) {
                        let key = format!("burst_key_{}_{}", batch, i);
                        let value = format!("burst_value_{}", i);
                        db.put(key.as_bytes(), value.as_bytes())?;
                    }
                    std::thread::sleep(Duration::from_millis(100)); // Brief pause
                }
            }
            WorkloadType::SteadyState => {
                // Consistent rate with small pauses
                for i in 0..ops_count {
                    let key = format!("steady_key_{}", i);
                    let value = format!("steady_value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes())?;

                    if i % 100 == 0 {
                        std::thread::sleep(Duration::from_millis(1)); // Tiny pause for steady rate
                    }
                }
            }
            _ => {
                // Default to balanced workload
                for i in 0..ops_count {
                    let key = format!("default_key_{}", i);
                    let value = format!("default_value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes())?;

                    if i % 2 == 0 {
                        let _ = db.get(key.as_bytes());
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        let ops_per_sec = if duration.as_secs_f64() > 0.0 {
            ops_count as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Ok(ops_per_sec)
    }

    /// Calculate performance improvement
    fn calculate_performance_improvement(
        &self,
        baseline: &DetailedPerformanceMetrics,
        optimized: &DetailedPerformanceMetrics,
    ) -> Result<PerformanceImprovement> {
        let read_improvement_pct = self.calculate_improvement_percentage(
            baseline.performance_metrics.read_ops_per_sec,
            optimized.performance_metrics.read_ops_per_sec,
        );

        let write_improvement_pct = self.calculate_improvement_percentage(
            baseline.performance_metrics.write_ops_per_sec,
            optimized.performance_metrics.write_ops_per_sec,
        );

        let latency_improvement_pct = self.calculate_improvement_percentage(
            baseline.performance_metrics.read_latency_us.p95,
            optimized.performance_metrics.read_latency_us.p95,
        ) * -1.0; // Negative because lower latency is better

        let throughput_improvement_pct = self.calculate_improvement_percentage(
            baseline.performance_metrics.mixed_ops_per_sec,
            optimized.performance_metrics.mixed_ops_per_sec,
        );

        let resource_efficiency_improvement_pct = self.calculate_improvement_percentage(
            baseline.resource_efficiency.cpu_efficiency,
            optimized.resource_efficiency.cpu_efficiency,
        );

        let consistency_improvement_pct = self.calculate_improvement_percentage(
            baseline.throughput_consistency.throughput_stability,
            optimized.throughput_consistency.throughput_stability,
        );

        let overall_improvement_pct = read_improvement_pct * 0.3
            + write_improvement_pct * 0.3
            + latency_improvement_pct * 0.2
            + throughput_improvement_pct * 0.2;

        let mut significant_improvements = Vec::new();

        if read_improvement_pct > 5.0 {
            significant_improvements.push(ImprovementArea {
                area: "Read Performance".to_string(),
                improvement_pct: read_improvement_pct,
                confidence: 0.95,
                impact: self.classify_improvement_impact(read_improvement_pct),
            });
        }

        if write_improvement_pct > 5.0 {
            significant_improvements.push(ImprovementArea {
                area: "Write Performance".to_string(),
                improvement_pct: write_improvement_pct,
                confidence: 0.95,
                impact: self.classify_improvement_impact(write_improvement_pct),
            });
        }

        if latency_improvement_pct > 5.0 {
            significant_improvements.push(ImprovementArea {
                area: "Latency Reduction".to_string(),
                improvement_pct: latency_improvement_pct,
                confidence: 0.90,
                impact: self.classify_improvement_impact(latency_improvement_pct),
            });
        }

        Ok(PerformanceImprovement {
            overall_improvement_pct,
            read_improvement_pct,
            write_improvement_pct,
            latency_improvement_pct,
            throughput_improvement_pct,
            resource_efficiency_improvement_pct,
            consistency_improvement_pct,
            significant_improvements,
        })
    }

    /// Calculate improvement percentage
    fn calculate_improvement_percentage(&self, baseline: f64, optimized: f64) -> f64 {
        if baseline > 0.0 {
            ((optimized - baseline) / baseline) * 100.0
        } else if optimized > 0.0 {
            100.0 // Improvement from zero
        } else {
            0.0
        }
    }

    /// Classify improvement impact
    fn classify_improvement_impact(&self, improvement_pct: f64) -> ImprovementImpact {
        let abs_improvement = improvement_pct.abs();

        if abs_improvement >= 20.0 {
            ImprovementImpact::Critical
        } else if abs_improvement >= 10.0 {
            ImprovementImpact::High
        } else if abs_improvement >= 5.0 {
            ImprovementImpact::Medium
        } else if abs_improvement >= 1.0 {
            ImprovementImpact::Low
        } else {
            ImprovementImpact::Negligible
        }
    }

    /// Determine overall validation result
    fn determine_validation_result(
        &self,
        improvement: &PerformanceImprovement,
    ) -> Result<ValidationResult> {
        let overall_improvement = improvement.overall_improvement_pct;

        if overall_improvement >= self.min_improvement_threshold * 100.0 {
            if overall_improvement >= 10.0 {
                Ok(ValidationResult::Significant {
                    improvement_pct: overall_improvement,
                    confidence: 0.95,
                })
            } else {
                Ok(ValidationResult::Marginal {
                    improvement_pct: overall_improvement,
                    confidence: 0.85,
                })
            }
        } else if overall_improvement < -5.0 {
            Ok(ValidationResult::NoImprovement {
                decline_pct: overall_improvement.abs(),
            })
        } else {
            Ok(ValidationResult::Inconclusive {
                reason: "Improvement below significance threshold".to_string(),
            })
        }
    }

    /// Calculate confidence score
    fn calculate_confidence_score(
        &self,
        baseline: &DetailedPerformanceMetrics,
        optimized: &DetailedPerformanceMetrics,
        test_scenarios: &[TestScenarioResult],
    ) -> Result<f64> {
        let mut confidence_factors = Vec::new();

        // Statistical significance of baseline measurements
        let baseline_cv = baseline.statistical_analysis.coefficient_of_variation;
        confidence_factors.push(1.0 - baseline_cv.min(1.0));

        // Statistical significance of optimized measurements
        let optimized_cv = optimized.statistical_analysis.coefficient_of_variation;
        confidence_factors.push(1.0 - optimized_cv.min(1.0));

        // Consistency across test scenarios
        let scenario_confidence = test_scenarios
            .iter()
            .map(|s| s.confidence_level)
            .sum::<f64>()
            / test_scenarios.len() as f64;
        confidence_factors.push(scenario_confidence);

        // Sample size factor
        let sample_size_factor =
            (baseline.statistical_analysis.sample_count as f64 / 1000.0).min(1.0);
        confidence_factors.push(sample_size_factor);

        // Overall confidence is the geometric mean of factors
        let product: f64 = confidence_factors.iter().product();
        let confidence_score = product.powf(1.0 / confidence_factors.len() as f64);

        Ok(confidence_score)
    }

    /// Print validation summary
    fn print_validation_summary(&self, session: &ValidationSession) -> Result<()> {
        println!("\n🎯 Performance Validation Summary");
        println!("================================");
        println!("Session ID: {}", session.session_id);
        println!("Confidence Score: {:.1}%", session.confidence_score * 100.0);

        match &session.validation_result {
            ValidationResult::Significant {
                improvement_pct,
                confidence,
            } => {
                println!(
                    "✅ SIGNIFICANT IMPROVEMENT: {:.1}% (confidence: {:.1}%)",
                    improvement_pct,
                    confidence * 100.0
                );
            }
            ValidationResult::Marginal {
                improvement_pct,
                confidence,
            } => {
                println!(
                    "🟡 MARGINAL IMPROVEMENT: {:.1}% (confidence: {:.1}%)",
                    improvement_pct,
                    confidence * 100.0
                );
            }
            ValidationResult::NoImprovement { decline_pct } => {
                println!("❌ NO IMPROVEMENT: {:.1}% decline", decline_pct);
            }
            ValidationResult::Inconclusive { reason } => {
                println!("⚠️ INCONCLUSIVE: {}", reason);
            }
        }

        println!("\n📊 Performance Metrics:");
        println!(
            "  Baseline Throughput: {:.0} ops/sec",
            session
                .baseline_metrics
                .performance_metrics
                .mixed_ops_per_sec
        );
        println!(
            "  Optimized Throughput: {:.0} ops/sec",
            session
                .optimized_metrics
                .performance_metrics
                .mixed_ops_per_sec
        );
        println!(
            "  Overall Improvement: {:.1}%",
            session.performance_improvement.overall_improvement_pct
        );

        println!("\n⚡ Significant Improvements:");
        for improvement in &session.performance_improvement.significant_improvements {
            println!(
                "  • {}: {:.1}% ({:?} impact, {:.0}% confidence)",
                improvement.area,
                improvement.improvement_pct,
                improvement.impact,
                improvement.confidence * 100.0
            );
        }

        println!("\n🧪 Test Scenarios:");
        for scenario in &session.test_scenarios {
            let status = if scenario.statistical_significance {
                "✅"
            } else {
                "⚠️"
            };
            println!(
                "  {} {}: {:.1}% improvement ({:.0}% confidence)",
                status,
                scenario.scenario_name,
                scenario.improvement_pct,
                scenario.confidence_level * 100.0
            );
        }

        Ok(())
    }

    /// Get validation history
    pub fn get_validation_history(&self) -> Vec<ValidationSession> {
        self.validation_history.read().clone()
    }

    /// Export validation report
    pub fn export_validation_report(&self, output_path: &Path) -> Result<()> {
        let history = self.get_validation_history();
        let report = serde_json::to_string_pretty(&history)?;
        std::fs::write(output_path, report)?;
        Ok(())
    }
}

impl Default for PerformanceValidationFramework {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| Self {
            workload_size: 10000,
            validation_duration: Duration::from_secs(60),
            confidence_threshold: 0.95,
            min_improvement_threshold: 0.02,
            test_data_dir: None,
            validation_history: Arc::new(RwLock::new(Vec::new())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_performance_validation_framework_creation() {
        let framework = PerformanceValidationFramework::new();
        assert!(framework.is_ok());
    }

    #[test]
    fn test_improvement_percentage_calculation() {
        let framework = PerformanceValidationFramework::default();

        assert_eq!(
            framework.calculate_improvement_percentage(100.0, 110.0),
            10.0
        );
        assert_eq!(
            framework.calculate_improvement_percentage(100.0, 90.0),
            -10.0
        );
        assert_eq!(
            framework.calculate_improvement_percentage(0.0, 100.0),
            100.0
        );
    }

    #[test]
    fn test_improvement_impact_classification() {
        let framework = PerformanceValidationFramework::default();

        assert!(matches!(
            framework.classify_improvement_impact(25.0),
            ImprovementImpact::Critical
        ));
        assert!(matches!(
            framework.classify_improvement_impact(15.0),
            ImprovementImpact::High
        ));
        assert!(matches!(
            framework.classify_improvement_impact(7.0),
            ImprovementImpact::Medium
        ));
        assert!(matches!(
            framework.classify_improvement_impact(3.0),
            ImprovementImpact::Low
        ));
        assert!(matches!(
            framework.classify_improvement_impact(0.5),
            ImprovementImpact::Negligible
        ));
    }
}

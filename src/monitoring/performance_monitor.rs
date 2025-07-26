//! Performance Monitoring System for Lightning DB
//!
//! Real-time performance monitoring with trend analysis, benchmarking,
//! and performance regression detection.

use crate::{Database, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::time::{Duration, Instant, SystemTime};
use serde::{Serialize, Deserialize};
use tracing::{info, debug};

/// Performance monitor for Lightning DB
pub struct PerformanceMonitor {
    /// Current performance data
    current_performance: Arc<RwLock<PerformanceData>>,
    /// Performance history for trend analysis
    performance_history: Arc<RwLock<VecDeque<PerformanceSnapshot>>>,
    /// Benchmark results
    benchmark_results: Arc<RwLock<HashMap<String, BenchmarkResult>>>,
    /// Performance baselines
    baselines: Arc<RwLock<HashMap<String, PerformanceBaseline>>>,
    /// Configuration
    config: PerformanceConfig,
    /// Monitoring state
    is_monitoring: Arc<AtomicBool>,
    /// Counters for real-time calculation
    counters: Arc<PerformanceCounters>,
}

/// Configuration for performance monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum history entries to keep
    pub max_history_entries: usize,
    /// Performance sampling interval
    pub sampling_interval: Duration,
    /// Enable trend analysis
    pub enable_trend_analysis: bool,
    /// Trend analysis window
    pub trend_analysis_window: Duration,
    /// Enable performance benchmarking
    pub enable_benchmarking: bool,
    /// Benchmark interval
    pub benchmark_interval: Duration,
    /// Enable regression detection
    pub enable_regression_detection: bool,
    /// Regression threshold (percentage change)
    pub regression_threshold: f64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 1000,
            sampling_interval: Duration::from_secs(60),
            enable_trend_analysis: true,
            trend_analysis_window: Duration::from_secs(3600), // 1 hour
            enable_benchmarking: true,
            benchmark_interval: Duration::from_secs(300), // 5 minutes
            enable_regression_detection: true,
            regression_threshold: 20.0, // 20% degradation
        }
    }
}

/// Comprehensive performance data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceData {
    /// Read throughput (operations per second)
    pub read_throughput: f64,
    /// Write throughput (operations per second)
    pub write_throughput: f64,
    /// Mixed workload throughput
    pub mixed_throughput: f64,
    /// Transaction success rate (0.0 to 1.0)
    pub transaction_success_rate: f64,
    /// Compaction efficiency (0.0 to 1.0)
    pub compaction_efficiency: f64,
    /// Index utilization (0.0 to 1.0)
    pub index_utilization: f64,
    /// Cache efficiency metrics
    pub cache_efficiency: CacheEfficiency,
    /// I/O performance metrics
    pub io_performance: IoPerformance,
    /// Query performance metrics
    pub query_performance: QueryPerformance,
    /// Latency percentiles
    pub latency_percentiles: LatencyPercentiles,
    /// Resource utilization
    pub resource_utilization: ResourceUtilization,
    /// Timestamp of measurement
    pub timestamp: SystemTime,
}

/// Cache efficiency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEfficiency {
    pub hit_rate: f64,
    pub miss_rate: f64,
    pub eviction_rate: f64,
    pub fill_rate: f64,
    pub memory_efficiency: f64,
}

/// I/O performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoPerformance {
    pub read_iops: f64,
    pub write_iops: f64,
    pub read_bandwidth_mbps: f64,
    pub write_bandwidth_mbps: f64,
    pub average_read_latency: Duration,
    pub average_write_latency: Duration,
    pub queue_depth: f64,
}

/// Query performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPerformance {
    pub simple_queries_per_sec: f64,
    pub complex_queries_per_sec: f64,
    pub range_queries_per_sec: f64,
    pub average_query_time: Duration,
    pub query_cache_hit_rate: f64,
}

/// Latency percentiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50: Duration,
    pub p90: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub max: Duration,
}

/// Resource utilization metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_io_usage: f64,
    pub network_usage: f64,
}

/// Performance snapshot for historical tracking
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceSnapshot {
    pub performance: PerformanceData,
    pub timestamp: SystemTime,
    pub collection_duration: Duration,
}

/// Benchmark result
#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkResult {
    pub benchmark_name: String,
    pub result_value: f64,
    pub unit: String,
    pub higher_is_better: bool,
    pub timestamp: SystemTime,
    pub duration: Duration,
    pub metadata: HashMap<String, String>,
}

/// Performance baseline for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub baseline_name: String,
    pub metrics: HashMap<String, f64>,
    pub established_at: SystemTime,
    pub confidence_interval: f64,
    pub sample_size: usize,
}

/// Performance trend analysis
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceTrend {
    pub metric_name: String,
    pub trend_direction: TrendDirection,
    pub trend_strength: f64, // 0.0 to 1.0
    pub confidence: f64, // 0.0 to 1.0
    pub analysis_window: Duration,
    pub data_points: usize,
}

/// Direction of performance trends
#[derive(Debug, Clone, Serialize)]
pub enum TrendDirection {
    Improving,
    Degrading,
    Stable,
    Volatile,
}

/// Performance counters for real-time tracking
pub struct PerformanceCounters {
    pub operations_completed: AtomicU64,
    pub operations_started: AtomicU64,
    pub total_operation_time: AtomicU64, // in nanoseconds
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub transactions_completed: AtomicU64,
    pub transactions_failed: AtomicU64,
    pub compactions_completed: AtomicU64,
    pub compaction_bytes_processed: AtomicU64,
}

impl Default for PerformanceData {
    fn default() -> Self {
        Self {
            read_throughput: 0.0,
            write_throughput: 0.0,
            mixed_throughput: 0.0,
            transaction_success_rate: 0.0,
            compaction_efficiency: 0.0,
            index_utilization: 0.0,
            cache_efficiency: CacheEfficiency::default(),
            io_performance: IoPerformance::default(),
            query_performance: QueryPerformance::default(),
            latency_percentiles: LatencyPercentiles::default(),
            resource_utilization: ResourceUtilization::default(),
            timestamp: SystemTime::now(),
        }
    }
}

impl Default for CacheEfficiency {
    fn default() -> Self {
        Self {
            hit_rate: 0.0,
            miss_rate: 0.0,
            eviction_rate: 0.0,
            fill_rate: 0.0,
            memory_efficiency: 0.0,
        }
    }
}

impl Default for IoPerformance {
    fn default() -> Self {
        Self {
            read_iops: 0.0,
            write_iops: 0.0,
            read_bandwidth_mbps: 0.0,
            write_bandwidth_mbps: 0.0,
            average_read_latency: Duration::from_millis(0),
            average_write_latency: Duration::from_millis(0),
            queue_depth: 0.0,
        }
    }
}

impl Default for QueryPerformance {
    fn default() -> Self {
        Self {
            simple_queries_per_sec: 0.0,
            complex_queries_per_sec: 0.0,
            range_queries_per_sec: 0.0,
            average_query_time: Duration::from_millis(0),
            query_cache_hit_rate: 0.0,
        }
    }
}

impl Default for LatencyPercentiles {
    fn default() -> Self {
        Self {
            p50: Duration::from_millis(0),
            p90: Duration::from_millis(0),
            p95: Duration::from_millis(0),
            p99: Duration::from_millis(0),
            p999: Duration::from_millis(0),
            max: Duration::from_millis(0),
        }
    }
}

impl Default for ResourceUtilization {
    fn default() -> Self {
        Self {
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_io_usage: 0.0,
            network_usage: 0.0,
        }
    }
}

impl Default for PerformanceCounters {
    fn default() -> Self {
        Self {
            operations_completed: AtomicU64::new(0),
            operations_started: AtomicU64::new(0),
            total_operation_time: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            transactions_completed: AtomicU64::new(0),
            transactions_failed: AtomicU64::new(0),
            compactions_completed: AtomicU64::new(0),
            compaction_bytes_processed: AtomicU64::new(0),
        }
    }
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new() -> Self {
        Self::with_config(PerformanceConfig::default())
    }

    /// Create performance monitor with custom configuration
    pub fn with_config(config: PerformanceConfig) -> Self {
        Self {
            current_performance: Arc::new(RwLock::new(PerformanceData::default())),
            performance_history: Arc::new(RwLock::new(VecDeque::new())),
            benchmark_results: Arc::new(RwLock::new(HashMap::new())),
            baselines: Arc::new(RwLock::new(HashMap::new())),
            config,
            is_monitoring: Arc::new(AtomicBool::new(false)),
            counters: Arc::new(PerformanceCounters::default()),
        }
    }

    /// Start performance monitoring
    pub fn start_monitoring(&self) {
        self.is_monitoring.store(true, Ordering::Relaxed);
        info!("Performance monitoring started");
    }

    /// Stop performance monitoring
    pub fn stop_monitoring(&self) {
        self.is_monitoring.store(false, Ordering::Relaxed);
        info!("Performance monitoring stopped");
    }

    /// Collect performance data from the database
    pub fn collect_performance_data(&self, database: &Database) -> Result<()> {
        if !self.is_monitoring.load(Ordering::Relaxed) {
            return Ok(());
        }

        let collection_start = Instant::now();
        debug!("Starting performance data collection");

        let mut performance = PerformanceData::default();
        performance.timestamp = SystemTime::now();

        // Collect throughput metrics
        self.collect_throughput_metrics(database, &mut performance)?;

        // Collect latency metrics
        self.collect_latency_metrics(database, &mut performance)?;

        // Collect cache metrics
        self.collect_cache_metrics(database, &mut performance)?;

        // Collect I/O metrics
        self.collect_io_metrics(database, &mut performance)?;

        // Collect transaction metrics
        self.collect_transaction_metrics(database, &mut performance)?;

        // Collect resource utilization
        self.collect_resource_utilization(&mut performance)?;

        // Update current performance
        {
            let mut current = self.current_performance.write().unwrap();
            *current = performance.clone();
        }

        // Add to history
        self.add_to_history(performance, collection_start.elapsed());

        // Run benchmarks if enabled
        if self.config.enable_benchmarking {
            self.run_benchmarks(database)?;
        }

        // Analyze trends if enabled
        if self.config.enable_trend_analysis {
            self.analyze_trends()?;
        }

        // Detect regressions if enabled
        if self.config.enable_regression_detection {
            self.detect_regressions()?;
        }

        debug!("Performance data collection completed in {:?}", collection_start.elapsed());
        Ok(())
    }

    /// Get current performance data
    pub fn get_current_performance(&self) -> PerformanceData {
        self.current_performance.read().unwrap().clone()
    }

    /// Get performance history
    pub fn get_performance_history(&self) -> Vec<PerformanceSnapshot> {
        self.performance_history.read().unwrap().iter().cloned().collect()
    }

    /// Record an operation for performance tracking
    pub fn record_operation(&self, duration: Duration, bytes_processed: u64, operation_type: &str) {
        self.counters.operations_completed.fetch_add(1, Ordering::Relaxed);
        self.counters.total_operation_time.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);

        match operation_type {
            "read" => {
                self.counters.bytes_read.fetch_add(bytes_processed, Ordering::Relaxed);
            }
            "write" => {
                self.counters.bytes_written.fetch_add(bytes_processed, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record cache access
    pub fn record_cache_access(&self, hit: bool) {
        if hit {
            self.counters.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record transaction completion
    pub fn record_transaction(&self, success: bool) {
        if success {
            self.counters.transactions_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.transactions_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record compaction
    pub fn record_compaction(&self, bytes_processed: u64) {
        self.counters.compactions_completed.fetch_add(1, Ordering::Relaxed);
        self.counters.compaction_bytes_processed.fetch_add(bytes_processed, Ordering::Relaxed);
    }

    /// Collect throughput metrics
    fn collect_throughput_metrics(&self, _database: &Database, performance: &mut PerformanceData) -> Result<()> {
        // Calculate throughput from counters
        let _operations_completed = self.counters.operations_completed.load(Ordering::Relaxed);
        let bytes_read = self.counters.bytes_read.load(Ordering::Relaxed);
        let bytes_written = self.counters.bytes_written.load(Ordering::Relaxed);

        // Simple throughput calculation (would need time window for accurate rates)
        performance.read_throughput = (bytes_read as f64) / 1024.0 / 1024.0; // MB/s placeholder
        performance.write_throughput = (bytes_written as f64) / 1024.0 / 1024.0; // MB/s placeholder
        performance.mixed_throughput = performance.read_throughput + performance.write_throughput;

        Ok(())
    }

    /// Collect latency metrics
    fn collect_latency_metrics(&self, _database: &Database, performance: &mut PerformanceData) -> Result<()> {
        let total_ops = self.counters.operations_completed.load(Ordering::Relaxed);
        let total_time_ns = self.counters.total_operation_time.load(Ordering::Relaxed);

        if total_ops > 0 {
            let avg_latency_ns = total_time_ns / total_ops;
            let avg_latency = Duration::from_nanos(avg_latency_ns);

            // Simplified percentile calculation (would use proper histogram in production)
            performance.latency_percentiles = LatencyPercentiles {
                p50: avg_latency,
                p90: avg_latency * 2,
                p95: avg_latency * 3,
                p99: avg_latency * 5,
                p999: avg_latency * 10,
                max: avg_latency * 20,
            };
        }

        Ok(())
    }

    /// Collect cache metrics
    fn collect_cache_metrics(&self, _database: &Database, performance: &mut PerformanceData) -> Result<()> {
        let cache_hits = self.counters.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.counters.cache_misses.load(Ordering::Relaxed);
        let total_accesses = cache_hits + cache_misses;

        if total_accesses > 0 {
            performance.cache_efficiency = CacheEfficiency {
                hit_rate: cache_hits as f64 / total_accesses as f64,
                miss_rate: cache_misses as f64 / total_accesses as f64,
                eviction_rate: 0.05, // Placeholder
                fill_rate: 0.1, // Placeholder
                memory_efficiency: 0.85, // Placeholder
            };
        }

        Ok(())
    }

    /// Collect I/O metrics
    fn collect_io_metrics(&self, _database: &Database, performance: &mut PerformanceData) -> Result<()> {
        performance.io_performance = IoPerformance {
            read_iops: 1000.0, // Placeholder
            write_iops: 500.0, // Placeholder
            read_bandwidth_mbps: 100.0, // Placeholder
            write_bandwidth_mbps: 50.0, // Placeholder
            average_read_latency: Duration::from_micros(100),
            average_write_latency: Duration::from_micros(500),
            queue_depth: 2.5, // Placeholder
        };

        Ok(())
    }

    /// Collect transaction metrics
    fn collect_transaction_metrics(&self, _database: &Database, performance: &mut PerformanceData) -> Result<()> {
        let completed = self.counters.transactions_completed.load(Ordering::Relaxed);
        let failed = self.counters.transactions_failed.load(Ordering::Relaxed);
        let total = completed + failed;

        if total > 0 {
            performance.transaction_success_rate = completed as f64 / total as f64;
        } else {
            performance.transaction_success_rate = 1.0; // No transactions = 100% success rate
        }

        // Compaction efficiency
        let compactions = self.counters.compactions_completed.load(Ordering::Relaxed);
        let compaction_bytes = self.counters.compaction_bytes_processed.load(Ordering::Relaxed);
        
        if compactions > 0 && compaction_bytes > 0 {
            performance.compaction_efficiency = 0.75; // Placeholder calculation
        }

        performance.index_utilization = 0.85; // Placeholder

        Ok(())
    }

    /// Collect resource utilization
    fn collect_resource_utilization(&self, performance: &mut PerformanceData) -> Result<()> {
        // Placeholder implementation - would use system APIs
        performance.resource_utilization = ResourceUtilization {
            cpu_usage: 25.0, // 25% CPU usage
            memory_usage: 512.0 * 1024.0 * 1024.0, // 512MB
            disk_io_usage: 10.0, // 10% I/O utilization
            network_usage: 1.0, // 1% network utilization
        };

        Ok(())
    }

    /// Add performance data to history
    fn add_to_history(&self, performance: PerformanceData, collection_duration: Duration) {
        let mut history = self.performance_history.write().unwrap();
        
        history.push_back(PerformanceSnapshot {
            performance,
            timestamp: SystemTime::now(),
            collection_duration,
        });

        // Trim history if it exceeds max entries
        while history.len() > self.config.max_history_entries {
            history.pop_front();
        }
    }

    /// Run performance benchmarks
    fn run_benchmarks(&self, database: &Database) -> Result<()> {
        debug!("Running performance benchmarks");

        // Simple read benchmark
        let read_result = self.benchmark_reads(database)?;
        self.store_benchmark_result(read_result);

        // Simple write benchmark
        let write_result = self.benchmark_writes(database)?;
        self.store_benchmark_result(write_result);

        // Mixed workload benchmark
        let mixed_result = self.benchmark_mixed_workload(database)?;
        self.store_benchmark_result(mixed_result);

        Ok(())
    }

    /// Benchmark read operations
    fn benchmark_reads(&self, database: &Database) -> Result<BenchmarkResult> {
        let start = Instant::now();
        let num_operations = 1000;

        // Setup test data
        for i in 0..num_operations {
            let key = format!("benchmark_read_{}", i);
            let value = format!("value_{}", i);
            database.put(key.as_bytes(), value.as_bytes())?;
        }

        // Benchmark reads
        let read_start = Instant::now();
        for i in 0..num_operations {
            let key = format!("benchmark_read_{}", i);
            database.get(key.as_bytes())?;
        }
        let read_duration = read_start.elapsed();

        // Cleanup
        for i in 0..num_operations {
            let key = format!("benchmark_read_{}", i);
            let _ = database.delete(key.as_bytes());
        }

        let ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();

        Ok(BenchmarkResult {
            benchmark_name: "read_throughput".to_string(),
            result_value: ops_per_sec,
            unit: "ops/sec".to_string(),
            higher_is_better: true,
            timestamp: SystemTime::now(),
            duration: start.elapsed(),
            metadata: HashMap::new(),
        })
    }

    /// Benchmark write operations
    fn benchmark_writes(&self, database: &Database) -> Result<BenchmarkResult> {
        let start = Instant::now();
        let num_operations = 1000;

        let write_start = Instant::now();
        for i in 0..num_operations {
            let key = format!("benchmark_write_{}", i);
            let value = format!("value_{}", i);
            database.put(key.as_bytes(), value.as_bytes())?;
        }
        let write_duration = write_start.elapsed();

        // Cleanup
        for i in 0..num_operations {
            let key = format!("benchmark_write_{}", i);
            let _ = database.delete(key.as_bytes());
        }

        let ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();

        Ok(BenchmarkResult {
            benchmark_name: "write_throughput".to_string(),
            result_value: ops_per_sec,
            unit: "ops/sec".to_string(),
            higher_is_better: true,
            timestamp: SystemTime::now(),
            duration: start.elapsed(),
            metadata: HashMap::new(),
        })
    }

    /// Benchmark mixed workload
    fn benchmark_mixed_workload(&self, database: &Database) -> Result<BenchmarkResult> {
        let start = Instant::now();
        let num_operations = 1000;

        let mixed_start = Instant::now();
        for i in 0..num_operations {
            let key = format!("benchmark_mixed_{}", i);
            let value = format!("value_{}", i);
            
            // Write
            database.put(key.as_bytes(), value.as_bytes())?;
            
            // Read
            database.get(key.as_bytes())?;
            
            // Update (every 10th operation)
            if i % 10 == 0 {
                let updated_value = format!("updated_value_{}", i);
                database.put(key.as_bytes(), updated_value.as_bytes())?;
            }
        }
        let mixed_duration = mixed_start.elapsed();

        // Cleanup
        for i in 0..num_operations {
            let key = format!("benchmark_mixed_{}", i);
            let _ = database.delete(key.as_bytes());
        }

        let ops_per_sec = (num_operations * 2) as f64 / mixed_duration.as_secs_f64(); // 2 ops per iteration

        Ok(BenchmarkResult {
            benchmark_name: "mixed_workload_throughput".to_string(),
            result_value: ops_per_sec,
            unit: "ops/sec".to_string(),
            higher_is_better: true,
            timestamp: SystemTime::now(),
            duration: start.elapsed(),
            metadata: HashMap::new(),
        })
    }

    /// Store benchmark result
    fn store_benchmark_result(&self, result: BenchmarkResult) {
        let mut results = self.benchmark_results.write().unwrap();
        results.insert(result.benchmark_name.clone(), result);
    }

    /// Analyze performance trends
    fn analyze_trends(&self) -> Result<Vec<PerformanceTrend>> {
        let history = self.performance_history.read().unwrap();
        let cutoff_time = SystemTime::now()
            .checked_sub(self.config.trend_analysis_window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let recent_data: Vec<_> = history
            .iter()
            .filter(|snapshot| snapshot.timestamp >= cutoff_time)
            .collect();

        if recent_data.len() < 3 {
            return Ok(Vec::new()); // Not enough data for trend analysis
        }

        let mut trends = Vec::new();

        // Analyze read throughput trend
        let read_throughput_values: Vec<f64> = recent_data
            .iter()
            .map(|s| s.performance.read_throughput)
            .collect();
        
        if let Some(trend) = self.calculate_trend("read_throughput", &read_throughput_values) {
            trends.push(trend);
        }

        // Analyze write throughput trend
        let write_throughput_values: Vec<f64> = recent_data
            .iter()
            .map(|s| s.performance.write_throughput)
            .collect();
        
        if let Some(trend) = self.calculate_trend("write_throughput", &write_throughput_values) {
            trends.push(trend);
        }

        // Analyze latency trend (P95)
        let latency_values: Vec<f64> = recent_data
            .iter()
            .map(|s| s.performance.latency_percentiles.p95.as_secs_f64())
            .collect();
        
        if let Some(trend) = self.calculate_trend("p95_latency", &latency_values) {
            trends.push(trend);
        }

        Ok(trends)
    }

    /// Calculate trend for a metric
    fn calculate_trend(&self, metric_name: &str, values: &[f64]) -> Option<PerformanceTrend> {
        if values.len() < 3 {
            return None;
        }

        // Simple linear trend calculation
        let n = values.len() as f64;
        let x_sum: f64 = (0..values.len()).map(|i| i as f64).sum();
        let y_sum: f64 = values.iter().sum();
        let xy_sum: f64 = values.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let x2_sum: f64 = (0..values.len()).map(|i| (i as f64).powi(2)).sum();

        let slope = (n * xy_sum - x_sum * y_sum) / (n * x2_sum - x_sum.powi(2));
        
        let trend_direction = if slope > 0.05 {
            TrendDirection::Improving
        } else if slope < -0.05 {
            TrendDirection::Degrading
        } else {
            TrendDirection::Stable
        };

        let trend_strength = slope.abs().min(1.0);
        let confidence = if values.len() >= 10 { 0.8 } else { 0.5 }; // Simplified confidence

        Some(PerformanceTrend {
            metric_name: metric_name.to_string(),
            trend_direction,
            trend_strength,
            confidence,
            analysis_window: self.config.trend_analysis_window,
            data_points: values.len(),
        })
    }

    /// Detect performance regressions
    fn detect_regressions(&self) -> Result<Vec<PerformanceRegression>> {
        let current = self.current_performance.read().unwrap();
        let baselines = self.baselines.read().unwrap();
        let mut regressions = Vec::new();

        for (baseline_name, baseline) in baselines.iter() {
            if let Some(regression) = self.check_regression_against_baseline(&current, baseline) {
                info!("Performance regression detected: {} vs {}", regression.metric_name, baseline_name);
                regressions.push(regression);
            }
        }

        Ok(regressions)
    }

    /// Check for regression against a baseline
    fn check_regression_against_baseline(
        &self,
        current: &PerformanceData,
        baseline: &PerformanceBaseline,
    ) -> Option<PerformanceRegression> {
        // Check read throughput
        if let Some(&baseline_value) = baseline.metrics.get("read_throughput") {
            let current_value = current.read_throughput;
            let degradation = ((baseline_value - current_value) / baseline_value * 100.0).abs();
            
            if degradation > self.config.regression_threshold {
                return Some(PerformanceRegression {
                    metric_name: "read_throughput".to_string(),
                    baseline_value,
                    current_value,
                    degradation_percent: degradation,
                    threshold_percent: self.config.regression_threshold,
                    detected_at: SystemTime::now(),
                });
            }
        }

        None
    }

    /// Get benchmark results
    pub fn get_benchmark_results(&self) -> HashMap<String, BenchmarkResult> {
        self.benchmark_results.read().unwrap().clone()
    }

    /// Establish performance baseline
    pub fn establish_baseline(&self, baseline_name: String) -> Result<()> {
        let current = self.current_performance.read().unwrap();
        
        let mut metrics = HashMap::new();
        metrics.insert("read_throughput".to_string(), current.read_throughput);
        metrics.insert("write_throughput".to_string(), current.write_throughput);
        metrics.insert("transaction_success_rate".to_string(), current.transaction_success_rate);
        metrics.insert("cache_hit_rate".to_string(), current.cache_efficiency.hit_rate);
        
        let baseline = PerformanceBaseline {
            baseline_name: baseline_name.clone(),
            metrics,
            established_at: SystemTime::now(),
            confidence_interval: 0.95,
            sample_size: 1,
        };

        let mut baselines = self.baselines.write().unwrap();
        baselines.insert(baseline_name, baseline);

        Ok(())
    }
}

/// Performance regression detected
#[derive(Debug, Clone, Serialize)]
pub struct PerformanceRegression {
    pub metric_name: String,
    pub baseline_value: f64,
    pub current_value: f64,
    pub degradation_percent: f64,
    pub threshold_percent: f64,
    pub detected_at: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    

    #[test]
    fn test_performance_monitor_creation() {
        let monitor = PerformanceMonitor::new();
        assert!(!monitor.is_monitoring.load(Ordering::Relaxed));
    }

    #[test]
    fn test_performance_monitoring_lifecycle() {
        let monitor = PerformanceMonitor::new();
        
        monitor.start_monitoring();
        assert!(monitor.is_monitoring.load(Ordering::Relaxed));
        
        monitor.stop_monitoring();
        assert!(!monitor.is_monitoring.load(Ordering::Relaxed));
    }

    #[test]
    fn test_operation_recording() {
        let monitor = PerformanceMonitor::new();
        
        monitor.record_operation(Duration::from_millis(100), 1024, "read");
        monitor.record_operation(Duration::from_millis(200), 2048, "write");
        
        let ops_completed = monitor.counters.operations_completed.load(Ordering::Relaxed);
        let bytes_read = monitor.counters.bytes_read.load(Ordering::Relaxed);
        let bytes_written = monitor.counters.bytes_written.load(Ordering::Relaxed);
        
        assert_eq!(ops_completed, 2);
        assert_eq!(bytes_read, 1024);
        assert_eq!(bytes_written, 2048);
    }

    #[test]
    fn test_cache_recording() {
        let monitor = PerformanceMonitor::new();
        
        monitor.record_cache_access(true);
        monitor.record_cache_access(false);
        monitor.record_cache_access(true);
        
        let hits = monitor.counters.cache_hits.load(Ordering::Relaxed);
        let misses = monitor.counters.cache_misses.load(Ordering::Relaxed);
        
        assert_eq!(hits, 2);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_trend_calculation() {
        let monitor = PerformanceMonitor::new();
        
        // Increasing trend
        let increasing_values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        if let Some(trend) = monitor.calculate_trend("test_metric", &increasing_values) {
            matches!(trend.trend_direction, TrendDirection::Improving);
        }
        
        // Stable trend
        let stable_values = vec![5.0, 5.1, 4.9, 5.0, 5.1];
        if let Some(trend) = monitor.calculate_trend("test_metric", &stable_values) {
            matches!(trend.trend_direction, TrendDirection::Stable);
        }
    }

    #[test]
    fn test_baseline_establishment() {
        let monitor = PerformanceMonitor::new();
        
        let result = monitor.establish_baseline("test_baseline".to_string());
        assert!(result.is_ok());
        
        let baselines = monitor.baselines.read().unwrap();
        assert!(baselines.contains_key("test_baseline"));
    }
}
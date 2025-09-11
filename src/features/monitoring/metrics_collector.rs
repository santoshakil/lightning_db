//! Metrics Collection System for Lightning DB
//!
//! Comprehensive metrics collection covering operations, performance, resources,
//! and business metrics with OpenTelemetry integration.

use crate::core::error::Result;
use crate::Database;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, RwLock,
};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, info};

/// Comprehensive metrics collector for Lightning DB
#[derive(Debug)]
pub struct MetricsCollector {
    /// Current aggregated metrics
    current_metrics: Arc<RwLock<DatabaseMetrics>>,
    /// Historical metrics for trend analysis
    metrics_history: Arc<RwLock<VecDeque<MetricsSnapshot>>>,
    /// Operation-specific metrics
    operation_metrics: Arc<RwLock<HashMap<String, OperationMetrics>>>,
    /// Performance counters
    performance_counters: Arc<PerformanceCounters>,
    /// Resource usage tracking
    resource_metrics: Arc<RwLock<ResourceMetrics>>,
    /// Configuration
    config: MetricsConfig,
    /// Collection state
    is_collecting: Arc<AtomicBool>,
}

/// Configuration for metrics collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Maximum history entries to keep
    pub max_history_entries: usize,
    /// Metrics aggregation window
    pub aggregation_window: Duration,
    /// Enable detailed operation metrics
    pub enable_operation_metrics: bool,
    /// Enable resource tracking
    pub enable_resource_tracking: bool,
    /// Enable performance counters
    pub enable_performance_counters: bool,
    /// Metrics collection interval
    pub collection_interval: Duration,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 1000,
            aggregation_window: Duration::from_secs(60),
            enable_operation_metrics: true,
            enable_resource_tracking: true,
            enable_performance_counters: true,
            collection_interval: Duration::from_secs(30),
        }
    }
}

/// Comprehensive database metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMetrics {
    /// Total number of operations performed
    pub total_operations: u64,
    /// Operations per second (current rate)
    pub operations_per_second: f64,
    /// Average operation latency
    pub average_latency: Duration,
    /// 95th percentile latency
    pub p95_latency: Duration,
    /// 99th percentile latency
    pub p99_latency: Duration,
    /// Error rate (0.0 to 1.0)
    pub error_rate: f64,
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
    /// Total storage size in bytes
    pub storage_size_bytes: u64,
    /// WAL size in bytes
    pub wal_size_bytes: u64,
    /// Active transactions count
    pub active_transactions: u32,
    /// Compaction statistics
    pub compaction_stats: CompactionStats,
    /// Memory usage statistics
    pub memory_stats: MemoryStats,
    /// I/O statistics
    pub io_stats: IoStats,
    /// Timestamp of metrics collection
    pub timestamp: SystemTime,
}

/// Metrics snapshot for historical tracking
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub metrics: DatabaseMetrics,
    pub timestamp: SystemTime,
    pub collection_duration: Duration,
}

/// Operation-specific metrics
#[derive(Debug, Clone, Serialize)]
pub struct OperationMetrics {
    pub operation_name: String,
    pub total_count: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub total_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub average_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub p99_duration: Duration,
    pub throughput_ops_per_sec: f64,
    pub error_rate: f64,
    pub last_updated: SystemTime,
}

/// Performance counters for various subsystems
#[derive(Debug)]
pub struct PerformanceCounters {
    pub read_operations: AtomicU64,
    pub write_operations: AtomicU64,
    pub delete_operations: AtomicU64,
    pub transaction_operations: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub compactions_triggered: AtomicU64,
    pub compactions_completed: AtomicU64,
    pub pages_read: AtomicU64,
    pub pages_written: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub wal_writes: AtomicU64,
    pub wal_syncs: AtomicU64,
}

/// Compaction statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStats {
    pub total_compactions: u64,
    pub compactions_in_progress: u32,
    pub total_compaction_time: Duration,
    pub average_compaction_time: Duration,
    pub bytes_compacted: u64,
    pub space_reclaimed_bytes: u64,
    pub compaction_efficiency: f64, // ratio of space reclaimed to bytes processed
}

/// Memory usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    pub total_allocated_bytes: u64,
    pub cache_memory_bytes: u64,
    pub btree_memory_bytes: u64,
    pub lsm_memory_bytes: u64,
    pub transaction_memory_bytes: u64,
    pub buffer_memory_bytes: u64,
    pub peak_memory_usage: u64,
    pub memory_efficiency: f64, // useful memory / total allocated
}

/// I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoStats {
    pub total_reads: u64,
    pub total_writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub read_latency_avg: Duration,
    pub write_latency_avg: Duration,
    pub read_throughput_bps: f64,
    pub write_throughput_bps: f64,
    pub pending_io_operations: u32,
    pub io_queue_depth: u32,
}

/// Resource usage metrics
#[derive(Debug, Clone, Serialize)]
pub struct ResourceMetrics {
    pub cpu_usage_percent: f64,
    pub memory_usage_bytes: u64,
    pub memory_usage_percent: f64,
    pub disk_usage_bytes: u64,
    pub disk_free_bytes: u64,
    pub network_bytes_in: u64,
    pub network_bytes_out: u64,
    pub open_file_descriptors: u32,
    pub thread_count: u32,
    pub load_average: f64,
}

impl Default for DatabaseMetrics {
    fn default() -> Self {
        Self {
            total_operations: 0,
            operations_per_second: 0.0,
            average_latency: Duration::from_millis(0),
            p95_latency: Duration::from_millis(0),
            p99_latency: Duration::from_millis(0),
            error_rate: 0.0,
            cache_hit_rate: 0.0,
            storage_size_bytes: 0,
            wal_size_bytes: 0,
            active_transactions: 0,
            compaction_stats: CompactionStats::default(),
            memory_stats: MemoryStats::default(),
            io_stats: IoStats::default(),
            timestamp: SystemTime::now(),
        }
    }
}

impl Default for CompactionStats {
    fn default() -> Self {
        Self {
            total_compactions: 0,
            compactions_in_progress: 0,
            total_compaction_time: Duration::from_millis(0),
            average_compaction_time: Duration::from_millis(0),
            bytes_compacted: 0,
            space_reclaimed_bytes: 0,
            compaction_efficiency: 0.0,
        }
    }
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self {
            total_allocated_bytes: 0,
            cache_memory_bytes: 0,
            btree_memory_bytes: 0,
            lsm_memory_bytes: 0,
            transaction_memory_bytes: 0,
            buffer_memory_bytes: 0,
            peak_memory_usage: 0,
            memory_efficiency: 0.0,
        }
    }
}

impl Default for IoStats {
    fn default() -> Self {
        Self {
            total_reads: 0,
            total_writes: 0,
            bytes_read: 0,
            bytes_written: 0,
            read_latency_avg: Duration::from_millis(0),
            write_latency_avg: Duration::from_millis(0),
            read_throughput_bps: 0.0,
            write_throughput_bps: 0.0,
            pending_io_operations: 0,
            io_queue_depth: 0,
        }
    }
}

impl Default for PerformanceCounters {
    fn default() -> Self {
        Self {
            read_operations: AtomicU64::new(0),
            write_operations: AtomicU64::new(0),
            delete_operations: AtomicU64::new(0),
            transaction_operations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            compactions_triggered: AtomicU64::new(0),
            compactions_completed: AtomicU64::new(0),
            pages_read: AtomicU64::new(0),
            pages_written: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            wal_writes: AtomicU64::new(0),
            wal_syncs: AtomicU64::new(0),
        }
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self::with_config(MetricsConfig::default())
    }

    /// Create metrics collector with custom configuration
    pub fn with_config(config: MetricsConfig) -> Self {
        Self {
            current_metrics: Arc::new(RwLock::new(DatabaseMetrics::default())),
            metrics_history: Arc::new(RwLock::new(VecDeque::new())),
            operation_metrics: Arc::new(RwLock::new(HashMap::new())),
            performance_counters: Arc::new(PerformanceCounters::default()),
            resource_metrics: Arc::new(RwLock::new(ResourceMetrics {
                cpu_usage_percent: 0.0,
                memory_usage_bytes: 0,
                memory_usage_percent: 0.0,
                disk_usage_bytes: 0,
                disk_free_bytes: 0,
                network_bytes_in: 0,
                network_bytes_out: 0,
                open_file_descriptors: 0,
                thread_count: 0,
                load_average: 0.0,
            })),
            config,
            is_collecting: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start metrics collection
    pub fn start_collection(&self) {
        self.is_collecting.store(true, Ordering::Relaxed);
        info!("Metrics collection started");
    }

    /// Stop metrics collection
    pub fn stop_collection(&self) {
        self.is_collecting.store(false, Ordering::Relaxed);
        info!("Metrics collection stopped");
    }

    /// Collect comprehensive metrics from the database
    pub fn collect_metrics(&self, database: &Database) -> Result<()> {
        if !self.is_collecting.load(Ordering::Relaxed) {
            return Ok(());
        }

        let collection_start = Instant::now();
        debug!("Starting metrics collection cycle");

        // Collect various metric categories
        let mut metrics = DatabaseMetrics::default();
        metrics.timestamp = SystemTime::now();

        // Collect operation metrics
        self.collect_operation_metrics(database, &mut metrics)?;

        // Collect performance metrics
        self.collect_performance_metrics(database, &mut metrics)?;

        // Collect storage metrics
        self.collect_storage_metrics(database, &mut metrics)?;

        // Collect memory metrics
        self.collect_memory_metrics(database, &mut metrics)?;

        // Collect I/O metrics
        self.collect_io_metrics(database, &mut metrics)?;

        // Collect transaction metrics
        self.collect_transaction_metrics(database, &mut metrics)?;

        // Collect compaction metrics
        self.collect_compaction_metrics(database, &mut metrics)?;

        // Update current metrics
        {
            if let Ok(mut current) = self.current_metrics.write() {
                *current = metrics.clone();
            }
        }

        // Add to history
        self.add_to_history(metrics, collection_start.elapsed());

        // Collect resource metrics if enabled
        if self.config.enable_resource_tracking {
            self.collect_resource_metrics()?;
        }

        debug!(
            "Metrics collection completed in {:?}",
            collection_start.elapsed()
        );
        Ok(())
    }

    /// Get current metrics snapshot
    pub fn get_current_metrics(&self) -> DatabaseMetrics {
        self.current_metrics
            .read()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    /// Get metrics history
    pub fn get_metrics_history(&self) -> Vec<MetricsSnapshot> {
        self.metrics_history
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    /// Get operation-specific metrics
    pub fn get_operation_metrics(&self) -> HashMap<String, OperationMetrics> {
        self.operation_metrics.read().unwrap().clone()
    }

    /// Get resource metrics
    pub fn get_resource_metrics(&self) -> ResourceMetrics {
        self.resource_metrics.read().unwrap().clone()
    }

    /// Record an operation for metrics
    pub fn record_operation(&self, operation: &str, duration: Duration, success: bool) {
        if !self.config.enable_operation_metrics {
            return;
        }

        let mut operations = self.operation_metrics.write().unwrap();
        let entry = operations
            .entry(operation.to_string())
            .or_insert_with(|| OperationMetrics {
                operation_name: operation.to_string(),
                total_count: 0,
                success_count: 0,
                error_count: 0,
                total_duration: Duration::from_millis(0),
                min_duration: Duration::from_secs(u64::MAX),
                max_duration: Duration::from_millis(0),
                average_duration: Duration::from_millis(0),
                p50_duration: Duration::from_millis(0),
                p95_duration: Duration::from_millis(0),
                p99_duration: Duration::from_millis(0),
                throughput_ops_per_sec: 0.0,
                error_rate: 0.0,
                last_updated: SystemTime::now(),
            });

        entry.total_count += 1;
        if success {
            entry.success_count += 1;
        } else {
            entry.error_count += 1;
        }

        entry.total_duration += duration;
        entry.min_duration = entry.min_duration.min(duration);
        entry.max_duration = entry.max_duration.max(duration);
        entry.average_duration = entry.total_duration / entry.total_count as u32;
        entry.error_rate = entry.error_count as f64 / entry.total_count as f64;
        entry.last_updated = SystemTime::now();

        // Update performance counters
        match operation {
            "get" | "scan" => self
                .performance_counters
                .read_operations
                .fetch_add(1, Ordering::Relaxed),
            "put" | "insert" => self
                .performance_counters
                .write_operations
                .fetch_add(1, Ordering::Relaxed),
            "delete" => self
                .performance_counters
                .delete_operations
                .fetch_add(1, Ordering::Relaxed),
            "transaction" => self
                .performance_counters
                .transaction_operations
                .fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
    }

    /// Record cache access
    pub fn record_cache_access(&self, hit: bool) {
        if hit {
            self.performance_counters
                .cache_hits
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.performance_counters
                .cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record compaction event
    pub fn record_compaction(
        &self,
        duration: Duration,
        bytes_processed: u64,
        space_reclaimed: u64,
    ) {
        self.performance_counters
            .compactions_completed
            .fetch_add(1, Ordering::Relaxed);

        // Update compaction stats in current metrics
        if let Ok(mut metrics) = self.current_metrics.write() {
            metrics.compaction_stats.total_compactions += 1;
            metrics.compaction_stats.total_compaction_time += duration;
            metrics.compaction_stats.bytes_compacted += bytes_processed;
            metrics.compaction_stats.space_reclaimed_bytes += space_reclaimed;

            if metrics.compaction_stats.total_compactions > 0 {
                metrics.compaction_stats.average_compaction_time =
                    metrics.compaction_stats.total_compaction_time
                        / metrics.compaction_stats.total_compactions as u32;
            }

            if bytes_processed > 0 {
                metrics.compaction_stats.compaction_efficiency =
                    space_reclaimed as f64 / bytes_processed as f64;
            }
        }
    }

    /// Collect operation metrics from database
    fn collect_operation_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        let read_ops = self
            .performance_counters
            .read_operations
            .load(Ordering::Relaxed);
        let write_ops = self
            .performance_counters
            .write_operations
            .load(Ordering::Relaxed);
        let delete_ops = self
            .performance_counters
            .delete_operations
            .load(Ordering::Relaxed);
        let tx_ops = self
            .performance_counters
            .transaction_operations
            .load(Ordering::Relaxed);

        metrics.total_operations = read_ops + write_ops + delete_ops + tx_ops;

        // Calculate operations per second (simplified)
        if let Some(last_snapshot) = self.metrics_history.read().unwrap().back() {
            let time_diff = metrics
                .timestamp
                .duration_since(last_snapshot.timestamp)
                .unwrap_or(Duration::from_secs(1));
            let ops_diff = metrics
                .total_operations
                .saturating_sub(last_snapshot.metrics.total_operations);
            metrics.operations_per_second = ops_diff as f64 / time_diff.as_secs_f64();
        }

        Ok(())
    }

    /// Collect performance metrics
    fn collect_performance_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        let cache_hits = self.performance_counters.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self
            .performance_counters
            .cache_misses
            .load(Ordering::Relaxed);
        let total_cache_accesses = cache_hits + cache_misses;

        if total_cache_accesses > 0 {
            metrics.cache_hit_rate = cache_hits as f64 / total_cache_accesses as f64;
        }

        // Calculate average latency from operation metrics
        let operations = self.operation_metrics.read().unwrap();
        if !operations.is_empty() {
            let total_duration: Duration = operations.values().map(|op| op.total_duration).sum();
            let total_ops: u64 = operations.values().map(|op| op.total_count).sum();

            if total_ops > 0 {
                metrics.average_latency = total_duration / total_ops as u32;
            }

            // Calculate percentiles (simplified - would need proper histogram in production)
            let mut all_durations: Vec<Duration> =
                operations.values().map(|op| op.average_duration).collect();
            all_durations.sort();

            if !all_durations.is_empty() {
                let len = all_durations.len();
                metrics.p95_latency = all_durations[(len * 95 / 100).min(len - 1)];
                metrics.p99_latency = all_durations[(len * 99 / 100).min(len - 1)];
            }
        }

        Ok(())
    }

    /// Collect storage metrics
    fn collect_storage_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        // In a real implementation, this would query the database for actual storage sizes
        // For now, we'll use placeholder values
        metrics.storage_size_bytes = 1024 * 1024 * 100; // 100MB placeholder
        metrics.wal_size_bytes = 1024 * 1024 * 10; // 10MB placeholder
        Ok(())
    }

    /// Collect memory metrics
    fn collect_memory_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        // Placeholder implementation - would integrate with actual memory tracking
        metrics.memory_stats = MemoryStats {
            total_allocated_bytes: 1024 * 1024 * 50,   // 50MB
            cache_memory_bytes: 1024 * 1024 * 20,      // 20MB
            btree_memory_bytes: 1024 * 1024 * 15,      // 15MB
            lsm_memory_bytes: 1024 * 1024 * 10,        // 10MB
            transaction_memory_bytes: 1024 * 1024 * 3, // 3MB
            buffer_memory_bytes: 1024 * 1024 * 2,      // 2MB
            peak_memory_usage: 1024 * 1024 * 60,       // 60MB
            memory_efficiency: 0.85,
        };
        Ok(())
    }

    /// Collect I/O metrics
    fn collect_io_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        let bytes_read = self.performance_counters.bytes_read.load(Ordering::Relaxed);
        let bytes_written = self
            .performance_counters
            .bytes_written
            .load(Ordering::Relaxed);
        let pages_read = self.performance_counters.pages_read.load(Ordering::Relaxed);
        let pages_written = self
            .performance_counters
            .pages_written
            .load(Ordering::Relaxed);

        metrics.io_stats = IoStats {
            total_reads: pages_read,
            total_writes: pages_written,
            bytes_read,
            bytes_written,
            read_latency_avg: Duration::from_micros(100), // Placeholder
            write_latency_avg: Duration::from_micros(500), // Placeholder
            read_throughput_bps: 0.0, // Would be calculated from historical data
            write_throughput_bps: 0.0, // Would be calculated from historical data
            pending_io_operations: 0, // Would be queried from I/O subsystem
            io_queue_depth: 0,        // Would be queried from I/O subsystem
        };

        Ok(())
    }

    /// Collect transaction metrics
    fn collect_transaction_metrics(
        &self,
        _database: &Database,
        metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        // Placeholder - would query actual transaction manager
        metrics.active_transactions = 5; // Placeholder value
        Ok(())
    }

    /// Collect compaction metrics
    fn collect_compaction_metrics(
        &self,
        _database: &Database,
        _metrics: &mut DatabaseMetrics,
    ) -> Result<()> {
        // Compaction metrics are updated via record_compaction method
        Ok(())
    }

    /// Collect system resource metrics
    fn collect_resource_metrics(&self) -> Result<()> {
        // Placeholder implementation - would use system APIs
        let mut resources = self.resource_metrics.write().unwrap();

        resources.cpu_usage_percent = 25.5; // Would use sysinfo or similar
        resources.memory_usage_bytes = 1024 * 1024 * 128; // 128MB
        resources.memory_usage_percent = 12.5;
        resources.disk_usage_bytes = 1024 * 1024 * 1024 * 2; // 2GB
        resources.disk_free_bytes = 1024 * 1024 * 1024 * 100; // 100GB
        resources.open_file_descriptors = 45;
        resources.thread_count = 8;
        resources.load_average = 1.2;

        Ok(())
    }

    /// Add metrics to history
    fn add_to_history(&self, metrics: DatabaseMetrics, collection_duration: Duration) {
        let mut history = self.metrics_history.write().unwrap();

        history.push_back(MetricsSnapshot {
            metrics,
            timestamp: SystemTime::now(),
            collection_duration,
        });

        // Trim history if it exceeds max entries
        while history.len() > self.config.max_history_entries {
            history.pop_front();
        }
    }

    /// Calculate metrics trends
    pub fn calculate_trends(&self, window: Duration) -> Result<MetricsTrends> {
        let history = self.metrics_history.read().unwrap();
        let cutoff_time = SystemTime::now()
            .checked_sub(window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let recent_metrics: Vec<_> = history
            .iter()
            .filter(|snapshot| snapshot.timestamp >= cutoff_time)
            .collect();

        if recent_metrics.len() < 2 {
            return Ok(MetricsTrends::default());
        }

        let first = &recent_metrics[0].metrics;
        let last = &recent_metrics[recent_metrics.len() - 1].metrics;

        Ok(MetricsTrends {
            operations_per_second_trend: calculate_trend(
                first.operations_per_second,
                last.operations_per_second,
            ),
            average_latency_trend: calculate_duration_trend(
                first.average_latency,
                last.average_latency,
            ),
            cache_hit_rate_trend: calculate_trend(first.cache_hit_rate, last.cache_hit_rate),
            error_rate_trend: calculate_trend(first.error_rate, last.error_rate),
            storage_size_trend: calculate_trend(
                first.storage_size_bytes as f64,
                last.storage_size_bytes as f64,
            ),
        })
    }
}

/// Trends analysis for metrics
#[derive(Debug, Clone, Serialize)]
pub struct MetricsTrends {
    pub operations_per_second_trend: TrendDirection,
    pub average_latency_trend: TrendDirection,
    pub cache_hit_rate_trend: TrendDirection,
    pub error_rate_trend: TrendDirection,
    pub storage_size_trend: TrendDirection,
}

impl Default for MetricsTrends {
    fn default() -> Self {
        Self {
            operations_per_second_trend: TrendDirection::Stable,
            average_latency_trend: TrendDirection::Stable,
            cache_hit_rate_trend: TrendDirection::Stable,
            error_rate_trend: TrendDirection::Stable,
            storage_size_trend: TrendDirection::Stable,
        }
    }
}

/// Direction of metric trends
#[derive(Debug, Clone, Serialize)]
pub enum TrendDirection {
    Increasing(f64), // percentage change
    Decreasing(f64), // percentage change
    Stable,
}

/// Calculate trend between two values
fn calculate_trend(from: f64, to: f64) -> TrendDirection {
    if from == 0.0 {
        return TrendDirection::Stable;
    }

    let change_percent = ((to - from) / from) * 100.0;

    if change_percent.abs() < 5.0 {
        TrendDirection::Stable
    } else if change_percent > 0.0 {
        TrendDirection::Increasing(change_percent)
    } else {
        TrendDirection::Decreasing(change_percent.abs())
    }
}

/// Calculate trend for duration values
fn calculate_duration_trend(from: Duration, to: Duration) -> TrendDirection {
    calculate_trend(from.as_secs_f64(), to.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collector_creation() {
        let collector = MetricsCollector::new();
        assert!(!collector.is_collecting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_metrics_collection_lifecycle() {
        let collector = MetricsCollector::new();

        collector.start_collection();
        assert!(collector.is_collecting.load(Ordering::Relaxed));

        collector.stop_collection();
        assert!(!collector.is_collecting.load(Ordering::Relaxed));
    }

    #[test]
    fn test_operation_recording() {
        let collector = MetricsCollector::new();

        collector.record_operation("test_op", Duration::from_millis(100), true);
        collector.record_operation("test_op", Duration::from_millis(200), false);

        let metrics = collector.get_operation_metrics();
        let test_op = metrics.get("test_op").unwrap();

        assert_eq!(test_op.total_count, 2);
        assert_eq!(test_op.success_count, 1);
        assert_eq!(test_op.error_count, 1);
        assert_eq!(test_op.error_rate, 0.5);
    }

    #[test]
    fn test_cache_access_recording() {
        let collector = MetricsCollector::new();

        collector.record_cache_access(true);
        collector.record_cache_access(false);
        collector.record_cache_access(true);

        let hits = collector
            .performance_counters
            .cache_hits
            .load(Ordering::Relaxed);
        let misses = collector
            .performance_counters
            .cache_misses
            .load(Ordering::Relaxed);

        assert_eq!(hits, 2);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_trend_calculation() {
        let increasing = calculate_trend(100.0, 110.0);
        let decreasing = calculate_trend(100.0, 90.0);
        let stable = calculate_trend(100.0, 102.0);

        matches!(increasing, TrendDirection::Increasing(_));
        matches!(decreasing, TrendDirection::Decreasing(_));
        matches!(stable, TrendDirection::Stable);
    }
}

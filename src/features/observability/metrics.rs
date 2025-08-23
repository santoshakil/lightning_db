use crate::core::error::Result;
use crate::features::observability::{CacheType, ErrorType, OperationType, TransactionEvent};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tokio::time::interval;

use super::ObservabilityConfig;

/// Core metrics collection engine
pub struct MetricsEngine {
    operations: OperationMetrics,
    performance: PerformanceMetrics,
    cache: CacheMetrics,
    transactions: TransactionMetrics,
    errors: ErrorMetrics,
    resources: ResourceMetrics,
    storage: StorageMetrics,
    network: NetworkMetrics,
    config: ObservabilityConfig,
    running: AtomicBool,
    collection_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

/// Database operation metrics
#[derive(Debug)]
pub struct OperationMetrics {
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub scans: AtomicU64,
    pub checkpoints: AtomicU64,
    pub compactions: AtomicU64,
    pub vacuums: AtomicU64,
    pub backups: AtomicU64,
    
    // Success/failure counters
    pub successful_operations: AtomicU64,
    pub failed_operations: AtomicU64,
    
    // Size metrics
    pub total_bytes_read: AtomicU64,
    pub total_bytes_written: AtomicU64,
}

/// Performance metrics with histograms
#[derive(Debug)]
pub struct PerformanceMetrics {
    pub latency_histogram: Arc<RwLock<LatencyHistogram>>,
    pub throughput_tracker: Arc<RwLock<ThroughputTracker>>,
    pub active_operations: AtomicUsize,
    pub queue_depth: AtomicUsize,
    pub concurrent_reads: AtomicUsize,
    pub concurrent_writes: AtomicUsize,
}

/// Cache performance metrics
#[derive(Debug)]
pub struct CacheMetrics {
    pub page_cache_hits: AtomicU64,
    pub page_cache_misses: AtomicU64,
    pub btree_cache_hits: AtomicU64,
    pub btree_cache_misses: AtomicU64,
    pub lsm_cache_hits: AtomicU64,
    pub lsm_cache_misses: AtomicU64,
    pub metadata_cache_hits: AtomicU64,
    pub metadata_cache_misses: AtomicU64,
    
    pub cache_evictions: AtomicU64,
    pub cache_size_bytes: AtomicU64,
    pub cache_entries: AtomicUsize,
}

/// Transaction metrics
#[derive(Debug)]
pub struct TransactionMetrics {
    pub transactions_begun: AtomicU64,
    pub transactions_committed: AtomicU64,
    pub transactions_rolled_back: AtomicU64,
    pub transaction_conflicts: AtomicU64,
    pub deadlocks: AtomicU64,
    
    pub active_transactions: AtomicUsize,
    pub max_transaction_duration: Arc<RwLock<Duration>>,
    pub avg_transaction_size: AtomicU64,
}

/// Error metrics
#[derive(Debug)]
pub struct ErrorMetrics {
    pub io_errors: AtomicU64,
    pub corruption_errors: AtomicU64,
    pub oom_errors: AtomicU64,
    pub disk_full_errors: AtomicU64,
    pub network_errors: AtomicU64,
    pub serialization_errors: AtomicU64,
    pub validation_errors: AtomicU64,
    pub timeout_errors: AtomicU64,
    pub permission_errors: AtomicU64,
    pub config_errors: AtomicU64,
    
    pub total_errors: AtomicU64,
    pub error_rate: Arc<RwLock<f64>>,
    pub last_error: Arc<RwLock<Option<(SystemTime, String)>>>,
}

/// Resource utilization metrics
#[derive(Debug)]
pub struct ResourceMetrics {
    pub memory_used: AtomicU64,
    pub memory_available: AtomicU64,
    pub disk_used: AtomicU64,
    pub disk_available: AtomicU64,
    pub cpu_usage: Arc<RwLock<f64>>,
    pub thread_count: AtomicUsize,
    pub open_files: AtomicUsize,
    pub network_connections: AtomicUsize,
}

/// Storage-specific metrics
#[derive(Debug)]
pub struct StorageMetrics {
    pub database_size: AtomicU64,
    pub wal_size: AtomicU64,
    pub index_size: AtomicU64,
    pub page_reads: AtomicU64,
    pub page_writes: AtomicU64,
    pub page_splits: AtomicU64,
    pub page_merges: AtomicU64,
    pub fragmentation_ratio: Arc<RwLock<f64>>,
}

/// Network metrics (for distributed features)
#[derive(Debug)]
pub struct NetworkMetrics {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub connections_opened: AtomicU64,
    pub connections_closed: AtomicU64,
    pub connection_errors: AtomicU64,
    pub active_connections: AtomicUsize,
    pub avg_latency: Arc<RwLock<Duration>>,
}

/// Latency histogram for tracking operation latencies
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    buckets: Vec<(Duration, u64)>,
    count: u64,
    sum: Duration,
    min: Duration,
    max: Duration,
    p50: Duration,
    p90: Duration,
    p95: Duration,
    p99: Duration,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        let buckets = vec![
            (Duration::from_nanos(100), 0),
            (Duration::from_micros(1), 0),
            (Duration::from_micros(10), 0),
            (Duration::from_micros(100), 0),
            (Duration::from_millis(1), 0),
            (Duration::from_millis(10), 0),
            (Duration::from_millis(100), 0),
            (Duration::from_secs(1), 0),
            (Duration::from_secs(10), 0),
        ];

        Self {
            buckets,
            count: 0,
            sum: Duration::ZERO,
            min: Duration::MAX,
            max: Duration::ZERO,
            p50: Duration::ZERO,
            p90: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
        }
    }

    pub fn record(&mut self, latency: Duration) {
        self.count += 1;
        self.sum += latency;
        self.min = self.min.min(latency);
        self.max = self.max.max(latency);

        // Update buckets
        for (threshold, count) in &mut self.buckets {
            if latency <= *threshold {
                *count += 1;
            }
        }

        // Update percentiles (simplified calculation)
        self.update_percentiles();
    }

    fn update_percentiles(&mut self) {
        if self.count == 0 {
            return;
        }

        let mut cumulative = 0u64;
        let p50_target = (self.count as f64 * 0.5) as u64;
        let p90_target = (self.count as f64 * 0.9) as u64;
        let p95_target = (self.count as f64 * 0.95) as u64;
        let p99_target = (self.count as f64 * 0.99) as u64;

        for (threshold, count) in &self.buckets {
            cumulative += count;
            
            if self.p50 == Duration::ZERO && cumulative >= p50_target {
                self.p50 = *threshold;
            }
            if self.p90 == Duration::ZERO && cumulative >= p90_target {
                self.p90 = *threshold;
            }
            if self.p95 == Duration::ZERO && cumulative >= p95_target {
                self.p95 = *threshold;
            }
            if self.p99 == Duration::ZERO && cumulative >= p99_target {
                self.p99 = *threshold;
            }
        }
    }

    pub fn average(&self) -> Duration {
        if self.count == 0 {
            Duration::ZERO
        } else {
            self.sum / self.count as u32
        }
    }

    pub fn percentile(&self, p: f64) -> Duration {
        match p {
            50.0 => self.p50,
            90.0 => self.p90,
            95.0 => self.p95,
            99.0 => self.p99,
            _ => {
                // Fallback calculation
                if self.count == 0 {
                    return Duration::ZERO;
                }
                let target = (self.count as f64 * p / 100.0) as u64;
                let mut seen = 0u64;
                for (threshold, count) in &self.buckets {
                    seen += count;
                    if seen >= target {
                        return *threshold;
                    }
                }
                self.max
            }
        }
    }

    pub fn reset(&mut self) {
        for (_, count) in &mut self.buckets {
            *count = 0;
        }
        self.count = 0;
        self.sum = Duration::ZERO;
        self.min = Duration::MAX;
        self.max = Duration::ZERO;
        self.p50 = Duration::ZERO;
        self.p90 = Duration::ZERO;
        self.p95 = Duration::ZERO;
        self.p99 = Duration::ZERO;
    }
}

/// Throughput tracker for measuring operations per second
#[derive(Debug, Clone)]
pub struct ThroughputTracker {
    window: Duration,
    samples: Vec<(Instant, u64)>,
    current_rate: f64,
    peak_rate: f64,
}

impl ThroughputTracker {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: Vec::new(),
            current_rate: 0.0,
            peak_rate: 0.0,
        }
    }

    pub fn record(&mut self, count: u64) {
        let now = Instant::now();
        self.samples.push((now, count));

        // Remove old samples
        let cutoff = now - self.window;
        self.samples.retain(|(time, _)| *time > cutoff);

        // Calculate current rate
        self.update_rate();
    }

    fn update_rate(&mut self) {
        if self.samples.len() < 2 {
            self.current_rate = 0.0;
            return;
        }

        let total: u64 = self.samples.iter().map(|(_, count)| count).sum();
        let duration = self.samples.last().unwrap().0 - self.samples.first().unwrap().0;

        if duration.as_secs_f64() > 0.0 {
            self.current_rate = total as f64 / duration.as_secs_f64();
            self.peak_rate = self.peak_rate.max(self.current_rate);
        }
    }

    pub fn rate(&self) -> f64 {
        self.current_rate
    }

    pub fn peak_rate(&self) -> f64 {
        self.peak_rate
    }

    pub fn reset(&mut self) {
        self.samples.clear();
        self.current_rate = 0.0;
        self.peak_rate = 0.0;
    }
}

impl MetricsEngine {
    pub fn new(config: ObservabilityConfig) -> Result<Self> {
        Ok(Self {
            operations: OperationMetrics {
                reads: AtomicU64::new(0),
                writes: AtomicU64::new(0),
                deletes: AtomicU64::new(0),
                scans: AtomicU64::new(0),
                checkpoints: AtomicU64::new(0),
                compactions: AtomicU64::new(0),
                vacuums: AtomicU64::new(0),
                backups: AtomicU64::new(0),
                successful_operations: AtomicU64::new(0),
                failed_operations: AtomicU64::new(0),
                total_bytes_read: AtomicU64::new(0),
                total_bytes_written: AtomicU64::new(0),
            },
            performance: PerformanceMetrics {
                latency_histogram: Arc::new(RwLock::new(LatencyHistogram::new())),
                throughput_tracker: Arc::new(RwLock::new(ThroughputTracker::new(Duration::from_secs(60)))),
                active_operations: AtomicUsize::new(0),
                queue_depth: AtomicUsize::new(0),
                concurrent_reads: AtomicUsize::new(0),
                concurrent_writes: AtomicUsize::new(0),
            },
            cache: CacheMetrics {
                page_cache_hits: AtomicU64::new(0),
                page_cache_misses: AtomicU64::new(0),
                btree_cache_hits: AtomicU64::new(0),
                btree_cache_misses: AtomicU64::new(0),
                lsm_cache_hits: AtomicU64::new(0),
                lsm_cache_misses: AtomicU64::new(0),
                metadata_cache_hits: AtomicU64::new(0),
                metadata_cache_misses: AtomicU64::new(0),
                cache_evictions: AtomicU64::new(0),
                cache_size_bytes: AtomicU64::new(0),
                cache_entries: AtomicUsize::new(0),
            },
            transactions: TransactionMetrics {
                transactions_begun: AtomicU64::new(0),
                transactions_committed: AtomicU64::new(0),
                transactions_rolled_back: AtomicU64::new(0),
                transaction_conflicts: AtomicU64::new(0),
                deadlocks: AtomicU64::new(0),
                active_transactions: AtomicUsize::new(0),
                max_transaction_duration: Arc::new(RwLock::new(Duration::ZERO)),
                avg_transaction_size: AtomicU64::new(0),
            },
            errors: ErrorMetrics {
                io_errors: AtomicU64::new(0),
                corruption_errors: AtomicU64::new(0),
                oom_errors: AtomicU64::new(0),
                disk_full_errors: AtomicU64::new(0),
                network_errors: AtomicU64::new(0),
                serialization_errors: AtomicU64::new(0),
                validation_errors: AtomicU64::new(0),
                timeout_errors: AtomicU64::new(0),
                permission_errors: AtomicU64::new(0),
                config_errors: AtomicU64::new(0),
                total_errors: AtomicU64::new(0),
                error_rate: Arc::new(RwLock::new(0.0)),
                last_error: Arc::new(RwLock::new(None)),
            },
            resources: ResourceMetrics {
                memory_used: AtomicU64::new(0),
                memory_available: AtomicU64::new(0),
                disk_used: AtomicU64::new(0),
                disk_available: AtomicU64::new(0),
                cpu_usage: Arc::new(RwLock::new(0.0)),
                thread_count: AtomicUsize::new(0),
                open_files: AtomicUsize::new(0),
                network_connections: AtomicUsize::new(0),
            },
            storage: StorageMetrics {
                database_size: AtomicU64::new(0),
                wal_size: AtomicU64::new(0),
                index_size: AtomicU64::new(0),
                page_reads: AtomicU64::new(0),
                page_writes: AtomicU64::new(0),
                page_splits: AtomicU64::new(0),
                page_merges: AtomicU64::new(0),
                fragmentation_ratio: Arc::new(RwLock::new(0.0)),
            },
            network: NetworkMetrics {
                bytes_sent: AtomicU64::new(0),
                bytes_received: AtomicU64::new(0),
                connections_opened: AtomicU64::new(0),
                connections_closed: AtomicU64::new(0),
                connection_errors: AtomicU64::new(0),
                active_connections: AtomicUsize::new(0),
                avg_latency: Arc::new(RwLock::new(Duration::ZERO)),
            },
            config,
            running: AtomicBool::new(false),
            collection_handle: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.running.store(true, Ordering::Relaxed);

        // Start background collection task
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let interval_duration = self.config.aggregation_interval;

        let handle = tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            while running_clone.load(Ordering::Relaxed) {
                interval.tick().await;
                
                // Perform periodic calculations and cleanup
                // This could include updating derived metrics, cleaning old data, etc.
                tracing::debug!("Metrics collection tick");
            }
        });

        {
            let mut handle_guard = self.collection_handle.write().await;
            *handle_guard = Some(handle);
        }
        tracing::info!("Metrics collection started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        
        {
            let mut handle_guard = self.collection_handle.write().await;
            if let Some(handle) = handle_guard.take() {
                handle.abort();
            }
        }

        tracing::info!("Metrics collection stopped");
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Record a database operation
    pub fn record_operation(
        &self,
        operation_type: OperationType,
        latency: Duration,
        size: Option<usize>,
        success: bool,
    ) {
        // Update operation counters
        match operation_type {
            OperationType::Read => {
                self.operations.reads.fetch_add(1, Ordering::Relaxed);
                if let Some(size) = size {
                    self.operations.total_bytes_read.fetch_add(size as u64, Ordering::Relaxed);
                }
            }
            OperationType::Write => {
                self.operations.writes.fetch_add(1, Ordering::Relaxed);
                if let Some(size) = size {
                    self.operations.total_bytes_written.fetch_add(size as u64, Ordering::Relaxed);
                }
            }
            OperationType::Delete => {
                self.operations.deletes.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Scan => {
                self.operations.scans.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Checkpoint => {
                self.operations.checkpoints.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Compaction => {
                self.operations.compactions.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Vacuum => {
                self.operations.vacuums.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Backup => {
                self.operations.backups.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update success/failure counters
        if success {
            self.operations.successful_operations.fetch_add(1, Ordering::Relaxed);
        } else {
            self.operations.failed_operations.fetch_add(1, Ordering::Relaxed);
        }

        // Update performance metrics
        if let Ok(mut histogram) = self.performance.latency_histogram.try_write() {
            histogram.record(latency);
        }

        if let Ok(mut throughput) = self.performance.throughput_tracker.try_write() {
            throughput.record(1);
        }
    }

    /// Record cache access
    pub fn record_cache_access(&self, cache_type: CacheType, hit: bool, size: Option<usize>) {
        match (cache_type, hit) {
            (CacheType::PageCache, true) => {
                self.cache.page_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::PageCache, false) => {
                self.cache.page_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::BTreeCache, true) => {
                self.cache.btree_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::BTreeCache, false) => {
                self.cache.btree_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::LSMCache, true) => {
                self.cache.lsm_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::LSMCache, false) => {
                self.cache.lsm_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::MetadataCache, true) => {
                self.cache.metadata_cache_hits.fetch_add(1, Ordering::Relaxed);
            }
            (CacheType::MetadataCache, false) => {
                self.cache.metadata_cache_misses.fetch_add(1, Ordering::Relaxed);
            }
        }

        if let Some(size) = size {
            self.cache.cache_size_bytes.fetch_add(size as u64, Ordering::Relaxed);
        }
    }

    /// Record transaction event
    pub fn record_transaction(&self, event: TransactionEvent, duration: Option<Duration>) {
        match event {
            TransactionEvent::Begin => {
                self.transactions.transactions_begun.fetch_add(1, Ordering::Relaxed);
                self.transactions.active_transactions.fetch_add(1, Ordering::Relaxed);
            }
            TransactionEvent::Commit => {
                self.transactions.transactions_committed.fetch_add(1, Ordering::Relaxed);
                self.transactions.active_transactions.fetch_sub(1, Ordering::Relaxed);
                
                if let Some(duration) = duration {
                    if let Ok(mut max_duration) = self.transactions.max_transaction_duration.try_write() {
                        if duration > *max_duration {
                            *max_duration = duration;
                        }
                    }
                }
            }
            TransactionEvent::Rollback => {
                self.transactions.transactions_rolled_back.fetch_add(1, Ordering::Relaxed);
                self.transactions.active_transactions.fetch_sub(1, Ordering::Relaxed);
            }
            TransactionEvent::Conflict => {
                self.transactions.transaction_conflicts.fetch_add(1, Ordering::Relaxed);
            }
            TransactionEvent::Deadlock => {
                self.transactions.deadlocks.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record error
    pub fn record_error(&self, error_type: ErrorType, error_msg: Option<String>) {
        self.errors.total_errors.fetch_add(1, Ordering::Relaxed);

        match error_type {
            ErrorType::IO => {
                self.errors.io_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::Corruption => {
                self.errors.corruption_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::OutOfMemory => {
                self.errors.oom_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::DiskFull => {
                self.errors.disk_full_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::NetworkError => {
                self.errors.network_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::SerializationError => {
                self.errors.serialization_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::ValidationError => {
                self.errors.validation_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::TimeoutError => {
                self.errors.timeout_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::PermissionError => {
                self.errors.permission_errors.fetch_add(1, Ordering::Relaxed);
            }
            ErrorType::ConfigurationError => {
                self.errors.config_errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Record last error
        if let Ok(mut last_error) = self.errors.last_error.try_write() {
            *last_error = Some((SystemTime::now(), error_msg.unwrap_or_else(|| format!("{:?}", error_type))));
        }
    }

    /// Update resource metrics
    pub fn update_resource_metrics(&self, resources: ResourceMetrics) {
        self.resources.memory_used.store(resources.memory_used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.memory_available.store(resources.memory_available.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.disk_used.store(resources.disk_used.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.disk_available.store(resources.disk_available.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.thread_count.store(resources.thread_count.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.open_files.store(resources.open_files.load(Ordering::Relaxed), Ordering::Relaxed);
        self.resources.network_connections.store(resources.network_connections.load(Ordering::Relaxed), Ordering::Relaxed);

        if let Ok(mut cpu_usage) = self.resources.cpu_usage.try_write() {
            if let Ok(source_cpu) = resources.cpu_usage.try_read() {
                *cpu_usage = *source_cpu;
            }
        }
    }

    /// Get metrics snapshot
    pub async fn get_snapshot(&self) -> Result<MetricsSnapshot> {
        let latency_histogram = self.performance.latency_histogram.read().await.clone();
        let throughput_tracker = self.performance.throughput_tracker.read().await.clone();

        Ok(MetricsSnapshot {
            timestamp: SystemTime::now(),
            operations: OperationSnapshot {
                reads: self.operations.reads.load(Ordering::Relaxed),
                writes: self.operations.writes.load(Ordering::Relaxed),
                deletes: self.operations.deletes.load(Ordering::Relaxed),
                scans: self.operations.scans.load(Ordering::Relaxed),
                checkpoints: self.operations.checkpoints.load(Ordering::Relaxed),
                compactions: self.operations.compactions.load(Ordering::Relaxed),
                vacuums: self.operations.vacuums.load(Ordering::Relaxed),
                backups: self.operations.backups.load(Ordering::Relaxed),
                successful_operations: self.operations.successful_operations.load(Ordering::Relaxed),
                failed_operations: self.operations.failed_operations.load(Ordering::Relaxed),
                total_bytes_read: self.operations.total_bytes_read.load(Ordering::Relaxed),
                total_bytes_written: self.operations.total_bytes_written.load(Ordering::Relaxed),
            },
            performance: PerformanceSnapshot {
                avg_latency: latency_histogram.average(),
                p50_latency: latency_histogram.percentile(50.0),
                p90_latency: latency_histogram.percentile(90.0),
                p95_latency: latency_histogram.percentile(95.0),
                p99_latency: latency_histogram.percentile(99.0),
                max_latency: latency_histogram.max,
                throughput_ops_per_sec: throughput_tracker.rate(),
                peak_throughput_ops_per_sec: throughput_tracker.peak_rate(),
                active_operations: self.performance.active_operations.load(Ordering::Relaxed),
                queue_depth: self.performance.queue_depth.load(Ordering::Relaxed),
                concurrent_reads: self.performance.concurrent_reads.load(Ordering::Relaxed),
                concurrent_writes: self.performance.concurrent_writes.load(Ordering::Relaxed),
            },
            cache: CacheSnapshot {
                page_cache_hits: self.cache.page_cache_hits.load(Ordering::Relaxed),
                page_cache_misses: self.cache.page_cache_misses.load(Ordering::Relaxed),
                btree_cache_hits: self.cache.btree_cache_hits.load(Ordering::Relaxed),
                btree_cache_misses: self.cache.btree_cache_misses.load(Ordering::Relaxed),
                lsm_cache_hits: self.cache.lsm_cache_hits.load(Ordering::Relaxed),
                lsm_cache_misses: self.cache.lsm_cache_misses.load(Ordering::Relaxed),
                metadata_cache_hits: self.cache.metadata_cache_hits.load(Ordering::Relaxed),
                metadata_cache_misses: self.cache.metadata_cache_misses.load(Ordering::Relaxed),
                cache_evictions: self.cache.cache_evictions.load(Ordering::Relaxed),
                cache_size_bytes: self.cache.cache_size_bytes.load(Ordering::Relaxed),
                cache_entries: self.cache.cache_entries.load(Ordering::Relaxed),
                hit_rate: self.calculate_hit_rate(),
                hits: self.cache.page_cache_hits.load(Ordering::Relaxed) + self.cache.btree_cache_hits.load(Ordering::Relaxed) + self.cache.lsm_cache_hits.load(Ordering::Relaxed) + self.cache.metadata_cache_hits.load(Ordering::Relaxed),
                misses: self.cache.page_cache_misses.load(Ordering::Relaxed) + self.cache.btree_cache_misses.load(Ordering::Relaxed) + self.cache.lsm_cache_misses.load(Ordering::Relaxed) + self.cache.metadata_cache_misses.load(Ordering::Relaxed),
                size_bytes: self.cache.cache_size_bytes.load(Ordering::Relaxed),
            },
            transactions: TransactionSnapshot {
                transactions_begun: self.transactions.transactions_begun.load(Ordering::Relaxed),
                transactions_committed: self.transactions.transactions_committed.load(Ordering::Relaxed),
                transactions_rolled_back: self.transactions.transactions_rolled_back.load(Ordering::Relaxed),
                transaction_conflicts: self.transactions.transaction_conflicts.load(Ordering::Relaxed),
                deadlocks: self.transactions.deadlocks.load(Ordering::Relaxed),
                active_transactions: self.transactions.active_transactions.load(Ordering::Relaxed),
                max_transaction_duration: *self.transactions.max_transaction_duration.read().await,
                avg_transaction_size: self.transactions.avg_transaction_size.load(Ordering::Relaxed),
                commits: self.transactions.transactions_committed.load(Ordering::Relaxed),
                rollbacks: self.transactions.transactions_rolled_back.load(Ordering::Relaxed),
                conflicts: self.transactions.transaction_conflicts.load(Ordering::Relaxed),
                active: self.transactions.active_transactions.load(Ordering::Relaxed),
            },
            errors: ErrorSnapshot {
                total_errors: self.errors.total_errors.load(Ordering::Relaxed),
                io_errors: self.errors.io_errors.load(Ordering::Relaxed),
                corruption_errors: self.errors.corruption_errors.load(Ordering::Relaxed),
                oom_errors: self.errors.oom_errors.load(Ordering::Relaxed),
                disk_full_errors: self.errors.disk_full_errors.load(Ordering::Relaxed),
                network_errors: self.errors.network_errors.load(Ordering::Relaxed),
                serialization_errors: self.errors.serialization_errors.load(Ordering::Relaxed),
                validation_errors: self.errors.validation_errors.load(Ordering::Relaxed),
                timeout_errors: self.errors.timeout_errors.load(Ordering::Relaxed),
                permission_errors: self.errors.permission_errors.load(Ordering::Relaxed),
                config_errors: self.errors.config_errors.load(Ordering::Relaxed),
                error_rate: *self.errors.error_rate.read().await,
                last_error: self.errors.last_error.read().await.clone(),
                total: self.errors.total_errors.load(Ordering::Relaxed),
                by_type: {
                    let mut by_type = HashMap::new();
                    by_type.insert("io".to_string(), self.errors.io_errors.load(Ordering::Relaxed));
                    by_type.insert("corruption".to_string(), self.errors.corruption_errors.load(Ordering::Relaxed));
                    by_type.insert("oom".to_string(), self.errors.oom_errors.load(Ordering::Relaxed));
                    by_type.insert("disk_full".to_string(), self.errors.disk_full_errors.load(Ordering::Relaxed));
                    by_type.insert("network".to_string(), self.errors.network_errors.load(Ordering::Relaxed));
                    by_type.insert("serialization".to_string(), self.errors.serialization_errors.load(Ordering::Relaxed));
                    by_type.insert("validation".to_string(), self.errors.validation_errors.load(Ordering::Relaxed));
                    by_type.insert("timeout".to_string(), self.errors.timeout_errors.load(Ordering::Relaxed));
                    by_type.insert("permission".to_string(), self.errors.permission_errors.load(Ordering::Relaxed));
                    by_type.insert("config".to_string(), self.errors.config_errors.load(Ordering::Relaxed));
                    by_type
                },
            },
            resources: ResourceSnapshot {
                memory_used: self.resources.memory_used.load(Ordering::Relaxed),
                memory_available: self.resources.memory_available.load(Ordering::Relaxed),
                disk_used: self.resources.disk_used.load(Ordering::Relaxed),
                disk_available: self.resources.disk_available.load(Ordering::Relaxed),
                cpu_usage: *self.resources.cpu_usage.read().await,
                thread_count: self.resources.thread_count.load(Ordering::Relaxed),
                open_files: self.resources.open_files.load(Ordering::Relaxed),
                network_connections: self.resources.network_connections.load(Ordering::Relaxed),
            },
            storage: StorageSnapshot {
                database_size: self.storage.database_size.load(Ordering::Relaxed),
                wal_size: self.storage.wal_size.load(Ordering::Relaxed),
                index_size: self.storage.index_size.load(Ordering::Relaxed),
                page_reads: self.storage.page_reads.load(Ordering::Relaxed),
                page_writes: self.storage.page_writes.load(Ordering::Relaxed),
                page_splits: self.storage.page_splits.load(Ordering::Relaxed),
                page_merges: self.storage.page_merges.load(Ordering::Relaxed),
                fragmentation_ratio: *self.storage.fragmentation_ratio.read().await,
            },
            network: NetworkSnapshot {
                bytes_sent: self.network.bytes_sent.load(Ordering::Relaxed),
                bytes_received: self.network.bytes_received.load(Ordering::Relaxed),
                connections_opened: self.network.connections_opened.load(Ordering::Relaxed),
                connections_closed: self.network.connections_closed.load(Ordering::Relaxed),
                connection_errors: self.network.connection_errors.load(Ordering::Relaxed),
                active_connections: self.network.active_connections.load(Ordering::Relaxed),
                avg_latency: *self.network.avg_latency.read().await,
            },
            latency: LatencySnapshot {
                sum: latency_histogram.sum.as_secs_f64() * 1000.0, // Convert to milliseconds
                count: latency_histogram.count,
            },
        })
    }

    fn calculate_hit_rate(&self) -> f64 {
        let total_hits = self.cache.page_cache_hits.load(Ordering::Relaxed) +
                        self.cache.btree_cache_hits.load(Ordering::Relaxed) +
                        self.cache.lsm_cache_hits.load(Ordering::Relaxed) +
                        self.cache.metadata_cache_hits.load(Ordering::Relaxed);
        
        let total_misses = self.cache.page_cache_misses.load(Ordering::Relaxed) +
                          self.cache.btree_cache_misses.load(Ordering::Relaxed) +
                          self.cache.lsm_cache_misses.load(Ordering::Relaxed) +
                          self.cache.metadata_cache_misses.load(Ordering::Relaxed);
        
        let total_accesses = total_hits + total_misses;
        
        if total_accesses == 0 {
            0.0
        } else {
            total_hits as f64 / total_accesses as f64
        }
    }
}

/// Snapshot of all metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: SystemTime,
    pub operations: OperationSnapshot,
    pub performance: PerformanceSnapshot,
    pub cache: CacheSnapshot,
    pub transactions: TransactionSnapshot,
    pub errors: ErrorSnapshot,
    pub resources: ResourceSnapshot,
    pub storage: StorageSnapshot,
    pub network: NetworkSnapshot,
    pub latency: LatencySnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySnapshot {
    pub sum: f64,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub scans: u64,
    pub checkpoints: u64,
    pub compactions: u64,
    pub vacuums: u64,
    pub backups: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub avg_latency: Duration,
    pub p50_latency: Duration,
    pub p90_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub max_latency: Duration,
    pub throughput_ops_per_sec: f64,
    pub peak_throughput_ops_per_sec: f64,
    pub active_operations: usize,
    pub queue_depth: usize,
    pub concurrent_reads: usize,
    pub concurrent_writes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSnapshot {
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,
    pub btree_cache_hits: u64,
    pub btree_cache_misses: u64,
    pub lsm_cache_hits: u64,
    pub lsm_cache_misses: u64,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_size_bytes: u64,
    pub cache_entries: usize,
    pub hit_rate: f64,
    pub hits: u64,
    pub misses: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionSnapshot {
    pub transactions_begun: u64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub transaction_conflicts: u64,
    pub deadlocks: u64,
    pub active_transactions: usize,
    pub max_transaction_duration: Duration,
    pub avg_transaction_size: u64,
    pub commits: u64,
    pub rollbacks: u64,
    pub conflicts: u64,
    pub active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorSnapshot {
    pub total_errors: u64,
    pub io_errors: u64,
    pub corruption_errors: u64,
    pub oom_errors: u64,
    pub disk_full_errors: u64,
    pub network_errors: u64,
    pub serialization_errors: u64,
    pub validation_errors: u64,
    pub timeout_errors: u64,
    pub permission_errors: u64,
    pub config_errors: u64,
    pub error_rate: f64,
    pub last_error: Option<(SystemTime, String)>,
    pub total: u64,
    pub by_type: std::collections::HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub memory_used: u64,
    pub memory_available: u64,
    pub disk_used: u64,
    pub disk_available: u64,
    pub cpu_usage: f64,
    pub thread_count: usize,
    pub open_files: usize,
    pub network_connections: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSnapshot {
    pub database_size: u64,
    pub wal_size: u64,
    pub index_size: u64,
    pub page_reads: u64,
    pub page_writes: u64,
    pub page_splits: u64,
    pub page_merges: u64,
    pub fragmentation_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSnapshot {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub connections_opened: u64,
    pub connections_closed: u64,
    pub connection_errors: u64,
    pub active_connections: usize,
    pub avg_latency: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_metrics_engine_creation() {
        let config = ObservabilityConfig::default();
        let engine = MetricsEngine::new(config).unwrap();
        assert!(!engine.is_running());
    }

    #[tokio::test]
    async fn test_operation_recording() {
        let config = ObservabilityConfig::default();
        let engine = MetricsEngine::new(config).unwrap();

        engine.record_operation(
            OperationType::Read,
            Duration::from_micros(100),
            Some(1024),
            true,
        );

        let snapshot = engine.get_snapshot().await.unwrap();
        assert_eq!(snapshot.operations.reads, 1);
        assert_eq!(snapshot.operations.successful_operations, 1);
        assert_eq!(snapshot.operations.total_bytes_read, 1024);
    }

    #[tokio::test]
    async fn test_cache_recording() {
        let config = ObservabilityConfig::default();
        let engine = MetricsEngine::new(config).unwrap();

        engine.record_cache_access(CacheType::PageCache, true, Some(4096));
        engine.record_cache_access(CacheType::PageCache, false, Some(4096));

        let snapshot = engine.get_snapshot().await.unwrap();
        assert_eq!(snapshot.cache.page_cache_hits, 1);
        assert_eq!(snapshot.cache.page_cache_misses, 1);
        assert_eq!(snapshot.cache.hit_rate, 0.5);
    }

    #[tokio::test]
    async fn test_transaction_recording() {
        let config = ObservabilityConfig::default();
        let engine = MetricsEngine::new(config).unwrap();

        engine.record_transaction(TransactionEvent::Begin, None);
        engine.record_transaction(TransactionEvent::Commit, Some(Duration::from_millis(10)));

        let snapshot = engine.get_snapshot().await.unwrap();
        assert_eq!(snapshot.transactions.transactions_begun, 1);
        assert_eq!(snapshot.transactions.transactions_committed, 1);
        assert_eq!(snapshot.transactions.active_transactions, 0);
    }

    #[test]
    fn test_latency_histogram() {
        let mut histogram = LatencyHistogram::new();

        histogram.record(Duration::from_micros(50));
        histogram.record(Duration::from_micros(150));
        histogram.record(Duration::from_millis(1));

        assert_eq!(histogram.count, 3);
        assert!(histogram.average() > Duration::ZERO);
        assert!(histogram.max >= Duration::from_millis(1));
    }

    #[test]
    fn test_throughput_tracker() {
        let mut tracker = ThroughputTracker::new(Duration::from_secs(60));

        tracker.record(10);
        tracker.record(20);
        tracker.record(30);

        assert!(tracker.rate() >= 0.0);
        assert!(tracker.peak_rate() >= tracker.rate());
    }
}
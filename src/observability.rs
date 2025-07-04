use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use std::collections::HashMap;

/// Production observability metrics for Lightning DB
#[derive(Debug, Clone)]
pub struct Metrics {
    // Operation counters
    pub operations: OperationMetrics,
    
    // Performance metrics
    pub performance: PerformanceMetrics,
    
    // Resource metrics
    pub resources: ResourceMetrics,
    
    // Error metrics
    pub errors: ErrorMetrics,
    
    // Transaction metrics
    pub transactions: TransactionMetrics,
    
    // Cache metrics
    pub cache: CacheMetrics,
    
    // WAL metrics
    pub wal: WalMetrics,
    
    // System health
    pub health: HealthMetrics,
    
    // Start time for uptime calculation
    start_time: Instant,
}

#[derive(Debug, Clone)]
pub struct OperationMetrics {
    pub reads: Arc<AtomicU64>,
    pub writes: Arc<AtomicU64>,
    pub deletes: Arc<AtomicU64>,
    pub scans: Arc<AtomicU64>,
    pub checkpoints: Arc<AtomicU64>,
    pub compactions: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub latencies: Arc<Mutex<LatencyHistogram>>,
    pub throughput: Arc<Mutex<ThroughputTracker>>,
    pub queue_depth: Arc<AtomicUsize>,
    pub active_operations: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    pub memory_usage: Arc<AtomicU64>,
    pub disk_usage: Arc<AtomicU64>,
    pub open_files: Arc<AtomicUsize>,
    pub thread_count: Arc<AtomicUsize>,
    pub cpu_usage: Arc<Mutex<f64>>,
}

#[derive(Debug, Clone)]
pub struct ErrorMetrics {
    pub total_errors: Arc<AtomicU64>,
    pub io_errors: Arc<AtomicU64>,
    pub corruption_errors: Arc<AtomicU64>,
    pub oom_errors: Arc<AtomicU64>,
    pub timeout_errors: Arc<AtomicU64>,
    pub error_types: Arc<Mutex<HashMap<String, u64>>>,
}

#[derive(Debug, Clone)]
pub struct TransactionMetrics {
    pub active_transactions: Arc<AtomicUsize>,
    pub committed: Arc<AtomicU64>,
    pub aborted: Arc<AtomicU64>,
    pub conflicts: Arc<AtomicU64>,
    pub avg_tx_size: Arc<AtomicU64>,
    pub max_tx_duration: Arc<Mutex<Duration>>,
}

#[derive(Debug, Clone)]
pub struct CacheMetrics {
    pub hits: Arc<AtomicU64>,
    pub misses: Arc<AtomicU64>,
    pub evictions: Arc<AtomicU64>,
    pub size_bytes: Arc<AtomicU64>,
    pub entry_count: Arc<AtomicUsize>,
    pub hit_rate: Arc<Mutex<f64>>,
}

#[derive(Debug, Clone)]
pub struct WalMetrics {
    pub writes: Arc<AtomicU64>,
    pub syncs: Arc<AtomicU64>,
    pub rotations: Arc<AtomicU64>,
    pub size_bytes: Arc<AtomicU64>,
    pub recovery_time: Arc<Mutex<Option<Duration>>>,
    pub checkpoint_lag: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct HealthMetrics {
    pub status: Arc<Mutex<HealthStatus>>,
    pub uptime: Arc<Mutex<Duration>>,
    pub last_error: Arc<Mutex<Option<(SystemTime, String)>>>,
    pub warnings: Arc<AtomicU64>,
    pub degraded_operations: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

/// Latency histogram for tracking operation latencies
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    buckets: Vec<(Duration, u64)>,
    count: u64,
    sum: Duration,
    min: Duration,
    max: Duration,
}

impl LatencyHistogram {
    fn new() -> Self {
        let buckets = vec![
            (Duration::from_micros(1), 0),
            (Duration::from_micros(10), 0),
            (Duration::from_micros(100), 0),
            (Duration::from_millis(1), 0),
            (Duration::from_millis(10), 0),
            (Duration::from_millis(100), 0),
            (Duration::from_secs(1), 0),
        ];
        
        Self {
            buckets,
            count: 0,
            sum: Duration::ZERO,
            min: Duration::MAX,
            max: Duration::ZERO,
        }
    }
    
    fn record(&mut self, latency: Duration) {
        self.count += 1;
        self.sum += latency;
        self.min = self.min.min(latency);
        self.max = self.max.max(latency);
        
        // Update buckets
        for (threshold, count) in &mut self.buckets {
            if latency <= *threshold {
                *count += 1;
                break;
            }
        }
    }
    
    fn percentile(&self, p: f64) -> Duration {
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
    
    fn average(&self) -> Duration {
        if self.count == 0 {
            Duration::ZERO
        } else {
            self.sum / self.count as u32
        }
    }
}

/// Throughput tracker for measuring operations per second
#[derive(Debug, Clone)]
pub struct ThroughputTracker {
    window: Duration,
    samples: Vec<(Instant, u64)>,
}

impl ThroughputTracker {
    fn new(window: Duration) -> Self {
        Self {
            window,
            samples: Vec::new(),
        }
    }
    
    fn record(&mut self, count: u64) {
        let now = Instant::now();
        self.samples.push((now, count));
        
        // Remove old samples
        let cutoff = now - self.window;
        self.samples.retain(|(time, _)| *time > cutoff);
    }
    
    fn rate(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        
        let total: u64 = self.samples.iter().map(|(_, count)| count).sum();
        let duration = self.samples.last().unwrap().0 - self.samples.first().unwrap().0;
        
        if duration.as_secs_f64() > 0.0 {
            total as f64 / duration.as_secs_f64()
        } else {
            0.0
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            operations: OperationMetrics {
                reads: Arc::new(AtomicU64::new(0)),
                writes: Arc::new(AtomicU64::new(0)),
                deletes: Arc::new(AtomicU64::new(0)),
                scans: Arc::new(AtomicU64::new(0)),
                checkpoints: Arc::new(AtomicU64::new(0)),
                compactions: Arc::new(AtomicU64::new(0)),
            },
            performance: PerformanceMetrics {
                latencies: Arc::new(Mutex::new(LatencyHistogram::new())),
                throughput: Arc::new(Mutex::new(ThroughputTracker::new(Duration::from_secs(60)))),
                queue_depth: Arc::new(AtomicUsize::new(0)),
                active_operations: Arc::new(AtomicUsize::new(0)),
            },
            resources: ResourceMetrics {
                memory_usage: Arc::new(AtomicU64::new(0)),
                disk_usage: Arc::new(AtomicU64::new(0)),
                open_files: Arc::new(AtomicUsize::new(0)),
                thread_count: Arc::new(AtomicUsize::new(0)),
                cpu_usage: Arc::new(Mutex::new(0.0)),
            },
            errors: ErrorMetrics {
                total_errors: Arc::new(AtomicU64::new(0)),
                io_errors: Arc::new(AtomicU64::new(0)),
                corruption_errors: Arc::new(AtomicU64::new(0)),
                oom_errors: Arc::new(AtomicU64::new(0)),
                timeout_errors: Arc::new(AtomicU64::new(0)),
                error_types: Arc::new(Mutex::new(HashMap::new())),
            },
            transactions: TransactionMetrics {
                active_transactions: Arc::new(AtomicUsize::new(0)),
                committed: Arc::new(AtomicU64::new(0)),
                aborted: Arc::new(AtomicU64::new(0)),
                conflicts: Arc::new(AtomicU64::new(0)),
                avg_tx_size: Arc::new(AtomicU64::new(0)),
                max_tx_duration: Arc::new(Mutex::new(Duration::ZERO)),
            },
            cache: CacheMetrics {
                hits: Arc::new(AtomicU64::new(0)),
                misses: Arc::new(AtomicU64::new(0)),
                evictions: Arc::new(AtomicU64::new(0)),
                size_bytes: Arc::new(AtomicU64::new(0)),
                entry_count: Arc::new(AtomicUsize::new(0)),
                hit_rate: Arc::new(Mutex::new(0.0)),
            },
            wal: WalMetrics {
                writes: Arc::new(AtomicU64::new(0)),
                syncs: Arc::new(AtomicU64::new(0)),
                rotations: Arc::new(AtomicU64::new(0)),
                size_bytes: Arc::new(AtomicU64::new(0)),
                recovery_time: Arc::new(Mutex::new(None)),
                checkpoint_lag: Arc::new(AtomicU64::new(0)),
            },
            health: HealthMetrics {
                status: Arc::new(Mutex::new(HealthStatus::Healthy)),
                uptime: Arc::new(Mutex::new(Duration::ZERO)),
                last_error: Arc::new(Mutex::new(None)),
                warnings: Arc::new(AtomicU64::new(0)),
                degraded_operations: Arc::new(AtomicU64::new(0)),
            },
            start_time: Instant::now(),
        }
    }
    
    /// Record a read operation
    pub fn record_read(&self, latency: Duration) {
        self.operations.reads.fetch_add(1, Ordering::Relaxed);
        self.record_operation_latency("read", latency);
    }
    
    /// Record a write operation
    pub fn record_write(&self, latency: Duration) {
        self.operations.writes.fetch_add(1, Ordering::Relaxed);
        self.record_operation_latency("write", latency);
    }
    
    /// Record a delete operation
    pub fn record_delete(&self, latency: Duration) {
        self.operations.deletes.fetch_add(1, Ordering::Relaxed);
        self.record_operation_latency("delete", latency);
    }
    
    /// Record an operation latency
    fn record_operation_latency(&self, _op_type: &str, latency: Duration) {
        if let Ok(mut hist) = self.performance.latencies.lock() {
            hist.record(latency);
        }
        
        if let Ok(mut tracker) = self.performance.throughput.lock() {
            tracker.record(1);
        }
    }
    
    /// Record an error
    pub fn record_error(&self, error_type: &str) {
        self.errors.total_errors.fetch_add(1, Ordering::Relaxed);
        
        match error_type {
            "io" => self.errors.io_errors.fetch_add(1, Ordering::Relaxed),
            "corruption" => self.errors.corruption_errors.fetch_add(1, Ordering::Relaxed),
            "oom" => self.errors.oom_errors.fetch_add(1, Ordering::Relaxed),
            "timeout" => self.errors.timeout_errors.fetch_add(1, Ordering::Relaxed),
            _ => 0,
        };
        
        if let Ok(mut error_types) = self.errors.error_types.lock() {
            *error_types.entry(error_type.to_string()).or_insert(0) += 1;
        }
        
        if let Ok(mut last_error) = self.health.last_error.lock() {
            *last_error = Some((SystemTime::now(), error_type.to_string()));
        }
    }
    
    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.cache.hits.fetch_add(1, Ordering::Relaxed);
        self.update_cache_hit_rate();
    }
    
    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.cache.misses.fetch_add(1, Ordering::Relaxed);
        self.update_cache_hit_rate();
    }
    
    /// Update cache hit rate
    fn update_cache_hit_rate(&self) {
        let hits = self.cache.hits.load(Ordering::Relaxed);
        let misses = self.cache.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total > 0 {
            if let Ok(mut hit_rate) = self.cache.hit_rate.lock() {
                *hit_rate = hits as f64 / total as f64;
            }
        }
    }
    
    /// Record transaction commit
    pub fn record_transaction_commit(&self, duration: Duration, operations: usize) {
        self.transactions.committed.fetch_add(1, Ordering::Relaxed);
        self.transactions.active_transactions.fetch_sub(1, Ordering::Relaxed);
        
        // Update average transaction size
        let current_avg = self.transactions.avg_tx_size.load(Ordering::Relaxed);
        let committed = self.transactions.committed.load(Ordering::Relaxed);
        let new_avg = ((current_avg * (committed - 1)) + operations as u64) / committed;
        self.transactions.avg_tx_size.store(new_avg, Ordering::Relaxed);
        
        // Update max transaction duration
        if let Ok(mut max_duration) = self.transactions.max_tx_duration.lock() {
            if duration > *max_duration {
                *max_duration = duration;
            }
        }
    }
    
    /// Record transaction abort
    pub fn record_transaction_abort(&self) {
        self.transactions.aborted.fetch_add(1, Ordering::Relaxed);
        self.transactions.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }
    
    /// Start a transaction
    pub fn start_transaction(&self) {
        self.transactions.active_transactions.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Update resource metrics
    pub fn update_resources(&self, memory: u64, disk: u64, files: usize, threads: usize, cpu: f64) {
        self.resources.memory_usage.store(memory, Ordering::Relaxed);
        self.resources.disk_usage.store(disk, Ordering::Relaxed);
        self.resources.open_files.store(files, Ordering::Relaxed);
        self.resources.thread_count.store(threads, Ordering::Relaxed);
        
        if let Ok(mut cpu_usage) = self.resources.cpu_usage.lock() {
            *cpu_usage = cpu;
        }
    }
    
    /// Update health status
    pub fn update_health(&self, status: HealthStatus) {
        if let Ok(mut health_status) = self.health.status.lock() {
            *health_status = status;
        }
        
        // Update uptime
        if let Ok(mut uptime) = self.health.uptime.lock() {
            *uptime = self.start_time.elapsed();
        }
    }
    
    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let latencies = self.performance.latencies.lock().unwrap().clone();
        let throughput = self.performance.throughput.lock().unwrap().rate();
        let cache_hit_rate = *self.cache.hit_rate.lock().unwrap();
        let health_status = self.health.status.lock().unwrap().clone();
        let uptime = self.start_time.elapsed();
        
        MetricsSnapshot {
            timestamp: SystemTime::now(),
            operations: OperationSnapshot {
                reads: self.operations.reads.load(Ordering::Relaxed),
                writes: self.operations.writes.load(Ordering::Relaxed),
                deletes: self.operations.deletes.load(Ordering::Relaxed),
                scans: self.operations.scans.load(Ordering::Relaxed),
                checkpoints: self.operations.checkpoints.load(Ordering::Relaxed),
                compactions: self.operations.compactions.load(Ordering::Relaxed),
            },
            performance: PerformanceSnapshot {
                throughput_ops_sec: throughput,
                latency_avg: latencies.average(),
                latency_p50: latencies.percentile(50.0),
                latency_p95: latencies.percentile(95.0),
                latency_p99: latencies.percentile(99.0),
                latency_max: latencies.max,
                queue_depth: self.performance.queue_depth.load(Ordering::Relaxed),
                active_operations: self.performance.active_operations.load(Ordering::Relaxed),
            },
            resources: ResourceSnapshot {
                memory_usage_bytes: self.resources.memory_usage.load(Ordering::Relaxed),
                disk_usage_bytes: self.resources.disk_usage.load(Ordering::Relaxed),
                open_files: self.resources.open_files.load(Ordering::Relaxed),
                thread_count: self.resources.thread_count.load(Ordering::Relaxed),
                cpu_usage_percent: *self.resources.cpu_usage.lock().unwrap(),
            },
            errors: ErrorSnapshot {
                total_errors: self.errors.total_errors.load(Ordering::Relaxed),
                io_errors: self.errors.io_errors.load(Ordering::Relaxed),
                corruption_errors: self.errors.corruption_errors.load(Ordering::Relaxed),
                oom_errors: self.errors.oom_errors.load(Ordering::Relaxed),
                timeout_errors: self.errors.timeout_errors.load(Ordering::Relaxed),
                last_error: self.health.last_error.lock().unwrap().clone(),
            },
            transactions: TransactionSnapshot {
                active: self.transactions.active_transactions.load(Ordering::Relaxed),
                committed: self.transactions.committed.load(Ordering::Relaxed),
                aborted: self.transactions.aborted.load(Ordering::Relaxed),
                conflicts: self.transactions.conflicts.load(Ordering::Relaxed),
                avg_size: self.transactions.avg_tx_size.load(Ordering::Relaxed),
            },
            cache: CacheSnapshot {
                hits: self.cache.hits.load(Ordering::Relaxed),
                misses: self.cache.misses.load(Ordering::Relaxed),
                hit_rate_percent: cache_hit_rate * 100.0,
                evictions: self.cache.evictions.load(Ordering::Relaxed),
                size_bytes: self.cache.size_bytes.load(Ordering::Relaxed),
                entry_count: self.cache.entry_count.load(Ordering::Relaxed),
            },
            wal: WalSnapshot {
                writes: self.wal.writes.load(Ordering::Relaxed),
                syncs: self.wal.syncs.load(Ordering::Relaxed),
                rotations: self.wal.rotations.load(Ordering::Relaxed),
                size_bytes: self.wal.size_bytes.load(Ordering::Relaxed),
                checkpoint_lag_bytes: self.wal.checkpoint_lag.load(Ordering::Relaxed),
            },
            health: HealthSnapshot {
                status: health_status,
                uptime,
                warnings: self.health.warnings.load(Ordering::Relaxed),
                degraded_operations: self.health.degraded_operations.load(Ordering::Relaxed),
            },
        }
    }
    
    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let snapshot = self.snapshot();
        let mut output = String::new();
        
        // Operation metrics
        output.push_str(&format!("# TYPE lightning_db_operations_total counter\n"));
        output.push_str(&format!("lightning_db_operations_total{{type=\"read\"}} {}\n", snapshot.operations.reads));
        output.push_str(&format!("lightning_db_operations_total{{type=\"write\"}} {}\n", snapshot.operations.writes));
        output.push_str(&format!("lightning_db_operations_total{{type=\"delete\"}} {}\n", snapshot.operations.deletes));
        
        // Latency metrics
        output.push_str(&format!("# TYPE lightning_db_latency_seconds summary\n"));
        output.push_str(&format!("lightning_db_latency_seconds{{quantile=\"0.5\"}} {:.6}\n", snapshot.performance.latency_p50.as_secs_f64()));
        output.push_str(&format!("lightning_db_latency_seconds{{quantile=\"0.95\"}} {:.6}\n", snapshot.performance.latency_p95.as_secs_f64()));
        output.push_str(&format!("lightning_db_latency_seconds{{quantile=\"0.99\"}} {:.6}\n", snapshot.performance.latency_p99.as_secs_f64()));
        
        // Cache metrics
        output.push_str(&format!("# TYPE lightning_db_cache_hit_rate gauge\n"));
        output.push_str(&format!("lightning_db_cache_hit_rate {:.4}\n", snapshot.cache.hit_rate_percent / 100.0));
        
        // Resource metrics
        output.push_str(&format!("# TYPE lightning_db_memory_bytes gauge\n"));
        output.push_str(&format!("lightning_db_memory_bytes {}\n", snapshot.resources.memory_usage_bytes));
        
        // Error metrics
        output.push_str(&format!("# TYPE lightning_db_errors_total counter\n"));
        output.push_str(&format!("lightning_db_errors_total{{type=\"io\"}} {}\n", snapshot.errors.io_errors));
        output.push_str(&format!("lightning_db_errors_total{{type=\"corruption\"}} {}\n", snapshot.errors.corruption_errors));
        
        // Transaction metrics
        output.push_str(&format!("# TYPE lightning_db_transactions_total counter\n"));
        output.push_str(&format!("lightning_db_transactions_total{{result=\"committed\"}} {}\n", snapshot.transactions.committed));
        output.push_str(&format!("lightning_db_transactions_total{{result=\"aborted\"}} {}\n", snapshot.transactions.aborted));
        
        // Health metric
        output.push_str(&format!("# TYPE lightning_db_up gauge\n"));
        let up = match snapshot.health.status {
            HealthStatus::Healthy => 1,
            _ => 0,
        };
        output.push_str(&format!("lightning_db_up {}\n", up));
        
        output
    }
}

/// Snapshot of all metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub timestamp: SystemTime,
    pub operations: OperationSnapshot,
    pub performance: PerformanceSnapshot,
    pub resources: ResourceSnapshot,
    pub errors: ErrorSnapshot,
    pub transactions: TransactionSnapshot,
    pub cache: CacheSnapshot,
    pub wal: WalSnapshot,
    pub health: HealthSnapshot,
}

#[derive(Debug, Clone)]
pub struct OperationSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub scans: u64,
    pub checkpoints: u64,
    pub compactions: u64,
}

#[derive(Debug, Clone)]
pub struct PerformanceSnapshot {
    pub throughput_ops_sec: f64,
    pub latency_avg: Duration,
    pub latency_p50: Duration,
    pub latency_p95: Duration,
    pub latency_p99: Duration,
    pub latency_max: Duration,
    pub queue_depth: usize,
    pub active_operations: usize,
}

#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub memory_usage_bytes: u64,
    pub disk_usage_bytes: u64,
    pub open_files: usize,
    pub thread_count: usize,
    pub cpu_usage_percent: f64,
}

#[derive(Debug, Clone)]
pub struct ErrorSnapshot {
    pub total_errors: u64,
    pub io_errors: u64,
    pub corruption_errors: u64,
    pub oom_errors: u64,
    pub timeout_errors: u64,
    pub last_error: Option<(SystemTime, String)>,
}

#[derive(Debug, Clone)]
pub struct TransactionSnapshot {
    pub active: usize,
    pub committed: u64,
    pub aborted: u64,
    pub conflicts: u64,
    pub avg_size: u64,
}

#[derive(Debug, Clone)]
pub struct CacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub hit_rate_percent: f64,
    pub evictions: u64,
    pub size_bytes: u64,
    pub entry_count: usize,
}

#[derive(Debug, Clone)]
pub struct WalSnapshot {
    pub writes: u64,
    pub syncs: u64,
    pub rotations: u64,
    pub size_bytes: u64,
    pub checkpoint_lag_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub status: HealthStatus,
    pub uptime: Duration,
    pub warnings: u64,
    pub degraded_operations: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_recording() {
        let metrics = Metrics::new();
        
        // Record some operations
        metrics.record_read(Duration::from_micros(100));
        metrics.record_write(Duration::from_micros(200));
        metrics.record_delete(Duration::from_micros(150));
        
        // Check counters
        assert_eq!(metrics.operations.reads.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.operations.writes.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.operations.deletes.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_cache_hit_rate() {
        let metrics = Metrics::new();
        
        // Record cache operations
        for _ in 0..70 {
            metrics.record_cache_hit();
        }
        for _ in 0..30 {
            metrics.record_cache_miss();
        }
        
        // Check hit rate
        let hit_rate = *metrics.cache.hit_rate.lock().unwrap();
        assert!((hit_rate - 0.7).abs() < 0.01);
    }
    
    #[test]
    fn test_prometheus_export() {
        let metrics = Metrics::new();
        
        // Record some data
        metrics.record_read(Duration::from_micros(100));
        metrics.record_write(Duration::from_micros(200));
        metrics.record_error("io");
        
        // Export and check format
        let export = metrics.export_prometheus();
        assert!(export.contains("lightning_db_operations_total{type=\"read\"} 1"));
        assert!(export.contains("lightning_db_operations_total{type=\"write\"} 1"));
        assert!(export.contains("lightning_db_errors_total{type=\"io\"} 1"));
    }
}
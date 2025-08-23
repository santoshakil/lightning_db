use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram, Recorder};
use metrics_exporter_prometheus::PrometheusBuilder;
use parking_lot::{RwLock, Mutex};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use hdrhistogram::Histogram as HdrHistogram;

#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    // Operation metrics
    pub operations_total: Counter,
    pub operations_duration: Histogram,
    pub operations_errors: Counter,
    
    // Cache metrics
    pub cache_hits: Counter,
    pub cache_misses: Counter,
    pub cache_size: Gauge,
    pub cache_evictions: Counter,
    
    // Transaction metrics
    pub transactions_active: Gauge,
    pub transactions_committed: Counter,
    pub transactions_aborted: Counter,
    pub transaction_duration: Histogram,
    
    // Lock metrics
    pub locks_acquired: Counter,
    pub locks_contention: Histogram,
    pub locks_timeout: Counter,
    
    // Storage metrics
    pub bytes_written: Counter,
    pub bytes_read: Counter,
    pub disk_usage: Gauge,
    pub file_handles: Gauge,
    
    // Memory metrics
    pub memory_usage: Gauge,
    pub memory_allocated: Counter,
    pub memory_freed: Counter,
    
    // Performance metrics
    pub slow_operations: Counter,
    pub throughput: Gauge,
    pub latency_p50: Gauge,
    pub latency_p95: Gauge,
    pub latency_p99: Gauge,
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        // Register metrics descriptions
        metrics::describe_counter!("db_operations_total", "Total database operations");
        metrics::describe_histogram!("db_operations_duration_seconds", "Database operation duration");
        metrics::describe_counter!("db_operations_errors_total", "Total database operation errors");
        
        metrics::describe_counter!("db_cache_hits_total", "Cache hits");
        metrics::describe_counter!("db_cache_misses_total", "Cache misses");
        metrics::describe_gauge!("db_cache_size_bytes", "Current cache size in bytes");
        metrics::describe_counter!("db_cache_evictions_total", "Cache evictions");
        
        metrics::describe_gauge!("db_transactions_active", "Active transactions");
        metrics::describe_counter!("db_transactions_committed_total", "Committed transactions");
        metrics::describe_counter!("db_transactions_aborted_total", "Aborted transactions");
        
        Self {
            // Operations
            operations_total: counter!("db_operations_total"),
            operations_duration: histogram!("db_operations_duration_seconds"),
            operations_errors: counter!("db_operations_errors_total"),
            
            // Cache
            cache_hits: counter!("db_cache_hits_total"),
            cache_misses: counter!("db_cache_misses_total"),
            cache_size: gauge!("db_cache_size_bytes"),
            cache_evictions: counter!("db_cache_evictions_total"),
            
            // Transactions
            transactions_active: gauge!("db_transactions_active"),
            transactions_committed: counter!("db_transactions_committed_total"),
            transactions_aborted: counter!("db_transactions_aborted_total"),
            transaction_duration: histogram!("db_transaction_duration_seconds"),
            
            // Locks
            locks_acquired: counter!("db_locks_acquired_total"),
            locks_contention: histogram!("db_lock_contention_seconds"),
            locks_timeout: counter!("db_locks_timeout_total"),
            
            // Storage
            bytes_written: counter!("db_bytes_written_total"),
            bytes_read: counter!("db_bytes_read_total"),
            disk_usage: gauge!("db_disk_usage_bytes"),
            file_handles: gauge!("db_file_handles_open"),
            
            // Memory
            memory_usage: gauge!("db_memory_usage_bytes"),
            memory_allocated: counter!("db_memory_allocated_bytes_total"),
            memory_freed: counter!("db_memory_freed_bytes_total"),
            
            // Performance
            slow_operations: counter!("db_slow_operations_total"),
            throughput: gauge!("db_throughput_ops_per_second"),
            latency_p50: gauge!("db_latency_p50_seconds"),
            latency_p95: gauge!("db_latency_p95_seconds"),
            latency_p99: gauge!("db_latency_p99_seconds"),
        }
    }
    
    pub fn record_operation(&self, operation: &str, duration: Duration, success: bool) {
        let _labels = vec![("operation", operation.to_string())];
        
        self.operations_total.increment(1);
        self.operations_duration.record(duration.as_secs_f64());
        
        if !success {
            self.operations_errors.increment(1);
        }
        
        // Track slow operations
        if duration > Duration::from_millis(100) {
            self.slow_operations.increment(1);
        }
    }
    
    pub fn record_cache_hit(&self, hit: bool) {
        if hit {
            self.cache_hits.increment(1);
        } else {
            self.cache_misses.increment(1);
        }
    }
    
    pub fn update_cache_size(&self, size: u64) {
        self.cache_size.set(size as f64);
    }
    
    pub fn record_transaction_start(&self) {
        self.transactions_active.increment(1.0);
    }
    
    pub fn record_transaction_end(&self, duration: Duration, committed: bool) {
        self.transactions_active.decrement(1.0);
        self.transaction_duration.record(duration.as_secs_f64());
        
        if committed {
            self.transactions_committed.increment(1);
        } else {
            self.transactions_aborted.increment(1);
        }
    }
    
    pub fn record_lock_contention(&self, duration: Duration) {
        self.locks_contention.record(duration.as_secs_f64());
    }
    
    pub fn update_memory_usage(&self, bytes: u64) {
        self.memory_usage.set(bytes as f64);
    }
    
    pub fn record_io(&self, bytes_written: u64, bytes_read: u64) {
        self.bytes_written.increment(bytes_written);
        self.bytes_read.increment(bytes_read);
    }
}

pub struct HistogramCollector {
    histograms: Arc<RwLock<HashMap<String, Arc<Mutex<HdrHistogram<u64>>>>>>,
    start_time: Instant,
    operation_counts: Arc<RwLock<HashMap<String, u64>>>,
}

impl HistogramCollector {
    pub fn new() -> Self {
        Self {
            histograms: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            operation_counts: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn record(&self, name: &str, value: Duration) {
        let micros = value.as_micros() as u64;
        
        // Get or create histogram
        let histograms = self.histograms.read();
        let histogram = if let Some(hist) = histograms.get(name) {
            hist.clone()
        } else {
            drop(histograms);
            let mut histograms = self.histograms.write();
            let hist = Arc::new(Mutex::new(HdrHistogram::new(3).unwrap()));
            histograms.insert(name.to_string(), hist.clone());
            hist
        };
        
        // Record value
        if let Some(mut hist) = histogram.try_lock() {
            let _ = hist.record(micros);
        }
        
        // Update operation count
        let mut counts = self.operation_counts.write();
        *counts.entry(name.to_string()).or_insert(0) += 1;
    }
    
    pub fn get_percentiles(&self, name: &str) -> Option<LatencyPercentiles> {
        let histograms = self.histograms.read();
        if let Some(histogram) = histograms.get(name) {
            if let Some(hist) = histogram.try_lock() {
                return Some(LatencyPercentiles {
                    p50: Duration::from_micros(hist.value_at_quantile(0.5)),
                    p90: Duration::from_micros(hist.value_at_quantile(0.9)),
                    p95: Duration::from_micros(hist.value_at_quantile(0.95)),
                    p99: Duration::from_micros(hist.value_at_quantile(0.99)),
                    p999: Duration::from_micros(hist.value_at_quantile(0.999)),
                    count: hist.len(),
                    min: Duration::from_micros(hist.min()),
                    max: Duration::from_micros(hist.max()),
                    mean: Duration::from_micros(hist.mean() as u64),
                });
            }
        }
        None
    }
    
    pub fn get_throughput(&self, name: &str) -> f64 {
        let counts = self.operation_counts.read();
        if let Some(&count) = counts.get(name) {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return count as f64 / elapsed;
            }
        }
        0.0
    }
    
    pub fn reset(&self) {
        let mut histograms = self.histograms.write();
        histograms.clear();
        let mut counts = self.operation_counts.write();
        counts.clear();
    }
    
    pub fn snapshot(&self) -> MetricsSnapshot {
        let histograms = self.histograms.read();
        let counts = self.operation_counts.read();
        let mut data = HashMap::new();
        
        for (name, histogram) in histograms.iter() {
            if let Some(hist) = histogram.try_lock() {
                if let Some(&count) = counts.get(name) {
                    data.insert(name.clone(), OperationMetrics {
                        name: name.clone(),
                        count,
                        throughput: self.get_throughput(name),
                        percentiles: LatencyPercentiles {
                            p50: Duration::from_micros(hist.value_at_quantile(0.5)),
                            p90: Duration::from_micros(hist.value_at_quantile(0.9)),
                            p95: Duration::from_micros(hist.value_at_quantile(0.95)),
                            p99: Duration::from_micros(hist.value_at_quantile(0.99)),
                            p999: Duration::from_micros(hist.value_at_quantile(0.999)),
                            count: hist.len(),
                            min: Duration::from_micros(hist.min()),
                            max: Duration::from_micros(hist.max()),
                            mean: Duration::from_micros(hist.mean() as u64),
                        },
                    });
                }
            }
        }
        
        MetricsSnapshot {
            timestamp: SystemTime::now(),
            operations: data,
            uptime: self.start_time.elapsed(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyPercentiles {
    pub p50: Duration,
    pub p90: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub count: u64,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
}

#[derive(Debug, Clone)]
pub struct OperationMetrics {
    pub name: String,
    pub count: u64,
    pub throughput: f64,
    pub percentiles: LatencyPercentiles,
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub timestamp: SystemTime,
    pub operations: HashMap<String, OperationMetrics>,
    pub uptime: Duration,
}

impl MetricsSnapshot {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "timestamp": self.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "uptime_seconds": self.uptime.as_secs(),
            "operations": self.operations.iter().map(|(name, metrics)| {
                (name.clone(), serde_json::json!({
                    "count": metrics.count,
                    "throughput_ops_per_sec": metrics.throughput,
                    "latency": {
                        "p50_ms": metrics.percentiles.p50.as_millis(),
                        "p90_ms": metrics.percentiles.p90.as_millis(),
                        "p95_ms": metrics.percentiles.p95.as_millis(),
                        "p99_ms": metrics.percentiles.p99.as_millis(),
                        "p999_ms": metrics.percentiles.p999.as_millis(),
                        "min_ms": metrics.percentiles.min.as_millis(),
                        "max_ms": metrics.percentiles.max.as_millis(),
                        "mean_ms": metrics.percentiles.mean.as_millis()
                    }
                }))
            }).collect::<HashMap<_, _>>()
        })
    }
}

pub struct MetricsExporter {
    collector: Arc<HistogramCollector>,
    database_metrics: Arc<DatabaseMetrics>,
    export_interval: Duration,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl MetricsExporter {
    pub fn new(
        collector: Arc<HistogramCollector>,
        database_metrics: Arc<DatabaseMetrics>,
        export_interval: Duration,
    ) -> Self {
        Self {
            collector,
            database_metrics,
            export_interval,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    
    pub fn start_prometheus_exporter(&self, _port: u16) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
        let builder = PrometheusBuilder::new();
        builder.install()?;
        
        let shutdown = self.shutdown.clone();
        
        Ok(tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            
            while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                interval.tick().await;
                // Metrics are automatically collected by the prometheus registry
            }
        }))
    }
    
    pub fn export_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = self.collector.snapshot();
        let json = snapshot.to_json();
        std::fs::write(path, serde_json::to_string_pretty(&json)?)?;
        Ok(())
    }
    
    pub fn stop(&self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_histogram_collector() {
        let collector = HistogramCollector::new();
        
        collector.record("test_op", Duration::from_millis(100));
        collector.record("test_op", Duration::from_millis(200));
        collector.record("test_op", Duration::from_millis(150));
        
        let percentiles = collector.get_percentiles("test_op").unwrap();
        assert_eq!(percentiles.count, 3);
        assert!(percentiles.p50 >= Duration::from_millis(100));
        assert!(percentiles.p50 <= Duration::from_millis(200));
    }
    
    #[test]
    fn test_database_metrics() {
        let metrics = DatabaseMetrics::new();
        
        metrics.record_operation("get", Duration::from_millis(10), true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(false);
        metrics.update_memory_usage(1024);
    }
    
    #[test]
    fn test_metrics_snapshot() {
        let collector = HistogramCollector::new();
        collector.record("test", Duration::from_millis(100));
        
        let snapshot = collector.snapshot();
        let json = snapshot.to_json();
        
        assert!(json["operations"]["test"]["count"].as_u64().unwrap() == 1);
    }
}
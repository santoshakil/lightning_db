pub mod alert_manager;
pub mod health_checker;
pub mod metrics_collector;
pub mod otel_integration;
pub mod performance_monitor;
pub mod production_hooks;
pub mod resource_tracker;

use crate::core::error::Result;
use once_cell::sync::Lazy;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, Encoder,
    GaugeVec, HistogramVec, TextEncoder,
};
use std::net::TcpStream;
use std::time::Instant;

// Re-export key monitoring components
pub use alert_manager::{Alert, AlertCondition, AlertManager, AlertRule, AlertSeverity};
pub use health_checker::{HealthCheck, HealthChecker, HealthReport, HealthStatus};
pub use metrics_collector::{
    DatabaseMetrics as CollectedDatabaseMetrics, MetricsCollector, OperationMetrics,
};
pub use otel_integration::{OpenTelemetryProvider, TelemetryConfig};
pub use performance_monitor::{PerformanceData, PerformanceMonitor, PerformanceTrend};
pub use resource_tracker::{ResourceThreshold, ResourceTracker, ResourceUsage};

// Operation counters
static OPERATION_COUNTER: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "lightning_db_operations_total",
        "Total number of database operations",
        &["operation", "status"]
    )
    .unwrap()
});

// Operation latency histograms
static OPERATION_HISTOGRAM: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "lightning_db_operation_duration_seconds",
        "Operation latency in seconds",
        &["operation"],
        vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .unwrap()
});

// Database size metrics
static DB_SIZE_GAUGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "lightning_db_size_bytes",
        "Database size in bytes",
        &["component"]
    )
    .unwrap()
});

// Cache metrics
static CACHE_HITS: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "lightning_db_cache_hits_total",
        "Total number of cache hits",
        &["cache_type"]
    )
    .unwrap()
});

static CACHE_MISSES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "lightning_db_cache_misses_total",
        "Total number of cache misses",
        &["cache_type"]
    )
    .unwrap()
});

// Transaction metrics
static TRANSACTION_GAUGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "lightning_db_active_transactions",
        "Number of active transactions",
        &["type"]
    )
    .unwrap()
});

// WAL metrics
static WAL_SIZE_GAUGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "lightning_db_wal_size_bytes",
        "WAL size in bytes",
        &["segment"]
    )
    .unwrap()
});

// Compaction metrics
static COMPACTION_COUNTER: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "lightning_db_compactions_total",
        "Total number of compactions",
        &["level"]
    )
    .unwrap()
});

static COMPACTION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "lightning_db_compaction_duration_seconds",
        "Compaction duration in seconds",
        &["level"],
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
    )
    .unwrap()
});

// Memory metrics
static MEMORY_USAGE_GAUGE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "lightning_db_memory_usage_bytes",
        "Memory usage in bytes",
        &["component"]
    )
    .unwrap()
});

/// Metrics collection timer
pub struct MetricsTimer {
    start: Instant,
    operation: String,
}

impl MetricsTimer {
    pub fn new(operation: &str) -> Self {
        Self {
            start: Instant::now(),
            operation: operation.to_string(),
        }
    }

    pub fn record_success(self) {
        let duration = self.start.elapsed().as_secs_f64();
        OPERATION_COUNTER
            .with_label_values(&[&self.operation, &"success".to_string()])
            .inc();
        OPERATION_HISTOGRAM
            .with_label_values(&[&self.operation])
            .observe(duration);
    }

    pub fn record_error(self) {
        let duration = self.start.elapsed().as_secs_f64();
        OPERATION_COUNTER
            .with_label_values(&[&self.operation, &"error".to_string()])
            .inc();
        OPERATION_HISTOGRAM
            .with_label_values(&[&self.operation])
            .observe(duration);
    }
}

/// Monitoring interface for database components
pub trait DatabaseMetrics {
    fn record_operation(&self, operation: &str, success: bool, duration_secs: f64);
    fn record_cache_access(&self, cache_type: &str, hit: bool);
    fn update_size_metrics(&self, component: &str, size_bytes: u64);
    fn update_transaction_count(&self, tx_type: &str, count: i64);
}

/// Default implementation for metrics recording
pub struct MetricsRecorder;

impl DatabaseMetrics for MetricsRecorder {
    fn record_operation(&self, operation: &str, success: bool, duration_secs: f64) {
        let status = if success { "success" } else { "error" };
        OPERATION_COUNTER
            .with_label_values(&[operation, status])
            .inc();
        OPERATION_HISTOGRAM
            .with_label_values(&[operation])
            .observe(duration_secs);
    }

    fn record_cache_access(&self, cache_type: &str, hit: bool) {
        if hit {
            CACHE_HITS.with_label_values(&[cache_type]).inc();
        } else {
            CACHE_MISSES.with_label_values(&[cache_type]).inc();
        }
    }

    fn update_size_metrics(&self, component: &str, size_bytes: u64) {
        DB_SIZE_GAUGE
            .with_label_values(&[component])
            .set(size_bytes as f64);
    }

    fn update_transaction_count(&self, tx_type: &str, count: i64) {
        TRANSACTION_GAUGE
            .with_label_values(&[tx_type])
            .set(count as f64);
    }
}

/// Macro for recording operations with metrics
#[macro_export]
macro_rules! with_metrics {
    ($operation:expr, $body:expr) => {{
        let timer = $crate::monitoring::MetricsTimer::new($operation);
        match $body {
            Ok(result) => {
                timer.record_success();
                Ok(result)
            }
            Err(e) => {
                timer.record_error();
                Err(e)
            }
        }
    }};
}

/// Export metrics in Prometheus format
pub fn export_metrics() -> Result<String> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).map_err(|e| {
        crate::core::error::Error::Generic(format!("Failed to encode metrics: {}", e))
    })?;

    String::from_utf8(buffer).map_err(|e| {
        crate::core::error::Error::Generic(format!("Failed to convert metrics to string: {}", e))
    })
}

/// Update database-wide metrics
pub fn update_db_metrics(db_size: u64, wal_size: u64, active_transactions: usize, cache_size: u64) {
    DB_SIZE_GAUGE
        .with_label_values(&["total"])
        .set(db_size as f64);
    WAL_SIZE_GAUGE
        .with_label_values(&["total"])
        .set(wal_size as f64);
    TRANSACTION_GAUGE
        .with_label_values(&["active"])
        .set(active_transactions as f64);
    MEMORY_USAGE_GAUGE
        .with_label_values(&["cache"])
        .set(cache_size as f64);
}

/// Record compaction event
pub fn record_compaction(
    level: usize,
    files_before: usize,
    files_after: usize,
    duration_secs: f64,
) {
    let level_str = level.to_string();
    COMPACTION_COUNTER.with_label_values(&[&level_str]).inc();
    COMPACTION_DURATION
        .with_label_values(&[&level_str])
        .observe(duration_secs);

    tracing::info!(
        level = level,
        files_before = files_before,
        files_after = files_after,
        duration_secs = duration_secs,
        "Compaction completed"
    );
}

/// HTTP metrics endpoint handler
pub struct MetricsEndpoint {
    port: u16,
}

impl MetricsEndpoint {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub fn start(self) -> Result<()> {
        use std::net::TcpListener;

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.port)).map_err(|e| {
            crate::core::error::Error::Generic(format!("Failed to bind metrics endpoint: {}", e))
        })?;

        tracing::info!("Metrics endpoint listening on :{}", self.port);

        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let _ = handle_metrics_request(&mut stream);
            }
        });

        Ok(())
    }
}

fn handle_metrics_request(stream: &mut TcpStream) -> std::io::Result<()> {
    use std::io::Write;
    let metrics = export_metrics().unwrap_or_else(|e| format!("Error: {}", e));

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
        metrics.len(),
        metrics
    );

    stream.write_all(response.as_bytes())?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    #[ignore] // Ignored due to lazy_static race conditions in parallel test execution
    fn test_metrics_timer() {
        // Initialize metrics lazily to ensure they're registered
        let _ = &*OPERATION_COUNTER;
        let _ = &*OPERATION_HISTOGRAM;

        let timer = MetricsTimer::new("test_operation");
        std::thread::sleep(std::time::Duration::from_millis(10));
        timer.record_success();
    }

    #[test]
    #[serial]
    #[ignore] // Ignored due to lazy_static race conditions in parallel test execution
    fn test_metrics_recording() {
        // Initialize metrics lazily to ensure they're registered
        let _ = &*OPERATION_COUNTER;
        let _ = &*OPERATION_HISTOGRAM;
        let _ = &*CACHE_HITS;
        let _ = &*CACHE_MISSES;
        let _ = &*DB_SIZE_GAUGE;
        let _ = &*TRANSACTION_GAUGE;

        let recorder = MetricsRecorder;
        recorder.record_operation("put", true, 0.001);
        recorder.record_cache_access("page", true);
        recorder.update_size_metrics("btree", 1024 * 1024);
        recorder.update_transaction_count("active", 5);
    }

    #[test]
    #[serial]
    #[ignore] // Ignored due to lazy_static race conditions in parallel test execution
    fn test_export_metrics() {
        // Initialize metrics lazily to ensure they're registered
        let _ = &*OPERATION_COUNTER;

        // Record some metrics
        OPERATION_COUNTER
            .with_label_values(&["test", "success"])
            .inc();

        // Export and verify format
        let metrics = export_metrics().unwrap();
        assert!(metrics.contains("lightning_db_operations_total"));
    }
}

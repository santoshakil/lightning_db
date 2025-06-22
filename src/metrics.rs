use crate::error::{Error, Result};
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec,
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, TextEncoder,
};
use std::sync::Arc;
use std::time::Instant;

lazy_static::lazy_static! {
    // Operation counters
    static ref OPERATION_COUNTER: CounterVec = register_counter_vec!(
        "lightning_db_operations_total",
        "Total number of database operations",
        &["operation", "status"]
    ).unwrap();

    // Operation latency histograms
    static ref OPERATION_LATENCY: HistogramVec = register_histogram_vec!(
        HistogramOpts::new(
            "lightning_db_operation_duration_seconds",
            "Database operation latency in seconds"
        ).buckets(vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0]),
        &["operation"]
    ).unwrap();

    // Database size metrics
    static ref DATABASE_SIZE: GaugeVec = register_gauge_vec!(
        "lightning_db_size_bytes",
        "Database size in bytes",
        &["type"]
    ).unwrap();

    // Cache metrics
    static ref CACHE_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_cache_metrics",
        "Cache statistics",
        &["metric"]
    ).unwrap();

    // Transaction metrics
    static ref TRANSACTION_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_transaction_metrics",
        "Transaction statistics",
        &["metric"]
    ).unwrap();

    // WAL metrics
    static ref WAL_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_wal_metrics",
        "Write-ahead log metrics",
        &["metric"]
    ).unwrap();

    // B+Tree metrics
    static ref BTREE_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_btree_metrics",
        "B+Tree statistics",
        &["metric"]
    ).unwrap();

    // LSM metrics
    static ref LSM_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_lsm_metrics",
        "LSM tree statistics",
        &["metric"]
    ).unwrap();

    // Replication metrics
    static ref REPLICATION_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_replication_metrics",
        "Replication statistics",
        &["metric", "role"]
    ).unwrap();

    // Sharding metrics
    static ref SHARDING_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_sharding_metrics",
        "Sharding statistics",
        &["metric", "shard"]
    ).unwrap();

    // Compression metrics
    static ref COMPRESSION_METRICS: GaugeVec = register_gauge_vec!(
        "lightning_db_compression_metrics",
        "Compression statistics",
        &["algorithm", "metric"]
    ).unwrap();
}

pub struct MetricsRecorder {
    enabled: bool,
}

impl Default for MetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder {
    pub fn new() -> Self {
        Self { enabled: true }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    // Record operation start and return a guard that records completion
    pub fn record_operation(&self, operation: &str) -> OperationGuard {
        OperationGuard::new(operation, self.enabled)
    }

    // Record operation result
    pub fn record_result(&self, operation: &str, success: bool) {
        if self.enabled {
            let status = if success { "success" } else { "failure" };
            OPERATION_COUNTER
                .with_label_values(&[operation, status])
                .inc();
        }
    }

    // Update database size metrics
    pub fn update_database_size(&self, data_size: u64, index_size: u64, wal_size: u64) {
        if self.enabled {
            DATABASE_SIZE
                .with_label_values(&["data"])
                .set(data_size as f64);
            DATABASE_SIZE
                .with_label_values(&["index"])
                .set(index_size as f64);
            DATABASE_SIZE
                .with_label_values(&["wal"])
                .set(wal_size as f64);
        }
    }

    // Update cache metrics
    pub fn update_cache_metrics(&self, hits: u64, misses: u64, evictions: u64, size: u64) {
        if self.enabled {
            CACHE_METRICS
                .with_label_values(&["hits"])
                .set(hits as f64);
            CACHE_METRICS
                .with_label_values(&["misses"])
                .set(misses as f64);
            CACHE_METRICS
                .with_label_values(&["evictions"])
                .set(evictions as f64);
            CACHE_METRICS
                .with_label_values(&["size_bytes"])
                .set(size as f64);
            
            let hit_rate = if hits + misses > 0 {
                hits as f64 / (hits + misses) as f64
            } else {
                0.0
            };
            CACHE_METRICS
                .with_label_values(&["hit_rate"])
                .set(hit_rate);
        }
    }

    // Update transaction metrics
    pub fn update_transaction_metrics(&self, active: u64, committed: u64, aborted: u64) {
        if self.enabled {
            TRANSACTION_METRICS
                .with_label_values(&["active"])
                .set(active as f64);
            TRANSACTION_METRICS
                .with_label_values(&["committed"])
                .set(committed as f64);
            TRANSACTION_METRICS
                .with_label_values(&["aborted"])
                .set(aborted as f64);
        }
    }

    // Update WAL metrics
    pub fn update_wal_metrics(&self, entries: u64, size_bytes: u64, sync_count: u64) {
        if self.enabled {
            WAL_METRICS
                .with_label_values(&["entries"])
                .set(entries as f64);
            WAL_METRICS
                .with_label_values(&["size_bytes"])
                .set(size_bytes as f64);
            WAL_METRICS
                .with_label_values(&["sync_count"])
                .set(sync_count as f64);
        }
    }

    // Update B+Tree metrics
    pub fn update_btree_metrics(&self, height: u32, nodes: u64, entries: u64) {
        if self.enabled {
            BTREE_METRICS
                .with_label_values(&["height"])
                .set(height as f64);
            BTREE_METRICS
                .with_label_values(&["nodes"])
                .set(nodes as f64);
            BTREE_METRICS
                .with_label_values(&["entries"])
                .set(entries as f64);
        }
    }

    // Update LSM metrics
    pub fn update_lsm_metrics(&self, levels: u32, sstables: u64, compactions: u64) {
        if self.enabled {
            LSM_METRICS
                .with_label_values(&["levels"])
                .set(levels as f64);
            LSM_METRICS
                .with_label_values(&["sstables"])
                .set(sstables as f64);
            LSM_METRICS
                .with_label_values(&["compactions"])
                .set(compactions as f64);
        }
    }

    // Update replication metrics
    pub fn update_replication_lag(&self, role: &str, lag_ms: u64) {
        if self.enabled {
            REPLICATION_METRICS
                .with_label_values(&["lag_ms", role])
                .set(lag_ms as f64);
        }
    }

    // Update sharding metrics
    pub fn update_shard_metrics(&self, shard_id: &str, entries: u64, size_bytes: u64) {
        if self.enabled {
            SHARDING_METRICS
                .with_label_values(&["entries", shard_id])
                .set(entries as f64);
            SHARDING_METRICS
                .with_label_values(&["size_bytes", shard_id])
                .set(size_bytes as f64);
        }
    }

    // Update compression metrics
    pub fn update_compression_ratio(&self, algorithm: &str, ratio: f64) {
        if self.enabled {
            COMPRESSION_METRICS
                .with_label_values(&[algorithm, "ratio"])
                .set(ratio);
        }
    }

    // Export metrics in Prometheus format
    pub fn export(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = Vec::new();
        
        encoder.encode(&metric_families, &mut buffer)
            .map_err(|e| Error::Generic(format!("Failed to encode metrics: {}", e)))?;
        
        String::from_utf8(buffer)
            .map_err(|e| Error::Generic(format!("Failed to convert metrics to string: {}", e)))
    }
}

pub struct OperationGuard {
    operation: String,
    start: Instant,
    enabled: bool,
    completed: bool,
}

impl OperationGuard {
    fn new(operation: &str, enabled: bool) -> Self {
        Self {
            operation: operation.to_string(),
            start: Instant::now(),
            enabled,
            completed: false,
        }
    }

    pub fn complete(mut self, success: bool) {
        self.completed = true;
        if self.enabled {
            let duration = self.start.elapsed().as_secs_f64();
            OPERATION_LATENCY
                .with_label_values(&[&self.operation])
                .observe(duration);
            
            let status = if success { "success" } else { "failure" };
            OPERATION_COUNTER
                .with_label_values(&[&self.operation, status])
                .inc();
        }
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.completed && self.enabled {
            // If not explicitly completed, assume failure
            let duration = self.start.elapsed().as_secs_f64();
            OPERATION_LATENCY
                .with_label_values(&[&self.operation])
                .observe(duration);
            
            OPERATION_COUNTER
                .with_label_values(&[&self.operation, "failure"])
                .inc();
        }
    }
}

// Global metrics instance
lazy_static::lazy_static! {
    pub static ref METRICS: Arc<parking_lot::RwLock<MetricsRecorder>> = 
        Arc::new(parking_lot::RwLock::new(MetricsRecorder::new()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_metrics() {
        let recorder = MetricsRecorder::new();
        
        // Record successful operation
        {
            let guard = recorder.record_operation("get");
            // Simulate some work
            std::thread::sleep(std::time::Duration::from_millis(1));
            guard.complete(true);
        }
        
        // Record failed operation
        {
            let guard = recorder.record_operation("put");
            std::thread::sleep(std::time::Duration::from_millis(1));
            guard.complete(false);
        }
        
        // Export metrics
        let metrics = recorder.export().unwrap();
        assert!(metrics.contains("lightning_db_operations_total"));
        assert!(metrics.contains("lightning_db_operation_duration_seconds"));
    }

    #[test]
    fn test_cache_metrics() {
        let recorder = MetricsRecorder::new();
        
        recorder.update_cache_metrics(1000, 200, 50, 1024 * 1024);
        
        let metrics = recorder.export().unwrap();
        assert!(metrics.contains("lightning_db_cache_metrics"));
        assert!(metrics.contains("hit_rate"));
    }

    #[test]
    fn test_disabled_metrics() {
        let mut recorder = MetricsRecorder::new();
        
        // First record something while enabled
        {
            let guard = recorder.record_operation("initial");
            guard.complete(true);
        }
        
        // Now disable and verify new operations aren't recorded
        recorder.set_enabled(false);
        
        let metrics_before = recorder.export().unwrap();
        
        // Operations should not be recorded
        {
            let guard = recorder.record_operation("test");
            guard.complete(true);
        }
        
        let metrics_after = recorder.export().unwrap();
        
        // Metrics should be the same (no new data recorded)
        assert_eq!(metrics_before.len(), metrics_after.len());
    }
}
use crate::statistics::metrics::{ComponentMetrics, DatabaseMetrics, MetricsSnapshot};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Collects and aggregates metrics from various database components
pub struct MetricsCollector {
    database_metrics: Arc<DatabaseMetrics>,
    component_metrics: Arc<RwLock<HashMap<String, Arc<ComponentMetrics>>>>,
    collection_interval: Duration,
    history_size: usize,
    history: Arc<RwLock<Vec<TimestampedSnapshot>>>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    collection_thread: Arc<RwLock<Option<thread::JoinHandle<()>>>>,
}

impl std::fmt::Debug for MetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsCollector")
            .field("collection_interval", &self.collection_interval)
            .field("history_size", &self.history_size)
            .field(
                "shutdown",
                &self.shutdown.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "has_collection_thread",
                &self.collection_thread.read().is_some(),
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct TimestampedSnapshot {
    pub timestamp: Instant,
    pub snapshot: MetricsSnapshot,
    pub component_snapshots: HashMap<String, HashMap<String, f64>>,
}

impl MetricsCollector {
    pub fn new(collection_interval: Duration, history_size: usize) -> Self {
        Self {
            database_metrics: Arc::new(DatabaseMetrics::new()),
            component_metrics: Arc::new(RwLock::new(HashMap::new())),
            collection_interval,
            history_size,
            history: Arc::new(RwLock::new(Vec::with_capacity(history_size))),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            collection_thread: Arc::new(RwLock::new(None)),
        }
    }

    /// Get reference to database metrics for recording
    pub fn database_metrics(&self) -> Arc<DatabaseMetrics> {
        Arc::clone(&self.database_metrics)
    }

    /// Register a component for metrics collection
    pub fn register_component(&self, name: &str) -> Arc<ComponentMetrics> {
        let component = Arc::new(ComponentMetrics::new(name.to_string()));
        self.component_metrics
            .write()
            .insert(name.to_string(), Arc::clone(&component));
        component
    }

    /// Get component metrics by name
    pub fn get_component(&self, name: &str) -> Option<Arc<ComponentMetrics>> {
        self.component_metrics.read().get(name).cloned()
    }

    /// Start background metrics collection
    pub fn start(&self) {
        let database_metrics = Arc::clone(&self.database_metrics);
        let component_metrics = Arc::clone(&self.component_metrics);
        let history = Arc::clone(&self.history);
        let shutdown = Arc::clone(&self.shutdown);
        let interval = self.collection_interval;
        let history_size = self.history_size;

        let handle = thread::spawn(move || {
            info!("Started metrics collection thread");

            let mut last_collection = std::time::Instant::now();

            while !shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Sleep in small increments to be more responsive to shutdown
                std::thread::sleep(std::time::Duration::from_millis(100));

                // Only collect metrics at the specified interval
                if last_collection.elapsed() < interval {
                    continue;
                }
                last_collection = std::time::Instant::now();

                // Collect database metrics snapshot
                let db_snapshot = database_metrics.get_snapshot();

                // Collect component metrics
                let mut component_snapshots = HashMap::new();
                {
                    let components = component_metrics.read();
                    for (name, component) in components.iter() {
                        component_snapshots.insert(name.clone(), component.get_metrics());
                    }
                }

                // Create timestamped snapshot
                let snapshot = TimestampedSnapshot {
                    timestamp: Instant::now(),
                    snapshot: db_snapshot,
                    component_snapshots,
                };

                // Add to history
                {
                    let mut hist = history.write();
                    hist.push(snapshot);

                    // Maintain history size limit
                    if hist.len() > history_size {
                        hist.remove(0);
                    }
                }

                debug!("Collected metrics snapshot");
            }

            info!("Metrics collection thread stopped");
        });

        *self.collection_thread.write() = Some(handle);
    }

    /// Stop background metrics collection
    pub fn stop(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);

        if let Some(handle) = self.collection_thread.write().take() {
            let _ = handle.join();
        }
    }

    /// Get current metrics snapshot
    pub fn get_current_snapshot(&self) -> MetricsSnapshot {
        self.database_metrics.get_snapshot()
    }

    /// Get metrics history
    pub fn get_history(&self) -> Vec<TimestampedSnapshot> {
        self.history.read().clone()
    }

    /// Get latest N snapshots
    pub fn get_recent_history(&self, n: usize) -> Vec<TimestampedSnapshot> {
        let history = self.history.read();
        let start = history.len().saturating_sub(n);
        history[start..].to_vec()
    }

    /// Get summary metrics as a HashMap (for compatibility)
    pub fn get_summary_metrics(&self) -> HashMap<String, serde_json::Value> {
        let snapshot = self.get_current_snapshot();
        let mut metrics = HashMap::new();

        // Total operations
        metrics.insert(
            "total_operations".to_string(),
            serde_json::Value::Number(serde_json::Number::from(snapshot.reads + snapshot.writes)),
        );

        // Calculate collection duration
        let history = self.history.read();
        let duration_secs = if !history.is_empty() {
            history
                .last()
                .map(|last| last.timestamp.elapsed().as_secs_f64())
                .unwrap_or(1.0)
        } else {
            1.0
        };
        metrics.insert(
            "collection_duration_secs".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(duration_secs)
                    .unwrap_or_else(|| serde_json::Number::from(1)),
            ),
        );

        // Latency metrics
        metrics.insert(
            "avg_latency_us".to_string(),
            serde_json::Value::Number(serde_json::Number::from(
                (snapshot.avg_read_latency_us + snapshot.avg_write_latency_us) / 2,
            )),
        );
        // Use average latency as approximation for p99 and p95 (not ideal but for compatibility)
        let avg_latency = (snapshot.avg_read_latency_us + snapshot.avg_write_latency_us) / 2;
        metrics.insert(
            "p99_latency_us".to_string(),
            serde_json::Value::Number(serde_json::Number::from(avg_latency * 2)), // Approximate p99 as 2x average
        );
        metrics.insert(
            "p95_latency_us".to_string(),
            serde_json::Value::Number(serde_json::Number::from((avg_latency * 3) / 2)), // Approximate p95 as 1.5x average
        );

        // Error rate
        let total_ops = snapshot.reads + snapshot.writes;
        let error_rate = if total_ops > 0 {
            (snapshot.read_errors + snapshot.write_errors) as f64 / total_ops as f64
        } else {
            0.0
        };
        metrics.insert(
            "error_rate".to_string(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(error_rate)
                    .unwrap_or_else(|| serde_json::Number::from(0)),
            ),
        );

        metrics
    }

    /// Calculate metrics over a time window
    pub fn get_window_metrics(&self, window: Duration) -> Option<WindowMetrics> {
        let history = self.history.read();
        if history.is_empty() {
            return None;
        }

        let now = Instant::now();
        let window_start = now - window;

        // Find snapshots within the window
        let window_snapshots: Vec<&TimestampedSnapshot> = history
            .iter()
            .filter(|s| s.timestamp >= window_start)
            .collect();

        if window_snapshots.is_empty() {
            return None;
        }

        // Calculate aggregated metrics
        let first = &window_snapshots[0].snapshot;
        let last = &window_snapshots[window_snapshots.len() - 1].snapshot;

        let duration = window_snapshots[window_snapshots.len() - 1]
            .timestamp
            .duration_since(window_snapshots[0].timestamp);

        let reads_per_sec = if duration.as_secs() > 0 {
            (last.reads - first.reads) as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        let writes_per_sec = if duration.as_secs() > 0 {
            (last.writes - first.writes) as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        // Calculate average latencies
        let mut total_read_latency = 0u64;
        let mut total_write_latency = 0u64;
        let mut count = 0;

        for snapshot in &window_snapshots {
            total_read_latency += snapshot.snapshot.avg_read_latency_us;
            total_write_latency += snapshot.snapshot.avg_write_latency_us;
            count += 1;
        }

        Some(WindowMetrics {
            window,
            reads_per_sec,
            writes_per_sec,
            avg_read_latency_us: if count > 0 {
                total_read_latency / count as u64
            } else {
                0
            },
            avg_write_latency_us: if count > 0 {
                total_write_latency / count as u64
            } else {
                0
            },
            cache_hit_rate: last.cache_hit_rate,
            total_compactions: last.compactions - first.compactions,
            error_rate: if last.reads + last.writes > first.reads + first.writes {
                ((last.read_errors + last.write_errors) - (first.read_errors + first.write_errors))
                    as f64
                    / ((last.reads + last.writes) - (first.reads + first.writes)) as f64
            } else {
                0.0
            },
        })
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.database_metrics.reset();
        self.history.write().clear();
    }
}

/// Aggregated metrics over a time window
#[derive(Debug, Clone)]
pub struct WindowMetrics {
    pub window: Duration,
    pub reads_per_sec: f64,
    pub writes_per_sec: f64,
    pub avg_read_latency_us: u64,
    pub avg_write_latency_us: u64,
    pub cache_hit_rate: f64,
    pub total_compactions: u64,
    pub error_rate: f64,
}

impl Drop for MetricsCollector {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Helper trait for instrumenting operations with metrics
pub trait MetricsInstrumented {
    fn record_operation<F, R>(&self, op_type: OperationType, f: F) -> R
    where
        F: FnOnce() -> R;
}

#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Transaction,
}

impl MetricsInstrumented for Arc<DatabaseMetrics> {
    fn record_operation<F, R>(&self, op_type: OperationType, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let duration = start.elapsed();

        match op_type {
            OperationType::Read => self.record_read(duration),
            OperationType::Write => self.record_write(duration),
            OperationType::Delete => self.record_delete(duration),
            OperationType::Transaction => self.record_transaction(duration),
        }

        result
    }
}

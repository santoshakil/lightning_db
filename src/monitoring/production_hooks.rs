use crate::error::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Production monitoring event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MonitoringEvent {
    /// Database operation events
    OperationStarted {
        operation_id: String,
        operation_type: OperationType,
        timestamp: u64,
    },
    OperationCompleted {
        operation_id: String,
        operation_type: OperationType,
        duration_ms: u64,
        success: bool,
        error: Option<String>,
    },

    /// Performance events
    SlowOperation {
        operation_type: OperationType,
        duration_ms: u64,
        threshold_ms: u64,
    },
    CacheEviction {
        evicted_count: usize,
        cache_size: usize,
        reason: String,
    },

    /// Resource events
    MemoryUsage {
        total_bytes: usize,
        cache_bytes: usize,
        index_bytes: usize,
        wal_bytes: usize,
    },
    DiskUsage {
        total_bytes: u64,
        data_bytes: u64,
        wal_bytes: u64,
        free_space_bytes: u64,
    },

    /// Health events
    HealthCheck {
        status: HealthStatus,
        details: HashMap<String, String>,
    },
    Checkpoint {
        duration_ms: u64,
        pages_written: usize,
        wal_size_before: u64,
        wal_size_after: u64,
    },

    /// Error events
    ErrorOccurred {
        error_type: String,
        message: String,
        recoverable: bool,
        context: HashMap<String, String>,
    },
    CorruptionDetected {
        location: String,
        severity: String,
        details: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Transaction,
    Checkpoint,
    Compaction,
    Backup,
    Restore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Critical,
}

/// Monitoring hook trait for custom implementations
pub trait MonitoringHook: Send + Sync {
    /// Called when a monitoring event occurs
    fn on_event(&self, event: &MonitoringEvent);

    /// Called periodically to collect metrics
    fn collect_metrics(&self) -> HashMap<String, f64>;

    /// Called to check if the hook is healthy
    fn is_healthy(&self) -> bool {
        true
    }
}

/// Default monitoring hook that logs to tracing
pub struct LoggingMonitoringHook;

impl MonitoringHook for LoggingMonitoringHook {
    fn on_event(&self, event: &MonitoringEvent) {
        match event {
            MonitoringEvent::OperationCompleted {
                operation_type,
                duration_ms,
                success,
                ..
            } => {
                if *success {
                    tracing::debug!(
                        operation = ?operation_type,
                        duration_ms = duration_ms,
                        "Operation completed"
                    );
                } else {
                    tracing::warn!(
                        operation = ?operation_type,
                        duration_ms = duration_ms,
                        "Operation failed"
                    );
                }
            }
            MonitoringEvent::SlowOperation {
                operation_type,
                duration_ms,
                threshold_ms,
            } => {
                tracing::warn!(
                    operation = ?operation_type,
                    duration_ms = duration_ms,
                    threshold_ms = threshold_ms,
                    "Slow operation detected"
                );
            }
            MonitoringEvent::ErrorOccurred {
                error_type,
                message,
                ..
            } => {
                tracing::error!(error_type = error_type, message = message, "Error occurred");
            }
            MonitoringEvent::CorruptionDetected {
                location,
                severity,
                details,
            } => {
                tracing::error!(
                    location = location,
                    severity = severity,
                    details = details,
                    "Data corruption detected"
                );
            }
            _ => {
                tracing::trace!(event = ?event, "Monitoring event");
            }
        }
    }

    fn collect_metrics(&self) -> HashMap<String, f64> {
        HashMap::new()
    }
}

/// Production monitoring system
pub struct ProductionMonitor {
    hooks: Arc<RwLock<Vec<Arc<dyn MonitoringHook>>>>,
    operation_thresholds: Arc<RwLock<HashMap<OperationType, Duration>>>,
    metrics_collector: Arc<RwLock<MetricsCollector>>,
}

impl std::fmt::Debug for ProductionMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProductionMonitor")
            .field("hooks_count", &self.hooks.read().len())
            .field("operation_thresholds", &*self.operation_thresholds.read())
            .finish()
    }
}

impl ProductionMonitor {
    pub fn new() -> Self {
        let mut thresholds = HashMap::new();
        // Default thresholds
        thresholds.insert(OperationType::Read, Duration::from_millis(10));
        thresholds.insert(OperationType::Write, Duration::from_millis(50));
        thresholds.insert(OperationType::Transaction, Duration::from_millis(100));
        thresholds.insert(OperationType::Checkpoint, Duration::from_secs(1));

        Self {
            hooks: Arc::new(RwLock::new(vec![Arc::new(LoggingMonitoringHook)])),
            operation_thresholds: Arc::new(RwLock::new(thresholds)),
            metrics_collector: Arc::new(RwLock::new(MetricsCollector::new())),
        }
    }

    /// Register a monitoring hook
    pub fn register_hook(&self, hook: Arc<dyn MonitoringHook>) {
        self.hooks.write().push(hook);
    }

    /// Set operation threshold
    pub fn set_threshold(&self, operation: OperationType, threshold: Duration) {
        self.operation_thresholds
            .write()
            .insert(operation, threshold);
    }

    /// Record an operation
    pub fn record_operation<F, R>(
        &self,
        operation_type: OperationType,
        operation_id: String,
        operation: F,
    ) -> Result<R>
    where
        F: FnOnce() -> Result<R>,
    {
        let start = Instant::now();

        // Notify start
        self.emit_event(MonitoringEvent::OperationStarted {
            operation_id: operation_id.clone(),
            operation_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        });

        // Execute operation
        let result = operation();
        let duration = start.elapsed();

        // Update metrics
        self.metrics_collector
            .write()
            .record_operation(operation_type, duration, result.is_ok());

        // Check for slow operation
        if let Some(threshold) = self.operation_thresholds.read().get(&operation_type) {
            if duration > *threshold {
                self.emit_event(MonitoringEvent::SlowOperation {
                    operation_type,
                    duration_ms: duration.as_millis() as u64,
                    threshold_ms: threshold.as_millis() as u64,
                });
            }
        }

        // Notify completion
        self.emit_event(MonitoringEvent::OperationCompleted {
            operation_id,
            operation_type,
            duration_ms: duration.as_millis() as u64,
            success: result.is_ok(),
            error: result.as_ref().err().map(|e| e.to_string()),
        });

        result
    }

    /// Emit a monitoring event
    pub fn emit_event(&self, event: MonitoringEvent) {
        let hooks = self.hooks.read();
        for hook in hooks.iter() {
            hook.on_event(&event);
        }
    }

    /// Collect all metrics
    pub fn collect_metrics(&self) -> HashMap<String, f64> {
        let mut all_metrics = HashMap::new();

        // Collect from metrics collector
        all_metrics.extend(self.metrics_collector.read().get_metrics());

        // Collect from hooks
        let hooks = self.hooks.read();
        for hook in hooks.iter() {
            all_metrics.extend(hook.collect_metrics());
        }

        all_metrics
    }

    /// Perform health check
    pub fn health_check(&self) -> HealthStatus {
        let hooks = self.hooks.read();
        let unhealthy_hooks = hooks.iter().filter(|h| !h.is_healthy()).count();

        if unhealthy_hooks == 0 {
            HealthStatus::Healthy
        } else if unhealthy_hooks < hooks.len() / 2 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Critical
        }
    }
}

/// Internal metrics collector
struct MetricsCollector {
    operation_counts: HashMap<OperationType, usize>,
    operation_times: HashMap<OperationType, Duration>,
    operation_errors: HashMap<OperationType, usize>,
    start_time: Instant,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            operation_counts: HashMap::new(),
            operation_times: HashMap::new(),
            operation_errors: HashMap::new(),
            start_time: Instant::now(),
        }
    }

    fn record_operation(&mut self, op_type: OperationType, duration: Duration, success: bool) {
        *self.operation_counts.entry(op_type).or_insert(0) += 1;
        *self
            .operation_times
            .entry(op_type)
            .or_insert(Duration::ZERO) += duration;

        if !success {
            *self.operation_errors.entry(op_type).or_insert(0) += 1;
        }
    }

    fn get_metrics(&self) -> HashMap<String, f64> {
        let mut metrics = HashMap::new();
        let uptime_secs = self.start_time.elapsed().as_secs_f64();

        for (op_type, count) in &self.operation_counts {
            let op_name = format!("{:?}", op_type).to_lowercase();

            // Operation rate
            metrics.insert(
                format!("{}_ops_per_sec", op_name),
                *count as f64 / uptime_secs,
            );

            // Average latency
            if let Some(total_time) = self.operation_times.get(op_type) {
                metrics.insert(
                    format!("{}_avg_latency_ms", op_name),
                    total_time.as_millis() as f64 / *count as f64,
                );
            }

            // Error rate
            let errors = self.operation_errors.get(op_type).copied().unwrap_or(0);
            metrics.insert(
                format!("{}_error_rate", op_name),
                errors as f64 / *count as f64,
            );
        }

        metrics
    }
}

/// Prometheus metrics hook implementation
/// Available in all builds - use existing prometheus dependency
pub struct PrometheusMonitoringHook {
    _registry: (), // Placeholder until prometheus integration is complete
}

impl MonitoringHook for PrometheusMonitoringHook {
    fn on_event(&self, _event: &MonitoringEvent) {
        // Prometheus integration placeholder
    }

    fn collect_metrics(&self) -> HashMap<String, f64> {
        HashMap::new()
    }
}

/// OpenTelemetry monitoring hook implementation (feature-gated)
/// Enable with --features="opentelemetry"
#[cfg(feature = "opentelemetry")]
pub struct OpenTelemetryMonitoringHook {
    _placeholder: (), // Placeholder until opentelemetry integration is complete
}

#[cfg(feature = "opentelemetry")]
impl MonitoringHook for OpenTelemetryMonitoringHook {
    fn on_event(&self, _event: &MonitoringEvent) {
        // OpenTelemetry integration placeholder
    }

    fn collect_metrics(&self) -> HashMap<String, f64> {
        HashMap::new()
    }
}

impl Default for ProductionMonitor {
    fn default() -> Self {
        Self::new()
    }
}

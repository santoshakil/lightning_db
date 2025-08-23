pub mod config;
pub mod logger;
pub mod redaction;
pub mod metrics;
pub mod sampling;
pub mod telemetry;
pub mod performance;
pub mod context;

pub use config::*;
pub use logger::*;
pub use redaction::*;
pub use metrics::*;
pub use sampling::*;
pub use telemetry::*;
pub use performance::*;
pub use context::*;

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tracing::{error, trace, warn, Level};
use serde_json;
use once_cell::sync::Lazy;

// Global instances
static LOGGING_SYSTEM: Lazy<Arc<LoggingSystem>> = Lazy::new(|| {
    Arc::new(LoggingSystem::new())
});

pub struct LoggingSystem {
    pub metrics: Arc<DatabaseMetrics>,
    pub histogram_collector: Arc<HistogramCollector>,
    pub performance_monitor: Arc<PerformanceMonitor>,
    pub context_manager: Arc<ContextManager>,
    pub telemetry_manager: Option<Arc<TelemetryManager>>,
    pub sampler: Arc<Sampler>,
}

impl LoggingSystem {
    pub fn new() -> Self {
        let config = LoggingConfig::from_env();
        let metrics = Arc::new(DatabaseMetrics::new());
        let histogram_collector = Arc::new(HistogramCollector::new());
        let performance_monitor = Arc::new(PerformanceMonitor::new(true, None));
        let context_manager = Arc::new(ContextManager::new());
        let sampling_config = sampling::SamplingConfig {
            enabled: config.sampling.enabled,
            default_rate: config.sampling.trace_sample_rate,
            level_rates: {
                let mut rates = std::collections::HashMap::new();
                rates.insert(tracing::Level::TRACE, config.sampling.trace_sample_rate);
                rates.insert(tracing::Level::DEBUG, config.sampling.debug_sample_rate);
                rates.insert(tracing::Level::INFO, 1.0);
                rates.insert(tracing::Level::WARN, 1.0);
                rates.insert(tracing::Level::ERROR, 1.0);
                rates
            },
            operation_rates: config.sampling.high_frequency_operations
                .iter()
                .map(|op| (op.clone(), config.sampling.high_frequency_sample_rate))
                .collect(),
            adaptive_sampling: sampling::AdaptiveSamplingConfig::default(),
        };
        let sampler = Arc::new(Sampler::new(sampling_config));
        
        let telemetry_manager = if config.telemetry.enabled {
            TelemetryManager::new(
                &config.telemetry.service_name,
                &config.telemetry.service_version,
                config.output.jaeger.as_ref().map(|j| j.endpoint.as_str())
            ).ok().map(Arc::new)
        } else {
            None
        };
        
        Self {
            metrics,
            histogram_collector,
            performance_monitor,
            context_manager,
            telemetry_manager,
            sampler,
        }
    }
    
    pub fn initialize_global(config: Option<LoggingConfig>) -> Result<(), Box<dyn std::error::Error>> {
        let config = config.unwrap_or_else(LoggingConfig::from_env);
        Logger::init_global(config)
    }
    
    pub fn get_instance() -> Arc<LoggingSystem> {
        LOGGING_SYSTEM.clone()
    }
}

/// Initialize production logging with configurable settings
pub fn init_logging(level: Level, json_output: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = LoggingConfig::from_env();
    config.level = match level {
        Level::TRACE => LogLevel::Trace,
        Level::DEBUG => LogLevel::Debug,
        Level::INFO => LogLevel::Info,
        Level::WARN => LogLevel::Warn,
        Level::ERROR => LogLevel::Error,
    };
    config.format = if json_output { LogFormat::Json } else { LogFormat::Pretty };
    
    LoggingSystem::initialize_global(Some(config))
}

/// Initialize logging with full configuration
pub fn init_logging_with_config(config: LoggingConfig) -> Result<(), Box<dyn std::error::Error>> {
    LoggingSystem::initialize_global(Some(config))
}

/// Get the global logging system instance
pub fn get_logging_system() -> Arc<LoggingSystem> {
    LoggingSystem::get_instance()
}

// Enhanced logging macros with context and sampling
#[macro_export]
macro_rules! log_operation {
    ($level:expr, $op:expr, $key:expr, $duration:expr, $result:expr) => {
        let system = crate::features::logging::get_logging_system();
        
        // Check sampling decision
        if system.sampler.should_sample($level, Some($op)) {
            // Record metrics
            system.metrics.record_operation($op, $duration, $result.is_ok());
            system.histogram_collector.record($op, $duration);
            
            // Get current trace context
            let context = crate::features::logging::context::get_current_context();
            
            match $level {
                tracing::Level::TRACE => tracing::trace!(
                    operation = $op,
                    key = ?$key,
                    duration_us = $duration.as_micros() as u64,
                    result = ?$result,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    "Database operation completed"
                ),
                tracing::Level::DEBUG => tracing::debug!(
                    operation = $op,
                    key = ?$key,
                    duration_us = $duration.as_micros() as u64,
                    result = ?$result,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    "Database operation completed"
                ),
                _ => {}
            }
        }
    };
}

#[macro_export]
macro_rules! log_transaction {
    ($event:expr, $tx_id:expr, $duration:expr) => {
        let system = crate::features::logging::get_logging_system();
        let context = crate::features::logging::context::get_current_context();
        
        match $event {
            "start" => system.metrics.record_transaction_start(),
            "commit" => system.metrics.record_transaction_end($duration, true),
            "abort" => system.metrics.record_transaction_end($duration, false),
            _ => {}
        }
        
        tracing::info!(
            event = $event,
            tx_id = $tx_id,
            duration_ms = $duration.as_millis() as u64,
            trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
            correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
            "Transaction event"
        )
    };
}

#[macro_export]
macro_rules! log_cache_event {
    ($event:expr, $key:expr, $hit:expr) => {
        let system = crate::features::logging::get_logging_system();
        let context = crate::features::logging::context::get_current_context();
        
        system.metrics.record_cache_hit($hit);
        
        if system.sampler.should_sample(tracing::Level::DEBUG, Some("cache_lookup")) {
            tracing::debug!(
                event = $event,
                key = ?$key,
                cache_hit = $hit,
                trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                "Cache event"
            )
        }
    };
}

#[macro_export]
macro_rules! log_compaction {
    ($level:expr, $files_before:expr, $files_after:expr, $duration:expr) => {
        let context = crate::features::logging::context::get_current_context();
        
        tracing::info!(
            level = $level,
            files_before = $files_before,
            files_after = $files_after,
            duration_ms = $duration.as_millis() as u64,
            trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
            correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
            "Compaction completed"
        )
    };
}

#[macro_export]
macro_rules! log_recovery {
    ($phase:expr, $progress:expr, $total:expr) => {
        let context = crate::features::logging::context::get_current_context();
        
        tracing::info!(
            phase = $phase,
            progress = $progress,
            total = $total,
            percent = ($progress as f64 / $total as f64 * 100.0) as u32,
            trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
            correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
            "Recovery progress"
        )
    };
}

// New comprehensive logging macro
#[macro_export]
macro_rules! db_log {
    ($level:expr, $operation:expr, $($field:ident = $value:expr),* $(,)?) => {
        let system = crate::features::logging::get_logging_system();
        
        if system.sampler.should_sample($level, Some($operation)) {
            let context = crate::features::logging::context::get_current_context();
            
            match $level {
                tracing::Level::TRACE => tracing::trace!(
                    operation = $operation,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    $($field = $value,)*
                ),
                tracing::Level::DEBUG => tracing::debug!(
                    operation = $operation,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    $($field = $value,)*
                ),
                tracing::Level::INFO => tracing::info!(
                    operation = $operation,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    $($field = $value,)*
                ),
                tracing::Level::WARN => tracing::warn!(
                    operation = $operation,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    $($field = $value,)*
                ),
                tracing::Level::ERROR => tracing::error!(
                    operation = $operation,
                    trace_id = context.as_ref().map(|c| c.trace_id.as_str()).unwrap_or(""),
                    correlation_id = context.as_ref().map(|c| c.correlation_id.as_str()).unwrap_or(""),
                    $($field = $value,)*
                ),
            }
        }
    };
}

// Performance monitoring macro
#[macro_export]
macro_rules! with_perf_monitoring {
    ($operation:expr, $code:block) => {
        {
            let system = crate::features::logging::get_logging_system();
            let _token = system.performance_monitor.start_operation($operation);
            $code
        }
    };
}

// Legacy operation timer - kept for backwards compatibility
pub struct OperationTimer {
    start: Instant,
    operation: &'static str,
    key: Option<Vec<u8>>,
}

impl OperationTimer {
    pub fn new(operation: &'static str) -> Self {
        let system = get_logging_system();
        let _perf_token = system.performance_monitor.start_operation(operation);
        
        Self {
            start: Instant::now(),
            operation,
            key: None,
        }
    }

    pub fn with_key(operation: &'static str, key: &[u8]) -> Self {
        let system = get_logging_system();
        let _perf_token = system.performance_monitor.start_operation(operation);
        
        Self {
            start: Instant::now(),
            operation,
            key: Some(key.to_vec()),
        }
    }

    pub fn complete<T>(self, result: &Result<T, crate::core::error::Error>) {
        let duration = self.start.elapsed();
        let system = get_logging_system();
        
        // Record metrics
        system.metrics.record_operation(self.operation, duration, result.is_ok());
        system.histogram_collector.record(self.operation, duration);

        match result {
            Ok(_) => {
                if duration > Duration::from_millis(100) {
                    warn!(
                        operation = self.operation,
                        key = ?self.key,
                        duration_ms = duration.as_millis() as u64,
                        "Slow operation detected"
                    );
                } else {
                    trace!(
                        operation = self.operation,
                        key = ?self.key,
                        duration_us = duration.as_micros() as u64,
                        "Operation completed"
                    );
                }
            }
            Err(e) => {
                error!(
                    operation = self.operation,
                    key = ?self.key,
                    duration_us = duration.as_micros() as u64,
                    error = %e,
                    "Operation failed"
                );
            }
        }
    }
}

// Utility functions for metrics export and monitoring
pub fn export_metrics_to_file(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let system = get_logging_system();
    let snapshot = system.histogram_collector.snapshot();
    let json = snapshot.to_json();
    std::fs::write(path, serde_json::to_string_pretty(&json)?)?;
    Ok(())
}

pub fn get_performance_stats() -> HashMap<String, crate::features::logging::performance::OperationStats> {
    let system = get_logging_system();
    system.performance_monitor.get_all_operation_stats()
}

pub fn get_metrics_snapshot() -> crate::features::logging::metrics::MetricsSnapshot {
    let system = get_logging_system();
    system.histogram_collector.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logging_system_initialization() {
        let config = LoggingConfig::default();
        assert!(LoggingSystem::initialize_global(Some(config)).is_ok());
        
        let _system = get_logging_system();
        // Metrics counter initialized successfully
    }

    #[test]
    fn test_operation_timer() {
        // Initialize logging system first
        let _ = LoggingSystem::initialize_global(None);
        
        let timer = OperationTimer::with_key("test_op", b"test_key");
        std::thread::sleep(Duration::from_millis(10));
        timer.complete::<()>(&Ok(()));
    }

    #[test]
    fn test_logging_macros() {
        // Initialize logging system first
        let _ = LoggingSystem::initialize_global(None);
        
        log_operation!(
            Level::DEBUG,
            "put",
            b"key1",
            Duration::from_micros(100),
            Ok::<(), String>(())
        );
        log_transaction!("commit", 123, Duration::from_millis(50));
        log_cache_event!("get", b"key1", true);
        log_compaction!(0, 10, 3, Duration::from_secs(2));
        log_recovery!("replay", 100, 1000);
    }
    
    #[test]
    fn test_context_propagation() {
        let _ = LoggingSystem::initialize_global(None);
        
        let context = TraceContext::new()
            .with_user_id("user123".to_string())
            .with_request_id("req456".to_string());
            
        // Test trace context creation
        assert_eq!(context.user_id, Some("user123".to_string()));
        assert_eq!(context.request_id, Some("req456".to_string()));
    }
    
    #[test]
    fn test_performance_monitoring() {
        let _ = LoggingSystem::initialize_global(None);
        
        with_perf_monitoring!("test_operation", {
            std::thread::sleep(Duration::from_millis(1));
            42
        });
        
        let stats = get_performance_stats();
        assert!(stats.contains_key("test_operation"));
    }
}

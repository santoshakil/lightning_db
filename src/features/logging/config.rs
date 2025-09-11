use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::Level;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub format: LogFormat,
    pub output: LogOutput,
    pub sampling: SamplingConfig,
    pub telemetry: TelemetryConfig,
    pub correlation_id_header: String,
    pub redaction: RedactionConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    Json,
    Compact,
    Pretty,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogOutput {
    pub console: bool,
    pub file: Option<FileOutput>,
    pub jaeger: Option<JaegerConfig>,
    pub prometheus: Option<PrometheusConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOutput {
    pub path: String,
    pub rotation: FileRotation,
    pub max_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileRotation {
    Hourly,
    Daily,
    Size(u64), // bytes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JaegerConfig {
    pub endpoint: String,
    pub service_name: String,
    pub batch_timeout: Duration,
    pub max_batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    pub endpoint: String,
    pub push_interval: Duration,
    pub registry_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    pub enabled: bool,
    pub trace_sample_rate: f64,
    pub debug_sample_rate: f64,
    pub high_frequency_operations: Vec<String>,
    pub high_frequency_sample_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub service_version: String,
    pub environment: String,
    pub resource_attributes: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionConfig {
    pub enabled: bool,
    pub patterns: Vec<String>,
    pub replacement: String,
    pub redact_keys: bool,
    pub redact_values: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub slow_operation_threshold: Duration,
    pub enable_operation_timing: bool,
    pub enable_memory_tracking: bool,
    pub enable_resource_monitoring: bool,
    pub metrics_buffer_size: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Json,
            output: LogOutput {
                console: true,
                file: Some(FileOutput {
                    path: "logs/lightning_db.log".to_string(),
                    rotation: FileRotation::Daily,
                    max_files: 30,
                }),
                jaeger: None,
                prometheus: Some(PrometheusConfig {
                    endpoint: "http://localhost:9090".to_string(),
                    push_interval: Duration::from_secs(30),
                    registry_name: "lightning_db".to_string(),
                }),
            },
            sampling: SamplingConfig {
                enabled: true,
                trace_sample_rate: 0.01,
                debug_sample_rate: 0.1,
                high_frequency_operations: vec![
                    "get".to_string(),
                    "cache_lookup".to_string(),
                    "lock_acquire".to_string(),
                ],
                high_frequency_sample_rate: 0.001,
            },
            telemetry: TelemetryConfig {
                enabled: true,
                service_name: "lightning_db".to_string(),
                service_version: env!("CARGO_PKG_VERSION").to_string(),
                environment: "development".to_string(),
                resource_attributes: std::collections::HashMap::new(),
            },
            correlation_id_header: "x-correlation-id".to_string(),
            redaction: RedactionConfig {
                enabled: true,
                patterns: vec![
                    r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b".to_string(), // Credit cards
                    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b".to_string(), // Emails
                    r"\b\d{3}-?\d{2}-?\d{4}\b".to_string(),                    // SSN
                ],
                replacement: "[REDACTED]".to_string(),
                redact_keys: false,
                redact_values: true,
            },
            performance: PerformanceConfig {
                slow_operation_threshold: Duration::from_millis(100),
                enable_operation_timing: true,
                enable_memory_tracking: true,
                enable_resource_monitoring: true,
                metrics_buffer_size: 10000,
            },
        }
    }
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl LoggingConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(level) = std::env::var("LIGHTNING_LOG_LEVEL") {
            config.level = match level.to_lowercase().as_str() {
                "trace" => LogLevel::Trace,
                "debug" => LogLevel::Debug,
                "info" => LogLevel::Info,
                "warn" => LogLevel::Warn,
                "error" => LogLevel::Error,
                _ => LogLevel::Info,
            };
        }

        if let Ok(format) = std::env::var("LIGHTNING_LOG_FORMAT") {
            config.format = match format.to_lowercase().as_str() {
                "json" => LogFormat::Json,
                "compact" => LogFormat::Compact,
                "pretty" => LogFormat::Pretty,
                "full" => LogFormat::Full,
                _ => LogFormat::Json,
            };
        }

        if let Ok(env) = std::env::var("LIGHTNING_ENV") {
            config.telemetry.environment = env;
        }

        config
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.sampling.trace_sample_rate < 0.0 || self.sampling.trace_sample_rate > 1.0 {
            return Err("trace_sample_rate must be between 0.0 and 1.0".to_string());
        }

        if self.sampling.debug_sample_rate < 0.0 || self.sampling.debug_sample_rate > 1.0 {
            return Err("debug_sample_rate must be between 0.0 and 1.0".to_string());
        }

        if self.sampling.high_frequency_sample_rate < 0.0
            || self.sampling.high_frequency_sample_rate > 1.0
        {
            return Err("high_frequency_sample_rate must be between 0.0 and 1.0".to_string());
        }

        if self.performance.metrics_buffer_size == 0 {
            return Err("metrics_buffer_size must be greater than 0".to_string());
        }

        Ok(())
    }
}

use crate::core::error::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

pub mod metrics;
pub mod prometheus;
pub mod health;
#[path = "tracing.rs"]
pub mod tracing_module;
pub mod aggregation;
pub mod dashboards;
pub mod alerts;
pub mod profiling;
pub mod diagnostics;

pub use metrics::*;
pub use prometheus::*;
pub use health::*;
pub use tracing_module::*;
pub use aggregation::*;
pub use dashboards::*;
pub use alerts::*;
pub use profiling::*;
pub use diagnostics::*;

/// Main observability system for Lightning DB
pub struct ObservabilitySystem {
    metrics: Arc<MetricsEngine>,
    health: Arc<HealthSystem>,
    tracing: Arc<TracingSystem>,
    aggregation: Arc<MetricsAggregator>,
    profiling: Arc<ProfilingSystem>,
    diagnostics: Arc<DiagnosticsSystem>,
    prometheus: Arc<PrometheusExporter>,
    config: ObservabilityConfig,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub metrics_enabled: bool,
    pub health_check_enabled: bool,
    pub tracing_enabled: bool,
    pub profiling_enabled: bool,
    pub metrics_port: u16,
    pub health_port: u16,
    pub retention_period: Duration,
    pub aggregation_interval: Duration,
    pub max_metrics_memory: usize,
    pub enable_dashboards: bool,
    pub enable_alerts: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            health_check_enabled: true,
            tracing_enabled: true,
            profiling_enabled: false,
            metrics_port: 9090,
            health_port: 9091,
            retention_period: Duration::from_secs(24 * 60 * 60), // 24 hours
            aggregation_interval: Duration::from_secs(60), // 1 minute
            max_metrics_memory: 100 * 1024 * 1024, // 100MB
            enable_dashboards: true,
            enable_alerts: true,
        }
    }
}

impl ObservabilitySystem {
    pub fn new(config: ObservabilityConfig) -> Result<Self> {
        let metrics = Arc::new(MetricsEngine::new(config.clone())?);
        let health = Arc::new(HealthSystem::new(config.clone())?);
        let tracing = Arc::new(TracingSystem::new(config.clone())?);
        let aggregation = Arc::new(MetricsAggregator::new(
            metrics.clone(),
            config.aggregation_interval,
        ));
        let profiling = Arc::new(ProfilingSystem::new(config.clone())?);
        let diagnostics = Arc::new(DiagnosticsSystem::new(config.clone())?);
        let prometheus = Arc::new(PrometheusExporter::new(
            metrics.clone(),
            health.clone(),
            config.clone(),
        )?);

        Ok(Self {
            metrics,
            health,
            tracing,
            aggregation,
            profiling,
            diagnostics,
            prometheus,
            config,
            server_handle: None,
        })
    }

    /// Start all observability services
    pub async fn start(&mut self) -> Result<()> {
        // Start metrics collection
        if self.config.metrics_enabled {
            self.metrics.start().await?;
        }

        // Start health checks
        if self.config.health_check_enabled {
            self.health.start().await?;
        }

        // Start distributed tracing
        if self.config.tracing_enabled {
            self.tracing.start().await?;
        }

        // Start metrics aggregation
        self.aggregation.start().await?;

        // Start profiling if enabled
        if self.config.profiling_enabled {
            self.profiling.start().await?;
        }

        // Start HTTP server for metrics and health endpoints
        let server_handle = self.start_http_server().await?;
        self.server_handle = Some(server_handle);

        // Observability system started successfully
        Ok(())
    }

    /// Stop all observability services
    pub async fn stop(&mut self) -> Result<()> {
        // Stop HTTP server
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }

        // Stop all services
        self.profiling.stop().await?;
        self.aggregation.stop().await?;
        self.tracing.stop().await?;
        self.health.stop().await?;
        self.metrics.stop().await?;

        // Observability system stopped
        Ok(())
    }

    /// Get current metrics snapshot
    pub async fn get_metrics(&self) -> Result<MetricsSnapshot> {
        self.metrics.get_snapshot().await
    }

    /// Export metrics in Prometheus format
    pub async fn export_prometheus(&self) -> Result<String> {
        self.prometheus.export().await
    }

    /// Get health status
    pub async fn get_health_status(&self) -> Result<HealthStatusReport> {
        self.health.get_status().await
    }

    /// Get diagnostic information
    pub async fn get_diagnostics(&self) -> Result<DiagnosticReport> {
        self.diagnostics.generate_report().await
    }

    /// Start performance profiling
    pub async fn start_profiling(&self, duration: Duration) -> Result<ProfilingSession> {
        self.profiling.start_session(duration).await
    }

    /// Get profiling results
    pub async fn get_profiling_results(&self, session_id: String) -> Result<ProfilingResults> {
        self.profiling.get_results(session_id).await
    }

    async fn start_http_server(&self) -> Result<tokio::task::JoinHandle<()>> {
        let prometheus = self.prometheus.clone();
        let health = self.health.clone();
        let diagnostics = self.diagnostics.clone();
        let metrics_port = self.config.metrics_port;

        let handle = tokio::spawn(async move {
            let listener = match TcpListener::bind(format!("0.0.0.0:{}", metrics_port)).await {
                Ok(l) => l,
                Err(_e) => {
                    // Error:("Failed to bind observability server: {}", e);
                    return;
                }
            };

            // Info:("Observability HTTP server listening on :{}", metrics_port);

            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let prometheus = prometheus.clone();
                        let health = health.clone();
                        let diagnostics = diagnostics.clone();

                        tokio::spawn(async move {
                            if let Err(_e) = Self::handle_connection(
                                stream, addr, prometheus, health, diagnostics
                            ).await {
                                // Warn:("Error handling connection from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(_e) => {
                        // Error:("Failed to accept connection: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(handle)
    }

    async fn handle_connection(
        stream: tokio::net::TcpStream,
        _addr: std::net::SocketAddr,
        prometheus: Arc<PrometheusExporter>,
        health: Arc<HealthSystem>,
        diagnostics: Arc<DiagnosticsSystem>,
    ) -> Result<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line).await?;

        let mut stream = reader.into_inner();
        
        match request_line.trim() {
            line if line.starts_with("GET /metrics") => {
                let metrics = prometheus.export().await?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                     Content-Length: {}\r\n\
                     \r\n{}",
                    metrics.len(),
                    metrics
                );
                stream.write_all(response.as_bytes()).await?;
            }
            line if line.starts_with("GET /health") => {
                let health_status = health.get_status().await?;
                let json = serde_json::to_string(&health_status)?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Content-Length: {}\r\n\
                     \r\n{}",
                    json.len(),
                    json
                );
                stream.write_all(response.as_bytes()).await?;
            }
            line if line.starts_with("GET /diagnostics") => {
                let diagnostic_report = diagnostics.generate_report().await?;
                let json = serde_json::to_string(&diagnostic_report)?;
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                     Content-Type: application/json\r\n\
                     Content-Length: {}\r\n\
                     \r\n{}",
                    json.len(),
                    json
                );
                stream.write_all(response.as_bytes()).await?;
            }
            _ => {
                let response = "HTTP/1.1 404 Not Found\r\n\r\n";
                stream.write_all(response.as_bytes()).await?;
            }
        }

        stream.flush().await?;
        Ok(())
    }

    /// Record a database operation
    pub fn record_operation(
        &self,
        operation_type: OperationType,
        latency: Duration,
        size: Option<usize>,
        success: bool,
    ) {
        if self.config.metrics_enabled {
            self.metrics.record_operation(operation_type, latency, size, success);
        }
    }

    /// Record cache access
    pub fn record_cache_access(&self, cache_type: CacheType, hit: bool, size: Option<usize>) {
        if self.config.metrics_enabled {
            self.metrics.record_cache_access(cache_type, hit, size);
        }
    }

    /// Record transaction event
    pub fn record_transaction(&self, event: TransactionEvent, duration: Option<Duration>) {
        if self.config.metrics_enabled {
            self.metrics.record_transaction(event, duration);
        }
    }

    /// Record error
    pub fn record_error(&self, error_type: ErrorType, error_msg: Option<String>) {
        if self.config.metrics_enabled {
            self.metrics.record_error(error_type, error_msg.clone());
        }

        // Update health status on critical errors
        if matches!(error_type, ErrorType::Corruption | ErrorType::OutOfMemory | ErrorType::DiskFull) {
            self.health.record_critical_error(error_type, error_msg);
        }
    }

    /// Update resource metrics
    pub fn update_resource_metrics(&self, resources: ObservabilityResourceMetrics) {
        if self.config.metrics_enabled {
            // Convert ObservabilityResourceMetrics to ResourceMetrics
            let converted = ResourceMetrics {
                memory_used: AtomicU64::new(resources.memory_used),
                memory_available: AtomicU64::new(resources.memory_available),
                disk_used: AtomicU64::new(resources.disk_used),
                disk_available: AtomicU64::new(resources.disk_available),
                cpu_usage: Arc::new(RwLock::new(resources.cpu_usage)),
                thread_count: AtomicUsize::new(resources.thread_count),
                open_files: AtomicUsize::new(resources.open_files),
                network_connections: AtomicUsize::new(resources.network_connections),
            };
            self.metrics.update_resource_metrics(converted);
        }
    }
}

/// Database operation types for metrics
#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Read,
    Write,
    Delete,
    Scan,
    Checkpoint,
    Compaction,
    Vacuum,
    Backup,
}

/// Cache types for metrics
#[derive(Debug, Clone, Copy)]
pub enum CacheType {
    PageCache,
    BTreeCache,
    LSMCache,
    MetadataCache,
}

/// Transaction events for metrics
#[derive(Debug, Clone, Copy)]
pub enum TransactionEvent {
    Begin,
    Commit,
    Rollback,
    Conflict,
    Deadlock,
}

/// Error types for metrics and health monitoring
#[derive(Debug, Clone, Copy)]
pub enum ErrorType {
    IO,
    Corruption,
    OutOfMemory,
    DiskFull,
    NetworkError,
    SerializationError,
    ValidationError,
    TimeoutError,
    PermissionError,
    ConfigurationError,
}

/// Resource metrics structure for observability
#[derive(Debug, Clone)]
pub struct ObservabilityResourceMetrics {
    pub memory_used: u64,
    pub memory_available: u64,
    pub disk_used: u64,
    pub disk_available: u64,
    pub cpu_usage: f64,
    pub thread_count: usize,
    pub open_files: usize,
    pub network_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_observability_system_creation() {
        let config = ObservabilityConfig::default();
        let system = ObservabilitySystem::new(config).unwrap();
        
        // Test that all components are created
        assert!(system.metrics.is_running() || !system.config.metrics_enabled);
    }

    #[tokio::test]
    async fn test_metrics_recording() {
        let config = ObservabilityConfig::default();
        let system = ObservabilitySystem::new(config).unwrap();

        // Record some operations
        system.record_operation(
            OperationType::Read,
            Duration::from_micros(100),
            Some(1024),
            true,
        );

        system.record_cache_access(CacheType::PageCache, true, Some(4096));
        system.record_transaction(TransactionEvent::Commit, Some(Duration::from_millis(1)));
        
        // Verify metrics were recorded (in a real test, we'd check the internal state)
        let metrics = system.get_metrics().await.unwrap();
        assert!(metrics.operations.reads > 0);
    }

    #[test]
    fn test_config_defaults() {
        let config = ObservabilityConfig::default();
        assert!(config.metrics_enabled);
        assert!(config.health_check_enabled);
        assert_eq!(config.metrics_port, 9090);
        assert_eq!(config.health_port, 9091);
    }
}
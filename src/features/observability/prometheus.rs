use crate::core::error::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::fmt::Write;

use super::metrics::MetricsEngine;
use super::health::HealthSystem;
use super::ObservabilityConfig;

/// Prometheus metrics exporter
pub struct PrometheusExporter {
    metrics_engine: Arc<MetricsEngine>,
    health_system: Arc<HealthSystem>,
    config: ObservabilityConfig,
    custom_labels: HashMap<String, String>,
}

impl PrometheusExporter {
    pub fn new(
        metrics_engine: Arc<MetricsEngine>,
        health_system: Arc<HealthSystem>,
        config: ObservabilityConfig,
    ) -> Result<Self> {
        Ok(Self {
            metrics_engine,
            health_system,
            config,
            custom_labels: HashMap::new(),
        })
    }

    /// Export metrics in Prometheus format
    pub async fn export(&self) -> Result<String> {
        let mut output = String::with_capacity(8192);
        
        // Database operation metrics
        self.write_operation_metrics(&mut output).await?;
        
        // Cache metrics
        self.write_cache_metrics(&mut output).await?;
        
        // Transaction metrics
        self.write_transaction_metrics(&mut output).await?;
        
        // Resource metrics
        self.write_resource_metrics(&mut output).await?;
        
        // Error metrics
        self.write_error_metrics(&mut output).await?;
        
        // Health status
        self.write_health_metrics(&mut output).await?;
        
        Ok(output)
    }

    async fn write_operation_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "# HELP lightning_db_operations_total Total database operations")?;
        writeln!(output, "# TYPE lightning_db_operations_total counter")?;
        
        let metrics = self.metrics_engine.get_snapshot().await?;
        
        writeln!(output, "lightning_db_operations_total{{type=\"read\"}} {}", metrics.operations.reads)?;
        writeln!(output, "lightning_db_operations_total{{type=\"write\"}} {}", metrics.operations.writes)?;
        writeln!(output, "lightning_db_operations_total{{type=\"delete\"}} {}", metrics.operations.deletes)?;
        writeln!(output, "lightning_db_operations_total{{type=\"scan\"}} {}", metrics.operations.scans)?;
        
        writeln!(output, "\n# HELP lightning_db_operation_duration_seconds Operation latency")?;
        writeln!(output, "# TYPE lightning_db_operation_duration_seconds histogram")?;
        
        // Convert LatencySnapshot to HistogramData
        let histogram_data = HistogramData {
            bucket_le_1ms: 0, // We don't have bucket data from LatencySnapshot
            bucket_le_10ms: 0,
            bucket_le_100ms: 0,
            bucket_le_1s: metrics.latency.count, // Use total count as fallback
            count: metrics.latency.count,
            sum: metrics.latency.sum,
        };
        self.write_histogram(output, "lightning_db_operation_duration_seconds", &histogram_data)?;
        
        Ok(())
    }

    async fn write_cache_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "\n# HELP lightning_db_cache_hits_total Cache hits")?;
        writeln!(output, "# TYPE lightning_db_cache_hits_total counter")?;
        
        let metrics = self.metrics_engine.get_snapshot().await?;
        
        writeln!(output, "lightning_db_cache_hits_total {}", metrics.cache.hits)?;
        
        writeln!(output, "\n# HELP lightning_db_cache_misses_total Cache misses")?;
        writeln!(output, "# TYPE lightning_db_cache_misses_total counter")?;
        writeln!(output, "lightning_db_cache_misses_total {}", metrics.cache.misses)?;
        
        writeln!(output, "\n# HELP lightning_db_cache_size_bytes Current cache size")?;
        writeln!(output, "# TYPE lightning_db_cache_size_bytes gauge")?;
        writeln!(output, "lightning_db_cache_size_bytes {}", metrics.cache.size_bytes)?;
        
        Ok(())
    }

    async fn write_transaction_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "\n# HELP lightning_db_transactions_total Transaction counts")?;
        writeln!(output, "# TYPE lightning_db_transactions_total counter")?;
        
        let metrics = self.metrics_engine.get_snapshot().await?;
        
        writeln!(output, "lightning_db_transactions_total{{type=\"commit\"}} {}", metrics.transactions.commits)?;
        writeln!(output, "lightning_db_transactions_total{{type=\"rollback\"}} {}", metrics.transactions.rollbacks)?;
        writeln!(output, "lightning_db_transactions_total{{type=\"conflict\"}} {}", metrics.transactions.conflicts)?;
        
        writeln!(output, "\n# HELP lightning_db_transactions_active Active transactions")?;
        writeln!(output, "# TYPE lightning_db_transactions_active gauge")?;
        writeln!(output, "lightning_db_transactions_active {}", metrics.transactions.active)?;
        
        Ok(())
    }

    async fn write_resource_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "\n# HELP lightning_db_memory_bytes Memory usage")?;
        writeln!(output, "# TYPE lightning_db_memory_bytes gauge")?;
        
        let metrics = self.metrics_engine.get_snapshot().await?;
        
        writeln!(output, "lightning_db_memory_bytes{{type=\"used\"}} {}", metrics.resources.memory_used)?;
        writeln!(output, "lightning_db_memory_bytes{{type=\"available\"}} {}", metrics.resources.memory_available)?;
        
        writeln!(output, "\n# HELP lightning_db_disk_bytes Disk usage")?;
        writeln!(output, "# TYPE lightning_db_disk_bytes gauge")?;
        writeln!(output, "lightning_db_disk_bytes{{type=\"used\"}} {}", metrics.resources.disk_used)?;
        writeln!(output, "lightning_db_disk_bytes{{type=\"available\"}} {}", metrics.resources.disk_available)?;
        
        writeln!(output, "\n# HELP lightning_db_cpu_usage CPU usage percentage")?;
        writeln!(output, "# TYPE lightning_db_cpu_usage gauge")?;
        writeln!(output, "lightning_db_cpu_usage {}", metrics.resources.cpu_usage)?;
        
        Ok(())
    }

    async fn write_error_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "\n# HELP lightning_db_errors_total Error counts")?;
        writeln!(output, "# TYPE lightning_db_errors_total counter")?;
        
        let metrics = self.metrics_engine.get_snapshot().await?;
        
        for (error_type, count) in &metrics.errors.by_type {
            writeln!(output, "lightning_db_errors_total{{type=\"{}\"}} {}", error_type, count)?;
        }
        
        Ok(())
    }

    async fn write_health_metrics(&self, output: &mut String) -> Result<()> {
        writeln!(output, "\n# HELP lightning_db_health Database health status")?;
        writeln!(output, "# TYPE lightning_db_health gauge")?;
        
        let health = self.health_system.get_status().await?;
        let health_value = match health.overall_status.as_str() {
            "healthy" => 1.0,
            "degraded" => 0.5,
            "unhealthy" => 0.0,
            _ => 0.0,
        };
        
        writeln!(output, "lightning_db_health {}", health_value)?;
        
        Ok(())
    }

    fn write_histogram(&self, output: &mut String, name: &str, histogram: &HistogramData) -> Result<()> {
        writeln!(output, "{}_bucket{{le=\"0.001\"}} {}", name, histogram.bucket_le_1ms)?;
        writeln!(output, "{}_bucket{{le=\"0.01\"}} {}", name, histogram.bucket_le_10ms)?;
        writeln!(output, "{}_bucket{{le=\"0.1\"}} {}", name, histogram.bucket_le_100ms)?;
        writeln!(output, "{}_bucket{{le=\"1\"}} {}", name, histogram.bucket_le_1s)?;
        writeln!(output, "{}_bucket{{le=\"+Inf\"}} {}", name, histogram.count)?;
        writeln!(output, "{}_sum {}", name, histogram.sum)?;
        writeln!(output, "{}_count {}", name, histogram.count)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct HistogramData {
    pub bucket_le_1ms: u64,
    pub bucket_le_10ms: u64,
    pub bucket_le_100ms: u64,
    pub bucket_le_1s: u64,
    pub count: u64,
    pub sum: f64,
}
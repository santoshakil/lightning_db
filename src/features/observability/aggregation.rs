use crate::core::error::Result;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use super::metrics::MetricsEngine;
use super::ObservabilityConfig;

/// Metrics aggregation system for time-series data
pub struct MetricsAggregator {
    metrics_engine: Arc<MetricsEngine>,
    config: ObservabilityConfig,
    aggregated_data: Arc<RwLock<AggregatedMetrics>>,
    last_aggregation: Arc<RwLock<Instant>>,
}

#[derive(Debug, Clone, Default)]
pub struct AggregatedMetrics {
    pub hourly: Vec<HourlyMetrics>,
    pub daily: Vec<DailyMetrics>,
    pub percentiles: PercentileMetrics,
    pub trends: TrendAnalysis,
}

#[derive(Debug, Clone)]
pub struct HourlyMetrics {
    pub timestamp: u64,
    pub operations_per_second: f64,
    pub avg_latency_ms: f64,
    pub cache_hit_rate: f64,
    pub error_rate: f64,
    pub memory_usage_mb: f64,
    pub active_connections: u64,
}

#[derive(Debug, Clone)]
pub struct DailyMetrics {
    pub date: String,
    pub total_operations: u64,
    pub total_errors: u64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub peak_memory_mb: f64,
    pub avg_cpu_percent: f64,
}

#[derive(Debug, Clone, Default)]
pub struct PercentileMetrics {
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
}

#[derive(Debug, Clone, Default)]
pub struct TrendAnalysis {
    pub ops_trend: String, // "increasing", "decreasing", "stable"
    pub latency_trend: String,
    pub error_trend: String,
    pub resource_trend: String,
    pub anomalies: Vec<String>,
}

impl MetricsAggregator {
    pub fn new(metrics_engine: Arc<MetricsEngine>, interval: Duration) -> Self {
        Self {
            metrics_engine,
            config: ObservabilityConfig {
                aggregation_interval: interval,
                ..Default::default()
            },
            aggregated_data: Arc::new(RwLock::new(AggregatedMetrics::default())),
            last_aggregation: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let metrics_engine = self.metrics_engine.clone();
        let aggregated_data = self.aggregated_data.clone();
        let last_aggregation = self.last_aggregation.clone();
        let interval = self.config.aggregation_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            
            loop {
                interval.tick().await;
                
                // Get current metrics snapshot
                if let Ok(snapshot) = metrics_engine.get_snapshot().await {
                    // Calculate aggregated metrics
                    let hourly = Self::calculate_hourly_metrics(&snapshot);
                    let percentiles = Self::calculate_percentiles(&snapshot);
                    let trends = Self::analyze_trends(&snapshot);
                    
                    let mut data = aggregated_data.write().await;
                    
                    // Add hourly metrics
                    data.hourly.push(hourly);
                    if data.hourly.len() > 24 {
                        data.hourly.remove(0);
                    }
                    
                    // Update percentiles and trends
                    data.percentiles = percentiles;
                    data.trends = trends;
                    
                    // Calculate daily metrics if needed
                    if data.hourly.len() == 24 {
                        let daily = Self::calculate_daily_metrics(&data.hourly);
                        data.daily.push(daily);
                        if data.daily.len() > 30 {
                            data.daily.remove(0);
                        }
                    }
                    
                    *last_aggregation.write().await = Instant::now();
                }
            }
        });
        
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        // Cleanup would be done here
        Ok(())
    }

    pub async fn get_aggregated_metrics(&self) -> AggregatedMetrics {
        self.aggregated_data.read().await.clone()
    }

    fn calculate_hourly_metrics(snapshot: &super::metrics::MetricsSnapshot) -> HourlyMetrics {
        let total_ops = snapshot.operations.reads + snapshot.operations.writes + 
                       snapshot.operations.deletes + snapshot.operations.scans;
        let cache_total = snapshot.cache.hits + snapshot.cache.misses;
        let cache_hit_rate = if cache_total > 0 {
            snapshot.cache.hits as f64 / cache_total as f64
        } else {
            0.0
        };
        
        HourlyMetrics {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            operations_per_second: total_ops as f64,
            avg_latency_ms: snapshot.latency.sum / snapshot.latency.count.max(1) as f64 * 1000.0,
            cache_hit_rate,
            error_rate: snapshot.errors.total as f64 / total_ops.max(1) as f64,
            memory_usage_mb: snapshot.resources.memory_used as f64 / 1_048_576.0,
            active_connections: snapshot.resources.network_connections as u64,
        }
    }

    fn calculate_percentiles(_snapshot: &super::metrics::MetricsSnapshot) -> PercentileMetrics {
        // In a real implementation, this would calculate actual percentiles from histogram data
        PercentileMetrics {
            p50: 0.001,
            p75: 0.005,
            p90: 0.010,
            p95: 0.020,
            p99: 0.050,
            p999: 0.100,
        }
    }

    fn analyze_trends(snapshot: &super::metrics::MetricsSnapshot) -> TrendAnalysis {
        // Simple trend analysis - in production would use more sophisticated algorithms
        TrendAnalysis {
            ops_trend: "stable".to_string(),
            latency_trend: "stable".to_string(),
            error_trend: if snapshot.errors.total > 10 { "increasing".to_string() } else { "stable".to_string() },
            resource_trend: "stable".to_string(),
            anomalies: Vec::new(),
        }
    }

    fn calculate_daily_metrics(hourly: &[HourlyMetrics]) -> DailyMetrics {
        let total_ops: u64 = hourly.iter().map(|h| h.operations_per_second as u64).sum();
        let total_errors: u64 = hourly.iter().map(|h| (h.error_rate * h.operations_per_second) as u64).sum();
        let avg_latency = hourly.iter().map(|h| h.avg_latency_ms).sum::<f64>() / hourly.len() as f64;
        let peak_memory = hourly.iter().map(|h| h.memory_usage_mb).fold(0.0, f64::max);
        
        DailyMetrics {
            date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            total_operations: total_ops * 3600, // Convert to per hour
            total_errors,
            avg_latency_ms: avg_latency,
            p95_latency_ms: avg_latency * 1.5, // Simplified
            p99_latency_ms: avg_latency * 2.0, // Simplified
            peak_memory_mb: peak_memory,
            avg_cpu_percent: 50.0, // Placeholder
        }
    }
}
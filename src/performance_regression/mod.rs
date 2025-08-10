//! Advanced Performance Regression Detection System
//!
//! Provides comprehensive performance monitoring, regression detection, and automated
//! alerting capabilities for Lightning DB. Integrates with the distributed tracing
//! system to provide deep performance insights and early warning systems.

use crate::distributed_tracing::Span;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

pub mod alert_manager;
pub mod baseline_profiler;
pub mod bisection_tools;
pub mod performance_tracker;
pub mod statistical_analysis;
pub mod trend_analysis;

/// Main performance regression detection system
pub struct PerformanceRegressionDetector {
    baseline_profiler: Arc<baseline_profiler::BaselineProfiler>,
    statistical_analyzer: Arc<statistical_analysis::StatisticalAnalyzer>,
    alert_manager: Arc<alert_manager::AlertManager>,
    trend_analyzer: Arc<trend_analysis::TrendAnalyzer>,
    bisection_tools: Arc<Mutex<bisection_tools::BisectionTools>>,
    config: RegressionDetectorConfig,
    metrics_storage: Arc<RwLock<MetricsStorage>>,
}

/// Configuration for the regression detection system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionDetectorConfig {
    pub baseline_window_hours: u64,
    pub detection_sensitivity: f64,
    pub min_samples_for_analysis: usize,
    pub max_stored_metrics: usize,
    pub alert_cooldown_minutes: u64,
    pub trend_analysis_enabled: bool,
    pub bisection_enabled: bool,
    pub auto_baseline_updates: bool,
}

impl Default for RegressionDetectorConfig {
    fn default() -> Self {
        Self {
            baseline_window_hours: 24,
            detection_sensitivity: 0.15, // 15% performance degradation threshold
            min_samples_for_analysis: 100,
            max_stored_metrics: 100_000,
            alert_cooldown_minutes: 30,
            trend_analysis_enabled: true,
            bisection_enabled: true,
            auto_baseline_updates: true,
        }
    }
}

/// Performance metric data point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetric {
    pub timestamp: SystemTime,
    pub operation_type: String,
    pub duration_micros: u64,
    pub throughput_ops_per_sec: f64,
    pub memory_usage_bytes: u64,
    pub cpu_usage_percent: f64,
    pub error_rate: f64,
    pub additional_metrics: HashMap<String, f64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

/// Performance baseline for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub operation_type: String,
    pub created_at: SystemTime,
    pub sample_count: usize,
    pub mean_duration_micros: f64,
    pub std_deviation_micros: f64,
    pub p50_duration_micros: u64,
    pub p95_duration_micros: u64,
    pub p99_duration_micros: u64,
    pub mean_throughput: f64,
    pub memory_baseline: u64,
    pub cpu_baseline: f64,
    pub confidence_interval: (f64, f64),
}

/// Regression detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionDetectionResult {
    pub detected: bool,
    pub severity: RegressionSeverity,
    pub operation_type: String,
    pub current_performance: PerformanceMetric,
    pub baseline_performance: PerformanceBaseline,
    pub degradation_percentage: f64,
    pub statistical_confidence: f64,
    pub recommended_actions: Vec<String>,
    pub detection_timestamp: SystemTime,
}

/// Severity levels for performance regressions
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegressionSeverity {
    Minor,    // 10-20% degradation
    Moderate, // 20-40% degradation
    Major,    // 40-70% degradation
    Critical, // >70% degradation
}

/// Storage for performance metrics and baselines
struct MetricsStorage {
    metrics: BTreeMap<SystemTime, Vec<PerformanceMetric>>,
    baselines: HashMap<String, PerformanceBaseline>,
    recent_detections: VecDeque<RegressionDetectionResult>,
    operation_stats: HashMap<String, OperationStatistics>,
}

/// Statistics for a specific operation type
#[derive(Debug, Clone)]
struct OperationStatistics {
    total_samples: usize,
    recent_samples: VecDeque<PerformanceMetric>,
    moving_average: f64,
    trend_direction: TrendDirection,
    last_regression: Option<SystemTime>,
}

/// Trend direction for performance metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrendDirection {
    Improving,
    Stable,
    Degrading,
    Unknown,
}

impl PerformanceRegressionDetector {
    /// Create a new performance regression detection system
    pub fn new(config: RegressionDetectorConfig) -> Self {
        let metrics_storage = Arc::new(RwLock::new(MetricsStorage {
            metrics: BTreeMap::new(),
            baselines: HashMap::new(),
            recent_detections: VecDeque::new(),
            operation_stats: HashMap::new(),
        }));

        let baseline_profiler = Arc::new(baseline_profiler::BaselineProfiler::new(
            metrics_storage.clone(),
            config.clone(),
        ));

        let statistical_analyzer = Arc::new(statistical_analysis::StatisticalAnalyzer::new(
            config.clone(),
        ));

        let alert_manager = Arc::new(alert_manager::AlertManager::new(config.clone()));

        let trend_analyzer = Arc::new(trend_analysis::TrendAnalyzer::new(
            metrics_storage.clone(),
            config.clone(),
        ));

        let bisection_tools = Arc::new(Mutex::new(bisection_tools::BisectionTools::new(
            config.clone(),
        )));

        Self {
            baseline_profiler,
            statistical_analyzer,
            alert_manager,
            trend_analyzer,
            bisection_tools,
            config,
            metrics_storage,
        }
    }

    /// Record a performance metric from a traced operation
    pub fn record_metric(&self, metric: PerformanceMetric) -> Result<()> {
        // Store the metric
        if let Ok(mut storage) = self.metrics_storage.write() {
            let timestamp = metric.timestamp;
            storage
                .metrics
                .entry(timestamp)
                .or_insert_with(Vec::new)
                .push(metric.clone());

            // Update operation statistics
            let op_stats = storage
                .operation_stats
                .entry(metric.operation_type.clone())
                .or_insert_with(|| OperationStatistics {
                    total_samples: 0,
                    recent_samples: VecDeque::new(),
                    moving_average: 0.0,
                    trend_direction: TrendDirection::Unknown,
                    last_regression: None,
                });

            op_stats.total_samples += 1;
            op_stats.recent_samples.push_back(metric.clone());

            // Keep only recent samples (sliding window)
            let max_recent = 1000;
            while op_stats.recent_samples.len() > max_recent {
                op_stats.recent_samples.pop_front();
            }

            // Update moving average
            let recent_durations: Vec<f64> = op_stats
                .recent_samples
                .iter()
                .map(|m| m.duration_micros as f64)
                .collect();

            if !recent_durations.is_empty() {
                op_stats.moving_average =
                    recent_durations.iter().sum::<f64>() / recent_durations.len() as f64;
            }

            // Clean up old metrics
            self.cleanup_old_metrics(&mut storage);
        }

        // Check for regressions
        self.check_for_regression(&metric)?;

        Ok(())
    }

    /// Record performance metric from a span
    pub fn record_from_span(&self, span: &Span) -> Result<()> {
        if let Some(duration_micros) = span.duration_micros() {
            // Extract additional metrics from span tags
            let mut additional_metrics = HashMap::new();
            let mut throughput = 0.0;
            let mut memory_usage = 0;
            let mut cpu_usage = 0.0;
            let mut error_rate = 0.0;

            // Parse known performance metrics from span tags
            if let Some(throughput_str) = span.tags.get("performance.throughput") {
                throughput = throughput_str.parse().unwrap_or(0.0);
            }
            if let Some(memory_str) = span.tags.get("performance.memory_bytes") {
                memory_usage = memory_str.parse().unwrap_or(0);
            }
            if let Some(cpu_str) = span.tags.get("performance.cpu_percent") {
                cpu_usage = cpu_str.parse().unwrap_or(0.0);
            }

            // Calculate error rate based on span status
            error_rate = match span.status {
                crate::distributed_tracing::SpanStatus::Error(_) => 1.0,
                _ => 0.0,
            };

            // Add custom metrics from span tags
            for (key, value) in &span.tags {
                if key.starts_with("metric.") {
                    if let Ok(metric_value) = value.parse::<f64>() {
                        additional_metrics.insert(key.clone(), metric_value);
                    }
                }
            }

            let metric = PerformanceMetric {
                timestamp: span.start_time,
                operation_type: span.operation_name.clone(),
                duration_micros,
                throughput_ops_per_sec: throughput,
                memory_usage_bytes: memory_usage,
                cpu_usage_percent: cpu_usage,
                error_rate,
                additional_metrics,
                trace_id: Some(span.trace_id.clone()),
                span_id: Some(span.span_id.clone()),
            };

            self.record_metric(metric)
        } else {
            Ok(())
        }
    }

    /// Check for performance regression
    pub fn check_for_regression(
        &self,
        metric: &PerformanceMetric,
    ) -> Result<Option<RegressionDetectionResult>> {
        // Get baseline for this operation type
        let baseline = if let Ok(storage) = self.metrics_storage.read() {
            storage.baselines.get(&metric.operation_type).cloned()
        } else {
            return Ok(None);
        };

        let baseline = match baseline {
            Some(b) => b,
            None => {
                // No baseline yet, trigger baseline creation if we have enough samples
                self.baseline_profiler
                    .try_create_baseline(&metric.operation_type)?;
                return Ok(None);
            }
        };

        // Use statistical analyzer to detect regression
        let regression_result = self
            .statistical_analyzer
            .analyze_regression(metric, &baseline)?;

        if let Some(mut result) = regression_result {
            // Add recommended actions based on severity
            result.recommended_actions = self.generate_recommendations(&result);

            // Store detection result
            if let Ok(mut storage) = self.metrics_storage.write() {
                storage.recent_detections.push_back(result.clone());

                // Keep only recent detections
                while storage.recent_detections.len() > 1000 {
                    storage.recent_detections.pop_front();
                }

                // Update operation statistics
                if let Some(op_stats) = storage.operation_stats.get_mut(&metric.operation_type) {
                    op_stats.last_regression = Some(result.detection_timestamp);
                    op_stats.trend_direction = if result.degradation_percentage > 0.0 {
                        TrendDirection::Degrading
                    } else {
                        TrendDirection::Improving
                    };
                }
            }

            // Send alert if needed
            if result.detected {
                self.alert_manager.send_regression_alert(&result)?;
            }

            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// Get current performance baselines
    pub fn get_baselines(&self) -> Result<HashMap<String, PerformanceBaseline>> {
        if let Ok(storage) = self.metrics_storage.read() {
            Ok(storage.baselines.clone())
        } else {
            Ok(HashMap::new())
        }
    }

    /// Get recent regression detections
    pub fn get_recent_detections(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<RegressionDetectionResult>> {
        if let Ok(storage) = self.metrics_storage.read() {
            let detections: Vec<_> = storage
                .recent_detections
                .iter()
                .rev()
                .take(limit.unwrap_or(100))
                .cloned()
                .collect();
            Ok(detections)
        } else {
            Ok(Vec::new())
        }
    }

    /// Get performance trends for an operation
    pub fn get_performance_trends(
        &self,
        operation_type: &str,
        hours: u64,
    ) -> Result<Vec<PerformanceMetric>> {
        self.trend_analyzer.get_trends(operation_type, hours)
    }

    /// Start bisection analysis for a detected regression
    pub fn start_bisection_analysis(
        &self,
        regression: &RegressionDetectionResult,
    ) -> Result<bisection_tools::BisectionSession> {
        if let Ok(mut tools) = self.bisection_tools.lock() {
            tools.start_bisection(regression)
        } else {
            Err(crate::Error::Generic(
                "Failed to acquire bisection tools lock".to_string(),
            ))
        }
    }

    /// Force baseline update for an operation type
    pub fn update_baseline(&self, operation_type: &str) -> Result<PerformanceBaseline> {
        self.baseline_profiler.force_update_baseline(operation_type)
    }

    /// Get comprehensive performance report
    pub fn generate_performance_report(&self) -> Result<PerformanceReport> {
        let baselines = self.get_baselines()?;
        let recent_detections = self.get_recent_detections(Some(50))?;

        let mut operation_summaries = HashMap::new();

        if let Ok(storage) = self.metrics_storage.read() {
            for (op_type, stats) in &storage.operation_stats {
                let trend_data = self.trend_analyzer.get_trends(op_type, 24)?;

                let summary = OperationSummary {
                    operation_type: op_type.clone(),
                    total_samples: stats.total_samples,
                    recent_average_micros: stats.moving_average,
                    trend_direction: stats.trend_direction,
                    last_regression: stats.last_regression,
                    baseline: baselines.get(op_type).cloned(),
                    recent_performance: trend_data.last().cloned(),
                };

                operation_summaries.insert(op_type.clone(), summary);
            }
        }

        Ok(PerformanceReport {
            generated_at: SystemTime::now(),
            total_operations_tracked: operation_summaries.len(),
            active_regressions: recent_detections
                .iter()
                .filter(|d| d.detected && d.severity >= RegressionSeverity::Major)
                .count(),
            recent_detections,
            operation_summaries,
            system_health_score: self.calculate_health_score()?,
        })
    }

    /// Generate recommendations for a regression
    fn generate_recommendations(&self, result: &RegressionDetectionResult) -> Vec<String> {
        let mut recommendations = Vec::new();

        match result.severity {
            RegressionSeverity::Critical => {
                recommendations.push(
                    "IMMEDIATE ACTION REQUIRED: Critical performance regression detected"
                        .to_string(),
                );
                recommendations.push("Consider rolling back recent changes".to_string());
                recommendations.push("Scale up resources immediately".to_string());
                recommendations
                    .push("Enable detailed profiling for root cause analysis".to_string());
            }
            RegressionSeverity::Major => {
                recommendations.push("Major performance degradation detected".to_string());
                recommendations
                    .push("Review recent code changes for performance impacts".to_string());
                recommendations.push("Consider increasing resource allocation".to_string());
                recommendations.push("Run detailed performance profiling".to_string());
            }
            RegressionSeverity::Moderate => {
                recommendations.push("Moderate performance regression detected".to_string());
                recommendations
                    .push("Monitor closely and investigate during maintenance window".to_string());
                recommendations.push("Consider optimization opportunities".to_string());
            }
            RegressionSeverity::Minor => {
                recommendations.push("Minor performance regression detected".to_string());
                recommendations
                    .push("Monitor trend and investigate if degradation continues".to_string());
            }
        }

        // Add specific recommendations based on metric type
        if result.current_performance.memory_usage_bytes
            > result.baseline_performance.memory_baseline * 2
        {
            recommendations.push("High memory usage detected - check for memory leaks".to_string());
        }

        if result.current_performance.cpu_usage_percent
            > result.baseline_performance.cpu_baseline * 1.5
        {
            recommendations
                .push("High CPU usage detected - check for CPU-intensive operations".to_string());
        }

        if result.current_performance.error_rate > 0.1 {
            recommendations.push("Elevated error rate detected - check system logs".to_string());
        }

        recommendations
    }

    /// Clean up old metrics beyond retention period
    fn cleanup_old_metrics(&self, storage: &mut MetricsStorage) {
        let retention_duration = Duration::from_secs(self.config.baseline_window_hours * 3600 * 7); // Keep 7x baseline window
        let cutoff_time = SystemTime::now() - retention_duration;

        // Remove old metrics
        storage
            .metrics
            .retain(|&timestamp, _| timestamp > cutoff_time);

        // Cleanup recent detections
        let detection_retention = Duration::from_secs(self.config.baseline_window_hours * 3600 * 3); // Keep 3x baseline window
        let detection_cutoff = SystemTime::now() - detection_retention;

        storage
            .recent_detections
            .retain(|detection| detection.detection_timestamp > detection_cutoff);

        // Enforce max storage limits
        if storage.metrics.len() > self.config.max_stored_metrics {
            let excess = storage.metrics.len() - self.config.max_stored_metrics;
            let keys_to_remove: Vec<_> = storage.metrics.keys().take(excess).cloned().collect();
            for key in keys_to_remove {
                storage.metrics.remove(&key);
            }
        }
    }

    /// Calculate overall system health score (0.0 to 1.0)
    pub fn calculate_health_score(&self) -> Result<f64> {
        if let Ok(storage) = self.metrics_storage.read() {
            let total_operations = storage.operation_stats.len() as f64;
            if total_operations == 0.0 {
                return Ok(1.0); // No data means no problems
            }

            let mut health_sum = 0.0;
            for stats in storage.operation_stats.values() {
                let op_health = match stats.trend_direction {
                    TrendDirection::Improving => 1.0,
                    TrendDirection::Stable => 0.8,
                    TrendDirection::Degrading => 0.3,
                    TrendDirection::Unknown => 0.6,
                };

                // Reduce health if recent regression
                let regression_penalty = if let Some(last_regression) = stats.last_regression {
                    let time_since = SystemTime::now()
                        .duration_since(last_regression)
                        .unwrap_or_default()
                        .as_secs() as f64;
                    let hours_since = time_since / 3600.0;

                    if hours_since < 1.0 {
                        0.5 // Recent regression, significant penalty
                    } else if hours_since < 24.0 {
                        0.2 // Regression within 24h, moderate penalty
                    } else {
                        0.0 // Old regression, no penalty
                    }
                } else {
                    0.0
                };

                let adjusted_health: f64 = op_health - regression_penalty;
                health_sum += adjusted_health.max(0.0);
            }

            Ok((health_sum / total_operations).min(1.0))
        } else {
            Ok(0.5) // Unable to assess, neutral score
        }
    }
}

/// Comprehensive performance report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub generated_at: SystemTime,
    pub total_operations_tracked: usize,
    pub active_regressions: usize,
    pub recent_detections: Vec<RegressionDetectionResult>,
    pub operation_summaries: HashMap<String, OperationSummary>,
    pub system_health_score: f64,
}

/// Summary of performance for a specific operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationSummary {
    pub operation_type: String,
    pub total_samples: usize,
    pub recent_average_micros: f64,
    pub trend_direction: TrendDirection,
    pub last_regression: Option<SystemTime>,
    pub baseline: Option<PerformanceBaseline>,
    pub recent_performance: Option<PerformanceMetric>,
}

// Implement Serialize/Deserialize for TrendDirection
impl Serialize for TrendDirection {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            TrendDirection::Improving => serializer.serialize_str("improving"),
            TrendDirection::Stable => serializer.serialize_str("stable"),
            TrendDirection::Degrading => serializer.serialize_str("degrading"),
            TrendDirection::Unknown => serializer.serialize_str("unknown"),
        }
    }
}

impl<'de> Deserialize<'de> for TrendDirection {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "improving" => Ok(TrendDirection::Improving),
            "stable" => Ok(TrendDirection::Stable),
            "degrading" => Ok(TrendDirection::Degrading),
            "unknown" => Ok(TrendDirection::Unknown),
            _ => Ok(TrendDirection::Unknown),
        }
    }
}

/// Integration with distributed tracing system
pub fn integrate_with_tracing() -> Result<()> {
    // This function would set up automatic performance metric collection
    // from the distributed tracing system
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regression_detector_creation() {
        let config = RegressionDetectorConfig::default();
        let detector = PerformanceRegressionDetector::new(config);

        let baselines = detector.get_baselines().unwrap();
        assert!(baselines.is_empty());
    }

    #[test]
    fn test_metric_recording() {
        let config = RegressionDetectorConfig::default();
        let detector = PerformanceRegressionDetector::new(config);

        let metric = PerformanceMetric {
            timestamp: SystemTime::now(),
            operation_type: "test_operation".to_string(),
            duration_micros: 1000,
            throughput_ops_per_sec: 100.0,
            memory_usage_bytes: 1024,
            cpu_usage_percent: 5.0,
            error_rate: 0.0,
            additional_metrics: HashMap::new(),
            trace_id: None,
            span_id: None,
        };

        assert!(detector.record_metric(metric).is_ok());
    }

    #[test]
    fn test_health_score_calculation() {
        let config = RegressionDetectorConfig::default();
        let detector = PerformanceRegressionDetector::new(config);

        // With no data, should return high health score
        let score = detector.calculate_health_score().unwrap();
        assert_eq!(score, 1.0);
    }

    #[test]
    fn test_performance_report_generation() {
        let config = RegressionDetectorConfig::default();
        let detector = PerformanceRegressionDetector::new(config);

        let report = detector.generate_performance_report().unwrap();
        assert_eq!(report.total_operations_tracked, 0);
        assert_eq!(report.active_regressions, 0);
        assert_eq!(report.system_health_score, 1.0);
    }
}

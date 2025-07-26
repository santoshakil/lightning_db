//! Performance Trend Analysis and Forecasting
//!
//! Provides trend analysis, forecasting, and pattern detection capabilities
//! for performance metrics over time.

use super::{PerformanceMetric, RegressionDetectorConfig, MetricsStorage};
use crate::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, Duration};
use serde::{Serialize, Deserialize};

/// Trend analyzer for performance metrics
pub struct TrendAnalyzer {
    metrics_storage: Arc<RwLock<MetricsStorage>>,
    config: RegressionDetectorConfig,
    trend_cache: Arc<RwLock<HashMap<String, CachedTrend>>>,
}

/// Cached trend analysis result
#[derive(Debug, Clone)]
struct CachedTrend {
    trend: TrendAnalysisResult,
    computed_at: SystemTime,
    valid_until: SystemTime,
}

/// Trend analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAnalysisResult {
    pub operation_type: String,
    pub time_window_hours: u64,
    pub sample_count: usize,
    pub trend_direction: TrendDirection,
    pub trend_strength: f64, // 0.0 to 1.0
    pub slope: f64, // Performance change per hour
    pub correlation_coefficient: f64,
    pub seasonal_patterns: Vec<SeasonalPattern>,
    pub anomalies: Vec<PerformanceAnomaly>,
    pub forecast: Option<PerformanceForecast>,
    pub confidence_score: f64,
}

/// Trend direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TrendDirection {
    StronglyImproving,  // > 10% improvement
    Improving,          // 2-10% improvement
    Stable,             // < 2% change
    Degrading,          // 2-10% degradation
    StronglyDegrading,  // > 10% degradation
}

/// Seasonal pattern detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeasonalPattern {
    pub pattern_type: SeasonalityType,
    pub period_hours: f64,
    pub amplitude: f64,
    pub confidence: f64,
    pub description: String,
}

/// Types of seasonality
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SeasonalityType {
    Hourly,    // Within-day patterns
    Daily,     // Day-of-week patterns
    Weekly,    // Week-of-month patterns
    Custom,    // Custom detected periods
}

/// Performance anomaly detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAnomaly {
    pub timestamp: SystemTime,
    pub actual_value: f64,
    pub expected_value: f64,
    pub anomaly_score: f64,
    pub anomaly_type: AnomalyType,
    pub description: String,
}

/// Types of anomalies
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum AnomalyType {
    Spike,        // Sudden performance degradation
    Drop,         // Sudden performance improvement
    Drift,        // Gradual long-term change
    Oscillation,  // Unusual oscillatory behavior
}

/// Performance forecast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceForecast {
    pub forecast_horizon_hours: u64,
    pub predicted_values: Vec<ForecastPoint>,
    pub confidence_intervals: Vec<ConfidenceInterval>,
    pub forecast_accuracy: f64,
    pub method_used: ForecastMethod,
}

/// Single forecast point
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForecastPoint {
    pub timestamp: SystemTime,
    pub predicted_value: f64,
    pub confidence: f64,
}

/// Confidence interval for forecast
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfidenceInterval {
    pub timestamp: SystemTime,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub confidence_level: f64,
}

/// Forecasting methods
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ForecastMethod {
    LinearRegression,
    ExponentialSmoothing,
    MovingAverage,
    SeasonalDecomposition,
    ARIMA,
}

impl TrendAnalyzer {
    /// Create a new trend analyzer
    pub fn new(
        metrics_storage: Arc<RwLock<MetricsStorage>>,
        config: RegressionDetectorConfig,
    ) -> Self {
        Self {
            metrics_storage,
            config,
            trend_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get performance trends for an operation
    pub fn get_trends(&self, operation_type: &str, hours: u64) -> Result<Vec<PerformanceMetric>> {
        let cutoff_time = SystemTime::now() - Duration::from_secs(hours * 3600);
        
        if let Ok(storage) = self.metrics_storage.read() {
            let mut matching_metrics = Vec::new();
            
            for (timestamp, metrics_batch) in &storage.metrics {
                if *timestamp > cutoff_time {
                    for metric in metrics_batch {
                        if metric.operation_type == operation_type {
                            matching_metrics.push(metric.clone());
                        }
                    }
                }
            }
            
            // Sort by timestamp
            matching_metrics.sort_by_key(|m| m.timestamp);
            Ok(matching_metrics)
        } else {
            Ok(Vec::new())
        }
    }

    /// Analyze trends for an operation type
    pub fn analyze_trends(&self, operation_type: &str, hours: u64) -> Result<TrendAnalysisResult> {
        // Check cache first
        if let Some(cached) = self.get_cached_trend(operation_type, hours)? {
            return Ok(cached);
        }

        let metrics = self.get_trends(operation_type, hours)?;
        
        if metrics.is_empty() {
            return Ok(self.create_empty_result(operation_type, hours));
        }

        let trend_result = self.compute_trend_analysis(operation_type, hours, &metrics)?;
        
        // Cache the result
        self.cache_trend_result(operation_type, hours, &trend_result)?;
        
        Ok(trend_result)
    }

    /// Compute comprehensive trend analysis
    fn compute_trend_analysis(
        &self,
        operation_type: &str,
        hours: u64,
        metrics: &[PerformanceMetric],
    ) -> Result<TrendAnalysisResult> {
        // Basic trend analysis
        let (slope, correlation) = self.calculate_linear_trend(metrics)?;
        let trend_direction = self.determine_trend_direction(slope, hours);
        let trend_strength = correlation.abs();

        // Seasonal pattern detection
        let seasonal_patterns = self.detect_seasonal_patterns(metrics)?;

        // Anomaly detection
        let anomalies = self.detect_anomalies(metrics)?;

        // Forecasting
        let forecast = if metrics.len() >= 10 {
            Some(self.generate_forecast(metrics, 24)?) // 24-hour forecast
        } else {
            None
        };

        // Calculate confidence score
        let confidence_score = self.calculate_confidence_score(metrics, correlation, &seasonal_patterns);

        Ok(TrendAnalysisResult {
            operation_type: operation_type.to_string(),
            time_window_hours: hours,
            sample_count: metrics.len(),
            trend_direction,
            trend_strength,
            slope,
            correlation_coefficient: correlation,
            seasonal_patterns,
            anomalies,
            forecast,
            confidence_score,
        })
    }

    /// Calculate linear trend (slope and correlation)
    fn calculate_linear_trend(&self, metrics: &[PerformanceMetric]) -> Result<(f64, f64)> {
        if metrics.len() < 2 {
            return Ok((0.0, 0.0));
        }

        // Convert timestamps to hours since first measurement
        let first_time = metrics[0].timestamp;
        let mut x_values = Vec::new();
        let mut y_values = Vec::new();

        for metric in metrics {
            let hours_since_start = metric.timestamp
                .duration_since(first_time)
                .unwrap_or_default()
                .as_secs_f64() / 3600.0;
            
            x_values.push(hours_since_start);
            y_values.push(metric.duration_micros as f64);
        }

        let n = x_values.len() as f64;
        let sum_x: f64 = x_values.iter().sum();
        let sum_y: f64 = y_values.iter().sum();
        let sum_xy: f64 = x_values.iter().zip(&y_values).map(|(x, y)| x * y).sum();
        let sum_x2: f64 = x_values.iter().map(|x| x * x).sum();
        let sum_y2: f64 = y_values.iter().map(|y| y * y).sum();

        // Calculate slope (beta)
        let slope = if n * sum_x2 - sum_x * sum_x != 0.0 {
            (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        } else {
            0.0
        };

        // Calculate correlation coefficient
        let correlation = if n * sum_x2 - sum_x * sum_x != 0.0 && n * sum_y2 - sum_y * sum_y != 0.0 {
            (n * sum_xy - sum_x * sum_y) / 
            ((n * sum_x2 - sum_x * sum_x).sqrt() * (n * sum_y2 - sum_y * sum_y).sqrt())
        } else {
            0.0
        };

        Ok((slope, correlation))
    }

    /// Determine trend direction from slope
    fn determine_trend_direction(&self, slope: f64, time_window_hours: u64) -> TrendDirection {
        // Calculate percentage change over time window
        let avg_duration = 1000.0; // Assume 1ms baseline for calculation
        let total_change = slope * time_window_hours as f64;
        let percentage_change = total_change / avg_duration;

        if percentage_change > 0.1 {
            TrendDirection::StronglyDegrading
        } else if percentage_change > 0.02 {
            TrendDirection::Degrading
        } else if percentage_change < -0.1 {
            TrendDirection::StronglyImproving
        } else if percentage_change < -0.02 {
            TrendDirection::Improving
        } else {
            TrendDirection::Stable
        }
    }

    /// Detect seasonal patterns using simple period detection
    fn detect_seasonal_patterns(&self, metrics: &[PerformanceMetric]) -> Result<Vec<SeasonalPattern>> {
        let mut patterns = Vec::new();

        if metrics.len() < 24 { // Need at least 24 data points
            return Ok(patterns);
        }

        // Extract hourly patterns (if data spans multiple days)
        if let Some(hourly_pattern) = self.detect_hourly_pattern(metrics)? {
            patterns.push(hourly_pattern);
        }

        // Extract daily patterns (if data spans multiple weeks)
        if let Some(daily_pattern) = self.detect_daily_pattern(metrics)? {
            patterns.push(daily_pattern);
        }

        Ok(patterns)
    }

    /// Detect hourly patterns within days
    fn detect_hourly_pattern(&self, metrics: &[PerformanceMetric]) -> Result<Option<SeasonalPattern>> {
        // Group metrics by hour of day
        let mut hourly_averages = vec![0.0; 24];
        let mut hourly_counts = vec![0; 24];

        for metric in metrics {
            // Simple hour extraction (this would use proper datetime handling in practice)
            let hours_since_epoch = metric.timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() / 3600;
            let hour_of_day = (hours_since_epoch % 24) as usize;

            hourly_averages[hour_of_day] += metric.duration_micros as f64;
            hourly_counts[hour_of_day] += 1;
        }

        // Calculate averages
        for i in 0..24 {
            if hourly_counts[i] > 0 {
                hourly_averages[i] /= hourly_counts[i] as f64;
            }
        }

        // Calculate variance to determine if there's a pattern
        let mean: f64 = hourly_averages.iter().sum::<f64>() / 24.0;
        let variance: f64 = hourly_averages.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / 24.0;

        let std_dev = variance.sqrt();
        let coefficient_of_variation = if mean > 0.0 { std_dev / mean } else { 0.0 };

        // If coefficient of variation is significant, we have a pattern
        if coefficient_of_variation > 0.1 {
            let max_val = hourly_averages.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            let min_val = hourly_averages.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            let amplitude = max_val - min_val;

            return Ok(Some(SeasonalPattern {
                pattern_type: SeasonalityType::Hourly,
                period_hours: 24.0,
                amplitude,
                confidence: coefficient_of_variation.min(1.0),
                description: format!("Daily hourly pattern with {:.1}% variation", coefficient_of_variation * 100.0),
            }));
        }

        Ok(None)
    }

    /// Detect daily patterns within weeks
    fn detect_daily_pattern(&self, metrics: &[PerformanceMetric]) -> Result<Option<SeasonalPattern>> {
        // Group metrics by day of week
        let mut daily_averages = vec![0.0; 7];
        let mut daily_counts = vec![0; 7];

        for metric in metrics {
            // Simple day-of-week calculation
            let days_since_epoch = metric.timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() / (24 * 3600);
            let day_of_week = (days_since_epoch % 7) as usize;

            daily_averages[day_of_week] += metric.duration_micros as f64;
            daily_counts[day_of_week] += 1;
        }

        // Calculate averages
        for i in 0..7 {
            if daily_counts[i] > 0 {
                daily_averages[i] /= daily_counts[i] as f64;
            }
        }

        // Calculate variance
        let mean: f64 = daily_averages.iter().sum::<f64>() / 7.0;
        let variance: f64 = daily_averages.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / 7.0;

        let std_dev = variance.sqrt();
        let coefficient_of_variation = if mean > 0.0 { std_dev / mean } else { 0.0 };

        if coefficient_of_variation > 0.1 {
            let max_val = daily_averages.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            let min_val = daily_averages.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
            let amplitude = max_val - min_val;

            return Ok(Some(SeasonalPattern {
                pattern_type: SeasonalityType::Daily,
                period_hours: 24.0 * 7.0,
                amplitude,
                confidence: coefficient_of_variation.min(1.0),
                description: format!("Weekly daily pattern with {:.1}% variation", coefficient_of_variation * 100.0),
            }));
        }

        Ok(None)
    }

    /// Detect performance anomalies using statistical methods
    fn detect_anomalies(&self, metrics: &[PerformanceMetric]) -> Result<Vec<PerformanceAnomaly>> {
        let mut anomalies = Vec::new();

        if metrics.len() < 10 {
            return Ok(anomalies);
        }

        // Calculate rolling statistics
        let window_size = 10.min(metrics.len() / 3);
        
        for i in window_size..metrics.len() {
            let window_start = i.saturating_sub(window_size);
            let window_metrics = &metrics[window_start..i];
            
            // Calculate window statistics
            let window_values: Vec<f64> = window_metrics.iter()
                .map(|m| m.duration_micros as f64)
                .collect();
            
            let mean = window_values.iter().sum::<f64>() / window_values.len() as f64;
            let variance = window_values.iter()
                .map(|&x| (x - mean).powi(2))
                .sum::<f64>() / window_values.len() as f64;
            let std_dev = variance.sqrt();

            let current_value = metrics[i].duration_micros as f64;
            let z_score = if std_dev > 0.0 {
                (current_value - mean) / std_dev
            } else {
                0.0
            };

            // Detect anomalies (z-score > 2.5 or < -2.5)
            if z_score.abs() > 2.5 {
                let anomaly_type = if z_score > 0.0 {
                    AnomalyType::Spike
                } else {
                    AnomalyType::Drop
                };

                anomalies.push(PerformanceAnomaly {
                    timestamp: metrics[i].timestamp,
                    actual_value: current_value,
                    expected_value: mean,
                    anomaly_score: z_score.abs(),
                    anomaly_type,
                    description: format!(
                        "{} anomaly: {:.1}ms vs expected {:.1}ms (z-score: {:.2})",
                        match anomaly_type {
                            AnomalyType::Spike => "Performance spike",
                            AnomalyType::Drop => "Performance drop",
                            _ => "Performance anomaly",
                        },
                        current_value / 1000.0,
                        mean / 1000.0,
                        z_score
                    ),
                });
            }
        }

        Ok(anomalies)
    }

    /// Generate performance forecast
    fn generate_forecast(&self, metrics: &[PerformanceMetric], forecast_hours: u64) -> Result<PerformanceForecast> {
        // Use linear regression for simple forecasting
        let (slope, correlation) = self.calculate_linear_trend(metrics)?;
        
        let last_metric = metrics.last().unwrap();
        let last_value = last_metric.duration_micros as f64;
        let last_time = last_metric.timestamp;

        let mut predicted_values = Vec::new();
        let mut confidence_intervals = Vec::new();

        // Generate hourly predictions
        for hour in 1..=forecast_hours {
            let prediction_time = last_time + Duration::from_secs(hour * 3600);
            let predicted_value = last_value + slope * hour as f64;
            
            // Simple confidence calculation based on correlation
            let confidence = correlation.abs();
            
            predicted_values.push(ForecastPoint {
                timestamp: prediction_time,
                predicted_value,
                confidence,
            });

            // Calculate confidence interval (simplified)
            let error_margin = predicted_value * (1.0 - confidence) * 0.2; // 20% max error
            confidence_intervals.push(ConfidenceInterval {
                timestamp: prediction_time,
                lower_bound: predicted_value - error_margin,
                upper_bound: predicted_value + error_margin,
                confidence_level: 0.95,
            });
        }

        // Calculate forecast accuracy based on correlation
        let forecast_accuracy = correlation.abs();

        Ok(PerformanceForecast {
            forecast_horizon_hours: forecast_hours,
            predicted_values,
            confidence_intervals,
            forecast_accuracy,
            method_used: ForecastMethod::LinearRegression,
        })
    }

    /// Calculate overall confidence score for trend analysis
    fn calculate_confidence_score(
        &self,
        metrics: &[PerformanceMetric],
        correlation: f64,
        seasonal_patterns: &[SeasonalPattern],
    ) -> f64 {
        // Base confidence on sample size
        let sample_factor = if metrics.len() >= 100 {
            1.0
        } else if metrics.len() >= 50 {
            0.8
        } else if metrics.len() >= 20 {
            0.6
        } else {
            0.4
        };

        // Correlation factor
        let correlation_factor = correlation.abs();

        // Seasonal pattern factor
        let pattern_factor = if !seasonal_patterns.is_empty() {
            seasonal_patterns.iter()
                .map(|p| p.confidence)
                .sum::<f64>() / seasonal_patterns.len() as f64
        } else {
            0.5
        };

        // Weighted combination
        (sample_factor * 0.4 + correlation_factor * 0.4 + pattern_factor * 0.2).min(1.0)
    }

    /// Get cached trend result if available and valid
    fn get_cached_trend(&self, operation_type: &str, hours: u64) -> Result<Option<TrendAnalysisResult>> {
        if let Ok(cache) = self.trend_cache.read() {
            let cache_key = format!("{}_{}", operation_type, hours);
            if let Some(cached) = cache.get(&cache_key) {
                if SystemTime::now() < cached.valid_until {
                    return Ok(Some(cached.trend.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Cache trend analysis result
    fn cache_trend_result(&self, operation_type: &str, hours: u64, result: &TrendAnalysisResult) -> Result<()> {
        if let Ok(mut cache) = self.trend_cache.write() {
            let cache_key = format!("{}_{}", operation_type, hours);
            let cache_duration = Duration::from_secs(self.config.baseline_window_hours * 3600 / 4); // Quarter of baseline window
            
            cache.insert(cache_key, CachedTrend {
                trend: result.clone(),
                computed_at: SystemTime::now(),
                valid_until: SystemTime::now() + cache_duration,
            });

            // Cleanup old cache entries
            let cutoff = SystemTime::now() - Duration::from_secs(24 * 3600);
            cache.retain(|_, cached| cached.computed_at > cutoff);
        }
        Ok(())
    }

    /// Create empty result for operations with no data
    fn create_empty_result(&self, operation_type: &str, hours: u64) -> TrendAnalysisResult {
        TrendAnalysisResult {
            operation_type: operation_type.to_string(),
            time_window_hours: hours,
            sample_count: 0,
            trend_direction: TrendDirection::Stable,
            trend_strength: 0.0,
            slope: 0.0,
            correlation_coefficient: 0.0,
            seasonal_patterns: Vec::new(),
            anomalies: Vec::new(),
            forecast: None,
            confidence_score: 0.0,
        }
    }

    /// Get trend summary for multiple operations
    pub fn get_trend_summary(&self, hours: u64) -> Result<HashMap<String, TrendAnalysisResult>> {
        let mut summary = HashMap::new();
        
        if let Ok(storage) = self.metrics_storage.read() {
            // Get all unique operation types
            let mut operation_types = std::collections::HashSet::new();
            for metrics_batch in storage.metrics.values() {
                for metric in metrics_batch {
                    operation_types.insert(metric.operation_type.clone());
                }
            }

            // Analyze trends for each operation type
            for operation_type in operation_types {
                if let Ok(trend) = self.analyze_trends(&operation_type, hours) {
                    summary.insert(operation_type, trend);
                }
            }
        }

        Ok(summary)
    }

    /// Detect performance degradation trends that might lead to regressions
    pub fn detect_degradation_trends(&self, hours: u64) -> Result<Vec<String>> {
        let trend_summary = self.get_trend_summary(hours)?;
        let mut degrading_operations = Vec::new();

        for (operation_type, trend) in trend_summary {
            if matches!(trend.trend_direction, TrendDirection::Degrading | TrendDirection::StronglyDegrading) {
                if trend.confidence_score > 0.7 && trend.trend_strength > 0.5 {
                    degrading_operations.push(operation_type);
                }
            }
        }

        Ok(degrading_operations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_test_storage_with_metrics() -> Arc<RwLock<MetricsStorage>> {
        let storage = Arc::new(RwLock::new(MetricsStorage {
            metrics: BTreeMap::new(),
            baselines: HashMap::new(),
            recent_detections: std::collections::VecDeque::new(),
            operation_stats: HashMap::new(),
        }));

        // Add test metrics with trend
        if let Ok(mut s) = storage.write() {
            let base_time = SystemTime::now() - Duration::from_secs(24 * 3600); // 24 hours ago
            
            for i in 0..48 { // 48 data points over 24 hours
                let timestamp = base_time + Duration::from_secs(i * 1800); // Every 30 minutes
                let duration = 1000 + (i * 5); // Increasing trend (degradation)
                
                let metric = PerformanceMetric {
                    timestamp,
                    operation_type: "test_op".to_string(),
                    duration_micros: duration,
                    throughput_ops_per_sec: 100.0,
                    memory_usage_bytes: 1024,
                    cpu_usage_percent: 5.0,
                    error_rate: 0.0,
                    additional_metrics: HashMap::new(),
                    trace_id: None,
                    span_id: None,
                };

                s.metrics.entry(timestamp).or_insert_with(Vec::new).push(metric);
            }
        }

        storage
    }

    #[test]
    fn test_trend_analyzer_creation() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let _analyzer = TrendAnalyzer::new(storage, config);
    }

    #[test]
    fn test_get_trends() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let trends = analyzer.get_trends("test_op", 24).unwrap();
        assert!(!trends.is_empty());
        assert_eq!(trends[0].operation_type, "test_op");
    }

    #[test]
    fn test_linear_trend_calculation() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let trends = analyzer.get_trends("test_op", 24).unwrap();
        let (slope, correlation) = analyzer.calculate_linear_trend(&trends).unwrap();

        assert!(slope > 0.0); // Should detect increasing trend
        assert!(correlation > 0.5); // Should have reasonable correlation
    }

    #[test]
    fn test_trend_direction_determination() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let direction = analyzer.determine_trend_direction(10.0, 24); // 10 micros per hour over 24 hours
        assert!(matches!(direction, TrendDirection::Degrading | TrendDirection::StronglyDegrading));

        let stable_direction = analyzer.determine_trend_direction(0.1, 24); // Very small change
        assert_eq!(stable_direction, TrendDirection::Stable);
    }

    #[test]
    fn test_trend_analysis() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let analysis = analyzer.analyze_trends("test_op", 24).unwrap();
        
        assert_eq!(analysis.operation_type, "test_op");
        assert!(analysis.sample_count > 0);
        assert!(matches!(analysis.trend_direction, TrendDirection::Degrading | TrendDirection::StronglyDegrading));
        assert!(analysis.confidence_score > 0.0);
    }

    #[test]
    fn test_empty_metrics_handling() {
        let storage = Arc::new(RwLock::new(MetricsStorage {
            metrics: BTreeMap::new(),
            baselines: HashMap::new(),
            recent_detections: std::collections::VecDeque::new(),
            operation_stats: HashMap::new(),
        }));

        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let analysis = analyzer.analyze_trends("nonexistent_op", 24).unwrap();
        assert_eq!(analysis.sample_count, 0);
        assert_eq!(analysis.trend_direction, TrendDirection::Stable);
    }

    #[test]
    fn test_anomaly_detection() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        // Create metrics with an anomaly
        let mut metrics = Vec::new();
        for i in 0..20 {
            let duration = if i == 10 { 5000 } else { 1000 }; // Spike at index 10
            metrics.push(PerformanceMetric {
                timestamp: SystemTime::now() + Duration::from_secs(i * 60),
                operation_type: "test_op".to_string(),
                duration_micros: duration,
                throughput_ops_per_sec: 100.0,
                memory_usage_bytes: 1024,
                cpu_usage_percent: 5.0,
                error_rate: 0.0,
                additional_metrics: HashMap::new(),
                trace_id: None,
                span_id: None,
            });
        }

        let anomalies = analyzer.detect_anomalies(&metrics).unwrap();
        assert!(!anomalies.is_empty());
    }

    #[test]
    fn test_degradation_trend_detection() {
        let storage = create_test_storage_with_metrics();
        let config = RegressionDetectorConfig::default();
        let analyzer = TrendAnalyzer::new(storage, config);

        let degrading_ops = analyzer.detect_degradation_trends(24).unwrap();
        assert!(!degrading_ops.is_empty());
        assert!(degrading_ops.contains(&"test_op".to_string()));
    }
}
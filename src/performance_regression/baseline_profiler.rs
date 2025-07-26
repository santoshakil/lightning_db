//! Baseline Performance Profiler
//!
//! Creates and maintains performance baselines for different operation types.
//! Baselines are used as reference points for regression detection.

use super::{PerformanceMetric, PerformanceBaseline, RegressionDetectorConfig, MetricsStorage};
use crate::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, Duration};

/// Baseline profiler for creating performance reference points
pub struct BaselineProfiler {
    metrics_storage: Arc<RwLock<MetricsStorage>>,
    config: RegressionDetectorConfig,
    baseline_cache: Arc<RwLock<HashMap<String, CachedBaseline>>>,
}

/// Cached baseline with metadata
#[derive(Debug, Clone)]
struct CachedBaseline {
    baseline: PerformanceBaseline,
    last_updated: SystemTime,
    sample_count_at_creation: usize,
}

/// Baseline creation strategies
#[derive(Debug, Clone, Copy)]
pub enum BaselineStrategy {
    /// Use recent samples within a time window
    RecentWindow,
    /// Use percentile-based approach (exclude outliers)
    PercentileBased,
    /// Use adaptive approach based on data distribution
    Adaptive,
    /// Use rolling average with decay
    RollingAverage,
}

/// Baseline quality metrics
#[derive(Debug, Clone)]
pub struct BaselineQuality {
    pub confidence_score: f64,
    pub sample_size: usize,
    pub data_variance: f64,
    pub outlier_percentage: f64,
    pub temporal_stability: f64,
}

impl BaselineProfiler {
    /// Create a new baseline profiler
    pub fn new(
        metrics_storage: Arc<RwLock<MetricsStorage>>,
        config: RegressionDetectorConfig,
    ) -> Self {
        Self {
            metrics_storage,
            config,
            baseline_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Try to create a baseline for an operation type if enough samples exist
    pub fn try_create_baseline(&self, operation_type: &str) -> Result<Option<PerformanceBaseline>> {
        let samples = self.collect_recent_samples(operation_type)?;
        
        if samples.len() >= self.config.min_samples_for_analysis {
            let baseline = self.create_baseline_from_samples(operation_type, &samples, BaselineStrategy::Adaptive)?;
            
            // Store the baseline
            if let Ok(mut storage) = self.metrics_storage.write() {
                storage.baselines.insert(operation_type.to_string(), baseline.clone());
            }
            
            // Cache the baseline
            if let Ok(mut cache) = self.baseline_cache.write() {
                cache.insert(operation_type.to_string(), CachedBaseline {
                    baseline: baseline.clone(),
                    last_updated: SystemTime::now(),
                    sample_count_at_creation: samples.len(),
                });
            }
            
            Ok(Some(baseline))
        } else {
            Ok(None)
        }
    }

    /// Force update baseline for an operation type
    pub fn force_update_baseline(&self, operation_type: &str) -> Result<PerformanceBaseline> {
        let samples = self.collect_recent_samples(operation_type)?;
        
        if samples.is_empty() {
            return Err(crate::Error::Generic(
                format!("No samples available for operation type: {}", operation_type)
            ));
        }

        let baseline = self.create_baseline_from_samples(
            operation_type, 
            &samples, 
            BaselineStrategy::Adaptive
        )?;
        
        // Store the baseline
        if let Ok(mut storage) = self.metrics_storage.write() {
            storage.baselines.insert(operation_type.to_string(), baseline.clone());
        }
        
        // Update cache
        if let Ok(mut cache) = self.baseline_cache.write() {
            cache.insert(operation_type.to_string(), CachedBaseline {
                baseline: baseline.clone(),
                last_updated: SystemTime::now(),
                sample_count_at_creation: samples.len(),
            });
        }
        
        Ok(baseline)
    }

    /// Create baseline from samples using specified strategy
    pub fn create_baseline_from_samples(
        &self,
        operation_type: &str,
        samples: &[PerformanceMetric],
        strategy: BaselineStrategy,
    ) -> Result<PerformanceBaseline> {
        if samples.is_empty() {
            return Err(crate::Error::Generic("No samples provided for baseline creation".to_string()));
        }

        match strategy {
            BaselineStrategy::RecentWindow => self.create_recent_window_baseline(operation_type, samples),
            BaselineStrategy::PercentileBased => self.create_percentile_baseline(operation_type, samples),
            BaselineStrategy::Adaptive => self.create_adaptive_baseline(operation_type, samples),
            BaselineStrategy::RollingAverage => self.create_rolling_average_baseline(operation_type, samples),
        }
    }

    /// Create baseline using recent time window
    fn create_recent_window_baseline(
        &self,
        operation_type: &str,
        samples: &[PerformanceMetric],
    ) -> Result<PerformanceBaseline> {
        let window_duration = Duration::from_secs(self.config.baseline_window_hours * 3600);
        let cutoff_time = SystemTime::now() - window_duration;
        
        let recent_samples: Vec<_> = samples.iter()
            .filter(|s| s.timestamp > cutoff_time)
            .collect();
            
        if recent_samples.is_empty() {
            return Err(crate::Error::Generic("No recent samples in window".to_string()));
        }

        self.calculate_baseline_statistics(operation_type, &recent_samples)
    }

    /// Create baseline using percentile-based approach (removes outliers)
    fn create_percentile_baseline(
        &self,
        operation_type: &str,
        samples: &[PerformanceMetric],
    ) -> Result<PerformanceBaseline> {
        let mut durations: Vec<u64> = samples.iter()
            .map(|s| s.duration_micros)
            .collect();
        durations.sort_unstable();

        // Remove outliers (bottom 5% and top 5%)
        let start_idx = (durations.len() as f64 * 0.05) as usize;
        let end_idx = (durations.len() as f64 * 0.95) as usize;
        
        if end_idx <= start_idx {
            return self.calculate_baseline_statistics(operation_type, &samples.iter().collect::<Vec<_>>());
        }

        let filtered_durations = &durations[start_idx..end_idx];
        let filtered_samples: Vec<_> = samples.iter()
            .filter(|s| {
                let duration = s.duration_micros;
                duration >= filtered_durations[0] && duration <= filtered_durations[filtered_durations.len() - 1]
            })
            .collect();

        self.calculate_baseline_statistics(operation_type, &filtered_samples)
    }

    /// Create adaptive baseline based on data distribution
    fn create_adaptive_baseline(
        &self,
        operation_type: &str,
        samples: &[PerformanceMetric],
    ) -> Result<PerformanceBaseline> {
        // Analyze data distribution to choose best strategy
        let quality = self.analyze_data_quality(samples);
        
        let strategy = if quality.outlier_percentage > 0.15 {
            // High outlier percentage, use percentile-based
            BaselineStrategy::PercentileBased
        } else if quality.temporal_stability < 0.7 {
            // Low temporal stability, use recent window
            BaselineStrategy::RecentWindow
        } else {
            // Stable data, use rolling average
            BaselineStrategy::RollingAverage
        };

        match strategy {
            BaselineStrategy::Adaptive => unreachable!(), // Prevent infinite recursion
            _ => self.create_baseline_from_samples(operation_type, samples, strategy),
        }
    }

    /// Create baseline using rolling average with exponential decay
    fn create_rolling_average_baseline(
        &self,
        operation_type: &str,
        samples: &[PerformanceMetric],
    ) -> Result<PerformanceBaseline> {
        // Sort samples by timestamp
        let mut sorted_samples = samples.to_vec();
        sorted_samples.sort_by_key(|s| s.timestamp);

        // Apply exponential decay (more recent samples have higher weight)
        let decay_factor: f64 = 0.95;
        let mut weighted_sum = 0.0;
        let mut total_weight = 0.0;
        let mut weighted_memory_sum = 0.0;
        let mut weighted_cpu_sum = 0.0;

        for (i, sample) in sorted_samples.iter().enumerate() {
            let weight = decay_factor.powi(sorted_samples.len() as i32 - i as i32 - 1);
            weighted_sum += sample.duration_micros as f64 * weight;
            weighted_memory_sum += sample.memory_usage_bytes as f64 * weight;
            weighted_cpu_sum += sample.cpu_usage_percent * weight;
            total_weight += weight;
        }

        if total_weight == 0.0 {
            return self.calculate_baseline_statistics(operation_type, &samples.iter().collect::<Vec<_>>());
        }

        let weighted_mean = weighted_sum / total_weight;
        let weighted_memory_mean = weighted_memory_sum / total_weight;
        let weighted_cpu_mean = weighted_cpu_sum / total_weight;

        // Calculate weighted variance
        let mut weighted_variance_sum = 0.0;
        let mut variance_weight_sum = 0.0;

        for (i, sample) in sorted_samples.iter().enumerate() {
            let weight = decay_factor.powi(sorted_samples.len() as i32 - i as i32 - 1);
            let diff = sample.duration_micros as f64 - weighted_mean;
            weighted_variance_sum += diff * diff * weight;
            variance_weight_sum += weight;
        }

        let weighted_variance = if variance_weight_sum > 0.0 {
            weighted_variance_sum / variance_weight_sum
        } else {
            0.0
        };

        let std_deviation = weighted_variance.sqrt();

        // Calculate percentiles from all samples
        let mut durations: Vec<u64> = sorted_samples.iter().map(|s| s.duration_micros).collect();
        durations.sort_unstable();

        let p50 = self.calculate_percentile(&durations, 50.0);
        let p95 = self.calculate_percentile(&durations, 95.0);
        let p99 = self.calculate_percentile(&durations, 99.0);

        let confidence_interval = self.calculate_confidence_interval(weighted_mean, std_deviation, sorted_samples.len());

        Ok(PerformanceBaseline {
            operation_type: operation_type.to_string(),
            created_at: SystemTime::now(),
            sample_count: sorted_samples.len(),
            mean_duration_micros: weighted_mean,
            std_deviation_micros: std_deviation,
            p50_duration_micros: p50,
            p95_duration_micros: p95,
            p99_duration_micros: p99,
            mean_throughput: self.calculate_mean_throughput(&sorted_samples),
            memory_baseline: weighted_memory_mean as u64,
            cpu_baseline: weighted_cpu_mean,
            confidence_interval,
        })
    }

    /// Calculate baseline statistics from sample references
    fn calculate_baseline_statistics(
        &self,
        operation_type: &str,
        samples: &[&PerformanceMetric],
    ) -> Result<PerformanceBaseline> {
        if samples.is_empty() {
            return Err(crate::Error::Generic("No samples for baseline calculation".to_string()));
        }

        // Calculate duration statistics
        let durations: Vec<u64> = samples.iter().map(|s| s.duration_micros).collect();
        let mean_duration = durations.iter().sum::<u64>() as f64 / durations.len() as f64;
        
        let variance = durations.iter()
            .map(|&d| {
                let diff = d as f64 - mean_duration;
                diff * diff
            })
            .sum::<f64>() / durations.len() as f64;
        let std_deviation = variance.sqrt();

        // Calculate percentiles
        let mut sorted_durations = durations.clone();
        sorted_durations.sort_unstable();
        
        let p50 = self.calculate_percentile(&sorted_durations, 50.0);
        let p95 = self.calculate_percentile(&sorted_durations, 95.0);
        let p99 = self.calculate_percentile(&sorted_durations, 99.0);

        // Calculate other metrics
        let mean_throughput = samples.iter()
            .map(|s| s.throughput_ops_per_sec)
            .sum::<f64>() / samples.len() as f64;

        let memory_baseline = samples.iter()
            .map(|s| s.memory_usage_bytes)
            .sum::<u64>() / samples.len() as u64;

        let cpu_baseline = samples.iter()
            .map(|s| s.cpu_usage_percent)
            .sum::<f64>() / samples.len() as f64;

        let confidence_interval = self.calculate_confidence_interval(mean_duration, std_deviation, samples.len());

        Ok(PerformanceBaseline {
            operation_type: operation_type.to_string(),
            created_at: SystemTime::now(),
            sample_count: samples.len(),
            mean_duration_micros: mean_duration,
            std_deviation_micros: std_deviation,
            p50_duration_micros: p50,
            p95_duration_micros: p95,
            p99_duration_micros: p99,
            mean_throughput,
            memory_baseline,
            cpu_baseline,
            confidence_interval,
        })
    }

    /// Calculate percentile from sorted data
    fn calculate_percentile(&self, sorted_data: &[u64], percentile: f64) -> u64 {
        if sorted_data.is_empty() {
            return 0;
        }

        let index = (percentile / 100.0) * (sorted_data.len() - 1) as f64;
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            sorted_data[lower_index]
        } else {
            let lower_value = sorted_data[lower_index] as f64;
            let upper_value = sorted_data[upper_index] as f64;
            let fraction = index - lower_index as f64;
            (lower_value + fraction * (upper_value - lower_value)) as u64
        }
    }

    /// Calculate confidence interval for the mean
    fn calculate_confidence_interval(&self, mean: f64, std_dev: f64, sample_size: usize) -> (f64, f64) {
        if sample_size <= 1 {
            return (mean, mean);
        }

        // Use t-distribution for 95% confidence interval
        let t_value = match sample_size {
            n if n >= 30 => 1.96, // Normal approximation for large samples
            n if n >= 20 => 2.09,
            n if n >= 10 => 2.26,
            n if n >= 5 => 2.78,
            _ => 3.18,
        };

        let margin_of_error = t_value * (std_dev / (sample_size as f64).sqrt());
        (mean - margin_of_error, mean + margin_of_error)
    }

    /// Calculate mean throughput from samples
    fn calculate_mean_throughput(&self, samples: &[PerformanceMetric]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        samples.iter()
            .map(|s| s.throughput_ops_per_sec)
            .sum::<f64>() / samples.len() as f64
    }

    /// Collect recent samples for an operation type
    fn collect_recent_samples(&self, operation_type: &str) -> Result<Vec<PerformanceMetric>> {
        if let Ok(storage) = self.metrics_storage.read() {
            let mut samples = Vec::new();
            
            for metrics_batch in storage.metrics.values() {
                for metric in metrics_batch {
                    if metric.operation_type == operation_type {
                        samples.push(metric.clone());
                    }
                }
            }
            
            // Sort by timestamp (most recent first)
            samples.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            
            Ok(samples)
        } else {
            Ok(Vec::new())
        }
    }

    /// Analyze data quality for adaptive baseline strategy
    fn analyze_data_quality(&self, samples: &[PerformanceMetric]) -> BaselineQuality {
        if samples.is_empty() {
            return BaselineQuality {
                confidence_score: 0.0,
                sample_size: 0,
                data_variance: 0.0,
                outlier_percentage: 0.0,
                temporal_stability: 0.0,
            };
        }

        let durations: Vec<u64> = samples.iter().map(|s| s.duration_micros).collect();
        let mean = durations.iter().sum::<u64>() as f64 / durations.len() as f64;
        
        // Calculate variance
        let variance = durations.iter()
            .map(|&d| {
                let diff = d as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / durations.len() as f64;

        // Detect outliers using IQR method
        let mut sorted_durations = durations.clone();
        sorted_durations.sort_unstable();
        
        let q1 = self.calculate_percentile(&sorted_durations, 25.0) as f64;
        let q3 = self.calculate_percentile(&sorted_durations, 75.0) as f64;
        let iqr = q3 - q1;
        let lower_bound = q1 - 1.5 * iqr;
        let upper_bound = q3 + 1.5 * iqr;
        
        let outlier_count = durations.iter()
            .filter(|&&d| (d as f64) < lower_bound || (d as f64) > upper_bound)
            .count();
        
        let outlier_percentage = outlier_count as f64 / durations.len() as f64;

        // Calculate temporal stability (how stable performance is over time)
        let temporal_stability = self.calculate_temporal_stability(samples);

        // Calculate confidence score based on sample size and variance
        let confidence_score = self.calculate_confidence_score(samples.len(), variance, outlier_percentage);

        BaselineQuality {
            confidence_score,
            sample_size: samples.len(),
            data_variance: variance,
            outlier_percentage,
            temporal_stability,
        }
    }

    /// Calculate temporal stability of performance data
    fn calculate_temporal_stability(&self, samples: &[PerformanceMetric]) -> f64 {
        if samples.len() < 10 {
            return 0.5; // Neutral score for insufficient data
        }

        // Sort samples by timestamp
        let mut sorted_samples = samples.to_vec();
        sorted_samples.sort_by_key(|s| s.timestamp);

        // Divide into time windows and calculate variance between windows
        let window_size = sorted_samples.len() / 5; // 5 windows
        if window_size == 0 {
            return 0.5;
        }

        let mut window_means = Vec::new();
        for i in 0..5 {
            let start = i * window_size;
            let end = if i == 4 { sorted_samples.len() } else { (i + 1) * window_size };
            
            if start < sorted_samples.len() {
                let window_samples = &sorted_samples[start..end.min(sorted_samples.len())];
                let window_mean = window_samples.iter()
                    .map(|s| s.duration_micros as f64)
                    .sum::<f64>() / window_samples.len() as f64;
                window_means.push(window_mean);
            }
        }

        if window_means.len() <= 1 {
            return 0.5;
        }

        // Calculate coefficient of variation between windows
        let overall_mean = window_means.iter().sum::<f64>() / window_means.len() as f64;
        let variance = window_means.iter()
            .map(|&mean| {
                let diff = mean - overall_mean;
                diff * diff
            })
            .sum::<f64>() / window_means.len() as f64;

        let coefficient_of_variation = if overall_mean > 0.0 {
            variance.sqrt() / overall_mean
        } else {
            1.0
        };

        // Convert to stability score (lower CV = higher stability)
        (1.0 - coefficient_of_variation.min(1.0)).max(0.0)
    }

    /// Calculate confidence score for baseline quality
    fn calculate_confidence_score(&self, sample_size: usize, variance: f64, outlier_percentage: f64) -> f64 {
        // Sample size factor (logarithmic scaling)
        let size_factor = if sample_size >= self.config.min_samples_for_analysis {
            let log_size = (sample_size as f64).ln();
            let log_min = (self.config.min_samples_for_analysis as f64).ln();
            ((log_size - log_min) / 3.0).min(1.0) // Cap at 1.0
        } else {
            0.0
        };

        // Variance factor (lower variance = higher confidence)
        let variance_factor = 1.0 / (1.0 + variance / 1000000.0); // Normalize by 1ms variance

        // Outlier factor (fewer outliers = higher confidence)
        let outlier_factor = 1.0 - outlier_percentage;

        // Weighted combination
        (size_factor * 0.4 + variance_factor * 0.3 + outlier_factor * 0.3).min(1.0).max(0.0)
    }

    /// Check if baseline needs updating
    pub fn should_update_baseline(&self, operation_type: &str) -> Result<bool> {
        if let Ok(cache) = self.baseline_cache.read() {
            if let Some(cached) = cache.get(operation_type) {
                let age = SystemTime::now().duration_since(cached.last_updated)
                    .unwrap_or_default();
                
                let max_age = Duration::from_secs(self.config.baseline_window_hours * 3600);
                
                // Update if baseline is old or if we have significantly more samples
                let current_samples = self.collect_recent_samples(operation_type)?.len();
                let sample_growth = current_samples > cached.sample_count_at_creation * 2;
                
                Ok(age > max_age || sample_growth)
            } else {
                Ok(true) // No cached baseline, needs creation
            }
        } else {
            Ok(true)
        }
    }

    /// Get baseline quality assessment
    pub fn assess_baseline_quality(&self, operation_type: &str) -> Result<Option<BaselineQuality>> {
        let samples = self.collect_recent_samples(operation_type)?;
        if samples.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.analyze_data_quality(&samples)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_test_storage() -> Arc<RwLock<MetricsStorage>> {
        Arc::new(RwLock::new(MetricsStorage {
            metrics: BTreeMap::new(),
            baselines: HashMap::new(),
            recent_detections: std::collections::VecDeque::new(),
            operation_stats: HashMap::new(),
        }))
    }

    fn create_test_metric(operation_type: &str, duration_micros: u64, timestamp_offset_secs: i64) -> PerformanceMetric {
        let timestamp = SystemTime::now() + Duration::from_secs(timestamp_offset_secs as u64);
        PerformanceMetric {
            timestamp,
            operation_type: operation_type.to_string(),
            duration_micros,
            throughput_ops_per_sec: 100.0,
            memory_usage_bytes: 1024,
            cpu_usage_percent: 5.0,
            error_rate: 0.0,
            additional_metrics: HashMap::new(),
            trace_id: None,
            span_id: None,
        }
    }

    #[test]
    fn test_baseline_profiler_creation() {
        let storage = create_test_storage();
        let config = RegressionDetectorConfig::default();
        let profiler = BaselineProfiler::new(storage, config);
        
        let result = profiler.try_create_baseline("nonexistent_op");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_percentile_calculation() {
        let storage = create_test_storage();
        let config = RegressionDetectorConfig::default();
        let profiler = BaselineProfiler::new(storage, config);
        
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        
        assert_eq!(profiler.calculate_percentile(&data, 50.0), 5);
        assert_eq!(profiler.calculate_percentile(&data, 90.0), 9);
        assert_eq!(profiler.calculate_percentile(&data, 0.0), 1);
        assert_eq!(profiler.calculate_percentile(&data, 100.0), 10);
    }

    #[test]
    fn test_confidence_interval_calculation() {
        let storage = create_test_storage();
        let config = RegressionDetectorConfig::default();
        let profiler = BaselineProfiler::new(storage, config);
        
        let (lower, upper) = profiler.calculate_confidence_interval(100.0, 10.0, 30);
        assert!(lower < 100.0);
        assert!(upper > 100.0);
        assert!(upper - lower > 0.0);
    }

    #[test]
    fn test_data_quality_analysis() {
        let storage = create_test_storage();
        let config = RegressionDetectorConfig::default();
        let profiler = BaselineProfiler::new(storage, config);
        
        // Create samples with some outliers
        let mut samples = Vec::new();
        for i in 0..100 {
            let duration = if i < 95 { 1000 + i * 10 } else { 10000 }; // Last 5 are outliers
            samples.push(create_test_metric("test_op", duration, i as i64));
        }
        
        let quality = profiler.analyze_data_quality(&samples);
        assert_eq!(quality.sample_size, 100);
        assert!(quality.outlier_percentage > 0.0);
        assert!(quality.confidence_score > 0.0);
    }

    #[test]
    fn test_temporal_stability() {
        let storage = create_test_storage();
        let config = RegressionDetectorConfig::default();
        let profiler = BaselineProfiler::new(storage, config);
        
        // Create stable samples
        let mut stable_samples = Vec::new();
        for i in 0..50 {
            stable_samples.push(create_test_metric("stable_op", 1000, i));
        }
        
        let stable_stability = profiler.calculate_temporal_stability(&stable_samples);
        
        // Create unstable samples (performance degrading over time)
        let mut unstable_samples = Vec::new();
        for i in 0..50 {
            unstable_samples.push(create_test_metric("unstable_op", 1000 + i * 100, i as i64));
        }
        
        let unstable_stability = profiler.calculate_temporal_stability(&unstable_samples);
        
        assert!(stable_stability > unstable_stability);
    }
}
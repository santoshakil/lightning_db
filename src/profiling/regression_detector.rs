//! Performance Regression Detection Module
//!
//! Detects performance regressions by comparing current performance metrics
//! against historical baselines and identifying significant degradations.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error, debug};

/// Regression detector that analyzes performance over time
pub struct RegressionDetector {
    baselines: Arc<RwLock<HashMap<String, PerformanceBaseline>>>,
    regression_history: Arc<RwLock<VecDeque<RegressionEvent>>>,
    config: RegressionConfig,
    data_directory: PathBuf,
}

/// Configuration for regression detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionConfig {
    /// Minimum percentage degradation to consider a regression
    pub regression_threshold: f64,
    /// Number of samples needed to establish a baseline
    pub baseline_samples: usize,
    /// Maximum age of baseline data (days)
    pub baseline_max_age_days: u64,
    /// Minimum confidence level for regression detection
    pub confidence_threshold: f64,
    /// Window size for moving average calculations
    pub smoothing_window: usize,
}

impl Default for RegressionConfig {
    fn default() -> Self {
        Self {
            regression_threshold: 10.0, // 10% degradation
            baseline_samples: 100,
            baseline_max_age_days: 30,
            confidence_threshold: 0.8, // 80% confidence
            smoothing_window: 10,
        }
    }
}

/// Performance baseline for a specific metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub metric_name: String,
    pub baseline_value: f64,
    pub standard_deviation: f64,
    pub sample_count: usize,
    pub established_at: SystemTime,
    pub last_updated: SystemTime,
    pub historical_values: VecDeque<f64>,
    pub confidence_level: f64,
}

/// Detected regression event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionEvent {
    pub metric_name: String,
    pub regression_type: RegressionType,
    pub severity: RegressionSeverity,
    pub baseline_value: f64,
    pub current_value: f64,
    pub degradation_percentage: f64,
    pub confidence: f64,
    pub detected_at: SystemTime,
    pub description: String,
    pub recommendations: Vec<String>,
}

/// Types of performance regressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegressionType {
    Latency,        // Increased response times
    Throughput,     // Decreased operations per second
    Resource,       // Higher resource usage
    ErrorRate,      // Increased error rates
    Efficiency,     // Decreased efficiency metrics
}

/// Severity levels for regressions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegressionSeverity {
    Critical,       // > 50% degradation
    High,          // 25-50% degradation  
    Medium,        // 10-25% degradation
    Low,           // 5-10% degradation
    Marginal,      // < 5% degradation
}

/// Analysis result for a profiling session
#[derive(Debug, Serialize)]
pub struct RegressionAnalysis {
    pub session_id: String,
    pub analyzed_at: SystemTime,
    pub regressions_found: Vec<RegressionEvent>,
    pub new_baselines: Vec<String>,
    pub updated_baselines: Vec<String>,
    pub overall_performance_trend: PerformanceTrend,
}

/// Overall performance trend
#[derive(Debug, Clone, Serialize)]
pub enum PerformanceTrend {
    Improving,
    Stable,
    Degrading,
    Unstable,
}

impl RegressionDetector {
    /// Create a new regression detector
    pub fn new(data_directory: PathBuf) -> Self {
        let detector = Self {
            baselines: Arc::new(RwLock::new(HashMap::new())),
            regression_history: Arc::new(RwLock::new(VecDeque::new())),
            config: RegressionConfig::default(),
            data_directory: data_directory.clone(),
        };

        // Load existing baselines
        if let Err(e) = detector.load_baselines() {
            warn!("Failed to load existing baselines: {}", e);
        }

        detector
    }

    /// Analyze a profiling session for regressions
    pub fn analyze_session(&self, session: &super::ProfileSession) -> Result<Vec<RegressionEvent>, super::ProfilingError> {
        info!("Analyzing session {} for performance regressions", session.session_id);

        let mut regressions = Vec::new();
        let metrics = &session.metrics_summary;

        // Analyze key performance metrics
        let metrics_to_analyze = vec![
            ("cpu_usage", metrics.cpu_samples as f64, RegressionType::Resource),
            ("memory_usage", metrics.peak_memory_usage as f64, RegressionType::Resource),
            ("io_throughput_read", metrics.total_bytes_read as f64, RegressionType::Throughput),
            ("io_throughput_write", metrics.total_bytes_written as f64, RegressionType::Throughput),
            ("performance_score", metrics.performance_score, RegressionType::Efficiency),
        ];

        for (metric_name, current_value, regression_type) in metrics_to_analyze {
            if let Some(regression) = self.detect_regression(metric_name, current_value, regression_type)? {
                regressions.push(regression);
            }

            // Update or create baseline
            self.update_baseline(metric_name, current_value)?;
        }

        // Save regressions to history
        {
            let mut history = self.regression_history.write().unwrap();
            for regression in &regressions {
                history.push_back(regression.clone());
                
                // Keep only last 1000 regression events
                if history.len() > 1000 {
                    history.pop_front();
                }
            }
        }

        // Save updated baselines
        if let Err(e) = self.save_baselines() {
            error!("Failed to save baselines: {}", e);
        }

        Ok(regressions)
    }

    /// Detect regression for a specific metric
    fn detect_regression(
        &self,
        metric_name: &str,
        current_value: f64,
        regression_type: RegressionType,
    ) -> Result<Option<RegressionEvent>, super::ProfilingError> {
        let baselines = self.baselines.read().unwrap();
        
        let baseline = match baselines.get(metric_name) {
            Some(baseline) => baseline,
            None => {
                debug!("No baseline found for metric: {}", metric_name);
                return Ok(None);
            }
        };

        // Check if baseline is too old
        let baseline_age = SystemTime::now()
            .duration_since(baseline.established_at)
            .unwrap_or_default()
            .as_secs() / (24 * 60 * 60); // Convert to days

        if baseline_age > self.config.baseline_max_age_days {
            debug!("Baseline for {} is too old ({} days), ignoring", metric_name, baseline_age);
            return Ok(None);
        }

        // Check if baseline has enough samples
        if baseline.sample_count < self.config.baseline_samples {
            debug!("Baseline for {} has insufficient samples ({}), ignoring", metric_name, baseline.sample_count);
            return Ok(None);
        }

        // Calculate degradation percentage
        let degradation_percentage = match regression_type {
            RegressionType::Latency | RegressionType::Resource | RegressionType::ErrorRate => {
                // Higher values are worse
                if current_value > baseline.baseline_value {
                    ((current_value - baseline.baseline_value) / baseline.baseline_value) * 100.0
                } else {
                    0.0 // No regression if value is lower
                }
            }
            RegressionType::Throughput | RegressionType::Efficiency => {
                // Lower values are worse
                if current_value < baseline.baseline_value {
                    ((baseline.baseline_value - current_value) / baseline.baseline_value) * 100.0
                } else {
                    0.0 // No regression if value is higher
                }
            }
        };

        // Check if degradation exceeds threshold
        if degradation_percentage < self.config.regression_threshold {
            return Ok(None);
        }

        // Calculate statistical confidence
        let confidence = self.calculate_confidence(baseline, current_value);
        if confidence < self.config.confidence_threshold {
            debug!("Regression confidence too low ({:.2}) for {}", confidence, metric_name);
            return Ok(None);
        }

        // Determine severity
        let severity = match degradation_percentage {
            p if p >= 50.0 => RegressionSeverity::Critical,
            p if p >= 25.0 => RegressionSeverity::High,
            p if p >= 10.0 => RegressionSeverity::Medium,
            p if p >= 5.0 => RegressionSeverity::Low,
            _ => RegressionSeverity::Marginal,
        };

        // Generate recommendations
        let recommendations = self.generate_recommendations(metric_name, &regression_type, degradation_percentage);

        let regression = RegressionEvent {
            metric_name: metric_name.to_string(),
            regression_type,
            severity,
            baseline_value: baseline.baseline_value,
            current_value,
            degradation_percentage,
            confidence,
            detected_at: SystemTime::now(),
            description: format!(
                "{} regression detected: {:.1}% degradation from baseline {:.2} to current {:.2}",
                metric_name, degradation_percentage, baseline.baseline_value, current_value
            ),
            recommendations,
        };

        info!("Regression detected: {}", regression.description);
        Ok(Some(regression))
    }

    /// Update or create a performance baseline
    fn update_baseline(&self, metric_name: &str, value: f64) -> Result<(), super::ProfilingError> {
        let mut baselines = self.baselines.write().unwrap();
        
        let baseline = baselines.entry(metric_name.to_string()).or_insert_with(|| {
            PerformanceBaseline {
                metric_name: metric_name.to_string(),
                baseline_value: value,
                standard_deviation: 0.0,
                sample_count: 0,
                established_at: SystemTime::now(),
                last_updated: SystemTime::now(),
                historical_values: VecDeque::new(),
                confidence_level: 0.0,
            }
        });

        // Add new value to historical data
        baseline.historical_values.push_back(value);
        baseline.sample_count += 1;
        baseline.last_updated = SystemTime::now();

        // Keep only recent values for baseline calculation
        let max_values = self.config.baseline_samples * 2;
        if baseline.historical_values.len() > max_values {
            baseline.historical_values.pop_front();
        }

        // Recalculate baseline statistics
        self.recalculate_baseline_stats(baseline);

        Ok(())
    }

    /// Recalculate baseline statistics
    fn recalculate_baseline_stats(&self, baseline: &mut PerformanceBaseline) {
        if baseline.historical_values.is_empty() {
            return;
        }

        // Apply smoothing if we have enough samples
        let values: Vec<f64> = if baseline.historical_values.len() >= self.config.smoothing_window {
            self.apply_moving_average(&baseline.historical_values)
        } else {
            baseline.historical_values.iter().cloned().collect()
        };

        // Calculate mean
        let sum: f64 = values.iter().sum();
        baseline.baseline_value = sum / values.len() as f64;

        // Calculate standard deviation
        let variance = values.iter()
            .map(|&x| (x - baseline.baseline_value).powi(2))
            .sum::<f64>() / values.len() as f64;
        baseline.standard_deviation = variance.sqrt();

        // Calculate confidence level based on sample size and stability
        let stability_factor = if baseline.standard_deviation == 0.0 {
            1.0
        } else {
            1.0 / (1.0 + baseline.standard_deviation / baseline.baseline_value.abs())
        };

        let sample_factor = (baseline.sample_count as f64 / self.config.baseline_samples as f64).min(1.0);
        baseline.confidence_level = stability_factor * sample_factor;
    }

    /// Apply moving average smoothing to reduce noise
    fn apply_moving_average(&self, values: &VecDeque<f64>) -> Vec<f64> {
        let window = self.config.smoothing_window;
        let mut smoothed = Vec::new();

        for i in 0..values.len() {
            let start = if i >= window { i - window + 1 } else { 0 };
            let end = i + 1;
            
            let window_sum: f64 = values.range(start..end).sum();
            let window_size = end - start;
            smoothed.push(window_sum / window_size as f64);
        }

        smoothed
    }

    /// Calculate statistical confidence for regression detection
    fn calculate_confidence(&self, baseline: &PerformanceBaseline, current_value: f64) -> f64 {
        if baseline.standard_deviation == 0.0 {
            return 1.0; // Perfect confidence if no variation
        }

        // Calculate z-score
        let z_score = (current_value - baseline.baseline_value).abs() / baseline.standard_deviation;
        
        // Convert z-score to confidence (approximation)
        let confidence = match z_score {
            z if z >= 3.0 => 0.99,  // 99% confidence for 3+ sigma
            z if z >= 2.0 => 0.95,  // 95% confidence for 2+ sigma
            z if z >= 1.5 => 0.86,  // 86% confidence for 1.5+ sigma 
            z if z >= 1.0 => 0.68,  // 68% confidence for 1+ sigma
            _ => z_score / 2.0,     // Linear scaling for smaller deviations
        };

        // Factor in baseline confidence
        confidence * baseline.confidence_level
    }

    /// Generate recommendations for addressing regressions
    fn generate_recommendations(&self, metric_name: &str, regression_type: &RegressionType, degradation: f64) -> Vec<String> {
        let mut recommendations = Vec::new();

        match (regression_type, metric_name) {
            (RegressionType::Latency, _) => {
                recommendations.push("Profile CPU usage to identify hot functions".to_string());
                recommendations.push("Check for I/O bottlenecks and disk performance".to_string());
                recommendations.push("Review recent code changes that might affect performance".to_string());
                if degradation > 25.0 {
                    recommendations.push("Consider reverting recent changes as emergency measure".to_string());
                }
            }
            (RegressionType::Throughput, name) if name.contains("io") => {
                recommendations.push("Analyze I/O patterns and consider batching operations".to_string());
                recommendations.push("Check disk utilization and consider storage optimization".to_string());
                recommendations.push("Review concurrent access patterns".to_string());
            }
            (RegressionType::Throughput, _) => {
                recommendations.push("Analyze throughput bottlenecks and optimize critical paths".to_string());
                recommendations.push("Check for resource contention and consider scaling".to_string());
                recommendations.push("Review algorithm efficiency and data structures".to_string());
            }
            (RegressionType::Resource, name) if name.contains("memory") => {
                recommendations.push("Profile memory allocations to identify leaks".to_string());
                recommendations.push("Check for memory fragmentation issues".to_string());
                recommendations.push("Review cache sizing and eviction policies".to_string());
            }
            (RegressionType::Resource, name) if name.contains("cpu") => {
                recommendations.push("Profile CPU usage to identify inefficient algorithms".to_string());
                recommendations.push("Check for increased lock contention".to_string());
                recommendations.push("Review thread utilization patterns".to_string());
            }
            (RegressionType::Resource, _) => {
                recommendations.push("Monitor resource usage patterns and optimize allocation".to_string());
                recommendations.push("Check for resource leaks and proper cleanup".to_string());
                recommendations.push("Consider resource pooling and reuse strategies".to_string());
            }
            (RegressionType::ErrorRate, _) => {
                recommendations.push("Check error logs for patterns and root causes".to_string());
                recommendations.push("Review recent configuration changes".to_string());
                recommendations.push("Verify system resources are not exhausted".to_string());
            }
            (RegressionType::Efficiency, _) => {
                recommendations.push("Analyze overall system performance holistically".to_string());
                recommendations.push("Check for resource contention between components".to_string());
                recommendations.push("Review workload patterns for changes".to_string());
            }
        }

        // Add severity-based recommendations
        if degradation > 50.0 {
            recommendations.insert(0, "CRITICAL: Consider immediate rollback or hotfix".to_string());
        } else if degradation > 25.0 {
            recommendations.insert(0, "HIGH PRIORITY: Schedule immediate investigation".to_string());
        }

        recommendations
    }

    /// Load baselines from disk
    fn load_baselines(&self) -> Result<(), super::ProfilingError> {
        let baseline_file = self.data_directory.join("performance_baselines.json");
        
        if !baseline_file.exists() {
            debug!("No existing baseline file found");
            return Ok(());
        }

        let file = File::open(&baseline_file)
            .map_err(|e| super::ProfilingError::IoError(format!("Failed to open baseline file: {}", e)))?;
        let reader = BufReader::new(file);
        
        let loaded_baselines: HashMap<String, PerformanceBaseline> = serde_json::from_reader(reader)
            .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;

        {
            let mut baselines = self.baselines.write().unwrap();
            *baselines = loaded_baselines;
        }

        info!("Loaded {} performance baselines", self.baselines.read().unwrap().len());
        Ok(())
    }

    /// Save baselines to disk
    fn save_baselines(&self) -> Result<(), super::ProfilingError> {
        let baseline_file = self.data_directory.join("performance_baselines.json");
        
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&baseline_file)
            .map_err(|e| super::ProfilingError::IoError(format!("Failed to create baseline file: {}", e)))?;
        
        let mut writer = BufWriter::new(file);
        let baselines = self.baselines.read().unwrap();
        
        serde_json::to_writer_pretty(&mut writer, &*baselines)
            .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;
        
        writer.flush()
            .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

        debug!("Saved {} performance baselines", baselines.len());
        Ok(())
    }

    /// Get regression history
    pub fn get_regression_history(&self) -> Vec<RegressionEvent> {
        let history = self.regression_history.read().unwrap();
        history.iter().cloned().collect()
    }

    /// Get current baselines
    pub fn get_baselines(&self) -> HashMap<String, PerformanceBaseline> {
        self.baselines.read().unwrap().clone()
    }

    /// Clear old regression events
    pub fn cleanup_history(&self, max_age_days: u64) {
        let cutoff_time = SystemTime::now() - Duration::from_secs(max_age_days * 24 * 60 * 60);
        
        let mut history = self.regression_history.write().unwrap();
        history.retain(|event| event.detected_at >= cutoff_time);
        
        info!("Cleaned up regression history, {} events remaining", history.len());
    }

    /// Generate performance report
    pub fn generate_performance_report(&self) -> Result<String, super::ProfilingError> {
        let baselines = self.baselines.read().unwrap();
        let history = self.regression_history.read().unwrap();
        
        let mut report = String::new();
        report.push_str("Performance Regression Analysis Report\n");
        report.push_str("=" .repeat(50).as_str());
        report.push_str("\n\n");

        // Baseline summary
        report.push_str(&format!("Established baselines: {}\n", baselines.len()));
        report.push_str(&format!("Recent regressions: {}\n\n", history.len()));

        // Recent regressions
        if !history.is_empty() {
            report.push_str("Recent Regressions:\n");
            report.push_str("-".repeat(20).as_str());
            report.push_str("\n");

            let recent_regressions: Vec<_> = history.iter()
                .rev()
                .take(10)
                .collect();

            for regression in recent_regressions {
                report.push_str(&format!(
                    "• {} ({:?}): {:.1}% degradation - {}\n",
                    regression.metric_name,
                    regression.severity,
                    regression.degradation_percentage,
                    regression.description
                ));
            }
            report.push('\n');
        }

        // Baseline summary
        if !baselines.is_empty() {
            report.push_str("Performance Baselines:\n");
            report.push_str("-" .repeat(20).as_str());
            report.push_str("\n");

            for (name, baseline) in baselines.iter() {
                report.push_str(&format!(
                    "• {}: {:.2} ± {:.2} (samples: {}, confidence: {:.1}%)\n",
                    name,
                    baseline.baseline_value,
                    baseline.standard_deviation,
                    baseline.sample_count,
                    baseline.confidence_level * 100.0
                ));
            }
            report.push('\n');
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_regression_detector_creation() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        assert_eq!(detector.config.regression_threshold, 10.0);
        assert_eq!(detector.config.baseline_samples, 100);
    }

    #[test]
    fn test_baseline_update() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        // Add some values
        for i in 1..=10 {
            detector.update_baseline("test_metric", i as f64).unwrap();
        }

        let baselines = detector.get_baselines();
        let baseline = baselines.get("test_metric").unwrap();
        
        assert_eq!(baseline.sample_count, 10);
        assert_eq!(baseline.baseline_value, 5.5); // Mean of 1..10
        assert!(baseline.standard_deviation > 0.0);
    }

    #[test]
    fn test_regression_severity_classification() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        // Establish baseline
        for _ in 0..100 {
            detector.update_baseline("latency", 100.0).unwrap();
        }

        // Test different severity levels
        let regression = detector.detect_regression("latency", 160.0, RegressionType::Latency).unwrap();
        assert!(regression.is_some());
        let regression = regression.unwrap();
        assert!(matches!(regression.severity, RegressionSeverity::Critical)); // 60% degradation
    }

    #[test]
    fn test_moving_average() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        let mut values = VecDeque::new();
        for i in 1..=20 {
            values.push_back(i as f64);
        }

        let smoothed = detector.apply_moving_average(&values);
        assert_eq!(smoothed.len(), 20);
        
        // First value should be 1.0 (average of just first element)
        assert_eq!(smoothed[0], 1.0);
        
        // Last value should be average of last window
        let last_window_avg = (11..=20).sum::<i32>() as f64 / 10.0;
        assert_eq!(smoothed[19], last_window_avg);
    }

    #[test]
    fn test_confidence_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        let baseline = PerformanceBaseline {
            metric_name: "test".to_string(),
            baseline_value: 100.0,
            standard_deviation: 10.0,
            sample_count: 100,
            established_at: SystemTime::now(),
            last_updated: SystemTime::now(),
            historical_values: VecDeque::new(),
            confidence_level: 1.0,
        };

        // Test various deviations
        let confidence_1_sigma = detector.calculate_confidence(&baseline, 110.0); // 1 sigma
        let confidence_2_sigma = detector.calculate_confidence(&baseline, 120.0); // 2 sigma
        let confidence_3_sigma = detector.calculate_confidence(&baseline, 130.0); // 3 sigma

        assert!(confidence_3_sigma > confidence_2_sigma);
        assert!(confidence_2_sigma > confidence_1_sigma);
    }

    #[test]
    fn test_recommendation_generation() {
        let temp_dir = TempDir::new().unwrap();
        let detector = RegressionDetector::new(temp_dir.path().to_path_buf());
        
        let recommendations = detector.generate_recommendations(
            "cpu_usage",
            &RegressionType::Resource,
            60.0
        );

        assert!(!recommendations.is_empty());
        assert!(recommendations[0].contains("CRITICAL"));
        assert!(recommendations.iter().any(|r| r.contains("CPU") || r.contains("cpu")));
    }
}
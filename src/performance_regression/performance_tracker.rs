use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

/// Performance tracking and regression detection for Lightning DB
/// 
/// This module provides tools for tracking performance over time,
/// detecting regressions, and generating detailed performance reports.

/// Performance measurement result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMeasurement {
    /// Timestamp of the measurement
    pub timestamp: u64,
    /// Git commit hash if available
    pub commit_hash: Option<String>,
    /// Test configuration used
    pub test_config: TestConfiguration,
    /// Measured operations per second
    pub ops_per_sec: f64,
    /// Latency percentiles (microseconds)
    pub latency_p50_us: f64,
    pub latency_p95_us: f64,
    pub latency_p99_us: f64,
    /// Memory usage in MB
    pub memory_usage_mb: f64,
    /// Test duration
    pub test_duration_ms: u64,
    /// Number of operations performed
    pub total_operations: u64,
    /// Thread count used
    pub thread_count: usize,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Test configuration parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfiguration {
    /// Test name/type
    pub test_name: String,
    /// Cache size used
    pub cache_size_mb: u64,
    /// Key size in bytes
    pub key_size: usize,
    /// Value size in bytes
    pub value_size: usize,
    /// Number of pre-populated records
    pub dataset_size: usize,
    /// Compression enabled
    pub compression_enabled: bool,
    /// Other configuration options
    pub options: HashMap<String, String>,
}

/// Performance baseline expectations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    /// Test name
    pub test_name: String,
    /// Expected operations per second
    pub expected_ops_per_sec: f64,
    /// Expected P50 latency (microseconds)
    pub expected_latency_p50_us: f64,
    /// Expected P99 latency (microseconds)  
    pub expected_latency_p99_us: f64,
    /// Acceptable regression threshold (0.0-1.0)
    pub regression_threshold: f64,
    /// Last updated timestamp
    pub last_updated: u64,
}

/// Performance regression analysis result
#[derive(Debug, Clone)]
pub struct RegressionAnalysis {
    /// Test name analyzed
    pub test_name: String,
    /// Current measurement
    pub current: PerformanceMeasurement,
    /// Baseline for comparison
    pub baseline: PerformanceBaseline,
    /// Performance ratio (current / baseline)
    pub performance_ratio: f64,
    /// Is this considered a regression?
    pub is_regression: bool,
    /// Severity of regression (0.0-1.0, higher = more severe)
    pub regression_severity: f64,
    /// Confidence in regression detection (0.0-1.0)
    pub confidence: f64,
    /// Historical trend analysis
    pub trend_analysis: TrendAnalysis,
}

/// Trend analysis over time
#[derive(Debug, Clone)]
pub struct TrendAnalysis {
    /// Trend direction: "improving", "stable", "degrading"
    pub direction: String,
    /// Rate of change per day (ops/sec per day)
    pub change_rate: f64,
    /// Variance in measurements
    pub variance: f64,
    /// Number of data points analyzed
    pub data_points: usize,
    /// Time span covered (days)
    pub time_span_days: f64,
}

/// Performance tracking database
pub struct PerformanceTracker {
    /// Historical measurements by test name
    measurements: HashMap<String, Vec<PerformanceMeasurement>>,
    /// Baselines for each test
    baselines: HashMap<String, PerformanceBaseline>,
    /// Storage path for persistence
    storage_path: String,
}

impl PerformanceTracker {
    /// Create new performance tracker
    pub fn new(storage_path: &str) -> Self {
        let mut tracker = Self {
            measurements: HashMap::new(),
            baselines: HashMap::new(),
            storage_path: storage_path.to_string(),
        };
        
        // Load existing data
        if let Err(e) = tracker.load_from_disk() {
            eprintln!("Warning: Could not load existing performance data: {}", e);
        }
        
        // Initialize with documented baselines if empty
        if tracker.baselines.is_empty() {
            tracker.initialize_documented_baselines();
        }
        
        tracker
    }

    /// Initialize with documented Lightning DB baselines
    fn initialize_documented_baselines(&mut self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // From documented benchmark results
        self.baselines.insert("read_performance".to_string(), PerformanceBaseline {
            test_name: "read_performance".to_string(),
            expected_ops_per_sec: 20_400_000.0,  // 20.4M ops/sec
            expected_latency_p50_us: 0.042,      // 0.042 μs P50
            expected_latency_p99_us: 0.089,      // 0.089 μs P99
            regression_threshold: 0.90,          // Allow 10% regression
            last_updated: timestamp,
        });

        self.baselines.insert("write_performance".to_string(), PerformanceBaseline {
            test_name: "write_performance".to_string(),
            expected_ops_per_sec: 1_140_000.0,   // 1.14M ops/sec
            expected_latency_p50_us: 0.771,      // 0.771 μs P50
            expected_latency_p99_us: 1.234,      // 1.234 μs P99
            regression_threshold: 0.90,          // Allow 10% regression
            last_updated: timestamp,
        });

        self.baselines.insert("mixed_workload".to_string(), PerformanceBaseline {
            test_name: "mixed_workload".to_string(),
            expected_ops_per_sec: 885_000.0,     // 885K ops/sec sustained
            expected_latency_p50_us: 1.0,        // Estimated
            expected_latency_p99_us: 2.0,        // Estimated
            regression_threshold: 0.85,          // Allow 15% regression
            last_updated: timestamp,
        });

        self.baselines.insert("concurrent_performance".to_string(), PerformanceBaseline {
            test_name: "concurrent_performance".to_string(),
            expected_ops_per_sec: 1_400_000.0,   // 1.4M ops/sec with 8 threads
            expected_latency_p50_us: 2.0,        // Estimated for concurrent
            expected_latency_p99_us: 5.0,        // Estimated for concurrent
            regression_threshold: 0.85,          // Allow 15% regression
            last_updated: timestamp,
        });

        self.baselines.insert("transaction_performance".to_string(), PerformanceBaseline {
            test_name: "transaction_performance".to_string(),
            expected_ops_per_sec: 412_371.0,     // From documented benchmarks
            expected_latency_p50_us: 2.0,        // Estimated
            expected_latency_p99_us: 3.234,      // From documented benchmarks
            regression_threshold: 0.80,          // Allow 20% regression (ACID overhead)
            last_updated: timestamp,
        });
    }

    /// Record new performance measurement
    pub fn record_measurement(&mut self, measurement: PerformanceMeasurement) {
        let test_name = measurement.test_config.test_name.clone();
        
        self.measurements
            .entry(test_name.clone())
            .or_insert_with(Vec::new)
            .push(measurement);
        
        // Keep only last 1000 measurements per test to prevent unbounded growth
        if let Some(measurements) = self.measurements.get_mut(&test_name) {
            if measurements.len() > 1000 {
                measurements.drain(0..measurements.len() - 1000);
            }
        }
        
        // Auto-save after recording
        if let Err(e) = self.save_to_disk() {
            eprintln!("Warning: Could not save performance data: {}", e);
        }
    }

    /// Analyze regression for specific test
    pub fn analyze_regression(&self, test_name: &str) -> Option<RegressionAnalysis> {
        let measurements = self.measurements.get(test_name)?;
        let baseline = self.baselines.get(test_name)?;
        let current = measurements.last()?;

        let performance_ratio = current.ops_per_sec / baseline.expected_ops_per_sec;
        let is_regression = performance_ratio < baseline.regression_threshold;
        
        // Calculate regression severity (how far below threshold)
        let regression_severity = if is_regression {
            (baseline.regression_threshold - performance_ratio) / baseline.regression_threshold
        } else {
            0.0
        };

        // Calculate confidence based on measurement consistency
        let confidence = self.calculate_confidence(test_name);

        // Perform trend analysis
        let trend_analysis = self.analyze_trend(test_name);

        Some(RegressionAnalysis {
            test_name: test_name.to_string(),
            current: current.clone(),
            baseline: baseline.clone(),
            performance_ratio,
            is_regression,
            regression_severity,
            confidence,
            trend_analysis,
        })
    }

    /// Calculate confidence in regression detection
    fn calculate_confidence(&self, test_name: &str) -> f64 {
        let measurements = match self.measurements.get(test_name) {
            Some(m) => m,
            None => return 0.0,
        };

        if measurements.len() < 3 {
            return 0.5; // Low confidence with few measurements
        }

        // Calculate coefficient of variation for recent measurements
        let recent: Vec<_> = measurements.iter().rev().take(10).collect();
        let mean: f64 = recent.iter().map(|m| m.ops_per_sec).sum::<f64>() / recent.len() as f64;
        let variance: f64 = recent
            .iter()
            .map(|m| (m.ops_per_sec - mean).powi(2))
            .sum::<f64>() / recent.len() as f64;
        let std_dev = variance.sqrt();
        let cv = std_dev / mean; // Coefficient of variation

        // Higher confidence with lower variance
        // CV of 0.1 (10%) gives confidence of 0.9
        // CV of 0.5 (50%) gives confidence of 0.5
        (1.0 - cv).max(0.1).min(1.0)
    }

    /// Analyze performance trend over time
    fn analyze_trend(&self, test_name: &str) -> TrendAnalysis {
        let measurements = match self.measurements.get(test_name) {
            Some(m) => m,
            None => return TrendAnalysis {
                direction: "unknown".to_string(),
                change_rate: 0.0,
                variance: 0.0,
                data_points: 0,
                time_span_days: 0.0,
            },
        };

        if measurements.len() < 2 {
            return TrendAnalysis {
                direction: "insufficient_data".to_string(),
                change_rate: 0.0,
                variance: 0.0,
                data_points: measurements.len(),
                time_span_days: 0.0,
            };
        }

        // Calculate linear regression slope for trend
        let n = measurements.len() as f64;
        let sum_x: f64 = measurements.iter().enumerate().map(|(i, _)| i as f64).sum();
        let sum_y: f64 = measurements.iter().map(|m| m.ops_per_sec).sum();
        let sum_xy: f64 = measurements
            .iter()
            .enumerate()
            .map(|(i, m)| i as f64 * m.ops_per_sec)
            .sum();
        let sum_x2: f64 = measurements
            .iter()
            .enumerate()
            .map(|(i, _)| (i as f64).powi(2))
            .sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);
        
        // Convert slope to ops/sec per day
        let first_timestamp = measurements.first().unwrap().timestamp;
        let last_timestamp = measurements.last().unwrap().timestamp;
        let time_span_seconds = last_timestamp - first_timestamp;
        let time_span_days = time_span_seconds as f64 / 86400.0; // seconds to days
        
        let change_rate = if time_span_days > 0.0 {
            slope * measurements.len() as f64 / time_span_days
        } else {
            0.0
        };

        // Calculate variance
        let mean = sum_y / n;
        let variance = measurements
            .iter()
            .map(|m| (m.ops_per_sec - mean).powi(2))
            .sum::<f64>() / n;

        // Determine trend direction
        let direction = if change_rate > mean * 0.01 {
            "improving".to_string()
        } else if change_rate < -mean * 0.01 {
            "degrading".to_string()
        } else {
            "stable".to_string()
        };

        TrendAnalysis {
            direction,
            change_rate,
            variance,
            data_points: measurements.len(),
            time_span_days,
        }
    }

    /// Generate comprehensive performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        report.push_str("# Lightning DB Performance Regression Report\n\n");
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        report.push_str(&format!("Generated: {}\n\n", 
            chrono::DateTime::from_timestamp(timestamp as i64, 0)
                .unwrap_or_default()
                .format("%Y-%m-%d %H:%M:%S UTC")));

        // Executive summary
        report.push_str("## Executive Summary\n\n");
        
        let mut total_tests = 0;
        let mut regression_count = 0;
        let mut avg_performance_ratio = 0.0;
        
        for test_name in self.baselines.keys() {
            if let Some(analysis) = self.analyze_regression(test_name) {
                total_tests += 1;
                if analysis.is_regression {
                    regression_count += 1;
                }
                avg_performance_ratio += analysis.performance_ratio;
            }
        }
        
        if total_tests > 0 {
            avg_performance_ratio /= total_tests as f64;
            
            report.push_str(&format!("- **Tests Analyzed**: {}\n", total_tests));
            report.push_str(&format!("- **Regressions Detected**: {}\n", regression_count));
            report.push_str(&format!("- **Regression Rate**: {:.1}%\n", 
                (regression_count as f64 / total_tests as f64) * 100.0));
            report.push_str(&format!("- **Average Performance**: {:.1}% of baseline\n\n", 
                avg_performance_ratio * 100.0));
        }

        // Detailed analysis for each test
        report.push_str("## Detailed Performance Analysis\n\n");
        
        let mut test_names: Vec<_> = self.baselines.keys().collect();
        test_names.sort();
        
        for test_name in test_names {
            if let Some(analysis) = self.analyze_regression(test_name) {
                report.push_str(&format!("### {}\n\n", analysis.test_name));
                
                let status = if analysis.is_regression {
                    "❌ **REGRESSION DETECTED**"
                } else {
                    "✅ **PASS**"
                };
                
                report.push_str(&format!("**Status**: {}\n\n", status));
                
                report.push_str("**Performance Metrics**:\n");
                report.push_str(&format!("- Current: {:>12.0} ops/sec\n", analysis.current.ops_per_sec));
                report.push_str(&format!("- Baseline: {:>11.0} ops/sec\n", analysis.baseline.expected_ops_per_sec));
                report.push_str(&format!("- Ratio: {:>15.1}% of baseline\n", analysis.performance_ratio * 100.0));
                report.push_str(&format!("- Threshold: {:>11.1}% minimum\n", analysis.baseline.regression_threshold * 100.0));
                
                if analysis.current.latency_p50_us > 0.0 {
                    report.push_str(&format!("- Latency P50: {:>8.3} μs\n", analysis.current.latency_p50_us));
                    report.push_str(&format!("- Latency P99: {:>8.3} μs\n", analysis.current.latency_p99_us));
                }
                
                report.push_str("\n**Trend Analysis**:\n");
                report.push_str(&format!("- Direction: {}\n", analysis.trend_analysis.direction));
                report.push_str(&format!("- Change Rate: {:.0} ops/sec per day\n", analysis.trend_analysis.change_rate));
                report.push_str(&format!("- Data Points: {}\n", analysis.trend_analysis.data_points));
                report.push_str(&format!("- Confidence: {:.1}%\n", analysis.confidence * 100.0));
                
                if analysis.is_regression {
                    report.push_str(&format!("\n**Regression Details**:\n"));
                    report.push_str(&format!("- Severity: {:.1}% below threshold\n", 
                        analysis.regression_severity * 100.0));
                    
                    // Recommendations
                    report.push_str("\n**Recommendations**:\n");
                    if analysis.regression_severity > 0.2 {
                        report.push_str("- 🔥 **CRITICAL**: Immediate investigation required\n");
                    } else if analysis.regression_severity > 0.1 {
                        report.push_str("- ⚠️ **HIGH**: Performance optimization needed\n");
                    } else {
                        report.push_str("- 📊 **MEDIUM**: Monitor closely, minor optimization\n");
                    }
                    
                    if analysis.trend_analysis.direction == "degrading" {
                        report.push_str("- 📉 **Degrading trend detected** - investigate recent changes\n");
                    }
                }
                
                report.push_str("\n");
            }
        }

        // Performance comparison table
        report.push_str("## Performance Comparison Table\n\n");
        report.push_str("| Test | Current (ops/sec) | Baseline (ops/sec) | Ratio | Status |\n");
        report.push_str("|------|------------------|-------------------|-------|--------|\n");
        
        for test_name in self.baselines.keys() {
            if let Some(analysis) = self.analyze_regression(test_name) {
                let status = if analysis.is_regression { "❌" } else { "✅" };
                report.push_str(&format!("| {} | {:>12.0} | {:>13.0} | {:>4.1}% | {} |\n",
                    analysis.test_name,
                    analysis.current.ops_per_sec,
                    analysis.baseline.expected_ops_per_sec,
                    analysis.performance_ratio * 100.0,
                    status
                ));
            }
        }
        
        report.push_str("\n");

        // Historical performance data
        report.push_str("## Historical Performance Trends\n\n");
        
        for test_name in self.baselines.keys() {
            if let Some(measurements) = self.measurements.get(test_name) {
                if measurements.len() >= 5 {
                    report.push_str(&format!("### {} - Recent History\n\n", test_name));
                    
                    let recent: Vec<_> = measurements.iter().rev().take(10).rev().collect();
                    report.push_str("| Timestamp | Ops/Sec | Latency P50 | Latency P99 | Notes |\n");
                    report.push_str("|-----------|---------|-------------|-------------|-------|\n");
                    
                    for measurement in recent {
                        let ts = chrono::DateTime::from_timestamp(measurement.timestamp as i64, 0)
                            .unwrap_or_default()
                            .format("%m-%d %H:%M");
                        
                        let commit = measurement.commit_hash
                            .as_ref()
                            .map(|h| &h[..8])
                            .unwrap_or("N/A");
                            
                        report.push_str(&format!(
                            "| {} | {:>8.0} | {:>8.3} μs | {:>8.3} μs | {} |\n",
                            ts,
                            measurement.ops_per_sec,
                            measurement.latency_p50_us,
                            measurement.latency_p99_us,
                            commit
                        ));
                    }
                    
                    report.push_str("\n");
                }
            }
        }

        // Recommendations and next steps
        if regression_count > 0 {
            report.push_str("## Recommendations\n\n");
            report.push_str("**Immediate Actions**:\n");
            report.push_str("1. 🔍 **Investigate regressions** - Profile performance bottlenecks\n");
            report.push_str("2. 📊 **Compare with git history** - Identify performance-impacting changes\n");
            report.push_str("3. 🏃 **Run targeted benchmarks** - Isolate specific performance issues\n");
            report.push_str("4. 🔧 **Apply optimizations** - Address identified bottlenecks\n\n");
            
            report.push_str("**Long-term Actions**:\n");
            report.push_str("1. 📈 **Set up CI performance monitoring** - Catch regressions early\n");
            report.push_str("2. 🎯 **Establish performance SLAs** - Define acceptable performance ranges\n");
            report.push_str("3. 📋 **Regular performance reviews** - Schedule weekly/monthly analysis\n");
            report.push_str("4. 🚀 **Performance optimization roadmap** - Plan systematic improvements\n\n");
        } else {
            report.push_str("## Summary\n\n");
            report.push_str("🎉 **All performance tests are passing!**\n\n");
            report.push_str("Lightning DB maintains its documented performance characteristics.\n");
            report.push_str("The reliability improvements have not significantly impacted performance.\n\n");
            
            report.push_str("**Recommended Actions**:\n");
            report.push_str("1. ✅ **Continue monitoring** - Maintain current tracking\n");
            report.push_str("2. 📊 **Consider raising targets** - Current performance may support higher baselines\n");
            report.push_str("3. 🔍 **Profile for hotspots** - Look for additional optimization opportunities\n");
            report.push_str("4. 📚 **Document current performance** - Update benchmarks and documentation\n\n");
        }

        report.push_str("---\n");
        report.push_str(&format!("*Report generated by Lightning DB Performance Tracker v1.0*\n"));
        
        report
    }

    /// Save performance data to disk
    fn save_to_disk(&self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Path::new(&self.storage_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let data = serde_json::to_string_pretty(&(
            &self.measurements,
            &self.baselines,
        ))?;
        
        fs::write(path, data)?;
        Ok(())
    }

    /// Load performance data from disk
    fn load_from_disk(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Path::new(&self.storage_path);
        if !path.exists() {
            return Ok(()); // No data to load yet
        }

        let data = fs::read_to_string(path)?;
        let (measurements, baselines): (
            HashMap<String, Vec<PerformanceMeasurement>>,
            HashMap<String, PerformanceBaseline>,
        ) = serde_json::from_str(&data)?;

        self.measurements = measurements;
        self.baselines = baselines;
        
        Ok(())
    }

    /// Export performance data to CSV for external analysis
    pub fn export_to_csv(&self, test_name: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let measurements = match self.measurements.get(test_name) {
            Some(m) => m,
            None => return Err(format!("No measurements found for test: {}", test_name).into()),
        };

        let mut csv_content = String::new();
        csv_content.push_str("timestamp,commit_hash,ops_per_sec,latency_p50_us,latency_p95_us,latency_p99_us,memory_usage_mb,thread_count\n");

        for measurement in measurements {
            csv_content.push_str(&format!(
                "{},{},{},{},{},{},{},{}\n",
                measurement.timestamp,
                measurement.commit_hash.as_ref().unwrap_or(&"".to_string()),
                measurement.ops_per_sec,
                measurement.latency_p50_us,
                measurement.latency_p95_us,
                measurement.latency_p99_us,
                measurement.memory_usage_mb,
                measurement.thread_count
            ));
        }

        fs::write(output_path, csv_content)?;
        Ok(())
    }

    /// Get summary statistics for all tests
    pub fn get_summary_stats(&self) -> HashMap<String, (f64, f64, usize)> {
        let mut stats = HashMap::new();
        
        for (test_name, measurements) in &self.measurements {
            if !measurements.is_empty() {
                let ops_per_sec: Vec<f64> = measurements.iter().map(|m| m.ops_per_sec).collect();
                let mean = ops_per_sec.iter().sum::<f64>() / ops_per_sec.len() as f64;
                let variance = ops_per_sec
                    .iter()
                    .map(|x| (x - mean).powi(2))
                    .sum::<f64>() / ops_per_sec.len() as f64;
                let std_dev = variance.sqrt();
                
                stats.insert(test_name.clone(), (mean, std_dev, measurements.len()));
            }
        }
        
        stats
    }
}

/// Convenience function to create a performance measurement
pub fn create_measurement(
    test_name: &str,
    ops_per_sec: f64,
    latency_p50_us: f64,
    latency_p99_us: f64,
    config: TestConfiguration,
) -> PerformanceMeasurement {
    PerformanceMeasurement {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        commit_hash: None, // Could be populated from git
        test_config: config,
        ops_per_sec,
        latency_p50_us,
        latency_p95_us: latency_p99_us * 0.8, // Estimate P95 from P99
        latency_p99_us,
        memory_usage_mb: 0.0,
        test_duration_ms: 10000, // Default 10 second test
        total_operations: (ops_per_sec * 10.0) as u64,
        thread_count: 1,
        metadata: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_performance_tracker_basic_functionality() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("perf_tracker.json");
        let mut tracker = PerformanceTracker::new(storage_path.to_str().unwrap());

        // Create test measurement
        let config = TestConfiguration {
            test_name: "test_performance".to_string(),
            cache_size_mb: 256,
            key_size: 32,
            value_size: 1024,
            dataset_size: 10000,
            compression_enabled: false,
            options: HashMap::new(),
        };

        let measurement = create_measurement(
            "test_performance",
            500_000.0,  // 500K ops/sec
            2.0,        // 2μs P50
            5.0,        // 5μs P99
            config,
        );

        tracker.record_measurement(measurement);

        // Analyze regression
        let analysis = tracker.analyze_regression("test_performance");
        assert!(analysis.is_some());
        
        let analysis = analysis.unwrap();
        assert_eq!(analysis.test_name, "test_performance");
        assert!(analysis.performance_ratio > 0.0);
    }

    #[test]
    fn test_regression_detection() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().join("regression_test.json");
        let mut tracker = PerformanceTracker::new(storage_path.to_str().unwrap());

        let config = TestConfiguration {
            test_name: "read_performance".to_string(),
            cache_size_mb: 256,
            key_size: 32,
            value_size: 1024,
            dataset_size: 10000,
            compression_enabled: false,
            options: HashMap::new(),
        };

        // Record measurement well below baseline (should trigger regression)
        let poor_measurement = create_measurement(
            "read_performance",
            1_000_000.0,  // 1M ops/sec (way below 20.4M baseline)
            10.0,         // 10μs P50 (way above 0.042μs baseline)
            20.0,         // 20μs P99
            config,
        );

        tracker.record_measurement(poor_measurement);

        let analysis = tracker.analyze_regression("read_performance").unwrap();
        assert!(analysis.is_regression, "Should detect regression with 1M ops/sec vs 20.4M baseline");
        assert!(analysis.regression_severity > 0.0);
    }
}
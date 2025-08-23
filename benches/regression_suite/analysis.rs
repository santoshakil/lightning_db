use crate::metrics::{BenchmarkMetrics, AggregatedMetrics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticalTest {
    pub test_name: String,
    pub p_value: f64,
    pub significance_level: f64,
    pub is_significant: bool,
    pub effect_size: f64,
    pub confidence_interval: (f64, f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionAnalysis {
    pub benchmark_name: String,
    pub current_performance: f64,
    pub baseline_performance: f64,
    pub change_percent: f64,
    pub is_regression: bool,
    pub statistical_test: StatisticalTest,
    pub severity: RegressionSeverity,
    pub recommended_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegressionSeverity {
    None,
    Minor,     // < 5% degradation
    Moderate,  // 5-15% degradation
    Major,     // 15-30% degradation
    Critical,  // > 30% degradation
}

impl RegressionSeverity {
    pub fn from_percent_change(change: f64) -> Self {
        let abs_change = change.abs();
        if abs_change < 5.0 {
            Self::None
        } else if abs_change < 15.0 {
            Self::Minor
        } else if abs_change < 30.0 {
            Self::Moderate
        } else if abs_change < 50.0 {
            Self::Major
        } else {
            Self::Critical
        }
    }
}

pub struct RegressionAnalyzer {
    significance_threshold: f64,
    min_sample_size: usize,
    variance_threshold: f64,
}

impl RegressionAnalyzer {
    pub fn new(significance_threshold: f64) -> Self {
        Self {
            significance_threshold,
            min_sample_size: 5,
            variance_threshold: 0.1, // 10% coefficient of variation threshold
        }
    }

    pub fn analyze_regression(
        &self,
        benchmark_name: &str,
        current_metrics: &[BenchmarkMetrics],
        baseline_metrics: &[BenchmarkMetrics],
    ) -> RegressionAnalysis {
        let current_perf = self.calculate_performance_metric(current_metrics);
        let baseline_perf = self.calculate_performance_metric(baseline_metrics);
        
        let change_percent = if baseline_perf > 0.0 {
            ((current_perf - baseline_perf) / baseline_perf) * 100.0
        } else {
            0.0
        };

        let statistical_test = self.perform_statistical_test(current_metrics, baseline_metrics);
        let severity = RegressionSeverity::from_percent_change(change_percent);
        let is_regression = change_percent < 0.0 && statistical_test.is_significant;
        
        let recommended_action = self.get_recommended_action(&severity, change_percent);

        RegressionAnalysis {
            benchmark_name: benchmark_name.to_string(),
            current_performance: current_perf,
            baseline_performance: baseline_perf,
            change_percent,
            is_regression,
            statistical_test,
            severity,
            recommended_action,
        }
    }

    pub fn is_significant_regression(
        &self,
        current_performance: f64,
        baseline_performance: f64,
        sample_size: usize,
    ) -> bool {
        if sample_size < self.min_sample_size {
            return false;
        }

        let change_percent = if baseline_performance > 0.0 {
            ((current_performance - baseline_performance) / baseline_performance) * 100.0
        } else {
            0.0
        };

        // Simple threshold-based regression detection
        // In a more sophisticated implementation, this would use proper statistical tests
        change_percent < -5.0 // 5% degradation threshold
    }

    fn calculate_performance_metric(&self, metrics: &[BenchmarkMetrics]) -> f64 {
        if metrics.is_empty() {
            return 0.0;
        }

        // Use trimmed mean to reduce impact of outliers
        let mut ops_per_sec: Vec<f64> = metrics.iter().map(|m| m.ops_per_sec).collect();
        ops_per_sec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Remove top and bottom 10% if we have enough samples
        let trim_size = if ops_per_sec.len() >= 10 {
            ops_per_sec.len() / 10
        } else {
            0
        };

        let trimmed_values = if trim_size > 0 {
            &ops_per_sec[trim_size..ops_per_sec.len() - trim_size]
        } else {
            &ops_per_sec
        };

        trimmed_values.iter().sum::<f64>() / trimmed_values.len() as f64
    }

    fn perform_statistical_test(
        &self,
        current_metrics: &[BenchmarkMetrics],
        baseline_metrics: &[BenchmarkMetrics],
    ) -> StatisticalTest {
        let current_values: Vec<f64> = current_metrics.iter().map(|m| m.ops_per_sec).collect();
        let baseline_values: Vec<f64> = baseline_metrics.iter().map(|m| m.ops_per_sec).collect();

        // Perform Welch's t-test for unequal variances
        let (t_statistic, p_value) = self.welch_t_test(&current_values, &baseline_values);
        
        let effect_size = self.cohens_d(&current_values, &baseline_values);
        let confidence_interval = self.calculate_confidence_interval(&current_values, 0.95);

        StatisticalTest {
            test_name: "Welch's t-test".to_string(),
            p_value,
            significance_level: self.significance_threshold,
            is_significant: p_value < self.significance_threshold,
            effect_size,
            confidence_interval,
        }
    }

    fn welch_t_test(&self, sample1: &[f64], sample2: &[f64]) -> (f64, f64) {
        if sample1.len() < 2 || sample2.len() < 2 {
            return (0.0, 1.0); // No significance if insufficient data
        }

        let mean1 = sample1.iter().sum::<f64>() / sample1.len() as f64;
        let mean2 = sample2.iter().sum::<f64>() / sample2.len() as f64;
        
        let var1 = self.variance(sample1, mean1);
        let var2 = self.variance(sample2, mean2);
        
        if var1 + var2 == 0.0 {
            return (0.0, 1.0);
        }

        let se = (var1 / sample1.len() as f64 + var2 / sample2.len() as f64).sqrt();
        let t_statistic = (mean1 - mean2) / se;
        
        // Welch's degrees of freedom
        let df = if var1 == 0.0 && var2 == 0.0 {
            (sample1.len() + sample2.len() - 2) as f64
        } else {
            let s1_sq_n = var1 / sample1.len() as f64;
            let s2_sq_n = var2 / sample2.len() as f64;
            let numerator = (s1_sq_n + s2_sq_n).powi(2);
            let denominator = s1_sq_n.powi(2) / (sample1.len() - 1) as f64
                + s2_sq_n.powi(2) / (sample2.len() - 1) as f64;
            numerator / denominator
        };

        let p_value = self.t_test_p_value(t_statistic.abs(), df);
        (t_statistic, p_value)
    }

    fn variance(&self, values: &[f64], mean: f64) -> f64 {
        if values.len() <= 1 {
            return 0.0;
        }
        
        let sum_sq_diff = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>();
        sum_sq_diff / (values.len() - 1) as f64
    }

    fn cohens_d(&self, sample1: &[f64], sample2: &[f64]) -> f64 {
        if sample1.is_empty() || sample2.is_empty() {
            return 0.0;
        }

        let mean1 = sample1.iter().sum::<f64>() / sample1.len() as f64;
        let mean2 = sample2.iter().sum::<f64>() / sample2.len() as f64;
        
        let var1 = self.variance(sample1, mean1);
        let var2 = self.variance(sample2, mean2);
        
        // Pooled standard deviation
        let pooled_sd = if sample1.len() + sample2.len() > 2 {
            let pooled_var = ((sample1.len() - 1) as f64 * var1 + (sample2.len() - 1) as f64 * var2)
                / (sample1.len() + sample2.len() - 2) as f64;
            pooled_var.sqrt()
        } else {
            1.0
        };

        if pooled_sd == 0.0 {
            0.0
        } else {
            (mean1 - mean2) / pooled_sd
        }
    }

    fn calculate_confidence_interval(&self, values: &[f64], confidence: f64) -> (f64, f64) {
        if values.len() < 2 {
            return (0.0, 0.0);
        }

        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = self.variance(values, mean);
        let std_error = (variance / values.len() as f64).sqrt();
        
        // Approximate critical value for t-distribution (simplified)
        let df = values.len() - 1;
        let t_critical = self.t_critical_value(confidence, df as f64);
        
        let margin_of_error = t_critical * std_error;
        (mean - margin_of_error, mean + margin_of_error)
    }

    fn t_critical_value(&self, confidence: f64, df: f64) -> f64 {
        // Simplified approximation for t-critical values
        // In a real implementation, you'd want a proper t-distribution lookup
        let alpha = 1.0 - confidence;
        let alpha_half = alpha / 2.0;
        
        // Rough approximation based on common values
        if df >= 30.0 {
            match alpha_half {
                x if x <= 0.005 => 2.576,
                x if x <= 0.01 => 2.326,
                x if x <= 0.025 => 1.96,
                x if x <= 0.05 => 1.645,
                _ => 1.0,
            }
        } else {
            // For smaller df, values are slightly higher
            match alpha_half {
                x if x <= 0.005 => 3.0,
                x if x <= 0.01 => 2.5,
                x if x <= 0.025 => 2.2,
                x if x <= 0.05 => 1.8,
                _ => 1.2,
            }
        }
    }

    fn t_test_p_value(&self, t_statistic: f64, df: f64) -> f64 {
        // Simplified p-value calculation
        // In a real implementation, you'd want proper statistical functions
        if df <= 0.0 || t_statistic.is_nan() || t_statistic.is_infinite() {
            return 1.0;
        }

        // Very rough approximation for p-value based on t-statistic
        let abs_t = t_statistic.abs();
        if abs_t > 3.0 {
            0.001
        } else if abs_t > 2.5 {
            0.01
        } else if abs_t > 2.0 {
            0.05
        } else if abs_t > 1.5 {
            0.1
        } else {
            0.2
        }
    }

    fn get_recommended_action(&self, severity: &RegressionSeverity, change_percent: f64) -> String {
        match severity {
            RegressionSeverity::None => {
                if change_percent > 0.0 {
                    "Performance improved! Consider investigating what caused the improvement.".to_string()
                } else {
                    "No significant performance change detected.".to_string()
                }
            }
            RegressionSeverity::Minor => {
                "Minor regression detected. Monitor in future runs and investigate if trend continues.".to_string()
            }
            RegressionSeverity::Moderate => {
                "Moderate regression detected. Investigate recent changes and consider reverting problematic commits.".to_string()
            }
            RegressionSeverity::Major => {
                "Major regression detected! Immediate investigation required. Consider blocking deployment.".to_string()
            }
            RegressionSeverity::Critical => {
                "CRITICAL regression detected! Stop deployment immediately and investigate root cause.".to_string()
            }
        }
    }

    pub fn detect_outliers(&self, metrics: &[BenchmarkMetrics]) -> Vec<usize> {
        if metrics.len() < 4 {
            return Vec::new(); // Need at least 4 points for outlier detection
        }

        let values: Vec<f64> = metrics.iter().map(|m| m.ops_per_sec).collect();
        let mut outliers = Vec::new();
        
        // Use IQR method for outlier detection
        let mut sorted_values = values.clone();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let n = sorted_values.len();
        let q1 = sorted_values[n / 4];
        let q3 = sorted_values[3 * n / 4];
        let iqr = q3 - q1;
        let lower_fence = q1 - 1.5 * iqr;
        let upper_fence = q3 + 1.5 * iqr;
        
        for (i, &value) in values.iter().enumerate() {
            if value < lower_fence || value > upper_fence {
                outliers.push(i);
            }
        }
        
        outliers
    }

    pub fn calculate_trend(&self, metrics: &[BenchmarkMetrics]) -> TrendAnalysis {
        if metrics.len() < 2 {
            return TrendAnalysis::default();
        }

        let x_values: Vec<f64> = (0..metrics.len()).map(|i| i as f64).collect();
        let y_values: Vec<f64> = metrics.iter().map(|m| m.ops_per_sec).collect();
        
        let (slope, intercept, r_squared) = self.linear_regression(&x_values, &y_values);
        
        let trend_direction = if slope > 0.01 {
            TrendDirection::Improving
        } else if slope < -0.01 {
            TrendDirection::Degrading
        } else {
            TrendDirection::Stable
        };

        TrendAnalysis {
            slope,
            intercept,
            r_squared,
            direction: trend_direction,
            confidence: if r_squared > 0.8 { TrendConfidence::High }
                      else if r_squared > 0.5 { TrendConfidence::Medium }
                      else { TrendConfidence::Low },
        }
    }

    fn linear_regression(&self, x: &[f64], y: &[f64]) -> (f64, f64, f64) {
        if x.len() != y.len() || x.len() < 2 {
            return (0.0, 0.0, 0.0);
        }

        let n = x.len() as f64;
        let sum_x = x.iter().sum::<f64>();
        let sum_y = y.iter().sum::<f64>();
        let sum_xy = x.iter().zip(y.iter()).map(|(xi, yi)| xi * yi).sum::<f64>();
        let sum_xx = x.iter().map(|xi| xi * xi).sum::<f64>();
        let sum_yy = y.iter().map(|yi| yi * yi).sum::<f64>();
        
        let denominator = n * sum_xx - sum_x * sum_x;
        if denominator == 0.0 {
            return (0.0, sum_y / n, 0.0);
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denominator;
        let intercept = (sum_y - slope * sum_x) / n;
        
        // Calculate R²
        let mean_y = sum_y / n;
        let ss_tot = y.iter().map(|yi| (yi - mean_y).powi(2)).sum::<f64>();
        let ss_res = x.iter().zip(y.iter())
            .map(|(xi, yi)| {
                let predicted = slope * xi + intercept;
                (yi - predicted).powi(2)
            })
            .sum::<f64>();
        
        let r_squared = if ss_tot == 0.0 {
            0.0
        } else {
            1.0 - (ss_res / ss_tot)
        };

        (slope, intercept, r_squared)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAnalysis {
    pub slope: f64,
    pub intercept: f64,
    pub r_squared: f64,
    pub direction: TrendDirection,
    pub confidence: TrendConfidence,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrendDirection {
    Improving,
    Stable,
    Degrading,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrendConfidence {
    High,
    Medium,
    Low,
}

impl Default for TrendAnalysis {
    fn default() -> Self {
        Self {
            slope: 0.0,
            intercept: 0.0,
            r_squared: 0.0,
            direction: TrendDirection::Stable,
            confidence: TrendConfidence::Low,
        }
    }
}

pub struct PerformanceAnalyzer {
    regression_analyzer: RegressionAnalyzer,
}

impl PerformanceAnalyzer {
    pub fn new(significance_threshold: f64) -> Self {
        Self {
            regression_analyzer: RegressionAnalyzer::new(significance_threshold),
        }
    }

    pub fn analyze_performance_history(
        &self,
        metrics_history: &HashMap<String, Vec<BenchmarkMetrics>>,
    ) -> HashMap<String, PerformanceReport> {
        let mut reports = HashMap::new();

        for (benchmark_name, metrics) in metrics_history {
            if metrics.len() < 2 {
                continue;
            }

            let aggregated = AggregatedMetrics::from_metrics(metrics);
            let trend = self.regression_analyzer.calculate_trend(metrics);
            let outliers = self.regression_analyzer.detect_outliers(metrics);

            let recent_metrics = if metrics.len() > 5 {
                &metrics[metrics.len() - 5..]
            } else {
                metrics
            };

            let baseline_metrics = if metrics.len() > 10 {
                &metrics[0..5]
            } else if metrics.len() > 5 {
                &metrics[0..metrics.len() / 2]
            } else {
                &metrics[0..1]
            };

            let regression_analysis = self.regression_analyzer.analyze_regression(
                benchmark_name,
                recent_metrics,
                baseline_metrics,
            );

            let report = PerformanceReport {
                benchmark_name: benchmark_name.clone(),
                aggregated_metrics: aggregated,
                trend_analysis: trend,
                regression_analysis,
                outlier_indices: outliers,
                total_runs: metrics.len(),
                data_quality_score: self.calculate_data_quality_score(metrics),
            };

            reports.insert(benchmark_name.clone(), report);
        }

        reports
    }

    fn calculate_data_quality_score(&self, metrics: &[BenchmarkMetrics]) -> f64 {
        if metrics.is_empty() {
            return 0.0;
        }

        // Factors affecting data quality:
        // 1. Number of samples
        // 2. Variance in measurements
        // 3. Number of errors
        // 4. Consistency of test parameters

        let sample_size_score = if metrics.len() >= 20 {
            1.0
        } else if metrics.len() >= 10 {
            0.8
        } else if metrics.len() >= 5 {
            0.6
        } else {
            0.4
        };

        let values: Vec<f64> = metrics.iter().map(|m| m.ops_per_sec).collect();
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
        let cv = if mean > 0.0 { (variance.sqrt() / mean) } else { 1.0 };
        
        let variance_score = if cv < 0.05 {
            1.0
        } else if cv < 0.1 {
            0.8
        } else if cv < 0.2 {
            0.6
        } else {
            0.4
        };

        let total_errors: u64 = metrics.iter().map(|m| m.error_count).sum();
        let error_rate = total_errors as f64 / metrics.len() as f64;
        let error_score = if error_rate == 0.0 {
            1.0
        } else if error_rate < 0.01 {
            0.9
        } else if error_rate < 0.05 {
            0.7
        } else {
            0.5
        };

        // Check parameter consistency
        let first_params = (&metrics[0].thread_count, &metrics[0].value_size, &metrics[0].cache_size);
        let consistent_params = metrics.iter().all(|m| {
            (&m.thread_count, &m.value_size, &m.cache_size) == first_params
        });
        let consistency_score = if consistent_params { 1.0 } else { 0.7 };

        (sample_size_score + variance_score + error_score + consistency_score) / 4.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub benchmark_name: String,
    pub aggregated_metrics: AggregatedMetrics,
    pub trend_analysis: TrendAnalysis,
    pub regression_analysis: RegressionAnalysis,
    pub outlier_indices: Vec<usize>,
    pub total_runs: usize,
    pub data_quality_score: f64,
}
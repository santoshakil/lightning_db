//! Statistical Analysis for Performance Regression Detection
//!
//! Provides statistical methods for detecting performance regressions by comparing
//! current performance metrics against established baselines.

use super::{PerformanceMetric, PerformanceBaseline, RegressionDetectionResult, RegressionSeverity, RegressionDetectorConfig};
use crate::Result;
use std::time::SystemTime;

/// Statistical analyzer for regression detection
pub struct StatisticalAnalyzer {
    config: RegressionDetectorConfig,
}

/// Statistical test types for regression detection
#[derive(Debug, Clone, Copy)]
pub enum StatisticalTest {
    /// Simple threshold-based detection
    Threshold,
    /// Z-score based detection (assumes normal distribution)
    ZScore,
    /// T-test for comparing means
    TTest,
    /// Mann-Whitney U test (non-parametric)
    MannWhitneyU,
    /// Welch's t-test (unequal variances)
    WelchTTest,
    /// Kolmogorov-Smirnov test (distribution comparison)
    KolmogorovSmirnov,
}

/// Statistical test result
#[derive(Debug, Clone)]
pub struct StatisticalTestResult {
    pub test_type: StatisticalTest,
    pub p_value: f64,
    pub test_statistic: f64,
    pub is_significant: bool,
    pub confidence_level: f64,
    pub effect_size: f64,
}

/// Regression analysis configuration
#[derive(Debug, Clone)]
pub struct RegressionAnalysisConfig {
    pub primary_test: StatisticalTest,
    pub fallback_tests: Vec<StatisticalTest>,
    pub significance_level: f64,
    pub minimum_effect_size: f64,
    pub bootstrap_samples: usize,
}

impl Default for RegressionAnalysisConfig {
    fn default() -> Self {
        Self {
            primary_test: StatisticalTest::WelchTTest,
            fallback_tests: vec![StatisticalTest::ZScore, StatisticalTest::Threshold],
            significance_level: 0.05, // 5% significance level
            minimum_effect_size: 0.2,  // Cohen's d >= 0.2 (small effect)
            bootstrap_samples: 1000,
        }
    }
}

impl StatisticalAnalyzer {
    /// Create a new statistical analyzer
    pub fn new(config: RegressionDetectorConfig) -> Self {
        Self { config }
    }

    /// Analyze a metric for regression against baseline
    pub fn analyze_regression(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<Option<RegressionDetectionResult>> {
        // Calculate degradation percentage
        let degradation_percentage = self.calculate_degradation_percentage(metric, baseline);

        // Quick threshold check first
        if degradation_percentage.abs() < self.config.detection_sensitivity {
            return Ok(None); // No significant change
        }

        // Perform statistical tests
        let analysis_config = RegressionAnalysisConfig::default();
        let test_results = self.perform_statistical_tests(metric, baseline, &analysis_config)?;

        // Determine if regression is statistically significant
        let is_regression = self.is_statistically_significant(&test_results, &analysis_config);

        if !is_regression {
            return Ok(None);
        }

        // Calculate overall confidence
        let statistical_confidence = self.calculate_overall_confidence(&test_results);

        // Determine severity
        let severity = self.determine_severity(degradation_percentage);

        // Generate recommended actions
        let recommended_actions = self.generate_statistical_recommendations(
            metric,
            baseline,
            &test_results,
            degradation_percentage,
        );

        Ok(Some(RegressionDetectionResult {
            detected: true,
            severity,
            operation_type: metric.operation_type.clone(),
            current_performance: metric.clone(),
            baseline_performance: baseline.clone(),
            degradation_percentage,
            statistical_confidence,
            recommended_actions,
            detection_timestamp: SystemTime::now(),
        }))
    }

    /// Calculate performance degradation percentage
    fn calculate_degradation_percentage(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> f64 {
        if baseline.mean_duration_micros == 0.0 {
            return 0.0;
        }

        let current_duration = metric.duration_micros as f64;
        let baseline_duration = baseline.mean_duration_micros;

        (current_duration - baseline_duration) / baseline_duration
    }

    /// Perform multiple statistical tests
    fn perform_statistical_tests(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
        config: &RegressionAnalysisConfig,
    ) -> Result<Vec<StatisticalTestResult>> {
        let mut results = Vec::new();

        // Primary test
        let primary_result = self.perform_single_test(metric, baseline, config.primary_test)?;
        results.push(primary_result);

        // Fallback tests
        for &test_type in &config.fallback_tests {
            let result = self.perform_single_test(metric, baseline, test_type)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Perform a single statistical test
    fn perform_single_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
        test_type: StatisticalTest,
    ) -> Result<StatisticalTestResult> {
        match test_type {
            StatisticalTest::Threshold => self.threshold_test(metric, baseline),
            StatisticalTest::ZScore => self.z_score_test(metric, baseline),
            StatisticalTest::TTest => self.t_test(metric, baseline),
            StatisticalTest::MannWhitneyU => self.mann_whitney_test(metric, baseline),
            StatisticalTest::WelchTTest => self.welch_t_test(metric, baseline),
            StatisticalTest::KolmogorovSmirnov => self.kolmogorov_smirnov_test(metric, baseline),
        }
    }

    /// Simple threshold-based test
    fn threshold_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        let degradation = self.calculate_degradation_percentage(metric, baseline);
        let threshold = self.config.detection_sensitivity;
        
        let is_significant = degradation.abs() >= threshold;
        let effect_size = degradation.abs();

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::Threshold,
            p_value: if is_significant { 0.01 } else { 0.99 }, // Dummy p-value
            test_statistic: degradation,
            is_significant,
            confidence_level: if is_significant { 0.95 } else { 0.05 },
            effect_size,
        })
    }

    /// Z-score test (assuming normal distribution)
    fn z_score_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        if baseline.std_deviation_micros == 0.0 {
            return self.threshold_test(metric, baseline);
        }

        let z_score = (metric.duration_micros as f64 - baseline.mean_duration_micros) 
            / baseline.std_deviation_micros;

        let p_value = self.calculate_z_test_p_value(z_score);
        let is_significant = p_value < 0.05; // 5% significance level
        let effect_size = z_score.abs();

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::ZScore,
            p_value,
            test_statistic: z_score,
            is_significant,
            confidence_level: 1.0 - p_value,
            effect_size,
        })
    }

    /// T-test for single sample against baseline
    fn t_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        if baseline.std_deviation_micros == 0.0 || baseline.sample_count <= 1 {
            return self.z_score_test(metric, baseline);
        }

        let t_statistic = (metric.duration_micros as f64 - baseline.mean_duration_micros) 
            / (baseline.std_deviation_micros / (baseline.sample_count as f64).sqrt());

        let degrees_of_freedom = baseline.sample_count - 1;
        let p_value = self.calculate_t_test_p_value(t_statistic, degrees_of_freedom);
        let is_significant = p_value < 0.05;
        let effect_size = t_statistic.abs() / (degrees_of_freedom as f64).sqrt();

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::TTest,
            p_value,
            test_statistic: t_statistic,
            is_significant,
            confidence_level: 1.0 - p_value,
            effect_size,
        })
    }

    /// Welch's t-test (unequal variances)
    fn welch_t_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        if baseline.std_deviation_micros == 0.0 {
            return self.t_test(metric, baseline);
        }

        // For single observation vs baseline, use modified approach
        let current_value = metric.duration_micros as f64;
        let baseline_mean = baseline.mean_duration_micros;
        let baseline_std = baseline.std_deviation_micros;
        let baseline_n = baseline.sample_count as f64;

        // Assume current observation has some uncertainty (use baseline std as estimate)
        let current_std = baseline_std * 0.5; // Conservative estimate
        let current_n = 1.0;

        let pooled_se = ((baseline_std * baseline_std / baseline_n) + 
                        (current_std * current_std / current_n)).sqrt();

        let t_statistic = (current_value - baseline_mean) / pooled_se;

        // Welch's degrees of freedom
        let s1_sq_n1 = (baseline_std * baseline_std) / baseline_n;
        let s2_sq_n2 = (current_std * current_std) / current_n;
        let df = (s1_sq_n1 + s2_sq_n2).powi(2) / 
                 ((s1_sq_n1 * s1_sq_n1) / (baseline_n - 1.0) + 
                  (s2_sq_n2 * s2_sq_n2) / (current_n - 1.0));

        let p_value = self.calculate_t_test_p_value(t_statistic, df as usize);
        let is_significant = p_value < 0.05;
        let effect_size = (current_value - baseline_mean).abs() / baseline_std;

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::WelchTTest,
            p_value,
            test_statistic: t_statistic,
            is_significant,
            confidence_level: 1.0 - p_value,
            effect_size,
        })
    }

    /// Mann-Whitney U test (non-parametric)
    fn mann_whitney_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        // For single observation, use simplified rank-based approach
        let current_value = metric.duration_micros as f64;
        let baseline_mean = baseline.mean_duration_micros;
        
        // Estimate rank based on percentiles
        let rank_estimate = if current_value <= baseline.p50_duration_micros as f64 {
            0.5
        } else if current_value <= baseline.p95_duration_micros as f64 {
            0.75
        } else {
            0.95
        };

        // Convert rank to test statistic
        let u_statistic = rank_estimate * baseline.sample_count as f64;
        let z_statistic = (u_statistic - (baseline.sample_count as f64 / 2.0)) / 
                         (baseline.sample_count as f64 / 12.0).sqrt();

        let p_value = self.calculate_z_test_p_value(z_statistic);
        let is_significant = p_value < 0.05;
        let effect_size = (current_value - baseline_mean).abs() / baseline.std_deviation_micros;

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::MannWhitneyU,
            p_value,
            test_statistic: z_statistic,
            is_significant,
            confidence_level: 1.0 - p_value,
            effect_size,
        })
    }

    /// Kolmogorov-Smirnov test (distribution comparison)
    fn kolmogorov_smirnov_test(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
    ) -> Result<StatisticalTestResult> {
        // For single observation, estimate how unusual it is within the baseline distribution
        let current_value = metric.duration_micros as f64;
        
        // Calculate CDF position in baseline distribution (assuming normal)
        let z_score = (current_value - baseline.mean_duration_micros) / baseline.std_deviation_micros;
        let cdf_position = self.normal_cdf(z_score);
        
        // KS statistic is maximum deviation from expected CDF
        let ks_statistic = (cdf_position - 0.5).abs(); // Compare against median
        
        // Approximate p-value for KS test
        let n = baseline.sample_count as f64;
        let p_value = 2.0 * (-2.0 * n * ks_statistic * ks_statistic).exp();
        
        let is_significant = p_value < 0.05;
        let effect_size = ks_statistic;

        Ok(StatisticalTestResult {
            test_type: StatisticalTest::KolmogorovSmirnov,
            p_value,
            test_statistic: ks_statistic,
            is_significant,
            confidence_level: 1.0 - p_value,
            effect_size,
        })
    }

    /// Calculate p-value for z-test
    fn calculate_z_test_p_value(&self, z_score: f64) -> f64 {
        // Two-tailed test
        let one_tailed = 1.0 - self.normal_cdf(z_score.abs());
        2.0 * one_tailed
    }

    /// Calculate p-value for t-test (approximate)
    fn calculate_t_test_p_value(&self, t_statistic: f64, df: usize) -> f64 {
        // Approximate t-distribution with normal for large df
        if df >= 30 {
            self.calculate_z_test_p_value(t_statistic)
        } else {
            // Simple approximation for small df
            let adjusted_t = t_statistic * (df as f64 / (df as f64 + t_statistic * t_statistic)).sqrt();
            self.calculate_z_test_p_value(adjusted_t) * (1.0 + 1.0 / (4.0 * df as f64))
        }
    }

    /// Normal CDF approximation
    fn normal_cdf(&self, x: f64) -> f64 {
        // Abramowitz and Stegun approximation
        let a1 =  0.254829592;
        let a2 = -0.284496736;
        let a3 =  1.421413741;
        let a4 = -1.453152027;
        let a5 =  1.061405429;
        let p  =  0.3275911;

        let sign = if x < 0.0 { -1.0 } else { 1.0 };
        let x = x.abs();

        let t = 1.0 / (1.0 + p * x);
        let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x / 2.0).exp();

        0.5 * (1.0 + sign * y)
    }

    /// Determine if tests show statistically significant regression
    fn is_statistically_significant(
        &self,
        test_results: &[StatisticalTestResult],
        config: &RegressionAnalysisConfig,
    ) -> bool {
        if test_results.is_empty() {
            return false;
        }

        // Primary test must be significant
        if !test_results[0].is_significant {
            return false;
        }

        // Check effect size
        if test_results[0].effect_size < config.minimum_effect_size {
            return false;
        }

        // If multiple tests available, require at least 2 to agree
        if test_results.len() > 2 {
            let significant_count = test_results.iter()
                .filter(|r| r.is_significant)
                .count();
            
            significant_count >= 2
        } else {
            true
        }
    }

    /// Calculate overall confidence from multiple test results
    fn calculate_overall_confidence(&self, test_results: &[StatisticalTestResult]) -> f64 {
        if test_results.is_empty() {
            return 0.0;
        }

        // Weight different tests
        let mut weighted_confidence = 0.0;
        let mut total_weight = 0.0;

        for result in test_results {
            let weight = match result.test_type {
                StatisticalTest::WelchTTest => 3.0,
                StatisticalTest::TTest => 2.5,
                StatisticalTest::ZScore => 2.0,
                StatisticalTest::MannWhitneyU => 2.0,
                StatisticalTest::KolmogorovSmirnov => 1.5,
                StatisticalTest::Threshold => 1.0,
            };

            weighted_confidence += result.confidence_level * weight;
            total_weight += weight;
        }

        if total_weight > 0.0 {
            weighted_confidence / total_weight
        } else {
            0.0
        }
    }

    /// Determine regression severity based on degradation percentage
    fn determine_severity(&self, degradation_percentage: f64) -> RegressionSeverity {
        let abs_degradation = degradation_percentage.abs();

        if abs_degradation >= 0.7 {
            RegressionSeverity::Critical
        } else if abs_degradation >= 0.4 {
            RegressionSeverity::Major
        } else if abs_degradation >= 0.2 {
            RegressionSeverity::Moderate
        } else {
            RegressionSeverity::Minor
        }
    }

    /// Generate statistical recommendations
    fn generate_statistical_recommendations(
        &self,
        metric: &PerformanceMetric,
        baseline: &PerformanceBaseline,
        test_results: &[StatisticalTestResult],
        degradation_percentage: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Statistical confidence recommendations
        let avg_confidence = self.calculate_overall_confidence(test_results);
        if avg_confidence > 0.95 {
            recommendations.push("High statistical confidence in regression detection".to_string());
        } else if avg_confidence > 0.8 {
            recommendations.push("Moderate statistical confidence - monitor closely".to_string());
        } else {
            recommendations.push("Low statistical confidence - collect more data before acting".to_string());
        }

        // Test-specific recommendations
        for test_result in test_results {
            if test_result.is_significant {
                match test_result.test_type {
                    StatisticalTest::WelchTTest => {
                        recommendations.push("Welch's t-test confirms regression with unequal variances considered".to_string());
                    }
                    StatisticalTest::MannWhitneyU => {
                        recommendations.push("Non-parametric test confirms regression (distribution-free)".to_string());
                    }
                    StatisticalTest::KolmogorovSmirnov => {
                        recommendations.push("Distribution comparison shows significant change".to_string());
                    }
                    _ => {}
                }
            }
        }

        // Effect size recommendations
        let primary_effect_size = test_results.first().map(|r| r.effect_size).unwrap_or(0.0);
        if primary_effect_size > 2.0 {
            recommendations.push("Very large effect size - immediate investigation required".to_string());
        } else if primary_effect_size > 0.8 {
            recommendations.push("Large effect size - significant performance impact".to_string());
        } else if primary_effect_size > 0.5 {
            recommendations.push("Medium effect size - noticeable performance change".to_string());
        }

        // Baseline quality recommendations
        if baseline.sample_count < 50 {
            recommendations.push("Small baseline sample size - consider collecting more baseline data".to_string());
        }

        if baseline.std_deviation_micros / baseline.mean_duration_micros > 0.5 {
            recommendations.push("High baseline variability - performance may be naturally unstable".to_string());
        }

        // Multi-metric recommendations
        if metric.memory_usage_bytes as f64 > baseline.memory_baseline as f64 * 1.5 {
            recommendations.push("Memory usage also increased - potential memory-related regression".to_string());
        }

        if metric.cpu_usage_percent > baseline.cpu_baseline * 1.5 {
            recommendations.push("CPU usage also increased - potential CPU-related regression".to_string());
        }

        if degradation_percentage > 0.0 {
            recommendations.push("Performance degradation detected".to_string());
        } else {
            recommendations.push("Performance improvement detected".to_string());
        }

        recommendations
    }

    /// Perform bootstrap analysis for confidence estimation
    pub fn bootstrap_analysis(
        &self,
        baseline: &PerformanceBaseline,
        current_value: f64,
        bootstrap_samples: usize,
    ) -> Result<f64> {
        // Simple bootstrap simulation
        let mut bootstrap_means = Vec::new();
        let baseline_mean = baseline.mean_duration_micros;
        let baseline_std = baseline.std_deviation_micros;

        for _ in 0..bootstrap_samples {
            // Generate bootstrap sample from baseline distribution
            let mut sample_sum = 0.0;
            for _ in 0..baseline.sample_count {
                let sample_value = self.generate_normal_sample(baseline_mean, baseline_std);
                sample_sum += sample_value;
            }
            let bootstrap_mean = sample_sum / baseline.sample_count as f64;
            bootstrap_means.push(bootstrap_mean);
        }

        // Calculate how many bootstrap means are more extreme than current value
        let more_extreme_count = bootstrap_means.iter()
            .filter(|&&mean| (mean - baseline_mean).abs() >= (current_value - baseline_mean).abs())
            .count();

        Ok(more_extreme_count as f64 / bootstrap_samples as f64)
    }

    /// Generate a normal random sample (Box-Muller transform)
    fn generate_normal_sample(&self, mean: f64, std_dev: f64) -> f64 {
        use std::f64::consts::PI;
        
        // Simple linear congruential generator for reproducible randomness
        static mut SEED: u64 = 12345;
        unsafe {
            SEED = SEED.wrapping_mul(1103515245).wrapping_add(12345);
            let u1 = (SEED as f64) / (u64::MAX as f64);
            
            SEED = SEED.wrapping_mul(1103515245).wrapping_add(12345);
            let u2 = (SEED as f64) / (u64::MAX as f64);
            
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos();
            mean + std_dev * z
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_baseline() -> PerformanceBaseline {
        PerformanceBaseline {
            operation_type: "test_op".to_string(),
            created_at: SystemTime::now(),
            sample_count: 100,
            mean_duration_micros: 1000.0,
            std_deviation_micros: 100.0,
            p50_duration_micros: 1000,
            p95_duration_micros: 1200,
            p99_duration_micros: 1300,
            mean_throughput: 100.0,
            memory_baseline: 1024,
            cpu_baseline: 5.0,
            confidence_interval: (950.0, 1050.0),
        }
    }

    fn create_test_metric(duration_micros: u64) -> PerformanceMetric {
        PerformanceMetric {
            timestamp: SystemTime::now(),
            operation_type: "test_op".to_string(),
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
    fn test_statistical_analyzer_creation() {
        let config = RegressionDetectorConfig::default();
        let _analyzer = StatisticalAnalyzer::new(config);
    }

    #[test]
    fn test_degradation_calculation() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        let baseline = create_test_baseline();
        let metric = create_test_metric(1200); // 20% slower
        
        let degradation = analyzer.calculate_degradation_percentage(&metric, &baseline);
        assert!((degradation - 0.2).abs() < 0.001); // Should be ~20%
    }

    #[test]
    fn test_z_score_test() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        let baseline = create_test_baseline();
        let metric = create_test_metric(1300); // 3 standard deviations from mean
        
        let result = analyzer.z_score_test(&metric, &baseline).unwrap();
        assert!(result.is_significant);
        assert!(result.test_statistic > 2.0); // Should be ~3.0
    }

    #[test]
    fn test_normal_cdf() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        // Test known values
        assert!((analyzer.normal_cdf(0.0) - 0.5).abs() < 0.001);
        assert!(analyzer.normal_cdf(1.96) > 0.975); // ~97.5% for z=1.96
        assert!(analyzer.normal_cdf(-1.96) < 0.025); // ~2.5% for z=-1.96
    }

    #[test]
    fn test_regression_analysis_no_regression() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        let baseline = create_test_baseline();
        let metric = create_test_metric(1050); // Small change within threshold
        
        let result = analyzer.analyze_regression(&metric, &baseline).unwrap();
        assert!(result.is_none()); // Should not detect regression
    }

    #[test]
    fn test_regression_analysis_with_regression() {
        let mut config = RegressionDetectorConfig::default();
        config.detection_sensitivity = 0.1; // 10% threshold
        let analyzer = StatisticalAnalyzer::new(config);
        
        let baseline = create_test_baseline();
        let metric = create_test_metric(1500); // 50% slower - clear regression
        
        let result = analyzer.analyze_regression(&metric, &baseline).unwrap();
        assert!(result.is_some());
        
        let regression = result.unwrap();
        assert!(regression.detected);
        assert!(regression.degradation_percentage > 0.4); // Should be ~50%
        assert!(matches!(regression.severity, RegressionSeverity::Major));
    }

    #[test]
    fn test_severity_determination() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        assert_eq!(analyzer.determine_severity(0.1), RegressionSeverity::Minor);
        assert_eq!(analyzer.determine_severity(0.3), RegressionSeverity::Moderate);
        assert_eq!(analyzer.determine_severity(0.5), RegressionSeverity::Major);
        assert_eq!(analyzer.determine_severity(0.8), RegressionSeverity::Critical);
    }

    #[test]
    fn test_multiple_statistical_tests() {
        let config = RegressionDetectorConfig::default();
        let analyzer = StatisticalAnalyzer::new(config);
        
        let baseline = create_test_baseline();
        let metric = create_test_metric(1400); // Clear regression
        
        let analysis_config = RegressionAnalysisConfig::default();
        let test_results = analyzer.perform_statistical_tests(&metric, &baseline, &analysis_config).unwrap();
        
        assert!(!test_results.is_empty());
        assert!(test_results.iter().any(|r| r.is_significant));
    }
}
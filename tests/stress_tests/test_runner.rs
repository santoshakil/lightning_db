use super::{
    endurance_tests::{EnduranceRunner, EnduranceMetrics, PerformanceAnalysis},
    stress_limits_tests::{StressLimitsTester, StressTestResult},
    chaos_tests::{ChaosTestEngine, ChaosTestResult},
    recovery_tests::{RecoveryTestSuite, RecoveryTestResult},
    compatibility_tests::{CompatibilityTestSuite, CompatibilityTestResult},
};

use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use std::fs;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use tempfile::tempdir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StressTestSummary {
    pub test_suite: String,
    pub start_time: u64,
    pub end_time: u64,
    pub duration_seconds: u64,
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub success_rate: f64,
    pub overall_score: f64,
    pub system_info: SystemInfo,
    pub test_results: HashMap<String, TestSuiteResult>,
    pub performance_metrics: PerformanceMetrics,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub cpu_cores: usize,
    pub memory_gb: f64,
    pub rust_version: String,
    pub database_version: String,
    pub test_timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestSuiteResult {
    pub suite_name: String,
    pub success: bool,
    pub duration_ms: u64,
    pub tests_passed: usize,
    pub tests_failed: usize,
    pub performance_score: f64,
    pub key_metrics: HashMap<String, f64>,
    pub issues_found: Vec<String>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub peak_throughput_ops_sec: f64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub memory_usage_peak_mb: f64,
    pub memory_usage_stable_mb: f64,
    pub error_rate_percent: f64,
    pub recovery_time_ms: u64,
    pub stability_score: f64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            peak_throughput_ops_sec: 0.0,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            memory_usage_peak_mb: 0.0,
            memory_usage_stable_mb: 0.0,
            error_rate_percent: 0.0,
            recovery_time_ms: 0,
            stability_score: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StressTestConfig {
    pub run_endurance_tests: bool,
    pub endurance_duration_hours: u64,
    pub run_stress_limits: bool,
    pub run_chaos_tests: bool,
    pub run_recovery_tests: bool,
    pub run_compatibility_tests: bool,
    pub output_format: OutputFormat,
    pub save_detailed_metrics: bool,
    pub test_database_config: LightningDbConfig,
}

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Console,
    Json,
    Html,
    All,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            run_endurance_tests: false, // Disabled by default due to long runtime
            endurance_duration_hours: 1,
            run_stress_limits: true,
            run_chaos_tests: true,
            run_recovery_tests: true,
            run_compatibility_tests: true,
            output_format: OutputFormat::Console,
            save_detailed_metrics: true,
            test_database_config: LightningDbConfig {
                cache_size: 100 * 1024 * 1024,
                use_improved_wal: true,
                wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
                prefetch_enabled: true,
                compression_enabled: true,
                ..Default::default()
            },
        }
    }
}

pub struct StressTestRunner {
    config: StressTestConfig,
    test_dir: PathBuf,
    start_time: Instant,
    system_info: SystemInfo,
}

impl StressTestRunner {
    pub fn new(config: StressTestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let test_dir = tempdir()?.into_path();
        let system_info = Self::detect_system_info();

        Ok(Self {
            config,
            test_dir,
            start_time: Instant::now(),
            system_info,
        })
    }

    fn detect_system_info() -> SystemInfo {
        SystemInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_cores: num_cpus::get(),
            memory_gb: Self::get_total_memory_gb(),
            rust_version: Self::get_rust_version(),
            database_version: "0.1.0".to_string(), // TODO: Get from Cargo.toml
            test_timestamp: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        }
    }

    fn get_total_memory_gb() -> f64 {
        #[cfg(not(target_env = "msvc"))]
        {
            sysinfo::System::new_all().total_memory() as f64 / 1024.0 / 1024.0 / 1024.0
        }
        #[cfg(target_env = "msvc")]
        {
            8.0 // Fallback estimate
        }
    }

    fn get_rust_version() -> String {
        format!("{}", std::env!("RUSTC_VERSION"))
    }

    pub fn run_all_tests(&mut self) -> Result<StressTestSummary, Box<dyn std::error::Error>> {
        println!("🚀 Starting Lightning DB Stress Test Suite");
        println!("===========================================");
        println!("System: {} {} with {} cores, {:.1} GB RAM", 
            self.system_info.os, self.system_info.arch, 
            self.system_info.cpu_cores, self.system_info.memory_gb);
        println!("Rust version: {}", self.system_info.rust_version);
        println!("Test started at: {}", self.system_info.test_timestamp);
        println!();

        let mut test_results = HashMap::new();
        let mut overall_performance = PerformanceMetrics::default();
        let mut total_tests = 0;
        let mut passed_tests = 0;

        // Run compatibility tests first
        if self.config.run_compatibility_tests {
            let result = self.run_compatibility_tests()?;
            total_tests += result.tests_passed + result.tests_failed;
            passed_tests += result.tests_passed;
            test_results.insert("compatibility".to_string(), result);
        }

        // Run stress limits tests
        if self.config.run_stress_limits {
            let result = self.run_stress_limits_tests()?;
            total_tests += result.tests_passed + result.tests_failed;
            passed_tests += result.tests_passed;
            self.update_performance_metrics(&mut overall_performance, &result);
            test_results.insert("stress_limits".to_string(), result);
        }

        // Run recovery tests
        if self.config.run_recovery_tests {
            let result = self.run_recovery_tests()?;
            total_tests += result.tests_passed + result.tests_failed;
            passed_tests += result.tests_passed;
            self.update_performance_metrics(&mut overall_performance, &result);
            test_results.insert("recovery".to_string(), result);
        }

        // Run chaos tests
        if self.config.run_chaos_tests {
            let result = self.run_chaos_tests()?;
            total_tests += result.tests_passed + result.tests_failed;
            passed_tests += result.tests_passed;
            test_results.insert("chaos".to_string(), result);
        }

        // Run endurance tests (if enabled)
        if self.config.run_endurance_tests {
            let result = self.run_endurance_tests()?;
            total_tests += result.tests_passed + result.tests_failed;
            passed_tests += result.tests_passed;
            self.update_performance_metrics(&mut overall_performance, &result);
            test_results.insert("endurance".to_string(), result);
        }

        let end_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let start_time_unix = end_time - self.start_time.elapsed().as_secs();
        let duration = self.start_time.elapsed().as_secs();

        let success_rate = if total_tests > 0 {
            passed_tests as f64 / total_tests as f64
        } else {
            0.0
        };

        let overall_score = self.calculate_overall_score(&test_results, success_rate);
        let recommendations = self.generate_recommendations(&test_results, &overall_performance);

        let summary = StressTestSummary {
            test_suite: "Lightning DB Production Stress Tests".to_string(),
            start_time: start_time_unix,
            end_time,
            duration_seconds: duration,
            total_tests,
            passed_tests,
            failed_tests: total_tests - passed_tests,
            success_rate,
            overall_score,
            system_info: self.system_info.clone(),
            test_results,
            performance_metrics: overall_performance,
            recommendations,
        };

        self.output_results(&summary)?;

        Ok(summary)
    }

    fn run_compatibility_tests(&self) -> Result<TestSuiteResult, Box<dyn std::error::Error>> {
        println!("🔍 Running Compatibility Tests...");
        let start = Instant::now();

        let suite = CompatibilityTestSuite::new();
        let results = suite.run_compatibility_test_suite();

        let mut passed = 0;
        let mut failed = 0;
        let mut performance_sum = 0.0;
        let mut key_metrics = HashMap::new();
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();

        for result in &results {
            if result.success {
                passed += 1;
            } else {
                failed += 1;
                issues.push(format!("{}: {}", result.test_name, 
                    result.error_details.as_deref().unwrap_or("failed")));
            }
            performance_sum += result.performance_baseline;
        }

        if failed > 0 {
            recommendations.push("Some compatibility tests failed. Check platform-specific requirements.".to_string());
        }

        let duration = start.elapsed();
        println!("   ✓ Compatibility tests completed in {:?}", duration);

        Ok(TestSuiteResult {
            suite_name: "Compatibility".to_string(),
            success: failed == 0,
            duration_ms: duration.as_millis() as u64,
            tests_passed: passed,
            tests_failed: failed,
            performance_score: if results.is_empty() { 0.0 } else { performance_sum / results.len() as f64 },
            key_metrics,
            issues_found: issues,
            recommendations,
        })
    }

    fn run_stress_limits_tests(&self) -> Result<TestSuiteResult, Box<dyn std::error::Error>> {
        println!("💪 Running Stress Limits Tests...");
        let start = Instant::now();

        let db = std::sync::Arc::new(Database::open(&self.test_dir, self.config.test_database_config.clone())?);
        let tester = StressLimitsTester::new(db);

        let mut results = Vec::new();
        let mut key_metrics = HashMap::new();

        // Run individual stress tests
        println!("   Testing maximum concurrent connections...");
        let conn_result = tester.test_max_concurrent_connections();
        results.push(("max_connections", conn_result));

        println!("   Testing maximum transaction rate...");
        let tx_result = tester.test_max_transaction_rate();
        results.push(("max_tx_rate", tx_result));

        println!("   Testing key/value size limits...");
        let size_results = tester.test_max_key_value_sizes();
        for (test_name, result) in size_results {
            results.push((test_name.as_str(), result));
        }

        println!("   Testing resource exhaustion scenarios...");
        let resource_results = tester.test_resource_exhaustion_scenarios();
        for (test_name, result) in resource_results {
            results.push((test_name.as_str(), result));
        }

        let mut passed = 0;
        let mut failed = 0;
        let mut max_throughput = 0.0;
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();

        for (test_name, result) in &results {
            if result.max_achieved > 0 && result.error_rate < 0.1 {
                passed += 1;
            } else {
                failed += 1;
                issues.push(format!("{}: achieved {}, error rate {:.2}%", 
                    test_name, result.max_achieved, result.error_rate * 100.0));
            }

            max_throughput = max_throughput.max(result.throughput_ops_sec);
            key_metrics.insert(format!("{}_max_achieved", test_name), result.max_achieved as f64);
            key_metrics.insert(format!("{}_throughput", test_name), result.throughput_ops_sec);
        }

        if failed > 0 {
            recommendations.push("Some stress limits were reached. Consider increasing system resources.".to_string());
        }

        let duration = start.elapsed();
        println!("   ✓ Stress limits tests completed in {:?}", duration);

        Ok(TestSuiteResult {
            suite_name: "Stress Limits".to_string(),
            success: failed == 0,
            duration_ms: duration.as_millis() as u64,
            tests_passed: passed,
            tests_failed: failed,
            performance_score: max_throughput,
            key_metrics,
            issues_found: issues,
            recommendations,
        })
    }

    fn run_recovery_tests(&self) -> Result<TestSuiteResult, Box<dyn std::error::Error>> {
        println!("🔄 Running Recovery Tests...");
        let start = Instant::now();

        let recovery_suite = RecoveryTestSuite::new(self.test_dir.clone(), self.config.test_database_config.clone());
        let results = recovery_suite.run_recovery_test_suite();

        let mut passed = 0;
        let mut failed = 0;
        let mut key_metrics = HashMap::new();
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();
        let mut total_recovery_time = 0u64;
        let mut avg_data_recovery = 0.0;

        for result in &results {
            if result.integrity_check_passed && result.data_recovery_rate > 0.8 {
                passed += 1;
            } else {
                failed += 1;
                issues.push(format!("{}: recovery_rate={:.1}%, integrity={}", 
                    result.test_name, result.data_recovery_rate * 100.0, result.integrity_check_passed));
            }

            total_recovery_time += result.recovery_time_ms;
            avg_data_recovery += result.data_recovery_rate;

            key_metrics.insert(format!("{}_recovery_time_ms", result.test_name), result.recovery_time_ms as f64);
            key_metrics.insert(format!("{}_data_recovery_rate", result.test_name), result.data_recovery_rate);
        }

        if !results.is_empty() {
            avg_data_recovery /= results.len() as f64;
            key_metrics.insert("avg_recovery_time_ms".to_string(), total_recovery_time as f64 / results.len() as f64);
            key_metrics.insert("avg_data_recovery_rate".to_string(), avg_data_recovery);
        }

        if failed > 0 {
            recommendations.push("Some recovery tests failed. Check WAL and backup configurations.".to_string());
        }

        let duration = start.elapsed();
        println!("   ✓ Recovery tests completed in {:?}", duration);

        Ok(TestSuiteResult {
            suite_name: "Recovery".to_string(),
            success: failed == 0,
            duration_ms: duration.as_millis() as u64,
            tests_passed: passed,
            tests_failed: failed,
            performance_score: avg_data_recovery * 100.0,
            key_metrics,
            issues_found: issues,
            recommendations,
        })
    }

    fn run_chaos_tests(&self) -> Result<TestSuiteResult, Box<dyn std::error::Error>> {
        println!("🌪️  Running Chaos Tests...");
        let start = Instant::now();

        let chaos_engine = ChaosTestEngine::new(self.test_dir.clone(), self.config.test_database_config.clone());
        let results = chaos_engine.run_chaos_test_suite();

        let mut passed = 0;
        let mut failed = 0;
        let mut key_metrics = HashMap::new();
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();
        let mut avg_integrity = 0.0;

        for result in &results {
            if result.success {
                passed += 1;
            } else {
                failed += 1;
                issues.push(format!("{}: integrity={:.1}%{}", 
                    result.test_name, 
                    result.data_integrity_score * 100.0,
                    if let Some(ref error) = result.error_details { 
                        format!(", error={}", error) 
                    } else { 
                        String::new() 
                    }));
            }

            avg_integrity += result.data_integrity_score;
            key_metrics.insert(format!("{}_integrity_score", result.test_name), result.data_integrity_score);
            key_metrics.insert(format!("{}_recovery_time_ms", result.test_name), result.recovery_time_ms as f64);
        }

        if !results.is_empty() {
            avg_integrity /= results.len() as f64;
            key_metrics.insert("avg_integrity_score".to_string(), avg_integrity);
        }

        if failed > 0 {
            recommendations.push("Some chaos tests failed. Database may not be resilient to all failure modes.".to_string());
        }

        let duration = start.elapsed();
        println!("   ✓ Chaos tests completed in {:?}", duration);

        Ok(TestSuiteResult {
            suite_name: "Chaos".to_string(),
            success: failed == 0,
            duration_ms: duration.as_millis() as u64,
            tests_passed: passed,
            tests_failed: failed,
            performance_score: avg_integrity * 100.0,
            key_metrics,
            issues_found: issues,
            recommendations,
        })
    }

    fn run_endurance_tests(&self) -> Result<TestSuiteResult, Box<dyn std::error::Error>> {
        println!("⏱️  Running Endurance Tests ({} hours)...", self.config.endurance_duration_hours);
        let start = Instant::now();

        let db = std::sync::Arc::new(Database::open(&self.test_dir, self.config.test_database_config.clone())?);
        let runner = EnduranceRunner::new(db);

        let metrics_history = runner.start_endurance_test(self.config.endurance_duration_hours)?;
        let analysis = runner.analyze_performance_degradation();

        let mut key_metrics = HashMap::new();
        let mut issues = Vec::new();
        let mut recommendations = Vec::new();

        // Analyze results
        let passed = if analysis.throughput_degradation_percent < 20.0 && 
                        analysis.memory_growth_percent < 50.0 && 
                        analysis.stability_score > 0.9 { 1 } else { 0 };
        let failed = 1 - passed;

        if analysis.throughput_degradation_percent > 15.0 {
            issues.push(format!("Throughput degraded by {:.1}%", analysis.throughput_degradation_percent));
        }

        if analysis.memory_growth_percent > 30.0 {
            issues.push(format!("Memory grew by {:.1}%", analysis.memory_growth_percent));
            recommendations.push("Potential memory leak detected. Review memory management.".to_string());
        }

        key_metrics.insert("throughput_degradation_percent".to_string(), analysis.throughput_degradation_percent);
        key_metrics.insert("memory_growth_percent".to_string(), analysis.memory_growth_percent);
        key_metrics.insert("stability_score".to_string(), analysis.stability_score);

        if let Some(final_metrics) = metrics_history.last() {
            key_metrics.insert("final_operations_completed".to_string(), final_metrics.operations_completed as f64);
            key_metrics.insert("final_throughput_ops_sec".to_string(), final_metrics.throughput_ops_sec);
        }

        let duration = start.elapsed();
        println!("   ✓ Endurance tests completed in {:?}", duration);

        Ok(TestSuiteResult {
            suite_name: "Endurance".to_string(),
            success: passed > 0,
            duration_ms: duration.as_millis() as u64,
            tests_passed: passed,
            tests_failed: failed,
            performance_score: analysis.stability_score * 100.0,
            key_metrics,
            issues_found: issues,
            recommendations,
        })
    }

    fn update_performance_metrics(&self, overall: &mut PerformanceMetrics, suite_result: &TestSuiteResult) {
        overall.peak_throughput_ops_sec = overall.peak_throughput_ops_sec.max(suite_result.performance_score);
        
        if let Some(&recovery_time) = suite_result.key_metrics.get("avg_recovery_time_ms") {
            overall.recovery_time_ms = overall.recovery_time_ms.max(recovery_time as u64);
        }
        
        if let Some(&stability) = suite_result.key_metrics.get("stability_score") {
            overall.stability_score = overall.stability_score.max(stability);
        }

        // Update memory usage
        overall.memory_usage_peak_mb = overall.memory_usage_peak_mb.max(Self::get_current_memory_usage());
    }

    fn calculate_overall_score(&self, test_results: &HashMap<String, TestSuiteResult>, success_rate: f64) -> f64 {
        let mut score = success_rate * 100.0;
        
        // Adjust score based on performance
        let mut performance_factor = 1.0;
        
        for result in test_results.values() {
            if result.suite_name == "Stress Limits" && result.performance_score > 10000.0 {
                performance_factor += 0.1; // Bonus for high performance
            }
            if result.suite_name == "Recovery" && result.performance_score > 95.0 {
                performance_factor += 0.1; // Bonus for good recovery
            }
            if result.suite_name == "Chaos" && result.performance_score > 90.0 {
                performance_factor += 0.1; // Bonus for chaos resilience
            }
        }
        
        score *= performance_factor;
        score.min(100.0) // Cap at 100
    }

    fn generate_recommendations(&self, test_results: &HashMap<String, TestSuiteResult>, metrics: &PerformanceMetrics) -> Vec<String> {
        let mut recommendations = Vec::new();

        // Collect recommendations from individual test suites
        for result in test_results.values() {
            recommendations.extend(result.recommendations.clone());
        }

        // General performance recommendations
        if metrics.peak_throughput_ops_sec < 1000.0 {
            recommendations.push("Consider increasing cache size or optimizing hardware for better performance.".to_string());
        }

        if metrics.recovery_time_ms > 5000 {
            recommendations.push("Recovery time is high. Consider faster storage or optimized WAL settings.".to_string());
        }

        if metrics.stability_score < 0.9 {
            recommendations.push("System stability could be improved. Review error handling and resource management.".to_string());
        }

        // System-specific recommendations
        if self.system_info.memory_gb < 4.0 {
            recommendations.push("System has limited memory. Consider reducing cache sizes for production deployment.".to_string());
        }

        if self.system_info.cpu_cores < 4 {
            recommendations.push("Limited CPU cores detected. Monitor CPU usage under load.".to_string());
        }

        // Remove duplicates
        recommendations.sort();
        recommendations.dedup();

        if recommendations.is_empty() {
            recommendations.push("All tests passed successfully! The database is ready for production use.".to_string());
        }

        recommendations
    }

    fn output_results(&self, summary: &StressTestSummary) -> Result<(), Box<dyn std::error::Error>> {
        match self.config.output_format {
            OutputFormat::Console => self.output_console(summary),
            OutputFormat::Json => self.output_json(summary)?,
            OutputFormat::Html => self.output_html(summary)?,
            OutputFormat::All => {
                self.output_console(summary);
                self.output_json(summary)?;
                self.output_html(summary)?;
            }
        }
        Ok(())
    }

    fn output_console(&self, summary: &StressTestSummary) {
        println!("\n🎯 Stress Test Summary");
        println!("======================");
        println!("Duration: {}s ({:.1} minutes)", summary.duration_seconds, summary.duration_seconds as f64 / 60.0);
        println!("Success rate: {:.1}% ({}/{} tests passed)", 
            summary.success_rate * 100.0, summary.passed_tests, summary.total_tests);
        println!("Overall score: {:.1}/100", summary.overall_score);
        println!();

        println!("📊 Performance Metrics:");
        println!("  Peak throughput: {:.0} ops/sec", summary.performance_metrics.peak_throughput_ops_sec);
        println!("  Recovery time: {} ms", summary.performance_metrics.recovery_time_ms);
        println!("  Stability score: {:.3}", summary.performance_metrics.stability_score);
        println!("  Peak memory usage: {:.1} MB", summary.performance_metrics.memory_usage_peak_mb);
        println!();

        println!("🔍 Test Suite Results:");
        for (suite_name, result) in &summary.test_results {
            let status = if result.success { "✓" } else { "✗" };
            println!("  {} {}: {}/{} tests passed, score: {:.1}, duration: {}ms", 
                status, suite_name, result.tests_passed, 
                result.tests_passed + result.tests_failed, 
                result.performance_score, result.duration_ms);
        }
        println!();

        if !summary.recommendations.is_empty() {
            println!("💡 Recommendations:");
            for (i, rec) in summary.recommendations.iter().enumerate() {
                println!("  {}. {}", i + 1, rec);
            }
            println!();
        }

        // Show issues if any
        for (suite_name, result) in &summary.test_results {
            if !result.issues_found.is_empty() {
                println!("⚠️  Issues found in {}:", suite_name);
                for issue in &result.issues_found {
                    println!("  - {}", issue);
                }
                println!();
            }
        }
    }

    fn output_json(&self, summary: &StressTestSummary) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(summary)?;
        let filename = format!("stress_test_results_{}.json", 
            chrono::Utc::now().format("%Y%m%d_%H%M%S"));
        fs::write(&filename, json)?;
        println!("📄 Results saved to: {}", filename);
        Ok(())
    }

    fn output_html(&self, summary: &StressTestSummary) -> Result<(), Box<dyn std::error::Error>> {
        let html = self.generate_html_report(summary);
        let filename = format!("stress_test_results_{}.html", 
            chrono::Utc::now().format("%Y%m%d_%H%M%S"));
        fs::write(&filename, html)?;
        println!("🌐 HTML report saved to: {}", filename);
        Ok(())
    }

    fn generate_html_report(&self, summary: &StressTestSummary) -> String {
        format!(r#"
<!DOCTYPE html>
<html>
<head>
    <title>Lightning DB Stress Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .metric {{ background: #f9f9f9; padding: 10px; margin: 10px 0; border-radius: 3px; }}
        .success {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .error {{ color: #dc3545; }}
        .test-suite {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .score-high {{ background-color: #d4edda; }}
        .score-medium {{ background-color: #fff3cd; }}
        .score-low {{ background-color: #f8d7da; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Lightning DB Stress Test Report</h1>
        <p><strong>Generated:</strong> {}</p>
        <p><strong>System:</strong> {} {} ({} cores, {:.1} GB RAM)</p>
        <p><strong>Duration:</strong> {}s ({:.1} minutes)</p>
    </div>

    <div class="metric">
        <h2>Overall Results</h2>
        <p><strong>Success Rate:</strong> <span class="{}">{:.1}% ({}/{} tests)</span></p>
        <p><strong>Overall Score:</strong> <span class="{}">{:.1}/100</span></p>
    </div>

    <div class="metric">
        <h2>Performance Metrics</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>
            <tr><td>Peak Throughput</td><td>{:.0} ops/sec</td></tr>
            <tr><td>Recovery Time</td><td>{} ms</td></tr>
            <tr><td>Stability Score</td><td>{:.3}</td></tr>
            <tr><td>Peak Memory Usage</td><td>{:.1} MB</td></tr>
        </table>
    </div>

    <h2>Test Suite Results</h2>
    {}

    <div class="metric">
        <h2>Recommendations</h2>
        <ul>
            {}
        </ul>
    </div>
</body>
</html>
        "#,
            summary.system_info.test_timestamp,
            summary.system_info.os, summary.system_info.arch,
            summary.system_info.cpu_cores, summary.system_info.memory_gb,
            summary.duration_seconds, summary.duration_seconds as f64 / 60.0,
            if summary.success_rate > 0.9 { "success" } else if summary.success_rate > 0.7 { "warning" } else { "error" },
            summary.success_rate * 100.0, summary.passed_tests, summary.total_tests,
            if summary.overall_score > 80.0 { "success" } else if summary.overall_score > 60.0 { "warning" } else { "error" },
            summary.overall_score,
            summary.performance_metrics.peak_throughput_ops_sec,
            summary.performance_metrics.recovery_time_ms,
            summary.performance_metrics.stability_score,
            summary.performance_metrics.memory_usage_peak_mb,
            self.generate_test_suite_html(&summary.test_results),
            summary.recommendations.iter().map(|r| format!("<li>{}</li>", r)).collect::<Vec<_>>().join("")
        )
    }

    fn generate_test_suite_html(&self, test_results: &HashMap<String, TestSuiteResult>) -> String {
        let mut html = String::new();
        
        for (suite_name, result) in test_results {
            let status_class = if result.success { "success" } else { "error" };
            let score_class = if result.performance_score > 80.0 { "score-high" } 
                            else if result.performance_score > 60.0 { "score-medium" } 
                            else { "score-low" };
            
            html.push_str(&format!(r#"
                <div class="test-suite">
                    <h3 class="{}">{}</h3>
                    <p><strong>Status:</strong> {}/{} tests passed</p>
                    <p><strong>Performance Score:</strong> <span class="{}">{:.1}</span></p>
                    <p><strong>Duration:</strong> {} ms</p>
                    {}
                    {}
                </div>
            "#,
                status_class, suite_name,
                result.tests_passed, result.tests_passed + result.tests_failed,
                score_class, result.performance_score,
                result.duration_ms,
                if result.issues_found.is_empty() { String::new() } else {
                    format!("<p><strong>Issues:</strong></p><ul>{}</ul>", 
                        result.issues_found.iter().map(|i| format!("<li>{}</li>", i)).collect::<Vec<_>>().join(""))
                },
                if result.recommendations.is_empty() { String::new() } else {
                    format!("<p><strong>Suite Recommendations:</strong></p><ul>{}</ul>", 
                        result.recommendations.iter().map(|r| format!("<li>{}</li>", r)).collect::<Vec<_>>().join(""))
                }
            ));
        }
        
        html
    }

    fn get_current_memory_usage() -> f64 {
        #[cfg(not(target_env = "msvc"))]
        {
            tikv_jemalloc_ctl::stats::allocated::read()
                .map(|bytes| bytes as f64 / 1024.0 / 1024.0)
                .unwrap_or(0.0)
        }
        #[cfg(target_env = "msvc")]
        {
            0.0
        }
    }
}

// Main stress test runner function
#[test]
#[ignore = "Full stress test suite - run with: cargo test test_full_stress_suite -- --ignored"]
fn test_full_stress_suite() {
    let config = StressTestConfig {
        run_endurance_tests: false, // Disable for CI/CD
        endurance_duration_hours: 1,
        run_stress_limits: true,
        run_chaos_tests: true,
        run_recovery_tests: true,
        run_compatibility_tests: true,
        output_format: OutputFormat::All,
        save_detailed_metrics: true,
        ..Default::default()
    };

    let mut runner = StressTestRunner::new(config).unwrap();
    let summary = runner.run_all_tests().unwrap();

    // Assert minimum quality thresholds
    assert!(summary.success_rate >= 0.8, "Overall success rate too low: {:.1}%", summary.success_rate * 100.0);
    assert!(summary.overall_score >= 70.0, "Overall score too low: {:.1}", summary.overall_score);
    
    // Assert that critical test suites passed
    assert!(summary.test_results.get("compatibility").map_or(true, |r| r.success), 
        "Compatibility tests failed");
    assert!(summary.test_results.get("recovery").map_or(true, |r| r.success), 
        "Recovery tests failed");
}

#[test]
#[ignore = "Quick stress test - run with: cargo test test_quick_stress -- --ignored"]
fn test_quick_stress() {
    let config = StressTestConfig {
        run_endurance_tests: false,
        run_stress_limits: true,
        run_chaos_tests: false,
        run_recovery_tests: true,
        run_compatibility_tests: true,
        output_format: OutputFormat::Console,
        ..Default::default()
    };

    let mut runner = StressTestRunner::new(config).unwrap();
    let summary = runner.run_all_tests().unwrap();

    assert!(summary.success_rate >= 0.7, "Quick stress test success rate too low: {:.1}%", summary.success_rate * 100.0);
}
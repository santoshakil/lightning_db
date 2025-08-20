use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

/// Performance report data structures
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub timestamp: String,
    pub environment: EnvironmentInfo,
    pub targets: PerformanceTargets,
    pub core_performance: CorePerformanceResults,
    pub latency_analysis: LatencyAnalysis,
    pub scalability_analysis: ScalabilityAnalysis,
    pub workload_results: WorkloadResults,
    pub validation_summary: ValidationSummary,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub cpu_cores: usize,
    pub memory_gb: f64,
    pub os: String,
    pub architecture: String,
    pub rust_version: String,
    pub database_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceTargets {
    pub read_ops_per_sec: f64,
    pub write_ops_per_sec: f64,
    pub latency_p99_us: f64,
    pub memory_usage_gb: f64,
    pub cpu_utilization_percent: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CorePerformanceResults {
    pub sequential_reads: ThroughputResult,
    pub random_reads: ThroughputResult,
    pub sequential_writes: ThroughputResult,
    pub random_writes: ThroughputResult,
    pub mixed_workload: HashMap<String, ThroughputResult>,
    pub transaction_throughput: ThroughputResult,
    pub range_scans: ThroughputResult,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ThroughputResult {
    pub single_thread_ops_per_sec: f64,
    pub multi_thread_ops_per_sec: f64,
    pub scaling_efficiency: f64, // multi_thread / (single_thread * thread_count)
    pub meets_target: bool,
    pub target_ratio: f64, // actual / target
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyAnalysis {
    pub read_latency: LatencyStats,
    pub write_latency: LatencyStats,
    pub latency_under_load: HashMap<String, LatencyStats>,
    pub jitter_analysis: JitterStats,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub mean_us: f64,
    pub max_us: f64,
    pub meets_p99_target: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JitterStats {
    pub p99_p95_diff_us: f64,
    pub p95_p90_diff_us: f64,
    pub coefficient_of_variation: f64,
    pub is_stable: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScalabilityAnalysis {
    pub thread_scaling: ThreadScalingResults,
    pub database_size_scaling: DatabaseSizeResults,
    pub connection_scaling: ConnectionScalingResults,
    pub memory_scaling: MemoryScalingResults,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ThreadScalingResults {
    pub linear_scaling_efficiency: HashMap<usize, f64>,
    pub optimal_thread_count: usize,
    pub scaling_coefficient: f64, // How well it scales (1.0 = perfect linear)
    pub diminishing_returns_point: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseSizeResults {
    pub performance_by_size: HashMap<String, f64>, // Size -> ops/sec
    pub scaling_factor: f64, // How performance degrades with size
    pub memory_efficiency: f64, // Working set / database size
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectionScalingResults {
    pub max_concurrent_connections: usize,
    pub performance_degradation: f64, // % degradation at max connections
    pub connection_overhead_us: f64, // Average overhead per connection
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryScalingResults {
    pub peak_memory_usage_gb: f64,
    pub memory_efficiency_ratio: f64, // Useful data / total memory
    pub gc_pressure: f64, // Memory allocation rate
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadResults {
    pub oltp_tpcc: WorkloadResult,
    pub analytics_tpch: WorkloadResult,
    pub key_value_ycsb: HashMap<String, WorkloadResult>, // Workload A, B, C
    pub time_series: WorkloadResult,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkloadResult {
    pub throughput_ops_per_sec: f64,
    pub latency_p95_us: f64,
    pub cpu_utilization_percent: f64,
    pub memory_usage_gb: f64,
    pub io_utilization_percent: f64,
    pub realistic_performance_rating: String, // Excellent, Good, Acceptable, Poor
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub targets_met: HashMap<String, bool>,
    pub performance_grade: String, // A, B, C, D, F
    pub critical_issues: Vec<String>,
    pub warnings: Vec<String>,
    pub strengths: Vec<String>,
}

impl Default for PerformanceTargets {
    fn default() -> Self {
        Self {
            read_ops_per_sec: 14_000_000.0,
            write_ops_per_sec: 350_000.0,
            latency_p99_us: 1000.0,
            memory_usage_gb: 1.0,
            cpu_utilization_percent: 80.0,
        }
    }
}

/// Performance report generator
pub struct PerformanceReportGenerator {
    targets: PerformanceTargets,
}

impl PerformanceReportGenerator {
    pub fn new() -> Self {
        Self {
            targets: PerformanceTargets::default(),
        }
    }

    pub fn with_targets(targets: PerformanceTargets) -> Self {
        Self { targets }
    }

    /// Generate a comprehensive performance report from benchmark results
    pub fn generate_report(&self, benchmark_data: BenchmarkData) -> PerformanceReport {
        let environment = self.collect_environment_info();
        let core_performance = self.analyze_core_performance(&benchmark_data);
        let latency_analysis = self.analyze_latency(&benchmark_data);
        let scalability_analysis = self.analyze_scalability(&benchmark_data);
        let workload_results = self.analyze_workloads(&benchmark_data);
        let validation_summary = self.validate_performance(&core_performance, &latency_analysis, &workload_results);
        let recommendations = self.generate_recommendations(&validation_summary, &core_performance);

        PerformanceReport {
            timestamp: chrono::Utc::now().to_rfc3339(),
            environment,
            targets: self.targets.clone(),
            core_performance,
            latency_analysis,
            scalability_analysis,
            workload_results,
            validation_summary,
            recommendations,
        }
    }

    fn collect_environment_info(&self) -> EnvironmentInfo {
        EnvironmentInfo {
            cpu_cores: num_cpus::get(),
            memory_gb: self.get_total_memory_gb(),
            os: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            rust_version: env!("RUSTC_VERSION").to_string(),
            database_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn get_total_memory_gb(&self) -> f64 {
        #[cfg(feature = "sys-info")]
        {
            use sysinfo::{System, SystemExt};
            let mut system = System::new_all();
            system.refresh_all();
            system.total_memory() as f64 / (1024.0 * 1024.0 * 1024.0)
        }
        #[cfg(not(feature = "sys-info"))]
        {
            16.0 // Default assumption
        }
    }

    fn analyze_core_performance(&self, data: &BenchmarkData) -> CorePerformanceResults {
        CorePerformanceResults {
            sequential_reads: self.analyze_throughput_result(
                &data.sequential_read_single_thread,
                &data.sequential_read_multi_thread,
                self.targets.read_ops_per_sec,
            ),
            random_reads: self.analyze_throughput_result(
                &data.random_read_single_thread,
                &data.random_read_multi_thread,
                self.targets.read_ops_per_sec,
            ),
            sequential_writes: self.analyze_throughput_result(
                &data.sequential_write_single_thread,
                &data.sequential_write_multi_thread,
                self.targets.write_ops_per_sec,
            ),
            random_writes: self.analyze_throughput_result(
                &data.random_write_single_thread,
                &data.random_write_multi_thread,
                self.targets.write_ops_per_sec,
            ),
            mixed_workload: data.mixed_workload.iter().map(|(k, v)| {
                (k.clone(), self.analyze_single_throughput_result(v, self.targets.read_ops_per_sec))
            }).collect(),
            transaction_throughput: self.analyze_single_throughput_result(
                &data.transaction_throughput,
                self.targets.write_ops_per_sec,
            ),
            range_scans: self.analyze_single_throughput_result(
                &data.range_scans,
                self.targets.read_ops_per_sec,
            ),
        }
    }

    fn analyze_throughput_result(
        &self,
        single_thread: &f64,
        multi_thread: &f64,
        target: f64,
    ) -> ThroughputResult {
        let thread_count = 8.0; // Assume 8 threads for scaling calculation
        let scaling_efficiency = multi_thread / (single_thread * thread_count);
        let meets_target = *multi_thread >= target;
        let target_ratio = multi_thread / target;

        ThroughputResult {
            single_thread_ops_per_sec: *single_thread,
            multi_thread_ops_per_sec: *multi_thread,
            scaling_efficiency,
            meets_target,
            target_ratio,
        }
    }

    fn analyze_single_throughput_result(&self, throughput: &f64, target: f64) -> ThroughputResult {
        ThroughputResult {
            single_thread_ops_per_sec: *throughput,
            multi_thread_ops_per_sec: *throughput,
            scaling_efficiency: 1.0,
            meets_target: *throughput >= target,
            target_ratio: throughput / target,
        }
    }

    fn analyze_latency(&self, data: &BenchmarkData) -> LatencyAnalysis {
        LatencyAnalysis {
            read_latency: self.convert_latency_stats(&data.read_latency),
            write_latency: self.convert_latency_stats(&data.write_latency),
            latency_under_load: data.latency_under_load.iter().map(|(k, v)| {
                (k.clone(), self.convert_latency_stats(v))
            }).collect(),
            jitter_analysis: self.analyze_jitter(&data.read_latency),
        }
    }

    fn convert_latency_stats(&self, stats: &RawLatencyStats) -> LatencyStats {
        LatencyStats {
            p50_us: stats.p50_us,
            p90_us: stats.p90_us,
            p95_us: stats.p95_us,
            p99_us: stats.p99_us,
            p999_us: stats.p999_us,
            mean_us: stats.mean_us,
            max_us: stats.max_us,
            meets_p99_target: stats.p99_us <= self.targets.latency_p99_us,
        }
    }

    fn analyze_jitter(&self, stats: &RawLatencyStats) -> JitterStats {
        let p99_p95_diff = stats.p99_us - stats.p95_us;
        let p95_p90_diff = stats.p95_us - stats.p90_us;
        let coefficient_of_variation = (stats.max_us - stats.mean_us) / stats.mean_us;
        let is_stable = coefficient_of_variation < 2.0 && p99_p95_diff < stats.mean_us;

        JitterStats {
            p99_p95_diff_us: p99_p95_diff,
            p95_p90_diff_us: p95_p90_diff,
            coefficient_of_variation,
            is_stable,
        }
    }

    fn analyze_scalability(&self, data: &BenchmarkData) -> ScalabilityAnalysis {
        ScalabilityAnalysis {
            thread_scaling: self.analyze_thread_scaling(&data.thread_scaling),
            database_size_scaling: self.analyze_database_size_scaling(&data.database_size_scaling),
            connection_scaling: self.analyze_connection_scaling(&data.connection_scaling),
            memory_scaling: self.analyze_memory_scaling(&data.memory_usage),
        }
    }

    fn analyze_thread_scaling(&self, data: &HashMap<usize, f64>) -> ThreadScalingResults {
        let single_thread_perf = data.get(&1).copied().unwrap_or(0.0);
        let linear_scaling_efficiency: HashMap<usize, f64> = data.iter()
            .map(|(&threads, &perf)| {
                let expected = single_thread_perf * threads as f64;
                let efficiency = if expected > 0.0 { perf / expected } else { 0.0 };
                (threads, efficiency)
            })
            .collect();

        let optimal_thread_count = linear_scaling_efficiency
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(&threads, _)| threads)
            .unwrap_or(1);

        let scaling_coefficient = linear_scaling_efficiency.get(&8).copied().unwrap_or(0.0);
        let diminishing_returns_point = self.find_diminishing_returns_point(&linear_scaling_efficiency);

        ThreadScalingResults {
            linear_scaling_efficiency,
            optimal_thread_count,
            scaling_coefficient,
            diminishing_returns_point,
        }
    }

    fn find_diminishing_returns_point(&self, efficiency: &HashMap<usize, f64>) -> usize {
        let mut sorted: Vec<_> = efficiency.iter().collect();
        sorted.sort_by_key(|&(threads, _)| threads);

        for i in 1..sorted.len() {
            let prev_efficiency = sorted[i-1].1;
            let curr_efficiency = sorted[i].1;
            if curr_efficiency < prev_efficiency * 0.9 { // 10% drop in efficiency
                return *sorted[i].0;
            }
        }

        sorted.last().map(|&(threads, _)| *threads).unwrap_or(1)
    }

    fn analyze_database_size_scaling(&self, data: &HashMap<String, f64>) -> DatabaseSizeResults {
        let mut performance_by_size = HashMap::new();
        let mut sizes_and_perfs: Vec<_> = data.iter().collect();
        sizes_and_perfs.sort_by_key(|&(size, _)| size);

        for (size, perf) in &sizes_and_perfs {
            performance_by_size.insert((*size).clone(), **perf);
        }

        let scaling_factor = if sizes_and_perfs.len() >= 2 {
            let first_perf = sizes_and_perfs[0].1;
            let last_perf = sizes_and_perfs[sizes_and_perfs.len() - 1].1;
            last_perf / first_perf
        } else {
            1.0
        };

        DatabaseSizeResults {
            performance_by_size,
            scaling_factor,
            memory_efficiency: 0.8, // Placeholder - would need actual measurement
        }
    }

    fn analyze_connection_scaling(&self, data: &HashMap<usize, f64>) -> ConnectionScalingResults {
        let single_conn_perf = data.get(&1).copied().unwrap_or(0.0);
        let max_connections = *data.keys().max().unwrap_or(&1);
        let max_conn_perf = data.get(&max_connections).copied().unwrap_or(0.0);
        
        let performance_degradation = if single_conn_perf > 0.0 {
            (single_conn_perf - max_conn_perf) / single_conn_perf * 100.0
        } else {
            0.0
        };

        ConnectionScalingResults {
            max_concurrent_connections: max_connections,
            performance_degradation,
            connection_overhead_us: 10.0, // Placeholder - would need actual measurement
        }
    }

    fn analyze_memory_scaling(&self, data: &MemoryUsageData) -> MemoryScalingResults {
        MemoryScalingResults {
            peak_memory_usage_gb: data.peak_usage_gb,
            memory_efficiency_ratio: data.efficiency_ratio,
            gc_pressure: data.allocation_rate_gb_per_sec,
        }
    }

    fn analyze_workloads(&self, data: &BenchmarkData) -> WorkloadResults {
        WorkloadResults {
            oltp_tpcc: self.convert_workload_result(&data.oltp_workload),
            analytics_tpch: self.convert_workload_result(&data.analytics_workload),
            key_value_ycsb: data.key_value_workloads.iter().map(|(k, v)| {
                (k.clone(), self.convert_workload_result(v))
            }).collect(),
            time_series: self.convert_workload_result(&data.time_series_workload),
        }
    }

    fn convert_workload_result(&self, data: &RawWorkloadData) -> WorkloadResult {
        let rating = self.calculate_performance_rating(data.throughput_ops_per_sec, data.latency_p95_us);
        
        WorkloadResult {
            throughput_ops_per_sec: data.throughput_ops_per_sec,
            latency_p95_us: data.latency_p95_us,
            cpu_utilization_percent: data.cpu_utilization_percent,
            memory_usage_gb: data.memory_usage_gb,
            io_utilization_percent: data.io_utilization_percent,
            realistic_performance_rating: rating,
        }
    }

    fn calculate_performance_rating(&self, throughput: f64, latency: f64) -> String {
        let throughput_score = if throughput >= self.targets.read_ops_per_sec * 0.8 { 3 }
                              else if throughput >= self.targets.read_ops_per_sec * 0.6 { 2 }
                              else if throughput >= self.targets.read_ops_per_sec * 0.4 { 1 }
                              else { 0 };

        let latency_score = if latency <= self.targets.latency_p99_us * 0.5 { 3 }
                           else if latency <= self.targets.latency_p99_us { 2 }
                           else if latency <= self.targets.latency_p99_us * 2.0 { 1 }
                           else { 0 };

        let total_score = throughput_score + latency_score;

        match total_score {
            5..=6 => "Excellent".to_string(),
            4 => "Good".to_string(),
            2..=3 => "Acceptable".to_string(),
            1 => "Poor".to_string(),
            _ => "Unacceptable".to_string(),
        }
    }

    fn validate_performance(
        &self,
        core: &CorePerformanceResults,
        latency: &LatencyAnalysis,
        workloads: &WorkloadResults,
    ) -> ValidationSummary {
        let mut targets_met = HashMap::new();
        let mut critical_issues = Vec::new();
        let mut warnings = Vec::new();
        let mut strengths = Vec::new();

        // Check read performance
        targets_met.insert("read_throughput".to_string(), core.random_reads.meets_target);
        if !core.random_reads.meets_target {
            critical_issues.push(format!(
                "Read throughput {:.0} ops/sec below target {:.0} ops/sec",
                core.random_reads.multi_thread_ops_per_sec,
                self.targets.read_ops_per_sec
            ));
        } else {
            strengths.push(format!(
                "Excellent read performance: {:.0} ops/sec ({:.1}x target)",
                core.random_reads.multi_thread_ops_per_sec,
                core.random_reads.target_ratio
            ));
        }

        // Check write performance
        targets_met.insert("write_throughput".to_string(), core.random_writes.meets_target);
        if !core.random_writes.meets_target {
            critical_issues.push(format!(
                "Write throughput {:.0} ops/sec below target {:.0} ops/sec",
                core.random_writes.multi_thread_ops_per_sec,
                self.targets.write_ops_per_sec
            ));
        } else {
            strengths.push(format!(
                "Strong write performance: {:.0} ops/sec ({:.1}x target)",
                core.random_writes.multi_thread_ops_per_sec,
                core.random_writes.target_ratio
            ));
        }

        // Check latency
        targets_met.insert("latency_p99".to_string(), latency.read_latency.meets_p99_target);
        if !latency.read_latency.meets_p99_target {
            critical_issues.push(format!(
                "P99 read latency {:.2} μs exceeds target {:.0} μs",
                latency.read_latency.p99_us,
                self.targets.latency_p99_us
            ));
        } else {
            strengths.push(format!(
                "Low latency: P99 {:.2} μs (target: {:.0} μs)",
                latency.read_latency.p99_us,
                self.targets.latency_p99_us
            ));
        }

        // Check jitter
        if !latency.jitter_analysis.is_stable {
            warnings.push(format!(
                "High latency jitter detected (CV: {:.2})",
                latency.jitter_analysis.coefficient_of_variation
            ));
        }

        // Calculate overall grade
        let targets_met_count = targets_met.values().filter(|&&met| met).count();
        let total_targets = targets_met.len();
        let performance_grade = match targets_met_count * 100 / total_targets {
            90..=100 => "A",
            80..=89 => "B", 
            70..=79 => "C",
            60..=69 => "D",
            _ => "F",
        };

        ValidationSummary {
            targets_met,
            performance_grade: performance_grade.to_string(),
            critical_issues,
            warnings,
            strengths,
        }
    }

    fn generate_recommendations(
        &self,
        validation: &ValidationSummary,
        core: &CorePerformanceResults,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if validation.performance_grade == "F" || validation.performance_grade == "D" {
            recommendations.push("Consider profiling to identify performance bottlenecks".to_string());
            recommendations.push("Review memory allocation patterns and garbage collection".to_string());
        }

        if !validation.targets_met.get("read_throughput").unwrap_or(&true) {
            recommendations.push("Optimize read path with better caching strategies".to_string());
            recommendations.push("Consider read-only replicas for read-heavy workloads".to_string());
        }

        if !validation.targets_met.get("write_throughput").unwrap_or(&true) {
            recommendations.push("Implement write batching to improve write throughput".to_string());
            recommendations.push("Consider async write-ahead logging".to_string());
        }

        if core.random_reads.scaling_efficiency < 0.7 {
            recommendations.push("Investigate lock contention in read path".to_string());
            recommendations.push("Consider lock-free data structures for hot paths".to_string());
        }

        if !validation.warnings.is_empty() {
            recommendations.push("Address latency jitter with more consistent scheduling".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Performance is excellent! Consider stress testing with larger datasets".to_string());
        }

        recommendations
    }

    /// Generate and save performance report
    pub fn save_report(&self, report: &PerformanceReport, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Save JSON report
        let json_content = serde_json::to_string_pretty(report)?;
        fs::write(format!("{}.json", path), json_content)?;

        // Generate markdown report
        let markdown_content = self.generate_markdown_report(report);
        fs::write(format!("{}.md", path), markdown_content)?;

        // Generate HTML report
        let html_content = self.generate_html_report(report);
        fs::write(format!("{}.html", path), html_content)?;

        Ok(())
    }

    fn generate_markdown_report(&self, report: &PerformanceReport) -> String {
        format!(r#"# Lightning DB Performance Report

Generated: {}
Database Version: {}

## Executive Summary

**Performance Grade: {}**

### Targets Met
{}

### Critical Issues
{}

### Recommendations
{}

## Environment
- CPU Cores: {}
- Memory: {:.1} GB
- OS: {} ({})
- Rust Version: {}

## Core Performance Results

### Read Performance
- Sequential Single Thread: {:.0} ops/sec
- Sequential Multi Thread: {:.0} ops/sec
- Random Single Thread: {:.0} ops/sec  
- Random Multi Thread: {:.0} ops/sec
- Scaling Efficiency: {:.1}%

### Write Performance
- Sequential Single Thread: {:.0} ops/sec
- Sequential Multi Thread: {:.0} ops/sec
- Random Single Thread: {:.0} ops/sec
- Random Multi Thread: {:.0} ops/sec
- Scaling Efficiency: {:.1}%

## Latency Analysis

### Read Latency
- P50: {:.2} μs
- P95: {:.2} μs
- P99: {:.2} μs
- P999: {:.2} μs

### Write Latency  
- P50: {:.2} μs
- P95: {:.2} μs
- P99: {:.2} μs
- P999: {:.2} μs

## Real-World Workloads

### OLTP (TPC-C like)
- Throughput: {:.0} ops/sec
- Latency P95: {:.2} μs
- Rating: {}

### Analytics (TPC-H like)
- Throughput: {:.0} ops/sec
- Latency P95: {:.2} μs
- Rating: {}

### Key-Value (YCSB)
- Workload A: {:.0} ops/sec
- Workload B: {:.0} ops/sec
- Workload C: {:.0} ops/sec

### Time Series
- Ingestion: {:.0} ops/sec
- Query Latency P95: {:.2} μs
- Rating: {}

## Scalability Analysis

### Thread Scaling
- Optimal Thread Count: {}
- Scaling Coefficient: {:.2}
- Diminishing Returns Point: {} threads

### Database Size Scaling
- Performance retention: {:.1}%
- Memory efficiency: {:.1}%

## Validation Summary

**Overall Grade: {}**

✅ **Strengths:**
{}

⚠️ **Warnings:**
{}

❌ **Critical Issues:**
{}

---
*Report generated by Lightning DB Performance Suite*
"#,
            report.timestamp,
            report.environment.database_version,
            report.validation_summary.performance_grade,
            report.validation_summary.targets_met.iter()
                .map(|(k, v)| format!("- {}: {}", k, if *v { "✅" } else { "❌" }))
                .collect::<Vec<_>>()
                .join("\n"),
            report.validation_summary.critical_issues.iter()
                .map(|issue| format!("- ❌ {}", issue))
                .collect::<Vec<_>>()
                .join("\n"),
            report.recommendations.iter()
                .map(|rec| format!("- {}", rec))
                .collect::<Vec<_>>()
                .join("\n"),
            report.environment.cpu_cores,
            report.environment.memory_gb,
            report.environment.os,
            report.environment.architecture,
            report.environment.rust_version,
            report.core_performance.sequential_reads.single_thread_ops_per_sec,
            report.core_performance.sequential_reads.multi_thread_ops_per_sec,
            report.core_performance.random_reads.single_thread_ops_per_sec,
            report.core_performance.random_reads.multi_thread_ops_per_sec,
            report.core_performance.random_reads.scaling_efficiency * 100.0,
            report.core_performance.sequential_writes.single_thread_ops_per_sec,
            report.core_performance.sequential_writes.multi_thread_ops_per_sec,
            report.core_performance.random_writes.single_thread_ops_per_sec,
            report.core_performance.random_writes.multi_thread_ops_per_sec,
            report.core_performance.random_writes.scaling_efficiency * 100.0,
            report.latency_analysis.read_latency.p50_us,
            report.latency_analysis.read_latency.p95_us,
            report.latency_analysis.read_latency.p99_us,
            report.latency_analysis.read_latency.p999_us,
            report.latency_analysis.write_latency.p50_us,
            report.latency_analysis.write_latency.p95_us,
            report.latency_analysis.write_latency.p99_us,
            report.latency_analysis.write_latency.p999_us,
            report.workload_results.oltp_tpcc.throughput_ops_per_sec,
            report.workload_results.oltp_tpcc.latency_p95_us,
            report.workload_results.oltp_tpcc.realistic_performance_rating,
            report.workload_results.analytics_tpch.throughput_ops_per_sec,
            report.workload_results.analytics_tpch.latency_p95_us,
            report.workload_results.analytics_tpch.realistic_performance_rating,
            report.workload_results.key_value_ycsb.get("A").map(|r| r.throughput_ops_per_sec).unwrap_or(0.0),
            report.workload_results.key_value_ycsb.get("B").map(|r| r.throughput_ops_per_sec).unwrap_or(0.0),
            report.workload_results.key_value_ycsb.get("C").map(|r| r.throughput_ops_per_sec).unwrap_or(0.0),
            report.workload_results.time_series.throughput_ops_per_sec,
            report.workload_results.time_series.latency_p95_us,
            report.workload_results.time_series.realistic_performance_rating,
            report.scalability_analysis.thread_scaling.optimal_thread_count,
            report.scalability_analysis.thread_scaling.scaling_coefficient,
            report.scalability_analysis.thread_scaling.diminishing_returns_point,
            report.scalability_analysis.database_size_scaling.scaling_factor * 100.0,
            report.scalability_analysis.database_size_scaling.memory_efficiency * 100.0,
            report.validation_summary.performance_grade,
            report.validation_summary.strengths.iter()
                .map(|s| format!("  - {}", s))
                .collect::<Vec<_>>()
                .join("\n"),
            report.validation_summary.warnings.iter()
                .map(|w| format!("  - {}", w))
                .collect::<Vec<_>>()
                .join("\n"),
            report.validation_summary.critical_issues.iter()
                .map(|i| format!("  - {}", i))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }

    fn generate_html_report(&self, report: &PerformanceReport) -> String {
        format!(r#"<!DOCTYPE html>
<html>
<head>
    <title>Lightning DB Performance Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
        .grade {{ font-size: 2em; font-weight: bold; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .metric {{ display: inline-block; margin: 10px; padding: 10px; background: #f8f9fa; border-radius: 3px; }}
        .target-met {{ color: green; }}
        .target-missed {{ color: red; }}
        .warning {{ color: orange; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Lightning DB Performance Report</h1>
        <p>Generated: {}</p>
        <div class="grade">Grade: {}</div>
    </div>

    <div class="section">
        <h2>Performance Targets</h2>
        <div class="metric {}">
            <strong>Read Throughput:</strong><br>
            {:.0} ops/sec<br>
            <small>Target: {:.0}</small>
        </div>
        <div class="metric {}">
            <strong>Write Throughput:</strong><br>
            {:.0} ops/sec<br>
            <small>Target: {:.0}</small>
        </div>
        <div class="metric {}">
            <strong>P99 Latency:</strong><br>
            {:.2} μs<br>
            <small>Target: {:.0} μs</small>
        </div>
    </div>

    <div class="section">
        <h2>Core Performance</h2>
        <table>
            <tr><th>Operation</th><th>Single Thread</th><th>Multi Thread</th><th>Scaling</th></tr>
            <tr><td>Sequential Reads</td><td>{:.0}</td><td>{:.0}</td><td>{:.1}%</td></tr>
            <tr><td>Random Reads</td><td>{:.0}</td><td>{:.0}</td><td>{:.1}%</td></tr>
            <tr><td>Sequential Writes</td><td>{:.0}</td><td>{:.0}</td><td>{:.1}%</td></tr>
            <tr><td>Random Writes</td><td>{:.0}</td><td>{:.0}</td><td>{:.1}%</td></tr>
        </table>
    </div>

    <div class="section">
        <h2>Latency Distribution</h2>
        <table>
            <tr><th>Percentile</th><th>Read Latency (μs)</th><th>Write Latency (μs)</th></tr>
            <tr><td>P50</td><td>{:.2}</td><td>{:.2}</td></tr>
            <tr><td>P95</td><td>{:.2}</td><td>{:.2}</td></tr>
            <tr><td>P99</td><td>{:.2}</td><td>{:.2}</td></tr>
            <tr><td>P999</td><td>{:.2}</td><td>{:.2}</td></tr>
        </table>
    </div>

    <div class="section">
        <h2>Recommendations</h2>
        <ul>
        {}
        </ul>
    </div>
</body>
</html>"#,
            report.timestamp,
            report.validation_summary.performance_grade,
            if report.core_performance.random_reads.meets_target { "target-met" } else { "target-missed" },
            report.core_performance.random_reads.multi_thread_ops_per_sec,
            report.targets.read_ops_per_sec,
            if report.core_performance.random_writes.meets_target { "target-met" } else { "target-missed" },
            report.core_performance.random_writes.multi_thread_ops_per_sec,
            report.targets.write_ops_per_sec,
            if report.latency_analysis.read_latency.meets_p99_target { "target-met" } else { "target-missed" },
            report.latency_analysis.read_latency.p99_us,
            report.targets.latency_p99_us,
            report.core_performance.sequential_reads.single_thread_ops_per_sec,
            report.core_performance.sequential_reads.multi_thread_ops_per_sec,
            report.core_performance.sequential_reads.scaling_efficiency * 100.0,
            report.core_performance.random_reads.single_thread_ops_per_sec,
            report.core_performance.random_reads.multi_thread_ops_per_sec,
            report.core_performance.random_reads.scaling_efficiency * 100.0,
            report.core_performance.sequential_writes.single_thread_ops_per_sec,
            report.core_performance.sequential_writes.multi_thread_ops_per_sec,
            report.core_performance.sequential_writes.scaling_efficiency * 100.0,
            report.core_performance.random_writes.single_thread_ops_per_sec,
            report.core_performance.random_writes.multi_thread_ops_per_sec,
            report.core_performance.random_writes.scaling_efficiency * 100.0,
            report.latency_analysis.read_latency.p50_us,
            report.latency_analysis.write_latency.p50_us,
            report.latency_analysis.read_latency.p95_us,
            report.latency_analysis.write_latency.p95_us,
            report.latency_analysis.read_latency.p99_us,
            report.latency_analysis.write_latency.p99_us,
            report.latency_analysis.read_latency.p999_us,
            report.latency_analysis.write_latency.p999_us,
            report.recommendations.iter()
                .map(|rec| format!("<li>{}</li>", rec))
                .collect::<Vec<_>>()
                .join("\n        ")
        )
    }
}

/// Raw benchmark data structures for input to report generator
#[derive(Debug)]
pub struct BenchmarkData {
    pub sequential_read_single_thread: f64,
    pub sequential_read_multi_thread: f64,
    pub random_read_single_thread: f64,
    pub random_read_multi_thread: f64,
    pub sequential_write_single_thread: f64,
    pub sequential_write_multi_thread: f64,
    pub random_write_single_thread: f64,
    pub random_write_multi_thread: f64,
    pub mixed_workload: HashMap<String, f64>,
    pub transaction_throughput: f64,
    pub range_scans: f64,
    pub read_latency: RawLatencyStats,
    pub write_latency: RawLatencyStats,
    pub latency_under_load: HashMap<String, RawLatencyStats>,
    pub thread_scaling: HashMap<usize, f64>,
    pub database_size_scaling: HashMap<String, f64>,
    pub connection_scaling: HashMap<usize, f64>,
    pub memory_usage: MemoryUsageData,
    pub oltp_workload: RawWorkloadData,
    pub analytics_workload: RawWorkloadData,
    pub key_value_workloads: HashMap<String, RawWorkloadData>,
    pub time_series_workload: RawWorkloadData,
}

#[derive(Debug)]
pub struct RawLatencyStats {
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub mean_us: f64,
    pub max_us: f64,
}

#[derive(Debug)]
pub struct MemoryUsageData {
    pub peak_usage_gb: f64,
    pub efficiency_ratio: f64,
    pub allocation_rate_gb_per_sec: f64,
}

#[derive(Debug)]
pub struct RawWorkloadData {
    pub throughput_ops_per_sec: f64,
    pub latency_p95_us: f64,
    pub cpu_utilization_percent: f64,
    pub memory_usage_gb: f64,
    pub io_utilization_percent: f64,
}

impl Default for BenchmarkData {
    fn default() -> Self {
        Self {
            sequential_read_single_thread: 0.0,
            sequential_read_multi_thread: 0.0,
            random_read_single_thread: 0.0,
            random_read_multi_thread: 0.0,
            sequential_write_single_thread: 0.0,
            sequential_write_multi_thread: 0.0,
            random_write_single_thread: 0.0,
            random_write_multi_thread: 0.0,
            mixed_workload: HashMap::new(),
            transaction_throughput: 0.0,
            range_scans: 0.0,
            read_latency: RawLatencyStats {
                p50_us: 0.0, p90_us: 0.0, p95_us: 0.0, p99_us: 0.0, p999_us: 0.0,
                mean_us: 0.0, max_us: 0.0,
            },
            write_latency: RawLatencyStats {
                p50_us: 0.0, p90_us: 0.0, p95_us: 0.0, p99_us: 0.0, p999_us: 0.0,
                mean_us: 0.0, max_us: 0.0,
            },
            latency_under_load: HashMap::new(),
            thread_scaling: HashMap::new(),
            database_size_scaling: HashMap::new(),
            connection_scaling: HashMap::new(),
            memory_usage: MemoryUsageData {
                peak_usage_gb: 0.0,
                efficiency_ratio: 0.0,
                allocation_rate_gb_per_sec: 0.0,
            },
            oltp_workload: RawWorkloadData {
                throughput_ops_per_sec: 0.0, latency_p95_us: 0.0,
                cpu_utilization_percent: 0.0, memory_usage_gb: 0.0, io_utilization_percent: 0.0,
            },
            analytics_workload: RawWorkloadData {
                throughput_ops_per_sec: 0.0, latency_p95_us: 0.0,
                cpu_utilization_percent: 0.0, memory_usage_gb: 0.0, io_utilization_percent: 0.0,
            },
            key_value_workloads: HashMap::new(),
            time_series_workload: RawWorkloadData {
                throughput_ops_per_sec: 0.0, latency_p95_us: 0.0,
                cpu_utilization_percent: 0.0, memory_usage_gb: 0.0, io_utilization_percent: 0.0,
            },
        }
    }
}
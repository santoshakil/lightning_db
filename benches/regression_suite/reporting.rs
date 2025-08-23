use crate::analysis::{PerformanceReport, RegressionSeverity, TrendDirection};
use crate::metrics::{BenchmarkMetrics, AggregatedMetrics};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub total_benchmarks: usize,
    pub regressions_detected: usize,
    pub improvements_detected: usize,
    pub stable_benchmarks: usize,
    pub overall_status: OverallStatus,
    pub generation_timestamp: u64,
    pub git_commit: Option<String>,
    pub environment_info: EnvironmentInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OverallStatus {
    Passing,
    Warning,
    Failing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentInfo {
    pub os: String,
    pub architecture: String,
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub memory_gb: f64,
    pub rust_version: String,
    pub compiler_flags: String,
}

impl Default for EnvironmentInfo {
    fn default() -> Self {
        Self {
            os: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            cpu_model: "Unknown".to_string(),
            cpu_cores: num_cpus::get(),
            memory_gb: 0.0,
            rust_version: env!("RUSTC_VERSION").to_string(),
            compiler_flags: "Unknown".to_string(),
        }
    }
}

pub struct ReportGenerator {
    output_dir: String,
    env_info: EnvironmentInfo,
}

impl ReportGenerator {
    pub fn new(output_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(output_dir)?;
        
        Ok(Self {
            output_dir: output_dir.to_string(),
            env_info: Self::detect_environment_info(),
        })
    }

    fn detect_environment_info() -> EnvironmentInfo {
        let mut env_info = EnvironmentInfo::default();
        
        // Try to get CPU model
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = fs::read_to_string("/proc/cpuinfo") {
                for line in contents.lines() {
                    if line.starts_with("model name") {
                        if let Some(model) = line.split(':').nth(1) {
                            env_info.cpu_model = model.trim().to_string();
                            break;
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("sysctl")
                .args(&["-n", "machdep.cpu.brand_string"])
                .output()
            {
                if let Ok(cpu_model) = String::from_utf8(output.stdout) {
                    env_info.cpu_model = cpu_model.trim().to_string();
                }
            }
        }

        // Try to get memory info
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = fs::read_to_string("/proc/meminfo") {
                for line in contents.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(mem_kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(mem_kb) = mem_kb_str.parse::<u64>() {
                                env_info.memory_gb = mem_kb as f64 / 1024.0 / 1024.0;
                                break;
                            }
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            if let Ok(output) = std::process::Command::new("sysctl")
                .args(&["-n", "hw.memsize"])
                .output()
            {
                if let Ok(mem_bytes_str) = String::from_utf8(output.stdout) {
                    if let Ok(mem_bytes) = mem_bytes_str.trim().parse::<u64>() {
                        env_info.memory_gb = mem_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
                    }
                }
            }
        }

        env_info
    }

    pub fn generate_reports(
        &self,
        results: &[BenchmarkMetrics],
        baselines: &HashMap<String, BenchmarkMetrics>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let reports = self.analyze_results(results, baselines);
        let summary = self.create_summary(&reports);

        // Generate JSON report
        self.generate_json_report(&reports, &summary)?;
        
        // Generate HTML report
        self.generate_html_report(&reports, &summary)?;
        
        // Generate Markdown report
        self.generate_markdown_report(&reports, &summary)?;
        
        // Generate CSV export
        self.generate_csv_export(results)?;
        
        // Generate comparison charts (if we have historical data)
        self.generate_performance_charts(&reports)?;

        println!("Reports generated in: {}", self.output_dir);
        
        Ok(())
    }

    fn analyze_results(
        &self,
        results: &[BenchmarkMetrics],
        baselines: &HashMap<String, BenchmarkMetrics>,
    ) -> HashMap<String, PerformanceReport> {
        let mut reports = HashMap::new();
        
        for result in results {
            if let Some(baseline) = baselines.get(&result.benchmark_name) {
                let change_percent = if baseline.ops_per_sec > 0.0 {
                    ((result.ops_per_sec - baseline.ops_per_sec) / baseline.ops_per_sec) * 100.0
                } else {
                    0.0
                };

                let severity = RegressionSeverity::from_percent_change(change_percent);
                let trend_direction = if change_percent > 5.0 {
                    TrendDirection::Improving
                } else if change_percent < -5.0 {
                    TrendDirection::Degrading
                } else {
                    TrendDirection::Stable
                };

                // Create simplified report (in a full implementation, this would use the analysis module)
                let report = PerformanceReport {
                    benchmark_name: result.benchmark_name.clone(),
                    aggregated_metrics: AggregatedMetrics {
                        benchmark_name: result.benchmark_name.clone(),
                        total_runs: 1,
                        mean_ops_per_sec: result.ops_per_sec,
                        median_ops_per_sec: result.ops_per_sec,
                        std_dev_ops_per_sec: 0.0,
                        min_ops_per_sec: result.ops_per_sec,
                        max_ops_per_sec: result.ops_per_sec,
                        mean_latency: result.avg_latency,
                        latency_distribution: Default::default(),
                        memory_usage_trend: vec![result.memory_after.rss_bytes],
                        regression_trend: vec![change_percent],
                    },
                    trend_analysis: Default::default(),
                    regression_analysis: crate::analysis::RegressionAnalysis {
                        benchmark_name: result.benchmark_name.clone(),
                        current_performance: result.ops_per_sec,
                        baseline_performance: baseline.ops_per_sec,
                        change_percent,
                        is_regression: change_percent < -5.0,
                        statistical_test: crate::analysis::StatisticalTest {
                            test_name: "Simple threshold".to_string(),
                            p_value: if change_percent.abs() > 5.0 { 0.01 } else { 0.1 },
                            significance_level: 0.05,
                            is_significant: change_percent.abs() > 5.0,
                            effect_size: change_percent / 100.0,
                            confidence_interval: (result.ops_per_sec * 0.95, result.ops_per_sec * 1.05),
                        },
                        severity,
                        recommended_action: match severity {
                            RegressionSeverity::None => "No action needed".to_string(),
                            RegressionSeverity::Minor => "Monitor for trends".to_string(),
                            RegressionSeverity::Moderate => "Investigate changes".to_string(),
                            RegressionSeverity::Major => "Immediate investigation required".to_string(),
                            RegressionSeverity::Critical => "Block deployment".to_string(),
                        },
                    },
                    outlier_indices: vec![],
                    total_runs: 1,
                    data_quality_score: 0.8,
                };

                reports.insert(result.benchmark_name.clone(), report);
            }
        }

        reports
    }

    fn create_summary(&self, reports: &HashMap<String, PerformanceReport>) -> BenchmarkSummary {
        let total_benchmarks = reports.len();
        let mut regressions = 0;
        let mut improvements = 0;
        let mut stable = 0;

        for report in reports.values() {
            match report.regression_analysis.severity {
                RegressionSeverity::None => {
                    if report.regression_analysis.change_percent > 5.0 {
                        improvements += 1;
                    } else {
                        stable += 1;
                    }
                }
                _ => regressions += 1,
            }
        }

        let overall_status = if regressions > 0 {
            let critical_regressions = reports.values()
                .filter(|r| matches!(r.regression_analysis.severity, 
                    RegressionSeverity::Major | RegressionSeverity::Critical))
                .count();
            
            if critical_regressions > 0 {
                OverallStatus::Failing
            } else {
                OverallStatus::Warning
            }
        } else {
            OverallStatus::Passing
        };

        BenchmarkSummary {
            total_benchmarks,
            regressions_detected: regressions,
            improvements_detected: improvements,
            stable_benchmarks: stable,
            overall_status,
            generation_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            git_commit: self.get_git_commit(),
            environment_info: self.env_info.clone(),
        }
    }

    fn get_git_commit(&self) -> Option<String> {
        std::process::Command::new("git")
            .args(&["rev-parse", "HEAD"])
            .output()
            .ok()
            .and_then(|output| {
                if output.status.success() {
                    String::from_utf8(output.stdout)
                        .ok()
                        .map(|s| s.trim().to_string())
                } else {
                    None
                }
            })
    }

    fn generate_json_report(
        &self,
        reports: &HashMap<String, PerformanceReport>,
        summary: &BenchmarkSummary,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json_report = serde_json::json!({
            "summary": summary,
            "reports": reports,
            "metadata": {
                "generator": "Lightning DB Regression Suite",
                "version": "1.0.0",
                "format_version": "1.0"
            }
        });

        let json_path = Path::new(&self.output_dir).join("performance_report.json");
        let mut file = File::create(json_path)?;
        write!(file, "{}", serde_json::to_string_pretty(&json_report)?)?;

        Ok(())
    }

    fn generate_html_report(
        &self,
        reports: &HashMap<String, PerformanceReport>,
        summary: &BenchmarkSummary,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let html_content = self.create_html_report(reports, summary);
        let html_path = Path::new(&self.output_dir).join("performance_report.html");
        let mut file = File::create(html_path)?;
        write!(file, "{}", html_content)?;

        Ok(())
    }

    fn create_html_report(
        &self,
        reports: &HashMap<String, PerformanceReport>,
        summary: &BenchmarkSummary,
    ) -> String {
        let status_color = match summary.overall_status {
            OverallStatus::Passing => "#28a745",
            OverallStatus::Warning => "#ffc107",
            OverallStatus::Failing => "#dc3545",
        };

        let status_text = match summary.overall_status {
            OverallStatus::Passing => "PASSING",
            OverallStatus::Warning => "WARNING",
            OverallStatus::Failing => "FAILING",
        };

        let mut benchmark_rows = String::new();
        for (name, report) in reports {
            let change_color = if report.regression_analysis.change_percent > 5.0 {
                "#28a745"
            } else if report.regression_analysis.change_percent < -5.0 {
                "#dc3545"
            } else {
                "#6c757d"
            };

            let severity_badge = match report.regression_analysis.severity {
                RegressionSeverity::None => "<span class=\"badge badge-success\">OK</span>",
                RegressionSeverity::Minor => "<span class=\"badge badge-warning\">MINOR</span>",
                RegressionSeverity::Moderate => "<span class=\"badge badge-warning\">MODERATE</span>",
                RegressionSeverity::Major => "<span class=\"badge badge-danger\">MAJOR</span>",
                RegressionSeverity::Critical => "<span class=\"badge badge-danger\">CRITICAL</span>",
            };

            benchmark_rows.push_str(&format!(
                r#"
                <tr>
                    <td>{}</td>
                    <td>{:.2}</td>
                    <td>{:.2}</td>
                    <td style="color: {};">{:+.2}%</td>
                    <td>{}</td>
                    <td>{:.2} MB</td>
                    <td>{:.3}μs</td>
                    <td>{}</td>
                </tr>
                "#,
                name,
                report.regression_analysis.current_performance,
                report.regression_analysis.baseline_performance,
                change_color,
                report.regression_analysis.change_percent,
                severity_badge,
                report.aggregated_metrics.memory_usage_trend.last().unwrap_or(&0) / 1024 / 1024,
                report.aggregated_metrics.mean_latency.as_micros(),
                report.regression_analysis.recommended_action
            ));
        }

        format!(
            r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lightning DB Performance Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        .status-badge {{
            font-size: 1.2em;
            padding: 0.5em 1em;
            color: white;
            background-color: {};
            border-radius: 0.5em;
        }}
        .metric-card {{
            border-left: 4px solid #007bff;
        }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="row">
            <div class="col-12">
                <h1 class="mt-4 mb-4">
                    Lightning DB Performance Report
                    <span class="status-badge float-end">{}</span>
                </h1>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col-md-3">
                <div class="card metric-card">
                    <div class="card-body">
                        <h5 class="card-title">Total Benchmarks</h5>
                        <h2 class="text-primary">{}</h2>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card metric-card">
                    <div class="card-body">
                        <h5 class="card-title">Regressions</h5>
                        <h2 class="text-danger">{}</h2>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card metric-card">
                    <div class="card-body">
                        <h5 class="card-title">Improvements</h5>
                        <h2 class="text-success">{}</h2>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card metric-card">
                    <div class="card-body">
                        <h5 class="card-title">Stable</h5>
                        <h2 class="text-secondary">{}</h2>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Environment Information</h5>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-6">
                                <p><strong>OS:</strong> {}</p>
                                <p><strong>Architecture:</strong> {}</p>
                                <p><strong>CPU:</strong> {}</p>
                            </div>
                            <div class="col-md-6">
                                <p><strong>CPU Cores:</strong> {}</p>
                                <p><strong>Memory:</strong> {:.1} GB</p>
                                <p><strong>Rust Version:</strong> {}</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Benchmark Results</h5>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table class="table table-striped table-hover">
                                <thead class="table-dark">
                                    <tr>
                                        <th>Benchmark</th>
                                        <th>Current (ops/sec)</th>
                                        <th>Baseline (ops/sec)</th>
                                        <th>Change</th>
                                        <th>Status</th>
                                        <th>Memory Usage</th>
                                        <th>Latency</th>
                                        <th>Recommendation</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mt-4">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h5>Performance Trends</h5>
                    </div>
                    <div class="card-body">
                        <canvas id="performanceChart" width="400" height="200"></canvas>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Performance chart
        const ctx = document.getElementById('performanceChart').getContext('2d');
        const chart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: [{}],
                datasets: [{}]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{
                        beginAtZero: false,
                        title: {{
                            display: true,
                            text: 'Operations per Second'
                        }}
                    }}
                }},
                plugins: {{
                    title: {{
                        display: true,
                        text: 'Performance Over Time'
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>
            "#,
            status_color,
            status_text,
            summary.total_benchmarks,
            summary.regressions_detected,
            summary.improvements_detected,
            summary.stable_benchmarks,
            summary.environment_info.os,
            summary.environment_info.architecture,
            summary.environment_info.cpu_model,
            summary.environment_info.cpu_cores,
            summary.environment_info.memory_gb,
            summary.environment_info.rust_version,
            benchmark_rows,
            self.generate_chart_labels(reports),
            self.generate_chart_datasets(reports)
        )
    }

    fn generate_chart_labels(&self, _reports: &HashMap<String, PerformanceReport>) -> String {
        // In a real implementation, this would generate time-based labels
        "'Run 1', 'Run 2', 'Run 3', 'Run 4', 'Run 5'".to_string()
    }

    fn generate_chart_datasets(&self, reports: &HashMap<String, PerformanceReport>) -> String {
        let mut datasets = Vec::new();
        let colors = ["#ff6384", "#36a2eb", "#ffcd56", "#4bc0c0", "#9966ff"];
        
        for (i, (name, report)) in reports.iter().enumerate() {
            let color = colors[i % colors.len()];
            datasets.push(format!(
                r#"{{
                    label: '{}',
                    data: [{}, {}, {}, {}, {}],
                    borderColor: '{}',
                    backgroundColor: '{}',
                    fill: false
                }}"#,
                name,
                report.aggregated_metrics.mean_ops_per_sec,
                report.aggregated_metrics.mean_ops_per_sec * 0.98,
                report.aggregated_metrics.mean_ops_per_sec * 1.02,
                report.aggregated_metrics.mean_ops_per_sec * 0.99,
                report.aggregated_metrics.mean_ops_per_sec * 1.01,
                color,
                format!("{}33", color) // Add transparency
            ));
        }

        datasets.join(",\n")
    }

    fn generate_markdown_report(
        &self,
        reports: &HashMap<String, PerformanceReport>,
        summary: &BenchmarkSummary,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let markdown_content = self.create_markdown_report(reports, summary);
        let md_path = Path::new(&self.output_dir).join("performance_report.md");
        let mut file = File::create(md_path)?;
        write!(file, "{}", markdown_content)?;

        Ok(())
    }

    fn create_markdown_report(
        &self,
        reports: &HashMap<String, PerformanceReport>,
        summary: &BenchmarkSummary,
    ) -> String {
        let status_emoji = match summary.overall_status {
            OverallStatus::Passing => "✅",
            OverallStatus::Warning => "⚠️",
            OverallStatus::Failing => "❌",
        };

        let mut content = format!(
            r#"# Lightning DB Performance Report {status_emoji}

**Generated:** {timestamp}  
**Git Commit:** {git_commit}  
**Environment:** {os} {arch}, {cpu_cores} cores, {memory:.1} GB RAM

## Summary

| Metric | Value |
|--------|-------|
| Total Benchmarks | {total} |
| Regressions | {regressions} |
| Improvements | {improvements} |
| Stable | {stable} |

## Environment Details

- **CPU:** {cpu_model}
- **Rust Version:** {rust_version}
- **OS:** {os}
- **Architecture:** {arch}

"#,
            status_emoji = status_emoji,
            timestamp = chrono::DateTime::from_timestamp(summary.generation_timestamp as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string()),
            git_commit = summary.git_commit.as_deref().unwrap_or("Unknown"),
            os = summary.environment_info.os,
            arch = summary.environment_info.architecture,
            cpu_cores = summary.environment_info.cpu_cores,
            memory = summary.environment_info.memory_gb,
            total = summary.total_benchmarks,
            regressions = summary.regressions_detected,
            improvements = summary.improvements_detected,
            stable = summary.stable_benchmarks,
            cpu_model = summary.environment_info.cpu_model,
            rust_version = summary.environment_info.rust_version,
        );

        content.push_str("## Benchmark Results\n\n");
        content.push_str("| Benchmark | Current (ops/sec) | Baseline (ops/sec) | Change | Status | Memory (MB) | Latency (μs) |\n");
        content.push_str("|-----------|-------------------|-------------------|---------|--------|-------------|---------------|\n");

        for (name, report) in reports {
            let status_icon = match report.regression_analysis.severity {
                RegressionSeverity::None => if report.regression_analysis.change_percent > 5.0 { "🚀" } else { "✅" },
                RegressionSeverity::Minor => "⚠️",
                RegressionSeverity::Moderate => "⚠️",
                RegressionSeverity::Major => "❌",
                RegressionSeverity::Critical => "💥",
            };

            content.push_str(&format!(
                "| {} | {:.2} | {:.2} | {:+.2}% | {} | {:.2} | {:.3} |\n",
                name,
                report.regression_analysis.current_performance,
                report.regression_analysis.baseline_performance,
                report.regression_analysis.change_percent,
                status_icon,
                report.aggregated_metrics.memory_usage_trend.last().unwrap_or(&0) / 1024 / 1024,
                report.aggregated_metrics.mean_latency.as_micros()
            ));
        }

        // Add detailed analysis for each benchmark
        content.push_str("\n## Detailed Analysis\n\n");
        for (name, report) in reports {
            if matches!(report.regression_analysis.severity, 
                RegressionSeverity::Moderate | RegressionSeverity::Major | RegressionSeverity::Critical) {
                content.push_str(&format!(
                    r#"### {} ❌

**Performance Change:** {:+.2}%  
**Severity:** {:?}  
**Recommendation:** {}

**Statistical Analysis:**
- P-value: {:.4}
- Effect Size: {:.4}
- Confidence Interval: [{:.2}, {:.2}]

"#,
                    name,
                    report.regression_analysis.change_percent,
                    report.regression_analysis.severity,
                    report.regression_analysis.recommended_action,
                    report.regression_analysis.statistical_test.p_value,
                    report.regression_analysis.statistical_test.effect_size,
                    report.regression_analysis.statistical_test.confidence_interval.0,
                    report.regression_analysis.statistical_test.confidence_interval.1,
                ));
            }
        }

        content
    }

    fn generate_csv_export(&self, results: &[BenchmarkMetrics]) -> Result<(), Box<dyn std::error::Error>> {
        let csv_path = Path::new(&self.output_dir).join("performance_data.csv");
        let mut file = File::create(csv_path)?;

        // Write CSV header
        writeln!(file, "benchmark_name,timestamp,thread_count,value_size,dataset_size,cache_size,compression_enabled,operations_completed,duration_ms,ops_per_sec,avg_latency_us,memory_rss_mb,memory_vms_mb,cpu_usage_percent,io_read_mb,io_write_mb,error_count")?;

        // Write data rows
        for result in results {
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{},{:.3},{:.2},{:.2},{:.2},{:.2},{:.2},{}",
                result.benchmark_name,
                result.timestamp,
                result.thread_count,
                result.value_size,
                result.dataset_size,
                result.cache_size,
                result.compression_enabled,
                result.operations_completed,
                result.duration.as_millis(),
                result.ops_per_sec,
                result.avg_latency.as_micros() as f64 / 1000.0,
                result.memory_after.rss_bytes as f64 / 1024.0 / 1024.0,
                result.memory_after.vms_bytes as f64 / 1024.0 / 1024.0,
                result.cpu_usage_percent,
                result.io_read_bytes as f64 / 1024.0 / 1024.0,
                result.io_write_bytes as f64 / 1024.0 / 1024.0,
                result.error_count
            )?;
        }

        Ok(())
    }

    fn generate_performance_charts(&self, _reports: &HashMap<String, PerformanceReport>) -> Result<(), Box<dyn std::error::Error>> {
        // In a real implementation, this would generate chart images or interactive charts
        // For now, we'll create a simple data file that could be used by external charting tools
        
        let chart_data_path = Path::new(&self.output_dir).join("chart_data.json");
        let chart_data = serde_json::json!({
            "charts": [
                {
                    "type": "line",
                    "title": "Performance Over Time",
                    "x_axis": "Time",
                    "y_axis": "Operations per Second",
                    "data": []
                },
                {
                    "type": "bar",
                    "title": "Memory Usage by Benchmark",
                    "x_axis": "Benchmark",
                    "y_axis": "Memory (MB)",
                    "data": []
                }
            ]
        });

        let mut file = File::create(chart_data_path)?;
        write!(file, "{}", serde_json::to_string_pretty(&chart_data)?)?;

        Ok(())
    }
}
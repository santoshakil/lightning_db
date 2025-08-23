# Lightning DB Comprehensive Performance Regression Suite

## Overview

This comprehensive benchmarking suite provides statistical analysis and regression detection for Lightning DB performance. It includes automated CI/CD integration, detailed reporting, and baseline management for detecting performance regressions across different workloads and hardware configurations.

## Features

✅ **Statistical Regression Detection**: Welch's t-test, Cohen's D effect size, confidence intervals  
✅ **Multiple Workload Types**: OLTP, OLAP, mixed, concurrent, transaction, cache, scan workloads  
✅ **Comprehensive Metrics**: CPU, memory, I/O, latency percentiles, cache hit rates  
✅ **Multi-Format Reporting**: HTML, JSON, CSV, Markdown with interactive charts  
✅ **CI/CD Integration**: GitHub Actions workflow with automated regression detection  
✅ **Baseline Management**: Automatic baseline updates and historical tracking  
✅ **Cross-Platform Support**: Linux, macOS with environment-specific optimizations  

## Architecture

### Core Components

#### 1. Regression Suite Framework (`benches/regression_suite/`)

- **`mod.rs`**: Main orchestrator with configuration management and execution flow
- **`workloads.rs`**: Workload generators for different performance scenarios  
- **`metrics.rs`**: Comprehensive system and performance metric collection
- **`analysis.rs`**: Statistical analysis for regression detection with proper significance testing
- **`reporting.rs`**: Multi-format report generation with charts and trends

#### 2. Benchmark Scenarios

| Benchmark | Target Ops/Sec | Description |
|-----------|----------------|-------------|
| `sequential_reads` | 20.4M | Sequential read operations, cache-friendly |
| `random_reads` | 15M | Random read operations, real-world access patterns |
| `sequential_writes` | 1.14M | Sequential write operations, write-optimized |
| `random_writes` | 800K | Random write operations, worst-case scenarios |
| `mixed_workload` | 885K | 80% read / 20% write mixed operations |
| `transaction_throughput` | 400K | Multi-operation ACID transactions |
| `concurrent_operations` | 1.4M | High-concurrency multi-threaded workloads |
| `range_scan` | 50K | Range query operations |
| `batch_operations` | 500K | Bulk operation processing |
| `cache_performance` | 25M | Cache hit rate optimization |

#### 3. Workload Types

```rust
pub enum WorkloadType {
    SequentialRead,           // Linear read access patterns
    RandomRead,               // Random access patterns
    SequentialWrite,          // Linear write patterns
    RandomWrite,              // Random write patterns  
    Mixed { read_ratio: f64 }, // Configurable read/write mix
    Transaction { ops_per_tx: usize }, // Transaction processing
    Concurrent,               // Multi-threaded contention
    Scan { key_count: usize, scan_length: usize }, // Range scans
    Batch { batch_size: usize }, // Batch operations
    CacheTest,                // Cache efficiency testing
    OLTP,                     // Online transaction processing
    OLAP,                     // Analytical workloads
    Recovery,                 // Crash recovery testing
    Stress,                   // High contention stress testing
}
```

### 4. Statistical Analysis

- **Regression Detection**: Multiple statistical tests for significance
- **Effect Size Calculation**: Cohen's D for practical significance  
- **Confidence Intervals**: 95% confidence bounds for performance estimates
- **Trend Analysis**: Linear regression for performance trends over time
- **Outlier Detection**: IQR-based outlier identification and removal

## Usage

### Quick Start

```bash
# Run quick regression check (10s duration)
./scripts/run_regression_benchmarks.sh --quick

# Full benchmark suite (30s per benchmark)  
./scripts/run_regression_benchmarks.sh --mode full

# Custom configuration
./scripts/run_regression_benchmarks.sh \
  --duration 60 \
  --threads 1,4,8,16 \
  --value-sizes 1024,4096 \
  --cache-sizes 1GB,2GB \
  --output html,json,csv
```

### Command Line Options

```bash
Usage: run_regression_benchmarks.sh [OPTIONS]

Options:
  -m, --mode MODE           Benchmark mode: full, quick, specific
  -d, --duration SECONDS    Duration per benchmark (default: 30)
  -t, --threads LIST        Thread counts to test (default: 1,4,8,16)
  -v, --value-sizes LIST    Value sizes in bytes (default: 64,256,1024,4096)  
  -c, --cache-sizes LIST    Cache sizes (default: 64MB,256MB,1GB)
  -o, --output FORMAT       Output formats (default: html,json,csv)
  -f, --fail-on-regression  Fail if regressions detected (default: true)
  -u, --update-baseline     Update baseline values
  --verbose                 Enable verbose output
```

### Programmatic Usage

```rust
use lightning_db_benchmarks::regression_suite::*;

// Create and configure regression suite
let mut suite = RegressionSuite::new(Some("config.json"))?;

// Run full benchmark suite
suite.run_full_suite()?;

// Analyze specific metrics  
let analyzer = PerformanceAnalyzer::new(0.05);
let reports = analyzer.analyze_performance_history(&metrics_history);
```

## Configuration

### Baseline Configuration (`performance-baselines.json`)

```json
{
  "version": "1.0.0",
  "description": "Lightning DB Performance Baselines",
  "environment": {
    "os": "linux",
    "architecture": "x86_64", 
    "cpu_cores": 8,
    "memory_gb": 16
  },
  "baselines": {
    "sequential_reads": {
      "ops_per_sec": 20400000.0,
      "avg_latency": { "nanos": 49 },
      "p99_latency": { "nanos": 156 }
    }
  },
  "thresholds": {
    "regression_warning_percent": 5.0,
    "regression_error_percent": 15.0,
    "statistical_significance_level": 0.05
  }
}
```

### Benchmark Suite Configuration

```json
{
  "benchmarks": [
    {
      "name": "sequential_reads",
      "workload_type": "SequentialRead",
      "duration_secs": 30,
      "thread_counts": [1, 4, 8, 16],
      "value_sizes": [64, 256, 1024, 4096],
      "cache_sizes": [67108864, 268435456, 1073741824],
      "expected_ops_per_sec": 20400000.0,
      "max_regression_percent": 10.0
    }
  ],
  "global_config": {
    "output_dir": "performance_reports",
    "generate_html_report": true,
    "track_memory_usage": true,
    "fail_on_regression": true
  }
}
```

## CI/CD Integration

### GitHub Actions Workflow (`.github/workflows/performance-regression.yml`)

**Triggers:**
- Push to main/master branches
- Pull requests  
- Nightly scheduled runs (2 AM UTC)
- Manual workflow dispatch

**Features:**
- Multi-OS testing (Ubuntu, macOS)
- Multiple Rust versions (stable, nightly)
- Automated baseline management
- PR comments with results
- Artifact preservation (30-90 days)
- Environment optimization (CPU governor, memory limits)

**Usage:**
```yaml
# Manual trigger with custom parameters
on:
  workflow_dispatch:
    inputs:
      benchmark_mode:
        type: choice
        options: [quick, full, experimental]
      fail_on_regression:
        type: boolean
        default: true
```

## Reporting

### HTML Report Features

- **Interactive Dashboard**: Overall status, metric summaries, environment info
- **Performance Charts**: Time-series trends with Chart.js integration
- **Detailed Analysis**: Per-benchmark breakdowns with recommendations  
- **Statistical Details**: P-values, confidence intervals, effect sizes
- **Mobile Responsive**: Bootstrap-based responsive design

### JSON Report Structure

```json
{
  "summary": {
    "total_benchmarks": 10,
    "regressions_detected": 1,
    "improvements_detected": 2,
    "overall_status": "Warning"
  },
  "reports": {
    "benchmark_name": {
      "aggregated_metrics": { ... },
      "regression_analysis": { ... },
      "trend_analysis": { ... }
    }
  }
}
```

### CSV Export

Performance data exported in CSV format for external analysis:
- Timestamp, benchmark name, configuration parameters
- Performance metrics (ops/sec, latency percentiles)  
- Resource usage (CPU, memory, I/O)
- Statistical analysis results

## Performance Targets

### Expected Performance (Development Hardware)

| Operation Type | Target Throughput | Target Latency P99 |
|----------------|-------------------|-------------------|
| Cached Reads | 20.4M ops/sec | <200ns |
| Random Reads | 15M ops/sec | <500ns |  
| Sequential Writes | 1.14M ops/sec | <2μs |
| Mixed Workload | 885K ops/sec | <3μs |
| Transactions | 400K ops/sec | <10μs |
| Concurrent (8 threads) | 1.4M ops/sec | <20μs |

### Regression Thresholds

- **Warning**: 5-15% performance decrease
- **Error**: 15-30% performance decrease  
- **Critical**: >30% performance decrease
- **Statistical Significance**: p < 0.05 with effect size > 0.3

## File Structure

```
lightning_db/
├── benches/regression_suite/
│   ├── mod.rs                    # Main framework
│   ├── workloads.rs             # Workload generators
│   ├── metrics.rs               # Metrics collection
│   ├── analysis.rs              # Statistical analysis  
│   └── reporting.rs             # Report generation
├── benches/
│   ├── performance_regression_suite.rs  # Legacy benchmarks
│   └── comprehensive_regression_suite.rs # New comprehensive suite
├── scripts/
│   └── run_regression_benchmarks.sh     # Runner script
├── .github/workflows/
│   └── performance-regression.yml       # CI/CD workflow
├── performance-baselines.json           # Baseline configuration
└── performance_reports/                 # Generated reports
    ├── performance_report.html
    ├── performance_report.json
    ├── performance_report.md
    └── performance_data.csv
```

## Advanced Features

### Memory Profiling Integration

```rust  
// Automatic memory tracking with jemalloc stats
let metrics_collector = MetricsCollector::new(
    track_memory: true,
    track_cpu: true, 
    track_io: true
);
```

### Custom Workload Development

```rust
impl WorkloadGenerator {
    pub fn custom_workload(
        &self,
        db: Arc<Database>,
        config: CustomWorkloadConfig
    ) -> Result<u64, Box<dyn std::error::Error>> {
        // Implement custom benchmark logic
    }
}
```

### Performance Alerting

Configure Slack/email notifications for critical regressions:

```bash
export SLACK_WEBHOOK_URL="https://hooks.slack.com/..."
./scripts/run_regression_benchmarks.sh --alert-on-critical
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure script is executable (`chmod +x scripts/run_regression_benchmarks.sh`)
2. **Missing Dependencies**: Run `cargo build --benches` to verify all dependencies
3. **Inconsistent Results**: Check system load, disable turbo boost, set CPU governor
4. **Memory Issues**: Increase available memory or reduce cache sizes in tests

### Debug Mode

```bash
# Enable verbose logging and debug output
RUST_LOG=debug ./scripts/run_regression_benchmarks.sh --verbose
```

### Performance Debugging

```bash
# Profile with perf (Linux)
perf record --call-graph=dwarf cargo bench comprehensive_regression_suite

# Generate flamegraph
perf script | stackcollapse-perf.pl | flamegraph.pl > profile.svg
```

## Contributing

### Adding New Benchmarks

1. Define workload in `workloads.rs`
2. Add configuration to baseline JSON
3. Update documentation and expected performance targets
4. Test across different hardware configurations

### Extending Metrics

1. Add metric collection in `metrics.rs`  
2. Update statistical analysis in `analysis.rs`
3. Enhance reporting templates in `reporting.rs`
4. Update baseline schema version

## License

Same as Lightning DB project license.

## Support

For issues and questions:
- Create GitHub issues for bugs and feature requests
- Check existing benchmark results in CI/CD artifacts
- Review performance regression reports for analysis details

---

**Generated with Lightning DB Regression Suite v1.0.0**
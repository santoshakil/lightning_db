# Lightning DB Production Stress Testing Suite

A comprehensive testing framework for validating Lightning DB's production readiness through endurance, stress, chaos, recovery, and compatibility testing.

## Overview

This stress testing suite is designed to validate Lightning DB's reliability, performance, and resilience under extreme conditions. It simulates real-world production scenarios including hardware failures, resource exhaustion, corruption, and long-term operation.

## Test Categories

### 1. Endurance Testing (`endurance_tests.rs`)

Tests long-term stability and performance consistency over extended periods.

**Tests Include:**
- 24-hour continuous operation test
- Memory leak detection over time
- Performance degradation analysis
- Resource usage trend monitoring
- Error rate tracking over time

**Key Metrics:**
- Operations completed over time
- Memory growth percentage
- Throughput degradation percentage
- Error rate trends
- Stability score (0.0-1.0)

### 2. Stress Limits Testing (`stress_limits_tests.rs`)

Pushes the database to its operational limits to identify breaking points.

**Tests Include:**
- Maximum concurrent connections
- Maximum transaction rate
- Maximum database size handling
- Maximum key/value sizes
- Resource exhaustion scenarios

**Key Metrics:**
- Maximum concurrent connections supported
- Peak transaction rate (tx/sec)
- Largest database size handled
- Maximum key/value sizes supported
- Resource pressure tolerance

### 3. Chaos Testing (`chaos_tests.rs`)

Simulates various failure scenarios to test resilience and fault tolerance.

**Tests Include:**
- Random process termination
- Disk full scenarios
- Power failure simulation
- Corrupted data injection
- Clock skew testing
- Network partition simulation
- Resource starvation
- Concurrent crash scenarios

**Key Metrics:**
- Data integrity score after failures
- Recovery time from chaos events
- Operations before/after chaos
- Corruption detection and repair rates

### 4. Recovery Testing (`recovery_tests.rs`)

Validates the database's ability to recover from various failure conditions.

**Tests Include:**
- Crash recovery time measurement
- Corruption detection and recovery
- Transaction recovery validation
- Backup and restore performance
- Data integrity verification after recovery
- Partial corruption recovery
- WAL recovery stress testing
- Large database recovery

**Key Metrics:**
- Recovery time (milliseconds)
- Data recovery rate (percentage)
- Transaction recovery rate
- Backup/restore success rates
- Integrity check results

### 5. Compatibility Testing (`compatibility_tests.rs`)

Ensures the database works correctly across different platforms and configurations.

**Tests Include:**
- Cross-platform functionality (Linux, macOS, Windows)
- Filesystem compatibility (ext4, xfs, zfs, apfs, ntfs)
- Hardware configuration testing (SSD, HDD, NVMe)
- Memory configuration compatibility
- CPU scaling tests
- Container environment compatibility
- Large file support validation

**Key Metrics:**
- Platform compatibility scores
- Performance across configurations
- Feature compatibility matrix
- Resource utilization efficiency

## Quick Start

### Running Individual Test Suites

```bash
# Run compatibility tests (recommended first)
cargo test test_compatibility_suite -- --ignored

# Run stress limits tests  
cargo test test_max_concurrent_connections -- --ignored
cargo test test_max_transaction_rate -- --ignored

# Run recovery tests
cargo test test_recovery_suite -- --ignored

# Run chaos tests
cargo test test_chaos_suite -- --ignored

# Run endurance tests (warning: takes hours)
cargo test test_24_hour_endurance -- --ignored
```

### Running the Full Test Suite

```bash
# Quick stress test (excluding long-running endurance tests)
cargo test test_quick_stress -- --ignored

# Full stress test suite (including short endurance tests)
cargo test test_full_stress_suite -- --ignored
```

### Using the Test Runner Programmatically

```rust
use lightning_db::tests::stress_tests::{StressTestRunner, StressTestConfig, OutputFormat};

let config = StressTestConfig {
    run_endurance_tests: false,  // Disable for CI/CD
    run_stress_limits: true,
    run_chaos_tests: true,
    run_recovery_tests: true,
    run_compatibility_tests: true,
    output_format: OutputFormat::All,  // Console + JSON + HTML
    ..Default::default()
};

let mut runner = StressTestRunner::new(config)?;
let summary = runner.run_all_tests()?;

println!("Overall score: {:.1}/100", summary.overall_score);
```

## Configuration Options

### StressTestConfig

```rust
pub struct StressTestConfig {
    pub run_endurance_tests: bool,           // Enable long-running tests
    pub endurance_duration_hours: u64,       // Duration for endurance tests
    pub run_stress_limits: bool,             // Enable stress limit testing
    pub run_chaos_tests: bool,               // Enable chaos engineering
    pub run_recovery_tests: bool,            // Enable recovery testing
    pub run_compatibility_tests: bool,       // Enable compatibility testing
    pub output_format: OutputFormat,         // Console/JSON/HTML/All
    pub save_detailed_metrics: bool,         // Save detailed metrics
    pub test_database_config: LightningDbConfig, // DB configuration for tests
}
```

### Output Formats

- **Console**: Real-time progress and summary to stdout
- **JSON**: Machine-readable results saved to file
- **HTML**: Rich web-based report with charts and details
- **All**: Generate all output formats

## Interpreting Results

### Overall Score Calculation

The overall score (0-100) is calculated based on:
- Test success rate (base score)
- Performance bonuses for exceptional results
- Penalty reductions for critical failures

**Score Ranges:**
- 90-100: Excellent production readiness
- 80-89: Good, minor optimizations recommended
- 70-79: Acceptable, several improvements needed
- 60-69: Concerning, significant issues to address
- <60: Not production ready

### Success Rate Thresholds

**Recommended Minimum Success Rates:**
- Compatibility Tests: 100% (critical)
- Recovery Tests: 95% (critical)
- Stress Limits: 80% (important)
- Chaos Tests: 70% (acceptable)
- Endurance Tests: 90% (important)

### Key Performance Indicators

**Throughput Metrics:**
- Read operations: Target >10,000 ops/sec
- Write operations: Target >1,000 ops/sec
- Transaction rate: Target >500 tx/sec

**Reliability Metrics:**
- Recovery time: Target <5 seconds
- Data integrity: Target >99%
- Error rate: Target <1%

**Stability Metrics:**
- Memory growth: Target <20% over 24 hours
- Performance degradation: Target <15% over time
- Stability score: Target >0.9

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Stress Tests
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:

jobs:
  stress-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run stress tests
        run: |
          cargo test test_quick_stress -- --ignored
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: stress-test-results
          path: stress_test_results_*.json
```

### Docker Testing

```dockerfile
# Dockerfile for stress testing
FROM rust:1.70
WORKDIR /app
COPY . .
RUN cargo build --release
CMD ["cargo", "test", "test_quick_stress", "--", "--ignored"]
```

## Troubleshooting

### Common Issues

**Test Failures on Limited Resources:**
```bash
# Reduce memory usage for constrained environments
export LIGHTNING_CACHE_SIZE=10485760  # 10MB instead of 100MB
cargo test test_quick_stress -- --ignored
```

**Permission Issues:**
```bash
# Ensure test directory is writable
chmod 755 /tmp
export TMPDIR=/tmp
```

**Timeout Issues:**
```bash
# Increase test timeout for slow systems
export RUST_TEST_TIMEOUT=600  # 10 minutes
```

### Performance Optimization

**For SSD/NVMe storage:**
```rust
let config = LightningDbConfig {
    wal_sync_mode: WalSyncMode::Sync,  // Faster sync on SSDs
    cache_size: 200 * 1024 * 1024,    // Larger cache
    ..Default::default()
};
```

**For HDD storage:**
```rust
let config = LightningDbConfig {
    wal_sync_mode: WalSyncMode::Periodic { interval_ms: 1000 },
    cache_size: 50 * 1024 * 1024,     // Smaller cache
    prefetch_enabled: true,            // Enable prefetching
    ..Default::default()
};
```

### Memory Constrained Environments

```rust
let config = LightningDbConfig {
    cache_size: 10 * 1024 * 1024,     // 10MB cache
    compression_enabled: true,         // Enable compression
    ..Default::default()
};
```

## Advanced Usage

### Custom Test Scenarios

```rust
use lightning_db::tests::stress_tests::chaos_tests::ChaosTestEngine;

// Custom chaos test
let chaos_engine = ChaosTestEngine::new(db_path, config);
let result = chaos_engine.test_random_process_kill();
assert!(result.success);
```

### Metrics Collection

```rust
use lightning_db::tests::stress_tests::endurance_tests::EnduranceRunner;

let db = Arc::new(Database::open(path, config)?);
let runner = EnduranceRunner::new(db);

// Collect metrics for 1 hour
let metrics = runner.start_endurance_test(1)?;

// Analyze performance trends
let analysis = runner.analyze_performance_degradation();
println!("Throughput degradation: {:.2}%", analysis.throughput_degradation_percent);
```

### Custom Recovery Scenarios

```rust
use lightning_db::tests::stress_tests::recovery_tests::RecoveryTestSuite;

let recovery_suite = RecoveryTestSuite::new(db_path, config);
let results = recovery_suite.run_recovery_test_suite();

for result in results {
    println!("{}: {:.1}% data recovered in {}ms", 
        result.test_name, 
        result.data_recovery_rate * 100.0, 
        result.recovery_time_ms);
}
```

## Contributing

### Adding New Tests

1. Create test in appropriate module
2. Follow naming convention: `test_*_scenario`
3. Add ignore attribute: `#[ignore = "Description"]`
4. Document expected behavior and thresholds
5. Update test runner integration

### Test Development Guidelines

- Always clean up resources (use tempdir)
- Set reasonable timeouts
- Include proper error handling
- Document performance expectations
- Add integration to test runner

### Benchmarking Integration

```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn stress_benchmark(c: &mut Criterion) {
    c.bench_function("stress_limits", |b| {
        b.iter(|| {
            // Stress test benchmark
        })
    });
}

criterion_group!(benches, stress_benchmark);
criterion_main!(benches);
```

## Environment Variables

```bash
# Test configuration
export LIGHTNING_STRESS_DURATION=3600    # Endurance test duration (seconds)
export LIGHTNING_CACHE_SIZE=104857600     # Cache size (bytes)
export LIGHTNING_TEST_THREADS=8           # Number of test threads
export LIGHTNING_OUTPUT_FORMAT=json       # Output format (console/json/html/all)

# Resource limits
export LIGHTNING_MAX_MEMORY=2147483648    # Max memory usage (bytes)
export LIGHTNING_MAX_CONNECTIONS=100      # Max concurrent connections
export LIGHTNING_TIMEOUT=300              # Test timeout (seconds)

# Platform-specific
export LIGHTNING_DIRECT_IO=true           # Enable direct I/O (Linux)
export LIGHTNING_MMAP_SIZE=268435456      # Memory map size (bytes)
```

## Best Practices

### Production Validation

1. **Always run compatibility tests first**
2. **Test on actual production hardware**
3. **Use realistic data sizes and patterns**
4. **Monitor system resources during tests**
5. **Validate results against production requirements**

### Test Environment Setup

```bash
# Prepare clean test environment
sudo sysctl vm.dirty_ratio=5
sudo sysctl vm.dirty_background_ratio=2
echo deadline | sudo tee /sys/block/*/queue/scheduler

# Monitor during tests
iostat -x 1 &
vmstat 1 &
top -p $(pgrep -f lightning_db) &
```

### Results Analysis

1. **Compare results across different systems**
2. **Track performance trends over time**
3. **Correlate failures with system conditions**
4. **Document any configuration changes needed**
5. **Share results with team for review**

## Support

For issues with the stress testing suite:

1. Check system requirements and resource availability
2. Review configuration options and environment variables
3. Run individual test suites to isolate problems
4. Check logs and error messages for specific failures
5. Open an issue with system details and test results

## License

This stress testing suite is part of Lightning DB and follows the same license terms.
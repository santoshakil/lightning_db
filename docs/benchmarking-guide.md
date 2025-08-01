# Lightning DB Benchmarking Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Benchmarking Tools](#benchmarking-tools)
3. [Performance Metrics](#performance-metrics)
4. [Benchmark Scenarios](#benchmark-scenarios)
5. [Hardware Considerations](#hardware-considerations)
6. [Running Benchmarks](#running-benchmarks)
7. [Interpreting Results](#interpreting-results)
8. [Performance Baselines](#performance-baselines)
9. [Optimization Tips](#optimization-tips)
10. [Troubleshooting](#troubleshooting)

---

## Introduction

This guide provides comprehensive instructions for benchmarking Lightning DB to:
- Establish performance baselines
- Compare different configurations
- Identify bottlenecks
- Validate performance improvements
- Plan capacity requirements

### Key Principles

1. **Reproducibility** - Benchmarks must be repeatable
2. **Realism** - Test actual workload patterns
3. **Isolation** - Minimize external factors
4. **Measurement** - Track multiple metrics
5. **Documentation** - Record all parameters

---

## Benchmarking Tools

### Built-in Benchmark Suite

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench btree

# Run with specific features
cargo bench --features "encryption,compression"

# Generate detailed report
cargo bench -- --save-baseline baseline_v1
```

### Custom Benchmark Framework

```rust
use lightning_db::{Database, BenchmarkConfig};
use std::time::Instant;

let config = BenchmarkConfig {
    duration: Duration::from_secs(60),
    threads: num_cpus::get(),
    key_size: 32,
    value_size: 1024,
    operation_mix: OperationMix {
        read_percent: 80,
        write_percent: 15,
        delete_percent: 5,
    },
};

let results = db.run_benchmark(config)?;
println!("Throughput: {} ops/sec", results.ops_per_second);
println!("P99 latency: {:?}", results.p99_latency);
```

### External Tools

1. **YCSB** (Yahoo! Cloud Serving Benchmark)
   ```bash
   # Run YCSB workload A (50/50 read/write)
   ./ycsb load lightning -P workloads/workloada
   ./ycsb run lightning -P workloads/workloada
   ```

2. **sysbench** for database workloads
3. **fio** for I/O subsystem testing
4. **perf** for CPU profiling

---

## Performance Metrics

### Primary Metrics

1. **Throughput**
   - Operations per second (ops/sec)
   - Megabytes per second (MB/s)
   - Transactions per second (TPS)

2. **Latency**
   - Average latency
   - P50, P95, P99, P99.9 percentiles
   - Maximum latency

3. **Resource Utilization**
   - CPU usage (%)
   - Memory consumption (MB/GB)
   - Disk I/O (IOPS, bandwidth)
   - Network traffic (if distributed)

### Secondary Metrics

1. **Efficiency Metrics**
   - Write amplification
   - Space amplification
   - Cache hit ratio
   - Compression ratio

2. **Stability Metrics**
   - Latency variance
   - Throughput consistency
   - Error rate
   - Recovery time

---

## Benchmark Scenarios

### 1. Random Read/Write

```rust
// Benchmark configuration
let config = BenchmarkConfig {
    workload: Workload::RandomUniform,
    key_range: 0..10_000_000,
    operation_mix: OperationMix {
        read_percent: 95,
        write_percent: 5,
        delete_percent: 0,
    },
    ..Default::default()
};
```

### 2. Sequential Write

```rust
// Bulk loading scenario
let config = BenchmarkConfig {
    workload: Workload::Sequential,
    batch_size: 1000,
    sync_interval: 10000,
    compression: CompressionType::Zstd,
    ..Default::default()
};
```

### 3. Hot Key Access

```rust
// Zipfian distribution (80/20 rule)
let config = BenchmarkConfig {
    workload: Workload::Zipfian {
        theta: 0.99, // Skew factor
    },
    cache_size: 1024 * 1024 * 1024, // 1GB
    ..Default::default()
};
```

### 4. Transaction Heavy

```rust
// OLTP-style workload
let config = BenchmarkConfig {
    workload: Workload::Transaction {
        operations_per_txn: 5,
        read_modify_write: true,
    },
    isolation_level: IsolationLevel::Serializable,
    ..Default::default()
};
```

### 5. Large Values

```rust
// Document/blob storage
let config = BenchmarkConfig {
    value_size_distribution: Distribution::Normal {
        mean: 64 * 1024,    // 64KB average
        stddev: 16 * 1024,  // 16KB standard deviation
    },
    compression: CompressionType::Lz4,
    ..Default::default()
};
```

---

## Hardware Considerations

### CPU Optimization

```bash
# Disable CPU frequency scaling
sudo cpupower frequency-set -g performance

# Pin benchmark to specific cores
taskset -c 0-7 cargo bench

# Enable turbo boost
echo 0 > /sys/devices/system/cpu/intel_pstate/no_turbo
```

### Memory Configuration

```bash
# Configure huge pages
echo 1024 > /proc/sys/vm/nr_hugepages

# Set swappiness to 0
echo 0 > /proc/sys/vm/swappiness

# Disable NUMA balancing for benchmarks
echo 0 > /proc/sys/kernel/numa_balancing
```

### Storage Optimization

```bash
# Use direct I/O mount options
mount -o noatime,nodiratime /dev/nvme0n1 /mnt/benchmark

# Disable filesystem barriers
mount -o nobarrier /dev/nvme0n1 /mnt/benchmark

# Pre-allocate test files
fallocate -l 100G /mnt/benchmark/test.db
```

---

## Running Benchmarks

### Standard Benchmark Run

```bash
# 1. Prepare environment
./scripts/prepare_benchmark_env.sh

# 2. Run warmup
cargo run --release --example benchmark_warmup

# 3. Run actual benchmark
cargo run --release --example comprehensive_benchmark > results.json

# 4. Generate report
./scripts/generate_benchmark_report.py results.json
```

### Automated Benchmark Suite

```rust
use lightning_db::benchmark::Suite;

#[tokio::main]
async fn main() -> Result<()> {
    let suite = Suite::new()
        .add_scenario("point_queries", point_query_bench())
        .add_scenario("range_scans", range_scan_bench())
        .add_scenario("mixed_workload", mixed_workload_bench())
        .add_scenario("concurrent_access", concurrent_bench())
        .with_duration(Duration::from_secs(300))
        .with_warmup(Duration::from_secs(60));
    
    let results = suite.run().await?;
    results.save_json("benchmark_results.json")?;
    results.generate_html_report("report.html")?;
    
    Ok(())
}
```

### Continuous Benchmarking

```yaml
# .github/workflows/benchmark.yml
name: Continuous Benchmark
on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
      - name: Run benchmarks
        run: cargo bench -- --save-baseline pr_${{ github.event.number }}
      - name: Compare results
        run: |
          cargo bench -- --baseline main --compare pr_${{ github.event.number }}
      - name: Comment PR
        uses: actions/github-script@v6
        with:
          script: |
            // Post benchmark comparison to PR
```

---

## Performance Baselines

### Reference Hardware

- **CPU**: AMD EPYC 7763 (64 cores, 128 threads)
- **Memory**: 256GB DDR4-3200
- **Storage**: Samsung PM9A3 NVMe (3.5GB/s read, 3.0GB/s write)
- **OS**: Ubuntu 22.04 LTS, kernel 5.15

### Baseline Results

#### Point Operations (32-byte key, 1KB value)

| Operation | Throughput | P50 Latency | P99 Latency |
|-----------|------------|-------------|-------------|
| Get       | 20.4M ops/s | 0.042 μs   | 0.089 μs   |
| Put       | 1.14M ops/s | 0.77 μs    | 1.23 μs    |
| Delete    | 952K ops/s  | 0.91 μs    | 1.45 μs    |

#### Range Queries

| Operation | Throughput | Records/sec |
|-----------|------------|-------------|
| Forward Scan | 142K iter/s | 142M rec/s |
| Reverse Scan | 138K iter/s | 138M rec/s |
| Prefix Scan  | 156K iter/s | 78M rec/s  |

#### Transactions

| Workload | TPS | P99 Latency |
|----------|-----|-------------|
| Read-only | 2.8M | 0.18 μs |
| Write-heavy | 412K | 3.2 μs |
| Mixed (80/20) | 1.1M | 1.4 μs |

#### Resource Usage

| Metric | Value |
|--------|-------|
| Memory per 1M keys | 38 MB |
| Write amplification | 3.2x |
| Compression ratio | 2.8x |
| Cache hit rate | 94.3% |

---

## Interpreting Results

### Performance Analysis

1. **Latency Distribution**
   ```
   Latency Percentiles (microseconds):
   P50: 0.82  <- Half of operations faster than this
   P90: 1.23  <- 90% of operations faster than this
   P95: 1.56
   P99: 2.34  <- Critical for user experience
   P99.9: 12.4 <- Tail latency, watch for outliers
   Max: 234.5  <- Worst case, investigate if high
   ```

2. **Throughput Patterns**
   - Steady state vs. peaks
   - Degradation over time
   - Impact of compaction
   - Cache warmup effects

3. **Resource Correlation**
   - CPU usage vs. throughput
   - Memory usage vs. cache hits
   - I/O bandwidth vs. write rate

### Common Issues

1. **High Tail Latency**
   - Check GC pauses
   - Look for lock contention
   - Verify compaction scheduling

2. **Throughput Degradation**
   - Monitor write amplification
   - Check cache efficiency
   - Verify no throttling

3. **Unstable Performance**
   - Disable power saving
   - Check for background processes
   - Ensure exclusive hardware access

---

## Optimization Tips

### Configuration Tuning

```rust
// Optimize for read-heavy workload
let config = LightningDbConfig {
    cache_size: available_memory * 0.5,
    bloom_filter_bits: 10,
    block_cache_size: 32 * 1024 * 1024,
    compression: CompressionType::None, // Faster reads
    ..Default::default()
};

// Optimize for write-heavy workload  
let config = LightningDbConfig {
    write_buffer_size: 128 * 1024 * 1024,
    max_write_buffer_number: 4,
    level0_file_num_compaction_trigger: 8,
    compression: CompressionType::Lz4, // Fast compression
    ..Default::default()
};
```

### Workload-Specific Optimizations

1. **Time-Series Data**
   - Use sequential keys
   - Enable prefix compression
   - Tune compaction for append-only

2. **Key-Value Cache**
   - Maximize cache size
   - Use no compression
   - Enable bloom filters

3. **Analytics Workload**
   - Use columnar storage
   - Enable vectorized execution
   - Optimize for range scans

---

## Troubleshooting

### Performance Debugging

```bash
# CPU profiling
perf record -g cargo run --release --example benchmark
perf report

# Memory profiling
valgrind --tool=massif cargo run --release --example benchmark
ms_print massif.out.*

# I/O tracing
iotop -o -b -d 1

# Lock contention
cargo build --release --features "lock_profiling"
```

### Common Solutions

| Problem | Solution |
|---------|----------|
| High CPU usage | Check compression settings, enable SIMD |
| Memory growth | Tune cache size, check for leaks |
| Slow writes | Increase write buffer, tune compaction |
| Poor cache hits | Increase cache size, check access patterns |
| I/O bottleneck | Enable compression, use faster storage |

---

## Best Practices

1. **Always warm up** - Run workload for 60+ seconds before measuring
2. **Multiple runs** - Average results from 5+ runs
3. **Monitor everything** - CPU, memory, I/O, network
4. **Document setup** - Hardware, OS, configuration
5. **Test at scale** - Use realistic data sizes
6. **Vary parameters** - Test different key/value sizes
7. **Check correctness** - Verify data integrity after benchmarks

---

## Example Benchmark Script

```bash
#!/bin/bash
# comprehensive_benchmark.sh

# Setup
echo "=== Lightning DB Comprehensive Benchmark ==="
date

# System info
echo "CPU: $(lscpu | grep 'Model name' | cut -d: -f2)"
echo "Memory: $(free -h | grep Mem | awk '{print $2}')"
echo "Storage: $(lsblk -d -o NAME,SIZE,MODEL | grep nvme)"

# Prepare
sync
echo 3 > /proc/sys/vm/drop_caches

# Run benchmarks
for workload in point_ops range_scan transactions large_values; do
    echo "Running $workload benchmark..."
    cargo run --release --example benchmark_$workload \
        --iterations 5 \
        --duration 300 \
        --output results_$workload.json
done

# Generate report
./scripts/analyze_benchmarks.py results_*.json > benchmark_report.md

echo "Benchmark complete!"
```

---

For latest benchmarks and comparisons, see [BENCHMARK_RESULTS.md](../BENCHMARK_RESULTS.md).
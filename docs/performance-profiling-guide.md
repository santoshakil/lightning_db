# Lightning DB Performance Profiling Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Profiling Tools](#profiling-tools)
3. [CPU Profiling](#cpu-profiling)
4. [Memory Profiling](#memory-profiling)
5. [I/O Profiling](#io-profiling)
6. [Lock Contention Analysis](#lock-contention-analysis)
7. [Cache Analysis](#cache-analysis)
8. [Query Profiling](#query-profiling)
9. [Production Profiling](#production-profiling)
10. [Common Performance Issues](#common-performance-issues)

---

## Introduction

Performance profiling is essential for:
- Identifying bottlenecks
- Optimizing critical paths
- Reducing resource usage
- Improving response times
- Capacity planning

### Profiling Methodology

1. **Baseline** - Establish normal performance
2. **Measure** - Profile under load
3. **Analyze** - Identify bottlenecks
4. **Optimize** - Make targeted improvements
5. **Verify** - Confirm improvements

---

## Profiling Tools

### Built-in Profiling

```rust
use lightning_db::{Database, ProfilingConfig};

// Enable profiling
let profiling = ProfilingConfig {
    cpu_profiling: true,
    memory_tracking: true,
    io_stats: true,
    lock_tracking: true,
    query_stats: true,
    sampling_rate: 0.01, // 1% sampling
};

db.enable_profiling(profiling)?;

// Get profiling data
let profile = db.get_profile()?;
profile.save_flamegraph("profile.svg")?;
profile.export_pprof("profile.pb")?;
```

### External Tools

#### Linux perf
```bash
# CPU profiling
perf record -F 99 -g --call-graph dwarf ./target/release/myapp
perf report

# Flame graph generation
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg
```

#### Valgrind Suite
```bash
# CPU profiling with callgrind
valgrind --tool=callgrind ./target/release/myapp

# Memory profiling with massif
valgrind --tool=massif --heap=yes --stacks=yes ./target/release/myapp

# Cache profiling with cachegrind
valgrind --tool=cachegrind ./target/release/myapp
```

#### BPF Tools
```bash
# Profile block I/O
sudo biolatency -m

# Profile file system latency
sudo filetop -C

# Profile TCP retransmissions
sudo tcpretrans
```

---

## CPU Profiling

### Identifying Hot Paths

```rust
// Enable CPU profiling
#[cfg(feature = "profiling")]
{
    let _guard = pprof::ProfilerGuard::new(100)?;
    
    // Run workload
    run_benchmark()?;
    
    // Generate report
    if let Ok(report) = _guard.report().build() {
        let file = File::create("cpu_profile.svg")?;
        report.flamegraph(&file)?;
    }
}
```

### Common CPU Bottlenecks

1. **Compression/Decompression**
   ```rust
   // Profile compression overhead
   db.set_profiling_filter(ProfilingFilter::Compression)?;
   let stats = db.get_compression_stats()?;
   println!("Compression CPU: {}%", stats.cpu_percentage);
   ```

2. **Serialization**
   ```rust
   // Check serialization cost
   let profile = db.profile_operation(|| {
       db.put(&key, &large_value)?;
   })?;
   println!("Serialization time: {:?}", profile.serialization_time);
   ```

3. **Lock Contention**
   ```rust
   // Measure lock wait time
   let lock_stats = db.get_lock_stats()?;
   for (lock_name, stats) in lock_stats {
       if stats.wait_time > Duration::from_millis(10) {
           println!("Lock {} contention: {:?}", lock_name, stats.wait_time);
       }
   }
   ```

### CPU Optimization Strategies

```rust
// 1. Enable SIMD optimizations
db.enable_simd()?;

// 2. Adjust thread pool size
db.set_worker_threads(num_cpus::get())?;

// 3. Batch operations
let mut batch = WriteBatch::new();
for (k, v) in items {
    batch.put(k, v);
}
db.write_batch(&batch)?;

// 4. Use appropriate compression
db.set_compression(CompressionType::Lz4)?; // Faster than Zstd
```

---

## Memory Profiling

### Memory Usage Analysis

```rust
// Track memory allocations
use lightning_db::MemoryProfiler;

let profiler = MemoryProfiler::new();
profiler.start()?;

// Run operations
perform_workload()?;

// Analyze results
let report = profiler.stop()?;
println!("Total allocated: {} MB", report.total_allocated_mb());
println!("Peak usage: {} MB", report.peak_usage_mb());
println!("Allocation rate: {} MB/s", report.allocation_rate_mb_per_sec());

// Find memory leaks
for leak in report.potential_leaks() {
    println!("Potential leak: {} bytes at {}", leak.size, leak.location);
}
```

### Heap Profiling

```bash
# Using jemalloc profiling
export MALLOC_CONF=prof:true,prof_prefix:jeprof.out
./target/release/myapp

# Generate heap profile
jeprof --show_bytes --pdf ./target/release/myapp jeprof.out.* > heap.pdf
```

### Memory Optimization

```rust
// 1. Configure cache size based on available memory
let available = sys_info::mem_info()?.avail * 1024;
db.set_cache_size(available / 2)?;

// 2. Enable memory pooling
db.enable_memory_pools()?;

// 3. Tune garbage collection
db.set_gc_interval(Duration::from_secs(300))?;
db.set_gc_threshold(0.8)?; // GC when 80% full

// 4. Use bloom filters to reduce memory access
db.set_bloom_filter_bits_per_key(10)?;
```

---

## I/O Profiling

### Disk I/O Analysis

```rust
// Enable I/O statistics
db.enable_io_stats()?;

// Get I/O metrics
let io_stats = db.get_io_stats()?;
println!("Read ops: {}", io_stats.read_ops);
println!("Write ops: {}", io_stats.write_ops);
println!("Read bytes: {} MB", io_stats.read_bytes / 1024 / 1024);
println!("Write bytes: {} MB", io_stats.write_bytes / 1024 / 1024);
println!("Read latency P99: {:?}", io_stats.read_latency_p99);
println!("Write latency P99: {:?}", io_stats.write_latency_p99);
```

### I/O Patterns

```bash
# Monitor I/O patterns with iotop
sudo iotop -o -b -d 1 -p $(pgrep myapp)

# Trace block I/O with blktrace
sudo blktrace -d /dev/nvme0n1 -o trace
blkparse -i trace -o trace.txt

# Analyze I/O size distribution
sudo bpftrace -e 'tracepoint:block:block_rq_issue { @bytes = hist(args->bytes); }'
```

### I/O Optimization

```rust
// 1. Enable direct I/O for large values
db.set_direct_io_threshold(64 * 1024)?; // 64KB

// 2. Configure read-ahead
db.set_readahead_size(256 * 1024)?; // 256KB

// 3. Batch writes
db.set_write_batch_size(1000)?;

// 4. Optimize compaction I/O
db.set_compaction_readahead(2 * 1024 * 1024)?; // 2MB
db.set_max_background_jobs(4)?;
```

---

## Lock Contention Analysis

### Lock Profiling

```rust
// Enable lock profiling
#[cfg(feature = "lock_profiling")]
db.enable_lock_profiling()?;

// Run workload
run_concurrent_workload()?;

// Analyze lock contention
let lock_report = db.get_lock_report()?;
for lock in lock_report.hot_locks() {
    println!("Lock: {}", lock.name);
    println!("  Acquisitions: {}", lock.acquisitions);
    println!("  Contention rate: {:.2}%", lock.contention_rate * 100.0);
    println!("  Avg wait time: {:?}", lock.avg_wait_time);
    println!("  Max wait time: {:?}", lock.max_wait_time);
}
```

### Visualizing Lock Contention

```rust
// Generate lock contention heat map
let heatmap = db.generate_lock_heatmap()?;
heatmap.save_svg("lock_contention.svg")?;

// Export lock wait graph
let wait_graph = db.export_lock_wait_graph()?;
wait_graph.save_dot("lock_waits.dot")?;
```

### Reducing Lock Contention

```rust
// 1. Use finer-grained locking
db.set_lock_granularity(LockGranularity::Page)?;

// 2. Enable lock-free data structures
db.enable_lock_free_index()?;

// 3. Shard hot locks
db.set_lock_striping(16)?; // 16-way striping

// 4. Use optimistic concurrency
db.set_optimistic_transaction_retries(3)?;
```

---

## Cache Analysis

### Cache Performance Metrics

```rust
// Get cache statistics
let cache_stats = db.get_cache_stats()?;
println!("Cache size: {} MB", cache_stats.size_mb);
println!("Hit rate: {:.2}%", cache_stats.hit_rate * 100.0);
println!("Miss rate: {:.2}%", cache_stats.miss_rate * 100.0);
println!("Eviction rate: {} /sec", cache_stats.evictions_per_sec);

// Analyze cache efficiency by data type
for (data_type, stats) in cache_stats.by_type() {
    println!("{}: hit_rate={:.2}%, avg_size={} bytes", 
        data_type, stats.hit_rate * 100.0, stats.avg_entry_size);
}
```

### Cache Profiling

```rust
// Profile cache misses
db.profile_cache_misses(|miss| {
    println!("Cache miss: key={:?}, reason={:?}", miss.key, miss.reason);
})?;

// Trace cache access patterns
let trace = db.trace_cache_access(Duration::from_secs(60))?;
trace.save_parquet("cache_trace.parquet")?;
```

### Cache Optimization

```rust
// 1. Tune cache size dynamically
db.enable_adaptive_cache_sizing()?;

// 2. Implement cache warming
db.prewarm_cache(frequently_accessed_keys)?;

// 3. Use tiered caching
db.enable_tiered_cache(TierConfig {
    l1_size: 1024 * 1024 * 1024,  // 1GB fast cache
    l2_size: 10 * 1024 * 1024 * 1024, // 10GB slower cache
})?;

// 4. Optimize cache admission policy
db.set_cache_admission_policy(AdmissionPolicy::TinyLFU)?;
```

---

## Query Profiling

### Query Performance Analysis

```rust
// Enable query profiling
db.enable_query_profiling()?;

// Execute queries
let result = db.execute_query(query)?;

// Get query profile
let profile = db.get_last_query_profile()?;
println!("Query execution time: {:?}", profile.total_time);
println!("Planning time: {:?}", profile.planning_time);
println!("Execution time: {:?}", profile.execution_time);

// Show query plan
println!("Query plan:");
for step in profile.execution_plan {
    println!("  {} (cost={}, rows={})", 
        step.operation, step.cost, step.estimated_rows);
}
```

### Slow Query Log

```rust
// Configure slow query logging
db.set_slow_query_threshold(Duration::from_millis(100))?;
db.enable_slow_query_log("slow_queries.log")?;

// Analyze slow queries
let slow_queries = db.get_slow_queries()?;
for query in slow_queries.top_10_by_total_time() {
    println!("Query: {}", query.query_text);
    println!("  Executions: {}", query.execution_count);
    println!("  Avg time: {:?}", query.avg_execution_time);
    println!("  Total time: {:?}", query.total_execution_time);
}
```

### Query Optimization

```rust
// 1. Create appropriate indexes
db.create_index("user_email_idx", "users", vec!["email"])?;

// 2. Enable query result caching
db.enable_query_cache(QueryCacheConfig {
    max_entries: 10000,
    ttl: Duration::from_secs(300),
})?;

// 3. Use prepared statements
let stmt = db.prepare("SELECT * FROM users WHERE age > ?")?;
let result = stmt.execute(&[&25])?;

// 4. Optimize join order
db.set_join_optimizer(JoinOptimizer::DynamicProgramming)?;
```

---

## Production Profiling

### Low-Overhead Profiling

```rust
// Configure production-safe profiling
let prod_profiling = ProfilingConfig {
    sampling_rate: 0.001, // 0.1% sampling
    max_overhead_percent: 1.0, // Max 1% overhead
    profile_duration: Duration::from_secs(300),
    async_export: true,
};

db.enable_production_profiling(prod_profiling)?;
```

### Continuous Profiling

```rust
// Set up continuous profiling
use lightning_db::ContinuousProfiler;

let profiler = ContinuousProfiler::new(db.clone());
profiler.configure(|config| {
    config.export_interval = Duration::from_hours(1);
    config.retention_days = 7;
    config.export_format = ExportFormat::Pprof;
    config.upload_endpoint = Some("https://profiling.internal/upload");
})?;

profiler.start()?;
```

### A/B Performance Testing

```rust
// Compare performance of two configurations
use lightning_db::ABTester;

let tester = ABTester::new();

// Configuration A: Default
let config_a = LightningDbConfig::default();

// Configuration B: Optimized
let config_b = LightningDbConfig {
    cache_size: 2 * config_a.cache_size,
    compression: CompressionType::None,
    ..config_a.clone()
};

let results = tester.run_comparison(
    config_a, 
    config_b,
    workload,
    Duration::from_hours(1),
)?;

println!("Configuration B vs A:");
println!("  Throughput: {:+.1}%", results.throughput_change * 100.0);
println!("  P99 latency: {:+.1}%", results.p99_latency_change * 100.0);
println!("  CPU usage: {:+.1}%", results.cpu_change * 100.0);
```

---

## Common Performance Issues

### 1. High CPU Usage

**Symptoms**: CPU at 100%, slow responses
**Diagnosis**:
```bash
# Check top CPU consumers
perf top -p $(pgrep myapp)

# Profile CPU usage
perf record -F 99 -g -p $(pgrep myapp) -- sleep 30
perf report
```

**Common Causes & Solutions**:
- Compression overhead → Use LZ4 instead of Zstd
- Inefficient serialization → Enable SIMD, use faster formats
- Lock contention → Enable lock-free structures
- Excessive GC → Tune GC parameters

### 2. Memory Growth

**Symptoms**: Increasing memory usage, OOM kills
**Diagnosis**:
```rust
// Track memory growth
let tracker = MemoryGrowthTracker::new();
tracker.start_monitoring(Duration::from_secs(60))?;

// Check for leaks
let leaks = tracker.detect_leaks()?;
```

**Common Causes & Solutions**:
- Cache unbounded growth → Set max cache size
- Memory fragmentation → Enable jemalloc
- Transaction leaks → Ensure all transactions commit/abort
- Large value accumulation → Enable value compression

### 3. Slow Queries

**Symptoms**: High query latency, timeouts
**Diagnosis**:
```rust
// Analyze query patterns
let analyzer = QueryAnalyzer::new(&db);
let report = analyzer.analyze_slow_queries()?;

for issue in report.issues {
    println!("Issue: {}", issue.description);
    println!("Impact: {} queries affected", issue.affected_queries);
    println!("Recommendation: {}", issue.recommendation);
}
```

**Common Causes & Solutions**:
- Missing indexes → Create appropriate indexes
- Poor cache locality → Reorganize data layout
- Inefficient scan patterns → Use bloom filters
- Lock contention → Implement MVCC

### 4. I/O Bottlenecks

**Symptoms**: High I/O wait, slow disk operations
**Diagnosis**:
```bash
# Monitor disk utilization
iostat -x 1

# Check I/O patterns
iotop -o -P -p $(pgrep myapp)
```

**Common Causes & Solutions**:
- Small random I/Os → Enable write batching
- Compaction storms → Tune compaction schedule
- Poor SSD performance → Enable TRIM, check alignment
- Insufficient parallelism → Increase background threads

---

## Best Practices

1. **Profile regularly** - Establish baseline, detect regressions
2. **Use production data** - Test with realistic workloads
3. **Profile holistically** - CPU, memory, I/O, locks together
4. **Automate analysis** - Set up continuous profiling
5. **Document findings** - Track optimizations and their impact
6. **Test optimizations** - Verify improvements don't break functionality
7. **Monitor overhead** - Ensure profiling doesn't impact production

---

## Profiling Checklist

Before profiling:
- [ ] Define performance goals
- [ ] Set up isolated environment  
- [ ] Prepare representative workload
- [ ] Baseline current performance
- [ ] Enable necessary profiling features

During profiling:
- [ ] Monitor profiling overhead
- [ ] Collect multiple samples
- [ ] Profile peak and steady state
- [ ] Save raw profiling data
- [ ] Document environment details

After profiling:
- [ ] Analyze hotspots
- [ ] Identify optimization opportunities
- [ ] Test proposed changes
- [ ] Measure improvement
- [ ] Update documentation

---

For more details, see the [Performance Tuning Guide](./PRODUCTION_PERFORMANCE_TUNING_GUIDE.md).
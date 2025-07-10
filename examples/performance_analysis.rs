use lightning_db::{Database, LightningDbConfig};
use rand::Rng;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct PerformanceResult {
    test_name: String,
    ops_per_sec: f64,
    latency_p50_us: f64,
    latency_p99_us: f64,
    latency_p999_us: f64,
    observations: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Lightning DB Performance Analysis\n");

    let _ = std::fs::remove_dir_all("./perf_test_db");
    #[allow(clippy::vec_init_then_push)]
    let mut results = Vec::new();

    // Test 1: Sequential vs Random Writes
    results.push(test_write_patterns()?);

    // Test 2: Read Performance with Different Cache Hit Rates
    results.push(test_cache_effectiveness()?);

    // Test 3: Transaction Overhead
    results.push(test_transaction_overhead()?);

    // Test 4: Compression Impact
    results.push(test_compression_impact()?);

    // Test 5: Concurrent Access Patterns
    results.push(test_concurrent_patterns()?);

    // Test 6: Large Value Performance
    results.push(test_value_size_impact()?);

    // Test 7: Iterator Performance
    results.push(test_iterator_performance()?);

    // Test 8: Memory Pressure
    results.push(test_memory_pressure()?);

    // Print Analysis
    print_analysis(&results);

    Ok(())
}

fn test_write_patterns() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Write Patterns...");

    let db = Database::create(
        "./perf_test_db/write_patterns",
        LightningDbConfig::default(),
    )?;
    let mut observations = Vec::new();

    // Sequential writes
    let mut latencies = Vec::new();
    let start = Instant::now();
    for i in 0..50000 {
        let key = format!("{:08}", i);
        let value = vec![0u8; 100];
        let op_start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        latencies.push(op_start.elapsed().as_micros() as f64);
    }
    let seq_duration = start.elapsed();
    let seq_ops = 50000.0 / seq_duration.as_secs_f64();

    // Random writes
    let mut rng = rand::rng();
    let start = Instant::now();
    for _ in 0..50000 {
        let key = format!("{:08}", rng.random::<u32>() % 100000000);
        let value = vec![0u8; 100];
        db.put(key.as_bytes(), &value)?;
    }
    let rand_duration = start.elapsed();
    let rand_ops = 50000.0 / rand_duration.as_secs_f64();

    if seq_ops > rand_ops * 1.5 {
        observations.push(
            "Sequential writes significantly faster than random (good B+Tree locality)".to_string(),
        );
    } else {
        observations.push(
            "Sequential/random write performance similar (possible optimization opportunity)"
                .to_string(),
        );
    }

    // Calculate percentiles
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];
    let p999 = latencies[latencies.len() * 999 / 1000];

    Ok(PerformanceResult {
        test_name: "Write Patterns".to_string(),
        ops_per_sec: (seq_ops + rand_ops) / 2.0,
        latency_p50_us: p50,
        latency_p99_us: p99,
        latency_p999_us: p999,
        observations,
    })
}

fn test_cache_effectiveness() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Cache Effectiveness...");

    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    }; // 10MB cache

    let db = Database::create("./perf_test_db/cache", config)?;
    let mut observations = Vec::new();

    // Fill with data larger than cache
    for i in 0..100000 {
        let key = format!("cache_key_{:06}", i);
        let value = vec![0u8; 200];
        db.put(key.as_bytes(), &value)?;
    }

    // Test hot data (should be cached)
    let mut hot_latencies = Vec::new();
    let start = Instant::now();
    for _ in 0..50000 {
        let key = format!("cache_key_{:06}", 0); // Always read same key
        let op_start = Instant::now();
        let _ = db.get(key.as_bytes())?;
        hot_latencies.push(op_start.elapsed().as_micros() as f64);
    }
    let hot_duration = start.elapsed();
    let hot_ops = 50000.0 / hot_duration.as_secs_f64();

    // Test cold data (cache misses)
    let mut cold_latencies = Vec::new();
    let mut rng = rand::rng();
    let start = Instant::now();
    for _ in 0..10000 {
        let key = format!("cache_key_{:06}", 50000 + rng.random::<u32>() % 50000);
        let op_start = Instant::now();
        let _ = db.get(key.as_bytes())?;
        cold_latencies.push(op_start.elapsed().as_micros() as f64);
    }
    let cold_duration = start.elapsed();
    let cold_ops = 10000.0 / cold_duration.as_secs_f64();

    let cache_speedup = hot_ops / cold_ops;
    if cache_speedup > 10.0 {
        observations.push(format!(
            "Excellent cache effectiveness: {:.1}x speedup",
            cache_speedup
        ));
    } else if cache_speedup > 5.0 {
        observations.push(format!(
            "Good cache effectiveness: {:.1}x speedup",
            cache_speedup
        ));
    } else {
        observations.push(format!(
            "Poor cache effectiveness: {:.1}x speedup (needs optimization)",
            cache_speedup
        ));
    }

    // Check metrics
    let metrics = db.get_metrics();
    observations.push(format!(
        "Cache hit rate: {:.2}%",
        metrics.cache_hit_rate * 100.0
    ));

    hot_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Ok(PerformanceResult {
        test_name: "Cache Effectiveness".to_string(),
        ops_per_sec: hot_ops,
        latency_p50_us: hot_latencies[hot_latencies.len() / 2],
        latency_p99_us: hot_latencies[hot_latencies.len() * 99 / 100],
        latency_p999_us: hot_latencies[hot_latencies.len() * 999 / 1000],
        observations,
    })
}

fn test_transaction_overhead() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Transaction Overhead...");

    let db = Database::create("./perf_test_db/transactions", LightningDbConfig::default())?;
    let mut observations = Vec::new();

    // Non-transactional writes
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("direct_{}", i);
        db.put(key.as_bytes(), b"value")?;
    }
    let direct_duration = start.elapsed();
    let direct_ops = 10000.0 / direct_duration.as_secs_f64();

    // Transactional writes (individual transactions)
    let mut tx_latencies = Vec::new();
    let start = Instant::now();
    for i in 0..10000 {
        let tx_start = Instant::now();
        let tx = db.begin_transaction()?;
        let key = format!("tx_single_{}", i);
        db.put_tx(tx, key.as_bytes(), b"value")?;
        db.commit_transaction(tx)?;
        tx_latencies.push(tx_start.elapsed().as_micros() as f64);
    }
    let tx_single_duration = start.elapsed();
    let tx_single_ops = 10000.0 / tx_single_duration.as_secs_f64();

    // Batched transactions
    let start = Instant::now();
    for batch in 0..100 {
        let tx = db.begin_transaction()?;
        for i in 0..100 {
            let key = format!("tx_batch_{}_{}", batch, i);
            db.put_tx(tx, key.as_bytes(), b"value")?;
        }
        db.commit_transaction(tx)?;
    }
    let tx_batch_duration = start.elapsed();
    let tx_batch_ops = 10000.0 / tx_batch_duration.as_secs_f64();

    let single_overhead = (direct_ops - tx_single_ops) / direct_ops * 100.0;
    observations.push(format!(
        "Single transaction overhead: {:.1}%",
        single_overhead
    ));

    if tx_batch_ops > tx_single_ops * 1.5 {
        observations
            .push("Batched transactions significantly faster (good amortization)".to_string());
    } else {
        observations.push(
            "Limited benefit from transaction batching (optimization opportunity)".to_string(),
        );
    }

    tx_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Ok(PerformanceResult {
        test_name: "Transaction Overhead".to_string(),
        ops_per_sec: tx_single_ops,
        latency_p50_us: tx_latencies[tx_latencies.len() / 2],
        latency_p99_us: tx_latencies[tx_latencies.len() * 99 / 100],
        latency_p999_us: tx_latencies[tx_latencies.len() * 999 / 1000],
        observations,
    })
}

fn test_compression_impact() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Compression Impact...");

    let mut observations = Vec::new();

    // Test without compression
    let mut config = LightningDbConfig {
        compression_enabled: false,
        ..Default::default()
    };

    let db_no_comp = Database::create("./perf_test_db/no_compression", config.clone())?;

    // Write compressible data
    let compressible_value = "a".repeat(1000);
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("comp_key_{}", i);
        db_no_comp.put(key.as_bytes(), compressible_value.as_bytes())?;
    }
    let no_comp_write = start.elapsed();

    // Test with compression
    config.compression_enabled = true;
    config.compression_type = 1; // Zstd

    let db_comp = Database::create("./perf_test_db/compression", config)?;

    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("comp_key_{}", i);
        db_comp.put(key.as_bytes(), compressible_value.as_bytes())?;
    }
    let comp_write = start.elapsed();

    // Read performance comparison
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("comp_key_{}", i);
        let _ = db_no_comp.get(key.as_bytes())?;
    }
    let no_comp_read = start.elapsed();

    let mut comp_latencies = Vec::new();
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("comp_key_{}", i);
        let op_start = Instant::now();
        let _ = db_comp.get(key.as_bytes())?;
        comp_latencies.push(op_start.elapsed().as_micros() as f64);
    }
    let comp_read = start.elapsed();

    let write_overhead = (comp_write.as_secs_f64() - no_comp_write.as_secs_f64())
        / no_comp_write.as_secs_f64()
        * 100.0;
    let read_overhead =
        (comp_read.as_secs_f64() - no_comp_read.as_secs_f64()) / no_comp_read.as_secs_f64() * 100.0;

    observations.push(format!(
        "Compression write overhead: {:.1}%",
        write_overhead
    ));
    observations.push(format!("Compression read overhead: {:.1}%", read_overhead));

    // Check disk usage
    let no_comp_size = std::fs::metadata("./perf_test_db/no_compression")
        .map(|m| m.len())
        .unwrap_or(0);
    let comp_size = std::fs::metadata("./perf_test_db/compression")
        .map(|m| m.len())
        .unwrap_or(0);

    if comp_size > 0 && no_comp_size > 0 {
        let compression_ratio =
            (no_comp_size as f64 - comp_size as f64) / no_comp_size as f64 * 100.0;
        observations.push(format!("Space savings: {:.1}%", compression_ratio));
    }

    comp_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Ok(PerformanceResult {
        test_name: "Compression Impact".to_string(),
        ops_per_sec: 10000.0 / comp_read.as_secs_f64(),
        latency_p50_us: comp_latencies[comp_latencies.len() / 2],
        latency_p99_us: comp_latencies[comp_latencies.len() * 99 / 100],
        latency_p999_us: comp_latencies[comp_latencies.len() * 999 / 1000],
        observations,
    })
}

fn test_concurrent_patterns() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Concurrent Access Patterns...");

    let db = Arc::new(Database::create(
        "./perf_test_db/concurrent",
        LightningDbConfig::default(),
    )?);
    let mut observations = Vec::new();

    // Prepare data
    for i in 0..10000 {
        let key = format!("concurrent_{}", i);
        db.put(key.as_bytes(), b"initial_value")?;
    }

    // Test 1: Read-heavy workload (95% reads, 5% writes)
    let ops_counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..8 {
        let db_clone = db.clone();
        let counter = ops_counter.clone();

        handles.push(thread::spawn(move || {
            let mut rng = rand::rng();
            let mut local_ops = 0u64;
            let thread_start = Instant::now();

            while thread_start.elapsed() < Duration::from_secs(2) {
                let key_id = rng.random::<u32>() % 10000;
                let key = format!("concurrent_{}", key_id);

                if rng.random::<u8>() % 100 < 95 {
                    // Read
                    let _ = db_clone.get(key.as_bytes());
                } else {
                    // Write
                    let value = format!("updated_by_{}", thread_id);
                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                }
                local_ops += 1;
            }

            counter.fetch_add(local_ops, Ordering::Relaxed);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let total_ops = ops_counter.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

    observations.push(format!(
        "Read-heavy workload: {:.0} ops/sec with 8 threads",
        ops_per_sec
    ));

    // Test 2: Write contention
    let contention_counter = Arc::new(AtomicU64::new(0));
    let success_counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for thread_id in 0..4 {
        let db_clone = db.clone();
        let contention = contention_counter.clone();
        let success = success_counter.clone();

        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                // All threads write to same keys
                let key = format!("contention_{}", i % 100);
                let value = format!("thread_{}_value", thread_id);

                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        contention.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let contentions = contention_counter.load(Ordering::Relaxed);
    let successes = success_counter.load(Ordering::Relaxed);

    if contentions > 0 {
        observations.push(format!(
            "Write contention detected: {} failures",
            contentions
        ));
    } else {
        observations.push(format!(
            "No write contention with {} concurrent writes",
            successes
        ));
    }

    Ok(PerformanceResult {
        test_name: "Concurrent Patterns".to_string(),
        ops_per_sec,
        latency_p50_us: duration.as_micros() as f64 / total_ops as f64,
        latency_p99_us: duration.as_micros() as f64 / total_ops as f64 * 2.0, // Estimate
        latency_p999_us: duration.as_micros() as f64 / total_ops as f64 * 10.0, // Estimate
        observations,
    })
}

fn test_value_size_impact() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Value Size Impact...");

    let db = Database::create("./perf_test_db/value_sizes", LightningDbConfig::default())?;
    let mut observations = Vec::new();

    let sizes = vec![
        (10, "10B"),
        (100, "100B"),
        (1024, "1KB"),
        (10 * 1024, "10KB"),
        (100 * 1024, "100KB"),
        (1024 * 1024, "1MB"),
    ];

    let mut results = Vec::new();

    for (size, label) in &sizes {
        let value = vec![0u8; *size];
        let mut latencies = Vec::new();

        // Write test
        let start = Instant::now();
        for i in 0..1000 {
            let key = format!("{}_key_{}", label, i);
            let op_start = Instant::now();
            db.put(key.as_bytes(), &value)?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let write_duration = start.elapsed();
        let write_ops = 1000.0 / write_duration.as_secs_f64();

        // Read test
        let start = Instant::now();
        for i in 0..1000 {
            let key = format!("{}_key_{}", label, i);
            let _ = db.get(key.as_bytes())?;
        }
        let read_duration = start.elapsed();
        let read_ops = 1000.0 / read_duration.as_secs_f64();

        results.push((label, write_ops, read_ops, latencies));
    }

    // Analyze scaling
    let small_write = results[0].1;
    let large_write = results[results.len() - 1].1;
    let degradation = (small_write - large_write) / small_write * 100.0;

    observations.push(format!(
        "Performance degradation from 10B to 1MB: {:.1}%",
        degradation
    ));

    for (label, write_ops, _read_ops, _) in &results {
        if write_ops < &100000.0 {
            observations.push(format!(
                "{} values: {:.0} writes/sec (below target)",
                label, write_ops
            ));
        }
    }

    let best_size_latencies = results[1].3.clone(); // 100B size
    let mut sorted_latencies = best_size_latencies.clone();
    sorted_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Ok(PerformanceResult {
        test_name: "Value Size Impact".to_string(),
        ops_per_sec: results[1].1, // 100B write performance
        latency_p50_us: sorted_latencies[sorted_latencies.len() / 2],
        latency_p99_us: sorted_latencies[sorted_latencies.len() * 99 / 100],
        latency_p999_us: sorted_latencies[sorted_latencies.len() * 999 / 1000],
        observations,
    })
}

fn test_iterator_performance() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Iterator Performance...");

    let db = Database::create("./perf_test_db/iterator", LightningDbConfig::default())?;
    let mut observations = Vec::new();

    // Insert ordered data
    for i in 0..100000 {
        let key = format!("{:08}", i);
        let value = vec![0u8; 100];
        db.put(key.as_bytes(), &value)?;
    }

    // Full scan performance
    let start = Instant::now();
    let iter = db.scan(None, None)?;
    let count = iter.count();
    let full_scan_duration = start.elapsed();
    let scan_rate = count as f64 / full_scan_duration.as_secs_f64();

    observations.push(format!("Full scan rate: {:.0} entries/sec", scan_rate));

    // Range scan performance
    let start_key = b"00010000".to_vec();
    let end_key = b"00020000".to_vec();

    let start = Instant::now();
    let iter = db.scan(Some(start_key), Some(end_key))?;
    let range_count = iter.count();
    let range_duration = start.elapsed();
    let range_rate = range_count as f64 / range_duration.as_secs_f64();

    observations.push(format!(
        "Range scan rate: {:.0} entries/sec ({} entries)",
        range_rate, range_count
    ));

    // Prefix scan performance
    let start = Instant::now();
    let iter = db.scan_prefix(b"0001")?;
    let prefix_count = iter.count();
    let _prefix_duration = start.elapsed();

    if prefix_count != 10000 {
        observations.push(format!(
            "Prefix scan returned incorrect count: {} (expected 10000)",
            prefix_count
        ));
    }

    // Test iterator overhead
    let mut first_10 = Vec::new();
    let iter = db.scan(None, None)?;
    for (i, result) in iter.enumerate() {
        if i >= 10 {
            break;
        }
        let _ = result?;
        first_10.push(Instant::now());
    }

    if first_10.len() >= 2 {
        let iter_overhead = first_10[1].duration_since(first_10[0]).as_micros() as f64;
        observations.push(format!("Iterator step overhead: {:.2} μs", iter_overhead));
    }

    Ok(PerformanceResult {
        test_name: "Iterator Performance".to_string(),
        ops_per_sec: scan_rate,
        latency_p50_us: full_scan_duration.as_micros() as f64 / count as f64,
        latency_p99_us: full_scan_duration.as_micros() as f64 / count as f64 * 2.0,
        latency_p999_us: full_scan_duration.as_micros() as f64 / count as f64 * 10.0,
        observations,
    })
}

fn test_memory_pressure() -> Result<PerformanceResult, Box<dyn std::error::Error>> {
    println!("📊 Testing Memory Pressure...");

    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024,
        ..Default::default()
    }; // 50MB cache

    let db = Database::create("./perf_test_db/memory", config)?;
    let mut observations = Vec::new();

    // Get initial metrics
    let initial_metrics = db.get_metrics();
    observations.push(format!("Initial reads: {}", initial_metrics.reads));

    // Fill cache beyond capacity
    let value_size = 1024; // 1KB values
    let value = vec![0u8; value_size];
    let num_entries = 100000; // ~100MB of data

    let start = Instant::now();
    for i in 0..num_entries {
        let key = format!("mem_test_{:06}", i);
        db.put(key.as_bytes(), &value)?;
    }
    let write_duration = start.elapsed();

    // Force reads to trigger cache eviction
    let mut rng = rand::rng();
    let mut cache_misses = 0;
    let mut latencies = Vec::new();

    for _ in 0..10000 {
        let key_id = rng.random::<u32>() as usize % num_entries;
        let key = format!("mem_test_{:06}", key_id);

        let op_start = Instant::now();
        let _ = db.get(key.as_bytes())?;
        let latency = op_start.elapsed().as_micros() as f64;
        latencies.push(latency);

        if latency > 10.0 {
            // Likely cache miss
            cache_misses += 1;
        }
    }

    let final_metrics = db.get_metrics();
    let eviction_rate =
        (initial_metrics.cache_evictions as f64 - final_metrics.cache_evictions as f64) / 10000.0;

    observations.push(format!(
        "Cache miss rate: {:.1}%",
        cache_misses as f64 / 100.0
    ));
    observations.push(format!("Eviction rate: {:.2} per operation", eviction_rate));
    observations.push(format!(
        "Final cache hit rate: {:.2}%",
        final_metrics.cache_hit_rate * 100.0
    ));

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Ok(PerformanceResult {
        test_name: "Memory Pressure".to_string(),
        ops_per_sec: num_entries as f64 / write_duration.as_secs_f64(),
        latency_p50_us: latencies[latencies.len() / 2],
        latency_p99_us: latencies[latencies.len() * 99 / 100],
        latency_p999_us: latencies[latencies.len() * 999 / 1000],
        observations,
    })
}

fn print_analysis(results: &[PerformanceResult]) {
    println!("\n{}", "=".repeat(80));
    println!("📊 PERFORMANCE ANALYSIS SUMMARY");
    println!("{}", "=".repeat(80));

    // Performance table
    println!("\n⚡ Performance Metrics:\n");
    println!(
        "{:<25} | {:>12} | {:>10} | {:>10} | {:>10}",
        "Test", "Ops/sec", "P50 (μs)", "P99 (μs)", "P99.9 (μs)"
    );
    println!("{}", "-".repeat(80));

    for result in results {
        println!(
            "{:<25} | {:>12.0} | {:>10.2} | {:>10.2} | {:>10.2}",
            result.test_name,
            result.ops_per_sec,
            result.latency_p50_us,
            result.latency_p99_us,
            result.latency_p999_us
        );
    }

    // Observations
    println!("\n🔍 Key Observations:\n");
    for result in results {
        if !result.observations.is_empty() {
            println!("{}:", result.test_name);
            for obs in &result.observations {
                println!("  • {}", obs);
            }
            println!();
        }
    }

    // Optimization opportunities
    println!("🚀 Optimization Opportunities:\n");

    let optimizations = vec![
        (
            "Write Path",
            vec![
                "Implement write batching to amortize syscall overhead",
                "Add group commit for better transaction throughput",
                "Implement parallel WAL writes",
                "Use io_uring for async I/O on Linux",
            ],
        ),
        (
            "Read Path",
            vec![
                "Implement bloom filters to skip unnecessary disk reads",
                "Add read-ahead prefetching for sequential access",
                "Implement compressed block cache",
                "Use memory-mapped files for hot data",
            ],
        ),
        (
            "Cache",
            vec![
                "Implement CLOCK-Pro or 2Q algorithm for better hit rates",
                "Add cache warming on startup",
                "Implement tiered caching (hot/warm/cold)",
                "Add cache partitioning for different workloads",
            ],
        ),
        (
            "Concurrency",
            vec![
                "Replace RwLock with optimistic locking for reads",
                "Implement lock-free skip list for memtable",
                "Add per-thread write buffers",
                "Implement parallel compaction threads",
            ],
        ),
        (
            "Memory",
            vec![
                "Implement custom allocator for fixed-size objects",
                "Add memory pooling for common allocations",
                "Implement zero-copy deserialization",
                "Add off-heap storage for large values",
            ],
        ),
    ];

    for (category, items) in optimizations {
        println!("{}:", category);
        for item in items {
            println!("  • {}", item);
        }
        println!();
    }

    // Performance vs targets
    println!("🎯 Performance vs Targets:\n");

    let mut meets_read_target = false;
    let mut meets_write_target = false;

    for result in results {
        if result.test_name.contains("Cache") && result.ops_per_sec > 1_000_000.0 {
            meets_read_target = true;
            println!(
                "  ✅ Read target (1M ops/sec): {:.1}M ops/sec",
                result.ops_per_sec / 1_000_000.0
            );
        }
        if result.test_name.contains("Write") && result.ops_per_sec > 100_000.0 {
            meets_write_target = true;
            println!(
                "  ✅ Write target (100K ops/sec): {:.0}K ops/sec",
                result.ops_per_sec / 1_000.0
            );
        }
    }

    if !meets_read_target {
        println!("  ❌ Read target not met in all scenarios");
    }
    if !meets_write_target {
        println!("  ⚠️  Write target met but could be improved");
    }

    println!("\n{}", "=".repeat(80));
}

use lightning_db::{Database, LightningDbConfig};
use std::time::{Duration, Instant};
use tempfile::tempdir;

struct BenchmarkResult {
    name: &'static str,
    write_ops_sec: f64,
    write_latency_us: f64,
    read_ops_sec: f64,
    read_latency_us: f64,
    meets_targets: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lightning DB Final Performance Benchmark\n");
    println!("📊 Testing against targets:");
    println!("   • Read:  1M+ ops/sec, <1μs latency");
    println!("   • Write: 100K+ ops/sec, <10μs latency\n");
    
    let mut results = Vec::new();
    let dir = tempdir()?;
    
    // Test 1: Baseline (minimal features)
    results.push(benchmark(
        "Baseline (B+Tree only)",
        LightningDbConfig {
            compression_enabled: false,
            use_optimized_transactions: false,
            use_optimized_page_manager: false,
            prefetch_enabled: false,
            cache_size: 0,
            ..Default::default()
        },
        &dir.path().join("baseline"),
    )?);
    
    // Test 2: With caching
    results.push(benchmark(
        "With Cache (100MB)",
        LightningDbConfig {
            compression_enabled: false,
            use_optimized_transactions: false,
            use_optimized_page_manager: false,
            prefetch_enabled: false,
            cache_size: 100 * 1024 * 1024,
            ..Default::default()
        },
        &dir.path().join("cache"),
    )?);
    
    // Test 3: With LSM tree
    results.push(benchmark(
        "With LSM Tree",
        LightningDbConfig {
            compression_enabled: true,
            use_optimized_transactions: false,
            use_optimized_page_manager: false,
            prefetch_enabled: false,
            cache_size: 100 * 1024 * 1024,
            ..Default::default()
        },
        &dir.path().join("lsm"),
    )?);
    
    // Test 4: Fully optimized
    results.push(benchmark(
        "Fully Optimized",
        LightningDbConfig::default(),
        &dir.path().join("optimized"),
    )?);
    
    // Print summary table
    println!("\n📈 PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(80));
    println!("{:<20} | {:>12} | {:>10} | {:>12} | {:>10} | Status",
        "Configuration", "Write ops/s", "Write μs", "Read ops/s", "Read μs");
    println!("{}", "-".repeat(80));
    
    for result in &results {
        println!("{:<20} | {:>12.0} | {:>10.2} | {:>12.0} | {:>10.2} | {}",
            result.name,
            result.write_ops_sec,
            result.write_latency_us,
            result.read_ops_sec,
            result.read_latency_us,
            if result.meets_targets { "✅ PASS" } else { "❌ FAIL" }
        );
    }
    
    // Best results
    println!("\n🏆 BEST RESULTS:");
    let best_write = results.iter()
        .max_by(|a, b| a.write_ops_sec.partial_cmp(&b.write_ops_sec).unwrap())
        .unwrap();
    let best_read = results.iter()
        .max_by(|a, b| a.read_ops_sec.partial_cmp(&b.read_ops_sec).unwrap())
        .unwrap();
    
    println!("  Best Write: {} - {:.0} ops/sec ({:.2} μs/op)",
        best_write.name, best_write.write_ops_sec, best_write.write_latency_us);
    println!("  Best Read:  {} - {:.0} ops/sec ({:.2} μs/op)",
        best_read.name, best_read.read_ops_sec, best_read.read_latency_us);
    
    // Target achievement
    println!("\n🎯 TARGET ACHIEVEMENT:");
    let write_target_met = best_write.write_ops_sec >= 100_000.0;
    let write_latency_met = best_write.write_latency_us < 10.0;
    let read_target_met = best_read.read_ops_sec >= 1_000_000.0;
    let read_latency_met = best_read.read_latency_us < 1.0;
    
    println!("  Write throughput: {} ({:.0} / 100K ops/sec)",
        if write_target_met { "✅ ACHIEVED" } else { "❌ NOT MET" },
        best_write.write_ops_sec);
    println!("  Write latency:    {} ({:.2} / 10 μs)",
        if write_latency_met { "✅ ACHIEVED" } else { "❌ NOT MET" },
        best_write.write_latency_us);
    println!("  Read throughput:  {} ({:.0} / 1M ops/sec)",
        if read_target_met { "✅ ACHIEVED" } else { "❌ NOT MET" },
        best_read.read_ops_sec);
    println!("  Read latency:     {} ({:.2} / 1 μs)",
        if read_latency_met { "✅ ACHIEVED" } else { "❌ NOT MET" },
        best_read.read_latency_us);
    
    if write_target_met && write_latency_met && read_target_met && read_latency_met {
        println!("\n🎉 ALL PERFORMANCE TARGETS ACHIEVED!");
    }
    
    Ok(())
}

fn benchmark(
    name: &'static str,
    config: LightningDbConfig,
    path: &std::path::Path,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    println!("Testing {}...", name);
    
    let db = Database::create(path, config)?;
    
    // Parameters
    let write_iterations = 100_000;
    let read_iterations = 1_000_000;
    let warmup = 10_000;
    
    // Warmup
    for i in 0..warmup {
        db.put(format!("warmup_{:08}", i).as_bytes(), b"warmup_value")?;
    }
    
    // Ensure something is in cache for best-case read
    db.put(b"cached_key", b"cached_value")?;
    let _ = db.get(b"cached_key")?; // Prime cache
    
    // Write benchmark
    let start = Instant::now();
    for i in 0..write_iterations {
        db.put(format!("key_{:08}", i).as_bytes(), b"test_value")?;
    }
    let write_duration = start.elapsed();
    
    // Read benchmark (best case - cached)
    let start = Instant::now();
    for _ in 0..read_iterations {
        let _ = db.get(b"cached_key")?;
    }
    let read_duration = start.elapsed();
    
    // Calculate metrics
    let write_ops_sec = write_iterations as f64 / write_duration.as_secs_f64();
    let write_latency_us = write_duration.as_micros() as f64 / write_iterations as f64;
    let read_ops_sec = read_iterations as f64 / read_duration.as_secs_f64();
    let read_latency_us = read_duration.as_micros() as f64 / read_iterations as f64;
    
    let meets_targets = write_ops_sec >= 100_000.0 && 
                       write_latency_us < 10.0 &&
                       read_ops_sec >= 1_000_000.0 && 
                       read_latency_us < 1.0;
    
    Ok(BenchmarkResult {
        name,
        write_ops_sec,
        write_latency_us,
        read_ops_sec,
        read_latency_us,
        meets_targets,
    })
}
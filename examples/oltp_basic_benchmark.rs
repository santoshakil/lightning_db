use lightning_db::{Database, LightningDbConfig};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{SeedableRng, RngCore};

struct BenchmarkMetrics {
    operation: String,
    ops_per_sec: f64,
    p50_latency_us: u64,
    p95_latency_us: u64,
    p99_latency_us: u64,
    total_ops: u64,
    duration_ms: u64,
}

fn benchmark_inserts(db: &Database, num_ops: u64) -> BenchmarkMetrics {
    println!("Benchmarking single inserts ({} ops)...", num_ops);
    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(42);
    
    for i in 0..num_ops {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:016}", rng.next_u64());
        
        let op_start = Instant::now();
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        latencies.push(op_start.elapsed().as_micros() as u64);
    }
    
    let duration = start.elapsed();
    calculate_metrics("single_insert", latencies, num_ops, duration)
}

fn benchmark_batch_ops(db: &Database, num_batches: u64, batch_size: u64) -> BenchmarkMetrics {
    println!("Benchmarking batch operations ({} batches x {} ops)...", num_batches, batch_size);
    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(43);
    
    for batch in 0..num_batches {
        let op_start = Instant::now();
        
        for i in 0..batch_size {
            let key = format!("batch_{:06}_{:04}", batch, i);
            let value = format!("value_{:016}", rng.next_u64());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        latencies.push(op_start.elapsed().as_micros() as u64);
    }
    
    let duration = start.elapsed();
    calculate_metrics("batch_ops", latencies, num_batches * batch_size, duration)
}

fn benchmark_reads(db: &Database, num_ops: u64) -> BenchmarkMetrics {
    println!("Benchmarking point reads ({} ops)...", num_ops);
    
    // First insert data
    for i in 0..num_ops {
        let key = format!("read_key_{:08}", i);
        let value = format!("read_value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Now benchmark reads
    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(44);
    
    for _ in 0..num_ops {
        let key_idx = (rng.next_u64() % num_ops as u64) as u64;
        let key = format!("read_key_{:08}", key_idx);
        
        let op_start = Instant::now();
        let _ = db.get(key.as_bytes()).unwrap();
        latencies.push(op_start.elapsed().as_micros() as u64);
    }
    
    let duration = start.elapsed();
    calculate_metrics("point_read", latencies, num_ops, duration)
}

fn benchmark_updates(db: &Database, num_ops: u64) -> BenchmarkMetrics {
    println!("Benchmarking updates ({} ops)...", num_ops);
    
    // Insert initial data
    for i in 0..num_ops {
        let key = format!("update_key_{:08}", i);
        let value = format!("initial_value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Benchmark updates
    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(46);
    
    for _ in 0..num_ops {
        let key_idx = (rng.next_u64() % num_ops as u64) as u64;
        let key = format!("update_key_{:08}", key_idx);
        let value = format!("updated_value_{:016}", rng.next_u64());
        
        let op_start = Instant::now();
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        latencies.push(op_start.elapsed().as_micros() as u64);
    }
    
    let duration = start.elapsed();
    calculate_metrics("update", latencies, num_ops, duration)
}

fn benchmark_mixed(db: &Database, num_ops: u64) -> BenchmarkMetrics {
    println!("Benchmarking mixed workload ({} ops)...", num_ops);
    
    // Pre-populate some data
    for i in 0..1000 {
        let key = format!("mixed_key_{:08}", i);
        let value = format!("mixed_value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let mut latencies = Vec::new();
    let start = Instant::now();
    let mut rng = ChaCha8Rng::seed_from_u64(47);
    
    for i in 0..num_ops {
        let op_type = (rng.next_u64() % 100) as u64;
        let op_start = Instant::now();
        
        if op_type < 30 {
            // 30% inserts
            let key = format!("mixed_new_{:08}", i);
            let value = format!("value_{:016}", rng.next_u64());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else if op_type < 80 {
            // 50% reads
            let key_idx = (rng.next_u64() % 1000) as u64;
            let key = format!("mixed_key_{:08}", key_idx);
            let _ = db.get(key.as_bytes()).unwrap();
        } else {
            // 20% updates
            let key_idx = (rng.next_u64() % 1000) as u64;
            let key = format!("mixed_key_{:08}", key_idx);
            let value = format!("updated_{:016}", rng.next_u64());
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        latencies.push(op_start.elapsed().as_micros() as u64);
    }
    
    let duration = start.elapsed();
    calculate_metrics("mixed_workload", latencies, num_ops, duration)
}

fn calculate_metrics(operation: &str, mut latencies: Vec<u64>, total_ops: u64, duration: Duration) -> BenchmarkMetrics {
    latencies.sort_unstable();
    
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[latencies.len() * 95 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("  {} results:", operation);
    println!("    Throughput: {:.0} ops/sec", ops_per_sec);
    println!("    Latency p50: {} μs", p50);
    println!("    Latency p95: {} μs", p95);
    println!("    Latency p99: {} μs", p99);
    println!();
    
    BenchmarkMetrics {
        operation: operation.to_string(),
        ops_per_sec,
        p50_latency_us: p50,
        p95_latency_us: p95,
        p99_latency_us: p99,
        total_ops,
        duration_ms: duration.as_millis() as u64,
    }
}

fn generate_report(metrics: &[BenchmarkMetrics]) {
    println!("\n{}", "=".repeat(60));
    println!("OLTP PERFORMANCE BENCHMARK REPORT");
    println!("{}", "=".repeat(60));
    println!();
    
    println!("| Operation       | Throughput (ops/s) | p50 (μs) | p95 (μs) | p99 (μs) |");
    println!("|-----------------|-------------------|----------|----------|----------|");
    
    for metric in metrics {
        println!("| {:15} | {:17.0} | {:8} | {:8} | {:8} |",
            metric.operation,
            metric.ops_per_sec,
            metric.p50_latency_us,
            metric.p95_latency_us,
            metric.p99_latency_us
        );
    }
    
    println!();
    println!("Summary:");
    println!("--------");
    
    let avg_throughput: f64 = metrics.iter()
        .map(|m| m.ops_per_sec)
        .sum::<f64>() / metrics.len() as f64;
    
    let avg_p50: u64 = metrics.iter()
        .map(|m| m.p50_latency_us)
        .sum::<u64>() / metrics.len() as u64;
    
    let avg_p99: u64 = metrics.iter()
        .map(|m| m.p99_latency_us)
        .sum::<u64>() / metrics.len() as u64;
    
    println!("Average Throughput: {:.0} ops/sec", avg_throughput);
    println!("Average p50 Latency: {} μs", avg_p50);
    println!("Average p99 Latency: {} μs", avg_p99);
    
    // Check against SLOs
    println!();
    println!("SLO Compliance:");
    println!("--------------");
    
    let p50_target = 1000; // 1ms in microseconds
    let p99_target = 50000; // 50ms
    let tps_target = 100000.0;
    
    let p50_pass = avg_p50 < p50_target;
    let p99_pass = avg_p99 < p99_target;
    let tps_pass = avg_throughput > tps_target;
    
    println!("p50 < 1ms: {} ({} μs)", 
        if p50_pass { "✅ PASS" } else { "❌ FAIL" }, 
        avg_p50
    );
    println!("p99 < 50ms: {} ({} μs)", 
        if p99_pass { "✅ PASS" } else { "❌ FAIL" },
        avg_p99
    );
    println!("TPS > 100K: {} ({:.0} ops/s)",
        if tps_pass { "✅ PASS" } else { "❌ FAIL" },
        avg_throughput
    );
    
    let all_pass = p50_pass && p99_pass && tps_pass;
    println!();
    println!("Overall: {}", if all_pass { "✅ ALL SLOs MET" } else { "⚠️ SLOs NOT MET" });
}

fn main() {
    println!("Lightning DB OLTP Performance Benchmark");
    println!("========================================");
    println!();
    
    // Create temporary database with tuned config for realistic OLTP
    let mut cfg = LightningDbConfig::default();
    // Reduce overhead to increase throughput in benchmark context
    cfg.compression_enabled = true;            // keep LSM enabled for write optimization
    cfg.cache_size = 0;                        // favor write throughput in this mixed bench
    cfg.write_batch_size = 2000;              // larger batching for write coalescing
    cfg.enable_statistics = false;            // disable background stats collection in benches

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tmp = std::env::temp_dir().join(format!("lightning_db_bench_{}", nanos));
    let db = Database::create(tmp, cfg).expect("Failed to create database");
    
    // Warm up
    println!("Warming up...");
    for i in 0..1000 {
        let key = format!("warmup_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    println!();
    
    // Run benchmarks
    let mut metrics = Vec::new();
    
    metrics.push(benchmark_inserts(&db, 10000));
    metrics.push(benchmark_batch_ops(&db, 100, 100));
    metrics.push(benchmark_reads(&db, 10000));
    metrics.push(benchmark_updates(&db, 5000));
    metrics.push(benchmark_mixed(&db, 10000));
    
    // Get database stats
    let stats = db.stats();
    println!("Database Statistics:");
    println!("-------------------");
    println!("Page count: {}", stats.page_count);
    println!("Free pages: {}", stats.free_page_count);
    println!("Tree height: {}", stats.tree_height);
    println!("Cache hit rate: {:.2}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    println!("Memory usage: {} MB", stats.memory_usage_bytes / (1024 * 1024));
    println!();
    
    // Generate comprehensive report
    generate_report(&metrics);
}

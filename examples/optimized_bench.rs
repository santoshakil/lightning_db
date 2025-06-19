use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use std::hint::black_box;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Optimized Performance Test\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("optimized.db");
    
    // Configuration optimized for performance
    let config = LightningDbConfig {
        compression_enabled: true,
        use_optimized_transactions: true,
        use_optimized_page_manager: true,
        prefetch_enabled: true,
        cache_size: 256 * 1024 * 1024, // 256MB cache
        ..Default::default()
    };
    
    let db = Database::create(&db_path, config)?;
    
    // Test data
    let small_key = b"key123";
    let medium_key = b"this_is_a_medium_sized_key_32b";
    let small_value = b"val";
    let medium_value = b"this is a medium sized value with some data";
    
    let iterations = 1_000_000;
    let warmup = 10_000;
    
    println!("Warming up cache...");
    for i in 0..warmup {
        let key = format!("warmup_{:06}", i);
        db.put(key.as_bytes(), medium_value)?;
    }
    
    // Pre-populate for read tests
    for i in 0..100_000 {
        let key = format!("test_{:06}", i);
        db.put(key.as_bytes(), medium_value)?;
    }
    
    println!("Cache warmed up. Starting benchmarks...\n");
    
    // Benchmark 1: Small key writes
    println!("1. Small key writes ({}B key, {}B value):", small_key.len(), small_value.len());
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("sk_{:08}", i);
        black_box(db.put(key.as_bytes(), small_value)?);
    }
    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   {:>10.0} ops/sec ({:.3} μs/op)", ops_per_sec, us_per_op);
    if us_per_op < 10.0 { println!("   ✅ Meets write latency target!"); }
    
    // Benchmark 2: Small key reads (hot cache)
    println!("\n2. Small key reads (hot cache):");
    db.put(small_key, small_value)?; // Ensure it's in cache
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(db.get(small_key)?);
    }
    let duration = start.elapsed();
    let ops_per_sec = iterations as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / iterations as f64;
    println!("   {:>10.0} ops/sec ({:.3} μs/op)", ops_per_sec, us_per_op);
    if us_per_op < 1.0 { println!("   ✅ Meets read latency target!"); }
    
    // Benchmark 3: Sequential reads (cache friendly)
    println!("\n3. Sequential reads (cache friendly):");
    let read_iterations = 100_000;
    let start = Instant::now();
    for i in 0..read_iterations {
        let key = format!("test_{:06}", i);
        black_box(db.get(key.as_bytes())?);
    }
    let duration = start.elapsed();
    let ops_per_sec = read_iterations as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / read_iterations as f64;
    println!("   {:>10.0} ops/sec ({:.3} μs/op)", ops_per_sec, us_per_op);
    
    // Benchmark 4: Batch operations
    println!("\n4. Batch operations (1000 ops/batch):");
    let batch_size = 1000;
    let num_batches = 1000;
    let mut batch = Vec::with_capacity(batch_size);
    
    for i in 0..batch_size {
        batch.push((format!("batch_{:06}", i).into_bytes(), medium_value.to_vec()));
    }
    
    let start = Instant::now();
    for _ in 0..num_batches {
        db.put_batch(&batch)?;
    }
    let duration = start.elapsed();
    let total_ops = (num_batches * batch_size) as f64;
    let ops_per_sec = total_ops / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / total_ops;
    println!("   {:>10.0} ops/sec ({:.3} μs/op)", ops_per_sec, us_per_op);
    if ops_per_sec > 1_000_000.0 { println!("   ✅ Exceeds 1M ops/sec!"); }
    
    // Benchmark 5: Mixed workload (80/20 read/write)
    println!("\n5. Mixed workload (80% read, 20% write):");
    let mixed_iterations = 500_000;
    let mut rng = rand::rng();
    use rand::Rng;
    
    let start = Instant::now();
    for i in 0..mixed_iterations {
        if rng.random_range(0..100) < 80 {
            let key = format!("test_{:06}", i % 100_000);
            black_box(db.get(key.as_bytes())?);
        } else {
            let key = format!("mixed_{:08}", i);
            black_box(db.put(key.as_bytes(), small_value)?);
        }
    }
    let duration = start.elapsed();
    let ops_per_sec = mixed_iterations as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / mixed_iterations as f64;
    println!("   {:>10.0} ops/sec ({:.3} μs/op)", ops_per_sec, us_per_op);
    
    // Summary
    println!("\n📊 PERFORMANCE SUMMARY");
    println!("{}", "=".repeat(50));
    
    let stats = db.stats();
    println!("\nDatabase Statistics:");
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  Active transactions: {}", stats.active_transactions);
    
    if let Some(cache_stats) = db.cache_stats() {
        println!("\nCache Statistics:");
        println!("  {}", cache_stats);
    }
    
    if let Some(lsm_stats) = db.lsm_stats() {
        println!("\nLSM Tree Statistics:");
        println!("  Memtable entries: {}", lsm_stats.memtable_size);
        println!("  Cache hit rate: {:.2}%", lsm_stats.cache_hit_rate * 100.0);
    }
    
    Ok(())
}
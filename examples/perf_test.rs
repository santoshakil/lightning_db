use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Performance Test\n");
    
    let dir = tempdir()?;
    
    // Test different configurations
    test_config("Basic B+Tree", LightningDbConfig {
        compression_enabled: false,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        prefetch_enabled: false,
        cache_size: 0,
        ..Default::default()
    }, &dir.path().join("basic"))?;
    
    test_config("With Cache", LightningDbConfig {
        compression_enabled: false,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        prefetch_enabled: false,
        cache_size: 100 * 1024 * 1024, // 100MB
        ..Default::default()
    }, &dir.path().join("cache"))?;
    
    test_config("With LSM", LightningDbConfig {
        compression_enabled: true,
        use_optimized_transactions: false,
        use_optimized_page_manager: false,
        prefetch_enabled: false,
        cache_size: 100 * 1024 * 1024,
        ..Default::default()
    }, &dir.path().join("lsm"))?;
    
    test_config("Fully Optimized", LightningDbConfig {
        compression_enabled: true,
        use_optimized_transactions: true,
        use_optimized_page_manager: true,
        prefetch_enabled: true,
        cache_size: 100 * 1024 * 1024,
        ..Default::default()
    }, &dir.path().join("optimized"))?;
    
    println!("\n🎯 Performance Targets:");
    println!("  • Read:  <1μs latency, 1M+ ops/sec");
    println!("  • Write: <10μs latency, 100K+ ops/sec");
    
    Ok(())
}

fn test_config(name: &str, config: LightningDbConfig, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== {} ===", name);
    
    let db = Database::create(path, config)?;
    
    // Test parameters
    let write_count = 100_000;
    let read_count = 1_000_000;
    let key_size = 16;
    let value_size = 100;
    
    // Prepare test data
    let test_key = b"test_key_123456";
    let test_value = vec![b'x'; value_size];
    
    // Warmup
    for i in 0..1000 {
        let key = format!("{:0width$}", i, width = key_size);
        db.put(key.as_bytes(), &test_value)?;
    }
    
    // Write benchmark
    let start = Instant::now();
    for i in 0..write_count {
        let key = format!("w{:0width$}", i, width = key_size - 1);
        db.put(key.as_bytes(), &test_value)?;
    }
    let write_duration = start.elapsed();
    let write_ops_per_sec = write_count as f64 / write_duration.as_secs_f64();
    let write_us_per_op = write_duration.as_micros() as f64 / write_count as f64;
    
    // Read benchmark (cache miss)
    let start = Instant::now();
    for i in 0..read_count / 10 {
        let key = format!("w{:0width$}", i % write_count, width = key_size - 1);
        let _ = db.get(key.as_bytes())?;
    }
    let read_miss_duration = start.elapsed();
    let read_miss_ops_per_sec = (read_count / 10) as f64 / read_miss_duration.as_secs_f64();
    let read_miss_us_per_op = read_miss_duration.as_micros() as f64 / (read_count / 10) as f64;
    
    // Read benchmark (cache hit)
    db.put(test_key, &test_value)?; // Ensure in cache
    let start = Instant::now();
    for _ in 0..read_count {
        let _ = db.get(test_key)?;
    }
    let read_hit_duration = start.elapsed();
    let read_hit_ops_per_sec = read_count as f64 / read_hit_duration.as_secs_f64();
    let read_hit_us_per_op = read_hit_duration.as_micros() as f64 / read_count as f64;
    
    // Results
    println!("  Write: {:>8.0} ops/sec ({:>6.2} μs/op) {}", 
        write_ops_per_sec, write_us_per_op,
        if write_us_per_op < 10.0 { "✅" } else { "❌" });
    println!("  Read (miss): {:>8.0} ops/sec ({:>6.2} μs/op)",
        read_miss_ops_per_sec, read_miss_us_per_op);
    println!("  Read (hit):  {:>8.0} ops/sec ({:>6.2} μs/op) {}",
        read_hit_ops_per_sec, read_hit_us_per_op,
        if read_hit_us_per_op < 1.0 { "✅" } else { "❌" });
    
    Ok(())
}
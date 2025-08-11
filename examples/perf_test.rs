use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create config with optimizations enabled
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB cache
        use_improved_wal: true,
        use_optimized_transactions: true,
        ..Default::default()
    };
    
    let db = Database::create("/tmp/perf_test", config)?;
    
    // Benchmark writes
    let start = Instant::now();
    let n = 10_000; // Reduced for faster testing
    for i in 0..n {
        let key = format!("key_{:09}", i);
        let value = format!("value_{:09}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    let write_time = start.elapsed();
    let write_ops_per_sec = n as f64 / write_time.as_secs_f64();
    
    // Benchmark reads
    let start = Instant::now();
    for i in 0..n {
        let key = format!("key_{:09}", i);
        db.get(key.as_bytes())?;
    }
    let read_time = start.elapsed();
    let read_ops_per_sec = n as f64 / read_time.as_secs_f64();
    
    // Benchmark range scan
    let start = Instant::now();
    let mut count = 0;
    let iter = db.scan(Some(b"key_".to_vec()), Some(b"key_~".to_vec()))?;
    for _ in iter.take(1000) {
        count += 1;
    }
    let scan_time = start.elapsed();
    
    println!("Lightning DB Performance Test Results");
    println!("=====================================");
    println!("Write: {:.0} ops/sec ({:.2} μs/op)", write_ops_per_sec, write_time.as_micros() as f64 / n as f64);
    println!("Read:  {:.0} ops/sec ({:.2} μs/op)", read_ops_per_sec, read_time.as_micros() as f64 / n as f64);
    println!("Range Scan: {} items in {:.2}ms", count, scan_time.as_secs_f64() * 1000.0);
    println!("\nTotal entries: {}", n);
    
    // Cleanup
    std::fs::remove_dir_all("/tmp/perf_test").ok();
    
    Ok(())
}
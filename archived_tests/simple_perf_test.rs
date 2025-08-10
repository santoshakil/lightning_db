use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Simple Performance Test");
    println!("=====================================");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;
    
    const NUM_OPS: usize = 10000;
    
    // Write test
    println!("\nTesting {} write operations...", NUM_OPS);
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    let write_duration = start.elapsed();
    let write_ops_per_sec = NUM_OPS as f64 / write_duration.as_secs_f64();
    println!("Write Performance: {:.0} ops/sec ({:.2} μs/op)", 
             write_ops_per_sec, 
             write_duration.as_micros() as f64 / NUM_OPS as f64);
    
    // Read test
    println!("\nTesting {} read operations...", NUM_OPS);
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:06}", i);
        let _value = db.get(key.as_bytes())?;
    }
    let read_duration = start.elapsed();
    let read_ops_per_sec = NUM_OPS as f64 / read_duration.as_secs_f64();
    println!("Read Performance: {:.0} ops/sec ({:.2} μs/op)", 
             read_ops_per_sec,
             read_duration.as_micros() as f64 / NUM_OPS as f64);
    
    // Summary
    println!("\n=== Performance Summary ===");
    println!("Writes: {:.0} ops/sec", write_ops_per_sec);
    println!("Reads:  {:.0} ops/sec", read_ops_per_sec);
    
    // Check against targets (from CLAUDE.md)
    // Target: 1M writes/sec, 10M reads/sec
    let write_target = 1_000_000.0;
    let read_target = 10_000_000.0;
    
    println!("\nPerformance vs Targets:");
    println!("Write: {:.1}% of target", (write_ops_per_sec / write_target) * 100.0);
    println!("Read:  {:.1}% of target", (read_ops_per_sec / read_target) * 100.0);
    
    Ok(())
}
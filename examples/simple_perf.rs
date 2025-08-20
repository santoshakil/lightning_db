use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Simple Performance Test");
    println!("====================================\n");
    
    // Use a temporary directory
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();
    
    // Create config with basic settings
    let config = LightningDbConfig {
        cache_size: 32 * 1024 * 1024, // 32MB cache
        ..Default::default()
    };
    
    println!("Creating database at: {:?}", db_path);
    let db = Database::create(db_path, config)?;
    
    // Small test size to avoid hangs
    let n = 100;
    
    // Benchmark writes
    println!("\nBenchmarking {} writes...", n);
    let start = Instant::now();
    for i in 0..n {
        let key = format!("k{:04}", i);
        let value = format!("v{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    let write_time = start.elapsed();
    let write_ops_per_sec = n as f64 / write_time.as_secs_f64();
    
    // Benchmark reads
    println!("Benchmarking {} reads...", n);
    let start = Instant::now();
    for i in 0..n {
        let key = format!("k{:04}", i);
        let _value = db.get(key.as_bytes())?;
    }
    let read_time = start.elapsed();
    let read_ops_per_sec = n as f64 / read_time.as_secs_f64();
    
    // Print results
    println!("\nResults:");
    println!("--------");
    println!("Write: {:.0} ops/sec ({:.2} μs/op)", 
             write_ops_per_sec, 
             write_time.as_micros() as f64 / n as f64);
    println!("Read:  {:.0} ops/sec ({:.2} μs/op)", 
             read_ops_per_sec, 
             read_time.as_micros() as f64 / n as f64);
    
    // Explicitly drop the database
    println!("\nShutting down database...");
    drop(db);
    
    // Give background tasks a moment to finish
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    println!("Test completed successfully!");
    
    // Temp directory will be cleaned up automatically
    
    Ok(())
}
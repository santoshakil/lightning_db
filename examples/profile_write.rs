use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Profiling write performance...\n");
    
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();
    
    // Use default config  
    let config = LightningDbConfig::default();
    println!("Config: cache_size={}, compression={}, prefetch={}, wal=improved/async", 
        config.cache_size, config.compression_enabled, config.prefetch_enabled);
    
    let db = Database::create(db_path, config)?;
    
    // Profile different stages
    let count = 100;
    let mut times = Vec::new();
    
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = vec![b'x'; 100]; // 100 byte value
        
        let start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        let elapsed = start.elapsed();
        
        times.push(elapsed.as_micros());
        
        if i < 10 || i % 10 == 0 {
            println!("Write {}: {} μs", i, elapsed.as_micros());
        }
    }
    
    // Calculate statistics
    times.sort();
    let min = times[0];
    let max = times[times.len() - 1];
    let median = times[times.len() / 2];
    let avg = times.iter().sum::<u128>() / times.len() as u128;
    
    println!("\nStatistics for {} writes:", count);
    println!("  Min:    {} μs", min);
    println!("  Median: {} μs", median);
    println!("  Avg:    {} μs", avg);
    println!("  Max:    {} μs", max);
    println!("  Ops/sec: {:.0}", 1_000_000.0 / avg as f64);
    
    Ok(())
}
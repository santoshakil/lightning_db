use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing simple writes...");
    
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();
    
    // Create minimal config
    let mut config = LightningDbConfig::default();
    config.prefetch_enabled = false;
    config.use_optimized_transactions = false;
    config.use_improved_wal = false;
    config.cache_size = 0;
    config.compression_enabled = false;
    config.use_optimized_page_manager = false;
    config.mmap_config = None;
    
    println!("Creating database...");
    let db = Database::create(db_path, config)?;
    
    println!("Writing 100 entries...");
    let start = Instant::now();
    
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        
        if i % 10 == 0 {
            println!("  Writing key {}", i);
        }
        
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let elapsed = start.elapsed();
    println!("Wrote 100 entries in {:?}", elapsed);
    println!("{:.0} ops/sec", 100.0 / elapsed.as_secs_f64());
    
    // Verify data
    println!("\nVerifying data...");
    for i in 0..10 {
        let key = format!("key_{:04}", i);
        let val = db.get(key.as_bytes())?;
        if let Some(v) = val {
            println!("  {} = {}", key, String::from_utf8_lossy(&v));
        }
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn test_config(name: &str, config: LightningDbConfig, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();
    
    let db = Database::create(db_path, config)?;
    
    let start = Instant::now();
    
    for i in 0..count {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    
    println!("{:<30} {:.0} ops/sec", name, ops_per_sec);
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing feature performance impact...\n");
    
    let count = 1000;
    
    // Baseline - minimal config
    let mut config = LightningDbConfig::default();
    config.prefetch_enabled = false;
    config.use_optimized_transactions = false;
    config.use_improved_wal = false;
    config.cache_size = 0;
    config.compression_enabled = false;
    config.use_optimized_page_manager = false;
    config.mmap_config = None;
    test_config("Minimal", config.clone(), count)?;
    
    // Test each feature
    let mut config = config.clone();
    config.use_improved_wal = true;
    test_config("+ Improved WAL", config.clone(), count)?;
    
    let mut config = config.clone();
    config.use_optimized_transactions = true;
    test_config("+ Optimized Transactions", config.clone(), count)?;
    
    let mut config = config.clone();
    config.cache_size = 10 * 1024 * 1024;
    test_config("+ Cache", config.clone(), count)?;
    
    let mut config = config.clone();
    config.compression_enabled = true;
    test_config("+ Compression", config.clone(), count)?;
    
    let mut config = config.clone();
    config.prefetch_enabled = true;
    test_config("+ Prefetch", config.clone(), count)?;
    
    Ok(())
}
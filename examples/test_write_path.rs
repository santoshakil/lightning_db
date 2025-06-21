use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn test_write_config(name: &str, config: LightningDbConfig, count: usize) -> Result<(), Box<dyn std::error::Error>> {
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
    
    println!("{:<40} {:.0} ops/sec", name, ops_per_sec);
    
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing write path performance...\n");
    
    let count = 1000;
    
    // Baseline - absolute minimal config
    let mut config = LightningDbConfig::default();
    config.prefetch_enabled = false;
    config.use_optimized_transactions = false;
    config.use_improved_wal = false;
    config.cache_size = 0;
    config.compression_enabled = false;
    config.use_optimized_page_manager = false;
    config.mmap_config = None;
    test_write_config("Minimal (no WAL, no cache)", config.clone(), count)?;
    
    // Test WAL impact
    config.use_improved_wal = true;
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;
    test_write_config("+ Async WAL", config.clone(), count)?;
    
    config.wal_sync_mode = lightning_db::WalSyncMode::Sync;
    test_write_config("+ Sync WAL", config.clone(), count)?;
    
    // Test cache impact
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;
    config.cache_size = 10 * 1024 * 1024;
    test_write_config("+ Cache", config.clone(), count)?;
    
    // Test compression impact
    config.compression_enabled = true;
    test_write_config("+ Compression", config.clone(), count)?;
    
    // Test with everything but optimized components
    config.prefetch_enabled = true;
    test_write_config("+ Prefetch", config.clone(), count)?;
    
    // Default config
    let default_config = LightningDbConfig::default();
    test_write_config("Default config", default_config, count)?;
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Testing database creation hang...");
    
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();
    
    // Create minimal config
    let mut config = LightningDbConfig::default();
    config.prefetch_enabled = false; // Disable prefetch
    config.use_optimized_transactions = false; // Disable optimized tx
    config.use_improved_wal = false; // Use simple WAL
    config.cache_size = 0; // Disable cache
    config.compression_enabled = false; // Disable compression
    config.use_optimized_page_manager = false; // Use standard page manager
    config.mmap_config = None; // No mmap
    
    println!("Creating database with minimal config...");
    let start = std::time::Instant::now();
    
    let db = Database::create(db_path, config)?;
    
    println!("Database created in {:?}", start.elapsed());
    
    // Simple test
    db.put(b"key", b"value")?;
    let val = db.get(b"key")?;
    println!("Got value: {:?}", val);
    
    Ok(())
}
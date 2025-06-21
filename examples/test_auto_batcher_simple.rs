use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing AutoBatcher creation...");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    // Use minimal config to isolate AutoBatcher issue
    let mut config = LightningDbConfig::default();
    config.cache_size = 0;
    config.compression_enabled = false;
    config.prefetch_enabled = false;
    config.use_optimized_transactions = false;
    config.use_improved_wal = false;
    config.use_optimized_page_manager = false;
    config.mmap_config = None;
    
    println!("Creating database...");
    let db = Arc::new(Database::create(&db_path, config)?);
    println!("Database created!");
    
    println!("Creating AutoBatcher...");
    let batcher = Database::create_auto_batcher(db.clone());
    println!("AutoBatcher created!");
    
    println!("Shutting down AutoBatcher...");
    batcher.shutdown();
    println!("AutoBatcher shutdown complete!");
    
    println!("All tests passed!");
    Ok(())
}
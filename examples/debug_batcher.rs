use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing AutoBatcher data persistence...\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;
    
    let db = Arc::new(Database::create(&db_path, config)?);
    let batcher = Database::create_auto_batcher(db.clone());
    
    // Write some data
    println!("Writing 10 keys...");
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        batcher.put(key.into_bytes(), value.into_bytes())?;
        println!("  Put key{}", i);
    }
    
    println!("\nFlushing batcher...");
    batcher.flush()?;
    
    // Wait a bit for any async operations
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    // Try to read the data
    println!("\nReading back data:");
    for i in 0..10 {
        let key = format!("key{}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                println!("  ✅ key{} = {}", i, String::from_utf8_lossy(&value));
            }
            None => {
                println!("  ❌ key{} not found!", i);
            }
        }
    }
    
    // Check batcher stats
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!("\nBatcher stats:");
    println!("  Writes submitted: {}", submitted);
    println!("  Writes completed: {}", completed);
    println!("  Batches flushed: {}", batches);
    println!("  Write errors: {}", errors);
    
    Ok(())
}
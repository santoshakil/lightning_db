use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    let db = Arc::new(Database::create(&db_path, config)?);
    
    // Create auto batcher
    let batcher = Database::create_auto_batcher(db.clone());
    
    // Write a single key
    println!("Writing key_0...");
    batcher.put(b"key_0".to_vec(), b"value".to_vec())?;
    
    // Wait for completion
    println!("Waiting for completion...");
    batcher.wait_for_completion()?;
    
    // Flush explicitly
    println!("Flushing...");
    batcher.flush()?;
    
    // Shutdown batcher
    println!("Shutting down batcher...");
    batcher.shutdown();
    
    // Try to read directly from db
    println!("Reading from db...");
    match db.get(b"key_0")? {
        Some(val) => println!("Found value: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Key not found!"),
    }
    
    // Sync the database
    println!("Syncing database...");
    db.sync()?;
    
    // Try again
    println!("Reading after sync...");
    match db.get(b"key_0")? {
        Some(val) => println!("Found value: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Key not found!"),
    }
    
    // Drop db and re-open
    drop(db);
    drop(batcher);
    
    println!("\nRe-opening database...");
    let db2 = Database::open(&db_path, LightningDbConfig::default())?;
    
    println!("Reading from reopened db...");
    match db2.get(b"key_0")? {
        Some(val) => println!("Found value: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Key not found!"),
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Enable debug logging
    tracing_subscriber::fmt()
        .with_env_filter("lightning_db=debug,info")
        .init();

    let db_path = Path::new("test_wal_recovery_btree_db");
    
    // Clean up any existing database
    if db_path.exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    println!("=== Phase 1: Creating database (B+Tree only, no LSM) and writing data ===");
    {
        let config = LightningDbConfig {
            compression_enabled: false, // This disables LSM
            ..Default::default()
        };
        let db = Database::create(db_path, config)?;
        
        // Write some test data
        for i in 0..5 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            println!("Writing: {} = {}", key, value);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Verify data is written
        for i in 0..5 {
            let key = format!("key_{}", i);
            let result = db.get(key.as_bytes())?;
            if let Some(value) = result {
                println!("Read back: {} = {}", key, String::from_utf8_lossy(&value));
            } else {
                println!("ERROR: Could not read back key: {}", key);
            }
        }
        
        // Sync to ensure data is persisted
        println!("Syncing database...");
        db.sync()?;
        
        println!("Database dropped, files should be persisted");
    }
    
    // Check what files were created
    println!("\n=== Files created ===");
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        println!("{}: {} bytes", path.display(), metadata.len());
        
        if path.is_dir() {
            // Check WAL directory
            for wal_entry in std::fs::read_dir(&path)? {
                let wal_entry = wal_entry?;
                let wal_path = wal_entry.path();
                let wal_metadata = wal_entry.metadata()?;
                println!("  {}: {} bytes", wal_path.display(), wal_metadata.len());
            }
        }
    }
    
    println!("\n=== Phase 2: Reopening database ===");
    {
        let config = LightningDbConfig {
            compression_enabled: false, // This disables LSM
            ..Default::default()
        };
        let db = Database::open(db_path, config)?;
        
        println!("Database reopened, checking for recovered data...");
        
        // Try to read the data again
        let mut found_count = 0;
        for i in 0..5 {
            let key = format!("key_{}", i);
            let result = db.get(key.as_bytes())?;
            if let Some(value) = result {
                println!("Found recovered: {} = {}", key, String::from_utf8_lossy(&value));
                found_count += 1;
            } else {
                println!("NOT FOUND: {}", key);
            }
        }
        
        println!("\nRecovered {} out of 5 entries", found_count);
        
        // Try writing new data to verify database is functional
        println!("\n=== Writing new data after recovery ===");
        db.put(b"new_key", b"new_value")?;
        let new_result = db.get(b"new_key")?;
        if let Some(value) = new_result {
            println!("New write successful: new_key = {}", String::from_utf8_lossy(&value));
        } else {
            println!("ERROR: New write failed");
        }
    }
    
    // Clean up
    std::fs::remove_dir_all(db_path)?;
    
    Ok(())
}
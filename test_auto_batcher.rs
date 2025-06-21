use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Simple AutoBatcher test");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    
    // Create database with async WAL to avoid sync issues
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;  // No sync to avoid hangs
    let db = Arc::new(Database::create(&db_path, config)?);
    
    // Test basic put/get first
    println!("Testing basic put/get...");
    db.put(b"test", b"value")?;
    let result = db.get(b"test")?;
    assert_eq!(result, Some(b"value".to_vec()));
    println!("✅ Basic put/get works");
    
    // Test AutoBatcher
    println!("Testing AutoBatcher...");
    let batcher = Database::create_auto_batcher(db.clone());
    
    // Submit a few writes
    for i in 0..5 {
        let key = format!("key{}", i);
        batcher.put(key.into_bytes(), b"value".to_vec())?;
        println!("Submitted write {}", i);
    }
    
    println!("Waiting for completion...");
    batcher.wait_for_completion()?;
    println!("✅ All writes completed");
    
    // Verify data
    for i in 0..5 {
        let key = format!("key{}", i);
        let result = db.get(key.as_bytes())?;
        assert_eq!(result, Some(b"value".to_vec()));
    }
    println!("✅ All data verified");
    
    batcher.shutdown();
    println!("✅ Test passed");
    
    Ok(())
}
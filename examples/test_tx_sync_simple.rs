use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Testing Transaction + Sync WAL\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test_tx_sync.db");
    
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    config.use_improved_wal = true;
    let db = Arc::new(Database::create(&db_path, config)?);
    
    println!("Starting transaction test with sync WAL...");
    
    // Test single transaction
    println!("Test 1: Single transaction with 1 operation");
    let start = Instant::now();
    
    let tx_id = db.begin_transaction()?;
    println!("  Transaction started: {}", tx_id);
    
    db.put_tx(tx_id, b"key1", b"value1")?;
    println!("  Put operation completed");
    
    println!("  Committing transaction...");
    db.commit_transaction(tx_id)?;
    
    let duration = start.elapsed();
    println!("  ✅ Transaction committed in {:.3}s", duration.as_secs_f64());
    
    // Test batch transaction
    println!("\nTest 2: Single transaction with 10 operations");
    let start = Instant::now();
    
    let tx_id = db.begin_transaction()?;
    for i in 0..10 {
        let key = format!("key_{}", i);
        db.put_tx(tx_id, key.as_bytes(), b"value")?;
        println!("  Put operation {} completed", i);
    }
    
    println!("  Committing transaction...");
    db.commit_transaction(tx_id)?;
    
    let duration = start.elapsed();
    println!("  ✅ Transaction committed in {:.3}s", duration.as_secs_f64());
    println!("  {:.0} ops/sec", 10.0 / duration.as_secs_f64());
    
    Ok(())
}
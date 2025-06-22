use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Testing Simple Sync WAL Performance\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().join("test_sync.db");
    
    // Test 1: Regular put with sync WAL
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(&db_path, config)?);
        
        println!("Test 1: Single writes with sync WAL");
        let start = Instant::now();
        let count = 10;
        
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
            println!("  Wrote key {}", i);
        }
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s\n", duration.as_secs_f64());
    }
    
    // Test 2: Batch writes in transaction
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(db_path.with_extension("tx"), config)?);
        
        println!("Test 2: Batch writes in transaction");
        let start = Instant::now();
        let count = 100;
        
        let tx_id = db.begin_transaction()?;
        for i in 0..count {
            let key = format!("tx_key_{:08}", i);
            db.put_tx(tx_id, key.as_bytes(), b"value")?;
        }
        db.commit_transaction(tx_id)?;
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    
    // Test 1: Async WAL performance (target: 248K ops/sec)
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Arc::new(Database::create(&dir.path().join("async.db"), config)?);
        
        println!("Testing Async WAL write performance...");
        let start = Instant::now();
        let count = 50000;
        
        for i in 0..count {
            let key = format!("key{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("Async WAL: {:.0} ops/sec", ops_per_sec);
        println!("Target: 248,000 ops/sec - {}", if ops_per_sec >= 248_000.0 { "✅ PASS" } else { "❌ FAIL" });
    }
    
    // Test 2: AutoBatcher performance
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Arc::new(Database::create(&dir.path().join("batcher.db"), config)?);
        let batcher = Database::create_auto_batcher(db.clone());
        
        println!("\nTesting AutoBatcher performance...");
        let start = Instant::now();
        let count = 50000;
        
        for i in 0..count {
            let key = format!("key{:08}", i);
            batcher.put(key.into_bytes(), b"value".to_vec())?;
        }
        
        batcher.wait_for_completion()?;
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("AutoBatcher: {:.0} ops/sec", ops_per_sec);
        println!("Target: 100,000+ ops/sec - {}", if ops_per_sec >= 100_000.0 { "✅ PASS" } else { "❌ FAIL" });
    }
    
    Ok(())
}
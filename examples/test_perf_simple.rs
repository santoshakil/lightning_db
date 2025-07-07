use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::time::Instant;
use tempfile::tempdir;

#[allow(dead_code)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Lightning DB performance with different configs...\n");

    let dir = tempdir()?;

    // Test 1: Default config (Sync mode)
    {
        println!("Test 1: Default config (WalSyncMode::Sync)");
        let db_path = dir.path().join("test1.db");
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config)?;

        let start = Instant::now();
        for i in 0..100 {
            db.put(format!("key{}", i).as_bytes(), b"value")?;
        }
        let duration = start.elapsed();
        let ops_sec = 100.0 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec\n", ops_sec);
    }

    // Test 2: Async mode
    {
        println!("Test 2: Async mode");
        let db_path = dir.path().join("test2.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Database::create(&db_path, config)?;

        let start = Instant::now();
        for i in 0..1000 {
            db.put(format!("key{}", i).as_bytes(), b"value")?;
        }
        let duration = start.elapsed();
        let ops_sec = 1000.0 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec\n", ops_sec);
    }

    // Test 3: Using AutoBatcher
    {
        println!("Test 3: Using AutoBatcher");
        let db_path = dir.path().join("test3.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = std::sync::Arc::new(Database::create(&db_path, config)?);

        let batcher = Database::create_auto_batcher(db);

        let start = Instant::now();
        for i in 0..1000 {
            batcher.put(format!("key{}", i).into_bytes(), b"value".to_vec())?;
        }
        batcher.flush()?;
        let duration = start.elapsed();
        let ops_sec = 1000.0 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec\n", ops_sec);
    }

    Ok(())
}

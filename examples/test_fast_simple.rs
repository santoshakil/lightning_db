use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FastAutoBatcher with small batch...");

    let dir = tempdir()?;
    let value = vec![0u8; 100];

    let db_path = dir.path().join("test.db");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;

    let db = Arc::new(Database::create(&db_path, config)?);
    let batcher = Database::create_fast_auto_batcher(db.clone());

    let count = 1000;
    println!("Writing {} operations...", count);
    let start = Instant::now();

    for i in 0..count {
        batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
    }

    println!("Flushing...");
    batcher.flush()?;

    println!("Waiting for completion...");
    batcher.wait_for_completion()?;

    let duration = start.elapsed();
    let ops_sec = count as f64 / duration.as_secs_f64();
    println!("Performance: {:.0} ops/sec", ops_sec);

    // Verify
    let test_key = format!("key{:06}", count / 2);
    if db.get(test_key.as_bytes())?.is_some() {
        println!("✅ Data verified");
    } else {
        println!("❌ Data not found!");
    }

    Ok(())
}

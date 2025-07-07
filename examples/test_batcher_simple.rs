use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing write batcher...");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async; // Use async to avoid sync delays
    let db = Arc::new(Database::create(&db_path, config)?);

    // Create write batcher
    let batcher = Database::create_with_batcher(db.clone());

    println!("Writing 5 entries...");
    for i in 0..5 {
        println!("  Writing key_{}", i);
        batcher.put(format!("key_{}", i).into_bytes(), b"value".to_vec())?;
    }

    println!("Flushing...");
    batcher.flush()?;

    println!("Getting stats...");
    let (queued, completed, batches) = batcher.get_stats();
    println!(
        "  Queued: {}, Completed: {}, Batches: {}",
        queued, completed, batches
    );

    println!("Shutting down...");
    batcher.shutdown();

    println!("Done!");
    Ok(())
}

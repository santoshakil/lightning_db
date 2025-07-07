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

    // Write just 5 items
    println!("Writing 5 items...");
    for i in 0..5 {
        let key = format!("key_{}", i);
        batcher.put(key.into_bytes(), b"value".to_vec())?;

        // Check stats after each write
        let (submitted, completed, batches, errors) = batcher.get_stats();
        println!(
            "  After write {}: submitted={}, completed={}, batches={}, errors={}",
            i, submitted, completed, batches, errors
        );
    }

    // Give some time for processing
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Check final stats
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!(
        "\nFinal stats: submitted={}, completed={}, batches={}, errors={}",
        submitted, completed, batches, errors
    );

    // Try to flush
    println!("\nFlushing...");
    batcher.flush()?;

    // Check stats again
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!(
        "After flush: submitted={}, completed={}, batches={}, errors={}",
        submitted, completed, batches, errors
    );

    // Shutdown
    batcher.shutdown();

    Ok(())
}

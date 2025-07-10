use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting test...");

    let dir = tempdir()?;
    println!("Created temp dir: {:?}", dir.path());

    let mut config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Async,
        ..Default::default()
    };
    config.use_improved_wal = false; // Disable improved WAL
    config.use_optimized_transactions = false; // Disable optimized transactions
    config.prefetch_enabled = false; // Disable prefetch
    config.cache_size = 0; // Disable cache
    config.compression_enabled = false; // Disable compression/LSM
    println!("Config created with all features disabled");

    println!("Creating database...");
    let db_path = dir.path().join("test.db");
    let db = Arc::new(Database::create(&db_path, config)?);
    println!("Database created");

    println!("Testing single put...");
    db.put(b"test", b"value")?;
    println!("Put complete");

    println!("Testing get...");
    let value = db.get(b"test")?;
    println!("Got value: {:?}", value);

    println!("Creating auto batcher...");
    let batcher = Database::create_auto_batcher(db.clone());
    println!("Auto batcher created");

    println!("Putting via batcher...");
    batcher.put(b"batch_test".to_vec(), b"batch_value".to_vec())?;
    println!("Batcher put complete");

    println!("Flushing batcher...");
    batcher.flush()?;
    println!("Flush complete");

    println!("All tests passed!");
    Ok(())
}

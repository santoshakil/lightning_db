use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    let db = Arc::new(Database::create(&db_path, config)?);

    // Try a single transaction
    println!("Testing transaction...");
    let tx_id = db.begin_transaction()?;
    println!("Transaction ID: {}", tx_id);

    println!("Put in transaction...");
    db.put_tx(tx_id, b"test_key", b"test_value")?;

    println!("Committing transaction...");
    match db.commit_transaction(tx_id) {
        Ok(_) => println!("✅ Commit succeeded"),
        Err(e) => {
            println!("❌ Commit failed: {}", e);
            return Ok(());
        }
    }

    // Read back
    println!("Reading key...");
    match db.get(b"test_key")? {
        Some(val) => println!("Found: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Not found!"),
    }

    Ok(())
}

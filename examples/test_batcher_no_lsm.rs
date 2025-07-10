use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create database without LSM tree
    let config = LightningDbConfig {
        compression_enabled: false,
        ..Default::default()
    }; // This disables LSM tree

    let db = Arc::new(Database::create(&db_path, config.clone())?);

    // Create auto batcher
    let batcher = Database::create_auto_batcher(db.clone());

    // Write through batcher
    println!("Writing through batcher...");
    batcher.put(b"key_batch".to_vec(), b"value_batch".to_vec())?;

    // Wait and flush
    println!("Waiting for completion...");
    batcher.wait_for_completion()?;

    // Read from db
    println!("Reading from db...");
    match db.get(b"key_batch")? {
        Some(val) => println!("Found: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Not found!"),
    }

    // Shutdown and drop
    batcher.shutdown();
    drop(batcher);
    drop(db);

    // Reopen
    println!("\nReopening database...");
    let db2 = Database::open(&db_path, config)?;

    println!("Reading from reopened db...");
    match db2.get(b"key_batch")? {
        Some(val) => println!("Found: {:?}", String::from_utf8_lossy(&val)),
        None => println!("Not found!"),
    }

    Ok(())
}

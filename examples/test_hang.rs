use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Test starting...");

    let dir = tempdir()?;
    println!("Temp dir created");

    let db_path = dir.path().join("test.db");
    println!("DB path: {:?}", db_path);

    let config = LightningDbConfig {
        wal_sync_mode: WalSyncMode::Async,
        ..Default::default()
    };
    println!("Config created");

    println!("About to create database...");
    let db = Arc::new(Database::create(&db_path, config)?);
    println!("Database created!");

    println!("About to create AutoBatcher...");
    let _batcher = Database::create_auto_batcher(db.clone());
    println!("AutoBatcher created!");

    Ok(())
}

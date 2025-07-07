use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_debug_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    println!("=== Phase 1: Writing data ===");
    println!("DB Path: {:?}", db_path);

    // Write data
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        config.use_improved_wal = true;

        println!("Opening database with config: {:?}", config.wal_sync_mode);
        let db = Database::open(db_path, config).unwrap();

        println!("Writing key-value pair...");
        db.put(b"test_key", b"test_value").unwrap();

        println!("Reading back immediately...");
        let value = db.get(b"test_key").unwrap();
        println!("Immediate read result: {:?}", value);
        assert_eq!(value.as_deref(), Some(b"test_value".as_ref()));

        println!("Calling checkpoint...");
        db.checkpoint().unwrap();

        println!("Dropping database...");
        drop(db);
        println!("Database dropped");
    }

    println!("\n=== Checking files ===");
    for entry in fs::read_dir(db_path).unwrap() {
        let entry = entry.unwrap();
        let metadata = fs::metadata(&entry.path()).unwrap();
        println!("  {:?} - {} bytes", entry.file_name(), metadata.len());

        // For WAL file, print first few bytes
        if entry.file_name().to_str().unwrap() == "wal" && metadata.is_file() && metadata.len() > 0
        {
            let data = fs::read(&entry.path()).unwrap();
            println!("    WAL first 32 bytes: {:?}", &data[..32.min(data.len())]);
        }
    }

    println!("\n=== Phase 2: Reading data ===");
    {
        let mut config = LightningDbConfig::default();
        config.use_improved_wal = true;

        println!("Opening database for recovery...");
        let db = Database::open(db_path, config).unwrap();
        println!("Database opened");

        println!("Attempting to read key...");
        let value = db.get(b"test_key").unwrap();
        println!("Read result: {:?}", value);

        if value.is_none() {
            println!("Data not found! Checking if key exists in btree...");

            // Try writing and reading a new key to see if database works
            println!("Writing new key...");
            db.put(b"new_key", b"new_value").unwrap();
            let new_value = db.get(b"new_key").unwrap();
            println!("New key read result: {:?}", new_value);
        }

        assert_eq!(value.as_deref(), Some(b"test_value".as_ref()));
    }
}

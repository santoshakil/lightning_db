use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use tempfile::tempdir;

#[test]
fn test_simple_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    println!("DB Path: {:?}", db_path);

    // Write data
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;

        let db = Database::open(db_path, config).unwrap();
        db.put(b"test_key", b"test_value").unwrap();

        // Verify we can read it back immediately
        let value = db.get(b"test_key").unwrap();
        assert_eq!(value.as_deref(), Some(b"test_value".as_ref()));

        // Call checkpoint to ensure data is on disk
        db.checkpoint().unwrap();

        println!("Data written and checkpointed");
    }

    // Check files exist
    println!("Files in db directory:");
    for entry in std::fs::read_dir(db_path).unwrap() {
        let entry = entry.unwrap();
        println!("  {:?}", entry.path());
    }

    // Read data back
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        println!("Database reopened");

        let value = db.get(b"test_key").unwrap();
        println!("Value read: {:?}", value);

        assert_eq!(value.as_deref(), Some(b"test_value".as_ref()));
    }
}

#[test]
fn test_persistence_without_checkpoint() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // Write data without checkpoint
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;

        let db = Database::open(db_path, config).unwrap();
        db.put(b"test_key2", b"test_value2").unwrap();

        // Just drop without checkpoint
        drop(db);
    }

    // Read data back
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        let value = db.get(b"test_key2").unwrap();
        assert_eq!(value.as_deref(), Some(b"test_value2".as_ref()));
    }
}

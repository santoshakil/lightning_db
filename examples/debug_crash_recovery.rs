use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let path = dir.path().to_path_buf();

    println!("Testing crash recovery with path: {:?}", path);

    // Phase 1: Write data
    {
        let config = LightningDbConfig {
            compression_enabled: false,
            use_improved_wal: false,
            prefetch_enabled: false,
            cache_size: 0,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::open(&path, config)?;

        // Test with the key from the failing property test
        let key = vec![0u8; 83];
        let value = vec![42u8; 600]; // Large value

        println!("Writing key={:?} value=<{} bytes>", &key[..5], value.len());
        db.put(&key, &value)?;

        // Verify we can read it immediately
        if let Some(retrieved) = db.get(&key)? {
            println!("Immediately after write: <{} bytes>", retrieved.len());
            assert_eq!(retrieved, value);
        } else {
            println!("ERROR: Could not read immediately after write!");
        }

        println!("Dropping database to simulate crash...");
        drop(db);
    }

    // Phase 2: Reopen and verify
    {
        let config = LightningDbConfig {
            compression_enabled: false,
            use_improved_wal: false,
            prefetch_enabled: false,
            cache_size: 0,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        println!("\nReopening database...");
        let db = Database::open(&path, config)?;

        let key = vec![0u8; 83];
        let expected_value = vec![42u8; 600];

        if let Some(retrieved) = db.get(&key)? {
            println!("After recovery: <{} bytes>", retrieved.len());
            assert_eq!(retrieved, expected_value);
            println!("SUCCESS: Data recovered correctly!");
        } else {
            println!("ERROR: Data not found after recovery!");
        }
    }

    Ok(())
}

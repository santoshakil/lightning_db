use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use tempfile::tempdir;

#[test]
fn test_tombstone_persistence_1000() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // Phase 1: Create database with config matching the failing test
    {
        let config = LightningDbConfig {
            cache_size: 10 * 1024 * 1024,
            compression_enabled: true,
            use_unified_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::create(db_path, config).unwrap();

        // Insert 1000 keys (matching the failing test)
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let value = vec![i as u8; 100];
            db.put(key.as_bytes(), &value).unwrap();
        }

        // Verify all keys exist
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist", key);
            assert_eq!(result.unwrap().len(), 100);
        }

        println!("Phase 1: Inserted and verified 1000 keys");
    }

    // Phase 2: Reopen and delete half
    {
        let db = Database::open(db_path, LightningDbConfig::default()).unwrap();

        // Delete first 500 keys
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Verify deletions work immediately
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_none(), "Key {} should be deleted", key);
        }

        // Verify remaining keys still exist
        for i in 500..1000 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should still exist", key);
        }

        println!("Phase 2: Deleted 500 keys and verified");

        // Force sync to disk
        db.sync().unwrap();
        println!("Phase 2: Database synced");
    }

    // Phase 3: Final verification
    {
        let db = Database::open(db_path, LightningDbConfig::default()).unwrap();

        println!("Phase 3: Database reopened");

        // First check with get()
        for i in 0..500 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_none(), "Key {} should be deleted after reopen", key);
        }

        for i in 500..1000 {
            let key = format!("key_{:04}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist after reopen", key);
        }

        println!("Phase 3: get() verification passed");

        // Now check with scan
        let mut count = 0;
        let mut deleted_found = 0;
        let iter = db.scan(None, None).unwrap();
        for result in iter {
            if let Ok((key, _value)) = result {
                count += 1;
                let key_str = String::from_utf8_lossy(&key);
                if let Some(num_str) = key_str.strip_prefix("key_") {
                    if let Ok(num) = num_str.parse::<usize>() {
                        if num < 500 {
                            deleted_found += 1;
                            println!("ERROR: Found deleted key in scan: {}", key_str);
                        }
                    }
                }
            }
        }

        println!("Phase 3: Scan found {} total keys, {} deleted keys", count, deleted_found);

        assert_eq!(deleted_found, 0, "Found {} keys that should have been deleted", deleted_found);
        assert_eq!(count, 500, "Expected 500 keys but found {}", count);

        println!("SUCCESS: All 1000-key tombstone tests passed!");
    }
}
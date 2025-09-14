use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_tombstone_persistence() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // Create database and insert some data
    {
        let db = Database::create(db_path, LightningDbConfig::default()).unwrap();

        // Insert 10 keys
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let value = format!("value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all keys exist
        for i in 0..10 {
            let key = format!("key_{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist", key);
        }

        // Delete first 5 keys
        for i in 0..5 {
            let key = format!("key_{:02}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Verify deletions work immediately
        for i in 0..5 {
            let key = format!("key_{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_none(), "Key {} should be deleted", key);
        }

        // Verify remaining keys still exist
        for i in 5..10 {
            let key = format!("key_{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should still exist", key);
        }

        println!("Before sync: deletions verified");

        // Force sync to disk
        db.sync().unwrap();

        println!("After sync: database synced");
    }

    // Reopen database and verify deletions persisted
    {
        let db = Database::open(db_path, LightningDbConfig::default()).unwrap();

        println!("Database reopened, checking deletions...");

        // Check deleted keys are still deleted using get()
        for i in 0..5 {
            let key = format!("key_{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_none(), "Key {} should be deleted after reopen", key);
        }

        // Check remaining keys still exist
        for i in 5..10 {
            let key = format!("key_{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist after reopen", key);
        }

        println!("After reopen: get() correctly returns None for deleted keys");

        // Now check with scan
        let mut found_keys = Vec::new();
        let iter = db.scan(None, None).unwrap();
        for result in iter {
            if let Ok((key, _value)) = result {
                let key_str = String::from_utf8_lossy(&key);
                if key_str.starts_with("key_") {
                    found_keys.push(key_str.to_string());
                }
            }
        }

        found_keys.sort();
        println!("Keys found in scan: {:?}", found_keys);

        // Check that deleted keys don't appear in scan
        for i in 0..5 {
            let key = format!("key_{:02}", i);
            assert!(!found_keys.contains(&key), "Deleted key {} found in scan!", key);
        }

        // Check that remaining keys appear in scan
        for i in 5..10 {
            let key = format!("key_{:02}", i);
            assert!(found_keys.contains(&key), "Key {} not found in scan!", key);
        }

        println!("SUCCESS: All tombstone tests passed!");
    }
}
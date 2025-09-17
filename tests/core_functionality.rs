use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;

#[test]
fn test_basic_crud() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Test put
    assert!(db.put(b"key1", b"value1").is_ok());

    // Test get
    match db.get(b"key1") {
        Ok(Some(val)) => assert_eq!(val, b"value1"),
        _ => panic!("Failed to get key1"),
    }

    // Test update
    assert!(db.put(b"key1", b"updated").is_ok());
    match db.get(b"key1") {
        Ok(Some(val)) => assert_eq!(val, b"updated"),
        _ => panic!("Failed to get updated key1"),
    }

    // Test delete
    assert!(db.delete(b"key1").is_ok());
    match db.get(b"key1") {
        Ok(None) => {},
        _ => panic!("Key should have been deleted"),
    }
}

#[test]
fn test_transaction_basics() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Test transaction commit
    let tx = db.begin_transaction().unwrap();
    assert!(db.put_tx(tx, b"tx_key", b"tx_value").is_ok());
    assert!(db.commit_transaction(tx).is_ok());

    // Verify committed data
    match db.get(b"tx_key") {
        Ok(Some(val)) => assert_eq!(val, b"tx_value"),
        _ => panic!("Transaction data not found"),
    }

    // Test transaction rollback
    let tx2 = db.begin_transaction().unwrap();
    assert!(db.put_tx(tx2, b"rollback_key", b"rollback_value").is_ok());
    assert!(db.abort_transaction(tx2).is_ok());

    // Verify rollback
    match db.get(b"rollback_key") {
        Ok(None) => {},
        _ => panic!("Rollback failed - key should not exist"),
    }
}

#[test]
fn test_concurrent_access() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Arc::new(Database::open(db_path.to_str().unwrap(), config).unwrap());
    let mut handles = vec![];

    // Spawn multiple threads doing concurrent operations
    for i in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for j in 0..10 {
                let key = format!("thread_{}_key_{}", i, j);
                let value = format!("thread_{}_value_{}", i, j);
                assert!(db_clone.put(key.as_bytes(), value.as_bytes()).is_ok());
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all data
    for i in 0..5 {
        for j in 0..10 {
            let key = format!("thread_{}_key_{}", i, j);
            let expected = format!("thread_{}_value_{}", i, j);
            match db.get(key.as_bytes()) {
                Ok(Some(val)) => assert_eq!(val, expected.as_bytes()),
                _ => panic!("Missing concurrent data: {}", key),
            }
        }
    }
}

#[test]
fn test_scan_operations() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Insert test data
    for i in 0..10 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{:02}", i);
        assert!(db.put(key.as_bytes(), value.as_bytes()).is_ok());
    }

    // Test full scan
    let iter = db.scan(None, None).unwrap();
    let mut count = 0;
    for _ in iter {
        count += 1;
    }
    assert_eq!(count, 10);

    // Test range scan
    let iter = db.scan(Some(b"key_02"), Some(b"key_05")).unwrap();
    let mut range_count = 0;
    for _ in iter {
        range_count += 1;
    }
    assert!((3..=4).contains(&range_count));
}

#[test]
fn test_error_handling() {
    let config = LightningDbConfig::default();
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let db = Database::open(db_path.to_str().unwrap(), config).unwrap();

    // Test non-existent key
    match db.get(b"non_existent") {
        Ok(None) => {},
        Ok(Some(_)) => panic!("Non-existent key returned value"),
        Err(e) => panic!("Unexpected error: {:?}", e),
    }

    // Test delete non-existent key (should not error)
    assert!(db.delete(b"non_existent").is_ok());

    // Test empty key - some databases don't support empty keys
    // Just verify it doesn't crash
    let _ = db.put(b"", b"empty_key_value");
    let _ = db.get(b"");

    // Test empty value
    assert!(db.put(b"empty_value", b"").is_ok());
    match db.get(b"empty_value") {
        Ok(Some(val)) => assert_eq!(val, b""),
        _ => panic!("Empty value not handled correctly"),
    }
}
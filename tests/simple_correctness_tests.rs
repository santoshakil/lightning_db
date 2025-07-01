use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

#[test]
fn test_basic_correctness() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test basic put/get
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    
    // Test overwrite
    db.put(b"key1", b"value2").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value2".to_vec()));
    
    // Test delete
    assert!(db.delete(b"key1").unwrap());
    assert_eq!(db.get(b"key1").unwrap(), None);
    
    // Test delete non-existent
    assert!(!db.delete(b"key1").unwrap());
}

#[test]
fn test_batch_operations() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test batch put
    let batch = vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
        (b"k3".to_vec(), b"v3".to_vec()),
    ];
    db.put_batch(&batch).unwrap();
    
    // Verify all exist
    assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    
    // Test batch delete
    let keys = vec![b"k1".to_vec(), b"k2".to_vec()];
    db.delete_batch(&keys).unwrap();
    
    // Verify deleted
    assert_eq!(db.get(b"k1").unwrap(), None);
    assert_eq!(db.get(b"k2").unwrap(), None);
    assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn test_transaction_correctness() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Setup initial data
    db.put(b"key1", b"initial1").unwrap();
    db.put(b"key2", b"initial2").unwrap();
    
    // Start transaction
    let tx_id = db.begin_transaction().unwrap();
    
    // Modify in transaction
    db.put_tx(tx_id, b"key1", b"tx1").unwrap();
    db.put_tx(tx_id, b"key3", b"tx3").unwrap();
    
    // Verify isolation - outside transaction sees old values
    assert_eq!(db.get(b"key1").unwrap(), Some(b"initial1".to_vec()));
    assert_eq!(db.get(b"key3").unwrap(), None);
    
    // Inside transaction sees new values
    assert_eq!(db.get_tx(tx_id, b"key1").unwrap(), Some(b"tx1".to_vec()));
    assert_eq!(db.get_tx(tx_id, b"key3").unwrap(), Some(b"tx3".to_vec()));
    
    // Commit transaction
    db.commit_transaction(tx_id).unwrap();
    
    // Now outside sees new values
    assert_eq!(db.get(b"key1").unwrap(), Some(b"tx1".to_vec()));
    assert_eq!(db.get(b"key3").unwrap(), Some(b"tx3".to_vec()));
}

#[test]
fn test_transaction_rollback() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Setup initial data
    db.put(b"key1", b"initial1").unwrap();
    
    // Start transaction
    let tx_id = db.begin_transaction().unwrap();
    
    // Modify in transaction
    db.put_tx(tx_id, b"key1", b"tx1").unwrap();
    db.put_tx(tx_id, b"key2", b"tx2").unwrap();
    
    // Abort transaction
    db.abort_transaction(tx_id).unwrap();
    
    // Verify rollback - should see original values
    assert_eq!(db.get(b"key1").unwrap(), Some(b"initial1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), None);
}

#[test]
fn test_key_ordering() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert keys in random order
    let keys = vec!["key5", "key1", "key3", "key2", "key4"];
    for (i, key) in keys.iter().enumerate() {
        db.put(key.as_bytes(), format!("value{}", i).as_bytes()).unwrap();
    }
    
    // Scan and verify order
    let mut scanned_keys = Vec::new();
    let results = db.scan(None, None).unwrap();
    
    for result in results {
        let (key, _value) = result.unwrap();
        if key.starts_with(b"key") {
            scanned_keys.push(String::from_utf8(key).unwrap());
        }
    }
    
    // Verify keys are sorted
    let mut expected = keys.clone();
    expected.sort();
    
    assert_eq!(scanned_keys, expected);
}

#[test]
fn test_persistence_simple() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();
    
    // Write data
    {
        let db = Database::open(db_path, LightningDbConfig::default()).unwrap();
        db.put(b"persist1", b"value1").unwrap();
        db.put(b"persist2", b"value2").unwrap();
        db.checkpoint().unwrap();
    }
    
    // Reopen and verify
    {
        let db = Database::open(db_path, LightningDbConfig::default()).unwrap();
        assert_eq!(db.get(b"persist1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(db.get(b"persist2").unwrap(), Some(b"value2".to_vec()));
    }
}

#[test]
fn test_large_values() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test with large value
    let large_value = vec![42u8; 100_000]; // 100KB
    db.put(b"large_key", &large_value).unwrap();
    
    let retrieved = db.get(b"large_key").unwrap().unwrap();
    assert_eq!(retrieved, large_value);
}

#[test]
fn test_many_keys() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert many keys
    let num_keys = 1000;
    for i in 0..num_keys {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Verify all keys
    for i in 0..num_keys {
        let key = format!("key_{:06}", i);
        let expected_value = format!("value_{:06}", i);
        let actual = db.get(key.as_bytes()).unwrap();
        assert_eq!(actual, Some(expected_value.into_bytes()));
    }
    
    // Verify count via scan
    let mut count = 0;
    let results = db.scan(None, None).unwrap();
    for _ in results {
        count += 1;
    }
    assert_eq!(count, num_keys);
}

#[test]
fn test_concurrent_reads() {
    use std::sync::Arc;
    use std::thread;
    
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Insert test data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Spawn concurrent readers
    let mut handles = vec![];
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let expected = format!("value_{:03}", i);
                let actual = db_clone.get(key.as_bytes()).unwrap();
                assert_eq!(actual, Some(expected.into_bytes()));
            }
            thread_id
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
}
use lightning_db::Database;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_empty_key() {
    let db = Database::create_temp().unwrap();
    let result = db.put(b"", b"value");
    assert!(result.is_err());
}

#[test]
fn test_empty_value() {
    let db = Database::create_temp().unwrap();
    db.put(b"key", b"").unwrap();
    let val = db.get(b"key").unwrap();
    assert_eq!(val, Some(vec![]));
}

#[test]
fn test_max_key_size() {
    let db = Database::create_temp().unwrap();
    let key = vec![b'k'; 4096]; // Max key size
    db.put(&key, b"value").unwrap();
    assert!(db.get(&key).unwrap().is_some());

    // Exceed max key size
    let big_key = vec![b'k'; 4097];
    assert!(db.put(&big_key, b"value").is_err());
}

#[test]
fn test_max_value_size() {
    let db = Database::create_temp().unwrap();
    let value = vec![b'v'; 1024 * 1024]; // 1MB max value
    db.put(b"key", &value).unwrap();
    assert_eq!(db.get(b"key").unwrap().unwrap(), value);

    // Exceed max value size
    let big_value = vec![b'v'; 1024 * 1024 + 1];
    assert!(db.put(b"key2", &big_value).is_err());
}

#[test]
fn test_unicode_keys_values() {
    let db = Database::create_temp().unwrap();

    // Various unicode strings
    let test_cases = vec![
        ("hello", "world"),
        ("你好", "世界"),
        ("مرحبا", "عالم"),
        ("🔥", "⚡"),
        ("mixed_中文_emoji_🚀", "value_值_💾"),
    ];

    for (key, value) in test_cases {
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let result = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(result, value.as_bytes());
    }
}

#[test]
fn test_binary_data() {
    let db = Database::create_temp().unwrap();

    // Test with all byte values
    let mut key = Vec::new();
    let mut value = Vec::new();
    for i in 0..=255u8 {
        key.push(i);
        value.push(255 - i);
    }

    db.put(&key, &value).unwrap();
    assert_eq!(db.get(&key).unwrap().unwrap(), value);
}

#[test]
fn test_rapid_create_delete() {
    let db = Database::create_temp().unwrap();

    for i in 0..1000 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);

        // Rapid create/delete cycles
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        assert!(db.get(key.as_bytes()).unwrap().is_some());
        db.delete(key.as_bytes()).unwrap();
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }
}

#[test]
fn test_concurrent_same_key_access() {
    let db = Arc::new(Database::create_temp().unwrap());
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    // 10 threads all trying to modify the same key
    for i in 0..10 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for j in 0..100 {
                let value = format!("thread_{}_iter_{}", i, j);
                let _ = db_clone.put(b"shared_key", value.as_bytes());
                let _ = db_clone.get(b"shared_key");
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_transaction_edge_cases() {
    let db = Database::create_temp().unwrap();

    // Multiple begins without commit
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();
    let tx3 = db.begin_transaction().unwrap();

    // Write with different transactions
    db.put_tx(tx1, b"key1", b"value1").unwrap();
    db.put_tx(tx2, b"key2", b"value2").unwrap();
    db.put_tx(tx3, b"key3", b"value3").unwrap();

    // Abort middle transaction
    db.abort_transaction(tx2).unwrap();

    // Commit others
    db.commit_transaction(tx1).unwrap();
    db.commit_transaction(tx3).unwrap();

    // Check results
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(db.get(b"key2").unwrap(), None); // Aborted
    assert_eq!(db.get(b"key3").unwrap(), Some(b"value3".to_vec()));
}

#[test]
fn test_double_commit_abort() {
    let db = Database::create_temp().unwrap();
    let tx = db.begin_transaction().unwrap();

    db.put_tx(tx, b"key", b"value").unwrap();
    db.commit_transaction(tx).unwrap();

    // Double commit should fail
    assert!(db.commit_transaction(tx).is_err());

    let tx2 = db.begin_transaction().unwrap();
    db.abort_transaction(tx2).unwrap();

    // Double abort should fail
    assert!(db.abort_transaction(tx2).is_err());
}

#[test]
fn test_overwrite_behavior() {
    let db = Database::create_temp().unwrap();

    // Initial value
    db.put(b"key", b"value1").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value1".to_vec()));

    // Overwrite with same size
    db.put(b"key", b"value2").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"value2".to_vec()));

    // Overwrite with larger value
    db.put(b"key", b"much_larger_value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"much_larger_value".to_vec()));

    // Overwrite with smaller value
    db.put(b"key", b"tiny").unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"tiny".to_vec()));
}

#[test]
fn test_pattern_keys() {
    let db = Database::create_temp().unwrap();

    // Keys with special patterns
    let patterns = vec![
        b"a".to_vec(),
        b"aa".to_vec(),
        b"aaa".to_vec(),
        vec![0xFF; 100],
        vec![0x00; 100],
        vec![0xFF, 0x00, 0xFF, 0x00],
        b"key with spaces".to_vec(),
        b"key\twith\ttabs".to_vec(),
        b"key\nwith\nnewlines".to_vec(),
    ];

    for (i, key) in patterns.iter().enumerate() {
        let value = format!("value_{}", i);
        db.put(key, value.as_bytes()).unwrap();
        assert_eq!(db.get(key).unwrap(), Some(value.into_bytes()));
    }
}

#[test]
fn test_sequential_keys_performance() {
    let db = Database::create_temp().unwrap();

    // Insert sequential keys (best case for B+Tree)
    for i in 0..1000 {
        let key = format!("{:08}", i); // Zero-padded for lexicographic order
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify in order
    for i in 0..1000 {
        let key = format!("{:08}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_reverse_sequential_keys() {
    let db = Database::create_temp().unwrap();

    // Insert reverse sequential keys (worst case for some data structures)
    for i in (0..1000).rev() {
        let key = format!("{:08}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify
    for i in 0..1000 {
        let key = format!("{:08}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn test_delete_non_existent() {
    let db = Database::create_temp().unwrap();

    // Delete non-existent key should succeed (idempotent)
    db.delete(b"non_existent").unwrap();

    // Add and delete
    db.put(b"key", b"value").unwrap();
    db.delete(b"key").unwrap();

    // Delete again should still succeed
    db.delete(b"key").unwrap();
}
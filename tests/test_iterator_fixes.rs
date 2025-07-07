use lightning_db::{Database, LightningDbConfig};
use std::collections::HashSet;
use tempfile::tempdir;

#[test]
fn test_iterator_ordering() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert keys in random order
    let keys = vec!["key5", "key1", "key3", "key2", "key4"];
    for key in &keys {
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Scan and verify ordering
    let iter = db.scan(None, None).unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    assert_eq!(results.len(), 5);

    // Verify keys are in sorted order
    let mut prev_key: Option<Vec<u8>> = None;
    for (key, _) in results {
        if let Some(prev) = prev_key {
            assert!(
                key > prev,
                "Keys not in order: {:?} <= {:?}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&prev)
            );
        }
        prev_key = Some(key);
    }
}

#[test]
fn test_iterator_deduplication() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert and update same keys multiple times
    for i in 0..5 {
        let key = format!("key{}", i);
        let value1 = format!("value{}_v1", i);
        let value2 = format!("value{}_v2", i);

        db.put(key.as_bytes(), value1.as_bytes()).unwrap();
        db.put(key.as_bytes(), value2.as_bytes()).unwrap(); // Update
    }

    // Scan and verify no duplicates
    let iter = db.scan(None, None).unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    assert_eq!(results.len(), 5, "Should have exactly 5 unique keys");

    // Verify each key appears only once and has the latest value
    let mut seen_keys = HashSet::new();
    for (key, value) in results {
        assert!(
            seen_keys.insert(key.clone()),
            "Duplicate key found: {:?}",
            String::from_utf8_lossy(&key)
        );

        // Verify we got the latest value (v2)
        let value_str = String::from_utf8_lossy(&value);
        assert!(value_str.ends_with("_v2"), "Got old value: {}", value_str);
    }
}

#[test]
fn test_iterator_bounds() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert keys
    for i in 0..10 {
        let key = format!("key{:02}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Test exclusive bounds
    let iter = db
        .scan(Some(b"key02".to_vec()), Some(b"key05".to_vec()))
        .unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    let keys: Vec<_> = results
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).to_string())
        .collect();

    assert_eq!(keys, vec!["key02", "key03", "key04"]);
}

#[test]
fn test_iterator_with_deletes() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert keys
    for i in 0..10 {
        let key = format!("key{:02}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Delete some keys
    db.delete(b"key03").unwrap();
    db.delete(b"key05").unwrap();
    db.delete(b"key07").unwrap();

    // Scan and verify deleted keys are not returned
    let iter = db.scan(None, None).unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    assert_eq!(results.len(), 7);

    let keys: Vec<_> = results
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).to_string())
        .collect();

    // Verify deleted keys are not present
    assert!(!keys.contains(&"key03".to_string()));
    assert!(!keys.contains(&"key05".to_string()));
    assert!(!keys.contains(&"key07".to_string()));
}

#[test]
fn test_reverse_iterator() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert keys
    let keys = vec!["a", "b", "c", "d", "e"];
    for key in &keys {
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Test reverse scan
    let iter = db.scan_reverse(None, None).unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    assert_eq!(results.len(), 5);

    // Verify reverse order
    let result_keys: Vec<_> = results
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).to_string())
        .collect();

    assert_eq!(result_keys, vec!["e", "d", "c", "b", "a"]);
}

#[test]
fn test_iterator_limit() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert many keys
    for i in 0..100 {
        let key = format!("key{:03}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    }

    // Test with limit
    let iter = db.scan_limit(None, 10).unwrap();
    let results: Result<Vec<_>, _> = iter.collect();
    let results = results.unwrap();

    assert_eq!(results.len(), 10);

    // Verify we got the first 10 keys
    for (i, (key, _)) in results.iter().enumerate().take(10) {
        let expected = format!("key{:03}", i);
        assert_eq!(key, expected.as_bytes());
    }
}

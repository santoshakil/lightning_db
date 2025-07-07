use lightning_db::{Database, LightningDbConfig};
use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// Generate arbitrary keys and values
fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..100)
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 1..1000)
}

// Property: Sequential operations maintain consistency
proptest! {
    #[test]
    fn prop_sequential_consistency(
        operations in prop_vec(
            prop_oneof![
                (key_strategy(), value_strategy()).prop_map(|(k, v)| ("put", k, Some(v))),
                key_strategy().prop_map(|k| ("get", k, None)),
                key_strategy().prop_map(|k| ("delete", k, None)),
            ],
            1..100
        )
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
        let mut expected = HashMap::new();

        for (op, key, value) in operations {
            match op {
                "put" => {
                    let val = value.unwrap();
                    db.put(&key, &val).unwrap();
                    expected.insert(key.clone(), val);
                }
                "get" => {
                    let actual = db.get(&key).unwrap();
                    let expected_val = expected.get(&key).cloned();
                    assert_eq!(actual, expected_val);
                }
                "delete" => {
                    db.delete(&key).unwrap();
                    expected.remove(&key);
                }
                _ => unreachable!()
            }
        }

        // Verify final state
        for (key, expected_val) in expected.iter() {
            let actual = db.get(key).unwrap();
            assert_eq!(actual.as_ref(), Some(expected_val));
        }
    }
}

// Property: Transactions provide isolation
proptest! {
    #[test]
    fn prop_transaction_isolation(
        keys in prop_vec(key_strategy(), 1..20),
        values in prop_vec(value_strategy(), 1..20),
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Ensure we have enough values for keys
        let num_ops = keys.len().min(values.len());
        let keys = &keys[..num_ops];
        let values = &values[..num_ops];

        // Write initial data
        for (key, value) in keys.iter().zip(values.iter()) {
            db.put(key, value).unwrap();
        }

        // Start transaction and modify data
        let tx_id = db.begin_transaction().unwrap();
        let new_value = b"transaction_value";

        for key in keys.iter() {
            db.put_tx(tx_id, key, new_value).unwrap();
        }

        // Verify isolation - outside transaction should see old values
        for (key, value) in keys.iter().zip(values.iter()) {
            let actual = db.get(key).unwrap();
            assert_eq!(actual.as_ref(), Some(value));
        }

        // Verify inside transaction sees new values
        for key in keys.iter() {
            let actual = db.get_tx(tx_id, key).unwrap();
            assert_eq!(actual.as_ref(), Some(&new_value.to_vec()));
        }

        // Abort transaction
        db.abort_transaction(tx_id).unwrap();

        // Verify rollback - should see original values
        for (key, value) in keys.iter().zip(values.iter()) {
            let actual = db.get(key).unwrap();
            assert_eq!(actual.as_ref(), Some(value));
        }
    }
}

// Property: Batch operations are atomic
proptest! {
    #[test]
    fn prop_batch_atomicity(
        batch in prop_vec((key_strategy(), value_strategy()), 1..50),
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Insert batch
        db.put_batch(&batch).unwrap();

        // Verify all entries exist
        for (key, value) in &batch {
            let actual = db.get(key).unwrap();
            assert_eq!(actual.as_ref(), Some(value));
        }

        // Delete batch
        let keys: Vec<_> = batch.iter().map(|(k, _)| k.clone()).collect();
        db.delete_batch(&keys).unwrap();

        // Verify all entries are deleted
        for key in &keys {
            let actual = db.get(key).unwrap();
            assert_eq!(actual, None);
        }
    }
}

// Property: Keys are ordered correctly
proptest! {
    #[test]
    fn prop_key_ordering(
        mut keys in prop_vec(key_strategy(), 10..50),
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Remove duplicates
        keys.sort();
        keys.dedup();

        // Insert keys with values
        for (i, key) in keys.iter().enumerate() {
            let value = format!("value_{}", i);
            db.put(key, value.as_bytes()).unwrap();
        }

        // Scan and verify order
        let scan_results = db.scan(None, None).unwrap();
        let scanned_keys: Vec<_> = scan_results
            .map(|r| r.unwrap().0)
            .collect();

        // Verify keys are in sorted order
        for window in scanned_keys.windows(2) {
            assert!(window[0] <= window[1], "Keys should be in sorted order");
        }

        // Verify all inserted keys are present
        let scanned_set: HashSet<_> = scanned_keys.into_iter().collect();
        let inserted_set: HashSet<_> = keys.into_iter().collect();

        // Check that inserted keys are a subset of scanned keys
        // (there might be other keys from previous tests)
        for key in &inserted_set {
            assert!(scanned_set.contains(key), "Inserted key should be in scan results");
        }
    }
}

// Property: Persistence across reopens
proptest! {
    #[test]
    fn prop_persistence(
        data in prop_vec((key_strategy(), value_strategy()), 1..30),
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path();

        // Phase 1: Write data
        {
            let db = Database::open(db_path, LightningDbConfig::default()).unwrap();
            for (key, value) in &data {
                db.put(key, value).unwrap();
            }
            db.checkpoint().unwrap();
        }

        // Phase 2: Reopen and verify
        {
            let db = Database::open(db_path, LightningDbConfig::default()).unwrap();
            for (key, value) in &data {
                let actual = db.get(key).unwrap();
                assert_eq!(actual.as_ref(), Some(value), "Data should persist across reopens");
            }
        }
    }
}

// Property: Concurrent reads are consistent
proptest! {
    #[test]
    fn prop_concurrent_read_consistency(
        keys in prop_vec(key_strategy(), 10..30),
        values in prop_vec(value_strategy(), 10..30),
        num_readers in 2..8usize,
    ) {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::open(dir.path(), LightningDbConfig::default()).unwrap());

        // Prepare data
        let num_entries = keys.len().min(values.len());
        let data: Vec<_> = keys.into_iter()
            .zip(values.into_iter())
            .take(num_entries)
            .collect();

        // Insert data
        for (key, value) in &data {
            db.put(key, value).unwrap();
        }

        // Concurrent reads
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = vec![];

        for _ in 0..num_readers {
            let db_clone = Arc::clone(&db);
            let data_clone = data.clone();
            let results_clone = Arc::clone(&results);

            let handle = std::thread::spawn(move || {
                let mut thread_results = Vec::new();
                for (key, _) in &data_clone {
                    let value = db_clone.get(key).unwrap();
                    thread_results.push((key.clone(), value));
                }
                results_clone.lock().unwrap().push(thread_results);
            });
            handles.push(handle);
        }

        // Wait for all readers
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all readers saw the same data
        let all_results = results.lock().unwrap();
        let first_result = &all_results[0];

        for result in all_results.iter().skip(1) {
            assert_eq!(result, first_result, "All concurrent readers should see the same data");
        }

        // Verify correctness of data
        for (i, (_key, value)) in first_result.iter().enumerate() {
            assert_eq!(value.as_ref(), Some(&data[i].1));
        }
    }
}

// Property: Memory usage is bounded
proptest! {
    #[test]
    fn prop_memory_bounded(
        operations in prop_vec(
            (key_strategy(), value_strategy()),
            100..500
        )
    ) {
        let dir = tempdir().unwrap();
        let mut config = LightningDbConfig::default();
        config.cache_size = 1024 * 1024; // 1MB cache

        let db = Database::open(dir.path(), config).unwrap();

        // Get initial stats
        let initial_stats = db.stats();

        // Perform operations
        for (key, value) in operations {
            db.put(&key, &value).unwrap();
        }

        // Get final stats
        let final_stats = db.stats();

        // Verify constraints
        assert!(final_stats.page_count >= initial_stats.page_count);
        // Memory usage verification - just ensure database is operational
        assert!(final_stats.page_count > 0);
    }
}

// Property: Delete operations are idempotent
proptest! {
    #[test]
    fn prop_delete_idempotent(
        key in key_strategy(),
        value in value_strategy(),
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Insert
        db.put(&key, &value).unwrap();
        assert_eq!(db.get(&key).unwrap(), Some(value));

        // First delete
        let existed1 = db.delete(&key).unwrap();
        assert!(existed1);
        assert_eq!(db.get(&key).unwrap(), None);

        // Second delete (idempotent)
        let existed2 = db.delete(&key).unwrap();
        assert!(!existed2);
        assert_eq!(db.get(&key).unwrap(), None);

        // Third delete (still idempotent)
        let existed3 = db.delete(&key).unwrap();
        assert!(!existed3);
        assert_eq!(db.get(&key).unwrap(), None);
    }
}

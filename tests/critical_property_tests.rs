use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use proptest::collection::{hash_map, vec as prop_vec};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use tempfile::tempdir;

proptest! {
    // Property: B+Tree maintains sorted order and all keys are findable
    #[test]
    fn prop_btree_sorted_order(
        operations in prop_vec(
            prop_oneof![
                (prop_vec(any::<u8>(), 1..100), prop_vec(any::<u8>(), 1..1000)).prop_map(|(k, v)| ("insert", k, Some(v))),
                prop_vec(any::<u8>(), 1..100).prop_map(|k| ("delete", k, None)),
            ],
            10..200
        )
    ) {
        let dir = tempdir().unwrap();
        let config = LightningDbConfig::default();
        let db = Database::open(dir.path(), config).unwrap();

        let mut expected_data = HashMap::new();

        // Apply operations
        for (op, key, value) in &operations {
            match op as &str {
                "insert" => {
                    let val = value.as_ref().unwrap();
                    db.put(key, val).unwrap();
                    expected_data.insert(key.clone(), val.clone());
                }
                "delete" => {
                    db.delete(key).unwrap();
                    expected_data.remove(key);
                }
                _ => unreachable!()
            }
        }

        // Verify all expected keys exist with correct values
        for (key, expected_value) in &expected_data {
            let actual = db.get(key).unwrap();
            prop_assert_eq!(actual.as_ref(), Some(expected_value));
        }

        // Verify deleted keys don't exist
        let all_keys: HashSet<_> = operations.iter()
            .map(|(_, k, _)| k.clone())
            .collect();

        for key in all_keys {
            if !expected_data.contains_key(&key) {
                prop_assert!(db.get(&key).unwrap().is_none());
            }
        }
    }
}

proptest! {
    // Property: WAL recovery preserves exact operation order
    #[test]
    fn prop_wal_operation_order(
        operations in prop_vec(
            (prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 1..200)),
            5..50
        )
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().to_path_buf();

        // Record operations with explicit ordering
        let mut operation_log = Vec::new();

        // Phase 1: Execute operations and track them
        {
            let config = LightningDbConfig {
                use_improved_wal: true,
                wal_sync_mode: WalSyncMode::Sync,
                ..Default::default()
            };

            let db = Database::open(&db_path, config).unwrap();

            for (i, (key, value)) in operations.iter().enumerate() {
                db.put(key, value).unwrap();
                operation_log.push((i, key.clone(), value.clone()));
            }

            db.checkpoint().unwrap();
        }

        // Phase 2: Verify recovery preserves all operations
        {
            let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

            for (_order, key, expected_value) in &operation_log {
                let actual = db.get(key).unwrap();
                prop_assert_eq!(actual.as_ref(), Some(expected_value));
            }
        }
    }
}

proptest! {
    // Property: Transactions provide true isolation
    #[test]
    fn prop_transaction_true_isolation(
        initial_data in hash_map(prop_vec(any::<u8>(), 1..20), prop_vec(any::<u8>(), 1..100), 5..20),
        tx_operations in prop_vec(
            prop_vec(
                (prop_vec(any::<u8>(), 1..20), prop_vec(any::<u8>(), 1..100)),
                1..10
            ),
            2..5
        )
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Setup initial data
        for (key, value) in &initial_data {
            db.put(key, value).unwrap();
        }

        // Start multiple transactions
        let mut transactions = Vec::new();
        for _ in 0..tx_operations.len() {
            transactions.push(db.begin_transaction().unwrap());
        }

        // Apply operations to each transaction
        for (tx_idx, operations) in tx_operations.iter().enumerate() {
            let tx_id = transactions[tx_idx];

            for (key, value) in operations {
                db.put_tx(tx_id, key, value).unwrap();
            }
        }

        // Verify isolation: no transaction sees another's changes
        for (tx_idx, operations) in tx_operations.iter().enumerate() {
            let tx_id = transactions[tx_idx];

            // Check this transaction sees its own changes
            for (key, value) in operations {
                let result = db.get_tx(tx_id, key).unwrap();
                prop_assert_eq!(result.as_ref(), Some(value));
            }

            // Check this transaction doesn't see other transactions' changes
            for (other_idx, other_operations) in tx_operations.iter().enumerate() {
                if other_idx != tx_idx {
                    for (key, _) in other_operations {
                        if !operations.iter().any(|(k, _)| k == key) {
                            let result = db.get_tx(tx_id, key).unwrap();
                            let expected = initial_data.get(key);
                            prop_assert_eq!(result.as_deref(), expected.map(|v| v.as_slice()));
                        }
                    }
                }
            }
        }

        // Commit first transaction
        db.commit_transaction(transactions[0]).unwrap();

        // New transaction should see committed changes
        let new_tx = db.begin_transaction().unwrap();
        for (key, value) in &tx_operations[0] {
            let result = db.get_tx(new_tx, key).unwrap();
            prop_assert_eq!(result.as_ref(), Some(value));
        }
    }
}

proptest! {
    // Property: Page allocation never reuses active pages
    #[test]
    fn prop_page_allocation_safety(
        num_allocations in 10usize..100,
        deallocate_pattern in prop_vec(any::<bool>(), 10..100)
    ) {
        let dir = tempdir().unwrap();
        use lightning_db::storage::PageManager;
        use std::sync::Arc;
        use parking_lot::RwLock;

        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 10 * 1024 * 1024).unwrap()
        ));

        let mut allocated_pages = Vec::new();
        let mut freed_pages = HashSet::new();

        // Allocate pages
        for _ in 0..num_allocations {
            let page_id = page_manager.write().allocate_page().unwrap();

            // Verify this page wasn't already allocated
            prop_assert!(!allocated_pages.contains(&page_id),
                        "Page {} was allocated twice!", page_id);

            allocated_pages.push(page_id);
        }

        // Deallocate some pages based on pattern
        for (i, should_free) in deallocate_pattern.iter().enumerate() {
            if *should_free && i < allocated_pages.len() {
                let page_id = allocated_pages[i];
                if !freed_pages.contains(&page_id) {
                    page_manager.write().free_page(page_id);
                    freed_pages.insert(page_id);
                }
            }
        }

        // Allocate more pages
        let mut new_allocations = Vec::new();
        for _ in 0..20 {
            let page_id = page_manager.write().allocate_page().unwrap();
            new_allocations.push(page_id);
        }

        // Verify no active pages were reused
        for &page_id in &new_allocations {
            if allocated_pages.contains(&page_id) {
                prop_assert!(freed_pages.contains(&page_id),
                            "Active page {} was reallocated!", page_id);
            }
        }
    }
}

proptest! {
    // Property: Cache consistency - cached data matches disk
    #[test]
    fn prop_cache_consistency(
        operations in prop_vec(
            prop_oneof![
                (prop_vec(any::<u8>(), 1..50), prop_vec(any::<u8>(), 1..500)).prop_map(|(k, v)| ("put", k, Some(v))),
                prop_vec(any::<u8>(), 1..50).prop_map(|k| ("get", k, None)),
                prop_vec(any::<u8>(), 1..50).prop_map(|k| ("delete", k, None)),
            ],
            50..200
        )
    ) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().to_path_buf();

        // Use a small cache to force evictions
        let config = LightningDbConfig {
            cache_size: 1024 * 1024, // 1MB cache
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();
        let mut expected_state = HashMap::new();

        // Execute operations
        for (op, key, value) in operations {
            match op {
                "put" => {
                    let val = value.unwrap();
                    db.put(&key, &val).unwrap();
                    expected_state.insert(key, val);
                }
                "get" => {
                    let actual = db.get(&key).unwrap();
                    let expected = expected_state.get(&key);
                    prop_assert_eq!(actual.as_ref(), expected);
                }
                "delete" => {
                    db.delete(&key).unwrap();
                    expected_state.remove(&key);
                }
                _ => unreachable!()
            }
        }

        // Force checkpoint to ensure data is on disk
        db.checkpoint().unwrap();

        // Clear cache by reopening database
        drop(db);
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        // Verify all data matches expected state
        for (key, expected_value) in expected_state {
            let actual = db.get(&key).unwrap();
            prop_assert_eq!(actual, Some(expected_value));
        }
    }
}

proptest! {
    // Property: Compression preserves data integrity
    #[test]
    fn prop_compression_integrity(
        data_entries in prop_vec(
            (prop_vec(any::<u8>(), 1..100), prop_vec(any::<u8>(), 100..10000)),
            10..50
        )
    ) {
        let dir = tempdir().unwrap();
        let config = LightningDbConfig {
            compression_enabled: true,
            compression_type: 1, // Zstd
            ..Default::default()
        };

        let db = Database::open(dir.path(), config).unwrap();

        // Insert data
        for (key, value) in &data_entries {
            db.put(key, value).unwrap();
        }

        // Force data to disk
        db.checkpoint().unwrap();

        // Verify all data is correctly decompressed
        for (key, expected_value) in &data_entries {
            let actual = db.get(key).unwrap();
            prop_assert_eq!(actual.as_ref(), Some(expected_value));
        }

        // Reopen database to test decompression on cold start
        drop(db);
        let db = Database::open(dir.path(), LightningDbConfig {
            compression_enabled: true,
            compression_type: 1, // Zstd
            ..Default::default()
        }).unwrap();

        for (key, expected_value) in &data_entries {
            let actual = db.get(key).unwrap();
            prop_assert_eq!(actual.as_ref(), Some(expected_value));
        }
    }
}

proptest! {
    // Property: Concurrent transactions maintain consistency
    #[test]
    fn prop_concurrent_transaction_consistency(
        initial_balance in 1000u64..10000,
        transfer_amounts in prop_vec(1u64..100, 20..50)
    ) {
        let dir = tempdir().unwrap();
        let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

        // Initialize two accounts
        db.put(b"account_a", &initial_balance.to_le_bytes()).unwrap();
        db.put(b"account_b", &initial_balance.to_le_bytes()).unwrap();

        let mut successful_transfers = 0;

        // Perform concurrent transfers
        for amount in transfer_amounts {
            // Try to transfer from A to B
            let mut retry_count = 0;
            let success = loop {
                if retry_count > 10 {
                    break false;
                }

                let tx_id = db.begin_transaction().unwrap();

                // Read balances
                let balance_a = db.get_tx(tx_id, b"account_a").unwrap()
                    .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);

                let balance_b = db.get_tx(tx_id, b"account_b").unwrap()
                    .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);

                // Check if transfer is possible
                if balance_a >= amount {
                    // Update balances
                    db.put_tx(tx_id, b"account_a", &(balance_a - amount).to_le_bytes()).unwrap();
                    db.put_tx(tx_id, b"account_b", &(balance_b + amount).to_le_bytes()).unwrap();

                    // Try to commit
                    match db.commit_transaction(tx_id) {
                        Ok(_) => break true,
                        Err(_) => {
                            retry_count += 1;
                            continue;
                        }
                    }
                } else {
                    db.abort_transaction(tx_id).unwrap();
                    break false;
                }
            };

            if success {
                successful_transfers += 1;
            }
        }

        // Verify final consistency
        let final_a = db.get(b"account_a").unwrap()
            .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
            .unwrap_or(0);

        let final_b = db.get(b"account_b").unwrap()
            .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
            .unwrap_or(0);

        // Total money should be conserved
        prop_assert_eq!(final_a + final_b, initial_balance * 2);

        // At least some transfers should succeed
        prop_assert!(successful_transfers > 0);
    }
}

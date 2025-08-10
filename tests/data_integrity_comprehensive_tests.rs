use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test comprehensive data validation framework across all operations
#[test]
fn test_comprehensive_data_validation() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        compression_enabled: true,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Phase 1: Initial data population with checksums
    let num_records = 1000;
    let mut expected_data = HashMap::new();

    for i in 0..num_records {
        let key = format!("validation_test_{:06}", i);
        let base_value = format!("initial_value_{}_", i);
        let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
        let value = format!("{}checksum_{}", base_value, checksum);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        expected_data.insert(key, value);
    }

    // Run initial integrity check
    let initial_verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    assert_eq!(
        initial_verification.checksum_errors.len(),
        0,
        "Should have no checksum errors after initial population"
    );

    // Phase 2: Complex operations while maintaining validation
    
    // Update operations with checksum validation
    for i in 0..200 {
        let key = format!("validation_test_{:06}", i);
        let old_value = expected_data.get(&key).unwrap();
        
        // Verify current data matches expectation
        let current = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(current).unwrap(), *old_value);
        
        // Create new value with updated checksum
        let base_value = format!("updated_value_{}_", i);
        let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
        let new_value = format!("{}checksum_{}", base_value, checksum);
        
        db.put(key.as_bytes(), new_value.as_bytes()).unwrap();
        expected_data.insert(key, new_value);
    }

    // Delete operations with validation
    let mut deleted_keys = HashSet::new();
    for i in 200..300 {
        let key = format!("validation_test_{:06}", i);
        
        // Verify data exists before deletion
        let current = db.get(key.as_bytes()).unwrap();
        assert!(current.is_some(), "Data should exist before deletion");
        
        db.delete(key.as_bytes()).unwrap();
        expected_data.remove(&key);
        deleted_keys.insert(key);
    }

    // Re-insert operations with new checksums
    for i in 300..400 {
        let key = format!("validation_test_{:06}", i);
        
        // Delete first
        db.delete(key.as_bytes()).unwrap();
        expected_data.remove(&key);
        
        // Re-insert with new data
        let base_value = format!("reinserted_value_{}_", i);
        let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
        let new_value = format!("{}checksum_{}", base_value, checksum);
        
        db.put(key.as_bytes(), new_value.as_bytes()).unwrap();
        expected_data.insert(key, new_value);
    }

    // Phase 3: Batch operations with validation
    let batch_size = 50;
    for batch_start in (400..600).step_by(batch_size) {
        let mut batch_operations = Vec::new();
        
        for i in batch_start..std::cmp::min(batch_start + batch_size, 600) {
            let key = format!("validation_test_{:06}", i);
            let base_value = format!("batch_value_{}_{}_", batch_start, i);
            let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
            let new_value = format!("{}checksum_{}", base_value, checksum);
            
            batch_operations.push((key.clone(), new_value.clone()));
        }
        
        // Apply batch operations
        for (key, value) in &batch_operations {
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            expected_data.insert(key.clone(), value.clone());
        }
        
        // Validate batch was applied correctly
        for (key, expected_value) in &batch_operations {
            let current = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(current).unwrap(), *expected_value);
        }
    }

    // Phase 4: Transaction operations with validation
    for tx_batch in 0..20 {
        let tx_id = db.begin_transaction().unwrap();
        let mut tx_operations = Vec::new();
        
        for i in 0..5 {
            let key = format!("validation_test_tx_{}_{:03}", tx_batch, i);
            let base_value = format!("transaction_value_{}_{}_", tx_batch, i);
            let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
            let value = format!("{}checksum_{}", base_value, checksum);
            
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            tx_operations.push((key, value));
        }
        
        // Verify transaction data is visible within transaction
        for (key, expected_value) in &tx_operations {
            let current = db.get_tx(tx_id, key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(current).unwrap(), *expected_value);
        }
        
        db.commit_transaction(tx_id).unwrap();
        
        // Verify committed data is visible outside transaction
        for (key, expected_value) in tx_operations {
            let current = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(current).unwrap(), expected_value);
            expected_data.insert(key, expected_value);
        }
    }

    // Phase 5: Comprehensive validation
    println!("Running comprehensive data validation...");
    
    // Verify all expected data exists and matches
    let mut validation_errors = 0;
    let mut checksum_errors = 0;
    
    for (key, expected_value) in &expected_data {
        match db.get(key.as_bytes()) {
            Ok(Some(actual_value)) => {
                let actual_str = String::from_utf8(actual_value).unwrap();
                if actual_str != *expected_value {
                    validation_errors += 1;
                    println!("Data mismatch for key {}: expected '{}', got '{}'", 
                            key, expected_value, actual_str);
                }
                
                // Validate checksum
                if let Some(checksum_pos) = actual_str.rfind("checksum_") {
                    let (base_data, checksum_part) = actual_str.split_at(checksum_pos);
                    let expected_checksum = xxhash_rust::xxh64::xxh64(base_data.as_bytes(), 0);
                    let stored_checksum: u64 = checksum_part[9..].parse().unwrap_or(0);
                    
                    if expected_checksum != stored_checksum {
                        checksum_errors += 1;
                        println!("Checksum error for key {}: expected {}, got {}", 
                                key, expected_checksum, stored_checksum);
                    }
                }
            }
            Ok(None) => {
                validation_errors += 1;
                println!("Missing expected data for key: {}", key);
            }
            Err(e) => {
                validation_errors += 1;
                println!("Error reading key {}: {:?}", key, e);
            }
        }
    }

    // Verify deleted data doesn't exist
    for key in &deleted_keys {
        match db.get(key.as_bytes()) {
            Ok(Some(_)) => {
                validation_errors += 1;
                println!("Deleted data still exists for key: {}", key);
            }
            Ok(None) => {}, // Expected
            Err(e) => {
                println!("Error checking deleted key {}: {:?}", key, e);
            }
        }
    }

    // Run full database integrity check
    let final_verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    println!("Comprehensive data validation results:");
    println!("  Expected data records: {}", expected_data.len());
    println!("  Deleted data records: {}", deleted_keys.len());
    println!("  Validation errors: {}", validation_errors);
    println!("  Checksum errors: {}", checksum_errors);
    println!("  Database checksum errors: {}", final_verification.checksum_errors.len());
    println!("  Database structure errors: {}", final_verification.structure_errors.len());
    println!("  Database consistency errors: {}", final_verification.consistency_errors.len());

    assert_eq!(validation_errors, 0, "Should have no data validation errors");
    assert_eq!(checksum_errors, 0, "Should have no checksum validation errors");
    assert_eq!(final_verification.checksum_errors.len(), 0, "Should have no database checksum errors");
    assert_eq!(final_verification.structure_errors.len(), 0, "Should have no database structure errors");
    assert_eq!(final_verification.consistency_errors.len(), 0, "Should have no database consistency errors");
}

/// Test cross-component validation consistency
/// Ensures all database components maintain consistent validation states
#[test]
fn test_cross_component_validation_consistency() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        cache_size: 20 * 1024 * 1024, // 20MB cache
        compression_enabled: true,
        prefetch_enabled: true,
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test data across different storage layers
    let test_scenarios = [
        ("btree_data", 1000, "btree"),      // Data that primarily lives in B-tree
        ("lsm_data", 2000, "lsm"),          // Data that goes through LSM
        ("cached_data", 500, "cache"),      // Data that should be cached
        ("wal_data", 300, "wal"),           // Data that exercises WAL
    ];

    let mut all_expected_data = HashMap::new();

    for (prefix, count, component) in &test_scenarios {
        println!("Testing {} component with {} records", component, count);
        
        for i in 0..*count {
            let key = format!("{}_{:06}", prefix, i);
            let base_value = format!("{}_value_{}_comp_{}_", prefix, i, component);
            
            // Add component-specific metadata
            let metadata = format!("meta_{}_{}", component, xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0));
            let full_value = format!("{}{}", base_value, metadata);
            
            db.put(key.as_bytes(), full_value.as_bytes()).unwrap();
            all_expected_data.insert(key, (full_value, component.to_string()));
            
            // Immediate read-back validation
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            let retrieved_str = String::from_utf8(retrieved).unwrap();
            assert_eq!(retrieved_str, all_expected_data.get(&format!("{}_{:06}", prefix, i)).unwrap().0);
        }

        // Component-specific operations
        match *component {
            "btree" => {
                // Test range operations for B-tree
                for start in (0..*count).step_by(100) {
                    let start_key = format!("{}_{:06}", prefix, start);
                    let end_key = format!("{}_{:06}", prefix, std::cmp::min(start + 50, *count - 1));
                    
                    // Note: This is a simplified range check - actual range iteration would be more complex
                    for i in start..std::cmp::min(start + 50, *count) {
                        let key = format!("{}_{:06}", prefix, i);
                        let value = db.get(key.as_bytes()).unwrap();
                        assert!(value.is_some(), "B-tree range data should be accessible");
                    }
                }
            }
            "lsm" => {
                // Force compaction to test LSM consistency
                db.checkpoint().unwrap();
                
                // Verify data after potential compaction
                for i in 0..*count / 2 {
                    let key = format!("{}_{:06}", prefix, i);
                    let value = db.get(key.as_bytes()).unwrap();
                    assert!(value.is_some(), "LSM data should survive compaction");
                }
            }
            "cache" => {
                // Access patterns that should populate cache
                for _ in 0..3 {
                    for i in (0..*count).step_by(10) {
                        let key = format!("{}_{:06}", prefix, i);
                        let _ = db.get(key.as_bytes()).unwrap();
                    }
                }
                
                // Verify cache stats show activity
                if let Some(stats) = db.cache_stats() {
                    assert!(stats.contains("hit") || stats.contains("miss"), "Cache should show activity");
                }
            }
            "wal" => {
                // Test WAL consistency with immediate sync
                for i in (*count / 2)..*count {
                    let key = format!("{}_{:06}", prefix, i);
                    let updated_value = format!("updated_{}", all_expected_data.get(&key).unwrap().0);
                    
                    db.put(key.as_bytes(), updated_value.as_bytes()).unwrap();
                    all_expected_data.insert(key.clone(), (updated_value, component.to_string()));
                    
                    // Immediate verification
                    let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
                    assert_eq!(String::from_utf8(retrieved).unwrap(), all_expected_data.get(&key).unwrap().0);
                }
            }
            _ => {}
        }
    }

    // Cross-component consistency check
    println!("Running cross-component consistency validation...");
    
    // Force all components to sync
    db.checkpoint().unwrap();
    
    // Verify all data across all components
    let mut component_validation_errors = HashMap::new();
    
    for (key, (expected_value, component)) in &all_expected_data {
        match db.get(key.as_bytes()) {
            Ok(Some(actual_value)) => {
                let actual_str = String::from_utf8(actual_value).unwrap();
                if actual_str != *expected_value {
                    let errors = component_validation_errors.entry(component.clone()).or_insert(0);
                    *errors += 1;
                    println!("Cross-component validation error in {} for key {}", component, key);
                }
            }
            Ok(None) => {
                let errors = component_validation_errors.entry(component.clone()).or_insert(0);
                *errors += 1;
                println!("Missing data in {} for key {}", component, key);
            }
            Err(e) => {
                let errors = component_validation_errors.entry(component.clone()).or_insert(0);
                *errors += 1;
                println!("Error reading {} data for key {}: {:?}", component, key, e);
            }
        }
    }

    // Run comprehensive integrity check
    let integrity_result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    println!("Cross-component validation results:");
    for (component, errors) in &component_validation_errors {
        println!("  {} component errors: {}", component, errors);
    }
    println!("  Total records validated: {}", all_expected_data.len());
    println!("  Integrity check errors: {}", 
            integrity_result.checksum_errors.len() +
            integrity_result.structure_errors.len() +
            integrity_result.consistency_errors.len());

    // All components should maintain consistency
    for (component, errors) in component_validation_errors {
        assert_eq!(errors, 0, "Component {} should have no validation errors", component);
    }

    assert_eq!(integrity_result.checksum_errors.len(), 0, "No checksum errors across components");
    assert_eq!(integrity_result.structure_errors.len(), 0, "No structure errors across components");
    assert_eq!(integrity_result.consistency_errors.len(), 0, "No consistency errors across components");
}

/// Test validation under concurrent load with integrity monitoring
#[test]
fn test_concurrent_validation_integrity() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        cache_size: 30 * 1024 * 1024, // 30MB cache
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    
    // Concurrent validation thread
    let db_validator = Arc::clone(&db);
    let validation_running = Arc::new(AtomicBool::new(true));
    let validation_running_clone = Arc::clone(&validation_running);
    let validation_errors = Arc::new(AtomicUsize::new(0));
    let validation_errors_clone = Arc::clone(&validation_errors);

    let validator_handle = thread::spawn(move || {
        while validation_running_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
            
            match tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(db_validator.verify_integrity()) {
                Ok(result) => {
                    let errors = result.checksum_errors.len() +
                                result.structure_errors.len() +
                                result.consistency_errors.len();
                    if errors > 0 {
                        validation_errors_clone.fetch_add(errors, Ordering::Relaxed);
                        println!("Validation found {} errors during concurrent operations", errors);
                    }
                }
                Err(_) => {
                    validation_errors_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });

    // Concurrent data operations
    let num_threads = 8;
    let operations_per_thread = 500;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let operation_successes = Arc::new(AtomicUsize::new(0));
    let operation_failures = Arc::new(AtomicUsize::new(0));
    let data_validation_errors = Arc::new(AtomicUsize::new(0));

    let mut operation_handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let successes_clone = Arc::clone(&operation_successes);
        let failures_clone = Arc::clone(&operation_failures);
        let data_errors_clone = Arc::clone(&data_validation_errors);

        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut thread_expected_data = HashMap::new();

            for i in 0..operations_per_thread {
                let key = format!("concurrent_val_{}_{:06}", thread_id, i);
                let base_value = format!("thread_{}_operation_{}_", thread_id, i);
                let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
                let value = format!("{}checksum_{}", base_value, checksum);

                // Write operation
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        successes_clone.fetch_add(1, Ordering::Relaxed);
                        thread_expected_data.insert(key.clone(), value.clone());
                        
                        // Immediate read-back validation
                        match db_clone.get(key.as_bytes()) {
                            Ok(Some(retrieved)) => {
                                let retrieved_str = String::from_utf8(retrieved).unwrap();
                                if retrieved_str != value {
                                    data_errors_clone.fetch_add(1, Ordering::Relaxed);
                                    println!("Immediate read-back failed for thread {} key {}", thread_id, key);
                                }
                            }
                            Ok(None) => {
                                data_errors_clone.fetch_add(1, Ordering::Relaxed);
                                println!("Immediate read-back missing for thread {} key {}", thread_id, key);
                            }
                            Err(_) => {
                                data_errors_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => failures_clone.fetch_add(1, Ordering::Relaxed),
                }

                // Occasional updates to existing data
                if i > 0 && i % 20 == 0 {
                    let update_index = (i - 20) % std::cmp::max(1, thread_expected_data.len());
                    if let Some((update_key, _)) = thread_expected_data.iter().nth(update_index) {
                        let update_key = update_key.clone();
                        let new_base_value = format!("updated_thread_{}_operation_{}_", thread_id, i);
                        let new_checksum = xxhash_rust::xxh64::xxh64(new_base_value.as_bytes(), 0);
                        let new_value = format!("{}checksum_{}", new_base_value, new_checksum);

                        if db_clone.put(update_key.as_bytes(), new_value.as_bytes()).is_ok() {
                            thread_expected_data.insert(update_key, new_value);
                        }
                    }
                }

                // Occasional deletions
                if i > 50 && i % 50 == 0 {
                    let delete_index = (i - 50) % std::cmp::max(1, thread_expected_data.len());
                    if let Some((delete_key, _)) = thread_expected_data.iter().nth(delete_index) {
                        let delete_key = delete_key.clone();
                        if db_clone.delete(delete_key.as_bytes()).is_ok() {
                            thread_expected_data.remove(&delete_key);
                        }
                    }
                }

                // Periodic validation of thread's data
                if i % 100 == 0 && i > 0 {
                    for (expected_key, expected_value) in &thread_expected_data {
                        match db_clone.get(expected_key.as_bytes()) {
                            Ok(Some(actual)) => {
                                let actual_str = String::from_utf8(actual).unwrap();
                                if actual_str != *expected_value {
                                    data_errors_clone.fetch_add(1, Ordering::Relaxed);
                                }
                                
                                // Validate checksum
                                if let Some(checksum_pos) = actual_str.rfind("checksum_") {
                                    let (base_data, checksum_part) = actual_str.split_at(checksum_pos);
                                    let expected_checksum = xxhash_rust::xxh64::xxh64(base_data.as_bytes(), 0);
                                    let stored_checksum: u64 = checksum_part[9..].parse().unwrap_or(0);
                                    
                                    if expected_checksum != stored_checksum {
                                        data_errors_clone.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                            Ok(None) => {
                                data_errors_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                data_errors_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        });
        operation_handles.push(handle);
    }

    // Let operations run for a while
    thread::sleep(Duration::from_secs(10));

    for handle in operation_handles {
        handle.join().unwrap();
    }

    // Stop validation thread
    validation_running.store(false, Ordering::Relaxed);
    validator_handle.join().unwrap();

    let total_successes = operation_successes.load(Ordering::Relaxed);
    let total_failures = operation_failures.load(Ordering::Relaxed);
    let total_validation_errors = validation_errors.load(Ordering::Relaxed);
    let total_data_errors = data_validation_errors.load(Ordering::Relaxed);

    println!("Concurrent validation integrity test:");
    println!("  Operation successes: {}", total_successes);
    println!("  Operation failures: {}", total_failures);
    println!("  Validation errors during operations: {}", total_validation_errors);
    println!("  Data validation errors: {}", total_data_errors);

    // Final comprehensive validation
    let final_verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    let final_errors = final_verification.checksum_errors.len() +
                      final_verification.structure_errors.len() +
                      final_verification.consistency_errors.len();

    println!("  Final integrity errors: {}", final_errors);

    // Should maintain integrity throughout concurrent operations
    assert_eq!(total_validation_errors, 0, "Should have no validation errors during concurrent operations");
    assert_eq!(total_data_errors, 0, "Should have no data validation errors");
    assert_eq!(final_errors, 0, "Should have no final integrity errors");
    assert!(total_successes > 2000, "Should have many successful operations");
}
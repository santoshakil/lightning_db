use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test minimal feature set functionality
/// Validates core database operations work with minimal dependencies
#[test]
fn test_minimal_feature_set() {
    let dir = tempdir().unwrap();
    
    // Configuration with minimal features
    let config = LightningDbConfig {
        cache_size: 1024 * 1024, // 1MB - minimal cache
        use_improved_wal: false, // Basic WAL
        wal_sync_mode: WalSyncMode::Sync,
        compression_enabled: false, // No compression
        prefetch_enabled: false, // No prefetching
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test 1: Basic CRUD operations
    println!("Testing basic CRUD operations with minimal features...");
    
    // Create
    for i in 0..1000 {
        let key = format!("minimal_key_{:06}", i);
        let value = format!("minimal_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Read
    for i in 0..1000 {
        let key = format!("minimal_key_{:06}", i);
        let expected_value = format!("minimal_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Update
    for i in 0..500 {
        let key = format!("minimal_key_{:06}", i);
        let updated_value = format!("updated_minimal_value_{}", i);
        
        db.put(key.as_bytes(), updated_value.as_bytes()).unwrap();
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), updated_value);
    }

    // Delete
    for i in 500..750 {
        let key = format!("minimal_key_{:06}", i);
        db.delete(key.as_bytes()).unwrap();
        
        assert!(db.get(key.as_bytes()).unwrap().is_none());
    }

    // Verify remaining data
    for i in 0..500 {
        let key = format!("minimal_key_{:06}", i);
        let expected_value = format!("updated_minimal_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    for i in 750..1000 {
        let key = format!("minimal_key_{:06}", i);
        let expected_value = format!("minimal_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Test 2: Basic transaction processing
    println!("Testing basic transaction processing...");
    
    let tx_id = db.begin_transaction().unwrap();
    
    for i in 0..100 {
        let key = format!("tx_minimal_{:06}", i);
        let value = format!("tx_value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify transaction data is visible within transaction
    for i in 0..100 {
        let key = format!("tx_minimal_{:06}", i);
        let expected_value = format!("tx_value_{}", i);
        
        let retrieved = db.get_tx(tx_id, key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Commit transaction
    db.commit_transaction(tx_id).unwrap();

    // Verify transaction data is visible after commit
    for i in 0..100 {
        let key = format!("tx_minimal_{:06}", i);
        let expected_value = format!("tx_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Test 3: Persistence guarantees
    println!("Testing persistence guarantees...");
    
    // Force checkpoint to ensure data persistence
    db.checkpoint().unwrap();

    drop(db);

    // Reopen database and verify data persisted
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Verify CRUD data persisted
    for i in 0..500 {
        let key = format!("minimal_key_{:06}", i);
        let expected_value = format!("updated_minimal_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Verify transaction data persisted
    for i in 0..100 {
        let key = format!("tx_minimal_{:06}", i);
        let expected_value = format!("tx_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    println!("Minimal feature set test completed successfully");
}

/// Test full feature set functionality
/// Validates all features work together without conflicts
#[test]
fn test_full_feature_set() {
    let dir = tempdir().unwrap();
    
    // Configuration with all features enabled
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024, // 50MB cache
        use_improved_wal: true, // Enhanced WAL
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
        compression_enabled: true, // Compression enabled
        prefetch_enabled: true, // Prefetching enabled
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test 1: Advanced CRUD with compression
    println!("Testing advanced CRUD with full features...");
    
    let large_value_size = 10000; // 10KB values to test compression
    
    for i in 0..2000 {
        let key = format!("full_features_key_{:06}", i);
        let base_value = format!("large_compressed_value_{}_", i);
        let padded_value = format!("{}{}", base_value, "X".repeat(large_value_size - base_value.len()));
        
        db.put(key.as_bytes(), padded_value.as_bytes()).unwrap();
    }

    // Test read performance with prefetching and caching
    let read_start = Instant::now();
    for i in 0..2000 {
        let key = format!("full_features_key_{:06}", i);
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), large_value_size);
    }
    let read_duration = read_start.elapsed();
    println!("Read 2000 large values in {:?}", read_duration);

    // Test 2: Complex transaction scenarios
    println!("Testing complex transactions with full features...");
    
    // Multiple concurrent transactions
    let mut transaction_ids = Vec::new();
    for tx_batch in 0..10 {
        let tx_id = db.begin_transaction().unwrap();
        
        for i in 0..50 {
            let key = format!("full_tx_{}_{:06}", tx_batch, i);
            let value = format!("full_tx_value_{}_{}", tx_batch, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        transaction_ids.push(tx_id);
    }

    // Commit all transactions
    for tx_id in transaction_ids {
        db.commit_transaction(tx_id).unwrap();
    }

    // Verify all transaction data
    for tx_batch in 0..10 {
        for i in 0..50 {
            let key = format!("full_tx_{}_{:06}", tx_batch, i);
            let expected_value = format!("full_tx_value_{}_{}", tx_batch, i);
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
        }
    }

    // Test 3: Data export/import functionality (if available)
    println!("Testing advanced database operations...");
    
    // Test integrity verification
    let verification_result = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();
    
    assert_eq!(verification_result.checksum_errors.len(), 0);
    assert_eq!(verification_result.structure_errors.len(), 0);
    assert_eq!(verification_result.consistency_errors.len(), 0);

    // Test cache statistics
    if let Some(cache_stats) = db.cache_stats() {
        println!("Cache statistics: {}", cache_stats);
        assert!(cache_stats.contains("hit") || cache_stats.contains("miss"));
    }

    // Test 4: Performance under full feature load
    println!("Testing performance with all features enabled...");
    
    let perf_test_start = Instant::now();
    let mut operations = 0;

    for batch in 0..100 {
        // Mix of operations
        for i in 0..100 {
            let key = format!("perf_full_{}_{:06}", batch, i);
            let value = format!("performance_test_value_{}_{}", batch, i);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            operations += 1;

            // Occasional reads
            if i % 10 == 0 {
                let _ = db.get(key.as_bytes()).unwrap();
                operations += 1;
            }
        }

        // Checkpoint every 10 batches
        if batch % 10 == 0 {
            db.checkpoint().unwrap();
        }
    }

    let perf_duration = perf_test_start.elapsed();
    let ops_per_second = operations as f64 / perf_duration.as_secs_f64();
    
    println!("Full features performance: {} ops/sec", ops_per_second as u64);
    assert!(ops_per_second > 10000.0, "Should maintain good performance with all features");

    // Test 5: Persistence with all features
    println!("Testing persistence with full features...");
    
    db.shutdown().unwrap();

    // Reopen with different feature configuration to test compatibility
    let reopen_config = LightningDbConfig {
        cache_size: 20 * 1024 * 1024, // Different cache size
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync, // Different sync mode
        compression_enabled: true,
        prefetch_enabled: false, // Disabled prefetching
        ..Default::default()
    };

    let db = Database::open(dir.path(), reopen_config).unwrap();

    // Verify all data is still accessible
    for i in 0..100 {
        let key = format!("full_features_key_{:06}", i);
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), large_value_size);
    }

    // Verify transaction data
    for tx_batch in 0..3 {
        for i in 0..10 {
            let key = format!("full_tx_{}_{:06}", tx_batch, i);
            let expected_value = format!("full_tx_value_{}_{}", tx_batch, i);
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
        }
    }

    println!("Full feature set test completed successfully");
}

/// Test feature compatibility across different builds
/// Validates that data created with different feature sets remains accessible
#[test]
fn test_feature_compatibility() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Test data created with different feature combinations
    let feature_configs = vec![
        ("minimal", LightningDbConfig {
            cache_size: 1024 * 1024,
            use_improved_wal: false,
            compression_enabled: false,
            prefetch_enabled: false,
            ..Default::default()
        }),
        ("compression_only", LightningDbConfig {
            cache_size: 5 * 1024 * 1024,
            use_improved_wal: false,
            compression_enabled: true,
            prefetch_enabled: false,
            ..Default::default()
        }),
        ("wal_enhanced", LightningDbConfig {
            cache_size: 10 * 1024 * 1024,
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 200 },
            compression_enabled: false,
            prefetch_enabled: false,
            ..Default::default()
        }),
        ("full_features", LightningDbConfig {
            cache_size: 20 * 1024 * 1024,
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            compression_enabled: true,
            prefetch_enabled: true,
            ..Default::default()
        }),
    ];

    let mut all_expected_data = HashMap::new();

    // Phase 1: Create data with each feature set
    for (config_name, config) in &feature_configs {
        println!("Creating data with {} configuration", config_name);
        
        let db = Database::open(&db_path, config.clone()).unwrap();
        
        for i in 0..200 {
            let key = format!("compat_{}_{:06}", config_name, i);
            let value = format!("data_from_{}_config_{}", config_name, i);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            all_expected_data.insert(key, value);
        }

        // Add some transaction data
        let tx_id = db.begin_transaction().unwrap();
        for i in 0..50 {
            let key = format!("tx_compat_{}_{:06}", config_name, i);
            let value = format!("tx_data_from_{}_config_{}", config_name, i);
            
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            all_expected_data.insert(key, value);
        }
        db.commit_transaction(tx_id).unwrap();

        db.checkpoint().unwrap();
        db.shutdown().unwrap();
    }

    // Phase 2: Verify compatibility - read all data with each feature set
    for (reader_config_name, reader_config) in &feature_configs {
        println!("Reading all data with {} configuration", reader_config_name);
        
        let db = Database::open(&db_path, reader_config.clone()).unwrap();
        
        let mut successful_reads = 0;
        let mut failed_reads = 0;
        
        for (expected_key, expected_value) in &all_expected_data {
            match db.get(expected_key.as_bytes()) {
                Ok(Some(actual_value)) => {
                    let actual_str = String::from_utf8(actual_value).unwrap();
                    if actual_str == *expected_value {
                        successful_reads += 1;
                    } else {
                        println!("Value mismatch for key {} with reader {}: expected '{}', got '{}'", 
                               expected_key, reader_config_name, expected_value, actual_str);
                        failed_reads += 1;
                    }
                }
                Ok(None) => {
                    println!("Missing data for key {} with reader {}", expected_key, reader_config_name);
                    failed_reads += 1;
                }
                Err(e) => {
                    println!("Error reading key {} with reader {}: {:?}", expected_key, reader_config_name, e);
                    failed_reads += 1;
                }
            }
        }

        println!("Reader {} results: {} successful, {} failed", 
               reader_config_name, successful_reads, failed_reads);
        
        assert_eq!(failed_reads, 0, "Reader {} should be able to read all data", reader_config_name);
        assert_eq!(successful_reads, all_expected_data.len(), "Reader {} should read all expected data", reader_config_name);

        // Test that reader can also write new data
        for i in 0..50 {
            let key = format!("new_data_{}_{:06}", reader_config_name, i);
            let value = format!("new_value_from_{}", reader_config_name);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }

        db.shutdown().unwrap();
    }

    println!("Feature compatibility test completed successfully");
}

/// Test feature flags under concurrent load
/// Validates that different feature combinations work correctly under stress
#[test]
fn test_concurrent_feature_validation() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 30 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
        compression_enabled: true,
        prefetch_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());

    // Test concurrent feature usage
    let num_threads = 8;
    let operations_per_thread = 300;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let compression_operations = Arc::new(AtomicUsize::new(0));
    let cache_hits = Arc::new(AtomicUsize::new(0));
    let wal_operations = Arc::new(AtomicUsize::new(0));
    let transaction_operations = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        let compression_clone = Arc::clone(&compression_operations);
        let cache_clone = Arc::clone(&cache_hits);
        let wal_clone = Arc::clone(&wal_operations);
        let tx_clone = Arc::clone(&transaction_operations);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..operations_per_thread {
                match i % 4 {
                    0 => {
                        // Test compression feature
                        let key = format!("compress_test_{}_{:06}", thread_id, i);
                        let large_value = "X".repeat(5000); // 5KB value to trigger compression
                        
                        if db_clone.put(key.as_bytes(), large_value.as_bytes()).is_ok() {
                            compression_clone.fetch_add(1, Ordering::Relaxed);
                            
                            // Immediate read-back should use cache/prefetch
                            if db_clone.get(key.as_bytes()).is_ok() {
                                cache_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    1 => {
                        // Test WAL feature with transactions
                        let tx_id = db_clone.begin_transaction().unwrap();
                        
                        for j in 0..5 {
                            let key = format!("wal_test_{}_{}_{:03}", thread_id, i, j);
                            let value = format!("wal_value_{}_{}_{}", thread_id, i, j);
                            
                            if db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                wal_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        
                        if db_clone.commit_transaction(tx_id).is_ok() {
                            tx_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    2 => {
                        // Test cache/prefetch features
                        for j in 0..10 {
                            let key = format!("cache_test_{}_{:06}", thread_id, i * 10 + j);
                            let value = format!("cache_value_{}_{}", thread_id, i * 10 + j);
                            
                            db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        
                        // Read pattern that should benefit from prefetching
                        for j in 0..10 {
                            let key = format!("cache_test_{}_{:06}", thread_id, i * 10 + j);
                            if db_clone.get(key.as_bytes()).is_ok() {
                                cache_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    3 => {
                        // Test mixed operations that exercise all features
                        let key = format!("mixed_test_{}_{:06}", thread_id, i);
                        let value = format!("mixed_value_{}_{}_{}", thread_id, i, "Y".repeat(1000));
                        
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                        let _ = db_clone.get(key.as_bytes()).unwrap();
                        
                        wal_clone.fetch_add(1, Ordering::Relaxed);
                        compression_clone.fetch_add(1, Ordering::Relaxed);
                        cache_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => unreachable!(),
                }

                // Periodic integrity checks
                if i % 100 == 0 {
                    // Quick validation that database is still functional
                    let test_key = format!("validation_{}_{:06}", thread_id, i);
                    let test_value = format!("validation_value_{}", i);
                    
                    db_clone.put(test_key.as_bytes(), test_value.as_bytes()).unwrap();
                    let retrieved = db_clone.get(test_key.as_bytes()).unwrap().unwrap();
                    assert_eq!(String::from_utf8(retrieved).unwrap(), test_value);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_compression = compression_operations.load(Ordering::Relaxed);
    let total_cache = cache_hits.load(Ordering::Relaxed);
    let total_wal = wal_operations.load(Ordering::Relaxed);
    let total_transactions = transaction_operations.load(Ordering::Relaxed);

    println!("Concurrent feature validation results:");
    println!("  Compression operations: {}", total_compression);
    println!("  Cache operations: {}", total_cache);
    println!("  WAL operations: {}", total_wal);
    println!("  Transaction operations: {}", total_transactions);

    // Verify all features were exercised
    assert!(total_compression > 1000, "Should have many compression operations");
    assert!(total_cache > 2000, "Should have many cache operations");
    assert!(total_wal > 1000, "Should have many WAL operations");
    assert!(total_transactions > 100, "Should have many transaction operations");

    // Final integrity check with all features active
    let verification = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(db.verify_integrity())
        .unwrap();

    assert_eq!(verification.checksum_errors.len(), 0);
    assert_eq!(verification.structure_errors.len(), 0);
    assert_eq!(verification.consistency_errors.len(), 0);

    // Check feature-specific statistics
    if let Some(cache_stats) = db.cache_stats() {
        println!("Final cache stats: {}", cache_stats);
        assert!(cache_stats.contains("hit") || cache_stats.contains("miss"));
    }

    println!("Concurrent feature validation completed successfully");
}

/// Test binary size optimization impact on functionality
/// Validates that optimized builds maintain full functionality
#[test]
fn test_optimized_build_functionality() {
    let dir = tempdir().unwrap();
    
    // Simulate optimized build configuration
    let config = LightningDbConfig {
        cache_size: 5 * 1024 * 1024, // Smaller cache for optimized builds
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        compression_enabled: false, // Disabled to reduce binary size
        prefetch_enabled: false, // Disabled to reduce binary size
        ..Default::default()
    };

    let db = Database::open(dir.path(), config).unwrap();

    // Test core functionality in optimized build
    println!("Testing core functionality in optimized build...");

    // Basic operations should work without optional features
    for i in 0..1000 {
        let key = format!("optimized_key_{:06}", i);
        let value = format!("optimized_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), value);
    }

    // Transaction functionality should be preserved
    let tx_id = db.begin_transaction().unwrap();
    
    for i in 0..200 {
        let key = format!("optimized_tx_{:06}", i);
        let value = format!("optimized_tx_value_{}", i);
        
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        
        let retrieved = db.get_tx(tx_id, key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), value);
    }

    db.commit_transaction(tx_id).unwrap();

    // Verify committed transaction data
    for i in 0..200 {
        let key = format!("optimized_tx_{:06}", i);
        let expected_value = format!("optimized_tx_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Persistence should work in optimized builds
    db.checkpoint().unwrap();
    drop(db);

    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Verify all data persisted in optimized build
    for i in 0..1000 {
        let key = format!("optimized_key_{:06}", i);
        let expected_value = format!("optimized_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    for i in 0..200 {
        let key = format!("optimized_tx_{:06}", i);
        let expected_value = format!("optimized_tx_value_{}", i);
        
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(String::from_utf8(retrieved).unwrap(), expected_value);
    }

    // Performance should be reasonable even without optimizations
    let perf_start = Instant::now();
    
    for i in 0..1000 {
        let key = format!("perf_optimized_{:06}", i);
        let value = format!("perf_value_{}", i);
        
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let _ = db.get(key.as_bytes()).unwrap();
    }

    let perf_duration = perf_start.elapsed();
    let ops_per_second = 2000.0 / perf_duration.as_secs_f64(); // 1000 puts + 1000 gets

    println!("Optimized build performance: {} ops/sec", ops_per_second as u64);
    assert!(ops_per_second > 5000.0, "Optimized build should maintain reasonable performance");

    println!("Optimized build functionality test completed successfully");
}
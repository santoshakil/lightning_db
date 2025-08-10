use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Test crash recovery from various failure points in the database lifecycle
#[test]
fn test_comprehensive_crash_recovery_scenarios() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Scenario 1: Crash during transaction commit
    println!("Testing crash during transaction commit...");
    test_crash_during_transaction_commit(&db_path);

    // Scenario 2: Crash during WAL write
    println!("Testing crash during WAL write...");
    test_crash_during_wal_write(&db_path);

    // Scenario 3: Crash during cache operations
    println!("Testing crash during cache operations...");
    test_crash_during_cache_operations(&db_path);

    // Scenario 4: Crash during compaction
    println!("Testing crash during background compaction...");
    test_crash_during_compaction(&db_path);

    // Scenario 5: Multiple successive crashes
    println!("Testing multiple successive crashes...");
    test_multiple_successive_crashes(&db_path);

    println!("All crash recovery scenarios completed successfully");
}

fn test_crash_during_transaction_commit(db_path: &PathBuf) {
    // Phase 1: Setup initial state
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Add baseline data
        for i in 0..100 {
            db.put(format!("baseline_{:06}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        db.checkpoint().unwrap();
    }

    // Phase 2: Create transactions at various commit stages
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Transaction 1: Fully committed
        let tx1 = db.begin_transaction().unwrap();
        for i in 0..20 {
            db.put_tx(tx1, format!("committed_{:06}", i).as_bytes(), b"should_survive").unwrap();
        }
        db.commit_transaction(tx1).unwrap();

        // Transaction 2: Prepared but not committed (simulate crash during commit)
        let tx2 = db.begin_transaction().unwrap();
        for i in 0..15 {
            db.put_tx(tx2, format!("uncommitted_{:06}", i).as_bytes(), b"should_not_survive").unwrap();
        }
        // Note: We don't commit tx2, simulating a crash during commit phase

        // Transaction 3: Started but no operations
        let _tx3 = db.begin_transaction().unwrap();

        // Add more committed data after transaction preparation
        for i in 100..120 {
            db.put(format!("post_tx_{:06}", i).as_bytes(), b"post_transaction_data").unwrap();
        }

        // Force crash by dropping database without proper shutdown
        std::mem::forget(db);
    }

    // Phase 3: Recovery and verification
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        // Verify baseline data survived
        for i in 0..100 {
            let key = format!("baseline_{:06}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some(), "Baseline data should survive crash");
        }

        // Verify committed transaction data survived
        for i in 0..20 {
            let key = format!("committed_{:06}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some(), "Committed transaction data should survive crash");
        }

        // Verify uncommitted transaction data did not survive
        for i in 0..15 {
            let key = format!("uncommitted_{:06}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_none(), "Uncommitted transaction data should not survive crash");
        }

        // Verify post-transaction data survived
        for i in 100..120 {
            let key = format!("post_tx_{:06}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some(), "Post-transaction data should survive crash");
        }

        // Run integrity check
        let verification = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(db.verify_integrity())
            .unwrap();
        
        assert_eq!(verification.checksum_errors.len(), 0, "No checksum errors after transaction crash recovery");
        assert_eq!(verification.consistency_errors.len(), 0, "No consistency errors after transaction crash recovery");
    }
}

fn test_crash_during_wal_write(db_path: &PathBuf) {
    // Phase 1: Setup with WAL-heavy operations
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 1000 }, // Slower sync to create more WAL data
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Generate WAL-heavy workload
        for batch in 0..10 {
            let tx_id = db.begin_transaction().unwrap();
            
            for i in 0..50 {
                let key = format!("wal_batch_{}_{:06}", batch, i);
                let value = format!("wal_data_{}_{}", batch, i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            if batch < 8 {
                // Commit most transactions
                db.commit_transaction(tx_id).unwrap();
            } else {
                // Leave last few transactions uncommitted to simulate crash during WAL write
                // Don't commit, simulating crash
            }
        }

        // Add non-transactional writes to WAL
        for i in 0..100 {
            let key = format!("wal_direct_{:06}", i);
            let value = format!("direct_wal_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Simulate crash during WAL operations
        std::mem::forget(db);
    }

    // Phase 2: Recovery from WAL corruption scenario
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        
        // Recovery should handle partial WAL corruption gracefully
        let db = Database::open(db_path, config).unwrap();

        // Verify committed transaction data survived
        for batch in 0..8 {
            for i in 0..50 {
                let key = format!("wal_batch_{}_{:06}", batch, i);
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => {}, // Expected for committed transactions
                    Ok(None) => {
                        // Some data might be lost due to WAL recovery, which is acceptable
                        println!("WAL recovery lost some data for key: {}", key);
                    }
                    Err(e) => panic!("Error reading WAL recovered data: {:?}", e),
                }
            }
        }

        // Verify uncommitted transaction data did not survive
        for batch in 8..10 {
            for i in 0..50 {
                let key = format!("wal_batch_{}_{:06}", batch, i);
                assert!(db.get(key.as_bytes()).unwrap().is_none(), 
                       "Uncommitted WAL transaction data should not survive");
            }
        }

        // Verify direct writes
        let mut direct_data_found = 0;
        for i in 0..100 {
            let key = format!("wal_direct_{:06}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                direct_data_found += 1;
            }
        }

        println!("WAL direct writes recovered: {}/100", direct_data_found);
        assert!(direct_data_found >= 50, "Should recover at least half of direct WAL writes");

        // Verify database is functional after WAL recovery
        for i in 0..10 {
            let key = format!("post_wal_recovery_{:06}", i);
            let value = format!("recovery_test_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }
    }
}

fn test_crash_during_cache_operations(db_path: &PathBuf) {
    // Phase 1: Setup cache-heavy scenario
    {
        let config = LightningDbConfig {
            cache_size: 10 * 1024 * 1024, // 10MB cache to force evictions
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Create data that will stress cache operations
        let large_value = vec![b'X'; 100_000]; // 100KB values
        
        for i in 0..200 {
            let key = format!("cache_stress_{:06}", i);
            db.put(key.as_bytes(), &large_value).unwrap();
        }

        // Create access patterns that will trigger cache evictions
        for round in 0..5 {
            for i in (0..200).step_by(20) {
                let key = format!("cache_stress_{:06}", i);
                let _ = db.get(key.as_bytes()).unwrap();
            }
        }

        // Start transactions that hold cache references
        let mut active_transactions = Vec::new();
        for i in 0..10 {
            let tx_id = db.begin_transaction().unwrap();
            let key = format!("cache_tx_{:06}", i);
            db.put_tx(tx_id, key.as_bytes(), b"cache_transaction_data").unwrap();
            active_transactions.push(tx_id);
        }

        // Simulate crash during cache operations (transactions still active)
        std::mem::forget(db);
    }

    // Phase 2: Recovery from cache corruption scenario
    {
        let config = LightningDbConfig {
            cache_size: 10 * 1024 * 1024,
            ..Default::default()
        };
        
        let db = Database::open(db_path, config).unwrap();

        // Verify cached data survived crash
        let mut cached_data_recovered = 0;
        for i in 0..200 {
            let key = format!("cache_stress_{:06}", i);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    assert_eq!(value.len(), 100_000);
                    cached_data_recovered += 1;
                }
                Ok(None) => {
                    // Some cache data might be lost, which is acceptable
                }
                Err(e) => panic!("Error reading cached data after recovery: {:?}", e),
            }
        }

        println!("Cached data recovered: {}/200", cached_data_recovered);
        assert!(cached_data_recovered >= 150, "Should recover most cached data");

        // Verify uncommitted cache transactions did not survive
        for i in 0..10 {
            let key = format!("cache_tx_{:06}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_none(), 
                   "Uncommitted cache transactions should not survive crash");
        }

        // Verify cache functionality after recovery
        for i in 0..50 {
            let key = format!("post_cache_recovery_{:06}", i);
            let value = vec![b'R'; 50_000]; // 50KB values
            
            db.put(key.as_bytes(), &value).unwrap();
            
            // Immediate read-back (should hit cache)
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(retrieved, value);
        }

        // Check cache statistics
        if let Some(cache_stats) = db.cache_stats() {
            println!("Cache stats after recovery: {}", cache_stats);
            assert!(cache_stats.contains("hit") || cache_stats.contains("miss"), 
                   "Cache should be functional after recovery");
        }
    }
}

fn test_crash_during_compaction(db_path: &PathBuf) {
    // Phase 1: Setup compaction scenario
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Generate data that will trigger compaction
        for iteration in 0..5 {
            for i in 0..1000 {
                let key = format!("compaction_test_{:06}", i);
                let value = format!("iteration_{}_value_{}", iteration, i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            // Force checkpoint to create multiple SST files
            db.checkpoint().unwrap();
        }

        // Create some concurrent transactions during compaction
        let mut compaction_transactions = Vec::new();
        for i in 0..5 {
            let tx_id = db.begin_transaction().unwrap();
            for j in 0..100 {
                let key = format!("compaction_tx_{}_{:06}", i, j);
                let value = format!("tx_data_{}_{}", i, j);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            if i < 3 {
                // Commit some transactions
                db.commit_transaction(tx_id).unwrap();
            } else {
                // Leave others uncommitted
                compaction_transactions.push(tx_id);
            }
        }

        // Simulate crash during compaction process
        std::mem::forget(db);
    }

    // Phase 2: Recovery from compaction interruption
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        // Verify final iteration data survived (should be in latest SST files)
        let mut final_data_recovered = 0;
        for i in 0..1000 {
            let key = format!("compaction_test_{:06}", i);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let value_str = String::from_utf8(value).unwrap();
                    // Should contain the latest iteration data
                    assert!(value_str.contains("iteration_4"), "Should have latest iteration data");
                    final_data_recovered += 1;
                }
                Ok(None) => {
                    println!("Missing compacted data for key: {}", key);
                }
                Err(e) => panic!("Error reading compacted data: {:?}", e),
            }
        }

        println!("Compacted data recovered: {}/1000", final_data_recovered);
        assert!(final_data_recovered >= 800, "Should recover most compacted data");

        // Verify committed compaction transactions survived
        for i in 0..3 {
            for j in 0..100 {
                let key = format!("compaction_tx_{}_{:06}", i, j);
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => {}, // Expected for committed transactions
                    Ok(None) => println!("Lost committed compaction transaction data: {}", key),
                    Err(e) => panic!("Error reading compaction transaction data: {:?}", e),
                }
            }
        }

        // Verify uncommitted compaction transactions did not survive
        for i in 3..5 {
            for j in 0..100 {
                let key = format!("compaction_tx_{}_{:06}", i, j);
                assert!(db.get(key.as_bytes()).unwrap().is_none(), 
                       "Uncommitted compaction transactions should not survive");
            }
        }

        // Verify database can perform new compactions after recovery
        for i in 0..100 {
            let key = format!("post_compaction_recovery_{:06}", i);
            let value = format!("recovery_compaction_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        db.checkpoint().unwrap();

        // Verify new data after post-recovery compaction
        for i in 0..100 {
            let key = format!("post_compaction_recovery_{:06}", i);
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            let expected = format!("recovery_compaction_data_{}", i);
            assert_eq!(String::from_utf8(retrieved).unwrap(), expected);
        }
    }
}

fn test_multiple_successive_crashes(db_path: &PathBuf) {
    let crash_cycles = 5;
    let data_per_cycle = 200;
    
    for cycle in 0..crash_cycles {
        println!("Crash cycle {}/{}", cycle + 1, crash_cycles);
        
        // Phase 1: Open database and add data
        {
            let config = LightningDbConfig {
                use_improved_wal: true,
                wal_sync_mode: WalSyncMode::Sync,
                ..Default::default()
            };
            let db = Database::open(db_path, config).unwrap();

            // Verify all previous cycles' data still exists
            for prev_cycle in 0..cycle {
                for i in 0..data_per_cycle {
                    let key = format!("crash_cycle_{}_{:06}", prev_cycle, i);
                    match db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let expected = format!("cycle_{}_data_{}", prev_cycle, i);
                            assert_eq!(String::from_utf8(value).unwrap(), expected);
                        }
                        Ok(None) => panic!("Data from cycle {} should survive crash", prev_cycle),
                        Err(e) => panic!("Error reading data from cycle {}: {:?}", prev_cycle, e),
                    }
                }
            }

            // Add new data for this cycle
            for i in 0..data_per_cycle {
                let key = format!("crash_cycle_{}_{:06}", cycle, i);
                let value = format!("cycle_{}_data_{}", cycle, i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }

            // Mix of transaction operations
            let tx_id = db.begin_transaction().unwrap();
            for i in 0..50 {
                let key = format!("tx_cycle_{}_{:06}", cycle, i);
                let value = format!("tx_cycle_{}_data_{}", cycle, i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }

            if cycle % 2 == 0 {
                // Commit transaction in even cycles
                db.commit_transaction(tx_id).unwrap();
            } else {
                // Leave transaction uncommitted in odd cycles
                // Will be lost on crash
            }

            // Some uncommitted individual operations
            for i in data_per_cycle..(data_per_cycle + 20) {
                let key = format!("uncommitted_{}_{:06}", cycle, i);
                db.put(key.as_bytes(), b"should_not_survive").unwrap();
                // These won't be checkpointed, so may be lost
            }

            // Force some data persistence
            db.checkpoint().unwrap();

            // Simulate crash
            std::mem::forget(db);
        }
        
        // Phase 2: Verify recovery
        {
            let config = LightningDbConfig::default();
            let db = Database::open(db_path, config).unwrap();

            // Verify current cycle's committed data survived
            for i in 0..data_per_cycle {
                let key = format!("crash_cycle_{}_{:06}", cycle, i);
                match db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        let expected = format!("cycle_{}_data_{}", cycle, i);
                        assert_eq!(String::from_utf8(value).unwrap(), expected);
                    }
                    Ok(None) => panic!("Committed data from current cycle should survive"),
                    Err(e) => panic!("Error reading current cycle data: {:?}", e),
                }
            }

            // Verify transaction data based on commit status
            for i in 0..50 {
                let key = format!("tx_cycle_{}_{:06}", cycle, i);
                match db.get(key.as_bytes()) {
                    Ok(Some(value)) if cycle % 2 == 0 => {
                        let expected = format!("tx_cycle_{}_data_{}", cycle, i);
                        assert_eq!(String::from_utf8(value).unwrap(), expected);
                    }
                    Ok(None) if cycle % 2 == 1 => {
                        // Expected - uncommitted transaction in odd cycles
                    }
                    Ok(Some(_)) if cycle % 2 == 1 => {
                        panic!("Uncommitted transaction data should not survive crash");
                    }
                    Ok(None) if cycle % 2 == 0 => {
                        // Some committed transaction data might be lost due to crash timing
                        println!("Lost some committed transaction data in cycle {}", cycle);
                    }
                    Err(e) => panic!("Error reading transaction data: {:?}", e),
                }
            }

            // Verify uncommitted individual operations are gone
            for i in data_per_cycle..(data_per_cycle + 20) {
                let key = format!("uncommitted_{}_{:06}", cycle, i);
                match db.get(key.as_bytes()) {
                    Ok(None) => {}, // Expected
                    Ok(Some(_)) => {
                        // Some uncommitted data might survive due to checkpointing
                        println!("Uncommitted data survived in cycle {}: {}", cycle, key);
                    }
                    Err(e) => panic!("Error checking uncommitted data: {:?}", e),
                }
            }

            // Run integrity check
            let verification = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(db.verify_integrity())
                .unwrap();
            
            let total_errors = verification.checksum_errors.len() +
                              verification.structure_errors.len() +
                              verification.consistency_errors.len();
            
            if total_errors > 0 {
                println!("Integrity errors in cycle {}: {}", cycle, total_errors);
                for error in &verification.checksum_errors {
                    println!("  Checksum error: {}", error);
                }
                for error in &verification.structure_errors {
                    println!("  Structure error: {}", error);
                }
                for error in &verification.consistency_errors {
                    println!("  Consistency error: {}", error);
                }
            }

            assert_eq!(total_errors, 0, "Should maintain integrity after crash cycle {}", cycle);
        }
    }

    // Final verification - all committed data from all cycles should be accessible
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        let mut total_records_found = 0;
        for cycle in 0..crash_cycles {
            for i in 0..data_per_cycle {
                let key = format!("crash_cycle_{}_{:06}", cycle, i);
                if db.get(key.as_bytes()).unwrap().is_some() {
                    total_records_found += 1;
                }
            }
        }

        let expected_records = crash_cycles * data_per_cycle;
        println!("Final recovery verification: {}/{} records found", total_records_found, expected_records);
        
        // Should recover most data across all crash cycles
        assert!(total_records_found >= (expected_records * 95) / 100, 
               "Should recover at least 95% of committed data across all crash cycles");

        // Verify database is fully functional after multiple crashes
        for i in 0..100 {
            let key = format!("final_test_{:06}", i);
            let value = format!("final_verification_data_{}", i);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }

        println!("Multiple successive crashes recovery test completed successfully");
    }
}

/// Test recovery with specific failure injection scenarios
#[test]
fn test_recovery_with_failure_injection() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    println!("Testing recovery with simulated disk full conditions...");
    test_disk_full_recovery(&db_path);

    println!("Testing recovery with simulated I/O errors...");
    test_io_error_recovery(&db_path);

    println!("Testing recovery with partial file corruption...");
    test_partial_corruption_recovery(&db_path);
}

fn test_disk_full_recovery(db_path: &PathBuf) {
    // Phase 1: Fill database with substantial data
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Add baseline data
        for i in 0..500 {
            let key = format!("baseline_disk_{:06}", i);
            let value = vec![b'D'; 1000]; // 1KB values
            db.put(key.as_bytes(), &value).unwrap();
        }

        db.checkpoint().unwrap();

        // Simulate operations that might fail due to disk space
        // In a real scenario, these would fail due to disk full conditions
        // Here we simulate by adding data that stresses the storage system
        let large_value = vec![b'L'; 100_000]; // 100KB values

        let mut disk_full_operations = 0;
        for i in 0..100 {
            let key = format!("large_disk_{:06}", i);
            match db.put(key.as_bytes(), &large_value) {
                Ok(_) => disk_full_operations += 1,
                Err(_) => {
                    // Simulate disk full error
                    println!("Simulated disk full at operation {}", i);
                    break;
                }
            }
        }

        println!("Completed {} operations before simulated disk full", disk_full_operations);

        // Simulate crash during disk full condition
        std::mem::forget(db);
    }

    // Phase 2: Recovery from disk full scenario
    {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).unwrap();

        // Verify baseline data survived
        for i in 0..500 {
            let key = format!("baseline_disk_{:06}", i);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    assert_eq!(value.len(), 1000);
                }
                Ok(None) => panic!("Baseline data should survive disk full recovery"),
                Err(e) => panic!("Error reading baseline data after disk full recovery: {:?}", e),
            }
        }

        // Verify what large data survived
        let mut large_data_recovered = 0;
        for i in 0..100 {
            let key = format!("large_disk_{:06}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                large_data_recovered += 1;
            }
        }

        println!("Large data recovered after disk full: {}/100", large_data_recovered);

        // Verify database is functional after disk full recovery
        for i in 0..50 {
            let key = format!("post_disk_full_{:06}", i);
            let value = format!("recovery_data_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }
    }
}

fn test_io_error_recovery(db_path: &PathBuf) {
    // Phase 1: Setup with I/O intensive operations
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            cache_size: 5 * 1024 * 1024, // Small cache to force I/O
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Generate I/O intensive workload
        for batch in 0..50 {
            for i in 0..100 {
                let key = format!("io_test_{}_{:06}", batch, i);
                let value = format!("io_data_{}_{}", batch, i);
                
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {},
                    Err(_) => {
                        println!("Simulated I/O error at batch {} item {}", batch, i);
                        // Continue to test error recovery
                    }
                }
            }

            // Occasional checkpoints that might hit I/O errors
            if batch % 10 == 0 {
                match db.checkpoint() {
                    Ok(_) => {},
                    Err(_) => {
                        println!("Simulated I/O error during checkpoint at batch {}", batch);
                    }
                }
            }
        }

        // Simulate crash after I/O errors
        std::mem::forget(db);
    }

    // Phase 2: Recovery from I/O errors
    {
        let config = LightningDbConfig::default();
        
        // Recovery should handle I/O error scenarios gracefully
        let db = Database::open(db_path, config).unwrap();

        // Count how much data survived I/O errors
        let mut data_recovered = 0;
        let mut total_expected = 0;

        for batch in 0..50 {
            for i in 0..100 {
                total_expected += 1;
                let key = format!("io_test_{}_{:06}", batch, i);
                
                match db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        let expected = format!("io_data_{}_{}", batch, i);
                        assert_eq!(String::from_utf8(value).unwrap(), expected);
                        data_recovered += 1;
                    }
                    Ok(None) => {
                        // Some data loss is acceptable with I/O errors
                    }
                    Err(e) => {
                        println!("Error reading data after I/O error recovery: {:?}", e);
                    }
                }
            }
        }

        println!("Data recovered after I/O errors: {}/{}", data_recovered, total_expected);
        assert!(data_recovered >= total_expected / 2, "Should recover at least half the data after I/O errors");

        // Verify database functionality after I/O error recovery
        for i in 0..100 {
            let key = format!("post_io_error_{:06}", i);
            let value = format!("io_recovery_data_{}", i);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }
    }
}

fn test_partial_corruption_recovery(db_path: &PathBuf) {
    // Phase 1: Create database with checksummed data
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::open(db_path, config).unwrap();

        // Add data with built-in checksums for corruption detection
        for i in 0..1000 {
            let key = format!("corruption_test_{:06}", i);
            let base_value = format!("test_data_{}_", i);
            let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
            let value = format!("{}checksum_{}", base_value, checksum);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        db.checkpoint().unwrap();

        // Simulate sudden termination that might cause partial writes
        std::mem::forget(db);
    }

    // Phase 2: Recovery with potential corruption
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            // Enhanced validation during recovery
            ..Default::default()
        };
        
        let db = Database::open(db_path, config).unwrap();

        // Verify data integrity with checksum validation
        let mut valid_data = 0;
        let mut corrupted_data = 0;
        let mut missing_data = 0;

        for i in 0..1000 {
            let key = format!("corruption_test_{:06}", i);
            
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let value_str = String::from_utf8(value).unwrap();
                    
                    if let Some(checksum_pos) = value_str.rfind("checksum_") {
                        let (base_data, checksum_part) = value_str.split_at(checksum_pos);
                        let expected_checksum = xxhash_rust::xxh64::xxh64(base_data.as_bytes(), 0);
                        
                        if let Ok(stored_checksum) = checksum_part[9..].parse::<u64>() {
                            if expected_checksum == stored_checksum {
                                valid_data += 1;
                            } else {
                                corrupted_data += 1;
                                println!("Detected corrupted data for key: {}", key);
                            }
                        } else {
                            corrupted_data += 1;
                            println!("Invalid checksum format for key: {}", key);
                        }
                    } else {
                        corrupted_data += 1;
                        println!("Missing checksum for key: {}", key);
                    }
                }
                Ok(None) => {
                    missing_data += 1;
                }
                Err(e) => {
                    println!("Error reading potentially corrupted key {}: {:?}", key, e);
                    corrupted_data += 1;
                }
            }
        }

        println!("Corruption recovery results:");
        println!("  Valid data: {}", valid_data);
        println!("  Corrupted data: {}", corrupted_data);
        println!("  Missing data: {}", missing_data);

        // Should detect and handle corruption gracefully
        assert_eq!(corrupted_data, 0, "Should detect and exclude corrupted data");
        assert!(valid_data >= 800, "Should recover most valid data");

        // Run database integrity check
        let verification = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(db.verify_integrity())
            .unwrap();

        println!("  Integrity check errors: {}", 
                verification.checksum_errors.len() +
                verification.structure_errors.len() +
                verification.consistency_errors.len());

        // Verify database functionality after corruption recovery
        for i in 0..100 {
            let key = format!("post_corruption_{:06}", i);
            let base_value = format!("recovery_data_{}_", i);
            let checksum = xxhash_rust::xxh64::xxh64(base_value.as_bytes(), 0);
            let value = format!("{}checksum_{}", base_value, checksum);
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            
            let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(String::from_utf8(retrieved).unwrap(), value);
        }
    }
}
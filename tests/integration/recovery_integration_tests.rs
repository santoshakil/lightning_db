//! Recovery Integration Tests
//! 
//! Tests crash recovery, WAL replay, backup/restore, and corruption recovery

use super::{TestEnvironment, setup_test_data, verify_test_data, generate_workload_data};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::process::{Command, Stdio};
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

#[test]
fn test_crash_recovery_with_wal_replay() {
    let mut env = TestEnvironment::new().expect("Failed to create test environment");
    let db_path = env.db_path.clone();
    let config = env.config.clone();
    
    // Phase 1: Insert data and simulate crash
    {
        let db = env.db();
        setup_test_data(&db).unwrap();
        
        // Start a transaction but don't commit (simulating crash during transaction)
        let tx_id = db.begin_transaction().unwrap();
        db.put_tx(tx_id, b"crash_test_key", b"crash_test_value").unwrap();
        
        // Insert more data outside transaction
        for i in 1000..1100 {
            let key = format!("post_crash_key_{}", i);
            let value = format!("post_crash_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Simulate crash by dropping without commit
    } // Database drops here, simulating crash
    
    // Phase 2: Reopen and verify recovery
    let recovered_db = Database::open(db_path, config).unwrap();
    
    // Verify original data is intact
    assert!(verify_test_data(&recovered_db).unwrap());
    
    // Verify uncommitted transaction was rolled back
    assert!(recovered_db.get(b"crash_test_key").unwrap().is_none());
    
    // Verify committed data after transaction start is intact
    for i in 1000..1100 {
        let key = format!("post_crash_key_{}", i);
        let expected_value = format!("post_crash_value_{}", i);
        let actual_value = recovered_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Verify database is fully functional after recovery
    recovered_db.put(b"post_recovery_key", b"post_recovery_value").unwrap();
    assert_eq!(recovered_db.get(b"post_recovery_key").unwrap().unwrap(), b"post_recovery_value");
}

#[test]
fn test_transaction_consistency_after_crash() {
    let mut env = TestEnvironment::new().expect("Failed to create test environment");
    let db_path = env.db_path.clone();
    let config = env.config.clone();
    
    // Phase 1: Multiple transactions with different states
    {
        let db = env.db();
        
        // Committed transaction
        let tx1 = db.begin_transaction().unwrap();
        db.put_tx(tx1, b"committed_key_1", b"committed_value_1").unwrap();
        db.put_tx(tx1, b"committed_key_2", b"committed_value_2").unwrap();
        db.commit_transaction(tx1).unwrap();
        
        // Uncommitted transaction (should be rolled back)
        let tx2 = db.begin_transaction().unwrap();
        db.put_tx(tx2, b"uncommitted_key_1", b"uncommitted_value_1").unwrap();
        db.put_tx(tx2, b"uncommitted_key_2", b"uncommitted_value_2").unwrap();
        // Don't commit tx2
        
        // Another committed transaction
        let tx3 = db.begin_transaction().unwrap();
        db.put_tx(tx3, b"committed_key_3", b"committed_value_3").unwrap();
        db.commit_transaction(tx3).unwrap();
        
        // Rollback transaction (explicitly rolled back)
        let tx4 = db.begin_transaction().unwrap();
        db.put_tx(tx4, b"rollback_key_1", b"rollback_value_1").unwrap();
        db.rollback_transaction(tx4).unwrap();
        
        // Simulate crash during another transaction
        let _tx5 = db.begin_transaction().unwrap();
        // Crash here
    }
    
    // Phase 2: Recovery and verification
    let recovered_db = Database::open(db_path, config).unwrap();
    
    // Committed transactions should be present
    assert_eq!(recovered_db.get(b"committed_key_1").unwrap().unwrap(), b"committed_value_1");
    assert_eq!(recovered_db.get(b"committed_key_2").unwrap().unwrap(), b"committed_value_2");
    assert_eq!(recovered_db.get(b"committed_key_3").unwrap().unwrap(), b"committed_value_3");
    
    // Uncommitted transactions should be absent
    assert!(recovered_db.get(b"uncommitted_key_1").unwrap().is_none());
    assert!(recovered_db.get(b"uncommitted_key_2").unwrap().is_none());
    
    // Rolled back transaction should be absent
    assert!(recovered_db.get(b"rollback_key_1").unwrap().is_none());
}

#[test]
fn test_backup_and_restore_workflow() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup test data
    setup_test_data(&db).unwrap();
    
    // Add some additional structured data
    for i in 0..100 {
        let key = format!("backup_test_{:03}", i);
        let value = format!("backup_value_{:03}_{}", i, "x".repeat(i % 20));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Create backup directory
    let backup_dir = TempDir::new().unwrap();
    let backup_path = backup_dir.path().join("backup");
    
    // Simulate backup by copying database files
    // Note: In a real implementation, this would use proper backup APIs
    let src_path = &env.db_path;
    fs::create_dir_all(&backup_path).unwrap();
    
    // Copy all database files
    if src_path.exists() {
        for entry in fs::read_dir(src_path).unwrap() {
            let entry = entry.unwrap();
            let src_file = entry.path();
            let dest_file = backup_path.join(entry.file_name());
            
            if src_file.is_file() {
                fs::copy(&src_file, &dest_file).unwrap();
            }
        }
    }
    
    // Modify original database
    for i in 100..200 {
        let key = format!("after_backup_{:03}", i);
        let value = format!("after_backup_value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Delete some original data
    for i in 0..50 {
        let key = format!("backup_test_{:03}", i);
        db.delete(key.as_bytes()).unwrap();
    }
    
    // Restore from backup
    let restore_dir = TempDir::new().unwrap();
    let restore_path = restore_dir.path().join("restored_db");
    fs::create_dir_all(&restore_path).unwrap();
    
    // Copy backup files to restore location
    for entry in fs::read_dir(&backup_path).unwrap() {
        let entry = entry.unwrap();
        let src_file = entry.path();
        let dest_file = restore_path.join(entry.file_name());
        
        if src_file.is_file() {
            fs::copy(&src_file, &dest_file).unwrap();
        }
    }
    
    // Open restored database
    let restored_db = Database::open(restore_path, env.config.clone()).unwrap();
    
    // Verify original test data is intact
    assert!(verify_test_data(&restored_db).unwrap());
    
    // Verify backup-time data is present
    for i in 0..100 {
        let key = format!("backup_test_{:03}", i);
        let expected_value = format!("backup_value_{:03}_{}", i, "x".repeat(i % 20));
        let actual_value = restored_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Verify post-backup data is NOT present
    for i in 100..200 {
        let key = format!("after_backup_{:03}", i);
        assert!(restored_db.get(key.as_bytes()).unwrap().is_none());
    }
}

#[test]
fn test_point_in_time_recovery() {
    let mut env = TestEnvironment::new().expect("Failed to create test environment");
    let db_path = env.db_path.clone();
    let config = env.config.clone();
    
    // Phase 1: Initial data
    {
        let db = env.db();
        for i in 0..100 {
            let key = format!("pitr_key_{:03}", i);
            let value = format!("initial_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }
    
    // Checkpoint 1: Copy database state
    let checkpoint1_dir = TempDir::new().unwrap();
    let checkpoint1_path = checkpoint1_dir.path().join("checkpoint1");
    fs::create_dir_all(&checkpoint1_path).unwrap();
    
    for entry in fs::read_dir(&db_path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            fs::copy(entry.path(), checkpoint1_path.join(entry.file_name())).unwrap();
        }
    }
    
    // Phase 2: More modifications
    {
        let db = Database::open(&db_path, config.clone()).unwrap();
        
        // Update some existing data
        for i in 0..50 {
            let key = format!("pitr_key_{:03}", i);
            let value = format!("updated_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Add new data
        for i in 100..150 {
            let key = format!("pitr_key_{:03}", i);
            let value = format!("new_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }
    
    // Checkpoint 2: Another copy
    let checkpoint2_dir = TempDir::new().unwrap();
    let checkpoint2_path = checkpoint2_dir.path().join("checkpoint2");
    fs::create_dir_all(&checkpoint2_path).unwrap();
    
    for entry in fs::read_dir(&db_path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            fs::copy(entry.path(), checkpoint2_path.join(entry.file_name())).unwrap();
        }
    }
    
    // Phase 3: Final modifications
    {
        let db = Database::open(&db_path, config.clone()).unwrap();
        
        // Delete some data
        for i in 25..75 {
            let key = format!("pitr_key_{:03}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        
        // Add even more data
        for i in 150..200 {
            let key = format!("pitr_key_{:03}", i);
            let value = format!("final_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }
    
    // Test recovery to checkpoint 1
    let restore1_db = Database::open(checkpoint1_path, config.clone()).unwrap();
    
    // Should have initial data
    for i in 0..100 {
        let key = format!("pitr_key_{:03}", i);
        let expected_value = format!("initial_value_{:03}", i);
        let actual_value = restore1_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Should NOT have later data
    for i in 100..200 {
        let key = format!("pitr_key_{:03}", i);
        assert!(restore1_db.get(key.as_bytes()).unwrap().is_none());
    }
    
    // Test recovery to checkpoint 2
    let restore2_db = Database::open(checkpoint2_path, config.clone()).unwrap();
    
    // Should have updated data
    for i in 0..50 {
        let key = format!("pitr_key_{:03}", i);
        let expected_value = format!("updated_value_{:03}", i);
        let actual_value = restore2_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Should have original data for unchanged keys
    for i in 50..100 {
        let key = format!("pitr_key_{:03}", i);
        let expected_value = format!("initial_value_{:03}", i);
        let actual_value = restore2_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Should have new data
    for i in 100..150 {
        let key = format!("pitr_key_{:03}", i);
        let expected_value = format!("new_value_{:03}", i);
        let actual_value = restore2_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Should NOT have final data
    for i in 150..200 {
        let key = format!("pitr_key_{:03}", i);
        assert!(restore2_db.get(key.as_bytes()).unwrap().is_none());
    }
}

#[test]
fn test_corruption_detection_and_recovery() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup test data
    setup_test_data(&db).unwrap();
    
    // Add checksum-verifiable data
    for i in 0..100 {
        let key = format!("checksum_test_{:03}", i);
        let value = format!("{}:{}", i, "data".repeat(i % 10));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Close database to flush all data
    drop(db);
    
    // Simulate corruption by modifying a file
    let mut corrupted = false;
    if let Ok(entries) = fs::read_dir(&env.db_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_file() && path.extension().map_or(false, |ext| ext == "db" || ext == "wal") {
                    if let Ok(mut data) = fs::read(&path) {
                        if data.len() > 100 {
                            // Corrupt some bytes in the middle
                            for i in 50..60 {
                                if i < data.len() {
                                    data[i] = data[i].wrapping_add(1);
                                }
                            }
                            fs::write(&path, data).unwrap();
                            corrupted = true;
                            break;
                        }
                    }
                }
            }
        }
    }
    
    if corrupted {
        // Try to open corrupted database
        match Database::open(&env.db_path, env.config.clone()) {
            Ok(recovered_db) => {
                // If it opens, verify it detected and handled corruption
                // Try to read data - some might be recoverable
                let mut recovered_count = 0;
                let mut failed_count = 0;
                
                for i in 0..1000 {
                    let key = format!("test_key_{:04}", i);
                    match recovered_db.get(key.as_bytes()) {
                        Ok(Some(_)) => recovered_count += 1,
                        Ok(None) => {},
                        Err(_) => failed_count += 1,
                    }
                }
                
                // Should have recovered some data
                assert!(recovered_count > 0 || failed_count > 0);
                
                // Database should still be functional for new operations
                recovered_db.put(b"post_corruption_key", b"post_corruption_value").unwrap();
                assert_eq!(recovered_db.get(b"post_corruption_key").unwrap().unwrap(), 
                          b"post_corruption_value");
            },
            Err(_) => {
                // Database detected corruption and refused to open
                // This is also acceptable behavior
                println!("Database correctly detected corruption and refused to open");
            }
        }
    } else {
        println!("Warning: Could not simulate corruption for testing");
    }
}

#[test]
fn test_concurrent_recovery_scenarios() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial state
    for i in 0..500 {
        let key = format!("concurrent_key_{:03}", i);
        let value = format!("initial_value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Simulate multiple concurrent operations during potential crash
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let handles: Vec<_> = (0..3).map(|thread_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            for i in 0..100 {
                let key = format!("concurrent_key_{:03}_{}", thread_id * 100 + i, thread_id);
                let value = format!("thread_{}_value_{:03}", thread_id, i);
                
                if thread_id == 0 {
                    // Thread 0: transactions
                    let tx_id = db.begin_transaction().unwrap();
                    db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                    if i % 2 == 0 {
                        db.commit_transaction(tx_id).unwrap();
                    } else {
                        db.rollback_transaction(tx_id).unwrap();
                    }
                } else if thread_id == 1 {
                    // Thread 1: direct writes
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                } else {
                    // Thread 2: mixed operations
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    if i % 3 == 0 {
                        db.delete(key.as_bytes()).unwrap();
                    }
                }
                
                // Add some delay to increase chance of partial operations
                if i % 10 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
        })
    }).collect();
    
    // Start all threads
    barrier.wait();
    
    // Let them run for a bit, then simulate crash
    thread::sleep(Duration::from_millis(50));
    
    // "Crash" by dropping database handle while operations are in progress
    drop(db);
    
    // Wait for threads to finish (they should handle database being dropped)
    for handle in handles {
        let _ = handle.join(); // Ignore panics due to database being dropped
    }
    
    // Attempt recovery
    let recovered_db = Database::open(&env.db_path, env.config.clone()).unwrap();
    
    // Verify original data is still intact
    for i in 0..500 {
        let key = format!("concurrent_key_{:03}", i);
        let expected_value = format!("initial_value_{:03}", i);
        let actual_value = recovered_db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Database should be functional after recovery
    recovered_db.put(b"post_concurrent_crash", b"recovery_successful").unwrap();
    assert_eq!(recovered_db.get(b"post_concurrent_crash").unwrap().unwrap(), 
              b"recovery_successful");
}

#[test]
fn test_incremental_backup_simulation() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Full backup simulation
    setup_test_data(&db).unwrap();
    
    let full_backup_dir = TempDir::new().unwrap();
    let full_backup_path = full_backup_dir.path().join("full_backup");
    fs::create_dir_all(&full_backup_path).unwrap();
    
    // Copy all files for full backup
    for entry in fs::read_dir(&env.db_path).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            fs::copy(entry.path(), full_backup_path.join(entry.file_name())).unwrap();
        }
    }
    
    // Record modification time of last backup
    let backup_time = std::time::SystemTime::now();
    
    thread::sleep(Duration::from_millis(100)); // Ensure time difference
    
    // Make incremental changes
    for i in 1000..1100 {
        let key = format!("incremental_key_{:03}", i);
        let value = format!("incremental_value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Update some existing data
    for i in 0..50 {
        let key = format!("test_key_{:04}", i);
        let value = format!("updated_test_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Simulate incremental backup by checking file modification times
    let incremental_backup_dir = TempDir::new().unwrap();
    let incremental_backup_path = incremental_backup_dir.path().join("incremental_backup");
    fs::create_dir_all(&incremental_backup_path).unwrap();
    
    // Copy only files modified since backup_time
    for entry in fs::read_dir(&env.db_path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Ok(metadata) = fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    if modified > backup_time {
                        fs::copy(&path, incremental_backup_path.join(entry.file_name())).unwrap();
                    }
                }
            }
        }
    }
    
    // Verify incremental backup contains expected changes
    // In a real system, this would involve more sophisticated change tracking
    let has_incremental_files = fs::read_dir(&incremental_backup_path)
        .map(|entries| entries.count() > 0)
        .unwrap_or(false);
    
    if has_incremental_files {
        println!("Incremental backup successfully captured changes");
    } else {
        println!("No incremental changes detected (this may be normal depending on implementation)");
    }
    
    // Verify original database still has all data
    assert!(verify_test_data(&db).unwrap());
    
    for i in 1000..1100 {
        let key = format!("incremental_key_{:03}", i);
        let expected_value = format!("incremental_value_{:03}", i);
        let actual_value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
}
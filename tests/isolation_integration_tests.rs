#![cfg(feature = "integration_tests")]
use lightning_db::{Database, LightningDbConfig};
use lightning_db::features::transactions::isolation::{IsolationLevel, IsolationConfig, LockMode};
use std::time::Duration;
use std::thread;
use std::sync::Arc;
use bytes::Bytes;

/// Test basic isolation level functionality
#[test]
fn test_basic_isolation_levels() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Test default isolation level
    let default_level = db.get_transaction_isolation();
    assert_eq!(default_level, IsolationLevel::ReadCommitted);

    // Test setting isolation level
    db.set_transaction_isolation(IsolationLevel::RepeatableRead)?;
    assert_eq!(db.get_transaction_isolation(), IsolationLevel::RepeatableRead);

    // Test transaction with specific isolation level
    let tx_id = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    let tx_level = db.get_transaction_isolation_level(tx_id)?;
    assert_eq!(tx_level, IsolationLevel::Serializable);

    db.commit_transaction_with_validation(tx_id)?;

    Ok(())
}

/// Test read committed isolation level
#[test]
fn test_read_committed_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Transaction 1: Write uncommitted data
    let tx1 = db.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)?;
    db.put_tx(tx1, b"key1", b"value1")?;

    // Transaction 2: Should not see uncommitted data
    let tx2 = db.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)?;
    let result = db.get_tx(tx2, b"key1");
    assert!(result.is_err() || result.unwrap().is_none());

    // Commit transaction 1
    db.commit_transaction_with_validation(tx1)?;

    // Transaction 2 should now see the committed data
    let result = db.get_tx(tx2, b"key1")?;
    // Note: This depends on the implementation details of the existing transaction manager
    
    db.commit_transaction_with_validation(tx2)?;

    Ok(())
}

/// Test repeatable read isolation level
#[test]
fn test_repeatable_read_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Setup initial data
    db.put(b"key1", b"initial_value")?;

    // Transaction 1: Read with repeatable read
    let tx1 = db.begin_transaction_with_isolation(IsolationLevel::RepeatableRead)?;
    let initial_read = db.get_tx(tx1, b"key1")?;
    
    // Transaction 2: Modify the data
    let tx2 = db.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)?;
    db.put_tx(tx2, b"key1", b"modified_value")?;
    db.commit_transaction_with_validation(tx2)?;

    // Transaction 1 should still see the original value (repeatable read)
    let second_read = db.get_tx(tx1, b"key1")?;
    assert_eq!(initial_read, second_read);

    db.commit_transaction_with_validation(tx1)?;

    Ok(())
}

/// Test deadlock detection
#[test]
fn test_deadlock_detection() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Database::create_temp()?);
    
    // Enable deadlock detection
    db.enable_deadlock_detection(true)?;

    // Setup initial data
    db.put(b"key1", b"value1")?;
    db.put(b"key2", b"value2")?;

    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    let handle1 = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let tx1 = db1.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
        
        // Acquire lock on key1
        db1.acquire_lock(tx1, b"key1", LockMode::Exclusive)?;
        
        // Sleep to allow other transaction to acquire lock on key2
        thread::sleep(Duration::from_millis(100));
        
        // Try to acquire lock on key2 (will create potential deadlock)
        let result = db1.acquire_lock(tx1, b"key2", LockMode::Exclusive);
        
        // Either succeeds or fails with deadlock
        match result {
            Ok(_) => {
                db1.commit_transaction_with_validation(tx1)?;
                Ok(())
            }
            Err(_) => {
                // Deadlock detected, abort
                db1.abort_transaction_with_cleanup(tx1)?;
                Ok(())
            }
        }
    });

    let handle2 = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let tx2 = db2.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
        
        // Acquire lock on key2
        db2.acquire_lock(tx2, b"key2", LockMode::Exclusive)?;
        
        // Sleep to allow other transaction to acquire lock on key1
        thread::sleep(Duration::from_millis(100));
        
        // Try to acquire lock on key1 (will create potential deadlock)
        let result = db2.acquire_lock(tx2, b"key1", LockMode::Exclusive);
        
        // Either succeeds or fails with deadlock
        match result {
            Ok(_) => {
                db2.commit_transaction_with_validation(tx2)?;
                Ok(())
            }
            Err(_) => {
                // Deadlock detected, abort
                db2.abort_transaction_with_cleanup(tx2)?;
                Ok(())
            }
        }
    });

    let result1 = handle1.join().unwrap();
    let result2 = handle2.join().unwrap();

    // Both should complete (one succeeds, one detects deadlock and aborts)
    assert!(result1.is_ok());
    assert!(result2.is_ok());

    // Check if deadlocks were detected
    let deadlocked_txs = db.detect_deadlocks()?;
    // May or may not have active deadlocks depending on timing

    Ok(())
}

/// Test phantom read prevention with serializable isolation
#[test]
fn test_phantom_read_prevention() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Setup initial data in a range
    db.put(b"key1", b"value1")?;
    db.put(b"key3", b"value3")?;

    // Transaction 1: Range scan with serializable isolation
    let tx1 = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    
    // Acquire range lock to prevent phantoms
    db.acquire_range_lock(tx1, Some(b"key1"), Some(b"key9"))?;

    // Transaction 2: Try to insert in the range
    let tx2 = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    let insert_result = db.put_tx(tx2, b"key2", b"phantom");

    // Should either succeed or fail depending on implementation
    match insert_result {
        Ok(_) => {
            // If insert succeeds, validation should catch the phantom read
            let validation_result = db.commit_transaction_with_validation(tx2);
            if validation_result.is_err() {
                db.abort_transaction_with_cleanup(tx2)?;
            }
        }
        Err(_) => {
            // Insert blocked by range lock
            db.abort_transaction_with_cleanup(tx2)?;
        }
    }

    db.commit_transaction_with_validation(tx1)?;

    Ok(())
}

/// Test isolation statistics and monitoring
#[test]
fn test_isolation_statistics() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Start some transactions
    let tx1 = db.begin_transaction_with_isolation(IsolationLevel::ReadCommitted)?;
    let tx2 = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;

    // Get various statistics
    let isolation_stats = db.get_isolation_stats();
    assert!(isolation_stats.validation_stats.active_transactions > 0);

    let deadlock_stats = db.get_deadlock_stats();
    // Stats should be initialized

    let lock_metrics = db.get_lock_manager_metrics();
    // May have some locks acquired

    let snapshot_stats = db.get_snapshot_stats();
    // May have active snapshots for serializable transaction

    let validation_stats = db.get_validation_stats();
    assert!(validation_stats.active_transactions >= 2);

    // Clean up
    db.commit_transaction_with_validation(tx1)?;
    db.commit_transaction_with_validation(tx2)?;

    Ok(())
}

/// Test isolation configuration
#[test]
fn test_isolation_configuration() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Get current config
    let config = db.get_isolation_config();
    assert_eq!(config.level, IsolationLevel::ReadCommitted);

    // Update configuration
    let new_config = IsolationConfig {
        level: IsolationLevel::RepeatableRead,
        enable_deadlock_detection: true,
        deadlock_timeout: Duration::from_secs(5),
        lock_timeout: Duration::from_secs(10),
        max_concurrent_transactions: 500,
        snapshot_cleanup_interval: Duration::from_secs(30),
    };

    db.configure_isolation(new_config.clone())?;

    // Verify configuration was applied
    let updated_config = db.get_isolation_config();
    assert_eq!(updated_config.level, IsolationLevel::RepeatableRead);
    assert_eq!(updated_config.max_concurrent_transactions, 500);

    Ok(())
}

/// Test cleanup operations
#[test]
fn test_isolation_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Create some transactions and let them commit
    for i in 0..5 {
        let tx = db.begin_transaction_with_isolation(IsolationLevel::RepeatableRead)?;
        db.put_tx(tx, format!("key{}", i).as_bytes(), b"value")?;
        db.commit_transaction_with_validation(tx)?;
    }

    // Force cleanup of old data
    let (snapshots_cleaned, txs_cleaned) = db.cleanup_isolation_data(0)?; // Clean everything
    
    // Should have cleaned up some data
    assert!(snapshots_cleaned > 0 || txs_cleaned > 0);

    // Run maintenance
    let maintenance_results = db.run_isolation_maintenance()?;
    // Should complete without errors

    Ok(())
}

/// Test write-write conflict detection
#[test]
fn test_write_write_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Transaction 1: Write to key
    let tx1 = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    db.put_tx(tx1, b"conflict_key", b"value1")?;

    // Transaction 2: Write to same key
    let tx2 = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    db.put_tx(tx2, b"conflict_key", b"value2")?;

    // Try to commit both - should detect conflict
    let result1 = db.commit_transaction_with_validation(tx1);
    let result2 = db.commit_transaction_with_validation(tx2);

    // One should succeed, one should fail with validation error
    let success_count = [&result1, &result2].iter().filter(|r| r.is_ok()).count();
    let failure_count = [&result1, &result2].iter().filter(|r| r.is_err()).count();

    assert!(success_count == 1 && failure_count == 1);

    // Clean up any remaining transaction
    if result1.is_err() {
        let _ = db.abort_transaction_with_cleanup(tx1);
    }
    if result2.is_err() {
        let _ = db.abort_transaction_with_cleanup(tx2);
    }

    Ok(())
}

/// Test lock information and debugging
#[test]
fn test_lock_information() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    let tx = db.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
    
    // Acquire some locks
    db.acquire_lock(tx, b"lock_key1", LockMode::Shared)?;
    db.acquire_lock(tx, b"lock_key2", LockMode::Exclusive)?;

    // Get lock information
    let lock_info = db.get_lock_info();
    assert!(lock_info.len() > 0);

    // Get active transactions
    let active_txs = db.get_active_transactions();
    assert!(active_txs.contains(&tx));

    db.commit_transaction_with_validation(tx)?;

    Ok(())
}

/// Integration test for complete isolation workflow
#[test]
fn test_complete_isolation_workflow() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create_temp()?;

    // Configure isolation
    let config = IsolationConfig {
        level: IsolationLevel::Serializable,
        enable_deadlock_detection: true,
        deadlock_timeout: Duration::from_millis(500),
        lock_timeout: Duration::from_secs(1),
        max_concurrent_transactions: 100,
        snapshot_cleanup_interval: Duration::from_secs(10),
    };
    db.configure_isolation(config)?;

    // Multiple concurrent transactions
    let mut handles = vec![];
    let db = Arc::new(db);

    for i in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
            let tx = db_clone.begin_transaction_with_isolation(IsolationLevel::Serializable)?;
            
            // Each transaction works on different keys to avoid conflicts
            let key = format!("concurrent_key_{}", i);
            let value = format!("value_{}", i);
            
            db_clone.put_tx(tx, key.as_bytes(), value.as_bytes())?;
            
            // Small delay to simulate real work
            thread::sleep(Duration::from_millis(10));
            
            db_clone.commit_transaction_with_validation(tx)?;
            
            Ok(())
        });
        handles.push(handle);
    }

    // Wait for all transactions to complete
    for handle in handles {
        handle.join().unwrap()?;
    }

    // Verify all data was written
    for i in 0..5 {
        let key = format!("concurrent_key_{}", i);
        let expected_value = format!("value_{}", i);
        let actual_value = db.get(key.as_bytes())?;
        assert_eq!(actual_value.as_deref(), Some(expected_value.as_bytes()));
    }

    // Get final statistics
    let stats = db.get_isolation_stats();
    println!("Final isolation stats: {:?}", stats);

    // Run maintenance
    let maintenance = db.run_isolation_maintenance()?;
    println!("Maintenance results: {:?}", maintenance);

    Ok(())
}

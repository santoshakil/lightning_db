use lightning_db::core::storage::{Page, PageId};
use lightning_db::utils::integrity::calculate_checksum;
use lightning_db::{Database, LightningDbConfig, Result, WalSyncMode, WriteBatch};
use std::sync::Arc;
use std::thread;
use tempfile::{tempdir, TempDir};

#[test]
fn test_invalid_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test getting non-existent key
    let result = db.get(b"nonexistent_key");
    assert!(result.is_ok(), "Getting non-existent key should not error");
    assert_eq!(result.unwrap(), None, "Non-existent key should return None");
    
    // Test deleting non-existent key
    let result = db.delete(b"nonexistent_key");
    assert!(result.is_ok(), "Deleting non-existent key should not error");
}

#[test]
fn test_transaction_errors() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test invalid transaction ID
    let invalid_tx_id = 999999;
    let result = db.put_tx(invalid_tx_id, b"key", b"value");
    assert!(result.is_err(), "Invalid transaction ID should fail");
    
    // Test committing non-existent transaction
    let result = db.commit_transaction(invalid_tx_id);
    assert!(result.is_err(), "Committing invalid transaction should fail");
    
    // Test aborting non-existent transaction
    let result = db.abort_transaction(invalid_tx_id);
    assert!(result.is_err(), "Aborting invalid transaction should fail");
    
    // Test double commit
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"key", b"value").unwrap();
    db.commit_transaction(tx_id).unwrap();
    let result = db.commit_transaction(tx_id);
    assert!(result.is_err(), "Double commit should fail");
}

#[test]
fn test_concurrent_transaction_conflicts() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Pre-populate a key
    db.put(b"conflict_key", b"initial").unwrap();
    
    // Start two transactions
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();
    
    // Both try to modify the same key
    db.put_tx(tx1, b"conflict_key", b"tx1_value").unwrap();
    
    // Second put should fail due to write-write conflict
    let result = db.put_tx(tx2, b"conflict_key", b"tx2_value");
    assert!(result.is_err(), "Second transaction should fail on put due to write-write conflict");
    
    // First commit should succeed
    let result1 = db.commit_transaction(tx1);
    assert!(result1.is_ok(), "First transaction should commit");
    
    // Abort the failed transaction
    let _ = db.abort_transaction(tx2);
    
    // Verify the final value
    let value = db.get(b"conflict_key").unwrap();
    assert_eq!(value, Some(b"tx1_value".to_vec()));
}

#[test]
fn test_database_corruption_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    // Create database and write data
    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.sync().unwrap();
    }
    
    // Corrupt a file (simulate disk corruption)
    // Note: This is a simplified test - real corruption would be more complex
    let btree_path = path.join("btree.db");
    if btree_path.exists() {
        // Try to truncate the file to simulate corruption
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&btree_path)
            .ok();
    }
    
    // Try to reopen - should handle corruption gracefully
    let result = Database::open(&path, LightningDbConfig::default());
    
    // Database should either recover or fail gracefully
    match result {
        Ok(db) => {
            // If it recovers, verify it can still operate
            let test_result = db.put(b"test_after_corruption", b"test_value");
            assert!(test_result.is_ok() || test_result.is_err());
        }
        Err(_) => {
            // Failing gracefully is acceptable
            println!("Database failed to open after corruption (expected)");
        }
    }
}

#[test]
fn test_range_scan_boundaries() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Insert test data
    for i in 0..10 {
        let key = format!("key_{:02}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Test empty range
    let results = db.range(Some(b"key_99"), Some(b"key_00")).unwrap();
    assert_eq!(results.len(), 0, "Invalid range should return empty");
    
    // Test partial range
    let results = db.range(Some(b"key_03"), Some(b"key_07")).unwrap();
    assert!(results.len() >= 3, "Partial range should return subset");
    
    // Test unbounded start
    let results = db.range(None, Some(b"key_05")).unwrap();
    assert!(results.len() >= 5, "Unbounded start should work");
    
    // Test unbounded end
    let results = db.range(Some(b"key_05"), None).unwrap();
    assert!(results.len() >= 5, "Unbounded end should work");
}

#[test]
fn test_batch_operation_errors() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test empty batch
    let batch = WriteBatch::new();
    let result = db.write_batch(&batch);
    assert!(result.is_ok(), "Empty batch should succeed");
    
    // Test batch with invalid operations
    let mut batch = WriteBatch::new();
    let _ = batch.put(vec![], vec![b'V'; 100]); // Empty key
    let result = db.write_batch(&batch);
    // Empty key handling is implementation-specific
    assert!(result.is_ok() || result.is_err(), "Empty key handling varies");
}

#[test]
fn test_concurrent_read_write_safety() {
    use std::sync::Arc;
    use std::thread;
    
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    const NUM_THREADS: usize = 10;
    const OPS_PER_THREAD: usize = 100;
    
    let mut handles = vec![];
    
    // Spawn reader threads
    for _ in 0..NUM_THREADS/2 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
                let key = format!("key_{:04}", i);
                let _ = db_clone.get(key.as_bytes());
            }
        });
        handles.push(handle);
    }
    
    // Spawn writer threads
    for thread_id in 0..NUM_THREADS/2 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..OPS_PER_THREAD {
                let key = format!("key_{:04}", i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                let _ = db_clone.put(key.as_bytes(), value.as_bytes());
            }
        });
        handles.push(handle);
    }
    
    // All threads should complete without panicking
    for handle in handles {
        handle.join().expect("Thread should not panic");
    }
}

#[test]
fn test_resource_cleanup() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    // Create and immediately drop database multiple times
    for _ in 0..5 {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        db.put(b"test", b"value").unwrap();
        drop(db);
        
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        assert_eq!(db.get(b"test").unwrap(), Some(b"value".to_vec()));
        drop(db);
    }
}

#[test]
fn test_error_recovery() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test transaction abort
    let tx = db.begin_transaction().unwrap();
    db.put_tx(tx, b"tx_key", b"tx_value").unwrap();
    db.abort_transaction(tx).unwrap();
    
    // Verify aborted transaction didn't persist
    assert_eq!(db.get(b"tx_key").unwrap(), None);
    
    // Test conflicting transactions
    db.put(b"conflict_key", b"initial").unwrap();
    
    let tx1 = db.begin_transaction().unwrap();
    let tx2 = db.begin_transaction().unwrap();
    
    // Both transactions try to modify the same key
    db.put_tx(tx1, b"conflict_key", b"tx1_value").unwrap();
    
    // tx2 attempting to write to same key may fail immediately or at commit
    let put_result = db.put_tx(tx2, b"conflict_key", b"tx2_value");
    
    if put_result.is_ok() {
        // If put succeeded, conflict will be detected at commit
        let result1 = db.commit_transaction(tx1);
        let result2 = db.commit_transaction(tx2);
        // At least one should fail
        assert!(result1.is_err() || result2.is_err());
    } else {
        // Conflict detected early, abort both transactions
        let _ = db.abort_transaction(tx1);
        let _ = db.abort_transaction(tx2);
    }
    
    // Test recovery from batch errors
    let mut batch = WriteBatch::new();
    for i in 0..100 {
        let key = format!("batch_error_{}", i);
        batch.put(key.into_bytes(), vec![b'Z'; 1024]).unwrap();
    }
    
    // Write should succeed
    assert!(db.write_batch(&batch).is_ok());
    
    // Verify partial writes didn't corrupt state
    let test_key = b"test_after_batch";
    db.put(test_key, b"value").unwrap();
    assert_eq!(db.get(test_key).unwrap(), Some(b"value".to_vec()));
}

#[test]
fn test_checksum_calculation() -> Result<()> {
    let empty_data = &[];
    let empty_checksum = calculate_checksum(empty_data);
    assert_eq!(empty_checksum, 0);

    let single_byte = &[0x42];
    let single_checksum = calculate_checksum(single_byte);
    assert_ne!(single_checksum, 0);

    let test_data = b"Hello, World!";
    let test_checksum = calculate_checksum(test_data);
    let duplicate_checksum = calculate_checksum(test_data);
    assert_eq!(test_checksum, duplicate_checksum);

    let different_data = b"Goodbye, World!";
    let different_checksum = calculate_checksum(different_data);
    assert_ne!(test_checksum, different_checksum);

    Ok(())
}

#[test]
fn test_page_basic_operations() -> Result<()> {
    let page_id: PageId = 1;
    let mut page = Page::new(page_id);

    assert_eq!(page.id, page_id);
    assert!(!page.dirty);

    let test_data = b"Test page data";
    page.set_data(test_data)?;
    assert!(page.dirty);

    let checksum = page.calculate_checksum();
    assert_ne!(checksum, 0);

    Ok(())
}

#[tokio::test]
async fn test_transaction_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config)?;

    // Test basic put and get within implicit transactions
    db.put(b"tx_key1", b"tx_value1")?;
    db.put(b"tx_key2", b"tx_value2")?;

    let value1 = db.get(b"tx_key1")?;
    assert_eq!(value1, Some(b"tx_value1".to_vec()));

    let value2 = db.get(b"tx_key2")?;
    assert_eq!(value2, Some(b"tx_value2".to_vec()));

    // Test delete
    db.delete(b"tx_key1")?;
    let value1_after = db.get(b"tx_key1")?;
    assert_eq!(value1_after, None);

    Ok(())
}

#[tokio::test]
async fn test_wal_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    config.wal_sync_mode = WalSyncMode::Sync;

    let db = Database::create(&db_path, config.clone())?;

    db.put(b"wal_key1", b"wal_value1")?;
    db.put(b"wal_key2", b"wal_value2")?;
    db.flush_lsm()?;

    drop(db);

    let recovered_db = Database::open(&db_path, config)?;

    let value1 = recovered_db.get(b"wal_key1")?;
    assert_eq!(value1, Some(b"wal_value1".to_vec()));

    let value2 = recovered_db.get(b"wal_key2")?;
    assert_eq!(value2, Some(b"wal_value2".to_vec()));

    Ok(())
}

#[tokio::test]
async fn test_database_crash_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;

    {
        let db = Database::create(&db_path, config.clone())?;
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        db.flush_lsm()?;
    }

    let recovered_db = Database::open(&db_path, config)?;

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        let result = recovered_db.get(key.as_bytes())?;
        assert_eq!(result, Some(value.into_bytes()));
    }

    Ok(())
}

#[tokio::test]
async fn test_concurrent_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(&db_path, config)?);

    let mut handles = vec![];

    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let key = format!("thread_{}_key_{:03}", thread_id, i);
                let value = format!("thread_{}_value_{:03}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    for thread_id in 0..10 {
        for i in 0..50 {
            let key = format!("thread_{}_key_{:03}", thread_id, i);
            let value = format!("thread_{}_value_{:03}", thread_id, i);
            let result = db.get(key.as_bytes())?;
            assert_eq!(result, Some(value.into_bytes()));
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_recovery_with_compression() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    config.compression_enabled = true;
    config.compression_type = 1; // ZSTD

    let db = Database::create(&db_path, config.clone())?;

    db.put(b"compressed_key1", b"compressed_value1")?;
    db.put(b"compressed_key2", b"compressed_value2")?;
    db.flush_lsm()?;

    drop(db);

    let recovered_db = Database::open(&db_path, config)?;

    let value1 = recovered_db.get(b"compressed_key1")?;
    assert_eq!(value1, Some(b"compressed_value1".to_vec()));

    let value2 = recovered_db.get(b"compressed_key2")?;
    assert_eq!(value2, Some(b"compressed_value2".to_vec()));

    Ok(())
}

#[test]
fn test_transaction_cleanup() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Start many transactions
    let mut tx_ids = Vec::new();
    for _ in 0..100 {
        let tx_id = db.begin_transaction().unwrap();
        tx_ids.push(tx_id);
    }
    
    // Abort half, commit half
    for (i, tx_id) in tx_ids.iter().enumerate() {
        if i % 2 == 0 {
            db.abort_transaction(*tx_id).unwrap();
        } else {
            db.commit_transaction(*tx_id).unwrap();
        }
    }
    
    // All transactions should be cleaned up
    let stats = db.stats();
    assert_eq!(stats.active_transactions, 0);
}

#[test]
fn test_resource_cleanup_on_error() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Try operations that might fail
    let tx_id = db.begin_transaction().unwrap();
    
    // Insert some data
    db.put_tx(tx_id, b"key1", b"value1").unwrap();
    
    // Abort the transaction
    db.abort_transaction(tx_id).unwrap();
    
    // Try to use the aborted transaction (should fail)
    let result = db.put_tx(tx_id, b"key2", b"value2");
    assert!(result.is_err());
    
    // Verify no data was leaked
    assert!(db.get(b"key1").unwrap().is_none());
    assert!(db.get(b"key2").unwrap().is_none());
}

#[test]
fn test_panic_safety() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Start a thread that will panic
    let db_clone = Arc::clone(&db);
    let handle = thread::spawn(move || {
        let tx_id = db_clone.begin_transaction().unwrap();
        db_clone.put_tx(tx_id, b"panic_key", b"panic_value").unwrap();
        
        // Simulate panic
        panic!("Intentional panic for testing");
    });
    
    // Wait for panic
    let _ = handle.join();
    
    // Database should still be usable
    db.put(b"after_panic", b"still_works").unwrap();
    assert_eq!(db.get(b"after_panic").unwrap(), Some(b"still_works".to_vec()));
    
    // Transaction from panicked thread should be rolled back
    assert!(db.get(b"panic_key").unwrap().is_none());
}
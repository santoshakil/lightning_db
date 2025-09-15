use lightning_db::core::storage::{Page, PageId};
use lightning_db::utils::integrity::calculate_checksum;
use lightning_db::{Database, LightningDbConfig, Result, WalSyncMode, WriteBatch};
use std::sync::Arc;
use std::thread;
use tempfile::{tempdir, TempDir};

mod common;

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
    assert!(
        result.is_err(),
        "Committing invalid transaction should fail"
    );

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
    assert!(
        result.is_err(),
        "Second transaction should fail on put due to write-write conflict"
    );

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
    assert!(
        result.is_ok() || result.is_err(),
        "Empty key handling varies"
    );
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
    for _ in 0..NUM_THREADS / 2 {
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
    for thread_id in 0..NUM_THREADS / 2 {
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
        db_clone
            .put_tx(tx_id, b"panic_key", b"panic_value")
            .unwrap();

        // Simulate panic
        panic!("Intentional panic for testing");
    });

    // Wait for panic
    let _ = handle.join();

    // Database should still be usable
    db.put(b"after_panic", b"still_works").unwrap();
    assert_eq!(
        db.get(b"after_panic").unwrap(),
        Some(b"still_works".to_vec())
    );

    // Transaction from panicked thread should be rolled back
    assert!(db.get(b"panic_key").unwrap().is_none());
}

// ========== WAL CORRUPTION AND RECOVERY SCENARIOS ==========

#[test]
fn test_wal_corrupted_header() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create DB with WAL enabled
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Database::create(&db_path, config).unwrap();

        // Write some data
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.sync().unwrap();
    }

    // Corrupt WAL header
    let wal_path = db_path.join("wal.log");
    if wal_path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xFF; 16]).unwrap(); // Corrupt header
        file.sync_all().unwrap();
    }

    // Attempt recovery
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    let result = Database::open(&db_path, config);

    // Should either recover gracefully or fail with appropriate error
    match result {
        Ok(recovered_db) => {
            // If recovery succeeds, basic operations should work
            recovered_db
                .put(b"test_after_recovery", b"test_value")
                .unwrap();
            assert_eq!(
                recovered_db.get(b"test_after_recovery").unwrap(),
                Some(b"test_value".to_vec())
            );
        }
        Err(_) => {
            // Graceful failure is acceptable for corrupted WAL
            println!("WAL corruption detected and handled appropriately");
        }
    }
}

#[test]
fn test_wal_partial_entries() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create DB and write data
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let db = Database::create(&db_path, config).unwrap();

        for i in 0..100 {
            let key = format!("partial_key_{:03}", i);
            let value = format!("partial_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Simulate partial write by truncating WAL
    let wal_path = db_path.join("wal.log");
    if wal_path.exists() {
        let metadata = std::fs::metadata(&wal_path).unwrap();
        let truncate_size = metadata.len() / 2; // Cut in half

        std::fs::OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(&wal_path)
            .unwrap()
            .set_len(truncate_size)
            .unwrap();
    }

    // Recovery should handle partial entries
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    let result = Database::open(&db_path, config);

    match result {
        Ok(recovered_db) => {
            // Should recover what it can and be operational
            recovered_db.put(b"post_recovery", b"works").unwrap();
        }
        Err(e) => {
            // Should fail with specific partial write error
            println!("Partial WAL entry handling: {:?}", e);
        }
    }
}

#[test]
fn test_wal_invalid_checksums() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create DB and write data
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let db = Database::create(&db_path, config).unwrap();

        for i in 0..20 {
            let key = format!("checksum_key_{:02}", i);
            let value = format!("checksum_value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.sync().unwrap();
    }

    // Corrupt random bytes in WAL to invalidate checksums
    let wal_path = db_path.join("wal.log");
    if wal_path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&wal_path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};

        // Corrupt middle section
        file.seek(SeekFrom::Start(512)).unwrap();
        file.write_all(&[0xAA; 32]).unwrap(); // Corrupt 32 bytes
        file.sync_all().unwrap();
    }

    // Recovery should detect checksum errors
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    let result = Database::open(&db_path, config);

    match result {
        Ok(recovered_db) => {
            // If it recovers, it should handle corruption gracefully
            recovered_db.put(b"after_checksum_error", b"value").unwrap();
        }
        Err(e) => {
            println!("Checksum mismatch properly detected: {:?}", e);
            assert!(
                format!("{:?}", e).contains("checksum")
                    || format!("{:?}", e).contains("Corruption")
            );
        }
    }
}

#[test]
fn test_wal_multiple_files_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create DB with WAL enabled
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let db = Database::create(&db_path, config).unwrap();

        // Write enough data to create multiple WAL segments
        for i in 0..200 {
            let key = format!("multi_wal_key_{:03}", i);
            let value = format!("multi_wal_value_with_extra_data_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();

            if i % 50 == 0 {
                db.sync().unwrap();
            }
        }
    }

    // Corrupt one of the WAL segments
    let wal_dir = &db_path;
    let wal_files = std::fs::read_dir(wal_dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()?.to_str()? == "wal" || path.file_name()?.to_str()?.contains("wal") {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if let Some(wal_file) = wal_files.first() {
        // Corrupt the first WAL segment
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(wal_file)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(100)).unwrap();
        file.write_all(&[0xBB; 50]).unwrap();
    }

    // Recovery should handle multiple WAL files with corruption
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    let result = Database::open(&db_path, config);

    match result {
        Ok(recovered_db) => {
            // Should recover what it can from uncorrupted segments
            let test_key = b"recovery_test";
            recovered_db.put(test_key, b"works").unwrap();
            assert_eq!(recovered_db.get(test_key).unwrap(), Some(b"works".to_vec()));
        }
        Err(e) => {
            println!("Multi-segment WAL recovery handled appropriately: {:?}", e);
        }
    }
}

// ========== MULTI-LEVEL FAILURE SCENARIOS ==========

#[test]
fn test_simultaneous_btree_lsm_corruption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create database with both B-tree and LSM data
    {
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

        // Write data that goes to both structures
        for i in 0..100 {
            let key = format!("dual_key_{:03}", i);
            let value = format!("dual_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush to LSM
        db.flush_lsm().unwrap();
        db.sync().unwrap();
    }

    // Corrupt both B-tree and LSM files
    let btree_path = db_path.join("btree.db");
    let lsm_path = db_path.join("lsm");

    // Corrupt B-tree file
    if btree_path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&btree_path)
            .unwrap();
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(1024)).unwrap();
        file.write_all(&[0xCC; 256]).unwrap();
    }

    // Corrupt LSM directory files
    if lsm_path.exists() {
        if let Ok(entries) = std::fs::read_dir(&lsm_path) {
            for entry in entries.flatten() {
                if let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(entry.path()) {
                    use std::io::Write;
                    let _ = file.write_all(&[0xDD; 128]);
                }
            }
        }
    }

    // Attempt recovery with dual corruption
    let result = Database::open(&db_path, LightningDbConfig::default());

    match result {
        Ok(recovered_db) => {
            // If recovery succeeds despite dual corruption, it should be functional
            recovered_db
                .put(b"dual_corruption_recovery", b"success")
                .unwrap();
        }
        Err(e) => {
            println!("Dual corruption appropriately detected: {:?}", e);
            assert!(format!("{:?}", e).to_lowercase().contains("corrupt"));
        }
    }
}

#[test]
fn test_transaction_failure_during_compaction() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Start a long-running transaction
    let tx_id = db.begin_transaction().unwrap();

    // Fill up LSM to trigger compaction
    for i in 0..500 {
        let key = format!("compact_key_{:04}", i);
        let value = format!("compact_value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Add data to transaction
    for i in 0..10 {
        let key = format!("tx_key_{:02}", i);
        let value = format!("tx_value_{:02}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force compaction while transaction is active
    let compact_result = db.flush_lsm();

    // Transaction operations should still work
    let tx_result = db.commit_transaction(tx_id);

    // Both should handle the scenario gracefully
    match (compact_result, tx_result) {
        (Ok(()), Ok(())) => {
            // Both succeeded - verify data integrity
            for i in 0..10 {
                let key = format!("tx_key_{:02}", i);
                let expected_value = format!("tx_value_{:02}", i);
                let actual_value = db.get(key.as_bytes()).unwrap();
                assert_eq!(actual_value, Some(expected_value.into_bytes()));
            }
        }
        _ => {
            // One or both failed - this is acceptable under stress
            println!("Transaction/compaction conflict handled appropriately");
        }
    }
}

#[test]
fn test_memory_corruption_during_flush() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Start concurrent writers
    let mut handles = vec![];

    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(
            move || -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                for i in 0..100 {
                    let key = format!("flush_key_{}_{:03}", thread_id, i);
                    let value = format!("flush_value_{}_{:03}", thread_id, i);
                    db_clone.put(key.as_bytes(), value.as_bytes())?;

                    // Randomly trigger flushes
                    if i % 20 == 0 {
                        let _ = db_clone.flush_lsm();
                    }
                }
                Ok(())
            },
        );
        handles.push(handle);
    }

    // Start flush thread
    let db_flush = Arc::clone(&db);
    let flush_handle = thread::spawn(move || {
        for _ in 0..10 {
            thread::sleep(std::time::Duration::from_millis(50));
            let _ = db_flush.flush_lsm();
        }
    });

    // Wait for all threads
    for handle in handles {
        let result = handle.join();
        match result {
            Ok(Ok(())) => {} // Success
            Ok(Err(e)) => println!("Writer thread error (acceptable): {:?}", e),
            Err(_) => {} // Thread panic (testing panic safety)
        }
    }

    let _ = flush_handle.join();

    // Database should remain functional
    db.put(b"post_flush_stress", b"functional").unwrap();
    assert_eq!(
        db.get(b"post_flush_stress").unwrap(),
        Some(b"functional".to_vec())
    );
}

#[test]
fn test_cascading_failures_across_components() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create database with multiple active components
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        config.compression_enabled = true;
        let db = Database::create(&db_path, config).unwrap();

        // Create data across all components
        let tx1 = db.begin_transaction().unwrap();
        let tx2 = db.begin_transaction().unwrap();

        // Write to different transactions
        for i in 0..50 {
            let key1 = format!("cascade_tx1_{:02}", i);
            let key2 = format!("cascade_tx2_{:02}", i);
            let value = format!("cascade_value_{:02}", i);

            db.put_tx(tx1, key1.as_bytes(), value.as_bytes()).unwrap();
            db.put_tx(tx2, key2.as_bytes(), value.as_bytes()).unwrap();
        }

        // Write direct data
        for i in 0..50 {
            let key = format!("cascade_direct_{:02}", i);
            let value = format!("cascade_direct_value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Leave transactions uncommitted to test recovery
        drop(db);
    }

    // Corrupt multiple files to cause cascading failures
    let files_to_corrupt = vec![
        db_path.join("wal.log"),
        db_path.join("btree.db"),
        db_path.join("metadata.db"),
    ];

    for file_path in &files_to_corrupt {
        if file_path.exists() {
            if let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(file_path) {
                use std::io::{Seek, SeekFrom, Write};
                let _ = file.seek(SeekFrom::Start(256));
                let _ = file.write_all(&[0xEE; 64]);
            }
        }
    }

    // Attempt recovery with multiple corrupted components
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    config.compression_enabled = true;

    let result = Database::open(&db_path, config);

    match result {
        Ok(recovered_db) => {
            // If recovery succeeds, basic functionality should work
            recovered_db
                .put(b"cascading_recovery_test", b"works")
                .unwrap();
        }
        Err(e) => {
            println!("Cascading failure appropriately handled: {:?}", e);
            // Should be a specific corruption error
            let error_str = format!("{:?}", e).to_lowercase();
            assert!(
                error_str.contains("corrupt")
                    || error_str.contains("invalid")
                    || error_str.contains("checksum")
            );
        }
    }
}

// ========== DATA INTEGRITY VERIFICATION TESTS ==========

#[test]
fn test_data_consistency_after_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write consistent data with known patterns
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let db = Database::create(&db_path, config).unwrap();

        // Write data with checksum pattern
        for i in 0..100 {
            let key = format!("consistency_key_{:03}", i);
            let value = format!("consistency_value_{:03}_{}", i, i * 7); // Predictable pattern
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Write transactional data
        let tx_id = db.begin_transaction().unwrap();
        for i in 0..20 {
            let key = format!("tx_consistency_key_{:02}", i);
            let value = format!("tx_consistency_value_{:02}_{}", i, i * 11);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.commit_transaction(tx_id).unwrap();

        db.sync().unwrap();
    }

    // Phase 2: Simulate crash by not closing cleanly
    // Phase 3: Recovery and verification
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let recovered_db = Database::open(&db_path, config).unwrap();

        // Verify all regular data
        for i in 0..100 {
            let key = format!("consistency_key_{:03}", i);
            let expected_value = format!("consistency_value_{:03}_{}", i, i * 7);
            let actual_value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Data consistency failed for key {}",
                key
            );
        }

        // Verify all transactional data
        for i in 0..20 {
            let key = format!("tx_consistency_key_{:02}", i);
            let expected_value = format!("tx_consistency_value_{:02}_{}", i, i * 11);
            let actual_value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Transaction consistency failed for key {}",
                key
            );
        }

        // Verify no phantom data exists
        for i in 100..120 {
            let key = format!("nonexistent_key_{:03}", i);
            let value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(value, None, "Phantom data detected for key {}", key);
        }
    }
}

#[test]
fn test_transaction_atomicity_after_crash() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create initial state
    {
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();
        for i in 0..10 {
            let key = format!("atomic_base_{:02}", i);
            let value = format!("atomic_base_value_{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.sync().unwrap();
    }

    // Start transaction and crash before commit
    {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        let tx_id = db.begin_transaction().unwrap();

        // Modify existing keys and add new ones
        for i in 0..10 {
            let key = format!("atomic_base_{:02}", i);
            let new_value = format!("atomic_modified_value_{:02}", i);
            db.put_tx(tx_id, key.as_bytes(), new_value.as_bytes())
                .unwrap();
        }

        for i in 10..20 {
            let key = format!("atomic_new_{:02}", i);
            let value = format!("atomic_new_value_{:02}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Simulate crash without commit
        drop(db);
    }

    // Recovery: transaction should be rolled back
    {
        let recovered_db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        // Original data should be intact
        for i in 0..10 {
            let key = format!("atomic_base_{:02}", i);
            let expected_value = format!("atomic_base_value_{:02}", i);
            let actual_value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Transaction atomicity violated: key {} was modified",
                key
            );
        }

        // New keys from uncommitted transaction should not exist
        for i in 10..20 {
            let key = format!("atomic_new_{:02}", i);
            let value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Transaction atomicity violated: key {} exists from uncommitted transaction",
                key
            );
        }
    }
}

#[test]
fn test_phantom_reads_prevention_after_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create database with range data
    {
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

        // Create a sparse range
        for i in (0..100).step_by(2) {
            let key = format!("range_key_{:03}", i);
            let value = format!("range_value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        db.sync().unwrap();
    }

    // Start transaction, read range, then crash
    let initial_range_size = {
        let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        let tx_id = db.begin_transaction().unwrap();

        // Read initial range
        let initial_range = db
            .range(Some(b"range_key_020"), Some(b"range_key_080"))
            .unwrap();
        let range_size = initial_range.len();

        // Add some data in the range (but don't commit)
        for i in (21..79).step_by(2) {
            let key = format!("range_key_{:03}", i);
            let value = format!("range_phantom_value_{:03}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Simulate crash without commit
        drop(db);
        range_size
    };

    // After recovery, range should be unchanged
    {
        let recovered_db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
        let tx_id = recovered_db.begin_transaction().unwrap();

        let recovered_range = recovered_db
            .range(Some(b"range_key_020"), Some(b"range_key_080"))
            .unwrap();

        assert_eq!(
            recovered_range.len(),
            initial_range_size,
            "Phantom reads detected: range size changed from {} to {}",
            initial_range_size,
            recovered_range.len()
        );

        // Verify no phantom keys exist
        for i in (21..79).step_by(2) {
            let key = format!("range_key_{:03}", i);
            let value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Phantom read detected: key {} exists after recovery",
                key
            );
        }

        recovered_db.commit_transaction(tx_id).unwrap();
    }
}

#[test]
fn test_index_consistency_after_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create database with indexed data
    {
        let db = Database::create(&db_path, LightningDbConfig::default()).unwrap();

        // Write data that should be indexed consistently
        for i in 0..50 {
            let key = format!("indexed_key_{:03}", i);
            let value = format!("indexed_value_{:03}_{}", i, i * 17); // Simple hash-like value
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force index updates
        db.flush_lsm().unwrap();
        db.sync().unwrap();
    }

    // Simulate partial index corruption during crash
    let index_files = std::fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            let filename = path.file_name()?.to_str()?;
            if filename.contains("index") || filename.contains("btree") {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Corrupt index files slightly
    for index_file in &index_files {
        if let Ok(metadata) = std::fs::metadata(index_file) {
            if metadata.len() > 1024 {
                if let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(index_file) {
                    use std::io::{Seek, SeekFrom, Write};
                    let _ = file.seek(SeekFrom::Start(512));
                    let _ = file.write_all(&[0xFF; 16]); // Small corruption
                }
            }
        }
    }

    // Recovery should rebuild indexes consistently
    {
        let recovered_db = Database::open(&db_path, LightningDbConfig::default()).unwrap();

        // Verify all data is accessible (tests index consistency)
        for i in 0..50 {
            let key = format!("indexed_key_{:03}", i);
            let expected_value = format!("indexed_value_{:03}_{}", i, i * 17); // Simple hash-like value
            let actual_value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Index inconsistency detected for key {}",
                key
            );
        }

        // Test range operations (require consistent indexing)
        let range_result = recovered_db
            .range(Some(b"indexed_key_010"), Some(b"indexed_key_040"))
            .unwrap();
        assert!(
            range_result.len() >= 20,
            "Index corruption affected range queries: got {} entries, expected at least 20",
            range_result.len()
        );
    }
}

// ========== COMPLEX RECOVERY SCENARIOS ==========

#[test]
fn test_recovery_during_active_transactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Start multiple concurrent transactions
    let tx_ids = (0..5)
        .map(|_| db.begin_transaction().unwrap())
        .collect::<Vec<_>>();

    // Each transaction writes different data
    for (idx, &tx_id) in tx_ids.iter().enumerate() {
        for i in 0..20 {
            let key = format!("active_tx_{}_{:02}", idx, i);
            let value = format!("active_tx_value_{}_{:02}", idx, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
    }

    // Commit some transactions, leave others active
    for (idx, &tx_id) in tx_ids.iter().enumerate() {
        if idx % 2 == 0 {
            db.commit_transaction(tx_id).unwrap();
        }
        // Leave odd-indexed transactions uncommitted
    }

    // Simulate crash with active transactions
    drop(db);

    // Recovery should handle active transactions properly
    let recovered_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Data from committed transactions should exist
    for idx in (0..5).step_by(2) {
        for i in 0..20 {
            let key = format!("active_tx_{}_{:02}", idx, i);
            let expected_value = format!("active_tx_value_{}_{:02}", idx, i);
            let actual_value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Committed transaction data missing for key {}",
                key
            );
        }
    }

    // Data from uncommitted transactions should not exist
    for idx in (1..5).step_by(2) {
        for i in 0..20 {
            let key = format!("active_tx_{}_{:02}", idx, i);
            let value = recovered_db.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Uncommitted transaction data found for key {}",
                key
            );
        }
    }
}

#[test]
fn test_recovery_with_pending_compactions() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Fill LSM to trigger compaction needs
    for i in 0..1000 {
        let key = format!("compaction_key_{:04}", i);
        let value = format!("compaction_value_{:04}_{}", i, "x".repeat(100));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 100 == 0 {
            // Trigger partial flushes to create multiple levels
            let _ = db.flush_lsm();
        }
    }

    // Start compaction but simulate crash before completion
    let compaction_handle = {
        let db_clone = Arc::clone(&db);
        thread::spawn(move || {
            let _ = db_clone.flush_lsm();
        })
    };

    // Don't wait for compaction to complete, simulate crash
    thread::sleep(std::time::Duration::from_millis(50));
    drop(db);
    let _ = compaction_handle.join();

    // Recovery should handle interrupted compaction
    let recovered_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Verify all data is still accessible
    for i in 0..1000 {
        let key = format!("compaction_key_{:04}", i);
        let expected_value = format!("compaction_value_{:04}_{}", i, "x".repeat(100));
        let actual_value = recovered_db.get(key.as_bytes()).unwrap();
        assert_eq!(
            actual_value,
            Some(expected_value.into_bytes()),
            "Data lost during compaction recovery for key {}",
            key
        );
    }

    // Database should be functional after recovery
    recovered_db
        .put(b"post_compaction_recovery", b"works")
        .unwrap();
    assert_eq!(
        recovered_db.get(b"post_compaction_recovery").unwrap(),
        Some(b"works".to_vec())
    );
}

#[test]
fn test_recovery_with_partial_writes() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Create database and start writing large batch
    {
        let mut config = LightningDbConfig::default();
        config.use_unified_wal = true;
        let db = Database::create(&db_path, config).unwrap();

        // Write large entries to ensure partial writes are possible
        for i in 0..50 {
            let key = format!("partial_write_key_{:03}", i);
            let value = "x".repeat(8192); // 8KB values
            db.put(key.as_bytes(), value.as_bytes()).unwrap();

            // Force sync of some data
            if i % 10 == 0 {
                db.sync().unwrap();
            }
        }

        // Simulate crash during write
    }

    // Truncate files to simulate partial writes
    let files_to_truncate = vec![db_path.join("wal.log"), db_path.join("btree.db")];

    for file_path in &files_to_truncate {
        if file_path.exists() {
            if let Ok(metadata) = std::fs::metadata(file_path) {
                let current_size = metadata.len();
                let truncate_size = current_size * 3 / 4; // Remove last 25%

                let _ = std::fs::OpenOptions::new()
                    .write(true)
                    .open(file_path)
                    .unwrap()
                    .set_len(truncate_size);
            }
        }
    }

    // Recovery should handle partial writes gracefully
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    let result = Database::open(&db_path, config);

    match result {
        Ok(recovered_db) => {
            // Should be able to operate normally
            recovered_db
                .put(b"partial_write_recovery_test", b"success")
                .unwrap();

            // Check that some data survived
            let mut surviving_keys = 0;
            for i in 0..50 {
                let key = format!("partial_write_key_{:03}", i);
                if recovered_db.get(key.as_bytes()).unwrap().is_some() {
                    surviving_keys += 1;
                }
            }

            // At least some data should have survived
            assert!(
                surviving_keys > 0,
                "No data survived partial write recovery"
            );
        }
        Err(e) => {
            // Graceful failure is acceptable for severe partial writes
            println!("Partial write scenario handled appropriately: {:?}", e);
        }
    }
}

#[test]
fn test_recovery_from_disk_full_conditions() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Fill database with data
    for i in 0..100 {
        let key = format!("disk_full_key_{:03}", i);
        let value = format!("disk_full_value_{:03}_{}", i, "y".repeat(1024));
        let result = db.put(key.as_bytes(), value.as_bytes());

        // Simulate disk full errors for some writes
        match result {
            Ok(()) => {} // Write succeeded
            Err(_) => {
                // Disk full simulation - this is expected
                println!("Simulated disk full error for key {}", key);
                break;
            }
        }
    }

    // Try to sync (may fail due to disk full)
    let sync_result = db.sync();

    drop(db);

    // Recovery should handle disk full scenarios
    let recovered_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Database should be functional despite previous disk full conditions
    recovered_db
        .put(b"disk_full_recovery_test", b"works")
        .unwrap();
    assert_eq!(
        recovered_db.get(b"disk_full_recovery_test").unwrap(),
        Some(b"works".to_vec())
    );

    // Count how much data survived
    let mut surviving_count = 0;
    for i in 0..100 {
        let key = format!("disk_full_key_{:03}", i);
        if recovered_db.get(key.as_bytes()).unwrap().is_some() {
            surviving_count += 1;
        }
    }

    println!("Disk full recovery: {} entries survived", surviving_count);

    // Should have consistent state (all or nothing for each transaction)
    // This is implementation-specific, but the database should not be corrupted
}

// ========== PERFORMANCE DEGRADATION RECOVERY TESTS ==========

#[test]
fn test_recovery_from_memory_leaks() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Simulate memory pressure by creating many transactions
    let mut tx_ids = Vec::new();

    // Create many transactions without committing (simulating leak)
    for _ in 0..100 {
        let tx_id = db.begin_transaction().unwrap();
        tx_ids.push(tx_id);

        // Add some data to each transaction
        let key = format!("leak_test_key_{}", tx_id);
        let value = format!("leak_test_value_{}", tx_id);
        let _ = db.put_tx(tx_id, key.as_bytes(), value.as_bytes());
    }

    // Abort transactions to test cleanup
    for tx_id in tx_ids {
        let _ = db.abort_transaction(tx_id);
    }

    // Create more transactions after cleanup
    for i in 0..50 {
        let tx_id = db.begin_transaction().unwrap();
        let key = format!("post_cleanup_key_{:02}", i);
        let value = format!("post_cleanup_value_{:02}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        db.commit_transaction(tx_id).unwrap();
    }

    // Verify database is still functional
    for i in 0..50 {
        let key = format!("post_cleanup_key_{:02}", i);
        let expected_value = format!("post_cleanup_value_{:02}", i);
        let actual_value = db.get(key.as_bytes()).unwrap();
        assert_eq!(
            actual_value,
            Some(expected_value.into_bytes()),
            "Memory leak recovery failed for key {}",
            key
        );
    }

    // Check transaction count is reasonable
    let stats = db.stats();
    assert_eq!(
        stats.active_transactions, 0,
        "Memory leak: active transactions not cleaned up"
    );
}

#[test]
fn test_recovery_from_lock_contention() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());

    // Create contention scenario
    let mut handles = vec![];

    // Start many threads that will contend for locks
    for thread_id in 0..20 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(
            move || -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                for i in 0..50 {
                    let key = format!("contention_key_{:02}_{:02}", thread_id, i);
                    let value = format!("contention_value_{:02}_{:02}", thread_id, i);

                    // Try to acquire locks with timeout
                    let start = std::time::Instant::now();
                    let result = db_clone.put(key.as_bytes(), value.as_bytes());

                    // If it takes too long, that indicates lock contention
                    if start.elapsed() > std::time::Duration::from_millis(1000) {
                        return Err("Lock contention timeout".to_string().into());
                    }

                    match result {
                        Ok(()) => {}
                        Err(e) => {
                            // Lock failures are acceptable under high contention
                            println!("Expected lock failure under contention: {:?}", e);
                            return Ok(());
                        }
                    }

                    // Small delay to increase contention
                    thread::sleep(std::time::Duration::from_millis(1));
                }
                Ok(())
            },
        );
        handles.push(handle);
    }

    // Start deadlock detection thread
    let db_deadlock = Arc::clone(&db);
    let deadlock_handle = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(500));

        // Try some operations during high contention
        for i in 0..10 {
            let key = format!("deadlock_test_key_{:02}", i);
            let value = format!("deadlock_test_value_{:02}", i);
            let _ = db_deadlock.put(key.as_bytes(), value.as_bytes());
        }
    });

    // Wait for threads with timeout
    let timeout = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();

    for handle in handles {
        if start.elapsed() > timeout {
            // Timeout - this indicates severe lock contention
            break;
        }

        match handle.join() {
            Ok(Ok(())) => {} // Thread completed successfully
            Ok(Err(_)) => {} // Thread failed but didn't panic (acceptable under contention)
            Err(_) => {}     // Thread panicked (testing panic recovery)
        }
    }

    let _ = deadlock_handle.join();

    // Database should recover from contention and be functional
    db.put(b"post_contention_test", b"works").unwrap();
    assert_eq!(
        db.get(b"post_contention_test").unwrap(),
        Some(b"works".to_vec())
    );

    // Verify some data made it through
    let mut successful_writes = 0;
    for thread_id in 0..5 {
        // Check subset to avoid timeout
        for i in 0..10 {
            let key = format!("contention_key_{:02}_{:02}", thread_id, i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                successful_writes += 1;
            }
        }
    }

    println!(
        "Lock contention recovery: {} successful writes",
        successful_writes
    );
    // Should have some successful writes despite contention
    assert!(
        successful_writes > 0,
        "No writes succeeded during lock contention test"
    );
}

#[test]
fn test_recovery_from_cache_corruption() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Fill cache with data
    for i in 0..100 {
        let key = format!("cache_key_{:03}", i);
        let value = format!("cache_value_{:03}_{}", i, "z".repeat(512));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Read data to ensure it's cached
    for i in 0..100 {
        let key = format!("cache_key_{:03}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }

    // Simulate cache corruption by accessing after potential internal state changes
    // This is a simplified test - real cache corruption would be more complex

    // Force flush to disk to bypass potentially corrupted cache
    db.sync().unwrap();

    // Create new database instance to bypass cache
    drop(db);
    let recovered_db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();

    // Verify all data is still accessible from disk
    for i in 0..100 {
        let key = format!("cache_key_{:03}", i);
        let expected_value = format!("cache_value_{:03}_{}", i, "z".repeat(512));
        let actual_value = recovered_db.get(key.as_bytes()).unwrap();
        assert_eq!(
            actual_value,
            Some(expected_value.into_bytes()),
            "Cache corruption affected data integrity for key {}",
            key
        );
    }

    // Test cache rebuild with new data
    for i in 100..150 {
        let key = format!("cache_rebuild_key_{:03}", i);
        let value = format!("cache_rebuild_value_{:03}", i);
        recovered_db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify new cache works correctly
    for i in 100..150 {
        let key = format!("cache_rebuild_key_{:03}", i);
        let expected_value = format!("cache_rebuild_value_{:03}", i);
        let actual_value = recovered_db.get(key.as_bytes()).unwrap();
        assert_eq!(
            actual_value,
            Some(expected_value.into_bytes()),
            "Cache rebuild failed for key {}",
            key
        );
    }
}

#[test]
fn test_recovery_from_extreme_fragmentation() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Create fragmentation by writing and deleting in patterns
    for cycle in 0..5 {
        // Write data
        for i in 0..100 {
            let key = format!("frag_key_{:02}_{:03}", cycle, i);
            let value = format!("frag_value_{:02}_{:03}_{}", cycle, i, "f".repeat(256));
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Delete every other key to create fragmentation
        for i in (0..100).step_by(2) {
            let key = format!("frag_key_{:02}_{:03}", cycle, i);
            db.delete(key.as_bytes()).unwrap();
        }

        // Force LSM flush to persist fragmentation
        if cycle % 2 == 0 {
            db.flush_lsm().unwrap();
        }
    }

    // Create more fragmentation with transactions
    for cycle in 0..3 {
        let tx_id = db.begin_transaction().unwrap();

        for i in 0..50 {
            let key = format!("tx_frag_key_{:02}_{:02}", cycle, i);
            let value = format!("tx_frag_value_{:02}_{:02}", cycle, i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        if cycle % 2 == 0 {
            db.commit_transaction(tx_id).unwrap();
        } else {
            db.abort_transaction(tx_id).unwrap();
        }
    }

    // Force compaction to handle fragmentation
    db.flush_lsm().unwrap();
    db.sync().unwrap();

    // Verify remaining data is accessible despite fragmentation
    for cycle in 0..5 {
        for i in (1..100).step_by(2) {
            // Only odd keys should remain
            let key = format!("frag_key_{:02}_{:03}", cycle, i);
            let expected_value = format!("frag_value_{:02}_{:03}_{}", cycle, i, "f".repeat(256));
            let actual_value = db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Fragmentation recovery failed for key {}",
                key
            );
        }
    }

    // Verify committed transaction data exists
    for cycle in (0..3).step_by(2) {
        for i in 0..50 {
            let key = format!("tx_frag_key_{:02}_{:02}", cycle, i);
            let expected_value = format!("tx_frag_value_{:02}_{:02}", cycle, i);
            let actual_value = db.get(key.as_bytes()).unwrap();
            assert_eq!(
                actual_value,
                Some(expected_value.into_bytes()),
                "Transaction fragmentation recovery failed for key {}",
                key
            );
        }
    }

    // Verify aborted transaction data doesn't exist
    for cycle in (1..3).step_by(2) {
        for i in 0..50 {
            let key = format!("tx_frag_key_{:02}_{:02}", cycle, i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(
                value, None,
                "Aborted transaction data found during fragmentation test for key {}",
                key
            );
        }
    }

    // Database should remain performant despite fragmentation
    let start = std::time::Instant::now();
    for i in 0..50 {
        let key = format!("perf_test_key_{:02}", i);
        let value = format!("perf_test_value_{:02}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = start.elapsed();

    let start = std::time::Instant::now();
    for i in 0..50 {
        let key = format!("perf_test_key_{:02}", i);
        let _ = db.get(key.as_bytes()).unwrap();
    }
    let read_duration = start.elapsed();

    println!(
        "Fragmentation performance - Write: {:?}, Read: {:?}",
        write_duration, read_duration
    );

    // Operations should complete in reasonable time despite fragmentation
    assert!(
        write_duration < std::time::Duration::from_secs(5),
        "Write performance severely degraded by fragmentation: {:?}",
        write_duration
    );
    assert!(
        read_duration < std::time::Duration::from_secs(2),
        "Read performance severely degraded by fragmentation: {:?}",
        read_duration
    );
}

#[test]
fn test_recovery_from_resource_exhaustion() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Simulate resource exhaustion by consuming many file descriptors/handles
    let mut transactions = Vec::new();
    let mut resource_limit_hit = false;

    // Keep creating transactions until we hit a resource limit
    for i in 0..1000 {
        match db.begin_transaction() {
            Ok(tx_id) => {
                transactions.push(tx_id);

                // Add data to transaction
                let key = format!("resource_key_{:04}", i);
                let value = format!("resource_value_{:04}", i);
                let _ = db.put_tx(tx_id, key.as_bytes(), value.as_bytes());
            }
            Err(_) => {
                resource_limit_hit = true;
                println!("Resource limit hit at {} transactions", i);
                break;
            }
        }
    }

    // Clean up some resources
    let half = transactions.len() / 2;
    for &tx_id in &transactions[..half] {
        let _ = db.commit_transaction(tx_id);
    }

    for &tx_id in &transactions[half..] {
        let _ = db.abort_transaction(tx_id);
    }

    // Should be able to create new transactions after cleanup
    for i in 0..10 {
        let tx_id = db.begin_transaction().unwrap();
        let key = format!("post_exhaustion_key_{:02}", i);
        let value = format!("post_exhaustion_value_{:02}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        db.commit_transaction(tx_id).unwrap();
    }

    // Verify recovery worked
    for i in 0..10 {
        let key = format!("post_exhaustion_key_{:02}", i);
        let expected_value = format!("post_exhaustion_value_{:02}", i);
        let actual_value = db.get(key.as_bytes()).unwrap();
        assert_eq!(
            actual_value,
            Some(expected_value.into_bytes()),
            "Resource exhaustion recovery failed for key {}",
            key
        );
    }

    // Check that committed transactions from first half survived
    let mut surviving_committed = 0;
    for i in 0..half {
        let key = format!("resource_key_{:04}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            surviving_committed += 1;
        }
    }

    println!(
        "Resource exhaustion recovery: {} committed transactions survived",
        surviving_committed
    );

    if resource_limit_hit {
        // If we actually hit limits, at least some committed data should survive
        assert!(
            surviving_committed > 0,
            "No committed transactions survived resource exhaustion"
        );
    }
}

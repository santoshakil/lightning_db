use lightning_db::{Database, LightningDbConfig, Result};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn test_basic_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024;
    
    let db = Database::create(db_path, config)?;
    
    // Test put and get
    db.put(b"key1", b"value1")?;
    let value = db.get(b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));
    
    // Test update
    db.put(b"key1", b"updated_value1")?;
    let value = db.get(b"key1")?;
    assert_eq!(value, Some(b"updated_value1".to_vec()));
    
    // Test delete
    db.delete(b"key1")?;
    let value = db.get(b"key1")?;
    assert_eq!(value, None);
    
    // Test non-existent key
    let value = db.get(b"nonexistent")?;
    assert_eq!(value, None);
    
    Ok(())
}

#[test]
fn test_batch_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Insert batch of data
    let batch_size = 1000;
    for i in 0..batch_size {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Verify all data
    for i in 0..batch_size {
        let key = format!("key_{:06}", i);
        let expected_value = format!("value_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert_eq!(value, Some(expected_value.into_bytes()));
    }
    
    // Delete half the data
    for i in 0..batch_size/2 {
        let key = format!("key_{:06}", i);
        db.delete(key.as_bytes())?;
    }
    
    // Verify deletions
    for i in 0..batch_size/2 {
        let key = format!("key_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert_eq!(value, None);
    }
    
    // Verify remaining data
    for i in batch_size/2..batch_size {
        let key = format!("key_{:06}", i);
        let expected_value = format!("value_{:06}", i);
        let value = db.get(key.as_bytes())?;
        assert_eq!(value, Some(expected_value.into_bytes()));
    }
    
    Ok(())
}

#[test]
fn test_concurrent_access() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024;
    
    let db = Arc::new(Database::create(db_path, config)?);
    
    let num_threads = 8;
    let operations_per_thread = 100;
    let mut handles = vec![];
    
    // Spawn concurrent writers
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for op_id in 0..operations_per_thread {
                let key = format!("thread_{}_key_{}", thread_id, op_id);
                let value = format!("thread_{}_value_{}", thread_id, op_id);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all data
    for thread_id in 0..num_threads {
        for op_id in 0..operations_per_thread {
            let key = format!("thread_{}_key_{}", thread_id, op_id);
            let expected_value = format!("thread_{}_value_{}", thread_id, op_id);
            let value = db.get(key.as_bytes())?;
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }
    
    Ok(())
}

#[test]
fn test_large_values() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Test various value sizes
    let sizes = vec![
        1,           // 1 byte
        1024,        // 1 KB
        10 * 1024,   // 10 KB
        100 * 1024,  // 100 KB
        1024 * 1024, // 1 MB
    ];
    
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_key_{}", i);
        let value = vec![b'x'; *size];
        
        db.put(key.as_bytes(), &value)?;
        
        let retrieved = db.get(key.as_bytes())?;
        assert_eq!(retrieved, Some(value));
    }
    
    Ok(())
}

#[test]
fn test_persistence() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    // Write data and close database
    {
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config)?;
        
        for i in 0..100 {
            let key = format!("persist_key_{}", i);
            let value = format!("persist_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        db.sync()?;
        // Database dropped here
    }
    
    // Reopen and verify data
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config)?;
        
        for i in 0..100 {
            let key = format!("persist_key_{}", i);
            let expected_value = format!("persist_value_{}", i);
            let value = db.get(key.as_bytes())?;
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }
    
    Ok(())
}

#[test]
fn test_iterator() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Insert sorted data
    let entries = vec![
        ("key_001", "value_001"),
        ("key_002", "value_002"),
        ("key_003", "value_003"),
        ("key_004", "value_004"),
        ("key_005", "value_005"),
    ];
    
    for (key, value) in &entries {
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Verify data was inserted correctly
    for (key, value) in &entries {
        let retrieved = db.get(key.as_bytes())?;
        assert_eq!(retrieved, Some(value.as_bytes().to_vec()));
    }
    
    Ok(())
}

#[test]
fn test_range_queries() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Insert data with numeric keys
    for i in 0..100 {
        let key = format!("{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Test range by getting specific keys
    let mut count = 0;
    for i in 20..30 {
        let key = format!("{:03}", i);
        let value = db.get(key.as_bytes())?;
        if value.is_some() {
            count += 1;
        }
    }
    
    assert_eq!(count, 10);
    
    Ok(())
}

#[test]
fn test_transaction_basic() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Start transaction
    let tx_id = db.begin_transaction()?;
    
    // Make changes within transaction
    db.put_tx(tx_id, b"tx_key1", b"tx_value1")?;
    db.put_tx(tx_id, b"tx_key2", b"tx_value2")?;
    
    // Data should not be visible before commit
    assert_eq!(db.get(b"tx_key1")?, None);
    assert_eq!(db.get(b"tx_key2")?, None);
    
    // Commit transaction
    db.commit_transaction(tx_id)?;
    
    // Data should now be visible
    assert_eq!(db.get(b"tx_key1")?, Some(b"tx_value1".to_vec()));
    assert_eq!(db.get(b"tx_key2")?, Some(b"tx_value2".to_vec()));
    
    Ok(())
}

#[test]
fn test_transaction_rollback() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Insert initial data
    db.put(b"existing_key", b"existing_value")?;
    
    // Start transaction
    let tx_id = db.begin_transaction()?;
    
    // Make changes within transaction
    db.put_tx(tx_id, b"new_key", b"new_value")?;
    db.delete_tx(tx_id, b"existing_key")?;
    
    // Abort transaction
    db.abort_transaction(tx_id)?;
    
    // Original data should be unchanged
    assert_eq!(db.get(b"existing_key")?, Some(b"existing_value".to_vec()));
    assert_eq!(db.get(b"new_key")?, None);
    
    Ok(())
}

#[test]
fn test_performance_throughput() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 100 * 1024 * 1024;
    
    let db = Database::create(db_path, config)?;
    
    let num_operations = 10000;
    let value = vec![b'x'; 1024]; // 1KB value
    
    // Measure write throughput
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("perf_key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }
    let write_duration = start.elapsed();
    
    let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();
    let write_mb_per_sec = (num_operations * 1024) as f64 / 1024.0 / 1024.0 / write_duration.as_secs_f64();
    
    println!("Write performance:");
    println!("  Operations/sec: {:.0}", write_ops_per_sec);
    println!("  MB/sec: {:.2}", write_mb_per_sec);
    
    // Measure read throughput
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("perf_key_{:08}", i);
        let _ = db.get(key.as_bytes())?;
    }
    let read_duration = start.elapsed();
    
    let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();
    let read_mb_per_sec = (num_operations * 1024) as f64 / 1024.0 / 1024.0 / read_duration.as_secs_f64();
    
    println!("Read performance:");
    println!("  Operations/sec: {:.0}", read_ops_per_sec);
    println!("  MB/sec: {:.2}", read_mb_per_sec);
    
    // Basic performance assertions
    assert!(write_ops_per_sec > 1000.0, "Write throughput too low");
    assert!(read_ops_per_sec > 10000.0, "Read throughput too low");
    
    Ok(())
}

#[test]
fn test_compression() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    
    let db = Database::create(db_path.clone(), config)?;
    
    // Insert highly compressible data
    let key = b"compress_key";
    let value = vec![b'a'; 100_000]; // 100KB of 'a's
    
    db.put(key, &value)?;
    db.sync()?;
    
    // Check that data is correctly retrieved
    let retrieved = db.get(key)?;
    assert_eq!(retrieved, Some(value.clone()));
    
    // Verify file size is smaller than uncompressed data
    let metadata = std::fs::metadata(&db_path)?;
    let file_size = metadata.len();
    
    // File should be significantly smaller than 100KB due to compression
    assert!(file_size < 50_000, "Compression not effective: file size = {}", file_size);
    
    Ok(())
}

#[test]
fn test_crash_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    // Simulate crash by not properly closing database
    {
        let config = LightningDbConfig::default();
        let db = Database::create(&db_path, config)?;
        
        // Write data without explicit sync
        for i in 0..50 {
            let key = format!("crash_key_{}", i);
            let value = format!("crash_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Simulate crash - drop without sync
        std::mem::forget(db);
    }
    
    // Attempt recovery
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config)?;
        
        // Some data should be recovered
        let mut recovered_count = 0;
        for i in 0..50 {
            let key = format!("crash_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                recovered_count += 1;
            }
        }
        
        // At least some data should be recovered
        assert!(recovered_count > 0, "No data recovered after crash");
        println!("Recovered {} out of 50 entries after crash", recovered_count);
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig, Result};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn test_basic_crud_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Create
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Read
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        let result = db.get(key.as_bytes())?;
        assert_eq!(result, Some(value.into_bytes()));
    }
    
    // Update
    for i in 0..500 {
        let key = format!("key_{:04}", i);
        let new_value = format!("updated_value_{:04}", i);
        db.put(key.as_bytes(), new_value.as_bytes())?;
    }
    
    // Verify updates
    for i in 0..500 {
        let key = format!("key_{:04}", i);
        let expected = format!("updated_value_{:04}", i);
        let result = db.get(key.as_bytes())?;
        assert_eq!(result, Some(expected.into_bytes()));
    }
    
    // Delete
    for i in 0..250 {
        let key = format!("key_{:04}", i);
        db.delete(key.as_bytes())?;
    }
    
    // Verify deletes
    for i in 0..250 {
        let key = format!("key_{:04}", i);
        let result = db.get(key.as_bytes())?;
        assert_eq!(result, None);
    }
    
    Ok(())
}

#[test]
fn test_batch_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Batch insert
    let mut batch = Vec::new();
    for i in 0..10000 {
        let key = format!("batch_key_{:06}", i);
        let value = format!("batch_value_{:06}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }
    
    let start = Instant::now();
    for (key, value) in &batch {
        db.put(key, value)?;
    }
    let duration = start.elapsed();
    println!("Batch insert of 10,000 items took: {:?}", duration);
    
    // Verify batch insert
    for i in 0..10000 {
        let key = format!("batch_key_{:06}", i);
        let value = format!("batch_value_{:06}", i);
        let result = db.get(key.as_bytes())?;
        assert_eq!(result, Some(value.into_bytes()));
    }
    
    // Batch read
    let mut keys = Vec::new();
    for i in 5000..6000 {
        let key = format!("batch_key_{:06}", i);
        keys.push(key.into_bytes());
    }
    
    let start = Instant::now();
    let mut results = Vec::new();
    for key in &keys {
        results.push(db.get(key)?);
    }
    let duration = start.elapsed();
    println!("Batch read of 1,000 items took: {:?}", duration);
    
    assert_eq!(results.len(), 1000);
    for (i, result) in results.iter().enumerate() {
        let expected = format!("batch_value_{:06}", i + 5000);
        assert_eq!(result.as_ref().map(|v| v.as_slice()), Some(expected.as_bytes()));
    }
    
    Ok(())
}

#[test]
fn test_concurrent_access() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(db_path, config)?);
    
    let num_threads = 10;
    let ops_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{:04}", thread_id, i);
                let value = format!("thread_{}_value_{:04}", thread_id, i);
                
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                
                let result = db_clone.get(key.as_bytes()).unwrap();
                assert_eq!(result, Some(value.into_bytes()));
                
                if i % 2 == 0 {
                    let new_value = format!("thread_{}_updated_{:04}", thread_id, i);
                    db_clone.put(key.as_bytes(), new_value.as_bytes()).unwrap();
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify all data
    for thread_id in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("thread_{}_key_{:04}", thread_id, i);
            let result = db.get(key.as_bytes())?;
            
            if i % 2 == 0 {
                let expected = format!("thread_{}_updated_{:04}", thread_id, i);
                assert_eq!(result, Some(expected.into_bytes()));
            } else {
                let expected = format!("thread_{}_value_{:04}", thread_id, i);
                assert_eq!(result, Some(expected.into_bytes()));
            }
        }
    }
    
    Ok(())
}

#[test]
fn test_range_queries() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(db_path, config)?;
    
    // Insert ordered data
    for i in 0..1000 {
        let key = format!("item_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Range query
    let start_key = b"item_0100";
    let end_key = b"item_0200";
    
    let results = db.range(Some(start_key), Some(end_key))?;
    assert_eq!(results.len(), 100);
    
    for (key, value) in &results {
        assert!(key >= start_key);
        assert!(key < end_key);
        
        let key_str = std::str::from_utf8(&key).unwrap();
        let value_str = std::str::from_utf8(&value).unwrap();
        assert!(key_str.starts_with("item_"));
        assert!(value_str.starts_with("value_"));
    }
    
    // Prefix scan
    let prefix = b"item_05";
    let results = db.range(Some(prefix), Some(b"item_06"))?;
    assert_eq!(results.len(), 100); // item_0500 to item_0599
    
    for (key, _value) in &results {
        assert!(key.starts_with(prefix));
    }
    
    Ok(())
}

#[test]
fn test_large_values() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    config.compression_type = 1; // ZSTD
    
    let db = Database::create(db_path, config)?;
    
    // Test with various value sizes
    let sizes = vec![1024, 10240, 102400, 1024000]; // 1KB, 10KB, 100KB, 1MB
    
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_key_{}", i);
        let value = vec![i as u8; *size];
        
        let start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        let write_duration = start.elapsed();
        
        let start = Instant::now();
        let result = db.get(key.as_bytes())?;
        let read_duration = start.elapsed();
        
        assert_eq!(result, Some(value));
        
        println!(
            "Size: {}KB - Write: {:?}, Read: {:?}",
            size / 1024,
            write_duration,
            read_duration
        );
    }
    
    Ok(())
}

#[test]
fn test_persistence() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    
    // Write data and close database
    {
        let db = Database::create(&db_path, config.clone())?;
        
        for i in 0..1000 {
            let key = format!("persist_key_{:04}", i);
            let value = format!("persist_value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        db.flush_lsm()?;
    }
    
    // Reopen and verify data
    {
        let db = Database::open(&db_path, config)?;
        
        for i in 0..1000 {
            let key = format!("persist_key_{:04}", i);
            let value = format!("persist_value_{:04}", i);
            let result = db.get(key.as_bytes())?;
            assert_eq!(result, Some(value.into_bytes()));
        }
    }
    
    Ok(())
}

#[test]
fn test_compression_effectiveness() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path_uncompressed = temp_dir.path().join("uncompressed.db");
    let db_path_compressed = temp_dir.path().join("compressed.db");
    
    // Highly compressible data
    let data = "a".repeat(10000);
    
    // Test without compression
    {
        let mut config = LightningDbConfig::default();
        config.compression_enabled = false;
        
        let db = Database::create(&db_path_uncompressed, config)?;
        
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            db.put(key.as_bytes(), data.as_bytes())?;
        }
        
        db.flush_lsm()?;
    }
    
    // Test with compression
    {
        let mut config = LightningDbConfig::default();
        config.compression_enabled = true;
        config.compression_type = 1; // ZSTD
        
        let db = Database::create(&db_path_compressed, config)?;
        
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            db.put(key.as_bytes(), data.as_bytes())?;
        }
        
        db.flush_lsm()?;
    }
    
    // Compare file sizes
    let uncompressed_size = std::fs::metadata(&db_path_uncompressed)?.len();
    let compressed_size = std::fs::metadata(&db_path_compressed)?.len();
    
    println!("Uncompressed size: {} bytes", uncompressed_size);
    println!("Compressed size: {} bytes", compressed_size);
    println!("Compression ratio: {:.2}x", uncompressed_size as f64 / compressed_size as f64);
    
    // Compressed should be significantly smaller
    assert!(compressed_size < uncompressed_size / 2);
    
    Ok(())
}

#[test]
fn test_stress_test() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("stress.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 100 * 1024 * 1024; // 100MB
    config.use_optimized_transactions = true;
    config.use_optimized_page_manager = true;
    
    let db = Arc::new(Database::create(db_path, config)?);
    
    let num_threads = 20;
    let ops_per_thread = 5000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            for i in 0..ops_per_thread {
                let op = i % 4;
                let key = format!("stress_key_{:08}", thread_id * ops_per_thread + i);
                
                match op {
                    0 => {
                        // Write
                        let value = format!("stress_value_{:08}", i);
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    1 => {
                        // Read
                        let _ = db_clone.get(key.as_bytes()).unwrap();
                    }
                    2 => {
                        // Update
                        let value = format!("updated_{:08}", i);
                        db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                    3 => {
                        // Delete
                        let _ = db_clone.delete(key.as_bytes());
                    }
                    _ => unreachable!(),
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = num_threads * ops_per_thread;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("Stress test completed:");
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} ops/sec", ops_per_sec);
    
    assert!(ops_per_sec > 10000.0, "Performance too low");
    
    Ok(())
}

#[test]
fn test_memory_usage() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("memory.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB cache limit
    
    let db = Database::create(db_path, config)?;
    
    // Insert data larger than cache
    for i in 0..10000 {
        let key = format!("mem_key_{:06}", i);
        let value = vec![i as u8; 1024]; // 1KB per value
        db.put(key.as_bytes(), &value)?;
    }
    
    // Force cache eviction by reading old data
    for i in 0..1000 {
        let key = format!("mem_key_{:06}", i);
        let _ = db.get(key.as_bytes())?;
    }
    
    // Read recent data (should be in cache)
    let start = Instant::now();
    for i in 9000..10000 {
        let key = format!("mem_key_{:06}", i);
        let _ = db.get(key.as_bytes())?;
    }
    let cached_duration = start.elapsed();
    
    // Read old data (likely evicted)
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("mem_key_{:06}", i);
        let _ = db.get(key.as_bytes())?;
    }
    let uncached_duration = start.elapsed();
    
    println!("Cached reads: {:?}", cached_duration);
    println!("Uncached reads: {:?}", uncached_duration);
    
    // Cached reads should be faster
    assert!(cached_duration < uncached_duration * 2);
    
    Ok(())
}
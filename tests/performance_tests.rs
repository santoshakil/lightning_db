use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

#[test]
fn test_write_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const NUM_OPS: usize = 10000;
    
    // Test write performance
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = start.elapsed();
    
    let writes_per_sec = NUM_OPS as f64 / write_duration.as_secs_f64();
    println!("Write performance: {:.0} ops/sec", writes_per_sec);
    
    // Test read performance
    let start = Instant::now();
    for i in 0..NUM_OPS {
        let key = format!("key_{:08}", i);
        let _value = db.get(key.as_bytes()).unwrap();
    }
    let read_duration = start.elapsed();
    
    let reads_per_sec = NUM_OPS as f64 / read_duration.as_secs_f64();
    println!("Read performance: {:.0} ops/sec", reads_per_sec);
    
    // Test range scan performance
    let start = Instant::now();
    let count = db.range(None, None).unwrap().len();
    let scan_duration = start.elapsed();
    
    let items_per_sec = count as f64 / scan_duration.as_secs_f64();
    println!("Range scan performance: {:.0} items/sec", items_per_sec);
    
    // Verify performance thresholds
    assert!(writes_per_sec > 5_000.0, "Write performance too low: {:.0}", writes_per_sec);
    assert!(reads_per_sec > 10_000.0, "Read performance too low: {:.0}", reads_per_sec);
    assert!(items_per_sec > 100_000.0, "Scan performance too low: {:.0}", items_per_sec);
}

#[test]
fn test_transaction_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const NUM_TXS: usize = 1000;
    const OPS_PER_TX: usize = 10;
    
    let start = Instant::now();
    for tx_id in 0..NUM_TXS {
        let tx = db.begin_transaction().unwrap();
        
        for op in 0..OPS_PER_TX {
            let key = format!("tx_{:04}_key_{:02}", tx_id, op);
            let value = format!("value_{:08}", tx_id * OPS_PER_TX + op);
            db.put_tx(tx, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx).unwrap();
    }
    let duration = start.elapsed();
    
    let txs_per_sec = NUM_TXS as f64 / duration.as_secs_f64();
    let ops_per_sec = (NUM_TXS * OPS_PER_TX) as f64 / duration.as_secs_f64();
    
    println!("Transaction performance: {:.0} txs/sec", txs_per_sec);
    println!("Operations in transactions: {:.0} ops/sec", ops_per_sec);
    
    assert!(txs_per_sec > 1_000.0, "Transaction performance too low: {:.0}", txs_per_sec);
    assert!(ops_per_sec > 10_000.0, "Operations performance too low: {:.0}", ops_per_sec);
}

#[test]
fn test_concurrent_read_write_performance() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    // Concurrent write load
    let write_start = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..2500 {
                    let key = format!("thread_{}_key_{:04}", thread_id, i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        let _ = handle.join();
    }
    
    let write_duration = write_start.elapsed();
    let write_ops = 10000;
    let write_ops_per_sec = write_ops as f64 / write_duration.as_secs_f64();
    println!("Concurrent write performance: {:.0} ops/sec", write_ops_per_sec);
    
    // Concurrent read load
    let read_start = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..2500 {
                    let key = format!("thread_{}_key_{:04}", thread_id, i);
                    let _value = db.get(key.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        let _ = handle.join();
    }
    
    let read_duration = read_start.elapsed();
    let read_ops = 10000;
    let read_ops_per_sec = read_ops as f64 / read_duration.as_secs_f64();
    println!("Concurrent read performance: {:.0} ops/sec", read_ops_per_sec);
    
    // Performance should be reasonable
    assert!(write_ops_per_sec > 1000.0);
    assert!(read_ops_per_sec > 1000.0);
}

#[test]
fn test_batch_operation_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const BATCH_SIZE: usize = 1000;
    const NUM_BATCHES: usize = 10;
    
    // Prepare batches
    let mut batches = Vec::new();
    for batch_id in 0..NUM_BATCHES {
        let mut batch = Vec::new();
        for i in 0..BATCH_SIZE {
            let key = format!("batch_{}_key_{:04}", batch_id, i);
            let value = format!("value_{}", i);
            batch.push((key.into_bytes(), value.into_bytes()));
        }
        batches.push(batch);
    }
    
    // Test batch write performance
    let start = Instant::now();
    for batch in batches {
        db.batch_put(&batch).unwrap();
    }
    let batch_duration = start.elapsed();
    
    let total_ops = NUM_BATCHES * BATCH_SIZE;
    let batch_ops_per_sec = total_ops as f64 / batch_duration.as_secs_f64();
    println!("Batch write performance: {:.0} ops/sec", batch_ops_per_sec);
    
    // Test batch read performance
    let keys: Vec<_> = (0..NUM_BATCHES)
        .flat_map(|batch_id| {
            (0..BATCH_SIZE).map(move |i| {
                format!("batch_{}_key_{:04}", batch_id, i).into_bytes()
            })
        })
        .collect();
    
    let start = Instant::now();
    let results = db.get_batch(&keys).unwrap();
    let batch_read_duration = start.elapsed();
    
    let batch_read_ops_per_sec = total_ops as f64 / batch_read_duration.as_secs_f64();
    println!("Batch read performance: {:.0} ops/sec", batch_read_ops_per_sec);
    
    assert_eq!(results.len(), total_ops);
    assert!(results.iter().all(|r| r.is_some()));
    
    assert!(batch_ops_per_sec > 10_000.0, "Batch write performance too low");
    assert!(batch_read_ops_per_sec > 50_000.0, "Batch read performance too low");
}

#[test]
fn test_memory_pressure_performance() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.cache_size = 256 * 1024; // Small cache to test pressure
    config.page_size = 4096;
    
    let db = Database::create(dir.path(), config).unwrap();
    
    const VALUE_SIZE: usize = 4000;
    const NUM_ENTRIES: usize = 1000;
    let value = vec![b'X'; VALUE_SIZE];
    
    let mut write_times = Vec::new();
    for i in 0..NUM_ENTRIES {
        let key = format!("pressure_key_{:06}", i);
        let start = Instant::now();
        db.put(key.as_bytes(), &value).unwrap();
        write_times.push(start.elapsed());
        
        if i % 100 == 99 {
            let stats = db.stats();
            println!("After {} entries:", i + 1);
            println!("  Pages: {}, Free: {}", stats.page_count, stats.free_page_count);
            println!("  Cache hit rate: {:.1}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
        }
    }
    
    let avg_write_time: Duration = write_times.iter().sum::<Duration>() / write_times.len() as u32;
    println!("\nAverage write time under memory pressure: {:?}", avg_write_time);
    
    // Test read performance under memory pressure
    let mut read_times = Vec::new();
    for i in 0..NUM_ENTRIES {
        let key = format!("pressure_key_{:06}", i);
        let start = Instant::now();
        let _ = db.get(key.as_bytes()).unwrap();
        read_times.push(start.elapsed());
    }
    
    let avg_read_time: Duration = read_times.iter().sum::<Duration>() / read_times.len() as u32;
    println!("Average read time under memory pressure: {:?}", avg_read_time);
    
    let stats = db.stats();
    println!("\nFinal statistics:");
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  Cache hit rate: {:.1}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    
    // Performance should degrade gracefully, not catastrophically
    assert!(avg_write_time < Duration::from_millis(100));
    assert!(avg_read_time < Duration::from_millis(50));
}

#[test]
fn test_large_value_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Test various value sizes
    let sizes = vec![
        1024,           // 1 KB
        10 * 1024,      // 10 KB
        100 * 1024,     // 100 KB
        1024 * 1024,    // 1 MB
    ];
    
    for (i, size) in sizes.iter().enumerate() {
        let key = format!("large_{}", i);
        let value = vec![b'X'; *size];
        
        let start = Instant::now();
        db.put(key.as_bytes(), &value).unwrap();
        let write_time = start.elapsed();
        
        let start = Instant::now();
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        let read_time = start.elapsed();
        
        assert_eq!(retrieved.len(), *size);
        println!(
            "Size: {} bytes - Write: {:?}, Read: {:?}",
            size, write_time, read_time
        );
        
        // Large values should still be handled reasonably quickly
        assert!(write_time < Duration::from_secs(1));
        assert!(read_time < Duration::from_millis(100));
    }
}

#[test]
fn test_compression_performance() {
    let dir = tempdir().unwrap();
    
    // Test without compression
    let start = Instant::now();
    let db_no_compression = Database::create(
        dir.path().join("no_compression"), 
        LightningDbConfig {
            compression_enabled: false,
            ..Default::default()
        }
    ).unwrap();
    
    let compressible_data = "A".repeat(10000);
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        db_no_compression.put(key.as_bytes(), compressible_data.as_bytes()).unwrap();
    }
    let no_compression_time = start.elapsed();
    
    // Test with compression
    let start = Instant::now();
    let db_compression = Database::create(
        dir.path().join("compression"), 
        LightningDbConfig {
            compression_enabled: true,
            compression_type: 1, // ZSTD
            ..Default::default()
        }
    ).unwrap();
    
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        db_compression.put(key.as_bytes(), compressible_data.as_bytes()).unwrap();
    }
    let compression_time = start.elapsed();
    
    println!("Write time without compression: {:?}", no_compression_time);
    println!("Write time with compression: {:?}", compression_time);
    
    // Test read performance
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let _ = db_no_compression.get(key.as_bytes()).unwrap();
    }
    let no_compression_read_time = start.elapsed();
    
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let _ = db_compression.get(key.as_bytes()).unwrap();
    }
    let compression_read_time = start.elapsed();
    
    println!("Read time without compression: {:?}", no_compression_read_time);
    println!("Read time with compression: {:?}", compression_read_time);
    
    // Check space savings
    let no_compression_stats = db_no_compression.stats();
    let compression_stats = db_compression.stats();
    
    println!("Pages without compression: {}", no_compression_stats.page_count);
    println!("Pages with compression: {}", compression_stats.page_count);
    
    // Compression should provide space savings for compressible data
    assert!(compression_stats.page_count <= no_compression_stats.page_count);
}

#[test]
fn test_scan_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    const NUM_KEYS: usize = 10000;
    
    // Insert sequential keys
    for i in 0..NUM_KEYS {
        let key = format!("scan_key_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Test full scan performance
    let start = Instant::now();
    let iter = db.scan(None, None).unwrap();
    let mut count = 0;
    for result in iter {
        if result.is_ok() {
            count += 1;
        }
    }
    let full_scan_time = start.elapsed();
    
    assert_eq!(count, NUM_KEYS);
    let full_scan_rate = NUM_KEYS as f64 / full_scan_time.as_secs_f64();
    println!("Full scan rate: {:.0} items/sec", full_scan_rate);
    
    // Test prefix scan performance
    let start = Instant::now();
    let iter = db.scan_prefix(b"scan_key_00").unwrap();
    let mut prefix_count = 0;
    for result in iter {
        if result.is_ok() {
            prefix_count += 1;
        }
    }
    let prefix_scan_time = start.elapsed();
    
    assert!(prefix_count >= 100); // scan_key_000000 through scan_key_000999
    let prefix_scan_rate = prefix_count as f64 / prefix_scan_time.as_secs_f64();
    println!("Prefix scan rate: {:.0} items/sec", prefix_scan_rate);
    
    // Test range scan performance
    let start = Instant::now();
    let range_results = db.range(Some(b"scan_key_001000"), Some(b"scan_key_002000")).unwrap();
    let range_scan_time = start.elapsed();
    
    assert!(range_results.len() >= 1000);
    let range_scan_rate = range_results.len() as f64 / range_scan_time.as_secs_f64();
    println!("Range scan rate: {:.0} items/sec", range_scan_rate);
    
    // Performance thresholds
    assert!(full_scan_rate > 100_000.0, "Full scan too slow");
    assert!(prefix_scan_rate > 50_000.0, "Prefix scan too slow");
    assert!(range_scan_rate > 50_000.0, "Range scan too slow");
}

#[tokio::test]
async fn test_recovery_performance() {
    use std::time::Duration;
    use tempfile::TempDir;
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024 * 10; // 10MB cache

    let start = Instant::now();

    {
        let db = Database::create(&db_path, config.clone()).unwrap();

        for i in 0..1000 {
            let key = format!("perf_key_{:06}", i);
            let value = vec![i as u8; 1024]; // 1KB per value
            db.put(key.as_bytes(), &value).unwrap();
        }

        db.flush_lsm().unwrap();
    }

    let write_duration = start.elapsed();

    let recovery_start = Instant::now();
    let recovered_db = Database::open(&db_path, config).unwrap();
    let recovery_duration = recovery_start.elapsed();

    for i in (0..1000).step_by(100) {
        let key = format!("perf_key_{:06}", i);
        let result = recovered_db.get(key.as_bytes()).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1024);
    }

    println!("Write duration: {:?}", write_duration);
    println!("Recovery duration: {:?}", recovery_duration);

    assert!(recovery_duration < Duration::from_secs(5));
    assert!(write_duration < Duration::from_secs(10));
}
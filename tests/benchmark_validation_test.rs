use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_write_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    let num_ops = 10_000;
    let start = Instant::now();
    
    for i in 0..num_ops {
        let key = format!("key_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();
    
    println!("Write Performance: {:.0} ops/sec", ops_per_sec);
    
    // Should achieve at least 10K writes/sec
    assert!(ops_per_sec > 10_000.0, "Write performance below threshold");
}

#[test]
fn test_read_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Setup data
    for i in 0..10_000 {
        let key = format!("key_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let num_ops = 10_000;
    let start = Instant::now();
    
    for i in 0..num_ops {
        let key = format!("key_{:06}", i);
        let _value = db.get(key.as_bytes()).unwrap();
    }
    
    let duration = start.elapsed();
    let ops_per_sec = num_ops as f64 / duration.as_secs_f64();
    
    println!("Read Performance: {:.0} ops/sec", ops_per_sec);
    
    // Should achieve at least 10K reads/sec
    assert!(ops_per_sec > 10_000.0, "Read performance below threshold");
}

#[test]
fn test_batch_write_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    let mut batch = lightning_db::WriteBatch::new();
    let batch_size = 1000;
    let num_batches = 10;
    
    let start = Instant::now();
    
    for batch_num in 0..num_batches {
        batch.clear();
        for i in 0..batch_size {
            let key = format!("batch_{:04}_{:04}", batch_num, i);
            let value = format!("value_{}", i);
            batch.put(key.into_bytes(), value.into_bytes()).unwrap();
        }
        db.write_batch(&batch).unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = (batch_size * num_batches) as f64;
    let ops_per_sec = total_ops / duration.as_secs_f64();
    
    println!("Batch Write Performance: {:.0} ops/sec", ops_per_sec);
    
    // Should achieve at least 10K batch writes/sec
    assert!(ops_per_sec > 10_000.0, "Batch write performance below threshold");
}

#[test]
fn test_range_scan_performance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Setup data
    for i in 0..10_000 {
        let key = format!("key_{:06}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let start = Instant::now();
    let results = db.range(None, None).unwrap();
    let duration = start.elapsed();
    
    let items_per_sec = results.len() as f64 / duration.as_secs_f64();
    
    println!("Range Scan Performance: {:.0} items/sec", items_per_sec);
    
    // Should achieve at least 100K items/sec for range scans
    assert!(items_per_sec > 100_000.0, "Range scan performance below threshold");
    assert_eq!(results.len(), 10_000, "Range scan didn't return all items");
}

#[test]
fn test_concurrent_performance() {
    use std::sync::Arc;
    use std::thread;
    
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    let num_threads = 4;
    let ops_per_thread = 2500;
    
    let start = Instant::now();
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("thread_{}_key_{:04}", thread_id, i);
                    let value = format!("value_{}", i);
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = (num_threads * ops_per_thread) as f64;
    let ops_per_sec = total_ops / duration.as_secs_f64();
    
    println!("Concurrent Write Performance: {:.0} ops/sec", ops_per_sec);
    
    // Should achieve at least 20K concurrent writes/sec
    assert!(ops_per_sec > 20_000.0, "Concurrent performance below threshold");
}

#[test]
fn test_memory_efficiency() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024; // 1MB cache
    
    let db = Database::create(dir.path(), config).unwrap();
    
    // Insert more data than cache can hold
    for i in 0..5000 {
        let key = format!("key_{:06}", i);
        let value = vec![b'X'; 1024]; // 1KB each, total 5MB
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Database should still function efficiently
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{:06}", i);
        let _value = db.get(key.as_bytes()).unwrap();
    }
    let duration = start.elapsed();
    
    let ops_per_sec = 1000.0 / duration.as_secs_f64();
    println!("Read performance with limited cache: {:.0} ops/sec", ops_per_sec);
    
    // Should still maintain reasonable performance
    assert!(ops_per_sec > 1000.0, "Performance degraded too much with limited cache");
}
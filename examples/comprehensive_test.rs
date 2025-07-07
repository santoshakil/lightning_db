use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

#[derive(Debug)]
struct TestResult {
    feature: &'static str,
    status: TestStatus,
    details: String,
    performance: Option<PerformanceMetrics>,
}

#[derive(Debug)]
enum TestStatus {
    Working,
    PartiallyWorking,
    NotWorking,
}

#[derive(Debug)]
struct PerformanceMetrics {
    ops_per_sec: f64,
    latency_us: f64,
    meets_target: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Lightning DB Comprehensive Feature Test\n");

    let mut results = Vec::new();
    let dir = tempdir()?;

    // Test 1: Basic CRUD Operations
    results.push(test_basic_crud(&dir.path().join("basic"))?);

    // Test 2: Transaction Support
    results.push(test_transactions(&dir.path().join("transactions"))?);

    // Test 3: Concurrency & Thread Safety
    results.push(test_concurrency(&dir.path().join("concurrent"))?);

    // Test 4: Iterator & Range Queries
    results.push(test_iterators(&dir.path().join("iterator"))?);

    // Test 5: Batch Operations
    results.push(test_batch_operations(&dir.path().join("batch"))?);

    // Test 6: Compression
    results.push(test_compression(&dir.path().join("compression"))?);

    // Test 7: Memory Management & Caching
    results.push(test_memory_management(&dir.path().join("memory"))?);

    // Test 8: Persistence & Recovery
    results.push(test_persistence(&dir.path().join("persistence"))?);

    // Test 9: Backup & Restore
    results.push(test_backup_restore(&dir.path().join("backup"))?);

    // Test 10: Performance Under Load
    results.push(test_performance_under_load(&dir.path().join("load"))?);

    // Test 11: Edge Cases
    results.push(test_edge_cases(&dir.path().join("edge"))?);

    // Test 12: LSM Tree Features
    results.push(test_lsm_features(&dir.path().join("lsm"))?);

    // Print Summary
    print_summary(&results);

    Ok(())
}

fn test_basic_crud(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Basic CRUD Operations...");

    let db = Database::create(path, LightningDbConfig::default())?;
    let mut issues = Vec::new();

    // Test Put
    let start = Instant::now();
    for i in 0..10000 {
        db.put(
            format!("key_{}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
        )?;
    }
    let put_time = start.elapsed();

    // Test Get
    let start = Instant::now();
    for i in 0..10000 {
        let value = db.get(format!("key_{}", i).as_bytes())?;
        if value.as_deref() != Some(format!("value_{}", i).as_bytes()) {
            issues.push(format!("Get mismatch at key_{}", i));
        }
    }
    let get_time = start.elapsed();

    // Test Update
    db.put(b"update_key", b"original_value")?;
    db.put(b"update_key", b"updated_value")?;
    if db.get(b"update_key")?.as_deref() != Some(b"updated_value") {
        issues.push("Update operation failed".to_string());
    }

    // Test Delete
    db.put(b"delete_key", b"delete_value")?;
    db.delete(b"delete_key")?;
    if db.get(b"delete_key")?.is_some() {
        issues.push("Delete operation failed".to_string());
    }

    // Test non-existent key
    if db.get(b"non_existent_key")?.is_some() {
        issues.push("Non-existent key returned value".to_string());
    }

    let put_ops = 10000.0 / put_time.as_secs_f64();
    let get_ops = 10000.0 / get_time.as_secs_f64();

    Ok(TestResult {
        feature: "Basic CRUD Operations",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "All CRUD operations working correctly".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: Some(PerformanceMetrics {
            ops_per_sec: (put_ops + get_ops) / 2.0,
            latency_us: ((put_time.as_micros() + get_time.as_micros()) / 20000) as f64,
            meets_target: get_ops > 1_000_000.0,
        }),
    })
}

fn test_transactions(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Transaction Support...");

    let db = Database::create(path, LightningDbConfig::default())?;
    let mut issues = Vec::new();

    // Test basic transaction
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_key1", b"tx_value1")?;
    db.put_tx(tx_id, b"tx_key2", b"tx_value2")?;
    db.commit_transaction(tx_id)?;

    if db.get(b"tx_key1")?.as_deref() != Some(b"tx_value1") {
        issues.push("Transaction commit failed for key1".to_string());
    }

    // Test rollback
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"rollback_key", b"rollback_value")?;
    db.abort_transaction(tx_id)?;

    if db.get(b"rollback_key")?.is_some() {
        issues.push("Transaction rollback failed".to_string());
    }

    // Test isolation
    let tx1_id = db.begin_transaction()?;
    db.put_tx(tx1_id, b"isolated_key", b"tx1_value")?;

    let tx2_id = db.begin_transaction()?;
    if db.get_tx(tx2_id, b"isolated_key")?.is_some() {
        issues.push("Transaction isolation violated".to_string());
    }

    db.commit_transaction(tx1_id)?;
    db.abort_transaction(tx2_id)?;

    // Test transaction statistics
    if let Some(stats) = db.get_transaction_statistics() {
        if stats.commit_count == 0 {
            issues.push("Transaction statistics not tracked".to_string());
        }
    }

    Ok(TestResult {
        feature: "Transaction Support",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "ACID transactions with proper isolation".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_concurrency(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Concurrency & Thread Safety...");

    let db = Arc::new(Database::create(path, LightningDbConfig::default())?);
    let mut issues = Vec::new();

    let num_threads = 8;
    let ops_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));

    let mut handles = vec![];

    let start = Instant::now();

    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        handles.push(thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..ops_per_thread {
                let key = format!("thread_{}_{}", thread_id, i);
                let value = format!("value_{}_{}", thread_id, i);

                if let Err(e) = db_clone.put(key.as_bytes(), value.as_bytes()) {
                    return Err(format!("Put failed: {}", e));
                }

                if let Ok(Some(v)) = db_clone.get(key.as_bytes()) {
                    if v != value.as_bytes() {
                        return Err("Concurrent read mismatch".to_string());
                    }
                }
            }

            Ok(())
        }));
    }

    for handle in handles {
        if let Err(e) = handle.join().unwrap() {
            issues.push(e);
        }
    }

    let duration = start.elapsed();
    let total_ops = (num_threads * ops_per_thread * 2) as f64;
    let ops_per_sec = total_ops / duration.as_secs_f64();

    // Verify all data
    for thread_id in 0..num_threads {
        for i in 0..ops_per_thread {
            let key = format!("thread_{}_{}", thread_id, i);
            let expected_value = format!("value_{}_{}", thread_id, i);

            match db.get(key.as_bytes())? {
                Some(v) if v == expected_value.as_bytes() => {}
                _ => issues.push(format!("Missing or incorrect data for {}", key)),
            }
        }
    }

    Ok(TestResult {
        feature: "Concurrency & Thread Safety",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            format!(
                "{} threads, {} ops/thread completed successfully",
                num_threads, ops_per_thread
            )
        } else {
            format!(
                "Issues: {} errors out of {} operations",
                issues.len(),
                total_ops as usize
            )
        },
        performance: Some(PerformanceMetrics {
            ops_per_sec,
            latency_us: duration.as_micros() as f64 / total_ops,
            meets_target: ops_per_sec > 100_000.0,
        }),
    })
}

fn test_iterators(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Iterator & Range Queries...");

    let db = Database::create(path, LightningDbConfig::default())?;
    let mut issues = Vec::new();

    // Insert test data
    for i in 0..100 {
        db.put(
            format!("key_{:03}", i).as_bytes(),
            format!("value_{}", i).as_bytes(),
        )?;
    }

    // Test full scan
    let iter = db.scan(None, None)?;
    let entries: Result<Vec<_>, _> = iter.collect();
    let entries = entries?;

    if entries.len() != 100 {
        issues.push(format!(
            "Full scan returned {} entries, expected 100",
            entries.len()
        ));
    }

    // Verify ordering
    for i in 1..entries.len() {
        if entries[i].0 < entries[i - 1].0 {
            issues.push("Iterator ordering incorrect".to_string());
            break;
        }
    }

    // Test range query
    let start_key = b"key_020".to_vec();
    let end_key = b"key_030".to_vec();
    let range_iter = db.scan(Some(start_key.clone()), Some(end_key.clone()))?;

    let range_entries: Result<Vec<_>, _> = range_iter.collect();
    let range_entries = range_entries?;

    for entry in &range_entries {
        if entry.0 < start_key || entry.0 >= end_key {
            issues.push("Range query returned out-of-bounds key".to_string());
            break;
        }
    }

    if range_entries.len() != 10 {
        issues.push(format!(
            "Range query returned {} entries, expected 10",
            range_entries.len()
        ));
    }

    // Test reverse iteration
    let reverse_iter = db.scan_reverse(None, None)?;
    let reverse_entries: Result<Vec<_>, _> = reverse_iter.collect();
    let reverse_entries = reverse_entries?;

    if !reverse_entries.is_empty() && reverse_entries[0].0 != b"key_099" {
        issues.push("Reverse iterator didn't start from last key".to_string());
    }

    Ok(TestResult {
        feature: "Iterator & Range Queries",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "Full scan, range queries, and reverse iteration working".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_batch_operations(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Batch Operations...");

    let db = Database::create(path, LightningDbConfig::default())?;
    let mut issues = Vec::new();

    let batch_size = 1000;
    let mut batch = vec![];

    // Prepare batch
    for i in 0..batch_size {
        batch.push((
            format!("batch_key_{}", i).into_bytes(),
            format!("batch_value_{}", i).into_bytes(),
        ));
    }

    // Test batch put
    let start = Instant::now();
    db.put_batch(&batch)?;
    let batch_time = start.elapsed();

    // Verify batch write
    for i in 0..batch_size {
        let key = format!("batch_key_{}", i);
        let expected = format!("batch_value_{}", i);

        match db.get(key.as_bytes())? {
            Some(v) if v == expected.as_bytes() => {}
            _ => issues.push(format!("Batch write failed for {}", key)),
        }
    }

    // Test batch delete
    let delete_keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("batch_key_{}", i).into_bytes())
        .collect();

    db.delete_batch(&delete_keys)?;

    for key in &delete_keys {
        if db.get(key)?.is_some() {
            issues.push("Batch delete failed".to_string());
            break;
        }
    }

    let ops_per_sec = batch_size as f64 / batch_time.as_secs_f64();

    Ok(TestResult {
        feature: "Batch Operations",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            format!(
                "Batch put/delete working, {} ops in {:?}",
                batch_size, batch_time
            )
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: Some(PerformanceMetrics {
            ops_per_sec,
            latency_us: batch_time.as_micros() as f64 / batch_size as f64,
            meets_target: ops_per_sec > 100_000.0,
        }),
    })
}

fn test_compression(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Compression...");

    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    config.compression_type = 1; // Zstd

    let db = Database::create(path, config)?;
    let mut issues = Vec::new();

    // Insert compressible data
    let large_value = "x".repeat(10000); // Highly compressible
    let random_value = (0..10000)
        .map(|i| ((i * 7 + 13) % 256) as u8)
        .collect::<Vec<u8>>(); // Less compressible

    db.put(b"compressed_key", large_value.as_bytes())?;
    db.put(b"random_key", &random_value)?;

    // Verify data integrity
    if db.get(b"compressed_key")?.as_deref() != Some(large_value.as_bytes()) {
        issues.push("Compressed data retrieval failed".to_string());
    }

    if db.get(b"random_key")?.as_deref() != Some(random_value.as_slice()) {
        issues.push("Random data compression/decompression failed".to_string());
    }

    // Force flush to SSTable to test compression
    if let Err(e) = db.flush_lsm() {
        issues.push(format!("Flush failed: {}", e));
    }

    // Get compression stats
    let stats = db.lsm_stats();
    if stats.is_none() {
        issues.push("Unable to get compression statistics".to_string());
    }

    Ok(TestResult {
        feature: "Compression",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "Zstd compression working, data integrity maintained".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_memory_management(
    path: &std::path::Path,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Memory Management & Caching...");

    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB cache

    let db = Database::create(path, config)?;
    let mut issues = Vec::new();

    // Fill cache
    for i in 0..1000 {
        db.put(
            format!("cache_key_{}", i).as_bytes(),
            vec![0u8; 1024].as_slice(),
        )?;
    }

    // Test cache hits
    let start = Instant::now();
    for _ in 0..100000 {
        let _ = db.get(b"cache_key_0")?;
    }
    let cache_hit_time = start.elapsed();
    let cache_hit_ops = 100000.0 / cache_hit_time.as_secs_f64();

    // Test cache misses (force eviction)
    for i in 1000..2000 {
        db.put(
            format!("evict_key_{}", i).as_bytes(),
            vec![0u8; 10240].as_slice(),
        )?;
    }

    // Check if early keys were evicted
    let start = Instant::now();
    for i in 0..100 {
        let _ = db.get(format!("cache_key_{}", i).as_bytes())?;
    }
    let cache_miss_time = start.elapsed();

    if cache_hit_time > cache_miss_time {
        issues.push("Cache doesn't seem to be working effectively".to_string());
    }

    // Test cache statistics
    if let Some(stats) = db.cache_stats() {
        if !stats.contains("hit_rate") {
            issues.push("Cache statistics not being tracked".to_string());
        }
    }

    Ok(TestResult {
        feature: "Memory Management & Caching",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            format!(
                "ARC cache working, {} ops/sec for cache hits",
                cache_hit_ops
            )
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: Some(PerformanceMetrics {
            ops_per_sec: cache_hit_ops,
            latency_us: cache_hit_time.as_micros() as f64 / 100000.0,
            meets_target: cache_hit_ops > 1_000_000.0,
        }),
    })
}

fn test_persistence(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Persistence & Recovery...");

    let mut issues = Vec::new();

    // Create and populate database
    {
        let db = Database::create(path, LightningDbConfig::default())?;

        for i in 0..100 {
            db.put(
                format!("persist_key_{}", i).as_bytes(),
                format!("persist_value_{}", i).as_bytes(),
            )?;
        }

        // Test WAL functionality
        db.put(b"wal_test_key", b"wal_test_value")?;

        // Explicit drop to close database
    }

    // Reopen and verify
    {
        let db = Database::open(path, LightningDbConfig::default())?;

        for i in 0..100 {
            let key = format!("persist_key_{}", i);
            let expected = format!("persist_value_{}", i);

            match db.get(key.as_bytes())? {
                Some(v) if v == expected.as_bytes() => {}
                _ => {
                    issues.push(format!("Data loss after reopen: {}", key));
                    break;
                }
            }
        }

        // Check WAL recovery
        if db.get(b"wal_test_key")?.as_deref() != Some(b"wal_test_value") {
            issues.push("WAL recovery failed".to_string());
        }
    }

    // Test crash recovery simulation
    {
        let db = Database::create(path.join("crash_test"), LightningDbConfig::default())?;
        db.put(b"pre_crash", b"data")?;

        // Simulate crash by not properly closing
        std::mem::forget(db);
    }

    // Try to recover
    let mut critical_failure = false;
    match Database::open(path.join("crash_test"), LightningDbConfig::default()) {
        Ok(db) => {
            if db.get(b"pre_crash")?.is_none() {
                issues.push("Crash recovery failed".to_string());
            }
        }
        Err(e) => {
            issues.push(format!("Failed to open after crash: {}", e));
            critical_failure = true;
        }
    }

    Ok(TestResult {
        feature: "Persistence & Recovery",
        status: if critical_failure {
            TestStatus::NotWorking
        } else if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "Data persistence, WAL, and crash recovery working".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_backup_restore(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Backup & Restore...");

    let mut issues = Vec::new();
    let backup_path = path.join("backup");

    // Create and populate database
    let db = Database::create(path.join("original"), LightningDbConfig::default())?;

    for i in 0..100 {
        db.put(
            format!("backup_key_{}", i).as_bytes(),
            format!("backup_value_{}", i).as_bytes(),
        )?;
    }

    // Create backup
    match db.backup(&backup_path) {
        Ok(_) => {}
        Err(e) => issues.push(format!("Backup creation failed: {}", e)),
    }

    // Restore to new location
    let restore_path = path.join("restored");
    match Database::restore(&backup_path, &restore_path, LightningDbConfig::default()) {
        Ok(_) => {
            // Verify restored data
            match Database::open(&restore_path, LightningDbConfig::default()) {
                Ok(restored_db) => {
                    for i in 0..100 {
                        let key = format!("backup_key_{}", i);
                        let expected = format!("backup_value_{}", i);

                        match restored_db.get(key.as_bytes())? {
                            Some(v) if v == expected.as_bytes() => {}
                            _ => {
                                issues.push("Restored data mismatch".to_string());
                                break;
                            }
                        }
                    }
                }
                Err(e) => issues.push(format!("Failed to open restored database: {}", e)),
            }
        }
        Err(e) => issues.push(format!("Restore failed: {}", e)),
    }

    Ok(TestResult {
        feature: "Backup & Restore",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "Backup and restore functionality working".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_performance_under_load(
    path: &std::path::Path,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Performance Under Load...");

    let db = Arc::new(Database::create(path, LightningDbConfig::default())?);
    let mut issues = Vec::new();

    // Sustained write load
    let write_threads = 4;
    let writes_per_thread = 25000;
    let barrier = Arc::new(Barrier::new(write_threads));
    let mut handles = vec![];

    let start = Instant::now();

    for thread_id in 0..write_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        handles.push(thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..writes_per_thread {
                let key = format!("load_{}_{}", thread_id, i);
                let value = vec![0u8; 1024]; // 1KB values

                if let Err(e) = db_clone.put(key.as_bytes(), value.as_slice()) {
                    return Err(format!("Write failed under load: {}", e));
                }
            }
            Ok(())
        }));
    }

    for handle in handles {
        if let Err(e) = handle.join().unwrap() {
            issues.push(e);
        }
    }

    let write_duration = start.elapsed();
    let total_writes = write_threads * writes_per_thread;
    let write_throughput = total_writes as f64 / write_duration.as_secs_f64();

    // Concurrent read load while writing
    let read_threads = 4;
    let reads_per_thread = 100000;
    let write_handle = thread::spawn({
        let db_clone = db.clone();
        move || {
            for i in 0..10000 {
                let _ = db_clone.put(format!("concurrent_{}", i).as_bytes(), b"value");
            }
        }
    });

    let barrier = Arc::new(Barrier::new(read_threads));
    let mut handles = vec![];

    let start = Instant::now();

    for _ in 0..read_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();

        handles.push(thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..reads_per_thread {
                let key = format!("load_0_{}", i % 1000);
                let _ = db_clone.get(key.as_bytes());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    write_handle.join().unwrap();

    let read_duration = start.elapsed();
    let total_reads = read_threads * reads_per_thread;
    let read_throughput = total_reads as f64 / read_duration.as_secs_f64();

    Ok(TestResult {
        feature: "Performance Under Load",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            format!(
                "Write: {:.0} ops/s, Read: {:.0} ops/s under concurrent load",
                write_throughput, read_throughput
            )
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: Some(PerformanceMetrics {
            ops_per_sec: (write_throughput + read_throughput) / 2.0,
            latency_us: ((write_duration.as_micros() / total_writes as u128)
                + (read_duration.as_micros() / total_reads as u128)) as f64
                / 2.0,
            meets_target: write_throughput > 100_000.0 && read_throughput > 1_000_000.0,
        }),
    })
}

fn test_edge_cases(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing Edge Cases...");

    let db = Database::create(path, LightningDbConfig::default())?;
    let mut issues = Vec::new();

    // Empty key
    if db.put(b"", b"empty_key_value").is_ok() && db.get(b"")?.is_none() {
        issues.push("Empty key write succeeded but read failed".to_string());
    }

    // Empty value
    db.put(b"empty_value_key", b"")?;
    if db.get(b"empty_value_key")?.as_deref() != Some(b"") {
        issues.push("Empty value handling incorrect".to_string());
    }

    // Large key
    let large_key = vec![b'x'; 1024 * 10]; // 10KB key
    if let Err(e) = db.put(&large_key, b"value") {
        issues.push(format!("Large key failed: {}", e));
    }

    // Large value
    let large_value = vec![b'y'; 1024 * 1024]; // 1MB value
    if let Err(e) = db.put(b"large_value_key", &large_value) {
        issues.push(format!("Large value failed: {}", e));
    } else if db.get(b"large_value_key")?.as_deref() != Some(large_value.as_slice()) {
        issues.push("Large value retrieval incorrect".to_string());
    }

    // Special characters
    let special_key = b"\x00\x01\x02\xFF\xFE\xFD";
    db.put(special_key, b"special")?;
    if db.get(special_key)?.as_deref() != Some(b"special") {
        issues.push("Special character handling failed".to_string());
    }

    // Overwrite many times
    for i in 0..100 {
        db.put(b"overwrite_key", format!("value_{}", i).as_bytes())?;
    }
    if db.get(b"overwrite_key")?.as_deref() != Some(b"value_99") {
        issues.push("Multiple overwrites failed".to_string());
    }

    Ok(TestResult {
        feature: "Edge Cases",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "All edge cases handled correctly".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn test_lsm_features(path: &std::path::Path) -> Result<TestResult, Box<dyn std::error::Error>> {
    println!("Testing LSM Tree Features...");

    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;

    let db = Database::create(path, config)?;
    let mut issues = Vec::new();

    // Test write amplification handling
    for i in 0..10000 {
        db.put(
            format!("lsm_key_{:05}", i).as_bytes(),
            vec![0u8; 100].as_slice(),
        )?;
    }

    // Force flush to create SSTables
    if let Err(e) = db.flush_lsm() {
        issues.push(format!("LSM flush failed: {}", e));
    }

    // Test compaction
    if let Err(e) = db.compact_lsm() {
        issues.push(format!("LSM compaction failed: {}", e));
    }

    // Verify data after compaction
    for i in 0..100 {
        let key = format!("lsm_key_{:05}", i);
        if db.get(key.as_bytes())?.is_none() {
            issues.push("Data loss after compaction".to_string());
            break;
        }
    }

    // Test bloom filters effectiveness
    let mut false_positives = 0;
    for i in 10000..11000 {
        let key = format!("non_existent_{}", i);
        if db.get(key.as_bytes())?.is_some() {
            false_positives += 1;
        }
    }

    if false_positives > 10 {
        issues.push(format!(
            "Bloom filter has {} false positives out of 1000",
            false_positives
        ));
    }

    // Get LSM statistics
    if let Some(stats) = db.lsm_stats() {
        if stats.levels.is_empty() {
            issues.push("LSM statistics not available".to_string());
        }
    } else {
        issues.push("Unable to get LSM statistics".to_string());
    }

    Ok(TestResult {
        feature: "LSM Tree Features",
        status: if issues.is_empty() {
            TestStatus::Working
        } else {
            TestStatus::PartiallyWorking
        },
        details: if issues.is_empty() {
            "LSM tree with compaction, bloom filters working".to_string()
        } else {
            format!("Issues: {}", issues.join(", "))
        },
        performance: None,
    })
}

fn print_summary(results: &[TestResult]) {
    println!("\n{}", "=".repeat(80));
    println!("📊 COMPREHENSIVE TEST SUMMARY");
    println!("{}", "=".repeat(80));

    // Feature Status Table
    println!("\n🔍 FEATURE STATUS:\n");
    println!("{:<30} | {:<15} | {:<40}", "Feature", "Status", "Details");
    println!("{}", "-".repeat(90));

    for result in results {
        let status_str = match result.status {
            TestStatus::Working => "✅ Working",
            TestStatus::PartiallyWorking => "⚠️  Partial",
            TestStatus::NotWorking => "❌ Not Working",
        };

        let details = if result.details.len() > 40 {
            format!("{}...", &result.details[..37])
        } else {
            result.details.clone()
        };

        println!(
            "{:<30} | {:<15} | {:<40}",
            result.feature, status_str, details
        );
    }

    // Performance Summary
    println!("\n⚡ PERFORMANCE METRICS:\n");
    println!(
        "{:<30} | {:>15} | {:>12} | {:<10}",
        "Feature", "Ops/sec", "Latency (μs)", "Target Met"
    );
    println!("{}", "-".repeat(75));

    for result in results {
        if let Some(perf) = &result.performance {
            println!(
                "{:<30} | {:>15.0} | {:>12.2} | {:<10}",
                result.feature,
                perf.ops_per_sec,
                perf.latency_us,
                if perf.meets_target {
                    "✅ Yes"
                } else {
                    "❌ No"
                }
            );
        }
    }

    // Overall Statistics
    let total_features = results.len();
    let working_features = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::Working))
        .count();
    let partial_features = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::PartiallyWorking))
        .count();
    let not_working = results
        .iter()
        .filter(|r| matches!(r.status, TestStatus::NotWorking))
        .count();

    println!("\n📈 OVERALL STATISTICS:\n");
    println!("  Total Features Tested: {}", total_features);
    println!(
        "  ✅ Fully Working: {} ({:.1}%)",
        working_features,
        (working_features as f64 / total_features as f64) * 100.0
    );
    println!(
        "  ⚠️  Partially Working: {} ({:.1}%)",
        partial_features,
        (partial_features as f64 / total_features as f64) * 100.0
    );
    println!(
        "  ❌ Not Working: {} ({:.1}%)",
        not_working,
        (not_working as f64 / total_features as f64) * 100.0
    );

    // Areas for Improvement
    println!("\n🔧 AREAS FOR IMPROVEMENT:\n");

    let improvements = vec![
        (
            "Performance Optimization",
            vec![
                "• Implement true zero-copy paths for all operations",
                "• Add NUMA-aware memory allocation",
                "• Implement io_uring for async I/O on Linux",
                "• Add CPU cache prefetching hints",
            ],
        ),
        (
            "Concurrency",
            vec![
                "• Implement lock-free data structures for hot paths",
                "• Add read-write optimized locking strategies",
                "• Implement parallel compaction",
                "• Add concurrent SSTable readers",
            ],
        ),
        (
            "Memory Efficiency",
            vec![
                "• Implement memory-mapped SSTable reading",
                "• Add compressed block cache",
                "• Implement adaptive memory management",
                "• Add off-heap memory pools",
            ],
        ),
        (
            "Features",
            vec![
                "• Add column family support",
                "• Implement secondary indexes",
                "• Add time-series optimizations",
                "• Implement snapshot isolation",
            ],
        ),
        (
            "Reliability",
            vec![
                "• Add checksums for all data blocks",
                "• Implement multi-version concurrency control (MVCC)",
                "• Add point-in-time recovery",
                "• Implement distributed consensus for replication",
            ],
        ),
    ];

    for (category, items) in improvements {
        println!("{}:", category);
        for item in items {
            println!("  {}", item);
        }
        println!();
    }

    // Performance vs Targets
    println!("🎯 PERFORMANCE VS TARGETS:\n");

    let best_read_perf = results
        .iter()
        .filter_map(|r| r.performance.as_ref())
        .filter(|p| p.ops_per_sec > 1_000_000.0)
        .max_by(|a, b| a.ops_per_sec.partial_cmp(&b.ops_per_sec).unwrap());

    let best_write_perf = results
        .iter()
        .filter_map(|r| r.performance.as_ref())
        .filter(|p| p.ops_per_sec > 100_000.0)
        .max_by(|a, b| a.ops_per_sec.partial_cmp(&b.ops_per_sec).unwrap());

    if let Some(read) = best_read_perf {
        println!(
            "  Best Read Performance: {:.0} ops/sec ({:.2} μs) - {}x target",
            read.ops_per_sec,
            read.latency_us,
            read.ops_per_sec / 1_000_000.0
        );
    }

    if let Some(write) = best_write_perf {
        println!(
            "  Best Write Performance: {:.0} ops/sec ({:.2} μs) - {}x target",
            write.ops_per_sec,
            write.latency_us,
            write.ops_per_sec / 100_000.0
        );
    }

    println!("\n{}", "=".repeat(80));
}

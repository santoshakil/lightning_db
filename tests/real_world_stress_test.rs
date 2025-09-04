use lightning_db::{Config, Database, Error, Transaction, TransactionMode};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use rand::{Rng, thread_rng, distributions::Alphanumeric};

const NUM_THREADS: usize = 50;
const OPS_PER_THREAD: usize = 10000;
const KEY_SPACE: usize = 100000;
const VALUE_SIZE_MIN: usize = 100;
const VALUE_SIZE_MAX: usize = 10000;

struct StressTestMetrics {
    successful_reads: AtomicU64,
    successful_writes: AtomicU64,
    failed_reads: AtomicU64,
    failed_writes: AtomicU64,
    transaction_conflicts: AtomicU64,
    recoveries: AtomicU64,
    data_mismatches: AtomicU64,
    total_bytes_written: AtomicU64,
    total_bytes_read: AtomicU64,
}

impl StressTestMetrics {
    fn new() -> Self {
        Self {
            successful_reads: AtomicU64::new(0),
            successful_writes: AtomicU64::new(0),
            failed_reads: AtomicU64::new(0),
            failed_writes: AtomicU64::new(0),
            transaction_conflicts: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            data_mismatches: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
        }
    }
    
    fn print_summary(&self, duration: Duration) {
        let total_ops = self.successful_reads.load(Ordering::Relaxed) + 
                       self.successful_writes.load(Ordering::Relaxed);
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        
        println!("\n=== Stress Test Results ===");
        println!("Duration: {:.2}s", duration.as_secs_f64());
        println!("Total Operations: {}", total_ops);
        println!("Operations/sec: {:.0}", ops_per_sec);
        println!("Successful Reads: {}", self.successful_reads.load(Ordering::Relaxed));
        println!("Successful Writes: {}", self.successful_writes.load(Ordering::Relaxed));
        println!("Failed Reads: {}", self.failed_reads.load(Ordering::Relaxed));
        println!("Failed Writes: {}", self.failed_writes.load(Ordering::Relaxed));
        println!("Transaction Conflicts: {}", self.transaction_conflicts.load(Ordering::Relaxed));
        println!("Recoveries: {}", self.recoveries.load(Ordering::Relaxed));
        println!("Data Mismatches: {}", self.data_mismatches.load(Ordering::Relaxed));
        println!("Total MB Written: {:.2}", self.total_bytes_written.load(Ordering::Relaxed) as f64 / 1_048_576.0);
        println!("Total MB Read: {:.2}", self.total_bytes_read.load(Ordering::Relaxed) as f64 / 1_048_576.0);
    }
}

fn generate_random_value(size: usize) -> Vec<u8> {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect()
}

fn random_key() -> Vec<u8> {
    let key_num = thread_rng().gen_range(0..KEY_SPACE);
    format!("key_{:08}", key_num).into_bytes()
}

fn stress_worker(
    db: Arc<Database>, 
    metrics: Arc<StressTestMetrics>,
    should_stop: Arc<AtomicBool>,
    worker_id: usize
) {
    let mut rng = thread_rng();
    let mut local_cache: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
    
    while !should_stop.load(Ordering::Relaxed) {
        for _ in 0..OPS_PER_THREAD {
            if should_stop.load(Ordering::Relaxed) {
                break;
            }
            
            let op_type = rng.gen_range(0..100);
            
            match op_type {
                0..=40 => {
                    // Write operation
                    let key = random_key();
                    let value_size = rng.gen_range(VALUE_SIZE_MIN..VALUE_SIZE_MAX);
                    let value = generate_random_value(value_size);
                    
                    match db.begin_transaction(TransactionMode::ReadWrite) {
                        Ok(mut tx) => {
                            match tx.put(&key, &value) {
                                Ok(_) => {
                                    match tx.commit() {
                                        Ok(_) => {
                                            metrics.successful_writes.fetch_add(1, Ordering::Relaxed);
                                            metrics.total_bytes_written.fetch_add(value.len() as u64, Ordering::Relaxed);
                                            local_cache.insert(key, value);
                                        }
                                        Err(_) => {
                                            metrics.transaction_conflicts.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Err(_) => {
                                    metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                41..=80 => {
                    // Read operation
                    let key = random_key();
                    
                    match db.begin_transaction(TransactionMode::ReadOnly) {
                        Ok(tx) => {
                            match tx.get(&key) {
                                Ok(Some(value)) => {
                                    metrics.successful_reads.fetch_add(1, Ordering::Relaxed);
                                    metrics.total_bytes_read.fetch_add(value.len() as u64, Ordering::Relaxed);
                                    
                                    // Verify against local cache if present
                                    if let Some(cached_value) = local_cache.get(&key) {
                                        if &value != cached_value {
                                            metrics.data_mismatches.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Ok(None) => {
                                    metrics.successful_reads.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    metrics.failed_reads.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            metrics.failed_reads.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                81..=90 => {
                    // Range scan
                    let start_key = random_key();
                    let end_key = random_key();
                    
                    match db.begin_transaction(TransactionMode::ReadOnly) {
                        Ok(tx) => {
                            let mut count = 0;
                            let iter = tx.range(&start_key..&end_key);
                            for result in iter.take(100) {
                                if let Ok((_k, v)) = result {
                                    count += 1;
                                    metrics.total_bytes_read.fetch_add(v.len() as u64, Ordering::Relaxed);
                                }
                            }
                            if count > 0 {
                                metrics.successful_reads.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            metrics.failed_reads.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                91..=95 => {
                    // Delete operation
                    let key = random_key();
                    
                    match db.begin_transaction(TransactionMode::ReadWrite) {
                        Ok(mut tx) => {
                            match tx.delete(&key) {
                                Ok(_) => {
                                    match tx.commit() {
                                        Ok(_) => {
                                            metrics.successful_writes.fetch_add(1, Ordering::Relaxed);
                                            local_cache.remove(&key);
                                        }
                                        Err(_) => {
                                            metrics.transaction_conflicts.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Err(_) => {
                                    metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(_) => {
                            metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => {
                    // Batch operation
                    let batch_size = rng.gen_range(5..20);
                    match db.begin_transaction(TransactionMode::ReadWrite) {
                        Ok(mut tx) => {
                            let mut batch_success = true;
                            for _ in 0..batch_size {
                                let key = random_key();
                                let value = generate_random_value(rng.gen_range(VALUE_SIZE_MIN..VALUE_SIZE_MAX));
                                if tx.put(&key, &value).is_err() {
                                    batch_success = false;
                                    break;
                                }
                            }
                            
                            if batch_success {
                                match tx.commit() {
                                    Ok(_) => {
                                        metrics.successful_writes.fetch_add(batch_size as u64, Ordering::Relaxed);
                                    }
                                    Err(_) => {
                                        metrics.transaction_conflicts.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            } else {
                                metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            
            // Occasionally yield to other threads
            if rng.gen_bool(0.01) {
                thread::yield_now();
            }
        }
    }
}

#[test]
fn test_real_world_stress() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 256 * 1024 * 1024, // 256MB cache
        max_concurrent_transactions: 100,
        enable_compression: true,
        compression_level: Some(3),
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let metrics = Arc::new(StressTestMetrics::new());
    let should_stop = Arc::new(AtomicBool::new(false));
    
    let start_time = Instant::now();
    
    // Spawn worker threads
    let mut handles = vec![];
    for worker_id in 0..NUM_THREADS {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        let should_stop_clone = Arc::clone(&should_stop);
        
        let handle = thread::spawn(move || {
            stress_worker(db_clone, metrics_clone, should_stop_clone, worker_id);
        });
        handles.push(handle);
    }
    
    // Run for a fixed duration
    thread::sleep(Duration::from_secs(30));
    should_stop.store(true, Ordering::Relaxed);
    
    // Wait for all workers to finish
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start_time.elapsed();
    metrics.print_summary(duration);
    
    // Verify database integrity
    println!("\nVerifying database integrity...");
    match db.verify_integrity() {
        Ok(_) => println!("✓ Database integrity verified"),
        Err(e) => panic!("Database integrity check failed: {}", e),
    }
    
    // Ensure no data corruption
    assert_eq!(metrics.data_mismatches.load(Ordering::Relaxed), 0, 
               "Data mismatches detected during stress test");
}

#[test]
fn test_crash_recovery_stress() {
    for iteration in 0..5 {
        println!("\n=== Crash Recovery Test Iteration {} ===", iteration + 1);
        
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        
        // Phase 1: Write data
        {
            let config = Config {
                path: path.clone(),
                cache_size: 64 * 1024 * 1024,
                max_concurrent_transactions: 10,
                enable_compression: true,
                compression_level: Some(3),
                fsync_mode: lightning_db::FsyncMode::EveryTransaction,
                page_size: 4096,
                enable_encryption: false,
                encryption_key: None,
            };
            
            let db = Database::open(config).unwrap();
            let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            
            // Write test data
            for i in 0..1000 {
                let key = format!("crash_test_{:06}", i);
                let value = format!("value_{:06}_iteration_{}", i, iteration);
                tx.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            tx.commit().unwrap();
            
            // Simulate abrupt shutdown (drop without explicit close)
            drop(db);
        }
        
        // Phase 2: Recover and verify
        {
            let config = Config {
                path: path.clone(),
                cache_size: 64 * 1024 * 1024,
                max_concurrent_transactions: 10,
                enable_compression: true,
                compression_level: Some(3),
                fsync_mode: lightning_db::FsyncMode::EveryTransaction,
                page_size: 4096,
                enable_encryption: false,
                encryption_key: None,
            };
            
            let db = Database::open(config).unwrap();
            let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
            
            // Verify all data is intact
            for i in 0..1000 {
                let key = format!("crash_test_{:06}", i);
                let expected_value = format!("value_{:06}_iteration_{}", i, iteration);
                
                match tx.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        assert_eq!(value, expected_value.as_bytes(),
                                  "Data corruption detected after recovery");
                    }
                    Ok(None) => {
                        panic!("Missing key after recovery: {}", key);
                    }
                    Err(e) => {
                        panic!("Error reading key after recovery: {}", e);
                    }
                }
            }
            
            println!("✓ All data recovered successfully");
        }
    }
}

#[test]
fn test_memory_pressure() {
    let dir = tempdir().unwrap();
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 16 * 1024 * 1024, // Small cache to force evictions
        max_concurrent_transactions: 50,
        enable_compression: true,
        compression_level: Some(3),
        fsync_mode: lightning_db::FsyncMode::Periodic(Duration::from_secs(1)),
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let metrics = Arc::new(StressTestMetrics::new());
    
    // Create large values to stress memory
    let large_value_size = 1024 * 1024; // 1MB values
    let num_large_values = 100;
    
    println!("Writing large values to stress memory...");
    
    let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
    for i in 0..num_large_values {
        let key = format!("large_key_{:04}", i);
        let value = generate_random_value(large_value_size);
        
        match tx.put(key.as_bytes(), &value) {
            Ok(_) => {
                metrics.successful_writes.fetch_add(1, Ordering::Relaxed);
                metrics.total_bytes_written.fetch_add(value.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                metrics.failed_writes.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        // Commit periodically to avoid transaction size limits
        if i % 10 == 9 {
            match tx.commit() {
                Ok(_) => {},
                Err(e) => println!("Commit failed: {}", e),
            }
            tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        }
    }
    tx.commit().unwrap();
    
    // Now read back with multiple threads to stress cache eviction
    println!("Reading back with high concurrency...");
    
    let mut handles = vec![];
    for thread_id in 0..20 {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                let key_idx = thread_rng().gen_range(0..num_large_values);
                let key = format!("large_key_{:04}", key_idx);
                
                let tx = db_clone.begin_transaction(TransactionMode::ReadOnly).unwrap();
                match tx.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        metrics_clone.successful_reads.fetch_add(1, Ordering::Relaxed);
                        metrics_clone.total_bytes_read.fetch_add(value.len() as u64, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        panic!("Key {} not found", key);
                    }
                    Err(_) => {
                        metrics_clone.failed_reads.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    println!("\nMemory Pressure Test Results:");
    println!("Successful Writes: {}", metrics.successful_writes.load(Ordering::Relaxed));
    println!("Successful Reads: {}", metrics.successful_reads.load(Ordering::Relaxed));
    println!("Failed Operations: {}", 
             metrics.failed_reads.load(Ordering::Relaxed) + metrics.failed_writes.load(Ordering::Relaxed));
    println!("Total MB Written: {:.2}", metrics.total_bytes_written.load(Ordering::Relaxed) as f64 / 1_048_576.0);
    println!("Total MB Read: {:.2}", metrics.total_bytes_read.load(Ordering::Relaxed) as f64 / 1_048_576.0);
}
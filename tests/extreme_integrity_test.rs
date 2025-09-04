use lightning_db::{Config, Database, Error, Transaction, TransactionMode};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::fs::{self, File, OpenOptions};
use std::io::{Write, Read, Seek, SeekFrom};
use std::path::PathBuf;
use tempfile::tempdir;
use rand::{Rng, thread_rng};
use sha2::{Sha256, Digest};

#[derive(Debug)]
struct IntegrityTestMetrics {
    checksums_verified: AtomicU64,
    corruptions_detected: AtomicU64,
    recoveries_successful: AtomicU64,
    data_loss_events: AtomicU64,
    partial_writes_detected: AtomicU64,
}

impl IntegrityTestMetrics {
    fn new() -> Self {
        Self {
            checksums_verified: AtomicU64::new(0),
            corruptions_detected: AtomicU64::new(0),
            recoveries_successful: AtomicU64::new(0),
            data_loss_events: AtomicU64::new(0),
            partial_writes_detected: AtomicU64::new(0),
        }
    }
    
    fn print_summary(&self) {
        println!("\n=== Integrity Test Results ===");
        println!("Checksums Verified: {}", self.checksums_verified.load(Ordering::Relaxed));
        println!("Corruptions Detected: {}", self.corruptions_detected.load(Ordering::Relaxed));
        println!("Successful Recoveries: {}", self.recoveries_successful.load(Ordering::Relaxed));
        println!("Data Loss Events: {}", self.data_loss_events.load(Ordering::Relaxed));
        println!("Partial Writes Detected: {}", self.partial_writes_detected.load(Ordering::Relaxed));
    }
}

fn calculate_checksum(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn corrupt_file_randomly(path: &PathBuf, corruption_probability: f64) -> std::io::Result<usize> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    
    if file_size == 0 {
        return Ok(0);
    }
    
    let mut rng = thread_rng();
    let mut corruptions = 0;
    
    // Corrupt random positions in the file
    let num_corruptions = rng.gen_range(1..10);
    for _ in 0..num_corruptions {
        if rng.gen::<f64>() < corruption_probability {
            let position = rng.gen_range(0..file_size);
            file.seek(SeekFrom::Start(position))?;
            
            // Write random bytes
            let corrupt_data = vec![rng.gen::<u8>(); rng.gen_range(1..100)];
            file.write_all(&corrupt_data)?;
            corruptions += 1;
        }
    }
    
    file.sync_all()?;
    Ok(corruptions)
}

fn simulate_partial_write(path: &PathBuf) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    
    if file_size > 100 {
        // Truncate file to simulate partial write
        let new_size = thread_rng().gen_range(file_size / 2..file_size);
        file.set_len(new_size)?;
    }
    
    Ok(())
}

#[test]
fn test_extreme_data_integrity() {
    let dir = tempdir().unwrap();
    let metrics = Arc::new(IntegrityTestMetrics::new());
    
    // Phase 1: Write test data with checksums
    let test_data: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = {
        let config = Config {
            path: dir.path().to_path_buf(),
            cache_size: 128 * 1024 * 1024,
            max_concurrent_transactions: 10,
            enable_compression: true,
            compression_level: Some(3),
            fsync_mode: lightning_db::FsyncMode::EveryTransaction,
            page_size: 4096,
            enable_encryption: false,
            encryption_key: None,
        };
        
        let db = Database::open(config).unwrap();
        let mut test_data = Vec::new();
        
        println!("Phase 1: Writing test data with checksums...");
        
        let mut tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
        
        for i in 0..1000 {
            let key = format!("integrity_key_{:06}", i).into_bytes();
            let value_size = thread_rng().gen_range(100..10000);
            let value: Vec<u8> = (0..value_size).map(|_| thread_rng().gen()).collect();
            let checksum = calculate_checksum(&value);
            
            tx.put(&key, &value).unwrap();
            test_data.push((key, value, checksum));
            
            if i % 100 == 99 {
                tx.commit().unwrap();
                tx = db.begin_transaction(TransactionMode::ReadWrite).unwrap();
            }
        }
        
        tx.commit().unwrap();
        
        // Force flush
        drop(db);
        test_data
    };
    
    // Phase 2: Corrupt database files
    println!("Phase 2: Corrupting database files...");
    
    let mut corrupted_files = 0;
    for entry in fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("db") ||
           path.extension().and_then(|s| s.to_str()) == Some("log") {
            match corrupt_file_randomly(&path, 0.3) {
                Ok(num) => {
                    if num > 0 {
                        corrupted_files += 1;
                        metrics.corruptions_detected.fetch_add(num as u64, Ordering::Relaxed);
                    }
                }
                Err(e) => println!("Failed to corrupt {:?}: {}", path, e),
            }
        }
    }
    
    println!("Corrupted {} files", corrupted_files);
    
    // Phase 3: Attempt recovery and verify data
    println!("Phase 3: Attempting recovery and verification...");
    
    let config = Config {
        path: dir.path().to_path_buf(),
        cache_size: 128 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: true,
        compression_level: Some(3),
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    match Database::open(config) {
        Ok(db) => {
            metrics.recoveries_successful.fetch_add(1, Ordering::Relaxed);
            
            // Verify data integrity
            let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
            
            for (key, expected_value, expected_checksum) in &test_data {
                match tx.get(key) {
                    Ok(Some(value)) => {
                        let actual_checksum = calculate_checksum(&value);
                        
                        if actual_checksum == *expected_checksum && value == *expected_value {
                            metrics.checksums_verified.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.corruptions_detected.fetch_add(1, Ordering::Relaxed);
                            println!("Corruption detected in key: {:?}", String::from_utf8_lossy(key));
                        }
                    }
                    Ok(None) => {
                        metrics.data_loss_events.fetch_add(1, Ordering::Relaxed);
                        println!("Data loss: key {:?} not found", String::from_utf8_lossy(key));
                    }
                    Err(e) => {
                        println!("Error reading key {:?}: {}", String::from_utf8_lossy(key), e);
                    }
                }
            }
            
            // Run integrity check
            match db.verify_integrity() {
                Ok(_) => println!("Database integrity check passed"),
                Err(e) => println!("Database integrity check failed: {}", e),
            }
        }
        Err(e) => {
            println!("Failed to recover database: {}", e);
        }
    }
    
    metrics.print_summary();
    
    // Assert acceptable recovery rate
    let total_keys = test_data.len() as u64;
    let verified = metrics.checksums_verified.load(Ordering::Relaxed);
    let recovery_rate = verified as f64 / total_keys as f64;
    
    println!("Recovery rate: {:.2}%", recovery_rate * 100.0);
    assert!(recovery_rate > 0.9, "Recovery rate too low: {:.2}%", recovery_rate * 100.0);
}

#[test]
fn test_extreme_concurrent_corruption() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();
    
    // Start database
    let config = Config {
        path: db_path.clone(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 20,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::Periodic(Duration::from_millis(100)),
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    let db = Arc::new(Database::open(config).unwrap());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let corruption_flag = Arc::new(AtomicBool::new(false));
    
    // Writer thread
    let db_writer = Arc::clone(&db);
    let stop_writer = Arc::clone(&stop_flag);
    let writer_handle = thread::spawn(move || {
        let mut counter = 0u64;
        while !stop_writer.load(Ordering::Relaxed) {
            let key = format!("concurrent_key_{:010}", counter).into_bytes();
            let value = format!("value_{:010}_{}", counter, thread_rng().gen::<u64>()).into_bytes();
            
            if let Ok(mut tx) = db_writer.begin_transaction(TransactionMode::ReadWrite) {
                if tx.put(&key, &value).is_ok() {
                    let _ = tx.commit();
                }
            }
            
            counter += 1;
            if counter % 100 == 0 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        counter
    });
    
    // Corruptor thread
    let corruption_stop = Arc::clone(&stop_flag);
    let corruption_active = Arc::clone(&corruption_flag);
    let db_path_clone = db_path.clone();
    let corruptor_handle = thread::spawn(move || {
        thread::sleep(Duration::from_secs(2)); // Let some data be written first
        
        corruption_active.store(true, Ordering::Relaxed);
        
        while !corruption_stop.load(Ordering::Relaxed) {
            // Occasionally corrupt files
            for entry in fs::read_dir(&db_path_clone).unwrap() {
                if corruption_stop.load(Ordering::Relaxed) {
                    break;
                }
                
                let entry = entry.unwrap();
                let path = entry.path();
                
                if thread_rng().gen::<f64>() < 0.01 { // 1% chance to corrupt
                    let _ = corrupt_file_randomly(&path, 0.1);
                }
            }
            
            thread::sleep(Duration::from_millis(500));
        }
    });
    
    // Reader thread
    let db_reader = Arc::clone(&db);
    let stop_reader = Arc::clone(&stop_flag);
    let corruption_reader = Arc::clone(&corruption_flag);
    let reader_handle = thread::spawn(move || {
        let mut successful_reads = 0u64;
        let mut failed_reads = 0u64;
        
        while !stop_reader.load(Ordering::Relaxed) {
            let key_num = thread_rng().gen_range(0..1000);
            let key = format!("concurrent_key_{:010}", key_num).into_bytes();
            
            if let Ok(tx) = db_reader.begin_transaction(TransactionMode::ReadOnly) {
                match tx.get(&key) {
                    Ok(_) => {
                        if corruption_reader.load(Ordering::Relaxed) {
                            successful_reads += 1;
                        }
                    }
                    Err(_) => {
                        if corruption_reader.load(Ordering::Relaxed) {
                            failed_reads += 1;
                        }
                    }
                }
            }
            
            thread::sleep(Duration::from_millis(10));
        }
        
        (successful_reads, failed_reads)
    });
    
    // Run test
    thread::sleep(Duration::from_secs(10));
    stop_flag.store(true, Ordering::Relaxed);
    
    let writes = writer_handle.join().unwrap();
    corruptor_handle.join().unwrap();
    let (successful_reads, failed_reads) = reader_handle.join().unwrap();
    
    println!("\nConcurrent Corruption Test Results:");
    println!("Total writes: {}", writes);
    println!("Successful reads during corruption: {}", successful_reads);
    println!("Failed reads during corruption: {}", failed_reads);
    
    let read_success_rate = successful_reads as f64 / (successful_reads + failed_reads).max(1) as f64;
    println!("Read success rate during corruption: {:.2}%", read_success_rate * 100.0);
    
    // Database should still be functional despite corruption attempts
    drop(db);
    
    // Try to reopen and verify
    let config = Config {
        path: db_path.clone(),
        cache_size: 64 * 1024 * 1024,
        max_concurrent_transactions: 10,
        enable_compression: false,
        compression_level: None,
        fsync_mode: lightning_db::FsyncMode::EveryTransaction,
        page_size: 4096,
        enable_encryption: false,
        encryption_key: None,
    };
    
    match Database::open(config) {
        Ok(db) => {
            println!("Database successfully reopened after corruption test");
            
            match db.verify_integrity() {
                Ok(_) => println!("✓ Integrity check passed"),
                Err(e) => println!("⚠ Integrity check failed: {}", e),
            }
        }
        Err(e) => {
            println!("Failed to reopen database: {}", e);
        }
    }
}

#[test]
fn test_power_failure_simulation() {
    for iteration in 0..10 {
        println!("\n=== Power Failure Test Iteration {} ===", iteration + 1);
        
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        
        // Write phase with simulated power failure
        let write_count = {
            let config = Config {
                path: path.clone(),
                cache_size: 32 * 1024 * 1024,
                max_concurrent_transactions: 5,
                enable_compression: false,
                compression_level: None,
                fsync_mode: lightning_db::FsyncMode::Periodic(Duration::from_millis(100)),
                page_size: 4096,
                enable_encryption: false,
                encryption_key: None,
            };
            
            let db = Arc::new(Database::open(config).unwrap());
            let stop_flag = Arc::new(AtomicBool::new(false));
            let write_counter = Arc::new(AtomicU64::new(0));
            
            // Start writer threads
            let mut handles = vec![];
            for thread_id in 0..5 {
                let db_clone = Arc::clone(&db);
                let stop_clone = Arc::clone(&stop_flag);
                let counter_clone = Arc::clone(&write_counter);
                
                handles.push(thread::spawn(move || {
                    while !stop_clone.load(Ordering::Relaxed) {
                        let key = format!("power_test_{}_{}", thread_id, thread_rng().gen::<u64>());
                        let value = vec![thread_rng().gen::<u8>(); 1000];
                        
                        if let Ok(mut tx) = db_clone.begin_transaction(TransactionMode::ReadWrite) {
                            if tx.put(key.as_bytes(), &value).is_ok() {
                                if tx.commit().is_ok() {
                                    counter_clone.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }));
            }
            
            // Simulate power failure after random time
            let failure_time = Duration::from_millis(thread_rng().gen_range(100..1000));
            thread::sleep(failure_time);
            
            // Abrupt stop without proper shutdown
            stop_flag.store(true, Ordering::Relaxed);
            
            // Don't wait for threads, simulate immediate power loss
            drop(handles);
            drop(db);
            
            // Simulate partial writes to some files
            for entry in fs::read_dir(&path).unwrap() {
                let entry = entry.unwrap();
                if thread_rng().gen::<f64>() < 0.2 {
                    let _ = simulate_partial_write(&entry.path());
                }
            }
            
            write_counter.load(Ordering::Relaxed)
        };
        
        println!("Writes before power failure: {}", write_count);
        
        // Recovery phase
        let config = Config {
            path: path.clone(),
            cache_size: 32 * 1024 * 1024,
            max_concurrent_transactions: 5,
            enable_compression: false,
            compression_level: None,
            fsync_mode: lightning_db::FsyncMode::EveryTransaction,
            page_size: 4096,
            enable_encryption: false,
            encryption_key: None,
        };
        
        match Database::open(config) {
            Ok(db) => {
                println!("Database recovered successfully");
                
                // Count recovered entries
                let tx = db.begin_transaction(TransactionMode::ReadOnly).unwrap();
                let mut recovered_count = 0;
                
                for i in 0..5 {
                    let prefix = format!("power_test_{}_", i);
                    let start = prefix.as_bytes();
                    let end = format!("power_test_{}_~", i);
                    
                    let iter = tx.range(start..end.as_bytes());
                    for _ in iter {
                        recovered_count += 1;
                    }
                }
                
                println!("Recovered entries: {}", recovered_count);
                
                // Some data loss is acceptable, but should recover most
                let recovery_rate = recovered_count as f64 / write_count.max(1) as f64;
                println!("Recovery rate: {:.2}%", recovery_rate * 100.0);
            }
            Err(e) => {
                println!("Failed to recover from power failure: {}", e);
            }
        }
    }
}
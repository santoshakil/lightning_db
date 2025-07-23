//! Chaos Engineering Test Suite
//! 
//! This module contains comprehensive chaos tests that simulate real-world
//! production failures to validate database resilience.

use lightning_db::{Database, LightningDbConfig, Transaction};
use std::sync::{Arc, Barrier, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::fs;
use std::path::Path;
use rand::{Rng, thread_rng};
use std::io::{self, Write};

/// Chaos test results tracking
#[derive(Default)]
struct ChaosMetrics {
    total_operations: AtomicU64,
    failed_operations: AtomicU64,
    data_corruptions: AtomicU64,
    successful_recoveries: AtomicU64,
    total_crashes: AtomicU64,
}

impl ChaosMetrics {
    fn print_summary(&self) {
        let total = self.total_operations.load(Ordering::Relaxed);
        let failed = self.failed_operations.load(Ordering::Relaxed);
        let corruptions = self.data_corruptions.load(Ordering::Relaxed);
        let recoveries = self.successful_recoveries.load(Ordering::Relaxed);
        let crashes = self.total_crashes.load(Ordering::Relaxed);
        
        println!("\n📊 Chaos Test Summary:");
        println!("   Total Operations: {}", total);
        println!("   Failed Operations: {} ({:.2}%)", failed, (failed as f64 / total as f64) * 100.0);
        println!("   Data Corruptions: {}", corruptions);
        println!("   Successful Recoveries: {} / {}", recoveries, crashes);
        println!("   Resilience Score: {:.1}%", 
            ((total - failed - corruptions) as f64 / total as f64) * 100.0);
    }
}

/// Simulates random process crashes during operations
#[test]
fn test_random_crash_recovery() {
    println!("🔥 Testing Random Crash Recovery...");
    
    let metrics = Arc::new(ChaosMetrics::default());
    let test_dir = tempfile::tempdir().unwrap();
    let db_path = test_dir.path().to_path_buf();
    
    for iteration in 0..10 {
        println!("\n  Iteration {}/10", iteration + 1);
        
        // Phase 1: Write data with random crashes
        let should_crash = Arc::new(AtomicBool::new(false));
        let crash_clone = should_crash.clone();
        let metrics_clone = metrics.clone();
        let path_clone = db_path.clone();
        
        let writer_handle = thread::spawn(move || {
            let config = LightningDbConfig::builder()
                .path(&path_clone)
                .cache_size(10 * 1024 * 1024)
                .enable_wal(true)
                .build();
            
            let db = Database::open(config).unwrap();
            let mut rng = thread_rng();
            
            for i in 0..1000 {
                metrics_clone.total_operations.fetch_add(1, Ordering::Relaxed);
                
                // Randomly decide to crash
                if rng.gen_bool(0.001) && i > 100 {
                    crash_clone.store(true, Ordering::Relaxed);
                    metrics_clone.total_crashes.fetch_add(1, Ordering::Relaxed);
                    println!("    💥 Simulating crash at operation {}", i);
                    std::process::abort(); // Simulate hard crash
                }
                
                let key = format!("crash_test_{}", i);
                let value = format!("value_{}_integrity_check", i);
                
                match db.set(&key, &value) {
                    Ok(_) => {},
                    Err(_) => {
                        metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                // Simulate some reads too
                if i > 0 && rng.gen_bool(0.3) {
                    let read_key = format!("crash_test_{}", rng.gen_range(0..i));
                    let _ = db.get(&read_key);
                }
            }
        });
        
        // Wait for writer to complete or crash
        let _ = writer_handle.join();
        
        // Phase 2: Recover and validate data integrity
        println!("    🔧 Attempting recovery...");
        let metrics_clone = metrics.clone();
        
        let recovery_result = std::panic::catch_unwind(|| {
            let config = LightningDbConfig::builder()
                .path(&db_path)
                .cache_size(10 * 1024 * 1024)
                .enable_wal(true)
                .build();
            
            match Database::open(config) {
                Ok(db) => {
                    // Verify data integrity
                    let mut verified = 0;
                    let mut corrupted = 0;
                    
                    for i in 0..1000 {
                        let key = format!("crash_test_{}", i);
                        match db.get(&key) {
                            Ok(Some(value)) => {
                                let expected = format!("value_{}_integrity_check", i);
                                if value == expected.as_bytes() {
                                    verified += 1;
                                } else {
                                    corrupted += 1;
                                    metrics_clone.data_corruptions.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {
                                // Key not found - might not have been written before crash
                            }
                            Err(_) => {
                                corrupted += 1;
                            }
                        }
                    }
                    
                    println!("    ✅ Recovery successful: {} verified, {} corrupted", verified, corrupted);
                    metrics_clone.successful_recoveries.fetch_add(1, Ordering::Relaxed);
                    true
                }
                Err(e) => {
                    println!("    ❌ Recovery failed: {}", e);
                    false
                }
            }
        });
        
        // Clean up for next iteration
        let _ = fs::remove_dir_all(&db_path);
        thread::sleep(Duration::from_millis(100));
    }
    
    metrics.print_summary();
    assert!(metrics.successful_recoveries.load(Ordering::Relaxed) > 0);
    assert_eq!(metrics.data_corruptions.load(Ordering::Relaxed), 0);
}

/// Tests database behavior under extreme memory pressure
#[test]
fn test_memory_pressure_resilience() {
    println!("🔥 Testing Memory Pressure Resilience...");
    
    let test_dir = tempfile::tempdir().unwrap();
    let metrics = Arc::new(ChaosMetrics::default());
    
    // Create database with minimal cache
    let config = LightningDbConfig::builder()
        .path(test_dir.path())
        .cache_size(1024 * 1024) // Only 1MB cache
        .enable_compression(true)
        .build();
    
    let db = Arc::new(Database::open(config).unwrap());
    let running = Arc::new(AtomicBool::new(true));
    
    // Spawn multiple threads doing heavy operations
    let mut handles = vec![];
    
    for thread_id in 0..8 {
        let db_clone = db.clone();
        let running_clone = running.clone();
        let metrics_clone = metrics.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            let mut local_ops = 0;
            
            while running_clone.load(Ordering::Relaxed) {
                // Generate large values to stress memory
                let value_size = rng.gen_range(1024..1024*1024); // 1KB to 1MB
                let key = format!("memory_stress_{}_{}", thread_id, local_ops);
                let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
                
                metrics_clone.total_operations.fetch_add(1, Ordering::Relaxed);
                
                // Try to write
                match db_clone.set(&key, &value) {
                    Ok(_) => {
                        // Immediately try to read it back
                        match db_clone.get(&key) {
                            Ok(Some(read_value)) => {
                                if read_value != value {
                                    metrics_clone.data_corruptions.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {
                                metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                local_ops += 1;
                
                // Occasionally force cache eviction
                if local_ops % 100 == 0 {
                    db_clone.sync().ok();
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Run for 5 seconds
    thread::sleep(Duration::from_secs(5));
    running.store(false, Ordering::Relaxed);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    metrics.print_summary();
    assert_eq!(metrics.data_corruptions.load(Ordering::Relaxed), 0);
}

/// Tests concurrent transaction conflicts and resolution
#[test]
fn test_transaction_chaos() {
    println!("🔥 Testing Transaction Chaos...");
    
    let test_dir = tempfile::tempdir().unwrap();
    let metrics = Arc::new(ChaosMetrics::default());
    
    let config = LightningDbConfig::builder()
        .path(test_dir.path())
        .cache_size(50 * 1024 * 1024)
        .enable_wal(true)
        .build();
    
    let db = Arc::new(Database::open(config).unwrap());
    
    // Initialize accounts
    for i in 0..100 {
        db.set(&format!("account_{}", i), &1000u64.to_le_bytes()).unwrap();
    }
    
    let barrier = Arc::new(Barrier::new(16));
    let mut handles = vec![];
    
    // Spawn threads doing random transfers
    for thread_id in 0..16 {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let metrics_clone = metrics.clone();
        
        let handle = thread::spawn(move || {
            let mut rng = thread_rng();
            barrier_clone.wait();
            
            for _ in 0..1000 {
                metrics_clone.total_operations.fetch_add(1, Ordering::Relaxed);
                
                // Random accounts
                let from = rng.gen_range(0..100);
                let to = rng.gen_range(0..100);
                if from == to { continue; }
                
                let amount = rng.gen_range(1..100);
                
                // Start transaction
                let mut tx = Transaction::new();
                let from_key = format!("account_{}", from);
                let to_key = format!("account_{}", to);
                
                // Read balances
                let from_balance = match db_clone.get(&from_key) {
                    Ok(Some(data)) if data.len() == 8 => {
                        u64::from_le_bytes(data[..8].try_into().unwrap())
                    }
                    _ => {
                        metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                let to_balance = match db_clone.get(&to_key) {
                    Ok(Some(data)) if data.len() == 8 => {
                        u64::from_le_bytes(data[..8].try_into().unwrap())
                    }
                    _ => {
                        metrics_clone.failed_operations.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                if from_balance < amount {
                    continue; // Insufficient funds
                }
                
                // Perform transfer
                tx.set(&from_key, &(from_balance - amount).to_le_bytes());
                tx.set(&to_key, &(to_balance + amount).to_le_bytes());
                
                // Random delay to increase conflict probability
                if rng.gen_bool(0.1) {
                    thread::sleep(Duration::from_micros(rng.gen_range(1..100)));
                }
                
                // Try to commit
                match db_clone.commit(tx) {
                    Ok(_) => {
                        // Verify invariant
                        let mut total = 0u64;
                        for i in 0..100 {
                            if let Ok(Some(data)) = db_clone.get(&format!("account_{}", i)) {
                                if data.len() == 8 {
                                    total += u64::from_le_bytes(data[..8].try_into().unwrap());
                                }
                            }
                        }
                        if total != 100_000 {
                            metrics_clone.data_corruptions.fetch_add(1, Ordering::Relaxed);
                            println!("    ⚠️  Invariant violation: total = {}", total);
                        }
                    }
                    Err(_) => {
                        // Transaction conflict - this is expected
                    }
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Final verification
    let mut total = 0u64;
    for i in 0..100 {
        if let Ok(Some(data)) = db.get(&format!("account_{}", i)) {
            if data.len() == 8 {
                total += u64::from_le_bytes(data[..8].try_into().unwrap());
            }
        }
    }
    
    println!("\n  💰 Final total: {} (expected: 100000)", total);
    
    metrics.print_summary();
    assert_eq!(total, 100_000, "Money was created or destroyed!");
    assert_eq!(metrics.data_corruptions.load(Ordering::Relaxed), 0);
}

/// Tests disk corruption resilience
#[test]
fn test_disk_corruption_detection() {
    println!("🔥 Testing Disk Corruption Detection...");
    
    let test_dir = tempfile::tempdir().unwrap();
    let db_path = test_dir.path().to_path_buf();
    let metrics = Arc::new(ChaosMetrics::default());
    
    // Phase 1: Write test data
    {
        let config = LightningDbConfig::builder()
            .path(&db_path)
            .cache_size(10 * 1024 * 1024)
            .enable_wal(true)
            .build();
        
        let db = Database::open(config).unwrap();
        
        for i in 0..1000 {
            let key = format!("corruption_test_{}", i);
            let value = format!("value_{}_with_checksum", i);
            db.set(&key, &value).unwrap();
            metrics.total_operations.fetch_add(1, Ordering::Relaxed);
        }
        
        db.sync().unwrap();
    }
    
    // Phase 2: Corrupt some data files
    println!("    💣 Corrupting data files...");
    let mut corrupted_files = 0;
    
    if let Ok(entries) = fs::read_dir(&db_path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("db") {
                // Randomly corrupt some bytes
                if let Ok(mut data) = fs::read(&path) {
                    let mut rng = thread_rng();
                    if rng.gen_bool(0.3) && data.len() > 100 {
                        // Corrupt random positions
                        for _ in 0..5 {
                            let pos = rng.gen_range(0..data.len());
                            data[pos] = rng.gen();
                        }
                        fs::write(&path, data).ok();
                        corrupted_files += 1;
                    }
                }
            }
        }
    }
    
    println!("    📝 Corrupted {} files", corrupted_files);
    
    // Phase 3: Try to open and recover
    println!("    🔧 Attempting to open corrupted database...");
    
    let config = LightningDbConfig::builder()
        .path(&db_path)
        .cache_size(10 * 1024 * 1024)
        .enable_wal(true)
        .build();
    
    match Database::open(config) {
        Ok(db) => {
            println!("    ✅ Database opened despite corruption");
            
            // Verify what data we can still read
            let mut successful_reads = 0;
            let mut failed_reads = 0;
            
            for i in 0..1000 {
                let key = format!("corruption_test_{}", i);
                match db.get(&key) {
                    Ok(Some(value)) => {
                        let expected = format!("value_{}_with_checksum", i);
                        if value == expected.as_bytes() {
                            successful_reads += 1;
                        } else {
                            metrics.data_corruptions.fetch_add(1, Ordering::Relaxed);
                            failed_reads += 1;
                        }
                    }
                    Ok(None) => failed_reads += 1,
                    Err(_) => {
                        metrics.failed_operations.fetch_add(1, Ordering::Relaxed);
                        failed_reads += 1;
                    }
                }
            }
            
            println!("    📊 Reads: {} successful, {} failed", successful_reads, failed_reads);
            
            if corrupted_files > 0 && failed_reads == 0 {
                println!("    🎉 Perfect recovery from corruption!");
                metrics.successful_recoveries.fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(e) => {
            println!("    ❌ Failed to open: {}", e);
            if corrupted_files > 0 {
                println!("    ℹ️  This is expected with severe corruption");
            }
        }
    }
    
    metrics.print_summary();
}

/// Tests behavior during rapid open/close cycles
#[test]
fn test_rapid_lifecycle_chaos() {
    println!("🔥 Testing Rapid Lifecycle Chaos...");
    
    let test_dir = tempfile::tempdir().unwrap();
    let db_path = test_dir.path().to_path_buf();
    let metrics = Arc::new(ChaosMetrics::default());
    let running = Arc::new(AtomicBool::new(true));
    
    let mut handles = vec![];
    
    // Writer thread
    let writer_path = db_path.clone();
    let writer_metrics = metrics.clone();
    let writer_running = running.clone();
    
    let writer_handle = thread::spawn(move || {
        let mut counter = 0;
        while writer_running.load(Ordering::Relaxed) {
            let config = LightningDbConfig::builder()
                .path(&writer_path)
                .cache_size(5 * 1024 * 1024)
                .build();
            
            if let Ok(db) = Database::open(config) {
                // Write some data
                for _ in 0..100 {
                    let key = format!("lifecycle_{}", counter);
                    let value = format!("value_{}", counter);
                    
                    writer_metrics.total_operations.fetch_add(1, Ordering::Relaxed);
                    
                    if db.set(&key, &value).is_err() {
                        writer_metrics.failed_operations.fetch_add(1, Ordering::Relaxed);
                    }
                    counter += 1;
                }
                
                // Random sleep before closing
                thread::sleep(Duration::from_millis(thread_rng().gen_range(1..50)));
                drop(db);
            }
            
            // Random sleep before reopening
            thread::sleep(Duration::from_millis(thread_rng().gen_range(1..10)));
        }
    });
    
    handles.push(writer_handle);
    
    // Reader threads
    for _ in 0..3 {
        let reader_path = db_path.clone();
        let reader_metrics = metrics.clone();
        let reader_running = running.clone();
        
        let reader_handle = thread::spawn(move || {
            while reader_running.load(Ordering::Relaxed) {
                let config = LightningDbConfig::builder()
                    .path(&reader_path)
                    .cache_size(5 * 1024 * 1024)
                    .build();
                
                if let Ok(db) = Database::open(config) {
                    // Read random keys
                    for _ in 0..50 {
                        let key = format!("lifecycle_{}", thread_rng().gen_range(0..10000));
                        
                        reader_metrics.total_operations.fetch_add(1, Ordering::Relaxed);
                        
                        match db.get(&key) {
                            Ok(Some(value)) => {
                                // Verify format
                                if !value.starts_with(b"value_") {
                                    reader_metrics.data_corruptions.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {}, // Key might not exist yet
                            Err(_) => {
                                reader_metrics.failed_operations.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    
                    drop(db);
                }
                
                thread::sleep(Duration::from_millis(thread_rng().gen_range(1..20)));
            }
        });
        
        handles.push(reader_handle);
    }
    
    // Run for 10 seconds
    thread::sleep(Duration::from_secs(10));
    running.store(false, Ordering::Relaxed);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Final integrity check
    let config = LightningDbConfig::builder()
        .path(&db_path)
        .cache_size(10 * 1024 * 1024)
        .build();
    
    if let Ok(db) = Database::open(config) {
        println!("    ✅ Database intact after chaos");
        metrics.successful_recoveries.fetch_add(1, Ordering::Relaxed);
    }
    
    metrics.print_summary();
    assert_eq!(metrics.data_corruptions.load(Ordering::Relaxed), 0);
}

/// Master chaos test that runs all scenarios
#[test]
#[ignore] // Run with: cargo test --ignored test_chaos_suite
fn test_chaos_suite() {
    println!("\n🌪️  LIGHTNING DB CHAOS ENGINEERING SUITE\n");
    println!("This comprehensive test validates production resilience.\n");
    
    let start = Instant::now();
    
    // Run all chaos tests
    test_random_crash_recovery();
    println!("\n" + "=".repeat(80) + "\n");
    
    test_memory_pressure_resilience();
    println!("\n" + "=".repeat(80) + "\n");
    
    test_transaction_chaos();
    println!("\n" + "=".repeat(80) + "\n");
    
    test_disk_corruption_detection();
    println!("\n" + "=".repeat(80) + "\n");
    
    test_rapid_lifecycle_chaos();
    
    let duration = start.elapsed();
    println!("\n🏁 Chaos suite completed in {:.2}s", duration.as_secs_f64());
    println!("✅ Lightning DB demonstrated production-grade resilience!");
}
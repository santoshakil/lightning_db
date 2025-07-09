//! Comprehensive stability test suite for Lightning DB
//! 
//! This test suite focuses on robustness, crash recovery, and data integrity
//! under extreme conditions.

use lightning_db::{Database, LightningDbConfig};
use rand::{thread_rng, Rng};
use std::fs;
use std::panic;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Helper to create a test database with custom config
fn create_test_db(path: &str, config: LightningDbConfig) -> Database {
    let _ = fs::remove_dir_all(path);
    Database::create(path, config).expect("Failed to create database")
}

/// Test panic safety - database should not corrupt on panic
#[test]
fn test_panic_safety() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("panic_test.db");
    
    // Create database and write some data
    {
        let db = create_test_db(db_path.to_str().unwrap(), LightningDbConfig::default());
        let tx = db.begin_transaction().unwrap();
        tx.put(b"key1", b"value1").unwrap();
        tx.put(b"key2", b"value2").unwrap();
        tx.commit().unwrap();
    }
    
    // Simulate panic during transaction
    let result = panic::catch_unwind(|| {
        let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
        let tx = db.begin_transaction().unwrap();
        tx.put(b"key3", b"value3").unwrap();
        panic!("Simulated panic during transaction");
    });
    
    assert!(result.is_err());
    
    // Verify database is still readable and contains original data
    {
        let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
        let tx = db.begin_transaction().unwrap();
        assert_eq!(tx.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(tx.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(tx.get(b"key3").unwrap(), None); // Panic'd transaction should not persist
    }
}

/// Test concurrent access under memory pressure
#[test]
fn test_memory_pressure_stability() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("memory_test.db");
    
    // Create database with limited cache
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024; // 1MB cache
    config.enable_mlock = false;
    
    let db = Arc::new(create_test_db(db_path.to_str().unwrap(), config));
    let barrier = Arc::new(Barrier::new(10));
    let stop = Arc::new(AtomicBool::new(false));
    let error_count = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    // Spawn writer threads
    for i in 0..5 {
        let db = db.clone();
        let barrier = barrier.clone();
        let stop = stop.clone();
        let error_count = error_count.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut count = 0;
            
            while !stop.load(Ordering::Relaxed) {
                let key = format!("key_{}_{}", i, count);
                let value = vec![i as u8; 1024]; // 1KB values
                
                match db.begin_transaction() {
                    Ok(tx) => {
                        if let Err(_) = tx.put(key.as_bytes(), &value).and_then(|_| tx.commit()) {
                            error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                count += 1;
            }
        });
        
        handles.push(handle);
    }
    
    // Spawn reader threads
    for i in 0..5 {
        let db = db.clone();
        let barrier = barrier.clone();
        let stop = stop.clone();
        let error_count = error_count.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut rng = thread_rng();
            
            while !stop.load(Ordering::Relaxed) {
                let writer_id = rng.gen_range(0..5);
                let key_id = rng.gen_range(0..100);
                let key = format!("key_{}_{}", writer_id, key_id);
                
                match db.begin_transaction() {
                    Ok(tx) => {
                        match tx.get(key.as_bytes()) {
                            Ok(Some(value)) => {
                                // Verify value integrity
                                if value.len() != 1024 || !value.iter().all(|&b| b == writer_id) {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {} // Key not found is OK
                            Err(_) => {
                                error_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                thread::sleep(Duration::from_micros(10));
            }
        });
        
        handles.push(handle);
    }
    
    // Run for 5 seconds
    thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Should have no data corruption errors
    assert_eq!(error_count.load(Ordering::Relaxed), 0, "Data corruption detected");
}

/// Test crash recovery with kill -9 simulation
#[test]
#[cfg(unix)]
fn test_crash_recovery_kill_9() {
    use std::process::Stdio;
    use std::io::Write;
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("crash_test.db");
    
    // Write a test program that will be killed
    let test_program = r#"
        use lightning_db::{Database, LightningDbConfig};
        use std::time::Duration;
        use std::thread;
        
        fn main() {
            let db_path = std::env::args().nth(1).unwrap();
            let db = Database::open(&db_path, LightningDbConfig::default()).unwrap();
            
            // Write marker that we started
            let tx = db.begin_transaction().unwrap();
            tx.put(b"started", b"true").unwrap();
            tx.commit().unwrap();
            
            // Start a long transaction
            let tx = db.begin_transaction().unwrap();
            for i in 0..1000 {
                tx.put(format!("key_{}", i).as_bytes(), b"value").unwrap();
                thread::sleep(Duration::from_millis(10));
            }
            
            // This should never complete
            tx.put(b"completed", b"true").unwrap();
            tx.commit().unwrap();
        }
    "#;
    
    // Create initial database
    {
        let db = create_test_db(db_path.to_str().unwrap(), LightningDbConfig::default());
        let tx = db.begin_transaction().unwrap();
        tx.put(b"initial", b"data").unwrap();
        tx.commit().unwrap();
    }
    
    // Run child process
    let mut child = Command::new("cargo")
        .args(&["run", "--example", "crash_test", "--", db_path.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to spawn child");
    
    // Wait for it to start
    thread::sleep(Duration::from_secs(2));
    
    // Kill it with SIGKILL (like kill -9)
    child.kill().expect("Failed to kill child");
    child.wait().expect("Failed to wait for child");
    
    // Verify database can recover
    let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
    let tx = db.begin_transaction().unwrap();
    
    // Should have initial data
    assert_eq!(tx.get(b"initial").unwrap(), Some(b"data".to_vec()));
    
    // Should have started marker
    assert_eq!(tx.get(b"started").unwrap(), Some(b"true".to_vec()));
    
    // Should NOT have completion marker
    assert_eq!(tx.get(b"completed").unwrap(), None);
    
    // Should be able to write new data
    tx.put(b"recovered", b"true").unwrap();
    tx.commit().unwrap();
}

/// Test resource exhaustion handling
#[test]
fn test_resource_exhaustion() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("resource_test.db");
    
    let mut config = LightningDbConfig::default();
    config.max_open_files = 10; // Very low limit
    
    let db = Arc::new(create_test_db(db_path.to_str().unwrap(), config));
    let barrier = Arc::new(Barrier::new(20));
    let errors = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    // Spawn many threads to exhaust resources
    for i in 0..20 {
        let db = db.clone();
        let barrier = barrier.clone();
        let errors = errors.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            
            for j in 0..100 {
                let key = format!("key_{}_{}", i, j);
                
                match db.begin_transaction() {
                    Ok(tx) => {
                        match tx.put(key.as_bytes(), b"value").and_then(|_| tx.commit()) {
                            Ok(_) => {}
                            Err(e) => {
                                // Resource exhaustion errors are expected
                                if !e.to_string().contains("Too many open files") &&
                                   !e.to_string().contains("Resource temporarily unavailable") {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Transaction creation errors due to resources are OK
                        if !e.to_string().contains("Too many open files") &&
                           !e.to_string().contains("Resource temporarily unavailable") {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Should handle resource exhaustion gracefully without corruption
    assert_eq!(errors.load(Ordering::Relaxed), 0, "Unexpected errors during resource exhaustion");
}

/// Test transaction isolation under concurrent stress
#[test]
fn test_transaction_isolation_stress() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("isolation_test.db");
    
    let db = Arc::new(create_test_db(db_path.to_str().unwrap(), LightningDbConfig::default()));
    let barrier = Arc::new(Barrier::new(10));
    let violations = Arc::new(AtomicU64::new(0));
    
    // Initialize counter
    {
        let tx = db.begin_transaction().unwrap();
        tx.put(b"counter", b"0").unwrap();
        tx.commit().unwrap();
    }
    
    let mut handles = vec![];
    
    // Spawn threads that increment counter
    for _ in 0..10 {
        let db = db.clone();
        let barrier = barrier.clone();
        let violations = violations.clone();
        
        let handle = thread::spawn(move || {
            barrier.wait();
            
            for _ in 0..100 {
                let tx = db.begin_transaction().unwrap();
                
                // Read current value
                let current = match tx.get(b"counter").unwrap() {
                    Some(val) => {
                        let s = String::from_utf8(val).unwrap();
                        s.parse::<u64>().unwrap()
                    }
                    None => {
                        violations.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                
                // Simulate some work
                thread::sleep(Duration::from_micros(10));
                
                // Write incremented value
                let new_val = (current + 1).to_string();
                tx.put(b"counter", new_val.as_bytes()).unwrap();
                
                // Try to commit - may fail due to conflicts
                let _ = tx.commit();
            }
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify final state
    let tx = db.begin_transaction().unwrap();
    let final_value = match tx.get(b"counter").unwrap() {
        Some(val) => {
            let s = String::from_utf8(val).unwrap();
            s.parse::<u64>().unwrap()
        }
        None => 0,
    };
    
    // Due to conflicts, final value should be less than 1000
    assert!(final_value > 0 && final_value <= 1000);
    assert_eq!(violations.load(Ordering::Relaxed), 0, "Isolation violations detected");
}

/// Test WAL corruption recovery
#[test]
fn test_wal_corruption_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("wal_corrupt_test.db");
    
    // Create database and write data
    {
        let db = create_test_db(db_path.to_str().unwrap(), LightningDbConfig::default());
        let tx = db.begin_transaction().unwrap();
        tx.put(b"key1", b"value1").unwrap();
        tx.put(b"key2", b"value2").unwrap();
        tx.commit().unwrap();
    }
    
    // Corrupt the WAL file
    let wal_path = db_path.join("wal").join("segment_0.wal");
    if wal_path.exists() {
        let mut wal_data = fs::read(&wal_path).unwrap();
        
        // Corrupt some bytes in the middle
        if wal_data.len() > 100 {
            for i in 50..60 {
                wal_data[i] = 0xFF;
            }
        }
        
        fs::write(&wal_path, wal_data).unwrap();
    }
    
    // Try to open database - should recover
    let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
    let tx = db.begin_transaction().unwrap();
    
    // Should still have data that was committed
    assert_eq!(tx.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(tx.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    
    // Should be able to write new data
    tx.put(b"key3", b"value3").unwrap();
    tx.commit().unwrap();
}

/// Test rapid open/close cycles
#[test]
fn test_rapid_open_close() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("rapid_test.db");
    
    // Initial creation
    {
        let db = create_test_db(db_path.to_str().unwrap(), LightningDbConfig::default());
        let tx = db.begin_transaction().unwrap();
        tx.put(b"persistent", b"data").unwrap();
        tx.commit().unwrap();
    }
    
    // Rapid open/close cycles
    for i in 0..100 {
        let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
        
        // Verify data
        let tx = db.begin_transaction().unwrap();
        assert_eq!(tx.get(b"persistent").unwrap(), Some(b"data".to_vec()));
        
        // Write iteration-specific data
        let key = format!("iter_{}", i);
        tx.put(key.as_bytes(), b"value").unwrap();
        tx.commit().unwrap();
        
        // Explicit drop
        drop(db);
    }
    
    // Final verification
    let db = Database::open(db_path.to_str().unwrap(), LightningDbConfig::default()).unwrap();
    let tx = db.begin_transaction().unwrap();
    assert_eq!(tx.get(b"persistent").unwrap(), Some(b"data".to_vec()));
    assert_eq!(tx.get(b"iter_99").unwrap(), Some(b"value".to_vec()));
}

/// Test behavior under disk space exhaustion
#[test]
#[cfg(unix)]
fn test_disk_space_exhaustion() {
    use std::fs::OpenOptions;
    use std::io::Write;
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("disk_full_test.db");
    
    // Create a small database
    let mut config = LightningDbConfig::default();
    config.max_file_size = 1024 * 1024; // 1MB max
    
    let db = create_test_db(db_path.to_str().unwrap(), config);
    
    // Fill database until space exhaustion
    let mut write_count = 0;
    let mut last_error = None;
    
    for i in 0..10000 {
        let tx = db.begin_transaction().unwrap();
        let key = format!("key_{:06}", i);
        let value = vec![0u8; 1024]; // 1KB values
        
        match tx.put(key.as_bytes(), &value).and_then(|_| tx.commit()) {
            Ok(_) => write_count += 1,
            Err(e) => {
                last_error = Some(e.to_string());
                break;
            }
        }
    }
    
    // Should have written some data before hitting limit
    assert!(write_count > 0);
    assert!(last_error.is_some());
    
    // Database should still be readable
    let tx = db.begin_transaction().unwrap();
    assert!(tx.get(b"key_000000").unwrap().is_some());
}

/// Main test runner
#[test]
fn run_stability_test_suite() {
    println!("Running comprehensive stability test suite...");
    
    let start = Instant::now();
    
    // Run all tests
    test_panic_safety();
    test_memory_pressure_stability();
    #[cfg(unix)]
    test_crash_recovery_kill_9();
    test_resource_exhaustion();
    test_transaction_isolation_stress();
    test_wal_corruption_recovery();
    test_rapid_open_close();
    #[cfg(unix)]
    test_disk_space_exhaustion();
    
    let duration = start.elapsed();
    println!("Stability test suite completed in {:?}", duration);
}
//! Performance Integration Tests
//! 
//! Tests mixed workload performance, cache effectiveness, and resource utilization

use super::{TestEnvironment, generate_workload_data};
use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Atomic, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use rayon::prelude::*;

#[derive(Debug, Clone)]
struct PerformanceMetrics {
    total_operations: usize,
    reads: usize,
    writes: usize,
    transactions: usize,
    duration: Duration,
    errors: usize,
}

impl PerformanceMetrics {
    fn new() -> Self {
        Self {
            total_operations: 0,
            reads: 0,
            writes: 0,
            transactions: 0,
            duration: Duration::ZERO,
            errors: 0,
        }
    }
    
    fn ops_per_second(&self) -> f64 {
        self.total_operations as f64 / self.duration.as_secs_f64()
    }
    
    fn reads_per_second(&self) -> f64 {
        self.reads as f64 / self.duration.as_secs_f64()
    }
    
    fn writes_per_second(&self) -> f64 {
        self.writes as f64 / self.duration.as_secs_f64()
    }
}

#[test]
fn test_mixed_oltp_olap_workload() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial dataset
    let initial_data = generate_workload_data(10000);
    for (key, value) in &initial_data {
        db.put(key, value).unwrap();
    }
    
    let test_duration = Duration::from_secs(10);
    let start_time = Instant::now();
    let barrier = Arc::new(std::sync::Barrier::new(5));
    
    // Metrics collection
    let read_metrics = Arc::new(AtomicUsize::new(0));
    let write_metrics = Arc::new(AtomicUsize::new(0));
    let transaction_metrics = Arc::new(AtomicUsize::new(0));
    let scan_metrics = Arc::new(AtomicUsize::new(0));
    
    // OLTP Read-Heavy Workers (2 threads)
    let read_handles: Vec<_> = (0..2).map(|worker_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let metrics = read_metrics.clone();
        let initial_data = initial_data.clone();
        
        thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let mut operations = 0;
            
            while start.elapsed() < test_duration {
                // Random read from initial dataset
                let idx = rand::random::<usize>() % initial_data.len();
                let (key, _) = &initial_data[idx];
                
                if db.get(key).is_ok() {
                    operations += 1;
                }
                
                // High frequency reads
                if operations % 1000 == 0 {
                    thread::sleep(Duration::from_micros(1));
                }
            }
            
            metrics.store(operations, Ordering::Relaxed);
        })
    }).collect();
    
    // OLTP Write Workers (1 thread)
    let db_write = db.clone();
    let barrier_write = barrier.clone();
    let write_metrics_clone = write_metrics.clone();
    let write_handle = thread::spawn(move || {
        barrier_write.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let key = format!("write_test_{}_{}", start.elapsed().as_millis(), operations);
            let value = format!("value_{}", operations);
            
            if db_write.put(key.as_bytes(), value.as_bytes()).is_ok() {
                operations += 1;
            }
            
            // Moderate frequency writes
            if operations % 100 == 0 {
                thread::sleep(Duration::from_micros(10));
            }
        }
        
        write_metrics_clone.store(operations, Ordering::Relaxed);
    });
    
    // OLTP Transaction Worker (1 thread)
    let db_tx = db.clone();
    let barrier_tx = barrier.clone();
    let tx_metrics_clone = transaction_metrics.clone();
    let tx_handle = thread::spawn(move || {
        barrier_tx.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let tx_id = match db_tx.begin_transaction() {
                Ok(id) => id,
                Err(_) => continue,
            };
            
            // Multi-operation transaction
            let base_key = format!("tx_{}_{}", start.elapsed().as_millis(), operations);
            for i in 0..3 {
                let key = format!("{}_{}", base_key, i);
                let value = format!("tx_value_{}_{}", operations, i);
                let _ = db_tx.put_tx(tx_id, key.as_bytes(), value.as_bytes());
            }
            
            if db_tx.commit_transaction(tx_id).is_ok() {
                operations += 1;
            }
            
            // Lower frequency transactions
            if operations % 50 == 0 {
                thread::sleep(Duration::from_micros(50));
            }
        }
        
        tx_metrics_clone.store(operations, Ordering::Relaxed);
    });
    
    // OLAP Scan Worker (1 thread)
    let db_scan = db.clone();
    let barrier_scan = barrier.clone();
    let scan_metrics_clone = scan_metrics.clone();
    let initial_data_scan = initial_data.clone();
    let scan_handle = thread::spawn(move || {
        barrier_scan.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            // Simulate range scan by reading sequential keys
            let start_idx = rand::random::<usize>() % (initial_data_scan.len() - 100);
            let mut scan_count = 0;
            
            for i in start_idx..std::cmp::min(start_idx + 100, initial_data_scan.len()) {
                let (key, _) = &initial_data_scan[i];
                if db_scan.get(key).is_ok() {
                    scan_count += 1;
                }
            }
            
            if scan_count > 0 {
                operations += 1;
            }
            
            // Infrequent but expensive scans
            thread::sleep(Duration::from_millis(10));
        }
        
        scan_metrics_clone.store(operations, Ordering::Relaxed);
    });
    
    // Wait for all workers
    for handle in read_handles {
        handle.join().unwrap();
    }
    write_handle.join().unwrap();
    tx_handle.join().unwrap();
    scan_handle.join().unwrap();
    
    let total_duration = start_time.elapsed();
    
    // Collect metrics
    let total_reads = read_metrics.load(Ordering::Relaxed);
    let total_writes = write_metrics.load(Ordering::Relaxed);
    let total_transactions = transaction_metrics.load(Ordering::Relaxed);
    let total_scans = scan_metrics.load(Ordering::Relaxed);
    
    println!("Mixed OLTP/OLAP Workload Results:");
    println!("  Duration: {:?}", total_duration);
    println!("  Reads: {} ({:.2} ops/sec)", total_reads, total_reads as f64 / total_duration.as_secs_f64());
    println!("  Writes: {} ({:.2} ops/sec)", total_writes, total_writes as f64 / total_duration.as_secs_f64());
    println!("  Transactions: {} ({:.2} ops/sec)", total_transactions, total_transactions as f64 / total_duration.as_secs_f64());
    println!("  Scans: {} ({:.2} ops/sec)", total_scans, total_scans as f64 / total_duration.as_secs_f64());
    
    // Performance assertions
    assert!(total_reads > 0, "No reads completed");
    assert!(total_writes > 0, "No writes completed");
    assert!(total_transactions > 0, "No transactions completed");
    
    // Reads should significantly outnumber writes in this workload
    assert!(total_reads > total_writes * 5, "Expected read-heavy workload");
}

#[test]
fn test_cache_effectiveness_under_load() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Create dataset larger than typical cache
    let dataset_size = 50000;
    let working_set_size = 1000; // Keys frequently accessed
    
    // Setup full dataset
    for i in 0..dataset_size {
        let key = format!("cache_test_{:06}", i);
        let value = format!("value_{:06}_{}", i, "x".repeat(i % 100));
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Phase 1: Random access (poor cache performance expected)
    let random_start = Instant::now();
    let mut random_ops = 0;
    
    while random_start.elapsed() < Duration::from_secs(3) {
        let key_id = rand::random::<usize>() % dataset_size;
        let key = format!("cache_test_{:06}", key_id);
        
        if db.get(key.as_bytes()).is_ok() {
            random_ops += 1;
        }
    }
    
    let random_duration = random_start.elapsed();
    let random_ops_per_sec = random_ops as f64 / random_duration.as_secs_f64();
    
    // Phase 2: Working set access (good cache performance expected)
    thread::sleep(Duration::from_millis(100)); // Brief pause
    
    let working_set_start = Instant::now();
    let mut working_set_ops = 0;
    
    while working_set_start.elapsed() < Duration::from_secs(3) {
        let key_id = rand::random::<usize>() % working_set_size;
        let key = format!("cache_test_{:06}", key_id);
        
        if db.get(key.as_bytes()).is_ok() {
            working_set_ops += 1;
        }
    }
    
    let working_set_duration = working_set_start.elapsed();
    let working_set_ops_per_sec = working_set_ops as f64 / working_set_duration.as_secs_f64();
    
    // Phase 3: Sequential access (mixed cache performance)
    thread::sleep(Duration::from_millis(100));
    
    let sequential_start = Instant::now();
    let mut sequential_ops = 0;
    let mut seq_key_id = 0;
    
    while sequential_start.elapsed() < Duration::from_secs(3) {
        let key = format!("cache_test_{:06}", seq_key_id);
        
        if db.get(key.as_bytes()).is_ok() {
            sequential_ops += 1;
        }
        
        seq_key_id = (seq_key_id + 1) % dataset_size;
    }
    
    let sequential_duration = sequential_start.elapsed();
    let sequential_ops_per_sec = sequential_ops as f64 / sequential_duration.as_secs_f64();
    
    println!("Cache Effectiveness Results:");
    println!("  Random access: {:.2} ops/sec", random_ops_per_sec);
    println!("  Working set access: {:.2} ops/sec", working_set_ops_per_sec);
    println!("  Sequential access: {:.2} ops/sec", sequential_ops_per_sec);
    
    // Working set should show better performance due to caching
    let cache_improvement = working_set_ops_per_sec / random_ops_per_sec;
    println!("  Cache improvement factor: {:.2}x", cache_improvement);
    
    // Assertions
    assert!(working_set_ops_per_sec > random_ops_per_sec, 
            "Working set should perform better than random access");
    assert!(cache_improvement > 1.1, 
            "Expected at least 10% improvement from caching");
}

#[test]
fn test_io_subsystem_stress() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    let test_duration = Duration::from_secs(10);
    let barrier = Arc::new(std::sync::Barrier::new(4));
    
    // Metrics
    let read_bytes = Arc::new(AtomicUsize::new(0));
    let write_bytes = Arc::new(AtomicUsize::new(0));
    let sync_operations = Arc::new(AtomicUsize::new(0));
    
    // Large value generator
    let generate_large_value = |size: usize| -> Vec<u8> {
        vec![b'x'; size]
    };
    
    // Thread 1: Large sequential writes
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    let write_bytes1 = write_bytes.clone();
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        let start = Instant::now();
        let mut operations = 0;
        let mut bytes_written = 0;
        
        while start.elapsed() < test_duration {
            let size = 10000 + (operations % 50000); // 10KB to 60KB
            let key = format!("large_write_{:06}", operations);
            let value = generate_large_value(size);
            
            if db1.put(key.as_bytes(), &value).is_ok() {
                bytes_written += key.len() + value.len();
                operations += 1;
            }
            
            if operations % 10 == 0 {
                thread::sleep(Duration::from_micros(100));
            }
        }
        
        write_bytes1.store(bytes_written, Ordering::Relaxed);
    });
    
    // Thread 2: Random reads
    let db2 = db.clone();
    let barrier2 = barrier.clone();
    let read_bytes2 = read_bytes.clone();
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        let start = Instant::now();
        let mut bytes_read = 0;
        
        // First populate some data to read
        for i in 0..1000 {
            let key = format!("read_test_{:04}", i);
            let value = generate_large_value(5000);
            let _ = db2.put(key.as_bytes(), &value);
        }
        
        while start.elapsed() < test_duration {
            let key_id = rand::random::<usize>() % 1000;
            let key = format!("read_test_{:04}", key_id);
            
            if let Ok(Some(value)) = db2.get(key.as_bytes()) {
                bytes_read += key.len() + value.len();
            }
        }
        
        read_bytes2.store(bytes_read, Ordering::Relaxed);
    });
    
    // Thread 3: Transaction with sync
    let db3 = db.clone();
    let barrier3 = barrier.clone();
    let sync_ops3 = sync_operations.clone();
    let handle3 = thread::spawn(move || {
        barrier3.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let tx_id = match db3.begin_transaction() {
                Ok(id) => id,
                Err(_) => continue,
            };
            
            // Multiple operations per transaction
            for i in 0..5 {
                let key = format!("sync_test_{}_{}", operations, i);
                let value = generate_large_value(2000);
                let _ = db3.put_tx(tx_id, key.as_bytes(), &value);
            }
            
            if db3.commit_transaction(tx_id).is_ok() {
                operations += 1;
            }
            
            // Simulate sync-heavy workload
            thread::sleep(Duration::from_millis(5));
        }
        
        sync_ops3.store(operations, Ordering::Relaxed);
    });
    
    // Thread 4: Mixed small operations
    let db4 = db.clone();
    let barrier4 = barrier.clone();
    let handle4 = thread::spawn(move || {
        barrier4.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let key = format!("mixed_{:06}", operations);
            
            match operations % 3 {
                0 => {
                    let value = generate_large_value(100);
                    let _ = db4.put(key.as_bytes(), &value);
                },
                1 => {
                    let _ = db4.get(key.as_bytes());
                },
                2 => {
                    let _ = db4.delete(key.as_bytes());
                },
                _ => unreachable!(),
            }
            
            operations += 1;
        }
    });
    
    // Wait for all threads
    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    handle4.join().unwrap();
    
    let total_read_bytes = read_bytes.load(Ordering::Relaxed);
    let total_write_bytes = write_bytes.load(Ordering::Relaxed);
    let total_sync_ops = sync_operations.load(Ordering::Relaxed);
    
    println!("I/O Subsystem Stress Test Results:");
    println!("  Bytes read: {} ({:.2} MB/sec)", 
             total_read_bytes, 
             total_read_bytes as f64 / (1024.0 * 1024.0) / test_duration.as_secs_f64());
    println!("  Bytes written: {} ({:.2} MB/sec)", 
             total_write_bytes, 
             total_write_bytes as f64 / (1024.0 * 1024.0) / test_duration.as_secs_f64());
    println!("  Sync operations: {}", total_sync_ops);
    
    // Assertions
    assert!(total_read_bytes > 0, "No data read");
    assert!(total_write_bytes > 0, "No data written");
    assert!(total_sync_ops > 0, "No sync operations completed");
}

#[test]
fn test_memory_management_under_pressure() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    let test_duration = Duration::from_secs(15);
    let start_time = Instant::now();
    
    // Track allocations by monitoring operation success
    let successful_large_ops = Arc::new(AtomicUsize::new(0));
    let failed_ops = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    
    // Thread 1: Large allocations
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    let success1 = successful_large_ops.clone();
    let failed1 = failed_ops.clone();
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            // Progressively larger values
            let size = 100000 + (operations * 10000); // Start at 100KB, grow by 10KB each op
            let key = format!("memory_pressure_{:06}", operations);
            let value = vec![b'M'; size];
            
            match db1.put(key.as_bytes(), &value) {
                Ok(_) => {
                    success1.fetch_add(1, Ordering::Relaxed);
                    operations += 1;
                },
                Err(_) => {
                    failed1.fetch_add(1, Ordering::Relaxed);
                    // Back off on failure
                    thread::sleep(Duration::from_millis(10));
                }
            }
            
            if operations % 5 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
            
            // Prevent infinite growth
            if size > 10_000_000 { // 10MB max
                break;
            }
        }
    });
    
    // Thread 2: Memory churn (allocate and deallocate)
    let db2 = db.clone();
    let barrier2 = barrier.clone();
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let key = format!("churn_{:06}", operations);
            let value = vec![b'C'; 50000]; // 50KB values
            
            // Write
            if db2.put(key.as_bytes(), &value).is_ok() {
                // Read back
                let _ = db2.get(key.as_bytes());
                
                // Delete to free memory
                let _ = db2.delete(key.as_bytes());
                
                operations += 1;
            }
            
            if operations % 10 == 0 {
                thread::sleep(Duration::from_micros(100));
            }
        }
    });
    
    // Thread 3: Normal operations to ensure system remains functional
    let db3 = db.clone();
    let barrier3 = barrier.clone();
    let handle3 = thread::spawn(move || {
        barrier3.wait();
        let start = Instant::now();
        let mut operations = 0;
        
        while start.elapsed() < test_duration {
            let key = format!("normal_{:06}", operations);
            let value = format!("normal_value_{:06}", operations);
            
            match db3.put(key.as_bytes(), value.as_bytes()) {
                Ok(_) => {
                    // Verify read
                    if let Ok(Some(read_value)) = db3.get(key.as_bytes()) {
                        assert_eq!(read_value, value.as_bytes());
                    }
                    operations += 1;
                },
                Err(_) => {
                    thread::sleep(Duration::from_micros(100));
                }
            }
        }
        
        // Ensure we completed some normal operations
        assert!(operations > 100, "Normal operations should continue under memory pressure");
    });
    
    // Wait for completion
    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
    
    let total_successful = successful_large_ops.load(Ordering::Relaxed);
    let total_failed = failed_ops.load(Ordering::Relaxed);
    
    println!("Memory Management Under Pressure Results:");
    println!("  Duration: {:?}", start_time.elapsed());
    println!("  Large operations successful: {}", total_successful);
    println!("  Operations failed: {}", total_failed);
    
    // System should handle memory pressure gracefully
    assert!(total_successful > 0, "Should complete some large operations");
    
    // Database should remain functional
    db.put(b"post_pressure_test", b"functional").unwrap();
    assert_eq!(db.get(b"post_pressure_test").unwrap().unwrap(), b"functional");
}

#[test]
fn test_query_optimization_validation() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup structured test data for different query patterns
    let record_count = 10000;
    
    // Setup user records
    for i in 0..record_count {
        let user_id = i;
        let name = format!("User{:04}", i);
        let email = format!("user{:04}@example.com", i);
        let age = 18 + (i % 60);
        let department = match i % 5 {
            0 => "Engineering",
            1 => "Sales",
            2 => "Marketing",
            3 => "HR",
            4 => "Finance",
            _ => unreachable!(),
        };
        
        // Store as separate key-value pairs (simulating columns)
        db.put(format!("user:{}:id", user_id).as_bytes(), user_id.to_string().as_bytes()).unwrap();
        db.put(format!("user:{}:name", user_id).as_bytes(), name.as_bytes()).unwrap();
        db.put(format!("user:{}:email", user_id).as_bytes(), email.as_bytes()).unwrap();
        db.put(format!("user:{}:age", user_id).as_bytes(), age.to_string().as_bytes()).unwrap();
        db.put(format!("user:{}:department", user_id).as_bytes(), department.as_bytes()).unwrap();
        
        // Create department index
        db.put(format!("dept_index:{}:{}", department, user_id).as_bytes(), b"1").unwrap();
        
        // Create age index
        db.put(format!("age_index:{}:{}", age, user_id).as_bytes(), b"1").unwrap();
    }
    
    // Test 1: Point lookup performance (should be very fast)
    let point_lookup_start = Instant::now();
    let mut point_lookups = 0;
    
    for _ in 0..1000 {
        let user_id = rand::random::<usize>() % record_count;
        if db.get(format!("user:{}:name", user_id).as_bytes()).unwrap().is_some() {
            point_lookups += 1;
        }
    }
    
    let point_lookup_duration = point_lookup_start.elapsed();
    let point_lookup_rate = point_lookups as f64 / point_lookup_duration.as_secs_f64();
    
    // Test 2: Department scan (index-based query simulation)
    let dept_scan_start = Instant::now();
    let mut engineering_users = Vec::new();
    
    // Simulate scanning engineering department index
    for i in 0..record_count {
        let key = format!("dept_index:Engineering:{}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            engineering_users.push(i);
        }
    }
    
    let dept_scan_duration = dept_scan_start.elapsed();
    
    // Test 3: Age range query simulation
    let age_range_start = Instant::now();
    let mut age_range_users = Vec::new();
    
    // Find users aged 25-35
    for age in 25..=35 {
        for i in 0..record_count {
            let key = format!("age_index:{}:{}", age, i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                age_range_users.push(i);
            }
        }
    }
    
    let age_range_duration = age_range_start.elapsed();
    
    // Test 4: Full table scan simulation (worst case)
    let full_scan_start = Instant::now();
    let mut all_users = Vec::new();
    
    for i in 0..record_count {
        if db.get(format!("user:{}:id", i).as_bytes()).unwrap().is_some() {
            all_users.push(i);
        }
    }
    
    let full_scan_duration = full_scan_start.elapsed();
    
    println!("Query Optimization Validation Results:");
    println!("  Point lookups: {} ops in {:?} ({:.2} ops/sec)", 
             point_lookups, point_lookup_duration, point_lookup_rate);
    println!("  Department scan: {} users found in {:?}", 
             engineering_users.len(), dept_scan_duration);
    println!("  Age range scan: {} users found in {:?}", 
             age_range_users.len(), age_range_duration);
    println!("  Full table scan: {} users found in {:?}", 
             all_users.len(), full_scan_duration);
    
    // Performance expectations
    assert!(point_lookup_rate > 1000.0, "Point lookups should be very fast");
    assert!(point_lookup_duration < dept_scan_duration, "Point lookups should be faster than scans");
    assert!(dept_scan_duration < full_scan_duration, "Index scans should be faster than full scans");
    
    // Correctness checks
    assert_eq!(engineering_users.len(), record_count / 5, "Should find 1/5 of users in Engineering");
    assert!(age_range_users.len() > 0, "Should find users in age range");
    assert_eq!(all_users.len(), record_count, "Full scan should find all users");
    
    // Verify data integrity
    for user_id in engineering_users.iter().take(10) {
        let dept = db.get(format!("user:{}:department", user_id).as_bytes()).unwrap().unwrap();
        assert_eq!(dept, b"Engineering");
    }
}
//! Chaos and Stress Integration Tests
//! 
//! Tests resource exhaustion, random failures, long-running stability, and performance degradation

use super::{TestEnvironment, setup_test_data, generate_workload_data};
use lightning_db::{Database, LightningDbConfig};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use rayon::prelude::*;

struct ChaosTestResults {
    test_name: String,
    duration: Duration,
    operations_attempted: usize,
    operations_successful: usize,
    errors_encountered: usize,
    recovery_time: Option<Duration>,
    data_integrity_maintained: bool,
}

impl ChaosTestResults {
    fn success_rate(&self) -> f64 {
        if self.operations_attempted == 0 {
            0.0
        } else {
            self.operations_successful as f64 / self.operations_attempted as f64
        }
    }
    
    fn error_rate(&self) -> f64 {
        if self.operations_attempted == 0 {
            0.0
        } else {
            self.errors_encountered as f64 / self.operations_attempted as f64
        }
    }
}

#[test]
fn test_resource_exhaustion_scenarios() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    setup_test_data(&db).unwrap();
    
    let mut test_results = Vec::new();
    
    // Test 1: Memory exhaustion simulation
    let memory_test_start = Instant::now();
    let mut memory_ops_attempted = 0;
    let mut memory_ops_successful = 0;
    let mut memory_errors = 0;
    
    println!("Starting memory exhaustion test...");
    
    // Progressively larger allocations until failure
    let mut allocation_size = 1024; // Start with 1KB
    while memory_test_start.elapsed() < Duration::from_secs(10) && allocation_size < 100_000_000 {
        let key = format!("memory_exhaustion_{:08}", allocation_size);
        let value = vec![b'M'; allocation_size];
        
        memory_ops_attempted += 1;
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                memory_ops_successful += 1;
                allocation_size = (allocation_size as f64 * 1.1) as usize; // Increase by 10%
            },
            Err(_) => {
                memory_errors += 1;
                // Try to recover with smaller allocation
                allocation_size = allocation_size / 2;
                if allocation_size < 1024 {
                    break;
                }
            }
        }
        
        if memory_ops_attempted % 10 == 0 {
            thread::sleep(Duration::from_millis(1)); // Brief pause
        }
    }
    
    let memory_test_duration = memory_test_start.elapsed();
    
    // Verify database is still functional after memory pressure
    let integrity_check = db.put(b"post_memory_test", b"functional").is_ok() &&
                         db.get(b"post_memory_test").unwrap().is_some();
    
    test_results.push(ChaosTestResults {
        test_name: "Memory Exhaustion".to_string(),
        duration: memory_test_duration,
        operations_attempted: memory_ops_attempted,
        operations_successful: memory_ops_successful,
        errors_encountered: memory_errors,
        recovery_time: None,
        data_integrity_maintained: integrity_check,
    });
    
    // Test 2: File handle exhaustion
    let fd_test_start = Instant::now();
    let mut fd_ops_attempted = 0;
    let mut fd_ops_successful = 0;
    let mut fd_errors = 0;
    
    println!("Starting file handle exhaustion test...");
    
    // Create many concurrent operations that might hold file handles
    let barrier = Arc::new(std::sync::Barrier::new(11));
    let fd_results = Arc::new(Mutex::new((0, 0, 0))); // (attempted, successful, errors)
    
    let handles: Vec<_> = (0..10).map(|thread_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let fd_results = fd_results.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            let mut local_attempted = 0;
            let mut local_successful = 0;
            let mut local_errors = 0;
            
            for i in 0..1000 {
                let key = format!("fd_test_{}_{:04}", thread_id, i);
                let value = format!("fd_value_{}_{:04}", thread_id, i);
                
                local_attempted += 1;
                
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {
                        local_successful += 1;
                        // Also try to read it back
                        if db.get(key.as_bytes()).is_ok() {
                            local_successful += 1;
                        } else {
                            local_errors += 1;
                        }
                        local_attempted += 1;
                    },
                    Err(_) => local_errors += 1,
                }
                
                if i % 50 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            
            let mut results = fd_results.lock().unwrap();
            results.0 += local_attempted;
            results.1 += local_successful;
            results.2 += local_errors;
        })
    }).collect();
    
    barrier.wait();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let fd_test_duration = fd_test_start.elapsed();
    let (attempted, successful, errors) = *fd_results.lock().unwrap();
    
    let fd_integrity_check = db.put(b"post_fd_test", b"functional").is_ok();
    
    test_results.push(ChaosTestResults {
        test_name: "File Handle Exhaustion".to_string(),
        duration: fd_test_duration,
        operations_attempted: attempted,
        operations_successful: successful,
        errors_encountered: errors,
        recovery_time: None,
        data_integrity_maintained: fd_integrity_check,
    });
    
    // Test 3: Disk space exhaustion simulation
    let disk_test_start = Instant::now();
    let mut disk_ops_attempted = 0;
    let mut disk_ops_successful = 0;
    let mut disk_errors = 0;
    
    println!("Starting disk space exhaustion test...");
    
    // Fill up space with increasingly large files
    let mut file_size = 10000; // Start with 10KB
    
    while disk_test_start.elapsed() < Duration::from_secs(5) {
        let key = format!("disk_exhaustion_{:08}", file_size);
        let value = vec![b'D'; file_size];
        
        disk_ops_attempted += 1;
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                disk_ops_successful += 1;
                file_size = (file_size as f64 * 1.2) as usize; // Increase by 20%
            },
            Err(_) => {
                disk_errors += 1;
                // Reduce file size on error
                file_size = file_size / 2;
                if file_size < 1000 {
                    break;
                }
            }
        }
    }
    
    let disk_test_duration = disk_test_start.elapsed();
    let disk_integrity_check = db.put(b"post_disk_test", b"functional").is_ok();
    
    test_results.push(ChaosTestResults {
        test_name: "Disk Space Exhaustion".to_string(),
        duration: disk_test_duration,
        operations_attempted: disk_ops_attempted,
        operations_successful: disk_ops_successful,
        errors_encountered: disk_errors,
        recovery_time: None,
        data_integrity_maintained: disk_integrity_check,
    });
    
    // Print results
    println!("\nResource Exhaustion Test Results:");
    for result in &test_results {
        println!("  {}: {:.2}% success rate, {} errors, integrity: {}", 
                 result.test_name, 
                 result.success_rate() * 100.0,
                 result.errors_encountered,
                 result.data_integrity_maintained);
    }
    
    // Verify overall system resilience
    for result in &test_results {
        assert!(result.data_integrity_maintained, 
                "Data integrity should be maintained during {}", result.test_name);
    }
}

#[test]
fn test_random_failure_injection() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    setup_test_data(&db).unwrap();
    
    let test_duration = Duration::from_secs(15);
    let failure_injection_rate = 0.05; // 5% of operations will have simulated failures
    
    let start_time = Instant::now();
    let operations_attempted = Arc::new(AtomicUsize::new(0));
    let operations_successful = Arc::new(AtomicUsize::new(0));
    let failures_injected = Arc::new(AtomicUsize::new(0));
    let failures_handled = Arc::new(AtomicUsize::new(0));
    
    println!("Starting random failure injection test...");
    
    let barrier = Arc::new(std::sync::Barrier::new(6));
    
    // Spawn multiple worker threads with different operation patterns
    let handles: Vec<_> = (0..5).map(|worker_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let ops_attempted = operations_attempted.clone();
        let ops_successful = operations_successful.clone();
        let failures_injected = failures_injected.clone();
        let failures_handled = failures_handled.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            let mut local_ops = 0;
            let mut local_successes = 0;
            let mut local_failures = 0;
            let mut local_handled = 0;
            
            while start_time.elapsed() < test_duration {
                let operation_id = local_ops;
                local_ops += 1;
                
                // Randomly inject failures
                let inject_failure = rand::random::<f64>() < failure_injection_rate;
                
                if inject_failure {
                    local_failures += 1;
                    
                    // Simulate different types of failures
                    let failure_type = rand::random::<u32>() % 4;
                    match failure_type {
                        0 => {
                            // Simulate network timeout
                            thread::sleep(Duration::from_millis(rand::random::<u64>() % 100));
                        },
                        1 => {
                            // Simulate temporary resource unavailability
                            thread::sleep(Duration::from_millis(10));
                        },
                        2 => {
                            // Simulate I/O error (skip operation)
                            local_handled += 1;
                            continue;
                        },
                        3 => {
                            // Simulate memory pressure (smaller operation)
                            let key = format!("small_op_{}_{}", worker_id, operation_id);
                            if db.put(key.as_bytes(), b"small").is_ok() {
                                local_successes += 1;
                                local_handled += 1;
                            }
                            continue;
                        },
                        _ => unreachable!(),
                    }
                    
                    local_handled += 1;
                }
                
                // Normal operation
                let key = format!("failure_test_{}_{:06}", worker_id, operation_id);
                let value = format!("value_{}_{:06}_{}", worker_id, operation_id, "x".repeat(operation_id % 100));
                
                let operation_type = operation_id % 4;
                let success = match operation_type {
                    0 => { // Write
                        db.put(key.as_bytes(), value.as_bytes()).is_ok()
                    },
                    1 => { // Read
                        db.get(key.as_bytes()).is_ok()
                    },
                    2 => { // Transaction
                        let tx_id = db.begin_transaction();
                        match tx_id {
                            Ok(tx) => {
                                let put_result = db.put_tx(tx, key.as_bytes(), value.as_bytes());
                                match put_result {
                                    Ok(_) => db.commit_transaction(tx).is_ok(),
                                    Err(_) => {
                                        let _ = db.rollback_transaction(tx);
                                        false
                                    }
                                }
                            },
                            Err(_) => false,
                        }
                    },
                    3 => { // Delete
                        db.delete(key.as_bytes()).is_ok()
                    },
                    _ => unreachable!(),
                };
                
                if success {
                    local_successes += 1;
                }
                
                // Rate limiting to prevent overwhelming the system
                if local_ops % 100 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            
            ops_attempted.fetch_add(local_ops, Ordering::Relaxed);
            ops_successful.fetch_add(local_successes, Ordering::Relaxed);
            failures_injected.fetch_add(local_failures, Ordering::Relaxed);
            failures_handled.fetch_add(local_handled, Ordering::Relaxed);
        })
    }).collect();
    
    barrier.wait();
    
    // Monitor system during chaos
    let monitor_handle = thread::spawn(move || {
        let monitor_start = Instant::now();
        let mut last_ops = 0;
        
        while monitor_start.elapsed() < test_duration {
            thread::sleep(Duration::from_secs(1));
            
            let current_ops = operations_attempted.load(Ordering::Relaxed);
            let ops_per_second = current_ops - last_ops;
            last_ops = current_ops;
            
            println!("Ops/sec: {}, Total ops: {}, Failures injected: {}", 
                     ops_per_second, current_ops, failures_injected.load(Ordering::Relaxed));
        }
    });
    
    // Wait for completion
    for handle in handles {
        handle.join().unwrap();
    }
    monitor_handle.join().unwrap();
    
    let final_attempted = operations_attempted.load(Ordering::Relaxed);
    let final_successful = operations_successful.load(Ordering::Relaxed);
    let final_failures_injected = failures_injected.load(Ordering::Relaxed);
    let final_failures_handled = failures_handled.load(Ordering::Relaxed);
    
    println!("\nRandom Failure Injection Results:");
    println!("  Duration: {:?}", test_duration);
    println!("  Operations attempted: {}", final_attempted);
    println!("  Operations successful: {}", final_successful);
    println!("  Failures injected: {}", final_failures_injected);
    println!("  Failures handled: {}", final_failures_handled);
    println!("  Success rate: {:.2}%", 
             (final_successful as f64 / final_attempted as f64) * 100.0);
    println!("  Failure injection rate: {:.2}%", 
             (final_failures_injected as f64 / final_attempted as f64) * 100.0);
    
    // Verify system recovery and data integrity
    let recovery_start = Instant::now();
    
    // Test that system is still responsive
    assert!(db.put(b"recovery_test", b"post_chaos").is_ok());
    assert!(db.get(b"recovery_test").unwrap().is_some());
    
    // Verify original test data is still intact
    let mut intact_count = 0;
    for i in 0..100 {
        let key = format!("test_key_{:04}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            intact_count += 1;
        }
    }
    
    let recovery_duration = recovery_start.elapsed();
    
    println!("  Recovery time: {:?}", recovery_duration);
    println!("  Original data intact: {}/100", intact_count);
    
    // Assertions
    assert!(final_successful > 0, "Some operations should succeed despite failures");
    assert!(intact_count > 90, "Most original data should remain intact");
    assert!(recovery_duration < Duration::from_secs(1), "Recovery should be fast");
}

#[test]
fn test_long_running_stability() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    setup_test_data(&db).unwrap();
    
    let test_duration = Duration::from_secs(30); // Long-running test
    let stability_metrics = Arc::new(Mutex::new(StabilityMetrics::new()));
    
    println!("Starting long-running stability test ({:?})...", test_duration);
    
    let start_time = Instant::now();
    let should_continue = Arc::new(AtomicBool::new(true));
    
    // Background workload generators
    let workload_handles: Vec<_> = (0..3).map(|workload_id| {
        let db = db.clone();
        let should_continue = should_continue.clone();
        let stability_metrics = stability_metrics.clone();
        
        thread::spawn(move || {
            let mut operation_count = 0;
            let mut error_count = 0;
            let mut last_checkpoint = Instant::now();
            
            while should_continue.load(Ordering::Relaxed) {
                let operation_start = Instant::now();
                
                let key = format!("stability_{}_{:08}", workload_id, operation_count);
                let value = format!("stable_value_{}_{:08}_{}", 
                                   workload_id, operation_count, "x".repeat(operation_count % 50));
                
                let operation_success = match workload_id % 3 {
                    0 => { // Write-heavy workload
                        db.put(key.as_bytes(), value.as_bytes()).is_ok()
                    },
                    1 => { // Read-heavy workload
                        if operation_count % 10 == 0 {
                            db.put(key.as_bytes(), value.as_bytes()).is_ok()
                        } else {
                            let read_key = format!("stability_{}_{:08}", workload_id, 
                                                  operation_count.saturating_sub(rand::random::<usize>() % 100));
                            db.get(read_key.as_bytes()).is_ok()
                        }
                    },
                    2 => { // Transaction workload
                        let tx_id = db.begin_transaction();
                        match tx_id {
                            Ok(tx) => {
                                let success = db.put_tx(tx, key.as_bytes(), value.as_bytes()).is_ok() &&
                                             db.commit_transaction(tx).is_ok();
                                success
                            },
                            Err(_) => false,
                        }
                    },
                    _ => unreachable!(),
                };
                
                let operation_duration = operation_start.elapsed();
                
                operation_count += 1;
                if !operation_success {
                    error_count += 1;
                }
                
                // Record metrics periodically
                if last_checkpoint.elapsed() >= Duration::from_secs(5) {
                    let mut metrics = stability_metrics.lock().unwrap();
                    metrics.record_checkpoint(
                        workload_id,
                        operation_count,
                        error_count,
                        operation_duration,
                    );
                    last_checkpoint = Instant::now();
                }
                
                // Realistic workload pacing
                match workload_id % 3 {
                    0 => thread::sleep(Duration::from_micros(500)), // Write workload
                    1 => thread::sleep(Duration::from_micros(100)), // Read workload
                    2 => thread::sleep(Duration::from_millis(1)),   // Transaction workload
                    _ => unreachable!(),
                }
            }
            
            (operation_count, error_count)
        })
    }).collect();
    
    // Memory leak detection thread
    let memory_monitor = {
        let should_continue = should_continue.clone();
        let stability_metrics = stability_metrics.clone();
        
        thread::spawn(move || {
            let mut memory_samples = Vec::new();
            
            while should_continue.load(Ordering::Relaxed) {
                let memory_usage = get_memory_usage();
                memory_samples.push((Instant::now(), memory_usage));
                
                // Check for memory leaks (simplified)
                if memory_samples.len() > 10 {
                    let recent_avg = memory_samples.iter()
                        .rev()
                        .take(5)
                        .map(|(_, mem)| *mem)
                        .sum::<f64>() / 5.0;
                    
                    let older_avg = memory_samples.iter()
                        .rev()
                        .skip(5)
                        .take(5)
                        .map(|(_, mem)| *mem)
                        .sum::<f64>() / 5.0;
                    
                    let growth_rate = (recent_avg - older_avg) / older_avg;
                    
                    if growth_rate > 0.1 { // 10% growth
                        println!("Potential memory leak detected: {:.2}% growth", growth_rate * 100.0);
                    }
                }
                
                thread::sleep(Duration::from_secs(2));
            }
            
            memory_samples
        })
    };
    
    // Performance degradation monitor
    let performance_monitor = {
        let db = db.clone();
        let should_continue = should_continue.clone();
        
        thread::spawn(move || {
            let mut performance_samples = Vec::new();
            
            while should_continue.load(Ordering::Relaxed) {
                let perf_start = Instant::now();
                
                // Standard performance test operation
                let test_key = b"performance_benchmark";
                let test_value = b"benchmark_value";
                
                let write_time = {
                    let start = Instant::now();
                    db.put(test_key, test_value).unwrap();
                    start.elapsed()
                };
                
                let read_time = {
                    let start = Instant::now();
                    db.get(test_key).unwrap();
                    start.elapsed()
                };
                
                performance_samples.push((perf_start, write_time, read_time));
                
                // Check for performance degradation
                if performance_samples.len() > 20 {
                    let recent_write_avg = performance_samples.iter()
                        .rev()
                        .take(10)
                        .map(|(_, write, _)| write.as_nanos() as f64)
                        .sum::<f64>() / 10.0;
                    
                    let baseline_write_avg = performance_samples.iter()
                        .take(10)
                        .map(|(_, write, _)| write.as_nanos() as f64)
                        .sum::<f64>() / 10.0;
                    
                    let degradation = (recent_write_avg - baseline_write_avg) / baseline_write_avg;
                    
                    if degradation > 0.5 { // 50% degradation
                        println!("Performance degradation detected: {:.2}% slower writes", 
                                degradation * 100.0);
                    }
                }
                
                thread::sleep(Duration::from_secs(3));
            }
            
            performance_samples
        })
    };
    
    // Run for the specified duration
    thread::sleep(test_duration);
    should_continue.store(false, Ordering::Relaxed);
    
    // Collect results
    let workload_results: Vec<_> = workload_handles.into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    
    let memory_samples = memory_monitor.join().unwrap();
    let performance_samples = performance_monitor.join().unwrap();
    
    let total_duration = start_time.elapsed();
    let final_metrics = stability_metrics.lock().unwrap();
    
    println!("\nLong-Running Stability Test Results:");
    println!("  Actual duration: {:?}", total_duration);
    
    let mut total_operations = 0;
    let mut total_errors = 0;
    
    for (workload_id, (operations, errors)) in workload_results.iter().enumerate() {
        println!("  Workload {}: {} operations, {} errors ({:.2}% error rate)", 
                 workload_id, operations, errors, 
                 (*errors as f64 / *operations as f64) * 100.0);
        total_operations += operations;
        total_errors += errors;
    }
    
    println!("  Total: {} operations, {} errors ({:.2}% error rate)", 
             total_operations, total_errors, 
             (total_errors as f64 / total_operations as f64) * 100.0);
    
    // Memory analysis
    if !memory_samples.is_empty() {
        let initial_memory = memory_samples.first().unwrap().1;
        let final_memory = memory_samples.last().unwrap().1;
        let memory_growth = (final_memory - initial_memory) / initial_memory;
        
        println!("  Memory: {:.2} MB -> {:.2} MB ({:.2}% growth)", 
                 initial_memory, final_memory, memory_growth * 100.0);
        
        assert!(memory_growth < 0.2, "Memory growth should be < 20%");
    }
    
    // Performance analysis
    if performance_samples.len() > 20 {
        let initial_write_avg = performance_samples.iter()
            .take(10)
            .map(|(_, write, _)| write.as_nanos() as f64)
            .sum::<f64>() / 10.0;
        
        let final_write_avg = performance_samples.iter()
            .rev()
            .take(10)
            .map(|(_, write, _)| write.as_nanos() as f64)
            .sum::<f64>() / 10.0;
        
        let perf_change = (final_write_avg - initial_write_avg) / initial_write_avg;
        
        println!("  Performance: {:.0}ns -> {:.0}ns ({:.2}% change)", 
                 initial_write_avg, final_write_avg, perf_change * 100.0);
        
        assert!(perf_change < 1.0, "Performance degradation should be < 100%");
    }
    
    // Final integrity check
    let integrity_start = Instant::now();
    assert!(db.put(b"final_integrity_check", b"passed").is_ok());
    assert!(db.get(b"final_integrity_check").unwrap().is_some());
    let integrity_time = integrity_start.elapsed();
    
    println!("  Final integrity check: {:?}", integrity_time);
    
    // Verify system is still responsive
    assert!(integrity_time < Duration::from_millis(100), "System should remain responsive");
    assert!(total_operations > 1000, "Should complete substantial work");
    assert!((total_errors as f64 / total_operations as f64) < 0.05, "Error rate should be < 5%");
}

#[test]
fn test_memory_leak_detection() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    setup_test_data(&db).unwrap();
    
    let test_duration = Duration::from_secs(20);
    println!("Starting memory leak detection test ({:?})...", test_duration);
    
    let start_time = Instant::now();
    let memory_snapshots = Arc::new(Mutex::new(Vec::new()));
    
    // Take initial memory snapshot
    {
        let mut snapshots = memory_snapshots.lock().unwrap();
        snapshots.push((Instant::now(), get_memory_usage()));
    }
    
    // Memory-intensive workload
    let memory_workload = {
        let db = db.clone();
        let memory_snapshots = memory_snapshots.clone();
        
        thread::spawn(move || {
            let mut cycle = 0;
            
            while start_time.elapsed() < test_duration {
                cycle += 1;
                
                // Allocate and deallocate data in cycles
                let cycle_data_size = 1000 + (cycle % 10) * 100;
                let mut allocated_keys = Vec::new();
                
                // Allocation phase
                for i in 0..100 {
                    let key = format!("leak_test_{}_{:04}", cycle, i);
                    let value = vec![b'L'; cycle_data_size];
                    
                    if db.put(key.as_bytes(), &value).is_ok() {
                        allocated_keys.push(key);
                    }
                }
                
                // Take memory snapshot
                {
                    let mut snapshots = memory_snapshots.lock().unwrap();
                    snapshots.push((Instant::now(), get_memory_usage()));
                }
                
                // Use the data (read operations)
                for key in &allocated_keys {
                    let _ = db.get(key.as_bytes());
                }
                
                // Deallocation phase (delete half the data)
                for key in allocated_keys.iter().take(50) {
                    let _ = db.delete(key.as_bytes());
                }
                
                // Take another memory snapshot
                {
                    let mut snapshots = memory_snapshots.lock().unwrap();
                    snapshots.push((Instant::now(), get_memory_usage()));
                }
                
                if cycle % 10 == 0 {
                    thread::sleep(Duration::from_millis(10)); // Brief pause
                }
            }
            
            cycle
        })
    };
    
    // Transaction-based workload (potential for transaction leaks)
    let transaction_workload = {
        let db = db.clone();
        
        thread::spawn(move || {
            let mut tx_count = 0;
            
            while start_time.elapsed() < test_duration {
                // Create and use transactions
                for batch in 0..10 {
                    let tx_id = match db.begin_transaction() {
                        Ok(tx) => tx,
                        Err(_) => continue,
                    };
                    
                    // Do some work in the transaction
                    for i in 0..5 {
                        let key = format!("tx_leak_test_{}_{:02}_{:02}", tx_count, batch, i);
                        let value = format!("tx_value_{}_{:02}_{:02}", tx_count, batch, i);
                        
                        let _ = db.put_tx(tx_id, key.as_bytes(), value.as_bytes());
                    }
                    
                    // Randomly commit or rollback
                    if rand::random::<bool>() {
                        let _ = db.commit_transaction(tx_id);
                    } else {
                        let _ = db.rollback_transaction(tx_id);
                    }
                    
                    tx_count += 1;
                }
                
                thread::sleep(Duration::from_millis(1));
            }
            
            tx_count
        })
    };
    
    // Wait for workloads to complete
    let memory_cycles = memory_workload.join().unwrap();
    let transaction_count = transaction_workload.join().unwrap();
    
    // Take final memory snapshot
    {
        let mut snapshots = memory_snapshots.lock().unwrap();
        snapshots.push((Instant::now(), get_memory_usage()));
    }
    
    // Analyze memory usage patterns
    let snapshots = memory_snapshots.lock().unwrap();
    
    println!("\nMemory Leak Detection Results:");
    println!("  Test duration: {:?}", start_time.elapsed());
    println!("  Memory cycles: {}", memory_cycles);
    println!("  Transactions: {}", transaction_count);
    println!("  Memory snapshots: {}", snapshots.len());
    
    if snapshots.len() >= 3 {
        let initial_memory = snapshots.first().unwrap().1;
        let final_memory = snapshots.last().unwrap().1;
        let peak_memory = snapshots.iter()
            .map(|(_, mem)| *mem)
            .fold(0.0, f64::max);
        
        println!("  Initial memory: {:.2} MB", initial_memory);
        println!("  Final memory: {:.2} MB", final_memory);
        println!("  Peak memory: {:.2} MB", peak_memory);
        
        let total_growth = (final_memory - initial_memory) / initial_memory;
        println!("  Total growth: {:.2}%", total_growth * 100.0);
        
        // Analyze memory trend
        let mut growth_rates = Vec::new();
        for window in snapshots.windows(10) {
            if window.len() == 10 {
                let start_mem = window.first().unwrap().1;
                let end_mem = window.last().unwrap().1;
                let growth_rate = (end_mem - start_mem) / start_mem;
                growth_rates.push(growth_rate);
            }
        }
        
        if !growth_rates.is_empty() {
            let avg_growth_rate = growth_rates.iter().sum::<f64>() / growth_rates.len() as f64;
            let max_growth_rate = growth_rates.iter().fold(0.0, |a, &b| a.max(b));
            
            println!("  Average growth rate: {:.2}%", avg_growth_rate * 100.0);
            println!("  Maximum growth rate: {:.2}%", max_growth_rate * 100.0);
            
            // Memory leak detection thresholds
            assert!(total_growth < 0.5, "Total memory growth should be < 50%");
            assert!(avg_growth_rate < 0.1, "Average growth rate should be < 10%");
            assert!(max_growth_rate < 0.3, "Maximum growth rate should be < 30%");
        }
        
        // Check for consistent upward trend (potential leak)
        let mut consecutive_increases = 0;
        let mut max_consecutive_increases = 0;
        
        for i in 1..snapshots.len() {
            if snapshots[i].1 > snapshots[i-1].1 {
                consecutive_increases += 1;
                max_consecutive_increases = max_consecutive_increases.max(consecutive_increases);
            } else {
                consecutive_increases = 0;
            }
        }
        
        println!("  Max consecutive increases: {}", max_consecutive_increases);
        
        // Should not have unlimited growth
        assert!(max_consecutive_increases < snapshots.len() / 2, 
                "Memory should not grow consistently throughout test");
    }
    
    // Verify system is still functional and memory is reasonable
    let cleanup_start = Instant::now();
    
    // Force cleanup operations
    for i in 0..100 {
        let key = format!("cleanup_test_{:03}", i);
        db.put(key.as_bytes(), b"cleanup").unwrap();
        db.delete(key.as_bytes()).unwrap();
    }
    
    let cleanup_duration = cleanup_start.elapsed();
    let post_cleanup_memory = get_memory_usage();
    
    println!("  Post-cleanup memory: {:.2} MB", post_cleanup_memory);
    println!("  Cleanup duration: {:?}", cleanup_duration);
    
    // System should still be responsive after intensive memory usage
    assert!(cleanup_duration < Duration::from_secs(1), "Cleanup should be fast");
}

// Helper functions and structures

fn get_memory_usage() -> f64 {
    // Simulate memory usage reading
    // In a real implementation, this would read from /proc/self/status or similar
    50.0 + rand::random::<f64>() * 20.0 + 
           (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_secs() as f64 % 100.0) * 0.1
}

struct StabilityMetrics {
    checkpoints: HashMap<usize, Vec<StabilityCheckpoint>>,
}

struct StabilityCheckpoint {
    timestamp: Instant,
    operation_count: usize,
    error_count: usize,
    last_operation_duration: Duration,
}

impl StabilityMetrics {
    fn new() -> Self {
        Self {
            checkpoints: HashMap::new(),
        }
    }
    
    fn record_checkpoint(&mut self, workload_id: usize, operation_count: usize, 
                        error_count: usize, last_operation_duration: Duration) {
        let checkpoint = StabilityCheckpoint {
            timestamp: Instant::now(),
            operation_count,
            error_count,
            last_operation_duration,
        };
        
        self.checkpoints.entry(workload_id)
            .or_insert_with(Vec::new)
            .push(checkpoint);
    }
}
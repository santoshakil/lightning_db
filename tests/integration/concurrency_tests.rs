//! Concurrency Integration Tests
//! 
//! Tests multi-threaded operations, lock interactions, and concurrent access patterns

use super::{TestEnvironment, generate_workload_data};
use lightning_db::Database;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use rayon::prelude::*;
use crossbeam_channel::{bounded, select};

#[test]
fn test_concurrent_read_write_workload() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial data
    for i in 0..1000 {
        db.put(format!("key_{:04}", i).as_bytes(), format!("value_{:04}", i).as_bytes()).unwrap();
    }
    
    let barrier = Arc::new(Barrier::new(11)); // 10 workers + 1 coordinator
    let errors = Arc::new(Mutex::new(Vec::new()));
    
    // Spawn read workers
    let read_handles: Vec<_> = (0..5).map(|worker_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let errors = errors.clone();
        
        thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let mut read_count = 0;
            
            while start.elapsed() < Duration::from_secs(5) {
                let key_id = rand::random::<usize>() % 1000;
                let key = format!("key_{:04}", key_id);
                
                match db.get(key.as_bytes()) {
                    Ok(Some(_)) => read_count += 1,
                    Ok(None) => {}, // Key might have been deleted
                    Err(e) => {
                        errors.lock().unwrap().push(format!("Read error: {}", e));
                        break;
                    }
                }
                
                if read_count % 100 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            
            read_count
        })
    }).collect();
    
    // Spawn write workers
    let write_handles: Vec<_> = (0..5).map(|worker_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let errors = errors.clone();
        
        thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let mut write_count = 0;
            
            while start.elapsed() < Duration::from_secs(5) {
                let key_id = rand::random::<usize>() % 1000;
                let key = format!("key_{:04}", key_id);
                let value = format!("updated_value_{}_{}", worker_id, write_count);
                
                match db.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => write_count += 1,
                    Err(e) => {
                        errors.lock().unwrap().push(format!("Write error: {}", e));
                        break;
                    }
                }
                
                if write_count % 50 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            
            write_count
        })
    }).collect();
    
    // Start all workers
    barrier.wait();
    
    // Collect results
    let total_reads: usize = read_handles.into_iter().map(|h| h.join().unwrap()).sum();
    let total_writes: usize = write_handles.into_iter().map(|h| h.join().unwrap()).sum();
    
    let errors = errors.lock().unwrap();
    assert!(errors.is_empty(), "Concurrent operations had errors: {:?}", *errors);
    assert!(total_reads > 0, "No reads performed");
    assert!(total_writes > 0, "No writes performed");
    
    println!("Concurrent workload: {} reads, {} writes", total_reads, total_writes);
}

#[test]
fn test_transaction_isolation() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial data
    db.put(b"isolation_key", b"initial_value").unwrap();
    
    let barrier = Arc::new(Barrier::new(3));
    let results = Arc::new(Mutex::new(Vec::new()));
    
    // Transaction 1: Long-running read
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    let results1 = results.clone();
    let handle1 = thread::spawn(move || {
        let tx_id = db1.begin_transaction().unwrap();
        
        barrier1.wait(); // Sync start
        
        // Read initial value
        let value1 = db1.get_tx(tx_id, b"isolation_key").unwrap().unwrap();
        
        // Sleep to allow other transaction to modify
        thread::sleep(Duration::from_millis(100));
        
        // Read again - should see same value (isolation)
        let value2 = db1.get_tx(tx_id, b"isolation_key").unwrap().unwrap();
        
        db1.commit_transaction(tx_id).unwrap();
        
        results1.lock().unwrap().push((
            String::from_utf8(value1).unwrap(),
            String::from_utf8(value2).unwrap()
        ));
    });
    
    // Transaction 2: Modify value
    let db2 = db.clone();
    let barrier2 = barrier.clone();
    let handle2 = thread::spawn(move || {
        barrier2.wait(); // Sync start
        
        // Small delay to ensure tx1 reads first
        thread::sleep(Duration::from_millis(50));
        
        let tx_id = db2.begin_transaction().unwrap();
        db2.put_tx(tx_id, b"isolation_key", b"modified_value").unwrap();
        db2.commit_transaction(tx_id).unwrap();
    });
    
    // Main thread coordination
    barrier.wait();
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 1);
    let (value1, value2) = &results[0];
    
    // Both reads in tx1 should see the same value (isolation)
    assert_eq!(value1, value2);
    assert_eq!(value1, "initial_value");
    
    // Verify final state
    let final_value = db.get(b"isolation_key").unwrap().unwrap();
    assert_eq!(final_value, b"modified_value");
}

#[test]
fn test_deadlock_detection_and_resolution() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup data for potential deadlock
    db.put(b"resource_a", b"value_a").unwrap();
    db.put(b"resource_b", b"value_b").unwrap();
    
    let barrier = Arc::new(Barrier::new(3));
    let results = Arc::new(Mutex::new(Vec::new()));
    
    // Thread 1: Lock A then B
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    let results1 = results.clone();
    let handle1 = thread::spawn(move || {
        barrier1.wait();
        
        let start = Instant::now();
        match db1.begin_transaction() {
            Ok(tx_id) => {
                match db1.put_tx(tx_id, b"resource_a", b"locked_by_tx1") {
                    Ok(_) => {
                        thread::sleep(Duration::from_millis(100)); // Hold lock
                        
                        match db1.put_tx(tx_id, b"resource_b", b"locked_by_tx1") {
                            Ok(_) => {
                                db1.commit_transaction(tx_id).unwrap();
                                results1.lock().unwrap().push(("tx1", "success", start.elapsed()));
                            },
                            Err(e) => {
                                db1.rollback_transaction(tx_id).unwrap();
                                results1.lock().unwrap().push(("tx1", "deadlock_detected", start.elapsed()));
                            }
                        }
                    },
                    Err(e) => {
                        db1.rollback_transaction(tx_id).unwrap();
                        results1.lock().unwrap().push(("tx1", "lock_failed", start.elapsed()));
                    }
                }
            },
            Err(e) => {
                results1.lock().unwrap().push(("tx1", "tx_failed", start.elapsed()));
            }
        }
    });
    
    // Thread 2: Lock B then A (opposite order)
    let db2 = db.clone();
    let barrier2 = barrier.clone();
    let results2 = results.clone();
    let handle2 = thread::spawn(move || {
        barrier2.wait();
        
        let start = Instant::now();
        match db2.begin_transaction() {
            Ok(tx_id) => {
                match db2.put_tx(tx_id, b"resource_b", b"locked_by_tx2") {
                    Ok(_) => {
                        thread::sleep(Duration::from_millis(100)); // Hold lock
                        
                        match db2.put_tx(tx_id, b"resource_a", b"locked_by_tx2") {
                            Ok(_) => {
                                db2.commit_transaction(tx_id).unwrap();
                                results2.lock().unwrap().push(("tx2", "success", start.elapsed()));
                            },
                            Err(e) => {
                                db2.rollback_transaction(tx_id).unwrap();
                                results2.lock().unwrap().push(("tx2", "deadlock_detected", start.elapsed()));
                            }
                        }
                    },
                    Err(e) => {
                        db2.rollback_transaction(tx_id).unwrap();
                        results2.lock().unwrap().push(("tx2", "lock_failed", start.elapsed()));
                    }
                }
            },
            Err(e) => {
                results2.lock().unwrap().push(("tx2", "tx_failed", start.elapsed()));
            }
        }
    });
    
    barrier.wait();
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    let results = results.lock().unwrap();
    
    // At least one transaction should complete or detect deadlock
    assert!(!results.is_empty());
    
    // Check that system handled potential deadlock gracefully
    let success_count = results.iter().filter(|(_, status, _)| *status == "success").count();
    let deadlock_count = results.iter().filter(|(_, status, _)| *status == "deadlock_detected").count();
    
    // Either both succeed (no actual deadlock) or one detects deadlock
    assert!(success_count >= 1 || deadlock_count >= 1);
    
    println!("Deadlock test results: {:?}", *results);
}

#[test]
fn test_connection_pooling_simulation() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    const POOL_SIZE: usize = 10;
    const OPERATIONS_PER_THREAD: usize = 100;
    
    // Simulate connection pool by sharing database handle
    let barrier = Arc::new(Barrier::new(POOL_SIZE + 1));
    let completion_times = Arc::new(Mutex::new(Vec::new()));
    
    let handles: Vec<_> = (0..POOL_SIZE).map(|worker_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let completion_times = completion_times.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            let start = Instant::now();
            let mut operations = 0;
            
            for i in 0..OPERATIONS_PER_THREAD {
                let key = format!("pool_test_{}_{}", worker_id, i);
                let value = format!("value_{}_{}", worker_id, i);
                
                // Simulate mixed workload
                match i % 4 {
                    0 => { // Read
                        db.get(key.as_bytes()).unwrap();
                    },
                    1 => { // Write
                        db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        operations += 1;
                    },
                    2 => { // Transaction
                        let tx_id = db.begin_transaction().unwrap();
                        db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                        db.commit_transaction(tx_id).unwrap();
                        operations += 1;
                    },
                    3 => { // Delete
                        db.delete(key.as_bytes()).unwrap();
                    }
                }
            }
            
            let elapsed = start.elapsed();
            completion_times.lock().unwrap().push((worker_id, elapsed, operations));
        })
    }).collect();
    
    barrier.wait();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let times = completion_times.lock().unwrap();
    assert_eq!(times.len(), POOL_SIZE);
    
    let total_operations: usize = times.iter().map(|(_, _, ops)| ops).sum();
    let avg_time: Duration = times.iter().map(|(_, time, _)| *time).sum::<Duration>() / POOL_SIZE as u32;
    
    println!("Pool test: {} workers, {} total operations, avg time: {:?}", 
             POOL_SIZE, total_operations, avg_time);
    
    assert!(total_operations > 0);
    assert!(avg_time < Duration::from_secs(10)); // Reasonable performance
}

#[test]
fn test_reader_writer_patterns() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial data
    for i in 0..100 {
        db.put(format!("rw_key_{}", i).as_bytes(), format!("rw_value_{}", i).as_bytes()).unwrap();
    }
    
    let barrier = Arc::new(Barrier::new(21)); // 20 readers + 1 writer
    let read_counts = Arc::new(Mutex::new(Vec::new()));
    let write_count = Arc::new(Mutex::new(0));
    
    // Spawn many readers
    let reader_handles: Vec<_> = (0..20).map(|reader_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let read_counts = read_counts.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            let start = Instant::now();
            let mut reads = 0;
            
            while start.elapsed() < Duration::from_secs(3) {
                let key_id = rand::random::<usize>() % 100;
                let key = format!("rw_key_{}", key_id);
                
                if db.get(key.as_bytes()).unwrap().is_some() {
                    reads += 1;
                }
                
                if reads % 100 == 0 {
                    thread::sleep(Duration::from_micros(1));
                }
            }
            
            read_counts.lock().unwrap().push((reader_id, reads));
        })
    }).collect();
    
    // Single writer
    let db_writer = db.clone();
    let barrier_writer = barrier.clone();
    let write_count_writer = write_count.clone();
    let writer_handle = thread::spawn(move || {
        barrier_writer.wait();
        
        let start = Instant::now();
        let mut writes = 0;
        
        while start.elapsed() < Duration::from_secs(3) {
            let key_id = rand::random::<usize>() % 100;
            let key = format!("rw_key_{}", key_id);
            let value = format!("updated_value_{}", writes);
            
            db_writer.put(key.as_bytes(), value.as_bytes()).unwrap();
            writes += 1;
            
            // Writers are more expensive, so less frequent
            thread::sleep(Duration::from_millis(10));
        }
        
        *write_count_writer.lock().unwrap() = writes;
    });
    
    barrier.wait();
    
    // Wait for completion
    for handle in reader_handles {
        handle.join().unwrap();
    }
    writer_handle.join().unwrap();
    
    let read_counts = read_counts.lock().unwrap();
    let total_reads: usize = read_counts.iter().map(|(_, count)| count).sum();
    let write_count = *write_count.lock().unwrap();
    
    assert!(total_reads > 0, "No reads performed");
    assert!(write_count > 0, "No writes performed");
    
    // Readers should significantly outnumber writes
    assert!(total_reads > write_count * 10, 
            "Expected more reads than writes: {} reads vs {} writes", 
            total_reads, write_count);
    
    println!("Reader-writer test: {} reads, {} writes", total_reads, write_count);
}

#[test]
fn test_high_contention_scenario() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Single hot key that all threads will contend for
    const HOT_KEY: &[u8] = b"hot_contended_key";
    db.put(HOT_KEY, b"0").unwrap();
    
    let thread_count = num_cpus::get();
    let barrier = Arc::new(Barrier::new(thread_count + 1));
    let results = Arc::new(Mutex::new(Vec::new()));
    
    let handles: Vec<_> = (0..thread_count).map(|thread_id| {
        let db = db.clone();
        let barrier = barrier.clone();
        let results = results.clone();
        
        thread::spawn(move || {
            barrier.wait();
            
            let start = Instant::now();
            let mut operations = 0;
            let mut retries = 0;
            
            while start.elapsed() < Duration::from_secs(2) && operations < 50 {
                let tx_id = match db.begin_transaction() {
                    Ok(tx) => tx,
                    Err(_) => {
                        retries += 1;
                        thread::sleep(Duration::from_micros(100));
                        continue;
                    }
                };
                
                // Read current value
                let current_value = match db.get_tx(tx_id, HOT_KEY) {
                    Ok(Some(val)) => String::from_utf8(val).unwrap().parse::<i32>().unwrap_or(0),
                    Ok(None) => 0,
                    Err(_) => {
                        db.rollback_transaction(tx_id).unwrap();
                        retries += 1;
                        continue;
                    }
                };
                
                // Increment
                let new_value = current_value + 1;
                
                match db.put_tx(tx_id, HOT_KEY, new_value.to_string().as_bytes()) {
                    Ok(_) => {
                        match db.commit_transaction(tx_id) {
                            Ok(_) => operations += 1,
                            Err(_) => retries += 1,
                        }
                    },
                    Err(_) => {
                        db.rollback_transaction(tx_id).unwrap();
                        retries += 1;
                    }
                }
                
                if retries > 0 && retries % 10 == 0 {
                    thread::sleep(Duration::from_micros(rand::random::<u64>() % 1000));
                }
            }
            
            results.lock().unwrap().push((thread_id, operations, retries));
        })
    }).collect();
    
    barrier.wait();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let results = results.lock().unwrap();
    let total_operations: usize = results.iter().map(|(_, ops, _)| ops).sum();
    let total_retries: usize = results.iter().map(|(_, _, retries)| retries).sum();
    
    // Verify final value matches total operations
    let final_value = db.get(HOT_KEY).unwrap().unwrap();
    let final_count = String::from_utf8(final_value).unwrap().parse::<i32>().unwrap();
    
    assert_eq!(final_count as usize, total_operations, 
               "Final count doesn't match total operations");
    
    println!("High contention test: {} operations, {} retries across {} threads", 
             total_operations, total_retries, thread_count);
    
    // System should handle contention gracefully
    assert!(total_operations > 0, "No operations completed under high contention");
}
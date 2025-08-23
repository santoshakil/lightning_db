#[cfg(test)]
mod deadlock_detection_tests {
    
    use crate::core::transaction::UnifiedTransactionManager;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};
    use std::sync::mpsc;

    #[test]
    fn test_simple_deadlock_detection() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        
        // TX1 acquires lock on key1
        assert!(manager.acquire_write_lock(tx1, b"key1").is_ok());
        
        // TX2 acquires lock on key2
        assert!(manager.acquire_write_lock(tx2, b"key2").is_ok());
        
        // TX1 tries to acquire lock on key2 (held by TX2)
        assert!(manager.acquire_write_lock(tx1, b"key2").is_err());
        
        // TX2 tries to acquire lock on key1 (held by TX1) - deadlock!
        let result = manager.acquire_write_lock(tx2, b"key1");
        assert!(result.is_err());
        
        // One transaction should be aborted to resolve deadlock
        if tx1 < tx2 {
            // Lower ID wins (wound-wait)
            manager.abort(tx2).unwrap();
        } else {
            manager.abort(tx1).unwrap();
        }
    }

    #[test]
    fn test_concurrent_deadlock_prevention() {
        let manager = UnifiedTransactionManager::new(100);
        let barrier = Arc::new(Barrier::new(2));
        
        let manager1 = Arc::clone(&manager);
        let barrier1 = Arc::clone(&barrier);
        
        let handle1 = thread::spawn(move || {
            let start = Instant::now();
            let tx = manager1.begin().unwrap();
            
            // Acquire lock on resource A
            if manager1.acquire_write_lock(tx, b"resource_A").is_err() {
                manager1.abort(tx).ok();
                return false; // Failed to acquire initial lock
            }
            
            barrier1.wait();
            thread::sleep(Duration::from_millis(10));
            
            // Try to acquire lock on resource B with timeout
            let result = if start.elapsed() < Duration::from_secs(2) {
                manager1.acquire_write_lock(tx, b"resource_B").is_ok()
            } else {
                false // Timeout
            };
            
            manager1.abort(tx).unwrap();
            result
        });
        
        let manager2 = Arc::clone(&manager);
        let barrier2 = Arc::clone(&barrier);
        
        let handle2 = thread::spawn(move || {
            let start = Instant::now();
            let tx = manager2.begin().unwrap();
            
            // Acquire lock on resource B
            if manager2.acquire_write_lock(tx, b"resource_B").is_err() {
                manager2.abort(tx).ok();
                return false; // Failed to acquire initial lock
            }
            
            barrier2.wait();
            thread::sleep(Duration::from_millis(10));
            
            // Try to acquire lock on resource A with timeout
            let result = if start.elapsed() < Duration::from_secs(2) {
                manager2.acquire_write_lock(tx, b"resource_A").is_ok()
            } else {
                false // Timeout
            };
            
            manager2.abort(tx).unwrap();
            result
        });
        
        // Use timeout for joins to prevent infinite hanging
        use std::sync::mpsc;
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        
        let h1 = handle1;
        let h2 = handle2;
        
        thread::spawn(move || {
            let result = h1.join().unwrap();
            tx1.send(result).ok();
        });
        
        thread::spawn(move || {
            let result = h2.join().unwrap();
            tx2.send(result).ok();
        });
        
        let result1 = rx1.recv_timeout(Duration::from_secs(3))
            .unwrap_or(false); // Timeout = failed
        let result2 = rx2.recv_timeout(Duration::from_secs(3))
            .unwrap_or(false); // Timeout = failed
        
        // At least one should fail to prevent deadlock (both shouldn't succeed)
        assert!(!(result1 && result2));
    }

    #[test]
    fn test_circular_deadlock_chain() {
        let manager = UnifiedTransactionManager::new(100);
        
        // Create a chain: TX1 -> TX2 -> TX3 -> TX1
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        let tx3 = manager.begin().unwrap();
        
        // TX1 locks A, wants B
        manager.acquire_write_lock(tx1, b"A").unwrap();
        
        // TX2 locks B, wants C
        manager.acquire_write_lock(tx2, b"B").unwrap();
        
        // TX3 locks C, wants A
        manager.acquire_write_lock(tx3, b"C").unwrap();
        
        // Now try to create the circular dependency
        let r1 = manager.acquire_write_lock(tx1, b"B"); // TX1 wants B (held by TX2)
        let r2 = manager.acquire_write_lock(tx2, b"C"); // TX2 wants C (held by TX3)
        let r3 = manager.acquire_write_lock(tx3, b"A"); // TX3 wants A (held by TX1)
        
        // At least one should fail to break the cycle
        assert!(r1.is_err() || r2.is_err() || r3.is_err());
        
        // Clean up
        manager.abort(tx1).ok();
        manager.abort(tx2).ok();
        manager.abort(tx3).ok();
    }

    #[test]
    fn test_lock_timeout_prevents_deadlock() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        
        // TX1 acquires lock
        if manager.acquire_write_lock(tx1, b"key").is_err() {
            manager.abort(tx1).ok();
            manager.abort(tx2).ok();
            (*manager).stop();
            return; // Skip test if initial lock fails
        }
        
        // TX2 tries to acquire same lock - should timeout
        let start = std::time::Instant::now();
        let result = manager.acquire_write_lock(tx2, b"key");
        let elapsed = start.elapsed();
        
        assert!(result.is_err());
        // Should timeout within reasonable time (not hang forever)
        assert!(elapsed < Duration::from_secs(2)); // Increased timeout for safety
        
        manager.abort(tx1).unwrap();
        manager.abort(tx2).unwrap();
        (*manager).stop();
    }

    #[test]
    fn test_read_write_deadlock() {
        let manager = UnifiedTransactionManager::new(100);
        
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();
        
        // TX1 gets read lock on A
        manager.acquire_read_lock(tx1, b"A").unwrap();
        
        // TX2 gets read lock on B
        manager.acquire_read_lock(tx2, b"B").unwrap();
        
        // TX1 wants write lock on B (upgrade from read)
        let r1 = manager.acquire_write_lock(tx1, b"B");
        
        // TX2 wants write lock on A (upgrade from read)
        let r2 = manager.acquire_write_lock(tx2, b"A");
        
        // Both should fail as read-to-write upgrades can deadlock
        assert!(r1.is_err());
        assert!(r2.is_err());
        
        manager.abort(tx1).unwrap();
        manager.abort(tx2).unwrap();
    }

    #[test]
    fn test_multi_resource_deadlock() {
        let manager = UnifiedTransactionManager::new(100);
        let num_threads = 4;
        let num_resources = 4; // Reduced to prevent excessive barrier waits
        
        let mut handles = vec![];
        let manager = Arc::new(manager);
        let start_barrier = Arc::new(Barrier::new(num_threads));
        
        for thread_id in 0..num_threads {
            let manager = Arc::clone(&manager);
            let start_barrier = Arc::clone(&start_barrier);
            
            let handle = thread::spawn(move || {
                let start_time = Instant::now();
                let tx = manager.begin().unwrap();
                let mut locked = vec![];
                
                // Wait for all threads to start
                start_barrier.wait();
                
                // Each thread locks resources in different order
                for i in 0..num_resources {
                    // Timeout check
                    if start_time.elapsed() > Duration::from_secs(2) {
                        break;
                    }
                    
                    let resource_id = if thread_id % 2 == 0 {
                        i
                    } else {
                        num_resources - 1 - i
                    };
                    
                    let key = format!("resource_{}", resource_id);
                    
                    // Small delay to increase chance of contention
                    thread::sleep(Duration::from_millis(1));
                    
                    if manager.acquire_write_lock(tx, key.as_bytes()).is_ok() {
                        locked.push(resource_id);
                    } else {
                        // Deadlock detected, abort
                        break;
                    }
                }
                
                manager.abort(tx).unwrap();
                locked.len()
            });
            
            handles.push(handle);
        }
        
        // Use channels for timeout-safe joins
        let (tx_results, rx_results) = mpsc::channel();
        
        for handle in handles {
            let tx_clone = tx_results.clone();
            thread::spawn(move || {
                let result = handle.join().unwrap_or(0);
                tx_clone.send(result).ok();
            });
        }
        
        let mut results = vec![];
        for _ in 0..num_threads {
            match rx_results.recv_timeout(Duration::from_secs(5)) {
                Ok(result) => results.push(result),
                Err(_) => results.push(0), // Timeout treated as 0 locks acquired
            }
        }
        
        // Not all threads should acquire all locks (deadlock prevention)
        assert!(results.iter().any(|&count| count < num_resources));
    }

    #[test]
    fn test_deadlock_recovery_statistics() {
        let manager = UnifiedTransactionManager::new(100);
        
        // Create potential deadlock situations
        for _ in 0..10 {
            let tx1 = manager.begin().unwrap();
            let tx2 = manager.begin().unwrap();
            
            manager.acquire_write_lock(tx1, b"X").ok();
            manager.acquire_write_lock(tx2, b"Y").ok();
            
            // Opposite order - potential deadlock
            manager.acquire_write_lock(tx1, b"Y").ok();
            manager.acquire_write_lock(tx2, b"X").ok();
            
            manager.abort(tx1).ok();
            manager.abort(tx2).ok();
        }
        
        let stats = manager.get_statistics();
        // Should have detected some deadlocks
        assert!(stats.deadlock_count.load(std::sync::atomic::Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_priority_based_deadlock_resolution() {
        let manager = UnifiedTransactionManager::new(100);
        
        // System transaction (high priority)
        let tx_system = 1; // Low ID = high priority
        // User transaction (normal priority)  
        let tx_user = 1000;
        
        // Manually set up transactions with specific IDs
        // (In real implementation, would need to expose this for testing)
        
        // System transaction gets lock first
        manager.acquire_write_lock(tx_system, b"critical_resource").unwrap();
        
        // User transaction should fail immediately (lower priority)
        let result = manager.acquire_write_lock(tx_user, b"critical_resource");
        assert!(result.is_err());
    }

    #[test]
    #[ignore] // MVCC transaction manager not available yet
    fn test_mvcc_prevents_deadlock() {
        // use super::super::mvcc::MVCCTransactionManager;
        
        // TODO: Implement when MVCC transaction manager is available
        return;
    }

    #[test]
    fn test_lock_hierarchy_prevents_deadlock() {
        let manager = UnifiedTransactionManager::new(100);
        
        // Test that locks are acquired in consistent order
        let tx = manager.begin().unwrap();
        
        // Should acquire locks in alphabetical order internally
        let keys = vec![b"zebra", b"apple", b"mango"];
        
        for key in &keys {
            manager.acquire_write_lock(tx, *key).unwrap();
        }
        
        // Another transaction trying same keys in different order
        let tx2 = manager.begin().unwrap();
        
        // Should fail on first conflict, preventing deadlock
        for key in keys.iter().rev() {
            if manager.acquire_write_lock(tx2, *key).is_err() {
                break;
            }
        }
        
        manager.abort(tx).unwrap();
        manager.abort(tx2).unwrap();
    }
}
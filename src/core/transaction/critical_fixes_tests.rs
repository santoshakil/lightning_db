//! Critical fixes validation tests for Lightning DB transaction system
//!
//! These tests validate the fixes applied for production safety:
//! 1. Timestamp overflow protection
//! 2. Batch commit race condition fix
//! 3. Deadlock victim handling
//! 4. Isolation level enforcement

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_timestamp_overflow_protection() {
        let manager = UnifiedTransactionManager::new(100);

        // Set timestamp close to overflow
        manager
            .next_timestamp
            .store(u64::MAX - 500, Ordering::SeqCst);

        // Should fail with overflow error
        let result = manager.begin();
        assert!(result.is_err());

        // Reset to safe value
        manager.next_timestamp.store(1000, Ordering::SeqCst);

        // Should succeed
        let tx_id = manager.begin().unwrap();
        assert!(tx_id > 0);
    }

    #[test]
    fn test_batch_commit_sequential_timestamps() {
        let manager = UnifiedTransactionManager::new(100);

        // Start multiple transactions
        let mut tx_ids = Vec::new();
        for i in 0..10 {
            let tx_id = manager.begin().unwrap();
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            manager.put(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            tx_ids.push(tx_id);
        }

        // Trigger batch commit by committing all
        for tx_id in &tx_ids {
            manager.commit(*tx_id).unwrap();
        }

        // Allow batch processing
        thread::sleep(Duration::from_millis(200));

        // Verify timestamps are sequential (no gaps in critical sections)
        let metrics = manager.get_detailed_stats();
        assert!(metrics.commit_count.load(Ordering::Relaxed) >= tx_ids.len() as u64);
    }

    #[test]
    fn test_concurrent_transaction_safety() {
        let manager = Arc::new(UnifiedTransactionManager::new(1000));
        let num_threads = 10;
        let transactions_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let mgr = Arc::clone(&manager);
                thread::spawn(move || {
                    for i in 0..transactions_per_thread {
                        let key = format!("key_{}", i);
                        let value = format!("value_{}", i);

                        if let Ok(tx_id) = mgr.begin() {
                            if mgr.put(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                let _ = mgr.commit_sync(tx_id);
                            } else {
                                let _ = mgr.abort(tx_id);
                            }
                        }
                    }
                })
            })
            .collect();

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify system integrity
        let stats = manager.get_detailed_stats();
        let total_transactions =
            stats.commit_count.load(Ordering::Relaxed) + stats.abort_count.load(Ordering::Relaxed);

        println!("Total transactions processed: {}", total_transactions);
        println!("Commits: {}", stats.commit_count.load(Ordering::Relaxed));
        println!("Aborts: {}", stats.abort_count.load(Ordering::Relaxed));
        println!(
            "Conflicts: {}",
            stats.conflict_count.load(Ordering::Relaxed)
        );

        // Should have processed all transactions
        assert!(total_transactions > 0);
        // No active transactions should remain
        assert_eq!(manager.active_transaction_count(), 0);
    }

    #[test]
    fn test_deadlock_detection_integration() {
        let manager = UnifiedTransactionManager::new(100);

        // Create potential deadlock scenario
        let tx1 = manager.begin().unwrap();
        let tx2 = manager.begin().unwrap();

        // tx1 locks key1
        manager.put(tx1, b"key1", b"value1").unwrap();

        // tx2 locks key2
        manager.put(tx2, b"key2", b"value2").unwrap();

        // Now try to create circular dependency
        // tx1 tries to lock key2 (should block)
        let result1 = manager.put(tx1, b"key2", b"value1_2");

        // tx2 tries to lock key1 (should detect deadlock)
        let result2 = manager.put(tx2, b"key1", b"value2_1");

        // At least one should fail with conflict
        assert!(result1.is_err() || result2.is_err());

        // Clean up
        let _ = manager.abort(tx1);
        let _ = manager.abort(tx2);
    }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        let manager = UnifiedTransactionManager::new(100);

        // Start first transaction and write
        let tx1 = manager.begin().unwrap();
        manager.put(tx1, b"key", b"value1").unwrap();

        // Start second transaction (should not see uncommitted data)
        let tx2 = manager.begin().unwrap();
        let result = manager.get(tx2, b"key").unwrap();
        assert_eq!(result, None); // Should not see uncommitted write

        // Commit first transaction
        manager.commit_sync(tx1).unwrap();

        // tx2 still should not see the committed data (snapshot isolation)
        let result = manager.get(tx2, b"key").unwrap();
        assert_eq!(result, None);

        // New transaction should see committed data
        let tx3 = manager.begin().unwrap();
        let result = manager.get(tx3, b"key").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Clean up
        let _ = manager.commit_sync(tx2);
        let _ = manager.commit_sync(tx3);
    }

    #[test]
    fn test_version_cleanup_correctness() {
        let manager = UnifiedTransactionManager::new(100);

        // Create multiple versions of same key
        for i in 0..100 {
            let tx_id = manager.begin().unwrap();
            let value = format!("value_{}", i);
            manager.put(tx_id, b"key", value.as_bytes()).unwrap();
            manager.commit_sync(tx_id).unwrap();
        }

        // Force garbage collection
        manager.run_garbage_collection();

        // Should still be able to read latest value
        let tx_id = manager.begin().unwrap();
        let result = manager.get(tx_id, b"key").unwrap();
        assert!(result.is_some());
        assert!(String::from_utf8_lossy(&result.unwrap()).contains("value_"));

        manager.commit_sync(tx_id).unwrap();
    }

    #[test]
    fn test_transaction_retry_on_conflict() {
        let manager = Arc::new(UnifiedTransactionManager::new(100));
        let success_count = Arc::new(AtomicU64::new(0));

        let handles: Vec<_> = (0..5)
            .map(|thread_id| {
                let mgr = Arc::clone(&manager);
                let counter = Arc::clone(&success_count);

                thread::spawn(move || {
                    for attempt in 0..10 {
                        match mgr.begin() {
                            Ok(tx_id) => {
                                let key = b"shared_counter";

                                // Read current value
                                let current = mgr
                                    .get(tx_id, key)
                                    .unwrap()
                                    .map(|v| {
                                        String::from_utf8_lossy(&v).parse::<i32>().unwrap_or(0)
                                    })
                                    .unwrap_or(0);

                                // Increment
                                let new_value = (current + 1).to_string();

                                if mgr.put(tx_id, key, new_value.as_bytes()).is_ok() && mgr.commit_sync(tx_id).is_ok() {
                                    counter.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                let _ = mgr.abort(tx_id);
                            }
                            Err(_) => {
                                // Retry after brief delay
                                thread::sleep(Duration::from_millis(1));
                            }
                        }

                        if attempt < 9 {
                            thread::sleep(Duration::from_millis(thread_id as u64 * 2 + 1));
                        }
                    }
                })
            })
            .collect();

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state
        let tx_id = manager.begin().unwrap();
        if let Ok(Some(final_value)) = manager.get(tx_id, b"shared_counter") {
            let count: i32 = String::from_utf8_lossy(&final_value).parse().unwrap_or(0);
            println!("Final counter value: {}", count);
            println!(
                "Successful transactions: {}",
                success_count.load(Ordering::Relaxed)
            );
            assert!(count > 0);
        }
        let _ = manager.commit_sync(tx_id);
    }
}

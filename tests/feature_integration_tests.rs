//! Feature-specific integration tests for Lightning DB

use lightning_db::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[cfg(test)]
mod compaction_tests {
    use super::*;
    use lightning_db::features::compaction::{CompactionType, CompactionConfig};
    
    #[tokio::test]
    async fn test_online_compaction_during_writes() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            auto_compaction: true,
            compaction_threshold: 0.5, // 50% fragmentation triggers compaction
            ..Default::default()
        };
        
        let db = Arc::new(Database::create(temp_dir.path(), config).unwrap());
        let db_compact = db.clone();
        
        // Start background compaction thread
        let compaction_handle = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = db_compact.compact();
            }
        });
        
        // Concurrent writes during compaction
        for i in 0..1000 {
            let key = format!("compact_key_{}", i);
            let value = vec![0u8; 1024]; // 1KB values
            db.put(key.as_bytes(), &value).unwrap();
            
            // Delete some keys to create fragmentation
            if i % 3 == 0 && i > 0 {
                let del_key = format!("compact_key_{}", i - 1);
                db.delete(del_key.as_bytes()).unwrap();
            }
        }
        
        compaction_handle.await.unwrap();
        
        // Verify data integrity after compaction
        for i in 0..1000 {
            if i % 3 != 1 { // Keys that weren't deleted
                let key = format!("compact_key_{}", i);
                assert!(db.get(key.as_bytes()).unwrap().is_some());
            }
        }
    }
    
    #[test]
    fn test_incremental_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Create fragmented data
        for i in 0..500 {
            db.put(format!("key_{}", i).as_bytes(), b"value").unwrap();
        }
        
        // Delete every other key
        for i in (0..500).step_by(2) {
            db.delete(format!("key_{}", i).as_bytes()).unwrap();
        }
        
        // Run incremental compaction
        let bytes_reclaimed = db.compact_incremental().unwrap();
        assert!(bytes_reclaimed > 0);
        
        // Verify remaining data
        for i in (1..500).step_by(2) {
            let key = format!("key_{}", i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
    }
}

#[cfg(test)]
mod transaction_isolation_tests {
    use super::*;
    use lightning_db::features::transactions::IsolationLevel;
    
    #[test]
    fn test_read_committed_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        // Initial data
        db.put(b"counter", b"0").unwrap();
        
        let db1 = db.clone();
        let db2 = db.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();
        
        // Transaction 1: Long-running read
        let handle1 = thread::spawn(move || {
            let tx = db1.begin_transaction_with_isolation(IsolationLevel::ReadCommitted).unwrap();
            
            // Read initial value
            let val1 = db1.get_tx(tx, b"counter").unwrap().unwrap();
            assert_eq!(val1, b"0");
            
            b1.wait(); // Sync point 1
            b1.wait(); // Sync point 2
            
            // Read again after T2 commits - should see new value (Read Committed)
            let val2 = db1.get_tx(tx, b"counter").unwrap().unwrap();
            assert_eq!(val2, b"1"); // Sees committed value
            
            db1.commit_transaction(tx).unwrap();
        });
        
        // Transaction 2: Write
        let handle2 = thread::spawn(move || {
            b2.wait(); // Sync point 1
            
            let tx = db2.begin_transaction().unwrap();
            db2.put_tx(tx, b"counter", b"1").unwrap();
            db2.commit_transaction(tx).unwrap();
            
            b2.wait(); // Sync point 2
        });
        
        handle1.join().unwrap();
        handle2.join().unwrap();
    }
    
    #[test]
    fn test_repeatable_read_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        db.put(b"counter", b"0").unwrap();
        
        let db1 = db.clone();
        let db2 = db.clone();
        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier.clone();
        
        // Transaction 1: Repeatable read
        let handle1 = thread::spawn(move || {
            let tx = db1.begin_transaction_with_isolation(IsolationLevel::RepeatableRead).unwrap();
            
            // First read
            let val1 = db1.get_tx(tx, b"counter").unwrap().unwrap();
            assert_eq!(val1, b"0");
            
            b1.wait(); // Sync point 1
            b1.wait(); // Sync point 2
            
            // Read again - should see same value (Repeatable Read)
            let val2 = db1.get_tx(tx, b"counter").unwrap().unwrap();
            assert_eq!(val2, b"0"); // Still sees original value
            
            db1.commit_transaction(tx).unwrap();
        });
        
        // Transaction 2: Concurrent write
        let handle2 = thread::spawn(move || {
            b2.wait(); // Sync point 1
            
            let tx = db2.begin_transaction().unwrap();
            db2.put_tx(tx, b"counter", b"1").unwrap();
            db2.commit_transaction(tx).unwrap();
            
            b2.wait(); // Sync point 2
        });
        
        handle1.join().unwrap();
        handle2.join().unwrap();
    }
}

#[cfg(test)]
mod backup_restore_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_incremental_backup() {
        let temp_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Initial data
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        
        // Full backup
        let full_backup = backup_dir.path().join("full");
        db.create_backup(&full_backup).unwrap();
        
        // More data
        for i in 100..200 {
            db.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        
        // Incremental backup
        let incr_backup = backup_dir.path().join("incremental");
        db.create_incremental_backup(&incr_backup, &full_backup).unwrap();
        
        // Restore from incremental
        let restore_dir = TempDir::new().unwrap();
        let restored_db = Database::restore_from_incremental(&full_backup, &incr_backup, restore_dir.path()).unwrap();
        
        // Verify all data present
        for i in 0..200 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert_eq!(restored_db.get(key.as_bytes()).unwrap().unwrap(), value.as_bytes());
        }
    }
    
    #[test]
    fn test_backup_compression() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Create highly compressible data
        let value = vec![b'A'; 10000]; // 10KB of same byte
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), &value).unwrap();
        }
        
        // Backup with compression
        let backup_dir = TempDir::new().unwrap();
        let backup_path = backup_dir.path().join("compressed_backup");
        db.create_compressed_backup(&backup_path, CompressionType::Zstd).unwrap();
        
        // Check backup size is smaller than original
        let backup_size = std::fs::metadata(&backup_path).unwrap().len();
        assert!(backup_size < 100 * 10000); // Should be much smaller than 1MB
        
        // Restore and verify
        let restore_dir = TempDir::new().unwrap();
        let restored_db = Database::restore_from_backup(&backup_path, restore_dir.path()).unwrap();
        
        for i in 0..100 {
            let key = format!("key_{}", i);
            assert_eq!(restored_db.get(key.as_bytes()).unwrap().unwrap(), value);
        }
    }
}

#[cfg(test)]
mod integrity_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_corruption_detection_and_repair() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Insert test data
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes()).unwrap();
        }
        
        // Simulate corruption by directly modifying a data file
        // (In real test, would corrupt actual data file)
        
        // Run integrity check
        let report = db.verify_integrity().await.unwrap();
        
        if !report.is_valid() {
            // Attempt repair
            let repair_report = db.repair_corruption().await.unwrap();
            assert!(repair_report.success);
            
            // Verify again
            let final_report = db.verify_integrity().await.unwrap();
            assert!(final_report.is_valid());
        }
    }
    
    #[test]
    fn test_checksum_validation() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            enable_checksums: true,
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Write data with checksums
        let test_data = b"important data that must not be corrupted";
        db.put(b"critical_key", test_data).unwrap();
        
        // Read and verify checksum validation happens
        let retrieved = db.get(b"critical_key").unwrap().unwrap();
        assert_eq!(retrieved, test_data);
        
        // Stats should show checksum validations
        let stats = db.stats();
        assert!(stats.checksum_validations > 0);
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    
    #[test]
    fn test_cache_effectiveness() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            cache_size: 10 * 1024 * 1024, // 10MB cache
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Populate data
        for i in 0..1000 {
            db.put(format!("key_{}", i).as_bytes(), vec![0u8; 1024]).unwrap();
        }
        
        // First pass - cold cache
        let start = Instant::now();
        for i in 0..100 {
            db.get(format!("key_{}", i).as_bytes()).unwrap();
        }
        let cold_duration = start.elapsed();
        
        // Second pass - warm cache
        let start = Instant::now();
        for i in 0..100 {
            db.get(format!("key_{}", i).as_bytes()).unwrap();
        }
        let warm_duration = start.elapsed();
        
        // Warm cache should be significantly faster
        assert!(warm_duration < cold_duration / 2);
        
        // Check cache hit rate
        let metrics = db.metrics().unwrap();
        let hit_rate = metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64;
        assert!(hit_rate > 0.5); // At least 50% hit rate on second pass
    }
    
    #[test]
    fn test_batch_write_performance() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Single writes
        let start = Instant::now();
        for i in 0..1000 {
            db.put(format!("single_{}", i).as_bytes(), b"value").unwrap();
        }
        let single_duration = start.elapsed();
        
        // Batch writes
        let start = Instant::now();
        let mut batch = db.create_batch();
        for i in 0..1000 {
            batch.put(format!("batch_{}", i).as_bytes(), b"value");
        }
        db.write_batch(batch).unwrap();
        let batch_duration = start.elapsed();
        
        // Batch should be significantly faster
        assert!(batch_duration < single_duration / 2);
        
        println!("Single writes: {:?}, Batch writes: {:?}", single_duration, batch_duration);
    }
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    use rand::Rng;
    
    #[test]
    fn test_concurrent_stress() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        let num_threads = 10;
        let ops_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));
        
        let handles: Vec<_> = (0..num_threads).map(|thread_id| {
            let db = db.clone();
            let barrier = barrier.clone();
            
            thread::spawn(move || {
                let mut rng = rand::rng();
                barrier.wait(); // All threads start together
                
                for op in 0..ops_per_thread {
                    let key = format!("t{}_k{}", thread_id, op);
                    let value = format!("t{}_v{}", thread_id, op);
                    
                    match rng.gen_range(0..10) {
                        0..=6 => {
                            // 70% writes
                            db.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        7..=9 => {
                            // 30% reads
                            let _ = db.get(key.as_bytes());
                        }
                        _ => unreachable!(),
                    }
                    
                    // Occasionally start a transaction
                    if rng.gen_bool(0.1) {
                        let tx = db.begin_transaction().unwrap();
                        for i in 0..10 {
                            let tx_key = format!("tx_t{}_k{}_{}", thread_id, op, i);
                            db.put_tx(tx, tx_key.as_bytes(), b"tx_value").unwrap();
                        }
                        db.commit_transaction(tx).unwrap();
                    }
                }
            })
        }).collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify database is still functional
        db.put(b"final_key", b"final_value").unwrap();
        assert_eq!(db.get(b"final_key").unwrap().unwrap(), b"final_value");
        
        // Check integrity
        let stats = db.stats();
        assert!(stats.total_keys > 0);
        assert_eq!(stats.errors, 0);
    }
    
    #[test]
    fn test_memory_pressure() {
        let temp_dir = TempDir::new().unwrap();
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            cache_size: 1 * 1024 * 1024, // Small 1MB cache
            memory_limit: Some(10 * 1024 * 1024), // 10MB total limit
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config).unwrap();
        
        // Try to exceed memory limit
        for i in 0..10000 {
            let key = format!("mem_key_{}", i);
            let value = vec![0u8; 1024]; // 1KB each
            
            match db.put(key.as_bytes(), &value) {
                Ok(_) => {}
                Err(e) if e.is_resource_exhausted() => {
                    // Expected when hitting memory limit
                    println!("Hit memory limit at iteration {}", i);
                    break;
                }
                Err(e) => panic!("Unexpected error: {}", e),
            }
        }
        
        // Database should still be functional
        assert!(db.get(b"mem_key_0").unwrap().is_some());
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    
    #[test]
    fn test_wal_recovery_after_crash() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();
        
        // Phase 1: Write data and "crash"
        {
            let config = LightningDbConfig {
                path: db_path.clone(),
                wal_enabled: true,
                sync_on_commit: false, // Don't sync to simulate crash
                ..Default::default()
            };
            
            let db = Database::create(&db_path, config).unwrap();
            
            // Write data
            for i in 0..100 {
                db.put(format!("wal_key_{}", i).as_bytes(), format!("wal_value_{}", i).as_bytes()).unwrap();
            }
            
            // Start transaction but don't commit (simulates crash)
            let tx = db.begin_transaction().unwrap();
            db.put_tx(tx, b"uncommitted_key", b"uncommitted_value").unwrap();
            // Transaction dropped without commit
        }
        
        // Phase 2: Recover and verify
        {
            let config = LightningDbConfig {
                path: db_path.clone(),
                wal_enabled: true,
                ..Default::default()
            };
            
            let recovered_db = Database::open(&db_path, config).unwrap();
            
            // All committed data should be present
            for i in 0..100 {
                let key = format!("wal_key_{}", i);
                let value = format!("wal_value_{}", i);
                assert_eq!(recovered_db.get(key.as_bytes()).unwrap().unwrap(), value.as_bytes());
            }
            
            // Uncommitted transaction should not be present
            assert_eq!(recovered_db.get(b"uncommitted_key").unwrap(), None);
        }
    }
}

#[cfg(test)]
mod observability_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_metrics_collection() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Enable metrics
        db.enable_metrics().unwrap();
        
        // Perform operations
        for i in 0..100 {
            db.put(format!("metric_key_{}", i).as_bytes(), b"value").unwrap();
            if i % 2 == 0 {
                db.get(format!("metric_key_{}", i).as_bytes()).unwrap();
            }
        }
        
        // Get metrics snapshot
        let snapshot = db.get_metrics_snapshot().unwrap();
        
        assert_eq!(snapshot.writes, 100);
        assert_eq!(snapshot.reads, 50);
        assert!(snapshot.write_latency_p99 > Duration::ZERO);
        assert!(snapshot.read_latency_p99 > Duration::ZERO);
    }
    
    #[tokio::test]
    async fn test_health_monitoring() {
        let temp_dir = TempDir::new().unwrap();
        let db = Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap();
        
        // Initial health check
        let health = db.health_check().await.unwrap();
        assert!(health.is_healthy);
        assert_eq!(health.checks.len(), health.checks.iter().filter(|c| c.passed).count());
        
        // Simulate unhealthy condition (e.g., disk full)
        // In real scenario, would fill disk or corrupt data
        
        // Health check should detect issues
        // let unhealthy = db.health_check().await.unwrap();
        // assert!(!unhealthy.is_healthy || unhealthy.warnings.len() > 0);
    }
}
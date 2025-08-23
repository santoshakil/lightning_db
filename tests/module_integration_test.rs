//! Comprehensive module integration tests for Lightning DB
//! 
//! These tests verify that all major modules work together correctly

use lightning_db::*;
use std::sync::Arc;
use std::path::PathBuf;
use tempfile::TempDir;

#[cfg(test)]
mod integration_tests {
    use super::*;
    use lightning_db::core::error::Result;
    
    /// Helper to create test database
    fn create_test_db() -> Result<(Database, TempDir)> {
        let temp_dir = TempDir::new()?;
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            cache_size: 10 * 1024 * 1024, // 10MB
            compression_enabled: true,
            wal_enabled: true,
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config)?;
        Ok((db, temp_dir))
    }
    
    #[test]
    fn test_core_operations_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Test basic CRUD operations
        db.put(b"key1", b"value1")?;
        assert_eq!(db.get(b"key1")?.as_deref(), Some(b"value1".as_ref()));
        
        db.delete(b"key1")?;
        assert_eq!(db.get(b"key1")?, None);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_transaction_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Test transaction with multiple operations
        let tx_id = db.begin_transaction()?;
        
        db.put_tx(tx_id, b"tx_key1", b"tx_value1")?;
        db.put_tx(tx_id, b"tx_key2", b"tx_value2")?;
        
        // Verify isolation - data not visible outside transaction
        assert_eq!(db.get(b"tx_key1")?, None);
        
        db.commit_transaction(tx_id)?;
        
        // Verify data visible after commit
        assert_eq!(db.get(b"tx_key1")?.as_deref(), Some(b"tx_value1".as_ref()));
        assert_eq!(db.get(b"tx_key2")?.as_deref(), Some(b"tx_value2".as_ref()));
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_compaction_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Insert data to trigger compaction
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Delete half the data to create fragmentation
        for i in 0..500 {
            let key = format!("key_{}", i);
            db.delete(key.as_bytes())?;
        }
        
        // Trigger compaction
        let bytes_reclaimed = db.compact()?;
        assert!(bytes_reclaimed > 0);
        
        // Verify remaining data intact
        for i in 500..1000 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert_eq!(db.get(key.as_bytes())?.as_deref(), Some(value.as_bytes()));
        }
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_backup_restore_integration() -> Result<()> {
        let (db, dir) = create_test_db()?;
        let backup_path = dir.path().join("backup");
        
        // Insert test data
        for i in 0..100 {
            let key = format!("backup_key_{}", i);
            let value = format!("backup_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Create backup
        db.create_backup(&backup_path)?;
        
        // Create new database from backup
        let restore_dir = TempDir::new()?;
        let restored_db = Database::restore_from_backup(&backup_path, restore_dir.path())?;
        
        // Verify all data restored
        for i in 0..100 {
            let key = format!("backup_key_{}", i);
            let value = format!("backup_value_{}", i);
            assert_eq!(restored_db.get(key.as_bytes())?.as_deref(), Some(value.as_bytes()));
        }
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_integrity_check_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Insert test data
        for i in 0..50 {
            let key = format!("integrity_key_{}", i);
            let value = format!("integrity_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Run integrity check
        let report = db.verify_integrity().await?;
        
        assert!(report.is_valid());
        assert_eq!(report.errors_found, 0);
        assert!(report.pages_checked > 0);
        
        Ok(())
    }
    
    #[test]
    fn test_metrics_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Perform operations
        for i in 0..10 {
            db.put(format!("k{}", i).as_bytes(), b"value")?;
            db.get(format!("k{}", i).as_bytes())?;
        }
        
        // Get metrics
        let metrics = db.metrics()?;
        
        assert!(metrics.total_writes >= 10);
        assert!(metrics.total_reads >= 10);
        assert!(metrics.cache_hits + metrics.cache_misses >= 10);
        
        Ok(())
    }
    
    #[test]
    fn test_compression_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Create compressible data
        let large_value = vec![b'A'; 10000]; // 10KB of same byte
        
        db.put(b"compressed_key", &large_value)?;
        
        // Verify data retrieval works with compression
        let retrieved = db.get(b"compressed_key")?.unwrap();
        assert_eq!(retrieved.len(), large_value.len());
        assert_eq!(retrieved, large_value);
        
        // Check that compression actually happened
        let stats = db.stats();
        assert!(stats.data_size < 10000); // Should be compressed
        
        Ok(())
    }
    
    #[tokio::test] 
    async fn test_concurrent_access_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        let db = Arc::new(db);
        
        let mut handles = vec![];
        
        // Spawn multiple concurrent writers
        for i in 0..10 {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                for j in 0..100 {
                    let key = format!("thread_{}_key_{}", i, j);
                    let value = format!("thread_{}_value_{}", i, j);
                    db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all threads
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify all data
        for i in 0..10 {
            for j in 0..100 {
                let key = format!("thread_{}_key_{}", i, j);
                let value = format!("thread_{}_value_{}", i, j);
                assert_eq!(db.get(key.as_bytes())?.as_deref(), Some(value.as_bytes()));
            }
        }
        
        Ok(())
    }
    
    #[test]
    fn test_crash_recovery_integration() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().to_path_buf();
        
        // Create database and insert data
        {
            let config = LightningDbConfig {
                path: db_path.clone(),
                wal_enabled: true,
                ..Default::default()
            };
            let db = Database::create(&db_path, config)?;
            
            for i in 0..100 {
                db.put(format!("key_{}", i).as_bytes(), format!("value_{}", i).as_bytes())?;
            }
            // Database dropped here, simulating crash
        }
        
        // Reopen database - should recover from WAL
        let config = LightningDbConfig {
            path: db_path.clone(),
            wal_enabled: true,
            ..Default::default()
        };
        let recovered_db = Database::open(&db_path, config)?;
        
        // Verify all data recovered
        for i in 0..100 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            assert_eq!(recovered_db.get(key.as_bytes())?.as_deref(), Some(value.as_bytes()));
        }
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_migration_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Insert v1 data
        db.put(b"v1_key", b"v1_value")?;
        
        // Run migration (simulated)
        use lightning_db::features::migration::{Migration, MigrationVersion};
        
        let migration = Migration {
            version: MigrationVersion::new(1, 0, 0),
            description: "Test migration".to_string(),
            up_script: "-- Migration up".to_string(),
            down_script: "-- Migration down".to_string(),
            checksum: "test_checksum".to_string(),
        };
        
        db.apply_migration(&migration).await?;
        
        // Verify migration applied
        let applied = db.list_applied_migrations().await?;
        assert!(!applied.is_empty());
        assert_eq!(applied[0].version, MigrationVersion::new(1, 0, 0));
        
        Ok(())
    }
    
    #[test]
    fn test_memory_monitoring_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Get initial memory stats
        let initial_stats = db.get_memory_stats()?;
        
        // Perform memory-intensive operations
        for i in 0..1000 {
            let key = format!("mem_key_{}", i);
            let value = vec![0u8; 1024]; // 1KB per value
            db.put(key.as_bytes(), &value)?;
        }
        
        // Get final memory stats
        let final_stats = db.get_memory_stats()?;
        
        // Verify memory tracking
        assert!(final_stats.allocated_bytes > initial_stats.allocated_bytes);
        assert!(final_stats.total_allocations > initial_stats.total_allocations);
        
        Ok(())
    }
    
    #[test]
    fn test_security_features_integration() -> Result<()> {
        // Create encrypted database
        let temp_dir = TempDir::new()?;
        let config = LightningDbConfig {
            path: temp_dir.path().to_path_buf(),
            encryption_enabled: true,
            encryption_key: Some(vec![0u8; 32]), // 256-bit key
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config)?;
        
        // Store sensitive data
        db.put(b"secret_key", b"secret_value")?;
        
        // Verify data is accessible with correct key
        assert_eq!(db.get(b"secret_key")?.as_deref(), Some(b"secret_value".as_ref()));
        
        // TODO: Verify raw file is encrypted (would need file inspection)
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_observability_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Enable observability
        db.enable_tracing()?;
        db.enable_metrics()?;
        
        // Perform traced operations
        for i in 0..10 {
            db.put(format!("traced_key_{}", i).as_bytes(), b"value")?;
        }
        
        // Get observability data
        let health = db.health_check().await?;
        assert!(health.is_healthy);
        
        let metrics_snapshot = db.get_metrics_snapshot()?;
        assert!(metrics_snapshot.operations > 0);
        
        Ok(())
    }
    
    #[test]
    fn test_resource_management_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        
        // Set resource limits
        db.set_memory_limit(50 * 1024 * 1024)?; // 50MB
        db.set_max_connections(100)?;
        
        // Verify limits enforced
        let resource_stats = db.get_resource_stats()?;
        assert!(resource_stats.memory_usage <= 50 * 1024 * 1024);
        assert!(resource_stats.active_connections <= 100);
        
        Ok(())
    }
}

/// Performance integration tests
#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;
    
    #[test]
    fn test_throughput_integration() -> Result<()> {
        let (db, _dir) = create_test_db()?;
        let iterations = 10000;
        
        // Measure write throughput
        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("perf_key_{}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        let write_duration = start.elapsed();
        let write_throughput = iterations as f64 / write_duration.as_secs_f64();
        
        // Measure read throughput
        let start = Instant::now();
        for i in 0..iterations {
            let key = format!("perf_key_{}", i);
            db.get(key.as_bytes())?;
        }
        let read_duration = start.elapsed();
        let read_throughput = iterations as f64 / read_duration.as_secs_f64();
        
        println!("Write throughput: {:.0} ops/sec", write_throughput);
        println!("Read throughput: {:.0} ops/sec", read_throughput);
        
        // Verify meets minimum performance targets
        assert!(write_throughput > 1000.0); // At least 1K writes/sec
        assert!(read_throughput > 10000.0); // At least 10K reads/sec
        
        Ok(())
    }
}

fn create_test_db() -> Result<(Database, TempDir)> {
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        path: temp_dir.path().to_path_buf(),
        cache_size: 10 * 1024 * 1024,
        compression_enabled: true,
        wal_enabled: true,
        ..Default::default()
    };
    
    let db = Database::create(temp_dir.path(), config)?;
    Ok((db, temp_dir))
}
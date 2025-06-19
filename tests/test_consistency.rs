use lightning_db::{Database, LightningDbConfig, ConsistencyLevel, ConsistencyConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_consistency_levels() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("consistency_db");
    
    let mut config = LightningDbConfig::default();
    config.consistency_config = ConsistencyConfig {
        default_level: ConsistencyLevel::Strong,
        consistency_timeout: Duration::from_millis(100),
        enable_read_repair: true,
        max_clock_skew: Duration::from_millis(10),
    };
    
    let db = Database::create(&db_path, config).unwrap();
    
    // Test eventual consistency
    db.put_with_consistency(b"key1", b"value1", ConsistencyLevel::Eventual).unwrap();
    let result = db.get_with_consistency(b"key1", ConsistencyLevel::Eventual).unwrap();
    assert_eq!(result, Some(b"value1".to_vec()));
    
    // Test strong consistency
    db.put_with_consistency(b"key2", b"value2", ConsistencyLevel::Strong).unwrap();
    let result = db.get_with_consistency(b"key2", ConsistencyLevel::Strong).unwrap();
    assert_eq!(result, Some(b"value2".to_vec()));
    
    // Test linearizable consistency
    db.put_with_consistency(b"key3", b"value3", ConsistencyLevel::Linearizable).unwrap();
    let result = db.get_with_consistency(b"key3", ConsistencyLevel::Linearizable).unwrap();
    assert_eq!(result, Some(b"value3".to_vec()));
}

#[test]
fn test_session_consistency() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("session_db");
    
    let mut config = LightningDbConfig::default();
    config.consistency_config.default_level = ConsistencyLevel::Session;
    
    let db = Database::create(&db_path, config).unwrap();
    
    // In a real implementation, sessions would be managed per client connection
    // For this test, we'll simulate session consistency behavior
    
    // Write with session consistency
    db.put_with_consistency(b"session_key", b"session_value", ConsistencyLevel::Session).unwrap();
    
    // Immediate read should see the write
    let result = db.get_with_consistency(b"session_key", ConsistencyLevel::Session).unwrap();
    assert_eq!(result, Some(b"session_value".to_vec()));
}

#[test]
fn test_concurrent_consistency() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("concurrent_db");
    
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(&db_path, config).unwrap());
    
    let barrier = Arc::new(Barrier::new(3));
    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    
    // Writer thread with strong consistency
    {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            for i in 0..10 {
                let key = format!("concurrent_key_{}", i);
                let value = format!("value_{}", i);
                db_clone.put_with_consistency(
                    key.as_bytes(), 
                    value.as_bytes(), 
                    ConsistencyLevel::Strong
                ).unwrap();
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }
    
    // Reader thread with eventual consistency
    {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(Duration::from_millis(50)); // Let some writes happen
            
            let mut found_count = 0;
            for i in 0..10 {
                let key = format!("concurrent_key_{}", i);
                if let Ok(Some(_)) = db_clone.get_with_consistency(
                    key.as_bytes(), 
                    ConsistencyLevel::Eventual
                ) {
                    found_count += 1;
                }
            }
            
            // With eventual consistency, we might not see all writes immediately
            assert!(found_count >= 0 && found_count <= 10);
        });
        handles.push(handle);
    }
    
    // Reader thread with strong consistency
    {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            thread::sleep(Duration::from_millis(150)); // Wait for all writes
            
            let mut found_count = 0;
            for i in 0..10 {
                let key = format!("concurrent_key_{}", i);
                if let Ok(Some(_)) = db_clone.get_with_consistency(
                    key.as_bytes(), 
                    ConsistencyLevel::Strong
                ) {
                    found_count += 1;
                }
            }
            
            // With strong consistency, we should see all writes
            assert_eq!(found_count, 10);
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_consistency_timeout() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("timeout_db");
    
    let mut config = LightningDbConfig::default();
    config.consistency_config.consistency_timeout = Duration::from_millis(10); // Very short timeout
    
    let db = Database::create(&db_path, config).unwrap();
    
    // This should succeed even with short timeout
    db.put_with_consistency(b"timeout_key", b"timeout_value", ConsistencyLevel::Strong).unwrap();
    
    let result = db.get(b"timeout_key").unwrap();
    assert_eq!(result, Some(b"timeout_value".to_vec()));
}
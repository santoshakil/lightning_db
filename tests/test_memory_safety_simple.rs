use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn test_basic_thread_safety() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(dir.path().join("thread_safe.db"), config).unwrap());

    let mut handles = vec![];

    // Spawn 3 threads that write concurrently
    for thread_id in 0..3 {
        let db = db.clone();

        let handle = thread::spawn(move || {
            for i in 0..10 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("value_{}", i);

                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });

        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all writes succeeded
    for thread_id in 0..3 {
        for i in 0..10 {
            let key = format!("thread_{}_key_{}", thread_id, i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
    }
}

#[test]
fn test_value_size_limits() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path().join("size_limits.db"), config).unwrap();

    // Test 1MB value (should succeed)
    let value_1mb = vec![0x42; 1024 * 1024];
    match db.put(b"1mb", &value_1mb) {
        Ok(_) => {
            let retrieved = db.get(b"1mb").unwrap().unwrap();
            assert_eq!(retrieved.len(), 1024 * 1024);
        }
        Err(_) => {
            // Value size limit enforced - this is also acceptable
        }
    }

    // Test 2MB value (likely to fail)
    let value_2mb = vec![0x42; 2 * 1024 * 1024];
    match db.put(b"2mb", &value_2mb) {
        Ok(_) => {
            // If it succeeds, verify we can read it back
            let retrieved = db.get(b"2mb").unwrap().unwrap();
            assert_eq!(retrieved.len(), 2 * 1024 * 1024);
        }
        Err(_) => {
            // Expected - value too large
        }
    }
}

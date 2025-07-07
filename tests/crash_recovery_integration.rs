use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

/// Test basic crash recovery with WAL replay
#[test]
fn test_basic_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data and crash
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();

        // Write some data
        for i in 0..100 {
            db.put(
                format!("key_{:04}", i).as_bytes(),
                format!("value_{:04}", i).as_bytes(),
            )
            .unwrap();
        }

        // Force checkpoint to ensure data is on disk
        db.checkpoint().unwrap();

        // Simulate crash by dropping without proper shutdown
        std::mem::forget(db);
    }

    // Phase 2: Recover and verify data
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Verify all data is recovered
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let expected = format!("value_{:04}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value.as_deref(), Some(expected.as_bytes()));
        }
    }
}

/// Test crash recovery with uncommitted transactions
#[test]
fn test_transaction_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Start transaction and crash
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();

        // Write some committed data
        for i in 0..50 {
            db.put(format!("committed_{:04}", i).as_bytes(), b"committed")
                .unwrap();
        }

        // Force checkpoint to ensure committed data is on disk
        db.checkpoint().unwrap();

        // Start transaction but don't commit
        let tx_id = db.begin_transaction().unwrap();
        for i in 0..50 {
            db.put_tx(
                tx_id,
                format!("uncommitted_{:04}", i).as_bytes(),
                b"uncommitted",
            )
            .unwrap();
        }

        // Simulate crash without committing
        std::mem::forget(db);
    }

    // Phase 2: Recover and verify only committed data exists
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Verify committed data exists
        for i in 0..50 {
            let key = format!("committed_{:04}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value.as_deref(), Some(b"committed".as_ref()));
        }

        // Verify uncommitted data does not exist
        for i in 0..50 {
            let key = format!("uncommitted_{:04}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert_eq!(value, None);
        }
    }
}

/// Test crash recovery with concurrent writes
#[test]
fn test_concurrent_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Concurrent writes and crash
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Arc::new(Database::open(&db_path, config).unwrap());
        let running = Arc::new(AtomicBool::new(true));

        let mut handles = vec![];

        // Start concurrent writers
        for thread_id in 0..4 {
            let db_clone = Arc::clone(&db);
            let running_clone = Arc::clone(&running);

            let handle = thread::spawn(move || {
                let mut i = 0;
                while running_clone.load(Ordering::Relaxed) {
                    let key = format!("thread_{}_key_{:06}", thread_id, i);
                    let value = format!("thread_{}_value_{:06}", thread_id, i);
                    let _ = db_clone.put(key.as_bytes(), value.as_bytes());
                    i += 1;
                    thread::sleep(Duration::from_micros(100));
                }
                i
            });
            handles.push(handle);
        }

        // Let them run for a bit
        thread::sleep(Duration::from_millis(100));

        // Simulate crash
        running.store(false, Ordering::Relaxed);
        for handle in handles {
            let _ = handle.join();
        }
        std::mem::forget(Arc::try_unwrap(db).ok());
    }

    // Phase 2: Recover and verify data consistency
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Count recovered entries per thread
        let mut thread_counts = [0; 4];

        for thread_id in 0..4 {
            let mut i = 0;
            loop {
                let key = format!("thread_{}_key_{:06}", thread_id, i);
                if db.get(key.as_bytes()).unwrap().is_some() {
                    thread_counts[thread_id] += 1;
                    i += 1;
                } else {
                    break;
                }
            }
        }

        // Verify we recovered some data from each thread
        for (thread_id, count) in thread_counts.iter().enumerate() {
            assert!(
                *count > 0,
                "Thread {} should have some recovered data",
                thread_id
            );
            println!("Thread {} recovered {} entries", thread_id, count);
        }
    }
}

/// Test crash recovery with partial WAL writes
#[test]
fn test_partial_wal_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data with artificial WAL truncation
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();

        // Write batch of data
        for i in 0..1000 {
            db.put(
                format!("key_{:06}", i).as_bytes(),
                format!("value_{:06}", i).as_bytes(),
            )
            .unwrap();
        }

        // Force checkpoint
        db.checkpoint().unwrap();

        // Force close without proper shutdown
        drop(db);

        // Simulate partial WAL write by truncating WAL file
        let wal_path = db_path.join("wal");
        if wal_path.exists() {
            let metadata = std::fs::metadata(&wal_path).unwrap();
            let original_size = metadata.len();
            if original_size > 100 {
                // Truncate to simulate partial write
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&wal_path)
                    .unwrap();
                file.set_len(original_size - 50).unwrap();
            }
        }
    }

    // Phase 2: Recover and verify as much data as possible
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Count recovered entries
        let mut recovered = 0;
        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            if db.get(key.as_bytes()).unwrap().is_some() {
                recovered += 1;
            } else {
                // Once we hit a missing key, rest should also be missing
                for j in i..1000 {
                    let key = format!("key_{:06}", j);
                    assert!(db.get(key.as_bytes()).unwrap().is_none());
                }
                break;
            }
        }

        println!("Recovered {} entries from partial WAL", recovered);
        assert!(recovered > 0, "Should recover at least some data");
    }
}

/// Test crash recovery with compaction in progress
#[test]
fn test_compaction_crash_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data to trigger compaction
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Async,
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();

        // Write enough data to potentially trigger compaction
        for batch in 0..10 {
            for i in 0..1000 {
                let key = format!("batch_{}_key_{:06}", batch, i);
                let value = format!("batch_{}_value_{:06}", batch, i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            // Checkpoint after each batch
            if batch % 2 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Simulate crash during potential compaction
        std::mem::forget(db);
    }

    // Phase 2: Recover and verify data integrity
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Verify all data is present
        let mut total_recovered = 0;
        for batch in 0..10 {
            for i in 0..1000 {
                let key = format!("batch_{}_key_{:06}", batch, i);
                let expected = format!("batch_{}_value_{:06}", batch, i);
                let value = db.get(key.as_bytes()).unwrap();
                if let Some(v) = value {
                    assert_eq!(v, expected.as_bytes());
                    total_recovered += 1;
                }
            }
        }

        println!(
            "Recovered {} entries after compaction crash",
            total_recovered
        );
        assert!(total_recovered >= 9000, "Should recover most data");
    }
}

/// Test recovery with corrupted page files
#[test]
fn test_corrupted_page_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    // Phase 1: Write data normally
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };

        let db = Database::open(&db_path, config).unwrap();

        // Write test data
        for i in 0..500 {
            db.put(
                format!("test_key_{:06}", i).as_bytes(),
                format!("test_value_{:06}", i).as_bytes(),
            )
            .unwrap();
        }

        drop(db);
    }

    // Corrupt a page file
    let btree_path = db_path.join("btree.db");
    if btree_path.exists() {
        let metadata = std::fs::metadata(&btree_path).unwrap();
        if metadata.len() > 4096 {
            // Corrupt a page in the middle
            let mut data = std::fs::read(&btree_path).unwrap();
            for i in 2048..2048 + 256 {
                if i < data.len() {
                    data[i] = 0xFF; // Corrupt data
                }
            }
            std::fs::write(&btree_path, data).unwrap();
        }
    }

    // Phase 2: Try to recover
    {
        let config = LightningDbConfig::default();
        // Database should either recover or fail gracefully
        match Database::open(&db_path, config) {
            Ok(db) => {
                // If it opens, verify what data we can access
                let mut accessible = 0;
                for i in 0..500 {
                    let key = format!("test_key_{:06}", i);
                    if db.get(key.as_bytes()).is_ok() {
                        accessible += 1;
                    }
                }
                println!("Accessible entries after corruption: {}", accessible);
            }
            Err(e) => {
                println!("Database failed to open after corruption: {}", e);
                // This is also acceptable - failing fast on corruption
            }
        }
    }
}

/// Test recovery after kill -9 simulation
#[test]
#[ignore] // This test requires special setup
fn test_kill_nine_recovery() {
    // This test would spawn a child process and kill it
    // Included here as a template for manual testing

    let dir = tempdir().unwrap();
    let _db_path = dir.path().to_path_buf();

    // Would spawn a child process that writes to the database
    // Then send SIGKILL to simulate kill -9
    // Then verify recovery in parent process

    println!("Kill -9 recovery test would run here with proper process spawning");
}

/// Helper to verify database integrity after recovery
#[allow(dead_code)]
fn verify_database_integrity(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Basic integrity checks
    let stats = db.stats();
    assert!(stats.page_count > 0);
    assert!(stats.free_page_count <= stats.page_count as usize);

    // Try some operations to ensure database is functional
    db.put(b"integrity_test", b"passed")?;
    let value = db.get(b"integrity_test")?;
    assert_eq!(value.as_deref(), Some(b"passed".as_ref()));
    db.delete(b"integrity_test")?;

    Ok(())
}

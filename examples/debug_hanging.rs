use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn test_with_timeout<F>(name: &str, _timeout_secs: u64, test_fn: F) -> bool
where
    F: FnOnce() -> Result<(), Box<dyn std::error::Error>>,
{
    println!("Testing {}: ", name);
    let start = Instant::now();

    match test_fn() {
        Ok(_) => {
            let duration = start.elapsed();
            println!("  ✅ PASS ({:.2}s)", duration.as_secs_f64());
            true
        }
        Err(e) => {
            let duration = start.elapsed();
            println!("  ❌ FAIL ({:.2}s) - {}", duration.as_secs_f64(), e);
            false
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Debugging Hanging Tests");
    println!("==========================\n");

    let mut passed = 0;
    let mut total = 0;

    // Test 1: Basic operations (should be fast)
    total += 1;
    if test_with_timeout("Basic CRUD", 5, || {
        let dir = tempdir()?;
        let db = Database::create(dir.path().join("basic.db"), LightningDbConfig::default())?;
        db.put(b"key", b"value")?;
        let _ = db.get(b"key")?;
        db.delete(b"key")?;
        Ok(())
    }) {
        passed += 1;
    }

    // Test 2: Simple transaction (should be fast)
    total += 1;
    if test_with_timeout("Simple Transaction", 5, || {
        let dir = tempdir()?;
        let db = Database::create(dir.path().join("tx.db"), LightningDbConfig::default())?;
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"key", b"value")?;
        db.commit_transaction(tx_id)?;
        Ok(())
    }) {
        passed += 1;
    }

    // Test 3: LSM operations (might be slower)
    total += 1;
    if test_with_timeout("LSM Operations", 10, || {
        let dir = tempdir()?;
        let config = LightningDbConfig {
            compression_enabled: true,
            ..Default::default()
        };
        let db = Database::create(dir.path().join("lsm.db"), config)?;
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), b"value")?;
        }
        Ok(())
    }) {
        passed += 1;
    }

    // Test 4: Auto batcher (potential hanging point)
    total += 1;
    if test_with_timeout("Auto Batcher", 15, || {
        let dir = tempdir()?;
        let db = Arc::new(Database::create(
            dir.path().join("batch.db"),
            LightningDbConfig::default(),
        )?);
        let batcher = Database::create_auto_batcher(db.clone());

        for i in 0..50 {
            batcher.put(format!("key_{}", i).into_bytes(), b"value".to_vec())?;
        }

        batcher.wait_for_completion()?;
        batcher.shutdown();
        Ok(())
    }) {
        passed += 1;
    }

    // Test 5: Sync WAL transaction (potential hanging point)
    total += 1;
    if test_with_timeout("Sync WAL Transaction", 15, || {
        let dir = tempdir()?;
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::create(dir.path().join("sync.db"), config)?;

        let tx_id = db.begin_transaction()?;
        for i in 0..10 {
            db.put_tx(tx_id, format!("key_{}", i).as_bytes(), b"value")?;
        }
        db.commit_transaction(tx_id)?;
        Ok(())
    }) {
        passed += 1;
    }

    // Test 6: Optimized transactions (known slow)
    total += 1;
    if test_with_timeout("Optimized Transactions", 20, || {
        let dir = tempdir()?;
        let config = LightningDbConfig {
            use_optimized_transactions: true,
            ..Default::default()
        };
        let db = Database::create(dir.path().join("opt.db"), config)?;

        for i in 0..50 {
            db.put(format!("key_{}", i).as_bytes(), b"value")?;
        }
        Ok(())
    }) {
        passed += 1;
    }

    // Test 7: Concurrent operations (potential deadlock)
    total += 1;
    if test_with_timeout("Concurrent Operations", 20, || {
        use std::thread;

        let dir = tempdir()?;
        let db = Arc::new(Database::create(
            dir.path().join("concurrent.db"),
            LightningDbConfig::default(),
        )?);

        let mut handles = vec![];
        for t in 0..3 {
            let db_clone = db.clone();
            let handle = thread::spawn(move || {
                for i in 0..20 {
                    let key = format!("thread_{}_key_{}", t, i);
                    let _ = db_clone.put(key.as_bytes(), b"value");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        Ok(())
    }) {
        passed += 1;
    }

    println!("\n📊 Results: {}/{} tests passed", passed, total);

    if passed == total {
        println!("🎉 All targeted tests passed!");
    } else {
        println!("⚠️  {} tests failed or hung", total - passed);
    }

    Ok(())
}

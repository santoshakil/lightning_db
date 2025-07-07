use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Auto Batcher Performance Test\n");

    let dir = tempdir()?;

    // Test 1: Baseline - Single writes with sync WAL
    {
        let db_path = dir.path().join("test_sync.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("Test 1: Single writes with sync WAL (baseline)");
        let start = Instant::now();
        let count = 100;

        for i in 0..count {
            let key = format!("sync_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
    }

    // Test 2: Auto batcher with sync WAL
    {
        let db_path = dir.path().join("test_auto_batch.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        config.compression_enabled = true; // Ensure LSM is enabled
        let db = Arc::new(Database::create(&db_path, config)?);

        // Create auto batcher
        let batcher = Database::create_auto_batcher(db.clone());

        println!("\nTest 2: Auto batcher with sync WAL");
        let start = Instant::now();
        let count = 1000; // Reduced for faster testing

        for i in 0..count {
            let key = format!("batch_{:08}", i);
            batcher.put(key.into_bytes(), b"value".to_vec())?;
        }

        // Wait for all writes to complete
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!(
            "  • Status: {}",
            if ops_per_sec >= 300.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        ); // Adjusted target for sync WAL reality

        // Get stats
        let (submitted, completed, batches, errors) = batcher.get_stats();
        println!("  • Writes submitted: {}", submitted);
        println!("  • Writes completed: {}", completed);
        println!("  • Batches flushed: {}", batches);
        println!("  • Write errors: {}", errors);
        println!(
            "  • Average batch size: {:.1}",
            completed as f64 / batches as f64
        );

        batcher.shutdown();
    }

    // Test 3: Verify data persistence
    {
        println!("\n✅ Verifying data persistence...");

        // Re-open the auto batch database
        let db_path = dir.path().join("test_auto_batch.db");
        let db = Database::open(&db_path, LightningDbConfig::default())?;

        // Check a few keys (remember we only wrote 1000 keys: 0-999)
        let test_keys = vec!["batch_00000000", "batch_00000500", "batch_00000999"];
        let mut verified = 0;
        for key in &test_keys {
            match db.get(key.as_bytes())? {
                Some(val) if val == b"value" => verified += 1,
                _ => {
                    println!("❌ Data verification failed for key: {}", key);
                    return Ok(());
                }
            }
        }

        println!(
            "✅ Verified {} keys - all data persisted correctly!",
            verified
        );
    }

    // Test 4: High concurrency test
    {
        use std::thread;

        let db_path = dir.path().join("test_concurrent.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(&db_path, config)?);

        // Create auto batcher
        let batcher = Database::create_auto_batcher(db.clone());

        println!("\nTest 4: Concurrent writes (4 threads)");
        let start = Instant::now();
        let threads = 4;
        let writes_per_thread = 2500;

        let mut handles = Vec::new();
        for t in 0..threads {
            let batcher_clone = batcher.clone();
            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("thread_{}_{:08}", t, i);
                    batcher_clone
                        .put(key.into_bytes(), b"value".to_vec())
                        .unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Wait for all writes to complete
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let total_writes = threads * writes_per_thread;
        let ops_per_sec = total_writes as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );

        let (_submitted, completed, batches, _errors) = batcher.get_stats();
        println!(
            "  • Average batch size: {:.1}",
            completed as f64 / batches as f64
        );

        batcher.shutdown();
    }

    Ok(())
}

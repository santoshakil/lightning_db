use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Write Batcher Performance Test\n");

    let dir = tempdir()?;

    // Test 1: Baseline - Single writes with sync WAL
    {
        let db_path = dir.path().join("test_sync.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("Test 1: Single writes with sync WAL");
        let start = Instant::now();
        let count = 100;

        for i in 0..count {
            let key = format!("sync_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
    }

    // Test 2: Async WAL (no sync)
    {
        let db_path = dir.path().join("test_async.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("\nTest 2: Single writes with async WAL");
        let start = Instant::now();
        let count = 10000;

        for i in 0..count {
            let key = format!("async_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );
    }

    // Test 3: Write batcher with sync WAL
    {
        let db_path = dir.path().join("test_batcher.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        let db = Arc::new(Database::create(&db_path, config)?);

        // Create write batcher
        let batcher = Database::create_with_batcher(db.clone());

        println!("\nTest 3: Write batcher with sync WAL");
        let start = Instant::now();
        let count = 10000;

        for i in 0..count {
            let key = format!("batch_{:08}", i);
            batcher.put(key.into_bytes(), b"value".to_vec())?;
        }

        // Flush any remaining writes
        batcher.flush()?;

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!(
            "  • Status: {}",
            if ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );

        // Get stats
        let (queued, completed, batches) = batcher.get_stats();
        println!("  • Writes queued: {}", queued);
        println!("  • Writes completed: {}", completed);
        println!("  • Batches flushed: {}", batches);
        println!(
            "  • Average batch size: {:.1}",
            completed as f64 / batches as f64
        );
    }

    // Test 4: Verify data persistence
    {
        println!("\n✅ Verifying data persistence...");

        // Re-open the batcher database
        let db_path = dir.path().join("test_batcher.db");
        let db = Database::open(&db_path, LightningDbConfig::default())?;

        // Check a few keys
        let test_keys = vec!["batch_00000000", "batch_00001000", "batch_00009999"];
        for key in test_keys {
            match db.get(key.as_bytes())? {
                Some(val) if val == b"value" => {}
                _ => {
                    println!("❌ Data verification failed for key: {}", key);
                    return Ok(());
                }
            }
        }

        println!("✅ All data persisted correctly!");
    }

    Ok(())
}

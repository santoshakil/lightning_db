use lightning_db::{simple_batcher::SimpleBatcher, Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Write Methods Performance Comparison\n");

    let dir = tempdir()?;
    let count = 10000;

    // Test 1: Individual puts with sync WAL (baseline)
    {
        let db_path = dir.path().join("test_individual.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        config.compression_enabled = false; // Disable LSM to force B+Tree
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("Test 1: Individual puts (baseline)");
        let start = Instant::now();

        for i in 0..count {
            let key = format!("individual_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
    }

    // Test 2: Manual transaction batching
    {
        let db_path = dir.path().join("test_manual.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        config.compression_enabled = false;
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("\nTest 2: Manual transaction batching (1000/batch)");
        let start = Instant::now();
        let batch_size = 1000;

        for batch_start in (0..count).step_by(batch_size) {
            let tx_id = db.begin_transaction()?;

            let batch_end = std::cmp::min(batch_start + batch_size, count);
            for i in batch_start..batch_end {
                let key = format!("manual_{:08}", i);
                db.put_tx(tx_id, key.as_bytes(), b"value")?;
            }

            db.commit_transaction(tx_id)?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
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
    }

    // Test 3: Simple batcher
    {
        let db_path = dir.path().join("test_simple.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        config.compression_enabled = false;
        let db = Arc::new(Database::create(&db_path, config)?);

        let batcher = SimpleBatcher::new(db.clone(), 1000);

        println!("\nTest 3: Simple batcher (1000/batch)");
        let start = Instant::now();

        // Prepare all writes
        let writes: Vec<_> = (0..count)
            .map(|i| (format!("simple_{:08}", i).into_bytes(), b"value".to_vec()))
            .collect();

        batcher.put_many(writes)?;

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
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
    }

    // Test 4: Async WAL for comparison
    {
        let db_path = dir.path().join("test_async.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        config.compression_enabled = false;
        let db = Arc::new(Database::create(&db_path, config)?);

        println!("\nTest 4: Individual puts with async WAL");
        let start = Instant::now();

        for i in 0..count {
            let key = format!("async_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
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
    }

    // Verification
    {
        println!("\n✅ Verifying data persistence...");

        // Check manual batching database
        let db_path = dir.path().join("test_manual.db");
        let db = Database::open(&db_path, LightningDbConfig::default())?;

        let test_keys = vec!["manual_00000000", "manual_00005000", "manual_00009999"];
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
            "✅ Verified {} keys - manual batching data persisted correctly!",
            verified
        );
    }

    Ok(())
}

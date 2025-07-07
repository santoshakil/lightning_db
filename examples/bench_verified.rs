use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Verified Performance Test\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with async writes for performance
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = lightning_db::WalSyncMode::Async;

    let write_count = 10_000;
    let value_size = 100;
    let test_value = vec![b'x'; value_size];

    // Phase 1: Write performance test
    println!("Phase 1: Testing write performance...");
    {
        let db = Database::create(&db_path, config.clone())?;

        let start = Instant::now();
        for i in 0..write_count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), &test_value)?;
        }
        // Ensure all data is flushed to disk
        db.sync()?;

        let write_duration = start.elapsed();
        let write_ops_per_sec = write_count as f64 / write_duration.as_secs_f64();
        let write_us_per_op = write_duration.as_micros() as f64 / write_count as f64;

        println!("  • {:.0} ops/sec", write_ops_per_sec);
        println!("  • {:.2} μs/op", write_us_per_op);
        println!(
            "  • Status: {}",
            if write_ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );

        // Verify some data before closing
        let test_key = format!("key_{:08}", 0);
        match db.get(test_key.as_bytes())? {
            Some(val) if val == test_value => println!("  • Pre-close verification: ✅ PASS"),
            _ => println!("  • Pre-close verification: ❌ FAIL"),
        }
    } // Database closed here

    // Phase 2: Reopen and verify persistence
    println!("\nPhase 2: Verifying data persistence...");
    {
        let db = Database::open(&db_path, config.clone())?;

        // Check first, middle, and last keys
        let test_indices = [0, write_count / 2, write_count - 1];
        let mut verified = 0;

        for &idx in &test_indices {
            let key = format!("key_{:08}", idx);
            match db.get(key.as_bytes())? {
                Some(val) if val == test_value => verified += 1,
                Some(_) => println!("  • Key {} has wrong value", key),
                None => println!("  • Key {} not found", key),
            }
        }

        if verified == test_indices.len() {
            println!(
                "  • Sample keys verified: ✅ PASS ({}/{})",
                verified,
                test_indices.len()
            );
        } else {
            println!(
                "  • Sample keys verified: ❌ FAIL ({}/{})",
                verified,
                test_indices.len()
            );
        }

        // Count all keys to ensure none were lost
        let mut count = 0;
        for i in 0..write_count {
            let key = format!("key_{:08}", i);
            if db.get(key.as_bytes())?.is_some() {
                count += 1;
            }
        }

        if count == write_count {
            println!("  • All {} keys persisted: ✅ PASS", count);
        } else {
            println!(
                "  • Key count mismatch: ❌ FAIL ({}/{})",
                count, write_count
            );
        }
    }

    // Phase 3: Test with batched writes for better performance
    println!("\nPhase 3: Testing batched writes with verification...");
    {
        let batch_db_path = dir.path().join("batch_test.db");
        let db = Database::create(&batch_db_path, config.clone())?;

        let batch_size = 1000;
        let num_batches = 10;
        let total_writes = batch_size * num_batches;

        let start = Instant::now();

        for batch in 0..num_batches {
            let tx_id = db.begin_transaction()?;

            for i in 0..batch_size {
                let key = format!("batch_{:04}_{:04}", batch, i);
                db.put_tx(tx_id, key.as_bytes(), &test_value)?;
            }

            db.commit_transaction(tx_id)?;
        }

        db.sync()?;

        let batch_duration = start.elapsed();
        let batch_ops_per_sec = total_writes as f64 / batch_duration.as_secs_f64();

        println!("  • Batched writes: {:.0} ops/sec", batch_ops_per_sec);
        println!(
            "  • Status: {}",
            if batch_ops_per_sec >= 100_000.0 {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );

        // Verify batch data
        drop(db); // Close database

        let db = Database::open(&batch_db_path, config.clone())?;
        let test_key = format!("batch_{:04}_{:04}", 0, 0);
        match db.get(test_key.as_bytes())? {
            Some(val) if val == test_value => println!("  • Batch data persisted: ✅ PASS"),
            _ => println!("  • Batch data persisted: ❌ FAIL"),
        }
    }

    println!("\n📊 Summary:");
    println!("  • Always verify data persistence after benchmarks");
    println!("  • Batched writes achieve much better performance");
    println!("  • sync() is crucial for durability with async mode");

    Ok(())
}

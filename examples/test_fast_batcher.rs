use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Fast AutoBatcher Performance Test\n");

    let dir = tempdir()?;
    let value = vec![0u8; 100];

    // Test 1: FastAutoBatcher
    {
        println!("Test 1: FastAutoBatcher (direct writes, no transactions)");
        let db_path = dir.path().join("fast_batcher.db");
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..LightningDbConfig::default()
        };
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_fast_auto_batcher(db.clone());

        let count = 100_000;
        let start = Instant::now();

        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
        println!(
            "  Latency: {:.2} μs/op",
            duration.as_micros() as f64 / count as f64
        );

        // Verify data
        let test_key = format!("key{:06}", count / 2);
        if db.get(test_key.as_bytes())?.is_some() {
            println!("  ✅ Data verified");
        } else {
            println!("  ❌ Data not found!");
        }

        let (submitted, completed, batches, errors) = batcher.get_stats();
        println!(
            "  Stats: {} submitted, {} completed, {} batches, {} errors",
            submitted, completed, batches, errors
        );
    }

    // Test 2: Compare with regular AutoBatcher
    {
        println!("\nTest 2: Regular AutoBatcher (transactional)");
        let db_path = dir.path().join("regular_batcher.db");
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..LightningDbConfig::default()
        };
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_auto_batcher(db.clone());

        let count = 100_000;
        let start = Instant::now();

        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
        println!(
            "  Latency: {:.2} μs/op",
            duration.as_micros() as f64 / count as f64
        );
    }

    // Test 3: Different batch sizes
    {
        println!("\nTest 3: FastAutoBatcher with different batch sizes");
        for &batch_size in &[100, 500, 1000, 5000, 10000] {
            let db_path = dir.path().join(format!("batch_{}.db", batch_size));
            let config = LightningDbConfig {
                wal_sync_mode: WalSyncMode::Async,
                ..LightningDbConfig::default()
            };
            let db = Arc::new(Database::create(&db_path, config)?);
            let batcher = Arc::new(lightning_db::FastAutoBatcher::new(
                db.clone(),
                batch_size,
                5,
            ));

            let count = 50_000;
            let start = Instant::now();

            for i in 0..count {
                batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
            }
            batcher.flush()?;
            batcher.wait_for_completion()?;

            let duration = start.elapsed();
            let ops_sec = count as f64 / duration.as_secs_f64();
            println!("  Batch size {}: {:.0} ops/sec", batch_size, ops_sec);
        }
    }

    Ok(())
}

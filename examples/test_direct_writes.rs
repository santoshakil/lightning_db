use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Direct Write Performance Test\n");

    let dir = tempdir()?;
    let value = vec![0u8; 100];

    // Test 1: Direct puts without AutoBatcher
    {
        println!("Test 1: Direct puts (no AutoBatcher)");
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..Default::default()
        };

        let db_path = dir.path().join("direct.db");
        let db = Database::create(&db_path, config)?;

        let count = 1000;
        let start = Instant::now();

        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), &value)?;
        }

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec", ops_sec);
    }

    // Test 2: Direct puts without LSM
    {
        println!("\nTest 2: Direct puts without LSM");
        let mut config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..Default::default()
        };
        config.compression_enabled = false;

        let db_path = dir.path().join("direct_no_lsm.db");
        let db = Database::create(&db_path, config)?;

        // Ensure write buffer is flushed periodically
        let flush_interval = 100;

        let count = 1000; // Test with full 1000 writes now that we have write buffer
        let start = Instant::now();

        for i in 0..count {
            if let Err(e) = db.put(format!("key{:06}", i).as_bytes(), &value) {
                eprintln!("Error at iteration {}: {:?}", i, e);
                return Err(e.into());
            }

            // Periodically flush write buffer
            if i > 0 && i % flush_interval == 0 {
                db.flush_write_buffer()?;
            }
        }

        // Final flush
        db.flush_write_buffer()?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec", ops_sec);
    }

    // Test 3: Transactional batch
    {
        println!("\nTest 3: Single transaction with 10K puts");
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..Default::default()
        };

        let db_path = dir.path().join("tx_batch.db");
        let db = Database::create(&db_path, config)?;

        let count = 10000;
        let start = Instant::now();

        let tx_id = db.begin_transaction()?;
        for i in 0..count {
            db.put_tx(tx_id, format!("key{:06}", i).as_bytes(), &value)?;
        }
        db.commit_transaction(tx_id)?;

        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec", ops_sec);
    }

    // Test 4: Multiple smaller transactions
    {
        println!("\nTest 4: 100 transactions with 100 puts each");
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Async,
            ..Default::default()
        };

        let db_path = dir.path().join("multi_tx.db");
        let db = Database::create(&db_path, config)?;

        let tx_count = 100;
        let ops_per_tx = 100;
        let start = Instant::now();

        for tx in 0..tx_count {
            let tx_id = db.begin_transaction()?;
            for i in 0..ops_per_tx {
                db.put_tx(tx_id, format!("tx{}_key{:06}", tx, i).as_bytes(), &value)?;
            }
            db.commit_transaction(tx_id)?;
        }

        let duration = start.elapsed();
        let total_ops = tx_count * ops_per_tx;
        let ops_sec = total_ops as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec", ops_sec);
        println!(
            "  Average: {:.2} ms per transaction",
            duration.as_secs_f64() * 1000.0 / tx_count as f64
        );
    }

    Ok(())
}

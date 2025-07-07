use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Testing Auto Batcher with Sync WAL\n");

    let dir = tempdir()?;

    // Test: Auto batcher with sync WAL
    let db_path = dir.path().join("test_auto_batch.db");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Sync;
    config.compression_enabled = true; // Ensure LSM is enabled
    println!("Creating database with sync WAL...");
    let db = Arc::new(Database::create(&db_path, config)?);

    // Create auto batcher
    println!("Creating auto batcher...");
    let batcher = Database::create_auto_batcher(db.clone());

    println!("Starting write test...");
    let start = Instant::now();
    let count = 100; // Start with fewer writes for debugging

    for i in 0..count {
        if i % 10 == 0 {
            println!("  Writing key {}/{}", i, count);
        }
        let key = format!("batch_{:08}", i);
        batcher.put(key.into_bytes(), b"value".to_vec())?;
    }

    println!("Waiting for completion...");
    // Wait for all writes to complete
    batcher.wait_for_completion()?;

    let duration = start.elapsed();
    let ops_per_sec = count as f64 / duration.as_secs_f64();
    println!("\nResults:");
    println!("  • {:.0} ops/sec", ops_per_sec);
    println!("  • Time: {:.2}s", duration.as_secs_f64());
    println!(
        "  • Status: {}",
        if ops_per_sec >= 1000.0 {
            "✅ PASS"
        } else {
            "❌ FAIL (target: 1000 ops/sec)"
        }
    );

    // Get stats
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!("\nStats:");
    println!("  • Writes submitted: {}", submitted);
    println!("  • Writes completed: {}", completed);
    println!("  • Batches flushed: {}", batches);
    println!("  • Write errors: {}", errors);
    if batches > 0 {
        println!(
            "  • Average batch size: {:.1}",
            completed as f64 / batches as f64
        );
    }

    batcher.shutdown();

    Ok(())
}

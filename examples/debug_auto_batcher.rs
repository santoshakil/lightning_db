use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Debugging Auto Batcher Hang");
    println!("===============================\n");

    let dir = tempdir()?;

    // Test 1: Create database and auto batcher
    println!("1. Creating database and auto batcher...");
    let start = Instant::now();
    let db = Arc::new(Database::create(
        dir.path().join("batch.db"),
        LightningDbConfig::default(),
    )?);
    println!(
        "   ⏱️  Database created in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    let start = Instant::now();
    let batcher = Database::create_auto_batcher(db.clone());
    println!(
        "   ⏱️  Auto batcher created in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    // Test 2: Submit a few operations
    println!("\n2. Submitting operations...");
    for i in 0..5 {
        let start = Instant::now();
        let key = format!("key_{}", i);
        batcher.put(key.into_bytes(), b"value".to_vec())?;
        println!("   ⏱️  Put {} in: {:.3}s", i, start.elapsed().as_secs_f64());
    }

    // Test 3: Check stats before waiting
    println!("\n3. Checking stats...");
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!(
        "   📊 Submitted: {}, Completed: {}, Batches: {}, Errors: {}",
        submitted, completed, batches, errors
    );

    // Test 4: Wait for completion (potential hang point)
    println!("\n4. Waiting for completion...");
    let start = Instant::now();

    // Set a manual timeout to see if this is where it hangs
    match batcher.wait_for_completion() {
        Ok(_) => {
            println!(
                "   ⏱️  Wait completed in: {:.3}s",
                start.elapsed().as_secs_f64()
            );
        }
        Err(e) => {
            println!(
                "   ❌ Wait failed after: {:.3}s - {}",
                start.elapsed().as_secs_f64(),
                e
            );
        }
    }

    // Test 5: Check final stats
    println!("\n5. Final stats...");
    let (submitted, completed, batches, errors) = batcher.get_stats();
    println!(
        "   📊 Submitted: {}, Completed: {}, Batches: {}, Errors: {}",
        submitted, completed, batches, errors
    );

    // Test 6: Shutdown
    println!("\n6. Shutting down...");
    let start = Instant::now();
    batcher.shutdown();
    println!("   ⏱️  Shutdown in: {:.3}s", start.elapsed().as_secs_f64());

    // Test 7: Verify data was written
    println!("\n7. Verifying data...");
    let result = db.get(b"key_0")?;
    if result == Some(b"value".to_vec()) {
        println!("   ✅ Data verification passed");
    } else {
        println!("   ❌ Data verification failed");
    }

    println!("\n🏁 Auto batcher debug complete");
    Ok(())
}

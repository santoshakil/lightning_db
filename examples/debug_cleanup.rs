use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn test_cleanup_scenario(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing {}", name);

    let dir = tempdir()?;

    // Create database
    let start = Instant::now();
    let db = Arc::new(Database::create(
        dir.path().join("test.db"),
        LightningDbConfig::default(),
    )?);
    println!(
        "  Database created in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    // Do some work
    let start = Instant::now();
    db.put(b"key", b"value")?;
    println!("  Work done in: {:.3}s", start.elapsed().as_secs_f64());

    // Drop database explicitly and measure time
    let start = Instant::now();
    drop(db);
    println!(
        "  Database dropped in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    println!("  ✅ {} completed\n", name);
    Ok(())
}

fn test_auto_batcher_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing auto batcher cleanup");

    let dir = tempdir()?;

    // Create database and auto batcher
    let start = Instant::now();
    let db = Arc::new(Database::create(
        dir.path().join("batch.db"),
        LightningDbConfig::default(),
    )?);
    let batcher = Database::create_auto_batcher(db.clone());
    println!("  Created in: {:.3}s", start.elapsed().as_secs_f64());

    // Do some work
    let start = Instant::now();
    batcher.put(b"key".to_vec(), b"value".to_vec())?;
    batcher.wait_for_completion()?;
    println!("  Work done in: {:.3}s", start.elapsed().as_secs_f64());

    // Drop auto batcher explicitly and measure time
    let start = Instant::now();
    batcher.shutdown();
    drop(batcher);
    println!(
        "  Auto batcher dropped in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    // Drop database
    let start = Instant::now();
    drop(db);
    println!(
        "  Database dropped in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    println!("  ✅ Auto batcher cleanup completed\n");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧹 Debugging Cleanup Times");
    println!("==========================\n");

    // Test 1: Basic database cleanup
    test_cleanup_scenario("Basic Database")?;

    // Test 2: LSM database cleanup
    {
        println!("Testing LSM Database");
        let dir = tempdir()?;
        let start = Instant::now();
        let mut config = LightningDbConfig::default();
        config.compression_enabled = true;
        let db = Database::create(dir.path().join("lsm.db"), config)?;
        println!(
            "  LSM Database created in: {:.3}s",
            start.elapsed().as_secs_f64()
        );

        let start = Instant::now();
        for i in 0..100 {
            db.put(format!("key_{}", i).as_bytes(), b"value")?;
        }
        println!("  LSM Work done in: {:.3}s", start.elapsed().as_secs_f64());

        let start = Instant::now();
        drop(db);
        println!(
            "  LSM Database dropped in: {:.3}s",
            start.elapsed().as_secs_f64()
        );
        println!("  ✅ LSM cleanup completed\n");
    }

    // Test 3: Auto batcher cleanup
    test_auto_batcher_cleanup()?;

    // Test 4: Multiple sequential tests (like what happens in test suite)
    println!("Testing sequential test pattern");
    for i in 0..3 {
        let start = Instant::now();
        test_cleanup_scenario(&format!("Sequential test {}", i))?;
        println!(
            "  Full cycle {} took: {:.3}s",
            i,
            start.elapsed().as_secs_f64()
        );
    }

    println!("🏁 Cleanup debug complete");
    Ok(())
}

use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🐌 Debugging Slowness");
    println!("====================\n");

    let dir = tempdir()?;

    // Test 1: Database creation time
    println!("1. Database creation...");
    let start = Instant::now();
    let db = Database::create(dir.path().join("test.db"), LightningDbConfig::default())?;
    println!(
        "   ⏱️  Database created in: {:.2}s",
        start.elapsed().as_secs_f64()
    );

    // Test 2: First put operation
    println!("\n2. First put operation...");
    let start = Instant::now();
    db.put(b"key1", b"value1")?;
    println!("   ⏱️  First put in: {:.3}s", start.elapsed().as_secs_f64());

    // Test 3: Subsequent put operations
    println!("\n3. Subsequent put operations...");
    for i in 2..=5 {
        let start = Instant::now();
        let key = format!("key{}", i);
        db.put(key.as_bytes(), b"value")?;
        println!("   ⏱️  Put {} in: {:.3}s", i, start.elapsed().as_secs_f64());
    }

    // Test 4: Get operations
    println!("\n4. Get operations...");
    for i in 1..=3 {
        let start = Instant::now();
        let key = format!("key{}", i);
        let _result = db.get(key.as_bytes())?;
        println!("   ⏱️  Get {} in: {:.3}s", i, start.elapsed().as_secs_f64());
    }

    // Test 5: Transaction operations
    println!("\n5. Transaction operations...");
    let start = Instant::now();
    let tx_id = db.begin_transaction()?;
    println!(
        "   ⏱️  Begin transaction in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    let start = Instant::now();
    db.put_tx(tx_id, b"tx_key", b"tx_value")?;
    println!(
        "   ⏱️  Put in transaction in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    let start = Instant::now();
    db.commit_transaction(tx_id)?;
    println!(
        "   ⏱️  Commit transaction in: {:.3}s",
        start.elapsed().as_secs_f64()
    );

    println!("\n🏁 Debug complete");
    Ok(())
}

use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Lightning DB Quick Functional Test\n");

    // Clean up
    let _ = std::fs::remove_dir_all("./quick_test_db");

    // Test 1: Basic operations
    println!("1️⃣ Testing basic operations...");
    let db = Database::create("./quick_test_db", LightningDbConfig::default())?;

    // Put
    db.put(b"key1", b"value1")?;
    println!("  ✓ Put operation successful");

    // Get
    let value = db.get(b"key1")?;
    if value.as_deref() == Some(&b"value1"[..]) {
        println!("  ✓ Get operation successful");
    } else {
        println!("  ❌ Get operation failed: got {:?}", value);
    }

    // Update
    db.put(b"key1", b"value2")?;
    let value = db.get(b"key1")?;
    if value.as_deref() == Some(&b"value2"[..]) {
        println!("  ✓ Update operation successful");
    } else {
        println!("  ❌ Update operation failed");
    }

    // Delete
    db.delete(b"key1")?;
    let value = db.get(b"key1")?;
    if value.is_none() {
        println!("  ✓ Delete operation successful");
    } else {
        println!("  ❌ Delete operation failed");
    }

    // Test 2: Batch operations
    println!("\n2️⃣ Testing batch operations...");
    let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
        .map(|i| {
            (
                format!("key_{}", i).into_bytes(),
                format!("value_{}", i).into_bytes(),
            )
        })
        .collect();

    let start = Instant::now();
    db.put_batch(&batch)?;
    let elapsed = start.elapsed();
    println!("  ✓ Batch put of 100 items in {:?}", elapsed);

    // Verify a few
    for i in [0, 50, 99] {
        let key = format!("key_{}", i);
        let expected = format!("value_{}", i);
        if db.get(key.as_bytes())?.as_deref() == Some(expected.as_bytes()) {
            println!("  ✓ Batch item {} verified", i);
        } else {
            println!("  ❌ Batch item {} failed", i);
        }
    }

    // Test 3: Transaction
    println!("\n3️⃣ Testing transactions...");
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx)?;

    if db.get(b"tx_key")?.as_deref() == Some(&b"tx_value"[..]) {
        println!("  ✓ Transaction commit successful");
    } else {
        println!("  ❌ Transaction commit failed");
    }

    // Test abort
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"abort_key", b"abort_value")?;
    db.abort_transaction(tx)?;

    if db.get(b"abort_key")?.is_none() {
        println!("  ✓ Transaction abort successful");
    } else {
        println!("  ❌ Transaction abort failed");
    }

    // Test 4: Range scan
    println!("\n4️⃣ Testing range queries...");

    // Insert ordered data
    for i in 0..10 {
        let key = format!("scan_{:02}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Scan range
    let iter = db.scan(Some(b"scan_03".to_vec()), Some(b"scan_07".to_vec()))?;
    let results: Vec<_> = iter.collect::<Result<Vec<_>, _>>()?;

    if results.len() == 4 {
        println!("  ✓ Range scan returned correct count: {}", results.len());
    } else {
        println!(
            "  ❌ Range scan returned wrong count: {} (expected 4)",
            results.len()
        );
    }

    // Test prefix scan
    let iter = db.scan_prefix(b"scan_")?;
    let count = iter.count();
    if count == 10 {
        println!("  ✓ Prefix scan successful: {} items", count);
    } else {
        println!("  ❌ Prefix scan failed: {} items (expected 10)", count);
    }

    // Test 5: Performance quick check
    println!("\n5️⃣ Testing performance...");

    // Write performance
    let start = Instant::now();
    for i in 0..10000 {
        db.put(format!("perf_{}", i).as_bytes(), b"value")?;
    }
    let write_elapsed = start.elapsed();
    let write_ops = 10000.0 / write_elapsed.as_secs_f64();
    println!(
        "  Write: {:.0} ops/sec ({:.2} μs/op)",
        write_ops,
        write_elapsed.as_micros() as f64 / 10000.0
    );

    // Read performance
    let start = Instant::now();
    for i in 0..10000 {
        let _ = db.get(format!("perf_{}", i).as_bytes())?;
    }
    let read_elapsed = start.elapsed();
    let read_ops = 10000.0 / read_elapsed.as_secs_f64();
    println!(
        "  Read: {:.0} ops/sec ({:.2} μs/op)",
        read_ops,
        read_elapsed.as_micros() as f64 / 10000.0
    );

    // Test 6: Metrics
    println!("\n6️⃣ Testing metrics and monitoring...");
    let metrics = db.get_metrics();
    println!("  Reads: {}", metrics.reads);
    println!("  Writes: {}", metrics.writes);
    println!("  Cache hit rate: {:.2}%", metrics.cache_hit_rate * 100.0);

    if let Some(tx_stats) = db.get_transaction_statistics() {
        println!("  Active transactions: {}", tx_stats.active_transactions);
        println!("  Commits: {}", tx_stats.commit_count);
    }

    // Test 7: Persistence
    println!("\n7️⃣ Testing persistence...");
    db.put(b"persist_key", b"persist_value")?;
    db.flush_lsm()?;
    drop(db);

    // Reopen
    let db = Database::open("./quick_test_db", LightningDbConfig::default())?;
    if db.get(b"persist_key")?.as_deref() == Some(&b"persist_value"[..]) {
        println!("  ✓ Data persisted correctly");
    } else {
        println!("  ❌ Data persistence failed");
    }

    println!("\n✅ Quick functional test completed!");

    Ok(())
}

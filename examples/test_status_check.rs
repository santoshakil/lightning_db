use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Status Check\n");

    let dir = tempdir()?;
    let mut results = Vec::new();

    // Test 1: Basic operations
    {
        print!("Test 1: Basic put/get...");
        let db = Database::create(dir.path().join("basic.db"), LightningDbConfig::default())?;
        db.put(b"key1", b"value1")?;
        let result = db.get(b"key1")?;
        if result == Some(b"value1".to_vec()) {
            println!(" ✅ PASS");
            results.push(("Basic operations", true));
        } else {
            println!(" ❌ FAIL");
            results.push(("Basic operations", false));
        }
    }

    // Test 2: Transactions
    {
        print!("Test 2: Transactions...");
        let db = Database::create(dir.path().join("tx.db"), LightningDbConfig::default())?;
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"tx_key", b"tx_value")?;
        db.commit_transaction(tx_id)?;
        let result = db.get(b"tx_key")?;
        if result == Some(b"tx_value".to_vec()) {
            println!(" ✅ PASS");
            results.push(("Transactions", true));
        } else {
            println!(" ❌ FAIL");
            results.push(("Transactions", false));
        }
    }

    // Test 3: Delete operations
    {
        print!("Test 3: Delete operations...");
        let db = Database::create(dir.path().join("delete.db"), LightningDbConfig::default())?;
        db.put(b"del_key", b"del_value")?;
        db.delete(b"del_key")?;
        let result = db.get(b"del_key")?;
        if result.is_none() {
            println!(" ✅ PASS");
            results.push(("Delete operations", true));
        } else {
            println!(" ❌ FAIL");
            results.push(("Delete operations", false));
        }
    }

    // Test 4: LSM tree
    {
        print!("Test 4: LSM tree...");
        let mut config = LightningDbConfig::default();
        config.compression_enabled = true;
        let db = Database::create(dir.path().join("lsm.db"), config)?;
        db.put(b"lsm_key", b"lsm_value")?;
        let result = db.get(b"lsm_key")?;
        if result == Some(b"lsm_value".to_vec()) {
            println!(" ✅ PASS");
            results.push(("LSM tree", true));
        } else {
            println!(" ❌ FAIL");
            results.push(("LSM tree", false));
        }
    }

    // Test 5: Auto batcher
    {
        print!("Test 5: Auto batcher...");
        let db = Arc::new(Database::create(
            dir.path().join("batch.db"),
            LightningDbConfig::default(),
        )?);
        let batcher = Database::create_auto_batcher(db.clone());
        batcher.put(b"batch_key".to_vec(), b"batch_value".to_vec())?;
        batcher.wait_for_completion()?;
        let result = db.get(b"batch_key")?;
        if result == Some(b"batch_value".to_vec()) {
            println!(" ✅ PASS");
            results.push(("Auto batcher", true));
        } else {
            println!(" ❌ FAIL");
            results.push(("Auto batcher", false));
        }
        batcher.shutdown();
    }

    // Summary
    println!("\n📊 SUMMARY:");
    let total = results.len();
    let passed = results.iter().filter(|(_, pass)| *pass).count();
    println!("  Total tests: {}", total);
    println!("  Passed: {} ({}%)", passed, passed * 100 / total);
    println!(
        "  Failed: {} ({}%)",
        total - passed,
        (total - passed) * 100 / total
    );

    println!("\n🔍 DETAILED RESULTS:");
    for (test, pass) in &results {
        println!("  {} {}", if *pass { "✅" } else { "❌" }, test);
    }

    Ok(())
}

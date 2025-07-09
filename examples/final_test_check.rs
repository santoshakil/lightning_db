use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Final Test Check - Key Functionality");
    println!("=======================================\n");

    let mut all_passed = true;

    // Test 1: Basic CRUD
    {
        println!("Test 1: Basic CRUD operations");
        let dir = tempdir()?;
        let db = Database::create(dir.path().join("crud.db"), LightningDbConfig::default())?;

        // Put/Get
        db.put(b"key1", b"value1")?;
        let result = db.get(b"key1")?;
        if result != Some(b"value1".to_vec()) {
            println!("  ❌ Put/Get failed");
            all_passed = false;
        } else {
            println!("  ✅ Put/Get works");
        }

        // Delete
        db.delete(b"key1")?;
        let result = db.get(b"key1")?;
        if result.is_some() {
            println!("  ❌ Delete failed");
            all_passed = false;
        } else {
            println!("  ✅ Delete works");
        }
    }

    // Test 2: Transactions
    {
        println!("\nTest 2: Transaction operations");
        let dir = tempdir()?;
        let db = Database::create(dir.path().join("tx.db"), LightningDbConfig::default())?;

        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"tx_key", b"tx_value")?;
        db.commit_transaction(tx_id)?;

        let result = db.get(b"tx_key")?;
        if result != Some(b"tx_value".to_vec()) {
            println!("  ❌ Transaction failed");
            all_passed = false;
        } else {
            println!("  ✅ Transactions work");
        }
    }

    // Test 3: LSM Tree
    {
        println!("\nTest 3: LSM Tree operations");
        let dir = tempdir()?;
        let config = LightningDbConfig {
            compression_enabled: true,
            ..Default::default()
        };
        let db = Database::create(dir.path().join("lsm.db"), config)?;

        for i in 0..10 {
            let key = format!("lsm_key_{}", i);
            let value = format!("lsm_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }

        let result = db.get(b"lsm_key_5")?;
        if result != Some(b"lsm_value_5".to_vec()) {
            println!("  ❌ LSM Tree failed");
            all_passed = false;
        } else {
            println!("  ✅ LSM Tree works");
        }
    }

    // Test 4: Auto Batcher
    {
        println!("\nTest 4: Auto Batcher");
        let dir = tempdir()?;
        let db = Arc::new(Database::create(
            dir.path().join("batch.db"),
            LightningDbConfig::default(),
        )?);
        let batcher = Database::create_auto_batcher(db.clone());

        for i in 0..10 {
            let key = format!("batch_key_{}", i);
            let value = format!("batch_value_{}", i);
            batcher.put(key.into_bytes(), value.into_bytes())?;
        }

        batcher.wait_for_completion()?;

        let result = db.get(b"batch_key_5")?;
        if result != Some(b"batch_value_5".to_vec()) {
            println!("  ❌ Auto Batcher failed");
            all_passed = false;
        } else {
            println!("  ✅ Auto Batcher works");
        }

        batcher.shutdown();
    }

    // Test 5: Sync WAL
    {
        println!("\nTest 5: Sync WAL");
        let dir = tempdir()?;
        let config = LightningDbConfig {
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        let db = Database::create(dir.path().join("sync.db"), config)?;

        let start = Instant::now();
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"sync_key", b"sync_value")?;
        db.commit_transaction(tx_id)?;
        let duration = start.elapsed();

        let result = db.get(b"sync_key")?;
        if result != Some(b"sync_value".to_vec()) {
            println!("  ❌ Sync WAL failed");
            all_passed = false;
        } else {
            println!(
                "  ✅ Sync WAL works ({:.1}ms)",
                duration.as_secs_f64() * 1000.0
            );
        }
    }

    // Final result
    println!("\n🏁 Final Result:");
    if all_passed {
        println!("  🎉 ALL CORE FUNCTIONALITY WORKS!");
        println!("  🚀 Database is ready for use");
    } else {
        println!("  ⚠️  Some functionality failed");
        println!("  🔧 Needs further investigation");
    }

    Ok(())
}

use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB LSM Flush Test");
    println!("============================\n");

    // Test 1: Without explicit flush
    println!("Test 1: Persistence without explicit flush");
    println!("-------------------------------------------");
    test_without_flush()?;

    // Test 2: With explicit flush
    println!("\nTest 2: Persistence with sync after commits");
    println!("---------------------------------------------");
    test_with_sync()?;

    // Test 3: Database restart test
    println!("\nTest 3: Data persistence across restart");
    println!("-----------------------------------------");
    test_restart_persistence()?;

    Ok(())
}

fn test_without_flush() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;

    // Write 100 values
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Read them back immediately
    let mut found = 0;
    for i in 0..100 {
        let key = format!("key_{}", i);
        if db.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }

    println!("  Wrote 100 entries");
    println!("  Found immediately: {}/100", found);

    // Now create a new database instance (simulating restart)
    drop(db);
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;

    let mut found_after_restart = 0;
    for i in 0..100 {
        let key = format!("key_{}", i);
        if db2.get(key.as_bytes())?.is_some() {
            found_after_restart += 1;
        }
    }

    println!("  Found after restart: {}/100", found_after_restart);

    if found_after_restart == 100 {
        println!("  ✅ PASSED: All data persisted");
    } else {
        println!("  ❌ FAILED: Lost {} entries", 100 - found_after_restart);
    }

    Ok(())
}

fn test_with_sync() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;

    // Write 100 values with sync
    for i in 0..100 {
        let key = format!("sync_key_{}", i);
        let value = format!("sync_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Force sync
    db.sync()?;

    // Now create a new database instance
    drop(db);
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;

    let mut found = 0;
    for i in 0..100 {
        let key = format!("sync_key_{}", i);
        if db2.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }

    println!("  Wrote 100 entries with sync");
    println!("  Found after restart: {}/100", found);

    if found == 100 {
        println!("  ✅ PASSED: All data persisted with sync");
    } else {
        println!("  ⚠️  WARNING: Lost {} entries even with sync", 100 - found);
    }

    Ok(())
}

fn test_restart_persistence() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    
    // Phase 1: Write data in transactions
    let entries_written = {
        let config = LightningDbConfig::default();
        let db = Database::create(dir.path(), config)?;

        let mut successful = 0;
        for i in 0..50 {
            let tx_id = db.begin_transaction()?;
            let key = format!("tx_key_{}", i);
            let value = format!("tx_value_{}", i);
            
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
            
            if db.commit_transaction(tx_id).is_ok() {
                successful += 1;
            } else {
                db.abort_transaction(tx_id)?;
            }
        }
        
        println!("  Phase 1: {} transactions committed", successful);
        successful
    }; // Database dropped here

    // Phase 2: Reopen and check
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
    
    let mut found = 0;
    for i in 0..50 {
        let key = format!("tx_key_{}", i);
        if db2.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }

    println!("  Phase 2: {}/{} entries found after restart", found, entries_written);

    if found == entries_written {
        println!("  ✅ PASSED: All committed transactions persisted");
    } else if found > 0 {
        println!("  ⚠️  WARNING: Only {}/{} transactions persisted", found, entries_written);
    } else {
        println!("  ❌ FAILED: No transactions persisted!");
    }

    // Phase 3: Write more data and check again
    for i in 50..100 {
        let key = format!("tx_key_{}", i);
        let value = format!("tx_value_{}", i);
        db2.put(key.as_bytes(), value.as_bytes())?;
    }
    
    db2.sync()?;
    drop(db2);

    // Final check
    let db3 = Database::open(dir.path(), LightningDbConfig::default())?;
    let mut total_found = 0;
    for i in 0..100 {
        let key = format!("tx_key_{}", i);
        if db3.get(key.as_bytes())?.is_some() {
            total_found += 1;
        }
    }

    println!("  Phase 3: {}/100 total entries after final restart", total_found);

    Ok(())
}
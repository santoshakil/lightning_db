use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB SSTable Recovery Debug Test");
    println!("=========================================\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Phase 1: Write test data");
    println!("------------------------");
    
    // Write data and force flush to SSTable
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Write some test data
        for i in 0..100 {
            let key = format!("test_key_{:04}", i);
            let value = format!("test_value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        println!("Wrote 100 entries");
        
        // Force sync to create SSTable
        db.sync()?;
        println!("Synced to disk");
        
        // Check if data is readable before crash
        let mut readable_before = 0;
        for i in 0..100 {
            let key = format!("test_key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                readable_before += 1;
            }
        }
        println!("Readable before crash: {}/100", readable_before);
        
        // Simulate crash
        println!("\n💥 Simulating crash...");
        std::mem::forget(db);
    }
    
    // Small delay
    thread::sleep(Duration::from_millis(100));
    
    // Check SSTable files on disk
    println!("\nPhase 2: Check SSTable files on disk");
    println!("-------------------------------------");
    
    let lsm_dir = db_path.join("lsm");
    if lsm_dir.exists() {
        let mut sst_count = 0;
        for entry in std::fs::read_dir(&lsm_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "sst" {
                    sst_count += 1;
                    let metadata = std::fs::metadata(&path)?;
                    println!("  Found SSTable: {} (size: {} bytes)", 
                            path.file_name().unwrap().to_string_lossy(),
                            metadata.len());
                }
            }
        }
        println!("Total SSTable files: {}", sst_count);
    } else {
        println!("No LSM directory found!");
    }
    
    println!("\nPhase 3: Recovery and verification");
    println!("-----------------------------------");
    
    // Reopen database
    let db = Database::open(&db_path, LightningDbConfig::default())?;
    
    // Count recovered entries
    let mut recovered = 0;
    let mut first_missing = None;
    
    for i in 0..100 {
        let key = format!("test_key_{:04}", i);
        if let Some(value) = db.get(key.as_bytes())? {
            let expected = format!("test_value_{:04}", i);
            if value == expected.as_bytes() {
                recovered += 1;
            } else {
                println!("❌ Wrong value for key {}: got {:?}, expected {:?}", 
                        key, String::from_utf8_lossy(&value), expected);
            }
        } else {
            if first_missing.is_none() {
                first_missing = Some(i);
            }
        }
    }
    
    println!("Recovered {}/100 entries", recovered);
    if let Some(idx) = first_missing {
        println!("First missing entry at index: {}", idx);
    }
    
    // Try to manually check LSM tree structure
    println!("\nPhase 4: Debug LSM tree state");
    println!("------------------------------");
    
    // Write a new entry to see if it works
    db.put(b"post_recovery_test", b"value")?;
    if db.get(b"post_recovery_test")?.is_some() {
        println!("✅ New writes work after recovery");
    } else {
        println!("❌ New writes don't work after recovery");
    }
    
    // Summary
    println!("\n=== SUMMARY ===");
    if recovered == 100 {
        println!("✅ SUCCESS: All data recovered from SSTable files");
    } else {
        println!("❌ FAILURE: Lost {} entries", 100 - recovered);
        println!("\nPossible issues:");
        println!("1. SSTable files not properly synced");
        println!("2. SSTable recovery not loading all data");
        println!("3. LSM tree get() not searching recovered SSTables");
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::path::Path;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Flush Debug Test");
    println!("==============================\n");
    
    let dir = tempdir()?;
    let config = LightningDbConfig::default();
    let db = Database::create(dir.path(), config)?;
    
    println!("Phase 1: Write some data");
    println!("-------------------------");
    
    // Write a small amount of data (less than 4MB memtable threshold)
    for i in 0..100 {
        let key = format!("key_{:06}", i);
        let value = format!("value_{:06}_padding_to_make_it_bigger_{}", i, "x".repeat(100));
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    println!("Wrote 100 entries");
    
    // Check SSTable files before sync
    let sst_count_before = count_sst_files(dir.path())?;
    println!("SSTable files before sync: {}", sst_count_before);
    
    println!("\nPhase 2: Call sync()");
    println!("---------------------");
    
    db.sync()?;
    println!("Sync completed");
    
    // Check SSTable files after sync
    let sst_count_after = count_sst_files(dir.path())?;
    println!("SSTable files after sync: {}", sst_count_after);
    
    if sst_count_after > sst_count_before {
        println!("✅ New SSTable created during sync");
    } else {
        println!("❌ No new SSTable created - data still in memory!");
    }
    
    println!("\nPhase 3: Close and reopen");
    println!("--------------------------");
    
    drop(db);
    
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
    
    // Try to read the data
    let mut recovered = 0;
    for i in 0..100 {
        let key = format!("key_{:06}", i);
        if db2.get(key.as_bytes())?.is_some() {
            recovered += 1;
        }
    }
    
    println!("Recovered {}/100 entries after restart", recovered);
    
    if recovered == 100 {
        println!("✅ SUCCESS: All data persisted correctly");
    } else {
        println!("❌ FAILURE: Lost {} entries", 100 - recovered);
    }
    
    Ok(())
}

fn count_sst_files(dir: &Path) -> Result<usize, Box<dyn std::error::Error>> {
    let mut count = 0;
    let lsm_dir = dir.join("lsm");
    
    if lsm_dir.exists() {
        for entry in std::fs::read_dir(&lsm_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "sst" {
                    count += 1;
                    println!("  Found SSTable: {}", path.display());
                }
            }
        }
    }
    Ok(count)
}
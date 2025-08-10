use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB LSM State Debug");
    println!("=============================\n");
    
    // Test with default config (compression_enabled = true)
    println!("Test 1: Default config (should have LSM)");
    println!("-----------------------------------------");
    test_config(LightningDbConfig::default())?;
    
    println!("\nTest 2: Compression disabled (no LSM)");
    println!("--------------------------------------");
    let mut config = LightningDbConfig::default();
    config.compression_enabled = false;
    test_config(config)?;
    
    Ok(())
}

fn test_config(config: LightningDbConfig) -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db = Database::create(dir.path(), config)?;
    
    println!("Writing data...");
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    println!("Calling sync()...");
    db.sync()?;
    
    // Check for LSM files
    let mut has_sst = false;
    let mut has_wal = false;
    let mut file_count = 0;
    
    for entry in std::fs::read_dir(dir.path())? {
        let entry = entry?;
        let path = entry.path();
        file_count += 1;
        
        if let Some(name) = path.file_name() {
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".sst") {
                has_sst = true;
                println!("  Found SSTable: {}", name_str);
            }
            if name_str.contains("wal") {
                has_wal = true;
                println!("  Found WAL: {}", name_str);
            }
        }
    }
    
    println!("Results:");
    println!("  Total files: {}", file_count);
    println!("  Has SSTable: {}", has_sst);
    println!("  Has WAL: {}", has_wal);
    
    // Test recovery
    drop(db);
    let db2 = Database::open(dir.path(), LightningDbConfig::default())?;
    
    let mut recovered = 0;
    for i in 0..10 {
        let key = format!("key_{}", i);
        if db2.get(key.as_bytes())?.is_some() {
            recovered += 1;
        }
    }
    
    println!("  Recovered: {}/10 entries", recovered);
    
    if recovered == 10 {
        println!("  ✅ Data persisted");
    } else {
        println!("  ❌ Data lost!");
    }
    
    Ok(())
}
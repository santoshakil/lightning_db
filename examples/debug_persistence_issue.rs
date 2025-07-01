use lightning_db::{Database, LightningDbConfig};
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Debugging Lightning DB Persistence Issue ===\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("debug_test.db");
    
    // Check if we're using the file-based backend
    println!("Creating database at: {:?}", db_path);
    
    {
        let config = LightningDbConfig {
            page_size: 4096,
            cache_size: 1024 * 1024, // 1MB
            mmap_size: Some(10 * 1024 * 1024), // 10MB
            use_improved_wal: true,
            ..Default::default()
        };
        
        let db = Database::create(&db_path, config)?;
        
        // Write a single key-value pair
        println!("\nWriting single test entry...");
        db.put(b"test_key", b"test_value")?;
        
        // Check file size immediately
        if db_path.exists() {
            let size = fs::metadata(&db_path)?.len();
            println!("File size after single write: {} bytes", size);
        } else {
            println!("WARNING: Database file doesn't exist!");
        }
        
        // Force sync
        println!("\nForcing checkpoint...");
        db.checkpoint()?;
        
        // Check file size after checkpoint
        if db_path.exists() {
            let size = fs::metadata(&db_path)?.len();
            println!("File size after checkpoint: {} bytes", size);
        }
        
        // Write more data
        println!("\nWriting 100 entries...");
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}_with_padding_{}", i, "x".repeat(100));
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Another checkpoint
        println!("\nAnother checkpoint...");
        db.checkpoint()?;
        
        if db_path.exists() {
            let size = fs::metadata(&db_path)?.len();
            println!("File size after 100 entries: {} bytes", size);
        }
        
        // Explicitly try to sync the page manager
        println!("\nCalling shutdown to ensure all data is flushed...");
        db.shutdown()?;
        
        println!("\nDatabase closed.");
    }
    
    // Check final file size
    if db_path.exists() {
        let final_size = fs::metadata(&db_path)?.len();
        println!("\nFinal file size: {} bytes", final_size);
        
        // Try to read raw file contents
        let contents = fs::read(&db_path)?;
        println!("First 100 bytes of file: {:?}", &contents[..contents.len().min(100)]);
    } else {
        println!("\nERROR: Database file doesn't exist after closing!");
    }
    
    // Now try to reopen and read
    println!("\n\nReopening database...");
    {
        let db = Database::open(&db_path, LightningDbConfig::default())?;
        
        match db.get(b"test_key")? {
            Some(value) => {
                println!("✓ Found test_key: {:?}", String::from_utf8_lossy(&value));
            }
            None => {
                println!("✗ test_key NOT FOUND!");
            }
        }
        
        // Count how many keys we can find
        let mut found = 0;
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }
        println!("Found {}/100 keys", found);
    }
    
    // Check for WAL files
    println!("\n\nChecking for WAL files in directory...");
    for entry in fs::read_dir(dir.path())? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        println!("  {:?} - {} bytes", path.file_name().unwrap(), metadata.len());
    }
    
    Ok(())
}
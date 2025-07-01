use lightning_db::{Database, LightningDbConfig};
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Tracing Lightning DB Persistence Issue ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();
    
    println!("Database directory: {:?}", db_path);
    
    // List files before creating database
    println!("\nFiles before creation:");
    list_directory_contents(db_path)?;
    
    {
        let config = LightningDbConfig {
            page_size: 4096,
            cache_size: 0, // Disable cache to ensure direct writes
            use_improved_wal: true,
            write_batch_size: 1, // Force immediate writes
            ..Default::default()
        };
        
        println!("\nCreating database...");
        let db = Database::create(db_path, config)?;
        
        println!("\nFiles after creation:");
        list_directory_contents(db_path)?;
        
        // Write a single key
        println!("\nWriting single key...");
        db.put(b"test_key", b"test_value")?;
        
        println!("\nFiles after first write:");
        list_directory_contents(db_path)?;
        
        // Force sync
        println!("\nCalling sync...");
        db.sync()?;
        
        println!("\nFiles after sync:");
        list_directory_contents(db_path)?;
        
        // Call checkpoint
        println!("\nCalling checkpoint...");
        db.checkpoint()?;
        
        println!("\nFiles after checkpoint:");
        list_directory_contents(db_path)?;
        
        // Write more data
        println!("\nWriting 10 more entries...");
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}_with_some_data", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        println!("\nFiles after more writes:");
        list_directory_contents(db_path)?;
        
        // Shutdown
        println!("\nShutting down database...");
        db.shutdown()?;
        
        println!("\nFiles after shutdown:");
        list_directory_contents(db_path)?;
    }
    
    println!("\nFiles after database dropped:");
    list_directory_contents(db_path)?;
    
    // Try to reopen and verify
    println!("\n\nReopening database...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        match db.get(b"test_key")? {
            Some(value) => {
                println!("✓ Found test_key: {:?}", String::from_utf8_lossy(&value));
            }
            None => {
                println!("✗ test_key NOT FOUND!");
            }
        }
        
        // Check other keys
        let mut found = 0;
        for i in 0..10 {
            let key = format!("key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }
        println!("Found {}/10 additional keys", found);
    }
    
    Ok(())
}

fn list_directory_contents(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let entries = fs::read_dir(path)?;
    let mut count = 0;
    
    for entry in entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let file_name = entry.file_name();
        let size = if metadata.is_file() {
            format!("{} bytes", metadata.len())
        } else {
            "directory".to_string()
        };
        
        println!("  {:?} - {}", file_name, size);
        count += 1;
    }
    
    if count == 0 {
        println!("  (empty directory)");
    }
    
    Ok(())
}
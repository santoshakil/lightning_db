use lightning_db::{Database, LightningDbConfig, WalSyncMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing B+Tree Range Scan Implementation");
    
    let db_path = "/tmp/test_range_scan.db";
    std::fs::remove_dir_all(db_path).ok(); // Clean up
    
    // Create database with async WAL to avoid sync issues
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;
    let db = Database::create(&db_path, config)?;
    
    println!("Database created, inserting test data...");
    
    // Insert test data
    let test_data = vec![
        ("key01", "value01"),
        ("key03", "value03"),
        ("key05", "value05"),
        ("key07", "value07"),
        ("key09", "value09"),
        ("key11", "value11"),
        ("key13", "value13"),
        ("key15", "value15"),
    ];
    
    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("Inserted: {} -> {}", key, value);
    }
    
    println!("\nTesting range scans:");
    
    // Test 1: Full range scan
    println!("\n1. Full range scan:");
    let results = db.range(None, None)?;
    println!("Found {} entries:", results.len());
    for (key, value) in &results {
        println!("  {} -> {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }
    
    // Test 2: Range with start key
    println!("\n2. Range from 'key05':");
    let results = db.range(Some(b"key05"), None)?;
    println!("Found {} entries:", results.len());
    for (key, value) in &results {
        println!("  {} -> {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }
    
    // Test 3: Range with start and end key
    println!("\n3. Range from 'key05' to 'key11':");
    let results = db.range(Some(b"key05"), Some(b"key11"))?;
    println!("Found {} entries:", results.len());
    for (key, value) in &results {
        println!("  {} -> {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }
    
    // Test 4: Direct B+Tree range scan
    println!("\n4. Direct B+Tree range scan 'key07' to 'key13':");
    let results = db.btree_range(Some(b"key07"), Some(b"key13"))?;
    println!("Found {} entries:", results.len());
    for (key, value) in &results {
        println!("  {} -> {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
    }
    
    println!("\n✅ Range scan tests completed successfully!");
    
    Ok(())
}
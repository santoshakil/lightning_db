/// Simple real-world test to verify basic functionality
use lightning_db::{Database, LightningDbConfig};
use tempfile::TempDir;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌍 SIMPLE REAL-WORLD VALIDATION");
    println!("======================================\n");
    
    // Test 1: Basic Cache Pattern
    println!("1️⃣ Basic Cache Pattern Test");
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB cache
    
    let db = Database::create(temp_dir.path(), config)?;
    let start = Instant::now();
    
    // Simple write test
    println!("   Writing 1000 keys...");
    for i in 0..1000 {
        let key = format!("key:{}", i);
        let value = format!("value:{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Simple read test
    println!("   Reading 1000 keys...");
    let mut hits = 0;
    for i in 0..1000 {
        let key = format!("key:{}", i);
        if db.get(key.as_bytes())?.is_some() {
            hits += 1;
        }
    }
    
    let duration = start.elapsed();
    println!("   Duration: {:.2}ms", duration.as_millis());
    println!("   Hit rate: {}%", hits * 100 / 1000);
    println!("   Throughput: {:.0} ops/sec", 2000.0 / duration.as_secs_f64());
    
    // Test 2: Batch Operations
    println!("\n2️⃣ Batch Operations Test");
    let start = Instant::now();
    
    // Batch write
    let mut batch = Vec::new();
    for i in 0..100 {
        let key = format!("batch:key:{}", i);
        let value = format!("batch:value:{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }
    
    for (key, value) in &batch {
        db.put(key, value)?;
    }
    
    // Batch read
    for (key, _) in &batch {
        let _ = db.get(key)?;
    }
    
    let duration = start.elapsed();
    println!("   Duration: {:.2}ms", duration.as_millis());
    println!("   Throughput: {:.0} ops/sec", 200.0 / duration.as_secs_f64());
    
    // Test 3: Scan Test
    println!("\n3️⃣ Scan Test");
    let start = Instant::now();
    
    let count = db.scan(
        Some(b"key:0".to_vec()),
        Some(b"key:9".to_vec())
    )?.count();
    
    let duration = start.elapsed();
    println!("   Found {} keys in {:.2}ms", count, duration.as_millis());
    
    println!("\n✅ ALL TESTS COMPLETED!");
    
    Ok(())
}
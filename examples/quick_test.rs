use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("quick_test.db");
    
    println!("Lightning DB Quick Performance Test");
    println!("===================================\n");
    
    let config = LightningDbConfig {
        page_size: 4096,
        cache_size: 10 * 1024 * 1024,
        compression_enabled: false,
        enable_statistics: true,
        ..Default::default()
    };
    
    let db = Database::open(&db_path, config)?;
    
    println!("Writing 1000 records...");
    let start = Instant::now();
    
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    let write_time = start.elapsed();
    println!("✅ Write time: {:.2?} ({:.0} ops/sec)\n", 
        write_time, 
        1000.0 / write_time.as_secs_f64());
    
    println!("Reading 1000 records...");
    let start = Instant::now();
    
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes())?;
        assert!(value.is_some());
    }
    
    let read_time = start.elapsed();
    println!("✅ Read time: {:.2?} ({:.0} ops/sec)\n", 
        read_time, 
        1000.0 / read_time.as_secs_f64());
    
    println!("Range query test...");
    let start = Instant::now();
    let mut count = 0;
    
    for _ in db.range(None, None)? {
        count += 1;
    }
    
    let range_time = start.elapsed();
    println!("✅ Found {} records in {:.2?}\n", count, range_time);
    
    let stats = db.stats();
    println!("Database Stats:");
    println!("  Pages: {}", stats.page_count);
    println!("  Tree height: {}", stats.tree_height);
    
    println!("\n✅ Test completed successfully!");
    
    Ok(())
}
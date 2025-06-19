use lightning_db::{Database, LightningDbConfig};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Lightning DB - Basic Usage Example\n");

    // Create a temporary directory for the database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    // Create database with default configuration
    println!("1. Creating database at {:?}", db_path);
    let db = Database::create(db_path, LightningDbConfig::default())?;

    // Basic CRUD operations
    println!("\n2. Basic CRUD Operations:");

    // Put
    println!("   - Inserting key-value pairs...");
    db.put(b"name", b"Lightning DB")?;
    db.put(b"version", b"0.1.0")?;
    db.put(b"language", b"Rust")?;

    // Get
    println!("   - Reading values:");
    if let Some(name) = db.get(b"name")? {
        println!("     name: {}", String::from_utf8_lossy(&name));
    }
    if let Some(version) = db.get(b"version")? {
        println!("     version: {}", String::from_utf8_lossy(&version));
    }

    // Update
    println!("   - Updating version...");
    db.put(b"version", b"0.2.0")?;
    if let Some(version) = db.get(b"version")? {
        println!("     new version: {}", String::from_utf8_lossy(&version));
    }

    // Delete
    println!("   - Deleting language key...");
    db.delete(b"language")?;
    match db.get(b"language")? {
        None => println!("     language key successfully deleted"),
        Some(_) => println!("     ERROR: language key still exists!"),
    }

    // Batch operations
    println!("\n3. Batch Operations:");
    let users = vec![
        (b"user:1".to_vec(), b"Alice".to_vec()),
        (b"user:2".to_vec(), b"Bob".to_vec()),
        (b"user:3".to_vec(), b"Charlie".to_vec()),
        (b"user:4".to_vec(), b"David".to_vec()),
        (b"user:5".to_vec(), b"Eve".to_vec()),
    ];
    
    println!("   - Batch inserting {} users...", users.len());
    db.put_batch(&users)?;

    // Transaction example
    println!("\n4. Transaction Example:");
    println!("   - Starting transaction...");
    let tx_id = db.begin_transaction()?;
    
    println!("   - Adding items in transaction...");
    db.put_tx(tx_id, b"tx:1", b"First transaction item")?;
    db.put_tx(tx_id, b"tx:2", b"Second transaction item")?;
    
    // Items not visible outside transaction yet
    assert!(db.get(b"tx:1")?.is_none());
    
    println!("   - Committing transaction...");
    db.commit_transaction(tx_id)?;
    
    // Now items are visible
    if let Some(val) = db.get(b"tx:1")? {
        println!("     Transaction item visible: {}", String::from_utf8_lossy(&val));
    }

    // Range query example
    println!("\n5. Range Query Example:");
    println!("   - Scanning all user entries:");
    let iter = db.scan_prefix(b"user:")?;
    for (i, result) in iter.enumerate() {
        let (key, value) = result?;
        println!("     {}: {} = {}", 
            i + 1,
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Performance test
    println!("\n6. Quick Performance Test:");
    let count = 10_000;
    
    // Write performance
    println!("   - Writing {} entries...", count);
    let start = std::time::Instant::now();
    for i in 0..count {
        db.put(format!("perf:{:06}", i).as_bytes(), b"test_value")?;
    }
    let write_duration = start.elapsed();
    let write_ops_sec = count as f64 / write_duration.as_secs_f64();
    println!("     Write: {:.0} ops/sec ({:.2} μs/op)", 
        write_ops_sec, 
        write_duration.as_micros() as f64 / count as f64
    );

    // Read performance
    println!("   - Reading {} entries...", count);
    let start = std::time::Instant::now();
    for i in 0..count {
        let _ = db.get(format!("perf:{:06}", i).as_bytes())?;
    }
    let read_duration = start.elapsed();
    let read_ops_sec = count as f64 / read_duration.as_secs_f64();
    println!("     Read: {:.0} ops/sec ({:.2} μs/op)", 
        read_ops_sec,
        read_duration.as_micros() as f64 / count as f64
    );

    // Database statistics
    println!("\n7. Database Statistics:");
    let stats = db.stats();
    println!("   - Page count: {}", stats.page_count);
    println!("   - Free pages: {}", stats.free_page_count);
    println!("   - B+Tree height: {}", stats.tree_height);
    println!("   - Active transactions: {}", stats.active_transactions);

    println!("\n✅ All operations completed successfully!");

    Ok(())
}
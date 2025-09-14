// This example demonstrates the Lightning DB API
// Run with: cargo run --example new_api_demo

use lightning_db::{Database, LightningDbConfig, Result};
use std::sync::Arc;

fn main() -> Result<()> {
    println!("🚀 Lightning DB - API Demo");

    // === 1. SIMPLE DATABASE CREATION ===
    println!("\n1. Creating database with config:");
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024,        // 50MB cache
        compression_enabled: true,
        compression_level: Some(3),
        ..Default::default()
    };

    let db = Database::create("./demo_db", config)?;
    println!("   ✅ Database created with optimized settings");

    // === 2. CORE OPERATIONS ===
    println!("\n2. Core CRUD operations:");

    // Basic operations
    db.put(b"user:1", b"John Doe")?;
    db.put(b"user:2", b"Jane Smith")?;

    if let Some(value) = db.get(b"user:1")? {
        println!("   ✅ Retrieved: {}", String::from_utf8_lossy(&value));
    }

    // Transaction operations
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"temp:key", b"temp:value")?;
    db.commit_transaction(tx_id)?;
    println!("   ✅ Transaction completed successfully");

    // === 3. BATCH OPERATIONS ===
    println!("\n3. Batch operations:");

    let batch_data = vec![
        (b"batch:1".to_vec(), b"value1".to_vec()),
        (b"batch:2".to_vec(), b"value2".to_vec()),
        (b"batch:3".to_vec(), b"value3".to_vec()),
    ];

    db.put_batch(&batch_data)?;
    println!("   ✅ Batch insert of {} records", batch_data.len());

    // Batch get
    let keys = vec![b"batch:1".to_vec(), b"batch:2".to_vec()];
    let values = db.get_batch(&keys)?;
    println!("   ✅ Batch retrieved {} values", values.iter().filter(|v| v.is_some()).count());

    // === 4. RANGE QUERIES ===
    println!("\n4. Range and scan operations:");

    // Insert some ordered data
    for i in 0..10 {
        let key = format!("item:{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Range scan
    let start = b"item:003".to_vec();
    let end = b"item:007".to_vec();
    let range_data = db.range(Some(&start), Some(&end))?;
    println!("   ✅ Range scan found {} items", range_data.len());

    // Prefix scan
    let iter = db.scan_prefix(b"item:")?;
    let count = iter.count();
    println!("   ✅ Prefix scan found {} items", count);

    // === 5. STATISTICS ===
    println!("\n5. Database statistics:");
    let stats = db.stats();
    println!("   ✅ Page count: {}", stats.page_count);
    println!("   ✅ Memory usage: {} bytes", stats.memory_usage_bytes);
    println!("   ✅ Disk usage: {} bytes", stats.disk_usage_bytes);
    println!("   ✅ Active transactions: {}", stats.active_transactions);

    // === 6. AUTO BATCHING (Advanced) ===
    println!("\n6. Auto-batching for high performance:");

    let db_arc = Arc::new(db);
    let batcher = Database::create_fast_auto_batcher(db_arc.clone());

    // The batcher automatically groups writes for better performance
    for i in 0..100 {
        let key = format!("auto:{:03}", i);
        let value = format!("value_{}", i);
        batcher.put(key.into_bytes(), value.into_bytes())?;
    }

    // Flush any pending writes
    batcher.flush()?;
    println!("   ✅ Auto-batched 100 writes efficiently");

    // === 7. CLEANUP ===
    println!("\n7. Cleanup:");

    // Delete test data
    db_arc.delete(b"user:1")?;
    db_arc.delete(b"user:2")?;

    // Clean up batch data
    let batch_keys = vec![b"batch:1".to_vec(), b"batch:2".to_vec(), b"batch:3".to_vec()];
    db_arc.delete_batch(&batch_keys)?;

    println!("   ✅ Test data cleaned up");

    // Sync to ensure all data is persisted
    db_arc.sync()?;
    println!("   ✅ Database synced to disk");

    println!("\n✨ Demo completed successfully!");

    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with cache enabled
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB cache
    config.compression_enabled = false; // Disable compression/LSM for simpler test

    println!("=== Testing Cache Metrics ===");
    let db = Database::create(db_path, config)?;

    // Insert test data
    println!("\nInserting test data...");
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Get initial metrics
    let metrics_reporter = db.get_metrics_reporter();
    println!("\n=== Initial Metrics ===");
    println!("{}", metrics_reporter.text_report());

    // Perform reads to generate cache activity
    println!("\n=== First read pass (cold cache) ===");
    for i in 0..20 {
        let key = format!("key_{:03}", i);
        let _value = db.get(key.as_bytes())?;
    }

    // Check metrics after first pass
    let snapshot1 = db.get_metrics();
    println!("\nAfter first read pass:");
    println!("Cache hits: {}", snapshot1.cache_hits);
    println!("Cache misses: {}", snapshot1.cache_misses);
    println!("Cache hit rate: {:.2}%", snapshot1.cache_hit_rate * 100.0);

    // Perform same reads again (should be cache hits)
    println!("\n=== Second read pass (warm cache) ===");
    for i in 0..20 {
        let key = format!("key_{:03}", i);
        let _value = db.get(key.as_bytes())?;
    }

    // Check metrics after second pass
    let snapshot2 = db.get_metrics();
    println!("\nAfter second read pass:");
    println!("Cache hits: {}", snapshot2.cache_hits);
    println!("Cache misses: {}", snapshot2.cache_misses);
    println!("Cache hit rate: {:.2}%", snapshot2.cache_hit_rate * 100.0);

    // Calculate delta
    let new_hits = snapshot2.cache_hits - snapshot1.cache_hits;
    let new_misses = snapshot2.cache_misses - snapshot1.cache_misses;
    println!("\nDelta from second pass:");
    println!("New cache hits: {}", new_hits);
    println!("New cache misses: {}", new_misses);

    // Get detailed report
    println!("\n=== Detailed Metrics Report ===");
    println!("{}", metrics_reporter.text_report());

    // Check if memory pool has stats
    if let Some(memory_pool) = db.get_memory_pool() {
        println!("\n=== Memory Pool Cache Stats ===");
        println!("{}", memory_pool.cache_stats());
    }

    // Test with different access patterns
    println!("\n=== Random access pattern ===");
    for j in 0..5 {
        for i in 0..10 {
            let idx = (i * 7 + j * 13) % 100; // Pseudo-random pattern
            let key = format!("key_{:03}", idx);
            let _value = db.get(key.as_bytes())?;
        }
    }

    let snapshot3 = db.get_metrics();
    println!("\nAfter random access:");
    println!("Total cache hits: {}", snapshot3.cache_hits);
    println!("Total cache misses: {}", snapshot3.cache_misses);
    println!(
        "Overall cache hit rate: {:.2}%",
        snapshot3.cache_hit_rate * 100.0
    );

    Ok(())
}

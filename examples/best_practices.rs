use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Best Practices Guide\n");

    // Best Practice 1: Use optimized configuration
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async; // Much faster than Sync
    config.compression_enabled = false; // Disable for pure B+Tree performance
    config.cache_size = 200 * 1024 * 1024; // 200MB cache for better performance

    let db = Arc::new(Database::create("./test_db", config)?);

    println!("✅ Best Practice 1: Optimized Configuration");
    println!("   - Use Async WAL mode for performance");
    println!("   - Disable compression for B+Tree workloads");
    println!("   - Increase cache size for your workload\n");

    // Best Practice 2: Use AutoBatcher for writes
    let batcher = Database::create_auto_batcher(db.clone());

    println!("✅ Best Practice 2: Use AutoBatcher for Writes");
    let count = 10_000;
    let start = Instant::now();

    for i in 0..count {
        batcher.put(
            format!("key{:06}", i).into_bytes(),
            format!("value{}", i).into_bytes(),
        )?;
    }
    batcher.flush()?;

    let duration = start.elapsed();
    let ops_sec = count as f64 / duration.as_secs_f64();
    println!("   - AutoBatcher performance: {:.0} ops/sec", ops_sec);
    println!("   - Much faster than individual puts\n");

    // Best Practice 3: Use batch operations when possible
    println!("✅ Best Practice 3: Use Batch Operations");
    let mut batch = Vec::new();
    for i in 0..1000 {
        batch.push((
            format!("batch_key{:04}", i).into_bytes(),
            format!("batch_value{}", i).into_bytes(),
        ));
    }

    let start = Instant::now();
    db.put_batch(&batch)?;
    let duration = start.elapsed();
    println!(
        "   - Batch put of 1000 items: {:.2}ms\n",
        duration.as_millis()
    );

    // Best Practice 4: Leverage caching for reads
    println!("✅ Best Practice 4: Leverage Caching");
    let test_key = b"cached_key";
    db.put(test_key, b"cached_value")?;

    // Warm the cache
    let _ = db.get(test_key)?;

    let start = Instant::now();
    for _ in 0..100_000 {
        let _ = db.get(test_key)?;
    }
    let duration = start.elapsed();
    let ops_sec = 100_000.0 / duration.as_secs_f64();
    println!("   - Cached read performance: {:.0} ops/sec\n", ops_sec);

    // Best Practice 5: Use transactions for consistency
    println!("✅ Best Practice 5: Use Transactions for Consistency");
    let tx_id = db.begin_transaction()?;

    db.put_tx(tx_id, b"tx_key1", b"tx_value1")?;
    db.put_tx(tx_id, b"tx_key2", b"tx_value2")?;

    db.commit_transaction(tx_id)?;
    println!("   - Transaction committed successfully\n");

    // Best Practice 6: Use range queries efficiently
    println!("✅ Best Practice 6: Efficient Range Queries");

    // Insert some data for range query
    for i in 0..100 {
        db.put(format!("range_{:03}", i).as_bytes(), b"value")?;
    }

    let start_key = b"range_010";
    let end_key = b"range_020";
    let mut count = 0;

    let start = Instant::now();
    // Use iterator for range queries
    let iter = db.range(Some(start_key), Some(end_key))?;
    for _ in iter {
        count += 1;
    }
    let duration = start.elapsed();

    println!(
        "   - Range query found {} items in {:.2}ms\n",
        count,
        duration.as_millis()
    );

    // Best Practice 7: Monitor performance
    println!("✅ Best Practice 7: Monitor Performance");
    let stats = db.stats();
    println!("   - Page count: {}", stats.page_count);
    println!("   - Tree height: {}", stats.tree_height);
    println!("   - Active transactions: {}\n", stats.active_transactions);

    // Performance Summary
    println!("📊 Performance Guidelines:");
    println!("   • Individual puts: ~800 ops/sec (avoid!)");
    println!("   • AutoBatcher: 10K-100K+ ops/sec (recommended)");
    println!("   • Batch operations: High throughput");
    println!("   • Cached reads: 1M-10M+ ops/sec");
    println!("\n💡 Key Takeaways:");
    println!("   1. Always use AutoBatcher or batch operations for writes");
    println!("   2. Configure WAL mode based on durability needs");
    println!("   3. Size cache appropriately for your working set");
    println!("   4. Use transactions for atomic operations");
    println!("   5. Monitor cache hit rate and adjust as needed");

    // Cleanup
    batcher.shutdown();
    drop(db);
    std::fs::remove_dir_all("./test_db").ok();

    Ok(())
}

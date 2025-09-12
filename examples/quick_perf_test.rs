use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() {
    println!("Lightning DB Quick Performance Test\n");
    
    let dir = tempdir().unwrap();
    let db = Database::create(
        dir.path(),
        LightningDbConfig {
            cache_size: 100 * 1024 * 1024, // 100MB cache
            compression_enabled: true,
            compression_type: 2, // LZ4
            prefetch_enabled: true,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            ..Default::default()
        },
    )
    .unwrap();

    // Test 1: Sequential writes
    println!("Test 1: Sequential writes");
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("seq_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = 10000.0 / elapsed.as_secs_f64();
    println!("  10,000 sequential writes: {:?} ({:.0} ops/sec)\n", elapsed, throughput);

    // Test 2: Random reads from cache
    println!("Test 2: Random reads (cached)");
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("seq_{:08}", i % 1000);
        db.get(key.as_bytes()).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = 10000.0 / elapsed.as_secs_f64();
    println!("  10,000 cached reads: {:?} ({:.0} ops/sec)\n", elapsed, throughput);

    // Test 3: Batch operations
    println!("Test 3: Batch operations");
    let mut batch = Vec::new();
    for i in 0..1000 {
        let key = format!("batch_{:04}", i);
        let value = format!("batch_value_{}", i);
        batch.push((key.into_bytes(), value.into_bytes()));
    }
    
    let start = Instant::now();
    db.batch_put(&batch).unwrap();
    let elapsed = start.elapsed();
    let throughput = 1000.0 / elapsed.as_secs_f64();
    println!("  1,000 batch inserts: {:?} ({:.0} ops/sec)\n", elapsed, throughput);

    // Test 4: Transactions
    println!("Test 4: Transactions");
    let start = Instant::now();
    for i in 0..100 {
        let tx_id = db.begin_transaction().unwrap();
        for j in 0..10 {
            let key = format!("tx_{}_{}", i, j);
            let value = format!("tx_value_{}_{}", i, j);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        db.commit_transaction(tx_id).unwrap();
    }
    let elapsed = start.elapsed();
    let throughput = 100.0 / elapsed.as_secs_f64();
    println!("  100 transactions (10 ops each): {:?} ({:.0} tx/sec)\n", elapsed, throughput);

    // Test 5: Range scans
    println!("Test 5: Range scans");
    let start = Instant::now();
    let iter = db.scan_prefix(b"seq_").unwrap();
    let count = iter.count();
    let elapsed = start.elapsed();
    let throughput = count as f64 / elapsed.as_secs_f64();
    println!("  Range scan {} entries: {:?} ({:.0} entries/sec)\n", count, elapsed, throughput);

    // Test 6: Mixed workload
    println!("Test 6: Mixed workload (70% read, 20% write, 10% delete)");
    let start = Instant::now();
    for i in 0..1000 {
        let operation = i % 10;
        let key = format!("mixed_{:04}", i % 500);
        
        if operation < 7 {
            // 70% reads
            db.get(key.as_bytes()).ok();
        } else if operation < 9 {
            // 20% writes
            let value = format!("mixed_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else {
            // 10% deletes
            db.delete(key.as_bytes()).ok();
        }
    }
    let elapsed = start.elapsed();
    let throughput = 1000.0 / elapsed.as_secs_f64();
    println!("  1,000 mixed operations: {:?} ({:.0} ops/sec)\n", elapsed, throughput);

    // Print cache statistics
    if let Ok(stats) = db.get_cache_stats() {
        println!("Cache Statistics:");
        println!("  Hits: {}", stats.hits);
        println!("  Misses: {}", stats.misses);
        println!("  Hit ratio: {:.2}%", stats.hits as f64 / (stats.hits + stats.misses) as f64 * 100.0);
        println!("  Evictions: {}", stats.evictions);
    }
}
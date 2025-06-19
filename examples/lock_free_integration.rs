use lightning_db::{Database, LightningDbConfig};
use lightning_db::lock_free::{
    LockFreeMetricsCollector, ShardedCache,
    OperationType, OperationTimer
};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lightning DB with Lock-Free Components");
    println!("========================================\n");
    
    let dir = tempdir()?;
    let db_path = dir.path();
    
    // Configure database with all optimizations
    let mut config = LightningDbConfig::default();
    config.use_improved_wal = true;
    config.use_optimized_transactions = true;
    config.use_optimized_page_manager = true;
    
    // Create database
    let db = Arc::new(Database::create(db_path, config)?);
    
    // Create lock-free components
    let metrics = Arc::new(LockFreeMetricsCollector::new());
    let cache = Arc::new(ShardedCache::<Vec<u8>, Vec<u8>>::new(10000, 16));
    
    println!("Running concurrent workload with lock-free components...\n");
    
    let start = Instant::now();
    let mut handles = vec![];
    let num_threads = 8;
    let ops_per_thread = 10_000;
    
    // Spawn worker threads
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let metrics_clone = Arc::clone(&metrics);
        let cache_clone = Arc::clone(&cache);
        
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("thread_{}_key_{:06}", thread_id, i).into_bytes();
                let value = format!("thread_{}_value_{:06}", thread_id, i).into_bytes();
                
                // Write operation with metrics
                {
                    let _timer = OperationTimer::new(&metrics_clone, OperationType::Write);
                    
                    // Check cache first
                    if cache_clone.get(&key).is_none() {
                        db_clone.put(&key, &value).unwrap();
                        cache_clone.insert(key.clone(), value.clone());
                    }
                }
                
                // Read operation with metrics
                if i % 10 == 0 {
                    let _timer = OperationTimer::new(&metrics_clone, OperationType::Read);
                    
                    // Try cache first
                    if let Some(cached_value) = cache_clone.get(&key) {
                        assert_eq!(cached_value, value);
                    } else {
                        // Fall back to database
                        let db_value = db_clone.get(&key).unwrap().unwrap();
                        assert_eq!(db_value, value);
                        
                        // Update cache
                        cache_clone.insert(key, db_value);
                    }
                }
                
                // Occasional deletes
                if i % 100 == 0 && i > 0 {
                    let _timer = OperationTimer::new(&metrics_clone, OperationType::Delete);
                    let old_key = format!("thread_{}_key_{:06}", thread_id, i - 100).into_bytes();
                    db_clone.delete(&old_key).unwrap();
                    cache_clone.remove(&old_key);
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    
    // Get final statistics
    let metrics_snapshot = metrics.get_snapshot();
    let cache_stats = cache.stats();
    let db_stats = db.stats();
    
    println!("Performance Results:");
    println!("===================");
    println!("Total time: {:?}", elapsed);
    println!();
    
    println!("Operations:");
    println!("  Writes: {} ({:.0} ops/sec)", 
        metrics_snapshot.writes, 
        metrics_snapshot.writes_per_sec
    );
    println!("  Reads:  {} ({:.0} ops/sec)", 
        metrics_snapshot.reads,
        metrics_snapshot.reads_per_sec
    );
    println!("  Deletes: {} ({:.0} ops/sec)",
        metrics_snapshot.deletes,
        metrics_snapshot.deletes_per_sec
    );
    println!("  Total:  {} ({:.0} ops/sec)",
        metrics_snapshot.total_ops,
        metrics_snapshot.ops_per_sec
    );
    println!();
    
    println!("Latencies:");
    println!("  Write: {:.1} μs avg", metrics_snapshot.avg_write_latency_us);
    println!("  Read:  {:.1} μs avg", metrics_snapshot.avg_read_latency_us);
    println!("  Delete: {:.1} μs avg", metrics_snapshot.avg_delete_latency_us);
    println!();
    
    println!("Cache Performance:");
    println!("  Size: {}/{}", cache_stats.size, cache_stats.capacity);
    println!("  Hit rate: {:.1}%", cache_stats.hit_rate * 100.0);
    println!("  Hits: {}", cache_stats.hits);
    println!("  Misses: {}", cache_stats.misses);
    println!();
    
    println!("Database Statistics:");
    println!("  Pages: {}", db_stats.page_count);
    println!("  Tree height: {}", db_stats.tree_height);
    println!("  Active transactions: {}", db_stats.active_transactions);
    println!();
    
    println!("✅ All operations completed successfully!");
    println!("\nKey Benefits of Lock-Free Components:");
    println!("  • Minimal contention under high concurrency");
    println!("  • Predictable latencies (no blocking)");
    println!("  • Better CPU cache utilization");
    println!("  • Scales linearly with core count");
    
    Ok(())
}
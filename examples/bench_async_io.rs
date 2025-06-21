use lightning_db::async_database::{AsyncDatabase, AsyncDatabaseConfigBuilder};
use std::time::Instant;
use tempfile::tempdir;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Async I/O Performance Benchmark\n");
    
    let dir = tempdir()?;
    let count = 10000;
    
    // Test 1: Basic async operations
    {
        println!("Test 1: Basic async operations");
        
        let config = AsyncDatabaseConfigBuilder::new()
            .worker_threads(4)
            .max_concurrent_ops(1000)
            .buffer_size(1024)
            .enable_write_coalescing(false) // Test without coalescing first
            .build();
        
        let db_path = dir.path().join("async_basic.db");
        let db = AsyncDatabase::create(&db_path, config).await?;
        
        let start = Instant::now();
        
        // Sequential async puts
        for i in 0..count {
            let key = format!("key_{:08}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).await?;
        }
        
        db.sync().await?;
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • Sequential puts: {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!("  • Status: {}", if ops_per_sec >= 10_000.0 { "✅ GOOD" } else { "❌ NEEDS IMPROVEMENT" });
    }
    
    // Test 2: Concurrent async operations
    {
        println!("\nTest 2: Concurrent async operations");
        
        let config = AsyncDatabaseConfigBuilder::new()
            .worker_threads(8)
            .max_concurrent_ops(2000)
            .buffer_size(2048)
            .enable_write_coalescing(false)
            .build();
        
        let db_path = dir.path().join("async_concurrent.db");
        let db = AsyncDatabase::create(&db_path, config).await?;
        
        let start = Instant::now();
        
        // Concurrent async puts
        let mut handles = Vec::new();
        let batch_size = count / 10; // 10 concurrent batches
        
        for batch in 0..10 {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                for i in 0..batch_size {
                    let key = format!("concurrent_{}_{:08}", batch, i);
                    let value = format!("value_{}_{}", batch, i);
                    db_clone.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }
        
        db.sync().await?;
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • Concurrent puts: {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!("  • Status: {}", if ops_per_sec >= 50_000.0 { "✅ EXCELLENT" } else if ops_per_sec >= 20_000.0 { "✅ GOOD" } else { "❌ NEEDS IMPROVEMENT" });
    }
    
    // Test 3: Write coalescing enabled
    {
        println!("\nTest 3: Write coalescing enabled");
        
        let config = AsyncDatabaseConfigBuilder::new()
            .worker_threads(8)
            .max_concurrent_ops(2000)
            .buffer_size(1000)
            .enable_write_coalescing(true)
            .write_coalescing_window_ms(5)
            .build();
        
        let db_path = dir.path().join("async_coalesced.db");
        let db = AsyncDatabase::create(&db_path, config).await?;
        
        let start = Instant::now();
        
        // High-frequency writes that should benefit from coalescing
        let mut handles = Vec::new();
        
        for batch in 0..20 {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                for i in 0..500 {
                    let key = format!("coalesced_{}_{:08}", batch, i);
                    let value = format!("value_{}_{}", batch, i);
                    db_clone.put(key.as_bytes(), value.as_bytes()).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }
        
        db.sync().await?;
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • Coalesced writes: {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!("  • Status: {}", if ops_per_sec >= 100_000.0 { "✅ EXCELLENT" } else if ops_per_sec >= 50_000.0 { "✅ GOOD" } else { "❌ NEEDS IMPROVEMENT" });
    }
    
    // Test 4: Batch processing
    {
        println!("\nTest 4: Batch processing");
        
        let config = AsyncDatabaseConfigBuilder::new()
            .worker_threads(8)
            .max_concurrent_ops(1000)
            .buffer_size(1000)
            .enable_write_coalescing(true)
            .build();
        
        let db_path = dir.path().join("async_batch.db");
        let db = AsyncDatabase::create(&db_path, config).await?;
        
        // Prepare batch data
        let writes: Vec<_> = (0..count)
            .map(|i| (
                format!("batch_key_{:08}", i).into_bytes(),
                format!("batch_value_{}", i).into_bytes(),
            ))
            .collect();
        
        let start = Instant::now();
        
        // Process in batches
        let batch_size = 1000;
        for chunk in writes.chunks(batch_size) {
            let result = db.put_batch(chunk.to_vec()).await?;
            println!("    Batch: {} ops in {:.1}ms ({:.0} ops/sec)", 
                     result.completed_count, 
                     result.total_time_ms,
                     result.throughput_ops_per_sec());
        }
        
        db.sync().await?;
        
        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        println!("  • Batch processing: {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!("  • Status: {}", if ops_per_sec >= 200_000.0 { "✅ EXCELLENT" } else if ops_per_sec >= 100_000.0 { "✅ GOOD" } else { "❌ NEEDS IMPROVEMENT" });
    }
    
    // Test 5: Transaction processing
    {
        println!("\nTest 5: Async transaction processing");
        
        let config = AsyncDatabaseConfigBuilder::new()
            .worker_threads(4)
            .max_concurrent_ops(100)
            .buffer_size(1000)
            .enable_write_coalescing(true)
            .build();
        
        let db_path = dir.path().join("async_tx.db");
        let db = AsyncDatabase::create(&db_path, config).await?;
        
        let start = Instant::now();
        
        // Process transactions concurrently
        let mut handles = Vec::new();
        let tx_count = 100;
        let writes_per_tx = 100;
        
        for tx_idx in 0..tx_count {
            let db_clone = db.clone();
            let handle = tokio::spawn(async move {
                let tx_id = db_clone.begin_transaction().await.unwrap();
                
                for i in 0..writes_per_tx {
                    let key = format!("tx_{}_{:08}", tx_idx, i);
                    let value = format!("tx_value_{}_{}", tx_idx, i);
                    db_clone.put_tx(tx_id, key.as_bytes(), value.as_bytes()).await.unwrap();
                }
                
                db_clone.commit_transaction(tx_id).await.unwrap();
            });
            handles.push(handle);
        }
        
        // Wait for all transactions
        for handle in handles {
            handle.await?;
        }
        
        db.sync().await?;
        
        let duration = start.elapsed();
        let total_ops = tx_count * writes_per_tx;
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        println!("  • Async transactions: {:.0} ops/sec", ops_per_sec);
        println!("  • Time: {:.2}s", duration.as_secs_f64());
        println!("  • Transactions: {}", tx_count);
        println!("  • Status: {}", if ops_per_sec >= 100_000.0 { "✅ EXCELLENT" } else if ops_per_sec >= 50_000.0 { "✅ GOOD" } else { "❌ NEEDS IMPROVEMENT" });
    }
    
    println!("\n🎯 ASYNC I/O PERFORMANCE SUMMARY:\n");
    println!("✅ Async I/O implementation provides:");
    println!("  • Non-blocking operations for better concurrency");
    println!("  • Write coalescing for improved throughput");
    println!("  • Batch processing for high-volume operations");
    println!("  • Concurrent transaction processing");
    println!("  • Configurable I/O parameters for optimization");
    
    println!("\n🚀 EXPECTED BENEFITS:");
    println!("  • Higher throughput through parallelism");
    println!("  • Better resource utilization");
    println!("  • Reduced I/O blocking");
    println!("  • Scalable concurrent operations");
    
    Ok(())
}
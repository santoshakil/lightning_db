use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ AutoBatcher Optimization Test\n");
    
    let dir = tempdir()?;
    let value = vec![0u8; 100];
    
    // Test different AutoBatcher configurations
    let batch_configs = vec![
        (100, 1),    // Small batches, minimal delay
        (500, 5),    // Medium batches
        (1000, 10),  // Current default
        (2000, 10),  // Larger batches
        (5000, 20),  // Very large batches
        (10000, 50), // Huge batches
    ];
    
    for (batch_size, delay_ms) in batch_configs {
        println!("Testing batch_size={}, delay={}ms", batch_size, delay_ms);
        
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        
        let db_path = dir.path().join(format!("batch_{}.db", batch_size));
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Create custom AutoBatcher
        let batcher = Arc::new(lightning_db::AutoBatcher::new(
            db.clone(),
            batch_size,
            delay_ms,
        ));
        
        // Test with 50K operations
        let count = 50_000;
        let start = Instant::now();
        
        for i in 0..count {
            batcher.put(
                format!("key{:08}", i).into_bytes(), 
                value.clone()
            )?;
        }
        
        batcher.flush()?;
        batcher.wait_for_completion()?;
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        
        // Get stats
        let (submitted, completed, batches, errors) = batcher.get_stats();
        let avg_batch_size = if batches > 0 { completed as f64 / batches as f64 } else { 0.0 };
        
        println!("  Performance: {:.0} ops/sec", ops_sec);
        println!("  Stats: {} submitted, {} completed, {} batches (avg {:.1} ops/batch), {} errors",
                 submitted, completed, batches, avg_batch_size, errors);
        println!("  Latency: {:.2} μs/op", duration.as_micros() as f64 / count as f64);
        println!();
    }
    
    // Test with different WAL configurations
    println!("\nTesting WAL configurations:");
    
    let wal_configs = vec![
        ("Async WAL", WalSyncMode::Async),
        ("Periodic WAL (10ms)", WalSyncMode::Periodic { interval_ms: 10 }),
        ("Periodic WAL (100ms)", WalSyncMode::Periodic { interval_ms: 100 }),
    ];
    
    for (name, wal_mode) in wal_configs {
        println!("Testing {}", name);
        
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = wal_mode;
        
        let db_path = dir.path().join(format!("{}.db", name.to_lowercase().replace(" ", "_")));
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_auto_batcher(db.clone());
        
        let count = 20_000;
        let start = Instant::now();
        
        for i in 0..count {
            batcher.put(
                format!("key{:08}", i).into_bytes(), 
                value.clone()
            )?;
        }
        
        batcher.flush()?;
        batcher.wait_for_completion()?;
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        
        println!("  Performance: {:.0} ops/sec", ops_sec);
        println!();
    }
    
    Ok(())
}
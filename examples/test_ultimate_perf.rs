use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Ultimate Performance Test\n");
    
    let dir = tempdir()?;
    let value = vec![0u8; 100];
    
    // Test 1: Optimized configuration for maximum write speed
    {
        println!("Test 1: Optimized for pure write speed");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        // Keep most features enabled for stability
        config.write_batch_size = 5000; // Larger batches
        config.cache_size = 50 * 1024 * 1024; // 50MB cache
        config.prefetch_enabled = false; // No prefetch overhead
        
        let db_path = dir.path().join("optimized.db");
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_auto_batcher(db.clone());
        
        let count = 100_000;
        let start = Instant::now();
        
        for i in 0..count {
            batcher.put(
                format!("key{:06}", i).into_bytes(), 
                value.clone()
            )?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
        
        // Verify
        let test_key = format!("key{:06}", count / 2);
        if db.get(test_key.as_bytes())?.is_some() {
            println!("  ✅ Data verified");
        } else {
            println!("  ❌ Data not found!");
        }
    }
    
    // Test 2: Use put_batch directly
    {
        println!("\nTest 2: Direct put_batch (no AutoBatcher)");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        
        let db_path = dir.path().join("batch.db");
        let db = Database::create(&db_path, config)?;
        
        let count = 100_000;
        let batch_size = 10_000;
        let start = Instant::now();
        
        for batch_start in (0..count).step_by(batch_size) {
            let batch: Vec<_> = (batch_start..batch_start + batch_size)
                .map(|i| (format!("key{:06}", i).into_bytes(), value.clone()))
                .collect();
            
            db.put_batch(&batch)?;
        }
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
    }
    
    // Test 3: FastAutoBatcher
    {
        println!("\nTest 3: FastAutoBatcher (direct writes)");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        
        let db_path = dir.path().join("fast.db");
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_fast_auto_batcher(db.clone());
        
        let count = 100_000;
        let start = Instant::now();
        
        for i in 0..count {
            batcher.put(
                format!("key{:06}", i).into_bytes(), 
                value.clone()
            )?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
    }
    
    // Test 4: Memory-only (no WAL)
    {
        println!("\nTest 4: Memory-only mode (no durability)");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        config.use_improved_wal = false;
        
        // TODO: Add a way to disable WAL completely for memory-only mode
        let db_path = dir.path().join("memory.db");
        let db = Arc::new(Database::create(&db_path, config)?);
        
        let count = 100_000;
        let start = Instant::now();
        
        // Use direct puts
        for i in 0..count {
            db.put(
                format!("key{:06}", i).as_bytes(),
                &value
            )?;
        }
        
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Write performance: {:.0} ops/sec", ops_sec);
    }
    
    Ok(())
}
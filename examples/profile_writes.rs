use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Write Performance Profiling\n");
    
    let dir = tempdir()?;
    let value = vec![0u8; 100];
    
    // Test different configurations
    let configs = vec![
        ("Default", LightningDbConfig::default()),
        ("No LSM", {
            let mut c = LightningDbConfig::default();
            c.compression_enabled = false;
            c
        }),
        ("No Improved WAL", {
            let mut c = LightningDbConfig::default();
            c.use_improved_wal = false;
            c
        }),
        ("No Optimized Transactions", {
            let mut c = LightningDbConfig::default();
            c.use_optimized_transactions = false;
            c
        }),
        ("Minimal", {
            let mut c = LightningDbConfig::default();
            c.compression_enabled = false;
            c.use_improved_wal = false;
            c.use_optimized_transactions = false;
            c.cache_size = 0;
            c.prefetch_enabled = false;
            c
        }),
        ("Large WAL batch", {
            let mut c = LightningDbConfig::default();
            c.write_batch_size = 5000;
            c
        }),
    ];
    
    for (name, mut config) in configs {
        println!("Testing configuration: {}", name);
        config.wal_sync_mode = WalSyncMode::Async;
        
        let db_path = dir.path().join(format!("{}.db", name.to_lowercase().replace(" ", "_")));
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_auto_batcher(db.clone());
        
        // Warmup
        for i in 0..1000 {
            batcher.put(format!("warmup{:06}", i).into_bytes(), value.clone())?;
        }
        batcher.flush()?;
        batcher.wait_for_completion()?;
        
        // Measure different batch sizes
        for &count in &[1000, 5000, 10000, 20000] {
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
            
            println!("  {} ops: {:.0} ops/sec ({:.2} μs/op)", 
                     count, ops_sec, duration.as_micros() as f64 / count as f64);
        }
        
        // Measure individual components
        println!("  Component timing (20K ops):");
        
        // Time just the put operations
        let start = Instant::now();
        for i in 0..20000 {
            batcher.put(format!("timing{:06}", i).into_bytes(), value.clone())?;
        }
        let put_time = start.elapsed();
        println!("    Put operations: {:.2} ms", put_time.as_secs_f64() * 1000.0);
        
        // Time the flush
        let start = Instant::now();
        batcher.flush()?;
        let flush_time = start.elapsed();
        println!("    Flush: {:.2} ms", flush_time.as_secs_f64() * 1000.0);
        
        // Time the wait
        let start = Instant::now();
        batcher.wait_for_completion()?;
        let wait_time = start.elapsed();
        println!("    Wait for completion: {:.2} ms", wait_time.as_secs_f64() * 1000.0);
        
        println!();
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging Optimized Transactions Performance\n");
    
    let count = 100;
    
    // Test 1: Baseline (no optimizations)
    {
        println!("Test 1: Baseline configuration");
        let temp_dir = tempfile::tempdir()?;
        
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = false;
        config.use_improved_wal = false;
        config.compression_enabled = false;
        config.cache_size = 0;
        config.prefetch_enabled = false;
        
        let db = Database::create(temp_dir.path(), config)?;
        
        let start = Instant::now();
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        let elapsed = start.elapsed();
        println!("  • {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());
    }
    
    // Test 2: With improved WAL only
    {
        println!("\nTest 2: + Improved WAL");
        let temp_dir = tempfile::tempdir()?;
        
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = false;
        config.use_improved_wal = true;
        config.compression_enabled = false;
        config.cache_size = 0;
        config.prefetch_enabled = false;
        
        let db = Database::create(temp_dir.path(), config)?;
        
        let start = Instant::now();
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        let elapsed = start.elapsed();
        println!("  • {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());
    }
    
    // Test 3: With optimized transactions
    {
        println!("\nTest 3: + Optimized Transactions");
        let temp_dir = tempfile::tempdir()?;
        
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = true;
        config.use_improved_wal = true;
        config.compression_enabled = false;
        config.cache_size = 0;
        config.prefetch_enabled = false;
        
        let db = Database::create(temp_dir.path(), config)?;
        
        let start = Instant::now();
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
            if i % 10 == 0 {
                println!("    Wrote {} keys in {:.3}s", i, start.elapsed().as_secs_f64());
            }
        }
        let elapsed = start.elapsed();
        println!("  • {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());
    }
    
    Ok(())
}
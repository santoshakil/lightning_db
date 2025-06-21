use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging Optimized Transaction Performance Issue\n");
    
    let temp_dir = tempfile::tempdir()?;
    let count = 100; // Smaller count for debugging
    
    // Test 1: Without optimized transactions
    {
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = false;
        config.use_improved_wal = true;
        config.cache_size = 0;
        config.compression_enabled = false;
        
        let db = Database::create(temp_dir.path().join("normal"), config)?;
        
        println!("Test 1: Normal transactions");
        let start = Instant::now();
        
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
            if i % 10 == 0 {
                println!("  Written {} keys in {:.2}s", i, start.elapsed().as_secs_f64());
            }
        }
        
        let elapsed = start.elapsed();
        println!("  Total: {} ops in {:.2}s = {:.0} ops/sec\n", 
                 count, elapsed.as_secs_f64(), count as f64 / elapsed.as_secs_f64());
    }
    
    // Test 2: With optimized transactions
    {
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = true;
        config.use_improved_wal = true;
        config.cache_size = 0;
        config.compression_enabled = false;
        
        let db = Database::create(temp_dir.path().join("optimized"), config)?;
        
        println!("Test 2: Optimized transactions");
        let start = Instant::now();
        
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
            if i % 10 == 0 {
                println!("  Written {} keys in {:.2}s", i, start.elapsed().as_secs_f64());
            }
        }
        
        let elapsed = start.elapsed();
        println!("  Total: {} ops in {:.2}s = {:.0} ops/sec\n", 
                 count, elapsed.as_secs_f64(), count as f64 / elapsed.as_secs_f64());
    }
    
    // Test 3: Using actual transactions
    {
        let mut config = LightningDbConfig::default();
        config.use_optimized_transactions = true;
        config.use_improved_wal = true;
        config.cache_size = 0;
        config.compression_enabled = false;
        
        let db = Database::create(temp_dir.path().join("tx_test"), config)?;
        
        println!("Test 3: Actual transaction operations");
        let start = Instant::now();
        
        for i in 0..count {
            let tx_id = db.begin_transaction()?;
            let key = format!("tx_key_{:08}", i);
            db.put_tx(tx_id, key.as_bytes(), b"value")?;
            db.commit_transaction(tx_id)?;
            if i % 10 == 0 {
                println!("  Committed {} transactions in {:.2}s", i, start.elapsed().as_secs_f64());
            }
        }
        
        let elapsed = start.elapsed();
        println!("  Total: {} txs in {:.2}s = {:.0} txs/sec", 
                 count, elapsed.as_secs_f64(), count as f64 / elapsed.as_secs_f64());
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Direct Write Performance Test\n");
    
    let dir = tempdir()?;
    
    // Test 1: Default config (with LSM, should be fast)
    {
        let db_path = dir.path().join("test_lsm.db");
        let config = LightningDbConfig::default(); // LSM enabled by default
        let db = Database::create(&db_path, config)?;
        
        println!("Test 1: With LSM (compression_enabled=true)");
        benchmark_writes(&db, 10_000)?;
    }
    
    // Test 2: B+Tree only (no LSM)
    {
        let db_path = dir.path().join("test_btree.db");
        let mut config = LightningDbConfig::default();
        config.compression_enabled = false; // This disables LSM
        let db = Database::create(&db_path, config)?;
        
        println!("\nTest 2: B+Tree only (compression_enabled=false)");
        benchmark_writes(&db, 1_000)?; // Fewer writes since it's slower
    }
    
    // Test 3: Async WAL mode
    {
        let db_path = dir.path().join("test_async.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = lightning_db::WalSyncMode::Async;
        let db = Database::create(&db_path, config)?;
        
        println!("\nTest 3: With async WAL");
        benchmark_writes(&db, 10_000)?;
    }
    
    Ok(())
}

fn benchmark_writes(db: &Database, count: usize) -> Result<(), Box<dyn std::error::Error>> {
    let value = vec![b'x'; 100];
    
    let start = Instant::now();
    for i in 0..count {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }
    
    let duration = start.elapsed();
    let ops_per_sec = count as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / count as f64;
    
    println!("  • {:.0} ops/sec", ops_per_sec);
    println!("  • {:.2} μs/op", us_per_op);
    println!("  • Status: {}", if ops_per_sec >= 100_000.0 { "✅ PASS" } else { "❌ FAIL" });
    
    // Verify data
    let test_key = format!("key_{:08}", 0);
    match db.get(test_key.as_bytes())? {
        Some(val) if val == value => println!("  • Data verified: ✅"),
        _ => println!("  • Data verified: ❌"),
    }
    
    Ok(())
}
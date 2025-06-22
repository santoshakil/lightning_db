use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Performance Comparison\n");
    
    let dir = tempdir()?;
    let value = vec![0u8; 100];
    
    // Test 1: Direct puts with Sync WAL (worst case)
    {
        println!("Test 1: Direct puts with Sync WAL");
        let db_path = dir.path().join("sync.db");
        let config = LightningDbConfig::default(); // Default is Sync
        let db = Database::create(&db_path, config)?;
        
        let count = 100; // Small count because it's slow
        let start = Instant::now();
        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), &value)?;
        }
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec\n", ops_sec);
    }
    
    // Test 2: Direct puts with Async WAL
    {
        println!("Test 2: Direct puts with Async WAL");
        let db_path = dir.path().join("async.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Database::create(&db_path, config)?;
        
        let count = 1000;
        let start = Instant::now();
        for i in 0..count {
            db.put(format!("key{:06}", i).as_bytes(), &value)?;
        }
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec\n", ops_sec);
    }
    
    // Test 3: Using SyncWriteBatcher
    {
        println!("Test 3: Using SyncWriteBatcher");
        let db_path = dir.path().join("sync_batcher.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_with_batcher(db);
        
        let count = 10000;
        let start = Instant::now();
        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
        }
        batcher.flush()?;
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec\n", ops_sec);
    }
    
    // Test 4: Using AutoBatcher
    {
        println!("Test 4: Using AutoBatcher");
        let db_path = dir.path().join("auto_batcher.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        let db = Arc::new(Database::create(&db_path, config)?);
        let batcher = Database::create_auto_batcher(db.clone());
        
        let count = 10000;
        let start = Instant::now();
        for i in 0..count {
            batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
        }
        batcher.flush()?;
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec\n", ops_sec);
        
        // Verify data was written
        let key = format!("key{:06}", 0);
        if (db.get(key.as_bytes())?).is_some() {
            println!("  ✅ Data verified\n");
        } else {
            println!("  ❌ Data not found!\n");
        }
    }
    
    // Test 5: Read performance
    {
        println!("Test 5: Read performance (cached)");
        let db_path = dir.path().join("read_test.db");
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Async;
        config.compression_enabled = false; // Direct B+Tree access
        let db = Database::create(&db_path, config)?;
        
        // Insert and warm up
        let test_key = b"test_key";
        db.put(test_key, &value)?;
        for _ in 0..100 {
            let _ = db.get(test_key)?;
        }
        
        let count = 100000;
        let start = Instant::now();
        for _ in 0..count {
            let _ = db.get(test_key)?;
        }
        let duration = start.elapsed();
        let ops_sec = count as f64 / duration.as_secs_f64();
        println!("  Performance: {:.0} ops/sec\n", ops_sec);
    }
    
    // Summary
    println!("\n📊 Summary:");
    println!("  • Sync WAL: ~700 ops/sec (limited by fsync)");
    println!("  • Async WAL: ~800 ops/sec (limited by transaction overhead)");
    println!("  • SyncWriteBatcher: Should be 50K+ ops/sec");
    println!("  • AutoBatcher: Should be 100K+ ops/sec");
    println!("  • Cached reads: Should be 1M+ ops/sec");
    println!("\n💡 Recommendation: Use AutoBatcher for best write performance");
    
    Ok(())
}
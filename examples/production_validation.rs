use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use std::sync::Arc;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Production Validation");
    println!("===================================\n");
    
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("validation_db");
    
    let config = LightningDbConfig {
        cache_size: 32 * 1024 * 1024, // 32MB cache for testing
        prefetch_enabled: true,
        use_optimized_transactions: true,
        ..Default::default()
    };
    
    println!("1. Testing basic operations...");
    let db = Database::create(&db_path, config.clone())?;
    
    // Basic write test
    db.put(b"test_key", b"test_value")?;
    
    // Basic read test
    if let Some(value) = db.get(b"test_key")? {
        assert_eq!(value, b"test_value");
        println!("   ✓ Basic read/write working");
    } else {
        panic!("Failed to retrieve value");
    }
    
    // Delete test
    db.delete(b"test_key")?;
    assert!(db.get(b"test_key")?.is_none());
    println!("   ✓ Delete operation working");
    
    println!("\n2. Testing performance (1000 operations)...");
    let n = 1000;
    
    // Write performance
    let start = Instant::now();
    for i in 0..n {
        let key = format!("k{:04}", i);
        let value = format!("v{:04}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    let write_time = start.elapsed();
    let write_ops_per_sec = n as f64 / write_time.as_secs_f64();
    println!("   Write: {:.0} ops/sec", write_ops_per_sec);
    
    // Read performance
    let start = Instant::now();
    for i in 0..n {
        let key = format!("k{:04}", i);
        db.get(key.as_bytes())?;
    }
    let read_time = start.elapsed();
    let read_ops_per_sec = n as f64 / read_time.as_secs_f64();
    println!("   Read: {:.0} ops/sec", read_ops_per_sec);
    
    println!("\n3. Testing concurrent access...");
    let db = Arc::new(db);
    let mut handles = vec![];
    
    for thread_id in 0..4 {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    println!("   ✓ Concurrent writes completed");
    
    println!("\n4. Validating data integrity...");
    for thread_id in 0..4 {
        for i in 0..100 {
            let key = format!("thread_{}_key_{}", thread_id, i);
            let expected_value = format!("thread_{}_value_{}", thread_id, i);
            if let Some(value) = db.get(key.as_bytes())? {
                assert_eq!(value, expected_value.as_bytes());
            } else {
                panic!("Missing key: {}", key);
            }
        }
    }
    println!("   ✓ All concurrent data verified");
    
    println!("\n5. Testing database statistics...");
    let stats = db.get_stats()?;
    println!("   Pages: {}", stats.page_count);
    println!("   B+Tree height: {}", stats.tree_height);
    println!("   Transactions: {}", stats.active_transactions);
    
    println!("\n✅ Production validation PASSED!");
    println!("\nPerformance Summary:");
    println!("   Write: {:.0} ops/sec", write_ops_per_sec);
    println!("   Read: {:.0} ops/sec", read_ops_per_sec);
    
    if write_ops_per_sec < 10000.0 || read_ops_per_sec < 10000.0 {
        println!("\n⚠️  Warning: Performance below expected targets");
        println!("   Expected: 350K+ writes/sec, 14M+ reads/sec");
        println!("   Note: This is a basic validation test with small dataset");
    }
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    println!("🔥 Lightning DB Edge Case Stress Test");
    println!("====================================\n");

    let db_path = "edge_case_test_db";
    let _ = std::fs::remove_dir_all(db_path);

    let config = LightningDbConfig {
        cache_size: 64 * 1024 * 1024, // 64MB cache
        use_optimized_transactions: true,
        use_improved_wal: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(db_path, config).expect("Failed to open database"));
    
    println!("Running edge case stress tests...\n");
    
    // Test 1: Extreme key sizes
    test_extreme_key_sizes(Arc::clone(&db));
    
    // Test 2: Extreme value sizes
    test_extreme_value_sizes(Arc::clone(&db));
    
    // Test 3: Key patterns that stress B+Tree
    test_key_patterns(Arc::clone(&db));
    
    // Test 4: Transaction conflicts
    test_transaction_conflicts(Arc::clone(&db));
    
    // Test 5: Rapid open/close cycles
    test_rapid_open_close(db_path);
    
    // Test 6: Memory exhaustion recovery
    test_memory_exhaustion(Arc::clone(&db));
    
    // Test 7: Concurrent transactions at scale
    test_concurrent_transactions(Arc::clone(&db));
    
    // Test 8: Deletion storms
    test_deletion_storms(Arc::clone(&db));
    
    println!("\n✅ All edge case tests completed!");
}

fn test_extreme_key_sizes(db: Arc<Database>) {
    println!("Test 1: Extreme key sizes");
    println!("-------------------------");
    
    // Very small keys
    for i in 0..1000 {
        let key = vec![i as u8];
        let value = b"small_key_value";
        db.put(&key, value).expect("Put failed");
    }
    println!("  ✓ 1000 single-byte keys");
    
    // Very large keys
    let large_key_sizes = vec![1024, 4096, 16384, 65536]; // Up to 64KB
    for size in large_key_sizes {
        let key = vec![0xAA; size];
        let value = format!("value_for_{}_byte_key", size);
        match db.put(&key, value.as_bytes()) {
            Ok(_) => println!("  ✓ {} byte key successful", size),
            Err(e) => println!("  ✗ {} byte key failed: {:?}", size, e),
        }
    }
    
    // Keys with special patterns
    let special_keys = vec![
        vec![0x00; 100], // All zeros
        vec![0xFF; 100], // All ones
        (0..255).collect::<Vec<u8>>(), // Sequential bytes
    ];
    
    for (i, key) in special_keys.iter().enumerate() {
        db.put(key, format!("special_{}", i).as_bytes()).expect("Put failed");
    }
    println!("  ✓ Special pattern keys\n");
}

fn test_extreme_value_sizes(db: Arc<Database>) {
    println!("Test 2: Extreme value sizes");
    println!("---------------------------");
    
    // Empty values
    for i in 0..100 {
        let key = format!("empty_value_{}", i);
        db.put(key.as_bytes(), b"").expect("Put failed");
    }
    println!("  ✓ 100 empty values");
    
    // Very large values
    let large_value_sizes = vec![
        1024 * 1024,      // 1MB
        10 * 1024 * 1024, // 10MB
        50 * 1024 * 1024, // 50MB
    ];
    
    for (i, size) in large_value_sizes.iter().enumerate() {
        let key = format!("large_value_{}", i);
        let value = vec![i as u8; *size];
        
        let start = Instant::now();
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                let elapsed = start.elapsed();
                println!("  ✓ {} MB value in {:?}", size / (1024 * 1024), elapsed);
            }
            Err(e) => println!("  ✗ {} MB value failed: {:?}", size / (1024 * 1024), e),
        }
    }
    
    println!();
}

fn test_key_patterns(db: Arc<Database>) {
    println!("Test 3: Key patterns that stress B+Tree");
    println!("---------------------------------------");
    
    // Sequential insertions (best case)
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("{:08}", i);
        db.put(key.as_bytes(), b"sequential").expect("Put failed");
    }
    println!("  Sequential: 10k inserts in {:?}", start.elapsed());
    
    // Reverse sequential (forces splits)
    let start = Instant::now();
    for i in (0..10000).rev() {
        let key = format!("rev_{:08}", i);
        db.put(key.as_bytes(), b"reverse").expect("Put failed");
    }
    println!("  Reverse: 10k inserts in {:?}", start.elapsed());
    
    // Random insertions (worst case)
    let start = Instant::now();
    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..10000 {
        let key = format!("rand_{:08}", rng.random::<u32>());
        db.put(key.as_bytes(), b"random").expect("Put failed");
    }
    println!("  Random: 10k inserts in {:?}", start.elapsed());
    
    // Clustered insertions (realistic pattern)
    let start = Instant::now();
    for cluster in 0..100 {
        for item in 0..100 {
            let key = format!("cluster_{:04}_{:04}", cluster, item);
            db.put(key.as_bytes(), b"clustered").expect("Put failed");
        }
    }
    println!("  Clustered: 10k inserts in {:?}\n", start.elapsed());
}

fn test_transaction_conflicts(db: Arc<Database>) {
    println!("Test 4: Transaction conflicts");
    println!("-----------------------------");
    
    let conflict_key = b"conflict_key";
    db.put(conflict_key, b"initial").expect("Put failed");
    
    let num_threads = 8;
    let mut handles = vec![];
    let start = Instant::now();
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        
        let handle = thread::spawn(move || {
            let mut success_count = 0;
            let mut conflict_count = 0;
            
            for i in 0..100 {
                let tx = db_clone.begin_transaction().expect("Begin failed");
                
                // Read the conflict key
                let _ = db_clone.get_tx(tx, conflict_key);
                
                // Simulate some work
                thread::sleep(Duration::from_micros(10));
                
                // Try to update
                let new_value = format!("thread_{}_update_{}", thread_id, i);
                db_clone.put_tx(tx, conflict_key, new_value.as_bytes()).expect("Put failed");
                
                // Try to commit
                match db_clone.commit_transaction(tx) {
                    Ok(_) => success_count += 1,
                    Err(_) => conflict_count += 1,
                }
            }
            
            (success_count, conflict_count)
        });
        
        handles.push(handle);
    }
    
    let mut total_success = 0;
    let mut total_conflicts = 0;
    
    for handle in handles {
        let (success, conflicts) = handle.join().unwrap();
        total_success += success;
        total_conflicts += conflicts;
    }
    
    println!("  Duration: {:?}", start.elapsed());
    println!("  Successful commits: {}", total_success);
    println!("  Conflicts: {}", total_conflicts);
    println!("  Conflict rate: {:.1}%\n", 
             (total_conflicts as f64 / (total_success + total_conflicts) as f64) * 100.0);
}

fn test_rapid_open_close(db_path: &str) {
    println!("Test 5: Rapid open/close cycles");
    println!("-------------------------------");
    
    let cycles = 50;
    let start = Instant::now();
    
    for i in 0..cycles {
        let config = LightningDbConfig::default();
        let db = Database::open(db_path, config).expect("Failed to open");
        
        // Do some operations
        for j in 0..10 {
            let key = format!("cycle_{}_key_{}", i, j);
            db.put(key.as_bytes(), b"test").expect("Put failed");
        }
        
        // Explicitly drop to close
        drop(db);
        
        if i % 10 == 0 {
            println!("  Completed {} cycles", i);
        }
    }
    
    println!("  {} open/close cycles in {:?}\n", cycles, start.elapsed());
}

fn test_memory_exhaustion(db: Arc<Database>) {
    println!("Test 6: Memory exhaustion recovery");
    println!("----------------------------------");
    
    // Try to exhaust memory with large values
    let mut i = 0;
    let value_size = 10 * 1024 * 1024; // 10MB values
    
    loop {
        let key = format!("exhaust_{}", i);
        let value = vec![i as u8; value_size];
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                i += 1;
                if i % 10 == 0 {
                    println!("  Stored {} large values ({} MB)", i, i * 10);
                }
            }
            Err(e) => {
                println!("  Memory exhaustion at {} values: {:?}", i, e);
                break;
            }
        }
        
        if i > 100 {
            println!("  Stopped at {} values to prevent system issues", i);
            break;
        }
    }
    
    // Try to recover by deleting some entries
    for j in 0..i/2 {
        let key = format!("exhaust_{}", j);
        db.delete(key.as_bytes()).expect("Delete failed");
    }
    
    println!("  Deleted {} entries to free memory", i/2);
    
    // Verify we can continue operating
    db.put(b"recovery_test", b"success").expect("Recovery put failed");
    println!("  ✓ Database recovered and operational\n");
}

fn test_concurrent_transactions(db: Arc<Database>) {
    println!("Test 7: Concurrent transactions at scale");
    println!("----------------------------------------");
    
    let num_threads = 16;
    let transactions_per_thread = 100;
    let mut handles = vec![];
    let start = Instant::now();
    
    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        
        let handle = thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            let mut completed = 0;
            
            for tx_num in 0..transactions_per_thread {
                let tx = db_clone.begin_transaction().expect("Begin failed");
                
                // Each transaction touches multiple keys
                for _ in 0..10 {
                    let key = format!("tx_key_{}", rng.random_range(0..1000));
                    let value = format!("thread_{}_tx_{}", thread_id, tx_num);
                    
                    // Mix of reads and writes
                    if rng.random_bool(0.5) {
                        let _ = db_clone.get_tx(tx, key.as_bytes());
                    } else {
                        db_clone.put_tx(tx, key.as_bytes(), value.as_bytes()).expect("Put failed");
                    }
                }
                
                if db_clone.commit_transaction(tx).is_ok() {
                    completed += 1;
                }
            }
            
            completed
        });
        
        handles.push(handle);
    }
    
    let mut total_completed = 0;
    for handle in handles {
        total_completed += handle.join().unwrap();
    }
    
    let elapsed = start.elapsed();
    println!("  Total transactions: {}/{}", 
             total_completed, num_threads * transactions_per_thread);
    println!("  Duration: {:?}", elapsed);
    println!("  Throughput: {:.2} tx/sec\n", total_completed as f64 / elapsed.as_secs_f64());
}

fn test_deletion_storms(db: Arc<Database>) {
    println!("Test 8: Deletion storms");
    println!("-----------------------");
    
    // First, populate with data
    let num_entries = 50000;
    println!("  Populating with {} entries...", num_entries);
    
    for i in 0..num_entries {
        let key = format!("delete_me_{:06}", i);
        db.put(key.as_bytes(), b"to_be_deleted").expect("Put failed");
    }
    
    // Now delete in various patterns
    
    // Pattern 1: Sequential deletion
    let start = Instant::now();
    for i in 0..10000 {
        let key = format!("delete_me_{:06}", i);
        db.delete(key.as_bytes()).expect("Delete failed");
    }
    println!("  Sequential deletion: 10k in {:?}", start.elapsed());
    
    // Pattern 2: Random deletion
    let start = Instant::now();
    let mut rng = StdRng::seed_from_u64(123);
    for _ in 0..10000 {
        let i = rng.random_range(10000..30000);
        let key = format!("delete_me_{:06}", i);
        db.delete(key.as_bytes()).expect("Delete failed");
    }
    println!("  Random deletion: 10k in {:?}", start.elapsed());
    
    // Pattern 3: Batch deletion with reads
    let start = Instant::now();
    for i in 30000..40000 {
        let key = format!("delete_me_{:06}", i);
        
        // Read before delete
        let _ = db.get(key.as_bytes());
        db.delete(key.as_bytes()).expect("Delete failed");
        
        // Try to read after delete
        assert!(db.get(key.as_bytes()).expect("Get failed").is_none());
    }
    println!("  Delete with verification: 10k in {:?}", start.elapsed());
    
    // Verify remaining entries
    let mut remaining = 0;
    for i in 40000..50000 {
        let key = format!("delete_me_{:06}", i);
        if db.get(key.as_bytes()).expect("Get failed").is_some() {
            remaining += 1;
        }
    }
    
    println!("  Remaining entries: {}/10000", remaining);
}
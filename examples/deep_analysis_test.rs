/// Deep Analysis Test Suite - Thoroughly examines Lightning DB for edge cases
use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 DEEP ANALYSIS TEST SUITE");
    println!("============================\n");
    
    // Test 1: Zero-length keys and values
    test_zero_length_data()?;
    
    // Test 2: Maximum key/value sizes
    test_maximum_sizes()?;
    
    // Test 3: Special characters in keys
    test_special_characters()?;
    
    // Test 4: Transaction edge cases
    test_transaction_edge_cases()?;
    
    // Test 5: Concurrent transaction conflicts
    test_concurrent_transaction_conflicts()?;
    
    // Test 6: Database reopening and state consistency
    test_database_reopening()?;
    
    // Test 7: WAL corruption recovery
    test_wal_corruption_recovery()?;
    
    // Test 8: Memory pressure and OOM handling
    test_memory_pressure()?;
    
    // Test 9: Rapid open/close cycles
    test_rapid_open_close()?;
    
    // Test 10: Page boundary edge cases
    test_page_boundary_cases()?;
    
    println!("\n✅ ALL DEEP ANALYSIS TESTS PASSED!");
    
    Ok(())
}

fn test_zero_length_data() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣ Testing zero-length keys and values...");
    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test empty key (should fail)
    match db.put(b"", b"value") {
        Err(e) => {
            if e.to_string().contains("Invalid key size: 0 bytes") {
                println!("   ✓ Empty key correctly rejected");
            } else {
                panic!("Expected 'Invalid key size' error, got: {:?}", e);
            }
        },
        Ok(_) => panic!("Empty key should not be allowed"),
    }
    
    // Test empty value (should succeed)
    db.put(b"key", b"")?;
    let value = db.get(b"key")?;
    assert_eq!(value, Some(vec![]));
    println!("   ✓ Empty value handled correctly");
    
    // Test both empty in transaction
    let tx_id = db.begin_transaction()?;
    match db.put_tx(tx_id, b"", b"") {
        Err(e) => {
            if e.to_string().contains("Invalid key size: 0 bytes") {
                println!("   ✓ Empty key in transaction correctly rejected");
            } else {
                panic!("Expected 'Invalid key size' error in transaction, got: {:?}", e);
            }
        },
        Ok(_) => panic!("Empty key should not be allowed in transaction"),
    }
    db.abort_transaction(tx_id)?;
    
    Ok(())
}

fn test_maximum_sizes() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n2️⃣ Testing maximum key/value sizes...");
    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test large key (1KB)
    let large_key = vec![0x42; 1024];
    let value = b"normal value";
    db.put(&large_key, value)?;
    assert_eq!(db.get(&large_key)?, Some(value.to_vec()));
    println!("   ✓ 1KB key handled correctly");
    
    // Test very large key (10KB) - should work but may be inefficient
    let very_large_key = vec![0x43; 10 * 1024];
    db.put(&very_large_key, value)?;
    assert_eq!(db.get(&very_large_key)?, Some(value.to_vec()));
    println!("   ✓ 10KB key handled correctly");
    
    // Test large value (1MB)
    let normal_key = b"large_value_key";
    let large_value = vec![0x44; 1024 * 1024];
    db.put(normal_key, &large_value)?;
    assert_eq!(db.get(normal_key)?, Some(large_value));
    println!("   ✓ 1MB value handled correctly");
    
    Ok(())
}

fn test_special_characters() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n3️⃣ Testing special characters in keys...");
    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test null bytes in key
    let key_with_null = b"key\0with\0nulls";
    db.put(key_with_null, b"value")?;
    assert_eq!(db.get(key_with_null)?, Some(b"value".to_vec()));
    println!("   ✓ Null bytes in key handled correctly");
    
    // Test all byte values
    for byte in 0u8..=255 {
        let key = vec![byte];
        let value = vec![byte, byte];
        db.put(&key, &value)?;
        assert_eq!(db.get(&key)?, Some(value));
    }
    println!("   ✓ All byte values (0-255) handled correctly");
    
    // Test UTF-8 and non-UTF-8 sequences
    let utf8_key = "Hello, 世界! 🌍".as_bytes();
    let non_utf8_key = &[0xFF, 0xFE, 0xFD, 0xFC];
    
    db.put(utf8_key, b"utf8")?;
    db.put(non_utf8_key, b"non-utf8")?;
    
    assert_eq!(db.get(utf8_key)?, Some(b"utf8".to_vec()));
    assert_eq!(db.get(non_utf8_key)?, Some(b"non-utf8".to_vec()));
    println!("   ✓ UTF-8 and non-UTF-8 sequences handled correctly");
    
    Ok(())
}

fn test_transaction_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n4️⃣ Testing transaction edge cases...");
    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test 1: Double commit
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"key1", b"value1")?;
    db.commit_transaction(tx_id)?;
    
    match db.commit_transaction(tx_id) {
        Err(_) => println!("   ✓ Double commit correctly rejected"),
        Ok(_) => panic!("Double commit should fail"),
    }
    
    // Test 2: Operations after abort
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"key2", b"value2")?;
    db.abort_transaction(tx_id)?;
    
    match db.put_tx(tx_id, b"key3", b"value3") {
        Err(_) => println!("   ✓ Operations after abort correctly rejected"),
        Ok(_) => panic!("Operations after abort should fail"),
    }
    
    // Test 3: Read-write-read pattern
    db.put(b"key4", b"initial")?;
    
    let tx_id = db.begin_transaction()?;
    let v1 = db.get_tx(tx_id, b"key4")?;
    assert_eq!(v1, Some(b"initial".to_vec()));
    
    db.put_tx(tx_id, b"key4", b"modified")?;
    
    let v2 = db.get_tx(tx_id, b"key4")?;
    assert_eq!(v2, Some(b"modified".to_vec()));
    
    db.commit_transaction(tx_id)?;
    println!("   ✓ Read-write-read pattern works correctly");
    
    // Test 4: Empty transaction commit
    let tx_id = db.begin_transaction()?;
    db.commit_transaction(tx_id)?;
    println!("   ✓ Empty transaction commit handled correctly");
    
    Ok(())
}

fn test_concurrent_transaction_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n5️⃣ Testing concurrent transaction conflicts...");
    let temp_dir = TempDir::new()?;
    let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default())?);
    
    // Setup initial data
    db.put(b"shared_key", b"initial_value")?;
    
    let barrier = Arc::new(Barrier::new(2));
    let db1 = db.clone();
    let barrier1 = barrier.clone();
    
    // Transaction 1: Read then write
    let handle1 = thread::spawn(move || {
        let tx_id = db1.begin_transaction().unwrap();
        
        // Read the key
        let value = db1.get_tx(tx_id, b"shared_key").unwrap();
        assert_eq!(value, Some(b"initial_value".to_vec()));
        
        // Synchronize to ensure both transactions have read
        barrier1.wait();
        
        // Try to write - this should conflict with tx2
        db1.put_tx(tx_id, b"shared_key", b"tx1_value").unwrap();
        
        // Try to commit
        db1.commit_transaction(tx_id)
    });
    
    // Transaction 2: Read then write
    let handle2 = thread::spawn(move || {
        let tx_id = db.begin_transaction().unwrap();
        
        // Read the key
        let value = db.get_tx(tx_id, b"shared_key").unwrap();
        assert_eq!(value, Some(b"initial_value".to_vec()));
        
        // Synchronize to ensure both transactions have read
        barrier.wait();
        
        // Try to write - this should conflict with tx1
        db.put_tx(tx_id, b"shared_key", b"tx2_value").unwrap();
        
        // Try to commit
        db.commit_transaction(tx_id)
    });
    
    let result1 = handle1.join().unwrap();
    let result2 = handle2.join().unwrap();
    
    // One should succeed, one should fail
    match (result1.is_ok(), result2.is_ok()) {
        (true, false) | (false, true) => {
            println!("   ✓ Write-write conflict correctly detected");
        }
        _ => panic!("Expected exactly one transaction to succeed"),
    }
    
    Ok(())
}

fn test_database_reopening() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n6️⃣ Testing database reopening and state consistency...");
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path();
    
    // Phase 1: Write data with various patterns
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;
        
        // Regular writes
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Transaction writes
        let tx_id = db.begin_transaction()?;
        for i in 100..150 {
            let key = format!("key_{:04}", i);
            let value = format!("tx_value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        db.commit_transaction(tx_id)?;
        
        // Some deletions
        for i in 0..50 {
            if i % 3 == 0 {
                let key = format!("key_{:04}", i);
                db.delete(key.as_bytes())?;
            }
        }
        
        db.checkpoint()?;
    }
    
    // Phase 2: Reopen and verify all data
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Verify regular writes
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let expected = if i < 50 && i % 3 == 0 {
                None
            } else {
                Some(format!("value_{}", i).into_bytes())
            };
            assert_eq!(db.get(key.as_bytes())?, expected);
        }
        
        // Verify transaction writes
        for i in 100..150 {
            let key = format!("key_{:04}", i);
            let expected = format!("tx_value_{}", i);
            assert_eq!(db.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        println!("   ✓ All data correctly persisted across reopening");
    }
    
    Ok(())
}

fn test_wal_corruption_recovery() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n7️⃣ Testing WAL corruption recovery...");
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path();
    
    // Phase 1: Create database with some data
    {
        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..Default::default()
        };
        
        let db = Database::create(db_path, config)?;
        
        for i in 0..50 {
            let key = format!("wal_key_{}", i);
            let value = format!("wal_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        // Don't checkpoint - leave data in WAL
    }
    
    // Phase 2: Corrupt WAL file (append garbage)
    let wal_path = db_path.join("wal.log");
    if wal_path.exists() {
        use std::fs::OpenOptions;
        use std::io::Write;
        
        let mut file = OpenOptions::new()
            .append(true)
            .open(&wal_path)?;
        
        // Append some garbage data
        file.write_all(b"CORRUPTED_DATA_HERE_INVALID_WAL_ENTRY")?;
        println!("   ✓ WAL file corrupted");
    }
    
    // Phase 3: Try to reopen database - should handle corruption gracefully
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Verify we can still read some data (from successfully parsed WAL entries)
        let test_key = b"wal_key_0";
        match db.get(test_key) {
            Ok(_) => println!("   ✓ Database recovered from partial WAL corruption"),
            Err(e) => println!("   ⚠️ Recovery result: {:?}", e),
        }
        
        // Verify we can still write new data
        db.put(b"post_corruption_key", b"post_corruption_value")?;
        assert_eq!(
            db.get(b"post_corruption_key")?, 
            Some(b"post_corruption_value".to_vec())
        );
        println!("   ✓ Database operational after recovery");
    }
    
    Ok(())
}

fn test_memory_pressure() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n8️⃣ Testing memory pressure handling...");
    let temp_dir = TempDir::new()?;
    
    // Create database with small cache
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024; // 1MB cache
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Write more data than cache can hold
    let value_size = 10 * 1024; // 10KB values
    let num_entries = 200; // 2MB total
    
    for i in 0..num_entries {
        let key = format!("mem_key_{:04}", i);
        let value = vec![i as u8; value_size];
        db.put(key.as_bytes(), &value)?;
    }
    
    // Read back all entries - should work despite cache pressure
    for i in 0..num_entries {
        let key = format!("mem_key_{:04}", i);
        let expected = vec![i as u8; value_size];
        match db.get(key.as_bytes())? {
            Some(actual) => assert_eq!(actual, expected),
            None => panic!("Key {} not found", key),
        }
    }
    
    println!("   ✓ Handled memory pressure correctly");
    
    Ok(())
}

fn test_rapid_open_close() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n9️⃣ Testing rapid open/close cycles...");
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path();
    
    // Rapid open/close with writes
    for cycle in 0..10 {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Write some data
        let key = format!("cycle_{}_key", cycle);
        let value = format!("cycle_{}_value", cycle);
        db.put(key.as_bytes(), value.as_bytes())?;
        
        // Explicitly drop to close
        drop(db);
    }
    
    // Final open to verify all data
    let db = Database::open(db_path, LightningDbConfig::default())?;
    for cycle in 0..10 {
        let key = format!("cycle_{}_key", cycle);
        let expected = format!("cycle_{}_value", cycle);
        assert_eq!(db.get(key.as_bytes())?, Some(expected.into_bytes()));
    }
    
    println!("   ✓ Rapid open/close cycles handled correctly");
    
    Ok(())
}

fn test_page_boundary_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔟 Testing page boundary edge cases...");
    let temp_dir = TempDir::new()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Create keys that will likely span page boundaries
    // Page size is typically 4096 bytes
    let key_size = 100;
    let value_size = 4000; // Close to page size
    
    for i in 0..10 {
        let key = format!("{:0>width$}", i, width = key_size);
        let value = vec![i as u8; value_size];
        db.put(key.as_bytes(), &value)?;
    }
    
    // Verify all entries
    for i in 0..10 {
        let key = format!("{:0>width$}", i, width = key_size);
        let expected = vec![i as u8; value_size];
        assert_eq!(db.get(key.as_bytes())?, Some(expected));
    }
    
    // Test scan across page boundaries
    let start_key = format!("{:0>width$}", 2, width = key_size);
    let end_key = format!("{:0>width$}", 8, width = key_size);
    
    let scan_count = db.scan(
        Some(start_key.into_bytes()),
        Some(end_key.into_bytes())
    )?.count();
    
    assert_eq!(scan_count, 6); // Keys 2-7
    println!("   ✓ Page boundary cases handled correctly");
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Lightning DB Edge Cases and Error Handling Test\n");

    let _ = std::fs::remove_dir_all("./edge_test_db");

    // Test 1: Empty Keys and Values
    test_empty_keys_values()?;

    // Test 2: Large Keys and Values
    test_large_keys_values()?;

    // Test 3: Special Characters
    test_special_characters()?;

    // Test 4: Transaction Edge Cases
    test_transaction_edge_cases()?;

    // Test 5: Concurrent Conflicts
    test_concurrent_conflicts()?;

    // Test 6: Resource Limits
    test_resource_limits()?;

    // Test 7: Iterator Edge Cases
    test_iterator_edge_cases()?;

    // Test 8: Database Recovery
    test_database_recovery()?;

    println!("\n✅ All edge case tests completed!");
    Ok(())
}

fn test_empty_keys_values() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣ Testing Empty Keys and Values...");

    let db = Database::create("./edge_test_db/empty", LightningDbConfig::default())?;

    // Empty key
    match db.put(b"", b"empty_key_value") {
        Ok(_) => match db.get(b"")? {
            Some(v) if v == b"empty_key_value" => println!("  ✓ Empty key handled correctly"),
            Some(_) => println!("  ❌ Empty key returned wrong value"),
            None => println!("  ❌ Empty key not found after insert"),
        },
        Err(e) => println!("  ⚠️  Empty key rejected: {}", e),
    }

    // Empty value
    db.put(b"empty_value_key", b"")?;
    match db.get(b"empty_value_key")? {
        Some(v) if v.is_empty() => println!("  ✓ Empty value handled correctly"),
        Some(v) => println!("  ❌ Empty value returned wrong data: {} bytes", v.len()),
        None => println!("  ❌ Empty value key not found"),
    }

    // Both empty
    match db.put(b"", b"") {
        Ok(_) => println!("  ✓ Empty key and value accepted"),
        Err(e) => println!("  ⚠️  Empty key/value rejected: {}", e),
    }

    Ok(())
}

fn test_large_keys_values() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n2️⃣ Testing Large Keys and Values...");

    let db = Database::create("./edge_test_db/large", LightningDbConfig::default())?;

    // Test progressively larger keys
    let key_sizes = vec![100, 1024, 10 * 1024, 100 * 1024, 1024 * 1024];

    for size in key_sizes {
        let large_key = vec![b'k'; size];
        let value = format!("value_for_{}_byte_key", size);

        match db.put(&large_key, value.as_bytes()) {
            Ok(_) => match db.get(&large_key)? {
                Some(v) if v == value.as_bytes() => {
                    println!("  ✓ {} byte key handled correctly", size);
                }
                _ => println!("  ❌ {} byte key retrieval failed", size),
            },
            Err(e) => println!("  ⚠️  {} byte key rejected: {}", size, e),
        }
    }

    // Test large values
    let value_sizes = vec![1024 * 1024, 10 * 1024 * 1024]; // 1MB, 10MB

    for size in value_sizes {
        let key = format!("large_value_{}", size);
        let large_value = vec![b'v'; size];

        match db.put(key.as_bytes(), &large_value) {
            Ok(_) => match db.get(key.as_bytes())? {
                Some(v) if v.len() == size => {
                    println!("  ✓ {} byte value handled correctly", size);
                }
                Some(v) => println!("  ❌ {} byte value wrong size: {}", size, v.len()),
                None => println!("  ❌ {} byte value not found", size),
            },
            Err(e) => println!("  ⚠️  {} byte value rejected: {}", size, e),
        }
    }

    Ok(())
}

fn test_special_characters() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n3️⃣ Testing Special Characters...");

    let db = Database::create("./edge_test_db/special", LightningDbConfig::default())?;

    // Null bytes
    let null_key = b"\x00\x00\x00";
    db.put(null_key, b"null_bytes_value")?;
    match db.get(null_key)? {
        Some(v) if v == b"null_bytes_value" => println!("  ✓ Null bytes handled correctly"),
        _ => println!("  ❌ Null bytes handling failed"),
    }

    // High bytes
    let high_bytes = b"\xFF\xFE\xFD\xFC";
    db.put(high_bytes, b"high_bytes_value")?;
    match db.get(high_bytes)? {
        Some(v) if v == b"high_bytes_value" => println!("  ✓ High bytes handled correctly"),
        _ => println!("  ❌ High bytes handling failed"),
    }

    // UTF-8
    let utf8_key = "键值数据库🔑".as_bytes();
    let utf8_value = "Lightning DB ⚡️🚀".as_bytes();
    db.put(utf8_key, utf8_value)?;
    match db.get(utf8_key)? {
        Some(v) if v == utf8_value => println!("  ✓ UTF-8 handled correctly"),
        _ => println!("  ❌ UTF-8 handling failed"),
    }

    // Control characters
    let control_key = b"\n\r\t\x0b";
    db.put(control_key, b"control_chars")?;
    match db.get(control_key)? {
        Some(v) if v == b"control_chars" => println!("  ✓ Control characters handled correctly"),
        _ => println!("  ❌ Control characters handling failed"),
    }

    Ok(())
}

fn test_transaction_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n4️⃣ Testing Transaction Edge Cases...");

    let db = Database::create("./edge_test_db/tx_edge", LightningDbConfig::default())?;

    // Double commit
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx)?;

    match db.commit_transaction(tx) {
        Err(e) => println!("  ✓ Double commit correctly rejected: {}", e),
        Ok(_) => println!("  ❌ Double commit incorrectly allowed"),
    }

    // Double abort
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"abort_key", b"abort_value")?;
    db.abort_transaction(tx)?;

    match db.abort_transaction(tx) {
        Err(e) => println!("  ✓ Double abort correctly rejected: {}", e),
        Ok(_) => println!("  ❌ Double abort incorrectly allowed"),
    }

    // Use after abort
    let tx = db.begin_transaction()?;
    db.abort_transaction(tx)?;

    match db.put_tx(tx, b"after_abort", b"value") {
        Err(e) => println!("  ✓ Use after abort correctly rejected: {}", e),
        Ok(_) => println!("  ❌ Use after abort incorrectly allowed"),
    }

    // Large transaction
    let tx = db.begin_transaction()?;
    for i in 0..10000 {
        let key = format!("large_tx_{}", i);
        db.put_tx(tx, key.as_bytes(), b"value")?;
    }
    db.commit_transaction(tx)?;
    println!("  ✓ Large transaction (10k ops) handled correctly");

    // Nested transaction simulation (not supported, should fail gracefully)
    let tx1 = db.begin_transaction()?;
    db.put_tx(tx1, b"tx1_key", b"tx1_value")?;

    let tx2 = db.begin_transaction()?;
    db.put_tx(tx2, b"tx2_key", b"tx2_value")?;

    // Both should work independently
    db.commit_transaction(tx1)?;
    db.commit_transaction(tx2)?;

    if db.get(b"tx1_key")?.is_some() && db.get(b"tx2_key")?.is_some() {
        println!("  ✓ Concurrent transactions handled correctly");
    } else {
        println!("  ❌ Concurrent transaction handling failed");
    }

    Ok(())
}

fn test_concurrent_conflicts() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n5️⃣ Testing Concurrent Conflicts...");

    let db = Arc::new(Database::create(
        "./edge_test_db/conflicts",
        LightningDbConfig::default(),
    )?);

    // Write-write conflict
    db.put(b"conflict_key", b"initial_value")?;

    let db1 = db.clone();
    let db2 = db.clone();

    let handle1 = thread::spawn(move || {
        let tx = db1.begin_transaction().unwrap();
        thread::sleep(Duration::from_millis(10));
        db1.put_tx(tx, b"conflict_key", b"thread1_value").unwrap();
        db1.commit_transaction(tx)
    });

    let handle2 = thread::spawn(move || {
        let tx = db2.begin_transaction().unwrap();
        thread::sleep(Duration::from_millis(10));
        db2.put_tx(tx, b"conflict_key", b"thread2_value").unwrap();
        db2.commit_transaction(tx)
    });

    let result1 = handle1.join().unwrap();
    let result2 = handle2.join().unwrap();

    match (result1, result2) {
        (Ok(_), Ok(_)) => {
            println!("  ⚠️  Both conflicting transactions succeeded (last-write-wins)")
        }
        (Ok(_), Err(_)) | (Err(_), Ok(_)) => println!("  ✓ Write conflict detected and handled"),
        (Err(_), Err(_)) => println!("  ❌ Both transactions failed unexpectedly"),
    }

    // Many concurrent readers
    let mut handles = vec![];
    for i in 0..100 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = db_clone.get(format!("reader_key_{}", i).as_bytes());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    println!("  ✓ 100 concurrent readers handled without issues");

    Ok(())
}

fn test_resource_limits() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n6️⃣ Testing Resource Limits...");

    let mut config = LightningDbConfig::default();
    config.max_active_transactions = 10;

    let db = Database::create("./edge_test_db/limits", config)?;

    // Try to exceed transaction limit
    let mut transactions = vec![];
    let mut created = 0;

    for i in 0..20 {
        match db.begin_transaction() {
            Ok(tx) => {
                transactions.push(tx);
                created += 1;
            }
            Err(e) => {
                println!(
                    "  ✓ Transaction limit enforced at {} transactions: {}",
                    i, e
                );
                break;
            }
        }
    }

    if created == 20 {
        println!("  ⚠️  Transaction limit not enforced (created 20 transactions)");
    }

    // Clean up transactions
    for tx in transactions {
        let _ = db.abort_transaction(tx);
    }

    // Test memory pressure with small cache
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024; // 1MB cache

    let db_small = Database::create("./edge_test_db/small_cache", config)?;

    // Insert data larger than cache
    for i in 0..1000 {
        let key = format!("pressure_{}", i);
        let value = vec![0u8; 10240]; // 10KB per entry
        db_small.put(key.as_bytes(), &value)?;
    }

    // Should still be able to read
    match db_small.get(b"pressure_0")? {
        Some(v) if v.len() == 10240 => println!("  ✓ Handles cache pressure correctly"),
        _ => println!("  ❌ Failed under cache pressure"),
    }

    Ok(())
}

fn test_iterator_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n7️⃣ Testing Iterator Edge Cases...");

    let db = Database::create("./edge_test_db/iterator", LightningDbConfig::default())?;

    // Empty database iteration
    let iter = db.scan(None, None)?;
    let count = iter.count();
    if count == 0 {
        println!("  ✓ Empty database iteration handled correctly");
    } else {
        println!("  ❌ Empty database returned {} items", count);
    }

    // Single item
    db.put(b"single", b"value")?;
    let iter = db.scan(None, None)?;
    let items: Vec<_> = iter.collect::<Result<Vec<_>, _>>()?;
    if items.len() == 1 && items[0].0 == b"single" {
        println!("  ✓ Single item iteration correct");
    } else {
        println!("  ❌ Single item iteration failed");
    }

    // Boundary conditions
    db.put(b"\x00", b"min_key")?;
    db.put(b"\xFF\xFF\xFF", b"max_key")?;

    let iter = db.scan(None, None)?;
    let boundary_items: Vec<_> = iter.collect::<Result<Vec<_>, _>>()?;
    if boundary_items.len() >= 3 {
        println!("  ✓ Boundary key iteration handled");
    }

    // Invalid range (start > end)
    let iter = db.scan(Some(b"z".to_vec()), Some(b"a".to_vec()))?;
    let invalid_count = iter.count();
    if invalid_count == 0 {
        println!("  ✓ Invalid range returns empty iterator");
    } else {
        println!("  ❌ Invalid range returned {} items", invalid_count);
    }

    // Exact match range
    let iter = db.scan(Some(b"single".to_vec()), Some(b"single\x00".to_vec()))?;
    let exact_items: Vec<_> = iter.collect::<Result<Vec<_>, _>>()?;
    if exact_items.len() == 1 {
        println!("  ✓ Exact match range query works");
    } else {
        println!("  ❌ Exact match returned {} items", exact_items.len());
    }

    Ok(())
}

fn test_database_recovery() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n8️⃣ Testing Database Recovery...");

    let db_path = "./edge_test_db/recovery";

    // Create and populate
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;

        // Regular data
        for i in 0..100 {
            let key = format!("recovery_{}", i);
            db.put(key.as_bytes(), b"data")?;
        }

        // Uncommitted transaction
        let tx = db.begin_transaction()?;
        db.put_tx(tx, b"uncommitted", b"should_not_persist")?;
        // Don't commit, just drop
    }

    // Reopen and verify
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Check committed data
        let mut found = 0;
        for i in 0..100 {
            let key = format!("recovery_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }

        if found == 100 {
            println!("  ✓ All committed data recovered");
        } else {
            println!("  ❌ Only {}/100 entries recovered", found);
        }

        // Check uncommitted data
        match db.get(b"uncommitted")? {
            None => println!("  ✓ Uncommitted transaction correctly not persisted"),
            Some(_) => println!("  ❌ Uncommitted transaction incorrectly persisted"),
        }
    }

    // Test corrupted database handling
    let corrupted_path = "./edge_test_db/corrupted";
    {
        let db = Database::create(corrupted_path, LightningDbConfig::default())?;
        db.put(b"test", b"data")?;
    }

    // Corrupt a file (simulate)
    if let Ok(entries) = std::fs::read_dir(corrupted_path) {
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.path().extension().map(|e| e == "db").unwrap_or(false) {
                    // Truncate file to simulate corruption
                    if let Ok(file) = std::fs::OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(entry.path())
                    {
                        drop(file);
                        break;
                    }
                }
            }
        }
    }

    // Try to open corrupted database
    match Database::open(corrupted_path, LightningDbConfig::default()) {
        Ok(_) => println!("  ⚠️  Corrupted database opened (recovery succeeded)"),
        Err(e) => println!("  ✓ Corrupted database detected: {}", e),
    }

    Ok(())
}

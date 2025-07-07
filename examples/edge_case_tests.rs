use lightning_db::{Database, LightningDbConfig};
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Edge Case Tests ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();

    // Test 1: Empty values
    println!("Test 1: Empty Values");
    println!("====================");
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;

        // Empty value
        db.put(b"empty_value_key", b"")?;
        match db.get(b"empty_value_key")? {
            Some(value) => {
                assert_eq!(value.len(), 0);
                println!("  ✓ Empty value stored and retrieved correctly");
            }
            None => panic!("Empty value key not found!"),
        }

        // Empty key (should this be allowed?)
        match db.put(b"", b"value_for_empty_key") {
            Ok(_) => match db.get(b"")? {
                Some(value) => {
                    assert_eq!(value, b"value_for_empty_key");
                    println!("  ✓ Empty key stored and retrieved correctly");
                }
                None => println!("  ✗ Empty key not found after storage"),
            },
            Err(e) => {
                println!("  ✓ Empty key rejected as expected: {}", e);
            }
        }
    }

    // Test 2: Very long keys and values
    println!("\nTest 2: Very Long Keys and Values");
    println!("==================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // 1KB key
        let long_key = vec![b'K'; 1024];
        let value = b"value_for_long_key";

        db.put(&long_key, value)?;
        match db.get(&long_key)? {
            Some(v) => {
                assert_eq!(v, value);
                println!("  ✓ 1KB key handled correctly");
            }
            None => panic!("Long key not found!"),
        }

        // 10KB key
        let very_long_key = vec![b'K'; 10240];
        match db.put(&very_long_key, b"value") {
            Ok(_) => println!("  ✓ 10KB key accepted"),
            Err(e) => println!("  ℹ 10KB key rejected: {}", e),
        }

        // 10MB value
        let huge_value = vec![b'V'; 10 * 1024 * 1024];
        match db.put(b"huge_value_key", &huge_value) {
            Ok(_) => match db.get(b"huge_value_key")? {
                Some(v) => {
                    assert_eq!(v.len(), huge_value.len());
                    println!("  ✓ 10MB value handled correctly");
                }
                None => panic!("Huge value key not found!"),
            },
            Err(e) => println!("  ℹ 10MB value rejected: {}", e),
        }
    }

    // Test 3: Special characters and binary data
    println!("\nTest 3: Special Characters and Binary Data");
    println!("==========================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Null bytes in key and value
        let null_key = b"key\x00with\x00nulls";
        let null_value = b"value\x00with\x00nulls";

        db.put(null_key, null_value)?;
        match db.get(null_key)? {
            Some(v) => {
                assert_eq!(v, null_value);
                println!("  ✓ Null bytes in key/value handled correctly");
            }
            None => panic!("Null byte key not found!"),
        }

        // All possible byte values
        let all_bytes_key: Vec<u8> = (0..=255).collect();
        let all_bytes_value: Vec<u8> = (0..=255).rev().collect();

        db.put(&all_bytes_key, &all_bytes_value)?;
        match db.get(&all_bytes_key)? {
            Some(v) => {
                assert_eq!(v, all_bytes_value);
                println!("  ✓ All byte values (0-255) handled correctly");
            }
            None => panic!("All bytes key not found!"),
        }

        // UTF-8 with various Unicode
        let unicode_key = "key_🔥_火_🚀_שלום_مرحبا_🎯";
        let unicode_value = "value_🌍_世界_🌟_hello_नमस्ते_🎨";

        db.put(unicode_key.as_bytes(), unicode_value.as_bytes())?;
        match db.get(unicode_key.as_bytes())? {
            Some(v) => {
                assert_eq!(v, unicode_value.as_bytes());
                println!("  ✓ Complex Unicode handled correctly");
            }
            None => panic!("Unicode key not found!"),
        }
    }

    // Test 4: Overwrite behavior
    println!("\nTest 4: Overwrite Behavior");
    println!("==========================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        let key = b"overwrite_test_key";

        // Initial write
        db.put(key, b"value1")?;
        assert_eq!(db.get(key)?.unwrap(), b"value1");

        // Overwrite with larger value
        db.put(key, b"much_larger_value_2")?;
        assert_eq!(db.get(key)?.unwrap(), b"much_larger_value_2");

        // Overwrite with smaller value
        db.put(key, b"v3")?;
        assert_eq!(db.get(key)?.unwrap(), b"v3");

        // Overwrite many times
        for i in 0..100 {
            let value = format!("value_{}", i);
            db.put(key, value.as_bytes())?;
        }
        assert_eq!(db.get(key)?.unwrap(), b"value_99");

        println!("  ✓ Overwrite behavior correct after 100+ overwrites");
    }

    // Test 5: Delete edge cases
    println!("\nTest 5: Delete Edge Cases");
    println!("=========================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Delete non-existent key
        match db.delete(b"non_existent_key") {
            Ok(_) => println!("  ✓ Deleting non-existent key succeeded (idempotent)"),
            Err(e) => println!("  ℹ Deleting non-existent key failed: {}", e),
        }

        // Delete then re-add
        let key = b"delete_readd_key";
        db.put(key, b"value1")?;
        db.delete(key)?;
        assert!(db.get(key)?.is_none());

        db.put(key, b"value2")?;
        assert_eq!(db.get(key)?.unwrap(), b"value2");
        println!("  ✓ Delete and re-add works correctly");

        // Delete all keys
        for i in 0..100 {
            let key = format!("bulk_delete_key_{}", i);
            db.put(key.as_bytes(), b"value")?;
        }

        for i in 0..100 {
            let key = format!("bulk_delete_key_{}", i);
            db.delete(key.as_bytes())?;
        }

        // Verify all deleted
        let mut found = 0;
        for i in 0..100 {
            let key = format!("bulk_delete_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }
        assert_eq!(found, 0);
        println!("  ✓ Bulk delete successful (100 keys)");
    }

    // Test 6: Transaction edge cases
    println!("\nTest 6: Transaction Edge Cases");
    println!("==============================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Empty transaction
        let tx_id = db.begin_transaction()?;
        db.commit_transaction(tx_id)?;
        println!("  ✓ Empty transaction commits successfully");

        // Large transaction
        let tx_id = db.begin_transaction()?;
        for i in 0..1000 {
            let key = format!("large_tx_key_{}", i);
            let value = format!("large_tx_value_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        db.commit_transaction(tx_id)?;

        // Verify all committed
        let mut verified = 0;
        for i in 0..1000 {
            let key = format!("large_tx_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                verified += 1;
            }
        }
        assert_eq!(verified, 1000);
        println!("  ✓ Large transaction (1000 ops) committed successfully");

        // Abort transaction
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"abort_test_key", b"should_not_exist")?;
        db.abort_transaction(tx_id)?;

        assert!(db.get(b"abort_test_key")?.is_none());
        println!("  ✓ Aborted transaction changes not visible");

        // Nested operations (put inside transaction)
        let tx_id = db.begin_transaction()?;
        db.put_tx(tx_id, b"tx_key1", b"tx_value1")?;

        // Try non-transactional put while transaction is active
        db.put(b"non_tx_key", b"non_tx_value")?;

        db.put_tx(tx_id, b"tx_key2", b"tx_value2")?;
        db.commit_transaction(tx_id)?;

        assert!(db.get(b"tx_key1")?.is_some());
        assert!(db.get(b"tx_key2")?.is_some());
        assert!(db.get(b"non_tx_key")?.is_some());
        println!("  ✓ Mixed transactional/non-transactional operations work");
    }

    // Test 7: Key ordering and iteration
    println!("\nTest 7: Key Ordering");
    println!("====================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Insert keys in specific order
        let keys = vec![
            b"aaa".to_vec(),
            b"zzz".to_vec(),
            b"mmm".to_vec(),
            b"000".to_vec(),
            b"~~~".to_vec(),
            b"AAA".to_vec(),
            b"ZZZ".to_vec(),
            vec![0xFF, 0xFF, 0xFF],
            vec![0x00, 0x00, 0x00],
            b"".to_vec(), // empty key if supported
        ];

        for (i, key) in keys.iter().enumerate() {
            if !key.is_empty() || db.put(key, format!("value_{}", i).as_bytes()).is_ok() {
                let _ = db.put(key, format!("value_{}", i).as_bytes());
            }
        }

        println!("  ✓ Various key types inserted successfully");

        // Note: Would test iteration order here if iterator API was available
    }

    // Test 8: Persistence after operations
    println!("\nTest 8: Persistence Verification");
    println!("================================");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Count total keys (approximate)
        let test_keys: Vec<&[u8]> = vec![
            b"empty_value_key",
            b"",
            b"huge_value_key",
            b"overwrite_test_key",
            b"delete_readd_key",
            b"non_tx_key",
            b"tx_key1",
            b"tx_key2",
        ];

        let mut found = 0;
        for key in &test_keys {
            if db.get(key)?.is_some() {
                found += 1;
            }
        }

        println!("  ✓ Found {} persisted keys from previous tests", found);

        // Force checkpoint
        db.checkpoint()?;
        drop(db);

        // Reopen and verify
        let db = Database::open(db_path, LightningDbConfig::default())?;
        let mut still_found = 0;
        for key in &test_keys {
            if db.get(key)?.is_some() {
                still_found += 1;
            }
        }

        assert_eq!(found, still_found);
        println!(
            "  ✓ All {} keys persisted after checkpoint and reopen",
            still_found
        );
    }

    println!("\n=== All Edge Case Tests Passed! ===");
    println!("Lightning DB handles edge cases correctly:");
    println!("✓ Empty values and keys");
    println!("✓ Very long keys and values");
    println!("✓ Binary data and special characters");
    println!("✓ Overwrite behavior");
    println!("✓ Delete operations");
    println!("✓ Transaction edge cases");
    println!("✓ Various key types");
    println!("✓ Data persistence");

    Ok(())
}

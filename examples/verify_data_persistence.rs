use lightning_db::{Database, LightningDbConfig};
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Data Persistence Verification ===\n");

    // Create a temporary directory for the test
    let dir = tempdir()?;
    let db_path = dir.path().join("persistence_test.db");

    println!("Step 1: Creating database at {:?}", db_path);

    // Test 1: Create database and write data
    {
        let db = Database::create(&db_path, LightningDbConfig::default())?;

        println!("\nStep 2: Writing test data...");

        // Write various types of data
        // Simple key-value pairs
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}_with_some_data_{}", i, "x".repeat(100));
            db.put(key.as_bytes(), value.as_bytes())?;

            if i % 100 == 0 {
                println!("  Written {} entries...", i);
            }
        }

        // Large values
        println!("\nStep 3: Writing large values...");
        for i in 0..10 {
            let key = format!("large_key_{}", i);
            let value = vec![i as u8; 1_000_000]; // 1MB values
            db.put(key.as_bytes(), &value)?;
            println!("  Written 1MB value {}", i);
        }

        // Special characters and binary data
        println!("\nStep 4: Writing binary data...");
        db.put(b"\x00\x01\x02\x03", b"binary_value")?;
        db.put("unicode_key_🔥".as_bytes(), "unicode_value_🚀".as_bytes())?;

        // Force data to disk
        println!("\nStep 5: Forcing checkpoint to ensure data is on disk...");
        db.checkpoint()?;

        println!("\nStep 6: Closing database (data should be persisted)...");
        // Database drops here, should flush all data
    }

    // Verify file exists and has size
    let file_size = fs::metadata(&db_path)?.len();
    println!(
        "\nStep 7: Database file size: {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / 1_048_576.0
    );

    if file_size < 1000 {
        panic!("Database file suspiciously small! Data may not be persisted!");
    }

    // Test 2: Reopen database and verify all data
    {
        println!("\nStep 8: Reopening database to verify persistence...");
        let db = Database::open(&db_path, LightningDbConfig::default())?;

        println!("\nStep 9: Verifying simple key-value pairs...");
        let mut verified = 0;
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let expected_value = format!("value_{:04}_with_some_data_{}", i, "x".repeat(100));

            match db.get(key.as_bytes())? {
                Some(value) => {
                    if value != expected_value.as_bytes() {
                        panic!("Data corruption! Key {} has wrong value", key);
                    }
                    verified += 1;
                }
                None => panic!("Data loss! Key {} not found after reopening", key),
            }

            if i % 100 == 0 {
                println!("  Verified {} entries...", i);
            }
        }
        println!("✓ Successfully verified {} simple entries", verified);

        println!("\nStep 10: Verifying large values...");
        for i in 0..10 {
            let key = format!("large_key_{}", i);
            let expected_value = vec![i as u8; 1_000_000];

            match db.get(key.as_bytes())? {
                Some(value) => {
                    if value != expected_value {
                        panic!("Large value corruption! Key {} has wrong value", key);
                    }
                    println!("  ✓ Verified 1MB value {}", i);
                }
                None => panic!("Large value loss! Key {} not found", key),
            }
        }

        println!("\nStep 11: Verifying binary data...");
        match db.get(b"\x00\x01\x02\x03")? {
            Some(value) => {
                assert_eq!(value, b"binary_value");
                println!("  ✓ Binary data verified");
            }
            None => panic!("Binary key not found!"),
        }

        match db.get("unicode_key_🔥".as_bytes())? {
            Some(value) => {
                assert_eq!(value, "unicode_value_🚀".as_bytes());
                println!("  ✓ Unicode data verified");
            }
            None => panic!("Unicode key not found!"),
        }

        // Test 3: Modify data and verify persistence
        println!("\nStep 12: Modifying data...");
        db.put(b"key_0000", b"modified_value")?;
        db.put(b"new_key_after_reopen", b"new_value")?;

        // Delete some data
        println!("\nStep 13: Deleting some entries...");
        for i in 500..600 {
            let key = format!("key_{:04}", i);
            db.delete(key.as_bytes())?;
        }

        println!("\nStep 14: Forcing another checkpoint...");
        db.checkpoint()?;
    }

    // Test 4: Final verification after modifications
    {
        println!("\nStep 15: Final reopening to verify all changes...");
        let db = Database::open(&db_path, LightningDbConfig::default())?;

        // Check modified value
        match db.get(b"key_0000")? {
            Some(value) => {
                assert_eq!(value, b"modified_value");
                println!("  ✓ Modified value persisted correctly");
            }
            None => panic!("Modified key not found!"),
        }

        // Check new key
        match db.get(b"new_key_after_reopen")? {
            Some(value) => {
                assert_eq!(value, b"new_value");
                println!("  ✓ New key persisted correctly");
            }
            None => panic!("New key not found!"),
        }

        // Check deletions
        let mut deleted_verified = 0;
        for i in 500..600 {
            let key = format!("key_{:04}", i);
            if db.get(key.as_bytes())?.is_some() {
                panic!("Deleted key {} still exists!", key);
            }
            deleted_verified += 1;
        }
        println!(
            "  ✓ Verified {} deletions persisted correctly",
            deleted_verified
        );

        // Check that non-deleted keys still exist
        for i in 100..200 {
            let key = format!("key_{:04}", i);
            if db.get(key.as_bytes())?.is_none() {
                panic!("Non-deleted key {} is missing!", key);
            }
        }
        println!("  ✓ Non-deleted keys still exist");

        // Run integrity check
        println!("\nStep 16: Running integrity verification...");
        let report = db.verify_integrity()?;
        println!("  Integrity check completed:");
        println!("  - Total pages: {}", report.statistics.total_pages);
        println!("  - Total keys: {}", report.statistics.total_keys);
        println!("  - Errors found: {}", report.errors.len());

        if !report.errors.is_empty() {
            println!("\n  ⚠️  Integrity errors detected:");
            for error in &report.errors {
                println!("    - {:?}", error);
            }
        }
    }

    // Final file size check
    let final_size = fs::metadata(&db_path)?.len();
    println!(
        "\nStep 17: Final database file size: {} bytes ({:.2} MB)",
        final_size,
        final_size as f64 / 1_048_576.0
    );

    println!("\n✅ ALL DATA PERSISTENCE TESTS PASSED!");
    println!("✅ Data is correctly persisted to disk and survives database restarts!");

    Ok(())
}

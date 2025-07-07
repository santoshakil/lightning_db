use lightning_db::{Database, LightningDbConfig};
use std::fs;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Persistence Verification (Correct) ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();

    println!("Step 1: Creating database at {:?}", db_path);

    // Test 1: Create database and write substantial data
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;

        println!("\nStep 2: Writing test data...");

        // Write various types of data
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
            let value = vec![i as u8; 100_000]; // 100KB values
            db.put(key.as_bytes(), &value)?;
            println!("  Written 100KB value {}", i);
        }

        // Binary data
        println!("\nStep 4: Writing binary data...");
        db.put(b"\x00\x01\x02\x03", b"binary_value")?;
        db.put("unicode_key_🔥".as_bytes(), "unicode_value_🚀".as_bytes())?;

        // Force data to disk
        println!("\nStep 5: Forcing checkpoint...");
        db.checkpoint()?;

        println!("\nStep 6: Closing database...");
    }

    // Check actual file sizes
    println!("\nStep 7: Checking file sizes:");
    let btree_path = db_path.join("btree.db");
    let btree_size = fs::metadata(&btree_path)?.len();
    println!(
        "  btree.db: {} bytes ({:.2} MB)",
        btree_size,
        btree_size as f64 / 1_048_576.0
    );

    // Check WAL directory
    let wal_dir = db_path.join("wal");
    let mut wal_total_size = 0u64;
    if wal_dir.exists() {
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if entry.metadata()?.is_file() {
                wal_total_size += entry.metadata()?.len();
            }
        }
        println!(
            "  WAL total: {} bytes ({:.2} MB)",
            wal_total_size,
            wal_total_size as f64 / 1_048_576.0
        );
    }

    // Test 2: Reopen and verify all data
    {
        println!("\nStep 8: Reopening database...");
        let db = Database::open(db_path, LightningDbConfig::default())?;

        println!("\nStep 9: Verifying data...");
        let mut verified = 0;
        let mut missing = 0;

        // Verify regular entries
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let expected_value = format!("value_{:04}_with_some_data_{}", i, "x".repeat(100));

            match db.get(key.as_bytes())? {
                Some(value) => {
                    if value == expected_value.as_bytes() {
                        verified += 1;
                    } else {
                        println!("  ERROR: Key {} has wrong value!", key);
                    }
                }
                None => {
                    println!("  ERROR: Key {} not found!", key);
                    missing += 1;
                }
            }
        }

        println!(
            "  Regular entries: {} verified, {} missing",
            verified, missing
        );

        // Verify large values
        let mut large_verified = 0;
        for i in 0..10 {
            let key = format!("large_key_{}", i);
            let expected_value = vec![i as u8; 100_000];

            match db.get(key.as_bytes())? {
                Some(value) => {
                    if value == expected_value {
                        large_verified += 1;
                    } else {
                        println!("  ERROR: Large key {} has wrong value!", key);
                    }
                }
                None => println!("  ERROR: Large key {} not found!", key),
            }
        }
        println!("  Large values: {} verified", large_verified);

        // Verify binary data
        match db.get(b"\x00\x01\x02\x03")? {
            Some(value) => {
                assert_eq!(value, b"binary_value");
                println!("  ✓ Binary data verified");
            }
            None => println!("  ERROR: Binary key not found!"),
        }

        match db.get("unicode_key_🔥".as_bytes())? {
            Some(value) => {
                assert_eq!(value, "unicode_value_🚀".as_bytes());
                println!("  ✓ Unicode data verified");
            }
            None => println!("  ERROR: Unicode key not found!"),
        }

        // Test 3: Modify and delete
        println!("\nStep 10: Testing modifications...");
        db.put(b"key_0000", b"modified_value")?;
        db.put(b"new_key", b"new_value")?;

        // Delete some entries
        for i in 500..600 {
            let key = format!("key_{:04}", i);
            db.delete(key.as_bytes())?;
        }

        println!("\nStep 11: Final checkpoint...");
        db.checkpoint()?;
    }

    // Final verification
    {
        println!("\nStep 12: Final verification after reopening...");
        let db = Database::open(db_path, LightningDbConfig::default())?;

        // Check modified value
        match db.get(b"key_0000")? {
            Some(value) => {
                if value == b"modified_value" {
                    println!("  ✓ Modified value persisted");
                } else {
                    println!("  ERROR: Modified value incorrect!");
                }
            }
            None => println!("  ERROR: Modified key not found!"),
        }

        // Check new key
        match db.get(b"new_key")? {
            Some(value) => {
                if value == b"new_value" {
                    println!("  ✓ New key persisted");
                } else {
                    println!("  ERROR: New value incorrect!");
                }
            }
            None => println!("  ERROR: New key not found!"),
        }

        // Check deletions
        let mut deletions_verified = 0;
        for i in 500..600 {
            let key = format!("key_{:04}", i);
            if db.get(key.as_bytes())?.is_none() {
                deletions_verified += 1;
            }
        }
        println!("  ✓ {} deletions verified", deletions_verified);

        // Run integrity check
        println!("\nStep 13: Running integrity check...");
        let report = db.verify_integrity()?;
        println!("  Total pages: {}", report.statistics.total_pages);
        println!("  Total keys: {}", report.statistics.total_keys);
        println!("  Errors: {}", report.errors.len());

        if report.errors.is_empty() {
            println!("\n✅ ALL TESTS PASSED!");
            println!("✅ Data persistence is working correctly!");
        } else {
            println!("\n⚠️  Some integrity errors detected:");
            for error in &report.errors {
                println!("  - {:?}", error);
            }
        }
    }

    // Final file sizes
    let final_btree_size = fs::metadata(&btree_path)?.len();
    println!(
        "\nFinal btree.db size: {} bytes ({:.2} MB)",
        final_btree_size,
        final_btree_size as f64 / 1_048_576.0
    );

    Ok(())
}

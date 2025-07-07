use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::fs;
use tempfile::tempdir;

fn main() {
    println!("🔍 Data Persistence Verification Test");

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_db");

    // Test 1: Basic persistence
    println!("\n📝 Test 1: Basic Put/Get Persistence");
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;

        let db = Database::create(&db_path, config).unwrap();

        // Insert test data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}_data", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Force flush to disk
        db.checkpoint().unwrap();
        db.sync().unwrap();

        println!("✅ Inserted 100 key-value pairs");

        // Verify data is readable before closing
        let test_value = db.get(b"key_050").unwrap().unwrap();
        assert_eq!(test_value, b"value_050_data");
        println!("✅ Data readable before database close");

        drop(db); // Close database
    }

    // Verify files exist on disk
    println!("\n📁 Verifying database files on disk:");
    let mut total_size = 0;
    for entry in fs::read_dir(&db_path).unwrap() {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        total_size += metadata.len();
        println!(
            "  📄 {} ({} bytes)",
            entry.file_name().to_string_lossy(),
            metadata.len()
        );
    }
    println!("  📊 Total database size: {} bytes", total_size);
    assert!(
        total_size > 0,
        "Database files should exist and have content"
    );

    // Test 2: Reopen and verify data persistence
    println!("\n🔄 Test 2: Reopening Database");
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        println!("✅ Database reopened successfully");

        // Verify all data is still there
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_{:03}_data", i);
            let actual_value = db.get(key.as_bytes()).unwrap();

            match actual_value {
                Some(value) => {
                    assert_eq!(
                        value,
                        expected_value.as_bytes(),
                        "Data mismatch for key {}",
                        key
                    );
                }
                None => panic!("Missing key: {}", key),
            }
        }

        println!("✅ All 100 key-value pairs verified after reopen");
    }

    // Test 3: Transaction persistence
    println!("\n💾 Test 3: Transaction Persistence");
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Start transaction and add more data
        let tx_id = db.begin_transaction().unwrap();

        for i in 100..150 {
            let key = format!("tx_key_{:03}", i);
            let value = format!("tx_value_{:03}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Commit transaction
        db.commit_transaction(tx_id).unwrap();
        db.sync().unwrap();

        println!("✅ Transaction with 50 operations committed");

        drop(db);
    }

    // Test 4: Verify transaction data persists after reopen
    println!("\n🔄 Test 4: Transaction Data Persistence");
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Verify original data
        let original_value = db.get(b"key_050").unwrap().unwrap();
        assert_eq!(original_value, b"value_050_data");

        // Verify transaction data
        for i in 100..150 {
            let key = format!("tx_key_{:03}", i);
            let expected_value = format!("tx_value_{:03}", i);
            let actual_value = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(actual_value, expected_value.as_bytes());
        }

        println!("✅ All transaction data verified after reopen");
        println!("✅ Total verified: 150 key-value pairs");
    }

    // Test 5: Delete operations persistence
    println!("\n🗑️  Test 5: Delete Operations Persistence");
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Delete some keys
        for i in 0..25 {
            let key = format!("key_{:03}", i);
            db.delete(key.as_bytes()).unwrap();
        }

        db.sync().unwrap();
        drop(db);
    }

    // Verify deletes persisted
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&db_path, config).unwrap();

        // Verify deleted keys are gone
        for i in 0..25 {
            let key = format!("key_{:03}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_none(), "Key {} should be deleted", key);
        }

        // Verify remaining keys still exist
        for i in 25..100 {
            let key = format!("key_{:03}", i);
            let value = db.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {} should still exist", key);
        }

        println!("✅ Delete operations verified: 25 keys deleted, 75 keys remaining");
    }

    println!("\n🎉 ALL PERSISTENCE TESTS PASSED!");
    println!("✅ Basic put/get persistence");
    println!("✅ Database file persistence");
    println!("✅ Cross-session data persistence");
    println!("✅ Transaction persistence");
    println!("✅ Delete operation persistence");
    println!("\n📋 Data persistence verification COMPLETE");
}

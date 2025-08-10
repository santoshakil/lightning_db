//! Property-Based Tests for Lightning DB
//!
//! Comprehensive property-based testing to verify correctness,
//! data integrity, and ACID compliance under various conditions.

use lightning_db::property_testing::{PropertyTestConfig, PropertyTester};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

/// Test basic ACID properties with property-based testing
#[test]
fn test_acid_properties() {
    println!("🧪 Testing ACID Properties with Property-Based Testing...");

    let test_dir = TempDir::new().unwrap();
    let config = PropertyTestConfig {
        iterations: 100,
        max_key_size: 256,
        max_value_size: 1024,
        concurrent_threads: 2,
        max_operations_per_sequence: 50,
        edge_case_probability: 0.2,
        enable_crash_testing: false, // Disable for basic test
        enable_performance_testing: true,
    };

    let tester = PropertyTester::new(config);
    let report = tester.run_tests(test_dir.path()).unwrap();

    println!("Test Results:");
    report.print_summary();

    // Assert high success rate
    assert!(
        report.success_rate >= 95.0,
        "ACID property tests should have high success rate, got {:.1}%",
        report.success_rate
    );

    // Ensure we ran a reasonable number of tests
    assert!(
        report.total_tests >= 500,
        "Should run comprehensive test suite"
    );
}

/// Test data integrity under concurrent access
#[test]
fn test_concurrent_data_integrity() {
    println!("🔄 Testing Concurrent Data Integrity...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(test_dir.path(), config).unwrap());

    let num_threads = 4;
    let operations_per_thread = 100;
    let mut handles = Vec::new();

    // Start multiple threads performing operations
    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..operations_per_thread {
                let key = format!("thread_{}_{}", thread_id, i);
                let value = format!("value_{}_{}", thread_id, i);

                // Put operation
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();

                // Verify immediate read
                let retrieved = db_clone.get(key.as_bytes()).unwrap();
                assert_eq!(retrieved.as_deref(), Some(value.as_bytes()));

                // Occasional delete
                if i % 10 == 0 {
                    db_clone.delete(key.as_bytes()).unwrap();
                    let deleted = db_clone.get(key.as_bytes()).unwrap();
                    assert_eq!(deleted, None);
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify final state consistency
    let mut total_keys = 0;
    for thread_id in 0..num_threads {
        for i in 0..operations_per_thread {
            let key = format!("thread_{}_{}", thread_id, i);
            if let Some(_value) = db.get(key.as_bytes()).unwrap() {
                total_keys += 1;
            }
        }
    }

    println!("  Final state: {} keys present", total_keys);

    // Should have most keys (some deleted)
    let expected_keys = num_threads * operations_per_thread * 9 / 10; // 90% kept
    assert!(
        total_keys >= expected_keys * 8 / 10, // Allow some variance
        "Expected around {} keys, found {}",
        expected_keys,
        total_keys
    );
}

/// Test transaction atomicity with property-based approach
#[test]
fn test_transaction_atomicity() {
    println!("⚛️ Testing Transaction Atomicity...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(test_dir.path(), config).unwrap();

    // Test successful transaction
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"tx_key1", b"tx_value1").unwrap();
    db.put_tx(tx_id, b"tx_key2", b"tx_value2").unwrap();
    db.put_tx(tx_id, b"tx_key3", b"tx_value3").unwrap();
    db.commit_transaction(tx_id).unwrap();

    // Verify all keys are present
    assert_eq!(
        db.get(b"tx_key1").unwrap().as_deref(),
        Some(b"tx_value1".as_ref())
    );
    assert_eq!(
        db.get(b"tx_key2").unwrap().as_deref(),
        Some(b"tx_value2".as_ref())
    );
    assert_eq!(
        db.get(b"tx_key3").unwrap().as_deref(),
        Some(b"tx_value3".as_ref())
    );

    // Test aborted transaction
    let tx_id2 = db.begin_transaction().unwrap();
    db.put_tx(tx_id2, b"abort_key1", b"abort_value1").unwrap();
    db.put_tx(tx_id2, b"abort_key2", b"abort_value2").unwrap();
    db.abort_transaction(tx_id2).unwrap();

    // Verify no keys from aborted transaction exist
    assert_eq!(db.get(b"abort_key1").unwrap(), None);
    assert_eq!(db.get(b"abort_key2").unwrap(), None);

    println!("  ✅ Transaction atomicity verified");
}

/// Test edge cases and boundary conditions
#[test]
fn test_edge_cases() {
    println!("🔬 Testing Edge Cases...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(test_dir.path(), config).unwrap();

    // Empty key
    db.put(b"", b"empty_key_value").unwrap();
    assert_eq!(
        db.get(b"").unwrap().as_deref(),
        Some(b"empty_key_value".as_ref())
    );

    // Empty value
    db.put(b"empty_value_key", b"").unwrap();
    assert_eq!(
        db.get(b"empty_value_key").unwrap().as_deref(),
        Some(b"".as_ref())
    );

    // Large key
    let large_key = vec![b'K'; 1024];
    db.put(&large_key, b"large_key_value").unwrap();
    assert_eq!(
        db.get(&large_key).unwrap().as_deref(),
        Some(b"large_key_value".as_ref())
    );

    // Large value
    let large_value = vec![b'V'; 64 * 1024]; // 64KB
    db.put(b"large_value_key", &large_value).unwrap();
    assert_eq!(
        db.get(b"large_value_key").unwrap().as_deref(),
        Some(large_value.as_ref())
    );

    // Special characters in keys
    let special_key = b"key\x00\xFF\x01\x7F";
    db.put(special_key, b"special_chars").unwrap();
    assert_eq!(
        db.get(special_key).unwrap().as_deref(),
        Some(b"special_chars".as_ref())
    );

    // Unicode in values (treated as bytes)
    let unicode_value = "Hello 世界! 🌍".as_bytes();
    db.put(b"unicode_key", unicode_value).unwrap();
    assert_eq!(
        db.get(b"unicode_key").unwrap().as_deref(),
        Some(unicode_value)
    );

    println!("  ✅ Edge cases handled correctly");
}

/// Test crash recovery and durability
#[test]
fn test_durability_and_recovery() {
    println!("💾 Testing Durability and Recovery...");

    let test_dir = TempDir::new().unwrap();
    let data_path = test_dir.path().to_path_buf();

    // Phase 1: Write data
    {
        let config = LightningDbConfig::default();
        let db = Database::create(&data_path, config).unwrap();

        for i in 0..100 {
            let key = format!("recovery_key_{}", i);
            let value = format!("recovery_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Ensure some data is committed in a transaction
        let tx_id = db.begin_transaction().unwrap();
        db.put_tx(tx_id, b"tx_recovery", b"tx_recovery_value")
            .unwrap();
        db.commit_transaction(tx_id).unwrap();

        // Database is dropped here, simulating a crash
    }

    // Phase 2: Reopen and verify data
    {
        let config = LightningDbConfig::default();
        let db = Database::open(&data_path, config).unwrap();

        // Verify all data is still present
        for i in 0..100 {
            let key = format!("recovery_key_{}", i);
            let expected_value = format!("recovery_value_{}", i);

            match db.get(key.as_bytes()).unwrap() {
                Some(value) => {
                    assert_eq!(value, expected_value.as_bytes());
                }
                None => panic!("Key {} not found after recovery", key),
            }
        }

        // Verify transaction data
        assert_eq!(
            db.get(b"tx_recovery").unwrap().as_deref(),
            Some(b"tx_recovery_value".as_ref())
        );

        println!("  ✅ Data recovered successfully after restart");
    }
}

/// Test performance characteristics with property-based testing
#[test]
fn test_performance_properties() {
    println!("⚡ Testing Performance Properties...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(test_dir.path(), config).unwrap();

    let num_operations = 1000;
    let start_time = std::time::Instant::now();

    // Measure write performance
    let write_start = std::time::Instant::now();
    for i in 0..num_operations {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    let write_duration = write_start.elapsed();

    // Measure read performance
    let read_start = std::time::Instant::now();
    for i in 0..num_operations {
        let key = format!("perf_key_{}", i);
        db.get(key.as_bytes()).unwrap();
    }
    let read_duration = read_start.elapsed();

    let total_duration = start_time.elapsed();

    // Calculate metrics
    let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();
    let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();
    let total_ops_per_sec = (num_operations * 2) as f64 / total_duration.as_secs_f64();

    println!("  Write performance: {:.0} ops/sec", write_ops_per_sec);
    println!("  Read performance: {:.0} ops/sec", read_ops_per_sec);
    println!("  Total performance: {:.0} ops/sec", total_ops_per_sec);

    // Performance assertions
    assert!(
        write_ops_per_sec > 1000.0,
        "Write performance should exceed 1K ops/sec"
    );
    assert!(
        read_ops_per_sec > 10000.0,
        "Read performance should exceed 10K ops/sec"
    );

    // Average latency should be reasonable
    let avg_write_latency = write_duration / num_operations as u32;
    let avg_read_latency = read_duration / num_operations as u32;

    assert!(
        avg_write_latency < Duration::from_millis(10),
        "Write latency should be < 10ms"
    );
    assert!(
        avg_read_latency < Duration::from_millis(1),
        "Read latency should be < 1ms"
    );

    println!("  ✅ Performance characteristics verified");
}

/// Test data consistency across multiple operations
#[test]
fn test_data_consistency() {
    println!("🔗 Testing Data Consistency...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(test_dir.path(), config).unwrap();

    // Create initial dataset
    let mut expected_state = std::collections::HashMap::new();

    for i in 0..100 {
        let key = format!("consistency_key_{}", i);
        let value = format!("initial_value_{}", i);

        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        expected_state.insert(key.clone(), Some(value));
    }

    // Perform mixed operations
    for i in 0..50 {
        let key = format!("consistency_key_{}", i);

        if i % 3 == 0 {
            // Update
            let new_value = format!("updated_value_{}", i);
            db.put(key.as_bytes(), new_value.as_bytes()).unwrap();
            expected_state.insert(key, Some(new_value));
        } else if i % 7 == 0 {
            // Delete
            db.delete(key.as_bytes()).unwrap();
            expected_state.insert(key, None);
        }
    }

    // Verify final state matches expectations
    for (key, expected_value) in &expected_state {
        let actual_value = db.get(key.as_bytes()).unwrap();

        match (expected_value, actual_value) {
            (Some(expected), Some(actual)) => {
                assert_eq!(
                    actual,
                    expected.as_bytes(),
                    "Value mismatch for key: {}",
                    key
                );
            }
            (None, None) => {
                // Both expect and actual are None - consistent
            }
            (Some(expected), None) => {
                panic!(
                    "Expected value '{}' for key '{}', but key not found",
                    expected, key
                );
            }
            (None, Some(actual)) => {
                panic!(
                    "Expected key '{}' to be deleted, but found value: {:?}",
                    key, actual
                );
            }
        }
    }

    println!(
        "  ✅ Data consistency maintained across {} operations",
        expected_state.len()
    );
}

/// Test boundary conditions and limits
#[test]
fn test_boundary_conditions() {
    println!("📏 Testing Boundary Conditions...");

    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Database::create(test_dir.path(), config).unwrap();

    // Test maximum number of keys
    let key_count = 10000;
    for i in 0..key_count {
        let key = format!("boundary_key_{:06}", i);
        let value = format!("boundary_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify all keys can be retrieved
    let mut found_keys = 0;
    for i in 0..key_count {
        let key = format!("boundary_key_{:06}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            found_keys += 1;
        }
    }

    assert_eq!(found_keys, key_count, "All keys should be retrievable");

    // Test repeated operations on same key
    let test_key = b"repeated_operations_key";
    for i in 0..100 {
        let value = format!("iteration_{}", i);
        db.put(test_key, value.as_bytes()).unwrap();

        let retrieved = db.get(test_key).unwrap().unwrap();
        assert_eq!(retrieved, value.as_bytes());
    }

    println!("  ✅ Boundary conditions handled correctly");
}

/// Comprehensive property-based test runner
#[test]
fn test_comprehensive_property_suite() {
    println!("🧪 Running Comprehensive Property Test Suite...");

    let test_dir = TempDir::new().unwrap();

    // Configuration for thorough testing
    let config = PropertyTestConfig {
        iterations: 200,
        max_key_size: 512,
        max_value_size: 2048,
        concurrent_threads: 4,
        max_operations_per_sequence: 100,
        edge_case_probability: 0.15,
        enable_crash_testing: true,
        enable_performance_testing: true,
    };

    let tester = PropertyTester::new(config);
    let report = tester.run_tests(test_dir.path()).unwrap();

    println!("\n=== Property Test Suite Results ===");
    report.print_summary();

    // Comprehensive assertions
    assert!(
        report.total_tests >= 1000,
        "Should run comprehensive test suite"
    );
    assert!(
        report.success_rate >= 98.0,
        "Should have very high success rate for production database"
    );
    assert!(
        report.failed_tests < report.total_tests / 50,
        "Failed tests should be minimal"
    );

    // Check for critical invariant violations
    let critical_violations = report.violations_by_type.get("DataIntegrity").unwrap_or(&0)
        + report.violations_by_type.get("Consistency").unwrap_or(&0);

    assert_eq!(
        critical_violations, 0,
        "No critical data integrity or consistency violations allowed"
    );

    println!("  ✅ Comprehensive property test suite passed");
}

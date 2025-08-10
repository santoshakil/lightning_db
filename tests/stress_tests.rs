//! Stress tests for Lightning DB
//!
//! These tests validate the database under extreme concurrent load conditions.

mod stress_testing_framework;

use std::time::Duration;
use stress_testing_framework::{print_results, StressTestConfig, StressTestFramework};

#[test]
fn test_moderate_concurrent_load() {
    let config = StressTestConfig {
        writer_threads: 8,
        reader_threads: 8,
        mixed_threads: 4,
        transaction_threads: 2,
        duration: Duration::from_secs(10),
        key_space_size: 5_000,
        value_size_range: (100, 1_000),
        enable_chaos: false,
        cache_size: 50 * 1024 * 1024,
        enable_compression: true,
        transaction_size: 5,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // Assertions
    assert_eq!(
        results.consistency_violations, 0,
        "Data consistency violations detected"
    );
    assert_eq!(results.data_races_detected, 0, "Data races detected");
    assert_eq!(results.deadlocks_detected, 0, "Deadlocks detected");
    assert!(
        results.successful_operations > 0,
        "No successful operations"
    );

    let success_rate = results.successful_operations as f64 / results.total_operations as f64;
    assert!(
        success_rate > 0.99,
        "Success rate below 99%: {:.2}%",
        success_rate * 100.0
    );
}

#[test]
fn test_high_concurrency() {
    let config = StressTestConfig {
        writer_threads: 16,
        reader_threads: 16,
        mixed_threads: 8,
        transaction_threads: 4,
        duration: Duration::from_secs(5),
        key_space_size: 10_000,
        value_size_range: (50, 500),
        enable_chaos: false,
        cache_size: 100 * 1024 * 1024,
        enable_compression: true,
        transaction_size: 3,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // With high concurrency, we still expect no data corruption
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations under high concurrency"
    );
    assert_eq!(
        results.data_races_detected, 0,
        "Data races under high concurrency"
    );

    // Allow some transaction conflicts but no deadlocks
    assert_eq!(results.deadlocks_detected, 0, "Deadlocks detected");
}

#[test]
fn test_transaction_conflicts() {
    let config = StressTestConfig {
        writer_threads: 2,
        reader_threads: 2,
        mixed_threads: 1,
        transaction_threads: 10, // Many transaction threads
        duration: Duration::from_secs(5),
        key_space_size: 100, // Small key space to force conflicts
        value_size_range: (100, 200),
        enable_chaos: false,
        cache_size: 10 * 1024 * 1024,
        enable_compression: false,
        transaction_size: 10,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // Even with conflicts, no consistency violations
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations in transactions"
    );
    assert_eq!(results.data_races_detected, 0, "Data races in transactions");

    // Transaction success rate may be lower due to conflicts
    let tx_success_rate = results.successful_operations as f64 / results.total_operations as f64;
    assert!(
        tx_success_rate > 0.5,
        "Transaction success rate too low: {:.2}%",
        tx_success_rate * 100.0
    );
}

#[test]
fn test_memory_pressure() {
    let config = StressTestConfig {
        writer_threads: 4,
        reader_threads: 4,
        mixed_threads: 2,
        transaction_threads: 1,
        duration: Duration::from_secs(5),
        key_space_size: 1_000,
        value_size_range: (10_000, 50_000), // Large values
        enable_chaos: false,
        cache_size: 5 * 1024 * 1024, // Small cache
        enable_compression: true,
        transaction_size: 5,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // Memory pressure should not cause data corruption
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations under memory pressure"
    );
    assert!(
        results.successful_operations > 0,
        "No successful operations under memory pressure"
    );
}

#[test]
fn test_chaos_resilience() {
    let config = StressTestConfig {
        writer_threads: 6,
        reader_threads: 6,
        mixed_threads: 3,
        transaction_threads: 2,
        duration: Duration::from_secs(5),
        key_space_size: 5_000,
        value_size_range: (100, 2_000),
        enable_chaos: true, // Enable chaos injection
        cache_size: 20 * 1024 * 1024,
        enable_compression: true,
        transaction_size: 5,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // Even with chaos, maintain consistency
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations under chaos"
    );

    // Allow lower success rate under chaos but should still be reasonable
    let success_rate = results.successful_operations as f64 / results.total_operations as f64;
    assert!(
        success_rate > 0.9,
        "Success rate too low under chaos: {:.2}%",
        success_rate * 100.0
    );
}

#[test]
#[ignore] // Long-running test
fn test_extended_stress() {
    let config = StressTestConfig {
        writer_threads: 32,
        reader_threads: 32,
        mixed_threads: 16,
        transaction_threads: 8,
        duration: Duration::from_secs(300), // 5 minutes
        key_space_size: 100_000,
        value_size_range: (100, 10_000),
        enable_chaos: true,
        cache_size: 200 * 1024 * 1024,
        enable_compression: true,
        transaction_size: 10,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // Extended stress test assertions
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations in extended test"
    );
    assert!(
        results.operations_per_second > 1000.0,
        "Performance too low in extended test"
    );

    let success_rate = results.successful_operations as f64 / results.total_operations as f64;
    assert!(
        success_rate > 0.95,
        "Success rate below 95% in extended test"
    );
}

#[test]
fn test_rapid_key_updates() {
    // Test scenario where same keys are rapidly updated by multiple threads
    let config = StressTestConfig {
        writer_threads: 20,
        reader_threads: 10,
        mixed_threads: 0,
        transaction_threads: 0,
        duration: Duration::from_secs(5),
        key_space_size: 10, // Very small key space to force contention
        value_size_range: (100, 100),
        enable_chaos: false,
        cache_size: 10 * 1024 * 1024,
        enable_compression: false,
        transaction_size: 0,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    print_results(&results);

    // High contention should not cause consistency issues
    assert_eq!(
        results.consistency_violations, 0,
        "Consistency violations with rapid updates"
    );
    assert_eq!(
        results.data_races_detected, 0,
        "Data races with rapid updates"
    );
}

// Helper test to verify the stress testing framework itself
#[test]
fn test_stress_framework_sanity() {
    // Minimal configuration to verify framework works
    let config = StressTestConfig {
        writer_threads: 1,
        reader_threads: 1,
        mixed_threads: 0,
        transaction_threads: 0,
        duration: Duration::from_secs(1),
        key_space_size: 100,
        value_size_range: (10, 100),
        enable_chaos: false,
        cache_size: 1024 * 1024,
        enable_compression: false,
        transaction_size: 0,
    };

    let framework = StressTestFramework::new(config);
    let results = framework.run();

    // Basic sanity checks
    assert!(results.total_operations > 0, "No operations performed");
    assert!(
        results.successful_operations > 0,
        "No successful operations"
    );
    assert!(
        results.average_latency_us > 0.0,
        "Invalid latency measurement"
    );
    assert!(
        results.operations_per_second > 0.0,
        "Invalid throughput measurement"
    );
}

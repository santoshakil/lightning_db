use lightning_db::{Database, LightningDbConfig};
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Performance baseline thresholds (in operations per second)
/// These should be calibrated based on your hardware and requirements
#[derive(Debug)]
struct PerformanceBaseline {
    sequential_write_ops_per_sec: f64,
    random_write_ops_per_sec: f64,
    sequential_read_ops_per_sec: f64,
    #[allow(dead_code)]
    random_read_ops_per_sec: f64,
    mixed_workload_ops_per_sec: f64,
    transaction_commits_per_sec: f64,
    large_value_write_mb_per_sec: f64,
    checkpoint_time_ms: u128,
}

/// Default performance baselines (conservative estimates)
impl Default for PerformanceBaseline {
    fn default() -> Self {
        Self {
            sequential_write_ops_per_sec: 50_000.0,    // 50K ops/sec
            random_write_ops_per_sec: 30_000.0,        // 30K ops/sec
            sequential_read_ops_per_sec: 15_000.0,     // 15K ops/sec (realistic baseline)
            random_read_ops_per_sec: 80_000.0,         // 80K ops/sec
            mixed_workload_ops_per_sec: 40_000.0,      // 40K ops/sec
            transaction_commits_per_sec: 5_000.0,      // 5K tx/sec
            large_value_write_mb_per_sec: 100.0,       // 100MB/sec
            checkpoint_time_ms: 1000,                  // 1 second
        }
    }
}

#[derive(Debug)]
struct PerformanceResult {
    #[allow(dead_code)]
    test_name: String,
    ops_per_sec: f64,
    latency_p50: Duration,
    latency_p95: Duration,
    latency_p99: Duration,
    total_duration: Duration,
}

/// Run a performance test and measure results
fn run_performance_test<F>(
    test_name: &str,
    num_operations: usize,
    mut operation: F,
) -> PerformanceResult 
where 
    F: FnMut(usize) -> Duration,
{
    let mut latencies = Vec::with_capacity(num_operations);
    let start = Instant::now();
    
    for i in 0..num_operations {
        let op_duration = operation(i);
        latencies.push(op_duration);
    }
    
    let total_duration = start.elapsed();
    
    // Sort latencies for percentile calculation
    latencies.sort();
    
    let p50_idx = latencies.len() / 2;
    let p95_idx = (latencies.len() as f64 * 0.95) as usize;
    let p99_idx = (latencies.len() as f64 * 0.99) as usize;
    
    PerformanceResult {
        test_name: test_name.to_string(),
        ops_per_sec: num_operations as f64 / total_duration.as_secs_f64(),
        latency_p50: latencies[p50_idx],
        latency_p95: latencies[p95_idx.min(latencies.len() - 1)],
        latency_p99: latencies[p99_idx.min(latencies.len() - 1)],
        total_duration,
    }
}

/// Test sequential write performance
#[test]
fn test_sequential_write_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB cache
        write_batch_size: 1000,
        ..Default::default()
    };
    
    let db = Database::open(dir.path(), config).unwrap();
    let num_operations = 100_000;
    
    let result = run_performance_test("Sequential Write", num_operations, |i| {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        
        let start = Instant::now();
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        start.elapsed()
    });
    
    println!("Sequential Write Performance:");
    println!("  Operations/sec: {:.2}", result.ops_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        result.ops_per_sec >= baseline.sequential_write_ops_per_sec * 0.8,
        "Sequential write performance regression detected: {:.2} ops/sec (expected >= {:.2})",
        result.ops_per_sec,
        baseline.sequential_write_ops_per_sec * 0.8
    );
}

/// Test random write performance
#[test]
fn test_random_write_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    let num_operations = 50_000;
    
    let result = run_performance_test("Random Write", num_operations, |i| {
        let key_id = rand::random::<u32>() % 1_000_000;
        let key = format!("key_{:08}", key_id);
        let value = format!("value_{:08}_{}", key_id, i);
        
        let start = Instant::now();
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        start.elapsed()
    });
    
    println!("Random Write Performance:");
    println!("  Operations/sec: {:.2}", result.ops_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        result.ops_per_sec >= baseline.random_write_ops_per_sec * 0.8,
        "Random write performance regression detected: {:.2} ops/sec (expected >= {:.2})",
        result.ops_per_sec,
        baseline.random_write_ops_per_sec * 0.8
    );
}

/// Test sequential read performance
#[test]
fn test_sequential_read_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Prepopulate data
    let num_keys = 100_000;
    for i in 0..num_keys {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Force data to disk
    db.checkpoint().unwrap();
    
    let result = run_performance_test("Sequential Read", num_keys, |i| {
        let key = format!("key_{:08}", i);
        
        let start = Instant::now();
        let _ = db.get(key.as_bytes()).unwrap();
        start.elapsed()
    });
    
    println!("Sequential Read Performance:");
    println!("  Operations/sec: {:.2}", result.ops_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        result.ops_per_sec >= baseline.sequential_read_ops_per_sec * 0.8,
        "Sequential read performance regression detected: {:.2} ops/sec (expected >= {:.2})",
        result.ops_per_sec,
        baseline.sequential_read_ops_per_sec * 0.8
    );
}

/// Test transaction commit performance
#[test]
fn test_transaction_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    let num_transactions = 1000;
    let ops_per_transaction = 10;
    
    let result = run_performance_test("Transaction Commits", num_transactions, |i| {
        let start = Instant::now();
        
        let tx_id = db.begin_transaction().unwrap();
        
        // Perform multiple operations in transaction
        for j in 0..ops_per_transaction {
            let key = format!("tx_{}_key_{}", i, j);
            let value = format!("tx_{}_value_{}", i, j);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.commit_transaction(tx_id).unwrap();
        
        start.elapsed()
    });
    
    println!("Transaction Performance:");
    println!("  Transactions/sec: {:.2}", result.ops_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        result.ops_per_sec >= baseline.transaction_commits_per_sec * 0.8,
        "Transaction performance regression detected: {:.2} tx/sec (expected >= {:.2})",
        result.ops_per_sec,
        baseline.transaction_commits_per_sec * 0.8
    );
}

/// Test large value write performance
#[test]
fn test_large_value_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    let value_size = 1_000_000; // 1MB values
    let num_values = 100;
    let large_value = vec![0xAB; value_size];
    
    let result = run_performance_test("Large Value Writes", num_values, |i| {
        let key = format!("large_key_{:04}", i);
        
        let start = Instant::now();
        db.put(key.as_bytes(), &large_value).unwrap();
        start.elapsed()
    });
    
    let mb_written = (num_values * value_size) as f64 / 1_000_000.0;
    let mb_per_sec = mb_written / result.total_duration.as_secs_f64();
    
    println!("Large Value Write Performance:");
    println!("  MB/sec: {:.2}", mb_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        mb_per_sec >= baseline.large_value_write_mb_per_sec * 0.8,
        "Large value write performance regression detected: {:.2} MB/sec (expected >= {:.2})",
        mb_per_sec,
        baseline.large_value_write_mb_per_sec * 0.8
    );
}

/// Test mixed workload performance
#[test]
fn test_mixed_workload_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Prepopulate some data
    for i in 0..10_000 {
        let key = format!("base_key_{:06}", i);
        let value = format!("base_value_{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let num_operations = 50_000;
    
    let result = run_performance_test("Mixed Workload", num_operations, |i| {
        let operation_type = i % 10;
        
        let start = Instant::now();
        
        match operation_type {
            0..=4 => {
                // 50% reads
                let key_id = rand::random::<u32>() % 10_000;
                let key = format!("base_key_{:06}", key_id);
                let _ = db.get(key.as_bytes()).unwrap();
            }
            5..=7 => {
                // 30% writes
                let key = format!("new_key_{:08}", i);
                let value = format!("new_value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            8 => {
                // 10% updates
                let key_id = rand::random::<u32>() % 10_000;
                let key = format!("base_key_{:06}", key_id);
                let value = format!("updated_value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
            9 => {
                // 10% deletes
                let key_id = rand::random::<u32>() % 10_000;
                let key = format!("base_key_{:06}", key_id);
                let _ = db.delete(key.as_bytes());
            }
            _ => unreachable!()
        }
        
        start.elapsed()
    });
    
    println!("Mixed Workload Performance:");
    println!("  Operations/sec: {:.2}", result.ops_per_sec);
    println!("  P50 latency: {:?}", result.latency_p50);
    println!("  P95 latency: {:?}", result.latency_p95);
    println!("  P99 latency: {:?}", result.latency_p99);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        result.ops_per_sec >= baseline.mixed_workload_ops_per_sec * 0.8,
        "Mixed workload performance regression detected: {:.2} ops/sec (expected >= {:.2})",
        result.ops_per_sec,
        baseline.mixed_workload_ops_per_sec * 0.8
    );
}

/// Test checkpoint performance
#[test]
fn test_checkpoint_performance() {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path(), LightningDbConfig::default()).unwrap();
    
    // Write significant amount of data
    for i in 0..50_000 {
        let key = format!("checkpoint_key_{:08}", i);
        let value = format!("checkpoint_value_{:08}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Measure checkpoint time
    let start = Instant::now();
    db.checkpoint().unwrap();
    let checkpoint_duration = start.elapsed();
    
    println!("Checkpoint Performance:");
    println!("  Checkpoint time: {:?}", checkpoint_duration);
    
    let baseline = PerformanceBaseline::default();
    assert!(
        checkpoint_duration.as_millis() <= baseline.checkpoint_time_ms,
        "Checkpoint performance regression detected: {:?} (expected <= {}ms)",
        checkpoint_duration,
        baseline.checkpoint_time_ms
    );
}

/// Test cache effectiveness
#[test]
fn test_cache_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024, // 50MB cache
        ..Default::default()
    };
    
    let db = Database::open(dir.path(), config).unwrap();
    
    // Write data that exceeds cache size
    let num_keys = 100_000;
    for i in 0..num_keys {
        let key = format!("cache_key_{:08}", i);
        let value = vec![0xCC; 1000]; // 1KB values
        db.put(key.as_bytes(), &value).unwrap();
    }
    
    // Force to disk
    db.checkpoint().unwrap();
    
    // Measure hot cache performance (repeated access to same keys)
    let hot_keys = 1000;
    let hot_result = run_performance_test("Hot Cache Reads", hot_keys * 10, |i| {
        let key_id = i % hot_keys;
        let key = format!("cache_key_{:08}", key_id);
        
        let start = Instant::now();
        let _ = db.get(key.as_bytes()).unwrap();
        start.elapsed()
    });
    
    // Measure cold cache performance (random access)
    let cold_result = run_performance_test("Cold Cache Reads", 10_000, |_| {
        let key_id = rand::random::<u32>() % num_keys as u32;
        let key = format!("cache_key_{:08}", key_id);
        
        let start = Instant::now();
        let _ = db.get(key.as_bytes()).unwrap();
        start.elapsed()
    });
    
    println!("Cache Performance:");
    println!("  Hot cache ops/sec: {:.2}", hot_result.ops_per_sec);
    println!("  Cold cache ops/sec: {:.2}", cold_result.ops_per_sec);
    println!("  Cache speedup: {:.2}x", hot_result.ops_per_sec / cold_result.ops_per_sec);
    
    // Hot cache should be significantly faster than cold cache
    assert!(
        hot_result.ops_per_sec > cold_result.ops_per_sec * 2.0,
        "Cache not providing expected performance benefit"
    );
}

/// Run all performance tests and generate a summary report
#[test]
#[ignore] // This test runs all others, so ignore by default
fn test_performance_suite() {
    println!("=== Lightning DB Performance Regression Test Suite ===\n");
    
    let tests = vec![
        test_sequential_write_performance,
        test_random_write_performance,
        test_sequential_read_performance,
        test_transaction_performance,
        test_large_value_performance,
        test_mixed_workload_performance,
        test_checkpoint_performance,
        test_cache_performance,
    ];
    
    for test in tests {
        test();
        println!("\n---\n");
    }
    
    println!("All performance tests completed successfully!");
}
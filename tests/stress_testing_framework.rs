//! Comprehensive Stress Testing Framework
//!
//! This framework tests Lightning DB under extreme concurrent load to identify
//! race conditions, deadlocks, performance degradation, and resource exhaustion.

use lightning_db::sharding::TransactionOp;
use lightning_db::{Database, LightningDbConfig};
use parking_lot::Mutex;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Generate a random alphanumeric string of specified length
fn generate_random_alphanumeric(rng: &mut impl Rng, len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Stress test configuration
#[derive(Clone)]
pub struct StressTestConfig {
    /// Number of concurrent writer threads
    pub writer_threads: usize,
    /// Number of concurrent reader threads
    pub reader_threads: usize,
    /// Number of mixed operation threads
    pub mixed_threads: usize,
    /// Number of transaction threads
    pub transaction_threads: usize,
    /// Test duration
    pub duration: Duration,
    /// Size of key space
    pub key_space_size: usize,
    /// Value size range (min, max)
    pub value_size_range: (usize, usize),
    /// Enable chaos injection
    pub enable_chaos: bool,
    /// Cache size
    pub cache_size: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Transaction size (ops per transaction)
    pub transaction_size: usize,
}

impl Default for StressTestConfig {
    fn default() -> Self {
        Self {
            writer_threads: 16,
            reader_threads: 16,
            mixed_threads: 8,
            transaction_threads: 4,
            duration: Duration::from_secs(60),
            key_space_size: 100_000,
            value_size_range: (100, 10_000),
            enable_chaos: true,
            cache_size: 100 * 1024 * 1024, // 100MB
            enable_compression: true,
            transaction_size: 10,
        }
    }
}

/// Stress test results
#[derive(Debug, Clone)]
pub struct StressTestResults {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub operations_per_second: f64,
    pub average_latency_us: f64,
    pub p99_latency_us: f64,
    pub max_latency_us: f64,
    pub data_races_detected: u64,
    pub deadlocks_detected: u64,
    pub consistency_violations: u64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub errors: Vec<String>,
}

/// Operation type for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OperationType {
    Read,
    Write,
    Update,
    Delete,
    Transaction,
    RangeScan,
}

/// Operation result tracking
struct OperationResult {
    op_type: OperationType,
    success: bool,
    latency_us: u64,
    error: Option<String>,
}

/// Main stress testing framework
pub struct StressTestFramework {
    config: StressTestConfig,
    test_dir: TempDir,
    stop_flag: Arc<AtomicBool>,
    results: Arc<Mutex<Vec<OperationResult>>>,
    consistency_checker: Arc<ConsistencyChecker>,
    chaos_injector: Arc<ChaosInjector>,
}

impl StressTestFramework {
    pub fn new(config: StressTestConfig) -> Self {
        Self {
            config,
            test_dir: TempDir::new().unwrap(),
            stop_flag: Arc::new(AtomicBool::new(false)),
            results: Arc::new(Mutex::new(Vec::with_capacity(1_000_000))),
            consistency_checker: Arc::new(ConsistencyChecker::new()),
            chaos_injector: Arc::new(ChaosInjector::new()),
        }
    }

    /// Run the stress test
    pub fn run(&self) -> StressTestResults {
        println!("🚀 Starting stress test with configuration:");
        println!(
            "   Writers: {}, Readers: {}, Mixed: {}, Transactions: {}",
            self.config.writer_threads,
            self.config.reader_threads,
            self.config.mixed_threads,
            self.config.transaction_threads
        );
        println!("   Duration: {:?}", self.config.duration);
        println!("   Key space: {}", self.config.key_space_size);
        println!("   Chaos enabled: {}", self.config.enable_chaos);

        let db_path = self.test_dir.path().join("stress_test_db");
        let db = self.create_database(&db_path);

        // Pre-populate some data
        self.pre_populate_data(&db);

        // Start monitoring thread
        let monitor_handle = self.start_monitoring_thread(db.clone());

        // Start all test threads
        let handles = self.start_all_threads(db.clone());

        // Run for specified duration
        thread::sleep(self.config.duration);

        // Stop all threads
        self.stop_flag.store(true, Ordering::Relaxed);

        // Wait for all threads to complete
        for handle in handles {
            handle.join().ok();
        }

        // Final consistency check
        let final_violations = self.perform_final_consistency_check(&db);

        // Calculate results
        self.calculate_results(final_violations)
    }

    /// Create database with stress test configuration
    fn create_database(&self, path: &Path) -> Arc<Database> {
        let config = LightningDbConfig {
            cache_size: self.config.cache_size as u64,
            compression_enabled: self.config.enable_compression,
            use_improved_wal: true,
            wal_sync_mode: lightning_db::WalSyncMode::Async, // Faster for stress testing
            use_optimized_transactions: true,
            ..Default::default()
        };

        Arc::new(Database::create(path, config).unwrap())
    }

    /// Pre-populate database with initial data
    fn pre_populate_data(&self, db: &Arc<Database>) {
        println!("Pre-populating database...");
        let mut rng = rand::rng();

        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            let value_size =
                rng.random_range(self.config.value_size_range.0..self.config.value_size_range.1);
            let value = generate_random_alphanumeric(&mut rng, value_size);

            db.put(key.as_bytes(), value.as_bytes()).ok();

            // Track initial state for consistency checking
            self.consistency_checker.record_write(key, value);
        }

        db.sync().unwrap();
        println!("Pre-population complete");
    }

    /// Start all test threads
    fn start_all_threads(&self, db: Arc<Database>) -> Vec<thread::JoinHandle<()>> {
        let mut handles = vec![];
        let barrier = Arc::new(Barrier::new(
            self.config.writer_threads
                + self.config.reader_threads
                + self.config.mixed_threads
                + self.config.transaction_threads
                + 1,
        ));

        // Writer threads
        for thread_id in 0..self.config.writer_threads {
            let handle = self.start_writer_thread(thread_id, db.clone(), barrier.clone());
            handles.push(handle);
        }

        // Reader threads
        for thread_id in 0..self.config.reader_threads {
            let handle = self.start_reader_thread(thread_id, db.clone(), barrier.clone());
            handles.push(handle);
        }

        // Mixed operation threads
        for thread_id in 0..self.config.mixed_threads {
            let handle = self.start_mixed_thread(thread_id, db.clone(), barrier.clone());
            handles.push(handle);
        }

        // Transaction threads
        for thread_id in 0..self.config.transaction_threads {
            let handle = self.start_transaction_thread(thread_id, db.clone(), barrier.clone());
            handles.push(handle);
        }

        // Start all threads simultaneously
        barrier.wait();

        handles
    }

    /// Start writer thread
    fn start_writer_thread(
        &self,
        thread_id: usize,
        db: Arc<Database>,
        barrier: Arc<Barrier>,
    ) -> thread::JoinHandle<()> {
        let stop_flag = self.stop_flag.clone();
        let results = self.results.clone();
        let consistency_checker = self.consistency_checker.clone();
        let chaos_injector = self.chaos_injector.clone();
        let config = self.config.clone();

        thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::rng();

            while !stop_flag.load(Ordering::Relaxed) {
                // Inject chaos if enabled
                if config.enable_chaos {
                    chaos_injector.maybe_inject_delay();
                }

                let key_num = rng.random_range(0..config.key_space_size);
                let key = format!("key_{:06}", key_num);
                let value_size =
                    rng.random_range(config.value_size_range.0..config.value_size_range.1);
                let value = generate_random_alphanumeric(&mut rng, value_size);

                let start = Instant::now();
                let result = db.put(key.as_bytes(), value.as_bytes());
                let latency_us = start.elapsed().as_micros() as u64;

                let op_result = OperationResult {
                    op_type: OperationType::Write,
                    success: result.is_ok(),
                    latency_us,
                    error: result.err().map(|e| e.to_string()),
                };

                if op_result.success {
                    consistency_checker.record_write(key, value);
                }

                results.lock().push(op_result);

                // Occasionally yield to increase concurrency
                if key_num % 100 == 0 {
                    thread::yield_now();
                }
            }
        })
    }

    /// Start reader thread
    fn start_reader_thread(
        &self,
        thread_id: usize,
        db: Arc<Database>,
        barrier: Arc<Barrier>,
    ) -> thread::JoinHandle<()> {
        let stop_flag = self.stop_flag.clone();
        let results = self.results.clone();
        let consistency_checker = self.consistency_checker.clone();
        let chaos_injector = self.chaos_injector.clone();
        let config = self.config.clone();

        thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::rng();

            while !stop_flag.load(Ordering::Relaxed) {
                if config.enable_chaos {
                    chaos_injector.maybe_inject_delay();
                }

                let key_num = rng.random_range(0..config.key_space_size);
                let key = format!("key_{:06}", key_num);

                let start = Instant::now();
                let result = db.get(key.as_bytes());
                let latency_us = start.elapsed().as_micros() as u64;

                let mut success = false;
                let mut error = None;

                match result {
                    Ok(Some(value)) => {
                        success = true;
                        // Verify consistency
                        let value_str = String::from_utf8_lossy(&value).to_string();
                        consistency_checker.verify_read(&key, &value_str);
                    }
                    Ok(None) => {
                        success = true; // Key not found is valid
                    }
                    Err(e) => {
                        error = Some(e.to_string());
                    }
                }

                let op_result = OperationResult {
                    op_type: OperationType::Read,
                    success,
                    latency_us,
                    error,
                };

                results.lock().push(op_result);
            }
        })
    }

    /// Start mixed operation thread
    fn start_mixed_thread(
        &self,
        thread_id: usize,
        db: Arc<Database>,
        barrier: Arc<Barrier>,
    ) -> thread::JoinHandle<()> {
        let stop_flag = self.stop_flag.clone();
        let results = self.results.clone();
        let consistency_checker = self.consistency_checker.clone();
        let chaos_injector = self.chaos_injector.clone();
        let config = self.config.clone();

        thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::rng();

            while !stop_flag.load(Ordering::Relaxed) {
                if config.enable_chaos {
                    chaos_injector.maybe_inject_delay();
                }

                let op_choice = rng.random_range(0..100);

                if op_choice < 30 {
                    // Write operation
                    let key_num = rng.random_range(0..config.key_space_size);
                    let key = format!("key_{:06}", key_num);
                    let value_size =
                        rng.random_range(config.value_size_range.0..config.value_size_range.1);
                    let value = generate_random_alphanumeric(&mut rng, value_size);

                    let start = Instant::now();
                    let result = db.put(key.as_bytes(), value.as_bytes());
                    let latency_us = start.elapsed().as_micros() as u64;

                    if result.is_ok() {
                        consistency_checker.record_write(key, value);
                    }

                    results.lock().push(OperationResult {
                        op_type: OperationType::Write,
                        success: result.is_ok(),
                        latency_us,
                        error: result.err().map(|e| e.to_string()),
                    });
                } else if op_choice < 60 {
                    // Read operation
                    let key_num = rng.random_range(0..config.key_space_size);
                    let key = format!("key_{:06}", key_num);

                    let start = Instant::now();
                    let result = db.get(key.as_bytes());
                    let latency_us = start.elapsed().as_micros() as u64;

                    let (success, error) = match result {
                        Ok(Some(value)) => {
                            let value_str = String::from_utf8_lossy(&value).to_string();
                            consistency_checker.verify_read(&key, &value_str);
                            (true, None)
                        }
                        Ok(None) => (true, None),
                        Err(e) => (false, Some(e.to_string())),
                    };

                    results.lock().push(OperationResult {
                        op_type: OperationType::Read,
                        success,
                        latency_us,
                        error,
                    });
                } else if op_choice < 80 {
                    // Update operation
                    let key_num = rng.random_range(0..config.key_space_size);
                    let key = format!("key_{:06}", key_num);

                    // Read-modify-write pattern
                    let start = Instant::now();
                    let mut success = false;
                    let mut error = None;

                    if let Ok(Some(old_value)) = db.get(key.as_bytes()) {
                        let new_value = format!(
                            "{}_updated_{}",
                            String::from_utf8_lossy(&old_value),
                            thread_id
                        );

                        match db.put(key.as_bytes(), new_value.as_bytes()) {
                            Ok(_) => {
                                success = true;
                                consistency_checker.record_write(key, new_value);
                            }
                            Err(e) => {
                                error = Some(e.to_string());
                            }
                        }
                    } else {
                        success = true; // Key not found is valid
                    }

                    let latency_us = start.elapsed().as_micros() as u64;

                    results.lock().push(OperationResult {
                        op_type: OperationType::Update,
                        success,
                        latency_us,
                        error,
                    });
                } else if op_choice < 90 {
                    // Delete operation
                    let key_num = rng.random_range(0..config.key_space_size);
                    let key = format!("key_{:06}", key_num);

                    let start = Instant::now();
                    let result = db.delete(key.as_bytes());
                    let latency_us = start.elapsed().as_micros() as u64;

                    if result.is_ok() {
                        consistency_checker.record_delete(key);
                    }

                    results.lock().push(OperationResult {
                        op_type: OperationType::Delete,
                        success: result.is_ok(),
                        latency_us,
                        error: result.err().map(|e| e.to_string()),
                    });
                } else {
                    // Range scan operation
                    let start_key = format!(
                        "key_{:06}",
                        rng.random_range(0..config.key_space_size - 100)
                    );
                    let end_key = format!(
                        "key_{:06}",
                        rng.random_range(0..100) + config.key_space_size - 100
                    );

                    let start = Instant::now();
                    let result = db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes()));
                    let latency_us = start.elapsed().as_micros() as u64;

                    results.lock().push(OperationResult {
                        op_type: OperationType::RangeScan,
                        success: result.is_ok(),
                        latency_us,
                        error: result.err().map(|e| e.to_string()),
                    });
                }
            }
        })
    }

    /// Start transaction thread
    fn start_transaction_thread(
        &self,
        thread_id: usize,
        db: Arc<Database>,
        barrier: Arc<Barrier>,
    ) -> thread::JoinHandle<()> {
        let stop_flag = self.stop_flag.clone();
        let results = self.results.clone();
        let consistency_checker = self.consistency_checker.clone();
        let chaos_injector = self.chaos_injector.clone();
        let config = self.config.clone();

        thread::spawn(move || {
            barrier.wait();
            let mut rng = rand::rng();

            while !stop_flag.load(Ordering::Relaxed) {
                if config.enable_chaos {
                    chaos_injector.maybe_inject_delay();
                }

                let start = Instant::now();
                let mut success = false;
                let mut error = None;

                // Start transaction
                match db.begin_transaction() {
                    Ok(tx_id) => {
                        let mut tx_ops = vec![];

                        // Perform multiple operations in transaction
                        for _ in 0..config.transaction_size {
                            let key_num = rng.random_range(0..config.key_space_size);
                            let key = format!("key_{:06}", key_num);
                            let value = generate_random_alphanumeric(&mut rng, 100);

                            if rng.gen_bool(0.7) {
                                // Write operation
                                if db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).is_ok() {
                                    tx_ops.push((key, Some(value)));
                                }
                            } else {
                                // Delete operation
                                if db.delete_tx(tx_id, key.as_bytes()).is_ok() {
                                    tx_ops.push((key, None));
                                }
                            }
                        }

                        // Attempt to commit
                        match db.commit_transaction(tx_id) {
                            Ok(_) => {
                                success = true;
                                // Record all operations as committed
                                for (key, value) in tx_ops {
                                    if let Some(val) = value {
                                        consistency_checker.record_write(key, val);
                                    } else {
                                        consistency_checker.record_delete(key);
                                    }
                                }
                            }
                            Err(e) => {
                                error = Some(format!("Transaction commit failed: {}", e));
                                // Rollback
                                let _ = db.abort_transaction(tx_id);
                            }
                        }
                    }
                    Err(e) => {
                        error = Some(format!("Transaction begin failed: {}", e));
                    }
                }

                let latency_us = start.elapsed().as_micros() as u64;

                results.lock().push(OperationResult {
                    op_type: OperationType::Transaction,
                    success,
                    latency_us,
                    error,
                });
            }
        })
    }

    /// Start monitoring thread
    fn start_monitoring_thread(&self, db: Arc<Database>) -> thread::JoinHandle<()> {
        let stop_flag = self.stop_flag.clone();
        let consistency_checker = self.consistency_checker.clone();

        thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5));

                // Periodic consistency check
                let violations = consistency_checker.check_consistency(&db);
                if violations > 0 {
                    println!("⚠️  Consistency violations detected: {}", violations);
                }

                // Memory usage check
                // Note: In production, use proper memory tracking
                println!("📊 Periodic health check completed");
            }
        })
    }

    /// Perform final consistency check
    fn perform_final_consistency_check(&self, db: &Arc<Database>) -> u64 {
        println!("Performing final consistency check...");
        self.consistency_checker.check_consistency(db)
    }

    /// Calculate test results
    fn calculate_results(&self, consistency_violations: u64) -> StressTestResults {
        let results = self.results.lock();

        let total_operations = results.len() as u64;
        let successful_operations = results.iter().filter(|r| r.success).count() as u64;
        let failed_operations = total_operations - successful_operations;

        // Calculate latencies
        let mut latencies: Vec<u64> = results.iter().map(|r| r.latency_us).collect();
        latencies.sort_unstable();

        let average_latency_us = if !latencies.is_empty() {
            latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
        } else {
            0.0
        };

        let p99_latency_us = if !latencies.is_empty() {
            latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)] as f64
        } else {
            0.0
        };

        let max_latency_us = latencies.last().copied().unwrap_or(0) as f64;

        // Operations per second
        let operations_per_second = total_operations as f64 / self.config.duration.as_secs_f64();

        // Collect errors
        let errors: Vec<String> = results
            .iter()
            .filter_map(|r| r.error.clone())
            .take(100) // Limit to first 100 errors
            .collect();

        // Count specific error types
        let data_races_detected = errors
            .iter()
            .filter(|e| e.contains("race") || e.contains("concurrent"))
            .count() as u64;

        let deadlocks_detected = errors
            .iter()
            .filter(|e| e.contains("deadlock") || e.contains("timeout"))
            .count() as u64;

        StressTestResults {
            total_operations,
            successful_operations,
            failed_operations,
            operations_per_second,
            average_latency_us,
            p99_latency_us,
            max_latency_us,
            data_races_detected,
            deadlocks_detected,
            consistency_violations,
            memory_usage_mb: 0.0,   // Would need actual memory tracking
            cpu_usage_percent: 0.0, // Would need actual CPU tracking
            errors,
        }
    }
}

/// Consistency checker for verifying data integrity
struct ConsistencyChecker {
    expected_state: Arc<RwLock<HashMap<String, Option<String>>>>,
}

impl ConsistencyChecker {
    fn new() -> Self {
        Self {
            expected_state: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn record_write(&self, key: String, value: String) {
        self.expected_state
            .write()
            .unwrap()
            .insert(key, Some(value));
    }

    fn record_delete(&self, key: String) {
        self.expected_state.write().unwrap().insert(key, None);
    }

    fn verify_read(&self, key: &str, value: &str) -> bool {
        let state = self.expected_state.read().unwrap();
        match state.get(key) {
            Some(Some(expected)) => expected == value,
            Some(None) => false, // Key should be deleted
            None => true,        // Unknown key, could be valid
        }
    }

    fn check_consistency(&self, db: &Database) -> u64 {
        let state = self.expected_state.read().unwrap();
        let mut violations = 0;

        for (key, expected_value) in state.iter() {
            match (db.get(key.as_bytes()), expected_value) {
                (Ok(Some(actual)), Some(expected)) => {
                    let actual_str = String::from_utf8_lossy(&actual);
                    if actual_str != *expected {
                        violations += 1;
                    }
                }
                (Ok(None), None) => {
                    // Correctly deleted
                }
                (Ok(Some(_)), None) => {
                    // Should be deleted but exists
                    violations += 1;
                }
                (Ok(None), Some(_)) => {
                    // Should exist but doesn't
                    violations += 1;
                }
                (Err(_), _) => {
                    // Read error
                    violations += 1;
                }
            }
        }

        violations
    }
}

/// Chaos injector for simulating adverse conditions
struct ChaosInjector {
    enabled: AtomicBool,
}

impl ChaosInjector {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
        }
    }

    fn maybe_inject_delay(&self) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let mut rng = rand::rng();

        // 1% chance of delay
        if rng.gen_bool(0.01) {
            let delay_ms = rng.random_range(1..50);
            thread::sleep(Duration::from_millis(delay_ms));
        }

        // 0.1% chance of long delay
        if rng.gen_bool(0.001) {
            let delay_ms = rng.random_range(100..500);
            thread::sleep(Duration::from_millis(delay_ms));
        }
    }
}

/// Print test results
pub fn print_results(results: &StressTestResults) {
    println!("\n=== Stress Test Results ===\n");

    println!("📊 Operations:");
    println!("   Total: {}", results.total_operations);
    println!(
        "   Successful: {} ({:.2}%)",
        results.successful_operations,
        (results.successful_operations as f64 / results.total_operations as f64) * 100.0
    );
    println!("   Failed: {}", results.failed_operations);
    println!("   Ops/sec: {:.0}", results.operations_per_second);

    println!("\n⏱️  Latency:");
    println!("   Average: {:.0} μs", results.average_latency_us);
    println!("   P99: {:.0} μs", results.p99_latency_us);
    println!("   Max: {:.0} μs", results.max_latency_us);

    println!("\n🔍 Integrity:");
    println!("   Data races detected: {}", results.data_races_detected);
    println!("   Deadlocks detected: {}", results.deadlocks_detected);
    println!(
        "   Consistency violations: {}",
        results.consistency_violations
    );

    if !results.errors.is_empty() {
        println!("\n❌ Sample errors:");
        for (i, error) in results.errors.iter().take(5).enumerate() {
            println!("   {}: {}", i + 1, error);
        }
    }

    let status = if results.consistency_violations == 0
        && results.data_races_detected == 0
        && results.deadlocks_detected == 0
    {
        "✅ PASSED"
    } else {
        "❌ FAILED"
    };

    println!("\n📋 Overall Status: {}", status);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_stress_test() {
        let config = StressTestConfig {
            writer_threads: 4,
            reader_threads: 4,
            mixed_threads: 2,
            transaction_threads: 1,
            duration: Duration::from_secs(5),
            key_space_size: 1000,
            ..Default::default()
        };

        let framework = StressTestFramework::new(config);
        let results = framework.run();

        print_results(&results);

        assert!(results.total_operations > 0);
        assert!(results.successful_operations > 0);
        assert_eq!(results.consistency_violations, 0);
    }

    #[test]
    #[ignore] // Long running test
    fn test_extreme_stress() {
        let config = StressTestConfig {
            writer_threads: 32,
            reader_threads: 32,
            mixed_threads: 16,
            transaction_threads: 8,
            duration: Duration::from_secs(300), // 5 minutes
            key_space_size: 1_000_000,
            value_size_range: (100, 100_000),
            enable_chaos: true,
            ..Default::default()
        };

        let framework = StressTestFramework::new(config);
        let results = framework.run();

        print_results(&results);

        // In extreme stress, we allow some failures but no consistency violations
        assert_eq!(results.consistency_violations, 0);
        assert!(results.successful_operations as f64 / results.total_operations as f64 > 0.95);
    }
}

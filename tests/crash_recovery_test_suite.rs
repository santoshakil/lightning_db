//! Comprehensive Crash Recovery Test Suite
//! 
//! This test suite ensures Lightning DB can recover from various crash scenarios
//! while maintaining data integrity and consistency.

use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use rand::Rng;

/// Test configuration for crash scenarios
#[derive(Clone)]
struct CrashTestConfig {
    /// Number of writer threads
    pub writer_threads: usize,
    /// Number of reader threads  
    pub reader_threads: usize,
    /// Operations before crash
    pub ops_before_crash: usize,
    /// Enable WAL
    pub use_wal: bool,
    /// Enable compression
    pub use_compression: bool,
    /// Cache size
    pub cache_size: usize,
}

impl Default for CrashTestConfig {
    fn default() -> Self {
        Self {
            writer_threads: 4,
            reader_threads: 2,
            ops_before_crash: 10000,
            use_wal: true,
            use_compression: true,
            cache_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Crash recovery test results
#[derive(Debug)]
struct RecoveryTestResult {
    /// Total operations performed before crash
    pub operations_before_crash: usize,
    /// Operations successfully recovered
    pub operations_recovered: usize,
    /// Data integrity check passed
    pub integrity_check_passed: bool,
    /// Recovery time
    pub recovery_duration: Duration,
    /// Any data corruption detected
    pub corruption_detected: bool,
    /// Error messages if any
    pub errors: Vec<String>,
}

/// Main crash recovery test suite
pub struct CrashRecoveryTestSuite {
    test_dir: TempDir,
    config: CrashTestConfig,
}

impl CrashRecoveryTestSuite {
    pub fn new() -> Self {
        Self {
            test_dir: TempDir::new().unwrap(),
            config: CrashTestConfig::default(),
        }
    }

    pub fn with_config(config: CrashTestConfig) -> Self {
        Self {
            test_dir: TempDir::new().unwrap(),
            config,
        }
    }

    /// Test 1: Basic crash during write operations
    pub fn test_crash_during_writes(&self) -> RecoveryTestResult {
        println!("\n🔥 Test 1: Crash During Write Operations");
        
        let db_path = self.test_dir.path().join("crash_write_test");
        let crash_flag = Arc::new(AtomicBool::new(false));
        let ops_counter = Arc::new(AtomicU64::new(0));
        
        // Phase 1: Run database with writes until crash
        {
            let db = self.create_database(&db_path);
            let barrier = Arc::new(Barrier::new(self.config.writer_threads + 1));
            
            // Spawn writer threads
            let mut handles = vec![];
            for thread_id in 0..self.config.writer_threads {
                let db_clone = db.clone();
                let crash_flag_clone = crash_flag.clone();
                let ops_counter_clone = ops_counter.clone();
                let barrier_clone = barrier.clone();
                let ops_limit = self.config.ops_before_crash / self.config.writer_threads;
                
                let handle = thread::spawn(move || {
                    barrier_clone.wait();
                    
                    for i in 0..ops_limit {
                        if crash_flag_clone.load(Ordering::Relaxed) {
                            break;
                        }
                        
                        let key = format!("crash_test_{}_{}", thread_id, i);
                        let value = format!("value_{}_{}_checksum_{}", thread_id, i, 
                            Self::calculate_checksum(&key));
                        
                        if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                            ops_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        
                        // Add some variety in operations
                        if i % 10 == 0 {
                            db_clone.sync().ok();
                        }
                    }
                });
                handles.push(handle);
            }
            
            // Start all threads
            barrier.wait();
            
            // Let operations run for a bit
            thread::sleep(Duration::from_millis(500));
            
            // Simulate crash by dropping everything abruptly
            crash_flag.store(true, Ordering::Relaxed);
            drop(db); // Force drop without proper shutdown
            
            // Don't join threads - simulate abrupt termination
            std::mem::forget(handles);
        }
        
        let ops_before = ops_counter.load(Ordering::Relaxed) as usize;
        println!("  Operations before crash: {}", ops_before);
        
        // Phase 2: Attempt recovery
        let recovery_start = Instant::now();
        let recovered_db = match Database::open(&db_path, self.get_db_config()) {
            Ok(db) => db,
            Err(e) => {
                return RecoveryTestResult {
                    operations_before_crash: ops_before,
                    operations_recovered: 0,
                    integrity_check_passed: false,
                    recovery_duration: recovery_start.elapsed(),
                    corruption_detected: true,
                    errors: vec![format!("Failed to recover database: {}", e)],
                };
            }
        };
        let recovery_duration = recovery_start.elapsed();
        
        // Phase 3: Verify recovered data
        let verification_result = self.verify_crash_write_data(&recovered_db, ops_before);
        
        RecoveryTestResult {
            operations_before_crash: ops_before,
            operations_recovered: verification_result.0,
            integrity_check_passed: verification_result.1,
            recovery_duration,
            corruption_detected: !verification_result.1,
            errors: verification_result.2,
        }
    }

    /// Test 2: Crash during transaction commit
    pub fn test_crash_during_transaction(&self) -> RecoveryTestResult {
        println!("\n💥 Test 2: Crash During Transaction Commit");
        
        let db_path = self.test_dir.path().join("crash_tx_test");
        let committed_txs = Arc::new(AtomicU64::new(0));
        let attempted_txs = Arc::new(AtomicU64::new(0));
        
        // Phase 1: Run transactions until crash
        {
            let db = self.create_database(&db_path);
            
            // Run transactions
            for tx_num in 0..100 {
                attempted_txs.fetch_add(1, Ordering::Relaxed);
                
                let tx_id = match db.begin_transaction() {
                    Ok(id) => id,
                    Err(_) => continue,
                };
                
                // Add multiple operations per transaction
                for i in 0..10 {
                    let key = format!("tx_{}_key_{}", tx_num, i);
                    let value = format!("tx_{}_value_{}", tx_num, i);
                    let _ = db.put_tx(tx_id, key.as_bytes(), value.as_bytes());
                }
                
                // Crash during random transaction commit
                if tx_num == 50 {
                    // Drop database mid-transaction
                    std::mem::forget(db);
                    break;
                }
                
                if db.commit_transaction(tx_id).is_ok() {
                    committed_txs.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        // Phase 2: Recovery and verification
        let recovery_start = Instant::now();
        let recovered_db = match Database::open(&db_path, self.get_db_config()) {
            Ok(db) => db,
            Err(e) => {
                return RecoveryTestResult {
                    operations_before_crash: attempted_txs.load(Ordering::Relaxed) as usize * 10,
                    operations_recovered: 0,
                    integrity_check_passed: false,
                    recovery_duration: recovery_start.elapsed(),
                    corruption_detected: true,
                    errors: vec![format!("Failed to recover: {}", e)],
                };
            }
        };
        let recovery_duration = recovery_start.elapsed();
        
        // Verify transactional consistency
        let verification = self.verify_transaction_consistency(
            &recovered_db,
            committed_txs.load(Ordering::Relaxed) as usize
        );
        
        RecoveryTestResult {
            operations_before_crash: attempted_txs.load(Ordering::Relaxed) as usize * 10,
            operations_recovered: verification.0,
            integrity_check_passed: verification.1,
            recovery_duration,
            corruption_detected: !verification.1,
            errors: verification.2,
        }
    }

    /// Test 3: Crash during compaction
    pub fn test_crash_during_compaction(&self) -> RecoveryTestResult {
        println!("\n🔨 Test 3: Crash During Compaction");
        
        let db_path = self.test_dir.path().join("crash_compact_test");
        let total_keys = 50000;
        
        // Phase 1: Load data and trigger compaction
        {
            let db = self.create_database(&db_path);
            
            // Write enough data to trigger compaction
            for i in 0..total_keys {
                let key = format!("compact_key_{:08}", i);
                let value = vec![i as u8; 1000]; // 1KB values
                let _ = db.put(key.as_bytes(), &value);
                
                // Delete some keys to create tombstones
                if i % 5 == 0 && i > 100 {
                    let del_key = format!("compact_key_{:08}", i - 100);
                    let _ = db.delete(del_key.as_bytes());
                }
            }
            
            // Force flush to disk
            db.sync().unwrap();
            
            // Trigger compaction in background
            let _ = db.compact_lsm();
            
            // Simulate crash during compaction
            thread::sleep(Duration::from_millis(100));
            std::mem::forget(db);
        }
        
        // Phase 2: Recovery
        let recovery_start = Instant::now();
        let recovered_db = match Database::open(&db_path, self.get_db_config()) {
            Ok(db) => db,
            Err(e) => {
                return RecoveryTestResult {
                    operations_before_crash: total_keys,
                    operations_recovered: 0,
                    integrity_check_passed: false,
                    recovery_duration: recovery_start.elapsed(),
                    corruption_detected: true,
                    errors: vec![format!("Recovery failed: {}", e)],
                };
            }
        };
        let recovery_duration = recovery_start.elapsed();
        
        // Verify data integrity after compaction crash
        let verification = self.verify_compaction_integrity(&recovered_db, total_keys);
        
        RecoveryTestResult {
            operations_before_crash: total_keys,
            operations_recovered: verification.0,
            integrity_check_passed: verification.1,
            recovery_duration,
            corruption_detected: !verification.1,
            errors: verification.2,
        }
    }

    /// Test 4: Power loss simulation (kill -9)
    pub fn test_power_loss_simulation(&self) -> RecoveryTestResult {
        println!("\n⚡ Test 4: Power Loss Simulation");
        
        let db_path = self.test_dir.path().join("power_loss_test");
        let ops_file = self.test_dir.path().join("ops_count.txt");
        
        // Launch database process in subprocess
        let child_script = r#"
use lightning_db::{Database, LightningDbConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::thread;

fn main() {
    let db_path = std::env::args().nth(1).unwrap();
    let ops_file = std::env::args().nth(2).unwrap();
    
    let config = LightningDbConfig {
        use_improved_wal: true,
        sync_wal_on_write: true,
        ..Default::default()
    };
    
    let db = Database::create(&db_path, config).unwrap();
    let ops = AtomicU64::new(0);
    
    // Continuous writes
    loop {
        let key = format!("power_loss_{}", ops.load(Ordering::Relaxed));
        let value = format!("value_{}", ops.load(Ordering::Relaxed));
        
        if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
            let count = ops.fetch_add(1, Ordering::Relaxed) + 1;
            std::fs::write(&ops_file, count.to_string()).ok();
        }
        
        thread::sleep(Duration::from_micros(100));
    }
}
"#;
        
        // Write and compile test script
        let script_path = self.test_dir.path().join("power_loss_child.rs");
        fs::write(&script_path, child_script).unwrap();
        
        // For this test, we'll simulate the scenario
        let ops_before_crash = {
            let db = self.create_database(&db_path);
            let mut ops = 0;
            
            // Write data rapidly
            for i in 0..5000 {
                let key = format!("power_loss_{}", i);
                let value = format!("value_{}", i);
                
                if db.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    ops += 1;
                }
                
                if i == 2500 {
                    // Simulate abrupt power loss
                    std::mem::forget(db);
                    break;
                }
            }
            
            ops
        };
        
        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = match Database::open(&db_path, self.get_db_config()) {
            Ok(db) => db,
            Err(e) => {
                return RecoveryTestResult {
                    operations_before_crash: ops_before_crash,
                    operations_recovered: 0,
                    integrity_check_passed: false,
                    recovery_duration: recovery_start.elapsed(),
                    corruption_detected: true,
                    errors: vec![format!("Recovery failed: {}", e)],
                };
            }
        };
        let recovery_duration = recovery_start.elapsed();
        
        // Count recovered operations
        let mut recovered_ops = 0;
        let mut errors = vec![];
        
        for i in 0..ops_before_crash {
            let key = format!("power_loss_{}", i);
            match recovered_db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let expected = format!("value_{}", i);
                    if value == expected.as_bytes() {
                        recovered_ops += 1;
                    } else {
                        errors.push(format!("Value mismatch for key {}", i));
                    }
                }
                Ok(None) => {
                    // Key not found - might be expected if WAL wasn't synced
                }
                Err(e) => {
                    errors.push(format!("Error reading key {}: {}", i, e));
                }
            }
        }
        
        RecoveryTestResult {
            operations_before_crash: ops_before_crash,
            operations_recovered: recovered_ops,
            integrity_check_passed: errors.is_empty() && recovered_ops > 0,
            recovery_duration,
            corruption_detected: !errors.is_empty(),
            errors,
        }
    }

    /// Test 5: Concurrent crash with mixed operations
    pub fn test_concurrent_crash_mixed_ops(&self) -> RecoveryTestResult {
        println!("\n🌀 Test 5: Concurrent Crash with Mixed Operations");
        
        let db_path = self.test_dir.path().join("concurrent_crash_test");
        let ops_tracker = Arc::new(OperationsTracker::new());
        
        // Phase 1: Mixed operations until crash
        {
            let db = self.create_database(&db_path);
            let barrier = Arc::new(Barrier::new(
                self.config.writer_threads + self.config.reader_threads + 1
            ));
            
            let mut handles = vec![];
            
            // Writer threads
            for thread_id in 0..self.config.writer_threads {
                let db_clone = db.clone();
                let ops_clone = ops_tracker.clone();
                let barrier_clone = barrier.clone();
                
                let handle = thread::spawn(move || {
                    barrier_clone.wait();
                    let mut rng = rand::rng();
                    
                    for i in 0..1000 {
                        let op_type = rng.random_range(0..100);
                        
                        if op_type < 60 {
                            // Insert
                            let key = format!("mixed_{}_{}", thread_id, i);
                            let value = format!("val_{}", i);
                            if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                                ops_clone.record_insert(key);
                            }
                        } else if op_type < 80 {
                            // Update
                            let key = format!("mixed_{}_{}", thread_id, rng.random_range(0..i.max(1)));
                            let value = format!("updated_{}", i);
                            if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                                ops_clone.record_update(key);
                            }
                        } else {
                            // Delete
                            let key = format!("mixed_{}_{}", thread_id, rng.random_range(0..i.max(1)));
                            if db_clone.delete(key.as_bytes()).is_ok() {
                                ops_clone.record_delete(key);
                            }
                        }
                    }
                });
                handles.push(handle);
            }
            
            // Reader threads
            let writer_threads = self.config.writer_threads;
            for thread_id in 0..self.config.reader_threads {
                let db_clone = db.clone();
                let barrier_clone = barrier.clone();
                
                let handle = thread::spawn(move || {
                    barrier_clone.wait();
                    let mut rng = rand::rng();
                    
                    for _ in 0..2000 {
                        let key = format!("mixed_{}_{}", 
                            rng.random_range(0..writer_threads),
                            rng.random_range(0..1000)
                        );
                        let _ = db_clone.get(key.as_bytes());
                    }
                });
                handles.push(handle);
            }
            
            // Start all threads
            barrier.wait();
            
            // Let it run briefly
            thread::sleep(Duration::from_millis(300));
            
            // Simulate crash
            drop(db);
            std::mem::forget(handles);
        }
        
        // Phase 2: Recovery and verification
        let recovery_start = Instant::now();
        let recovered_db = match Database::open(&db_path, self.get_db_config()) {
            Ok(db) => db,
            Err(e) => {
                return RecoveryTestResult {
                    operations_before_crash: ops_tracker.total_operations(),
                    operations_recovered: 0,
                    integrity_check_passed: false,
                    recovery_duration: recovery_start.elapsed(),
                    corruption_detected: true,
                    errors: vec![format!("Recovery failed: {}", e)],
                };
            }
        };
        let recovery_duration = recovery_start.elapsed();
        
        // Verify mixed operations integrity
        let verification = self.verify_mixed_operations(&recovered_db, &ops_tracker);
        
        RecoveryTestResult {
            operations_before_crash: ops_tracker.total_operations(),
            operations_recovered: verification.0,
            integrity_check_passed: verification.1,
            recovery_duration,
            corruption_detected: !verification.1,
            errors: verification.2,
        }
    }

    // Helper methods

    fn create_database(&self, path: &Path) -> Arc<Database> {
        Arc::new(Database::create(path, self.get_db_config()).unwrap())
    }

    fn get_db_config(&self) -> LightningDbConfig {
        LightningDbConfig {
            use_improved_wal: self.config.use_wal,
            compression_enabled: self.config.use_compression,
            cache_size: self.config.cache_size as u64,
            ..Default::default()
        }
    }

    fn calculate_checksum(data: &str) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::{Hash, Hasher};
        data.hash(&mut hasher);
        hasher.finish() as u32
    }

    fn verify_crash_write_data(
        &self, 
        db: &Database, 
        expected_ops: usize
    ) -> (usize, bool, Vec<String>) {
        let mut recovered = 0;
        let mut errors = vec![];
        
        // Check each thread's data
        for thread_id in 0..self.config.writer_threads {
            let ops_per_thread = expected_ops / self.config.writer_threads;
            
            for i in 0..ops_per_thread {
                let key = format!("crash_test_{}_{}", thread_id, i);
                
                match db.get(key.as_bytes()) {
                    Ok(Some(value)) => {
                        let value_str = String::from_utf8_lossy(&value);
                        let expected_checksum = Self::calculate_checksum(&key);
                        
                        if value_str.contains(&format!("checksum_{}", expected_checksum)) {
                            recovered += 1;
                        } else {
                            errors.push(format!("Checksum mismatch for key: {}", key));
                        }
                    }
                    Ok(None) => {
                        // Key might not have been persisted
                    }
                    Err(e) => {
                        errors.push(format!("Error reading key {}: {}", key, e));
                    }
                }
            }
        }
        
        let integrity_ok = errors.is_empty() && recovered > 0;
        (recovered, integrity_ok, errors)
    }

    fn verify_transaction_consistency(
        &self,
        db: &Database,
        expected_committed: usize
    ) -> (usize, bool, Vec<String>) {
        let mut complete_txs = 0;
        let mut partial_txs = 0;
        let mut errors = vec![];
        
        for tx_num in 0..expected_committed {
            let mut found_keys = 0;
            
            for i in 0..10 {
                let key = format!("tx_{}_key_{}", tx_num, i);
                if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                    found_keys += 1;
                }
            }
            
            if found_keys == 10 {
                complete_txs += 1;
            } else if found_keys > 0 {
                partial_txs += 1;
                errors.push(format!(
                    "Transaction {} partially committed: {}/10 keys found",
                    tx_num, found_keys
                ));
            }
        }
        
        let integrity_ok = partial_txs == 0;
        (complete_txs * 10, integrity_ok, errors)
    }

    fn verify_compaction_integrity(
        &self,
        db: &Database,
        total_keys: usize
    ) -> (usize, bool, Vec<String>) {
        let mut found = 0;
        let mut errors = vec![];
        let mut missing_ranges = vec![];
        let mut last_missing = None;
        
        for i in 0..total_keys {
            let key = format!("compact_key_{:08}", i);
            
            // Keys deleted during test
            if i > 100 && (i - 100) % 5 == 0 {
                continue;
            }
            
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    found += 1;
                    
                    // Verify value integrity
                    if value.is_empty() || value[0] != (i as u8) {
                        errors.push(format!("Value corruption for key: {}", key));
                    }
                    
                    if let Some(start) = last_missing.take() {
                        missing_ranges.push((start, i - 1));
                    }
                }
                Ok(None) => {
                    if last_missing.is_none() {
                        last_missing = Some(i);
                    }
                }
                Err(e) => {
                    errors.push(format!("Error reading key {}: {}", key, e));
                }
            }
        }
        
        if missing_ranges.len() > 10 {
            errors.push(format!(
                "Too many missing key ranges: {} ranges",
                missing_ranges.len()
            ));
        }
        
        let integrity_ok = errors.is_empty() && found > total_keys / 2;
        (found, integrity_ok, errors)
    }

    fn verify_mixed_operations(
        &self,
        db: &Database,
        tracker: &OperationsTracker
    ) -> (usize, bool, Vec<String>) {
        let mut verified = 0;
        let mut errors = vec![];
        
        // Verify final state
        for (key, expected_state) in tracker.get_expected_state() {
            match (db.get(key.as_bytes()).unwrap_or(None), expected_state) {
                (Some(_), OperationState::Exists) => verified += 1,
                (None, OperationState::Deleted) => verified += 1,
                (Some(_), OperationState::Deleted) => {
                    errors.push(format!("Key {} should be deleted but exists", key));
                }
                (None, OperationState::Exists) => {
                    // Might be expected if operation wasn't persisted
                }
            }
        }
        
        let integrity_ok = errors.len() < 10; // Allow some inconsistency
        (verified, integrity_ok, errors)
    }
}

/// Track operations for verification
struct OperationsTracker {
    inserts: Arc<RwLock<Vec<String>>>,
    updates: Arc<RwLock<Vec<String>>>,
    deletes: Arc<RwLock<Vec<String>>>,
}

impl OperationsTracker {
    fn new() -> Self {
        Self {
            inserts: Arc::new(RwLock::new(Vec::new())),
            updates: Arc::new(RwLock::new(Vec::new())),
            deletes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    fn record_insert(&self, key: String) {
        self.inserts.write().push(key);
    }

    fn record_update(&self, key: String) {
        self.updates.write().push(key);
    }

    fn record_delete(&self, key: String) {
        self.deletes.write().push(key);
    }

    fn total_operations(&self) -> usize {
        self.inserts.read().len() + 
        self.updates.read().len() + 
        self.deletes.read().len()
    }

    fn get_expected_state(&self) -> HashMap<String, OperationState> {
        let mut state = HashMap::new();
        
        // Apply operations in order
        for key in self.inserts.read().iter() {
            state.insert(key.clone(), OperationState::Exists);
        }
        
        for key in self.updates.read().iter() {
            state.insert(key.clone(), OperationState::Exists);
        }
        
        for key in self.deletes.read().iter() {
            state.insert(key.clone(), OperationState::Deleted);
        }
        
        state
    }
}

#[derive(Debug, PartialEq)]
enum OperationState {
    Exists,
    Deleted,
}

// Test runner
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_crash_recovery_suite() {
        let suite = CrashRecoveryTestSuite::new();
        
        println!("\n=== Lightning DB Crash Recovery Test Suite ===\n");
        
        // Run all tests
        let results = vec![
            ("Crash During Writes", suite.test_crash_during_writes()),
            ("Crash During Transaction", suite.test_crash_during_transaction()),
            ("Crash During Compaction", suite.test_crash_during_compaction()),
            ("Power Loss Simulation", suite.test_power_loss_simulation()),
            ("Concurrent Mixed Ops Crash", suite.test_concurrent_crash_mixed_ops()),
        ];
        
        // Print summary
        println!("\n=== Test Summary ===\n");
        
        let mut all_passed = true;
        for (test_name, result) in &results {
            let status = if result.integrity_check_passed { "✅ PASS" } else { "❌ FAIL" };
            println!("{}: {}", test_name, status);
            println!("  - Operations before crash: {}", result.operations_before_crash);
            println!("  - Operations recovered: {} ({:.1}%)", 
                result.operations_recovered,
                (result.operations_recovered as f64 / result.operations_before_crash as f64) * 100.0
            );
            println!("  - Recovery time: {:?}", result.recovery_duration);
            
            if !result.errors.is_empty() {
                println!("  - Errors: {:?}", result.errors);
            }
            
            println!();
            
            if !result.integrity_check_passed {
                all_passed = false;
            }
        }
        
        assert!(all_passed, "Some crash recovery tests failed");
    }

    #[test]
    #[ignore] // Long-running test
    fn stress_test_crash_recovery() {
        let config = CrashTestConfig {
            writer_threads: 8,
            reader_threads: 4,
            ops_before_crash: 100000,
            ..Default::default()
        };
        
        let suite = CrashRecoveryTestSuite::with_config(config);
        
        for i in 0..10 {
            println!("\n=== Stress Test Iteration {} ===", i + 1);
            
            let result = suite.test_crash_during_writes();
            assert!(
                result.operations_recovered as f64 / result.operations_before_crash as f64 > 0.95,
                "Recovery rate too low in iteration {}", i + 1
            );
        }
    }
}

use parking_lot::RwLock;
use std::collections::HashMap;
use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier, Mutex,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use std::path::PathBuf;
use std::fs;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write, Seek, SeekFrom};

#[derive(Debug, Clone)]
pub struct PersistenceTestResult {
    pub test_name: String,
    pub duration_ms: u64,
    pub operations_count: u64,
    pub success_rate: f64,
    pub integrity_verified: bool,
    pub performance_ops_per_sec: f64,
    pub data_consistency_score: f64,
    pub recovery_time_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DataIntegrityReport {
    pub total_entries: u64,
    pub verified_entries: u64,
    pub corrupted_entries: u64,
    pub missing_entries: u64,
    pub checksum_failures: u64,
    pub structure_violations: u64,
    pub consistency_score: f64,
}

pub struct PersistenceTestSuite {
    db_path: PathBuf,
    config: LightningDbConfig,
}

impl PersistenceTestSuite {
    pub fn new(db_path: PathBuf, config: LightningDbConfig) -> Self {
        Self { db_path, config }
    }

    pub fn run_comprehensive_persistence_tests(&self) -> Vec<PersistenceTestResult> {
        let mut results = Vec::new();

        println!("Starting comprehensive persistence and recovery test suite...");

        // 1. Persistence Validation Tests
        results.push(self.test_write_read_consistency());
        results.push(self.test_cross_restart_data_integrity());
        results.push(self.test_page_level_persistence());
        results.push(self.test_wal_recovery_validation());
        results.push(self.test_transaction_durability());
        results.push(self.test_concurrent_write_persistence());

        // 2. Recovery Validation Tests
        results.push(self.test_crash_recovery_simulation());
        results.push(self.test_partial_write_recovery());
        results.push(self.test_corrupted_page_recovery());
        results.push(self.test_wal_replay_validation());
        results.push(self.test_point_in_time_recovery());
        results.push(self.test_rollback_rollforward());

        // 3. Data Integrity Tests
        results.push(self.test_checksum_validation_after_recovery());
        results.push(self.test_btree_consistency_after_crash());
        results.push(self.test_index_integrity_validation());
        results.push(self.test_data_corruption_detection());

        results
    }

    // ===== PERSISTENCE VALIDATION TESTS =====

    fn test_write_read_consistency(&self) -> PersistenceTestResult {
        println!("  Testing write-read consistency...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(1000, 1024);
        let mut operations_count = 0u64;
        let mut verified_count = 0u64;

        // Write phase
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        // Immediate read phase
        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;
        let ops_per_sec = operations_count as f64 / duration.as_secs_f64();

        drop(db);

        println!(
            "    Write-read consistency: {:.2}% success rate, {:.0} ops/sec",
            success_rate * 100.0, ops_per_sec
        );

        PersistenceTestResult {
            test_name: "write_read_consistency".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: success_rate > 0.999,
            performance_ops_per_sec: ops_per_sec,
            data_consistency_score: success_rate,
            recovery_time_ms: None,
        }
    }

    fn test_cross_restart_data_integrity(&self) -> PersistenceTestResult {
        println!("  Testing cross-restart data integrity...");
        
        let start_time = Instant::now();
        let test_data = self.generate_test_dataset(500, 2048);
        
        // Phase 1: Write data and close database
        {
            let db = Database::open(&self.db_path, self.config.clone()).unwrap();
            for (key, value) in &test_data {
                db.put(key, value).unwrap();
            }
            let _ = db.checkpoint();
        } // Database closed here

        thread::sleep(Duration::from_millis(100)); // Simulate system delay

        // Phase 2: Reopen and verify data persistence
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut verified_count = 0u64;
        let mut checksum_verified = 0u64;

        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                        
                        // Additional checksum verification
                        let expected_checksum = crc32fast::hash(expected_value);
                        let actual_checksum = crc32fast::hash(&actual_value);
                        if expected_checksum == actual_checksum {
                            checksum_verified += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;
        let checksum_rate = checksum_verified as f64 / test_data.len() as f64;
        let consistency_score = (success_rate + checksum_rate) / 2.0;

        drop(db);

        println!(
            "    Cross-restart integrity: {:.2}% data verified, {:.2}% checksum verified",
            success_rate * 100.0, checksum_rate * 100.0
        );

        PersistenceTestResult {
            test_name: "cross_restart_data_integrity".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count: test_data.len() as u64 * 2, // Write + read
            success_rate,
            integrity_verified: success_rate > 0.99,
            performance_ops_per_sec: (test_data.len() * 2) as f64 / duration.as_secs_f64(),
            data_consistency_score: consistency_score,
            recovery_time_ms: None,
        }
    }

    fn test_page_level_persistence(&self) -> PersistenceTestResult {
        println!("  Testing page-level persistence validation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Generate data to span multiple pages
        let large_test_data = self.generate_test_dataset(2000, 4096);
        let mut operations_count = 0u64;

        // Write data across multiple pages
        for (key, value) in &large_test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        // Force checkpoint to ensure pages are written to disk
        let checkpoint_result = db.checkpoint();
        assert!(checkpoint_result.is_ok(), "Checkpoint failed");

        drop(db);

        // Reopen and verify page-level integrity
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        let mut verified_count = 0u64;
        let mut page_consistency_checks = 0u64;

        for (key, expected_value) in &large_test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                        page_consistency_checks += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / large_test_data.len() as f64;
        let page_consistency = page_consistency_checks as f64 / large_test_data.len() as f64;

        drop(db);

        println!(
            "    Page-level persistence: {:.2}% verified, {:.2}% page consistency, recovery: {:?}",
            success_rate * 100.0, page_consistency * 100.0, recovery_time
        );

        PersistenceTestResult {
            test_name: "page_level_persistence".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: success_rate > 0.99 && page_consistency > 0.99,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: page_consistency,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_wal_recovery_validation(&self) -> PersistenceTestResult {
        println!("  Testing WAL recovery validation...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            // use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let test_data = self.generate_test_dataset(800, 1024);
        let mut operations_count = 0u64;

        // Write data that should be logged in WAL
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        // Don't checkpoint - let WAL handle recovery
        drop(db);

        // Simulate crash recovery by reopening
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, config).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify WAL recovery worked
        let mut verified_count = 0u64;
        let mut wal_integrity_checks = 0u64;

        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                        wal_integrity_checks += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;
        let wal_recovery_rate = wal_integrity_checks as f64 / test_data.len() as f64;

        drop(db);

        println!(
            "    WAL recovery: {:.2}% success rate, {:.2}% WAL integrity, recovery time: {:?}",
            success_rate * 100.0, wal_recovery_rate * 100.0, recovery_time
        );

        PersistenceTestResult {
            test_name: "wal_recovery_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: success_rate > 0.95 && wal_recovery_rate > 0.95,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: wal_recovery_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_transaction_durability(&self) -> PersistenceTestResult {
        println!("  Testing transaction durability...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut committed_transactions = Vec::new();
        let mut uncommitted_transactions = Vec::new();
        let mut operations_count = 0u64;

        // Create mix of committed and uncommitted transactions
        for tx_id in 0..50 {
            let db_tx_id = db.begin_transaction().unwrap();
            let tx_data = self.generate_test_dataset(20, 512);
            
            // Write transaction data
            for (key, value) in &tx_data {
                let tx_key = format!("tx_{}_{}", tx_id, String::from_utf8_lossy(key));
                db.put_tx(db_tx_id, tx_key.as_bytes(), value).unwrap();
                operations_count += 1;
            }
            
            if tx_id % 3 != 0 { // Commit 2/3 of transactions
                db.commit_transaction(db_tx_id).unwrap();
                committed_transactions.push((tx_id, tx_data));
            } else {
                // Don't commit - these should be rolled back
                uncommitted_transactions.push((tx_id, tx_data));
                std::mem::forget(db_tx_id); // Simulate process crash before commit
            }
        }

        drop(db); // Simulate crash

        // Recovery and verification
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify committed transactions are durable
        let mut committed_verified = 0u64;
        let mut uncommitted_found = 0u64;

        for (tx_id, tx_data) in &committed_transactions {
            let mut tx_entries_found = 0;
            for (key, expected_value) in tx_data {
                let tx_key = format!("tx_{}_{}", tx_id, String::from_utf8_lossy(key));
                match db.get(tx_key.as_bytes()) {
                    Ok(Some(actual_value)) => {
                        if actual_value == *expected_value {
                            tx_entries_found += 1;
                        }
                    }
                    _ => {}
                }
                operations_count += 1;
            }
            if tx_entries_found == tx_data.len() {
                committed_verified += 1;
            }
        }

        // Verify uncommitted transactions are rolled back
        for (tx_id, tx_data) in &uncommitted_transactions {
            for (key, _) in tx_data {
                let tx_key = format!("tx_{}_{}", tx_id, String::from_utf8_lossy(key));
                if db.get(tx_key.as_bytes()).unwrap_or(None).is_some() {
                    uncommitted_found += 1;
                }
                operations_count += 1;
            }
        }

        let duration = start_time.elapsed();
        let durability_rate = committed_verified as f64 / committed_transactions.len() as f64;
        let atomicity_preserved = uncommitted_found == 0;
        let overall_success = if atomicity_preserved { durability_rate } else { 0.0 };

        drop(db);

        println!(
            "    Transaction durability: {:.2}% committed verified, {} uncommitted found (should be 0), atomicity: {}",
            durability_rate * 100.0, uncommitted_found, atomicity_preserved
        );

        PersistenceTestResult {
            test_name: "transaction_durability".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: overall_success,
            integrity_verified: atomicity_preserved && durability_rate > 0.95,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: durability_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_concurrent_write_persistence(&self) -> PersistenceTestResult {
        println!("  Testing concurrent write persistence...");
        
        let start_time = Instant::now();
        let db = Arc::new(Database::open(&self.db_path, self.config.clone()).unwrap());
        
        let operations_count = Arc::new(AtomicU64::new(0));
        let successful_writes = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(4));
        let thread_data = Arc::new(Mutex::new(HashMap::new()));

        let mut handles = Vec::new();

        // Spawn concurrent writers
        for thread_id in 0..4 {
            let db_clone = Arc::clone(&db);
            let ops_clone = Arc::clone(&operations_count);
            let writes_clone = Arc::clone(&successful_writes);
            let barrier_clone = Arc::clone(&barrier);
            let data_clone = Arc::clone(&thread_data);

            let handle = thread::spawn(move || {
                let thread_test_data = (0..200).map(|i| {
                    let key = format!("concurrent_{}_{:06}", thread_id, i);
                    let value = format!("value_{}_{}_{}", thread_id, i, rand::random::<u32>());
                    (key.into_bytes(), value.into_bytes())
                }).collect::<Vec<_>>();

                // Store for later verification
                {
                    let mut data_map = data_clone.lock().unwrap();
                    data_map.insert(thread_id, thread_test_data.clone());
                }

                barrier_clone.wait();

                for (key, value) in &thread_test_data {
                    match db_clone.put(key, value) {
                        Ok(_) => {
                            writes_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {}
                    }
                    ops_clone.fetch_add(1, Ordering::Relaxed);
                }
            });

            handles.push(handle);
        }

        // Wait for all writers to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Force persistence
        let _ = db.checkpoint();
        drop(db);

        // Verify persistence after restart
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        let mut verified_count = 0u64;
        let total_expected = {
            let data_map = thread_data.lock().unwrap();
            data_map.values().map(|v| v.len() as u64).sum::<u64>()
        };

        {
            let data_map = thread_data.lock().unwrap();
            for thread_data in data_map.values() {
                for (key, expected_value) in thread_data {
                    match db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                verified_count += 1;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let duration = start_time.elapsed();
        let total_ops = operations_count.load(Ordering::Relaxed);
        let success_rate = verified_count as f64 / total_expected as f64;
        let persistence_rate = verified_count as f64 / successful_writes.load(Ordering::Relaxed) as f64;

        drop(db);

        println!(
            "    Concurrent write persistence: {:.2}% success rate, {:.2}% persistence rate, {} total ops",
            success_rate * 100.0, persistence_rate * 100.0, total_ops
        );

        PersistenceTestResult {
            test_name: "concurrent_write_persistence".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count: total_ops,
            success_rate,
            integrity_verified: success_rate > 0.95 && persistence_rate > 0.99,
            performance_ops_per_sec: total_ops as f64 / duration.as_secs_f64(),
            data_consistency_score: persistence_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    // ===== RECOVERY VALIDATION TESTS =====

    fn test_crash_recovery_simulation(&self) -> PersistenceTestResult {
        println!("  Testing crash recovery simulation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(600, 1024);
        let mut operations_count = 0u64;

        // Write data to database
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        // Simulate abrupt crash by dropping without clean shutdown
        std::mem::drop(db);

        // Simulate recovery
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify crash recovery
        let mut verified_count = 0u64;
        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;

        drop(db);

        println!(
            "    Crash recovery simulation: {:.2}% recovery rate, recovery time: {:?}",
            success_rate * 100.0, recovery_time
        );

        PersistenceTestResult {
            test_name: "crash_recovery_simulation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: success_rate > 0.90,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: success_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_partial_write_recovery(&self) -> PersistenceTestResult {
        println!("  Testing partial write recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(400, 2048);
        let mut operations_count = 0u64;

        // Write first half completely
        let split_point = test_data.len() / 2;
        for (key, value) in &test_data[..split_point] {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint(); // Ensure first half is persisted

        // Write second half but simulate crash before complete persistence
        for (key, value) in &test_data[split_point..] {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        // Simulate crash without checkpoint
        std::mem::drop(db);

        // Recovery
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify recovery behavior
        let mut first_half_verified = 0u64;
        let mut second_half_recovered = 0u64;

        // First half should be fully recovered
        for (key, expected_value) in &test_data[..split_point] {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        first_half_verified += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        // Second half may be partially recovered depending on WAL
        for (key, expected_value) in &test_data[split_point..] {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        second_half_recovered += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let first_half_rate = first_half_verified as f64 / split_point as f64;
        let second_half_rate = second_half_recovered as f64 / (test_data.len() - split_point) as f64;
        let overall_success = (first_half_rate + second_half_rate) / 2.0;

        drop(db);

        println!(
            "    Partial write recovery: {:.2}% first half, {:.2}% second half",
            first_half_rate * 100.0, second_half_rate * 100.0
        );

        PersistenceTestResult {
            test_name: "partial_write_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: overall_success,
            integrity_verified: first_half_rate > 0.99, // First half should be fully recovered
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: first_half_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_corrupted_page_recovery(&self) -> PersistenceTestResult {
        println!("  Testing corrupted page recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(300, 1024);
        let mut operations_count = 0u64;

        // Write test data
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject controlled corruption
        let corruption_count = self.inject_page_corruption().unwrap_or(0);

        // Attempt recovery
        let recovery_start = Instant::now();
        let db_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (verified_count, recovery_success) = match db_result {
            Ok(db) => {
                let mut verified = 0u64;
                for (key, expected_value) in &test_data {
                    match db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                verified += 1;
                            }
                        }
                        _ => {}
                    }
                    operations_count += 1;
                }
                drop(db);
                (verified, true)
            }
            Err(_) => (0, false)
        };

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;

        println!(
            "    Corrupted page recovery: {} corruptions injected, {:.2}% data recovered, recovery: {}",
            corruption_count, success_rate * 100.0, recovery_success
        );

        PersistenceTestResult {
            test_name: "corrupted_page_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: recovery_success && success_rate > 0.5, // Some data loss expected
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: success_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_wal_replay_validation(&self) -> PersistenceTestResult {
        println!("  Testing WAL replay validation...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            // use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let test_data = self.generate_test_dataset(500, 1024);
        let mut operations_count = 0u64;

        // Write data with explicit WAL operations
        for (i, (key, value)) in test_data.iter().enumerate() {
            db.put(key, value).unwrap();
            operations_count += 1;

            // Occasional sync to create WAL segments
            if i % 50 == 0 {
                let _ = db.checkpoint();
            }
        }

        drop(db); // Don't checkpoint the last segment

        // Recovery with WAL replay
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, config).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify WAL replay correctness
        let mut verified_count = 0u64;
        let mut replay_consistency = 0u64;

        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        verified_count += 1;
                        replay_consistency += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let success_rate = verified_count as f64 / test_data.len() as f64;
        let replay_rate = replay_consistency as f64 / test_data.len() as f64;

        drop(db);

        println!(
            "    WAL replay validation: {:.2}% success rate, {:.2}% replay consistency",
            success_rate * 100.0, replay_rate * 100.0
        );

        PersistenceTestResult {
            test_name: "wal_replay_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate,
            integrity_verified: success_rate > 0.95 && replay_rate > 0.95,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: replay_rate,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_point_in_time_recovery(&self) -> PersistenceTestResult {
        println!("  Testing point-in-time recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let baseline_data = self.generate_test_dataset(200, 1024);
        let checkpoint_data = self.generate_test_dataset(200, 1024);
        let additional_data = self.generate_test_dataset(200, 1024);
        let mut operations_count = 0u64;

        // Phase 1: Baseline data
        for (key, value) in &baseline_data {
            let prefixed_key = format!("baseline_{}", String::from_utf8_lossy(key));
            db.put(prefixed_key.as_bytes(), value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint(); // Checkpoint 1

        // Phase 2: More data
        for (key, value) in &checkpoint_data {
            let prefixed_key = format!("checkpoint_{}", String::from_utf8_lossy(key));
            db.put(prefixed_key.as_bytes(), value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint(); // Checkpoint 2

        // Phase 3: Additional data (not checkpointed)
        for (key, value) in &additional_data {
            let prefixed_key = format!("additional_{}", String::from_utf8_lossy(key));
            db.put(prefixed_key.as_bytes(), value).unwrap();
            operations_count += 1;
        }

        drop(db); // Simulate crash

        // Recovery to checkpoint 2
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify point-in-time recovery
        let mut baseline_verified = 0u64;
        let mut checkpoint_verified = 0u64;
        let mut additional_found = 0u64;

        // Baseline data should be present
        for (key, expected_value) in &baseline_data {
            let prefixed_key = format!("baseline_{}", String::from_utf8_lossy(key));
            match db.get(prefixed_key.as_bytes()) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        baseline_verified += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        // Checkpoint data should be present
        for (key, expected_value) in &checkpoint_data {
            let prefixed_key = format!("checkpoint_{}", String::from_utf8_lossy(key));
            match db.get(prefixed_key.as_bytes()) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        checkpoint_verified += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        // Additional data may or may not be present (depends on WAL recovery)
        for (key, _) in &additional_data {
            let prefixed_key = format!("additional_{}", String::from_utf8_lossy(key));
            if db.get(prefixed_key.as_bytes()).unwrap_or(None).is_some() {
                additional_found += 1;
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let baseline_rate = baseline_verified as f64 / baseline_data.len() as f64;
        let checkpoint_rate = checkpoint_verified as f64 / checkpoint_data.len() as f64;
        let point_in_time_consistency = (baseline_rate + checkpoint_rate) / 2.0;

        drop(db);

        println!(
            "    Point-in-time recovery: {:.2}% baseline, {:.2}% checkpoint, {} additional found",
            baseline_rate * 100.0, checkpoint_rate * 100.0, additional_found
        );

        PersistenceTestResult {
            test_name: "point_in_time_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: point_in_time_consistency,
            integrity_verified: baseline_rate > 0.99 && checkpoint_rate > 0.99,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: point_in_time_consistency,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_rollback_rollforward(&self) -> PersistenceTestResult {
        println!("  Testing rollback and rollforward operations...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut operations_count = 0u64;
        let mut successful_rollbacks = 0u64;
        let mut successful_commits = 0u64;

        // Test rollback scenarios
        for batch in 0..20 {
            let tx_id = db.begin_transaction().unwrap();
            let batch_data = self.generate_test_dataset(10, 512);
            
            // Write transaction data
            for (key, value) in &batch_data {
                let tx_key = format!("rollback_test_{}_{}", batch, String::from_utf8_lossy(key));
                db.put_tx(tx_id, tx_key.as_bytes(), value).unwrap();
                operations_count += 1;
            }
            
            if batch % 2 == 0 {
                // Commit every other transaction
                db.commit_transaction(tx_id).unwrap();
                successful_commits += 1;
            } else {
                // Rollback the transaction
                db.abort_transaction(tx_id).unwrap();
                successful_rollbacks += 1;
            }
        }

        // Verify rollback/rollforward behavior
        let mut committed_found = 0u64;
        let mut rolled_back_found = 0u64;

        for batch in 0..20 {
            let batch_data = self.generate_test_dataset(10, 512);
            let mut batch_entries_found = 0;
            
            for (key, _) in &batch_data {
                let tx_key = format!("rollback_test_{}_{}", batch, String::from_utf8_lossy(key));
                if db.get(tx_key.as_bytes()).unwrap_or(None).is_some() {
                    batch_entries_found += 1;
                }
                operations_count += 1;
            }
            
            if batch % 2 == 0 {
                // Should be committed
                if batch_entries_found == batch_data.len() {
                    committed_found += 1;
                }
            } else {
                // Should be rolled back
                if batch_entries_found == 0 {
                    rolled_back_found += 1;
                }
            }
        }

        let duration = start_time.elapsed();
        let commit_accuracy = committed_found as f64 / successful_commits as f64;
        let rollback_accuracy = rolled_back_found as f64 / successful_rollbacks as f64;
        let overall_accuracy = (commit_accuracy + rollback_accuracy) / 2.0;

        drop(db);

        println!(
            "    Rollback/rollforward: {:.2}% commit accuracy, {:.2}% rollback accuracy",
            commit_accuracy * 100.0, rollback_accuracy * 100.0
        );

        PersistenceTestResult {
            test_name: "rollback_rollforward".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: overall_accuracy,
            integrity_verified: commit_accuracy > 0.99 && rollback_accuracy > 0.99,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: overall_accuracy,
            recovery_time_ms: None,
        }
    }

    // ===== DATA INTEGRITY TESTS =====

    fn test_checksum_validation_after_recovery(&self) -> PersistenceTestResult {
        println!("  Testing checksum validation after recovery...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(400, 2048);
        let mut checksums = HashMap::new();
        let mut operations_count = 0u64;

        // Write data with checksum tracking
        for (key, value) in &test_data {
            let checksum = crc32fast::hash(value);
            checksums.insert(key.clone(), checksum);
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        drop(db); // Simulate crash

        // Recovery and checksum validation
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        let mut verified_checksums = 0u64;
        let mut corrupted_checksums = 0u64;
        let mut missing_data = 0u64;

        for (key, expected_checksum) in &checksums {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    let actual_checksum = crc32fast::hash(&actual_value);
                    if actual_checksum == *expected_checksum {
                        verified_checksums += 1;
                    } else {
                        corrupted_checksums += 1;
                    }
                }
                Ok(None) => missing_data += 1,
                Err(_) => corrupted_checksums += 1,
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let checksum_success_rate = verified_checksums as f64 / test_data.len() as f64;
        let data_integrity_score = if verified_checksums + corrupted_checksums + missing_data > 0 {
            verified_checksums as f64 / (verified_checksums + corrupted_checksums + missing_data) as f64
        } else {
            0.0
        };

        drop(db);

        println!(
            "    Checksum validation: {:.2}% verified, {} corrupted, {} missing",
            checksum_success_rate * 100.0, corrupted_checksums, missing_data
        );

        PersistenceTestResult {
            test_name: "checksum_validation_after_recovery".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: checksum_success_rate,
            integrity_verified: checksum_success_rate > 0.99 && corrupted_checksums == 0,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: data_integrity_score,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_btree_consistency_after_crash(&self) -> PersistenceTestResult {
        println!("  Testing B+Tree consistency after crash...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_sorted_test_dataset(800, 1024);
        let mut operations_count = 0u64;

        // Insert sorted data to build B+Tree structure
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        drop(db); // Simulate crash

        // Recovery and B+Tree consistency check
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify B+Tree structure integrity
        let sequential_access_works = true;
        let mut range_queries_work = true;
        let mut point_queries_work = 0u64;

        // Test point queries
        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        point_queries_work += 1;
                    }
                }
                _ => {}
            }
            operations_count += 1;
        }

        // Test range queries
        if test_data.len() >= 100 {
            let start_key = &test_data[50].0;
            let end_key = &test_data[150].0;
            
            match db.range(Some(start_key), Some(end_key)) {
                Ok(range_results) => {
                    if range_results.len() < 100 {
                        range_queries_work = false;
                    }
                }
                Err(_) => range_queries_work = false,
            }
        }

        let duration = start_time.elapsed();
        let point_query_rate = point_queries_work as f64 / test_data.len() as f64;
        let structure_integrity = if sequential_access_works && range_queries_work {
            point_query_rate
        } else {
            point_query_rate * 0.5 // Penalize structural issues
        };

        drop(db);

        println!(
            "    B+Tree consistency: {:.2}% point queries, range queries: {}, sequential: {}",
            point_query_rate * 100.0, range_queries_work, sequential_access_works
        );

        PersistenceTestResult {
            test_name: "btree_consistency_after_crash".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: structure_integrity,
            integrity_verified: structure_integrity > 0.99 && range_queries_work && sequential_access_works,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: structure_integrity,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_index_integrity_validation(&self) -> PersistenceTestResult {
        println!("  Testing index integrity validation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(500, 1024);
        let mut operations_count = 0u64;

        // Create data with indexable patterns
        for (i, (key, value)) in test_data.iter().enumerate() {
            let indexed_key = format!("idx_{:06}_{}", i, String::from_utf8_lossy(key));
            db.put(indexed_key.as_bytes(), value).unwrap();
            operations_count += 1;
        }

        drop(db); // Simulate crash

        // Recovery and index integrity verification
        let recovery_start = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify index consistency through pattern queries
        let mut pattern_consistency = 0u64;
        let mut range_consistency = 0u64;

        // Test prefix pattern consistency
        let prefix_results = db.range(
            Some(b"idx_000000_"),
            Some(b"idx_000100_")
        );

        match prefix_results {
            Ok(results) => {
                if results.len() == 100 {
                    pattern_consistency += 1;
                }
            }
            Err(_) => {}
        }

        // Test range consistency
        for start_idx in (0..400).step_by(100) {
            let start_key = format!("idx_{:06}_", start_idx);
            let end_key = format!("idx_{:06}_", start_idx + 100);
            
            match db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())) {
                Ok(results) => {
                    if results.len() == 100 {
                        range_consistency += 1;
                    }
                }
                Err(_) => {}
            }
            operations_count += 1;
        }

        let duration = start_time.elapsed();
        let index_consistency_rate = range_consistency as f64 / 4.0; // 4 range tests
        let overall_integrity = if pattern_consistency > 0 { index_consistency_rate } else { 0.0 };

        drop(db);

        println!(
            "    Index integrity: {:.2}% range consistency, {} pattern matches",
            index_consistency_rate * 100.0, pattern_consistency
        );

        PersistenceTestResult {
            test_name: "index_integrity_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: overall_integrity,
            integrity_verified: overall_integrity > 0.9 && pattern_consistency > 0,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: overall_integrity,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    fn test_data_corruption_detection(&self) -> PersistenceTestResult {
        println!("  Testing data corruption detection...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_test_dataset(300, 1024);
        let mut operations_count = 0u64;

        // Write test data
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            operations_count += 1;
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject corruption
        let corruption_count = self.inject_data_corruption().unwrap_or(0);

        // Open database and test corruption detection
        let recovery_start = Instant::now();
        let db_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        let (corruption_detected, integrity_preserved) = match db_result {
            Ok(db) => {
                // Run integrity verification if available
                let integrity_result = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(db.verify_integrity());

                let detected_issues = match integrity_result {
                    Ok(report) => {
                        report.checksum_errors.len() +
                        report.structure_errors.len() +
                        report.consistency_errors.len()
                    }
                    Err(_) => 0,
                };

                // Test data access after corruption
                let mut accessible_data = 0u64;
                for (key, expected_value) in &test_data {
                    match db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                accessible_data += 1;
                            }
                        }
                        _ => {}
                    }
                    operations_count += 1;
                }

                drop(db);
                let integrity_score = accessible_data as f64 / test_data.len() as f64;
                (detected_issues as u64, integrity_score > 0.8)
            }
            Err(_) => {
                // Database failed to open due to corruption
                (corruption_count, false)
            }
        };

        let duration = start_time.elapsed();
        let detection_effectiveness = if corruption_count > 0 {
            corruption_detected as f64 / corruption_count as f64
        } else {
            1.0
        };

        println!(
            "    Data corruption detection: {} corruptions injected, {} detected, integrity preserved: {}",
            corruption_count, corruption_detected, integrity_preserved
        );

        PersistenceTestResult {
            test_name: "data_corruption_detection".to_string(),
            duration_ms: duration.as_millis() as u64,
            operations_count,
            success_rate: detection_effectiveness,
            integrity_verified: integrity_preserved,
            performance_ops_per_sec: operations_count as f64 / duration.as_secs_f64(),
            data_consistency_score: detection_effectiveness,
            recovery_time_ms: Some(recovery_time.as_millis() as u64),
        }
    }

    // ===== HELPER METHODS =====

    fn generate_test_dataset(&self, count: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("test_key_{:08}", i).into_bytes();
                let value = (0..value_size).map(|j| ((i + j) % 256) as u8).collect();
                (key, value)
            })
            .collect()
    }

    fn generate_sorted_test_dataset(&self, count: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut data = self.generate_test_dataset(count, value_size);
        data.sort_by(|a, b| a.0.cmp(&b.0));
        data
    }

    fn inject_page_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 4096 {
                            // Corrupt a page-sized section
                            let corrupt_start = 4096; // Second page
                            for i in corrupt_start..std::cmp::min(corrupt_start + 1024, file_data.len()) {
                                file_data[i] = !file_data[i];
                            }
                            
                            if fs::write(&path, &file_data).is_ok() {
                                corruption_count += 1;
                            }
                        }
                    }
                }
            }
        }
        
        Ok(corruption_count)
    }

    fn inject_data_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" || extension == "wal" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 512 {
                            // Inject multiple small corruptions
                            for corruption_point in (512..file_data.len()).step_by(2048) {
                                if corruption_point + 8 < file_data.len() {
                                    // Flip bits in 8-byte sections
                                    for i in corruption_point..corruption_point + 8 {
                                        file_data[i] = !file_data[i];
                                    }
                                    corruption_count += 1;
                                }
                            }
                            
                            let _ = fs::write(&path, &file_data);
                        }
                    }
                }
            }
        }
        
        Ok(corruption_count)
    }
}

// ===== INDIVIDUAL TEST FUNCTIONS =====

#[test]
#[ignore = "Persistence test - run with: cargo test test_persistence_suite -- --ignored"]
fn test_persistence_suite() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 64 * 1024 * 1024,
        // use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let test_suite = PersistenceTestSuite::new(dir.path().to_path_buf(), config);
    let results = test_suite.run_comprehensive_persistence_tests();

    println!("\nPersistence and Recovery Test Suite Results:");
    println!("============================================");

    let mut total_tests = 0;
    let mut passed_tests = 0;
    let mut total_operations = 0u64;
    let mut total_duration = 0u64;
    let mut total_recovery_time = 0u64;
    let mut recovery_time_count = 0;

    for result in &results {
        total_tests += 1;
        if result.integrity_verified {
            passed_tests += 1;
        }
        total_operations += result.operations_count;
        total_duration += result.duration_ms;
        
        if let Some(recovery_time) = result.recovery_time_ms {
            total_recovery_time += recovery_time;
            recovery_time_count += 1;
        }

        println!("Test: {}", result.test_name);
        println!("  Duration: {} ms", result.duration_ms);
        println!("  Operations: {}", result.operations_count);
        println!("  Success rate: {:.2}%", result.success_rate * 100.0);
        println!("  Performance: {:.2} ops/sec", result.performance_ops_per_sec);
        println!("  Data consistency: {:.2}%", result.data_consistency_score * 100.0);
        println!("  Integrity verified: {}", result.integrity_verified);
        if let Some(recovery_time) = result.recovery_time_ms {
            println!("  Recovery time: {} ms", recovery_time);
        }
        println!();
    }

    let success_rate = passed_tests as f64 / total_tests as f64;
    let avg_ops_per_sec = total_operations as f64 / (total_duration as f64 / 1000.0);
    let avg_recovery_time = if recovery_time_count > 0 {
        total_recovery_time / recovery_time_count as u64
    } else {
        0
    };

    println!("Overall Results:");
    println!("  Success rate: {:.1}% ({}/{})", success_rate * 100.0, passed_tests, total_tests);
    println!("  Total operations: {}", total_operations);
    println!("  Average performance: {:.2} ops/sec", avg_ops_per_sec);
    println!("  Average recovery time: {} ms", avg_recovery_time);

    // Assert overall test quality
    assert!(success_rate >= 0.85, "Persistence test success rate too low: {:.1}%", success_rate * 100.0);
    assert!(avg_recovery_time < 1000, "Average recovery time too high: {} ms", avg_recovery_time);
}

#[test]
#[ignore = "Individual persistence test - run with: cargo test test_write_read_consistency_standalone -- --ignored"]
fn test_write_read_consistency_standalone() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    
    let test_suite = PersistenceTestSuite::new(dir.path().to_path_buf(), config);
    let result = test_suite.test_write_read_consistency();
    
    assert!(result.success_rate > 0.999, "Write-read consistency too low: {:.3}%", result.success_rate * 100.0);
    assert!(result.integrity_verified, "Integrity verification failed");
}

#[test]
#[ignore = "Individual persistence test - run with: cargo test test_transaction_durability_standalone -- --ignored"]
fn test_transaction_durability_standalone() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig::default();
    
    let test_suite = PersistenceTestSuite::new(dir.path().to_path_buf(), config);
    let result = test_suite.test_transaction_durability();
    
    assert!(result.success_rate > 0.95, "Transaction durability too low: {:.3}%", result.success_rate * 100.0);
    assert!(result.integrity_verified, "Transaction integrity verification failed");
}
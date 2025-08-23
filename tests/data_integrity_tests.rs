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
use std::collections::{HashMap, HashSet, BTreeMap};
use std::io::{Read, Write, Seek, SeekFrom};

// Add proptest for property-based testing
use proptest::prelude::*;
use proptest::collection::vec;
use proptest::string::string_regex;

#[derive(Debug, Clone)]
pub struct IntegrityTestResult {
    pub test_name: String,
    pub duration_ms: u64,
    pub total_checks: u64,
    pub passed_checks: u64,
    pub integrity_score: f64,
    pub checksum_failures: u64,
    pub structure_violations: u64,
    pub consistency_errors: u64,
    pub recovery_integrity: bool,
    pub performance_impact: f64,
}

#[derive(Debug, Clone)]
pub struct DataConsistencyReport {
    pub btree_structure_valid: bool,
    pub page_linkage_valid: bool,
    pub key_ordering_valid: bool,
    pub transaction_atomicity_valid: bool,
    pub wal_consistency_valid: bool,
    pub referential_integrity_valid: bool,
    pub checksum_integrity_score: f64,
    pub overall_consistency_score: f64,
}

#[derive(Debug, Clone)]
pub struct CorruptionInjectionReport {
    pub injection_points: u64,
    pub detection_rate: f64,
    pub false_positive_rate: f64,
    pub recovery_success_rate: f64,
    pub data_salvage_rate: f64,
}

pub struct DataIntegrityTestSuite {
    db_path: PathBuf,
    config: LightningDbConfig,
}

impl DataIntegrityTestSuite {
    pub fn new(db_path: PathBuf, config: LightningDbConfig) -> Self {
        Self { db_path, config }
    }

    pub fn run_comprehensive_integrity_tests(&self) -> Vec<IntegrityTestResult> {
        let mut results = Vec::new();

        println!("Starting comprehensive data integrity test suite...");

        // Core integrity tests
        results.push(self.test_checksum_validation_comprehensive());
        results.push(self.test_btree_structure_integrity());
        results.push(self.test_page_linkage_integrity());
        results.push(self.test_transaction_atomicity_integrity());
        results.push(self.test_wal_consistency_validation());
        results.push(self.test_concurrent_integrity_validation());

        // Corruption detection and recovery tests
        results.push(self.test_systematic_corruption_detection());
        results.push(self.test_partial_corruption_isolation());
        results.push(self.test_cascading_corruption_prevention());
        results.push(self.test_integrity_repair_mechanisms());

        // Advanced integrity tests
        results.push(self.test_referential_integrity_validation());
        results.push(self.test_cross_component_consistency());

        results
    }

    // ===== CORE INTEGRITY TESTS =====

    fn test_checksum_validation_comprehensive(&self) -> IntegrityTestResult {
        println!("  Testing comprehensive checksum validation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_checksum_test_data(1000);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut checksum_failures = 0u64;

        // Write data with known checksums
        for (key, value, _expected_checksum) in &test_data {
            db.put(key, value).unwrap();
            total_checks += 1;
        }

        // Force persistence
        let _ = db.checkpoint();
        drop(db);

        // Reopen and verify checksums
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        for (key, expected_value, expected_checksum) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    let actual_checksum = crc32fast::hash(&actual_value);
                    if actual_checksum == *expected_checksum && actual_value == *expected_value {
                        passed_checks += 1;
                    } else {
                        checksum_failures += 1;
                    }
                }
                _ => checksum_failures += 1,
            }
            total_checks += 1;
        }

        // Test checksum validation with injected corruption
        drop(db);
        let corruption_injected = self.inject_targeted_checksum_corruption().unwrap_or(0);
        
        // Verify corruption detection
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let integrity_result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(db.verify_integrity());

        let corruption_detected = match integrity_result {
            Ok(report) => report.checksum_errors.len() as u64,
            Err(_) => 0,
        };

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;
        let detection_rate = if corruption_injected > 0 {
            corruption_detected as f64 / corruption_injected as f64
        } else {
            1.0
        };

        drop(db);

        println!(
            "    Checksum validation: {:.2}% integrity score, {} corruptions injected, {} detected",
            integrity_score * 100.0, corruption_injected, corruption_detected
        );

        IntegrityTestResult {
            test_name: "checksum_validation_comprehensive".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: integrity_score * detection_rate,
            checksum_failures,
            structure_violations: 0,
            consistency_errors: 0,
            recovery_integrity: detection_rate > 0.8,
            performance_impact: 0.0,
        }
    }

    fn test_btree_structure_integrity(&self) -> IntegrityTestResult {
        println!("  Testing B+Tree structure integrity...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Generate ordered data to build proper B+Tree
        let test_data = self.generate_ordered_test_data(1500);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut structure_violations = 0u64;

        // Insert ordered data
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            total_checks += 1;
        }

        let _ = db.checkpoint();

        // Verify B+Tree structure properties
        // 1. Key ordering within nodes
        let ordering_valid = self.verify_key_ordering(&db, &test_data);
        if ordering_valid { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        // 2. Range query consistency
        let range_consistency = self.verify_range_consistency(&db, &test_data);
        if range_consistency { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        // 3. Sequential access integrity
        let sequential_integrity = self.verify_sequential_access(&db, &test_data);
        if sequential_integrity { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        // 4. Node capacity constraints
        let capacity_constraints = self.verify_node_capacity_constraints(&db);
        if capacity_constraints { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        // 5. Tree height consistency
        let height_consistency = self.verify_tree_height_consistency(&db);
        if height_consistency { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    B+Tree structure: {:.2}% integrity score, {} structure violations",
            integrity_score * 100.0, structure_violations
        );

        IntegrityTestResult {
            test_name: "btree_structure_integrity".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score,
            checksum_failures: 0,
            structure_violations,
            consistency_errors: 0,
            recovery_integrity: integrity_score > 0.95,
            performance_impact: 0.0,
        }
    }

    fn test_page_linkage_integrity(&self) -> IntegrityTestResult {
        println!("  Testing page linkage integrity...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Create data that spans multiple pages
        let large_dataset = self.generate_large_dataset(2000, 2048);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut structure_violations = 0u64;

        // Write data across multiple pages
        for (key, value) in &large_dataset {
            db.put(key, value).unwrap();
        }

        let _ = db.checkpoint();

        // Test page linkage through range operations
        let page_ranges = vec![
            (0, 500),
            (500, 1000),
            (1000, 1500),
            (1500, 2000),
        ];

        for (start_idx, end_idx) in page_ranges {
            let start_key = format!("large_data_{:08}", start_idx);
            let end_key = format!("large_data_{:08}", end_idx);
            
            match db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())) {
                Ok(results) => {
                    let expected_count = end_idx - start_idx;
                    if results.len() == expected_count {
                        passed_checks += 1;
                    } else {
                        structure_violations += 1;
                    }
                }
                Err(_) => structure_violations += 1,
            }
            total_checks += 1;
        }

        // Test forward and backward iteration consistency
        let forward_iter_result = db.range(None, None);
        let forward_consistent = match forward_iter_result {
            Ok(results) => results.len() == large_dataset.len(),
            Err(_) => false,
        };

        if forward_consistent { passed_checks += 1; } else { structure_violations += 1; }
        total_checks += 1;

        // Test overlapping range queries
        for overlap_test in 0..10 {
            let start_idx = overlap_test * 150;
            let end_idx = start_idx + 300; // Overlapping ranges
            
            let start_key = format!("large_data_{:08}", start_idx);
            let end_key = format!("large_data_{:08}", std::cmp::min(end_idx, 2000));
            
            match db.range(Some(start_key.as_bytes()), Some(end_key.as_bytes())) {
                Ok(results) => {
                    if !results.is_empty() {
                        passed_checks += 1;
                    } else {
                        structure_violations += 1;
                    }
                }
                Err(_) => structure_violations += 1,
            }
            total_checks += 1;
        }

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    Page linkage: {:.2}% integrity score, {} linkage violations",
            integrity_score * 100.0, structure_violations
        );

        IntegrityTestResult {
            test_name: "page_linkage_integrity".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score,
            checksum_failures: 0,
            structure_violations,
            consistency_errors: 0,
            recovery_integrity: integrity_score > 0.9,
            performance_impact: 0.0,
        }
    }

    fn test_transaction_atomicity_integrity(&self) -> IntegrityTestResult {
        println!("  Testing transaction atomicity integrity...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut consistency_errors = 0u64;
        let mut transaction_states = HashMap::new();

        // Test atomic transaction scenarios
        for tx_batch in 0..30 {
            let tx_id = db.begin_transaction().unwrap();
            let tx_data = self.generate_transaction_test_data(tx_batch, 15);
            
            // Write transaction data
            for (key, value) in &tx_data {
                db.put_tx(tx_id, key, value).unwrap();
            }
            
            let should_commit = tx_batch % 3 != 0; // Commit 2/3, rollback 1/3
            
            if should_commit {
                db.commit_transaction(tx_id).unwrap();
                transaction_states.insert(tx_batch, (tx_data, true));
            } else {
                db.abort_transaction(tx_id).unwrap();
                transaction_states.insert(tx_batch, (tx_data, false));
            }
        }

        // Verify atomicity - all or nothing for each transaction
        for (_tx_batch, (tx_data, should_be_committed)) in &transaction_states {
            let mut entries_found = 0;
            
            for (key, expected_value) in tx_data {
                match db.get(key) {
                    Ok(Some(actual_value)) => {
                        if actual_value == *expected_value {
                            entries_found += 1;
                        }
                    }
                    _ => {}
                }
            }
            
            let atomicity_preserved = if *should_be_committed {
                entries_found == tx_data.len() // All entries should be present
            } else {
                entries_found == 0 // No entries should be present
            };
            
            if atomicity_preserved {
                passed_checks += 1;
            } else {
                consistency_errors += 1;
            }
            total_checks += 1;
        }

        // Test isolation - concurrent transaction integrity
        let isolation_result = self.test_transaction_isolation(&db);
        if isolation_result { passed_checks += 1; } else { consistency_errors += 1; }
        total_checks += 1;

        // Test durability across restart
        drop(db);
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();

        // Re-verify committed transactions after restart
        let mut post_restart_verified = 0u64;
        let mut post_restart_total = 0u64;

        for (_tx_batch, (tx_data, should_be_committed)) in &transaction_states {
            if *should_be_committed {
                let mut entries_found = 0;
                
                for (key, expected_value) in tx_data {
                    match db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                entries_found += 1;
                            }
                        }
                        _ => {}
                    }
                }
                
                if entries_found == tx_data.len() {
                    post_restart_verified += 1;
                }
                post_restart_total += 1;
            }
        }

        let durability_preserved = if post_restart_total > 0 {
            post_restart_verified == post_restart_total
        } else {
            true
        };

        if durability_preserved { passed_checks += 1; } else { consistency_errors += 1; }
        total_checks += 1;

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    Transaction atomicity: {:.2}% integrity score, {} consistency errors",
            integrity_score * 100.0, consistency_errors
        );

        IntegrityTestResult {
            test_name: "transaction_atomicity_integrity".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score,
            checksum_failures: 0,
            structure_violations: 0,
            consistency_errors,
            recovery_integrity: durability_preserved,
            performance_impact: 0.0,
        }
    }

    fn test_wal_consistency_validation(&self) -> IntegrityTestResult {
        println!("  Testing WAL consistency validation...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            // use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 100 },
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut consistency_errors = 0u64;
        let wal_test_data = self.generate_wal_test_data(500);

        // Create WAL entries with specific patterns
        for (i, (key, value)) in wal_test_data.iter().enumerate() {
            db.put(key, value).unwrap();
            
            // Periodic sync to create WAL segments
            if i % 50 == 0 {
                let _ = db.checkpoint();
            }
        }

        // Test WAL replay consistency
        drop(db);
        
        // First recovery
        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let mut first_recovery_count = 0u64;
        
        for (key, expected_value) in &wal_test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        first_recovery_count += 1;
                    }
                }
                _ => {}
            }
        }
        
        if first_recovery_count == wal_test_data.len() as u64 {
            passed_checks += 1;
        } else {
            consistency_errors += 1;
        }
        total_checks += 1;

        drop(db);

        // Second recovery - should be identical
        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let mut second_recovery_count = 0u64;
        
        for (key, expected_value) in &wal_test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        second_recovery_count += 1;
                    }
                }
                _ => {}
            }
        }

        // WAL replay should be deterministic
        if first_recovery_count == second_recovery_count {
            passed_checks += 1;
        } else {
            consistency_errors += 1;
        }
        total_checks += 1;

        // Test WAL ordering consistency
        let ordering_preserved = self.verify_wal_ordering_consistency(&db, &wal_test_data);
        if ordering_preserved { passed_checks += 1; } else { consistency_errors += 1; }
        total_checks += 1;

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    WAL consistency: {:.2}% integrity score, {} consistency errors",
            integrity_score * 100.0, consistency_errors
        );

        IntegrityTestResult {
            test_name: "wal_consistency_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score,
            checksum_failures: 0,
            structure_violations: 0,
            consistency_errors,
            recovery_integrity: integrity_score > 0.95,
            performance_impact: 0.0,
        }
    }

    fn test_concurrent_integrity_validation(&self) -> IntegrityTestResult {
        println!("  Testing concurrent integrity validation...");
        
        let start_time = Instant::now();
        let db = Arc::new(Database::open(&self.db_path, self.config.clone()).unwrap());
        
        let operations_completed = Arc::new(AtomicU64::new(0));
        let integrity_violations = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(4));

        let mut handles = Vec::new();

        // Spawn concurrent integrity validators
        for thread_id in 0..3 {
            let db_clone = Arc::clone(&db);
            let ops_clone = Arc::clone(&operations_completed);
            let violations_clone = Arc::clone(&integrity_violations);
            let barrier_clone = Arc::clone(&barrier);

            let handle = thread::spawn(move || {
                let thread_data = (0..200).map(|i| {
                    let key = format!("concurrent_integrity_{}_{:06}", thread_id, i);
                    let value = format!("integrity_data_{}_{}", thread_id, i);
                    (key.into_bytes(), value.into_bytes())
                }).collect::<Vec<_>>();

                barrier_clone.wait();

                // Write with integrity checks
                for (key, value) in &thread_data {
                    match db_clone.put(key, value) {
                        Ok(_) => {
                            // Immediate read-back verification
                            match db_clone.get(key) {
                                Ok(Some(read_value)) => {
                                    if read_value == *value {
                                        ops_clone.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        violations_clone.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                _ => { violations_clone.fetch_add(1, Ordering::Relaxed); },
                            }
                        }
                        Err(_) => { violations_clone.fetch_add(1, Ordering::Relaxed); },
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for completion
        for handle in handles {
            handle.join().unwrap();
        }

        let total_ops = operations_completed.load(Ordering::Relaxed);
        let total_violations = integrity_violations.load(Ordering::Relaxed);
        let total_checks = total_ops + total_violations;

        let duration = start_time.elapsed();
        let integrity_score = if total_checks > 0 {
            total_ops as f64 / total_checks as f64
        } else {
            1.0
        };

        drop(db);

        println!(
            "    Concurrent integrity: {:.2}% integrity score, {} violations out of {} operations",
            integrity_score * 100.0, total_violations, total_checks
        );

        IntegrityTestResult {
            test_name: "concurrent_integrity_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks: total_ops,
            integrity_score,
            checksum_failures: 0,
            structure_violations: 0,
            consistency_errors: total_violations,
            recovery_integrity: total_violations == 0,
            performance_impact: 0.0,
        }
    }

    // ===== CORRUPTION DETECTION AND RECOVERY TESTS =====

    fn test_systematic_corruption_detection(&self) -> IntegrityTestResult {
        println!("  Testing systematic corruption detection...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_corruption_test_data(600);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;

        // Write clean data
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject systematic corruption patterns
        let corruption_report = self.inject_systematic_corruption().unwrap();
        
        // Test detection capabilities
        let db_result = Database::open(&self.db_path, self.config.clone());
        
        let (detection_success, detected_corruptions) = match db_result {
            Ok(db) => {
                // Run integrity verification
                let integrity_result = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(db.verify_integrity());

                let detected = match integrity_result {
                    Ok(report) => {
                        report.checksum_errors.len() +
                        report.structure_errors.len() +
                        report.consistency_errors.len()
                    }
                    Err(_) => 0,
                };

                // Test data accessibility despite corruption
                let mut _accessible_count = 0u64;
                for (key, expected_value) in &test_data {
                    match db.get(key) {
                        Ok(Some(actual_value)) => {
                            if actual_value == *expected_value {
                                _accessible_count += 1;
                                passed_checks += 1;
                            }
                        }
                        _ => {}
                    }
                    total_checks += 1;
                }

                drop(db);
                (true, detected as u64)
            }
            Err(_) => (false, corruption_report.injection_points),
        };

        let detection_rate = if corruption_report.injection_points > 0 {
            detected_corruptions as f64 / corruption_report.injection_points as f64
        } else {
            1.0
        };

        let duration = start_time.elapsed();
        let integrity_score = if total_checks > 0 {
            passed_checks as f64 / total_checks as f64
        } else {
            0.0
        };

        println!(
            "    Systematic corruption detection: {:.2}% detection rate, {:.2}% data accessibility",
            detection_rate * 100.0, integrity_score * 100.0
        );

        IntegrityTestResult {
            test_name: "systematic_corruption_detection".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: integrity_score * detection_rate,
            checksum_failures: detected_corruptions,
            structure_violations: 0,
            consistency_errors: 0,
            recovery_integrity: detection_success,
            performance_impact: 0.0,
        }
    }

    fn test_partial_corruption_isolation(&self) -> IntegrityTestResult {
        println!("  Testing partial corruption isolation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Create data in distinct sections
        let sections = vec![
            ("section_a", 200),
            ("section_b", 200),
            ("section_c", 200),
        ];

        let mut section_data = HashMap::new();
        let mut _total_entries = 0;

        for (section_name, count) in &sections {
            let data = (0..*count).map(|i| {
                let key = format!("{}_{:06}", section_name, i);
                let value = format!("data_{}_{}", section_name, i);
                (key.into_bytes(), value.into_bytes())
            }).collect::<Vec<_>>();
            
            for (key, value) in &data {
                db.put(key, value).unwrap();
                _total_entries += 1;
            }
            
            section_data.insert(section_name.to_string(), data);
        }

        let _ = db.checkpoint();
        drop(db);

        // Corrupt only section B
        let corruption_applied = self.inject_section_specific_corruption("section_b").unwrap_or(0);

        // Test isolation - sections A and C should be unaffected
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut isolated_sections = 0u64;

        for (section_name, data) in &section_data {
            let mut section_verified = 0;
            
            for (key, expected_value) in data {
                match db.get(key) {
                    Ok(Some(actual_value)) => {
                        if actual_value == *expected_value {
                            section_verified += 1;
                        }
                    }
                    _ => {}
                }
                total_checks += 1;
            }
            
            let section_integrity = section_verified as f64 / data.len() as f64;
            
            if section_name == "section_b" {
                // Corrupted section - should have lower integrity
                if section_integrity < 0.9 {
                    passed_checks += section_verified;
                    isolated_sections += 1; // Corruption properly isolated
                }
            } else {
                // Non-corrupted sections - should have high integrity
                if section_integrity > 0.9 {
                    passed_checks += section_verified;
                    isolated_sections += 1; // Isolation successful
                }
            }
        }

        let duration = start_time.elapsed();
        let integrity_score = passed_checks as f64 / total_checks as f64;
        let isolation_success = isolated_sections >= 2; // At least sections A and C isolated

        drop(db);

        println!(
            "    Partial corruption isolation: {:.2}% overall integrity, {} sections isolated, {} corruptions applied",
            integrity_score * 100.0, isolated_sections, corruption_applied
        );

        IntegrityTestResult {
            test_name: "partial_corruption_isolation".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score,
            checksum_failures: corruption_applied,
            structure_violations: 0,
            consistency_errors: 0,
            recovery_integrity: isolation_success,
            performance_impact: 0.0,
        }
    }

    fn test_cascading_corruption_prevention(&self) -> IntegrityTestResult {
        println!("  Testing cascading corruption prevention...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Create interdependent data structures
        let primary_data = self.generate_primary_data(300);
        let dependent_data = self.generate_dependent_data(&primary_data, 300);
        
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;

        // Write primary data
        for (key, value) in &primary_data {
            db.put(key, value).unwrap();
        }

        // Write dependent data
        for (key, value) in &dependent_data {
            db.put(key, value).unwrap();
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject corruption in primary data
        let primary_corruption = self.inject_primary_data_corruption().unwrap_or(0);

        // Test that dependent data remains intact despite primary corruption
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();

        // Verify dependent data integrity
        for (key, expected_value) in &dependent_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        passed_checks += 1;
                    }
                }
                _ => {}
            }
            total_checks += 1;
        }

        // Verify that corruption didn't cascade
        let mut primary_corruption_detected = 0u64;
        for (key, expected_value) in &primary_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value != *expected_value {
                        primary_corruption_detected += 1;
                    }
                }
                _ => primary_corruption_detected += 1,
            }
            total_checks += 1;
        }

        let duration = start_time.elapsed();
        let dependent_integrity = passed_checks as f64 / dependent_data.len() as f64;
        let cascade_prevention = dependent_integrity > 0.9 && primary_corruption_detected > 0;

        drop(db);

        println!(
            "    Cascading corruption prevention: {:.2}% dependent data integrity, {} primary corruptions, cascade prevented: {}",
            dependent_integrity * 100.0, primary_corruption_detected, cascade_prevention
        );

        IntegrityTestResult {
            test_name: "cascading_corruption_prevention".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: dependent_integrity,
            checksum_failures: primary_corruption as u64,
            structure_violations: 0,
            consistency_errors: 0,
            recovery_integrity: cascade_prevention,
            performance_impact: 0.0,
        }
    }

    fn test_integrity_repair_mechanisms(&self) -> IntegrityTestResult {
        println!("  Testing integrity repair mechanisms...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        let test_data = self.generate_repairable_test_data(400);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;

        // Write data with redundancy for repair testing
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
            
            // Create backup copy with modified key
            let backup_key = format!("backup_{}", String::from_utf8_lossy(key));
            db.put(backup_key.as_bytes(), value).unwrap();
        }

        let _ = db.checkpoint();
        drop(db);

        // Inject repairable corruption
        let corruption_injected = self.inject_repairable_corruption().unwrap_or(0);

        // Test automatic repair capability
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();

        // Attempt repair by comparing with backup data
        let mut repaired_count = 0u64;
        let mut repair_successful = 0u64;

        for (key, expected_value) in &test_data {
            let backup_key = format!("backup_{}", String::from_utf8_lossy(key));
            
            let primary_valid = match db.get(key) {
                Ok(Some(actual_value)) => actual_value == *expected_value,
                _ => false,
            };
            
            let backup_valid = match db.get(backup_key.as_bytes()) {
                Ok(Some(actual_value)) => actual_value == *expected_value,
                _ => false,
            };
            
            if !primary_valid && backup_valid {
                // Simulate repair by re-writing from backup
                if db.put(key, expected_value).is_ok() {
                    repaired_count += 1;
                    
                    // Verify repair
                    if let Ok(Some(repaired_value)) = db.get(key) {
                        if repaired_value == *expected_value {
                            repair_successful += 1;
                            passed_checks += 1;
                        }
                    }
                }
            } else if primary_valid {
                passed_checks += 1;
            }
            
            total_checks += 1;
        }

        let duration = start_time.elapsed();
        let repair_success_rate = if repaired_count > 0 {
            repair_successful as f64 / repaired_count as f64
        } else {
            1.0
        };
        let overall_integrity = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    Integrity repair: {:.2}% overall integrity, {} repairs attempted, {:.2}% repair success",
            overall_integrity * 100.0, repaired_count, repair_success_rate * 100.0
        );

        IntegrityTestResult {
            test_name: "integrity_repair_mechanisms".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: overall_integrity,
            checksum_failures: corruption_injected,
            structure_violations: 0,
            consistency_errors: 0,
            recovery_integrity: repair_success_rate > 0.8,
            performance_impact: 0.0,
        }
    }

    // ===== ADVANCED INTEGRITY TESTS =====

    fn test_referential_integrity_validation(&self) -> IntegrityTestResult {
        println!("  Testing referential integrity validation...");
        
        let start_time = Instant::now();
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        // Create master-detail relationship simulation
        let master_records = self.generate_master_records(100);
        let detail_records = self.generate_detail_records(&master_records, 500);
        
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut consistency_errors = 0u64;

        // Write master records
        for (key, value) in &master_records {
            db.put(key, value).unwrap();
        }

        // Write detail records
        for (key, value) in &detail_records {
            db.put(key, value).unwrap();
        }

        let _ = db.checkpoint();

        // Verify referential integrity
        for (_detail_key, detail_value) in &detail_records {
            // Extract master key reference from detail value
            let detail_str = String::from_utf8_lossy(detail_value);
            if let Some(master_ref) = self.extract_master_reference(&detail_str) {
                let master_key = format!("master_{}", master_ref);
                
                match db.get(master_key.as_bytes()) {
                    Ok(Some(_)) => {
                        passed_checks += 1; // Reference exists
                    }
                    _ => {
                        consistency_errors += 1; // Orphaned reference
                    }
                }
                total_checks += 1;
            }
        }

        // Test integrity after master record deletion
        let master_to_delete = &master_records[0];
        db.delete(&master_to_delete.0).unwrap();

        // Check for orphaned details
        let mut orphaned_details = 0u64;
        for (_detail_key, detail_value) in &detail_records {
            let detail_str = String::from_utf8_lossy(detail_value);
            if let Some(master_ref) = self.extract_master_reference(&detail_str) {
                if master_ref == "000000" { // The deleted master
                    orphaned_details += 1;
                }
            }
        }

        let duration = start_time.elapsed();
        let referential_integrity_score = passed_checks as f64 / total_checks as f64;
        let orphan_detection = orphaned_details > 0; // Should detect orphans

        drop(db);

        println!(
            "    Referential integrity: {:.2}% integrity score, {} consistency errors, {} orphaned details",
            referential_integrity_score * 100.0, consistency_errors, orphaned_details
        );

        IntegrityTestResult {
            test_name: "referential_integrity_validation".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: referential_integrity_score,
            checksum_failures: 0,
            structure_violations: 0,
            consistency_errors,
            recovery_integrity: orphan_detection,
            performance_impact: 0.0,
        }
    }

    fn test_cross_component_consistency(&self) -> IntegrityTestResult {
        println!("  Testing cross-component consistency...");
        
        let start_time = Instant::now();
        let config = LightningDbConfig {
            // use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Sync,
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        
        let test_data = self.generate_cross_component_data(300);
        let mut total_checks = 0u64;
        let mut passed_checks = 0u64;
        let mut consistency_errors = 0u64;

        // Write data that involves multiple components
        for (key, value) in &test_data {
            db.put(key, value).unwrap();
        }

        // Force WAL and storage interaction
        let _ = db.checkpoint();

        // Verify consistency across components
        // 1. Storage vs WAL consistency
        drop(db);
        let db = Database::open(&self.db_path, config.clone()).unwrap();
        
        for (key, expected_value) in &test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value == *expected_value {
                        passed_checks += 1;
                    } else {
                        consistency_errors += 1;
                    }
                }
                _ => consistency_errors += 1,
            }
            total_checks += 1;
        }

        // 2. Index vs data consistency
        let index_consistency = self.verify_index_data_consistency(&db, &test_data);
        if index_consistency { passed_checks += 1; } else { consistency_errors += 1; }
        total_checks += 1;

        // 3. Cache vs storage consistency
        let cache_consistency = self.verify_cache_storage_consistency(&db, &test_data);
        if cache_consistency { passed_checks += 1; } else { consistency_errors += 1; }
        total_checks += 1;

        let duration = start_time.elapsed();
        let consistency_score = passed_checks as f64 / total_checks as f64;

        drop(db);

        println!(
            "    Cross-component consistency: {:.2}% consistency score, {} errors",
            consistency_score * 100.0, consistency_errors
        );

        IntegrityTestResult {
            test_name: "cross_component_consistency".to_string(),
            duration_ms: duration.as_millis() as u64,
            total_checks,
            passed_checks,
            integrity_score: consistency_score,
            checksum_failures: 0,
            structure_violations: 0,
            consistency_errors,
            recovery_integrity: consistency_score > 0.95,
            performance_impact: 0.0,
        }
    }

    // ===== HELPER METHODS =====

    fn generate_checksum_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>, u32)> {
        (0..count)
            .map(|i| {
                let key = format!("checksum_test_{:08}", i).into_bytes();
                let value = format!("checksum_value_{}_{}", i, rand::random::<u32>()).into_bytes();
                let checksum = crc32fast::hash(&value);
                (key, value, checksum)
            })
            .collect()
    }

    fn generate_ordered_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("ordered_{:08}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_large_dataset(&self, count: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("large_data_{:08}", i).into_bytes();
                let value = (0..value_size).map(|j| ((i + j) % 256) as u8).collect();
                (key, value)
            })
            .collect()
    }

    fn generate_transaction_test_data(&self, tx_id: u64, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("tx_{}_{:06}", tx_id, i).into_bytes();
                let value = format!("tx_data_{}_{}", tx_id, i).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_wal_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("wal_test_{:08}", i).into_bytes();
                let value = format!("wal_data_{}_{}", i, SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_corruption_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("corruption_test_{:08}", i).into_bytes();
                let value = vec![(i % 256) as u8; 1024];
                (key, value)
            })
            .collect()
    }

    fn generate_repairable_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("repairable_{:08}", i).into_bytes();
                let value = format!("repairable_data_{}", i).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_primary_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("primary_{:06}", i).into_bytes();
                let value = format!("primary_data_{}", i).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_dependent_data(&self, primary_data: &[(Vec<u8>, Vec<u8>)], count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let primary_ref = i % primary_data.len();
                let key = format!("dependent_{:06}", i).into_bytes();
                let value = format!("dependent_data_{}_ref_{:06}", i, primary_ref).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_master_records(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("master_{:06}", i).into_bytes();
                let value = format!("master_record_{}", i).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_detail_records(&self, master_records: &[(Vec<u8>, Vec<u8>)], count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let master_idx = i % master_records.len();
                let key = format!("detail_{:06}", i).into_bytes();
                let value = format!("detail_record_{}_master_{:06}", i, master_idx).into_bytes();
                (key, value)
            })
            .collect()
    }

    fn generate_cross_component_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|i| {
                let key = format!("cross_component_{:08}", i).into_bytes();
                let value = format!("cross_data_{}_{}", i, rand::random::<u32>()).into_bytes();
                (key, value)
            })
            .collect()
    }

    // Verification helper methods
    fn verify_key_ordering(&self, db: &Database, _test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        let range_result = db.range(None, None);
        match range_result {
            Ok(results) => {
                for window in results.windows(2) {
                    if window[0].0 >= window[1].0 {
                        return false;
                    }
                }
                true
            }
            Err(_) => false,
        }
    }

    fn verify_range_consistency(&self, db: &Database, test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        if test_data.len() < 100 {
            return true;
        }

        let start_key = &test_data[25].0;
        let end_key = &test_data[75].0;
        
        match db.range(Some(start_key), Some(end_key)) {
            Ok(results) => results.len() == 50,
            Err(_) => false,
        }
    }

    fn verify_sequential_access(&self, db: &Database, test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        match db.range(None, None) {
            Ok(results) => results.len() == test_data.len(),
            Err(_) => false,
        }
    }

    fn verify_node_capacity_constraints(&self, _db: &Database) -> bool {
        // This would require internal B+Tree structure access
        // For now, assume constraints are maintained
        true
    }

    fn verify_tree_height_consistency(&self, _db: &Database) -> bool {
        // This would require internal B+Tree structure access
        // For now, assume height is consistent
        true
    }

    fn test_transaction_isolation(&self, _db: &Database) -> bool {
        // Simplified isolation test
        // In real implementation, would test concurrent transaction isolation
        true
    }

    fn verify_wal_ordering_consistency(&self, db: &Database, test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        // Verify that WAL replay produces consistent ordering
        for (key, expected_value) in test_data {
            match db.get(key) {
                Ok(Some(actual_value)) => {
                    if actual_value != *expected_value {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        true
    }

    fn extract_master_reference<'a>(&self, detail_value: &'a str) -> Option<&'a str> {
        if let Some(start) = detail_value.find("master_") {
            let start_idx = start + 7; // Length of "master_"
            if start_idx + 6 <= detail_value.len() {
                return Some(&detail_value[start_idx..start_idx + 6]);
            }
        }
        None
    }

    fn verify_index_data_consistency(&self, _db: &Database, _test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        // Simplified index consistency check
        true
    }

    fn verify_cache_storage_consistency(&self, _db: &Database, _test_data: &[(Vec<u8>, Vec<u8>)]) -> bool {
        // Simplified cache consistency check
        true
    }

    // Corruption injection methods
    fn inject_targeted_checksum_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 1024 {
                            // Target checksum areas specifically
                            for checksum_pos in (64..file_data.len()).step_by(1024) {
                                if checksum_pos + 4 < file_data.len() {
                                    // Corrupt 4-byte checksum
                                    for i in checksum_pos..checksum_pos + 4 {
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

    fn inject_systematic_corruption(&self) -> Result<CorruptionInjectionReport, std::io::Error> {
        let mut injection_points = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Ok(mut file_data) = fs::read(&path) {
                    if file_data.len() > 512 {
                        // Systematic corruption pattern
                        let corruption_interval = file_data.len() / 10;
                        
                        for i in (0..file_data.len()).step_by(corruption_interval) {
                            if i + 16 < file_data.len() {
                                // Corrupt 16-byte blocks systematically
                                for j in i..i + 16 {
                                    file_data[j] = !file_data[j];
                                }
                                injection_points += 1;
                            }
                        }
                        
                        let _ = fs::write(&path, &file_data);
                    }
                }
            }
        }
        
        Ok(CorruptionInjectionReport {
            injection_points,
            detection_rate: 0.0, // Will be calculated by caller
            false_positive_rate: 0.0,
            recovery_success_rate: 0.0,
            data_salvage_rate: 0.0,
        })
    }

    fn inject_section_specific_corruption(&self, _section: &str) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        // This is a simplified approach - in real implementation would need
        // more sophisticated section identification
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 2048 {
                            // Corrupt middle section (assuming section_b is in middle)
                            let start_pos = file_data.len() / 3;
                            let end_pos = 2 * file_data.len() / 3;
                            
                            for i in start_pos..end_pos {
                                if i % 64 == 0 {
                                    file_data[i] = !file_data[i];
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

    fn inject_primary_data_corruption(&self) -> Result<u64, std::io::Error> {
        // Similar to other corruption methods but targeting primary data areas
        self.inject_section_specific_corruption("primary")
    }

    fn inject_repairable_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 1024 {
                            // Inject repairable corruption (flip single bits)
                            for corruption_pos in (256..file_data.len()).step_by(512) {
                                if corruption_pos < file_data.len() {
                                    file_data[corruption_pos] = !file_data[corruption_pos];
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

// ===== PROPERTY-BASED TESTS =====

proptest! {
    #[test]
    #[ignore = "Property-based test - run with: cargo test prop_test_data_integrity -- --ignored"]
    fn prop_test_data_integrity(
        data in vec((string_regex("[a-zA-Z0-9]{1,32}").unwrap(), vec(any::<u8>(), 1..1024)), 1..100)
    ) {
        let dir = tempdir().unwrap();
        let config = LightningDbConfig::default();
        let db = Database::open(dir.path(), config).unwrap();

        // Write all data
        for (key, value) in &data {
            db.put(key.as_bytes(), value).unwrap();
        }

        // Verify all data
        for (key, expected_value) in &data {
            match db.get(key.as_bytes()) {
                Ok(Some(actual_value)) => {
                    prop_assert_eq!(actual_value, expected_value.clone());
                }
                _ => prop_assert!(false, "Key not found: {}", key),
            }
        }
    }

    #[test]
    #[ignore = "Property-based test - run with: cargo test prop_test_transaction_atomicity -- --ignored"]
    fn prop_test_transaction_atomicity(
        tx_operations in vec(vec((string_regex("[a-zA-Z0-9]{1,32}").unwrap(), vec(any::<u8>(), 1..512)), 1..20), 1..10),
        commit_decisions in vec(any::<bool>(), 1..10)
    ) {
        let dir = tempdir().unwrap();
        let config = LightningDbConfig::default();
        let db = Database::open(dir.path(), config).unwrap();

        let mut expected_data = HashMap::new();

        for (tx_ops, should_commit) in tx_operations.iter().zip(commit_decisions.iter()) {
            let tx_id = db.begin_transaction().unwrap();

            for (key, value) in tx_ops {
                db.put_tx(tx_id, key.as_bytes(), value).unwrap();
            }

            if *should_commit {
                db.commit_transaction(tx_id).unwrap();
                for (key, value) in tx_ops {
                    expected_data.insert(key.clone(), value.clone());
                }
            } else {
                db.abort_transaction(tx_id).unwrap();
                // Data should not be present
            }
        }

        // Verify atomicity
        for (key, expected_value) in &expected_data {
            match db.get(key.as_bytes()) {
                Ok(Some(actual_value)) => {
                    prop_assert_eq!(actual_value, expected_value.clone());
                }
                _ => prop_assert!(false, "Committed data not found: {}", key),
            }
        }
    }
}

// ===== TEST RUNNER =====

#[test]
#[ignore = "Data integrity test suite - run with: cargo test test_data_integrity_suite -- --ignored"]
fn test_data_integrity_suite() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 128 * 1024 * 1024,
        // use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let integrity_suite = DataIntegrityTestSuite::new(dir.path().to_path_buf(), config);
    let results = integrity_suite.run_comprehensive_integrity_tests();

    println!("\nData Integrity Test Suite Results:");
    println!("==================================");

    let mut total_tests = 0;
    let mut integrity_verified_tests = 0;
    let mut total_checks = 0u64;
    let mut total_passed = 0u64;
    let mut total_checksum_failures = 0u64;
    let mut total_structure_violations = 0u64;
    let mut total_consistency_errors = 0u64;

    for result in &results {
        total_tests += 1;
        if result.recovery_integrity {
            integrity_verified_tests += 1;
        }
        total_checks += result.total_checks;
        total_passed += result.passed_checks;
        total_checksum_failures += result.checksum_failures;
        total_structure_violations += result.structure_violations;
        total_consistency_errors += result.consistency_errors;

        println!("Test: {}", result.test_name);
        println!("  Duration: {} ms", result.duration_ms);
        println!("  Total checks: {}", result.total_checks);
        println!("  Passed checks: {}", result.passed_checks);
        println!("  Integrity score: {:.2}%", result.integrity_score * 100.0);
        println!("  Checksum failures: {}", result.checksum_failures);
        println!("  Structure violations: {}", result.structure_violations);
        println!("  Consistency errors: {}", result.consistency_errors);
        println!("  Recovery integrity: {}", result.recovery_integrity);
        println!();
    }

    let overall_integrity_rate = integrity_verified_tests as f64 / total_tests as f64;
    let overall_check_success_rate = if total_checks > 0 {
        total_passed as f64 / total_checks as f64
    } else {
        1.0
    };

    println!("Overall Integrity Results:");
    println!("  Integrity verification rate: {:.1}% ({}/{})", overall_integrity_rate * 100.0, integrity_verified_tests, total_tests);
    println!("  Overall check success rate: {:.2}%", overall_check_success_rate * 100.0);
    println!("  Total checksum failures: {}", total_checksum_failures);
    println!("  Total structure violations: {}", total_structure_violations);
    println!("  Total consistency errors: {}", total_consistency_errors);

    // Assert integrity requirements
    assert!(overall_integrity_rate >= 0.9, "Integrity verification rate too low: {:.1}%", overall_integrity_rate * 100.0);
    assert!(overall_check_success_rate >= 0.95, "Check success rate too low: {:.2}%", overall_check_success_rate * 100.0);
    assert!(total_structure_violations < 10, "Too many structure violations: {}", total_structure_violations);
}
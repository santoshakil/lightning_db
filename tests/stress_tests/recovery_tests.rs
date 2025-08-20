use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use std::path::PathBuf;
use std::fs;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct RecoveryTestResult {
    pub test_name: String,
    pub recovery_time_ms: u64,
    pub data_recovery_rate: f64,
    pub transaction_recovery_rate: f64,
    pub corruption_detected: u64,
    pub corruption_repaired: u64,
    pub backup_restore_success: bool,
    pub performance_after_recovery: f64,
    pub integrity_check_passed: bool,
}

#[derive(Debug, Clone)]
pub struct RecoveryMetrics {
    pub total_operations_before: u64,
    pub committed_operations_before: u64,
    pub recovered_operations: u64,
    pub lost_operations: u64,
    pub corrupted_entries: u64,
    pub repaired_entries: u64,
    pub recovery_start_time: SystemTime,
    pub recovery_end_time: SystemTime,
}

pub struct RecoveryTestSuite {
    db_path: PathBuf,
    config: LightningDbConfig,
}

impl RecoveryTestSuite {
    pub fn new(db_path: PathBuf, config: LightningDbConfig) -> Self {
        Self { db_path, config }
    }

    pub fn run_recovery_test_suite(&self) -> Vec<RecoveryTestResult> {
        let mut results = Vec::new();

        println!("Starting recovery test suite...");

        // Test 1: Crash recovery time measurement
        results.push(self.test_crash_recovery_time());

        // Test 2: Corruption detection and recovery
        results.push(self.test_corruption_recovery());

        // Test 3: Transaction recovery validation
        results.push(self.test_transaction_recovery());

        // Test 4: Backup and restore performance
        results.push(self.test_backup_restore_performance());

        // Test 5: Data integrity after recovery
        results.push(self.test_data_integrity_after_recovery());

        // Test 6: Partial corruption recovery
        results.push(self.test_partial_corruption_recovery());

        // Test 7: WAL recovery stress test
        results.push(self.test_wal_recovery_stress());

        // Test 8: Large database recovery
        results.push(self.test_large_database_recovery());

        results
    }

    fn test_crash_recovery_time(&self) -> RecoveryTestResult {
        println!("  Testing crash recovery time...");

        let recovery_metrics = self.setup_database_for_crash_test();
        
        // Simulate crash by dropping database without proper shutdown
        let crash_time = Instant::now();
        
        // Recovery phase
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        match recovery_result {
            Ok(recovered_db) => {
                // Verify recovery quality
                let verification_result = self.verify_recovery_quality(&recovered_db, &recovery_metrics);
                
                // Test performance after recovery
                let perf_start = Instant::now();
                for i in 0..1000 {
                    let key = format!("perf_test_{:06}", i);
                    let value = format!("perf_value_{}", i);
                    let _ = recovered_db.put(key.as_bytes(), value.as_bytes());
                }
                let perf_time = perf_start.elapsed();
                let ops_per_sec = 1000.0 / perf_time.as_secs_f64();

                println!(
                    "    Crash recovery: {:?} recovery time, {:.2}% data recovered, {:.0} ops/sec after recovery",
                    recovery_time, verification_result.data_recovery_rate * 100.0, ops_per_sec
                );

                RecoveryTestResult {
                    test_name: "crash_recovery_time".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: verification_result.data_recovery_rate,
                    transaction_recovery_rate: verification_result.transaction_recovery_rate,
                    corruption_detected: 0,
                    corruption_repaired: 0,
                    backup_restore_success: true,
                    performance_after_recovery: ops_per_sec,
                    integrity_check_passed: verification_result.data_recovery_rate > 0.9,
                }
            }
            Err(e) => {
                println!("    Crash recovery failed: {:?}", e);
                RecoveryTestResult {
                    test_name: "crash_recovery_time".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: 0.0,
                    transaction_recovery_rate: 0.0,
                    corruption_detected: 1,
                    corruption_repaired: 0,
                    backup_restore_success: false,
                    performance_after_recovery: 0.0,
                    integrity_check_passed: false,
                }
            }
        }
    }

    fn setup_database_for_crash_test(&self) -> RecoveryMetrics {
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut total_ops = 0u64;
        let mut committed_ops = 0u64;

        // Insert baseline data
        for i in 0..500 {
            let key = format!("crash_baseline_{:06}", i);
            let value = format!("baseline_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
            total_ops += 1;
            committed_ops += 1;
        }

        // Create some transactions
        for tx_batch in 0..10 {
            let tx_id = db.begin_transaction().unwrap();
            
            for i in 0..20 {
                let key = format!("crash_tx_{}_{:03}", tx_batch, i);
                let value = format!("tx_value_{}_{}", tx_batch, i);
                db.put_tx(tx_id, key.as_bytes(), value.as_bytes()).unwrap();
                total_ops += 1;
            }
            
            // Commit only some transactions
            if tx_batch % 2 == 0 {
                db.commit_transaction(tx_id).unwrap();
                committed_ops += 20;
            } else {
                // Leave some transactions uncommitted to test rollback
                std::mem::forget(tx_id);
            }
        }

        // Force some data to disk
        let _ = db.checkpoint();
        
        // Add some more uncommitted data
        for i in 500..600 {
            let key = format!("crash_uncommitted_{:06}", i);
            let value = format!("uncommitted_value_{}", i);
            let _ = db.put(key.as_bytes(), value.as_bytes());
            total_ops += 1;
        }

        drop(db); // Simulate crash

        RecoveryMetrics {
            total_operations_before: total_ops,
            committed_operations_before: committed_ops,
            recovered_operations: 0,
            lost_operations: 0,
            corrupted_entries: 0,
            repaired_entries: 0,
            recovery_start_time: SystemTime::now(),
            recovery_end_time: SystemTime::now(),
        }
    }

    fn verify_recovery_quality(&self, db: &Database, metrics: &RecoveryMetrics) -> RecoveryVerification {
        let mut recovered_baseline = 0;
        let mut recovered_tx = 0;
        let mut recovered_uncommitted = 0;

        // Check baseline data
        for i in 0..500 {
            let key = format!("crash_baseline_{:06}", i);
            if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                recovered_baseline += 1;
            }
        }

        // Check committed transaction data
        for tx_batch in 0..10 {
            if tx_batch % 2 == 0 { // These were committed
                for i in 0..20 {
                    let key = format!("crash_tx_{}_{:03}", tx_batch, i);
                    if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                        recovered_tx += 1;
                    }
                }
            }
        }

        // Check uncommitted data (should mostly be lost)
        for i in 500..600 {
            let key = format!("crash_uncommitted_{:06}", i);
            if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                recovered_uncommitted += 1;
            }
        }

        let total_recovered = recovered_baseline + recovered_tx + recovered_uncommitted;
        let data_recovery_rate = total_recovered as f64 / metrics.total_operations_before as f64;
        let tx_recovery_rate = recovered_tx as f64 / 100.0; // 5 committed transactions * 20 ops each

        RecoveryVerification {
            data_recovery_rate,
            transaction_recovery_rate: tx_recovery_rate,
        }
    }

    fn test_corruption_recovery(&self) -> RecoveryTestResult {
        println!("  Testing corruption detection and recovery...");

        // Setup database with known data
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        for i in 0..200 {
            let key = format!("corruption_test_{:06}", i);
            let value = format!("original_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        let _ = db.checkpoint();
        drop(db);

        // Inject corruption
        let corruption_count = self.inject_controlled_corruption().unwrap_or(0);

        // Attempt recovery
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        match recovery_result {
            Ok(recovered_db) => {
                // Run integrity check if available
                let integrity_result = tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(recovered_db.verify_integrity());

                let (corruption_detected, integrity_passed) = match integrity_result {
                    Ok(report) => {
                        let total_errors = report.checksum_errors.len() +
                            report.structure_errors.len() +
                            report.consistency_errors.len();
                        (total_errors as u64, total_errors == 0)
                    }
                    Err(_) => (corruption_count, false),
                };

                // Verify data recovery
                let mut recovered_count = 0;
                for i in 0..200 {
                    let key = format!("corruption_test_{:06}", i);
                    if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                        recovered_count += 1;
                    }
                }

                let recovery_rate = recovered_count as f64 / 200.0;

                println!(
                    "    Corruption recovery: {} corruptions injected, {} detected, {:.2}% data recovered",
                    corruption_count, corruption_detected, recovery_rate * 100.0
                );

                RecoveryTestResult {
                    test_name: "corruption_recovery".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: recovery_rate,
                    transaction_recovery_rate: 1.0,
                    corruption_detected,
                    corruption_repaired: corruption_count.saturating_sub(corruption_detected),
                    backup_restore_success: true,
                    performance_after_recovery: 0.0,
                    integrity_check_passed: integrity_passed,
                }
            }
            Err(_) => {
                RecoveryTestResult {
                    test_name: "corruption_recovery".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: 0.0,
                    transaction_recovery_rate: 0.0,
                    corruption_detected: corruption_count,
                    corruption_repaired: 0,
                    backup_restore_success: false,
                    performance_after_recovery: 0.0,
                    integrity_check_passed: false,
                }
            }
        }
    }

    fn inject_controlled_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;

        // Find database files and inject controlled corruption
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" || extension == "wal" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 1024 {
                            // Corrupt a few bytes in the middle
                            let corrupt_pos = file_data.len() / 2;
                            file_data[corrupt_pos] = !file_data[corrupt_pos];
                            file_data[corrupt_pos + 1] = !file_data[corrupt_pos + 1];
                            
                            if fs::write(&path, &file_data).is_ok() {
                                corruption_count += 1;
                                println!("    Injected corruption into {:?}", path);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(corruption_count)
    }

    fn test_transaction_recovery(&self) -> RecoveryTestResult {
        println!("  Testing transaction recovery validation...");

        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut committed_tx_count = 0;
        let mut uncommitted_tx_count = 0;

        // Create a mix of committed and uncommitted transactions
        for tx_id in 0..20 {
            let db_tx_id = db.begin_transaction().unwrap();
            
            // Add data to transaction
            for op in 0..10 {
                let key = format!("tx_recovery_{}_{:03}", tx_id, op);
                let value = format!("tx_value_{}_{}", tx_id, op);
                db.put_tx(db_tx_id, key.as_bytes(), value.as_bytes()).unwrap();
            }
            
            if tx_id % 3 != 0 { // Commit 2/3 of transactions
                db.commit_transaction(db_tx_id).unwrap();
                committed_tx_count += 1;
            } else {
                // Leave 1/3 uncommitted
                uncommitted_tx_count += 1;
                std::mem::forget(db_tx_id); // Simulate crash before commit
            }
        }

        drop(db); // Simulate crash

        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify transaction recovery
        let mut recovered_committed = 0;
        let mut recovered_uncommitted = 0;

        for tx_id in 0..20 {
            let mut tx_ops_found = 0;
            
            for op in 0..10 {
                let key = format!("tx_recovery_{}_{:03}", tx_id, op);
                if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                    tx_ops_found += 1;
                }
            }
            
            if tx_ops_found == 10 {
                if tx_id % 3 != 0 {
                    recovered_committed += 1;
                } else {
                    recovered_uncommitted += 1;
                }
            }
        }

        let tx_recovery_rate = if committed_tx_count > 0 {
            recovered_committed as f64 / committed_tx_count as f64
        } else {
            1.0
        };

        let atomicity_preserved = recovered_uncommitted == 0;

        println!(
            "    Transaction recovery: {}/{} committed tx recovered, {} uncommitted recovered (should be 0), atomicity: {}",
            recovered_committed, committed_tx_count, recovered_uncommitted, atomicity_preserved
        );

        RecoveryTestResult {
            test_name: "transaction_recovery".to_string(),
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_recovery_rate: tx_recovery_rate,
            transaction_recovery_rate: tx_recovery_rate,
            corruption_detected: 0,
            corruption_repaired: 0,
            backup_restore_success: atomicity_preserved,
            performance_after_recovery: 0.0,
            integrity_check_passed: atomicity_preserved && tx_recovery_rate > 0.9,
        }
    }

    fn test_backup_restore_performance(&self) -> RecoveryTestResult {
        println!("  Testing backup and restore performance...");

        // Create database with substantial data
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        for i in 0..1000 {
            let key = format!("backup_test_{:06}", i);
            let value = vec![(i % 256) as u8; 1024]; // 1KB values
            db.put(key.as_bytes(), &value).unwrap();
        }

        // Force checkpoint to ensure data is written
        let _ = db.checkpoint();
        drop(db);

        // Measure backup time (simulation - copy files)
        let backup_start = Instant::now();
        let backup_dir = tempdir().unwrap();
        
        let backup_success = self.simulate_backup(&backup_dir.path()).unwrap_or(false);
        let backup_time = backup_start.elapsed();

        if !backup_success {
            return RecoveryTestResult {
                test_name: "backup_restore_performance".to_string(),
                recovery_time_ms: 0,
                data_recovery_rate: 0.0,
                transaction_recovery_rate: 0.0,
                corruption_detected: 0,
                corruption_repaired: 0,
                backup_restore_success: false,
                performance_after_recovery: 0.0,
                integrity_check_passed: false,
            };
        }

        // Simulate database loss
        let _ = fs::remove_dir_all(&self.db_path);
        let _ = fs::create_dir_all(&self.db_path);

        // Measure restore time
        let restore_start = Instant::now();
        let restore_success = self.simulate_restore(&backup_dir.path()).unwrap_or(false);
        let restore_time = restore_start.elapsed();

        if !restore_success {
            return RecoveryTestResult {
                test_name: "backup_restore_performance".to_string(),
                recovery_time_ms: restore_time.as_millis() as u64,
                data_recovery_rate: 0.0,
                transaction_recovery_rate: 0.0,
                corruption_detected: 0,
                corruption_repaired: 0,
                backup_restore_success: false,
                performance_after_recovery: 0.0,
                integrity_check_passed: false,
            };
        }

        // Verify restored data
        let restored_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut verified_count = 0;

        for i in 0..1000 {
            let key = format!("backup_test_{:06}", i);
            if let Ok(Some(value)) = restored_db.get(key.as_bytes()) {
                if value.len() == 1024 && value[0] == (i % 256) as u8 {
                    verified_count += 1;
                }
            }
        }

        let recovery_rate = verified_count as f64 / 1000.0;
        let total_time = backup_time + restore_time;

        println!(
            "    Backup/restore: backup {:?}, restore {:?}, {:.2}% data verified",
            backup_time, restore_time, recovery_rate * 100.0
        );

        RecoveryTestResult {
            test_name: "backup_restore_performance".to_string(),
            recovery_time_ms: total_time.as_millis() as u64,
            data_recovery_rate: recovery_rate,
            transaction_recovery_rate: recovery_rate,
            corruption_detected: 0,
            corruption_repaired: 0,
            backup_restore_success: recovery_rate > 0.99,
            performance_after_recovery: 1000.0 / total_time.as_secs_f64(),
            integrity_check_passed: recovery_rate > 0.99,
        }
    }

    fn simulate_backup(&self, backup_path: &std::path::Path) -> Result<bool, std::io::Error> {
        // Simple file-based backup simulation
        if !self.db_path.exists() {
            return Ok(false);
        }

        fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<(), std::io::Error> {
            fs::create_dir_all(dst)?;
            
            for entry in fs::read_dir(src)? {
                let entry = entry?;
                let src_path = entry.path();
                let dst_path = dst.join(entry.file_name());
                
                if src_path.is_dir() {
                    copy_dir_recursive(&src_path, &dst_path)?;
                } else {
                    fs::copy(&src_path, &dst_path)?;
                }
            }
            
            Ok(())
        }

        copy_dir_recursive(&self.db_path, backup_path)?;
        Ok(true)
    }

    fn simulate_restore(&self, backup_path: &std::path::Path) -> Result<bool, std::io::Error> {
        if !backup_path.exists() {
            return Ok(false);
        }

        fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<(), std::io::Error> {
            fs::create_dir_all(dst)?;
            
            for entry in fs::read_dir(src)? {
                let entry = entry?;
                let src_path = entry.path();
                let dst_path = dst.join(entry.file_name());
                
                if src_path.is_dir() {
                    copy_dir_recursive(&src_path, &dst_path)?;
                } else {
                    fs::copy(&src_path, &dst_path)?;
                }
            }
            
            Ok(())
        }

        copy_dir_recursive(backup_path, &self.db_path)?;
        Ok(true)
    }

    fn test_data_integrity_after_recovery(&self) -> RecoveryTestResult {
        println!("  Testing data integrity after recovery...");

        // Create database with checksummed data
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let mut checksums = HashMap::new();

        for i in 0..300 {
            let key = format!("integrity_test_{:06}", i);
            let value = format!("integrity_value_{}_{}", i, rand::random::<u32>());
            
            // Store checksum for later verification
            let checksum = crc32fast::hash(value.as_bytes());
            checksums.insert(key.clone(), (value.clone(), checksum));
            
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        drop(db); // Simulate crash

        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify data integrity using checksums
        let mut verified_count = 0;
        let mut corruption_count = 0;

        for (key, (original_value, original_checksum)) in &checksums {
            match recovered_db.get(key.as_bytes()) {
                Ok(Some(recovered_value)) => {
                    let recovered_checksum = crc32fast::hash(&recovered_value);
                    if recovered_checksum == *original_checksum {
                        verified_count += 1;
                    } else {
                        corruption_count += 1;
                    }
                }
                Ok(None) => {} // Data loss
                Err(_) => corruption_count += 1,
            }
        }

        let integrity_rate = verified_count as f64 / checksums.len() as f64;

        // Run database integrity check
        let integrity_check_result = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(recovered_db.verify_integrity());

        let integrity_check_passed = match integrity_check_result {
            Ok(report) => {
                let total_errors = report.checksum_errors.len() +
                    report.structure_errors.len() +
                    report.consistency_errors.len();
                total_errors == 0
            }
            Err(_) => false,
        };

        println!(
            "    Data integrity: {}/{} entries verified ({:.2}%), {} corruptions, integrity check: {}",
            verified_count, checksums.len(), integrity_rate * 100.0, corruption_count, integrity_check_passed
        );

        RecoveryTestResult {
            test_name: "data_integrity_after_recovery".to_string(),
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_recovery_rate: integrity_rate,
            transaction_recovery_rate: integrity_rate,
            corruption_detected: corruption_count,
            corruption_repaired: 0,
            backup_restore_success: true,
            performance_after_recovery: 0.0,
            integrity_check_passed,
        }
    }

    fn test_partial_corruption_recovery(&self) -> RecoveryTestResult {
        println!("  Testing partial corruption recovery...");

        // Create database with known data pattern
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        
        for i in 0..400 {
            let key = format!("partial_test_{:06}", i);
            let value = format!("partial_value_{}_pattern", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        let _ = db.checkpoint();
        drop(db);

        // Inject partial corruption to some files
        let corruption_count = self.inject_partial_corruption().unwrap_or(0);

        // Recovery with corruption present
        let recovery_start = Instant::now();
        let recovery_result = Database::open(&self.db_path, self.config.clone());
        let recovery_time = recovery_start.elapsed();

        match recovery_result {
            Ok(recovered_db) => {
                // Verify partial recovery
                let mut recovered_count = 0;
                let mut pattern_verified = 0;

                for i in 0..400 {
                    let key = format!("partial_test_{:06}", i);
                    match recovered_db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            recovered_count += 1;
                            let value_str = String::from_utf8_lossy(&value);
                            if value_str.contains("pattern") {
                                pattern_verified += 1;
                            }
                        }
                        Ok(None) => {}
                        Err(_) => {}
                    }
                }

                let recovery_rate = recovered_count as f64 / 400.0;
                let pattern_integrity = if recovered_count > 0 {
                    pattern_verified as f64 / recovered_count as f64
                } else {
                    0.0
                };

                println!(
                    "    Partial corruption recovery: {} corruptions, {:.2}% data recovered, {:.2}% pattern integrity",
                    corruption_count, recovery_rate * 100.0, pattern_integrity * 100.0
                );

                RecoveryTestResult {
                    test_name: "partial_corruption_recovery".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: recovery_rate,
                    transaction_recovery_rate: pattern_integrity,
                    corruption_detected: corruption_count,
                    corruption_repaired: 0,
                    backup_restore_success: recovery_rate > 0.5,
                    performance_after_recovery: 0.0,
                    integrity_check_passed: pattern_integrity > 0.9,
                }
            }
            Err(_) => {
                RecoveryTestResult {
                    test_name: "partial_corruption_recovery".to_string(),
                    recovery_time_ms: recovery_time.as_millis() as u64,
                    data_recovery_rate: 0.0,
                    transaction_recovery_rate: 0.0,
                    corruption_detected: corruption_count,
                    corruption_repaired: 0,
                    backup_restore_success: false,
                    performance_after_recovery: 0.0,
                    integrity_check_passed: false,
                }
            }
        }
    }

    fn inject_partial_corruption(&self) -> Result<u64, std::io::Error> {
        let mut corruption_count = 0;

        // Find and partially corrupt multiple files
        let entries = fs::read_dir(&self.db_path)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(extension) = path.extension() {
                if extension == "db" || extension == "wal" || extension == "log" {
                    if let Ok(mut file_data) = fs::read(&path) {
                        if file_data.len() > 2048 {
                            // Corrupt multiple small sections
                            let section_size = 16;
                            let num_sections = std::cmp::min(5, file_data.len() / (section_size * 10));
                            
                            for section in 0..num_sections {
                                let start_pos = (section * file_data.len() / num_sections) + 512;
                                let end_pos = std::cmp::min(start_pos + section_size, file_data.len());
                                
                                for pos in start_pos..end_pos {
                                    file_data[pos] = !file_data[pos];
                                }
                                corruption_count += 1;
                            }
                            
                            let _ = fs::write(&path, &file_data);
                            println!("    Injected {} partial corruptions into {:?}", num_sections, path);
                        }
                    }
                }
            }
        }
        
        Ok(corruption_count)
    }

    fn test_wal_recovery_stress(&self) -> RecoveryTestResult {
        println!("  Testing WAL recovery under stress...");

        let config = LightningDbConfig {
            use_improved_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 50 },
            ..self.config.clone()
        };

        let db = Database::open(&self.db_path, config.clone()).unwrap();
        let operations_count = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));

        // High-frequency WAL operations
        let db_clone = db.clone();
        let ops_clone = Arc::clone(&operations_count);
        let running_clone = Arc::clone(&running);

        let wal_stress_handle = thread::spawn(move || {
            let mut op_count = 0;
            while running_clone.load(Ordering::Relaxed) && op_count < 1000 {
                let key = format!("wal_stress_{:08}", op_count);
                let value = format!("wal_value_{}", op_count);
                
                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    ops_clone.fetch_add(1, Ordering::Relaxed);
                    op_count += 1;
                }
                
                // No sleep - maximum WAL stress
            }
        });

        // Let it run for a short time
        thread::sleep(Duration::from_millis(500));
        running.store(false, Ordering::Relaxed);
        let _ = wal_stress_handle.join();

        let ops_before_crash = operations_count.load(Ordering::Relaxed);
        drop(db); // Simulate crash during high WAL activity

        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, config).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify WAL recovery
        let mut recovered_ops = 0;
        for i in 0..ops_before_crash {
            let key = format!("wal_stress_{:08}", i);
            if recovered_db.get(key.as_bytes()).unwrap_or(None).is_some() {
                recovered_ops += 1;
            }
        }

        let recovery_rate = if ops_before_crash > 0 {
            recovered_ops as f64 / ops_before_crash as f64
        } else {
            1.0
        };

        println!(
            "    WAL recovery stress: {} ops before crash, {} recovered ({:.2}%), recovery time: {:?}",
            ops_before_crash, recovered_ops, recovery_rate * 100.0, recovery_time
        );

        RecoveryTestResult {
            test_name: "wal_recovery_stress".to_string(),
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_recovery_rate: recovery_rate,
            transaction_recovery_rate: recovery_rate,
            corruption_detected: 0,
            corruption_repaired: 0,
            backup_restore_success: true,
            performance_after_recovery: 0.0,
            integrity_check_passed: recovery_rate > 0.8,
        }
    }

    fn test_large_database_recovery(&self) -> RecoveryTestResult {
        println!("  Testing large database recovery...");

        // Create larger dataset
        let db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let large_value = vec![0u8; 10240]; // 10KB values
        
        for i in 0..500 {
            let key = format!("large_db_{:08}", i);
            db.put(key.as_bytes(), &large_value).unwrap();
        }

        let _ = db.checkpoint();
        drop(db);

        // Recovery
        let recovery_start = Instant::now();
        let recovered_db = Database::open(&self.db_path, self.config.clone()).unwrap();
        let recovery_time = recovery_start.elapsed();

        // Verify large database recovery
        let mut recovered_count = 0;
        let mut size_verified = 0;

        for i in 0..500 {
            let key = format!("large_db_{:08}", i);
            match recovered_db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    recovered_count += 1;
                    if value.len() == 10240 {
                        size_verified += 1;
                    }
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let recovery_rate = recovered_count as f64 / 500.0;
        let size_integrity = if recovered_count > 0 {
            size_verified as f64 / recovered_count as f64
        } else {
            0.0
        };

        println!(
            "    Large DB recovery: {}/500 entries recovered ({:.2}%), {:.2}% size integrity, recovery time: {:?}",
            recovered_count, recovery_rate * 100.0, size_integrity * 100.0, recovery_time
        );

        RecoveryTestResult {
            test_name: "large_database_recovery".to_string(),
            recovery_time_ms: recovery_time.as_millis() as u64,
            data_recovery_rate: recovery_rate,
            transaction_recovery_rate: size_integrity,
            corruption_detected: 0,
            corruption_repaired: 0,
            backup_restore_success: true,
            performance_after_recovery: 0.0,
            integrity_check_passed: recovery_rate > 0.95 && size_integrity > 0.99,
        }
    }
}

#[derive(Debug)]
struct RecoveryVerification {
    data_recovery_rate: f64,
    transaction_recovery_rate: f64,
}

// Individual recovery tests

#[test]
#[ignore = "Recovery test - run with: cargo test test_recovery_suite -- --ignored"]
fn test_recovery_suite() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let recovery_suite = RecoveryTestSuite::new(dir.path().to_path_buf(), config);
    let results = recovery_suite.run_recovery_test_suite();

    println!("\nRecovery Test Suite Results:");
    println!("============================");

    let mut total_tests = 0;
    let mut passed_tests = 0;
    let mut total_recovery_time = 0u64;
    let mut total_data_recovery = 0.0;

    for result in &results {
        total_tests += 1;
        if result.integrity_check_passed {
            passed_tests += 1;
        }
        total_recovery_time += result.recovery_time_ms;
        total_data_recovery += result.data_recovery_rate;

        println!("Test: {}", result.test_name);
        println!("  Recovery time: {} ms", result.recovery_time_ms);
        println!("  Data recovery rate: {:.2}%", result.data_recovery_rate * 100.0);
        println!("  Transaction recovery rate: {:.2}%", result.transaction_recovery_rate * 100.0);
        println!("  Corruption detected: {}", result.corruption_detected);
        println!("  Corruption repaired: {}", result.corruption_repaired);
        println!("  Backup/restore success: {}", result.backup_restore_success);
        println!("  Integrity check passed: {}", result.integrity_check_passed);
        println!();
    }

    let avg_recovery_time = total_recovery_time / total_tests as u64;
    let avg_data_recovery = total_data_recovery / total_tests as f64;
    let success_rate = passed_tests as f64 / total_tests as f64;

    println!("Overall Results:");
    println!("  Success rate: {:.1}% ({}/{})", success_rate * 100.0, passed_tests, total_tests);
    println!("  Average recovery time: {} ms", avg_recovery_time);
    println!("  Average data recovery rate: {:.2}%", avg_data_recovery * 100.0);

    // Assert recovery performance
    assert!(success_rate >= 0.8, "Recovery test success rate too low: {:.1}%", success_rate * 100.0);
    assert!(avg_recovery_time < 5000, "Average recovery time too high: {} ms", avg_recovery_time);
    assert!(avg_data_recovery >= 0.9, "Average data recovery rate too low: {:.2}%", avg_data_recovery * 100.0);
}

#[test]
#[ignore = "Individual recovery test - run with: cargo test test_crash_recovery_performance -- --ignored"]
fn test_crash_recovery_performance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Sync,
        ..Default::default()
    };

    let recovery_suite = RecoveryTestSuite::new(dir.path().to_path_buf(), config);
    let result = recovery_suite.test_crash_recovery_time();

    assert!(result.recovery_time_ms < 3000, "Crash recovery took too long: {} ms", result.recovery_time_ms);
    assert!(result.data_recovery_rate > 0.9, "Data recovery rate too low: {:.2}%", result.data_recovery_rate * 100.0);
    assert!(result.integrity_check_passed, "Integrity check failed after recovery");
}
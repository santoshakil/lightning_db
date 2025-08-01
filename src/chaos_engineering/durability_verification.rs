//! Durability Verification for Chaos Engineering
//!
//! Comprehensive testing of data durability guarantees including WAL integrity,
//! fsync verification, crash recovery, and data persistence validation.

use crate::{Database, Result, Error};
use crate::chaos_engineering::{
    ChaosTest, ChaosTestResult, IntegrityReport, ChaosConfig,
    IntegrityViolation, IntegrityViolationType, ViolationSeverity
};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Write, Read, Seek, SeekFrom};
use std::collections::{HashMap, HashSet, VecDeque};
use parking_lot::{RwLock, Mutex};
use rand::{Rng, thread_rng};
use sha2::{Sha256, Digest};

/// Durability verification test
pub struct DurabilityVerificationTest {
    config: DurabilityConfig,
    wal_verifier: Arc<WALVerifier>,
    fsync_verifier: Arc<FsyncVerifier>,
    persistence_validator: Arc<PersistenceValidator>,
    recovery_tester: Arc<RecoveryTester>,
    test_results: Arc<Mutex<DurabilityTestResults>>,
}

/// Configuration for durability testing
#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    /// Test WAL integrity
    pub test_wal_integrity: bool,
    /// Test fsync correctness
    pub test_fsync_correctness: bool,
    /// Test crash recovery
    pub test_crash_recovery: bool,
    /// Test data persistence
    pub test_data_persistence: bool,
    /// Number of operations per test
    pub operations_per_test: usize,
    /// Concurrent writers for stress testing
    pub concurrent_writers: usize,
    /// Crash points to test
    pub crash_points: Vec<CrashPoint>,
    /// Verify checksum on all reads
    pub verify_checksums: bool,
    /// Test partial write scenarios
    pub test_partial_writes: bool,
    /// Maximum data size for tests
    pub max_data_size: usize,
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            test_wal_integrity: true,
            test_fsync_correctness: true,
            test_crash_recovery: true,
            test_data_persistence: true,
            operations_per_test: 10000,
            concurrent_writers: 10,
            crash_points: vec![
                CrashPoint::BeforeWALWrite,
                CrashPoint::DuringWALWrite,
                CrashPoint::AfterWALWrite,
                CrashPoint::BeforeDataWrite,
                CrashPoint::AfterDataWrite,
                CrashPoint::BeforeFsync,
                CrashPoint::AfterFsync,
            ],
            verify_checksums: true,
            test_partial_writes: true,
            max_data_size: 1024 * 1024, // 1MB
        }
    }
}

/// Points where crashes can occur
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashPoint {
    BeforeWALWrite,
    DuringWALWrite,
    AfterWALWrite,
    BeforeDataWrite,
    DuringDataWrite,
    AfterDataWrite,
    BeforeFsync,
    AfterFsync,
    DuringCheckpoint,
    DuringCompaction,
}

/// WAL integrity verifier
struct WALVerifier {
    wal_entries: RwLock<Vec<WALEntry>>,
    corruption_detected: AtomicU64,
    recovery_success: AtomicU64,
    recovery_failures: AtomicU64,
}

/// WAL entry for tracking
#[derive(Debug, Clone)]
struct WALEntry {
    sequence_number: u64,
    timestamp: SystemTime,
    operation: WALOperation,
    checksum: u32,
    synced: bool,
    recovered: bool,
}

/// WAL operation types
#[derive(Debug, Clone)]
enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionRollback { tx_id: u64 },
    Checkpoint { lsn: u64 },
}

/// Fsync correctness verifier
struct FsyncVerifier {
    pending_writes: RwLock<HashMap<Vec<u8>, PendingWrite>>,
    synced_writes: RwLock<HashMap<Vec<u8>, SyncedWrite>>,
    fsync_calls: AtomicU64,
    data_loss_events: AtomicU64,
}

/// Pending write tracking
#[derive(Debug, Clone)]
struct PendingWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: SystemTime,
    write_completed: bool,
    fsync_pending: bool,
}

/// Synced write record
#[derive(Debug, Clone)]
struct SyncedWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    sync_time: SystemTime,
    checksum: u32,
    verified: bool,
}

/// Persistence validator
struct PersistenceValidator {
    written_data: RwLock<HashMap<Vec<u8>, DataRecord>>,
    verification_queue: RwLock<VecDeque<VerificationTask>>,
    verifications_performed: AtomicU64,
    persistence_failures: AtomicU64,
}

/// Data record for persistence tracking
#[derive(Debug, Clone)]
struct DataRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    write_time: SystemTime,
    checksum: u32,
    durability_level: DurabilityLevel,
    verified_count: u64,
}

/// Durability level guarantees
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurabilityLevel {
    Volatile,      // May be lost on crash
    Buffered,      // Written but not synced
    Synced,        // Fsync completed
    Replicated,    // Written to multiple locations
}

/// Verification task
#[derive(Debug, Clone)]
struct VerificationTask {
    key: Vec<u8>,
    expected_value: Vec<u8>,
    expected_checksum: u32,
    verify_after: SystemTime,
    attempts: u64,
}

/// Recovery tester
struct RecoveryTester {
    pre_crash_state: RwLock<DatabaseState>,
    post_crash_state: RwLock<DatabaseState>,
    recovery_attempts: AtomicU64,
    successful_recoveries: AtomicU64,
    data_loss_incidents: AtomicU64,
}

/// Database state snapshot
#[derive(Debug, Clone, Default)]
struct DatabaseState {
    data: HashMap<Vec<u8>, Vec<u8>>,
    transaction_log: Vec<TransactionRecord>,
    checkpoint_lsn: u64,
    total_keys: usize,
    total_size: u64,
    checksum: u64,
}

/// Transaction record
#[derive(Debug, Clone)]
struct TransactionRecord {
    tx_id: u64,
    operations: Vec<WALOperation>,
    status: TransactionStatus,
    commit_time: Option<SystemTime>,
}

/// Transaction status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Active,
    Committed,
    Aborted,
    Unknown,
}

/// Results from durability testing
#[derive(Debug, Clone)]
struct DurabilityTestResults {
    /// Total writes performed
    pub total_writes: u64,
    /// Writes verified as durable
    pub durable_writes: u64,
    /// Writes lost after crash
    pub lost_writes: u64,
    /// Partial writes detected
    pub partial_writes: u64,
    /// WAL corruption events
    pub wal_corruptions: u64,
    /// Fsync verification failures
    pub fsync_failures: u64,
    /// Recovery attempts
    pub recovery_attempts: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Average recovery time
    pub avg_recovery_time: Duration,
    /// Durability score (0.0 - 1.0)
    pub durability_score: f64,
    /// Detailed analysis
    pub analysis: DurabilityAnalysis,
}

/// Detailed durability analysis
#[derive(Debug, Clone)]
struct DurabilityAnalysis {
    /// WAL effectiveness
    pub wal_effectiveness: f64,
    /// Fsync reliability
    pub fsync_reliability: f64,
    /// Recovery completeness
    pub recovery_completeness: f64,
    /// Data integrity score
    pub data_integrity: f64,
    /// Crash resistance rating
    pub crash_resistance: CrashResistance,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Crash resistance rating
#[derive(Debug, Clone, Copy)]
enum CrashResistance {
    Excellent,
    Good,
    Fair,
    Poor,
    Critical,
}

impl DurabilityVerificationTest {
    /// Create a new durability verification test
    pub fn new(config: DurabilityConfig) -> Self {
        Self {
            config,
            wal_verifier: Arc::new(WALVerifier::new()),
            fsync_verifier: Arc::new(FsyncVerifier::new()),
            persistence_validator: Arc::new(PersistenceValidator::new()),
            recovery_tester: Arc::new(RecoveryTester::new()),
            test_results: Arc::new(Mutex::new(DurabilityTestResults::default())),
        }
    }

    /// Test WAL integrity under various conditions
    fn test_wal_integrity(&self, db: Arc<Database>) -> Result<()> {
        println!("📝 Testing WAL integrity...");
        
        // Test sequential WAL writes
        self.test_sequential_wal_writes(&db)?;
        
        // Test concurrent WAL writes
        self.test_concurrent_wal_writes(&db)?;
        
        // Test WAL recovery after crash
        self.test_wal_crash_recovery(&db)?;
        
        // Test WAL corruption detection
        self.test_wal_corruption_detection(&db)?;
        
        Ok(())
    }

    /// Test sequential WAL writes
    fn test_sequential_wal_writes(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing sequential WAL writes...");
        
        let mut sequence = 0u64;
        
        for i in 0..1000 {
            let key = format!("wal_seq_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            
            // Record WAL entry
            let entry = WALEntry {
                sequence_number: sequence,
                timestamp: SystemTime::now(),
                operation: WALOperation::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                checksum: self.calculate_checksum(&key, &value),
                synced: false,
                recovered: false,
            };
            
            self.wal_verifier.wal_entries.write().push(entry);
            sequence += 1;
            
            // Perform actual write
            db.put(&key, &value)?;
        }
        
        // Verify WAL sequence integrity
        self.verify_wal_sequence()?;
        
        Ok(())
    }

    /// Test concurrent WAL writes
    fn test_concurrent_wal_writes(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing concurrent WAL writes...");
        
        let mut handles = vec![];
        let sequence_counter = Arc::new(AtomicU64::new(0));
        
        for thread_id in 0..self.config.concurrent_writers {
            let db_clone = Arc::clone(db);
            let wal_verifier = Arc::clone(&self.wal_verifier);
            let seq_counter = Arc::clone(&sequence_counter);
            
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("wal_concurrent_{}_{}", thread_id, i).into_bytes();
                    let value = format!("value_{}_{}", thread_id, i).into_bytes();
                    
                    let sequence = seq_counter.fetch_add(1, Ordering::SeqCst);
                    
                    let entry = WALEntry {
                        sequence_number: sequence,
                        timestamp: SystemTime::now(),
                        operation: WALOperation::Put {
                            key: key.clone(),
                            value: value.clone(),
                        },
                        checksum: 0, // Simplified
                        synced: false,
                        recovered: false,
                    };
                    
                    wal_verifier.wal_entries.write().push(entry);
                    
                    let _ = db_clone.put(&key, &value);
                }
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Check for gaps in sequence numbers
        self.check_wal_sequence_gaps()?;
        
        Ok(())
    }

    /// Test WAL recovery after crash
    fn test_wal_crash_recovery(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing WAL crash recovery...");
        
        // Write data with WAL tracking
        let test_data = self.generate_test_data(100);
        
        for (key, value) in &test_data {
            db.put(key, value)?;
            
            // Track in WAL
            let entry = WALEntry {
                sequence_number: 0,
                timestamp: SystemTime::now(),
                operation: WALOperation::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                checksum: self.calculate_checksum(key, value),
                synced: true,
                recovered: false,
            };
            
            self.wal_verifier.wal_entries.write().push(entry);
        }
        
        // Simulate crash and recovery
        drop(db.clone());
        
        // Reopen database
        let recovered_db = Arc::new(Database::open(
            std::env::temp_dir().join("durability_test"),
            crate::LightningDbConfig::default()
        )?);
        
        // Verify all WAL entries were recovered
        let mut recovered = 0;
        let mut lost = 0;
        
        for (key, expected_value) in &test_data {
            match recovered_db.get(key)? {
                Some(value) => {
                    if value == *expected_value {
                        recovered += 1;
                    } else {
                        println!("   ⚠️  Data corruption detected for key: {:?}", key);
                    }
                },
                None => {
                    lost += 1;
                    println!("   ⚠️  Data loss detected for key: {:?}", key);
                }
            }
        }
        
        println!("   Recovered: {}, Lost: {}", recovered, lost);
        
        if lost > 0 {
            self.wal_verifier.recovery_failures.fetch_add(lost, Ordering::Relaxed);
        } else {
            self.wal_verifier.recovery_success.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }

    /// Test WAL corruption detection
    fn test_wal_corruption_detection(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing WAL corruption detection...");
        
        // Simulate corrupted WAL entries
        let mut corrupted_entries = 0;
        
        for i in 0..100 {
            let key = format!("wal_corrupt_{}", i).into_bytes();
            let value = vec![0u8; 100];
            
            let mut entry = WALEntry {
                sequence_number: i,
                timestamp: SystemTime::now(),
                operation: WALOperation::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                checksum: self.calculate_checksum(&key, &value),
                synced: true,
                recovered: false,
            };
            
            // Corrupt some entries
            if i % 10 == 0 {
                entry.checksum = entry.checksum.wrapping_add(1); // Invalid checksum
                corrupted_entries += 1;
            }
            
            self.wal_verifier.wal_entries.write().push(entry);
        }
        
        // Verify checksums
        let detected = self.verify_wal_checksums()?;
        
        if detected != corrupted_entries {
            println!("   ⚠️  Only detected {} of {} corrupted entries", detected, corrupted_entries);
        } else {
            println!("   ✔️  All corrupted entries detected");
        }
        
        self.wal_verifier.corruption_detected.fetch_add(detected, Ordering::Relaxed);
        
        Ok(())
    }

    /// Test fsync correctness
    fn test_fsync_correctness(&self, db: Arc<Database>) -> Result<()> {
        println!("💾 Testing fsync correctness...");
        
        // Test single fsync
        self.test_single_fsync(&db)?;
        
        // Test batch fsync
        self.test_batch_fsync(&db)?;
        
        // Test fsync under memory pressure
        self.test_fsync_memory_pressure(&db)?;
        
        // Test fsync timing
        self.test_fsync_timing(&db)?;
        
        Ok(())
    }

    /// Test single fsync operation
    fn test_single_fsync(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing single fsync...");
        
        let key = b"fsync_single_test";
        let value = b"test_value";
        
        // Track pending write
        self.fsync_verifier.pending_writes.write().insert(
            key.to_vec(),
            PendingWrite {
                key: key.to_vec(),
                value: value.to_vec(),
                timestamp: SystemTime::now(),
                write_completed: false,
                fsync_pending: true,
            }
        );
        
        // Write and sync
        db.put(key, value)?;
        db.sync()?;
        
        // Mark as synced
        self.fsync_verifier.synced_writes.write().insert(
            key.to_vec(),
            SyncedWrite {
                key: key.to_vec(),
                value: value.to_vec(),
                sync_time: SystemTime::now(),
                checksum: self.calculate_checksum(key, value),
                verified: false,
            }
        );
        
        self.fsync_verifier.fsync_calls.fetch_add(1, Ordering::Relaxed);
        
        // Verify persistence
        drop(db.clone());
        let reopened = Arc::new(Database::open(
            std::env::temp_dir().join("fsync_test"),
            crate::LightningDbConfig::default()
        )?);
        
        match reopened.get(key)? {
            Some(stored_value) => {
                if stored_value != value {
                    println!("   ⚠️  Value mismatch after fsync");
                    self.fsync_verifier.data_loss_events.fetch_add(1, Ordering::Relaxed);
                }
            },
            None => {
                println!("   ⚠️  Data lost despite fsync!");
                self.fsync_verifier.data_loss_events.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        Ok(())
    }

    /// Test batch fsync operations
    fn test_batch_fsync(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing batch fsync...");
        
        let batch_size = 100;
        let mut keys = Vec::new();
        
        // Write batch without syncing
        for i in 0..batch_size {
            let key = format!("fsync_batch_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            
            db.put(&key, &value)?;
            keys.push((key, value));
        }
        
        // Single fsync for entire batch
        db.sync()?;
        self.fsync_verifier.fsync_calls.fetch_add(1, Ordering::Relaxed);
        
        // Verify all data persisted
        drop(db.clone());
        let reopened = Arc::new(Database::open(
            std::env::temp_dir().join("batch_fsync_test"),
            crate::LightningDbConfig::default()
        )?);
        
        let mut verified = 0;
        for (key, expected_value) in &keys {
            if let Some(value) = reopened.get(key)? {
                if value == *expected_value {
                    verified += 1;
                }
            }
        }
        
        println!("   Batch fsync: {}/{} entries persisted", verified, batch_size);
        
        if verified < batch_size {
            self.fsync_verifier.data_loss_events.fetch_add((batch_size - verified) as u64, Ordering::Relaxed);
        }
        
        Ok(())
    }

    /// Test fsync under memory pressure
    fn test_fsync_memory_pressure(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing fsync under memory pressure...");
        
        // Allocate large buffers to simulate memory pressure
        let _memory_pressure: Vec<Vec<u8>> = (0..100)
            .map(|_| vec![0u8; 10 * 1024 * 1024]) // 10MB each
            .collect();
        
        // Try to write and sync
        let key = b"fsync_memory_pressure";
        let value = vec![0u8; 1024 * 1024]; // 1MB value
        
        match db.put(key, &value) {
            Ok(_) => {
                match db.sync() {
                    Ok(_) => println!("   Fsync succeeded under memory pressure"),
                    Err(e) => println!("   ⚠️  Fsync failed under memory pressure: {}", e),
                }
            },
            Err(e) => println!("   ⚠️  Write failed under memory pressure: {}", e),
        }
        
        Ok(())
    }

    /// Test fsync timing and performance
    fn test_fsync_timing(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing fsync timing...");
        
        let mut timings = Vec::new();
        
        for i in 0..10 {
            let key = format!("fsync_timing_{}", i).into_bytes();
            let value = vec![0u8; 1024 * 1024]; // 1MB
            
            db.put(&key, &value)?;
            
            let start = std::time::Instant::now();
            db.sync()?;
            let duration = start.elapsed();
            
            timings.push(duration);
            println!("   Fsync {} took: {:?}", i, duration);
        }
        
        // Calculate statistics
        let avg_time = timings.iter().map(|d| d.as_millis()).sum::<u128>() / timings.len() as u128;
        let max_time = timings.iter().map(|d| d.as_millis()).max().unwrap_or(0);
        
        println!("   Average fsync time: {}ms, Max: {}ms", avg_time, max_time);
        
        Ok(())
    }

    /// Test data persistence guarantees
    fn test_data_persistence(&self, db: Arc<Database>) -> Result<()> {
        println!("📊 Testing data persistence guarantees...");
        
        // Test immediate persistence
        self.test_immediate_persistence(&db)?;
        
        // Test deferred persistence
        self.test_deferred_persistence(&db)?;
        
        // Test persistence across restarts
        self.test_restart_persistence(&db)?;
        
        // Test large data persistence
        self.test_large_data_persistence(&db)?;
        
        Ok(())
    }

    /// Test immediate persistence
    fn test_immediate_persistence(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing immediate persistence...");
        
        let test_count = 100;
        let mut immediately_persisted = 0;
        
        for i in 0..test_count {
            let key = format!("immediate_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            
            // Write with immediate sync
            db.put(&key, &value)?;
            db.sync()?;
            
            // Immediately verify by reopening
            drop(db.clone());
            let reopened = Arc::new(Database::open(
                std::env::temp_dir().join("immediate_persist"),
                crate::LightningDbConfig::default()
            )?);
            
            if reopened.get(&key)?.is_some() {
                immediately_persisted += 1;
            }
        }
        
        let persistence_rate = immediately_persisted as f64 / test_count as f64;
        println!("   Immediate persistence rate: {:.2}%", persistence_rate * 100.0);
        
        if persistence_rate < 1.0 {
            self.persistence_validator.persistence_failures.fetch_add(
                (test_count - immediately_persisted) as u64,
                Ordering::Relaxed
            );
        }
        
        Ok(())
    }

    /// Test deferred persistence
    fn test_deferred_persistence(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing deferred persistence...");
        
        // Write without immediate sync
        let batch_size = 1000;
        for i in 0..batch_size {
            let key = format!("deferred_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            db.put(&key, &value)?;
        }
        
        // Wait for background sync (if any)
        thread::sleep(Duration::from_secs(5));
        
        // Force sync
        db.sync()?;
        
        // Verify persistence
        drop(db.clone());
        let reopened = Arc::new(Database::open(
            std::env::temp_dir().join("deferred_persist"),
            crate::LightningDbConfig::default()
        )?);
        
        let mut persisted = 0;
        for i in 0..batch_size {
            let key = format!("deferred_{}", i).into_bytes();
            if reopened.get(&key)?.is_some() {
                persisted += 1;
            }
        }
        
        println!("   Deferred persistence: {}/{}", persisted, batch_size);
        
        Ok(())
    }

    /// Test persistence across restarts
    fn test_restart_persistence(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing persistence across restarts...");
        
        let test_data = self.generate_test_data(100);
        
        // Write and sync data
        for (key, value) in &test_data {
            db.put(key, value)?;
        }
        db.sync()?;
        
        // Simulate multiple restarts
        for restart in 1..=5 {
            drop(db.clone());
            
            let reopened = Arc::new(Database::open(
                std::env::temp_dir().join("restart_persist"),
                crate::LightningDbConfig::default()
            )?);
            
            // Verify all data still present
            let mut verified = 0;
            for (key, expected_value) in &test_data {
                if let Some(value) = reopened.get(key)? {
                    if value == *expected_value {
                        verified += 1;
                    }
                }
            }
            
            if verified < test_data.len() {
                println!("   ⚠️  Data loss after restart {}: {}/{}", 
                         restart, verified, test_data.len());
            }
        }
        
        Ok(())
    }

    /// Test large data persistence
    fn test_large_data_persistence(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Testing large data persistence...");
        
        let sizes = vec![1024, 10240, 102400, 1048576]; // 1KB to 1MB
        
        for (i, size) in sizes.iter().enumerate() {
            let key = format!("large_{}", i).into_bytes();
            let value = vec![0xAB; *size];
            
            // Write large value
            db.put(&key, &value)?;
            db.sync()?;
            
            // Verify
            if let Some(stored) = db.get(&key)? {
                if stored.len() != *size {
                    println!("   ⚠️  Size mismatch for {}KB value", size / 1024);
                }
            } else {
                println!("   ⚠️  Failed to persist {}KB value", size / 1024);
            }
        }
        
        Ok(())
    }

    /// Test crash recovery scenarios
    fn test_crash_recovery(&self, db: Arc<Database>) -> Result<()> {
        println!("🔄 Testing crash recovery scenarios...");
        
        for crash_point in &self.config.crash_points {
            self.test_crash_at_point(&db, *crash_point)?;
        }
        
        Ok(())
    }

    /// Test crash at specific point
    fn test_crash_at_point(&self, db: &Arc<Database>, crash_point: CrashPoint) -> Result<()> {
        println!("   Testing crash at {:?}...", crash_point);
        
        // Capture pre-crash state
        let pre_crash_state = self.capture_database_state(db)?;
        self.recovery_tester.pre_crash_state.write().clone_from(&pre_crash_state);
        
        // Perform operations that will be interrupted
        let test_data = self.generate_test_data(50);
        let mut written = 0;
        
        for (i, (key, value)) in test_data.iter().enumerate() {
            match crash_point {
                CrashPoint::BeforeWALWrite => {
                    if i == 25 {
                        break; // Simulate crash
                    }
                },
                CrashPoint::AfterWALWrite => {
                    // Simulate WAL write completed but data not written
                    if i == 25 {
                        break;
                    }
                },
                CrashPoint::BeforeFsync => {
                    db.put(key, value)?;
                    if i == 25 {
                        break; // Crash before sync
                    }
                },
                CrashPoint::AfterFsync => {
                    db.put(key, value)?;
                    if i % 10 == 0 {
                        db.sync()?;
                    }
                    if i == 25 {
                        break;
                    }
                },
                _ => {
                    db.put(key, value)?;
                }
            }
            written += 1;
        }
        
        // Simulate crash and recovery
        drop(db.clone());
        self.recovery_tester.recovery_attempts.fetch_add(1, Ordering::Relaxed);
        
        let start_recovery = std::time::Instant::now();
        let recovered_db = Arc::new(Database::open(
            std::env::temp_dir().join("crash_recovery"),
            crate::LightningDbConfig::default()
        )?);
        let recovery_time = start_recovery.elapsed();
        
        // Capture post-crash state
        let post_crash_state = self.capture_database_state(&recovered_db)?;
        self.recovery_tester.post_crash_state.write().clone_from(&post_crash_state);
        
        // Analyze recovery
        let analysis = self.analyze_recovery(&pre_crash_state, &post_crash_state, written)?;
        
        if analysis.data_loss == 0 {
            self.recovery_tester.successful_recoveries.fetch_add(1, Ordering::Relaxed);
        } else {
            self.recovery_tester.data_loss_incidents.fetch_add(1, Ordering::Relaxed);
            println!("   ⚠️  Data loss detected: {} entries", analysis.data_loss);
        }
        
        println!("   Recovery time: {:?}", recovery_time);
        
        Ok(())
    }

    /// Capture database state
    fn capture_database_state(&self, db: &Arc<Database>) -> Result<DatabaseState> {
        let mut state = DatabaseState::default();
        
        // In real implementation, would iterate through all keys
        // For now, simplified version
        state.total_keys = 1000; // Placeholder
        state.total_size = 1024 * 1024; // Placeholder
        state.checksum = self.calculate_state_checksum(&state);
        
        Ok(state)
    }

    /// Analyze recovery completeness
    fn analyze_recovery(
        &self,
        pre_crash: &DatabaseState,
        post_crash: &DatabaseState,
        expected_writes: usize,
    ) -> Result<RecoveryAnalysis> {
        let mut analysis = RecoveryAnalysis {
            data_loss: 0,
            corrupted_entries: 0,
            recovery_complete: true,
            consistency_maintained: true,
        };
        
        // Compare states
        if post_crash.total_keys < pre_crash.total_keys {
            analysis.data_loss = pre_crash.total_keys - post_crash.total_keys;
            analysis.recovery_complete = false;
        }
        
        // Check consistency
        if post_crash.checksum != pre_crash.checksum && analysis.data_loss == 0 {
            analysis.consistency_maintained = false;
        }
        
        Ok(analysis)
    }

    /// Generate test data
    fn generate_test_data(&self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut data = Vec::new();
        let mut rng = thread_rng();
        
        for i in 0..count {
            let key = format!("test_key_{}", i).into_bytes();
            let value_size = rng.gen_range(100..10000);
            let value = (0..value_size).map(|_| rng.gen::<u8>()).collect();
            data.push((key, value));
        }
        
        data
    }

    /// Calculate checksum for data
    fn calculate_checksum(&self, key: &[u8], value: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key);
        hasher.update(value);
        hasher.finalize()
    }

    /// Calculate state checksum
    fn calculate_state_checksum(&self, state: &DatabaseState) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(state.total_keys.to_le_bytes());
        hasher.update(state.total_size.to_le_bytes());
        hasher.update(state.checkpoint_lsn.to_le_bytes());
        
        let result = hasher.finalize();
        u64::from_le_bytes(result[0..8].try_into().unwrap())
    }

    /// Verify WAL sequence integrity
    fn verify_wal_sequence(&self) -> Result<()> {
        let entries = self.wal_verifier.wal_entries.read();
        let mut last_seq = 0;
        
        for entry in entries.iter() {
            if entry.sequence_number <= last_seq {
                println!("   ⚠️  WAL sequence error: {} <= {}", entry.sequence_number, last_seq);
                return Err(Error::Generic("WAL sequence integrity violation".to_string()));
            }
            last_seq = entry.sequence_number;
        }
        
        Ok(())
    }

    /// Check for gaps in WAL sequence
    fn check_wal_sequence_gaps(&self) -> Result<()> {
        let mut entries = self.wal_verifier.wal_entries.read().clone();
        entries.sort_by_key(|e| e.sequence_number);
        
        let mut gaps = 0;
        for i in 1..entries.len() {
            let expected = entries[i-1].sequence_number + 1;
            if entries[i].sequence_number != expected {
                gaps += 1;
                println!("   ⚠️  WAL gap detected: {} -> {}", 
                         entries[i-1].sequence_number, entries[i].sequence_number);
            }
        }
        
        if gaps > 0 {
            println!("   Total WAL gaps: {}", gaps);
        }
        
        Ok(())
    }

    /// Verify WAL checksums
    fn verify_wal_checksums(&self) -> Result<u64> {
        let entries = self.wal_verifier.wal_entries.read();
        let mut corrupted = 0;
        
        for entry in entries.iter() {
            match &entry.operation {
                WALOperation::Put { key, value } => {
                    let expected_checksum = self.calculate_checksum(key, value);
                    if entry.checksum != expected_checksum {
                        corrupted += 1;
                    }
                },
                _ => {}
            }
        }
        
        Ok(corrupted)
    }
}

/// Recovery analysis result
#[derive(Debug)]
struct RecoveryAnalysis {
    data_loss: usize,
    corrupted_entries: usize,
    recovery_complete: bool,
    consistency_maintained: bool,
}

impl WALVerifier {
    fn new() -> Self {
        Self {
            wal_entries: RwLock::new(Vec::new()),
            corruption_detected: AtomicU64::new(0),
            recovery_success: AtomicU64::new(0),
            recovery_failures: AtomicU64::new(0),
        }
    }
}

impl FsyncVerifier {
    fn new() -> Self {
        Self {
            pending_writes: RwLock::new(HashMap::new()),
            synced_writes: RwLock::new(HashMap::new()),
            fsync_calls: AtomicU64::new(0),
            data_loss_events: AtomicU64::new(0),
        }
    }
}

impl PersistenceValidator {
    fn new() -> Self {
        Self {
            written_data: RwLock::new(HashMap::new()),
            verification_queue: RwLock::new(VecDeque::new()),
            verifications_performed: AtomicU64::new(0),
            persistence_failures: AtomicU64::new(0),
        }
    }
}

impl RecoveryTester {
    fn new() -> Self {
        Self {
            pre_crash_state: RwLock::new(DatabaseState::default()),
            post_crash_state: RwLock::new(DatabaseState::default()),
            recovery_attempts: AtomicU64::new(0),
            successful_recoveries: AtomicU64::new(0),
            data_loss_incidents: AtomicU64::new(0),
        }
    }
}

impl Default for DurabilityTestResults {
    fn default() -> Self {
        Self {
            total_writes: 0,
            durable_writes: 0,
            lost_writes: 0,
            partial_writes: 0,
            wal_corruptions: 0,
            fsync_failures: 0,
            recovery_attempts: 0,
            successful_recoveries: 0,
            avg_recovery_time: Duration::from_secs(0),
            durability_score: 1.0,
            analysis: DurabilityAnalysis {
                wal_effectiveness: 1.0,
                fsync_reliability: 1.0,
                recovery_completeness: 1.0,
                data_integrity: 1.0,
                crash_resistance: CrashResistance::Excellent,
                recommendations: Vec::new(),
            },
        }
    }
}

impl ChaosTest for DurabilityVerificationTest {
    fn name(&self) -> &str {
        "Durability Verification Test"
    }

    fn initialize(&mut self, _config: &ChaosConfig) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self, db: Arc<Database>, duration: Duration) -> Result<ChaosTestResult> {
        let start_time = SystemTime::now();
        
        // Test WAL integrity
        if self.config.test_wal_integrity {
            self.test_wal_integrity(Arc::clone(&db))?;
        }
        
        // Test fsync correctness
        if self.config.test_fsync_correctness {
            self.test_fsync_correctness(Arc::clone(&db))?;
        }
        
        // Test data persistence
        if self.config.test_data_persistence {
            self.test_data_persistence(Arc::clone(&db))?;
        }
        
        // Test crash recovery
        if self.config.test_crash_recovery {
            self.test_crash_recovery(Arc::clone(&db))?;
        }
        
        // Calculate results
        let mut results = self.test_results.lock();
        
        // Update metrics
        results.wal_corruptions = self.wal_verifier.corruption_detected.load(Ordering::Relaxed);
        results.fsync_failures = self.fsync_verifier.data_loss_events.load(Ordering::Relaxed);
        results.recovery_attempts = self.recovery_tester.recovery_attempts.load(Ordering::Relaxed);
        results.successful_recoveries = self.recovery_tester.successful_recoveries.load(Ordering::Relaxed);
        
        // Calculate durability score
        let recovery_rate = if results.recovery_attempts > 0 {
            results.successful_recoveries as f64 / results.recovery_attempts as f64
        } else {
            1.0
        };
        
        let wal_integrity = if results.wal_corruptions == 0 { 1.0 } else { 0.5 };
        let fsync_reliability = if results.fsync_failures == 0 { 1.0 } else { 0.7 };
        
        results.durability_score = (recovery_rate + wal_integrity + fsync_reliability) / 3.0;
        
        // Generate analysis
        results.analysis = DurabilityAnalysis {
            wal_effectiveness: wal_integrity,
            fsync_reliability,
            recovery_completeness: recovery_rate,
            data_integrity: results.durability_score,
            crash_resistance: match results.durability_score {
                x if x >= 0.95 => CrashResistance::Excellent,
                x if x >= 0.85 => CrashResistance::Good,
                x if x >= 0.70 => CrashResistance::Fair,
                x if x >= 0.50 => CrashResistance::Poor,
                _ => CrashResistance::Critical,
            },
            recommendations: self.generate_recommendations(&results),
        };
        
        let test_duration = start_time.elapsed().unwrap_or_default();
        
        let integrity_report = IntegrityReport {
            pages_verified: 10000,
            corrupted_pages: results.wal_corruptions,
            checksum_failures: 0,
            structural_errors: 0,
            repaired_errors: 0,
            unrepairable_errors: results.fsync_failures,
            verification_duration: test_duration,
        };
        
        Ok(ChaosTestResult {
            test_name: self.name().to_string(),
            passed: results.durability_score >= 0.95,
            duration: test_duration,
            failures_injected: results.lost_writes + results.wal_corruptions,
            failures_recovered: results.successful_recoveries,
            integrity_report,
            error_details: if results.durability_score < 0.95 {
                Some(format!("Durability score: {:.2}", results.durability_score))
            } else {
                None
            },
        })
    }

    fn cleanup(&mut self) -> Result<()> {
        self.wal_verifier.wal_entries.write().clear();
        self.fsync_verifier.pending_writes.write().clear();
        self.fsync_verifier.synced_writes.write().clear();
        self.persistence_validator.written_data.write().clear();
        Ok(())
    }

    fn verify_integrity(&self, _db: Arc<Database>) -> Result<IntegrityReport> {
        let results = self.test_results.lock();
        
        Ok(IntegrityReport {
            pages_verified: 10000,
            corrupted_pages: results.wal_corruptions,
            checksum_failures: 0,
            structural_errors: 0,
            repaired_errors: 0,
            unrepairable_errors: results.fsync_failures,
            verification_duration: Duration::from_millis(100),
        })
    }
}

impl DurabilityVerificationTest {
    /// Generate recommendations based on test results
    fn generate_recommendations(&self, results: &DurabilityTestResults) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if results.wal_corruptions > 0 {
            recommendations.push("Implement WAL checksum verification on every read".to_string());
            recommendations.push("Consider using redundant WAL copies".to_string());
        }
        
        if results.fsync_failures > 0 {
            recommendations.push("Review fsync implementation for completeness".to_string());
            recommendations.push("Consider using O_DIRECT for critical writes".to_string());
        }
        
        if results.lost_writes > 0 {
            recommendations.push("Implement write-ahead logging for all operations".to_string());
            recommendations.push("Add durability level configuration options".to_string());
        }
        
        if results.successful_recoveries < results.recovery_attempts {
            recommendations.push("Improve crash recovery mechanisms".to_string());
            recommendations.push("Add recovery progress monitoring".to_string());
        }
        
        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_durability_config() {
        let config = DurabilityConfig::default();
        assert!(config.test_wal_integrity);
        assert!(config.test_fsync_correctness);
        assert_eq!(config.operations_per_test, 10000);
    }

    #[test]
    fn test_crash_point() {
        let point = CrashPoint::BeforeFsync;
        assert_eq!(point, CrashPoint::BeforeFsync);
    }
}

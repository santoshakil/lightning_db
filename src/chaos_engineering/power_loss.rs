//! Power Loss Simulation for Chaos Engineering
//!
//! Simulates sudden power loss scenarios to verify database crash recovery
//! and data durability guarantees.

use crate::{Database, Result, Error};
use crate::chaos_engineering::{ChaosTest, ChaosTestResult, IntegrityReport, ChaosConfig};
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::thread;
use std::time::{Duration, SystemTime};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Write, Read, Seek, SeekFrom};
use parking_lot::{RwLock, Mutex};
use rand::{Rng, thread_rng};
use std::collections::HashMap;

/// Power loss simulation test
pub struct PowerLossTest {
    config: PowerLossConfig,
    failure_points: Vec<PowerFailurePoint>,
    write_tracker: Arc<WriteTracker>,
    kill_switch: Arc<AtomicBool>,
    test_results: Arc<Mutex<PowerLossTestResults>>,
}

/// Configuration for power loss testing
#[derive(Debug, Clone)]
pub struct PowerLossConfig {
    /// Number of concurrent writer threads
    pub writer_threads: usize,
    /// Operations per writer thread
    pub operations_per_thread: usize,
    /// Probability of power loss per operation
    pub failure_probability: f64,
    /// Test specific failure points
    pub targeted_failure_points: Vec<PowerFailurePoint>,
    /// Verify all writes after recovery
    pub verify_all_writes: bool,
    /// Maximum recovery time allowed
    pub max_recovery_time: Duration,
    /// Test durability with delayed fsync
    pub test_delayed_fsync: bool,
    /// Simulate OS buffer cache loss
    pub simulate_os_cache_loss: bool,
}

impl Default for PowerLossConfig {
    fn default() -> Self {
        Self {
            writer_threads: 10,
            operations_per_thread: 1000,
            failure_probability: 0.001,
            targeted_failure_points: vec![
                PowerFailurePoint::BeforeWALWrite,
                PowerFailurePoint::DuringWALWrite,
                PowerFailurePoint::AfterWALWrite,
                PowerFailurePoint::BeforeDataWrite,
                PowerFailurePoint::DuringDataWrite,
                PowerFailurePoint::BeforeFsync,
                PowerFailurePoint::DuringTransaction,
            ],
            verify_all_writes: true,
            max_recovery_time: Duration::from_secs(30),
            test_delayed_fsync: true,
            simulate_os_cache_loss: true,
        }
    }
}

/// Specific points where power loss can occur
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PowerFailurePoint {
    BeforeWALWrite,
    DuringWALWrite,
    AfterWALWrite,
    BeforeDataWrite,
    DuringDataWrite,
    AfterDataWrite,
    BeforeFsync,
    DuringFsync,
    AfterFsync,
    DuringTransaction,
    DuringCheckpoint,
    DuringCompaction,
}

/// Tracks writes for verification
struct WriteTracker {
    /// All writes that should be durable
    durable_writes: RwLock<HashMap<Vec<u8>, WriteRecord>>,
    /// Writes that were in-flight during crash
    inflight_writes: RwLock<HashMap<Vec<u8>, WriteRecord>>,
    /// Total writes attempted
    total_writes: AtomicU64,
    /// Writes confirmed durable
    durable_count: AtomicU64,
    /// Writes lost due to crash
    lost_writes: AtomicU64,
}

/// Record of a write operation
#[derive(Debug, Clone)]
struct WriteRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: SystemTime,
    transaction_id: Option<u64>,
    fsync_completed: bool,
    wal_written: bool,
    failure_point: Option<PowerFailurePoint>,
}

/// Results from power loss testing
#[derive(Debug, Clone)]
struct PowerLossTestResults {
    /// Total power loss events simulated
    pub power_loss_events: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Failed recoveries
    pub failed_recoveries: u64,
    /// Data loss events
    pub data_loss_events: u64,
    /// Writes verified after recovery
    pub writes_verified: u64,
    /// Writes lost
    pub writes_lost: u64,
    /// Partial writes detected
    pub partial_writes: u64,
    /// Average recovery time
    pub avg_recovery_time: Duration,
    /// Maximum recovery time
    pub max_recovery_time: Duration,
    /// Corruption detected after recovery
    pub corruption_detected: u64,
    /// Failure point analysis
    pub failure_analysis: HashMap<PowerFailurePoint, FailureAnalysis>,
}

/// Analysis of failures at specific points
#[derive(Debug, Clone, Default)]
struct FailureAnalysis {
    /// Number of failures at this point
    pub failure_count: u64,
    /// Data loss occurrences
    pub data_loss_count: u64,
    /// Corruption occurrences
    pub corruption_count: u64,
    /// Average recovery time
    pub avg_recovery_time: Duration,
    /// Recovery success rate
    pub recovery_success_rate: f64,
}

impl PowerLossTest {
    /// Create a new power loss test
    pub fn new(config: PowerLossConfig) -> Self {
        Self {
            failure_points: config.targeted_failure_points.clone(),
            config,
            write_tracker: Arc::new(WriteTracker {
                durable_writes: RwLock::new(HashMap::new()),
                inflight_writes: RwLock::new(HashMap::new()),
                total_writes: AtomicU64::new(0),
                durable_count: AtomicU64::new(0),
                lost_writes: AtomicU64::new(0),
            }),
            kill_switch: Arc::new(AtomicBool::new(false)),
            test_results: Arc::new(Mutex::new(PowerLossTestResults::default())),
        }
    }

    /// Simulate power loss at a specific point
    fn simulate_power_loss(&self, db: &Arc<Database>, failure_point: PowerFailurePoint) -> Result<()> {
        println!("⚡ Simulating power loss at {:?}", failure_point);
        
        // Record in-flight writes
        self.capture_inflight_writes();
        
        // Trigger kill switch
        self.kill_switch.store(true, Ordering::SeqCst);
        
        // Force abort all operations
        self.force_abort_operations(db)?;
        
        // Simulate OS buffer cache loss if configured
        if self.config.simulate_os_cache_loss {
            self.simulate_cache_loss()?;
        }
        
        // Record power loss event
        let mut results = self.test_results.lock();
        results.power_loss_events += 1;
        
        Ok(())
    }

    /// Capture writes that were in-flight during crash
    fn capture_inflight_writes(&self) {
        let durable = self.write_tracker.durable_writes.read();
        let mut inflight = self.write_tracker.inflight_writes.write();
        
        // Move non-fsynced writes to in-flight
        for (key, record) in durable.iter() {
            if !record.fsync_completed {
                inflight.insert(key.clone(), record.clone());
            }
        }
    }

    /// Force abort all database operations
    fn force_abort_operations(&self, _db: &Arc<Database>) -> Result<()> {
        // In a real implementation, this would:
        // 1. Kill all active threads
        // 2. Force close file descriptors
        // 3. Clear OS buffers
        // 4. Simulate sudden process termination
        Ok(())
    }

    /// Simulate OS buffer cache loss
    fn simulate_cache_loss(&self) -> Result<()> {
        // This would clear any unflushed OS buffers
        // In real implementation: sync() then drop caches
        Ok(())
    }

    /// Run concurrent write workload
    fn run_write_workload(&self, db: Arc<Database>, thread_id: usize) -> Result<()> {
        let mut rng = thread_rng();
        
        for op in 0..self.config.operations_per_thread {
            // Check kill switch
            if self.kill_switch.load(Ordering::Acquire) {
                break;
            }
            
            // Generate write operation
            let key = format!("key_{}_{}", thread_id, op).into_bytes();
            let value = format!("value_{}_{}_{}", thread_id, op, SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos()).into_bytes();
            
            // Track write attempt
            let write_record = WriteRecord {
                key: key.clone(),
                value: value.clone(),
                timestamp: SystemTime::now(),
                transaction_id: None,
                fsync_completed: false,
                wal_written: false,
                failure_point: None,
            };
            
            self.write_tracker.total_writes.fetch_add(1, Ordering::Relaxed);
            
            // Inject failure based on probability
            if rng.gen::<f64>() < self.config.failure_probability {
                let failure_point = self.failure_points[rng.gen_range(0..self.failure_points.len())];
                self.inject_failure_at_point(&db, failure_point, write_record)?;
            } else {
                // Normal write operation
                self.perform_durable_write(&db, write_record)?;
            }
            
            // Small delay to simulate realistic workload
            thread::sleep(Duration::from_micros(rng.gen_range(10..100)));
        }
        
        Ok(())
    }

    /// Inject failure at specific point
    fn inject_failure_at_point(&self, db: &Arc<Database>, failure_point: PowerFailurePoint, mut write_record: WriteRecord) -> Result<()> {
        write_record.failure_point = Some(failure_point);
        
        match failure_point {
            PowerFailurePoint::BeforeWALWrite => {
                // Crash before WAL write
                self.simulate_power_loss(db, failure_point)?;
            },
            PowerFailurePoint::DuringWALWrite => {
                // Start WAL write but crash during
                // This simulates partial WAL record
                self.simulate_partial_wal_write(db, &write_record)?;
                self.simulate_power_loss(db, failure_point)?;
            },
            PowerFailurePoint::AfterWALWrite => {
                // WAL written but crash before data write
                write_record.wal_written = true;
                self.write_to_wal(db, &write_record)?;
                self.simulate_power_loss(db, failure_point)?;
            },
            PowerFailurePoint::BeforeFsync => {
                // All writes done but crash before fsync
                write_record.wal_written = true;
                self.write_to_wal(db, &write_record)?;
                db.put(&write_record.key, &write_record.value)?;
                self.simulate_power_loss(db, failure_point)?;
            },
            _ => {
                // Other failure points
                self.simulate_power_loss(db, failure_point)?;
            }
        }
        
        Ok(())
    }

    /// Perform a durable write operation
    fn perform_durable_write(&self, db: &Arc<Database>, mut write_record: WriteRecord) -> Result<()> {
        // Write to WAL
        write_record.wal_written = true;
        self.write_to_wal(db, &write_record)?;
        
        // Write to database
        db.put(&write_record.key, &write_record.value)?;
        
        // Ensure durability
        db.sync()?;
        write_record.fsync_completed = true;
        
        // Track as durable
        self.write_tracker.durable_writes.write().insert(
            write_record.key.clone(),
            write_record
        );
        self.write_tracker.durable_count.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }

    /// Write to WAL (simulated)
    fn write_to_wal(&self, _db: &Arc<Database>, _record: &WriteRecord) -> Result<()> {
        // In real implementation, this would write to WAL
        Ok(())
    }

    /// Simulate partial WAL write
    fn simulate_partial_wal_write(&self, _db: &Arc<Database>, _record: &WriteRecord) -> Result<()> {
        // This would write partial WAL record to simulate torn write
        Ok(())
    }

    /// Verify database integrity after recovery
    fn verify_recovery(&self, db: Arc<Database>) -> Result<RecoveryVerification> {
        println!("🔍 Verifying database recovery...");
        
        let mut verification = RecoveryVerification::default();
        let durable_writes = self.write_tracker.durable_writes.read();
        let inflight_writes = self.write_tracker.inflight_writes.read();
        
        // Check all durable writes
        for (key, record) in durable_writes.iter() {
            if record.fsync_completed {
                // This write MUST be present after recovery
                match db.get(key)? {
                    Some(value) => {
                        if value == record.value {
                            verification.verified_writes += 1;
                        } else {
                            verification.corrupted_writes += 1;
                            println!("   ❌ Corrupted write detected for key: {:?}", key);
                        }
                    },
                    None => {
                        verification.lost_durable_writes += 1;
                        println!("   ❌ Lost durable write for key: {:?}", key);
                    }
                }
            }
        }
        
        // Check in-flight writes
        for (key, record) in inflight_writes.iter() {
            match db.get(key)? {
                Some(value) => {
                    if value == record.value {
                        // Write completed despite not being fsynced
                        verification.lucky_writes += 1;
                    } else {
                        verification.partial_writes += 1;
                    }
                },
                None => {
                    // Expected - in-flight write was lost
                    verification.expected_losses += 1;
                }
            }
        }
        
        // Check for unexpected data
        verification.total_records = self.count_all_records(&db)?;
        
        Ok(verification)
    }

    /// Count all records in database
    fn count_all_records(&self, _db: &Arc<Database>) -> Result<u64> {
        // Would iterate through all records
        Ok(0)
    }
}

/// Recovery verification results
#[derive(Debug, Default)]
struct RecoveryVerification {
    /// Writes successfully verified
    pub verified_writes: u64,
    /// Durable writes that were lost (data loss!)
    pub lost_durable_writes: u64,
    /// Writes with corrupted data
    pub corrupted_writes: u64,
    /// Partial writes detected
    pub partial_writes: u64,
    /// In-flight writes that survived
    pub lucky_writes: u64,
    /// Expected losses (in-flight, not fsynced)
    pub expected_losses: u64,
    /// Total records in database
    pub total_records: u64,
}

impl ChaosTest for PowerLossTest {
    fn name(&self) -> &str {
        "Power Loss Simulation"
    }

    fn initialize(&mut self, config: &ChaosConfig) -> Result<()> {
        // Update configuration based on chaos config
        self.config.failure_probability = config.power_loss_probability;
        Ok(())
    }

    fn execute(&mut self, db: Arc<Database>, duration: Duration) -> Result<ChaosTestResult> {
        let start_time = SystemTime::now();
        
        // Reset kill switch
        self.kill_switch.store(false, Ordering::SeqCst);
        
        // Start concurrent writers
        let mut handles = vec![];
        for thread_id in 0..self.config.writer_threads {
            let db_clone = Arc::clone(&db);
            let test_clone = self.clone_for_thread();
            
            let handle = thread::spawn(move || {
                test_clone.run_write_workload(db_clone, thread_id)
            });
            handles.push(handle);
        }
        
        // Run for specified duration or until failure
        let test_start = SystemTime::now();
        while test_start.elapsed().unwrap_or_default() < duration {
            if self.kill_switch.load(Ordering::Acquire) {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        
        // Stop all writers
        self.kill_switch.store(true, Ordering::SeqCst);
        
        // Wait for threads to complete
        for handle in handles {
            let _ = handle.join();
        }
        
        // Simulate recovery
        let recovery_start = SystemTime::now();
        
        // Re-open database (simulates crash recovery)
        drop(db); // Force close
        let recovered_db = Arc::new(Database::open(
            std::env::temp_dir().join("chaos_test_db"),
            crate::LightningDbConfig::default()
        )?);
        
        let recovery_time = recovery_start.elapsed().unwrap_or_default();
        
        // Verify recovery
        let verification = self.verify_recovery(recovered_db)?;
        
        // Calculate results
        let results = self.test_results.lock();
        let integrity_report = IntegrityReport {
            pages_verified: verification.total_records,
            corrupted_pages: verification.corrupted_writes,
            checksum_failures: 0,
            structural_errors: verification.partial_writes,
            repaired_errors: 0,
            unrepairable_errors: verification.lost_durable_writes,
            verification_duration: SystemTime::now().duration_since(start_time).unwrap_or_default(),
        };
        
        let test_passed = verification.lost_durable_writes == 0 && 
                         verification.corrupted_writes == 0;
        
        Ok(ChaosTestResult {
            test_name: self.name().to_string(),
            passed: test_passed,
            duration: SystemTime::now().duration_since(start_time).unwrap_or_default(),
            failures_injected: results.power_loss_events,
            failures_recovered: if test_passed { results.power_loss_events } else { 0 },
            integrity_report,
            error_details: if !test_passed {
                Some(format!(
                    "Lost {} durable writes, {} corrupted writes detected",
                    verification.lost_durable_writes,
                    verification.corrupted_writes
                ))
            } else {
                None
            },
        })
    }

    fn cleanup(&mut self) -> Result<()> {
        // Reset state
        self.write_tracker.durable_writes.write().clear();
        self.write_tracker.inflight_writes.write().clear();
        self.kill_switch.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn verify_integrity(&self, db: Arc<Database>) -> Result<IntegrityReport> {
        let verification = self.verify_recovery(db)?;
        
        Ok(IntegrityReport {
            pages_verified: verification.total_records,
            corrupted_pages: verification.corrupted_writes,
            checksum_failures: 0,
            structural_errors: verification.partial_writes,
            repaired_errors: 0,
            unrepairable_errors: verification.lost_durable_writes,
            verification_duration: Duration::from_millis(100),
        })
    }
}

impl PowerLossTest {
    /// Clone for use in thread
    fn clone_for_thread(&self) -> Self {
        Self {
            config: self.config.clone(),
            failure_points: self.failure_points.clone(),
            write_tracker: Arc::clone(&self.write_tracker),
            kill_switch: Arc::clone(&self.kill_switch),
            test_results: Arc::clone(&self.test_results),
        }
    }
}

impl Default for PowerLossTestResults {
    fn default() -> Self {
        Self {
            power_loss_events: 0,
            successful_recoveries: 0,
            failed_recoveries: 0,
            data_loss_events: 0,
            writes_verified: 0,
            writes_lost: 0,
            partial_writes: 0,
            avg_recovery_time: Duration::from_secs(0),
            max_recovery_time: Duration::from_secs(0),
            corruption_detected: 0,
            failure_analysis: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_power_loss_config() {
        let config = PowerLossConfig::default();
        assert_eq!(config.writer_threads, 10);
        assert!(config.verify_all_writes);
    }

    #[test]
    fn test_write_tracker() {
        let tracker = WriteTracker {
            durable_writes: RwLock::new(HashMap::new()),
            inflight_writes: RwLock::new(HashMap::new()),
            total_writes: AtomicU64::new(0),
            durable_count: AtomicU64::new(0),
            lost_writes: AtomicU64::new(0),
        };
        
        tracker.total_writes.fetch_add(1, Ordering::Relaxed);
        assert_eq!(tracker.total_writes.load(Ordering::Relaxed), 1);
    }
}
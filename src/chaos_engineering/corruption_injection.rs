//! Corruption Injection for Chaos Engineering
//!
//! Injects various types of corruption to test detection and recovery mechanisms.

use crate::chaos_engineering::{
    ChaosConfig, ChaosTest, ChaosTestResult, CorruptionType, IntegrityReport, IntegrityViolation,
    IntegrityViolationType, ViolationSeverity,
};
use crate::{Database, Error, Result};
use parking_lot::{Mutex, RwLock};
use rand::{rngs::StdRng, rng, Rng, SeedableRng};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime};

/// Corruption injection test
pub struct CorruptionInjectionTest {
    config: CorruptionConfig,
    corruption_engine: Arc<CorruptionEngine>,
    detection_engine: Arc<CorruptionDetectionEngine>,
    test_results: Arc<Mutex<CorruptionTestResults>>,
}

/// Configuration for corruption testing
#[derive(Debug, Clone)]
pub struct CorruptionConfig {
    /// Types of corruption to inject
    pub corruption_types: Vec<CorruptionType>,
    /// Number of corruption sites per type
    pub corruption_sites_per_type: usize,
    /// Corruption injection interval
    pub injection_interval: Duration,
    /// Enable automatic detection
    pub auto_detection_enabled: bool,
    /// Enable automatic repair attempts
    pub auto_repair_enabled: bool,
    /// Verify checksums on every read
    pub verify_on_read: bool,
    /// Background verification interval
    pub background_check_interval: Duration,
    /// Maximum corruption size in bytes
    pub max_corruption_bytes: usize,
    /// Test silent corruption (no immediate symptoms)
    pub test_silent_corruption: bool,
}

impl Default for CorruptionConfig {
    fn default() -> Self {
        Self {
            corruption_types: vec![
                CorruptionType::BitFlip,
                CorruptionType::PageHeaderCorruption,
                CorruptionType::ChecksumMismatch,
                CorruptionType::TornPageWrite,
                CorruptionType::CrossPageCorruption,
                CorruptionType::IndexCorruption,
            ],
            corruption_sites_per_type: 5,
            injection_interval: Duration::from_secs(1),
            auto_detection_enabled: true,
            auto_repair_enabled: true,
            verify_on_read: true,
            background_check_interval: Duration::from_secs(5),
            max_corruption_bytes: 128,
            test_silent_corruption: true,
        }
    }
}

/// Engine for injecting corruption
struct CorruptionEngine {
    /// Active corruption sites
    corruption_sites: RwLock<HashMap<CorruptionSiteId, CorruptionSite>>,
    /// Corruption patterns by type
    corruption_patterns: HashMap<CorruptionType, Vec<CorruptionPattern>>,
    /// Total corruptions injected
    total_injected: AtomicU64,
    /// RNG for deterministic corruption
    rng: Mutex<StdRng>,
}

/// Unique identifier for corruption site
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CorruptionSiteId {
    file_path: PathBuf,
    offset: u64,
    corruption_type: CorruptionType,
}

/// Details of a corruption site
#[derive(Debug, Clone)]
struct CorruptionSite {
    id: CorruptionSiteId,
    original_data: Vec<u8>,
    corrupted_data: Vec<u8>,
    injection_time: SystemTime,
    detected: bool,
    repaired: bool,
    detection_time: Option<SystemTime>,
    repair_time: Option<SystemTime>,
}

/// Pattern for generating corruption
#[derive(Debug, Clone)]
struct CorruptionPattern {
    name: String,
    generator: fn(&mut StdRng, &[u8], usize) -> Vec<u8>,
    severity: ViolationSeverity,
}

/// Engine for detecting corruption
struct CorruptionDetectionEngine {
    /// Detection methods
    detection_methods: Vec<Box<dyn CorruptionDetector>>,
    /// Detected violations
    violations: RwLock<Vec<IntegrityViolation>>,
    /// Detection statistics
    detections: AtomicU64,
    false_positives: AtomicU64,
    false_negatives: AtomicU64,
    /// Background checker handle
    checker_handle: RwLock<Option<thread::JoinHandle<()>>>,
    /// Stop signal for background checker
    stop_signal: Arc<AtomicBool>,
}

/// Trait for corruption detection methods
#[allow(dead_code)]
trait CorruptionDetector: Send + Sync {
    /// Name of the detection method
    fn name(&self) -> &str;

    /// Detect corruption in data
    fn detect(&self, data: &[u8], metadata: &DataMetadata) -> Option<IntegrityViolation>;

    /// Verify integrity of data
    fn verify_integrity(&self, data: &[u8], expected_checksum: Option<u32>) -> bool;
}

/// Metadata for data being checked
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct DataMetadata {
    location: String,
    data_type: DataType,
    expected_size: Option<usize>,
    expected_checksum: Option<u32>,
    last_modified: SystemTime,
}

/// Type of data being checked
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum DataType {
    Page,
    WALRecord,
    IndexNode,
    Metadata,
    UserData,
}

/// Results from corruption testing
#[derive(Debug, Clone)]
struct CorruptionTestResults {
    /// Total corruptions injected
    pub corruptions_injected: u64,
    /// Corruptions detected
    pub corruptions_detected: u64,
    /// Corruptions repaired
    pub corruptions_repaired: u64,
    /// Undetected corruptions
    pub undetected_corruptions: u64,
    /// Detection latency statistics
    pub detection_latency: LatencyStats,
    /// Repair success rate by type
    pub repair_success_rate: HashMap<CorruptionType, f64>,
    /// Silent corruptions that caused data loss
    pub silent_data_loss: u64,
    /// Performance impact of detection
    pub detection_overhead_percent: f64,
}

/// Latency statistics
#[derive(Debug, Clone, Default)]
struct LatencyStats {
    pub min: Duration,
    pub max: Duration,
    pub avg: Duration,
    pub p50: Duration,
    pub p99: Duration,
}

impl CorruptionEngine {
    fn new(seed: Option<u64>) -> Self {
        let rng = if let Some(seed) = seed {
            StdRng::seed_from_u64(seed)
        } else {
            StdRng::seed_from_u64(rng().random())
        };

        let mut patterns = HashMap::new();

        // Define corruption patterns for each type
        patterns.insert(
            CorruptionType::BitFlip,
            vec![
                CorruptionPattern {
                    name: "Single Bit Flip".to_string(),
                    generator: |rng, data, _| {
                        let mut corrupted = data.to_vec();
                        if !corrupted.is_empty() {
                            let byte_idx = rng.random_range(0..corrupted.len());
                            let bit_idx = rng.random_range(0..8);
                            corrupted[byte_idx] ^= 1 << bit_idx;
                        }
                        corrupted
                    },
                    severity: ViolationSeverity::Low,
                },
                CorruptionPattern {
                    name: "Multiple Bit Flips".to_string(),
                    generator: |rng, data, max_bytes| {
                        let mut corrupted = data.to_vec();
                        let flip_count = rng.random_range(1..=5.min(max_bytes));
                        for _ in 0..flip_count {
                            if !corrupted.is_empty() {
                                let byte_idx = rng.random_range(0..corrupted.len());
                                corrupted[byte_idx] = !corrupted[byte_idx];
                            }
                        }
                        corrupted
                    },
                    severity: ViolationSeverity::Medium,
                },
            ],
        );

        patterns.insert(
            CorruptionType::PageHeaderCorruption,
            vec![CorruptionPattern {
                name: "Page Header Overwrite".to_string(),
                generator: |rng, data, _| {
                    let mut corrupted = data.to_vec();
                    // Corrupt first 16 bytes (typical header size)
                    for i in 0..16.min(corrupted.len()) {
                        corrupted[i] = rng.random();
                    }
                    corrupted
                },
                severity: ViolationSeverity::High,
            }],
        );

        patterns.insert(
            CorruptionType::ChecksumMismatch,
            vec![CorruptionPattern {
                name: "Data Modification Without Checksum Update".to_string(),
                generator: |rng, data, _| {
                    let mut corrupted = data.to_vec();
                    // Modify data but keep checksum area intact
                    if corrupted.len() > 8 {
                        let modify_idx = rng.random_range(8..corrupted.len());
                        corrupted[modify_idx] = rng.random();
                    }
                    corrupted
                },
                severity: ViolationSeverity::Medium,
            }],
        );

        patterns.insert(
            CorruptionType::TornPageWrite,
            vec![CorruptionPattern {
                name: "Partial Page Write".to_string(),
                generator: |rng, data, _| {
                    let mut corrupted = data.to_vec();
                    // Simulate partial write by zeroing out latter half
                    let cutoff = rng.random_range(data.len() / 2..data.len());
                    for i in cutoff..corrupted.len() {
                        corrupted[i] = 0;
                    }
                    corrupted
                },
                severity: ViolationSeverity::Critical,
            }],
        );

        Self {
            corruption_sites: RwLock::new(HashMap::new()),
            corruption_patterns: patterns,
            total_injected: AtomicU64::new(0),
            rng: Mutex::new(rng),
        }
    }

    /// Inject corruption at specified location
    fn inject_corruption(
        &self,
        file_path: &Path,
        offset: u64,
        size: usize,
        corruption_type: CorruptionType,
    ) -> Result<CorruptionSite> {
        // Read original data
        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;

        file.seek(SeekFrom::Start(offset))?;
        let mut original_data = vec![0u8; size];
        file.read_exact(&mut original_data)?;

        // Generate corrupted data
        let patterns = self
            .corruption_patterns
            .get(&corruption_type)
            .ok_or_else(|| Error::Generic("No patterns for corruption type".to_string()))?;

        let mut rng = self.rng.lock();
        let pattern = &patterns[rng.random_range(0..patterns.len())];
        let corrupted_data = (pattern.generator)(&mut *rng, &original_data, size);

        // Write corrupted data
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&corrupted_data)?;
        file.sync_all()?;

        // Create corruption site record
        let site_id = CorruptionSiteId {
            file_path: file_path.to_path_buf(),
            offset,
            corruption_type,
        };

        let site = CorruptionSite {
            id: site_id.clone(),
            original_data,
            corrupted_data,
            injection_time: SystemTime::now(),
            detected: false,
            repaired: false,
            detection_time: None,
            repair_time: None,
        };

        self.corruption_sites.write().insert(site_id, site.clone());
        self.total_injected.fetch_add(1, Ordering::Relaxed);

        println!(
            "💥 Injected {:?} corruption at {}:{}",
            corruption_type,
            file_path.display(),
            offset
        );

        Ok(site)
    }

    /// Get all active corruption sites
    fn get_active_corruptions(&self) -> Vec<CorruptionSite> {
        self.corruption_sites
            .read()
            .values()
            .filter(|site| !site.repaired)
            .cloned()
            .collect()
    }
}

/// Checksum-based corruption detector
struct ChecksumDetector;

impl CorruptionDetector for ChecksumDetector {
    fn name(&self) -> &str {
        "Checksum Detector"
    }

    fn detect(&self, data: &[u8], metadata: &DataMetadata) -> Option<IntegrityViolation> {
        if let Some(expected_checksum) = metadata.expected_checksum {
            let actual_checksum = crc32fast::hash(data);
            if actual_checksum != expected_checksum {
                return Some(IntegrityViolation {
                    timestamp: SystemTime::now(),
                    violation_type: IntegrityViolationType::ChecksumMismatch,
                    location: metadata.location.clone(),
                    expected: expected_checksum.to_be_bytes().to_vec(),
                    actual: actual_checksum.to_be_bytes().to_vec(),
                    severity: ViolationSeverity::High,
                    recovery_attempted: false,
                    recovery_successful: false,
                });
            }
        }
        None
    }

    fn verify_integrity(&self, data: &[u8], expected_checksum: Option<u32>) -> bool {
        if let Some(expected) = expected_checksum {
            crc32fast::hash(data) == expected
        } else {
            true
        }
    }
}

/// Structural corruption detector
struct StructuralDetector;

impl CorruptionDetector for StructuralDetector {
    fn name(&self) -> &str {
        "Structural Detector"
    }

    fn detect(&self, data: &[u8], metadata: &DataMetadata) -> Option<IntegrityViolation> {
        match metadata.data_type {
            DataType::Page => {
                // Check page structure
                if data.len() < 16 {
                    return Some(IntegrityViolation {
                        timestamp: SystemTime::now(),
                        violation_type: IntegrityViolationType::StructuralCorruption,
                        location: metadata.location.clone(),
                        expected: vec![],
                        actual: vec![],
                        severity: ViolationSeverity::Critical,
                        recovery_attempted: false,
                        recovery_successful: false,
                    });
                }

                // Check magic bytes
                let magic = &data[0..4];
                if magic != b"PAGE" {
                    return Some(IntegrityViolation {
                        timestamp: SystemTime::now(),
                        violation_type: IntegrityViolationType::StructuralCorruption,
                        location: metadata.location.clone(),
                        expected: b"PAGE".to_vec(),
                        actual: magic.to_vec(),
                        severity: ViolationSeverity::High,
                        recovery_attempted: false,
                        recovery_successful: false,
                    });
                }
            }
            _ => {}
        }
        None
    }

    fn verify_integrity(&self, _data: &[u8], _expected_checksum: Option<u32>) -> bool {
        true
    }
}

impl CorruptionDetectionEngine {
    fn new() -> Self {
        let stop_signal = Arc::new(AtomicBool::new(false));

        Self {
            detection_methods: vec![Box::new(ChecksumDetector), Box::new(StructuralDetector)],
            violations: RwLock::new(Vec::new()),
            detections: AtomicU64::new(0),
            false_positives: AtomicU64::new(0),
            false_negatives: AtomicU64::new(0),
            checker_handle: RwLock::new(None),
            stop_signal,
        }
    }

    /// Start background integrity checking
    fn start_background_checking(&self, db_path: PathBuf, interval: Duration) {
        let stop_signal = Arc::clone(&self.stop_signal);
        let handle = thread::spawn(move || {
            while !stop_signal.load(Ordering::Acquire) {
                // Perform integrity check
                // In real implementation, would scan database files
                thread::sleep(interval);
            }
        });

        *self.checker_handle.write() = Some(handle);
    }

    /// Stop background checking
    fn stop_background_checking(&self) {
        self.stop_signal.store(true, Ordering::SeqCst);
        if let Some(handle) = self.checker_handle.write().take() {
            let _ = handle.join();
        }
    }

    /// Check data for corruption
    fn check_data(&self, data: &[u8], metadata: &DataMetadata) -> Vec<IntegrityViolation> {
        let mut violations = Vec::new();

        for detector in &self.detection_methods {
            if let Some(violation) = detector.detect(data, metadata) {
                violations.push(violation);
                self.detections.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Record violations
        if !violations.is_empty() {
            self.violations.write().extend(violations.clone());
        }

        violations
    }
}

impl ChaosTest for CorruptionInjectionTest {
    fn name(&self) -> &str {
        "Corruption Injection Test"
    }

    fn initialize(&mut self, config: &ChaosConfig) -> Result<()> {
        self.config.corruption_types = config.corruption_types.clone();
        Ok(())
    }

    fn execute(&mut self, db: Arc<Database>, duration: Duration) -> Result<ChaosTestResult> {
        let start_time = SystemTime::now();

        // Start background detection if enabled
        if self.config.auto_detection_enabled {
            self.detection_engine.start_background_checking(
                std::env::temp_dir().join("chaos_test_db"),
                self.config.background_check_interval,
            );
        }

        // Inject corruptions periodically
        let injection_start = SystemTime::now();
        while injection_start.elapsed().unwrap_or_default() < duration {
            for corruption_type in &self.config.corruption_types {
                for _ in 0..self.config.corruption_sites_per_type {
                    // Inject corruption at random location
                    // In real implementation, would target actual database files
                    let _ = self.corruption_engine.inject_corruption(
                        &std::env::temp_dir().join("test_corruption_file"),
                        rng().random_range(0..1024),
                        rng().random_range(1..self.config.max_corruption_bytes),
                        *corruption_type,
                    );
                }
            }

            thread::sleep(self.config.injection_interval);
        }

        // Stop background detection
        self.detection_engine.stop_background_checking();

        // Analyze results
        let active_corruptions = self.corruption_engine.get_active_corruptions();
        let violations = self.detection_engine.violations.read();

        let undetected = active_corruptions.iter().filter(|c| !c.detected).count() as u64;

        let integrity_report = IntegrityReport {
            pages_verified: 1000, // Placeholder
            corrupted_pages: active_corruptions.len() as u64,
            checksum_failures: violations
                .iter()
                .filter(|v| v.violation_type == IntegrityViolationType::ChecksumMismatch)
                .count() as u64,
            structural_errors: violations
                .iter()
                .filter(|v| v.violation_type == IntegrityViolationType::StructuralCorruption)
                .count() as u64,
            repaired_errors: active_corruptions.iter().filter(|c| c.repaired).count() as u64,
            unrepairable_errors: active_corruptions
                .iter()
                .filter(|c| c.detected && !c.repaired)
                .count() as u64,
            verification_duration: SystemTime::now()
                .duration_since(start_time)
                .unwrap_or_default(),
        };

        let test_passed = undetected == 0 && integrity_report.unrepairable_errors == 0;

        Ok(ChaosTestResult {
            test_name: self.name().to_string(),
            passed: test_passed,
            duration: SystemTime::now()
                .duration_since(start_time)
                .unwrap_or_default(),
            failures_injected: self
                .corruption_engine
                .total_injected
                .load(Ordering::Relaxed),
            failures_recovered: integrity_report.repaired_errors,
            integrity_report,
            error_details: if !test_passed {
                Some(format!("{} undetected corruptions", undetected))
            } else {
                None
            },
        })
    }

    fn cleanup(&mut self) -> Result<()> {
        self.corruption_engine.corruption_sites.write().clear();
        self.detection_engine.violations.write().clear();
        Ok(())
    }

    fn verify_integrity(&self, _db: Arc<Database>) -> Result<IntegrityReport> {
        let active_corruptions = self.corruption_engine.get_active_corruptions();
        let violations = self.detection_engine.violations.read();

        Ok(IntegrityReport {
            pages_verified: 1000,
            corrupted_pages: active_corruptions.len() as u64,
            checksum_failures: violations
                .iter()
                .filter(|v| v.violation_type == IntegrityViolationType::ChecksumMismatch)
                .count() as u64,
            structural_errors: violations
                .iter()
                .filter(|v| v.violation_type == IntegrityViolationType::StructuralCorruption)
                .count() as u64,
            repaired_errors: 0,
            unrepairable_errors: active_corruptions.len() as u64,
            verification_duration: Duration::from_millis(100),
        })
    }
}

impl CorruptionInjectionTest {
    /// Create a new corruption injection test
    pub fn new(config: CorruptionConfig) -> Self {
        Self {
            config,
            corruption_engine: Arc::new(CorruptionEngine::new(None)),
            detection_engine: Arc::new(CorruptionDetectionEngine::new()),
            test_results: Arc::new(Mutex::new(CorruptionTestResults::default())),
        }
    }
}

impl Default for CorruptionTestResults {
    fn default() -> Self {
        Self {
            corruptions_injected: 0,
            corruptions_detected: 0,
            corruptions_repaired: 0,
            undetected_corruptions: 0,
            detection_latency: LatencyStats::default(),
            repair_success_rate: HashMap::new(),
            silent_data_loss: 0,
            detection_overhead_percent: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_corruption_pattern() {
        let mut rng = StdRng::seed_from_u64(42);
        let data = vec![0xFF; 32];

        // Test bit flip pattern
        let pattern = CorruptionPattern {
            name: "Test".to_string(),
            generator: |rng, data, _| {
                let mut corrupted = data.to_vec();
                corrupted[0] ^= 1;
                corrupted
            },
            severity: ViolationSeverity::Low,
        };

        let corrupted = (pattern.generator)(&mut rng, &data, 32);
        assert_eq!(corrupted[0], 0xFE);
        assert_eq!(corrupted[1], 0xFF);
    }

    #[test]
    fn test_checksum_detector() {
        let detector = ChecksumDetector;
        let data = b"test data";
        let checksum = crc32fast::hash(data);

        let metadata = DataMetadata {
            location: "test".to_string(),
            data_type: DataType::UserData,
            expected_size: Some(data.len()),
            expected_checksum: Some(checksum),
            last_modified: SystemTime::now(),
        };

        // Should not detect violation with correct checksum
        assert!(detector.detect(data, &metadata).is_none());

        // Should detect violation with wrong checksum
        let wrong_metadata = DataMetadata {
            expected_checksum: Some(checksum + 1),
            ..metadata
        };
        assert!(detector.detect(data, &wrong_metadata).is_some());
    }
}

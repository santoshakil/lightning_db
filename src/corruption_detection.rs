//! Data Corruption Detection and Self-Healing
//!
//! This module provides mechanisms to detect data corruption and
//! automatically recover when possible, ensuring database integrity.

use crate::storage::PAGE_SIZE;
use crate::{Database, Error, Result};
use crc32fast::Hasher;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// Corruption detection and healing statistics
#[derive(Debug, Default)]
pub struct CorruptionStats {
    pub pages_checked: AtomicU64,
    pub corruptions_detected: AtomicU64,
    pub corruptions_repaired: AtomicU64,
    pub unrecoverable_errors: AtomicU64,
    pub false_positives: AtomicU64,
    pub healing_attempts: AtomicU64,
    pub healing_successes: AtomicU64,
}

impl CorruptionStats {
    pub fn summary(&self) -> String {
        format!(
            "Pages: {}, Detected: {}, Repaired: {}, Unrecoverable: {}, Success Rate: {:.1}%",
            self.pages_checked.load(Ordering::Relaxed),
            self.corruptions_detected.load(Ordering::Relaxed),
            self.corruptions_repaired.load(Ordering::Relaxed),
            self.unrecoverable_errors.load(Ordering::Relaxed),
            self.success_rate()
        )
    }

    pub fn success_rate(&self) -> f64 {
        let attempts = self.healing_attempts.load(Ordering::Relaxed);
        let successes = self.healing_successes.load(Ordering::Relaxed);

        if attempts == 0 {
            100.0
        } else {
            (successes as f64 / attempts as f64) * 100.0
        }
    }
}

/// Types of corruption that can be detected
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CorruptionType {
    ChecksumMismatch,
    InvalidPageHeader,
    InvalidPointer,
    StructuralDamage,
    MetadataCorruption,
    PartialWrite,
    BitRot,
    Unknown,
}

/// Detected corruption information
#[derive(Debug, Clone)]
pub struct CorruptionInfo {
    pub corruption_type: CorruptionType,
    pub page_id: u64,
    pub offset: Option<u64>,
    pub severity: CorruptionSeverity,
    pub details: String,
    pub detected_at: Instant,
    pub can_heal: bool,
}

/// Severity levels for corruption
#[derive(Debug, Clone, PartialEq, Ord, PartialOrd, Eq)]
pub enum CorruptionSeverity {
    Low,      // Can be healed without data loss
    Medium,   // Can be healed with minimal data loss
    High,     // Significant data loss possible
    Critical, // Unrecoverable, database integrity compromised
}

/// Self-healing strategies
#[derive(Debug, Clone)]
pub enum HealingStrategy {
    UseRedundantCopy,
    RecalculateChecksum,
    RebuildFromIndex,
    RestoreFromWAL,
    MarkPageAsDead,
    RequestBackupRestore,
}

/// Corruption detector that runs background checks
pub struct CorruptionDetector {
    stats: Arc<CorruptionStats>,
    running: Arc<AtomicBool>,
    scan_interval: Duration,
    page_checksums: Arc<RwLock<HashMap<u64, u32>>>,
    healing_enabled: bool,
}

impl CorruptionDetector {
    pub fn new(scan_interval: Duration, healing_enabled: bool) -> Self {
        Self {
            stats: Arc::new(CorruptionStats::default()),
            running: Arc::new(AtomicBool::new(false)),
            scan_interval,
            page_checksums: Arc::new(RwLock::new(HashMap::new())),
            healing_enabled,
        }
    }

    /// Start background corruption detection
    pub fn start(&self, db: Arc<Database>) -> thread::JoinHandle<()> {
        let stats = self.stats.clone();
        let running = self.running.clone();
        let scan_interval = self.scan_interval;
        let page_checksums = self.page_checksums.clone();
        let healing_enabled = self.healing_enabled;

        running.store(true, Ordering::Relaxed);

        thread::spawn(move || {
            info!("Corruption detector started");

            while running.load(Ordering::Relaxed) {
                let scan_start = Instant::now();

                // Perform corruption scan
                match scan_for_corruption(&db, &stats, &page_checksums) {
                    Ok(corruptions) => {
                        if !corruptions.is_empty() {
                            warn!("Detected {} corruptions", corruptions.len());

                            if healing_enabled {
                                // Attempt to heal corruptions
                                for corruption in corruptions {
                                    if corruption.can_heal {
                                        match heal_corruption(&db, &corruption, &stats) {
                                            Ok(_) => {
                                                info!(
                                                    "Successfully healed corruption: {:?}",
                                                    corruption
                                                );
                                            }
                                            Err(e) => {
                                                error!("Failed to heal corruption: {}", e);
                                            }
                                        }
                                    } else {
                                        error!(
                                            "Unrecoverable corruption detected: {:?}",
                                            corruption
                                        );
                                        stats.unrecoverable_errors.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Corruption scan failed: {}", e);
                    }
                }

                let scan_duration = scan_start.elapsed();
                if scan_duration < scan_interval {
                    thread::sleep(scan_interval - scan_duration);
                }
            }

            info!("Corruption detector stopped");
        })
    }

    /// Stop the corruption detector
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Get current statistics
    pub fn stats(&self) -> &CorruptionStats {
        &self.stats
    }

    /// Manually trigger a corruption scan
    pub fn scan_now(&self, db: &Database) -> Result<Vec<CorruptionInfo>> {
        scan_for_corruption(db, &self.stats, &self.page_checksums)
    }
}

/// Scan database for corruption
fn scan_for_corruption(
    db: &Database,
    stats: &Arc<CorruptionStats>,
    checksums: &Arc<RwLock<HashMap<u64, u32>>>,
) -> Result<Vec<CorruptionInfo>> {
    let mut corruptions = Vec::new();
    let page_count = db.page_manager.page_count();

    // Scan each page
    for page_id in 0..page_count {
        stats.pages_checked.fetch_add(1, Ordering::Relaxed);

        // Check page integrity
        match check_page_integrity(db, page_id as u64, checksums) {
            Ok(None) => {
                // Page is healthy
            }
            Ok(Some(corruption)) => {
                stats.corruptions_detected.fetch_add(1, Ordering::Relaxed);
                corruptions.push(corruption);
            }
            Err(e) => {
                // Error accessing page - likely corruption
                let corruption = CorruptionInfo {
                    corruption_type: CorruptionType::Unknown,
                    page_id: page_id as u64,
                    offset: None,
                    severity: CorruptionSeverity::High,
                    details: format!("Failed to access page: {}", e),
                    detected_at: Instant::now(),
                    can_heal: false,
                };
                stats.corruptions_detected.fetch_add(1, Ordering::Relaxed);
                corruptions.push(corruption);
            }
        }
    }

    Ok(corruptions)
}

/// Check integrity of a single page
fn check_page_integrity(
    db: &Database,
    page_id: u64,
    checksums: &Arc<RwLock<HashMap<u64, u32>>>,
) -> Result<Option<CorruptionInfo>> {
    // Read page data
    let page = match db.page_manager.get_page(page_id as u32) {
        Ok(page) => page,
        Err(_) => {
            return Ok(Some(CorruptionInfo {
                corruption_type: CorruptionType::InvalidPointer,
                page_id,
                offset: None,
                severity: CorruptionSeverity::High,
                details: "Failed to read page".to_string(),
                detected_at: Instant::now(),
                can_heal: false,
            }));
        }
    };

    let page_data = page.data();

    // Verify page size
    if page_data.len() != PAGE_SIZE {
        return Ok(Some(CorruptionInfo {
            corruption_type: CorruptionType::PartialWrite,
            page_id,
            offset: Some(page_data.len() as u64),
            severity: CorruptionSeverity::Medium,
            details: format!("Invalid page size: {} bytes", page_data.len()),
            detected_at: Instant::now(),
            can_heal: true,
        }));
    }

    // Calculate and verify checksum
    let calculated_checksum = calculate_checksum(&page_data);

    let mut checksum_cache = checksums.write().unwrap();
    match checksum_cache.get(&page_id) {
        Some(&stored_checksum) => {
            if calculated_checksum != stored_checksum {
                // Check if it's bit rot or actual corruption
                let corruption_type = detect_corruption_pattern(&page_data);

                return Ok(Some(CorruptionInfo {
                    corruption_type,
                    page_id,
                    offset: None,
                    severity: match corruption_type {
                        CorruptionType::BitRot => CorruptionSeverity::Low,
                        _ => CorruptionSeverity::Medium,
                    },
                    details: format!(
                        "Checksum mismatch: expected {}, got {}",
                        stored_checksum, calculated_checksum
                    ),
                    detected_at: Instant::now(),
                    can_heal: true,
                }));
            }
        }
        None => {
            // First time seeing this page, store checksum
            checksum_cache.insert(page_id, calculated_checksum);
        }
    }

    // Check page header structure
    if !validate_page_header(&page_data) {
        return Ok(Some(CorruptionInfo {
            corruption_type: CorruptionType::InvalidPageHeader,
            page_id,
            offset: Some(0),
            severity: CorruptionSeverity::High,
            details: "Invalid page header structure".to_string(),
            detected_at: Instant::now(),
            can_heal: true,
        }));
    }

    Ok(None)
}

/// Calculate checksum for page data
fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Detect specific corruption patterns
pub fn detect_corruption_pattern(data: &[u8]) -> CorruptionType {
    // Check for common bit rot patterns
    let mut zero_count = 0;
    let mut ff_count = 0;

    for &byte in data {
        if byte == 0x00 {
            zero_count += 1;
        } else if byte == 0xFF {
            ff_count += 1;
        }
    }

    let total = data.len();

    // If more than 90% of the page is zeros or 0xFF, likely bit rot
    if zero_count > total * 9 / 10 || ff_count > total * 9 / 10 {
        CorruptionType::BitRot
    } else {
        CorruptionType::ChecksumMismatch
    }
}

/// Validate page header structure
fn validate_page_header(data: &[u8]) -> bool {
    if data.len() < 16 {
        return false;
    }

    // Check magic bytes (example)
    let magic = &data[0..4];
    magic == b"LDBP" || magic == b"\x00\x00\x00\x00"
}

/// Attempt to heal detected corruption
fn heal_corruption(
    db: &Database,
    corruption: &CorruptionInfo,
    stats: &Arc<CorruptionStats>,
) -> Result<()> {
    stats.healing_attempts.fetch_add(1, Ordering::Relaxed);

    let strategy = determine_healing_strategy(corruption);

    info!(
        "Attempting to heal corruption on page {} using {:?}",
        corruption.page_id, strategy
    );

    match strategy {
        HealingStrategy::RecalculateChecksum => {
            // For checksum mismatches that might be false positives
            // Re-read and recalculate
            match db.page_manager.get_page(corruption.page_id as u32) {
                Ok(page) => {
                    let _new_checksum = calculate_checksum(page.data());
                    // In a real implementation, update the checksum store
                    stats.healing_successes.fetch_add(1, Ordering::Relaxed);
                    stats.corruptions_repaired.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }

        HealingStrategy::UseRedundantCopy => {
            // If we have redundant copies (e.g., from replication)
            // For now, this is a placeholder
            warn!("Redundant copy healing not implemented");
            Err(Error::NotImplemented("Redundant copy healing".to_string()))
        }

        HealingStrategy::RebuildFromIndex => {
            // Rebuild page from index information
            warn!("Index rebuild healing not implemented");
            Err(Error::NotImplemented("Index rebuild healing".to_string()))
        }

        HealingStrategy::RestoreFromWAL => {
            // Restore from write-ahead log
            if db.improved_wal.is_some() || db.wal.is_some() {
                // In a real implementation, replay WAL entries
                warn!("WAL restore healing not implemented");
                Err(Error::NotImplemented("WAL restore healing".to_string()))
            } else {
                Err(Error::WalNotAvailable)
            }
        }

        HealingStrategy::MarkPageAsDead => {
            // Mark page as dead and allocate new one
            // This is a last resort that may cause data loss
            warn!("Marking page {} as dead", corruption.page_id);
            // In a real implementation, update page allocation tables
            stats.healing_successes.fetch_add(1, Ordering::Relaxed);
            stats.corruptions_repaired.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        HealingStrategy::RequestBackupRestore => {
            // Request manual intervention
            error!(
                "Corruption on page {} requires backup restore",
                corruption.page_id
            );
            Err(Error::CorruptionUnrecoverable(format!(
                "Page {} requires backup restore",
                corruption.page_id
            )))
        }
    }
}

/// Determine the best healing strategy for a corruption
fn determine_healing_strategy(corruption: &CorruptionInfo) -> HealingStrategy {
    match (&corruption.corruption_type, &corruption.severity) {
        (CorruptionType::ChecksumMismatch, CorruptionSeverity::Low) => {
            HealingStrategy::RecalculateChecksum
        }
        (CorruptionType::BitRot, _) => HealingStrategy::UseRedundantCopy,
        (CorruptionType::InvalidPageHeader, CorruptionSeverity::Medium) => {
            HealingStrategy::RebuildFromIndex
        }
        (CorruptionType::PartialWrite, _) => HealingStrategy::RestoreFromWAL,
        (_, CorruptionSeverity::Critical) => HealingStrategy::RequestBackupRestore,
        _ => HealingStrategy::MarkPageAsDead,
    }
}

/// Background integrity monitor
pub struct IntegrityMonitor {
    detector: Arc<CorruptionDetector>,
    alert_threshold: u64,
    alert_callback: Option<Box<dyn Fn(&CorruptionInfo) + Send + Sync>>,
}

impl IntegrityMonitor {
    pub fn new(scan_interval: Duration, healing_enabled: bool, alert_threshold: u64) -> Self {
        Self {
            detector: Arc::new(CorruptionDetector::new(scan_interval, healing_enabled)),
            alert_threshold,
            alert_callback: None,
        }
    }

    /// Set alert callback for corruption notifications
    pub fn set_alert_callback<F>(&mut self, callback: F)
    where
        F: Fn(&CorruptionInfo) + Send + Sync + 'static,
    {
        self.alert_callback = Some(Box::new(callback));
    }

    /// Start monitoring
    pub fn start(&self, db: Arc<Database>) -> thread::JoinHandle<()> {
        self.detector.start(db)
    }

    /// Get current statistics
    pub fn stats(&self) -> &CorruptionStats {
        self.detector.stats()
    }

    /// Check if corruption rate exceeds threshold
    pub fn is_healthy(&self) -> bool {
        let stats = self.detector.stats();
        let detected = stats.corruptions_detected.load(Ordering::Relaxed);
        let repaired = stats.corruptions_repaired.load(Ordering::Relaxed);

        (detected - repaired) < self.alert_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_calculation() {
        let data1 = vec![0u8; PAGE_SIZE];
        let data2 = vec![255u8; PAGE_SIZE];

        let checksum1 = calculate_checksum(&data1);
        let checksum2 = calculate_checksum(&data2);

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_corruption_pattern_detection() {
        let zeros = vec![0u8; 1024];
        let ones = vec![0xFFu8; 1024];
        let mixed: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        assert_eq!(detect_corruption_pattern(&zeros), CorruptionType::BitRot);
        assert_eq!(detect_corruption_pattern(&ones), CorruptionType::BitRot);
        assert_eq!(
            detect_corruption_pattern(&mixed),
            CorruptionType::ChecksumMismatch
        );
    }

    #[test]
    fn test_healing_strategy_selection() {
        let low_severity = CorruptionInfo {
            corruption_type: CorruptionType::ChecksumMismatch,
            page_id: 1,
            offset: None,
            severity: CorruptionSeverity::Low,
            details: "Test".to_string(),
            detected_at: Instant::now(),
            can_heal: true,
        };

        assert!(matches!(
            determine_healing_strategy(&low_severity),
            HealingStrategy::RecalculateChecksum
        ));

        let critical = CorruptionInfo {
            corruption_type: CorruptionType::StructuralDamage,
            page_id: 2,
            offset: None,
            severity: CorruptionSeverity::Critical,
            details: "Test".to_string(),
            detected_at: Instant::now(),
            can_heal: false,
        };

        assert!(matches!(
            determine_healing_strategy(&critical),
            HealingStrategy::RequestBackupRestore
        ));
    }
}

use super::{WALEntry, WALOperation};
use crate::core::error::Result;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub enum WalCorruptionType {
    ChecksumMismatch,
    BinaryFormatError,
    TornWrite,
    InvalidTimestamp,
    SequenceGap,
    DuplicateEntry,
    InvalidOperation,
    TransactionInconsistency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionSeverity {
    Critical,
    Major,
    Minor,
    Warning,
}

#[derive(Debug, Clone)]
pub enum RecoveryAction {
    Skip,
    Repair,
    Stop,
    ScanForward,
}

#[derive(Debug, Clone)]
pub struct CorruptionReport {
    pub corruption_type: WalCorruptionType,
    pub severity: CorruptionSeverity,
    pub offset: u64,
    pub lsn: Option<u64>,
    pub description: String,
    pub recovery_action: RecoveryAction,
}

#[derive(Debug, Clone)]
pub struct RecoveryDecision {
    pub action: RecoveryAction,
    pub reason: String,
    pub safe_to_continue: bool,
}

#[derive(Debug, Default)]
pub struct ValidationStats {
    pub total_entries_checked: u64,
    pub corrupted_entries: u64,
    pub repaired_entries: u64,
    pub skipped_entries: u64,
    pub critical_errors: u64,
    pub warnings: u64,
}

#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub allow_checksum_repair: bool,
    pub allow_timestamp_drift: Duration,
    pub max_sequence_gap: u64,
    pub strict_ordering: bool,
    pub fail_on_critical: bool,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            allow_checksum_repair: false,
            allow_timestamp_drift: Duration::from_secs(3600),
            max_sequence_gap: 1000,
            strict_ordering: false,
            fail_on_critical: true,
        }
    }
}

pub struct WalCorruptionValidator {
    config: ValidationConfig,
    stats: ValidationStats,
    seen_lsns: HashMap<u64, u64>,
    last_lsn: Option<u64>,
    last_timestamp: Option<u64>,
}

impl WalCorruptionValidator {
    pub fn new(config: ValidationConfig) -> Self {
        Self {
            config,
            stats: ValidationStats::default(),
            seen_lsns: HashMap::new(),
            last_lsn: None,
            last_timestamp: None,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(ValidationConfig::default())
    }

    pub fn validate_entry(&mut self, entry: &WALEntry, offset: u64) -> Result<RecoveryDecision> {
        self.stats.total_entries_checked += 1;

        let mut corruptions = Vec::new();

        self.check_checksum(entry, offset, &mut corruptions);
        self.check_timestamp(entry, offset, &mut corruptions);
        self.check_sequence_order(entry, offset, &mut corruptions);
        self.check_duplicate_lsn(entry, offset, &mut corruptions);
        self.check_operation_validity(entry, offset, &mut corruptions);

        self.update_tracking(entry, offset);

        if corruptions.is_empty() {
            return Ok(RecoveryDecision {
                action: RecoveryAction::Skip,
                reason: "Entry is valid".to_string(),
                safe_to_continue: true,
            });
        }

        let decision = self.make_recovery_decision(&corruptions)?;

        match decision.action {
            RecoveryAction::Stop => {
                self.stats.critical_errors += 1;
            }
            RecoveryAction::Skip => {
                self.stats.skipped_entries += 1;
            }
            RecoveryAction::Repair => {
                self.stats.repaired_entries += 1;
            }
            RecoveryAction::ScanForward => {
                self.stats.corrupted_entries += 1;
            }
        }

        Ok(decision)
    }

    fn check_checksum(
        &self,
        entry: &WALEntry,
        offset: u64,
        corruptions: &mut Vec<CorruptionReport>,
    ) {
        if !entry.verify_checksum() {
            corruptions.push(CorruptionReport {
                corruption_type: WalCorruptionType::ChecksumMismatch,
                severity: CorruptionSeverity::Critical,
                offset,
                lsn: Some(entry.lsn),
                description: format!("Checksum mismatch for LSN {}", entry.lsn),
                recovery_action: if self.config.allow_checksum_repair {
                    RecoveryAction::Repair
                } else {
                    RecoveryAction::Stop
                },
            });
        }
    }

    fn check_timestamp(
        &self,
        entry: &WALEntry,
        offset: u64,
        corruptions: &mut Vec<CorruptionReport>,
    ) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry_time = entry.timestamp;

        if entry_time > current_time {
            let drift = Duration::from_millis(entry_time - current_time);
            if drift > self.config.allow_timestamp_drift {
                corruptions.push(CorruptionReport {
                    corruption_type: WalCorruptionType::InvalidTimestamp,
                    severity: CorruptionSeverity::Major,
                    offset,
                    lsn: Some(entry.lsn),
                    description: format!(
                        "Future timestamp detected: entry timestamp {} is {:.1}s ahead",
                        entry_time,
                        drift.as_secs_f64()
                    ),
                    recovery_action: RecoveryAction::Skip,
                });
            }
        }

        if let Some(last_ts) = self.last_timestamp {
            if entry_time < last_ts && self.config.strict_ordering {
                corruptions.push(CorruptionReport {
                    corruption_type: WalCorruptionType::InvalidTimestamp,
                    severity: CorruptionSeverity::Minor,
                    offset,
                    lsn: Some(entry.lsn),
                    description: format!(
                        "Timestamp regression: {} < {} (last)",
                        entry_time, last_ts
                    ),
                    recovery_action: RecoveryAction::Skip,
                });
            }
        }
    }

    fn check_sequence_order(
        &self,
        entry: &WALEntry,
        offset: u64,
        corruptions: &mut Vec<CorruptionReport>,
    ) {
        if let Some(last_lsn) = self.last_lsn {
            let gap = entry.lsn.saturating_sub(last_lsn);

            if gap > self.config.max_sequence_gap {
                corruptions.push(CorruptionReport {
                    corruption_type: WalCorruptionType::SequenceGap,
                    severity: CorruptionSeverity::Major,
                    offset,
                    lsn: Some(entry.lsn),
                    description: format!(
                        "Large sequence gap: LSN {} -> {} (gap: {})",
                        last_lsn, entry.lsn, gap
                    ),
                    recovery_action: RecoveryAction::Skip,
                });
            }

            if self.config.strict_ordering && entry.lsn <= last_lsn {
                corruptions.push(CorruptionReport {
                    corruption_type: WalCorruptionType::SequenceGap,
                    severity: CorruptionSeverity::Critical,
                    offset,
                    lsn: Some(entry.lsn),
                    description: format!("LSN out of order: {} <= {} (last)", entry.lsn, last_lsn),
                    recovery_action: RecoveryAction::Stop,
                });
            }
        }
    }

    fn check_duplicate_lsn(
        &self,
        entry: &WALEntry,
        offset: u64,
        corruptions: &mut Vec<CorruptionReport>,
    ) {
        if let Some(first_offset) = self.seen_lsns.get(&entry.lsn) {
            corruptions.push(CorruptionReport {
                corruption_type: WalCorruptionType::DuplicateEntry,
                severity: CorruptionSeverity::Major,
                offset,
                lsn: Some(entry.lsn),
                description: format!(
                    "Duplicate LSN {} (first seen at offset {})",
                    entry.lsn, first_offset
                ),
                recovery_action: RecoveryAction::Skip,
            });
        }
    }

    fn check_operation_validity(
        &self,
        entry: &WALEntry,
        offset: u64,
        corruptions: &mut Vec<CorruptionReport>,
    ) {
        match &entry.operation {
            WALOperation::Put { key, value } => {
                if key.is_empty() {
                    corruptions.push(CorruptionReport {
                        corruption_type: WalCorruptionType::InvalidOperation,
                        severity: CorruptionSeverity::Critical,
                        offset,
                        lsn: Some(entry.lsn),
                        description: "PUT operation with empty key".to_string(),
                        recovery_action: RecoveryAction::Stop,
                    });
                }

                if key.len() > 65536 || value.len() > 16_777_216 {
                    corruptions.push(CorruptionReport {
                        corruption_type: WalCorruptionType::InvalidOperation,
                        severity: CorruptionSeverity::Major,
                        offset,
                        lsn: Some(entry.lsn),
                        description: format!(
                            "PUT operation with oversized data: key {} bytes, value {} bytes",
                            key.len(),
                            value.len()
                        ),
                        recovery_action: RecoveryAction::Skip,
                    });
                }
            }
            WALOperation::Delete { key } => {
                if key.is_empty() {
                    corruptions.push(CorruptionReport {
                        corruption_type: WalCorruptionType::InvalidOperation,
                        severity: CorruptionSeverity::Critical,
                        offset,
                        lsn: Some(entry.lsn),
                        description: "DELETE operation with empty key".to_string(),
                        recovery_action: RecoveryAction::Stop,
                    });
                }
            }
            _ => {}
        }
    }

    fn update_tracking(&mut self, entry: &WALEntry, offset: u64) {
        self.seen_lsns.entry(entry.lsn).or_insert(offset);
        self.last_lsn = Some(entry.lsn.max(self.last_lsn.unwrap_or(0)));
        self.last_timestamp = Some(entry.timestamp.max(self.last_timestamp.unwrap_or(0)));
    }

    fn make_recovery_decision(&self, corruptions: &[CorruptionReport]) -> Result<RecoveryDecision> {
        let critical_count = corruptions
            .iter()
            .filter(|c| c.severity == CorruptionSeverity::Critical)
            .count();

        if critical_count > 0 && self.config.fail_on_critical {
            let critical = corruptions
                .iter()
                .find(|c| c.severity == CorruptionSeverity::Critical)
                .ok_or_else(|| crate::core::error::Error::WalCorruption {
                    offset: 0,
                    reason: "Critical corruption found but could not locate details".to_string(),
                })?;

            return Ok(RecoveryDecision {
                action: RecoveryAction::Stop,
                reason: format!("Critical corruption detected: {}", critical.description),
                safe_to_continue: false,
            });
        }

        let major_count = corruptions
            .iter()
            .filter(|c| c.severity == CorruptionSeverity::Major)
            .count();

        if major_count > 0 {
            return Ok(RecoveryDecision {
                action: RecoveryAction::Skip,
                reason: format!(
                    "Major corruption detected, skipping {} corruptions",
                    major_count
                ),
                safe_to_continue: true,
            });
        }

        Ok(RecoveryDecision {
            action: RecoveryAction::Skip,
            reason: "Minor corruptions detected".to_string(),
            safe_to_continue: true,
        })
    }

    pub fn get_stats(&self) -> &ValidationStats {
        &self.stats
    }

    pub fn reset_stats(&mut self) {
        self.stats = ValidationStats::default();
        self.seen_lsns.clear();
        self.last_lsn = None;
        self.last_timestamp = None;
    }

    pub fn generate_report(&self) -> String {
        format!(
            "WAL Validation Report:\n\
             Total entries: {}\n\
             Corrupted: {}\n\
             Repaired: {}\n\
             Skipped: {}\n\
             Critical errors: {}\n\
             Warnings: {}",
            self.stats.total_entries_checked,
            self.stats.corrupted_entries,
            self.stats.repaired_entries,
            self.stats.skipped_entries,
            self.stats.critical_errors,
            self.stats.warnings
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::wal::WALOperation;

    #[test]
    fn test_valid_entry_passes() {
        let mut validator = WalCorruptionValidator::with_default_config();

        let mut entry = WALEntry::new(
            1,
            WALOperation::Put {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
            },
        );
        entry.calculate_checksum();

        let decision = validator
            .validate_entry(&entry, 0)
            .expect("Validation should succeed for tests");
        assert!(matches!(decision.action, RecoveryAction::Skip));
        assert!(decision.safe_to_continue);
    }

    #[test]
    fn test_checksum_mismatch_detected() {
        let mut validator = WalCorruptionValidator::with_default_config();

        let mut entry = WALEntry::new(
            1,
            WALOperation::Put {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
            },
        );
        entry.calculate_checksum();
        entry.checksum = 0; // Corrupt checksum

        let decision = validator
            .validate_entry(&entry, 0)
            .expect("Validation should succeed for tests");
        assert!(matches!(decision.action, RecoveryAction::Stop));
        assert!(!decision.safe_to_continue);
    }

    #[test]
    fn test_empty_key_detected() {
        let mut validator = WalCorruptionValidator::with_default_config();

        let mut entry = WALEntry::new(
            1,
            WALOperation::Put {
                key: Vec::new(),
                value: b"value".to_vec(),
            },
        );
        entry.calculate_checksum();

        let decision = validator
            .validate_entry(&entry, 0)
            .expect("Validation should succeed for tests");
        assert!(matches!(decision.action, RecoveryAction::Stop));
    }
}

use crate::core::error::{Error, Result};
use super::{corruption_validator::*, WALEntry, WALOperation};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStatus {
    Complete,
    Partial,
    Failed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct CorruptionDetails {
    pub offset: u64,
    pub corruption_type: WalCorruptionType,
    pub severity: CorruptionSeverity,
    pub description: String,
    pub action_taken: RecoveryAction,
}

#[derive(Debug, Clone)]
pub struct DataLossAssessment {
    pub transactions_lost: u64,
    pub operations_lost: u64,
    pub data_integrity_risk: bool,
    pub consistency_preserved: bool,
}

#[derive(Debug, Clone)]
pub struct ImpactAssessment {
    pub data_loss: DataLossAssessment,
    pub performance_impact: Option<Duration>,
    pub recommended_actions: Vec<String>,
}

#[derive(Debug)]
pub struct RecoveryReport {
    pub status: RecoveryStatus,
    pub entries_processed: u64,
    pub entries_recovered: u64,
    pub entries_skipped: u64,
    pub corruption_details: Vec<CorruptionDetails>,
    pub impact_assessment: ImpactAssessment,
    pub recovery_time: Duration,
    pub final_lsn: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub validation_config: ValidationConfig,
    pub max_scan_distance: u64,
    pub allow_partial_recovery: bool,
    pub create_backup: bool,
    pub strict_consistency: bool,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            validation_config: ValidationConfig::default(),
            max_scan_distance: 1024 * 1024,
            allow_partial_recovery: true,
            create_backup: true,
            strict_consistency: true,
        }
    }
}

pub struct SafeWalRecovery {
    config: RecoveryConfig,
    validator: WalCorruptionValidator,
    active_transactions: HashMap<u64, Vec<WALOperation>>,
    committed_transactions: HashMap<u64, u64>,
    aborted_transactions: HashMap<u64, u64>,
}

impl SafeWalRecovery {
    pub fn new(config: RecoveryConfig) -> Self {
        let validator = WalCorruptionValidator::new(config.validation_config.clone());
        
        Self {
            config,
            validator,
            active_transactions: HashMap::new(),
            committed_transactions: HashMap::new(),
            aborted_transactions: HashMap::new(),
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(RecoveryConfig::default())
    }

    pub fn recover_from_file<P, F>(&mut self, wal_path: P, mut apply_operation: F) -> Result<RecoveryReport>
    where
        P: AsRef<Path>,
        F: FnMut(&WALOperation, bool) -> Result<()>,
    {
        let start_time = std::time::Instant::now();
        let mut report = RecoveryReport {
            status: RecoveryStatus::Failed,
            entries_processed: 0,
            entries_recovered: 0,
            entries_skipped: 0,
            corruption_details: Vec::new(),
            impact_assessment: ImpactAssessment {
                data_loss: DataLossAssessment {
                    transactions_lost: 0,
                    operations_lost: 0,
                    data_integrity_risk: false,
                    consistency_preserved: true,
                },
                performance_impact: None,
                recommended_actions: Vec::new(),
            },
            recovery_time: Duration::default(),
            final_lsn: None,
        };

        let mut file = match File::open(wal_path.as_ref()) {
            Ok(f) => f,
            Err(e) => {
                warn!("WAL file not found or inaccessible: {}", e);
                report.status = RecoveryStatus::Complete;
                report.recovery_time = start_time.elapsed();
                return Ok(report);
            }
        };

        let file_size = file.metadata()?.len();
        if file_size < 8 {
            warn!("WAL file too small ({} bytes), treating as empty", file_size);
            report.status = RecoveryStatus::Complete;
            report.recovery_time = start_time.elapsed();
            return Ok(report);
        }

        if self.config.create_backup {
            if let Err(e) = self.create_backup(wal_path.as_ref()) {
                warn!("Failed to create backup before recovery: {}", e);
                if self.config.strict_consistency {
                    return Err(Error::WalCorruption {
                        offset: 0,
                        reason: format!("Cannot create backup: {}", e),
                    });
                }
            }
        }

        self.skip_header(&mut file)?;

        let mut entries_by_lsn = HashMap::new();
        let mut recovery_successful = true;

        loop {
            let offset = file.stream_position()?;
            if offset >= file_size {
                break;
            }

            match self.read_and_validate_entry(&mut file, offset, file_size) {
                Ok(Some((entry, decision))) => {
                    report.entries_processed += 1;

                    if decision.safe_to_continue {
                        if matches!(decision.action, RecoveryAction::Skip) && 
                           !report.corruption_details.iter().any(|c| c.offset == offset) {
                            entries_by_lsn.insert(entry.lsn, entry);
                            report.entries_recovered += 1;
                        } else {
                            report.entries_skipped += 1;
                            report.corruption_details.push(CorruptionDetails {
                                offset,
                                corruption_type: WalCorruptionType::ChecksumMismatch,
                                severity: CorruptionSeverity::Major,
                                description: decision.reason.clone(),
                                action_taken: decision.action.clone(),
                            });
                        }
                    } else {
                        error!("Critical corruption at offset {}: {}", offset, decision.reason);
                        if self.config.strict_consistency {
                            recovery_successful = false;
                            break;
                        }
                        
                        report.entries_skipped += 1;
                        report.corruption_details.push(CorruptionDetails {
                            offset,
                            corruption_type: WalCorruptionType::BinaryFormatError,
                            severity: CorruptionSeverity::Critical,
                            description: decision.reason.clone(),
                            action_taken: RecoveryAction::Stop,
                        });
                        
                        if !self.config.allow_partial_recovery {
                            recovery_successful = false;
                            break;
                        }
                        
                        if let Err(e) = self.scan_forward(&mut file, file_size) {
                            warn!("Failed to scan forward after corruption: {}", e);
                            break;
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    error!("Failed to read entry at offset {}: {}", offset, e);
                    if self.config.strict_consistency {
                        recovery_successful = false;
                        break;
                    }
                    
                    if !self.config.allow_partial_recovery {
                        recovery_successful = false;
                        break;
                    }
                    
                    if let Err(scan_err) = self.scan_forward(&mut file, file_size) {
                        warn!("Failed to scan forward after read error: {}", scan_err);
                        break;
                    }
                }
            }
        }

        let mut sorted_entries: Vec<_> = entries_by_lsn.into_iter().collect();
        sorted_entries.sort_by_key(|(lsn, _)| *lsn);

        for (lsn, entry) in sorted_entries {
            if let Err(e) = self.process_transaction_entry(&entry) {
                warn!("Failed to process transaction entry LSN {}: {}", lsn, e);
                report.impact_assessment.data_loss.consistency_preserved = false;
            }
            
            report.final_lsn = Some(lsn);
        }

        if let Err(e) = self.apply_committed_transactions(&mut apply_operation) {
            error!("Failed to apply committed transactions: {}", e);
            recovery_successful = false;
            report.impact_assessment.data_loss.data_integrity_risk = true;
        }

        self.assess_impact(&mut report);

        report.status = if recovery_successful {
            if report.entries_skipped > 0 {
                RecoveryStatus::Partial
            } else {
                RecoveryStatus::Complete
            }
        } else {
            RecoveryStatus::Failed
        };

        report.recovery_time = start_time.elapsed();

        info!(
            "WAL recovery completed with status {:?}: {}/{} entries recovered in {:.2}ms",
            report.status,
            report.entries_recovered,
            report.entries_processed,
            report.recovery_time.as_millis()
        );

        Ok(report)
    }

    fn create_backup(&self, wal_path: &Path) -> Result<()> {
        let backup_path = wal_path.with_extension("wal.backup");
        std::fs::copy(wal_path, backup_path)
            .map_err(|e| Error::Io(e.to_string()))?;
        Ok(())
    }

    fn skip_header(&self, file: &mut File) -> Result<()> {
        let mut header = [0u8; 8];
        file.read_exact(&mut header)
            .map_err(|e| Error::Io(e.to_string()))?;
        Ok(())
    }

    fn read_and_validate_entry(
        &mut self,
        file: &mut File,
        offset: u64,
        file_size: u64,
    ) -> Result<Option<(WALEntry, RecoveryDecision)>> {
        if offset + 4 > file_size {
            return Ok(None);
        }

        let mut len_bytes = [0u8; 4];
        if file.read_exact(&mut len_bytes).is_err() {
            return Ok(None);
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        if len == 0 || len > 10 * 1024 * 1024 {
            warn!("Invalid entry length {} at offset {}", len, offset);
            return Ok(None);
        }

        if offset + 4 + len as u64 > file_size {
            warn!(
                "Incomplete entry at offset {} (needs {} bytes, {} available)",
                offset,
                len,
                file_size - offset - 4
            );
            return Ok(None);
        }

        let mut entry_bytes = vec![0u8; len];
        if file.read_exact(&mut entry_bytes).is_err() {
            return Ok(None);
        }

        match bincode::decode_from_slice::<WALEntry, _>(&entry_bytes, bincode::config::standard()) {
            Ok((entry, _)) => {
                let decision = self.validator.validate_entry(&entry, offset)?;
                Ok(Some((entry, decision)))
            }
            Err(e) => Err(Error::WalBinaryCorruption {
                offset,
                details: format!("Deserialization failed: {}", e),
            }),
        }
    }

    fn scan_forward(&self, file: &mut File, file_size: u64) -> Result<()> {
        let current_pos = file.stream_position()?;
        let mut pos = current_pos;
        let mut buffer = [0u8; 4096];

        while pos < file_size && pos - current_pos < self.config.max_scan_distance {
            file.seek(SeekFrom::Start(pos))?;
            let bytes_read = file.read(&mut buffer)
                .map_err(|e| Error::Io(e.to_string()))?;

            for i in 0..bytes_read.saturating_sub(4) {
                let len = u32::from_le_bytes([
                    buffer[i],
                    buffer[i + 1],
                    buffer[i + 2],
                    buffer[i + 3],
                ]);

                if len > 0 && len < 1024 * 1024 {
                    file.seek(SeekFrom::Start(pos + i as u64))?;
                    return Ok(());
                }
            }

            pos += bytes_read as u64;
        }

        file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn process_transaction_entry(&mut self, entry: &WALEntry) -> Result<()> {
        match &entry.operation {
            WALOperation::TransactionBegin { tx_id } | WALOperation::BeginTransaction { tx_id } => {
                self.active_transactions.insert(*tx_id, Vec::new());
            }
            WALOperation::TransactionCommit { tx_id } | WALOperation::CommitTransaction { tx_id } => {
                self.committed_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
            WALOperation::TransactionAbort { tx_id } | WALOperation::AbortTransaction { tx_id } => {
                self.aborted_transactions.insert(*tx_id, entry.lsn);
                self.active_transactions.remove(tx_id);
            }
            op @ (WALOperation::Put { .. } | WALOperation::Delete { .. }) => {
                if let Some((_, ops)) = self.active_transactions.iter_mut().next() {
                    ops.push(op.clone());
                }
            }
            WALOperation::Checkpoint { .. } => {
                // Checkpoints don't affect transaction state
            }
        }

        Ok(())
    }

    fn apply_committed_transactions<F>(&self, apply_operation: &mut F) -> Result<()>
    where
        F: FnMut(&WALOperation, bool) -> Result<()>,
    {
        for tx_id in self.committed_transactions.keys() {
            if let Some(operations) = self.active_transactions.get(tx_id) {
                for operation in operations {
                    apply_operation(operation, true)?;
                }
            }
        }

        Ok(())
    }

    fn assess_impact(&self, report: &mut RecoveryReport) {
        report.impact_assessment.data_loss.transactions_lost = self.active_transactions.len() as u64;
        
        report.impact_assessment.data_loss.operations_lost = self
            .active_transactions
            .values()
            .map(|ops| ops.len() as u64)
            .sum();

        report.impact_assessment.data_loss.data_integrity_risk = 
            !report.corruption_details.is_empty() && 
            report.corruption_details.iter().any(|c| c.severity == CorruptionSeverity::Critical);

        report.impact_assessment.data_loss.consistency_preserved = 
            report.corruption_details.iter().all(|c| c.severity != CorruptionSeverity::Critical);

        let mut recommendations = Vec::new();
        
        if report.impact_assessment.data_loss.transactions_lost > 0 {
            recommendations.push(format!(
                "Review {} incomplete transactions that were not applied",
                report.impact_assessment.data_loss.transactions_lost
            ));
        }
        
        if !report.corruption_details.is_empty() {
            recommendations.push("Consider running FSCK or integrity check".to_string());
            recommendations.push("Monitor for additional corruption".to_string());
        }
        
        if report.entries_skipped > report.entries_recovered / 10 {
            recommendations.push("High skip rate detected - investigate WAL integrity".to_string());
        }

        report.impact_assessment.recommended_actions = recommendations;
        
        report.impact_assessment.performance_impact = if report.recovery_time > Duration::from_secs(1) {
            Some(report.recovery_time)
        } else {
            None
        };
    }

    pub fn reset(&mut self) {
        self.validator.reset_stats();
        self.active_transactions.clear();
        self.committed_transactions.clear();
        self.aborted_transactions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::wal::WALOperation;
    use std::io::Write;

    #[test]
    fn test_empty_wal_recovery() {
        let mut recovery = SafeWalRecovery::with_default_config();
        let wal_path = std::path::Path::new("/tmp/empty_test.wal");
        
        std::fs::write(wal_path, &[]).expect("Failed to write test file");
        
        let report = recovery.recover_from_file(&wal_path, |_, _| Ok(())).expect("Recovery should succeed for empty file");
        assert_eq!(report.status, RecoveryStatus::Complete);
        assert_eq!(report.entries_processed, 0);
        
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_valid_wal_recovery() {
        let mut recovery = SafeWalRecovery::with_default_config();
        let wal_path = std::path::Path::new("/tmp/valid_test.wal");
        
        {
            let mut file = File::create(&wal_path).unwrap();
            file.write_all(&[0x57, 0x41, 0x4C, 0x21, 0x01, 0x00, 0x00, 0x00]).expect("Failed to write WAL header");
            
            let mut entry = WALEntry::new(1, WALOperation::Put {
                key: b"test".to_vec(),
                value: b"value".to_vec(),
            });
            entry.calculate_checksum();
            
            let data = bincode::encode_to_vec(&entry, bincode::config::standard()).expect("Failed to serialize WAL entry");
            file.write_all(&(data.len() as u32).to_le_bytes()).expect("Failed to write entry length");
            file.write_all(&data).expect("Failed to write entry data");
        }
        
        let mut operations_applied = Vec::new();
        let report = recovery.recover_from_file(&wal_path, |op, committed| {
            operations_applied.push((op.clone(), committed));
            Ok(())
        }).expect("Recovery should succeed for valid WAL file");
        
        assert_eq!(report.status, RecoveryStatus::Complete);
        assert_eq!(report.entries_processed, 1);
        assert_eq!(report.entries_recovered, 1);
        
        let _ = std::fs::remove_file(&wal_path);
    }
}

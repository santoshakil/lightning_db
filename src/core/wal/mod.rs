pub mod recovery_fixes;
pub mod corruption_validator;
pub mod safe_recovery;
pub mod unified_wal;
pub mod optimized_wal;
pub mod atomic_wal;
pub mod checkpoint_manager;
pub mod log_rotation;
pub mod group_commit_wal;

pub use recovery_fixes::WALRecoveryContext;
pub use corruption_validator::{
    WalCorruptionValidator, ValidationConfig, CorruptionReport, WalCorruptionType,
    CorruptionSeverity, RecoveryAction, RecoveryDecision, ValidationStats,
};
pub use safe_recovery::{
    SafeWalRecovery, RecoveryReport, RecoveryConfig, CorruptionDetails, 
    RecoveryStatus, DataLossAssessment, ImpactAssessment,
};
pub use unified_wal::{
    UnifiedWriteAheadLog, UnifiedWalConfig, WalSyncMode as UnifiedWalSyncMode,
    TransactionRecoveryState as UnifiedTransactionRecoveryState,
    RecoveryInfo as UnifiedRecoveryInfo,
};
pub use atomic_wal::{AtomicWriteAheadLog, AtomicWalConfig};
pub use checkpoint_manager::{CheckpointManager, CheckpointConfig, CheckpointMetadata, CheckpointInfo};
pub use log_rotation::{LogRotationManager, LogRotationConfig, SegmentMetadata};
pub use group_commit_wal::GroupCommitWAL;

pub type LogSequenceNumber = u64;
pub type WalEntry = WALEntry;

// Re-export unified WAL types for compatibility
pub use unified_wal::{WALOperation, WALEntry, WriteAheadLog as UnifiedWriteAheadLogTrait};

use crate::core::error::{Error, Result};

// WALEntry is re-exported from unified_wal above

/// Basic WriteAheadLog trait
pub trait WriteAheadLog: Send + Sync + std::fmt::Debug {
    fn append(&self, operation: WALOperation) -> Result<u64>;
    fn sync(&self) -> Result<()>;
    fn replay(&self) -> Result<Vec<WALOperation>>;
    fn truncate(&self, lsn: u64) -> Result<()>;
    fn checkpoint(&self) -> Result<()>;
}

/// Basic Write-Ahead Log implementation
pub struct BasicWriteAheadLog {
    file: std::sync::Mutex<std::fs::File>,
    next_lsn: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for BasicWriteAheadLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicWriteAheadLog")
            .field(
                "next_lsn",
                &self.next_lsn.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

impl BasicWriteAheadLog {
    pub fn create(path: &std::path::Path) -> Result<Self> {
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

        Ok(Self {
            file: std::sync::Mutex::new(file),
            next_lsn: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub fn open(path: &std::path::Path) -> Result<Self> {
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom};

        let mut file = OpenOptions::new()
            .create(false)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

        // Scan existing entries to find the highest LSN
        let mut max_lsn = 0;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

        let mut length_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut length_buf) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(crate::core::error::Error::Io(e.to_string())),
            }

            let length = u32::from_le_bytes(length_buf) as usize;
            if length == 0 || length > 10 * 1024 * 1024 {
                // Invalid length, stop reading to prevent infinite loop
                break;
            }

            let mut data = vec![0u8; length];
            match file.read_exact(&mut data) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Partial write detected - truncate file to last valid entry
                    let current_pos = file.stream_position()
                        .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
                    file.set_len(current_pos - 4)
                        .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
                    break;
                },
                Err(e) => return Err(crate::core::error::Error::Io(e.to_string())),
            }

            // Try to deserialize and get LSN
            if let Ok((entry, _)) = bincode::decode_from_slice::<WALEntry, _>(&data, bincode::config::standard()) {
                if entry.verify_checksum() {
                    max_lsn = max_lsn.max(entry.lsn);
                } else {
                    // Corrupted entry detected - stop reading
                    break;
                }
            } else {
                // Deserialization failed - stop reading
                break;
            }
        }

        // Reset file position for future writes
        file.seek(SeekFrom::End(0))
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

        Ok(Self {
            file: std::sync::Mutex::new(file),
            next_lsn: std::sync::atomic::AtomicU64::new(max_lsn + 1),
        })
    }
}

impl WriteAheadLog for BasicWriteAheadLog {
    fn append(&self, operation: WALOperation) -> Result<u64> {
        use std::io::Write;
        use std::sync::atomic::Ordering;

        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let mut entry = WALEntry::new(lsn, operation);
        entry.calculate_checksum();

        let mut file = self.file.lock().map_err(|_| Error::LockFailed {
            resource: "WAL file mutex".to_string(),
        })?;

        // Simple format: length + bincode serialized data
        let data = bincode::encode_to_vec(&entry, bincode::config::standard())
            .map_err(|e| crate::core::error::Error::Serialization(e.to_string()))?;

        file.write_all(&(data.len() as u32).to_le_bytes())
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
        file.write_all(&data)
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
        
        // Use fdatasync when available for better performance while maintaining durability
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            match unsafe { libc::fsync(fd) } {
                0 => {},
                _ => {
                    // Fallback to sync_all on fdatasync failure
                    file.sync_all()
                        .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
                }
            }
        }
        #[cfg(not(unix))]
        {
            file.sync_all()
                .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
        }

        Ok(lsn)
    }

    fn sync(&self) -> Result<()> {
        let file = self.file.lock().map_err(|_| Error::LockFailed {
            resource: "WAL file mutex".to_string(),
        })?;
        
        // Use fdatasync for better performance while maintaining durability
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            match unsafe { libc::fsync(fd) } {
                0 => {},
                _ => {
                    // Fallback to sync_all on fdatasync failure
                    file.sync_all()
                        .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
                }
            }
        }
        #[cfg(not(unix))]
        {
            file.sync_all()
                .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;
        }
        Ok(())
    }

    fn replay(&self) -> Result<Vec<WALOperation>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = self.file.lock().map_err(|_| Error::LockFailed {
            resource: "WAL file mutex".to_string(),
        })?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

        let mut operations = Vec::new();
        let mut length_buf = [0u8; 4];

        loop {
            match file.read_exact(&mut length_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(crate::core::error::Error::Io(e.to_string())),
            }

            let length = u32::from_le_bytes(length_buf) as usize;
            if length == 0 || length > 1024 * 1024 {
                // Invalid length, stop reading
                break;
            }

            let mut data = vec![0u8; length];
            file.read_exact(&mut data)
                .map_err(|e| crate::core::error::Error::Io(e.to_string()))?;

            match bincode::decode_from_slice::<WALEntry, _>(&data, bincode::config::standard()) {
                Ok((entry, _)) => {
                    if !entry.verify_checksum() {
                        // CRITICAL: Checksum verification failed - corruption detected
                        return Err(crate::core::error::Error::WalChecksumMismatch {
                            offset: file.stream_position().unwrap_or(0),
                            expected: {
                                let mut test_entry = entry.clone();
                                test_entry.checksum = 0;
                                test_entry.calculate_checksum();
                                test_entry.checksum
                            },
                            found: entry.checksum,
                        });
                    }
                    operations.push(entry.operation);
                }
                Err(deserialization_error) => {
                    // CRITICAL: Deserialization failed - binary corruption detected
                    return Err(crate::core::error::Error::WalBinaryCorruption {
                        offset: file.stream_position().unwrap_or(0),
                        details: format!("Entry deserialization failed: {}", deserialization_error),
                    });
                }
            }
        }

        Ok(operations)
    }

    fn truncate(&self, _lsn: u64) -> Result<()> {
        // For simplicity, this basic implementation doesn't support truncation
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        // Basic checkpoint just syncs the file
        self.sync()
    }
}

pub mod recovery_fixes;
pub mod corruption_validator;
pub mod safe_recovery;

pub use recovery_fixes::WALRecoveryContext;
pub use corruption_validator::{
    WalCorruptionValidator, ValidationConfig, CorruptionReport, WalCorruptionType,
    CorruptionSeverity, RecoveryAction, RecoveryDecision, ValidationStats,
};
pub use safe_recovery::{
    SafeWalRecovery, RecoveryReport, RecoveryConfig, CorruptionDetails, 
    RecoveryStatus, DataLossAssessment, ImpactAssessment,
};

pub type LogSequenceNumber = u64;
pub type WalEntry = WALEntry;

use crate::error::{Error, Result};
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

/// WAL operation types
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    BeginTransaction { tx_id: u64 },
    CommitTransaction { tx_id: u64 },
    AbortTransaction { tx_id: u64 },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionAbort { tx_id: u64 },
    Checkpoint { lsn: u64 },
}

/// WAL entry structure
#[derive(Debug, Clone, Encode, Decode)]
pub struct WALEntry {
    pub lsn: u64,
    pub timestamp: u64,
    pub operation: WALOperation,
    pub checksum: u32,
}

impl WALEntry {
    pub fn new(lsn: u64, operation: WALOperation) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            lsn,
            timestamp,
            operation,
            checksum: 0, // Will be calculated before writing
        }
    }

    pub fn calculate_checksum(&mut self) {
        // Simple checksum calculation
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.lsn.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());

        // Hash operation data
        match &self.operation {
            WALOperation::Put { key, value } => {
                hasher.update(b"PUT");
                hasher.update(key);
                hasher.update(value);
            }
            WALOperation::Delete { key } => {
                hasher.update(b"DEL");
                hasher.update(key);
            }
            WALOperation::BeginTransaction { tx_id } => {
                hasher.update(b"BEGIN");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::CommitTransaction { tx_id } => {
                hasher.update(b"COMMIT");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::AbortTransaction { tx_id } => {
                hasher.update(b"ABORT");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionBegin { tx_id } => {
                hasher.update(b"TXBEGIN");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionCommit { tx_id } => {
                hasher.update(b"TXCOMMIT");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionAbort { tx_id } => {
                hasher.update(b"TXABORT");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::Checkpoint { lsn } => {
                hasher.update(b"CHECKPOINT");
                hasher.update(&lsn.to_le_bytes());
            }
        }

        self.checksum = hasher.finalize();
    }

    pub fn verify_checksum(&self) -> bool {
        let mut copy = self.clone();
        copy.checksum = 0;
        copy.calculate_checksum();
        copy.checksum == self.checksum
    }

    pub fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        // Read length prefix
        let mut len_bytes = [0u8; 8];
        reader
            .read_exact(&mut len_bytes)
            .map_err(|e| Error::Io(e.to_string()))?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        // Read entry data
        let mut data = vec![0u8; len];
        reader
            .read_exact(&mut data)
            .map_err(|e| Error::Io(e.to_string()))?;

        // Decode entry
        bincode::decode_from_slice(&data, bincode::config::standard())
            .map(|(entry, _)| entry)
            .map_err(|e| Error::Serialization(e.to_string()))
    }
}

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
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        Ok(Self {
            file: std::sync::Mutex::new(file),
            next_lsn: std::sync::atomic::AtomicU64::new(1),
        })
    }

    pub fn open(path: &std::path::Path) -> Result<Self> {
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .create(false)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        // TODO: Read existing entries to set next_lsn correctly
        Ok(Self {
            file: std::sync::Mutex::new(file),
            next_lsn: std::sync::atomic::AtomicU64::new(1),
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
            .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;

        file.write_all(&(data.len() as u32).to_le_bytes())
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;
        file.write_all(&data)
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;
        file.sync_all()
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        Ok(lsn)
    }

    fn sync(&self) -> Result<()> {
        let file = self.file.lock().map_err(|_| Error::LockFailed {
            resource: "WAL file mutex".to_string(),
        })?;
        file.sync_all()
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;
        Ok(())
    }

    fn replay(&self) -> Result<Vec<WALOperation>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = self.file.lock().map_err(|_| Error::LockFailed {
            resource: "WAL file mutex".to_string(),
        })?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        let mut operations = Vec::new();
        let mut length_buf = [0u8; 4];

        loop {
            match file.read_exact(&mut length_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(crate::error::Error::Io(e.to_string())),
            }

            let length = u32::from_le_bytes(length_buf) as usize;
            if length == 0 || length > 1024 * 1024 {
                // Invalid length, stop reading
                break;
            }

            let mut data = vec![0u8; length];
            file.read_exact(&mut data)
                .map_err(|e| crate::error::Error::Io(e.to_string()))?;

            match bincode::decode_from_slice::<WALEntry, _>(&data, bincode::config::standard()) {
                Ok((entry, _)) => {
                    if !entry.verify_checksum() {
                        // CRITICAL: Checksum verification failed - corruption detected
                        return Err(crate::error::Error::WalChecksumMismatch {
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
                    return Err(crate::error::Error::WalBinaryCorruption {
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

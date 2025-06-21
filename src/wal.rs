use crate::error::{Error, Result};
use bincode::{Decode, Encode};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const WAL_MAGIC: u32 = 0x57414C21; // "WAL!"
const WAL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionAbort { tx_id: u64 },
    Checkpoint { lsn: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
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

        let mut entry = Self {
            lsn,
            timestamp,
            operation,
            checksum: 0,
        };

        // Calculate checksum
        entry.checksum = entry.calculate_checksum();
        entry
    }

    fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.lsn.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());

        match &self.operation {
            WALOperation::Put { key, value } => {
                hasher.update(b"put");
                hasher.update(&(key.len() as u32).to_le_bytes());
                hasher.update(key);
                hasher.update(&(value.len() as u32).to_le_bytes());
                hasher.update(value);
            }
            WALOperation::Delete { key } => {
                hasher.update(b"del");
                hasher.update(&(key.len() as u32).to_le_bytes());
                hasher.update(key);
            }
            WALOperation::TransactionBegin { tx_id } => {
                hasher.update(b"txb");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionCommit { tx_id } => {
                hasher.update(b"txc");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::TransactionAbort { tx_id } => {
                hasher.update(b"txa");
                hasher.update(&tx_id.to_le_bytes());
            }
            WALOperation::Checkpoint { lsn } => {
                hasher.update(b"chk");
                hasher.update(&lsn.to_le_bytes());
            }
        }

        hasher.finalize()
    }

    pub fn verify_checksum(&self) -> bool {
        let calculated = self.calculate_checksum();
        calculated == self.checksum
    }
}

pub struct WriteAheadLog {
    path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
    next_lsn: AtomicU64,
    last_checkpoint_lsn: AtomicU64,
}

impl WriteAheadLog {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let wal_path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_path)
            ?;

        let mut writer = BufWriter::new(file);

        // Write WAL header
        writer
            .write_all(&WAL_MAGIC.to_le_bytes())
            ?;
        writer
            .write_all(&WAL_VERSION.to_le_bytes())
            ?;
        writer.flush()?;

        Ok(Self {
            path: wal_path,
            writer: Arc::new(Mutex::new(writer)),
            next_lsn: AtomicU64::new(1),
            last_checkpoint_lsn: AtomicU64::new(0),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let wal_path = path.as_ref().to_path_buf();

        let mut file = File::open(&wal_path)?;

        // Verify WAL header
        let mut magic = [0u8; 4];
        let mut version = [0u8; 4];
        file.read_exact(&mut magic)?;
        file.read_exact(&mut version)?;

        let magic = u32::from_le_bytes(magic);
        let version = u32::from_le_bytes(version);

        if magic != WAL_MAGIC {
            return Err(Error::Storage("Invalid WAL magic number".to_string()));
        }

        if version != WAL_VERSION {
            return Err(Error::Storage("Unsupported WAL version".to_string()));
        }

        // Scan to find the last LSN
        let mut last_lsn = 0;
        let mut last_checkpoint_lsn = 0;

        while let Ok(entry) = Self::read_entry(&mut file) {
            if entry.verify_checksum() {
                last_lsn = entry.lsn;
                if let WALOperation::Checkpoint { lsn } = entry.operation {
                    last_checkpoint_lsn = lsn;
                }
            } else {
                // Corrupted entry, stop reading
                break;
            }
        }

        // Reopen file for writing (append mode)
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&wal_path)
            ?;

        let writer = BufWriter::new(file);

        Ok(Self {
            path: wal_path,
            writer: Arc::new(Mutex::new(writer)),
            next_lsn: AtomicU64::new(last_lsn + 1),
            last_checkpoint_lsn: AtomicU64::new(last_checkpoint_lsn),
        })
    }

    pub fn append(&self, operation: WALOperation) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let entry = WALEntry::new(lsn, operation);

        let mut writer = self.writer.lock().unwrap();
        Self::write_entry(&mut *writer, &entry)?;
        writer.flush()?;

        Ok(lsn)
    }

    fn write_entry<W: Write>(writer: &mut W, entry: &WALEntry) -> Result<()> {
        let serialized = bincode::encode_to_vec(entry, bincode::config::standard())
            .map_err(|e| Error::Storage(format!("Failed to serialize WAL entry: {}", e)))?;

        // Write entry length followed by entry data
        writer
            .write_all(&(serialized.len() as u32).to_le_bytes())
            ?;
        writer.write_all(&serialized)?;

        Ok(())
    }

    fn read_entry<R: Read>(reader: &mut R) -> Result<WALEntry> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let mut entry_bytes = vec![0u8; len];
        reader.read_exact(&mut entry_bytes)?;

        let (entry, _): (WALEntry, usize) =
            bincode::decode_from_slice(&entry_bytes, bincode::config::standard())
                .map_err(|e| Error::Storage(format!("Failed to deserialize WAL entry: {}", e)))?;

        Ok(entry)
    }

    pub fn replay_from_checkpoint<F>(&self, mut apply_operation: F) -> Result<()>
    where
        F: FnMut(&WALOperation) -> Result<()>,
    {
        let mut file = File::open(&self.path)?;

        // Skip header
        file.seek(SeekFrom::Start(8))?;

        let checkpoint_lsn = self.last_checkpoint_lsn.load(Ordering::SeqCst);
        let mut in_replay_range = checkpoint_lsn == 0; // If no checkpoint, replay from beginning

        while let Ok(entry) = Self::read_entry(&mut file) {
            if !entry.verify_checksum() {
                break; // Stop on corrupted entry
            }

            // Check if we've reached the checkpoint
            if !in_replay_range {
                if let WALOperation::Checkpoint { lsn } = entry.operation {
                    if lsn == checkpoint_lsn {
                        in_replay_range = true;
                    }
                }
                continue;
            }

            // Apply operation
            apply_operation(&entry.operation)?;
        }

        Ok(())
    }

    pub fn checkpoint(&self) -> Result<u64> {
        let current_lsn = self.next_lsn.load(Ordering::SeqCst);
        let checkpoint_lsn = self.append(WALOperation::Checkpoint { lsn: current_lsn })?;
        self.last_checkpoint_lsn
            .store(checkpoint_lsn, Ordering::SeqCst);
        Ok(checkpoint_lsn)
    }

    pub fn truncate_before(&self, lsn: u64) -> Result<()> {
        // Create a new WAL file with entries after the given LSN
        let temp_path = self.path.with_extension("tmp");
        let new_wal = Self::create(&temp_path)?;

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(8))?; // Skip header

        while let Ok(entry) = Self::read_entry(&mut file) {
            if !entry.verify_checksum() {
                break;
            }

            if entry.lsn >= lsn {
                let mut writer = new_wal.writer.lock().unwrap();
                Self::write_entry(&mut *writer, &entry)?;
            }
        }

        // Replace the old WAL with the new one
        std::fs::rename(&temp_path, &self.path)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()?;
        Ok(())
    }

    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst)
    }

    pub fn last_checkpoint_lsn(&self) -> u64 {
        self.last_checkpoint_lsn.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_wal_basic_operations() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        let wal = WriteAheadLog::create(&wal_path).unwrap();

        // Test basic operations
        let lsn1 = wal
            .append(WALOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();

        let lsn2 = wal
            .append(WALOperation::Delete {
                key: b"key2".to_vec(),
            })
            .unwrap();

        assert!(lsn2 > lsn1);

        // Test checkpoint
        let checkpoint_lsn = wal.checkpoint().unwrap();
        assert!(checkpoint_lsn > lsn2);
    }

    #[test]
    fn test_wal_replay() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");

        {
            let wal = WriteAheadLog::create(&wal_path).unwrap();

            wal.append(WALOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            })
            .unwrap();

            wal.append(WALOperation::Delete {
                key: b"key2".to_vec(),
            })
            .unwrap();

            wal.checkpoint().unwrap();

            wal.append(WALOperation::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
            })
            .unwrap();
        }

        // Reopen and replay
        let wal = WriteAheadLog::open(&wal_path).unwrap();

        let mut operations = Vec::new();
        wal.replay_from_checkpoint(|op| {
            operations.push(op.clone());
            Ok(())
        })
        .unwrap();

        // Should only replay operations after checkpoint
        assert_eq!(operations.len(), 1);
        match &operations[0] {
            WALOperation::Put { key, value } => {
                assert_eq!(key, b"key3");
                assert_eq!(value, b"value3");
            }
            _ => panic!("Expected Put operation"),
        }
    }

    #[test]
    fn test_wal_entry_checksum() {
        let entry = WALEntry::new(
            1,
            WALOperation::Put {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            },
        );

        assert!(entry.verify_checksum());

        // Corrupt the entry
        let mut corrupted = entry.clone();
        corrupted.checksum = 0;
        assert!(!corrupted.verify_checksum());
    }
}

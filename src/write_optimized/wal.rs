//! Write-Ahead Log (WAL) Implementation
//!
//! The WAL ensures durability by logging all write operations before they are
//! applied to the memtable. In case of a crash, the WAL can be replayed to
//! recover uncommitted data.

use crate::{Result, Error};
use std::fs::{File, OpenOptions};
use std::io::{Write, Read, Seek, SeekFrom, BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crc32fast::Hasher;

const WAL_MAGIC: u32 = 0x57414C47; // "WALG"
const WAL_VERSION: u32 = 1;
const BLOCK_SIZE: usize = 32768; // 32KB blocks

/// WAL entry types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalEntryType {
    Put = 1,
    Delete = 2,
    Checkpoint = 3,
    Begin = 4,
    Commit = 5,
    Rollback = 6,
}

/// WAL entry header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntryHeader {
    pub entry_type: WalEntryType,
    pub sequence_number: u64,
    pub timestamp: u64,
    pub key_len: u32,
    pub value_len: u32,
    pub crc: u32,
}

/// WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub header: WalEntryHeader,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl WalEntry {
    /// Create a new PUT entry
    pub fn put(sequence_number: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let mut hasher = Hasher::new();
        hasher.update(&key);
        hasher.update(&value);
        let crc = hasher.finalize();
        
        Self {
            header: WalEntryHeader {
                entry_type: WalEntryType::Put,
                sequence_number,
                timestamp,
                key_len: key.len() as u32,
                value_len: value.len() as u32,
                crc,
            },
            key,
            value: Some(value),
        }
    }

    /// Create a new DELETE entry
    pub fn delete(sequence_number: u64, key: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let mut hasher = Hasher::new();
        hasher.update(&key);
        let crc = hasher.finalize();
        
        Self {
            header: WalEntryHeader {
                entry_type: WalEntryType::Delete,
                sequence_number,
                timestamp,
                key_len: key.len() as u32,
                value_len: 0,
                crc,
            },
            key,
            value: None,
        }
    }

    /// Create a checkpoint entry
    pub fn checkpoint(sequence_number: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            header: WalEntryHeader {
                entry_type: WalEntryType::Checkpoint,
                sequence_number,
                timestamp,
                key_len: 0,
                value_len: 0,
                crc: 0,
            },
            key: Vec::new(),
            value: None,
        }
    }

    /// Serialize entry to bytes
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        
        // Write header
        data.extend_from_slice(&(self.header.entry_type as u8).to_le_bytes());
        data.extend_from_slice(&self.header.sequence_number.to_le_bytes());
        data.extend_from_slice(&self.header.timestamp.to_le_bytes());
        data.extend_from_slice(&self.header.key_len.to_le_bytes());
        data.extend_from_slice(&self.header.value_len.to_le_bytes());
        data.extend_from_slice(&self.header.crc.to_le_bytes());
        
        // Write key
        data.extend_from_slice(&self.key);
        
        // Write value if present
        if let Some(ref value) = self.value {
            data.extend_from_slice(value);
        }
        
        Ok(data)
    }

    /// Deserialize entry from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 33 { // Minimum header size
            return Err(Error::InvalidFormat("WAL entry too small".to_string()));
        }
        
        let mut offset = 0;
        
        // Read header
        let entry_type = match data[offset] {
            1 => WalEntryType::Put,
            2 => WalEntryType::Delete,
            3 => WalEntryType::Checkpoint,
            4 => WalEntryType::Begin,
            5 => WalEntryType::Commit,
            6 => WalEntryType::Rollback,
            _ => return Err(Error::InvalidFormat("Invalid WAL entry type".to_string())),
        };
        offset += 1;
        
        let sequence_number = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
        offset += 8;
        
        let timestamp = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
        offset += 8;
        
        let key_len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
        offset += 4;
        
        let value_len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
        offset += 4;
        
        let crc = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
        offset += 4;
        
        // Read key
        let key = if key_len > 0 {
            if offset + key_len as usize > data.len() {
                return Err(Error::InvalidFormat("WAL entry key truncated".to_string()));
            }
            data[offset..offset + key_len as usize].to_vec()
        } else {
            Vec::new()
        };
        offset += key_len as usize;
        
        // Read value
        let value = if value_len > 0 {
            if offset + value_len as usize > data.len() {
                return Err(Error::InvalidFormat("WAL entry value truncated".to_string()));
            }
            Some(data[offset..offset + value_len as usize].to_vec())
        } else {
            None
        };
        
        // Verify CRC
        if entry_type == WalEntryType::Put || entry_type == WalEntryType::Delete {
            let mut hasher = Hasher::new();
            hasher.update(&key);
            if let Some(ref v) = value {
                hasher.update(v);
            }
            let computed_crc = hasher.finalize();
            
            if computed_crc != crc {
                return Err(Error::InvalidFormat("WAL entry CRC mismatch".to_string()));
            }
        }
        
        Ok(Self {
            header: WalEntryHeader {
                entry_type,
                sequence_number,
                timestamp,
                key_len,
                value_len,
                crc,
            },
            key,
            value,
        })
    }

    /// Get entry size in bytes
    pub fn size(&self) -> usize {
        33 + self.key.len() + self.value.as_ref().map_or(0, |v| v.len())
    }
}

/// Write-Ahead Log
pub struct WriteAheadLog {
    /// Current WAL file
    file: Arc<Mutex<BufWriter<File>>>,
    /// WAL directory
    wal_dir: PathBuf,
    /// Current file ID
    file_id: AtomicU64,
    /// Current file size
    file_size: AtomicU64,
    /// Maximum file size before rotation
    max_file_size: u64,
    /// Sequence number
    sequence_number: AtomicU64,
    /// Sync mode
    sync_mode: super::WalSyncMode,
    /// Last sync time
    last_sync: Arc<Mutex<Instant>>,
    /// Closed flag
    closed: AtomicBool,
}

impl WriteAheadLog {
    /// Create a new WAL
    pub fn new(wal_dir: PathBuf, max_file_size: u64, sync_mode: super::WalSyncMode) -> Result<Self> {
        std::fs::create_dir_all(&wal_dir)?;
        
        // Find the latest WAL file
        let (file_id, sequence_number) = Self::find_latest_wal(&wal_dir)?;
        let file_id = file_id + 1; // Start with next file
        
        // Create new WAL file
        let file_path = wal_dir.join(format!("wal_{:010}.log", file_id));
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_path)?;
        
        // Write header
        let mut writer = BufWriter::new(file);
        Self::write_header(&mut writer)?;
        
        Ok(Self {
            file: Arc::new(Mutex::new(writer)),
            wal_dir,
            file_id: AtomicU64::new(file_id),
            file_size: AtomicU64::new(8), // Header size
            max_file_size,
            sequence_number: AtomicU64::new(sequence_number),
            sync_mode,
            last_sync: Arc::new(Mutex::new(Instant::now())),
            closed: AtomicBool::new(false),
        })
    }

    /// Write header to WAL file
    fn write_header(writer: &mut BufWriter<File>) -> Result<()> {
        writer.write_all(&WAL_MAGIC.to_le_bytes())?;
        writer.write_all(&WAL_VERSION.to_le_bytes())?;
        writer.flush()?;
        Ok(())
    }

    /// Find the latest WAL file
    fn find_latest_wal(wal_dir: &Path) -> Result<(u64, u64)> {
        let mut latest_id = 0;
        let mut latest_seq = 0;
        
        for entry in std::fs::read_dir(wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                if let Ok(id) = name_str[4..14].parse::<u64>() {
                    if id > latest_id {
                        latest_id = id;
                        // Try to read the last sequence number from this file
                        if let Ok(seq) = Self::read_last_sequence(&entry.path()) {
                            latest_seq = seq;
                        }
                    }
                }
            }
        }
        
        Ok((latest_id, latest_seq))
    }

    /// Read the last sequence number from a WAL file
    fn read_last_sequence(path: &Path) -> Result<u64> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();
        
        if file_size < 8 {
            return Ok(0);
        }
        
        // Read header
        let mut header = [0u8; 8];
        file.read_exact(&mut header)?;
        
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(Error::InvalidFormat("Invalid WAL magic".to_string()));
        }
        
        // Read entries to find last sequence number
        let mut last_seq = 0;
        let mut offset = 8;
        
        while offset < file_size {
            file.seek(SeekFrom::Start(offset))?;
            
            let mut entry_header = vec![0u8; 33];
            match file.read_exact(&mut entry_header) {
                Ok(()) => {
                    let seq = u64::from_le_bytes(entry_header[1..9].try_into().unwrap());
                    if seq > last_seq {
                        last_seq = seq;
                    }
                    
                    let key_len = u32::from_le_bytes(entry_header[17..21].try_into().unwrap());
                    let value_len = u32::from_le_bytes(entry_header[21..25].try_into().unwrap());
                    
                    offset += 33 + key_len as u64 + value_len as u64;
                }
                Err(_) => break,
            }
        }
        
        Ok(last_seq)
    }

    /// Append an entry to the WAL
    pub fn append(&self, entry: WalEntry) -> Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::InvalidOperation { 
                reason: "WAL is closed".to_string() 
            });
        }
        
        let data = entry.serialize()?;
        let data_len = data.len() as u64;
        
        // Check if we need to rotate
        if self.file_size.load(Ordering::Relaxed) + data_len > self.max_file_size {
            self.rotate()?;
        }
        
        // Write entry
        {
            let mut file = self.file.lock();
            file.write_all(&data)?;
            
            // Handle sync based on mode
            match self.sync_mode {
                super::WalSyncMode::Sync => {
                    file.flush()?;
                    file.get_ref().sync_all()?;
                }
                super::WalSyncMode::Periodic { interval_ms } => {
                    let mut last_sync = self.last_sync.lock();
                    if last_sync.elapsed() >= std::time::Duration::from_millis(interval_ms) {
                        file.flush()?;
                        file.get_ref().sync_all()?;
                        *last_sync = Instant::now();
                    }
                }
                super::WalSyncMode::NoSync => {
                    // No sync, just buffer
                }
            }
        }
        
        self.file_size.fetch_add(data_len, Ordering::Relaxed);
        
        Ok(())
    }

    /// Rotate to a new WAL file
    fn rotate(&self) -> Result<()> {
        let new_file_id = self.file_id.fetch_add(1, Ordering::SeqCst) + 1;
        let file_path = self.wal_dir.join(format!("wal_{:010}.log", new_file_id));
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&file_path)?;
        
        let mut writer = BufWriter::new(file);
        Self::write_header(&mut writer)?;
        
        // Replace the file
        {
            let mut current_file = self.file.lock();
            current_file.flush()?;
            current_file.get_ref().sync_all()?;
            *current_file = writer;
        }
        
        self.file_size.store(8, Ordering::Relaxed); // Header size
        
        Ok(())
    }

    /// Get next sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Sync the WAL to disk
    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(())
    }

    /// Close the WAL
    pub fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Relaxed);
        self.sync()?;
        Ok(())
    }

    /// Create an iterator over all WAL files
    pub fn iter(&self) -> Result<WalIterator> {
        let mut files = Vec::new();
        
        for entry in std::fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                files.push(entry.path());
            }
        }
        
        files.sort();
        
        Ok(WalIterator::new(files))
    }

    /// Delete old WAL files
    pub fn cleanup(&self, keep_files: usize) -> Result<()> {
        let mut files = Vec::new();
        
        for entry in std::fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                if let Ok(id) = name_str[4..14].parse::<u64>() {
                    files.push((id, entry.path()));
                }
            }
        }
        
        files.sort_by_key(|f| f.0);
        
        // Keep the latest N files
        let current_id = self.file_id.load(Ordering::Relaxed);
        for (id, path) in files {
            if id < current_id.saturating_sub(keep_files as u64) {
                std::fs::remove_file(path)?;
            }
        }
        
        Ok(())
    }
}

/// WAL iterator
pub struct WalIterator {
    files: Vec<PathBuf>,
    current_file_index: usize,
    current_reader: Option<BufReader<File>>,
    current_offset: u64,
}

impl WalIterator {
    /// Create a new WAL iterator
    pub fn new(files: Vec<PathBuf>) -> Self {
        Self {
            files,
            current_file_index: 0,
            current_reader: None,
            current_offset: 0,
        }
    }

    /// Open the next file
    fn open_next_file(&mut self) -> Result<bool> {
        if self.current_file_index >= self.files.len() {
            return Ok(false);
        }
        
        let file = File::open(&self.files[self.current_file_index])?;
        let mut reader = BufReader::new(file);
        
        // Read and verify header
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(Error::InvalidFormat("Invalid WAL magic".to_string()));
        }
        
        self.current_reader = Some(reader);
        self.current_offset = 8;
        self.current_file_index += 1;
        
        Ok(true)
    }
}

impl Iterator for WalIterator {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_reader.is_none() {
                match self.open_next_file() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            
            let reader = self.current_reader.as_mut().unwrap();
            
            // Try to read entry header
            let mut header_buf = vec![0u8; 33];
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {
                    // Parse header to get entry size
                    let key_len = u32::from_le_bytes(header_buf[17..21].try_into().unwrap()) as usize;
                    let value_len = u32::from_le_bytes(header_buf[21..25].try_into().unwrap()) as usize;
                    
                    // Read key and value
                    let mut data = header_buf;
                    data.resize(33 + key_len + value_len, 0);
                    
                    match reader.read_exact(&mut data[33..]) {
                        Ok(()) => {
                            match WalEntry::deserialize(&data) {
                                Ok(entry) => return Some(Ok(entry)),
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Err(_) => {
                            // Corrupted entry, move to next file
                            self.current_reader = None;
                            continue;
                        }
                    }
                }
                Err(_) => {
                    // End of file or error, move to next file
                    self.current_reader = None;
                    continue;
                }
            }
        }
    }
}

/// WAL recovery
pub struct WalRecovery {
    entries: Vec<WalEntry>,
}

impl WalRecovery {
    /// Recover entries from WAL
    pub fn recover(wal_dir: &Path) -> Result<Self> {
        let mut files = Vec::new();
        
        for entry in std::fs::read_dir(wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("wal_") && name_str.ends_with(".log") {
                files.push(entry.path());
            }
        }
        
        files.sort();
        
        let mut entries = Vec::new();
        let iter = WalIterator::new(files);
        
        for entry in iter {
            match entry {
                Ok(e) => entries.push(e),
                Err(_) => {
                    // Log error but continue recovery
                    continue;
                }
            }
        }
        
        Ok(Self { entries })
    }

    /// Get recovered entries
    pub fn entries(&self) -> &[WalEntry] {
        &self.entries
    }

    /// Apply entries to a callback
    pub fn apply<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(&WalEntry) -> Result<()>,
    {
        for entry in &self.entries {
            callback(entry)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry::put(100, b"key".to_vec(), b"value".to_vec());
        let data = entry.serialize().unwrap();
        let deserialized = WalEntry::deserialize(&data).unwrap();
        
        assert_eq!(deserialized.header.sequence_number, 100);
        assert_eq!(deserialized.key, b"key");
        assert_eq!(deserialized.value, Some(b"value".to_vec()));
    }

    #[test]
    fn test_wal_basic_operations() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(
            dir.path().to_path_buf(),
            1024 * 1024, // 1MB
            super::super::WalSyncMode::NoSync,
        ).unwrap();
        
        // Write some entries
        let seq1 = wal.next_sequence();
        wal.append(WalEntry::put(seq1, b"key1".to_vec(), b"value1".to_vec())).unwrap();
        
        let seq2 = wal.next_sequence();
        wal.append(WalEntry::delete(seq2, b"key2".to_vec())).unwrap();
        
        // Sync and close
        wal.sync().unwrap();
        wal.close().unwrap();
        
        // Recover
        let recovery = WalRecovery::recover(dir.path()).unwrap();
        let entries = recovery.entries();
        
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"key1");
        assert_eq!(entries[1].key, b"key2");
    }

    #[test]
    fn test_wal_rotation() {
        let dir = TempDir::new().unwrap();
        let wal = WriteAheadLog::new(
            dir.path().to_path_buf(),
            100, // Small size to force rotation
            super::super::WalSyncMode::NoSync,
        ).unwrap();
        
        // Write entries to force rotation
        for i in 0..10 {
            let seq = wal.next_sequence();
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            wal.append(WalEntry::put(seq, key.into_bytes(), value.into_bytes())).unwrap();
        }
        
        wal.close().unwrap();
        
        // Check that multiple files were created
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("wal_"))
            .collect();
        
        assert!(files.len() > 1);
    }
}
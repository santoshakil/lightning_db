use crate::core::error::{Error, Result};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Seek, SeekFrom, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
// bincode traits are re-exported through WALEntry
use super::{WALEntry, WALOperation, WriteAheadLog};

const ATOMIC_WAL_MAGIC: u32 = 0x5741_4C41; // "WALA" - Atomic WAL
const ATOMIC_WAL_VERSION: u32 = 1;
const MAX_PENDING_WRITES: usize = 10000;
const COMMIT_BATCH_SIZE: usize = 100;
const COMMIT_TIMEOUT_MS: u64 = 10;

/// Atomic Write-Ahead Log with strict ordering guarantees
#[derive(Debug)]
pub struct AtomicWriteAheadLog {
    base_path: PathBuf,
    current_file: Arc<RwLock<BufWriter<File>>>,
    next_lsn: AtomicU64,
    commit_lsn: AtomicU64,
    pending_writes: Arc<Mutex<VecDeque<PendingWrite>>>,
    commit_notify: Arc<Condvar>,
    commit_thread_handle: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    config: AtomicWalConfig,
}

#[derive(Debug, Clone)]
pub struct AtomicWalConfig {
    pub sync_on_commit: bool,
    pub batch_commits: bool,
    pub max_batch_size: usize,
    pub commit_timeout: Duration,
    pub enable_checksums: bool,
    pub buffer_size: usize,
}

impl Default for AtomicWalConfig {
    fn default() -> Self {
        Self {
            sync_on_commit: true,
            batch_commits: true,
            max_batch_size: COMMIT_BATCH_SIZE,
            commit_timeout: Duration::from_millis(COMMIT_TIMEOUT_MS),
            enable_checksums: true,
            buffer_size: 1024 * 1024, // 1MB buffer
        }
    }
}

#[derive(Debug)]
struct PendingWrite {
    lsn: u64,
    entry: WALEntry,
    completion: Arc<Mutex<Option<Result<u64>>>>,
}

impl AtomicWriteAheadLog {
    pub fn create<P: AsRef<Path>>(path: P, config: AtomicWalConfig) -> Result<Arc<Self>> {
        let base_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)
            .map_err(|e| Error::Io(format!("Failed to create WAL directory: {}", e)))?;

        let wal_file_path = base_path.join("atomic.wal");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&wal_file_path)
            .map_err(|e| Error::Io(format!("Failed to create WAL file: {}", e)))?;

        let mut writer = BufWriter::with_capacity(config.buffer_size, file);

        // Write file header
        writer.write_all(&ATOMIC_WAL_MAGIC.to_le_bytes())
            .map_err(|e| Error::Io(format!("Failed to write WAL magic: {}", e)))?;
        writer.write_all(&ATOMIC_WAL_VERSION.to_le_bytes())
            .map_err(|e| Error::Io(format!("Failed to write WAL version: {}", e)))?;
        writer.write_all(&0u64.to_le_bytes()) // Start LSN
            .map_err(|e| Error::Io(format!("Failed to write start LSN: {}", e)))?;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        writer.write_all(&timestamp.to_le_bytes())
            .map_err(|e| Error::Io(format!("Failed to write timestamp: {}", e)))?;

        writer.flush()
            .map_err(|e| Error::Io(format!("Failed to flush WAL header: {}", e)))?;

        let wal = Arc::new(Self {
            base_path,
            current_file: Arc::new(RwLock::new(writer)),
            next_lsn: AtomicU64::new(1),
            commit_lsn: AtomicU64::new(0),
            pending_writes: Arc::new(Mutex::new(VecDeque::new())),
            commit_notify: Arc::new(Condvar::new()),
            commit_thread_handle: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            config,
        });

        // Start commit thread if batching is enabled
        let wal_clone = Arc::clone(&wal);
        let _handle = std::thread::spawn(move || {
            wal_clone.commit_loop();
        });

        // Store the handle (we can't modify the Arc after creation, so we'll need to restructure this)
        Ok(wal)
    }

    pub fn open<P: AsRef<Path>>(path: P, config: AtomicWalConfig) -> Result<Arc<Self>> {
        let base_path = path.as_ref().to_path_buf();
        let wal_file_path = base_path.join("atomic.wal");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_file_path)
            .map_err(|e| Error::Io(format!("Failed to open WAL file: {}", e)))?;

        // Read and validate header
        let mut magic = [0u8; 4];
        let mut version = [0u8; 4];
        let mut start_lsn = [0u8; 8];
        let mut timestamp = [0u8; 8];

        file.read_exact(&mut magic)
            .map_err(|e| Error::Io(format!("Failed to read WAL magic: {}", e)))?;
        file.read_exact(&mut version)
            .map_err(|e| Error::Io(format!("Failed to read WAL version: {}", e)))?;
        file.read_exact(&mut start_lsn)
            .map_err(|e| Error::Io(format!("Failed to read start LSN: {}", e)))?;
        file.read_exact(&mut timestamp)
            .map_err(|e| Error::Io(format!("Failed to read timestamp: {}", e)))?;

        if u32::from_le_bytes(magic) != ATOMIC_WAL_MAGIC {
            return Err(Error::WalCorruption {
                offset: 0,
                reason: "Invalid WAL magic number".to_string(),
            });
        }

        if u32::from_le_bytes(version) != ATOMIC_WAL_VERSION {
            return Err(Error::WalCorruption {
                offset: 4,
                reason: format!("Unsupported WAL version: {}", u32::from_le_bytes(version)),
            });
        }

        // Scan for highest committed LSN
        let (max_lsn, committed_lsn) = Self::scan_wal_file(&mut file)?;

        // Position at end of file for appending
        file.seek(SeekFrom::End(0))
            .map_err(|e| Error::Io(format!("Failed to seek to end: {}", e)))?;

        let writer = BufWriter::with_capacity(config.buffer_size, file);

        let wal = Arc::new(Self {
            base_path,
            current_file: Arc::new(RwLock::new(writer)),
            next_lsn: AtomicU64::new(max_lsn + 1),
            commit_lsn: AtomicU64::new(committed_lsn),
            pending_writes: Arc::new(Mutex::new(VecDeque::new())),
            commit_notify: Arc::new(Condvar::new()),
            commit_thread_handle: None,
            shutdown: Arc::new(AtomicBool::new(false)),
            config,
        });

        let wal_clone = Arc::clone(&wal);
        let _handle = std::thread::spawn(move || {
            wal_clone.commit_loop();
        });

        Ok(wal)
    }

    fn scan_wal_file(file: &mut File) -> Result<(u64, u64)> {
        let mut max_lsn = 0;
        let mut committed_lsn = 0;
        let mut length_buf = [0u8; 4];

        loop {
            match file.read_exact(&mut length_buf) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(format!("Error reading entry length: {}", e))),
            }

            let length = u32::from_le_bytes(length_buf) as usize;
            if length == 0 || length > 10 * 1024 * 1024 {
                break; // Invalid length
            }

            let mut entry_data = vec![0u8; length];
            match file.read_exact(&mut entry_data) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(format!("Error reading entry data: {}", e))),
            }

            if let Ok((entry, _)) = bincode::decode_from_slice::<WALEntry, _>(&entry_data, bincode::config::standard()) {
                if entry.verify_checksum() {
                    max_lsn = max_lsn.max(entry.lsn);
                    // For commit entries, update committed LSN
                    match &entry.operation {
                        WALOperation::TransactionCommit { .. } | WALOperation::CommitTransaction { .. } => {
                            committed_lsn = committed_lsn.max(entry.lsn);
                        }
                        WALOperation::Checkpoint { lsn } => {
                            committed_lsn = committed_lsn.max(*lsn);
                        }
                        _ => {}
                    }
                } else {
                    // Corrupted entry - stop scanning
                    break;
                }
            } else {
                // Failed to deserialize - stop scanning
                break;
            }
        }

        Ok((max_lsn, committed_lsn))
    }

    fn commit_loop(self: &Arc<Self>) {
        while !self.shutdown.load(Ordering::Acquire) {
            let pending_writes = {
                let mut pending = self.pending_writes.lock();
                
                // Wait for writes or timeout
                if pending.is_empty() {
                    let timeout_result = self.commit_notify.wait_for(&mut pending, self.config.commit_timeout);
                    if timeout_result.timed_out() && pending.is_empty() {
                        continue;
                    }
                }

                // Collect batch
                let batch_size = pending.len().min(self.config.max_batch_size);
                if batch_size == 0 {
                    continue;
                }

                let mut batch = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    if let Some(write) = pending.pop_front() {
                        batch.push(write);
                    }
                }
                batch
            };

            // Process the batch
            self.process_write_batch(pending_writes);
        }
    }

    fn process_write_batch(&self, batch: Vec<PendingWrite>) {
        let mut writer = self.current_file.write();
        let mut all_successful = true;
        let mut last_error: Option<Error> = None;

        for write in &batch {
            // Serialize entry
            match bincode::encode_to_vec(&write.entry, bincode::config::standard()) {
                Ok(data) => {
                    let length = data.len() as u32;
                    
                    // Write length + data atomically
                    if writer.write_all(&length.to_le_bytes()).is_err() ||
                       writer.write_all(&data).is_err() {
                        all_successful = false;
                        last_error = Some(Error::Io("Failed to write WAL entry".to_string()));
                        break;
                    }
                }
                Err(e) => {
                    all_successful = false;
                    last_error = Some(Error::Serialization(e.to_string()));
                    break;
                }
            }
        }

        // Flush and sync if configured
        if all_successful {
            if writer.flush().is_err() {
                all_successful = false;
                last_error = Some(Error::Io("Failed to flush WAL buffer".to_string()));
            } else if self.config.sync_on_commit {
                let file = writer.get_mut();
                #[cfg(unix)]
                {
                    use std::os::unix::io::AsRawFd;
                    let fd = file.as_raw_fd();
                    if unsafe { libc::fsync(fd) } != 0 {
                        if file.sync_all().is_err() {
                            all_successful = false;
                            last_error = Some(Error::Io("Failed to sync WAL file".to_string()));
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    if file.sync_all().is_err() {
                        all_successful = false;
                        last_error = Some(Error::Io("Failed to sync WAL file".to_string()));
                    }
                }
            }
        }

        // Update committed LSN and notify waiters
        if all_successful {
            let max_lsn = batch.iter().map(|w| w.lsn).max().unwrap_or(0);
            self.commit_lsn.store(max_lsn, Ordering::SeqCst);
        }

        // Notify all waiters
        for write in batch {
            let result = if all_successful {
                Ok(write.lsn)
            } else {
                Err(last_error.clone().unwrap_or_else(|| Error::Io("Unknown error".to_string())))
            };
            
            let mut completion = write.completion.lock();
            *completion = Some(result);
        }
    }

    /// Wait for an LSN to be committed with timeout
    pub fn wait_for_commit(&self, lsn: u64, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        
        while self.commit_lsn.load(Ordering::Acquire) < lsn {
            if start.elapsed() > timeout {
                return Err(Error::Timeout("WAL commit timeout".to_string()));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        
        Ok(())
    }

    /// Get the current committed LSN
    pub fn committed_lsn(&self) -> u64 {
        self.commit_lsn.load(Ordering::Acquire)
    }

    /// Shutdown the WAL gracefully
    pub fn shutdown(&self) -> Result<()> {
        // Signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);
        self.commit_notify.notify_all();

        // Process any remaining pending writes
        let remaining_writes = {
            let mut pending = self.pending_writes.lock();
            pending.drain(..).collect::<Vec<_>>()
        };

        if !remaining_writes.is_empty() {
            self.process_write_batch(remaining_writes);
        }

        // Final sync
        let mut writer = self.current_file.write();
        writer.flush()
            .map_err(|e| Error::Io(format!("Failed to flush on shutdown: {}", e)))?;
        
        let file = writer.get_mut();
        file.sync_all()
            .map_err(|e| Error::Io(format!("Failed to sync on shutdown: {}", e)))?;

        Ok(())
    }
}

impl WriteAheadLog for AtomicWriteAheadLog {
    fn append(&self, operation: WALOperation) -> Result<u64> {
        // Allocate LSN atomically
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        
        // Create entry with checksum
        let mut entry = WALEntry::new(lsn, operation);
        if self.config.enable_checksums {
            entry.calculate_checksum();
        }

        // Create completion signal
        let completion = Arc::new(Mutex::new(None));
        
        let pending_write = PendingWrite {
            lsn,
            entry,
            completion: completion.clone(),
        };

        // Queue for processing
        {
            let mut pending = self.pending_writes.lock();
            
            // Prevent memory exhaustion
            if pending.len() >= MAX_PENDING_WRITES {
                return Err(Error::ResourceExhausted { resource: "WAL pending queue".to_string() });
            }
            
            pending.push_back(pending_write);
        }
        
        // Notify commit thread
        self.commit_notify.notify_one();

        // Wait for completion
        loop {
            {
                let mut result = completion.lock();
                if let Some(res) = result.take() {
                    return res;
                }
            }
            
            // Brief sleep to avoid busy waiting
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    fn sync(&self) -> Result<()> {
        let mut writer = self.current_file.write();
        writer.flush()
            .map_err(|e| Error::Io(format!("Failed to flush: {}", e)))?;
        
        let file = writer.get_mut();
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            if unsafe { libc::fsync(fd) } != 0 {
                file.sync_all()
                    .map_err(|e| Error::Io(format!("Failed to sync: {}", e)))?;
            }
        }
        #[cfg(not(unix))]
        {
            file.sync_all()
                .map_err(|e| Error::Io(format!("Failed to sync: {}", e)))?;
        }
        
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        let current_lsn = self.next_lsn.load(Ordering::Acquire);
        let checkpoint_lsn = self.append(WALOperation::Checkpoint { lsn: current_lsn })?;
        
        // Wait for checkpoint to be committed
        self.wait_for_commit(checkpoint_lsn, Duration::from_secs(10))?;
        
        Ok(())
    }

    fn replay(&self) -> Result<Vec<WALOperation>> {
        // For the atomic WAL, we need to read from the file and replay operations
        let mut operations = Vec::new();
        let wal_file_path = self.base_path.join("atomic.wal");
        
        let mut file = File::open(&wal_file_path)
            .map_err(|e| Error::Io(format!("Failed to open WAL for replay: {}", e)))?;
            
        // Skip header (24 bytes: magic + version + start_lsn + timestamp)
        file.seek(SeekFrom::Start(24))
            .map_err(|e| Error::Io(format!("Failed to seek past header: {}", e)))?;
            
        let mut length_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut length_buf) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(format!("Failed to read entry length: {}", e))),
            }
            
            let length = u32::from_le_bytes(length_buf) as usize;
            if length == 0 || length > 10 * 1024 * 1024 {
                break; // Invalid length
            }
            
            let mut entry_data = vec![0u8; length];
            match file.read_exact(&mut entry_data) {
                Ok(()) => {},
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(format!("Failed to read entry data: {}", e))),
            }
            
            if let Ok((entry, _)) = bincode::decode_from_slice::<WALEntry, _>(&entry_data, bincode::config::standard()) {
                if !self.config.enable_checksums || entry.verify_checksum() {
                    operations.push(entry.operation);
                }
            }
        }
        
        Ok(operations)
    }

    fn truncate(&self, lsn: u64) -> Result<()> {
        // For truncation, we would need to rewrite the file up to the specified LSN
        // For now, this is a placeholder that doesn't actually truncate
        tracing::warn!("WAL truncation to LSN {} not fully implemented", lsn);
        Ok(())
    }

}

impl AtomicWriteAheadLog {
    /// Get the current LSN (not part of the base WriteAheadLog trait)
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire) - 1
    }
}

impl Drop for AtomicWriteAheadLog {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_atomic_wal_create_and_basic_operations() {
        let dir = tempdir().expect("Failed to create temp dir");
        let config = AtomicWalConfig::default();
        
        let wal = AtomicWriteAheadLog::create(dir.path(), config).expect("Failed to create WAL");
        
        let lsn1 = wal.append(WALOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        }).expect("Failed to append");
        
        let lsn2 = wal.append(WALOperation::Put {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
        }).expect("Failed to append");
        
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        
        wal.sync().expect("Failed to sync");
        
        // Wait for commits
        wal.wait_for_commit(lsn2, Duration::from_secs(1)).expect("Commit timeout");
        assert!(wal.committed_lsn() >= lsn2);
    }

    #[test]
    fn test_atomic_wal_reopen() {
        let dir = tempdir().expect("Failed to create temp dir");
        let config = AtomicWalConfig::default();
        
        // Create and write some entries
        {
            let wal = AtomicWriteAheadLog::create(dir.path(), config.clone()).expect("Failed to create WAL");
            
            let lsn1 = wal.append(WALOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            }).expect("Failed to append");
            
            wal.wait_for_commit(lsn1, Duration::from_secs(1)).expect("Commit timeout");
            wal.shutdown().expect("Failed to shutdown");
        }
        
        // Reopen and verify state
        {
            let wal = AtomicWriteAheadLog::open(dir.path(), config).expect("Failed to open WAL");
            
            // Should continue from where we left off
            let lsn2 = wal.append(WALOperation::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            }).expect("Failed to append");
            
            assert!(lsn2 > 1, "LSN should continue from previous session");
        }
    }
}
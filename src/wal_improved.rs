use crate::error::{Error, Result};
use crate::wal::{WALOperation, WALEntry};
use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock, Condvar};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

const WAL_MAGIC: u32 = 0x57414C22; // "WAL" version 2
const WAL_VERSION: u32 = 2;
const SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB segments
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_micros(100);
const GROUP_COMMIT_SIZE: usize = 2000;

/// Recovery information for resumable recovery
#[derive(Debug, Clone, Encode, Decode)]
pub struct RecoveryInfo {
    pub last_good_lsn: u64,
    pub last_good_offset: u64,
    pub recovered_transactions: HashMap<u64, TransactionRecoveryState>,
    pub checkpoint_lsn: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum TransactionRecoveryState {
    InProgress { operations: Vec<WALOperation> },
    Committed,
    Aborted,
}

/// Improved WAL segment management
pub struct WALSegment {
    segment_id: u64,
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    size: AtomicU64,
    is_active: AtomicBool,
}

impl WALSegment {
    fn create(base_path: &Path, segment_id: u64) -> Result<Self> {
        let path = base_path.join(format!("wal_{:08}.seg", segment_id));
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            ?;
        
        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer
        
        // Write segment header
        writer.write_all(&WAL_MAGIC.to_le_bytes())?;
        writer.write_all(&WAL_VERSION.to_le_bytes())?;
        writer.write_all(&segment_id.to_le_bytes())?;
        
        Ok(Self {
            segment_id,
            path,
            writer: Mutex::new(writer),
            size: AtomicU64::new(16), // Header size
            is_active: AtomicBool::new(true),
        })
    }
    
    fn open(path: PathBuf) -> Result<(Self, u64, u64)> {
        let mut file = File::open(&path)?;
        
        // Read and verify header
        let mut magic = [0u8; 4];
        let mut version = [0u8; 4];
        let mut segment_id_bytes = [0u8; 8];
        
        file.read_exact(&mut magic)?;
        file.read_exact(&mut version)?;
        file.read_exact(&mut segment_id_bytes)?;
        
        let magic = u32::from_le_bytes(magic);
        let version = u32::from_le_bytes(version);
        let segment_id = u64::from_le_bytes(segment_id_bytes);
        
        if magic != WAL_MAGIC {
            return Err(Error::Storage("Invalid WAL segment magic".to_string()));
        }
        
        if version != WAL_VERSION {
            return Err(Error::Storage("Unsupported WAL version".to_string()));
        }
        
        // Find the last valid entry and position
        let (last_lsn, file_size) = Self::scan_segment(&mut file)?;
        
        // Reopen for appending
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&path)
            ?;
        
        let writer = BufWriter::with_capacity(1024 * 1024, file);
        
        Ok((
            Self {
                segment_id,
                path,
                writer: Mutex::new(writer),
                size: AtomicU64::new(file_size),
                is_active: AtomicBool::new(true),
            },
            last_lsn,
            file_size,
        ))
    }
    
    fn scan_segment(file: &mut File) -> Result<(u64, u64)> {
        let mut last_lsn = 0;
        let mut last_valid_offset = 16; // After header
        
        loop {
            let offset = file.stream_position()?;
            
            match Self::read_entry_with_recovery(file) {
                Ok(entry) => {
                    if entry.verify_checksum() {
                        last_lsn = entry.lsn;
                        last_valid_offset = file.stream_position()?;
                    } else {
                        warn!("Corrupted entry at offset {}, stopping scan", offset);
                        break;
                    }
                }
                Err(e) => {
                    // Check if it's an incomplete write
                    if Self::is_incomplete_write(&e) {
                        info!("Found incomplete write at offset {}, truncating", offset);
                    } else {
                        debug!("Error reading entry at offset {}: {:?}", offset, e);
                    }
                    break;
                }
            }
        }
        
        Ok((last_lsn, last_valid_offset))
    }
    
    fn read_entry_with_recovery(reader: &mut impl Read) -> Result<WALEntry> {
        // Try to read length
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::Io(e.to_string()));
            }
            Err(e) => return Err(Error::Io(e.to_string())),
        }
        
        let len = u32::from_le_bytes(len_bytes) as usize;
        
        // Sanity check on length
        if len > 10 * 1024 * 1024 { // 10MB max entry size
            return Err(Error::Storage("Entry size too large".to_string()));
        }
        
        // Try to read entry data
        let mut entry_bytes = vec![0u8; len];
        match reader.read_exact(&mut entry_bytes) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Incomplete write detected
                return Err(Error::Io(e.to_string()));
            }
            Err(e) => return Err(Error::Io(e.to_string())),
        }
        
        let (entry, _): (WALEntry, usize) =
            bincode::decode_from_slice(&entry_bytes, bincode::config::standard())
                .map_err(|e| Error::Storage(format!("Failed to deserialize: {}", e)))?;
        
        Ok(entry)
    }
    
    fn is_incomplete_write(error: &Error) -> bool {
        match error {
            Error::Io(e) => e.contains("UnexpectedEof"),
            _ => false,
        }
    }
    
    fn write_entry(&self, entry: &WALEntry) -> Result<u64> {
        let serialized = bincode::encode_to_vec(entry, bincode::config::standard())
            .map_err(|e| Error::Storage(format!("Failed to serialize: {}", e)))?;
        
        let entry_size = 4 + serialized.len() as u64;
        
        let mut writer = self.writer.lock();
        
        // Write length and data
        writer.write_all(&(serialized.len() as u32).to_le_bytes())
            ?;
        writer.write_all(&serialized)?;
        
        // Update size
        let new_size = self.size.fetch_add(entry_size, Ordering::SeqCst) + entry_size;
        
        Ok(new_size)
    }
    
    fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        
        // Get the underlying file and sync to disk
        let file = writer.get_mut();
        file.sync_all()?;
        
        Ok(())
    }
    
    fn close(&self) -> Result<()> {
        self.is_active.store(false, Ordering::SeqCst);
        self.sync()?;
        Ok(())
    }
}

impl Drop for WALSegment {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Group commit batch
struct CommitBatch {
    entries: Vec<(WALEntry, Arc<Mutex<Option<Result<u64>>>>)>,
    total_size: usize,
}

/// Improved Write-Ahead Log with recovery handling
pub struct ImprovedWriteAheadLog {
    base_path: PathBuf,
    segments: Arc<RwLock<Vec<Arc<WALSegment>>>>,
    active_segment: Arc<RwLock<Arc<WALSegment>>>,
    next_lsn: AtomicU64,
    next_segment_id: AtomicU64,
    last_checkpoint_lsn: AtomicU64,
    
    // Group commit
    commit_queue: Arc<Mutex<VecDeque<(WALEntry, Arc<Mutex<Option<Result<u64>>>>)>>>,
    commit_condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    
    // Recovery tracking
    recovery_info: Arc<RwLock<RecoveryInfo>>,
    
    // Configuration
    max_segment_size: u64,
    sync_on_commit: bool,
    group_commit_enabled: bool,
}

impl ImprovedWriteAheadLog {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        Self::create_with_config(path, true, true)
    }
    
    pub fn create_with_config<P: AsRef<Path>>(
        path: P, 
        sync_on_commit: bool, 
        group_commit_enabled: bool
    ) -> Result<Arc<Self>> {
        let base_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;
        
        // Create initial segment
        let segment = Arc::new(WALSegment::create(&base_path, 0)?);
        
        let wal = Arc::new(Self {
            base_path,
            segments: Arc::new(RwLock::new(vec![segment.clone()])),
            active_segment: Arc::new(RwLock::new(segment)),
            next_lsn: AtomicU64::new(1),
            next_segment_id: AtomicU64::new(1),
            last_checkpoint_lsn: AtomicU64::new(0),
            commit_queue: Arc::new(Mutex::new(VecDeque::new())),
            commit_condvar: Arc::new(Condvar::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            recovery_info: Arc::new(RwLock::new(RecoveryInfo {
                last_good_lsn: 0,
                last_good_offset: 16,
                recovered_transactions: HashMap::new(),
                checkpoint_lsn: 0,
            })),
            max_segment_size: SEGMENT_SIZE,
            sync_on_commit,
            group_commit_enabled,
        });
        
        // Start group commit thread
        if wal.group_commit_enabled {
            let wal_clone = Arc::clone(&wal);
            std::thread::spawn(move || {
                wal_clone.group_commit_loop();
            });
        }
        
        Ok(wal)
    }
    
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        let base_path = path.as_ref().to_path_buf();
        
        // Find all segment files
        let mut segments = Vec::new();
        let mut max_lsn = 0;
        let mut max_segment_id = 0;
        let mut recovery_info = RecoveryInfo {
            last_good_lsn: 0,
            last_good_offset: 16,
            recovered_transactions: HashMap::new(),
            checkpoint_lsn: 0,
        };
        
        for entry in std::fs::read_dir(&base_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                let (segment, last_lsn, _) = WALSegment::open(path)?;
                max_lsn = max_lsn.max(last_lsn);
                max_segment_id = max_segment_id.max(segment.segment_id);
                
                // Track recovery info
                recovery_info.last_good_lsn = recovery_info.last_good_lsn.max(last_lsn);
                
                segments.push(Arc::new(segment));
            }
        }
        
        // Sort segments by ID
        segments.sort_by_key(|s| s.segment_id);
        
        // Use last segment as active, or create new one if none exist
        let active_segment = if let Some(segment) = segments.last() {
            segment.clone()
        } else {
            let new_segment = Arc::new(WALSegment::create(&base_path, 0).unwrap());
            segments.push(new_segment.clone());
            new_segment
        };
        
        let wal = Arc::new(Self {
            base_path,
            segments: Arc::new(RwLock::new(segments)),
            active_segment: Arc::new(RwLock::new(active_segment)),
            next_lsn: AtomicU64::new(max_lsn + 1),
            next_segment_id: AtomicU64::new(max_segment_id + 1),
            last_checkpoint_lsn: AtomicU64::new(recovery_info.checkpoint_lsn),
            commit_queue: Arc::new(Mutex::new(VecDeque::new())),
            commit_condvar: Arc::new(Condvar::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            recovery_info: Arc::new(RwLock::new(recovery_info)),
            max_segment_size: SEGMENT_SIZE,
            sync_on_commit: true,
            group_commit_enabled: true,
        });
        
        // Start group commit thread
        if wal.group_commit_enabled {
            let wal_clone = Arc::clone(&wal);
            std::thread::spawn(move || {
                wal_clone.group_commit_loop();
            });
        }
        
        Ok(wal)
    }
    
    pub fn append(&self, operation: WALOperation) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);
        let entry = WALEntry::new(lsn, operation);
        
        if self.group_commit_enabled {
            // Queue for group commit
            let result = Arc::new(Mutex::new(None));
            
            {
                let mut queue = self.commit_queue.lock();
                queue.push_back((entry, result.clone()));
                // Notify the commit thread
                self.commit_condvar.notify_one();
            }
            
            // Wait for result with exponential backoff
            let mut spin_count = 0;
            loop {
                {
                    let r = result.lock();
                    if let Some(res) = &*r {
                        return match res {
                            Ok(lsn) => Ok(*lsn),
                            Err(e) => Err(Error::Storage(format!("WAL write failed: {:?}", e))),
                        };
                    }
                }
                
                // Spin a few times before yielding
                spin_count += 1;
                if spin_count < 100 {
                    std::hint::spin_loop();
                } else if spin_count < 1000 {
                    std::thread::yield_now();
                } else {
                    std::thread::sleep(Duration::from_micros(10));
                }
            }
        } else {
            // Direct write
            self.write_entry_direct(entry)
        }
    }
    
    fn write_entry_direct(&self, entry: WALEntry) -> Result<u64> {
        let lsn = entry.lsn;
        
        // Check if we need to rotate segment
        let segment = {
            let active = self.active_segment.read();
            if active.size.load(Ordering::Relaxed) >= self.max_segment_size {
                drop(active);
                self.rotate_segment()?
            } else {
                active.clone()
            }
        };
        
        // Write entry
        segment.write_entry(&entry)?;
        
        // Only sync on transaction commit/abort operations when sync_on_commit is enabled
        // For Put/Delete operations within a transaction, sync will be done at commit time
        let should_sync = self.sync_on_commit && matches!(
            entry.operation,
            WALOperation::TransactionCommit { .. } | 
            WALOperation::TransactionAbort { .. } |
            WALOperation::Checkpoint { .. }
        );
        
        if should_sync {
            segment.sync()?;
        }
        
        Ok(lsn)
    }
    
    fn rotate_segment(&self) -> Result<Arc<WALSegment>> {
        let mut active = self.active_segment.write();
        
        // Double-check under write lock
        if active.size.load(Ordering::Relaxed) < self.max_segment_size {
            return Ok(active.clone());
        }
        
        // Close current segment
        active.close()?;
        
        // Create new segment
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let new_segment = Arc::new(WALSegment::create(&self.base_path, segment_id)?);
        
        // Update segments list
        {
            let mut segments = self.segments.write();
            segments.push(new_segment.clone());
        }
        
        // Update active segment
        *active = new_segment.clone();
        
        Ok(new_segment)
    }
    
    fn group_commit_loop(&self) {
        while !self.shutdown.load(Ordering::Relaxed) {
            let batch = self.collect_commit_batch();
            
            if !batch.entries.is_empty() {
                self.process_commit_batch(batch);
            } else {
                // Wait on condition variable instead of sleeping
                let mut queue = self.commit_queue.lock();
                if queue.is_empty() && !self.shutdown.load(Ordering::Relaxed) {
                    self.commit_condvar.wait_for(&mut queue, Duration::from_micros(100));
                }
            }
        }
    }
    
    fn collect_commit_batch(&self) -> CommitBatch {
        let mut queue = self.commit_queue.lock();
        let mut batch = CommitBatch {
            entries: Vec::new(),
            total_size: 0,
        };
        
        let deadline = Instant::now() + GROUP_COMMIT_INTERVAL;
        
        while batch.entries.len() < GROUP_COMMIT_SIZE && 
              Instant::now() < deadline &&
              !queue.is_empty() {
            
            if let Some((entry, result)) = queue.pop_front() {
                batch.total_size += std::mem::size_of_val(&entry);
                batch.entries.push((entry, result));
            }
        }
        
        batch
    }
    
    fn process_commit_batch(&self, batch: CommitBatch) {
        // Check if rotation needed
        let segment = {
            let active = self.active_segment.read();
            if active.size.load(Ordering::Relaxed) + batch.total_size as u64 >= self.max_segment_size {
                drop(active);
                match self.rotate_segment() {
                    Ok(seg) => seg,
                    Err(e) => {
                        // Notify all waiters of error
                        for (_, result) in batch.entries {
                            *result.lock() = Some(Err(Error::Storage(format!("Segment rotation failed: {:?}", e))));
                        }
                        return;
                    }
                }
            } else {
                active.clone()
            }
        };
        
        // Write all entries
        for (entry, result) in batch.entries {
            let write_result = segment.write_entry(&entry);
            *result.lock() = Some(write_result);
        }
        
        // Single sync for the batch
        if self.sync_on_commit {
            let _ = segment.sync();
        }
    }
    
    pub fn recover_with_progress<F, P>(&self, mut apply_op: F, mut progress: P) -> Result<()>
    where
        F: FnMut(&WALOperation, &TransactionRecoveryState) -> Result<()>,
        P: FnMut(u64, u64), // current_lsn, total_estimated
    {
        let segments = self.segments.read();
        let mut recovery_info = self.recovery_info.write();
        let mut current_tx_id: Option<u64> = None;
        
        for segment in segments.iter() {
            let mut file = BufReader::new(
                File::open(&segment.path)?
            );
            
            // Skip header
            file.seek(SeekFrom::Start(16))?;
            
            loop {
                let offset = file.stream_position()?;
                
                match WALSegment::read_entry_with_recovery(&mut file) {
                    Ok(entry) => {
                        eprintln!("Read entry: {:?}", entry.operation);
                        if !entry.verify_checksum() {
                            warn!("Corrupted entry at LSN {}, stopping recovery", entry.lsn);
                            break;
                        }
                        
                        // Update recovery info
                        recovery_info.last_good_lsn = entry.lsn;
                        recovery_info.last_good_offset = offset;
                        
                        // Handle transaction operations
                        match &entry.operation {
                            WALOperation::TransactionBegin { tx_id } => {
                                current_tx_id = Some(*tx_id);
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::InProgress { operations: Vec::new() }
                                );
                            }
                            WALOperation::TransactionCommit { tx_id } => {
                                // Clone operations before applying
                                let ops_to_apply = if let Some(TransactionRecoveryState::InProgress { operations }) = 
                                    recovery_info.recovered_transactions.get(tx_id) {
                                    operations.clone()
                                } else {
                                    Vec::new()
                                };
                                
                                // Apply all operations in the transaction
                                for op in ops_to_apply {
                                    apply_op(&op, &TransactionRecoveryState::Committed)?;
                                }
                                
                                // Mark transaction as committed
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::Committed
                                );
                                
                                if current_tx_id == Some(*tx_id) {
                                    current_tx_id = None;
                                }
                            }
                            WALOperation::TransactionAbort { tx_id } => {
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::Aborted
                                );
                                if current_tx_id == Some(*tx_id) {
                                    current_tx_id = None;
                                }
                            }
                            WALOperation::Checkpoint { lsn } => {
                                recovery_info.checkpoint_lsn = *lsn;
                            }
                            op @ (WALOperation::Put { .. } | WALOperation::Delete { .. }) => {
                                // If we have a current transaction, add the operation to it
                                if let Some(tx_id) = current_tx_id {
                                    if let Some(TransactionRecoveryState::InProgress { operations }) = 
                                        recovery_info.recovered_transactions.get_mut(&tx_id) {
                                        operations.push(op.clone());
                                    }
                                } else {
                                    // No active transaction - operation is implicitly committed
                                    apply_op(op, &TransactionRecoveryState::Committed)?;
                                }
                            }
                            WALOperation::BeginTransaction { tx_id } => {
                                current_tx_id = Some(*tx_id);
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::InProgress { operations: Vec::new() }
                                );
                            }
                            WALOperation::CommitTransaction { tx_id } => {
                                // Clone operations before applying
                                let ops_to_apply = if let Some(TransactionRecoveryState::InProgress { operations }) = 
                                    recovery_info.recovered_transactions.get(tx_id) {
                                    operations.clone()
                                } else {
                                    Vec::new()
                                };
                                
                                // Apply all operations in the transaction
                                for op in ops_to_apply {
                                    apply_op(&op, &TransactionRecoveryState::Committed)?;
                                }
                                
                                // Mark transaction as committed
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::Committed
                                );
                                
                                if current_tx_id == Some(*tx_id) {
                                    current_tx_id = None;
                                }
                            }
                            WALOperation::AbortTransaction { tx_id } => {
                                recovery_info.recovered_transactions.insert(
                                    *tx_id,
                                    TransactionRecoveryState::Aborted
                                );
                                if current_tx_id == Some(*tx_id) {
                                    current_tx_id = None;
                                }
                            }
                        }
                        
                        // Report progress
                        progress(entry.lsn, segment.size.load(Ordering::Relaxed));
                    }
                    Err(e) => {
                        if WALSegment::is_incomplete_write(&e) {
                            info!("Found incomplete write, recovery complete for segment");
                        } else {
                            warn!("Error during recovery: {:?}", e);
                        }
                        break;
                    }
                }
            }
        }
        
        // Log uncommitted transactions
        for (tx_id, state) in &recovery_info.recovered_transactions {
            if let TransactionRecoveryState::InProgress { operations } = state {
                warn!(
                    "Transaction {} was not committed, {} operations will be rolled back",
                    tx_id,
                    operations.len()
                );
            }
        }
        
        Ok(())
    }
    
    pub fn archive_old_segments(&self, keep_segments: usize) -> Result<Vec<PathBuf>> {
        let mut segments = self.segments.write();
        let mut archived = Vec::new();
        
        while segments.len() > keep_segments {
            if let Some(old_segment) = segments.first() {
                // Don't archive if it's the active segment
                let active_id = self.active_segment.read().segment_id;
                if old_segment.segment_id == active_id {
                    break;
                }
                
                // Close and archive
                old_segment.close()?;
                
                let archive_path = old_segment.path.with_extension("archive");
                std::fs::rename(&old_segment.path, &archive_path)?;
                
                archived.push(archive_path);
                segments.remove(0);
            } else {
                break;
            }
        }
        
        Ok(archived)
    }
    
    pub fn checkpoint(&self) -> Result<u64> {
        let current_lsn = self.current_lsn();
        let checkpoint_lsn = self.append(WALOperation::Checkpoint { lsn: current_lsn })?;
        self.last_checkpoint_lsn.store(checkpoint_lsn, Ordering::SeqCst);
        
        // Force sync after checkpoint
        self.sync()?;
        
        Ok(checkpoint_lsn)
    }
    
    pub fn sync(&self) -> Result<()> {
        let active = self.active_segment.read();
        active.sync()
    }
    
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst) - 1
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        
        // Flush any pending commits
        if self.group_commit_enabled {
            let batch = self.collect_commit_batch();
            if !batch.entries.is_empty() {
                self.process_commit_batch(batch);
            }
        }
        
        // Final sync
        let _ = self.sync();
    }
}

impl Drop for ImprovedWriteAheadLog {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_recovery_incomplete_write() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        
        // Create WAL and write some entries
        {
            let wal = ImprovedWriteAheadLog::create(&wal_path).unwrap();
            
            wal.append(WALOperation::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            }).unwrap();
            
            // Simulate incomplete write by corrupting the file
            let segment_path = wal_path.join("wal_00000000.seg");
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&segment_path)
                .unwrap();
            
            // Write partial length bytes
            file.write_all(&[1, 2]).unwrap();
        }
        
        // Recover should handle the incomplete write
        let wal = ImprovedWriteAheadLog::open(&wal_path).unwrap();
        
        let mut recovered = Vec::new();
        wal.recover_with_progress(
            |op, _| {
                recovered.push(op.clone());
                Ok(())
            },
            |_, _| {},
        ).unwrap();
        
        assert_eq!(recovered.len(), 1);
    }
    
    #[test]
    fn test_transaction_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");
        
        {
            let wal = ImprovedWriteAheadLog::create(&wal_path).unwrap();
            
            // Complete transaction
            wal.append(WALOperation::TransactionBegin { tx_id: 1 }).unwrap();
            wal.append(WALOperation::Put {
                key: b"tx1_key".to_vec(),
                value: b"tx1_value".to_vec(),
            }).unwrap();
            wal.append(WALOperation::TransactionCommit { tx_id: 1 }).unwrap();
            
            // Incomplete transaction
            wal.append(WALOperation::TransactionBegin { tx_id: 2 }).unwrap();
            wal.append(WALOperation::Put {
                key: b"tx2_key".to_vec(),
                value: b"tx2_value".to_vec(),
            }).unwrap();
            // No commit!
            
            // Force sync to ensure data is written
            wal.sync().unwrap();
            
            // Properly shutdown to ensure all data is written
            wal.shutdown();
        }
        
        // Recover
        let wal = ImprovedWriteAheadLog::open(&wal_path).unwrap();
        
        let mut committed_ops = Vec::new();
        let mut uncommitted_ops = Vec::new();
        
        wal.recover_with_progress(
            |op, state| {
                match state {
                    TransactionRecoveryState::Committed => {
                        // Only count Put/Delete operations
                        match op {
                            WALOperation::Put { .. } | WALOperation::Delete { .. } => {
                                committed_ops.push(op.clone());
                            }
                            _ => {}
                        }
                    }
                    _ => uncommitted_ops.push(op.clone()),
                }
                Ok(())
            },
            |_, _| {},
        ).unwrap();
        
        assert_eq!(committed_ops.len(), 1);
        assert_eq!(uncommitted_ops.len(), 0); // Uncommitted ops are not applied
    }
    
    // TODO: Add test for segment rotation once we expose configuration
}
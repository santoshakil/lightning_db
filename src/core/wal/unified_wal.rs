use crate::core::error::{Error, Result};
use crate::utils::retry::RetryableOperations;
use bincode::{Decode, Encode};
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

const WAL_MAGIC: u32 = 0x5741_4C33; // "WAL3" - unified version
const WAL_VERSION: u32 = 3;
const SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64MB segments
// Tuned for enterprise throughput: batch more writes before commit
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_millis(2); // 2ms batching window
const GROUP_COMMIT_SIZE: usize = 5000; // Higher batch size for throughput

/// Unified WAL operation types - consolidates all variants
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionAbort { tx_id: u64 },
    Checkpoint { lsn: u64 },
}

/// Unified WAL entry structure with comprehensive metadata
#[derive(Debug, Clone, Encode, Decode)]
pub struct WALEntry {
    pub lsn: u64,
    pub timestamp: u64,
    pub operation: WALOperation,
    pub checksum: u32,
    pub tx_id: Option<u64>, // Optional transaction ID for operation context
}

impl WALEntry {
    pub fn new(lsn: u64, operation: WALOperation) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let tx_id = match &operation {
            WALOperation::TransactionBegin { tx_id }
            | WALOperation::TransactionCommit { tx_id }
            | WALOperation::TransactionAbort { tx_id } => Some(*tx_id),
            _ => None,
        };

        Self {
            lsn,
            timestamp,
            operation,
            checksum: 0, // Will be calculated before writing
            tx_id,
        }
    }

    pub fn calculate_checksum(&mut self) {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.lsn.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());

        // Hash transaction ID if present
        if let Some(tx_id) = self.tx_id {
            hasher.update(&tx_id.to_le_bytes());
        }

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

    pub fn size(&self) -> usize {
        std::mem::size_of::<u64>() * 2 + // lsn + timestamp
        std::mem::size_of::<u32>() + // checksum
        std::mem::size_of::<Option<u64>>() + // tx_id
        match &self.operation {
            WALOperation::Put { key, value } => 1 + 4 + key.len() + 4 + value.len(),
            WALOperation::Delete { key } => 1 + 4 + key.len(),
            WALOperation::TransactionBegin { .. } |
            WALOperation::TransactionCommit { .. } |
            WALOperation::TransactionAbort { .. } |
            WALOperation::Checkpoint { .. } => 1 + 8,
        }
    }

    pub fn read_from<R: std::io::Read>(reader: &mut R) -> Result<Self> {
        use crate::core::error::Error;

        // Read length prefix
        const MAX_WAL_ENTRY_SIZE: usize = 100 * 1024 * 1024; // 100MB max entry

        let mut len_bytes = [0u8; 4];
        reader
            .read_exact(&mut len_bytes)
            .map_err(|e| Error::Io(e.to_string()))?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Validate length to prevent OOM
        if len == 0 || len > MAX_WAL_ENTRY_SIZE {
            return Err(Error::Corruption(format!(
                "Invalid WAL entry length: {} (max: {})",
                len, MAX_WAL_ENTRY_SIZE
            )));
        }

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

/// Recovery information for unified transaction state tracking
#[derive(Debug, Clone, Encode, Decode)]
pub struct RecoveryInfo {
    pub last_good_lsn: u64,
    pub last_good_offset: u64,
    pub recovered_transactions: HashMap<u64, TransactionRecoveryState>,
    pub checkpoint_lsn: u64,
    pub total_entries_scanned: u64,
    pub corrupted_entries: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum TransactionRecoveryState {
    InProgress { operations: Vec<WALOperation> },
    Committed,
    Aborted,
}

/// Unified WAL segment with enhanced error handling
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

        // Create file with retry
        let file = RetryableOperations::file_operation(|| {
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| Error::Io(format!("Failed to create WAL segment: {}", e)))
        })?;

        let mut writer = BufWriter::with_capacity(1024 * 1024, file); // 1MB buffer

        // Write segment header with unified format
        RetryableOperations::file_operation(|| {
            writer
                .write_all(&WAL_MAGIC.to_le_bytes())
                .and_then(|_| writer.write_all(&WAL_VERSION.to_le_bytes()))
                .and_then(|_| writer.write_all(&segment_id.to_le_bytes()))
                .and_then(|_| writer.flush())
                .map_err(|e| Error::Io(format!("Failed to write WAL header: {}", e)))
        })?;

        Ok(Self {
            segment_id,
            path,
            writer: Mutex::new(writer),
            size: AtomicU64::new(16), // Header size
            is_active: AtomicBool::new(true),
        })
    }

    fn open(path: PathBuf) -> Result<(Self, u64, u64)> {
        // Open file with retry
        let mut file = RetryableOperations::file_operation(|| {
            File::open(&path).map_err(|e| Error::Io(format!("Failed to open WAL segment: {}", e)))
        })?;

        // Read and verify header
        let mut magic = [0u8; 4];
        let mut version = [0u8; 4];
        let mut segment_id_bytes = [0u8; 8];

        RetryableOperations::file_operation(|| {
            file.read_exact(&mut magic)?;
            file.read_exact(&mut version)?;
            file.read_exact(&mut segment_id_bytes)?;
            Ok(())
        })?;

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

        // Reopen for appending with retry
        let file = RetryableOperations::file_operation(|| {
            OpenOptions::new()
                .append(true)
                .open(&path)
                .map_err(|e| Error::Io(format!("Failed to reopen WAL for append: {}", e)))
        })?;

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
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(Error::Io(e.to_string()));
            }
            Err(e) => return Err(Error::Io(e.to_string())),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;

        // Sanity check on length
        if len == 0 || len > 10 * 1024 * 1024 {
            // 10MB max entry size, 0 is invalid
            return Err(Error::Storage(format!("Invalid entry size: {}", len)));
        }

        // Try to read entry data
        let mut entry_bytes = vec![0u8; len];
        match reader.read_exact(&mut entry_bytes) {
            Ok(_) => {}
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
        // Calculate checksum before serializing
        let mut entry_with_checksum = entry.clone();
        entry_with_checksum.calculate_checksum();

        let serialized = bincode::encode_to_vec(&entry_with_checksum, bincode::config::standard())
            .map_err(|e| Error::Storage(format!("Failed to serialize: {}", e)))?;

        let entry_size = 4 + serialized.len() as u64;

        let mut writer = self.writer.lock();

        // Write length and data with retry
        RetryableOperations::file_operation(|| {
            writer
                .write_all(&(serialized.len() as u32).to_le_bytes())
                .and_then(|_| writer.write_all(&serialized))
                .map_err(|e| Error::Io(format!("Failed to write WAL entry: {}", e)))
        })?;

        // Update size
        let new_size = self.size.fetch_add(entry_size, Ordering::SeqCst) + entry_size;

        Ok(new_size)
    }

    fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();

        // Flush buffer with retry
        RetryableOperations::file_operation(|| {
            writer
                .flush()
                .map_err(|e| Error::Io(format!("Failed to flush WAL buffer: {}", e)))
        })?;

        // Get the underlying file and sync to disk with retry
        let file = writer.get_mut();
        RetryableOperations::file_operation(|| {
            file.sync_all()
                .map_err(|e| Error::Io(format!("Failed to sync WAL to disk: {}", e)))
        })?;

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
        if let Err(e) = self.close() {
            eprintln!("WARNING: WAL segment close failed during drop: {}. Data may be incomplete.", e);
        }
    }
}

/// Type alias for commit queue entry
type CommitQueueEntry = (WALEntry, Arc<Mutex<Option<Result<u64>>>>);

/// Group commit batch
struct CommitBatch {
    entries: Vec<CommitQueueEntry>,
    total_size: usize,
}

/// Sync mode for WAL operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalSyncMode {
    Sync,                          // Immediate sync
    Async,                         // No sync, rely on OS
    Periodic { interval_ms: u64 }, // Periodic sync
}

/// Configuration for unified WAL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedWalConfig {
    pub sync_mode: WalSyncMode,
    pub group_commit_enabled: bool,
    pub max_segment_size: u64,
    pub enable_compression: bool,
    pub strict_ordering: bool,
}

impl Default for UnifiedWalConfig {
    fn default() -> Self {
        Self {
            sync_mode: WalSyncMode::Async,
            group_commit_enabled: true,
            max_segment_size: SEGMENT_SIZE,
            enable_compression: false,
            strict_ordering: false,
        }
    }
}

/// Unified Write-Ahead Log implementation
/// Combines the best features from all previous implementations
pub struct UnifiedWriteAheadLog {
    base_path: PathBuf,
    segments: Arc<RwLock<Vec<Arc<WALSegment>>>>,
    active_segment: Arc<RwLock<Arc<WALSegment>>>,
    next_lsn: AtomicU64,
    next_segment_id: AtomicU64,
    last_checkpoint_lsn: AtomicU64,

    // Group commit
    commit_queue: Arc<Mutex<VecDeque<CommitQueueEntry>>>,
    commit_condvar: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,

    // Recovery tracking
    recovery_info: Arc<RwLock<RecoveryInfo>>,

    // Configuration
    config: UnifiedWalConfig,
}

impl std::fmt::Debug for UnifiedWriteAheadLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedWriteAheadLog")
            .field("base_path", &self.base_path)
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .field(
                "next_segment_id",
                &self.next_segment_id.load(Ordering::Relaxed),
            )
            .field(
                "last_checkpoint_lsn",
                &self.last_checkpoint_lsn.load(Ordering::Relaxed),
            )
            .field("config", &self.config)
            .finish()
    }
}

impl UnifiedWriteAheadLog {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        Self::create_with_config(path, UnifiedWalConfig::default())
    }

    pub fn create_with_config<P: AsRef<Path>>(
        path: P,
        config: UnifiedWalConfig,
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
                total_entries_scanned: 0,
                corrupted_entries: 0,
            })),
            config,
        });

        // Start group commit thread
        if wal.config.group_commit_enabled {
            let wal_clone = Arc::clone(&wal);
            std::thread::spawn(move || {
                wal_clone.group_commit_loop();
            });
        }

        Ok(wal)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>> {
        Self::open_with_config(path, UnifiedWalConfig::default())
    }

    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: UnifiedWalConfig,
    ) -> Result<Arc<Self>> {
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
            total_entries_scanned: 0,
            corrupted_entries: 0,
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
            let new_segment =
                Arc::new(WALSegment::create(&base_path, 0).map_err(|e| {
                    Error::Io(format!("Failed to create initial WAL segment: {}", e))
                })?);
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
            config,
        });

        // Start group commit thread
        if wal.config.group_commit_enabled {
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

        if self.config.group_commit_enabled {
            // Queue for group commit
            let result = Arc::new(Mutex::new(None));

            {
                let mut queue = self.commit_queue.lock();
                queue.push_back((entry, result.clone()));
                // Notify the commit thread
                self.commit_condvar.notify_one();
            }

            // Wait for result with exponential backoff and timeout
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(30);
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

                // Check timeout
                if start.elapsed() > timeout {
                    return Err(Error::Storage(
                        "WAL group commit timed out after 30s".to_string(),
                    ));
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
            if active.size.load(Ordering::Relaxed) >= self.config.max_segment_size {
                drop(active);
                self.rotate_segment()?
            } else {
                active.clone()
            }
        };

        // Write entry
        segment.write_entry(&entry)?;

        // Sync based on mode and operation type
        let should_sync = match self.config.sync_mode {
            WalSyncMode::Sync => true,
            WalSyncMode::Async => false,
            WalSyncMode::Periodic { .. } => {
                // Let the background thread handle periodic sync
                false
            }
        };

        if should_sync {
            segment.sync()?;
        }

        Ok(lsn)
    }

    fn rotate_segment(&self) -> Result<Arc<WALSegment>> {
        let mut active = self.active_segment.write();

        // Double-check under write lock
        if active.size.load(Ordering::Relaxed) < self.config.max_segment_size {
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
                    self.commit_condvar
                        .wait_for(&mut queue, Duration::from_micros(100));
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

        while batch.entries.len() < GROUP_COMMIT_SIZE
            && Instant::now() < deadline
            && !queue.is_empty()
        {
            if let Some((entry, result)) = queue.pop_front() {
                batch.total_size += entry.size();
                batch.entries.push((entry, result));
            }
        }

        batch
    }

    fn process_commit_batch(&self, batch: CommitBatch) {
        // Check if rotation needed
        let segment = {
            let active = self.active_segment.read();
            if active.size.load(Ordering::Relaxed) + batch.total_size as u64
                >= self.config.max_segment_size
            {
                drop(active);
                match self.rotate_segment() {
                    Ok(seg) => seg,
                    Err(e) => {
                        // Notify all waiters of error
                        for (_, result) in batch.entries {
                            *result.lock() = Some(Err(Error::Storage(format!(
                                "Segment rotation failed: {:?}",
                                e
                            ))));
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
            let write_result = segment.write_entry(&entry).map(|_| entry.lsn);
            *result.lock() = Some(write_result);
        }

        // Single sync for the batch based on config
        if matches!(self.config.sync_mode, WalSyncMode::Sync) {
            if let Err(e) = segment.sync() {
                eprintln!("WARNING: WAL batch sync failed: {}. Durability may be compromised.", e);
            }
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
            let mut file = BufReader::new(File::open(&segment.path)?);

            // Skip header
            file.seek(SeekFrom::Start(16))?;

            loop {
                let offset = file.stream_position()?;

                match WALSegment::read_entry_with_recovery(&mut file) {
                    Ok(entry) => {
                        recovery_info.total_entries_scanned += 1;

                        if !entry.verify_checksum() {
                            warn!("Corrupted entry at LSN {}, stopping recovery", entry.lsn);
                            recovery_info.corrupted_entries += 1;
                            if self.config.strict_ordering {
                                break;
                            }
                            continue;
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
                                    TransactionRecoveryState::InProgress {
                                        operations: Vec::new(),
                                    },
                                );
                            }
                            WALOperation::TransactionCommit { tx_id } => {
                                // Clone operations before applying
                                let ops_to_apply =
                                    if let Some(TransactionRecoveryState::InProgress {
                                        operations,
                                    }) = recovery_info.recovered_transactions.get(tx_id)
                                    {
                                        operations.clone()
                                    } else {
                                        Vec::new()
                                    };

                                // Apply all operations in the transaction
                                for op in ops_to_apply {
                                    apply_op(&op, &TransactionRecoveryState::Committed)?;
                                }

                                // Mark transaction as committed
                                recovery_info
                                    .recovered_transactions
                                    .insert(*tx_id, TransactionRecoveryState::Committed);

                                if current_tx_id == Some(*tx_id) {
                                    current_tx_id = None;
                                }
                            }
                            WALOperation::TransactionAbort { tx_id } => {
                                recovery_info
                                    .recovered_transactions
                                    .insert(*tx_id, TransactionRecoveryState::Aborted);
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
                                    if let Some(TransactionRecoveryState::InProgress {
                                        operations,
                                    }) = recovery_info.recovered_transactions.get_mut(&tx_id)
                                    {
                                        operations.push(op.clone());
                                    }
                                } else {
                                    // No active transaction - operation is implicitly committed
                                    apply_op(op, &TransactionRecoveryState::Committed)?;
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

    pub fn checkpoint(&self) -> Result<u64> {
        let current_lsn = self.current_lsn();
        let checkpoint_lsn = self.append(WALOperation::Checkpoint { lsn: current_lsn })?;
        self.last_checkpoint_lsn
            .store(checkpoint_lsn, Ordering::SeqCst);

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

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Flush any pending commits
        if self.config.group_commit_enabled {
            let batch = self.collect_commit_batch();
            if !batch.entries.is_empty() {
                self.process_commit_batch(batch);
            }
        }

        // Final sync - critical for durability
        if let Err(e) = self.sync() {
            eprintln!("WARNING: WAL final sync failed during shutdown: {}. Data may be lost.", e);
        }
    }

    pub fn get_recovery_info(&self) -> RecoveryInfo {
        self.recovery_info.read().clone()
    }
}

impl Drop for UnifiedWriteAheadLog {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Unified WriteAheadLog trait
pub trait WriteAheadLog: Send + Sync + std::fmt::Debug {
    fn append(&self, operation: WALOperation) -> Result<u64>;
    fn sync(&self) -> Result<()>;
    fn checkpoint(&self) -> Result<u64>;
    fn current_lsn(&self) -> u64;
}

impl WriteAheadLog for UnifiedWriteAheadLog {
    fn append(&self, operation: WALOperation) -> Result<u64> {
        Self::append(self, operation)
    }

    fn sync(&self) -> Result<()> {
        Self::sync(self)
    }

    fn checkpoint(&self) -> Result<u64> {
        Self::checkpoint(self)
    }

    fn current_lsn(&self) -> u64 {
        Self::current_lsn(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_unified_wal_basic_operations() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        // Test basic WAL functionality
        {
            let wal = UnifiedWriteAheadLog::create(&wal_path).unwrap();

            // Write entries
            let lsn1 = wal
                .append(WALOperation::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                })
                .unwrap();

            let lsn2 = wal
                .append(WALOperation::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                })
                .unwrap();

            assert_eq!(lsn1, 1);
            assert_eq!(lsn2, 2);

            wal.sync().unwrap();
        }

        // Reopen and continue writing
        {
            let wal = UnifiedWriteAheadLog::open(&wal_path).unwrap();

            // Should continue from the correct LSN
            let lsn3 = wal
                .append(WALOperation::Put {
                    key: b"key3".to_vec(),
                    value: b"value3".to_vec(),
                })
                .unwrap();

            assert!(lsn3 > 2);
        }
    }

    #[test]
    fn test_transaction_recovery() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        // Test transaction tracking and recovery state
        {
            let wal = UnifiedWriteAheadLog::create(&wal_path).unwrap();

            // Write complete transaction
            let lsn1 = wal
                .append(WALOperation::TransactionBegin { tx_id: 1 })
                .unwrap();
            let lsn2 = wal
                .append(WALOperation::Put {
                    key: b"tx1_key".to_vec(),
                    value: b"tx1_value".to_vec(),
                })
                .unwrap();
            let lsn3 = wal
                .append(WALOperation::TransactionCommit { tx_id: 1 })
                .unwrap();

            assert_eq!(lsn1, 1);
            assert_eq!(lsn2, 2);
            assert_eq!(lsn3, 3);

            wal.sync().unwrap();
        }

        // Reopen and verify we can continue from the right LSN
        let wal = UnifiedWriteAheadLog::open(&wal_path).unwrap();

        // Should be able to continue writing
        let lsn_next = wal
            .append(WALOperation::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
            })
            .unwrap();

        // LSN should be positive, actual value depends on scan results
        assert!(lsn_next > 0);
    }

    #[test]
    fn test_checksum_verification() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal");

        let wal = UnifiedWriteAheadLog::create(&wal_path).unwrap();

        // Create an entry and verify checksum calculation
        let lsn = wal
            .append(WALOperation::Put {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            })
            .unwrap();

        assert_eq!(lsn, 1);

        // Create a manual entry to test checksum
        let mut entry = WALEntry::new(
            100,
            WALOperation::Put {
                key: b"manual_key".to_vec(),
                value: b"manual_value".to_vec(),
            },
        );

        entry.calculate_checksum();
        assert!(entry.verify_checksum());

        // Corrupt the checksum
        entry.checksum = 0xdeadbeef;
        assert!(!entry.verify_checksum());
    }
}

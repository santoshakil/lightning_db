use crate::core::error::{Error, Result};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{VecDeque, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;
use std::mem;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

const WAL_MAGIC: u32 = 0x5741_4C34;
const WAL_VERSION: u32 = 4;
const SEGMENT_SIZE: u64 = 128 * 1024 * 1024; // 128MB segments
const BLOCK_SIZE: usize = 8192; // 8KB blocks
const GROUP_COMMIT_THRESHOLD: usize = 100;
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_micros(50);
const PREFETCH_DISTANCE: usize = 16; // Prefetch 16 blocks ahead
const COMPRESSION_THRESHOLD: usize = 1024; // Compress if > 1KB
const MAX_PARALLEL_WRITERS: usize = 4;
const WAL_BUFFER_SIZE: usize = 16 * 1024 * 1024; // 16MB write buffer

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Serialize, Deserialize)]
pub enum WALOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    TransactionBegin { tx_id: u64 },
    TransactionCommit { tx_id: u64 },
    TransactionAbort { tx_id: u64 },
    Checkpoint { lsn: u64 },
    Batch { operations: Vec<WALOperation> },
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct WALRecordHeader {
    pub magic: u32,
    pub record_type: u8,
    pub flags: u8,
    pub length: u32,
    pub lsn: u64,
    pub timestamp: u64,
    pub checksum: u32,
    pub prev_lsn: u64,
}

impl WALRecordHeader {
    const SIZE: usize = mem::size_of::<Self>();
    
    pub fn new(record_type: u8, length: u32, lsn: u64, prev_lsn: u64) -> Self {
        Self {
            magic: WAL_MAGIC,
            record_type,
            flags: 0,
            length,
            lsn,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            checksum: 0,
            prev_lsn,
        }
    }
    
    pub fn set_compressed(&mut self) {
        self.flags |= 0x01;
    }
    
    pub fn is_compressed(&self) -> bool {
        self.flags & 0x01 != 0
    }
    
    pub fn set_encrypted(&mut self) {
        self.flags |= 0x02;
    }
    
    pub fn is_encrypted(&self) -> bool {
        self.flags & 0x02 != 0
    }
    
    pub fn calculate_checksum(&mut self, data: &[u8]) {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") {
                self.checksum = unsafe { self.crc32c_hw(data) };
                return;
            }
        }
        
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&self.lsn.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(data);
        self.checksum = hasher.finalize();
    }
    
    #[cfg(target_arch = "x86_64")]
    unsafe fn crc32c_hw(&self, data: &[u8]) -> u32 {
        let mut crc = 0u32;
        
        for chunk in data.chunks(8) {
            if chunk.len() == 8 {
                let val = *(chunk.as_ptr() as *const u64);
                crc = std::arch::x86_64::_mm_crc32_u64(crc as u64, val) as u32;
            } else {
                for &byte in chunk {
                    crc = std::arch::x86_64::_mm_crc32_u8(crc, byte);
                }
            }
        }
        
        crc
    }
    
    pub fn verify_checksum(&self, data: &[u8]) -> bool {
        let mut header_copy = *self;
        header_copy.checksum = 0;
        header_copy.calculate_checksum(data);
        header_copy.checksum == self.checksum
    }
}

pub struct OptimizedWAL {
    config: WALConfig,
    current_lsn: Arc<AtomicU64>,
    current_segment: Arc<AtomicU64>,
    writer_pool: WriterPool,
    group_committer: Arc<GroupCommitter>,
    segment_manager: Arc<SegmentManager>,
    compression_engine: Arc<CompressionEngine>,
    prefetcher: Arc<Prefetcher>,
    metrics: Arc<WALMetrics>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct WALConfig {
    pub dir_path: PathBuf,
    pub segment_size: u64,
    pub max_segments: usize,
    pub compression_enabled: bool,
    pub encryption_enabled: bool,
    pub direct_io: bool,
    pub fsync_mode: FsyncMode,
    pub parallel_writers: usize,
    pub prefetch_enabled: bool,
    pub archive_enabled: bool,
    pub archive_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncMode {
    Never,
    EveryCommit,
    Interval(Duration),
    Adaptive,
}

#[derive(Clone)]
struct WriterPool {
    writers: Vec<WALWriter>,
    assignment: Arc<AtomicUsize>,
}

impl WriterPool {
    fn new(config: &WALConfig) -> Result<Self> {
        let mut writers = Vec::with_capacity(config.parallel_writers);
        
        for id in 0..config.parallel_writers {
            writers.push(WALWriter::new(id, config)?);
        }
        
        Ok(Self {
            writers,
            assignment: Arc::new(AtomicUsize::new(0)),
        })
    }
    
    fn get_writer(&self) -> &WALWriter {
        let idx = self.assignment.fetch_add(1, Ordering::Relaxed) % self.writers.len();
        &self.writers[idx]
    }
}

#[derive(Clone)]
struct WALWriter {
    id: usize,
    file: Arc<Mutex<File>>,
    buffer: Arc<Mutex<Vec<u8>>>,
    pending_syncs: Arc<AtomicU32>,
    last_sync: Arc<Mutex<Instant>>,
}

impl WALWriter {
    fn new(id: usize, config: &WALConfig) -> Result<Self> {
        let path = config.dir_path.join(format!("wal_writer_{}.tmp", id));
        
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(true);
        
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if config.direct_io {
                options.custom_flags(libc::O_DIRECT | libc::O_SYNC);
            }
        }
        
        let file = options.open(&path)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        Ok(Self {
            id,
            file: Arc::new(Mutex::new(file)),
            buffer: Arc::new(Mutex::new(Vec::with_capacity(WAL_BUFFER_SIZE))),
            pending_syncs: Arc::new(AtomicU32::new(0)),
            last_sync: Arc::new(Mutex::new(Instant::now())),
        })
    }
    
    fn write_record(&self, header: &WALRecordHeader, data: &[u8]) -> Result<()> {
        let mut buffer = self.buffer.lock();
        
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                header as *const _ as *const u8,
                WALRecordHeader::SIZE
            )
        };
        
        buffer.extend_from_slice(header_bytes);
        buffer.extend_from_slice(data);
        
        let buffer_len = buffer.len();
        let padding = (BLOCK_SIZE - (buffer_len % BLOCK_SIZE)) % BLOCK_SIZE;
        if padding > 0 {
            buffer.resize(buffer_len + padding, 0);
        }
        
        if buffer.len() >= WAL_BUFFER_SIZE - BLOCK_SIZE {
            self.flush_buffer(&mut buffer)?;
        }
        
        self.pending_syncs.fetch_add(1, Ordering::Release);
        
        Ok(())
    }
    
    fn flush_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }
        
        let mut file = self.file.lock();
        file.write_all(buffer)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        buffer.clear();
        
        Ok(())
    }
    
    fn sync(&self, mode: FsyncMode) -> Result<()> {
        let should_sync = match mode {
            FsyncMode::Never => false,
            FsyncMode::EveryCommit => true,
            FsyncMode::Interval(duration) => {
                let last_sync = *self.last_sync.lock();
                last_sync.elapsed() >= duration
            }
            FsyncMode::Adaptive => {
                let pending = self.pending_syncs.load(Ordering::Acquire);
                pending >= 100 || self.last_sync.lock().elapsed() >= Duration::from_millis(10)
            }
        };
        
        if should_sync {
            let mut buffer = self.buffer.lock();
            self.flush_buffer(&mut buffer)?;
            
            let file = self.file.lock();
            file.sync_all()
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            self.pending_syncs.store(0, Ordering::Release);
            *self.last_sync.lock() = Instant::now();
        }
        
        Ok(())
    }
}

struct GroupCommitter {
    pending: Arc<Mutex<Vec<PendingWrite>>>,
    commit_notify: Arc<Condvar>,
    commit_thread: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

struct PendingWrite {
    operation: WALOperation,
    completion: Arc<AtomicBool>,
}

impl GroupCommitter {
    fn new(wal: Arc<OptimizedWAL>) -> Self {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let commit_notify = Arc::new(Condvar::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let pending_clone = pending.clone();
        let notify_clone = commit_notify.clone();
        let shutdown_clone = shutdown.clone();
        
        let commit_thread = thread::spawn(move || {
            Self::commit_loop(wal, pending_clone, notify_clone, shutdown_clone);
        });
        
        Self {
            pending,
            commit_notify,
            commit_thread: Some(commit_thread),
            shutdown,
        }
    }
    
    fn submit(&self, operation: WALOperation) -> Arc<AtomicBool> {
        let completion = Arc::new(AtomicBool::new(false));
        
        let pending_write = PendingWrite {
            operation,
            completion: completion.clone(),
        };
        
        {
            let mut pending = self.pending.lock();
            pending.push(pending_write);
            
            if pending.len() >= GROUP_COMMIT_THRESHOLD {
                self.commit_notify.notify_one();
            }
        }
        
        completion
    }
    
    fn commit_loop(
        wal: Arc<OptimizedWAL>,
        pending: Arc<Mutex<Vec<PendingWrite>>>,
        notify: Arc<Condvar>,
        shutdown: Arc<AtomicBool>,
    ) {
        while !shutdown.load(Ordering::Acquire) {
            let mut pending_writes = pending.lock();
            
            let timeout = notify.wait_for(&mut pending_writes, GROUP_COMMIT_INTERVAL);
            
            if pending_writes.is_empty() && timeout.timed_out() {
                continue;
            }
            
            let batch: Vec<PendingWrite> = pending_writes.drain(..).collect();
            drop(pending_writes);
            
            if !batch.is_empty() {
                let operations: Vec<WALOperation> = batch.iter()
                    .map(|w| w.operation.clone())
                    .collect();
                
                if wal.write_batch_internal(operations).is_ok() {
                    for write in batch {
                        write.completion.store(true, Ordering::Release);
                    }
                }
            }
        }
    }
}

struct SegmentManager {
    dir_path: PathBuf,
    current_segment: AtomicU64,
    max_segments: usize,
    segments: RwLock<BTreeMap<u64, SegmentInfo>>,
    recycler: SegmentRecycler,
}

#[derive(Debug, Clone)]
struct SegmentInfo {
    id: u64,
    path: PathBuf,
    size: u64,
    start_lsn: u64,
    end_lsn: u64,
    created_at: SystemTime,
    sealed: bool,
}

struct SegmentRecycler {
    available: Arc<Mutex<Vec<PathBuf>>>,
    preallocated_size: u64,
}

impl SegmentRecycler {
    fn new(preallocated_size: u64) -> Self {
        Self {
            available: Arc::new(Mutex::new(Vec::new())),
            preallocated_size,
        }
    }
    
    fn get_or_create(&self, dir: &Path, segment_id: u64) -> Result<PathBuf> {
        let mut available = self.available.lock();
        
        if let Some(path) = available.pop() {
            Ok(path)
        } else {
            let path = dir.join(format!("wal_{:08}.log", segment_id));
            
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                unsafe {
                    libc::fallocate(
                        file.as_raw_fd(),
                        0,
                        0,
                        self.preallocated_size as i64
                    );
                }
            }
            
            file.set_len(self.preallocated_size)
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            Ok(path)
        }
    }
    
    fn recycle(&self, path: PathBuf) {
        let mut available = self.available.lock();
        if available.len() < 10 {
            available.push(path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct CompressionEngine {
    algorithm: CompressionAlgorithm,
    threshold: usize,
}

#[derive(Debug, Clone, Copy)]
enum CompressionAlgorithm {
    LZ4,
    Zstd,
    Snappy,
}

impl CompressionEngine {
    fn new(algorithm: CompressionAlgorithm, threshold: usize) -> Self {
        Self {
            algorithm,
            threshold,
        }
    }
    
    fn compress(&self, data: &[u8]) -> Option<Vec<u8>> {
        if data.len() < self.threshold {
            return None;
        }
        
        match self.algorithm {
            CompressionAlgorithm::LZ4 => {
                Some(lz4_flex::compress_prepend_size(data))
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd-compression")]
                {
                    zstd::encode_all(data, 1).ok()
                }
                #[cfg(not(feature = "zstd-compression"))]
                None
            }
            CompressionAlgorithm::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder.compress_vec(data).ok()
            }
        }
    }
    
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            CompressionAlgorithm::LZ4 => {
                lz4_flex::decompress_size_prepended(data)
                    .map_err(|e| Error::CompressionError(e.to_string()))
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd-compression")]
                {
                    zstd::decode_all(data)
                        .map_err(|e| Error::CompressionError(e.to_string()))
                }
                #[cfg(not(feature = "zstd-compression"))]
                Err(Error::CompressionError("ZSTD compression not available".to_string()))
            }
            CompressionAlgorithm::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress_vec(data)
                    .map_err(|e| Error::CompressionError(e.to_string()))
            }
        }
    }
}

struct Prefetcher {
    cache: Arc<RwLock<VecDeque<CachedBlock>>>,
    prefetch_thread: Option<thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone)]
struct CachedBlock {
    lsn: u64,
    data: Vec<u8>,
    timestamp: Instant,
}

impl Prefetcher {
    fn new(capacity: usize) -> Self {
        let cache = Arc::new(RwLock::new(VecDeque::with_capacity(capacity)));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        Self {
            cache,
            prefetch_thread: None,
            shutdown,
        }
    }
    
    fn prefetch(&self, path: &Path, start_lsn: u64, count: usize) -> Result<()> {
        let mut file = File::open(path)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let mut cache = self.cache.write();
        
        for i in 0..count {
            let mut header_buf = vec![0u8; WALRecordHeader::SIZE];
            file.read_exact(&mut header_buf)
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            let header = unsafe {
                *(header_buf.as_ptr() as *const WALRecordHeader)
            };
            
            let mut data = vec![0u8; header.length as usize];
            file.read_exact(&mut data)
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            cache.push_back(CachedBlock {
                lsn: start_lsn + i as u64,
                data,
                timestamp: Instant::now(),
            });
            
            if cache.len() > PREFETCH_DISTANCE {
                cache.pop_front();
            }
        }
        
        Ok(())
    }
    
    fn get(&self, lsn: u64) -> Option<Vec<u8>> {
        let cache = self.cache.read();
        cache.iter()
            .find(|block| block.lsn == lsn)
            .map(|block| block.data.clone())
    }
}

struct WALMetrics {
    writes_total: AtomicU64,
    bytes_written: AtomicU64,
    compression_ratio: AtomicU64,
    sync_count: AtomicU64,
    sync_duration_us: AtomicU64,
    group_commit_batches: AtomicU64,
    prefetch_hits: AtomicU64,
    prefetch_misses: AtomicU64,
}

impl WALMetrics {
    fn new() -> Self {
        Self {
            writes_total: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            compression_ratio: AtomicU64::new(100),
            sync_count: AtomicU64::new(0),
            sync_duration_us: AtomicU64::new(0),
            group_commit_batches: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            prefetch_misses: AtomicU64::new(0),
        }
    }
    
    fn record_write(&self, bytes: u64) {
        self.writes_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }
    
    fn record_sync(&self, duration: Duration) {
        self.sync_count.fetch_add(1, Ordering::Relaxed);
        self.sync_duration_us.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
    
    fn get_average_sync_time(&self) -> Duration {
        let count = self.sync_count.load(Ordering::Relaxed);
        let total_us = self.sync_duration_us.load(Ordering::Relaxed);
        
        if count > 0 {
            Duration::from_micros(total_us / count)
        } else {
            Duration::ZERO
        }
    }
}

impl OptimizedWAL {
    pub fn new(config: WALConfig) -> Result<Arc<Self>> {
        std::fs::create_dir_all(&config.dir_path)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let current_lsn = Arc::new(AtomicU64::new(1));
        let current_segment = Arc::new(AtomicU64::new(0));
        let writer_pool = WriterPool::new(&config)?;
        
        let segment_manager = Arc::new(SegmentManager {
            dir_path: config.dir_path.clone(),
            current_segment: AtomicU64::new(0),
            max_segments: config.max_segments,
            segments: RwLock::new(BTreeMap::new()),
            recycler: SegmentRecycler::new(config.segment_size),
        });
        
        let compression_engine = Arc::new(CompressionEngine::new(
            CompressionAlgorithm::LZ4,
            COMPRESSION_THRESHOLD,
        ));
        
        let prefetcher = Arc::new(Prefetcher::new(PREFETCH_DISTANCE * 2));
        let metrics = Arc::new(WALMetrics::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        
        // Create a placeholder GroupCommitter first
        let placeholder_committer = Arc::new(GroupCommitter {
            pending: Arc::new(Mutex::new(Vec::new())),
            commit_notify: Arc::new(Condvar::new()),
            commit_thread: None,
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        
        let wal = Arc::new(Self {
            config: config.clone(),
            current_lsn: current_lsn.clone(),
            current_segment: current_segment.clone(),
            writer_pool,
            group_committer: placeholder_committer,
            segment_manager,
            compression_engine,
            prefetcher,
            metrics,
            shutdown,
        });
        
        Ok(wal)
    }
    
    pub fn write(&self, operation: WALOperation) -> Result<u64> {
        let lsn = self.current_lsn.fetch_add(1, Ordering::SeqCst);
        
        let data = bincode::encode_to_vec(&operation, bincode::config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        let compressed = if self.config.compression_enabled {
            self.compression_engine.compress(&data)
        } else {
            None
        };
        
        let (final_data, mut header) = if let Some(compressed_data) = compressed {
            let ratio = (compressed_data.len() as f64 / data.len() as f64 * 100.0) as u64;
            self.metrics.compression_ratio.store(ratio, Ordering::Relaxed);
            
            let mut header = WALRecordHeader::new(
                0,
                compressed_data.len() as u32,
                lsn,
                lsn - 1,
            );
            header.set_compressed();
            (compressed_data, header)
        } else {
            (data.clone(), WALRecordHeader::new(0, data.len() as u32, lsn, lsn - 1))
        };
        
        header.calculate_checksum(&final_data);
        
        let writer = self.writer_pool.get_writer();
        writer.write_record(&header, &final_data)?;
        writer.sync(self.config.fsync_mode)?;
        
        self.metrics.record_write(final_data.len() as u64);
        
        Ok(lsn)
    }
    
    pub fn write_batch(&self, operations: Vec<WALOperation>) -> Result<Vec<u64>> {
        let batch_op = WALOperation::Batch { operations: operations.clone() };
        let batch_lsn = self.write(batch_op)?;
        
        let mut lsns = vec![batch_lsn];
        for _ in 1..operations.len() {
            lsns.push(self.current_lsn.fetch_add(1, Ordering::SeqCst));
        }
        
        self.metrics.group_commit_batches.fetch_add(1, Ordering::Relaxed);
        
        Ok(lsns)
    }
    
    fn write_batch_internal(&self, operations: Vec<WALOperation>) -> Result<()> {
        self.write_batch(operations)?;
        Ok(())
    }
    
    pub fn submit_async(&self, operation: WALOperation) -> Arc<AtomicBool> {
        self.group_committer.submit(operation)
    }
    
    pub fn checkpoint(&self) -> Result<u64> {
        let lsn = self.current_lsn.load(Ordering::SeqCst);
        self.write(WALOperation::Checkpoint { lsn })?;
        
        let segment_id = self.current_segment.fetch_add(1, Ordering::SeqCst);
        let new_segment_path = self.segment_manager.recycler.get_or_create(
            &self.config.dir_path,
            segment_id + 1
        )?;
        
        let mut segments = self.segment_manager.segments.write();
        segments.insert(segment_id, SegmentInfo {
            id: segment_id,
            path: new_segment_path,
            size: 0,
            start_lsn: lsn,
            end_lsn: lsn,
            created_at: SystemTime::now(),
            sealed: false,
        });
        
        self.archive_old_segments()?;
        
        Ok(lsn)
    }
    
    fn archive_old_segments(&self) -> Result<()> {
        if !self.config.archive_enabled {
            return Ok(());
        }
        
        let archive_path = self.config.archive_path.as_ref()
            .ok_or_else(|| Error::ConfigError("Archive path not configured".to_string()))?;
        
        std::fs::create_dir_all(archive_path)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let segments = self.segment_manager.segments.read();
        let current_segment = self.current_segment.load(Ordering::SeqCst);
        
        for (id, info) in segments.iter() {
            if *id < current_segment.saturating_sub(self.config.max_segments as u64) {
                let archive_file = archive_path.join(format!("archived_{:08}.wal", id));
                
                if self.config.compression_enabled {
                    self.compress_and_archive(&info.path, &archive_file)?;
                } else {
                    std::fs::copy(&info.path, &archive_file)
                        .map_err(|e| Error::IoError(e.to_string()))?;
                }
                
                self.segment_manager.recycler.recycle(info.path.clone());
            }
        }
        
        Ok(())
    }
    
    fn compress_and_archive(&self, source: &Path, _dest: &Path) -> Result<()> {
        #[cfg(feature = "zstd-compression")]
        {
            let input = std::fs::read(source)
                .map_err(|e| Error::IoError(e.to_string()))?;
            let compressed = zstd::encode_all(&input[..], 3)
                .map_err(|e| Error::CompressionError(e.to_string()))?;
            
            std::fs::write(_dest, compressed)
                .map_err(|e| Error::IoError(e.to_string()))?;
            Ok(())
        }
        
        #[cfg(not(feature = "zstd-compression"))]
        {
            let _ = source;
            Err(Error::CompressionError("ZSTD compression not available".to_string()))
        }
    }
    
    pub fn get_metrics(&self) -> WALMetricsSnapshot {
        WALMetricsSnapshot {
            writes_total: self.metrics.writes_total.load(Ordering::Relaxed),
            bytes_written: self.metrics.bytes_written.load(Ordering::Relaxed),
            compression_ratio: self.metrics.compression_ratio.load(Ordering::Relaxed),
            sync_count: self.metrics.sync_count.load(Ordering::Relaxed),
            average_sync_time: self.metrics.get_average_sync_time(),
            group_commit_batches: self.metrics.group_commit_batches.load(Ordering::Relaxed),
            prefetch_hit_rate: {
                let hits = self.metrics.prefetch_hits.load(Ordering::Relaxed);
                let misses = self.metrics.prefetch_misses.load(Ordering::Relaxed);
                let total = hits + misses;
                if total > 0 {
                    (hits as f64 / total as f64) * 100.0
                } else {
                    0.0
                }
            },
        }
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.group_committer.shutdown.store(true, Ordering::SeqCst);
        self.prefetcher.shutdown.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
pub struct WALMetricsSnapshot {
    pub writes_total: u64,
    pub bytes_written: u64,
    pub compression_ratio: u64,
    pub sync_count: u64,
    pub average_sync_time: Duration,
    pub group_commit_batches: u64,
    pub prefetch_hit_rate: f64,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            dir_path: PathBuf::from("./wal"),
            segment_size: SEGMENT_SIZE,
            max_segments: 10,
            compression_enabled: true,
            encryption_enabled: false,
            direct_io: false,
            fsync_mode: FsyncMode::Adaptive,
            parallel_writers: MAX_PARALLEL_WRITERS,
            prefetch_enabled: true,
            archive_enabled: false,
            archive_path: None,
        }
    }
}

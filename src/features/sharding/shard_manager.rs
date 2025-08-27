use std::sync::Arc;
use std::collections::{HashMap, VecDeque};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use crate::core::async_page_manager::PageManagerAsync;
use tokio::sync::{mpsc, oneshot, broadcast, Semaphore};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut, BufMut};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    pub shard_id: super::coordinator::ShardId,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
    pub size_bytes: AtomicUsize,
    pub record_count: AtomicU64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub last_compaction: Option<chrono::DateTime<chrono::Utc>>,
    pub bloom_filter: Option<BloomFilter>,
    pub statistics: ShardStatistics,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ShardState {
    Initializing,
    Active,
    ReadOnly,
    Compacting,
    Migrating,
    Splitting,
    Merging,
    Flushing,
    Recovering,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStatistics {
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub avg_key_size: usize,
    pub avg_value_size: usize,
    pub compression_ratio: f64,
    pub read_amplification: f64,
    pub write_amplification: f64,
    pub space_amplification: f64,
    pub hot_key_count: usize,
    pub cold_key_count: usize,
    pub tombstone_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BloomFilter {
    bits: Vec<u8>,
    num_hash_functions: usize,
    size_bits: usize,
    items_count: usize,
    false_positive_rate: f64,
}

impl BloomFilter {
    fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let size_bits = Self::optimal_size(expected_items, false_positive_rate);
        let num_hash_functions = Self::optimal_hash_functions(expected_items, size_bits);
        
        Self {
            bits: vec![0; (size_bits + 7) / 8],
            num_hash_functions,
            size_bits,
            items_count: 0,
            false_positive_rate,
        }
    }

    fn optimal_size(n: usize, p: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        let size = -(n as f64 * p.ln()) / (ln2 * ln2);
        size.ceil() as usize
    }

    fn optimal_hash_functions(n: usize, m: usize) -> usize {
        let k = (m as f64 / n as f64) * std::f64::consts::LN_2;
        k.ceil() as usize
    }

    fn add(&mut self, key: &[u8]) {
        for i in 0..self.num_hash_functions {
            let hash = self.hash(key, i);
            let bit_pos = hash % self.size_bits;
            let byte_pos = bit_pos / 8;
            let bit_offset = bit_pos % 8;
            
            self.bits[byte_pos] |= 1 << bit_offset;
        }
        self.items_count += 1;
    }

    fn contains(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hash_functions {
            let hash = self.hash(key, i);
            let bit_pos = hash % self.size_bits;
            let byte_pos = bit_pos / 8;
            let bit_offset = bit_pos % 8;
            
            if (self.bits[byte_pos] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, key: &[u8], seed: usize) -> usize {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }
}

pub struct ShardManager {
    shards: Arc<DashMap<super::coordinator::ShardId, Arc<Shard>>>,
    storage: Arc<dyn PageManagerAsync>,
    write_buffer_pool: Arc<WriteBufferPool>,
    compaction_scheduler: Arc<CompactionScheduler>,
    flush_scheduler: Arc<FlushScheduler>,
    metrics: Arc<ShardMetrics>,
    config: Arc<ShardConfig>,
    shutdown: Arc<AtomicBool>,
}

struct Shard {
    metadata: Arc<RwLock<ShardMetadata>>,
    state: Arc<RwLock<ShardState>>,
    memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<RwLock<VecDeque<Arc<MemTable>>>>,
    sstables: Arc<RwLock<Vec<Arc<SSTable>>>>,
    write_ahead_log: Arc<WriteAheadLog>,
    read_cache: Arc<ReadCache>,
    write_buffer: Arc<WriteBuffer>,
    compaction_filter: Option<Arc<dyn CompactionFilter>>,
}

struct MemTable {
    data: Arc<RwLock<skiplist::SkipList<Vec<u8>, Vec<u8>>>>,
    size_bytes: AtomicUsize,
    entry_count: AtomicU64,
    min_key: Arc<RwLock<Option<Vec<u8>>>>,
    max_key: Arc<RwLock<Option<Vec<u8>>>>,
    created_at: Instant,
    sequence_number: AtomicU64,
}

struct SSTable {
    id: u64,
    level: usize,
    file_path: std::path::PathBuf,
    size_bytes: usize,
    entry_count: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    bloom_filter: BloomFilter,
    index: SSTableIndex,
    metadata: SSTableMetadata,
}

struct SSTableIndex {
    entries: Vec<IndexEntry>,
    sparse_index: BTreeMap<Vec<u8>, usize>,
}

struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    size: u32,
}

struct SSTableMetadata {
    created_at: chrono::DateTime<chrono::Utc>,
    compression_type: CompressionType,
    checksum: u64,
    properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
enum CompressionType {
    None,
    Snappy,
    Lz4,
    Zstd,
}

struct WriteAheadLog {
    current_file: Arc<RwLock<std::fs::File>>,
    log_sequence_number: AtomicU64,
    buffer: Arc<RwLock<BytesMut>>,
    sync_on_write: bool,
}

struct ReadCache {
    cache: Arc<lru::LruCache<Vec<u8>, Arc<Vec<u8>>>>,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
}

struct WriteBuffer {
    buffer: Arc<RwLock<HashMap<Vec<u8>, WriteEntry>>>,
    size_bytes: AtomicUsize,
    max_size: usize,
}

struct WriteEntry {
    value: Vec<u8>,
    timestamp: u64,
    operation: WriteOperation,
}

#[derive(Debug, Clone, Copy)]
enum WriteOperation {
    Put,
    Delete,
    Merge,
}

struct WriteBufferPool {
    buffers: Arc<RwLock<Vec<Arc<WriteBuffer>>>>,
    available: Arc<Semaphore>,
    total_memory: AtomicUsize,
    max_memory: usize,
}

struct CompactionScheduler {
    pending: Arc<RwLock<VecDeque<CompactionJob>>>,
    active: Arc<RwLock<HashMap<super::coordinator::ShardId, CompactionJob>>>,
    worker_threads: usize,
    shutdown: Arc<AtomicBool>,
}

struct CompactionJob {
    shard_id: super::coordinator::ShardId,
    level: usize,
    input_files: Vec<Arc<SSTable>>,
    output_level: usize,
    priority: CompactionPriority,
    started_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CompactionPriority {
    Low,
    Normal,
    High,
    Critical,
}

struct FlushScheduler {
    pending: Arc<RwLock<VecDeque<FlushJob>>>,
    active: Arc<RwLock<HashMap<super::coordinator::ShardId, FlushJob>>>,
    flush_interval: Duration,
    shutdown: Arc<AtomicBool>,
}

struct FlushJob {
    shard_id: super::coordinator::ShardId,
    memtable: Arc<MemTable>,
    priority: FlushPriority,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FlushPriority {
    Background,
    Normal,
    Urgent,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub max_memtable_size: usize,
    pub max_immutable_memtables: usize,
    pub max_level0_files: usize,
    pub level_size_multiplier: usize,
    pub max_background_compactions: usize,
    pub max_background_flushes: usize,
    pub compression_type: CompressionType,
    pub block_size: usize,
    pub cache_size: usize,
    pub bloom_filter_bits_per_key: usize,
    pub write_buffer_size: usize,
    pub sync_writes: bool,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            max_memtable_size: 64 * 1024 * 1024,
            max_immutable_memtables: 2,
            max_level0_files: 10,
            level_size_multiplier: 10,
            max_background_compactions: 4,
            max_background_flushes: 2,
            compression_type: CompressionType::Lz4,
            block_size: 4096,
            cache_size: 128 * 1024 * 1024,
            bloom_filter_bits_per_key: 10,
            write_buffer_size: 4 * 1024 * 1024,
            sync_writes: false,
        }
    }
}

struct ShardMetrics {
    reads_total: AtomicU64,
    writes_total: AtomicU64,
    deletes_total: AtomicU64,
    compactions_total: AtomicU64,
    flushes_total: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    bloom_filter_useful: AtomicU64,
}

#[async_trait]
trait CompactionFilter: Send + Sync {
    async fn filter(&self, key: &[u8], value: &[u8], level: usize) -> FilterDecision;
}

enum FilterDecision {
    Keep,
    Remove,
    Transform(Vec<u8>),
}

use dashmap::DashMap;
use std::collections::BTreeMap;

impl ShardManager {
    pub async fn new(
        storage: Arc<dyn PageManagerAsync>,
        config: ShardConfig,
    ) -> Result<Self, Error> {
        let write_buffer_pool = Arc::new(WriteBufferPool::new(
            config.max_background_flushes,
            config.write_buffer_size * config.max_background_flushes,
        ));
        
        let compaction_scheduler = Arc::new(CompactionScheduler::new(
            config.max_background_compactions,
        ));
        
        let flush_scheduler = Arc::new(FlushScheduler::new(
            Duration::from_secs(60),
        ));
        
        Ok(Self {
            shards: Arc::new(DashMap::new()),
            storage,
            write_buffer_pool,
            compaction_scheduler,
            flush_scheduler,
            metrics: Arc::new(ShardMetrics {
                reads_total: AtomicU64::new(0),
                writes_total: AtomicU64::new(0),
                deletes_total: AtomicU64::new(0),
                compactions_total: AtomicU64::new(0),
                flushes_total: AtomicU64::new(0),
                bytes_read: AtomicU64::new(0),
                bytes_written: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                bloom_filter_useful: AtomicU64::new(0),
            }),
            config: Arc::new(config),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn create_shard(
        &self,
        shard_id: super::coordinator::ShardId,
        range_start: Vec<u8>,
        range_end: Vec<u8>,
    ) -> Result<(), Error> {
        if self.shards.contains_key(&shard_id) {
            return Err(Error::AlreadyExists(format!("Shard {:?} already exists", shard_id)));
        }
        
        let metadata = ShardMetadata {
            shard_id,
            range_start: range_start.clone(),
            range_end: range_end.clone(),
            size_bytes: AtomicUsize::new(0),
            record_count: AtomicU64::new(0),
            created_at: chrono::Utc::now(),
            last_modified: chrono::Utc::now(),
            last_compaction: None,
            bloom_filter: Some(BloomFilter::new(100000, 0.01)),
            statistics: ShardStatistics {
                min_key: range_start,
                max_key: range_end,
                avg_key_size: 0,
                avg_value_size: 0,
                compression_ratio: 1.0,
                read_amplification: 1.0,
                write_amplification: 1.0,
                space_amplification: 1.0,
                hot_key_count: 0,
                cold_key_count: 0,
                tombstone_count: 0,
            },
        };
        
        let shard = Arc::new(Shard::new(metadata, self.config.clone()).await?);
        self.shards.insert(shard_id, shard);
        
        Ok(())
    }

    pub async fn get(&self, shard_id: super::coordinator::ShardId, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        self.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
        
        if let Some(bloom) = &shard.metadata.read().bloom_filter {
            if !bloom.contains(key) {
                self.metrics.bloom_filter_useful.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        
        if let Some(value) = shard.read_cache.get(key) {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(value));
        }
        
        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        if let Some(entry) = shard.write_buffer.get(key) {
            return match entry.operation {
                WriteOperation::Put => Ok(Some(entry.value)),
                WriteOperation::Delete => Ok(None),
                _ => Ok(None),
            };
        }
        
        if let Some(value) = shard.memtable.read().get(key) {
            return Ok(Some(value));
        }
        
        for memtable in shard.immutable_memtables.read().iter() {
            if let Some(value) = memtable.get(key) {
                return Ok(Some(value));
            }
        }
        
        for sstable in shard.sstables.read().iter().rev() {
            if key >= &sstable.min_key && key <= &sstable.max_key {
                if sstable.bloom_filter.contains(key) {
                    if let Some(value) = sstable.get(key, &*self.storage).await? {
                        shard.read_cache.put(key.to_vec(), value.clone());
                        return Ok(Some(value));
                    }
                }
            }
        }
        
        Ok(None)
    }

    pub async fn put(
        &self,
        shard_id: super::coordinator::ShardId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        self.metrics.writes_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes_written.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
        
        shard.write_ahead_log.append(&key, &value, WriteOperation::Put).await?;
        
        shard.write_buffer.put(key.clone(), value.clone())?;
        
        if shard.write_buffer.should_flush() {
            let memtable = shard.memtable.read();
            if memtable.size() > self.config.max_memtable_size {
                self.trigger_flush(shard_id).await?;
            }
        }
        
        shard.memtable.write().put(key.clone(), value);
        
        if let Some(bloom) = &mut shard.metadata.write().bloom_filter {
            bloom.add(&key);
        }
        
        shard.metadata.write().record_count.fetch_add(1, Ordering::Relaxed);
        
        Ok(())
    }

    pub async fn delete(&self, shard_id: super::coordinator::ShardId, key: &[u8]) -> Result<(), Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        self.metrics.deletes_total.fetch_add(1, Ordering::Relaxed);
        
        shard.write_ahead_log.append(key, &[], WriteOperation::Delete).await?;
        
        shard.write_buffer.delete(key.to_vec())?;
        
        shard.memtable.write().delete(key.to_vec());
        
        shard.metadata.write().statistics.tombstone_count += 1;
        
        Ok(())
    }

    pub async fn scan(
        &self,
        shard_id: super::coordinator::ShardId,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        let mut results = Vec::new();
        let mut count = 0;
        
        let memtable_results = shard.memtable.read().scan(start, end, limit);
        for (k, v) in memtable_results {
            if count >= limit {
                break;
            }
            results.push((k, v));
            count += 1;
        }
        
        if count < limit {
            for memtable in shard.immutable_memtables.read().iter() {
                let imm_results = memtable.scan(start, end, limit - count);
                for (k, v) in imm_results {
                    if count >= limit {
                        break;
                    }
                    results.push((k, v));
                    count += 1;
                }
            }
        }
        
        if count < limit {
            for sstable in shard.sstables.read().iter() {
                if sstable.overlaps(start, end) {
                    let sst_results = sstable.scan(start, end, limit - count, &*self.storage).await?;
                    for (k, v) in sst_results {
                        if count >= limit {
                            break;
                        }
                        results.push((k, v));
                        count += 1;
                    }
                }
            }
        }
        
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results.dedup_by(|a, b| a.0 == b.0);
        results.truncate(limit);
        
        Ok(results)
    }

    async fn trigger_flush(&self, shard_id: super::coordinator::ShardId) -> Result<(), Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        let memtable = shard.memtable.read().clone();
        let new_memtable = Arc::new(MemTable::new());
        
        *shard.memtable.write() = new_memtable;
        
        shard.immutable_memtables.write().push_back(Arc::new(memtable));
        
        while shard.immutable_memtables.read().len() > self.config.max_immutable_memtables {
            if let Some(oldest) = shard.immutable_memtables.write().pop_front() {
                self.flush_memtable(shard_id, oldest).await?;
            }
        }
        
        self.flush_scheduler.schedule(FlushJob {
            shard_id,
            memtable: shard.immutable_memtables.read().back().unwrap().clone(),
            priority: FlushPriority::Normal,
        });
        
        Ok(())
    }

    async fn flush_memtable(
        &self,
        shard_id: super::coordinator::ShardId,
        memtable: Arc<MemTable>,
    ) -> Result<(), Error> {
        let sstable = self.build_sstable(memtable).await?;
        
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        shard.sstables.write().push(Arc::new(sstable));
        
        self.metrics.flushes_total.fetch_add(1, Ordering::Relaxed);
        
        self.maybe_trigger_compaction(shard_id).await?;
        
        Ok(())
    }

    async fn build_sstable(&self, memtable: Arc<MemTable>) -> Result<SSTable, Error> {
        Ok(SSTable {
            id: rand::random(),
            level: 0,
            file_path: std::path::PathBuf::new(),
            size_bytes: memtable.size_bytes.load(Ordering::Relaxed),
            entry_count: memtable.entry_count.load(Ordering::Relaxed),
            min_key: memtable.min_key.read().clone().unwrap_or_default(),
            max_key: memtable.max_key.read().clone().unwrap_or_default(),
            bloom_filter: BloomFilter::new(
                memtable.entry_count.load(Ordering::Relaxed) as usize,
                0.01,
            ),
            index: SSTableIndex {
                entries: Vec::new(),
                sparse_index: BTreeMap::new(),
            },
            metadata: SSTableMetadata {
                created_at: chrono::Utc::now(),
                compression_type: self.config.compression_type,
                checksum: 0,
                properties: HashMap::new(),
            },
        })
    }

    async fn maybe_trigger_compaction(&self, shard_id: super::coordinator::ShardId) -> Result<(), Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        let level0_count = shard.sstables.read()
            .iter()
            .filter(|sst| sst.level == 0)
            .count();
        
        if level0_count > self.config.max_level0_files {
            self.compaction_scheduler.schedule(CompactionJob {
                shard_id,
                level: 0,
                input_files: Vec::new(),
                output_level: 1,
                priority: CompactionPriority::Normal,
                started_at: None,
            });
            
            self.metrics.compactions_total.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }

    pub async fn compact(&self, shard_id: super::coordinator::ShardId) -> Result<(), Error> {
        let shard = self.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        *shard.state.write() = ShardState::Compacting;
        
        *shard.state.write() = ShardState::Active;
        
        shard.metadata.write().last_compaction = Some(chrono::Utc::now());
        
        Ok(())
    }

    pub fn get_metrics(&self) -> ShardMetricsSnapshot {
        ShardMetricsSnapshot {
            reads_total: self.metrics.reads_total.load(Ordering::Relaxed),
            writes_total: self.metrics.writes_total.load(Ordering::Relaxed),
            deletes_total: self.metrics.deletes_total.load(Ordering::Relaxed),
            compactions_total: self.metrics.compactions_total.load(Ordering::Relaxed),
            flushes_total: self.metrics.flushes_total.load(Ordering::Relaxed),
            bytes_read: self.metrics.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.metrics.bytes_written.load(Ordering::Relaxed),
            cache_hits: self.metrics.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.metrics.cache_misses.load(Ordering::Relaxed),
            bloom_filter_useful: self.metrics.bloom_filter_useful.load(Ordering::Relaxed),
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.compaction_scheduler.shutdown().await;
        self.flush_scheduler.shutdown().await;
    }
}

#[derive(Debug, Clone)]
pub struct ShardMetricsSnapshot {
    pub reads_total: u64,
    pub writes_total: u64,
    pub deletes_total: u64,
    pub compactions_total: u64,
    pub flushes_total: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bloom_filter_useful: u64,
}

impl Shard {
    async fn new(metadata: ShardMetadata, config: Arc<ShardConfig>) -> Result<Self, Error> {
        Ok(Self {
            metadata: Arc::new(RwLock::new(metadata)),
            state: Arc::new(RwLock::new(ShardState::Initializing)),
            memtable: Arc::new(RwLock::new(MemTable::new())),
            immutable_memtables: Arc::new(RwLock::new(VecDeque::new())),
            sstables: Arc::new(RwLock::new(Vec::new())),
            write_ahead_log: Arc::new(WriteAheadLog::new(config.sync_writes)?),
            read_cache: Arc::new(ReadCache::new(config.cache_size)),
            write_buffer: Arc::new(WriteBuffer::new(config.write_buffer_size)),
            compaction_filter: None,
        })
    }
}

impl MemTable {
    fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(skiplist::SkipList::new())),
            size_bytes: AtomicUsize::new(0),
            entry_count: AtomicU64::new(0),
            min_key: Arc::new(RwLock::new(None)),
            max_key: Arc::new(RwLock::new(None)),
            created_at: Instant::now(),
            sequence_number: AtomicU64::new(0),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.data.read().get(key).cloned()
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let size = key.len() + value.len();
        self.size_bytes.fetch_add(size, Ordering::Relaxed);
        self.entry_count.fetch_add(1, Ordering::Relaxed);
        
        self.update_key_range(&key);
        
        self.data.write().insert(key, value);
    }

    fn delete(&mut self, key: Vec<u8>) {
        self.data.write().remove(&key);
    }

    fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.data.read();
        let mut results = Vec::new();
        let mut count = 0;
        
        for (k, v) in data.iter() {
            if k >= start && k <= end {
                if count >= limit {
                    break;
                }
                results.push((k.clone(), v.clone()));
                count += 1;
            }
        }
        
        results
    }

    fn size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    fn update_key_range(&self, key: &[u8]) {
        let mut min = self.min_key.write();
        let mut max = self.max_key.write();
        
        if min.is_none() || key < min.as_ref().unwrap() {
            *min = Some(key.to_vec());
        }
        
        if max.is_none() || key > max.as_ref().unwrap() {
            *max = Some(key.to_vec());
        }
    }
}

impl SSTable {
    async fn get(&self, key: &[u8], storage: &dyn PageManagerAsync) -> Result<Option<Vec<u8>>, Error> {
        Ok(None)
    }

    async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        storage: &dyn PageManagerAsync,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        Ok(Vec::new())
    }

    fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        !(end < &self.min_key || start > &self.max_key)
    }
}

impl WriteAheadLog {
    fn new(sync_on_write: bool) -> Result<Self, Error> {
        Ok(Self {
            current_file: Arc::new(RwLock::new(std::fs::File::create("/tmp/wal.log")?)),
            log_sequence_number: AtomicU64::new(0),
            buffer: Arc::new(RwLock::new(BytesMut::new())),
            sync_on_write,
        })
    }

    async fn append(&self, key: &[u8], value: &[u8], op: WriteOperation) -> Result<(), Error> {
        let lsn = self.log_sequence_number.fetch_add(1, Ordering::SeqCst);
        
        let mut buffer = self.buffer.write();
        buffer.put_u64(lsn);
        buffer.put_u8(op as u8);
        buffer.put_u32(key.len() as u32);
        buffer.put(key);
        buffer.put_u32(value.len() as u32);
        buffer.put(value);
        
        if self.sync_on_write {
            use std::io::Write;
            let mut file = self.current_file.write();
            file.write_all(&buffer)?;
            file.sync_all()?;
            buffer.clear();
        }
        
        Ok(())
    }
}

impl ReadCache {
    fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(capacity / 1024).unwrap()
            )),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.cache.get(&key.to_vec()).map(|v| v.as_ref().clone())
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        self.cache.put(key, Arc::new(value));
    }
}

impl WriteBuffer {
    fn new(max_size: usize) -> Self {
        Self {
            buffer: Arc::new(RwLock::new(HashMap::new())),
            size_bytes: AtomicUsize::new(0),
            max_size,
        }
    }

    fn get(&self, key: &[u8]) -> Option<WriteEntry> {
        self.buffer.read().get(key).cloned()
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Error> {
        let size = key.len() + value.len();
        
        if self.size_bytes.load(Ordering::Relaxed) + size > self.max_size {
            return Err(Error::BufferFull);
        }
        
        self.buffer.write().insert(key, WriteEntry {
            value,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            operation: WriteOperation::Put,
        });
        
        self.size_bytes.fetch_add(size, Ordering::Relaxed);
        
        Ok(())
    }

    fn delete(&self, key: Vec<u8>) -> Result<(), Error> {
        self.buffer.write().insert(key, WriteEntry {
            value: Vec::new(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            operation: WriteOperation::Delete,
        });
        
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.size_bytes.load(Ordering::Relaxed) >= self.max_size * 80 / 100
    }
}

impl WriteBufferPool {
    fn new(count: usize, total_memory: usize) -> Self {
        let buffer_size = total_memory / count;
        let mut buffers = Vec::new();
        
        for _ in 0..count {
            buffers.push(Arc::new(WriteBuffer::new(buffer_size)));
        }
        
        Self {
            buffers: Arc::new(RwLock::new(buffers)),
            available: Arc::new(Semaphore::new(count)),
            total_memory: AtomicUsize::new(total_memory),
            max_memory: total_memory,
        }
    }
}

impl CompactionScheduler {
    fn new(worker_threads: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(VecDeque::new())),
            active: Arc::new(RwLock::new(HashMap::new())),
            worker_threads,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn schedule(&self, job: CompactionJob) {
        self.pending.write().push_back(job);
    }

    async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl FlushScheduler {
    fn new(flush_interval: Duration) -> Self {
        Self {
            pending: Arc::new(RwLock::new(VecDeque::new())),
            active: Arc::new(RwLock::new(HashMap::new())),
            flush_interval,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn schedule(&self, job: FlushJob) {
        self.pending.write().push_back(job);
    }

    async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

mod skiplist {
    use std::collections::BTreeMap;
    
    pub struct SkipList<K: Ord, V> {
        map: BTreeMap<K, V>,
    }
    
    impl<K: Ord + Clone, V: Clone> SkipList<K, V> {
        pub fn new() -> Self {
            Self {
                map: BTreeMap::new(),
            }
        }
        
        pub fn insert(&mut self, key: K, value: V) {
            self.map.insert(key, value);
        }
        
        pub fn get(&self, key: &K) -> Option<&V> {
            self.map.get(key)
        }
        
        pub fn remove(&mut self, key: &K) -> Option<V> {
            self.map.remove(key)
        }
        
        pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
            self.map.iter()
        }
    }
}
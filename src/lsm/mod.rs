pub mod background_compaction;
pub mod compaction;
pub mod delta_compression;
pub mod memtable;
pub mod sstable;
pub mod parallel_compaction;
mod iterator;

use crate::compression::CompressionType;
use crate::error::Result;
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub use compaction::{CompactionStrategy, Compactor, LeveledCompaction};
pub use delta_compression::{
    CompressionLevel, DeltaCompressedData, DeltaCompressionConfig, DeltaCompressor, DeltaType,
};
pub use memtable::MemTable;
pub use sstable::{SSTable, SSTableBuilder, SSTableReader};
pub use iterator::{LSMFullIterator, LSMMemTableIterator};
pub use parallel_compaction::{ParallelCompactionCoordinator, CompactionScheduler, CompactionStats, ParallelCompactionStats, CompactionState};

#[derive(Debug, Clone)]
pub struct LSMConfig {
    pub memtable_size: usize,           // Size in bytes before flush
    pub level0_file_num_trigger: usize, // Number of L0 files to trigger compaction
    pub max_levels: usize,              // Maximum number of levels
    pub level_size_multiplier: usize,   // Size multiplier between levels
    pub bloom_filter_bits_per_key: usize,
    pub compression_type: CompressionType,
    pub block_size: usize, // Size of data blocks in SSTable
    pub delta_compression_config: DeltaCompressionConfig,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024, // 4MB
            level0_file_num_trigger: 4,
            max_levels: 7,
            level_size_multiplier: 10,
            bloom_filter_bits_per_key: 10,
            compression_type: CompressionType::Zstd,
            block_size: 4096,
            delta_compression_config: DeltaCompressionConfig::default(),
        }
    }
}

pub struct LSMTree {
    memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
    levels: Arc<RwLock<Vec<Level>>>,
    config: LSMConfig,
    db_path: PathBuf,
    next_file_number: Arc<AtomicU64>,
    compaction_thread: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicUsize>,
    read_cache: Arc<RwLock<LruCache<Vec<u8>, Vec<u8>>>>,
    cache_hits: Arc<AtomicUsize>,
    cache_misses: Arc<AtomicUsize>,
    delta_compressor: Arc<RwLock<DeltaCompressor>>,
    parallel_compaction: Option<Arc<ParallelCompactionCoordinator>>,
    compaction_scheduler: Option<Arc<CompactionScheduler>>,
}

pub struct Level {
    level_num: usize,
    sstables: Vec<Arc<SSTable>>,
    size_bytes: usize,
    max_size_bytes: usize,
}

impl Level {
    fn new(level_num: usize, max_size_bytes: usize) -> Self {
        Self {
            level_num,
            sstables: Vec::new(),
            size_bytes: 0,
            max_size_bytes,
        }
    }
    
    pub fn total_size(&self) -> u64 {
        self.size_bytes as u64
    }
    
    pub fn tables(&self) -> &[Arc<SSTable>] {
        &self.sstables
    }

    fn add_sstable(&mut self, sstable: Arc<SSTable>) {
        self.size_bytes += sstable.size_bytes();
        self.sstables.push(sstable);
        // Keep sorted by min key
        self.sstables.sort_by(|a, b| a.min_key().cmp(b.min_key()));
    }



    fn overlapping_sstables(&self, min_key: &[u8], max_key: &[u8]) -> Vec<Arc<SSTable>> {
        self.sstables
            .iter()
            .filter(|sst| !(sst.max_key() < min_key || sst.min_key() > max_key))
            .cloned()
            .collect()
    }
}

impl LSMTree {
    /// Get reference to the current memtable
    pub fn get_memtable(&self) -> Arc<RwLock<MemTable>> {
        Arc::clone(&self.memtable)
    }
    const TOMBSTONE_MARKER: &'static [u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

    fn is_tombstone(value: &[u8]) -> bool {
        value == Self::TOMBSTONE_MARKER
    }

    pub fn new<P: AsRef<Path>>(db_path: P, config: LSMConfig) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_path)?;

        let memtable = Arc::new(RwLock::new(MemTable::new()));
        let immutable_memtables = Arc::new(RwLock::new(Vec::new()));

        // Initialize levels
        let mut levels = Vec::new();
        let mut level_size = 10 * 1024 * 1024; // L1: 10MB

        for i in 0..config.max_levels {
            levels.push(Level::new(i, level_size));
            level_size *= config.level_size_multiplier;
        }

        let levels = Arc::new(RwLock::new(levels));
        let shutdown = Arc::new(AtomicUsize::new(0));

        // Create read cache with 10K entries
        let cache_size = NonZeroUsize::new(10000).unwrap();
        let read_cache = Arc::new(RwLock::new(LruCache::new(cache_size)));

        // Initialize delta compressor
        let delta_compressor = Arc::new(RwLock::new(DeltaCompressor::new(
            config.delta_compression_config.clone(),
        )));

        // Initialize parallel compaction with 4 workers
        let parallel_compaction = ParallelCompactionCoordinator::new(4);
        let compaction_scheduler = Arc::new(CompactionScheduler::new(
            Arc::clone(&parallel_compaction),
            Arc::clone(&levels),
        ));

        let mut lsm = Self {
            memtable,
            immutable_memtables,
            levels,
            config,
            db_path,
            next_file_number: Arc::new(AtomicU64::new(1)),
            compaction_thread: None,
            shutdown,
            read_cache,
            cache_hits: Arc::new(AtomicUsize::new(0)),
            cache_misses: Arc::new(AtomicUsize::new(0)),
            delta_compressor,
            parallel_compaction: Some(parallel_compaction),
            compaction_scheduler: Some(compaction_scheduler),
        };

        // Start background compaction thread
        lsm.start_compaction_thread();

        Ok(lsm)
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Remove from cache to ensure fresh reads
        self.read_cache.write().pop(&key);

        let memtable = self.memtable.read();
        memtable.insert(key.clone(), value.clone());

        // Check if memtable needs to be flushed
        if memtable.size_bytes() > self.config.memtable_size {
            drop(memtable);
            self.rotate_memtable()?;
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check read cache first - need write lock for LRU update
        let cache_result = {
            let mut cache = self.read_cache.write();
            cache.get(&key.to_vec()).cloned()
        };

        if let Some(value) = cache_result {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            // Check if cached value is a tombstone
            if Self::is_tombstone(&value) {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Check active memtable
        let memtable = self.memtable.read();
        if let Some(value) = memtable.get(key) {
            // Check if this is a tombstone
            if Self::is_tombstone(&value) {
                // Update cache with tombstone
                self.read_cache.write().put(key.to_vec(), value);
                return Ok(None);
            }
            // Update cache
            self.read_cache.write().put(key.to_vec(), value.clone());
            return Ok(Some(value));
        }
        drop(memtable);

        // Check immutable memtables
        let immutable = self.immutable_memtables.read();
        for memtable in immutable.iter().rev() {
            if let Some(value) = memtable.get(key) {
                // Check if this is a tombstone
                if Self::is_tombstone(&value) {
                    // Update cache with tombstone
                    self.read_cache.write().put(key.to_vec(), value);
                    return Ok(None);
                }
                // Update cache
                self.read_cache.write().put(key.to_vec(), value.clone());
                return Ok(Some(value));
            }
        }
        drop(immutable);

        // Check SSTables level by level
        let levels = self.levels.read();
        for level in &*levels {
            // Use bloom filters to skip SSTables
            let mut candidates = Vec::new();
            for sstable in &level.sstables {
                if key >= sstable.min_key() && key <= sstable.max_key() {
                    if sstable.might_contain(key) {
                        candidates.push(sstable.clone());
                    }
                }
            }

            // Binary search within candidates (they're sorted)
            for sstable in candidates {
                if let Some(value) = sstable.get(key)? {
                    // Check if this is a tombstone
                    if Self::is_tombstone(&value) {
                        // Update cache with tombstone
                        self.read_cache.write().put(key.to_vec(), value);
                        return Ok(None);
                    }
                    // Update cache
                    self.read_cache.write().put(key.to_vec(), value.clone());
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        // Remove from cache first
        self.read_cache.write().pop(&key.to_vec());

        // Insert tombstone marker
        self.insert(key.to_vec(), Self::TOMBSTONE_MARKER.to_vec())
    }

    fn rotate_memtable(&self) -> Result<()> {
        let mut memtable = self.memtable.write();
        let old_memtable = std::mem::replace(&mut *memtable, MemTable::new());

        let old_memtable = Arc::new(old_memtable);
        self.immutable_memtables.write().push(old_memtable.clone());

        // Trigger flush in background
        self.schedule_flush(old_memtable)?;

        Ok(())
    }

    fn schedule_flush(&self, memtable: Arc<MemTable>) -> Result<()> {
        let file_number = self.next_file_number.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.db_path.join(format!("{:06}.sst", file_number));

        // Build SSTable from memtable
        let mut builder = SSTableBuilder::new(
            &sstable_path,
            self.config.block_size,
            self.config.compression_type.clone(),
            self.config.bloom_filter_bits_per_key,
        )?
        .with_delta_compression(self.config.delta_compression_config.clone());

        // Write all entries
        for entry in memtable.iter() {
            builder.add(entry.key(), entry.value())?;
        }

        let sstable = Arc::new(builder.finish()?);

        // Add to level 0
        self.levels.write()[0].add_sstable(sstable);

        // Remove from immutable list
        self.immutable_memtables
            .write()
            .retain(|m| !Arc::ptr_eq(m, &memtable));

        // Check if compaction is needed
        self.maybe_schedule_compaction();

        Ok(())
    }

    fn maybe_schedule_compaction(&self) {
        let levels = self.levels.read();

        // Check L0 compaction trigger
        if levels[0].sstables.len() >= self.config.level0_file_num_trigger {
            // Signal compaction thread
            drop(levels);
            // Compaction will be handled by background thread
        }
    }

    fn start_compaction_thread(&mut self) {
        let shutdown = self.shutdown.clone();
        let compaction_scheduler = self.compaction_scheduler.clone();

        if let Some(scheduler) = compaction_scheduler {
            self.compaction_thread = Some(std::thread::spawn(move || {
                while shutdown.load(Ordering::Relaxed) == 0 {
                    std::thread::sleep(std::time::Duration::from_secs(1));

                    // Use parallel compaction scheduler
                    let job_ids = scheduler.maybe_schedule_compaction();
                    
                    if !job_ids.is_empty() {
                        tracing::info!("Scheduled {} parallel compaction jobs", job_ids.len());
                    }
                }
            }));
        }
    }


    pub fn flush(&self) -> Result<()> {
        // Force flush of current memtable
        self.rotate_memtable()?;

        // Wait for all immutable memtables to be flushed
        while !self.immutable_memtables.read().is_empty() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(())
    }

    pub fn compact_all(&self) -> Result<()> {
        // Force full compaction
        self.flush()?;

        // Wait for compactions to complete
        loop {
            let levels = self.levels.read();
            let total_l0 = levels[0].sstables.len();
            drop(levels);

            if total_l0 == 0 {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        Ok(())
    }

    pub fn set_delta_reference(&self, reference_sstable: &SSTable) -> Result<()> {
        let mut compressor = self.delta_compressor.write();
        compressor.set_reference(reference_sstable)
    }

    pub fn compress_with_delta(&self, data: &[u8], sstable_id: u64) -> Result<DeltaCompressedData> {
        let compressor = self.delta_compressor.read();
        compressor.compress(data, sstable_id)
    }

    pub fn get_delta_compression_stats(&self) -> DeltaCompressionStats {
        let levels = self.levels.read();
        let mut total_compressed_size = 0;
        let mut total_original_size = 0;
        let mut compressed_sstables = 0;

        for level in levels.iter() {
            for sstable in &level.sstables {
                // In a real implementation, SSTables would track compression info
                total_original_size += sstable.size_bytes();
                total_compressed_size += sstable.size_bytes(); // Placeholder
                compressed_sstables += 1;
            }
        }

        let compression_ratio = if total_original_size > 0 {
            total_compressed_size as f64 / total_original_size as f64
        } else {
            1.0
        };

        DeltaCompressionStats {
            total_compressed_size,
            total_original_size,
            compression_ratio,
            compressed_sstables,
            enabled: self.config.delta_compression_config.enabled,
        }
    }

    pub fn stats(&self) -> LSMStats {
        let levels = self.levels.read();
        let level_stats: Vec<LevelStats> = levels
            .iter()
            .map(|level| LevelStats {
                level: level.level_num,
                num_files: level.sstables.len(),
                size_bytes: level.size_bytes,
            })
            .collect();

        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_reads = cache_hits + cache_misses;
        let cache_hit_rate = if total_reads > 0 {
            (cache_hits as f64 / total_reads as f64) * 100.0
        } else {
            0.0
        };

        LSMStats {
            memtable_size: self.memtable.read().size_bytes(),
            immutable_count: self.immutable_memtables.read().len(),
            levels: level_stats,
            cache_hits,
            cache_misses,
            cache_hit_rate,
        }
    }

    pub fn parallel_compaction_stats(&self) -> Option<ParallelCompactionStats> {
        self.parallel_compaction.as_ref().map(|pc| pc.stats())
    }
}

impl Drop for LSMTree {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(1, Ordering::Relaxed);

        // Wait for compaction thread
        if let Some(thread) = self.compaction_thread.take() {
            let _ = thread.join();
        }

        // Flush remaining data
        let _ = self.flush();
    }
}

#[derive(Debug, Clone)]
pub struct LSMStats {
    pub memtable_size: usize,
    pub immutable_count: usize,
    pub levels: Vec<LevelStats>,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Clone)]
pub struct LevelStats {
    pub level: usize,
    pub num_files: usize,
    pub size_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct DeltaCompressionStats {
    pub total_compressed_size: usize,
    pub total_original_size: usize,
    pub compression_ratio: f64,
    pub compressed_sstables: usize,
    pub enabled: bool,
}

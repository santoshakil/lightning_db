pub mod background_compaction;
pub mod compaction;
pub mod delta_compression;
mod iterator;
mod iterator_fixed;
pub mod memtable;
pub mod parallel_compaction;
pub mod sstable;

use crate::core::error::{Error, Result};
use crate::features::adaptive_compression::CompressionType;
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
pub use iterator::{LSMFullIterator, LSMMemTableIterator};
pub use iterator_fixed::LSMFullIteratorFixed;
pub use memtable::MemTable;
pub use parallel_compaction::{
    CompactionScheduler, CompactionState, CompactionStats, ParallelCompactionCoordinator,
    ParallelCompactionStats,
};
pub use sstable::{SSTable, SSTableBuilder, SSTableReader};

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
            #[cfg(feature = "zstd-compression")]
            compression_type: CompressionType::Zstd,
            #[cfg(not(feature = "zstd-compression"))]
            compression_type: CompressionType::Snappy,
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

impl std::fmt::Debug for LSMTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LSMTree")
            .field("config", &self.config)
            .field("db_path", &self.db_path)
            .field(
                "next_file_number",
                &self
                    .next_file_number
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .field("compaction_thread", &self.compaction_thread.is_some())
            .field(
                "shutdown",
                &self.shutdown.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "cache_hits",
                &self.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "cache_misses",
                &self.cache_misses.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
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

    pub fn get_immutable_memtables(&self) -> Arc<RwLock<Vec<Arc<MemTable>>>> {
        Arc::clone(&self.immutable_memtables)
    }

    pub fn get_levels(&self) -> Arc<RwLock<Vec<Level>>> {
        Arc::clone(&self.levels)
    }

    const TOMBSTONE_MARKER: &'static [u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

    pub fn is_tombstone(value: &[u8]) -> bool {
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
        let cache_size = NonZeroUsize::new(10000)
            .ok_or_else(|| Error::Generic("Invalid cache size".to_string()))?;
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

        // Load existing SSTable files
        lsm.recover_sstables()?;

        // Start background compaction thread
        lsm.start_compaction_thread();

        Ok(lsm)
    }

    fn recover_sstables(&mut self) -> Result<()> {
        eprintln!("LSM: Recovering existing SSTable files...");

        // Check if LSM directory exists
        if !self.db_path.exists() {
            eprintln!("LSM: No existing LSM directory found");
            return Ok(());
        }

        // Scan for .sst files
        let mut sst_files = Vec::new();
        for entry in std::fs::read_dir(&self.db_path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "sst" {
                    sst_files.push(path);
                }
            }
        }

        sst_files.sort(); // Sort by filename to maintain order

        eprintln!("LSM: Found {} SSTable files to recover", sst_files.len());

        // Load each SSTable file
        let mut levels = self.levels.write();
        for sst_path in sst_files {
            eprintln!("LSM: Loading SSTable: {:?}", sst_path);
            match SSTableReader::open(&sst_path) {
                Ok(sstable) => {
                    // Find the highest file number for next_file_number
                    if let Some(file_name) = sst_path.file_stem() {
                        if let Some(file_str) = file_name.to_str() {
                            if let Ok(file_num) = file_str.parse::<u64>() {
                                let current = self.next_file_number.load(Ordering::SeqCst);
                                if file_num >= current {
                                    self.next_file_number.store(file_num + 1, Ordering::SeqCst);
                                }
                            }
                        }
                    }

                    // Add to level 0 (we'll let compaction reorganize later)
                    levels[0].add_sstable(Arc::new(sstable));
                }
                Err(e) => {
                    eprintln!("LSM: Failed to load SSTable {:?}: {}", sst_path, e);
                }
            }
        }

        eprintln!("LSM: Recovery complete");
        Ok(())
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Remove from cache to ensure fresh reads
        self.read_cache.write().pop(&key);

        // CRITICAL FIX: Atomic rotation with proper synchronization
        loop {
            {
                let memtable = self.memtable.read();

                // Fast path: if memtable has space, insert directly
                if memtable.size_bytes() + key.len() + value.len() + 32 <= self.config.memtable_size
                {
                    memtable.insert(key, value);
                    return Ok(());
                }
            }

            // Slow path: need to rotate memtable
            // Use compare-and-swap pattern to ensure only one rotation happens
            let rotation_result = {
                let mut memtable_guard = self.memtable.write();

                // Double-check after acquiring write lock
                if memtable_guard.size_bytes() + key.len() + value.len() + 32
                    <= self.config.memtable_size
                {
                    memtable_guard.insert(key, value);
                    return Ok(());
                }

                // Perform atomic rotation
                self.rotate_memtable_atomic(&mut memtable_guard)
            };

            rotation_result?;
            // Retry insertion with new memtable
        }
    }

    /// Insert without WAL logging - used by auto batcher for optimized batch writes
    pub fn insert_no_wal(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Same as insert but called separately to indicate no WAL logging needed
        // The actual WAL logging is handled by the caller (auto batcher)
        self.insert(key, value)
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
                self.read_cache.write().put(key.to_vec(), value.clone());
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
                    self.read_cache.write().put(key.to_vec(), value.clone());
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
            // Use bloom filters to skip SSTables - early termination optimization
            for sstable in &level.sstables {
                // Quick bounds check first
                if key < sstable.min_key() || key > sstable.max_key() {
                    continue;
                }

                // Bloom filter check to avoid disk I/O
                if !sstable.might_contain(key) {
                    continue;
                }

                // Actually try to get the value
                if let Some(value) = sstable.get(key)? {
                    // Check if this is a tombstone
                    if Self::is_tombstone(&value) {
                        // Update cache with tombstone
                        self.read_cache.write().put(key.to_vec(), value);
                        return Ok(None);
                    }
                    // Update cache and return immediately (early termination)
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

    /// Atomic memtable rotation - must be called with write lock held
    fn rotate_memtable_atomic(
        &self,
        memtable_guard: &mut parking_lot::RwLockWriteGuard<MemTable>,
    ) -> Result<()> {
        // Create new memtable while holding the write lock
        let old_memtable = std::mem::replace(&mut **memtable_guard, MemTable::new());

        // Only add to immutable list if memtable has data
        if !old_memtable.is_empty() {
            let old_memtable = Arc::new(old_memtable);

            // Add to immutable list
            self.immutable_memtables.write().push(old_memtable.clone());

            // Schedule background flush
            self.schedule_flush(old_memtable)?;
        }

        Ok(())
    }

    fn _rotate_memtable(&self) -> Result<()> {
        let mut memtable_guard = self.memtable.write();
        self.rotate_memtable_atomic(&mut memtable_guard)
    }

    fn schedule_flush(&self, memtable: Arc<MemTable>) -> Result<()> {
        let file_number = self.next_file_number.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.db_path.join(format!("{:06}.sst", file_number));

        tracing::info!("LSM schedule_flush: creating SSTable at {:?}", sstable_path);

        // Ensure directory exists
        if let Some(parent) = sstable_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(format!("Failed to create directory: {}", e)))?;
        }

        // Build SSTable from memtable with error recovery
        let mut builder = match SSTableBuilder::new(
            &sstable_path,
            self.config.block_size,
            self.config.compression_type,
            self.config.bloom_filter_bits_per_key,
        ) {
            Ok(b) => b.with_delta_compression(self.config.delta_compression_config.clone()),
            Err(e) => {
                tracing::error!("Failed to create SSTable builder: {}", e);
                // Clean up immutable memtable to prevent memory leak
                self.immutable_memtables
                    .write()
                    .retain(|m| !Arc::ptr_eq(m, &memtable));
                return Err(e);
            }
        };

        // Write all entries with error handling
        let mut entry_count = 0;
        let mut error_count = 0;

        for entry in memtable.iter() {
            match builder.add(entry.key(), entry.value()) {
                Ok(()) => entry_count += 1,
                Err(e) => {
                    error_count += 1;
                    tracing::warn!("Failed to add entry to SSTable: {}", e);
                    if error_count > 10 {
                        tracing::error!("Too many errors during SSTable building, aborting");
                        // Clean up partial file
                        let _ = std::fs::remove_file(&sstable_path);
                        self.immutable_memtables
                            .write()
                            .retain(|m| !Arc::ptr_eq(m, &memtable));
                        return Err(Error::Generic("SSTable building failed".to_string()));
                    }
                }
            }
        }

        tracing::info!(
            "LSM schedule_flush: writing {} entries ({} errors)",
            entry_count,
            error_count
        );

        // Safety check: only create SSTable if we have entries
        if entry_count == 0 {
            tracing::info!("LSM schedule_flush: empty memtable, skipping SSTable creation");
            // Clean up empty file if created
            let _ = std::fs::remove_file(&sstable_path);
            // Remove empty memtable
            self.immutable_memtables
                .write()
                .retain(|m| !Arc::ptr_eq(m, &memtable));
            return Ok(());
        }

        // Finish SSTable with error recovery
        let sstable = match builder.finish() {
            Ok(sst) => Arc::new(sst),
            Err(e) => {
                tracing::error!("Failed to finish SSTable: {}", e);
                // Clean up partial file
                let _ = std::fs::remove_file(&sstable_path);
                self.immutable_memtables
                    .write()
                    .retain(|m| !Arc::ptr_eq(m, &memtable));
                return Err(e);
            }
        };

        // Atomically add to level 0
        {
            let mut levels_guard = self.levels.write();
            if levels_guard.is_empty() {
                return Err(Error::Generic("No levels initialized".to_string()));
            }
            levels_guard[0].add_sstable(sstable);
        }

        // Remove from immutable list only after successful SSTable creation
        self.immutable_memtables
            .write()
            .retain(|m| !Arc::ptr_eq(m, &memtable));

        // Check if compaction is needed
        self.maybe_schedule_compaction();

        tracing::info!(
            "LSM schedule_flush: successfully flushed {} entries to {:?}",
            entry_count,
            sstable_path
        );
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
        eprintln!("LSM flush: rotating memtable");

        // First, rotate the current memtable but don't flush it yet
        let mut memtable = self.memtable.write();
        let old_memtable = std::mem::take(&mut *memtable);
        drop(memtable);

        let old_memtable = Arc::new(old_memtable);
        self.immutable_memtables.write().push(old_memtable);

        // Now flush ALL immutable memtables synchronously
        loop {
            let memtable_to_flush = {
                let mut immutable = self.immutable_memtables.write();
                immutable.pop()
            };

            match memtable_to_flush {
                Some(memtable) => {
                    eprintln!(
                        "LSM flush: flushing immutable memtable with {} entries",
                        memtable.iter().count()
                    );
                    self.schedule_flush(memtable)?;
                }
                None => break,
            }
        }

        eprintln!("LSM flush: all memtables flushed");
        Ok(())
    }

    pub fn compact_all(&self) -> Result<()> {
        // Force full compaction
        self.flush()?;

        // Wait for compactions to complete with timeout
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);

        loop {
            let levels = self.levels.read();
            let total_l0 = levels[0].sstables.len();
            drop(levels);

            if total_l0 == 0 {
                break;
            }

            if start.elapsed() > timeout {
                eprintln!("LSM compact_all: timeout waiting for compaction to complete");
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

    /// Calculate write amplification metrics for monitoring
    pub fn get_write_amplification_stats(&self) -> WriteAmplificationStats {
        let levels = self.levels.read();
        let mut total_written = 0;
        let mut total_user_data = 0;
        let mut level_writes = Vec::new();

        for level in levels.iter() {
            let level_size: usize = level.sstables.iter().map(|sst| sst.size_bytes()).sum();
            total_written += level_size;

            // Estimate user data (accounting for metadata overhead)
            let estimated_user_data = (level_size as f64 * 0.85) as usize; // 15% overhead
            total_user_data += estimated_user_data;

            level_writes.push(WriteAmplificationLevelStats {
                level: level.level_num,
                bytes_written: level_size,
                compaction_count: 0, // Would be tracked in real implementation
                avg_file_size: if level.sstables.is_empty() {
                    0
                } else {
                    level_size / level.sstables.len()
                },
            });
        }

        let write_amplification = if total_user_data > 0 {
            total_written as f64 / total_user_data as f64
        } else {
            1.0
        };

        WriteAmplificationStats {
            total_bytes_written: total_written,
            total_user_bytes: total_user_data,
            write_amplification_factor: write_amplification,
            level_stats: level_writes,
            compaction_overhead_bytes: total_written.saturating_sub(total_user_data),
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

    /// Validate LSM tree invariants - critical for data integrity
    pub fn validate_invariants(&self) -> Result<LSMValidationReport> {
        let mut report = LSMValidationReport::new();
        let levels = self.levels.read();

        // Validate level structure
        for (level_idx, level) in levels.iter().enumerate() {
            // Check level number consistency
            if level.level_num != level_idx {
                report.add_error(format!(
                    "Level {} has incorrect level_num {}",
                    level_idx, level.level_num
                ));
            }

            // Validate SSTable ordering within level
            if level_idx > 0 {
                // Level 0 can have overlapping files
                let mut prev_max_key: Option<&[u8]> = None;
                for sstable in &level.sstables {
                    if let Some(prev_key) = prev_max_key {
                        if sstable.min_key() <= prev_key {
                            report.add_error(format!(
                                "Level {} SSTable {} overlaps with previous SSTable",
                                level_idx,
                                sstable.id()
                            ));
                        }
                    }
                    prev_max_key = Some(sstable.max_key());
                }
            }

            // Check size constraints
            if level.size_bytes > level.max_size_bytes && level_idx > 0 {
                report.add_warning(format!(
                    "Level {} exceeds size limit: {} > {}",
                    level_idx, level.size_bytes, level.max_size_bytes
                ));
            }

            // Validate individual SSTables
            for sstable in &level.sstables {
                if sstable.min_key() > sstable.max_key() {
                    report.add_error(format!("SSTable {} has invalid key range", sstable.id()));
                }

                if sstable.size_bytes() == 0 {
                    report.add_warning(format!("SSTable {} has zero size", sstable.id()));
                }

                // Verify file exists
                if !sstable.path().exists() {
                    report.add_error(format!(
                        "SSTable {} file missing: {:?}",
                        sstable.id(),
                        sstable.path()
                    ));
                }
            }
        }

        // Validate memtables
        let memtable_size = self.memtable.read().size_bytes();
        if memtable_size > self.config.memtable_size * 2 {
            report.add_warning(format!(
                "Active memtable size {} exceeds threshold by 2x",
                memtable_size
            ));
        }

        let immutable_count = self.immutable_memtables.read().len();
        if immutable_count > 10 {
            report.add_warning(format!("Too many immutable memtables: {}", immutable_count));
        }

        report.mark_complete();
        Ok(report)
    }

    /// Check and repair any inconsistencies found
    pub fn repair_inconsistencies(&self) -> Result<usize> {
        let mut repairs = 0;

        // Remove invalid SSTable references
        {
            let mut levels = self.levels.write();
            for level in levels.iter_mut() {
                let original_count = level.sstables.len();
                level.sstables.retain(|sst| sst.path().exists());
                let removed = original_count - level.sstables.len();
                if removed > 0 {
                    tracing::warn!(
                        "Removed {} invalid SSTable references from level {}",
                        removed,
                        level.level_num
                    );
                    repairs += removed;
                }

                // Recalculate level size
                level.size_bytes = level.sstables.iter().map(|sst| sst.size_bytes()).sum();
            }
        }

        // Force flush if too many immutable memtables
        let immutable_count = self.immutable_memtables.read().len();
        if immutable_count > 5 {
            tracing::info!("Force flushing {} immutable memtables", immutable_count);
            self.flush()?;
            repairs += immutable_count;
        }

        Ok(repairs)
    }
}

impl Drop for LSMTree {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(1, Ordering::Relaxed);

        // Wait for compaction thread with timeout
        if let Some(thread) = self.compaction_thread.take() {
            // Give thread a moment to exit cleanly
            std::thread::sleep(std::time::Duration::from_millis(100));

            // Note: We can't use a timeout on join() in stable Rust,
            // but the thread should exit quickly after shutdown flag is set
            let _ = thread.join();
        }

        // Flush remaining data (ignore errors during shutdown)
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

#[derive(Debug, Clone)]
pub struct LSMValidationReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub is_valid: bool,
    pub completed: bool,
}

impl LSMValidationReport {
    fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            is_valid: true,
            completed: false,
        }
    }

    fn add_error(&mut self, error: String) {
        self.errors.push(error);
        self.is_valid = false;
    }

    fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    fn mark_complete(&mut self) {
        self.completed = true;
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn summary(&self) -> String {
        format!(
            "LSM Validation: {} errors, {} warnings",
            self.errors.len(),
            self.warnings.len()
        )
    }
}

#[derive(Debug, Clone)]
pub struct WriteAmplificationStats {
    pub total_bytes_written: usize,
    pub total_user_bytes: usize,
    pub write_amplification_factor: f64,
    pub level_stats: Vec<WriteAmplificationLevelStats>,
    pub compaction_overhead_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct WriteAmplificationLevelStats {
    pub level: usize,
    pub bytes_written: usize,
    pub compaction_count: usize,
    pub avg_file_size: usize,
}

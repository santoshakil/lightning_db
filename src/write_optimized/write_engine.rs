//! Write-Optimized Storage Engine
//!
//! Main engine that integrates MemTables, SSTables, WAL, Compaction, and Tiered Storage
//! for optimal write performance while maintaining good read performance.

use crate::write_optimized::tiered_storage::TierConfig;
use crate::write_optimized::{
    CompactionManager, CompactionStrategy, MemTableManager, SSTableBuilder, SSTableReader,
    StorageTier, TieredStorageManager, WalEntry, WriteAheadLog, WriteBatch, WriteEngineStats,
    WriteOperation, WriteOptimizedConfig,
};
use crate::{Error, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Write engine configuration
#[derive(Debug, Clone)]
pub struct WriteEngineConfig {
    /// Base configuration
    pub base: WriteOptimizedConfig,
    /// Data directory
    pub data_dir: PathBuf,
    /// Hot tier directory
    pub hot_tier_dir: PathBuf,
    /// Warm tier directory
    pub warm_tier_dir: PathBuf,
    /// Cold tier directory
    pub cold_tier_dir: PathBuf,
}

impl WriteEngineConfig {
    /// Create a new configuration
    pub fn new(data_dir: PathBuf) -> Self {
        let hot_tier_dir = data_dir.join("hot");
        let warm_tier_dir = data_dir.join("warm");
        let cold_tier_dir = data_dir.join("cold");

        Self {
            base: WriteOptimizedConfig::default(),
            data_dir,
            hot_tier_dir,
            warm_tier_dir,
            cold_tier_dir,
        }
    }
}

/// Flush result
#[derive(Debug)]
pub struct FlushResult {
    pub sstable_metadata: Vec<crate::write_optimized::SSTableMetadata>,
    pub flushed_entries: u64,
    pub flushed_bytes: u64,
    pub flush_time: Duration,
}

/// Write-optimized storage engine
pub struct WriteOptimizedEngine {
    /// Configuration
    config: WriteEngineConfig,
    /// MemTable manager
    memtable_manager: Arc<MemTableManager>,
    /// Write-Ahead Log
    wal: Arc<WriteAheadLog>,
    /// Compaction manager
    compaction_manager: Arc<Mutex<CompactionManager>>,
    /// Tiered storage manager
    tiered_storage: Arc<TieredStorageManager>,
    /// SSTables by level
    sstables_by_level: Arc<RwLock<HashMap<usize, Vec<Arc<SSTableReader>>>>>,
    /// Sequence number generator
    sequence_generator: Arc<AtomicU64>,
    /// Statistics
    stats: Arc<RwLock<WriteEngineStats>>,
    /// Background workers
    workers: Vec<thread::JoinHandle<()>>,
    /// Running flag
    running: Arc<AtomicBool>,
    /// Flush queue
    flush_sender: Sender<FlushRequest>,
    flush_receiver: Receiver<FlushRequest>,
}

/// Flush request
#[derive(Debug)]
struct FlushRequest {
    memtable: Arc<crate::write_optimized::MemTable>,
    target_tier: StorageTier,
}

impl WriteOptimizedEngine {
    /// Create a new write-optimized engine
    pub fn new(config: WriteEngineConfig) -> Result<Self> {
        // Create directories
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(&config.hot_tier_dir)?;
        std::fs::create_dir_all(&config.warm_tier_dir)?;
        std::fs::create_dir_all(&config.cold_tier_dir)?;

        // Create WAL
        let wal_dir = config.data_dir.join("wal");
        let wal = Arc::new(WriteAheadLog::new(
            wal_dir,
            64 * 1024 * 1024, // 64MB per WAL file
            config.base.wal_sync_mode,
        )?);

        // Create MemTable manager
        let memtable_manager = Arc::new(MemTableManager::new(
            config.base.memtable_size,
            config.base.num_memtables,
        ));

        // Create compaction manager
        let compaction_manager = Arc::new(Mutex::new(CompactionManager::new(
            CompactionStrategy::Leveled,
            config.base.max_levels,
            config.base.compaction_threads,
            config.hot_tier_dir.clone(),
        )?));

        // Create tiered storage manager
        let tiered_storage = Arc::new(TieredStorageManager::new(Duration::from_secs(300))); // 5 min evaluation

        // Configure storage tiers
        tiered_storage.add_tier(TierConfig::hot_tier(
            config.hot_tier_dir.clone(),
            10 * 1024 * 1024 * 1024, // 10GB hot tier
        ))?;
        tiered_storage.add_tier(TierConfig::warm_tier(
            config.warm_tier_dir.clone(),
            100 * 1024 * 1024 * 1024, // 100GB warm tier
        ))?;
        tiered_storage.add_tier(TierConfig::cold_tier(
            config.cold_tier_dir.clone(),
            1024 * 1024 * 1024 * 1024, // 1TB cold tier
        ))?;

        let (flush_sender, flush_receiver) = unbounded();

        Ok(Self {
            config,
            memtable_manager,
            wal,
            compaction_manager,
            tiered_storage,
            sstables_by_level: Arc::new(RwLock::new(HashMap::new())),
            sequence_generator: Arc::new(AtomicU64::new(1)),
            stats: Arc::new(RwLock::new(WriteEngineStats::default())),
            workers: Vec::new(),
            running: Arc::new(AtomicBool::new(false)),
            flush_sender,
            flush_receiver,
        })
    }

    /// Start the engine
    pub fn start(&mut self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);

        // Start compaction manager
        self.compaction_manager.lock().start()?;

        // Start tiered storage manager
        let tiered_storage = Arc::clone(&self.tiered_storage);
        // Note: We can't call start on immutable reference, would need different design

        // Start flush worker
        let flush_receiver = self.flush_receiver.clone();
        let wal = Arc::clone(&self.wal);
        let running = Arc::clone(&self.running);
        let config = self.config.clone();
        let stats = Arc::clone(&self.stats);
        let compaction_manager = Arc::clone(&self.compaction_manager);
        let tiered_storage = Arc::clone(&self.tiered_storage);

        let flush_worker = thread::Builder::new()
            .name("flush-worker".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    match flush_receiver.recv_timeout(Duration::from_secs(1)) {
                        Ok(flush_request) => {
                            if let Err(_) = Self::execute_flush(
                                &flush_request,
                                &wal,
                                &config,
                                &stats,
                                &compaction_manager,
                                &tiered_storage,
                            ) {
                                // Log error but continue
                            }
                        }
                        Err(_) => continue, // Timeout
                    }
                }
            })?;

        self.workers.push(flush_worker);

        // Start background flush monitor
        let memtable_manager = Arc::clone(&self.memtable_manager);
        let flush_sender = self.flush_sender.clone();
        let running = Arc::clone(&self.running);

        let monitor_worker = thread::Builder::new()
            .name("flush-monitor".to_string())
            .spawn(move || {
                while running.load(Ordering::Relaxed) {
                    if let Some(memtable) = memtable_manager.get_flush_candidate() {
                        let flush_request = FlushRequest {
                            memtable,
                            target_tier: StorageTier::Hot, // New data goes to hot tier
                        };

                        if flush_sender.send(flush_request).is_err() {
                            break;
                        }
                    }

                    thread::sleep(Duration::from_millis(100));
                }
            })?;

        self.workers.push(monitor_worker);

        Ok(())
    }

    /// Stop the engine
    pub fn stop(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);

        // Stop workers
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| Error::Generic("Failed to join worker thread".to_string()))?;
        }

        // Stop compaction manager
        self.compaction_manager.lock().stop()?;

        // Close WAL
        self.wal.close()?;

        Ok(())
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let start = Instant::now();
        let sequence = self.sequence_generator.fetch_add(1, Ordering::SeqCst);

        // Write to WAL first
        if self.config.base.enable_wal {
            let wal_entry = WalEntry::put(sequence, key.clone(), value.clone())?;
            self.wal.append(wal_entry)?;
        }

        // Calculate sizes before moving values
        let key_len = key.len() as u64;
        let value_len = value.len() as u64;

        // Write to MemTable
        self.memtable_manager.put(key.clone(), value)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_writes += 1;
            stats.bytes_written += key_len + value_len;
        }

        // Record access pattern
        let latency_us = start.elapsed().as_micros() as f64;
        self.tiered_storage
            .record_access("memtable", false, latency_us);

        Ok(())
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> Result<()> {
        let sequence = self.sequence_generator.fetch_add(1, Ordering::SeqCst);

        // Write to WAL first
        if self.config.base.enable_wal {
            let wal_entry = WalEntry::delete(sequence, key.clone())?;
            self.wal.append(wal_entry)?;
        }

        // Write to MemTable
        self.memtable_manager.delete(key)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_deletes += 1;
        }

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let start = Instant::now();

        // Check MemTables first
        if let Some(value) = self.memtable_manager.get(key) {
            return Ok(Some(value));
        }

        // Check SSTables from newest to oldest
        let sstables = self.sstables_by_level.read();
        for level in 0..self.config.base.max_levels {
            if let Some(level_sstables) = sstables.get(&level) {
                for sstable in level_sstables.iter().rev() {
                    // Check bloom filter first
                    if !sstable.may_contain(key) {
                        continue;
                    }

                    let file_name = sstable
                        .metadata()
                        .file_path
                        .file_name()
                        .unwrap()
                        .to_string_lossy();

                    match sstable.get(key) {
                        Ok(Some(value)) => {
                            let latency_us = start.elapsed().as_micros() as f64;
                            self.tiered_storage
                                .record_access(&file_name, true, latency_us);
                            return Ok(Some(value));
                        }
                        Ok(None) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        Ok(None)
    }

    /// Execute a write batch atomically
    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        // Write all operations to WAL first
        if self.config.base.enable_wal {
            for operation in &batch.operations {
                let wal_entry = match operation {
                    WriteOperation::Put { key, value } => {
                        WalEntry::put(batch.sequence_number, key.clone(), value.clone())?
                    }
                    WriteOperation::Delete { key } => {
                        WalEntry::delete(batch.sequence_number, key.clone())?
                    }
                };
                self.wal.append(wal_entry)?;
            }
        }

        // Apply operations to MemTable
        for operation in &batch.operations {
            match operation {
                WriteOperation::Put { key, value } => {
                    self.memtable_manager.put(key.clone(), value.clone())?;
                }
                WriteOperation::Delete { key } => {
                    self.memtable_manager.delete(key.clone())?;
                }
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        stats.total_writes += 1;
                        stats.bytes_written += key.len() as u64 + value.len() as u64;
                    }
                    WriteOperation::Delete { .. } => {
                        stats.total_deletes += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Force flush of all MemTables
    pub fn flush(&self) -> Result<Vec<FlushResult>> {
        let mut results = Vec::new();

        // Get all immutable MemTables
        while let Some(memtable) = self.memtable_manager.get_flush_candidate() {
            let result = self.flush_memtable(memtable, StorageTier::Hot)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Flush a single MemTable
    fn flush_memtable(
        &self,
        memtable: Arc<crate::write_optimized::MemTable>,
        target_tier: StorageTier,
    ) -> Result<FlushResult> {
        let start = Instant::now();
        let tier_path = self.tiered_storage.get_tier_path(&target_tier)?;

        // Create SSTable from MemTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let sstable_path = tier_path.join(format!("sstable_{}.sst", timestamp));
        let mut builder = SSTableBuilder::new(&sstable_path, 0, self.config.base.compression_type)?;

        let mut flushed_entries = 0;
        let mut flushed_bytes = 0;

        // Write all entries from MemTable to SSTable
        for (key, entry) in memtable.iter() {
            match entry.entry_type {
                crate::write_optimized::memtable::EntryType::Put => {
                    if let Some(value) = entry.value {
                        builder.add(key.clone(), value.clone())?;
                        flushed_bytes += key.len() + value.len();
                    }
                }
                crate::write_optimized::memtable::EntryType::Delete => {
                    // For deletes, we still need to write a tombstone
                    builder.add(key.clone(), Vec::new())?;
                    flushed_bytes += key.len();
                }
            }
            flushed_entries += 1;
        }

        // Finalize SSTable
        let metadata = builder.finish()?;

        // Open as reader and add to engine
        let sstable_reader = Arc::new(SSTableReader::open(&sstable_path)?);

        // Add to level 0
        {
            let mut sstables = self.sstables_by_level.write();
            sstables
                .entry(0)
                .or_default()
                .push(Arc::clone(&sstable_reader));
        }

        // Add to tiered storage
        self.tiered_storage
            .add_sstable(Arc::clone(&sstable_reader), target_tier)?;

        // Add to compaction manager
        self.compaction_manager
            .lock()
            .add_sstable(0, sstable_reader)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_flushes += 1;
            stats.num_sstables += 1;
        }

        Ok(FlushResult {
            sstable_metadata: vec![metadata],
            flushed_entries,
            flushed_bytes: flushed_bytes as u64,
            flush_time: start.elapsed(),
        })
    }

    /// Execute flush request
    fn execute_flush(
        request: &FlushRequest,
        _wal: &Arc<WriteAheadLog>,
        config: &WriteEngineConfig,
        stats: &Arc<RwLock<WriteEngineStats>>,
        _compaction_manager: &Arc<Mutex<CompactionManager>>,
        tiered_storage: &Arc<TieredStorageManager>,
    ) -> Result<()> {
        let start = Instant::now();
        let tier_path = tiered_storage.get_tier_path(&request.target_tier)?;

        // Create SSTable from MemTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let sstable_path = tier_path.join(format!("bg_sstable_{}.sst", timestamp));
        let mut builder = SSTableBuilder::new(&sstable_path, 0, config.base.compression_type)?;

        let mut _flushed_entries = 0;

        // Write all entries from MemTable to SSTable
        for (key, entry) in request.memtable.iter() {
            match entry.entry_type {
                crate::write_optimized::memtable::EntryType::Put => {
                    if let Some(value) = entry.value {
                        builder.add(key, value)?;
                    }
                }
                crate::write_optimized::memtable::EntryType::Delete => {
                    // Write tombstone
                    builder.add(key, Vec::new())?;
                }
            }
            _flushed_entries += 1;
        }

        // Finalize SSTable
        let _metadata = builder.finish()?;

        // Update statistics
        {
            let mut stats = stats.write();
            stats.total_flushes += 1;
            stats.num_sstables += 1;
        }

        let _flush_time = start.elapsed();

        Ok(())
    }

    /// Get engine statistics
    pub fn stats(&self) -> WriteEngineStats {
        self.stats.read().clone()
    }

    /// Get MemTable statistics
    pub fn memtable_stats(&self) -> crate::write_optimized::memtable::MemTableManagerStats {
        self.memtable_manager.stats()
    }

    /// Get compaction statistics
    pub fn compaction_stats(&self) -> crate::write_optimized::compaction::CompactionStats {
        self.compaction_manager.lock().stats()
    }

    /// Get tier statistics
    pub fn tier_stats(
        &self,
    ) -> HashMap<StorageTier, crate::write_optimized::tiered_storage::TierStats> {
        self.tiered_storage.get_tier_stats()
    }

    /// Force compaction on a level
    pub fn force_compaction(&self, level: usize) -> Result<()> {
        self.compaction_manager.lock().force_compaction(level)
    }

    /// Force tier evaluation
    pub fn force_tier_evaluation(&self) -> Result<()> {
        self.tiered_storage.force_evaluation()
    }

    /// Recover from WAL
    pub fn recover(&self) -> Result<u64> {
        let wal_dir = self.config.data_dir.join("wal");
        let recovery = crate::write_optimized::wal::WalRecovery::recover(&wal_dir)?;

        let mut recovered_entries = 0;

        recovery.apply(|entry| {
            match entry.header.entry_type {
                crate::write_optimized::wal::WalEntryType::Put => {
                    if let Some(ref value) = entry.value {
                        self.memtable_manager
                            .put(entry.key.clone(), value.clone())?;
                        recovered_entries += 1;
                    }
                }
                crate::write_optimized::wal::WalEntryType::Delete => {
                    self.memtable_manager.delete(entry.key.clone())?;
                    recovered_entries += 1;
                }
                _ => {
                    // Ignore other entry types for now
                }
            }
            Ok(())
        })?;

        Ok(recovered_entries)
    }

    /// Sync all data to disk
    pub fn sync(&self) -> Result<()> {
        if self.config.base.enable_wal {
            self.wal.sync()?;
        }
        Ok(())
    }

    /// Get data directory
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }
}

impl Drop for WriteOptimizedEngine {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_engine_config() {
        let dir = TempDir::new().unwrap();
        let config = WriteEngineConfig::new(dir.path().to_path_buf());

        assert_eq!(config.data_dir, dir.path());
        assert_eq!(config.hot_tier_dir, dir.path().join("hot"));
        assert_eq!(config.warm_tier_dir, dir.path().join("warm"));
        assert_eq!(config.cold_tier_dir, dir.path().join("cold"));
    }

    #[test]
    fn test_write_engine_creation() {
        let dir = TempDir::new().unwrap();
        let config = WriteEngineConfig::new(dir.path().to_path_buf());

        let engine = WriteOptimizedEngine::new(config).unwrap();

        // Check that directories were created
        assert!(dir.path().join("hot").exists());
        assert!(dir.path().join("warm").exists());
        assert!(dir.path().join("cold").exists());
        assert!(dir.path().join("wal").exists());
    }

    #[test]
    fn test_write_engine_basic_operations() {
        let dir = TempDir::new().unwrap();
        let config = WriteEngineConfig::new(dir.path().to_path_buf());

        let mut engine = WriteOptimizedEngine::new(config).unwrap();
        engine.start().unwrap();

        // Test put and get
        engine.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        let result = engine.get(b"key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Test delete
        engine.delete(b"key1".to_vec()).unwrap();
        let result = engine.get(b"key1").unwrap();
        assert_eq!(result, None);

        engine.stop().unwrap();

        // Check statistics
        let stats = engine.stats();
        assert_eq!(stats.total_writes, 1);
        assert_eq!(stats.total_deletes, 1);
    }

    #[test]
    fn test_write_batch() {
        let dir = TempDir::new().unwrap();
        let config = WriteEngineConfig::new(dir.path().to_path_buf());

        let mut engine = WriteOptimizedEngine::new(config).unwrap();
        engine.start().unwrap();

        let batch = WriteBatch {
            operations: vec![
                WriteOperation::Put {
                    key: b"key1".to_vec(),
                    value: b"value1".to_vec(),
                },
                WriteOperation::Put {
                    key: b"key2".to_vec(),
                    value: b"value2".to_vec(),
                },
                WriteOperation::Delete {
                    key: b"key3".to_vec(),
                },
            ],
            sequence_number: 100,
        };

        engine.write_batch(batch).unwrap();

        // Verify results
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3").unwrap(), None);

        engine.stop().unwrap();
    }
}

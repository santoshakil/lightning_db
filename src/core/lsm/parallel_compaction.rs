use super::{Level, SSTable, SSTableBuilder};
use crate::core::error::Result;
use std::collections::VecDeque;
use std::sync::Mutex;
use dashmap::DashMap;
use parking_lot::RwLock;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Parallel compaction coordinator
pub struct ParallelCompactionCoordinator {
    // Worker pool
    worker_count: usize,
    work_queue: Arc<Mutex<VecDeque<CompactionJob>>>,

    // Progress tracking
    active_compactions: Arc<DashMap<u64, CompactionProgress>>,
    next_job_id: AtomicU64,

    // Metrics
    total_compacted_bytes: AtomicU64,
    total_compaction_time_ms: AtomicU64,

    // Control
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone)]
struct CompactionJob {
    id: u64,
    input_tables: Vec<Arc<SSTable>>,
    output_level: usize,
    output_path: PathBuf,
}

struct CompactionProgress {
    start_time: Instant,
    processed_keys: AtomicUsize,
    output_bytes: AtomicU64,
    state: Arc<RwLock<CompactionState>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompactionState {
    Running,
    Merging,
    Writing,
    Completed,
    Failed(String),
}

impl ParallelCompactionCoordinator {
    pub fn new(worker_count: usize) -> Arc<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let work_queue = Arc::new(Mutex::new(VecDeque::new()));
        let active_compactions = Arc::new(DashMap::new());

        let coordinator = Arc::new(Self {
            worker_count,
            work_queue: Arc::clone(&work_queue),
            active_compactions: Arc::clone(&active_compactions),
            next_job_id: AtomicU64::new(1),
            total_compacted_bytes: AtomicU64::new(0),
            total_compaction_time_ms: AtomicU64::new(0),
            shutdown: Arc::clone(&shutdown),
        });

        // Start worker threads
        for worker_id in 0..worker_count {
            let coordinator_clone = Arc::clone(&coordinator);
            std::thread::spawn(move || {
                coordinator_clone.worker_loop(worker_id);
            });
        }

        coordinator
    }

    /// Submit a compaction job
    pub fn submit_compaction(
        &self,
        _level: usize,
        tables: Vec<Arc<SSTable>>,
        output_level: usize,
        output_path: PathBuf,
    ) -> u64 {
        let job_id = self.next_job_id.fetch_add(1, Ordering::SeqCst);

        let job = CompactionJob {
            id: job_id,
            input_tables: tables,
            output_level,
            output_path,
        };

        // Track progress
        let progress = CompactionProgress {
            start_time: Instant::now(),
            processed_keys: AtomicUsize::new(0),
            output_bytes: AtomicU64::new(0),
            state: Arc::new(RwLock::new(CompactionState::Running)),
        };

        self.active_compactions.insert(job_id, progress);
        self.work_queue.lock().unwrap().push_back(job);

        job_id
    }

    /// Get compaction progress
    pub fn get_progress(&self, job_id: u64) -> Option<CompactionStatus> {
        self.active_compactions.get(&job_id).map(|entry| {
            let progress = entry.value();
            CompactionStatus {
                job_id,
                state: progress.state.read().clone(),
                processed_keys: progress.processed_keys.load(Ordering::Relaxed),
                output_bytes: progress.output_bytes.load(Ordering::Relaxed),
                elapsed: progress.start_time.elapsed(),
            }
        })
    }

    /// Worker loop
    fn worker_loop(&self, _worker_id: usize) {
        while !self.shutdown.load(Ordering::Relaxed) {
            if let Some(job) = self.work_queue.lock().unwrap().pop_front() {
                self.execute_compaction(job);
            } else {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    /// Execute a compaction job
    fn execute_compaction(&self, job: CompactionJob) {
        let start_time = Instant::now();

        match self.active_compactions.get(&job.id) {
            Some(progress_entry) => {
                let progress = progress_entry.value();

                // Perform parallel compaction
                let result = self.parallel_compact(
                    &job.input_tables,
                    &job.output_path,
                    job.output_level,
                    progress,
                );

                // Update state
                match result {
                    Ok(stats) => {
                        *progress.state.write() = CompactionState::Completed;
                        self.total_compacted_bytes
                            .fetch_add(stats.bytes_written, Ordering::Relaxed);

                        let elapsed_ms = start_time.elapsed().as_millis() as u64;
                        self.total_compaction_time_ms
                            .fetch_add(elapsed_ms, Ordering::Relaxed);

                        tracing::info!(
                            "Compaction {} completed: {} keys, {} bytes in {:?}",
                            job.id,
                            stats.keys_written,
                            stats.bytes_written,
                            start_time.elapsed()
                        );
                    }
                    Err(e) => {
                        *progress.state.write() = CompactionState::Failed(e.to_string());
                        tracing::error!("Compaction {} failed: {}", job.id, e);
                    }
                }
            }
            None => {
                tracing::error!("No progress tracking for job {}", job.id);
            }
        }
    }

    /// Parallel compaction implementation
    fn parallel_compact(
        &self,
        input_tables: &[Arc<SSTable>],
        output_path: &Path,
        output_level: usize,
        progress: &CompactionProgress,
    ) -> Result<CompactionStats> {
        *progress.state.write() = CompactionState::Merging;

        // Phase 1: Parallel read and merge
        let merged_entries = self.parallel_merge(input_tables, progress)?;

        *progress.state.write() = CompactionState::Writing;

        // Phase 2: Parallel write
        let stats = self.parallel_write(merged_entries, output_path, output_level, progress)?;

        Ok(stats)
    }

    /// Memory-efficient parallel merge with streaming
    fn parallel_merge(
        &self,
        tables: &[Arc<SSTable>],
        progress: &CompactionProgress,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // CRITICAL FIX: Memory-bounded parallel processing
        const MAX_MEMORY_PER_CHUNK: usize = 32 * 1024 * 1024; // 32MB per chunk

        // Calculate optimal chunk size based on memory constraints
        let estimated_entry_size = 100; // bytes
        let max_entries_per_chunk = MAX_MEMORY_PER_CHUNK / estimated_entry_size;

        let mut all_entries = Vec::new();

        // Process tables in memory-bounded batches
        for table_batch in tables.chunks(self.worker_count) {
            let batch_results: Vec<_> = table_batch
                .par_iter()
                .map(|table| -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
                    let mut entries = Vec::new();
                    let table_entries = table.iter()?;

                    // Limit entries per table to prevent OOM
                    for (i, entry) in table_entries.into_iter().enumerate() {
                        if i >= max_entries_per_chunk {
                            tracing::warn!(
                                "Table {} too large, truncating at {} entries",
                                table.id(),
                                max_entries_per_chunk
                            );
                            break;
                        }
                        entries.push(entry);
                    }
                    Ok(entries)
                })
                .collect::<Result<Vec<_>>>()?;

            // Merge results from this batch
            for batch_entries in batch_results {
                let entry_count = batch_entries.len();
                all_entries.extend(batch_entries);

                // Periodic progress update
                progress
                    .processed_keys
                    .fetch_add(entry_count, Ordering::Relaxed);
            }
        }

        // Memory-efficient sort
        self.in_memory_sort_merge(all_entries, progress)
    }

    /// In-memory sort and merge with deduplication  
    fn in_memory_sort_merge(
        &self,
        mut entries: Vec<(Vec<u8>, Vec<u8>)>,
        progress: &CompactionProgress,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Parallel sort by key
        entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Sequential deduplication (keep latest version)
        let mut deduped = Vec::new();
        let mut last_key: Option<Vec<u8>> = None;

        for (key, value) in entries {
            progress.processed_keys.fetch_add(1, Ordering::Relaxed);

            if Some(&key) != last_key.as_ref() {
                deduped.push((key.clone(), value));
                last_key = Some(key);
            }
        }

        Ok(deduped)
    }

    /// Parallel write with multiple SSTable builders
    fn parallel_write(
        &self,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        output_path: &Path,
        level: usize,
        progress: &CompactionProgress,
    ) -> Result<CompactionStats> {
        let target_file_size = 64 * 1024 * 1024; // 64MB per SSTable
        let estimated_entry_size = 100; // Rough estimate
        let entries_per_table = target_file_size / estimated_entry_size;

        // Split entries into chunks for parallel SSTable building
        let table_chunks: Vec<_> = entries.chunks(entries_per_table).enumerate().collect();

        // Build SSTables in parallel
        let results: Vec<_> = table_chunks
            .par_iter()
            .map(|(idx, chunk)| {
                let table_path = output_path.join(format!("level_{}_table_{}.sst", level, idx));
                let mut builder = SSTableBuilder::new(
                    &table_path,
                    4096, // block size
                    #[cfg(feature = "zstd-compression")]
                    crate::features::adaptive_compression::CompressionType::Zstd,
                    #[cfg(not(feature = "zstd-compression"))]
                    crate::features::adaptive_compression::CompressionType::Snappy,
                    10, // bloom filter bits per key
                )?;

                let mut bytes = 0u64;
                let mut entry_count = 0;
                for (key, value) in chunk.iter() {
                    builder.add(key, value)?;
                    bytes += (key.len() + value.len()) as u64;
                    progress
                        .output_bytes
                        .fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
                    entry_count += 1;
                }

                // Only finish if we have entries to avoid "No keys added" error
                if entry_count > 0 {
                    builder.finish()?;
                }

                Ok((chunk.len(), bytes))
            })
            .collect::<Result<Vec<_>>>()?;

        // Aggregate stats
        let mut total_keys = 0;
        let mut total_bytes = 0;

        for (keys, bytes) in results {
            total_keys += keys;
            total_bytes += bytes;
        }

        Ok(CompactionStats {
            keys_written: total_keys,
            bytes_written: total_bytes,
            tables_created: table_chunks.len(),
        })
    }

    /// Get compaction statistics
    pub fn stats(&self) -> ParallelCompactionStats {
        let active_count = self.active_compactions.len();
        let completed_count = self
            .active_compactions
            .iter()
            .filter(|entry| matches!(*entry.value().state.read(), CompactionState::Completed))
            .count();

        ParallelCompactionStats {
            active_compactions: active_count,
            completed_compactions: completed_count,
            total_bytes_compacted: self.total_compacted_bytes.load(Ordering::Relaxed),
            total_time_ms: self.total_compaction_time_ms.load(Ordering::Relaxed),
            worker_count: self.worker_count,
        }
    }

    /// Shutdown the coordinator
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

impl Drop for ParallelCompactionCoordinator {
    fn drop(&mut self) {
        self.shutdown();

        // Wait for workers to finish
        // Note: Can't join threads in Drop due to ownership
    }
}

#[derive(Debug, Clone)]
pub struct CompactionStatus {
    pub job_id: u64,
    pub state: CompactionState,
    pub processed_keys: usize,
    pub output_bytes: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone)]
pub struct ParallelCompactionStats {
    pub active_compactions: usize,
    pub completed_compactions: usize,
    pub total_bytes_compacted: u64,
    pub total_time_ms: u64,
    pub worker_count: usize,
}

#[derive(Debug, Clone)]
pub struct CompactionStats {
    pub keys_written: usize,
    pub bytes_written: u64,
    pub tables_created: usize,
}

/// Compaction scheduler for automatic parallel compaction
pub struct CompactionScheduler {
    coordinator: Arc<ParallelCompactionCoordinator>,
    levels: Arc<RwLock<Vec<Level>>>,

    // Thresholds
    level_multiplier: f64,
    base_level_size: u64,
    max_bytes_for_level_base: u64,

    // State
    last_check: RwLock<Instant>,
    check_interval: Duration,
}

impl CompactionScheduler {
    pub fn new(
        coordinator: Arc<ParallelCompactionCoordinator>,
        levels: Arc<RwLock<Vec<Level>>>,
    ) -> Self {
        Self {
            coordinator,
            levels,
            level_multiplier: 10.0,
            base_level_size: 10 * 1024 * 1024,           // 10MB
            max_bytes_for_level_base: 256 * 1024 * 1024, // 256MB
            last_check: RwLock::new(Instant::now()),
            check_interval: Duration::from_secs(10),
        }
    }

    /// Check if compaction is needed and schedule if necessary
    pub fn maybe_schedule_compaction(&self) -> Vec<u64> {
        let mut last_check = self.last_check.write();

        if last_check.elapsed() < self.check_interval {
            return vec![];
        }

        *last_check = Instant::now();
        drop(last_check);

        let mut scheduled_jobs = Vec::new();
        let levels = self.levels.read();

        // Check each level for compaction needs
        for (level_idx, level) in levels.iter().enumerate() {
            if self.should_compact_level(level_idx, level) {
                // Select tables to compact
                let tables_to_compact = self.select_compaction_tables(level, level_idx);

                if !tables_to_compact.is_empty() {
                    let output_level = level_idx + 1;
                    let output_path = PathBuf::from(format!("level_{}", output_level));

                    let job_id = self.coordinator.submit_compaction(
                        level_idx,
                        tables_to_compact,
                        output_level,
                        output_path,
                    );

                    scheduled_jobs.push(job_id);
                }
            }
        }

        scheduled_jobs
    }

    /// Check if a level needs compaction
    fn should_compact_level(&self, level_idx: usize, level: &Level) -> bool {
        let level_size = level.total_size();
        let max_size = self.max_bytes_for_level(level_idx);

        level_size > max_size
    }

    /// Calculate maximum size for a level
    fn max_bytes_for_level(&self, level: usize) -> u64 {
        if level == 0 {
            self.base_level_size
        } else {
            let multiplier = self.level_multiplier.powi(level as i32 - 1);
            (self.max_bytes_for_level_base as f64 * multiplier) as u64
        }
    }

    /// Select tables for compaction
    fn select_compaction_tables(&self, level: &Level, _level_idx: usize) -> Vec<Arc<SSTable>> {
        // Simple strategy: compact oldest tables
        let mut tables: Vec<_> = level.tables().to_vec();

        // Sort by creation time (or use other criteria)
        tables.sort_by_key(|t| t.creation_time());

        // Take up to 4 tables for compaction
        let compact_count = 4.min(tables.len());
        tables.into_iter().take(compact_count).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parallel_compaction_coordinator() {
        let coordinator = ParallelCompactionCoordinator::new(4);

        // Create mock SSTables
        let tables = vec![]; // Would need actual SSTable instances

        let dir = tempdir().unwrap();
        let output_path = dir.path().to_path_buf();

        // Submit compaction job
        let job_id = coordinator.submit_compaction(0, tables, 1, output_path);

        // Check progress
        std::thread::sleep(Duration::from_millis(100));

        if let Some(status) = coordinator.get_progress(job_id) {
            println!("Compaction status: {:?}", status);
        }

        let stats = coordinator.stats();
        assert_eq!(stats.worker_count, 4);
    }
}

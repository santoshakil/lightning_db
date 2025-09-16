use crate::core::error::Result;
use super::{TransactionId, TransactionState};
use crc32fast::Hasher;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VoteDecision {
    Commit,
    Abort,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Decision {
    Commit,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationType {
    Read,
    Write,
    Update,
    Delete,
    Lock,
    Unlock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogType {
    Begin,
    Prepare,
    Commit,
    Abort,
    Decision,
    Checkpoint,
    Undo,
    Redo,
    Compensation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub lsn: u64,
    pub txn_id: TransactionId,
    pub log_type: LogType,
    pub timestamp: u64,
    pub data: LogData,
    pub prev_lsn: Option<u64>,
    pub checksum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogData {
    Begin {
        isolation_level: super::isolation::IsolationLevel,
        read_timestamp: u64,
        participants: Vec<String>,
    },
    Prepare {
        participant_id: String,
        vote: VoteDecision,
        locks: Vec<String>,
    },
    Commit {
        commit_timestamp: u64,
        participants_committed: Vec<String>,
    },
    Abort {
        reason: String,
        participants_aborted: Vec<String>,
    },
    Decision {
        decision: Decision,
        coordinator_id: String,
    },
    Checkpoint {
        active_txns: Vec<TransactionId>,
        min_lsn: u64,
        max_lsn: u64,
    },
    Undo {
        operation: UndoOperation,
        before_image: Option<Vec<u8>>,
        after_image: Option<Vec<u8>>,
    },
    Redo {
        operation: RedoOperation,
        page_id: u64,
        data: Vec<u8>,
    },
    Compensation {
        compensated_lsn: u64,
        operation: CompensationOperation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoOperation {
    pub key: String,
    pub operation_type: OperationType,
    pub page_id: u64,
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedoOperation {
    pub key: String,
    pub operation_type: OperationType,
    pub page_id: u64,
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensationOperation {
    pub original_operation: UndoOperation,
    pub compensation_type: CompensationType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompensationType {
    Logical,
    Physical,
    Physiological,
}

pub struct TransactionLog {
    storage: Arc<dyn crate::core::storage::PageManagerAsync>,
    active_segment: Arc<RwLock<LogSegment>>,
    segments: Arc<DashMap<u64, LogSegment>>,
    lsn_generator: Arc<LsnGenerator>,
    buffer_pool: Arc<BufferPool>,
    checkpoint_manager: Arc<CheckpointManager>,
    archiver: Arc<LogArchiver>,
    metrics: Arc<LogMetrics>,
}

#[derive(Clone, Serialize, Deserialize)]
struct LogSegment {
    segment_id: u64,
    start_lsn: u64,
    end_lsn: Option<u64>,
    entries: Vec<LogEntry>,
    size: usize,
    created_at: u64,
    closed: bool,
    path: String,
}

struct LsnGenerator {
    current_lsn: Arc<std::sync::atomic::AtomicU64>,
    checkpoint_lsn: Arc<std::sync::atomic::AtomicU64>,
    recovery_lsn: Arc<std::sync::atomic::AtomicU64>,
}

struct BufferPool {
    write_buffer: Arc<RwLock<LogBuffer>>,
    flush_buffer: Arc<RwLock<LogBuffer>>,
    buffer_size: usize,
    flush_interval: Duration,
    last_flush: Arc<RwLock<Instant>>,
}

struct LogBuffer {
    entries: VecDeque<LogEntry>,
    size: usize,
    capacity: usize,
    dirty: bool,
}

struct CheckpointManager {
    checkpoint_interval: Duration,
    last_checkpoint: Arc<RwLock<Instant>>,
    checkpoint_lsn: Arc<std::sync::atomic::AtomicU64>,
    active_transactions: Arc<DashMap<TransactionId, TransactionLogState>>,
    dirty_pages: Arc<DashMap<u64, DirtyPageInfo>>,
}

struct TransactionLogState {
    txn_id: TransactionId,
    first_lsn: u64,
    last_lsn: u64,
    undo_chain: Vec<u64>,
    state: TransactionState,
}

struct DirtyPageInfo {
    page_id: u64,
    recovery_lsn: u64,
    last_update_lsn: u64,
    dirty_since: Instant,
}

struct LogArchiver {
    archive_path: String,
    retention_period: Duration,
    compression_enabled: bool,
    archive_queue: Arc<RwLock<VecDeque<u64>>>,
    last_archive: Arc<RwLock<Instant>>,
}

struct LogMetrics {
    total_entries: Arc<std::sync::atomic::AtomicU64>,
    total_bytes: Arc<std::sync::atomic::AtomicU64>,
    flush_count: Arc<std::sync::atomic::AtomicU64>,
    checkpoint_count: Arc<std::sync::atomic::AtomicU64>,
    recovery_count: Arc<std::sync::atomic::AtomicU64>,
    archive_count: Arc<std::sync::atomic::AtomicU64>,
}

impl TransactionLog {
    pub fn new(
        storage: Arc<dyn crate::core::storage::PageManagerAsync>,
        config: LogConfig,
    ) -> Self {
        let lsn_generator = Arc::new(LsnGenerator {
            current_lsn: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            checkpoint_lsn: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            recovery_lsn: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        });

        let initial_segment = LogSegment {
            segment_id: 0,
            start_lsn: 1,
            end_lsn: None,
            entries: Vec::new(),
            size: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            closed: false,
            path: format!("{}/segment_0000.log", config.log_path),
        };

        Self {
            storage,
            active_segment: Arc::new(RwLock::new(initial_segment)),
            segments: Arc::new(DashMap::new()),
            lsn_generator,
            buffer_pool: Arc::new(BufferPool {
                write_buffer: Arc::new(RwLock::new(LogBuffer {
                    entries: VecDeque::new(),
                    size: 0,
                    capacity: config.buffer_size,
                    dirty: false,
                })),
                flush_buffer: Arc::new(RwLock::new(LogBuffer {
                    entries: VecDeque::new(),
                    size: 0,
                    capacity: config.buffer_size,
                    dirty: false,
                })),
                buffer_size: config.buffer_size,
                flush_interval: config.flush_interval,
                last_flush: Arc::new(RwLock::new(Instant::now())),
            }),
            checkpoint_manager: Arc::new(CheckpointManager {
                checkpoint_interval: config.checkpoint_interval,
                last_checkpoint: Arc::new(RwLock::new(Instant::now())),
                checkpoint_lsn: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                active_transactions: Arc::new(DashMap::new()),
                dirty_pages: Arc::new(DashMap::new()),
            }),
            archiver: Arc::new(LogArchiver {
                archive_path: config.archive_path,
                retention_period: config.retention_period,
                compression_enabled: config.compression_enabled,
                archive_queue: Arc::new(RwLock::new(VecDeque::new())),
                last_archive: Arc::new(RwLock::new(Instant::now())),
            }),
            metrics: Arc::new(LogMetrics {
                total_entries: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                total_bytes: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                flush_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                checkpoint_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                recovery_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                archive_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
        }
    }

    pub async fn write(
        &self,
        log_type: LogType,
        txn_id: TransactionId,
        data: LogData,
    ) -> Result<u64> {
        let lsn = self
            .lsn_generator
            .current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let entry = LogEntry {
            lsn,
            txn_id,
            log_type,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data,
            prev_lsn: self.get_last_lsn_for_txn(txn_id).await,
            checksum: 0,
        };

        let mut entry = entry;
        entry.checksum = self.calculate_checksum(&entry);

        let mut buffer = self.buffer_pool.write_buffer.write().await;
        buffer.entries.push_back(entry.clone());
        buffer.size += std::mem::size_of::<LogEntry>();
        buffer.dirty = true;

        if buffer.size >= buffer.capacity {
            drop(buffer);
            self.flush().await?;
        }

        self.metrics
            .total_entries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if let LogData::Checkpoint { .. } = entry.data {
            self.checkpoint_manager
                .checkpoint_lsn
                .store(lsn, std::sync::atomic::Ordering::SeqCst);
        }

        Ok(lsn)
    }

    async fn get_last_lsn_for_txn(&self, txn_id: TransactionId) -> Option<u64> {
        if let Some(state) = self.checkpoint_manager.active_transactions.get(&txn_id) {
            return Some(state.last_lsn);
        }

        let segment = self.active_segment.read().await;
        for entry in segment.entries.iter().rev() {
            if entry.txn_id == txn_id {
                return Some(entry.lsn);
            }
        }

        None
    }

    fn calculate_checksum(&self, entry: &LogEntry) -> u32 {
        let mut hasher = Hasher::new();

        hasher.update(&entry.lsn.to_le_bytes());
        hasher.update(&serde_json::to_vec(&entry.txn_id).unwrap());
        hasher.update(&serde_json::to_vec(&entry.log_type).unwrap());
        hasher.update(&entry.timestamp.to_le_bytes());
        hasher.update(&serde_json::to_vec(&entry.data).unwrap());

        if let Some(prev_lsn) = entry.prev_lsn {
            hasher.update(&prev_lsn.to_le_bytes());
        }

        hasher.finalize()
    }

    pub async fn flush(&self) -> Result<()> {
        let mut write_buffer = self.buffer_pool.write_buffer.write().await;
        let mut flush_buffer = self.buffer_pool.flush_buffer.write().await;

        std::mem::swap(&mut *write_buffer, &mut *flush_buffer);
        write_buffer.dirty = false;

        drop(write_buffer);

        if flush_buffer.entries.is_empty() {
            return Ok(());
        }

        let mut segment = self.active_segment.write().await;

        for entry in flush_buffer.entries.drain(..) {
            segment.entries.push(entry.clone());
            segment.size += std::mem::size_of::<LogEntry>();

            if let Some(mut state) = self
                .checkpoint_manager
                .active_transactions
                .get_mut(&entry.txn_id)
            {
                state.last_lsn = entry.lsn;
                if state.first_lsn == 0 {
                    state.first_lsn = entry.lsn;
                }

                if matches!(entry.log_type, LogType::Undo) {
                    state.undo_chain.push(entry.lsn);
                }
            } else if matches!(entry.log_type, LogType::Begin) {
                self.checkpoint_manager.active_transactions.insert(
                    entry.txn_id,
                    TransactionLogState {
                        txn_id: entry.txn_id,
                        first_lsn: entry.lsn,
                        last_lsn: entry.lsn,
                        undo_chain: Vec::new(),
                        state: TransactionState::Active,
                    },
                );
            }

            if matches!(entry.log_type, LogType::Commit | LogType::Abort) {
                self.checkpoint_manager
                    .active_transactions
                    .remove(&entry.txn_id);
            }
        }

        flush_buffer.size = 0;
        flush_buffer.dirty = false;

        let serialized = serde_json::to_vec(&segment.entries).unwrap();
        self.storage
            .write_page(segment.path.as_bytes().to_vec(), serialized)
            .await?;

        if segment.size > 1024 * 1024 * 16 {
            self.rotate_segment().await?;
        }

        self.metrics
            .flush_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics
            .total_bytes
            .fetch_add(segment.size as u64, std::sync::atomic::Ordering::Relaxed);

        *self.buffer_pool.last_flush.write().await = Instant::now();

        Ok(())
    }

    async fn rotate_segment(&self) -> Result<()> {
        let mut current = self.active_segment.write().await;

        let current_lsn = self
            .lsn_generator
            .current_lsn
            .load(std::sync::atomic::Ordering::SeqCst);
        current.end_lsn = Some(current_lsn);
        current.closed = true;

        let old_segment = (*current).clone();
        self.segments.insert(old_segment.segment_id, old_segment);

        let new_segment_id = current.segment_id + 1;
        *current = LogSegment {
            segment_id: new_segment_id,
            start_lsn: current_lsn + 1,
            end_lsn: None,
            entries: Vec::new(),
            size: 0,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            closed: false,
            path: format!("logs/segment_{:04}.log", new_segment_id),
        };

        let mut archive_queue = self.archiver.archive_queue.write().await;
        archive_queue.push_back(new_segment_id - 1);

        Ok(())
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.metrics
            .checkpoint_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.flush().await?;

        let active_txns: Vec<TransactionId> = self
            .checkpoint_manager
            .active_transactions
            .iter()
            .map(|entry| *entry.key())
            .collect();

        let min_lsn = self
            .checkpoint_manager
            .active_transactions
            .iter()
            .map(|entry| entry.value().first_lsn)
            .min()
            .unwrap_or(0);

        let max_lsn = self
            .lsn_generator
            .current_lsn
            .load(std::sync::atomic::Ordering::SeqCst);

        let checkpoint_data = LogData::Checkpoint {
            active_txns: active_txns.clone(),
            min_lsn,
            max_lsn,
        };

        self.write(
            LogType::Checkpoint,
            0,
            checkpoint_data,
        )
        .await?;

        self.flush().await?;

        *self.checkpoint_manager.last_checkpoint.write().await = Instant::now();

        Ok(())
    }

    pub async fn recover(&self, start_lsn: Option<u64>) -> Result<RecoveryInfo> {
        self.metrics
            .recovery_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let start = start_lsn.unwrap_or_else(|| {
            self.checkpoint_manager
                .checkpoint_lsn
                .load(std::sync::atomic::Ordering::SeqCst)
        });

        let mut recovery_info = RecoveryInfo {
            start_lsn: start,
            end_lsn: 0,
            transactions_recovered: Vec::new(),
            redo_operations: Vec::new(),
            undo_operations: Vec::new(),
        };

        let entries = self.read_log_range(start, None).await?;

        let mut active_txns = HashMap::new();
        let mut redo_list = Vec::new();
        let mut undo_list = Vec::new();

        for entry in &entries {
            match entry.log_type {
                LogType::Begin => {
                    active_txns.insert(
                        entry.txn_id,
                        RecoveryTransaction {
                            txn_id: entry.txn_id,
                            state: RecoveryState::Active,
                            undo_chain: Vec::new(),
                        },
                    );
                }
                LogType::Commit => {
                    if let Some(txn) = active_txns.get_mut(&entry.txn_id) {
                        txn.state = RecoveryState::Committed;
                    }
                }
                LogType::Abort => {
                    if let Some(txn) = active_txns.get_mut(&entry.txn_id) {
                        txn.state = RecoveryState::Aborted;
                    }
                }
                LogType::Undo => {
                    if let Some(txn) = active_txns.get_mut(&entry.txn_id) {
                        txn.undo_chain.push(entry.lsn);
                    }

                    if let LogData::Undo { operation, .. } = &entry.data {
                        undo_list.push(operation.clone());
                    }
                }
                LogType::Redo => {
                    if let LogData::Redo { operation, .. } = &entry.data {
                        redo_list.push(operation.clone());
                    }
                }
                _ => {}
            }

            recovery_info.end_lsn = entry.lsn;
        }

        for entry in entries.iter().rev() {
            if let Some(txn) = active_txns.get(&entry.txn_id) {
                if txn.state == RecoveryState::Active {
                    for lsn in &txn.undo_chain {
                        if let Some(undo_entry) = entries.iter().find(|e| e.lsn == *lsn) {
                            if let LogData::Undo { operation, .. } = &undo_entry.data {
                                recovery_info.undo_operations.push(operation.clone());
                            }
                        }
                    }
                }
            }
        }

        recovery_info.redo_operations = redo_list;
        recovery_info.transactions_recovered = active_txns.keys().cloned().collect();

        self.lsn_generator
            .recovery_lsn
            .store(recovery_info.end_lsn, std::sync::atomic::Ordering::SeqCst);

        Ok(recovery_info)
    }

    async fn read_log_range(&self, start_lsn: u64, end_lsn: Option<u64>) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();

        let segment = self.active_segment.read().await;
        for entry in &segment.entries {
            if entry.lsn >= start_lsn {
                if let Some(end) = end_lsn {
                    if entry.lsn > end {
                        break;
                    }
                }
                entries.push(entry.clone());
            }
        }

        for segment_entry in self.segments.iter() {
            let segment = segment_entry.value();
            if let Some(end) = segment.end_lsn {
                if end < start_lsn {
                    continue;
                }
            }

            for entry in &segment.entries {
                if entry.lsn >= start_lsn {
                    if let Some(end) = end_lsn {
                        if entry.lsn > end {
                            break;
                        }
                    }
                    entries.push(entry.clone());
                }
            }
        }

        entries.sort_by_key(|e| e.lsn);

        Ok(entries)
    }

    pub async fn truncate(&self, lsn: u64) -> Result<()> {
        let mut segment = self.active_segment.write().await;
        segment.entries.retain(|entry| entry.lsn <= lsn);

        let segments_to_remove: Vec<u64> = self
            .segments
            .iter()
            .filter(|entry| {
                if let Some(end_lsn) = entry.value().end_lsn {
                    end_lsn > lsn
                } else {
                    false
                }
            })
            .map(|entry| *entry.key())
            .collect();

        for segment_id in segments_to_remove {
            self.segments.remove(&segment_id);
        }

        Ok(())
    }

    pub async fn archive_old_segments(&self) -> Result<()> {
        let mut archive_queue = self.archiver.archive_queue.write().await;

        while let Some(segment_id) = archive_queue.pop_front() {
            if let Some((_, segment)) = self.segments.remove(&segment_id) {
                let archive_path = format!(
                    "{}/segment_{:04}.archive",
                    self.archiver.archive_path, segment_id
                );

                let data = if self.archiver.compression_enabled {
                    let serialized = serde_json::to_vec(&segment).unwrap();
                    lz4_flex::compress_prepend_size(&serialized)
                } else {
                    serde_json::to_vec(&segment).unwrap()
                };

                self.storage
                    .write_page(archive_path.as_bytes().to_vec(), data)
                    .await?;

                self.metrics
                    .archive_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        *self.archiver.last_archive.write().await = Instant::now();

        Ok(())
    }

    pub async fn log_begin(
        &self,
        txn_id: TransactionId,
        isolation_level: super::isolation::IsolationLevel,
        participants: Vec<String>,
    ) -> Result<u64> {
        let lsn = self
            .lsn_generator
            .current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let entry = LogEntry {
            lsn,
            txn_id,
            log_type: LogType::Begin,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: LogData::Begin {
                isolation_level,
                read_timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                participants,
            },
            prev_lsn: None,
            checksum: 0,
        };

        let mut hasher = Hasher::new();
        hasher.update(&serde_json::to_vec(&entry.data).unwrap());
        let mut entry = entry;
        entry.checksum = hasher.finalize();

        self.append_entry(entry).await?;
        Ok(lsn)
    }

    pub async fn log_prepare(
        &self,
        txn_id: TransactionId,
        participant_id: String,
        vote: VoteDecision,
        locks: Vec<String>,
    ) -> Result<u64> {
        let lsn = self
            .lsn_generator
            .current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let entry = LogEntry {
            lsn,
            txn_id,
            log_type: LogType::Prepare,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: LogData::Prepare {
                participant_id,
                vote,
                locks,
            },
            prev_lsn: self.find_last_lsn_for_txn(txn_id).await,
            checksum: 0,
        };

        let mut hasher = Hasher::new();
        hasher.update(&serde_json::to_vec(&entry.data).unwrap());
        let mut entry = entry;
        entry.checksum = hasher.finalize();

        self.append_entry(entry).await?;
        Ok(lsn)
    }

    pub async fn log_commit(
        &self,
        txn_id: TransactionId,
        commit_timestamp: u64,
        participants_committed: Vec<String>,
    ) -> Result<u64> {
        let lsn = self
            .lsn_generator
            .current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let entry = LogEntry {
            lsn,
            txn_id,
            log_type: LogType::Commit,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: LogData::Commit {
                commit_timestamp,
                participants_committed,
            },
            prev_lsn: self.find_last_lsn_for_txn(txn_id).await,
            checksum: 0,
        };

        let mut hasher = Hasher::new();
        hasher.update(&serde_json::to_vec(&entry.data).unwrap());
        let mut entry = entry;
        entry.checksum = hasher.finalize();

        self.append_entry(entry).await?;
        Ok(lsn)
    }

    pub async fn log_abort(
        &self,
        txn_id: TransactionId,
        reason: String,
        participants_aborted: Vec<String>,
    ) -> Result<u64> {
        let lsn = self
            .lsn_generator
            .current_lsn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let entry = LogEntry {
            lsn,
            txn_id,
            log_type: LogType::Abort,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: LogData::Abort {
                reason,
                participants_aborted,
            },
            prev_lsn: self.find_last_lsn_for_txn(txn_id).await,
            checksum: 0,
        };

        let mut hasher = Hasher::new();
        hasher.update(&serde_json::to_vec(&entry.data).unwrap());
        let mut entry = entry;
        entry.checksum = hasher.finalize();

        self.append_entry(entry).await?;
        Ok(lsn)
    }

    async fn find_last_lsn_for_txn(&self, txn_id: TransactionId) -> Option<u64> {
        if let Some(state) = self.checkpoint_manager.active_transactions.get(&txn_id) {
            return Some(state.last_lsn);
        }
        None
    }

    async fn append_entry(&self, entry: LogEntry) -> Result<()> {
        // Update checkpoint manager
        self.checkpoint_manager
            .active_transactions
            .entry(entry.txn_id)
            .or_insert(TransactionLogState {
                txn_id: entry.txn_id,
                first_lsn: entry.lsn,
                last_lsn: entry.lsn,
                undo_chain: Vec::new(),
                state: TransactionState::Active,
            })
            .last_lsn = entry.lsn;

        // Add to write buffer
        let mut write_buffer = self.buffer_pool.write_buffer.write().await;
        write_buffer.entries.push_back(entry.clone());
        write_buffer.size += std::mem::size_of::<LogEntry>();
        write_buffer.dirty = true;

        // Check if we need to flush
        if write_buffer.size >= self.buffer_pool.buffer_size {
            drop(write_buffer);
            self.flush().await?;
        }

        // Update metrics
        self.metrics
            .total_entries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.total_bytes.fetch_add(
            std::mem::size_of::<LogEntry>() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub log_path: String,
    pub archive_path: String,
    pub buffer_size: usize,
    pub flush_interval: Duration,
    pub checkpoint_interval: Duration,
    pub retention_period: Duration,
    pub compression_enabled: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_path: "logs".to_string(),
            archive_path: "archive".to_string(),
            buffer_size: 1024 * 1024,
            flush_interval: Duration::from_millis(100),
            checkpoint_interval: Duration::from_secs(300),
            retention_period: Duration::from_secs(86400 * 7),
            compression_enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryInfo {
    pub start_lsn: u64,
    pub end_lsn: u64,
    pub transactions_recovered: Vec<TransactionId>,
    pub redo_operations: Vec<RedoOperation>,
    pub undo_operations: Vec<UndoOperation>,
}

#[derive(Debug, Clone)]
struct RecoveryTransaction {
    txn_id: TransactionId,
    state: RecoveryState,
    undo_chain: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryState {
    Active,
    Committed,
    Aborted,
}

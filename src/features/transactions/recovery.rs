use crate::core::error::{Error, Result};
use super::{TransactionId, TransactionState};
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryStrategy {
    ARIES,
    ShadowPaging,
    Logging,
    Checkpointing,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryPoint {
    pub lsn: u64,
    pub timestamp: u64,
    pub active_transactions: Vec<TransactionId>,
    pub dirty_pages: Vec<DirtyPage>,
    pub checkpoint_lsn: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirtyPage {
    pub page_id: u64,
    pub recovery_lsn: u64,
    pub last_update_lsn: u64,
}

pub struct RecoveryManager {
    strategy: RecoveryStrategy,
    transaction_log: Arc<super::transaction_log::TransactionLog>,
    checkpoint_manager: Arc<CheckpointManager>,
    aries_recovery: Arc<ARIESRecovery>,
    shadow_paging: Arc<ShadowPagingRecovery>,
    storage: Arc<dyn crate::core::storage::PageManagerAsync>,
    metrics: Arc<RecoveryMetrics>,
}

pub struct CheckpointManager {
    checkpoint_interval: Duration,
    last_checkpoint: Arc<RwLock<Instant>>,
    checkpoint_in_progress: Arc<std::sync::atomic::AtomicBool>,
    checkpoints: Arc<DashMap<u64, Checkpoint>>,
    active_transactions: Arc<DashMap<TransactionId, TransactionSnapshot>>,
    dirty_page_table: Arc<DashMap<u64, DirtyPageEntry>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    checkpoint_id: u64,
    begin_lsn: u64,
    end_lsn: u64,
    timestamp: u64,
    transaction_table: HashMap<TransactionId, TransactionSnapshot>,
    dirty_page_table: HashMap<u64, DirtyPageEntry>,
    completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransactionSnapshot {
    txn_id: TransactionId,
    state: TransactionState,
    first_lsn: u64,
    last_lsn: u64,
    undo_next_lsn: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirtyPageEntry {
    page_id: u64,
    recovery_lsn: u64,
    oldest_update: u64,
}

struct ARIESRecovery {
    analysis_phase: Arc<AnalysisPhase>,
    redo_phase: Arc<RedoPhase>,
    undo_phase: Arc<UndoPhase>,
    recovery_state: Arc<RwLock<ARIESState>>,
}

struct AnalysisPhase {
    transaction_table: Arc<DashMap<TransactionId, TransactionAnalysis>>,
    dirty_page_table: Arc<DashMap<u64, DirtyPageAnalysis>>,
    redo_lsn: Arc<std::sync::atomic::AtomicU64>,
}

struct TransactionAnalysis {
    txn_id: TransactionId,
    state: TransactionRecoveryState,
    last_lsn: u64,
    undo_next_lsn: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionRecoveryState {
    Active,
    Committing,
    Aborting,
    Committed,
    Aborted,
}

struct DirtyPageAnalysis {
    page_id: u64,
    recovery_lsn: u64,
}

struct RedoPhase {
    redo_operations: Arc<RwLock<Vec<RedoOperation>>>,
    pages_recovered: Arc<DashMap<u64, PageRecoveryInfo>>,
    redo_progress: Arc<std::sync::atomic::AtomicU64>,
}

struct RedoOperation {
    lsn: u64,
    page_id: u64,
    operation: super::transaction_log::RedoOperation,
    txn_id: TransactionId,
}

struct PageRecoveryInfo {
    page_id: u64,
    page_lsn: u64,
    recovered: bool,
    recovery_time: Instant,
}

struct UndoPhase {
    undo_list: Arc<RwLock<Vec<TransactionId>>>,
    compensation_log: Arc<DashMap<u64, CompensationLogRecord>>,
    undo_progress: Arc<std::sync::atomic::AtomicU64>,
}

struct CompensationLogRecord {
    clr_lsn: u64,
    txn_id: TransactionId,
    undo_next_lsn: Option<u64>,
    compensated_lsn: u64,
}

struct ARIESState {
    phase: RecoveryPhase,
    start_time: Instant,
    analysis_complete: bool,
    redo_complete: bool,
    undo_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryPhase {
    NotStarted,
    Analysis,
    Redo,
    Undo,
    Complete,
}

struct ShadowPagingRecovery {
    shadow_directory: Arc<RwLock<ShadowDirectory>>,
    page_table: Arc<DashMap<u64, ShadowPage>>,
    commit_table: Arc<DashMap<TransactionId, CommitRecord>>,
}

struct ShadowDirectory {
    current_version: u64,
    shadow_version: u64,
    page_mappings: HashMap<u64, PageMapping>,
}

struct PageMapping {
    logical_page: u64,
    physical_page: u64,
    version: u64,
}

struct ShadowPage {
    page_id: u64,
    shadow_id: u64,
    original_data: Bytes,
    shadow_data: Bytes,
    txn_id: TransactionId,
}

struct CommitRecord {
    txn_id: TransactionId,
    commit_time: u64,
    pages_modified: Vec<u64>,
    committed: bool,
}

struct RecoveryMetrics {
    recovery_count: Arc<std::sync::atomic::AtomicU64>,
    recovery_time_ms: Arc<std::sync::atomic::AtomicU64>,
    pages_recovered: Arc<std::sync::atomic::AtomicU64>,
    transactions_recovered: Arc<std::sync::atomic::AtomicU64>,
    checkpoints_created: Arc<std::sync::atomic::AtomicU64>,
    redo_operations: Arc<std::sync::atomic::AtomicU64>,
    undo_operations: Arc<std::sync::atomic::AtomicU64>,
}

impl RecoveryManager {
    pub fn new(
        strategy: RecoveryStrategy,
        transaction_log: Arc<super::transaction_log::TransactionLog>,
        storage: Arc<dyn crate::core::storage::PageManagerAsync>,
    ) -> Self {
        Self {
            strategy,
            transaction_log: transaction_log.clone(),
            checkpoint_manager: Arc::new(CheckpointManager {
                checkpoint_interval: Duration::from_secs(300),
                last_checkpoint: Arc::new(RwLock::new(Instant::now())),
                checkpoint_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                checkpoints: Arc::new(DashMap::new()),
                active_transactions: Arc::new(DashMap::new()),
                dirty_page_table: Arc::new(DashMap::new()),
            }),
            aries_recovery: Arc::new(ARIESRecovery {
                analysis_phase: Arc::new(AnalysisPhase {
                    transaction_table: Arc::new(DashMap::new()),
                    dirty_page_table: Arc::new(DashMap::new()),
                    redo_lsn: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                }),
                redo_phase: Arc::new(RedoPhase {
                    redo_operations: Arc::new(RwLock::new(Vec::new())),
                    pages_recovered: Arc::new(DashMap::new()),
                    redo_progress: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                }),
                undo_phase: Arc::new(UndoPhase {
                    undo_list: Arc::new(RwLock::new(Vec::new())),
                    compensation_log: Arc::new(DashMap::new()),
                    undo_progress: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                }),
                recovery_state: Arc::new(RwLock::new(ARIESState {
                    phase: RecoveryPhase::NotStarted,
                    start_time: Instant::now(),
                    analysis_complete: false,
                    redo_complete: false,
                    undo_complete: false,
                })),
            }),
            shadow_paging: Arc::new(ShadowPagingRecovery {
                shadow_directory: Arc::new(RwLock::new(ShadowDirectory {
                    current_version: 0,
                    shadow_version: 0,
                    page_mappings: HashMap::new(),
                })),
                page_table: Arc::new(DashMap::new()),
                commit_table: Arc::new(DashMap::new()),
            }),
            storage,
            metrics: Arc::new(RecoveryMetrics {
                recovery_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                recovery_time_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                pages_recovered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                transactions_recovered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                checkpoints_created: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                redo_operations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                undo_operations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
        }
    }

    pub async fn recover(&self) -> Result<RecoveryResult> {
        let start_time = Instant::now();
        self.metrics
            .recovery_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let result = match self.strategy {
            RecoveryStrategy::ARIES => self.aries_recover().await?,
            RecoveryStrategy::ShadowPaging => self.shadow_recover().await?,
            RecoveryStrategy::Logging => self.log_based_recover().await?,
            RecoveryStrategy::Checkpointing => self.checkpoint_recover().await?,
            RecoveryStrategy::Hybrid => self.hybrid_recover().await?,
        };

        let elapsed = start_time.elapsed().as_millis() as u64;
        self.metrics
            .recovery_time_ms
            .store(elapsed, std::sync::atomic::Ordering::Relaxed);

        Ok(result)
    }

    async fn aries_recover(&self) -> Result<RecoveryResult> {
        let mut state = self.aries_recovery.recovery_state.write().await;
        state.phase = RecoveryPhase::Analysis;
        state.start_time = Instant::now();
        drop(state);

        let checkpoint_lsn = self.find_last_checkpoint().await?;

        self.aries_analysis(checkpoint_lsn).await?;

        let redo_lsn = self
            .aries_recovery
            .analysis_phase
            .redo_lsn
            .load(std::sync::atomic::Ordering::SeqCst);

        self.aries_redo(redo_lsn).await?;

        self.aries_undo().await?;

        let mut state = self.aries_recovery.recovery_state.write().await;
        state.phase = RecoveryPhase::Complete;

        Ok(RecoveryResult {
            recovered_transactions: self.get_recovered_transactions().await,
            recovered_pages: self
                .metrics
                .pages_recovered
                .load(std::sync::atomic::Ordering::Relaxed),
            recovery_time: state.start_time.elapsed(),
            recovery_point: RecoveryPoint {
                lsn: redo_lsn,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                active_transactions: Vec::new(),
                dirty_pages: Vec::new(),
                checkpoint_lsn,
            },
        })
    }

    async fn aries_analysis(&self, checkpoint_lsn: u64) -> Result<()> {
        let mut state = self.aries_recovery.recovery_state.write().await;
        state.phase = RecoveryPhase::Analysis;
        drop(state);

        if checkpoint_lsn > 0 {
            if let Some(checkpoint) = self.checkpoint_manager.checkpoints.get(&checkpoint_lsn) {
                for (txn_id, snapshot) in &checkpoint.transaction_table {
                    self.aries_recovery.analysis_phase.transaction_table.insert(
                        *txn_id,
                        TransactionAnalysis {
                            txn_id: *txn_id,
                            state: TransactionRecoveryState::Active,
                            last_lsn: snapshot.last_lsn,
                            undo_next_lsn: snapshot.undo_next_lsn,
                        },
                    );
                }

                for (page_id, entry) in &checkpoint.dirty_page_table {
                    self.aries_recovery.analysis_phase.dirty_page_table.insert(
                        *page_id,
                        DirtyPageAnalysis {
                            page_id: *page_id,
                            recovery_lsn: entry.recovery_lsn,
                        },
                    );
                }
            }
        }

        let log_records = self
            .transaction_log
            .recover(Some(checkpoint_lsn))
            .await?
            .transactions_recovered;

        self.metrics.transactions_recovered.store(
            log_records.len() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let min_recovery_lsn = self
            .aries_recovery
            .analysis_phase
            .dirty_page_table
            .iter()
            .map(|entry| entry.value().recovery_lsn)
            .min()
            .unwrap_or(checkpoint_lsn);

        self.aries_recovery
            .analysis_phase
            .redo_lsn
            .store(min_recovery_lsn, std::sync::atomic::Ordering::SeqCst);

        let mut state = self.aries_recovery.recovery_state.write().await;
        state.analysis_complete = true;

        Ok(())
    }

    async fn aries_redo(&self, redo_lsn: u64) -> Result<()> {
        let mut state = self.aries_recovery.recovery_state.write().await;
        state.phase = RecoveryPhase::Redo;
        drop(state);

        let recovery_info = self.transaction_log.recover(Some(redo_lsn)).await?;

        for redo_op in recovery_info.redo_operations {
            if let Some(dirty_page) = self
                .aries_recovery
                .analysis_phase
                .dirty_page_table
                .get(&redo_op.page_id)
            {
                let page_data = self
                    .storage
                    .read_page(redo_op.key.as_bytes().to_vec())
                    .await
                    .unwrap_or_default();

                let page_lsn = self.get_page_lsn(&page_data);

                if page_lsn < dirty_page.recovery_lsn {
                    self.apply_redo_operation(&redo_op).await?;

                    self.aries_recovery.redo_phase.pages_recovered.insert(
                        redo_op.page_id,
                        PageRecoveryInfo {
                            page_id: redo_op.page_id,
                            page_lsn,
                            recovered: true,
                            recovery_time: Instant::now(),
                        },
                    );

                    self.metrics
                        .pages_recovered
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.metrics
                        .redo_operations
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        let mut state = self.aries_recovery.recovery_state.write().await;
        state.redo_complete = true;

        Ok(())
    }

    async fn aries_undo(&self) -> Result<()> {
        let mut state = self.aries_recovery.recovery_state.write().await;
        state.phase = RecoveryPhase::Undo;
        drop(state);

        let mut undo_list = Vec::new();
        for entry in self.aries_recovery.analysis_phase.transaction_table.iter() {
            if entry.value().state == TransactionRecoveryState::Active
                || entry.value().state == TransactionRecoveryState::Aborting
            {
                undo_list.push(*entry.key());
            }
        }

        *self.aries_recovery.undo_phase.undo_list.write().await = undo_list.clone();

        while !undo_list.is_empty() {
            let mut max_lsn = 0;
            let mut max_txn = None;

            for txn_id in &undo_list {
                if let Some(txn) = self
                    .aries_recovery
                    .analysis_phase
                    .transaction_table
                    .get(txn_id)
                {
                    if txn.last_lsn > max_lsn {
                        max_lsn = txn.last_lsn;
                        max_txn = Some(*txn_id);
                    }
                }
            }

            if let Some(txn_id) = max_txn {
                let recovery_info = self.transaction_log.recover(Some(max_lsn)).await?;

                for undo_op in recovery_info.undo_operations {
                    self.apply_undo_operation(&undo_op).await?;

                    self.write_compensation_log(txn_id, max_lsn).await?;

                    self.metrics
                        .undo_operations
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                undo_list.retain(|&id| id != txn_id);
            } else {
                break;
            }
        }

        let mut state = self.aries_recovery.recovery_state.write().await;
        state.undo_complete = true;

        Ok(())
    }

    async fn apply_redo_operation(&self, op: &super::transaction_log::RedoOperation) -> Result<()> {
        self.storage
            .write_page(op.key.as_bytes().to_vec(), vec![])
            .await?;

        Ok(())
    }

    async fn apply_undo_operation(&self, op: &super::transaction_log::UndoOperation) -> Result<()> {
        self.storage
            .write_page(op.key.as_bytes().to_vec(), vec![])
            .await?;

        Ok(())
    }

    async fn write_compensation_log(
        &self,
        txn_id: TransactionId,
        compensated_lsn: u64,
    ) -> Result<()> {
        let clr_lsn = self
            .transaction_log
            .write(
                super::transaction_log::LogType::Compensation,
                txn_id,
                super::transaction_log::LogData::Compensation {
                    compensated_lsn,
                    operation: super::transaction_log::CompensationOperation {
                        original_operation: super::transaction_log::UndoOperation {
                            key: String::new(),
                            operation_type: super::participant::OperationType::Write,
                            page_id: 0,
                            offset: 0,
                            length: 0,
                        },
                        compensation_type: super::transaction_log::CompensationType::Logical,
                    },
                },
            )
            .await?;

        self.aries_recovery.undo_phase.compensation_log.insert(
            clr_lsn,
            CompensationLogRecord {
                clr_lsn,
                txn_id,
                undo_next_lsn: None,
                compensated_lsn,
            },
        );

        Ok(())
    }

    fn get_page_lsn(&self, page_data: &[u8]) -> u64 {
        if page_data.len() >= 8 {
            u64::from_le_bytes(page_data[0..8].try_into().unwrap_or([0; 8]))
        } else {
            0
        }
    }

    async fn find_last_checkpoint(&self) -> Result<u64> {
        let checkpoints: Vec<_> = self
            .checkpoint_manager
            .checkpoints
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        Ok(checkpoints
            .iter()
            .filter(|(_, cp)| cp.completed)
            .map(|(lsn, _)| *lsn)
            .max()
            .unwrap_or(0))
    }

    async fn shadow_recover(&self) -> Result<RecoveryResult> {
        let directory = self.shadow_paging.shadow_directory.read().await;

        let mut recovered_pages = 0;
        for mapping in directory.page_mappings.values() {
            if mapping.version < directory.current_version {
                recovered_pages += 1;
            }
        }

        self.metrics
            .pages_recovered
            .store(recovered_pages, std::sync::atomic::Ordering::Relaxed);

        Ok(RecoveryResult {
            recovered_transactions: Vec::new(),
            recovered_pages,
            recovery_time: Duration::from_secs(0),
            recovery_point: RecoveryPoint {
                lsn: 0,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                active_transactions: Vec::new(),
                dirty_pages: Vec::new(),
                checkpoint_lsn: 0,
            },
        })
    }

    async fn log_based_recover(&self) -> Result<RecoveryResult> {
        let recovery_info = self.transaction_log.recover(None).await?;

        Ok(RecoveryResult {
            recovered_transactions: recovery_info.transactions_recovered,
            recovered_pages: 0,
            recovery_time: Duration::from_secs(0),
            recovery_point: RecoveryPoint {
                lsn: recovery_info.end_lsn,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                active_transactions: Vec::new(),
                dirty_pages: Vec::new(),
                checkpoint_lsn: recovery_info.start_lsn,
            },
        })
    }

    async fn checkpoint_recover(&self) -> Result<RecoveryResult> {
        let checkpoint_lsn = self.find_last_checkpoint().await?;

        if let Some(checkpoint) = self.checkpoint_manager.checkpoints.get(&checkpoint_lsn) {
            let recovered_txns: Vec<_> = checkpoint.transaction_table.keys().cloned().collect();

            Ok(RecoveryResult {
                recovered_transactions: recovered_txns,
                recovered_pages: checkpoint.dirty_page_table.len() as u64,
                recovery_time: Duration::from_secs(0),
                recovery_point: RecoveryPoint {
                    lsn: checkpoint.end_lsn,
                    timestamp: checkpoint.timestamp,
                    active_transactions: Vec::new(),
                    dirty_pages: Vec::new(),
                    checkpoint_lsn,
                },
            })
        } else {
            Ok(RecoveryResult::default())
        }
    }

    async fn hybrid_recover(&self) -> Result<RecoveryResult> {
        let checkpoint_result = self.checkpoint_recover().await?;
        let aries_result = self.aries_recover().await?;

        Ok(RecoveryResult {
            recovered_transactions: aries_result.recovered_transactions,
            recovered_pages: aries_result.recovered_pages + checkpoint_result.recovered_pages,
            recovery_time: aries_result.recovery_time,
            recovery_point: aries_result.recovery_point,
        })
    }

    async fn get_recovered_transactions(&self) -> Vec<TransactionId> {
        self.aries_recovery
            .analysis_phase
            .transaction_table
            .iter()
            .map(|entry| *entry.key())
            .collect()
    }
}

impl CheckpointManager {
    pub async fn create_checkpoint(&self) -> Result<u64> {
        if self
            .checkpoint_in_progress
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(Error::Generic("Checkpoint already in progress".to_string()));
        }

        let checkpoint_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let transaction_table: HashMap<_, _> = self
            .active_transactions
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        let dirty_page_table: HashMap<_, _> = self
            .dirty_page_table
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        let checkpoint = Checkpoint {
            checkpoint_id,
            begin_lsn: 0,
            end_lsn: 0,
            timestamp: checkpoint_id,
            transaction_table,
            dirty_page_table,
            completed: false,
        };

        self.checkpoints.insert(checkpoint_id, checkpoint);

        self.checkpoints.alter(&checkpoint_id, |_, mut cp| {
            cp.completed = true;
            cp
        });

        *self.last_checkpoint.write().await = Instant::now();
        self.checkpoint_in_progress
            .store(false, std::sync::atomic::Ordering::SeqCst);

        Ok(checkpoint_id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct RecoveryResult {
    pub recovered_transactions: Vec<TransactionId>,
    pub recovered_pages: u64,
    pub recovery_time: Duration,
    pub recovery_point: RecoveryPoint,
}

impl Default for RecoveryPoint {
    fn default() -> Self {
        Self {
            lsn: 0,
            timestamp: 0,
            active_transactions: Vec::new(),
            dirty_pages: Vec::new(),
            checkpoint_lsn: 0,
        }
    }
}

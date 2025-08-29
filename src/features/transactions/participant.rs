use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use crate::core::error::{Error, Result};
use dashmap::DashMap;
use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VoteDecision {
    Commit,
    Abort,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ParticipantState {
    Initial,
    Working,
    Prepared,
    Committed,
    Aborted,
    Uncertain,
    Recovering,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantInfo {
    pub node_id: String,
    pub endpoint: String,
    pub weight: f64,
    pub capabilities: HashSet<String>,
    pub max_concurrent_txns: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareRequest {
    pub txn_id: super::TransactionId,
    pub operations: Vec<Operation>,
    pub read_set: HashSet<String>,
    pub write_set: HashSet<String>,
    pub timestamp: u64,
    pub isolation_level: super::isolation::IsolationLevel,
    pub coordinator_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareResponse {
    pub vote: VoteDecision,
    pub prepared_at: u64,
    pub undo_log: Option<Vec<UndoRecord>>,
    pub locks_held: Vec<String>,
    pub conflict_info: Option<ConflictInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitRequest {
    pub txn_id: super::TransactionId,
    pub commit_timestamp: u64,
    pub decision: Decision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitResponse {
    pub committed: bool,
    pub commit_time: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub op_type: OperationType,
    pub key: String,
    pub value: Option<Vec<u8>>,
    pub condition: Option<Condition>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    Read,
    Write,
    Update,
    Delete,
    Lock,
    Unlock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub condition_type: ConditionType,
    pub expected_value: Option<Vec<u8>>,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionType {
    Exists,
    NotExists,
    ValueEquals,
    VersionEquals,
    VersionGreaterThan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoRecord {
    pub key: String,
    pub old_value: Option<Vec<u8>>,
    pub new_value: Option<Vec<u8>>,
    pub operation: OperationType,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictInfo {
    pub conflicting_txn: super::TransactionId,
    pub conflicting_keys: Vec<String>,
    pub conflict_type: ConflictType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictType {
    WriteWrite,
    WriteRead,
    ReadWrite,
    Deadlock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Commit,
    Abort,
}

#[async_trait]
pub trait ParticipantProtocol: Send + Sync {
    async fn prepare(&self, request: PrepareRequest) -> Result<PrepareResponse>;
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse>;
    async fn abort(&self, txn_id: super::TransactionId) -> Result<()>;
    async fn query_status(&self, txn_id: super::TransactionId) -> Result<ParticipantState>;
    async fn recover(&self) -> Result<Vec<super::TransactionId>>;
}

pub struct Participant {
    info: ParticipantInfo,
    state: Arc<RwLock<HashMap<super::TransactionId, ParticipantTransaction>>>,
    prepared_txns: Arc<DashMap<super::TransactionId, PreparedTransaction>>,
    lock_manager: Arc<LockManager>,
    undo_log: Arc<UndoLog>,
    storage: Arc<dyn crate::core::storage::PageManagerAsync>,
    isolation_manager: Arc<super::isolation::IsolationManager>,
    metrics: Arc<ParticipantMetrics>,
    recovery_log: Arc<RecoveryLog>,
}

struct ParticipantTransaction {
    txn_id: super::TransactionId,
    state: ParticipantState,
    operations: Vec<Operation>,
    locks: HashSet<String>,
    undo_records: Vec<UndoRecord>,
    start_time: Instant,
    prepare_time: Option<Instant>,
    commit_time: Option<Instant>,
}

#[derive(Clone)]
struct PreparedTransaction {
    txn_id: super::TransactionId,
    prepare_request: PrepareRequest,
    prepare_response: PrepareResponse,
    prepared_at: Instant,
    coordinator_id: String,
    timeout_at: Instant,
}

struct LockManager {
    locks: Arc<DashMap<String, LockInfo>>,
    wait_queue: Arc<RwLock<HashMap<String, Vec<LockRequest>>>>,
    deadlock_detector: Arc<super::deadlock::DeadlockDetector>,
}

struct LockInfo {
    holder: super::TransactionId,
    lock_type: LockType,
    acquired_at: Instant,
    waiters: Vec<super::TransactionId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockType {
    Shared,
    Exclusive,
    IntentShared,
    IntentExclusive,
    ShareIntentExclusive,
}

struct LockRequest {
    txn_id: super::TransactionId,
    key: String,
    lock_type: LockType,
    requested_at: Instant,
    timeout: Duration,
}

struct UndoLog {
    log: Arc<DashMap<super::TransactionId, Vec<UndoRecord>>>,
    persistent_log: Arc<dyn crate::core::storage::PageManagerAsync>,
    checkpoint_interval: Duration,
    last_checkpoint: Arc<RwLock<Instant>>,
}

struct RecoveryLog {
    prepared_txns: Arc<DashMap<super::TransactionId, PreparedTransactionLog>>,
    decision_log: Arc<DashMap<super::TransactionId, DecisionLog>>,
    storage: Arc<dyn crate::core::storage::PageManagerAsync>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PreparedTransactionLog {
    txn_id: super::TransactionId,
    coordinator_id: String,
    prepare_request: PrepareRequest,
    prepare_response: PrepareResponse,
    prepared_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionLog {
    txn_id: super::TransactionId,
    decision: Decision,
    decided_at: u64,
    executed: bool,
}

struct ParticipantMetrics {
    total_prepares: Arc<std::sync::atomic::AtomicU64>,
    total_commits: Arc<std::sync::atomic::AtomicU64>,
    total_aborts: Arc<std::sync::atomic::AtomicU64>,
    active_txns: Arc<std::sync::atomic::AtomicU64>,
    prepared_txns: Arc<std::sync::atomic::AtomicU64>,
    lock_conflicts: Arc<std::sync::atomic::AtomicU64>,
    deadlocks_detected: Arc<std::sync::atomic::AtomicU64>,
    recovery_count: Arc<std::sync::atomic::AtomicU64>,
}

impl Participant {
    pub fn new(
        info: ParticipantInfo,
        storage: Arc<dyn crate::core::storage::PageManagerAsync>,
        isolation_manager: Arc<super::isolation::IsolationManager>,
    ) -> Self {
        let deadlock_detector = Arc::new(super::deadlock::DeadlockDetector::new(
            info.node_id.clone(),
            Duration::from_secs(5),
        ));
        
        Self {
            info,
            state: Arc::new(RwLock::new(HashMap::new())),
            prepared_txns: Arc::new(DashMap::new()),
            lock_manager: Arc::new(LockManager {
                locks: Arc::new(DashMap::new()),
                wait_queue: Arc::new(RwLock::new(HashMap::new())),
                deadlock_detector,
            }),
            undo_log: Arc::new(UndoLog {
                log: Arc::new(DashMap::new()),
                persistent_log: storage.clone(),
                checkpoint_interval: Duration::from_secs(60),
                last_checkpoint: Arc::new(RwLock::new(Instant::now())),
            }),
            storage: storage.clone(),
            isolation_manager,
            metrics: Arc::new(ParticipantMetrics {
                total_prepares: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                total_commits: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                total_aborts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                active_txns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                prepared_txns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                lock_conflicts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                deadlocks_detected: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                recovery_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
            recovery_log: Arc::new(RecoveryLog {
                prepared_txns: Arc::new(DashMap::new()),
                decision_log: Arc::new(DashMap::new()),
                storage,
            }),
        }
    }

    async fn acquire_locks(
        &self,
        txn_id: super::TransactionId,
        operations: &[Operation],
    ) -> Result<Vec<String>> {
        let mut locked_keys = Vec::new();
        
        for op in operations {
            let lock_type = match op.op_type {
                OperationType::Read => LockType::Shared,
                OperationType::Write | OperationType::Update | OperationType::Delete => LockType::Exclusive,
                OperationType::Lock => LockType::Exclusive,
                OperationType::Unlock => continue,
            };
            
            if let Some(existing) = self.lock_manager.locks.get(&op.key) {
                if existing.holder != txn_id {
                    if !self.can_grant_lock(&existing, lock_type) {
                        self.metrics.lock_conflicts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        
                        for key in &locked_keys {
                            self.lock_manager.locks.remove(key);
                        }
                        
                        return Err(Error::Custom("Lock conflict detected".to_string()));
                    }
                }
            }
            
            self.lock_manager.locks.insert(
                op.key.clone(),
                LockInfo {
                    holder: txn_id,
                    lock_type,
                    acquired_at: Instant::now(),
                    waiters: Vec::new(),
                },
            );
            
            locked_keys.push(op.key.clone());
        }
        
        Ok(locked_keys)
    }

    fn can_grant_lock(&self, existing: &LockInfo, requested: LockType) -> bool {
        match (existing.lock_type, requested) {
            (LockType::Shared, LockType::Shared) => true,
            (LockType::Shared, LockType::IntentShared) => true,
            (LockType::IntentShared, LockType::Shared) => true,
            (LockType::IntentShared, LockType::IntentShared) => true,
            _ => false,
        }
    }

    async fn execute_operations(
        &self,
        txn_id: super::TransactionId,
        operations: &[Operation],
    ) -> Result<Vec<UndoRecord>> {
        let mut undo_records = Vec::new();
        
        for op in operations {
            let undo = match op.op_type {
                OperationType::Write => {
                    let old_value = self.storage.read_page(op.key.as_bytes().to_vec()).await.ok();
                    
                    self.storage.write_page(
                        op.key.as_bytes().to_vec(),
                        op.value.clone().unwrap_or_default().to_vec(),
                    ).await?;
                    
                    UndoRecord {
                        key: op.key.clone(),
                        old_value: old_value,
                        new_value: op.value.clone(),
                        operation: op.op_type,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    }
                }
                OperationType::Update => {
                    let old_value = self.storage.read_page(op.key.as_bytes().to_vec()).await?;
                    
                    self.storage.write_page(
                        op.key.as_bytes().to_vec(),
                        op.value.clone().unwrap_or_default().to_vec(),
                    ).await?;
                    
                    UndoRecord {
                        key: op.key.clone(),
                        old_value: Some(old_value),
                        new_value: op.value.clone(),
                        operation: op.op_type,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    }
                }
                OperationType::Delete => {
                    let old_value = self.storage.read_page(op.key.as_bytes().to_vec()).await?;
                    
                    self.storage.delete_page(op.key.as_bytes().to_vec()).await?;
                    
                    UndoRecord {
                        key: op.key.clone(),
                        old_value: Some(old_value),
                        new_value: None,
                        operation: op.op_type,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs(),
                    }
                }
                _ => continue,
            };
            
            undo_records.push(undo);
        }
        
        self.undo_log.log.insert(txn_id, undo_records.clone());
        
        Ok(undo_records)
    }

    async fn rollback(&self, txn_id: super::TransactionId) -> Result<()> {
        if let Some((_, undo_records)) = self.undo_log.log.remove(&txn_id) {
            for record in undo_records.iter().rev() {
                match record.operation {
                    OperationType::Write | OperationType::Update => {
                        if let Some(old_value) = &record.old_value {
                            self.storage.write_page(
                                record.key.as_bytes().to_vec(),
                                old_value.to_vec(),
                            ).await?;
                        } else {
                            self.storage.delete_page(record.key.as_bytes().to_vec()).await?;
                        }
                    }
                    OperationType::Delete => {
                        if let Some(old_value) = &record.old_value {
                            self.storage.write_page(
                                record.key.as_bytes().to_vec(),
                                old_value.to_vec(),
                            ).await?;
                        }
                    }
                    _ => {}
                }
            }
        }
        
        Ok(())
    }

    async fn release_locks(&self, txn_id: super::TransactionId) {
        let mut locks_to_remove = Vec::new();
        
        for entry in self.lock_manager.locks.iter() {
            if entry.value().holder == txn_id {
                locks_to_remove.push(entry.key().clone());
            }
        }
        
        for key in locks_to_remove {
            self.lock_manager.locks.remove(&key);
        }
    }

    pub async fn handle_recovery(&self) -> Result<Vec<super::TransactionId>> {
        self.metrics.recovery_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        let mut recovered_txns = Vec::new();
        
        for entry in self.recovery_log.prepared_txns.iter() {
            let log = entry.value();
            
            if let Some(decision) = self.recovery_log.decision_log.get(&log.txn_id) {
                if !decision.executed {
                    match decision.decision {
                        Decision::Commit => {
                            self.commit(CommitRequest {
                                txn_id: log.txn_id,
                                commit_timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                decision: Decision::Commit,
                            }).await?;
                        }
                        Decision::Abort => {
                            self.abort(log.txn_id).await?;
                        }
                    }
                    
                    self.recovery_log.decision_log.alter(&log.txn_id, |_, mut v| {
                        v.executed = true;
                        v
                    });
                }
            } else {
                recovered_txns.push(log.txn_id);
            }
        }
        
        Ok(recovered_txns)
    }
}

#[async_trait]
impl ParticipantProtocol for Participant {
    async fn prepare(&self, request: PrepareRequest) -> Result<PrepareResponse> {
        self.metrics.total_prepares.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.active_txns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        let mut state = self.state.write().await;
        state.insert(
            request.txn_id,
            ParticipantTransaction {
                txn_id: request.txn_id,
                state: ParticipantState::Working,
                operations: request.operations.clone(),
                locks: HashSet::new(),
                undo_records: Vec::new(),
                start_time: Instant::now(),
                prepare_time: None,
                commit_time: None,
            },
        );
        drop(state);
        
        let locked_keys = match self.acquire_locks(request.txn_id, &request.operations).await {
            Ok(keys) => keys,
            Err(e) => {
                self.metrics.active_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(PrepareResponse {
                    vote: VoteDecision::Abort,
                    prepared_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
                    undo_log: None,
                    locks_held: Vec::new(),
                    conflict_info: Some(ConflictInfo {
                        conflicting_txn: super::TransactionId::new(),
                        conflicting_keys: Vec::new(),
                        conflict_type: ConflictType::WriteWrite,
                    }),
                });
            }
        };
        
        let undo_records = match self.execute_operations(request.txn_id, &request.operations).await {
            Ok(records) => records,
            Err(e) => {
                self.release_locks(request.txn_id).await;
                self.metrics.active_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(PrepareResponse {
                    vote: VoteDecision::Abort,
                    prepared_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
                    undo_log: None,
                    locks_held: Vec::new(),
                    conflict_info: None,
                });
            }
        };
        
        let prepared_txn = PreparedTransaction {
            txn_id: request.txn_id,
            prepare_request: request.clone(),
            prepare_response: PrepareResponse {
                vote: VoteDecision::Commit,
                prepared_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
                undo_log: Some(undo_records.clone()),
                locks_held: locked_keys.clone(),
                conflict_info: None,
            },
            prepared_at: Instant::now(),
            coordinator_id: request.coordinator_id.clone(),
            timeout_at: Instant::now() + self.info.timeout,
        };
        
        self.prepared_txns.insert(request.txn_id, prepared_txn.clone());
        
        self.recovery_log.prepared_txns.insert(
            request.txn_id,
            PreparedTransactionLog {
                txn_id: request.txn_id,
                coordinator_id: request.coordinator_id.clone(),
                prepare_request: request,
                prepare_response: prepared_txn.prepare_response.clone(),
                prepared_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );
        
        let mut state = self.state.write().await;
        if let Some(txn) = state.get_mut(&prepared_txn.txn_id) {
            txn.state = ParticipantState::Prepared;
            txn.prepare_time = Some(Instant::now());
            txn.locks = locked_keys.iter().cloned().collect();
            txn.undo_records = undo_records;
        }
        
        self.metrics.prepared_txns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(prepared_txn.prepare_response)
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse> {
        self.metrics.total_commits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        if let Some((_, prepared)) = self.prepared_txns.remove(&request.txn_id) {
            self.undo_log.log.remove(&request.txn_id);
            
            self.release_locks(request.txn_id).await;
            
            self.recovery_log.decision_log.insert(
                request.txn_id,
                DecisionLog {
                    txn_id: request.txn_id,
                    decision: request.decision,
                    decided_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    executed: true,
                },
            );
            
            self.recovery_log.prepared_txns.remove(&request.txn_id);
            
            let mut state = self.state.write().await;
            if let Some(txn) = state.get_mut(&request.txn_id) {
                txn.state = ParticipantState::Committed;
                txn.commit_time = Some(Instant::now());
            }
            
            self.metrics.prepared_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            self.metrics.active_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            
            Ok(CommitResponse {
                committed: true,
                commit_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                error: None,
            })
        } else {
            Ok(CommitResponse {
                committed: false,
                commit_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                error: Some("Transaction not in prepared state".to_string()),
            })
        }
    }

    async fn abort(&self, txn_id: super::TransactionId) -> Result<()> {
        self.metrics.total_aborts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        self.rollback(txn_id).await?;
        
        self.release_locks(txn_id).await;
        
        self.prepared_txns.remove(&txn_id);
        self.undo_log.log.remove(&txn_id);
        
        self.recovery_log.decision_log.insert(
            txn_id,
            DecisionLog {
                txn_id,
                decision: Decision::Abort,
                decided_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                executed: true,
            },
        );
        
        self.recovery_log.prepared_txns.remove(&txn_id);
        
        let mut state = self.state.write().await;
        if let Some(txn) = state.get_mut(&txn_id) {
            txn.state = ParticipantState::Aborted;
        }
        
        if self.metrics.prepared_txns.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.metrics.prepared_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.metrics.active_txns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }

    async fn query_status(&self, txn_id: super::TransactionId) -> Result<ParticipantState> {
        let state = self.state.read().await;
        
        if let Some(txn) = state.get(&txn_id) {
            Ok(txn.state)
        } else if self.prepared_txns.contains_key(&txn_id) {
            Ok(ParticipantState::Prepared)
        } else {
            Ok(ParticipantState::Uncertain)
        }
    }

    async fn recover(&self) -> Result<Vec<super::TransactionId>> {
        self.handle_recovery().await
    }
}
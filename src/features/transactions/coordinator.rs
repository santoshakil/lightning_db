use crate::core::error::Error;
use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransactionId(pub Uuid);

impl TransactionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionState {
    Initializing,
    Active,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Aborting,
    Aborted,
    Failed,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub isolation_level: super::isolation::IsolationLevel,
    pub participants: Vec<ParticipantInfo>,
    pub operations: Vec<Operation>,
    pub start_time: SystemTime,
    pub prepare_time: Option<SystemTime>,
    pub decision_time: Option<SystemTime>,
    pub timeout: Duration,
    pub read_timestamp: u64,
    pub commit_timestamp: Option<u64>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantInfo {
    pub id: String,
    pub address: String,
    pub state: super::participant::ParticipantState,
    pub vote: Option<super::participant::VoteDecision>,
    pub last_contact: SystemTime,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Read {
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        timestamp: u64,
    },
    Write {
        key: Vec<u8>,
        value: Vec<u8>,
        previous: Option<Vec<u8>>,
    },
    Delete {
        key: Vec<u8>,
        previous: Option<Vec<u8>>,
    },
    RangeRead {
        start: Vec<u8>,
        end: Vec<u8>,
        results: Vec<(Vec<u8>, Vec<u8>)>,
    },
}

pub struct TransactionCoordinator {
    node_id: String,
    transactions: Arc<DashMap<TransactionId, Arc<RwLock<Transaction>>>>,
    active_transactions: Arc<RwLock<HashSet<TransactionId>>>,
    prepared_transactions: Arc<RwLock<HashMap<TransactionId, PreparedState>>>,

    transaction_log: Arc<super::transaction_log::TransactionLog>,
    deadlock_detector: Arc<super::deadlock::DeadlockDetector>,
    recovery_manager: Arc<super::recovery::RecoveryManager>,

    timestamp_oracle: Arc<TimestampOracle>,
    participant_manager: Arc<ParticipantManager>,

    config: Arc<TransactionConfig>,
    metrics: Arc<TransactionMetrics>,

    decision_log: Arc<DecisionLog>,
    undo_log: Arc<UndoLog>,

    event_bus: broadcast::Sender<TransactionEvent>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct PreparedState {
    transaction_id: TransactionId,
    participants_ready: HashSet<String>,
    prepare_timestamp: SystemTime,
    timeout_at: Instant,
}

#[derive(Debug, Clone)]
pub struct TransactionConfig {
    pub max_transaction_duration: Duration,
    pub prepare_timeout: Duration,
    pub commit_timeout: Duration,
    pub max_retry_attempts: u32,
    pub retry_backoff: Duration,
    pub enable_deadlock_detection: bool,
    pub deadlock_detection_interval: Duration,
    pub enable_auto_recovery: bool,
    pub checkpoint_interval: Duration,
    pub max_concurrent_transactions: usize,
    pub enable_optimistic_concurrency: bool,
    pub conflict_resolution_strategy: ConflictResolutionStrategy,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_transaction_duration: Duration::from_secs(300),
            prepare_timeout: Duration::from_secs(30),
            commit_timeout: Duration::from_secs(10),
            max_retry_attempts: 3,
            retry_backoff: Duration::from_millis(100),
            enable_deadlock_detection: true,
            deadlock_detection_interval: Duration::from_secs(10),
            enable_auto_recovery: true,
            checkpoint_interval: Duration::from_secs(60),
            max_concurrent_transactions: 10000,
            enable_optimistic_concurrency: false,
            conflict_resolution_strategy: ConflictResolutionStrategy::FirstWins,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConflictResolutionStrategy {
    FirstWins,
    LastWins,
    HighestPriority,
    Custom,
}

struct TimestampOracle {
    current_timestamp: AtomicU64,
    last_timestamp: AtomicU64,
    clock_skew_tolerance: Duration,
}

impl TimestampOracle {
    fn new() -> Self {
        Self {
            current_timestamp: AtomicU64::new(1),
            last_timestamp: AtomicU64::new(0),
            clock_skew_tolerance: Duration::from_millis(100),
        }
    }

    fn next_timestamp(&self) -> u64 {
        let mut ts = self.current_timestamp.fetch_add(1, Ordering::SeqCst);

        let physical_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        if physical_time > ts {
            ts = physical_time;
            self.current_timestamp.store(ts + 1, Ordering::SeqCst);
        }

        ts
    }

    fn current(&self) -> u64 {
        self.current_timestamp.load(Ordering::SeqCst)
    }
}

struct ParticipantManager {
    participants: Arc<DashMap<String, Arc<dyn Participant>>>,
    connection_pool: Arc<ConnectionPool>,
    health_checker: Arc<HealthChecker>,
}

#[async_trait]
trait Participant: Send + Sync {
    async fn prepare(
        &self,
        tx_id: TransactionId,
        operations: Vec<Operation>,
    ) -> Result<super::participant::VoteDecision, Error>;

    async fn commit(&self, tx_id: TransactionId) -> Result<(), Error>;

    async fn abort(&self, tx_id: TransactionId) -> Result<(), Error>;

    async fn get_state(
        &self,
        tx_id: TransactionId,
    ) -> Result<super::participant::ParticipantState, Error>;

    async fn recover(&self, tx_id: TransactionId) -> Result<(), Error>;
}

struct ConnectionPool {
    connections: Arc<DashMap<String, Arc<Connection>>>,
    max_connections: usize,
    connection_timeout: Duration,
}

struct Connection {
    address: String,
    client: Arc<dyn TransactionClient>,
    last_used: Instant,
    in_use: AtomicBool,
}

#[async_trait]
trait TransactionClient: Send + Sync {
    async fn send_prepare(&self, request: PrepareRequest) -> Result<PrepareResponse, Error>;
    async fn send_commit(&self, request: CommitRequest) -> Result<CommitResponse, Error>;
    async fn send_abort(&self, request: AbortRequest) -> Result<AbortResponse, Error>;
    async fn send_status(&self, request: StatusRequest) -> Result<StatusResponse, Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrepareRequest {
    transaction_id: TransactionId,
    operations: Vec<Operation>,
    prepare_timestamp: u64,
    timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrepareResponse {
    vote: super::participant::VoteDecision,
    prepared_timestamp: Option<u64>,
    conflicts: Vec<ConflictInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConflictInfo {
    key: Vec<u8>,
    conflicting_transaction: TransactionId,
    conflict_type: ConflictType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum ConflictType {
    WriteWrite,
    WriteRead,
    ReadWrite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitRequest {
    transaction_id: TransactionId,
    commit_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitResponse {
    success: bool,
    commit_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AbortRequest {
    transaction_id: TransactionId,
    reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AbortResponse {
    success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusRequest {
    transaction_id: TransactionId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusResponse {
    state: super::participant::ParticipantState,
    last_update: SystemTime,
}

struct HealthChecker {
    checks: Arc<DashMap<String, HealthStatus>>,
    check_interval: Duration,
}

#[derive(Debug, Clone)]
struct HealthStatus {
    is_healthy: bool,
    last_check: Instant,
    consecutive_failures: u32,
    latency_ms: u64,
}

struct DecisionLog {
    decisions: Arc<DashMap<TransactionId, Decision>>,
    persistent_log: Arc<RwLock<Vec<Decision>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Decision {
    transaction_id: TransactionId,
    decision_type: DecisionType,
    timestamp: SystemTime,
    participants: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum DecisionType {
    Commit,
    Abort,
}

struct UndoLog {
    entries: Arc<DashMap<TransactionId, Vec<UndoEntry>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UndoEntry {
    operation: Operation,
    inverse_operation: Operation,
    timestamp: u64,
}

struct TransactionMetrics {
    total_transactions: AtomicU64,
    committed_transactions: AtomicU64,
    aborted_transactions: AtomicU64,
    active_transactions: AtomicU64,
    prepare_phase_duration_ms: AtomicU64,
    commit_phase_duration_ms: AtomicU64,
    conflicts_detected: AtomicU64,
    deadlocks_detected: AtomicU64,
    recovery_attempts: AtomicU64,
}

#[derive(Debug, Clone)]
pub enum TransactionEvent {
    Started(TransactionId),
    Prepared(TransactionId),
    Committed(TransactionId),
    Aborted(TransactionId),
    ConflictDetected {
        tx: TransactionId,
        key: Vec<u8>,
    },
    DeadlockDetected {
        victim: TransactionId,
    },
    RecoveryInitiated(TransactionId),
    ParticipantFailed {
        tx: TransactionId,
        participant: String,
    },
}

impl TransactionCoordinator {
    pub async fn new(node_id: String, config: TransactionConfig) -> Result<Self, Error> {
        let (event_tx, _) = broadcast::channel(1024);

        // Create a shared storage instance for transaction log
        // Use a mock page manager for now - in production, this would be passed in
        struct MockPageManager;

        #[async_trait::async_trait]
        impl crate::core::storage::PageManagerAsync for MockPageManager {
            async fn load_page(&self, _page_id: u64) -> Result<crate::core::storage::Page, Error> {
                unimplemented!("Mock implementation")
            }
            async fn save_page(&self, _page: &crate::core::storage::Page) -> Result<(), Error> {
                unimplemented!("Mock implementation")
            }
            async fn is_page_allocated(&self, _page_id: u64) -> Result<bool, Error> {
                Ok(false)
            }
            async fn free_page(&self, _page_id: u64) -> Result<(), Error> {
                Ok(())
            }
            async fn get_all_allocated_pages(&self) -> Result<Vec<u64>, Error> {
                Ok(vec![])
            }
            async fn read_page(&self, _key: Vec<u8>) -> Result<Vec<u8>, Error> {
                Ok(vec![])
            }
            async fn write_page(&self, _key: Vec<u8>, _data: Vec<u8>) -> Result<(), Error> {
                Ok(())
            }
            async fn delete_page(&self, _key: Vec<u8>) -> Result<(), Error> {
                Ok(())
            }
        }

        let storage: Arc<dyn crate::core::storage::PageManagerAsync> = Arc::new(MockPageManager);

        let log_config = super::transaction_log::LogConfig::default();
        let transaction_log = Arc::new(super::transaction_log::TransactionLog::new(
            storage.clone(),
            log_config,
        ));

        let deadlock_detector = Arc::new(super::deadlock::DeadlockDetector::new(
            node_id.clone(),
            config.deadlock_detection_interval,
        ));

        let recovery_manager = Arc::new(super::recovery::RecoveryManager::new(
            super::recovery::RecoveryStrategy::ARIES,
            transaction_log.clone(),
            storage.clone(),
        ));

        Ok(Self {
            node_id,
            transactions: Arc::new(DashMap::new()),
            active_transactions: Arc::new(RwLock::new(HashSet::new())),
            prepared_transactions: Arc::new(RwLock::new(HashMap::new())),
            transaction_log,
            deadlock_detector,
            recovery_manager,
            timestamp_oracle: Arc::new(TimestampOracle::new()),
            participant_manager: Arc::new(ParticipantManager::new()),
            config: Arc::new(config),
            metrics: Arc::new(TransactionMetrics {
                total_transactions: AtomicU64::new(0),
                committed_transactions: AtomicU64::new(0),
                aborted_transactions: AtomicU64::new(0),
                active_transactions: AtomicU64::new(0),
                prepare_phase_duration_ms: AtomicU64::new(0),
                commit_phase_duration_ms: AtomicU64::new(0),
                conflicts_detected: AtomicU64::new(0),
                deadlocks_detected: AtomicU64::new(0),
                recovery_attempts: AtomicU64::new(0),
            }),
            decision_log: Arc::new(DecisionLog::new()),
            undo_log: Arc::new(UndoLog::new()),
            event_bus: event_tx,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn begin_transaction(
        &self,
        isolation_level: super::isolation::IsolationLevel,
        participants: Vec<String>,
    ) -> Result<TransactionId, Error> {
        let active_count = self.metrics.active_transactions.load(Ordering::Relaxed);
        if active_count >= self.config.max_concurrent_transactions as u64 {
            return Err(Error::ResourceExhausted {
                resource: "Too many concurrent transactions".to_string(),
            });
        }

        let tx_id = TransactionId::new();
        let read_timestamp = self.timestamp_oracle.next_timestamp();

        let participant_infos: Vec<ParticipantInfo> = participants
            .iter()
            .map(|p| ParticipantInfo {
                id: p.clone(),
                address: p.clone(),
                state: super::participant::ParticipantState::Initial,
                vote: None,
                last_contact: SystemTime::now(),
                retry_count: 0,
            })
            .collect();

        let transaction = Transaction {
            id: tx_id,
            state: TransactionState::Active,
            isolation_level,
            participants: participant_infos,
            operations: Vec::new(),
            start_time: SystemTime::now(),
            prepare_time: None,
            decision_time: None,
            timeout: self.config.max_transaction_duration,
            read_timestamp,
            commit_timestamp: None,
            metadata: HashMap::new(),
        };

        self.transactions
            .insert(tx_id, Arc::new(RwLock::new(transaction)));
        self.active_transactions.write().insert(tx_id);

        self.transaction_log
            .log_begin(tx_id, isolation_level, participants)
            .await?;

        self.metrics
            .total_transactions
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_transactions
            .fetch_add(1, Ordering::Relaxed);

        let _ = self.event_bus.send(TransactionEvent::Started(tx_id));

        if self.config.enable_deadlock_detection {
            self.deadlock_detector.add_transaction(tx_id, 0).await.ok();
        }

        Ok(tx_id)
    }

    pub async fn prepare(&self, tx_id: TransactionId) -> Result<bool, Error> {
        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or(Error::NotFound("Transaction not found".to_string()))?;

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if transaction.state != TransactionState::Active {
            return Err(Error::InvalidState(format!(
                "Transaction in invalid state: {:?}",
                transaction.state
            )));
        }

        transaction.state = TransactionState::Preparing;
        transaction.prepare_time = Some(SystemTime::now());

        let prepare_timestamp = self.timestamp_oracle.next_timestamp();
        let operations = transaction.operations.clone();
        let participants = transaction.participants.clone();

        drop(transaction);

        // Log prepare for each participant
        for participant in &participants {
            self.transaction_log
                .log_prepare(
                    tx_id,
                    participant.id.clone(),
                    super::participant::VoteDecision::Commit,
                    vec![],
                )
                .await?;
        }

        let prepare_start = Instant::now();
        let mut votes = Vec::new();

        for participant in participants {
            let vote = self
                .prepare_participant(
                    tx_id,
                    &participant.id,
                    operations.clone(),
                    prepare_timestamp,
                )
                .await;

            votes.push((participant.id.clone(), vote));
        }

        let all_prepared = votes
            .iter()
            .all(|(_, vote)| matches!(vote, Ok(super::participant::VoteDecision::Commit)));

        let prepare_duration = prepare_start.elapsed().as_millis() as u64;
        self.metrics
            .prepare_phase_duration_ms
            .fetch_add(prepare_duration, Ordering::Relaxed);

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if all_prepared {
            transaction.state = TransactionState::Prepared;

            let prepared_state = PreparedState {
                transaction_id: tx_id,
                participants_ready: votes
                    .iter()
                    .filter_map(
                        |(id, vote)| {
                            if vote.is_ok() {
                                Some(id.clone())
                            } else {
                                None
                            }
                        },
                    )
                    .collect(),
                prepare_timestamp: SystemTime::now(),
                timeout_at: Instant::now() + self.config.commit_timeout,
            };

            self.prepared_transactions
                .write()
                .insert(tx_id, prepared_state);

            let _ = self.event_bus.send(TransactionEvent::Prepared(tx_id));

            Ok(true)
        } else {
            transaction.state = TransactionState::Aborting;
            Ok(false)
        }
    }

    async fn prepare_participant(
        &self,
        tx_id: TransactionId,
        participant_id: &str,
        operations: Vec<Operation>,
        _prepare_timestamp: u64,
    ) -> Result<super::participant::VoteDecision, Error> {
        let participant = self
            .participant_manager
            .get_participant(participant_id)
            .await?;

        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < self.config.max_retry_attempts {
            match participant.prepare(tx_id, operations.clone()).await {
                Ok(vote) => return Ok(vote),
                Err(e) => {
                    last_error = Some(e);
                    retry_count += 1;

                    if retry_count < self.config.max_retry_attempts {
                        tokio::time::sleep(self.config.retry_backoff * retry_count).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Timeout("Consensus timeout".to_string())))
    }

    #[allow(clippy::await_holding_lock, clippy::manual_map)]
    pub async fn commit(&self, tx_id: TransactionId) -> Result<(), Error> {
        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or_else(|| Error::NotFound("Transaction not found".to_string()))?;

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if transaction.state != TransactionState::Prepared {
            return Err(Error::InvalidState(format!(
                "Cannot commit transaction in state: {:?}",
                transaction.state
            )));
        }

        transaction.state = TransactionState::Committing;
        let commit_timestamp = self.timestamp_oracle.next_timestamp();
        transaction.commit_timestamp = Some(commit_timestamp);
        transaction.decision_time = Some(SystemTime::now());

        let participants = transaction.participants.clone();
        drop(transaction);

        self.decision_log
            .log_decision(tx_id, DecisionType::Commit)
            .await?;
        let participants_committed: Vec<String> =
            participants.iter().map(|p| p.id.clone()).collect();
        self.transaction_log
            .log_commit(tx_id, commit_timestamp, participants_committed)
            .await?;

        let commit_start = Instant::now();

        let commit_futures: Vec<_> = participants
            .iter()
            .map(|p| self.commit_participant(tx_id, &p.id))
            .collect();

        let results = futures::future::join_all(commit_futures).await;

        let all_committed = results.iter().all(|r| r.is_ok());

        let commit_duration = commit_start.elapsed().as_millis() as u64;
        self.metrics
            .commit_phase_duration_ms
            .fetch_add(commit_duration, Ordering::Relaxed);

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if all_committed {
            transaction.state = TransactionState::Committed;

            self.active_transactions.write().remove(&tx_id);
            self.prepared_transactions.write().remove(&tx_id);

            self.metrics
                .committed_transactions
                .fetch_add(1, Ordering::Relaxed);
            self.metrics
                .active_transactions
                .fetch_sub(1, Ordering::Relaxed);

            let _ = self.event_bus.send(TransactionEvent::Committed(tx_id));

            Ok(())
        } else {
            transaction.state = TransactionState::Failed;
            Err(Error::CommitFailed(
                "Some participants failed to commit".to_string(),
            ))
        }
    }

    async fn commit_participant(
        &self,
        tx_id: TransactionId,
        participant_id: &str,
    ) -> Result<(), Error> {
        let participant = self
            .participant_manager
            .get_participant(participant_id)
            .await?;

        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < self.config.max_retry_attempts {
            match participant.commit(tx_id).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_error = Some(e);
                    retry_count += 1;

                    if retry_count < self.config.max_retry_attempts {
                        tokio::time::sleep(self.config.retry_backoff * retry_count).await;
                    }
                }
            }
        }

        if let Some(error) = last_error {
            let _ = self.event_bus.send(TransactionEvent::ParticipantFailed {
                tx: tx_id,
                participant: participant_id.to_string(),
            });

            Err(error)
        } else {
            Err(Error::Timeout("Participant response timeout".to_string()))
        }
    }

    pub async fn abort(&self, tx_id: TransactionId) -> Result<(), Error> {
        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or_else(|| Error::NotFound("Transaction not found".to_string()))?;

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if matches!(
            transaction.state,
            TransactionState::Committed | TransactionState::Aborted
        ) {
            return Ok(());
        }

        transaction.state = TransactionState::Aborting;
        transaction.decision_time = Some(SystemTime::now());

        let participants = transaction.participants.clone();
        let operations = transaction.operations.clone();
        drop(transaction);

        self.decision_log
            .log_decision(tx_id, DecisionType::Abort)
            .await?;
        let participants_aborted: Vec<String> = participants.iter().map(|p| p.id.clone()).collect();
        self.transaction_log
            .log_abort(
                tx_id,
                "Transaction aborted".to_string(),
                participants_aborted,
            )
            .await?;

        for operation in operations.iter().rev() {
            if let Some(undo_entry) = self.create_undo_entry(operation) {
                self.undo_log.add_entry(tx_id, undo_entry).await?;
            }
        }

        let abort_futures: Vec<_> = participants
            .iter()
            .map(|p| self.abort_participant(tx_id, &p.id))
            .collect();

        let _ = futures::future::join_all(abort_futures).await;

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();
        transaction.state = TransactionState::Aborted;

        self.active_transactions.write().remove(&tx_id);
        self.prepared_transactions.write().remove(&tx_id);

        self.metrics
            .aborted_transactions
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .active_transactions
            .fetch_sub(1, Ordering::Relaxed);

        let _ = self.event_bus.send(TransactionEvent::Aborted(tx_id));

        Ok(())
    }

    async fn abort_participant(
        &self,
        tx_id: TransactionId,
        participant_id: &str,
    ) -> Result<(), Error> {
        let participant = self
            .participant_manager
            .get_participant(participant_id)
            .await?;
        participant.abort(tx_id).await
    }

    fn create_undo_entry(&self, operation: &Operation) -> Option<UndoEntry> {
        match operation {
            Operation::Write {
                key,
                value,
                previous,
            } => Some(UndoEntry {
                operation: operation.clone(),
                inverse_operation: if let Some(prev) = previous {
                    Operation::Write {
                        key: key.clone(),
                        value: prev.clone(),
                        previous: Some(value.clone()),
                    }
                } else {
                    Operation::Delete {
                        key: key.clone(),
                        previous: Some(value.clone()),
                    }
                },
                timestamp: self.timestamp_oracle.current(),
            }),
            Operation::Delete { key, previous } => previous.as_ref().map(|prev| UndoEntry {
                operation: operation.clone(),
                inverse_operation: Operation::Write {
                    key: key.clone(),
                    value: prev.clone(),
                    previous: None,
                },
                timestamp: self.timestamp_oracle.current(),
            }),
            _ => None,
        }
    }

    pub async fn add_operation(
        &self,
        tx_id: TransactionId,
        operation: Operation,
    ) -> Result<(), Error> {
        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or_else(|| Error::NotFound("Transaction not found".to_string()))?;

        #[allow(clippy::await_holding_lock)]
        let mut transaction = tx.write();

        if transaction.state != TransactionState::Active {
            return Err(Error::InvalidState("Transaction not active".to_string()));
        }

        if self.config.enable_deadlock_detection {
            if let Some(key) = Self::extract_key(&operation) {
                let key_str = String::from_utf8_lossy(&key).to_string();
                let _ = self.deadlock_detector.add_lock(tx_id, key_str).await;

                if let Ok(Some(victim)) = self.deadlock_detector.detect_deadlock().await {
                    self.metrics
                        .deadlocks_detected
                        .fetch_add(1, Ordering::Relaxed);

                    let _ = self.event_bus.send(TransactionEvent::DeadlockDetected {
                        victim: victim.txn_id,
                    });

                    if victim.txn_id == tx_id {
                        return Err(Error::DeadlockVictim);
                    }
                }
            }
        }

        transaction.operations.push(operation);

        Ok(())
    }

    fn extract_key(operation: &Operation) -> Option<Vec<u8>> {
        match operation {
            Operation::Read { key, .. }
            | Operation::Write { key, .. }
            | Operation::Delete { key, .. } => Some(key.clone()),
            _ => None,
        }
    }

    pub async fn recover_transaction(&self, tx_id: TransactionId) -> Result<(), Error> {
        self.metrics
            .recovery_attempts
            .fetch_add(1, Ordering::Relaxed);

        let tx = self
            .transactions
            .get(&tx_id)
            .ok_or_else(|| Error::NotFound("Transaction not found".to_string()))?;

        #[allow(clippy::await_holding_lock)]
        let transaction = tx.read();
        let state = transaction.state;
        let participants = transaction.participants.clone();
        drop(transaction);

        let _ = self
            .event_bus
            .send(TransactionEvent::RecoveryInitiated(tx_id));

        match state {
            TransactionState::Preparing | TransactionState::Prepared => {
                let votes = self.query_participant_votes(tx_id, &participants).await?;

                if votes
                    .iter()
                    .all(|v| matches!(v, super::participant::VoteDecision::Commit))
                {
                    self.commit(tx_id).await
                } else {
                    self.abort(tx_id).await
                }
            }
            TransactionState::Committing => self.commit(tx_id).await,
            TransactionState::Aborting => self.abort(tx_id).await,
            _ => Ok(()),
        }
    }

    async fn query_participant_votes(
        &self,
        tx_id: TransactionId,
        participants: &[ParticipantInfo],
    ) -> Result<Vec<super::participant::VoteDecision>, Error> {
        let mut votes = Vec::new();

        for participant in participants {
            let p = self
                .participant_manager
                .get_participant(&participant.id)
                .await?;
            let state = p.get_state(tx_id).await?;

            let vote = match state {
                super::participant::ParticipantState::Prepared => {
                    super::participant::VoteDecision::Commit
                }
                _ => super::participant::VoteDecision::Abort,
            };

            votes.push(vote);
        }

        Ok(votes)
    }

    pub async fn garbage_collect(&self) -> Result<(), Error> {
        let cutoff = SystemTime::now() - self.config.max_transaction_duration;
        let mut to_remove = Vec::new();

        for entry in self.transactions.iter() {
            let transaction = entry.value().read();

            if matches!(
                transaction.state,
                TransactionState::Committed | TransactionState::Aborted
            ) && transaction.decision_time.unwrap_or(transaction.start_time) < cutoff
            {
                to_remove.push(*entry.key());
            }
        }

        for tx_id in to_remove {
            self.transactions.remove(&tx_id);
            self.undo_log.remove_entries(tx_id).await?;
        }

        Ok(())
    }

    pub fn get_metrics(&self) -> TransactionMetricsSnapshot {
        TransactionMetricsSnapshot {
            total_transactions: self.metrics.total_transactions.load(Ordering::Relaxed),
            committed_transactions: self.metrics.committed_transactions.load(Ordering::Relaxed),
            aborted_transactions: self.metrics.aborted_transactions.load(Ordering::Relaxed),
            active_transactions: self.metrics.active_transactions.load(Ordering::Relaxed),
            avg_prepare_duration_ms: self
                .metrics
                .prepare_phase_duration_ms
                .load(Ordering::Relaxed)
                / self
                    .metrics
                    .total_transactions
                    .load(Ordering::Relaxed)
                    .max(1),
            avg_commit_duration_ms: self
                .metrics
                .commit_phase_duration_ms
                .load(Ordering::Relaxed)
                / self
                    .metrics
                    .committed_transactions
                    .load(Ordering::Relaxed)
                    .max(1),
            conflicts_detected: self.metrics.conflicts_detected.load(Ordering::Relaxed),
            deadlocks_detected: self.metrics.deadlocks_detected.load(Ordering::Relaxed),
            recovery_attempts: self.metrics.recovery_attempts.load(Ordering::Relaxed),
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);

        let to_abort: Vec<_> = self.active_transactions.read().iter().copied().collect();
        for tx_id in to_abort {
            let _ = self.abort(tx_id).await;
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionMetricsSnapshot {
    pub total_transactions: u64,
    pub committed_transactions: u64,
    pub aborted_transactions: u64,
    pub active_transactions: u64,
    pub avg_prepare_duration_ms: u64,
    pub avg_commit_duration_ms: u64,
    pub conflicts_detected: u64,
    pub deadlocks_detected: u64,
    pub recovery_attempts: u64,
}

impl ParticipantManager {
    fn new() -> Self {
        Self {
            participants: Arc::new(DashMap::new()),
            connection_pool: Arc::new(ConnectionPool::new(100, Duration::from_secs(30))),
            health_checker: Arc::new(HealthChecker::new(Duration::from_secs(10))),
        }
    }

    async fn get_participant(&self, id: &str) -> Result<Arc<dyn Participant>, Error> {
        if let Some(participant) = self.participants.get(id) {
            if self.health_checker.is_healthy(id) {
                return Ok(participant.clone());
            }
        }

        let connection = self.connection_pool.get_connection(id).await?;
        let participant = Arc::new(RemoteParticipant::new(id.to_string(), connection));
        self.participants
            .insert(id.to_string(), participant.clone());

        Ok(participant as Arc<dyn Participant>)
    }
}

struct RemoteParticipant {
    id: String,
    connection: Arc<Connection>,
}

impl RemoteParticipant {
    fn new(id: String, connection: Arc<Connection>) -> Self {
        Self { id, connection }
    }
}

#[async_trait]
impl Participant for RemoteParticipant {
    async fn prepare(
        &self,
        tx_id: TransactionId,
        operations: Vec<Operation>,
    ) -> Result<super::participant::VoteDecision, Error> {
        let request = PrepareRequest {
            transaction_id: tx_id,
            operations,
            prepare_timestamp: 0,
            timeout: Duration::from_secs(30),
        };

        let response = self.connection.client.send_prepare(request).await?;
        Ok(response.vote)
    }

    #[allow(clippy::await_holding_lock)]
    async fn commit(&self, tx_id: TransactionId) -> Result<(), Error> {
        let request = CommitRequest {
            transaction_id: tx_id,
            commit_timestamp: 0,
        };

        self.connection.client.send_commit(request).await?;
        Ok(())
    }

    async fn abort(&self, tx_id: TransactionId) -> Result<(), Error> {
        let request = AbortRequest {
            transaction_id: tx_id,
            reason: "Coordinator abort".to_string(),
        };

        self.connection.client.send_abort(request).await?;
        Ok(())
    }

    async fn get_state(
        &self,
        tx_id: TransactionId,
    ) -> Result<super::participant::ParticipantState, Error> {
        let request = StatusRequest {
            transaction_id: tx_id,
        };

        let response = self.connection.client.send_status(request).await?;
        Ok(response.state)
    }

    async fn recover(&self, _tx_id: TransactionId) -> Result<(), Error> {
        Ok(())
    }
}

impl ConnectionPool {
    fn new(max_connections: usize, connection_timeout: Duration) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            max_connections,
            connection_timeout,
        }
    }

    async fn get_connection(&self, address: &str) -> Result<Arc<Connection>, Error> {
        if let Some(conn_ref) = self.connections.get(address) {
            let conn = Arc::clone(&*conn_ref);
            if !conn.in_use.load(Ordering::Acquire) {
                conn.in_use.store(true, Ordering::Release);
                return Ok(conn);
            }
        }

        Err(Error::ConnectionPoolExhausted)
    }
}

impl HealthChecker {
    fn new(check_interval: Duration) -> Self {
        Self {
            checks: Arc::new(DashMap::new()),
            check_interval,
        }
    }

    fn is_healthy(&self, participant: &str) -> bool {
        if let Some(status) = self.checks.get(participant) {
            status.is_healthy
        } else {
            true
        }
    }
}

impl DecisionLog {
    fn new() -> Self {
        Self {
            decisions: Arc::new(DashMap::new()),
            persistent_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn log_decision(
        &self,
        tx_id: TransactionId,
        decision_type: DecisionType,
    ) -> Result<(), Error> {
        let decision = Decision {
            transaction_id: tx_id,
            decision_type,
            timestamp: SystemTime::now(),
            participants: Vec::new(),
        };

        self.decisions.insert(tx_id, decision.clone());
        self.persistent_log.write().push(decision);

        Ok(())
    }
}

impl UndoLog {
    fn new() -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
        }
    }

    async fn add_entry(&self, tx_id: TransactionId, entry: UndoEntry) -> Result<(), Error> {
        self.entries
            .entry(tx_id)
            .or_insert_with(Vec::new)
            .push(entry);
        Ok(())
    }

    async fn remove_entries(&self, tx_id: TransactionId) -> Result<(), Error> {
        self.entries.remove(&tx_id);
        Ok(())
    }
}

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{RwLock, Mutex, mpsc, broadcast, oneshot};
use serde::{Serialize, Deserialize};
use crate::core::error::{Error, Result};
use dashmap::DashMap;

pub struct EnhancedRaftNode {
    node_id: u64,
    state: Arc<RwLock<NodeState>>,
    log: Arc<RaftLog>,
    state_machine: Arc<dyn StateMachine>,
    peers: Arc<DashMap<u64, PeerConnection>>,
    config: RaftConfig,
    membership: Arc<RwLock<ClusterMembership>>,
    snapshot_manager: Arc<SnapshotManager>,
    pre_vote: Arc<PreVoteManager>,
    linearizable_reads: Arc<LinearizableReadManager>,
    metrics: Arc<RaftMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeRole {
    Leader,
    Candidate,
    Follower,
    Learner,
    Observer,
}

struct NodeState {
    current_term: u64,
    voted_for: Option<u64>,
    role: NodeRole,
    leader_id: Option<u64>,
    commit_index: u64,
    last_applied: u64,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
    election_timeout: Instant,
    heartbeat_interval: Duration,
    last_heartbeat: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub max_batch_size: usize,
    pub pipeline_requests: bool,
    pub pre_vote: bool,
    pub auto_leave_on_failure: bool,
    pub snapshot_interval: u64,
    pub max_inflight_messages: usize,
    pub flow_control: FlowControlConfig,
    pub adaptive_optimization: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlConfig {
    pub max_bytes_per_second: u64,
    pub max_pending_bytes: u64,
    pub backpressure_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Command,
    pub timestamp: u64,
    pub client_id: Option<String>,
    pub request_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Write { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Transaction { ops: Vec<TransactionOp> },
    ConfigChange { change: ConfigChange },
    NoOp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOp {
    pub op_type: OpType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub condition: Option<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpType {
    Read,
    Write,
    Delete,
    CompareAndSwap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub field: String,
    pub operator: ComparisonOp,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ComparisonOp {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChange {
    AddNode { node_id: u64, address: String },
    RemoveNode { node_id: u64 },
    PromoteLearner { node_id: u64 },
    AddLearner { node_id: u64, address: String },
    UpdateAddress { node_id: u64, new_address: String },
}

#[async_trait::async_trait]
pub trait StateMachine: Send + Sync {
    async fn apply(&self, entry: LogEntry) -> Result<Vec<u8>>;
    async fn snapshot(&self) -> Result<Vec<u8>>;
    async fn restore(&self, snapshot: Vec<u8>) -> Result<()>;
}

struct RaftLog {
    entries: Arc<RwLock<Vec<LogEntry>>>,
    snapshot_index: Arc<RwLock<u64>>,
    snapshot_term: Arc<RwLock<u64>>,
    compacted_index: Arc<RwLock<u64>>,
    persistent_storage: Arc<dyn LogStorage>,
}

#[async_trait::async_trait]
trait LogStorage: Send + Sync {
    async fn append_entries(&self, entries: Vec<LogEntry>) -> Result<()>;
    async fn get_entries(&self, start: u64, end: u64) -> Result<Vec<LogEntry>>;
    async fn truncate(&self, index: u64) -> Result<()>;
    async fn save_snapshot(&self, index: u64, term: u64, data: Vec<u8>) -> Result<()>;
    async fn load_snapshot(&self) -> Result<Option<(u64, u64, Vec<u8>)>>;
}

struct ClusterMembership {
    members: HashSet<u64>,
    learners: HashSet<u64>,
    observers: HashSet<u64>,
    joint_consensus: Option<JointConsensus>,
    configuration_index: u64,
}

struct JointConsensus {
    old_members: HashSet<u64>,
    new_members: HashSet<u64>,
    start_index: u64,
}

struct PeerConnection {
    node_id: u64,
    address: String,
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    next_index: u64,
    match_index: u64,
    inflight: VecDeque<InflightMessage>,
    flow_control: FlowControl,
    last_contact: Instant,
    consecutive_failures: u32,
}

struct InflightMessage {
    index: u64,
    bytes: usize,
    sent_at: Instant,
}

struct FlowControl {
    bytes_in_flight: u64,
    max_bytes: u64,
    rate_limiter: RateLimiter,
}

struct RateLimiter {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

struct SnapshotManager {
    last_snapshot_index: Arc<RwLock<u64>>,
    snapshot_in_progress: Arc<RwLock<bool>>,
    chunk_size: usize,
    compression: CompressionType,
}

#[derive(Debug, Clone, Copy)]
enum CompressionType {
    None,
    Lz4,
    Zstd,
}

struct PreVoteManager {
    enabled: bool,
    pre_vote_responses: Arc<Mutex<HashMap<u64, PreVoteResponse>>>,
    pre_vote_term: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
struct PreVoteResponse {
    term: u64,
    vote_granted: bool,
    reason: Option<String>,
}

struct LinearizableReadManager {
    read_index: Arc<RwLock<u64>>,
    pending_reads: Arc<DashMap<u64, PendingRead>>,
    heartbeat_confirmations: Arc<DashMap<u64, HashSet<u64>>>,
}

struct PendingRead {
    request_id: u64,
    read_index: u64,
    response_channel: oneshot::Sender<Result<Vec<u8>>>,
    timeout: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: Option<u64>,
        conflict_index: Option<u64>,
        conflict_term: Option<u64>,
    },
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
        pre_vote: bool,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
        pre_vote: bool,
    },
    InstallSnapshot {
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    InstallSnapshotResponse {
        term: u64,
        bytes_stored: u64,
    },
    Heartbeat {
        term: u64,
        leader_id: u64,
        commit_index: u64,
        read_index: u64,
    },
    HeartbeatResponse {
        term: u64,
        node_id: u64,
    },
    ForwardRequest {
        client_id: String,
        request_id: u64,
        command: Command,
    },
    TimeoutNow {
        term: u64,
    },
}

struct RaftMetrics {
    term_changes: std::sync::atomic::AtomicU64,
    elections_started: std::sync::atomic::AtomicU64,
    elections_won: std::sync::atomic::AtomicU64,
    messages_sent: std::sync::atomic::AtomicU64,
    messages_received: std::sync::atomic::AtomicU64,
    entries_appended: std::sync::atomic::AtomicU64,
    entries_committed: std::sync::atomic::AtomicU64,
    snapshots_sent: std::sync::atomic::AtomicU64,
    snapshots_received: std::sync::atomic::AtomicU64,
}

impl EnhancedRaftNode {
    pub async fn new(
        node_id: u64,
        peers: Vec<(u64, String)>,
        state_machine: Arc<dyn StateMachine>,
        config: RaftConfig,
    ) -> Result<Self> {
        let state = Arc::new(RwLock::new(NodeState {
            current_term: 0,
            voted_for: None,
            role: NodeRole::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_timeout: Instant::now() + config.election_timeout_min,
            heartbeat_interval: config.heartbeat_interval,
            last_heartbeat: Instant::now(),
        }));
        
        let peer_connections = Arc::new(DashMap::new());
        for (peer_id, address) in peers {
            let (tx, rx) = mpsc::channel(1000);
            peer_connections.insert(peer_id, PeerConnection {
                node_id: peer_id,
                address,
                sender: tx,
                receiver: Arc::new(Mutex::new(rx)),
                next_index: 1,
                match_index: 0,
                inflight: VecDeque::new(),
                flow_control: FlowControl {
                    bytes_in_flight: 0,
                    max_bytes: config.flow_control.max_pending_bytes,
                    rate_limiter: RateLimiter {
                        tokens: config.flow_control.max_bytes_per_second as f64,
                        max_tokens: config.flow_control.max_bytes_per_second as f64,
                        refill_rate: config.flow_control.max_bytes_per_second as f64,
                        last_refill: Instant::now(),
                    },
                },
                last_contact: Instant::now(),
                consecutive_failures: 0,
            });
        }
        
        Ok(Self {
            node_id,
            state,
            log: Arc::new(RaftLog {
                entries: Arc::new(RwLock::new(Vec::new())),
                snapshot_index: Arc::new(RwLock::new(0)),
                snapshot_term: Arc::new(RwLock::new(0)),
                compacted_index: Arc::new(RwLock::new(0)),
                persistent_storage: Arc::new(InMemoryLogStorage::new()),
            }),
            state_machine,
            peers: peer_connections,
            config,
            membership: Arc::new(RwLock::new(ClusterMembership {
                members: peers.iter().map(|(id, _)| *id).collect(),
                learners: HashSet::new(),
                observers: HashSet::new(),
                joint_consensus: None,
                configuration_index: 0,
            })),
            snapshot_manager: Arc::new(SnapshotManager {
                last_snapshot_index: Arc::new(RwLock::new(0)),
                snapshot_in_progress: Arc::new(RwLock::new(false)),
                chunk_size: 64 * 1024,
                compression: CompressionType::Lz4,
            }),
            pre_vote: Arc::new(PreVoteManager {
                enabled: config.pre_vote,
                pre_vote_responses: Arc::new(Mutex::new(HashMap::new())),
                pre_vote_term: Arc::new(RwLock::new(0)),
            }),
            linearizable_reads: Arc::new(LinearizableReadManager {
                read_index: Arc::new(RwLock::new(0)),
                pending_reads: Arc::new(DashMap::new()),
                heartbeat_confirmations: Arc::new(DashMap::new()),
            }),
            metrics: Arc::new(RaftMetrics {
                term_changes: std::sync::atomic::AtomicU64::new(0),
                elections_started: std::sync::atomic::AtomicU64::new(0),
                elections_won: std::sync::atomic::AtomicU64::new(0),
                messages_sent: std::sync::atomic::AtomicU64::new(0),
                messages_received: std::sync::atomic::AtomicU64::new(0),
                entries_appended: std::sync::atomic::AtomicU64::new(0),
                entries_committed: std::sync::atomic::AtomicU64::new(0),
                snapshots_sent: std::sync::atomic::AtomicU64::new(0),
                snapshots_received: std::sync::atomic::AtomicU64::new(0),
            }),
        })
    }
    
    pub async fn start(&self) -> Result<()> {
        self.start_election_timer().await;
        self.start_heartbeat_timer().await;
        self.start_apply_loop().await;
        self.start_snapshot_loop().await;
        
        if self.config.adaptive_optimization {
            self.start_optimization_loop().await;
        }
        
        Ok(())
    }
    
    pub async fn propose(&self, command: Command) -> Result<u64> {
        let state = self.state.read().await;
        
        if state.role != NodeRole::Leader {
            if let Some(leader_id) = state.leader_id {
                return Err(Error::NotLeader(Some(leader_id)));
            } else {
                return Err(Error::NotLeader(None));
            }
        }
        
        drop(state);
        
        let entry = LogEntry {
            term: self.state.read().await.current_term,
            index: self.log.entries.read().await.len() as u64 + 1,
            command,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            client_id: None,
            request_id: None,
        };
        
        self.log.entries.write().await.push(entry.clone());
        self.replicate_entries().await?;
        
        Ok(entry.index)
    }
    
    pub async fn read_linearizable(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        let read_index = self.linearizable_reads.read_index.write().await.clone();
        let request_id = rand::random();
        
        let (tx, rx) = oneshot::channel();
        
        self.linearizable_reads.pending_reads.insert(
            request_id,
            PendingRead {
                request_id,
                read_index,
                response_channel: tx,
                timeout: Instant::now() + Duration::from_secs(5),
            },
        );
        
        self.send_heartbeat_to_majority().await?;
        
        rx.await.map_err(|_| Error::Timeout("Read timeout".to_string()))?
    }
    
    async fn start_election_timer(&self) {
        let state = self.state.clone();
        let config = self.config.clone();
        let pre_vote = self.pre_vote.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            loop {
                let timeout = {
                    use rand::Rng;
                    let mut rng = rand::thread_rng();
                    let min = config.election_timeout_min.as_millis() as u64;
                    let max = config.election_timeout_max.as_millis() as u64;
                    Duration::from_millis(rng.gen_range(min..max))
                };
                
                tokio::time::sleep(timeout).await;
                
                let mut s = state.write().await;
                if s.role == NodeRole::Leader {
                    continue;
                }
                
                let elapsed = s.last_heartbeat.elapsed();
                if elapsed < timeout {
                    continue;
                }
                
                if config.pre_vote && pre_vote.enabled {
                    drop(s);
                    Self::start_pre_vote(&state, &pre_vote, &metrics).await;
                } else {
                    s.role = NodeRole::Candidate;
                    s.current_term += 1;
                    s.voted_for = None;
                    metrics.elections_started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
    }
    
    async fn start_pre_vote(
        state: &Arc<RwLock<NodeState>>,
        pre_vote: &Arc<PreVoteManager>,
        metrics: &Arc<RaftMetrics>,
    ) {
        tracing::info!("Starting pre-vote");
    }
    
    async fn start_heartbeat_timer(&self) {
        let state = self.state.clone();
        let peers = self.peers.clone();
        let config = self.config.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(config.heartbeat_interval).await;
                
                let s = state.read().await;
                if s.role != NodeRole::Leader {
                    continue;
                }
                
                for entry in peers.iter() {
                    let peer = entry.value();
                    let message = Message::Heartbeat {
                        term: s.current_term,
                        leader_id: s.leader_id.unwrap(),
                        commit_index: s.commit_index,
                        read_index: 0,
                    };
                    
                    let _ = peer.sender.send(message).await;
                }
            }
        });
    }
    
    async fn start_apply_loop(&self) {
        let state = self.state.clone();
        let log = self.log.clone();
        let state_machine = self.state_machine.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                
                let s = state.read().await;
                if s.last_applied >= s.commit_index {
                    continue;
                }
                
                let start = s.last_applied + 1;
                let end = s.commit_index + 1;
                drop(s);
                
                let entries = log.entries.read().await.clone();
                
                for i in start..end {
                    if let Some(entry) = entries.get(i as usize - 1) {
                        let _ = state_machine.apply(entry.clone()).await;
                        state.write().await.last_applied = i;
                        metrics.entries_committed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        });
    }
    
    async fn start_snapshot_loop(&self) {
        let config = self.config.clone();
        let snapshot_manager = self.snapshot_manager.clone();
        let state_machine = self.state_machine.clone();
        let log = self.log.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                
                let last_applied = 0;
                let last_snapshot = *snapshot_manager.last_snapshot_index.read().await;
                
                if last_applied - last_snapshot > config.snapshot_interval {
                    let mut in_progress = snapshot_manager.snapshot_in_progress.write().await;
                    if *in_progress {
                        continue;
                    }
                    *in_progress = true;
                    drop(in_progress);
                    
                    if let Ok(data) = state_machine.snapshot().await {
                        let _ = log.persistent_storage.save_snapshot(
                            last_applied,
                            0,
                            data,
                        ).await;
                        
                        *snapshot_manager.last_snapshot_index.write().await = last_applied;
                    }
                    
                    *snapshot_manager.snapshot_in_progress.write().await = false;
                }
            }
        });
    }
    
    async fn start_optimization_loop(&self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
    
    async fn replicate_entries(&self) -> Result<()> {
        for entry in self.peers.iter() {
            let peer = entry.value();
            
            let state = self.state.read().await;
            let entries = self.log.entries.read().await.clone();
            
            let prev_index = peer.next_index - 1;
            let prev_term = if prev_index > 0 {
                entries.get(prev_index as usize - 1)
                    .map(|e| e.term)
                    .unwrap_or(0)
            } else {
                0
            };
            
            let message = Message::AppendEntries {
                term: state.current_term,
                leader_id: self.node_id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries: entries[peer.next_index as usize - 1..].to_vec(),
                leader_commit: state.commit_index,
            };
            
            let _ = peer.sender.send(message).await;
        }
        
        Ok(())
    }
    
    async fn send_heartbeat_to_majority(&self) -> Result<()> {
        let state = self.state.read().await;
        let majority = (self.peers.len() + 1) / 2 + 1;
        let mut confirmations = HashSet::new();
        confirmations.insert(self.node_id);
        
        for entry in self.peers.iter() {
            let peer = entry.value();
            let message = Message::Heartbeat {
                term: state.current_term,
                leader_id: self.node_id,
                commit_index: state.commit_index,
                read_index: 0,
            };
            
            let _ = peer.sender.send(message).await;
        }
        
        Ok(())
    }
}

struct InMemoryLogStorage {
    entries: Arc<RwLock<Vec<LogEntry>>>,
    snapshot: Arc<RwLock<Option<(u64, u64, Vec<u8>)>>>,
}

impl InMemoryLogStorage {
    fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
            snapshot: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl LogStorage for InMemoryLogStorage {
    async fn append_entries(&self, entries: Vec<LogEntry>) -> Result<()> {
        self.entries.write().await.extend(entries);
        Ok(())
    }
    
    async fn get_entries(&self, start: u64, end: u64) -> Result<Vec<LogEntry>> {
        let entries = self.entries.read().await;
        Ok(entries[start as usize..end as usize].to_vec())
    }
    
    async fn truncate(&self, index: u64) -> Result<()> {
        self.entries.write().await.truncate(index as usize);
        Ok(())
    }
    
    async fn save_snapshot(&self, index: u64, term: u64, data: Vec<u8>) -> Result<()> {
        *self.snapshot.write().await = Some((index, term, data));
        Ok(())
    }
    
    async fn load_snapshot(&self) -> Result<Option<(u64, u64, Vec<u8>)>> {
        Ok(self.snapshot.read().await.clone())
    }
}
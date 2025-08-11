use crate::error::{Error, Result};
use bincode::{Decode, Encode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
// use crate::storage::{DataStore, PageManager};
use parking_lot::Mutex;
use rand::{rng, Rng};
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Sleep;

pub mod leader_election;
pub mod log_replication;
pub mod rpc;
pub mod snapshot;
pub mod state_machine;
pub mod storage;

pub use rpc::{PeerConnection, RpcResponseType};

/// Unique identifier for a Raft node
pub type NodeId = u64;

/// Term number in Raft (monotonically increasing)
pub type Term = u64;

/// Log index (1-based, 0 means no entry)
pub type LogIndex = u64;

/// Raft node states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is a follower
    Follower,
    /// Node is a candidate for leadership
    Candidate,
    /// Node is the leader
    Leader,
}

/// Configuration for Raft consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Unique identifier for this node
    pub node_id: NodeId,

    /// Initial cluster members
    pub peers: Vec<NodeId>,

    /// Election timeout range (min, max) in milliseconds
    pub election_timeout_range: (u64, u64),

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval: u64,

    /// Maximum number of entries to send in AppendEntries
    pub max_append_entries: usize,

    /// Snapshot threshold (log size before compaction)
    pub snapshot_threshold: usize,

    /// Maximum size of a single RPC message
    pub max_message_size: usize,

    /// Enable pre-vote optimization
    pub pre_vote: bool,

    /// Enable pipeline replication
    pub pipeline_replication: bool,

    /// Enable batch optimization
    pub batch_optimization: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: vec![],
            election_timeout_range: (150, 300),
            heartbeat_interval: 50,
            max_append_entries: 64,
            snapshot_threshold: 10000,
            max_message_size: 64 * 1024 * 1024, // 64MB
            pre_vote: true,
            pipeline_replication: true,
            batch_optimization: true,
        }
    }
}

/// A log entry in the Raft log
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct LogEntry {
    /// Term when entry was received by leader
    pub term: Term,

    /// Position in the log (1-based)
    pub index: LogIndex,

    /// The actual command to apply
    pub command: Command,

    /// Client ID for deduplication
    pub client_id: Option<u64>,

    /// Request ID for idempotency
    pub request_id: Option<u64>,
}

/// Commands that can be replicated through Raft
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum Command {
    /// Database write operation
    Write { key: Vec<u8>, value: Vec<u8> },

    /// Database delete operation
    Delete { key: Vec<u8> },

    /// Transaction with multiple operations
    Transaction { ops: Vec<TransactionOp> },

    /// Configuration change
    ConfigChange(ConfigChange),

    /// No-op for new leader establishment
    NoOp,
}

/// Individual operation in a transaction
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum TransactionOp {
    Write { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Read { key: Vec<u8> },
}

/// Configuration changes
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ConfigChange {
    AddNode(NodeId, String), // node_id, address
    RemoveNode(NodeId),
    UpdateNode(NodeId, String), // node_id, new_address
}

/// Persistent state that must survive restarts
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct PersistentState {
    /// Latest term server has seen
    pub current_term: Term,

    /// Candidate that received vote in current term
    pub voted_for: Option<NodeId>,

    /// Log entries
    pub log: Vec<LogEntry>,
}

impl Default for PersistentState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
        }
    }
}

/// Volatile state on all servers
#[derive(Debug)]
pub struct VolatileState {
    /// Index of highest log entry known to be committed
    pub commit_index: AtomicU64,

    /// Index of highest log entry applied to state machine
    pub last_applied: AtomicU64,

    /// Current state of the node
    pub state: AtomicU8, // NodeState encoded as u8
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
            state: AtomicU8::new(NodeState::Follower as u8),
        }
    }
}

/// Volatile state on leaders
#[derive(Debug)]
pub struct LeaderState {
    /// For each server, index of the next log entry to send
    pub next_index: HashMap<NodeId, LogIndex>,

    /// For each server, index of highest log entry known to be replicated
    pub match_index: HashMap<NodeId, LogIndex>,

    /// Pending client requests
    pub pending_requests: HashMap<u64, ClientRequest>,

    /// In-flight AppendEntries RPCs
    pub in_flight: HashMap<NodeId, InFlightRequest>,

    /// Read index for linearizable reads
    pub read_index: LogIndex,

    /// Pending read requests
    pub pending_reads: VecDeque<ReadRequest>,
}

/// Client request tracking
#[derive(Debug)]
pub struct ClientRequest {
    pub command: Command,
    pub response_tx: oneshot::Sender<Result<Vec<u8>>>,
    pub submitted_at: Instant,
}

/// In-flight replication request
#[derive(Debug)]
pub struct InFlightRequest {
    pub prev_log_index: LogIndex,
    pub entries: Vec<LogEntry>,
    pub sent_at: Instant,
}

/// Read request for linearizable reads
#[derive(Debug)]
pub struct ReadRequest {
    pub key: Vec<u8>,
    pub read_index: LogIndex,
    pub response_tx: oneshot::Sender<Result<Option<Vec<u8>>>>,
    pub submitted_at: Instant,
}

/// Main Raft consensus implementation
pub struct RaftNode {
    /// Configuration
    config: RaftConfig,

    /// Persistent state
    persistent: Arc<RwLock<PersistentState>>,

    /// Volatile state
    volatile: Arc<VolatileState>,

    /// Leader-specific state
    leader: Arc<RwLock<Option<LeaderState>>>,

    /// State machine to apply commands
    state_machine: Arc<dyn StateMachine>,

    /// Storage for persistent state
    storage: Arc<dyn RaftStorage>,

    /// RPC handler
    rpc: Arc<RpcHandler>,

    /// Channel for incoming messages
    message_rx: Arc<Mutex<mpsc::UnboundedReceiver<RaftMessage>>>,

    /// Channel for outgoing messages
    message_tx: mpsc::UnboundedSender<RaftMessage>,

    /// Election timer
    election_timer: Arc<Mutex<Option<Pin<Box<Sleep>>>>>,

    /// Heartbeat timer (for leaders)
    heartbeat_timer: Arc<Mutex<Option<Pin<Box<Sleep>>>>>,

    /// Cluster membership
    membership: Arc<RwLock<ClusterMembership>>,

    /// Metrics
    metrics: Arc<RaftMetrics>,
}

impl Clone for RaftNode {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            persistent: self.persistent.clone(),
            volatile: self.volatile.clone(),
            leader: self.leader.clone(),
            state_machine: self.state_machine.clone(),
            storage: self.storage.clone(),
            rpc: self.rpc.clone(),
            message_rx: self.message_rx.clone(),
            message_tx: self.message_tx.clone(),
            election_timer: self.election_timer.clone(),
            heartbeat_timer: self.heartbeat_timer.clone(),
            membership: self.membership.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

/// Cluster membership information
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize)]
pub struct ClusterMembership {
    /// Current configuration
    pub current: HashMap<NodeId, NodeInfo>,

    /// Joint consensus configuration (during membership changes)
    pub joint: Option<HashMap<NodeId, NodeInfo>>,

    /// Configuration index
    pub config_index: LogIndex,
}

/// Information about a cluster node
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub voting: bool,
    pub learner: bool,
}

/// Raft protocol messages
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum RaftMessage {
    /// Request vote (or pre-vote)
    RequestVote(RequestVoteArgs),

    /// Response to vote request
    RequestVoteResponse(RequestVoteReply),

    /// Append entries (heartbeat/replication)
    AppendEntries(AppendEntriesArgs),

    /// Response to append entries
    AppendEntriesResponse(AppendEntriesReply),

    /// Install snapshot
    InstallSnapshot(InstallSnapshotArgs),

    /// Response to install snapshot
    InstallSnapshotResponse(InstallSnapshotReply),

    /// Client command
    ClientCommand(ClientCommandArgs),

    /// Client read
    ClientRead(ClientReadArgs),

    /// Forward to leader
    ForwardToLeader(ForwardArgs),
}

/// RequestVote RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RequestVoteArgs {
    /// Candidate's term
    pub term: Term,

    /// Candidate requesting vote
    pub candidate_id: NodeId,

    /// Index of candidate's last log entry
    pub last_log_index: LogIndex,

    /// Term of candidate's last log entry
    pub last_log_term: Term,

    /// True if this is a pre-vote
    pub pre_vote: bool,
}

/// RequestVote RPC reply
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RequestVoteReply {
    /// Current term, for candidate to update itself
    pub term: Term,

    /// True means candidate received vote
    pub vote_granted: bool,

    /// Reason if vote not granted
    pub reason: Option<String>,
}

/// AppendEntries RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AppendEntriesArgs {
    /// Leader's term
    pub term: Term,

    /// So follower can redirect clients
    pub leader_id: NodeId,

    /// Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,

    /// Term of prev_log_index entry
    pub prev_log_term: Term,

    /// Log entries to store (empty for heartbeat)
    pub entries: Vec<LogEntry>,

    /// Leader's commit_index
    pub leader_commit: LogIndex,

    /// Read index for linearizable reads
    pub read_index: Option<LogIndex>,
}

/// AppendEntries RPC reply
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct AppendEntriesReply {
    /// Current term, for leader to update itself
    pub term: Term,

    /// True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,

    /// Follower's last log index (for faster catch-up)
    pub last_log_index: LogIndex,

    /// Conflicting term (for faster back-tracking)
    pub conflict_term: Option<Term>,

    /// First index of conflict_term
    pub conflict_index: Option<LogIndex>,
}

/// InstallSnapshot RPC arguments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct InstallSnapshotArgs {
    /// Leader's term
    pub term: Term,

    /// So follower can redirect clients
    pub leader_id: NodeId,

    /// The snapshot replaces all entries up through this index
    pub last_included_index: LogIndex,

    /// Term of last_included_index
    pub last_included_term: Term,

    /// Byte offset where chunk is positioned
    pub offset: usize,

    /// Raw bytes of the snapshot chunk
    pub data: Vec<u8>,

    /// True if this is the last chunk
    pub done: bool,

    /// Configuration as of last_included_index
    pub config: ClusterMembership,
}

/// InstallSnapshot RPC reply
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct InstallSnapshotReply {
    /// Current term, for leader to update itself
    pub term: Term,

    /// Bytes received so far
    pub bytes_received: usize,
}

/// Client command arguments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ClientCommandArgs {
    pub command: Command,
    pub client_id: u64,
    pub request_id: u64,
}

/// Client read arguments
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ClientReadArgs {
    pub key: Vec<u8>,
    pub linearizable: bool,
}

/// Forward request to leader
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ForwardArgs {
    pub request: Vec<u8>,
    pub leader_hint: Option<NodeId>,
}

/// Trait for state machine implementation
pub trait StateMachine: Send + Sync {
    /// Apply a command to the state machine
    fn apply(&self, command: &Command) -> Result<Vec<u8>>;

    /// Read a value from the state machine
    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Create a snapshot of the current state
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Restore from a snapshot
    fn restore(&self, snapshot: &[u8]) -> Result<()>;
}

/// Trait for Raft storage implementation
pub trait RaftStorage: Send + Sync {
    /// Save persistent state
    fn save_state(&self, state: &PersistentState) -> Result<()>;

    /// Load persistent state
    fn load_state(&self) -> Result<PersistentState>;

    /// Append log entries
    fn append_entries(&self, entries: &[LogEntry]) -> Result<()>;

    /// Delete log entries from index
    fn delete_entries_from(&self, index: LogIndex) -> Result<()>;

    /// Save snapshot
    fn save_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()>;

    /// Load snapshot
    fn load_snapshot(&self) -> Result<Option<RaftSnapshot>>;
}

/// Raft snapshot
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct RaftSnapshot {
    /// Last included index
    pub last_index: LogIndex,

    /// Last included term
    pub last_term: Term,

    /// Configuration as of last_index
    pub config: ClusterMembership,

    /// Snapshot data
    pub data: Vec<u8>,

    /// Checksum
    pub checksum: u64,
}

/// RPC handler for network communication
pub struct RpcHandler {
    /// Node ID
    node_id: NodeId,

    /// Peer connections
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,

    /// Message sender
    message_tx: mpsc::UnboundedSender<RaftMessage>,
}

// PeerConnection is defined in rpc.rs

/// Metrics for monitoring
#[derive(Debug, Default)]
pub struct RaftMetrics {
    /// Number of elections
    pub elections: AtomicU64,

    /// Number of successful elections
    pub elections_won: AtomicU64,

    /// Number of log entries replicated
    pub entries_replicated: AtomicU64,

    /// Number of snapshots sent
    pub snapshots_sent: AtomicU64,

    /// Number of snapshots received
    pub snapshots_received: AtomicU64,

    /// Current leader
    pub current_leader: AtomicU64,

    /// Last contact with leader
    pub last_leader_contact: AtomicU64,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(
        config: RaftConfig,
        state_machine: Arc<dyn StateMachine>,
        storage: Arc<dyn RaftStorage>,
    ) -> Result<Self> {
        // Load persistent state
        let persistent = storage.load_state()?;

        // Create message channel
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        // Initialize membership
        let mut membership = ClusterMembership {
            current: HashMap::new(),
            joint: None,
            config_index: 0,
        };

        // Add self and peers to membership
        membership.current.insert(
            config.node_id,
            NodeInfo {
                id: config.node_id,
                address: String::new(), // Will be set by RPC layer
                voting: true,
                learner: false,
            },
        );

        for peer_id in &config.peers {
            membership.current.insert(
                *peer_id,
                NodeInfo {
                    id: *peer_id,
                    address: String::new(), // Will be set by RPC layer
                    voting: true,
                    learner: false,
                },
            );
        }

        // Create RPC handler
        let rpc = Arc::new(RpcHandler {
            node_id: config.node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            message_tx: message_tx.clone(),
        });

        Ok(Self {
            config,
            persistent: Arc::new(RwLock::new(persistent)),
            volatile: Arc::new(VolatileState::default()),
            leader: Arc::new(RwLock::new(None)),
            state_machine,
            storage,
            rpc,
            message_rx: Arc::new(Mutex::new(message_rx)),
            message_tx,
            election_timer: Arc::new(Mutex::new(None)),
            heartbeat_timer: Arc::new(Mutex::new(None)),
            membership: Arc::new(RwLock::new(membership)),
            metrics: Arc::new(RaftMetrics::default()),
        })
    }

    /// Start the Raft node
    pub async fn start(&self) -> Result<()> {
        // Reset election timer
        self.reset_election_timer().await;

        // Main event loop
        loop {
            // Check timers and handle messages
            tokio::select! {
                // Election timeout
                _ = self.wait_election_timeout() => {
                    self.handle_election_timeout().await?;
                }

                // Heartbeat timeout (if leader)
                _ = self.wait_heartbeat_timeout() => {
                    self.handle_heartbeat_timeout().await?;
                }

                // Incoming message
                Some(msg) = self.receive_message() => {
                    self.handle_message(msg).await?;
                }
            }
        }
    }

    /// Wait for election timeout
    async fn wait_election_timeout(&self) {
        if let Some(timer) = self.election_timer.lock().as_mut() {
            timer.await;
        } else {
            // No timer set, wait forever
            std::future::pending::<()>().await;
        }
    }

    /// Wait for heartbeat timeout
    async fn wait_heartbeat_timeout(&self) {
        if let Some(timer) = self.heartbeat_timer.lock().as_mut() {
            timer.await;
        } else {
            // No timer set, wait forever
            std::future::pending::<()>().await;
        }
    }

    /// Receive a message
    async fn receive_message(&self) -> Option<RaftMessage> {
        self.message_rx.lock().recv().await
    }

    /// Handle election timeout
    async fn handle_election_timeout(&self) -> Result<()> {
        // Implemented in leader_election module
        Ok(())
    }

    /// Handle heartbeat timeout
    async fn handle_heartbeat_timeout(&self) -> Result<()> {
        // Implemented in leader_election module
        Ok(())
    }

    /// Handle incoming message
    async fn handle_message(&self, message: RaftMessage) -> Result<()> {
        match message {
            RaftMessage::RequestVote(_args) => {
                // Handle in leader_election module
            }
            RaftMessage::AppendEntries(_args) => {
                // Handle in log_replication module
            }
            RaftMessage::InstallSnapshot(_args) => {
                // Handle in snapshot module
            }
            RaftMessage::ClientCommand(_args) => {
                // Handle client command
            }
            RaftMessage::ClientRead(_args) => {
                // Handle client read
            }
            _ => {}
        }
        Ok(())
    }

    /// Reset election timer with random timeout
    async fn reset_election_timer(&self) {
        let timeout = {
            let mut rng = rng();
            let (min, max) = self.config.election_timeout_range;
            Duration::from_millis(rng.random_range(min..(max + 1)))
        };

        let timer = tokio::time::sleep(timeout);
        *self.election_timer.lock() = Some(Box::pin(timer));
    }

    /// Get current state
    pub fn state(&self) -> NodeState {
        match self.volatile.state.load(Ordering::Acquire) {
            0 => NodeState::Follower,
            1 => NodeState::Candidate,
            2 => NodeState::Leader,
            _ => NodeState::Follower,
        }
    }

    /// Get current term
    pub fn current_term(&self) -> Term {
        self.persistent.read().current_term
    }

    /// Get current leader
    pub fn current_leader(&self) -> Option<NodeId> {
        let leader_id = self.metrics.current_leader.load(Ordering::Acquire);
        if leader_id == 0 {
            None
        } else {
            Some(leader_id)
        }
    }

    /// Read from state machine
    pub fn read_state_machine(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.state_machine.read(key)
    }

    /// Execute command directly on state machine (simplified for sharding demo)
    pub async fn execute_command_direct(&self, command: Command) -> Result<Vec<u8>> {
        // For now, directly apply to state machine (in production this would go through consensus)
        self.state_machine.apply(&command)
    }

    /// Become follower (used by snapshot module)
    pub async fn become_follower(&self, new_term: Term) -> Result<()> {
        {
            let mut persistent = self.persistent.write();
            persistent.current_term = new_term;
            persistent.voted_for = None;

            // Persist state
            self.storage.save_state(&persistent)?;
        }

        // Update volatile state
        self.volatile
            .state
            .store(NodeState::Follower as u8, Ordering::Release);
        *self.leader.write() = None;

        // Reset election timer
        self.reset_election_timer().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_state_encoding() {
        assert_eq!(NodeState::Follower as u8, 0);
        assert_eq!(NodeState::Candidate as u8, 1);
        assert_eq!(NodeState::Leader as u8, 2);
    }

    #[test]
    fn test_default_config() {
        let config = RaftConfig::default();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.election_timeout_range, (150, 300));
        assert_eq!(config.heartbeat_interval, 50);
    }

    #[test]
    fn test_persistent_state_default() {
        let state = PersistentState::default();
        assert_eq!(state.current_term, 0);
        assert_eq!(state.voted_for, None);
        assert!(state.log.is_empty());
    }
}

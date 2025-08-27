use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet, VecDeque};
use parking_lot::{RwLock, Mutex};
use tokio::sync::{mpsc, oneshot, broadcast};
use tokio::time::{interval, timeout, sleep};
use crate::core::error::Error;
use super::state_machine::{StateMachine, Command, CommandResult};
use super::log::{LogEntry, LogStore, LogMetadata};
use super::rpc::{RaftRpc, AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use super::storage::{RaftStorage, PersistentState};
use super::config::RaftConfig;
use super::snapshot::{Snapshot, SnapshotMetadata};
use super::membership::{ClusterConfig, MembershipChange};
use serde::{Serialize, Deserialize};
use rand::Rng;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const MIN_ELECTION_TIMEOUT: Duration = Duration::from_millis(300);
const MAX_ELECTION_TIMEOUT: Duration = Duration::from_millis(600);
const SNAPSHOT_THRESHOLD: usize = 10000;
const MAX_APPEND_ENTRIES_BATCH: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
    Learner,
}

#[derive(Debug, Clone)]
pub struct RaftNode {
    id: NodeId,
    state: Arc<RwLock<RaftState>>,
    current_term: Arc<AtomicU64>,
    voted_for: Arc<RwLock<Option<NodeId>>>,
    leader_id: Arc<RwLock<Option<NodeId>>>,
    
    log: Arc<dyn LogStore>,
    state_machine: Arc<dyn StateMachine>,
    storage: Arc<dyn RaftStorage>,
    rpc: Arc<dyn RaftRpc>,
    
    cluster_config: Arc<RwLock<ClusterConfig>>,
    config: Arc<RaftConfig>,
    
    commit_index: Arc<AtomicU64>,
    last_applied: Arc<AtomicU64>,
    
    next_index: Arc<RwLock<HashMap<NodeId, LogIndex>>>,
    match_index: Arc<RwLock<HashMap<NodeId, LogIndex>>>,
    
    election_timer: Arc<Mutex<Option<Instant>>>,
    heartbeat_timer: Arc<Mutex<Option<Instant>>>,
    
    pending_commands: Arc<Mutex<HashMap<u64, oneshot::Sender<CommandResult>>>>,
    
    snapshot_in_progress: Arc<AtomicBool>,
    last_snapshot_index: Arc<AtomicU64>,
    
    shutdown: Arc<AtomicBool>,
    event_tx: broadcast::Sender<RaftEvent>,
    
    metrics: Arc<RaftMetrics>,
}

#[derive(Debug, Clone)]
pub enum RaftEvent {
    StateChange(RaftState, RaftState),
    LeaderElected(NodeId),
    LogCommitted(LogIndex),
    SnapshotCreated(LogIndex),
    MembershipChanged(ClusterConfig),
    Error(String),
}

#[derive(Debug)]
struct RaftMetrics {
    elections_called: AtomicU64,
    elections_won: AtomicU64,
    elections_lost: AtomicU64,
    heartbeats_sent: AtomicU64,
    entries_appended: AtomicU64,
    entries_committed: AtomicU64,
    snapshots_created: AtomicU64,
    snapshots_installed: AtomicU64,
}

impl RaftNode {
    pub async fn new(
        id: NodeId,
        config: RaftConfig,
        log: Arc<dyn LogStore>,
        state_machine: Arc<dyn StateMachine>,
        storage: Arc<dyn RaftStorage>,
        rpc: Arc<dyn RaftRpc>,
    ) -> Result<Self, Error> {
        let persistent_state = storage.load_state().await?;
        
        let (event_tx, _) = broadcast::channel(1024);
        
        let node = Self {
            id,
            state: Arc::new(RwLock::new(RaftState::Follower)),
            current_term: Arc::new(AtomicU64::new(persistent_state.current_term)),
            voted_for: Arc::new(RwLock::new(persistent_state.voted_for)),
            leader_id: Arc::new(RwLock::new(None)),
            
            log,
            state_machine,
            storage,
            rpc,
            
            cluster_config: Arc::new(RwLock::new(config.initial_cluster.clone())),
            config: Arc::new(config),
            
            commit_index: Arc::new(AtomicU64::new(0)),
            last_applied: Arc::new(AtomicU64::new(0)),
            
            next_index: Arc::new(RwLock::new(HashMap::new())),
            match_index: Arc::new(RwLock::new(HashMap::new())),
            
            election_timer: Arc::new(Mutex::new(None)),
            heartbeat_timer: Arc::new(Mutex::new(None)),
            
            pending_commands: Arc::new(Mutex::new(HashMap::new())),
            
            snapshot_in_progress: Arc::new(AtomicBool::new(false)),
            last_snapshot_index: Arc::new(AtomicU64::new(0)),
            
            shutdown: Arc::new(AtomicBool::new(false)),
            event_tx,
            
            metrics: Arc::new(RaftMetrics {
                elections_called: AtomicU64::new(0),
                elections_won: AtomicU64::new(0),
                elections_lost: AtomicU64::new(0),
                heartbeats_sent: AtomicU64::new(0),
                entries_appended: AtomicU64::new(0),
                entries_committed: AtomicU64::new(0),
                snapshots_created: AtomicU64::new(0),
                snapshots_installed: AtomicU64::new(0),
            }),
        };
        
        Ok(node)
    }

    pub async fn start(&self) -> Result<(), Error> {
        self.reset_election_timer();
        
        let node = self.clone();
        tokio::spawn(async move {
            node.run_event_loop().await;
        });
        
        let node = self.clone();
        tokio::spawn(async move {
            node.run_apply_loop().await;
        });
        
        Ok(())
    }

    async fn run_event_loop(&self) {
        let mut interval = interval(Duration::from_millis(10));
        
        while !self.shutdown.load(Ordering::Acquire) {
            interval.tick().await;
            
            let state = *self.state.read();
            
            match state {
                RaftState::Follower | RaftState::Learner => {
                    if self.election_timeout_elapsed() && state != RaftState::Learner {
                        self.start_election().await;
                    }
                },
                RaftState::Candidate => {
                    if self.election_timeout_elapsed() {
                        self.start_election().await;
                    }
                },
                RaftState::Leader => {
                    if self.heartbeat_timeout_elapsed() {
                        self.send_heartbeats().await;
                    }
                },
            }
            
            if self.should_create_snapshot() {
                self.create_snapshot().await;
            }
        }
    }

    async fn run_apply_loop(&self) {
        let mut interval = interval(Duration::from_millis(10));
        
        while !self.shutdown.load(Ordering::Acquire) {
            interval.tick().await;
            
            let last_applied = self.last_applied.load(Ordering::Acquire);
            let commit_index = self.commit_index.load(Ordering::Acquire);
            
            if commit_index > last_applied {
                self.apply_committed_entries(last_applied + 1, commit_index).await;
            }
        }
    }

    async fn start_election(&self) {
        self.metrics.elections_called.fetch_add(1, Ordering::Relaxed);
        
        {
            let mut state = self.state.write();
            if *state == RaftState::Leader {
                return;
            }
            *state = RaftState::Candidate;
        }
        
        let new_term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;
        *self.voted_for.write() = Some(self.id);
        *self.leader_id.write() = None;
        
        self.persist_state().await;
        self.reset_election_timer();
        
        let last_log_index = self.log.last_index().await.unwrap_or(0);
        let last_log_term = self.log.term_at(last_log_index).await.unwrap_or(0);
        
        let request = VoteRequest {
            term: new_term,
            candidate_id: self.id,
            last_log_index,
            last_log_term,
        };
        
        let mut votes_received = 1;
        let peers = self.get_voting_peers();
        let majority = (peers.len() + 1) / 2 + 1;
        
        let mut vote_futures = Vec::new();
        
        for peer_id in peers {
            let rpc = self.rpc.clone();
            let req = request.clone();
            
            vote_futures.push(async move {
                timeout(Duration::from_millis(100), rpc.request_vote(peer_id, req)).await
            });
        }
        
        let results = futures::future::join_all(vote_futures).await;
        
        for result in results {
            if let Ok(Ok(response)) = result {
                if response.vote_granted {
                    votes_received += 1;
                    
                    if votes_received >= majority {
                        self.become_leader().await;
                        return;
                    }
                } else if response.term > new_term {
                    self.become_follower(response.term).await;
                    return;
                }
            }
        }
        
        if *self.state.read() == RaftState::Candidate {
            self.become_follower(new_term).await;
            self.metrics.elections_lost.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn become_leader(&self) {
        {
            let mut state = self.state.write();
            if *state != RaftState::Candidate {
                return;
            }
            *state = RaftState::Leader;
        }
        
        *self.leader_id.write() = Some(self.id);
        
        self.metrics.elections_won.fetch_add(1, Ordering::Relaxed);
        
        let last_log_index = self.log.last_index().await.unwrap_or(0);
        
        let peers = self.get_all_peers();
        let mut next_index = self.next_index.write();
        let mut match_index = self.match_index.write();
        
        for peer_id in peers {
            next_index.insert(peer_id, last_log_index + 1);
            match_index.insert(peer_id, 0);
        }
        
        self.append_noop_entry().await;
        
        self.send_heartbeats().await;
        
        let _ = self.event_tx.send(RaftEvent::LeaderElected(self.id));
    }

    async fn become_follower(&self, term: Term) {
        let old_state = *self.state.read();
        
        {
            let mut state = self.state.write();
            *state = RaftState::Follower;
        }
        
        if term > self.current_term.load(Ordering::Acquire) {
            self.current_term.store(term, Ordering::Release);
            *self.voted_for.write() = None;
            self.persist_state().await;
        }
        
        self.reset_election_timer();
        
        if old_state != RaftState::Follower {
            let _ = self.event_tx.send(RaftEvent::StateChange(old_state, RaftState::Follower));
        }
    }

    async fn send_heartbeats(&self) {
        if *self.state.read() != RaftState::Leader {
            return;
        }
        
        self.reset_heartbeat_timer();
        self.metrics.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        
        let peers = self.get_all_peers();
        
        for peer_id in peers {
            let node = self.clone();
            tokio::spawn(async move {
                node.send_append_entries(peer_id).await;
            });
        }
    }

    async fn send_append_entries(&self, peer_id: NodeId) {
        let next_index = {
            let next = self.next_index.read();
            *next.get(&peer_id).unwrap_or(&1)
        };
        
        let prev_log_index = if next_index > 0 { next_index - 1 } else { 0 };
        let prev_log_term = self.log.term_at(prev_log_index).await.unwrap_or(0);
        
        let entries = self.log.entries_from(next_index, MAX_APPEND_ENTRIES_BATCH).await
            .unwrap_or_default();
        
        let request = AppendEntriesRequest {
            term: self.current_term.load(Ordering::Acquire),
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries: entries.clone(),
            leader_commit: self.commit_index.load(Ordering::Acquire),
        };
        
        match timeout(Duration::from_millis(100), self.rpc.append_entries(peer_id, request)).await {
            Ok(Ok(response)) => {
                if response.success {
                    if !entries.is_empty() {
                        let new_match_index = prev_log_index + entries.len() as u64;
                        let new_next_index = new_match_index + 1;
                        
                        self.next_index.write().insert(peer_id, new_next_index);
                        self.match_index.write().insert(peer_id, new_match_index);
                        
                        self.update_commit_index().await;
                    }
                } else {
                    if response.term > self.current_term.load(Ordering::Acquire) {
                        self.become_follower(response.term).await;
                    } else {
                        let mut next = self.next_index.write();
                        let current = *next.get(&peer_id).unwrap_or(&1);
                        next.insert(peer_id, (current - 1).max(1));
                    }
                }
            },
            _ => {},
        }
    }

    async fn update_commit_index(&self) {
        if *self.state.read() != RaftState::Leader {
            return;
        }
        
        let match_index = self.match_index.read();
        let mut indices: Vec<_> = match_index.values().cloned().collect();
        indices.push(self.log.last_index().await.unwrap_or(0));
        indices.sort_unstable();
        
        let majority_index = indices[indices.len() / 2];
        let current_term = self.current_term.load(Ordering::Acquire);
        
        if majority_index > self.commit_index.load(Ordering::Acquire) {
            if self.log.term_at(majority_index).await.unwrap_or(0) == current_term {
                self.commit_index.store(majority_index, Ordering::Release);
                self.metrics.entries_committed.fetch_add(1, Ordering::Relaxed);
                
                let _ = self.event_tx.send(RaftEvent::LogCommitted(majority_index));
                
                self.notify_pending_commands(majority_index).await;
            }
        }
    }

    async fn apply_committed_entries(&self, start: LogIndex, end: LogIndex) {
        for index in start..=end {
            if let Ok(Some(entry)) = self.log.entry_at(index).await {
                if entry.entry_type == LogEntryType::Command {
                    let result = self.state_machine.apply(entry.data.clone()).await;
                    
                    if let Some(callback) = self.pending_commands.lock().remove(&entry.id) {
                        let _ = callback.send(result);
                    }
                }
                
                self.last_applied.store(index, Ordering::Release);
            }
        }
    }

    pub async fn handle_append_entries(&self, request: AppendEntriesRequest) -> AppendEntriesResponse {
        let current_term = self.current_term.load(Ordering::Acquire);
        
        if request.term < current_term {
            return AppendEntriesResponse {
                term: current_term,
                success: false,
            };
        }
        
        if request.term > current_term {
            self.become_follower(request.term).await;
        }
        
        *self.leader_id.write() = Some(request.leader_id);
        self.reset_election_timer();
        
        if request.prev_log_index > 0 {
            match self.log.term_at(request.prev_log_index).await {
                Ok(term) if term != request.prev_log_term => {
                    return AppendEntriesResponse {
                        term: self.current_term.load(Ordering::Acquire),
                        success: false,
                    };
                },
                Err(_) => {
                    return AppendEntriesResponse {
                        term: self.current_term.load(Ordering::Acquire),
                        success: false,
                    };
                },
                _ => {},
            }
        }
        
        if !request.entries.is_empty() {
            let mut index = request.prev_log_index + 1;
            
            for entry in &request.entries {
                match self.log.term_at(index).await {
                    Ok(existing_term) if existing_term != entry.term => {
                        self.log.truncate_from(index).await.ok();
                        break;
                    },
                    Err(_) => break,
                    _ => {},
                }
                index += 1;
            }
            
            self.log.append_entries(request.entries).await.ok();
            self.metrics.entries_appended.fetch_add(1, Ordering::Relaxed);
        }
        
        if request.leader_commit > self.commit_index.load(Ordering::Acquire) {
            let last_index = self.log.last_index().await.unwrap_or(0);
            let new_commit = request.leader_commit.min(last_index);
            self.commit_index.store(new_commit, Ordering::Release);
        }
        
        AppendEntriesResponse {
            term: self.current_term.load(Ordering::Acquire),
            success: true,
        }
    }

    pub async fn handle_vote_request(&self, request: VoteRequest) -> VoteResponse {
        let current_term = self.current_term.load(Ordering::Acquire);
        
        if request.term < current_term {
            return VoteResponse {
                term: current_term,
                vote_granted: false,
            };
        }
        
        if request.term > current_term {
            self.become_follower(request.term).await;
        }
        
        let vote_granted = {
            let voted_for = self.voted_for.read();
            let can_vote = voted_for.is_none() || *voted_for == Some(request.candidate_id);
            
            if !can_vote {
                false
            } else {
                let last_log_index = self.log.last_index().await.unwrap_or(0);
                let last_log_term = self.log.term_at(last_log_index).await.unwrap_or(0);
                
                let log_up_to_date = request.last_log_term > last_log_term ||
                    (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);
                
                if log_up_to_date {
                    true
                } else {
                    false
                }
            }
        };
        
        if vote_granted {
            *self.voted_for.write() = Some(request.candidate_id);
            self.persist_state().await;
            self.reset_election_timer();
        }
        
        VoteResponse {
            term: self.current_term.load(Ordering::Acquire),
            vote_granted,
        }
    }

    pub async fn propose_command(&self, command: Command) -> Result<CommandResult, Error> {
        if *self.state.read() != RaftState::Leader {
            return Err(Error::NotLeader(self.get_leader_id()));
        }
        
        let (tx, rx) = oneshot::channel();
        let entry_id = rand::thread_rng().gen();
        
        let entry = LogEntry {
            id: entry_id,
            term: self.current_term.load(Ordering::Acquire),
            index: 0,
            entry_type: LogEntryType::Command,
            data: command.serialize()?,
        };
        
        self.pending_commands.lock().insert(entry_id, tx);
        
        self.log.append(entry).await?;
        
        self.send_heartbeats().await;
        
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(Error::Cancelled),
            Err(_) => Err(Error::RequestTimeout),
        }
    }

    async fn append_noop_entry(&self) {
        let entry = LogEntry {
            id: rand::thread_rng().gen(),
            term: self.current_term.load(Ordering::Acquire),
            index: 0,
            entry_type: LogEntryType::Noop,
            data: Vec::new(),
        };
        
        self.log.append(entry).await.ok();
    }

    async fn persist_state(&self) {
        let state = PersistentState {
            current_term: self.current_term.load(Ordering::Acquire),
            voted_for: *self.voted_for.read(),
        };
        
        self.storage.save_state(state).await.ok();
    }

    async fn create_snapshot(&self) {
        if self.snapshot_in_progress.load(Ordering::Acquire) {
            return;
        }
        
        self.snapshot_in_progress.store(true, Ordering::Release);
        
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let snapshot_data = self.state_machine.create_snapshot().await;
        
        if let Ok(data) = snapshot_data {
            let metadata = SnapshotMetadata {
                last_included_index: last_applied,
                last_included_term: self.log.term_at(last_applied).await.unwrap_or(0),
                cluster_config: self.cluster_config.read().clone(),
            };
            
            let snapshot = Snapshot {
                metadata,
                data,
            };
            
            if self.storage.save_snapshot(snapshot).await.is_ok() {
                self.log.compact_to(last_applied).await.ok();
                self.last_snapshot_index.store(last_applied, Ordering::Release);
                self.metrics.snapshots_created.fetch_add(1, Ordering::Relaxed);
                
                let _ = self.event_tx.send(RaftEvent::SnapshotCreated(last_applied));
            }
        }
        
        self.snapshot_in_progress.store(false, Ordering::Release);
    }

    async fn notify_pending_commands(&self, up_to_index: LogIndex) {
        // Implementation would notify pending commands up to the given index
    }

    fn reset_election_timer(&self) {
        let timeout = rand::thread_rng().gen_range(MIN_ELECTION_TIMEOUT..MAX_ELECTION_TIMEOUT);
        *self.election_timer.lock() = Some(Instant::now() + timeout);
    }

    fn reset_heartbeat_timer(&self) {
        *self.heartbeat_timer.lock() = Some(Instant::now() + HEARTBEAT_INTERVAL);
    }

    fn election_timeout_elapsed(&self) -> bool {
        self.election_timer.lock()
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
    }

    fn heartbeat_timeout_elapsed(&self) -> bool {
        self.heartbeat_timer.lock()
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
    }

    fn should_create_snapshot(&self) -> bool {
        let last_applied = self.last_applied.load(Ordering::Acquire);
        let last_snapshot = self.last_snapshot_index.load(Ordering::Acquire);
        
        last_applied - last_snapshot > SNAPSHOT_THRESHOLD as u64
    }

    fn get_voting_peers(&self) -> Vec<NodeId> {
        self.cluster_config.read()
            .members
            .iter()
            .filter(|m| m.id != self.id && m.voting)
            .map(|m| m.id)
            .collect()
    }

    fn get_all_peers(&self) -> Vec<NodeId> {
        self.cluster_config.read()
            .members
            .iter()
            .filter(|m| m.id != self.id)
            .map(|m| m.id)
            .collect()
    }

    fn get_leader_id(&self) -> Option<NodeId> {
        *self.leader_id.read()
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogEntryType {
    Command,
    Noop,
    Configuration,
}
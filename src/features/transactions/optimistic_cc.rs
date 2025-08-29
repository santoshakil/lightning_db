use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, HashSet, BTreeMap};
use tokio::sync::RwLock;
use bytes::Bytes;
use crate::core::error::{Error, Result};
use dashmap::DashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationResult {
    Valid,
    Invalid,
    Retry,
}

pub struct OptimisticController {
    version_manager: Arc<VersionManager>,
    validation_engine: Arc<ValidationEngine>,
    conflict_resolver: Arc<ConflictResolver>,
    timestamp_oracle: Arc<TimestampOracle>,
    read_set_tracker: Arc<ReadSetTracker>,
    write_set_buffer: Arc<WriteSetBuffer>,
    metrics: Arc<OptimisticMetrics>,
}

struct VersionManager {
    versions: Arc<DashMap<String, VersionChain>>,
    global_version: Arc<std::sync::atomic::AtomicU64>,
    gc_threshold: Duration,
    last_gc: Arc<RwLock<Instant>>,
}

struct VersionChain {
    key: String,
    versions: BTreeMap<u64, VersionEntry>,
    latest_version: u64,
    readers: HashSet<super::TransactionId>,
}

struct VersionEntry {
    version: u64,
    value: Bytes,
    timestamp: u64,
    txn_id: super::TransactionId,
    prev_version: Option<u64>,
    deleted: bool,
}

struct ValidationEngine {
    validation_strategy: ValidationStrategy,
    snapshot_validator: Arc<SnapshotValidator>,
    serialization_validator: Arc<SerializationValidator>,
    range_validator: Arc<RangeValidator>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValidationStrategy {
    Snapshot,
    Serialization,
    Range,
    Hybrid,
}

struct SnapshotValidator {
    snapshots: Arc<DashMap<super::TransactionId, TransactionSnapshot>>,
    snapshot_isolation: bool,
}

struct TransactionSnapshot {
    txn_id: super::TransactionId,
    start_timestamp: u64,
    read_set: HashSet<ReadItem>,
    write_set: HashSet<WriteItem>,
    snapshot_version: u64,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ReadItem {
    key: String,
    version: u64,
    timestamp: u64,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct WriteItem {
    key: String,
    value: Bytes,
    operation: WriteOperation,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum WriteOperation {
    Insert,
    Update,
    Delete,
    Upsert,
}

struct SerializationValidator {
    conflict_graph: Arc<RwLock<ConflictGraph>>,
    precedence_graph: Arc<RwLock<PrecedenceGraph>>,
}

struct ConflictGraph {
    nodes: HashMap<super::TransactionId, ConflictNode>,
    edges: Vec<ConflictEdge>,
}

struct ConflictNode {
    txn_id: super::TransactionId,
    read_set: HashSet<String>,
    write_set: HashSet<String>,
    commit_timestamp: Option<u64>,
}

struct ConflictEdge {
    from: super::TransactionId,
    to: super::TransactionId,
    conflict_type: ConflictType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConflictType {
    ReadWrite,
    WriteRead,
    WriteWrite,
}

struct PrecedenceGraph {
    transactions: HashMap<super::TransactionId, PrecedenceNode>,
    dependencies: Vec<(super::TransactionId, super::TransactionId)>,
}

struct PrecedenceNode {
    txn_id: super::TransactionId,
    start_time: u64,
    commit_time: Option<u64>,
    depends_on: HashSet<super::TransactionId>,
    dependents: HashSet<super::TransactionId>,
}

struct RangeValidator {
    range_locks: Arc<DashMap<RangeKey, RangeLock>>,
    phantom_prevention: bool,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct RangeKey {
    start: String,
    end: String,
    inclusive: bool,
}

struct RangeLock {
    range: RangeKey,
    holder: super::TransactionId,
    lock_type: RangeLockType,
    acquired_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeLockType {
    Shared,
    Exclusive,
    Predicate,
}

pub struct ConflictResolver {
    resolution_policy: ResolutionPolicy,
    priority_manager: Arc<PriorityManager>,
    abort_manager: Arc<AbortManager>,
    retry_manager: Arc<RetryManager>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolutionPolicy {
    FirstWins,
    LastWins,
    PriorityBased,
    CostBased,
    Hybrid,
}

struct PriorityManager {
    priorities: Arc<DashMap<super::TransactionId, TransactionPriority>>,
    dynamic_adjustment: bool,
}

struct TransactionPriority {
    base_priority: i32,
    current_priority: i32,
    age_factor: f64,
    retry_count: u32,
    resource_usage: f64,
}

struct AbortManager {
    abort_list: Arc<DashMap<super::TransactionId, AbortReason>>,
    cascading_aborts: bool,
}

#[derive(Debug, Clone)]
enum AbortReason {
    ValidationFailed,
    Deadlock,
    Timeout,
    ResourceExhaustion,
    UserRequested,
}

struct RetryManager {
    retry_queue: Arc<RwLock<VecDeque<RetryRequest>>>,
    max_retries: u32,
    backoff_strategy: BackoffStrategy,
}

struct RetryRequest {
    txn_id: super::TransactionId,
    attempt: u32,
    last_failure: Instant,
    next_retry: Instant,
}

#[derive(Debug, Clone, Copy)]
enum BackoffStrategy {
    Immediate,
    Linear { base: Duration },
    Exponential { base: Duration, max: Duration },
    Adaptive,
}

struct TimestampOracle {
    current_timestamp: Arc<std::sync::atomic::AtomicU64>,
    timestamp_cache: Arc<DashMap<super::TransactionId, u64>>,
    clock_skew_tolerance: Duration,
}

struct ReadSetTracker {
    read_sets: Arc<DashMap<super::TransactionId, HashSet<TrackedRead>>>,
    read_dependencies: Arc<RwLock<HashMap<String, HashSet<super::TransactionId>>>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TrackedRead {
    key: String,
    version: u64,
    read_time: u64,
    value_hash: u64,
}

struct WriteSetBuffer {
    write_buffers: Arc<DashMap<super::TransactionId, WriteBuffer>>,
    pending_writes: Arc<RwLock<BTreeMap<u64, Vec<BufferedWrite>>>>,
}

struct WriteBuffer {
    txn_id: super::TransactionId,
    writes: Vec<BufferedWrite>,
    estimated_size: usize,
    created_at: Instant,
}

struct BufferedWrite {
    key: String,
    value: Bytes,
    operation: WriteOperation,
    timestamp: u64,
}

struct OptimisticMetrics {
    total_validations: Arc<std::sync::atomic::AtomicU64>,
    successful_validations: Arc<std::sync::atomic::AtomicU64>,
    failed_validations: Arc<std::sync::atomic::AtomicU64>,
    retry_count: Arc<std::sync::atomic::AtomicU64>,
    conflict_count: Arc<std::sync::atomic::AtomicU64>,
    abort_count: Arc<std::sync::atomic::AtomicU64>,
    avg_read_set_size: Arc<std::sync::atomic::AtomicU64>,
    avg_write_set_size: Arc<std::sync::atomic::AtomicU64>,
}

use std::collections::VecDeque;

impl OptimisticController {
    pub fn new(validation_strategy: ValidationStrategy) -> Self {
        Self {
            version_manager: Arc::new(VersionManager {
                versions: Arc::new(DashMap::new()),
                global_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
                gc_threshold: Duration::from_secs(300),
                last_gc: Arc::new(RwLock::new(Instant::now())),
            }),
            validation_engine: Arc::new(ValidationEngine {
                validation_strategy,
                snapshot_validator: Arc::new(SnapshotValidator {
                    snapshots: Arc::new(DashMap::new()),
                    snapshot_isolation: true,
                }),
                serialization_validator: Arc::new(SerializationValidator {
                    conflict_graph: Arc::new(RwLock::new(ConflictGraph {
                        nodes: HashMap::new(),
                        edges: Vec::new(),
                    })),
                    precedence_graph: Arc::new(RwLock::new(PrecedenceGraph {
                        transactions: HashMap::new(),
                        dependencies: Vec::new(),
                    })),
                }),
                range_validator: Arc::new(RangeValidator {
                    range_locks: Arc::new(DashMap::new()),
                    phantom_prevention: true,
                }),
            }),
            conflict_resolver: Arc::new(ConflictResolver {
                resolution_policy: ResolutionPolicy::PriorityBased,
                priority_manager: Arc::new(PriorityManager {
                    priorities: Arc::new(DashMap::new()),
                    dynamic_adjustment: true,
                }),
                abort_manager: Arc::new(AbortManager {
                    abort_list: Arc::new(DashMap::new()),
                    cascading_aborts: false,
                }),
                retry_manager: Arc::new(RetryManager {
                    retry_queue: Arc::new(RwLock::new(VecDeque::new())),
                    max_retries: 3,
                    backoff_strategy: BackoffStrategy::Exponential {
                        base: Duration::from_millis(10),
                        max: Duration::from_secs(1),
                    },
                }),
            }),
            timestamp_oracle: Arc::new(TimestampOracle {
                current_timestamp: Arc::new(std::sync::atomic::AtomicU64::new(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
                )),
                timestamp_cache: Arc::new(DashMap::new()),
                clock_skew_tolerance: Duration::from_millis(100),
            }),
            read_set_tracker: Arc::new(ReadSetTracker {
                read_sets: Arc::new(DashMap::new()),
                read_dependencies: Arc::new(RwLock::new(HashMap::new())),
            }),
            write_set_buffer: Arc::new(WriteSetBuffer {
                write_buffers: Arc::new(DashMap::new()),
                pending_writes: Arc::new(RwLock::new(BTreeMap::new())),
            }),
            metrics: Arc::new(OptimisticMetrics {
                total_validations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                successful_validations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                failed_validations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                retry_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                conflict_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                abort_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                avg_read_set_size: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                avg_write_set_size: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
        }
    }

    pub async fn begin_transaction(&self, txn_id: super::TransactionId) -> Result<u64> {
        let timestamp = self.timestamp_oracle.current_timestamp
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        self.timestamp_oracle.timestamp_cache.insert(txn_id, timestamp);
        
        self.validation_engine.snapshot_validator.snapshots.insert(
            txn_id,
            TransactionSnapshot {
                txn_id,
                start_timestamp: timestamp,
                read_set: HashSet::new(),
                write_set: HashSet::new(),
                snapshot_version: self.version_manager.global_version
                    .load(std::sync::atomic::Ordering::SeqCst),
            },
        );
        
        self.read_set_tracker.read_sets.insert(txn_id, HashSet::new());
        
        self.write_set_buffer.write_buffers.insert(
            txn_id,
            WriteBuffer {
                txn_id,
                writes: Vec::new(),
                estimated_size: 0,
                created_at: Instant::now(),
            },
        );
        
        Ok(timestamp)
    }

    pub async fn read(
        &self,
        txn_id: super::TransactionId,
        key: String,
    ) -> Result<Option<Bytes>> {
        let timestamp = self.timestamp_oracle.timestamp_cache
            .get(&txn_id)
            .map(|t| *t)
            .ok_or_else(|| Error::Custom("Transaction not found".to_string()))?;
        
        if let Some(write_buffer) = self.write_set_buffer.write_buffers.get(&txn_id) {
            for write in write_buffer.writes.iter().rev() {
                if write.key == key {
                    match write.operation {
                        WriteOperation::Delete => return Ok(None),
                        _ => return Ok(Some(write.value.clone())),
                    }
                }
            }
        }
        
        let version_chain = self.version_manager.versions
            .entry(key.clone())
            .or_insert_with(|| VersionChain {
                key: key.clone(),
                versions: BTreeMap::new(),
                latest_version: 0,
                readers: HashSet::new(),
            });
        
        let value = version_chain
            .versions
            .range(..=timestamp)
            .rev()
            .find(|(_, entry)| !entry.deleted)
            .map(|(_, entry)| entry.value.clone());
        
        if let Some(ref val) = value {
            let read_item = TrackedRead {
                key: key.clone(),
                version: version_chain.latest_version,
                read_time: timestamp,
                value_hash: self.hash_value(val),
            };
            
            if let Some(mut read_set) = self.read_set_tracker.read_sets.get_mut(&txn_id) {
                read_set.insert(read_item);
            }
            
            let mut read_deps = self.read_set_tracker.read_dependencies.write().await;
            read_deps.entry(key).or_insert_with(HashSet::new).insert(txn_id);
        }
        
        self.metrics.avg_read_set_size.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(value)
    }

    pub async fn write(
        &self,
        txn_id: super::TransactionId,
        key: String,
        value: Bytes,
        operation: WriteOperation,
    ) -> Result<()> {
        let timestamp = self.timestamp_oracle.timestamp_cache
            .get(&txn_id)
            .map(|t| *t)
            .ok_or_else(|| Error::Custom("Transaction not found".to_string()))?;
        
        if let Some(mut write_buffer) = self.write_set_buffer.write_buffers.get_mut(&txn_id) {
            write_buffer.writes.push(BufferedWrite {
                key,
                value,
                operation,
                timestamp,
            });
            write_buffer.estimated_size += std::mem::size_of::<BufferedWrite>();
        }
        
        self.metrics.avg_write_set_size.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }

    pub async fn validate(&self, txn_id: super::TransactionId) -> Result<ValidationResult> {
        self.metrics.total_validations.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        let result = match self.validation_engine.validation_strategy {
            ValidationStrategy::Snapshot => {
                self.validate_snapshot(txn_id).await?
            }
            ValidationStrategy::Serialization => {
                self.validate_serialization(txn_id).await?
            }
            ValidationStrategy::Range => {
                self.validate_range(txn_id).await?
            }
            ValidationStrategy::Hybrid => {
                let snapshot_valid = self.validate_snapshot(txn_id).await?;
                if snapshot_valid != ValidationResult::Valid {
                    snapshot_valid
                } else {
                    self.validate_serialization(txn_id).await?
                }
            }
        };
        
        match result {
            ValidationResult::Valid => {
                self.metrics.successful_validations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            ValidationResult::Invalid => {
                self.metrics.failed_validations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics.conflict_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            ValidationResult::Retry => {
                self.metrics.retry_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        
        Ok(result)
    }

    async fn validate_snapshot(&self, txn_id: super::TransactionId) -> Result<ValidationResult> {
        let snapshot = self.validation_engine.snapshot_validator.snapshots
            .get(&txn_id)
            .ok_or_else(|| Error::Custom("Snapshot not found".to_string()))?;
        
        if let Some(read_set) = self.read_set_tracker.read_sets.get(&txn_id) {
            for read in read_set.iter() {
                if let Some(version_chain) = self.version_manager.versions.get(&read.key) {
                    let current_version = version_chain.latest_version;
                    
                    if current_version > read.version {
                        if let Some(newer_entry) = version_chain.versions
                            .range((read.read_time + 1)..) 
                            .next() {
                            if newer_entry.1.timestamp < snapshot.start_timestamp {
                                return Ok(ValidationResult::Invalid);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(ValidationResult::Valid)
    }

    async fn validate_serialization(&self, txn_id: super::TransactionId) -> Result<ValidationResult> {
        let mut conflict_graph = self.validation_engine.serialization_validator
            .conflict_graph.write().await;
        
        let read_set = self.read_set_tracker.read_sets
            .get(&txn_id)
            .map(|s| s.iter().map(|r| r.key.clone()).collect::<HashSet<_>>())
            .unwrap_or_default();
        
        let write_set = self.write_set_buffer.write_buffers
            .get(&txn_id)
            .map(|b| b.writes.iter().map(|w| w.key.clone()).collect::<HashSet<_>>())
            .unwrap_or_default();
        
        conflict_graph.nodes.insert(
            txn_id,
            ConflictNode {
                txn_id,
                read_set: read_set.clone(),
                write_set: write_set.clone(),
                commit_timestamp: None,
            },
        );
        
        let mut new_edges = Vec::new();
        for (other_txn, other_node) in &conflict_graph.nodes {
            if *other_txn == txn_id {
                continue;
            }
            
            for key in &write_set {
                if other_node.read_set.contains(key) {
                    new_edges.push(ConflictEdge {
                        from: txn_id,
                        to: *other_txn,
                        conflict_type: ConflictType::WriteRead,
                    });
                }
            }
            
            for key in &read_set {
                if other_node.write_set.contains(key) {
                    new_edges.push(ConflictEdge {
                        from: *other_txn,
                        to: txn_id,
                        conflict_type: ConflictType::ReadWrite,
                    });
                }
            }
            
            for key in &write_set {
                if other_node.write_set.contains(key) {
                    new_edges.push(ConflictEdge {
                        from: txn_id,
                        to: *other_txn,
                        conflict_type: ConflictType::WriteWrite,
                    });
                }
            }
        }
        conflict_graph.edges.extend(new_edges);
        
        if self.has_cycle(&conflict_graph) {
            Ok(ValidationResult::Invalid)
        } else {
            Ok(ValidationResult::Valid)
        }
    }

    async fn validate_range(&self, txn_id: super::TransactionId) -> Result<ValidationResult> {
        Ok(ValidationResult::Valid)
    }

    fn has_cycle(&self, graph: &ConflictGraph) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        
        for node in graph.nodes.keys() {
            if self.has_cycle_util(node, &graph, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        
        false
    }

    fn has_cycle_util(
        &self,
        node: &super::TransactionId,
        graph: &ConflictGraph,
        visited: &mut HashSet<super::TransactionId>,
        rec_stack: &mut HashSet<super::TransactionId>,
    ) -> bool {
        if !visited.contains(node) {
            visited.insert(*node);
            rec_stack.insert(*node);
            
            for edge in &graph.edges {
                if edge.from == *node {
                    if !visited.contains(&edge.to) {
                        if self.has_cycle_util(&edge.to, graph, visited, rec_stack) {
                            return true;
                        }
                    } else if rec_stack.contains(&edge.to) {
                        return true;
                    }
                }
            }
        }
        
        rec_stack.remove(node);
        false
    }

    pub async fn commit(&self, txn_id: super::TransactionId) -> Result<()> {
        let validation = self.validate(txn_id).await?;
        
        match validation {
            ValidationResult::Valid => {
                self.apply_writes(txn_id).await?;
                self.cleanup_transaction(txn_id).await?;
                Ok(())
            }
            ValidationResult::Invalid => {
                self.abort(txn_id).await?;
                Err(Error::Custom("Validation failed".to_string()))
            }
            ValidationResult::Retry => {
                self.schedule_retry(txn_id).await?;
                Err(Error::Custom("Transaction needs retry".to_string()))
            }
        }
    }

    async fn apply_writes(&self, txn_id: super::TransactionId) -> Result<()> {
        let commit_timestamp = self.timestamp_oracle.current_timestamp
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        if let Some((_, write_buffer)) = self.write_set_buffer.write_buffers.remove(&txn_id) {
            for write in write_buffer.writes {
                let mut version_chain = self.version_manager.versions
                    .entry(write.key.clone())
                    .or_insert_with(|| VersionChain {
                        key: write.key.clone(),
                        versions: BTreeMap::new(),
                        latest_version: 0,
                        readers: HashSet::new(),
                    });
                
                let latest_version = version_chain.latest_version;
                let new_version = latest_version + 1;
                
                version_chain.versions.insert(
                    commit_timestamp,
                    VersionEntry {
                        version: new_version,
                        value: write.value,
                        timestamp: commit_timestamp,
                        txn_id,
                        prev_version: Some(latest_version),
                        deleted: write.operation == WriteOperation::Delete,
                    },
                );
                
                version_chain.latest_version = new_version;
            }
        }
        
        self.version_manager.global_version.store(
            commit_timestamp,
            std::sync::atomic::Ordering::SeqCst,
        );
        
        Ok(())
    }

    pub async fn abort(&self, txn_id: super::TransactionId) -> Result<()> {
        self.metrics.abort_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        self.conflict_resolver.abort_manager.abort_list.insert(
            txn_id,
            AbortReason::ValidationFailed,
        );
        
        self.cleanup_transaction(txn_id).await?;
        
        Ok(())
    }

    async fn cleanup_transaction(&self, txn_id: super::TransactionId) -> Result<()> {
        self.validation_engine.snapshot_validator.snapshots.remove(&txn_id);
        self.timestamp_oracle.timestamp_cache.remove(&txn_id);
        self.read_set_tracker.read_sets.remove(&txn_id);
        self.write_set_buffer.write_buffers.remove(&txn_id);
        
        let mut read_deps = self.read_set_tracker.read_dependencies.write().await;
        for deps in read_deps.values_mut() {
            deps.remove(&txn_id);
        }
        
        Ok(())
    }

    async fn schedule_retry(&self, txn_id: super::TransactionId) -> Result<()> {
        let mut retry_queue = self.conflict_resolver.retry_manager.retry_queue.write().await;
        
        let next_retry = match self.conflict_resolver.retry_manager.backoff_strategy {
            BackoffStrategy::Immediate => Instant::now(),
            BackoffStrategy::Linear { base } => Instant::now() + base,
            BackoffStrategy::Exponential { base, max } => {
                let delay = std::cmp::min(base * 2, max);
                Instant::now() + delay
            }
            BackoffStrategy::Adaptive => Instant::now() + Duration::from_millis(100),
        };
        
        retry_queue.push_back(RetryRequest {
            txn_id,
            attempt: 1,
            last_failure: Instant::now(),
            next_retry,
        });
        
        Ok(())
    }

    fn hash_value(&self, value: &Bytes) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    pub async fn garbage_collect(&self) -> Result<()> {
        let now = Instant::now();
        let mut last_gc = self.version_manager.last_gc.write().await;
        
        if now.duration_since(*last_gc) < self.version_manager.gc_threshold {
            return Ok(());
        }
        
        let min_timestamp = self.validation_engine.snapshot_validator.snapshots
            .iter()
            .map(|entry| entry.value().start_timestamp)
            .min()
            .unwrap_or(u64::MAX);
        
        for mut version_chain in self.version_manager.versions.iter_mut() {
            let keys_to_remove: Vec<u64> = version_chain.versions
                .range(..min_timestamp)
                .skip(1)
                .map(|(k, _)| *k)
                .collect();
            
            for key in keys_to_remove {
                version_chain.versions.remove(&key);
            }
        }
        
        *last_gc = now;
        
        Ok(())
    }
}
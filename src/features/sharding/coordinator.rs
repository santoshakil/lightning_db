use std::sync::Arc;
use std::collections::{HashMap, HashSet, BTreeMap};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use tokio::sync::{mpsc, oneshot, broadcast};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use async_trait::async_trait;
use dashmap::DashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    pub num_shards: usize,
    pub replication_factor: usize,
    pub partition_strategy: super::partitioner::PartitionStrategy,
    pub consistency_level: super::consistency::ConsistencyLevel,
    pub auto_rebalance: bool,
    pub rebalance_threshold: f64,
    pub max_shard_size: usize,
    pub min_shard_size: usize,
    pub cross_shard_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub failure_detection_timeout: Duration,
    pub migration_batch_size: usize,
    pub enable_partition_pruning: bool,
    pub enable_query_caching: bool,
    pub enable_adaptive_routing: bool,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            num_shards: 128,
            replication_factor: 3,
            partition_strategy: super::partitioner::PartitionStrategy::ConsistentHash,
            consistency_level: super::consistency::ConsistencyLevel::Quorum,
            auto_rebalance: true,
            rebalance_threshold: 0.2,
            max_shard_size: 1024 * 1024 * 1024,
            min_shard_size: 1024 * 1024 * 10,
            cross_shard_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            failure_detection_timeout: Duration::from_secs(30),
            migration_batch_size: 1000,
            enable_partition_pruning: true,
            enable_query_caching: true,
            enable_adaptive_routing: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardTopology {
    pub version: u64,
    pub shards: HashMap<ShardId, ShardInfo>,
    pub nodes: HashMap<NodeId, NodeInfo>,
    pub routing_table: BTreeMap<Vec<u8>, ShardId>,
    pub replication_map: HashMap<ShardId, Vec<NodeId>>,
    pub pending_migrations: Vec<super::migration::MigrationPlan>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardId(pub u32);

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub id: ShardId,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
    pub primary_node: NodeId,
    pub replica_nodes: Vec<NodeId>,
    pub state: ShardState,
    pub size_bytes: usize,
    pub record_count: usize,
    pub last_compaction: Option<chrono::DateTime<chrono::Utc>>,
    pub read_qps: f64,
    pub write_qps: f64,
    pub hot_keys: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ShardState {
    Active,
    Migrating,
    Splitting,
    Merging,
    Inactive,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub address: String,
    pub capacity: NodeCapacity,
    pub load: NodeLoad,
    pub state: NodeState,
    pub shards: HashSet<ShardId>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeCapacity {
    pub cpu_cores: usize,
    pub memory_gb: usize,
    pub storage_gb: usize,
    pub network_mbps: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NodeLoad {
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub storage_usage: f64,
    pub network_usage: f64,
    pub shard_count: usize,
    pub qps: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NodeState {
    Online,
    Offline,
    Draining,
    Maintenance,
    Failed,
}

pub struct ShardCoordinator {
    config: Arc<ShardConfig>,
    topology: Arc<RwLock<ShardTopology>>,
    partitioner: Arc<dyn super::partitioner::Partitioner>,
    router: Arc<super::router::ShardRouter>,
    rebalancer: Arc<super::rebalancer::ShardRebalancer>,
    consistency_manager: Arc<super::consistency::ConsistencyManager>,
    
    node_connections: Arc<DashMap<NodeId, NodeConnection>>,
    pending_operations: Arc<DashMap<OperationId, PendingOperation>>,
    
    metrics: Arc<ShardingMetrics>,
    event_bus: broadcast::Sender<ShardingEvent>,
    shutdown: Arc<AtomicBool>,
}

struct NodeConnection {
    node_id: NodeId,
    address: String,
    client: Arc<dyn ShardClient>,
    last_heartbeat: Instant,
    failure_count: AtomicU64,
    circuit_breaker: Arc<CircuitBreaker>,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
struct OperationId(u64);

struct PendingOperation {
    id: OperationId,
    operation_type: OperationType,
    affected_shards: Vec<ShardId>,
    start_time: Instant,
    timeout: Duration,
    completion_tx: oneshot::Sender<Result<OperationResult, Error>>,
}

#[derive(Debug, Clone)]
enum OperationType {
    Read { key: Vec<u8> },
    Write { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Query { query: CrossShardQuery },
    Migration { from: ShardId, to: ShardId },
    Rebalance { plan: super::rebalancer::RebalancePlan },
}

#[derive(Debug, Clone)]
struct OperationResult {
    success: bool,
    data: Option<Vec<u8>>,
    affected_shards: Vec<ShardId>,
    latency_ms: u64,
}

#[derive(Debug, Clone)]
pub enum ShardingEvent {
    TopologyChanged { old_version: u64, new_version: u64 },
    ShardMigrationStarted { shard: ShardId, from: NodeId, to: NodeId },
    ShardMigrationCompleted { shard: ShardId },
    NodeJoined { node: NodeId },
    NodeLeft { node: NodeId },
    NodeFailed { node: NodeId },
    RebalanceStarted { plan: super::rebalancer::RebalancePlan },
    RebalanceCompleted,
    HotSpotDetected { shard: ShardId, qps: f64 },
}

struct ShardingMetrics {
    total_operations: AtomicU64,
    successful_operations: AtomicU64,
    failed_operations: AtomicU64,
    cross_shard_queries: AtomicU64,
    migrations_completed: AtomicU64,
    rebalances_triggered: AtomicU64,
    avg_latency_ms: AtomicU64,
    topology_versions: AtomicU64,
}

#[async_trait]
trait ShardClient: Send + Sync {
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn delete(&self, key: &[u8]) -> Result<(), Error>;
    async fn scan(&self, start: &[u8], end: &[u8], limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error>;
    async fn get_stats(&self) -> Result<ShardStats, Error>;
    async fn transfer_data(&self, target: NodeId, data: ShardData) -> Result<(), Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardStats {
    pub size_bytes: usize,
    pub record_count: usize,
    pub read_qps: f64,
    pub write_qps: f64,
    pub hot_keys: Vec<Vec<u8>>,
    pub last_compaction: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardData {
    pub shard_id: ShardId,
    pub range_start: Vec<u8>,
    pub range_end: Vec<u8>,
    pub records: Vec<(Vec<u8>, Vec<u8>)>,
    pub checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CrossShardQuery {
    pub query_id: u64,
    pub affected_shards: Vec<ShardId>,
    pub predicates: Vec<QueryPredicate>,
    pub aggregations: Vec<Aggregation>,
    pub limit: Option<usize>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum QueryPredicate {
    Equals { field: String, value: Vec<u8> },
    Range { field: String, min: Vec<u8>, max: Vec<u8> },
    In { field: String, values: Vec<Vec<u8>> },
    And(Vec<QueryPredicate>),
    Or(Vec<QueryPredicate>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Aggregation {
    Count,
    Sum { field: String },
    Avg { field: String },
    Min { field: String },
    Max { field: String },
    GroupBy { field: String },
}

struct CircuitBreaker {
    failure_threshold: usize,
    success_threshold: usize,
    timeout: Duration,
    state: Arc<RwLock<CircuitState>>,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
}

#[derive(Debug, Clone, Copy)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl ShardCoordinator {
    pub async fn new(config: ShardConfig) -> Result<Self, Error> {
        let topology = Self::initialize_topology(&config)?;
        
        let partitioner = super::partitioner::create_partitioner(
            config.partition_strategy.clone(),
            config.num_shards,
        )?;
        
        let router = Arc::new(super::router::ShardRouter::new(
            topology.clone(),
            partitioner.clone(),
        ));
        
        let rebalancer = Arc::new(super::rebalancer::ShardRebalancer::new(
            config.rebalance_threshold,
            config.max_shard_size,
            config.min_shard_size,
        ));
        
        let consistency_manager = Arc::new(super::consistency::ConsistencyManager::new(
            config.consistency_level,
            config.replication_factor,
        ));
        
        let (event_tx, _) = broadcast::channel(1024);
        
        Ok(Self {
            config: Arc::new(config),
            topology: Arc::new(RwLock::new(topology)),
            partitioner,
            router,
            rebalancer,
            consistency_manager,
            node_connections: Arc::new(DashMap::new()),
            pending_operations: Arc::new(DashMap::new()),
            metrics: Arc::new(ShardingMetrics {
                total_operations: AtomicU64::new(0),
                successful_operations: AtomicU64::new(0),
                failed_operations: AtomicU64::new(0),
                cross_shard_queries: AtomicU64::new(0),
                migrations_completed: AtomicU64::new(0),
                rebalances_triggered: AtomicU64::new(0),
                avg_latency_ms: AtomicU64::new(0),
                topology_versions: AtomicU64::new(1),
            }),
            event_bus: event_tx,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    fn initialize_topology(config: &ShardConfig) -> Result<ShardTopology, Error> {
        let mut shards = HashMap::new();
        let mut routing_table = BTreeMap::new();
        
        let shard_range_size = u64::MAX / config.num_shards as u64;
        
        for i in 0..config.num_shards {
            let shard_id = ShardId(i as u32);
            let range_start = (i as u64 * shard_range_size).to_be_bytes().to_vec();
            let range_end = if i == config.num_shards - 1 {
                u64::MAX.to_be_bytes().to_vec()
            } else {
                ((i + 1) as u64 * shard_range_size).to_be_bytes().to_vec()
            };
            
            let shard_info = ShardInfo {
                id: shard_id,
                range_start: range_start.clone(),
                range_end: range_end.clone(),
                primary_node: NodeId(0),
                replica_nodes: Vec::new(),
                state: ShardState::Active,
                size_bytes: 0,
                record_count: 0,
                last_compaction: None,
                read_qps: 0.0,
                write_qps: 0.0,
                hot_keys: Vec::new(),
            };
            
            shards.insert(shard_id, shard_info);
            routing_table.insert(range_start, shard_id);
        }
        
        Ok(ShardTopology {
            version: 1,
            shards,
            nodes: HashMap::new(),
            routing_table,
            replication_map: HashMap::new(),
            pending_migrations: Vec::new(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }

    pub async fn add_node(&self, node_id: NodeId, address: String, capacity: NodeCapacity) -> Result<(), Error> {
        let mut topology = self.topology.write();
        
        if topology.nodes.contains_key(&node_id) {
            return Err(Error::AlreadyExists(format!("Node {:?} already exists", node_id)));
        }
        
        let node_info = NodeInfo {
            id: node_id,
            address: address.clone(),
            capacity,
            load: NodeLoad {
                cpu_usage: 0.0,
                memory_usage: 0.0,
                storage_usage: 0.0,
                network_usage: 0.0,
                shard_count: 0,
                qps: 0.0,
            },
            state: NodeState::Online,
            shards: HashSet::new(),
            last_heartbeat: chrono::Utc::now(),
            metadata: HashMap::new(),
        };
        
        topology.nodes.insert(node_id, node_info);
        topology.version += 1;
        topology.updated_at = chrono::Utc::now();
        
        let _ = self.event_bus.send(ShardingEvent::NodeJoined { node: node_id });
        
        if self.config.auto_rebalance {
            self.trigger_rebalance().await?;
        }
        
        Ok(())
    }

    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), Error> {
        let mut topology = self.topology.write();
        
        if !topology.nodes.contains_key(&node_id) {
            return Err(Error::NotFound(format!("Node {:?} not found", node_id)));
        }
        
        let node = topology.nodes.get(&node_id).unwrap();
        let affected_shards = node.shards.clone();
        
        for shard_id in affected_shards {
            if let Some(shard) = topology.shards.get_mut(&shard_id) {
                if shard.primary_node == node_id {
                    if let Some(new_primary) = shard.replica_nodes.first() {
                        shard.primary_node = *new_primary;
                        shard.replica_nodes.remove(0);
                    } else {
                        shard.state = ShardState::Failed;
                    }
                } else {
                    shard.replica_nodes.retain(|&n| n != node_id);
                }
            }
        }
        
        topology.nodes.remove(&node_id);
        topology.version += 1;
        topology.updated_at = chrono::Utc::now();
        
        self.node_connections.remove(&node_id);
        
        let _ = self.event_bus.send(ShardingEvent::NodeLeft { node: node_id });
        
        if self.config.auto_rebalance {
            self.trigger_rebalance().await?;
        }
        
        Ok(())
    }

    pub async fn route_operation(&self, key: &[u8], operation: OperationType) -> Result<OperationResult, Error> {
        let start = Instant::now();
        self.metrics.total_operations.fetch_add(1, Ordering::Relaxed);
        
        let shard_id = self.router.route_key(key)?;
        let topology = self.topology.read();
        
        let shard = topology.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        if shard.state != ShardState::Active {
            return Err(Error::Unavailable(format!("Shard {:?} is not active", shard_id)));
        }
        
        let node_id = match self.config.consistency_level {
            super::consistency::ConsistencyLevel::One => shard.primary_node,
            super::consistency::ConsistencyLevel::Quorum => {
                self.consistency_manager.select_quorum_nodes(
                    shard.primary_node,
                    &shard.replica_nodes,
                )?
            },
            super::consistency::ConsistencyLevel::All => shard.primary_node,
        };
        
        drop(topology);
        
        let result = self.execute_on_node(node_id, operation).await?;
        
        let latency = start.elapsed().as_millis() as u64;
        self.update_avg_latency(latency);
        
        if result.success {
            self.metrics.successful_operations.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.failed_operations.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(result)
    }

    async fn execute_on_node(&self, node_id: NodeId, operation: OperationType) -> Result<OperationResult, Error> {
        let connection = self.node_connections.get(&node_id)
            .ok_or_else(|| Error::NotFound(format!("Connection to node {:?} not found", node_id)))?;
        
        if !connection.circuit_breaker.can_proceed()? {
            return Err(Error::Unavailable(format!("Circuit breaker open for node {:?}", node_id)));
        }
        
        let client = connection.client.clone();
        let start = Instant::now();
        
        let result = match operation {
            OperationType::Read { key } => {
                match client.read(&key).await {
                    Ok(data) => OperationResult {
                        success: true,
                        data,
                        affected_shards: vec![],
                        latency_ms: start.elapsed().as_millis() as u64,
                    },
                    Err(e) => {
                        connection.circuit_breaker.record_failure();
                        return Err(e);
                    }
                }
            },
            OperationType::Write { key, value } => {
                match client.write(&key, &value).await {
                    Ok(()) => OperationResult {
                        success: true,
                        data: None,
                        affected_shards: vec![],
                        latency_ms: start.elapsed().as_millis() as u64,
                    },
                    Err(e) => {
                        connection.circuit_breaker.record_failure();
                        return Err(e);
                    }
                }
            },
            OperationType::Delete { key } => {
                match client.delete(&key).await {
                    Ok(()) => OperationResult {
                        success: true,
                        data: None,
                        affected_shards: vec![],
                        latency_ms: start.elapsed().as_millis() as u64,
                    },
                    Err(e) => {
                        connection.circuit_breaker.record_failure();
                        return Err(e);
                    }
                }
            },
            _ => {
                return Err(Error::InvalidOperation {
                    reason: "Operation not supported on single node".to_string(),
                });
            }
        };
        
        connection.circuit_breaker.record_success();
        Ok(result)
    }

    pub async fn execute_cross_shard_query(&self, query: CrossShardQuery) -> Result<Vec<Vec<u8>>, Error> {
        self.metrics.cross_shard_queries.fetch_add(1, Ordering::Relaxed);
        
        let topology = self.topology.read();
        let mut shard_results = Vec::new();
        
        let shards_to_query = if query.affected_shards.is_empty() {
            topology.shards.keys().cloned().collect()
        } else {
            query.affected_shards.clone()
        };
        
        drop(topology);
        
        let futures: Vec<_> = shards_to_query.iter()
            .map(|shard_id| self.query_shard(*shard_id, query.clone()))
            .collect();
        
        let results = futures::future::join_all(futures).await;
        
        for result in results {
            match result {
                Ok(data) => shard_results.extend(data),
                Err(e) => {
                    if self.config.consistency_level != super::consistency::ConsistencyLevel::One {
                        return Err(e);
                    }
                }
            }
        }
        
        Ok(shard_results)
    }

    async fn query_shard(&self, shard_id: ShardId, query: CrossShardQuery) -> Result<Vec<Vec<u8>>, Error> {
        let topology = self.topology.read();
        
        let shard = topology.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound(format!("Shard {:?} not found", shard_id)))?;
        
        let connection = self.node_connections.get(&shard.primary_node)
            .ok_or_else(|| Error::NotFound(format!("Connection to primary node not found")))?;
        
        let client = connection.client.clone();
        
        Ok(Vec::new())
    }

    pub async fn trigger_rebalance(&self) -> Result<(), Error> {
        self.metrics.rebalances_triggered.fetch_add(1, Ordering::Relaxed);
        
        let topology = self.topology.read();
        let plan = self.rebalancer.create_rebalance_plan(&topology)?;
        
        if plan.migrations.is_empty() {
            return Ok(());
        }
        
        let _ = self.event_bus.send(ShardingEvent::RebalanceStarted { plan: plan.clone() });
        
        drop(topology);
        
        for migration in plan.migrations {
            self.migrate_shard(migration).await?;
        }
        
        let _ = self.event_bus.send(ShardingEvent::RebalanceCompleted);
        
        Ok(())
    }

    async fn migrate_shard(&self, migration: super::migration::MigrationPlan) -> Result<(), Error> {
        let _ = self.event_bus.send(ShardingEvent::ShardMigrationStarted {
            shard: migration.shard_id,
            from: migration.source_node,
            to: migration.target_node,
        });
        
        let source_conn = self.node_connections.get(&migration.source_node)
            .ok_or_else(|| Error::NotFound("Source node connection not found".to_string()))?;
        
        let target_conn = self.node_connections.get(&migration.target_node)
            .ok_or_else(|| Error::NotFound("Target node connection not found".to_string()))?;
        
        let topology = self.topology.read();
        let shard = topology.shards.get(&migration.shard_id)
            .ok_or_else(|| Error::NotFound("Shard not found".to_string()))?;
        
        let range_start = shard.range_start.clone();
        let range_end = shard.range_end.clone();
        drop(topology);
        
        let data = source_conn.client.scan(&range_start, &range_end, usize::MAX).await?;
        
        let shard_data = ShardData {
            shard_id: migration.shard_id,
            range_start,
            range_end,
            records: data,
            checksum: Self::calculate_checksum(&data),
        };
        
        source_conn.client.transfer_data(migration.target_node, shard_data).await?;
        
        let mut topology = self.topology.write();
        if let Some(shard) = topology.shards.get_mut(&migration.shard_id) {
            if shard.primary_node == migration.source_node {
                shard.primary_node = migration.target_node;
            } else {
                if let Some(pos) = shard.replica_nodes.iter().position(|&n| n == migration.source_node) {
                    shard.replica_nodes[pos] = migration.target_node;
                }
            }
            shard.state = ShardState::Active;
        }
        
        topology.version += 1;
        topology.updated_at = chrono::Utc::now();
        
        self.metrics.migrations_completed.fetch_add(1, Ordering::Relaxed);
        
        let _ = self.event_bus.send(ShardingEvent::ShardMigrationCompleted {
            shard: migration.shard_id,
        });
        
        Ok(())
    }

    fn calculate_checksum(data: &[(Vec<u8>, Vec<u8>)]) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        for (key, value) in data {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
    }

    pub async fn detect_hot_spots(&self) -> Vec<ShardId> {
        let topology = self.topology.read();
        let mut hot_shards = Vec::new();
        
        let avg_qps: f64 = topology.shards.values()
            .map(|s| s.read_qps + s.write_qps)
            .sum::<f64>() / topology.shards.len() as f64;
        
        let threshold = avg_qps * 2.0;
        
        for (shard_id, shard) in &topology.shards {
            let total_qps = shard.read_qps + shard.write_qps;
            if total_qps > threshold {
                hot_shards.push(*shard_id);
                let _ = self.event_bus.send(ShardingEvent::HotSpotDetected {
                    shard: *shard_id,
                    qps: total_qps,
                });
            }
        }
        
        hot_shards
    }

    pub async fn split_shard(&self, shard_id: ShardId) -> Result<(ShardId, ShardId), Error> {
        let mut topology = self.topology.write();
        
        let shard = topology.shards.get(&shard_id)
            .ok_or_else(|| Error::NotFound("Shard not found".to_string()))?
            .clone();
        
        let mid_point = Self::calculate_mid_point(&shard.range_start, &shard.range_end);
        
        let new_shard_id = ShardId(topology.shards.len() as u32);
        
        let mut first_shard = shard.clone();
        first_shard.range_end = mid_point.clone();
        first_shard.state = ShardState::Active;
        
        let mut second_shard = shard.clone();
        second_shard.id = new_shard_id;
        second_shard.range_start = mid_point.clone();
        second_shard.state = ShardState::Active;
        
        topology.shards.insert(shard_id, first_shard);
        topology.shards.insert(new_shard_id, second_shard);
        
        topology.routing_table.insert(mid_point, new_shard_id);
        
        topology.version += 1;
        topology.updated_at = chrono::Utc::now();
        
        Ok((shard_id, new_shard_id))
    }

    fn calculate_mid_point(start: &[u8], end: &[u8]) -> Vec<u8> {
        let start_val = u64::from_be_bytes(start.try_into().unwrap_or([0; 8]));
        let end_val = u64::from_be_bytes(end.try_into().unwrap_or([255; 8]));
        let mid = start_val + (end_val - start_val) / 2;
        mid.to_be_bytes().to_vec()
    }

    pub async fn merge_shards(&self, shard1: ShardId, shard2: ShardId) -> Result<ShardId, Error> {
        let mut topology = self.topology.write();
        
        let s1 = topology.shards.get(&shard1)
            .ok_or_else(|| Error::NotFound("First shard not found".to_string()))?
            .clone();
        
        let s2 = topology.shards.get(&shard2)
            .ok_or_else(|| Error::NotFound("Second shard not found".to_string()))?
            .clone();
        
        let mut merged = s1.clone();
        merged.range_start = s1.range_start.min(s2.range_start);
        merged.range_end = s1.range_end.max(s2.range_end);
        merged.size_bytes = s1.size_bytes + s2.size_bytes;
        merged.record_count = s1.record_count + s2.record_count;
        merged.state = ShardState::Active;
        
        topology.shards.remove(&shard2);
        topology.shards.insert(shard1, merged);
        
        topology.routing_table.retain(|_, &mut v| v != shard2);
        
        topology.version += 1;
        topology.updated_at = chrono::Utc::now();
        
        Ok(shard1)
    }

    pub fn get_topology(&self) -> ShardTopology {
        self.topology.read().clone()
    }

    pub fn get_metrics(&self) -> ShardingMetricsSnapshot {
        ShardingMetricsSnapshot {
            total_operations: self.metrics.total_operations.load(Ordering::Relaxed),
            successful_operations: self.metrics.successful_operations.load(Ordering::Relaxed),
            failed_operations: self.metrics.failed_operations.load(Ordering::Relaxed),
            cross_shard_queries: self.metrics.cross_shard_queries.load(Ordering::Relaxed),
            migrations_completed: self.metrics.migrations_completed.load(Ordering::Relaxed),
            rebalances_triggered: self.metrics.rebalances_triggered.load(Ordering::Relaxed),
            avg_latency_ms: self.metrics.avg_latency_ms.load(Ordering::Relaxed),
            topology_versions: self.metrics.topology_versions.load(Ordering::Relaxed),
        }
    }

    fn update_avg_latency(&self, new_latency: u64) {
        let current = self.metrics.avg_latency_ms.load(Ordering::Relaxed);
        let count = self.metrics.total_operations.load(Ordering::Relaxed);
        
        let new_avg = if count == 1 {
            new_latency
        } else {
            ((current * (count - 1)) + new_latency) / count
        };
        
        self.metrics.avg_latency_ms.store(new_avg, Ordering::Relaxed);
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingMetricsSnapshot {
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub cross_shard_queries: u64,
    pub migrations_completed: u64,
    pub rebalances_triggered: u64,
    pub avg_latency_ms: u64,
    pub topology_versions: u64,
}

impl CircuitBreaker {
    fn new(failure_threshold: usize, success_threshold: usize, timeout: Duration) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            timeout,
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            last_failure_time: Arc::new(RwLock::new(None)),
        }
    }

    fn can_proceed(&self) -> Result<bool, Error> {
        let state = *self.state.read();
        
        match state {
            CircuitState::Closed => Ok(true),
            CircuitState::Open => {
                if let Some(last_failure) = *self.last_failure_time.read() {
                    if last_failure.elapsed() > self.timeout {
                        *self.state.write() = CircuitState::HalfOpen;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    Ok(false)
                }
            },
            CircuitState::HalfOpen => Ok(true),
        }
    }

    fn record_success(&self) {
        let state = *self.state.read();
        
        match state {
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count >= self.success_threshold as u64 {
                    *self.state.write() = CircuitState::Closed;
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                }
            },
            _ => {}
        }
    }

    fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        
        if count >= self.failure_threshold as u64 {
            *self.state.write() = CircuitState::Open;
            *self.last_failure_time.write() = Some(Instant::now());
        }
    }
}
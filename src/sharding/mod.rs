use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::fmt;
use crate::error::{Error, Result};
use crate::raft::{NodeId, RaftNode};
use parking_lot::RwLock;
use bincode::{Encode, Decode};
use std::time::{SystemTime, Duration};
use tokio::time::Instant;
use tokio::sync::mpsc;
use hex;
use async_trait;

pub mod strategies;
pub mod rebalancer;
pub mod router;
pub mod coordinator;
pub mod discovery;

/// Unique identifier for a shard
pub type ShardId = u64;

/// Partition key for routing data
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum PartitionKey {
    /// String-based key
    String(String),
    /// Integer-based key  
    Integer(i64),
    /// Binary key
    Binary(Vec<u8>),
    /// Composite key
    Composite(Vec<PartitionKey>),
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionKey::String(s) => write!(f, "str:{}", s),
            PartitionKey::Integer(i) => write!(f, "int:{}", i),
            PartitionKey::Binary(b) => write!(f, "bin:{}", hex::encode(b)),
            PartitionKey::Composite(keys) => {
                write!(f, "comp:[")?;
                for (i, key) in keys.iter().enumerate() {
                    if i > 0 { write!(f, ",")?; }
                    write!(f, "{}", key)?;
                }
                write!(f, "]")
            }
        }
    }
}

/// Sharding strategy type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum ShardingStrategy {
    /// Hash-based sharding with consistent hashing
    Hash,
    /// Range-based sharding for ordered data
    Range,
    /// Directory-based sharding with explicit mapping
    Directory,
    /// Hybrid strategy combining multiple approaches
    Hybrid,
}

/// Shard configuration and metadata
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ShardInfo {
    /// Unique shard identifier
    pub id: ShardId,
    
    /// Raft nodes responsible for this shard
    pub nodes: Vec<NodeId>,
    
    /// Current leader node for this shard
    pub leader: Option<NodeId>,
    
    /// Shard state
    pub state: ShardState,
    
    /// Key range for range-based sharding
    pub key_range: Option<KeyRange>,
    
    /// Hash ring position for hash-based sharding
    pub hash_range: Option<HashRange>,
    
    /// Creation timestamp
    pub created_at: u64,
    
    /// Last modified timestamp
    pub modified_at: u64,
    
    /// Shard statistics
    pub stats: ShardStats,
    
    /// Replication factor
    pub replication_factor: u32,
    
    /// Shard-specific configuration
    pub config: ShardConfig,
}

/// Shard operational state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum ShardState {
    /// Shard is initializing
    Initializing,
    /// Shard is active and serving requests
    Active,
    /// Shard is being rebalanced
    Rebalancing,
    /// Shard is being split
    Splitting,
    /// Shard is being merged
    Merging,
    /// Shard is temporarily unavailable
    Unavailable,
    /// Shard is being decommissioned
    Draining,
}

/// Key range for range-based sharding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct KeyRange {
    /// Start key (inclusive)
    pub start: PartitionKey,
    /// End key (exclusive)
    pub end: PartitionKey,
}

/// Hash range for hash-based sharding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct HashRange {
    /// Start hash value (inclusive)
    pub start: u64,
    /// End hash value (exclusive)  
    pub end: u64,
}

/// Shard statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct ShardStats {
    /// Number of keys in this shard
    pub key_count: u64,
    /// Total data size in bytes
    pub data_size: u64,
    /// Read operations per second
    pub read_ops: f64,
    /// Write operations per second
    pub write_ops: f64,
    /// Average latency in microseconds
    pub avg_latency: f64,
    /// CPU utilization percentage
    pub cpu_usage: f64,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// Network bandwidth usage
    pub network_usage: f64,
    /// Last update timestamp
    pub last_updated: u64,
}

/// Shard-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct ShardConfig {
    /// Maximum keys per shard before splitting
    pub max_keys: u64,
    /// Maximum data size per shard before splitting
    pub max_size: u64,
    /// Rebalancing threshold (load difference %)
    pub rebalance_threshold: f64,
    /// Enable automatic splitting
    pub auto_split: bool,
    /// Enable automatic merging
    pub auto_merge: bool,
    /// Minimum nodes for high availability
    pub min_nodes: u32,
    /// Preferred availability zone
    pub availability_zone: Option<String>,
    /// Custom shard tags
    pub tags: HashMap<String, String>,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            max_keys: 10_000_000,    // 10M keys
            max_size: 10 * 1024 * 1024 * 1024, // 10GB
            rebalance_threshold: 20.0, // 20% load difference
            auto_split: true,
            auto_merge: true,
            min_nodes: 3,
            availability_zone: None,
            tags: HashMap::new(),
        }
    }
}

/// Cluster topology and shard distribution
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// All shards in the cluster
    pub shards: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    
    /// Sharding strategy
    pub strategy: ShardingStrategy,
    
    /// Partition key to shard mapping
    pub routing_table: Arc<RwLock<RoutingTable>>,
    
    /// Node to shards mapping
    pub node_shards: Arc<RwLock<HashMap<NodeId, Vec<ShardId>>>>,
    
    /// Global configuration
    pub config: ClusterConfig,
    
    /// Topology version (for cache invalidation)
    pub version: Arc<RwLock<u64>>,
}

/// Routing table for partition key to shard mapping
#[derive(Debug, Clone)]
pub struct RoutingTable {
    /// Hash-based routing
    pub hash_ring: Option<ConsistentHashRing>,
    
    /// Range-based routing
    pub range_map: Option<BTreeMap<PartitionKey, ShardId>>,
    
    /// Directory-based routing
    pub directory: Option<HashMap<PartitionKey, ShardId>>,
    
    /// Default shard for unknown keys
    pub default_shard: Option<ShardId>,
    
    /// Routing table version
    pub version: u64,
}

/// Consistent hash ring for hash-based sharding
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// Virtual nodes on the ring
    pub virtual_nodes: BTreeMap<u64, ShardId>,
    
    /// Number of virtual nodes per shard
    pub virtual_nodes_per_shard: u32,
    
    /// Total capacity of the ring
    pub total_capacity: u64,
}

/// Global cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Default replication factor
    pub default_replication_factor: u32,
    
    /// Sharding strategy
    pub sharding_strategy: ShardingStrategy,
    
    /// Enable automatic rebalancing
    pub auto_rebalance: bool,
    
    /// Rebalancing interval in seconds
    pub rebalance_interval: u64,
    
    /// Maximum concurrent rebalance operations
    pub max_concurrent_rebalances: u32,
    
    /// Cross-shard transaction timeout
    pub cross_shard_timeout: Duration,
    
    /// Health check interval
    pub health_check_interval: Duration,
    
    /// Failure detection timeout
    pub failure_detection_timeout: Duration,
    
    /// Enable metrics collection
    pub enable_metrics: bool,
    
    /// Metrics collection interval
    pub metrics_interval: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            default_replication_factor: 3,
            sharding_strategy: ShardingStrategy::Hash,
            auto_rebalance: true,
            rebalance_interval: 300, // 5 minutes
            max_concurrent_rebalances: 2,
            cross_shard_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(10),
            failure_detection_timeout: Duration::from_secs(30),
            enable_metrics: true,
            metrics_interval: Duration::from_secs(60),
        }
    }
}

/// Shard operation types
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum ShardOperation {
    /// Create new shard
    Create {
        shard_id: ShardId,
        nodes: Vec<NodeId>,
        config: ShardConfig,
    },
    
    /// Split existing shard
    Split {
        source_shard: ShardId,
        new_shards: Vec<ShardId>,
        split_key: PartitionKey,
    },
    
    /// Merge multiple shards
    Merge {
        source_shards: Vec<ShardId>,
        target_shard: ShardId,
    },
    
    /// Move shard to different nodes
    Move {
        shard_id: ShardId,
        from_nodes: Vec<NodeId>,
        to_nodes: Vec<NodeId>,
    },
    
    /// Rebalance data across shards
    Rebalance {
        affected_shards: Vec<ShardId>,
        data_movement: Vec<DataMovement>,
    },
    
    /// Remove shard
    Remove {
        shard_id: ShardId,
        backup_location: Option<String>,
    },
}

/// Data movement operation
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DataMovement {
    /// Source shard
    pub from_shard: ShardId,
    /// Target shard  
    pub to_shard: ShardId,
    /// Key range to move
    pub key_range: KeyRange,
    /// Estimated data size
    pub estimated_size: u64,
    /// Priority (higher = more urgent)
    pub priority: u32,
}

/// Cross-shard transaction coordinator
#[derive(Debug)]
pub struct CrossShardTransaction {
    /// Transaction ID
    pub tx_id: String,
    /// Participating shards
    pub shards: Vec<ShardId>,
    /// Transaction state
    pub state: TransactionState,
    /// Operations per shard
    pub operations: HashMap<ShardId, Vec<TransactionOp>>,
    /// Timeout
    pub timeout: Instant,
    /// Client response channel (not cloneable, set to None when cloning)
    pub response_tx: Option<mpsc::UnboundedSender<Result<Vec<u8>>>>,
}

impl Clone for CrossShardTransaction {
    fn clone(&self) -> Self {
        Self {
            tx_id: self.tx_id.clone(),
            shards: self.shards.clone(),
            state: self.state,
            operations: self.operations.clone(),
            timeout: self.timeout,
            response_tx: None, // Don't clone the response channel
        }
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Preparing phase
    Preparing,
    /// Prepared phase (all shards ready)
    Prepared,
    /// Committing phase
    Committing,
    /// Committed successfully
    Committed,
    /// Aborting phase
    Aborting,
    /// Aborted
    Aborted,
    /// Failed
    Failed,
}

/// Individual transaction operation
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum TransactionOp {
    /// Read operation
    Read { key: Vec<u8> },
    /// Write operation
    Write { key: Vec<u8>, value: Vec<u8> },
    /// Delete operation
    Delete { key: Vec<u8> },
    /// Conditional write
    ConditionalWrite { 
        key: Vec<u8>, 
        value: Vec<u8>, 
        condition: WriteCondition 
    },
}

/// Write condition for conditional operations
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum WriteCondition {
    /// Key must not exist
    NotExists,
    /// Key must exist
    Exists,
    /// Key must have specific value
    ValueEquals(Vec<u8>),
    /// Key version must match
    VersionEquals(u64),
}

/// Shard manager - orchestrates all sharding operations
pub struct ShardManager {
    /// Cluster topology
    topology: Arc<ClusterTopology>,
    
    /// Active Raft nodes
    nodes: Arc<RwLock<HashMap<NodeId, Arc<RaftNode>>>>,
    
    /// Shard routing logic
    router: Arc<dyn ShardRouter>,
    
    /// Rebalancer for dynamic load balancing
    rebalancer: Arc<dyn ShardRebalancer>,
    
    /// Cross-shard transaction coordinator
    coordinator: Arc<dyn TransactionCoordinator>,
    
    /// Service discovery
    discovery: Arc<dyn ServiceDiscovery>,
    
    /// Background task handles
    tasks: Vec<tokio::task::JoinHandle<()>>,
    
    /// Metrics collector
    metrics: Arc<ShardingMetrics>,
}

/// Trait for shard routing strategies
pub trait ShardRouter: Send + Sync {
    /// Route a partition key to its shard
    fn route(&self, key: &PartitionKey) -> Result<ShardId>;
    
    /// Get all shards for a key range
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>>;
    
    /// Update routing table
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()>;
    
    /// Get routing statistics
    fn get_stats(&self) -> RoutingStats;
}

/// Trait for shard rebalancing
#[async_trait::async_trait]
pub trait ShardRebalancer: Send + Sync {
    /// Check if rebalancing is needed
    fn needs_rebalancing(&self, topology: &ClusterTopology) -> Result<bool>;
    
    /// Generate rebalancing plan
    fn generate_plan(&self, topology: &ClusterTopology) -> Result<RebalancePlan>;
    
    /// Execute rebalancing plan
    async fn execute_plan(&self, plan: RebalancePlan) -> Result<()>;
    
    /// Get rebalancing progress
    fn get_progress(&self) -> RebalanceProgress;
}

/// Trait for cross-shard transaction coordination
#[async_trait::async_trait]
pub trait TransactionCoordinator: Send + Sync {
    /// Start cross-shard transaction
    async fn begin_transaction(&self, operations: HashMap<ShardId, Vec<TransactionOp>>) -> Result<String>;
    
    /// Commit transaction
    async fn commit_transaction(&self, tx_id: &str) -> Result<()>;
    
    /// Abort transaction
    async fn abort_transaction(&self, tx_id: &str) -> Result<()>;
    
    /// Get transaction status
    fn get_transaction_status(&self, tx_id: &str) -> Option<TransactionState>;
}

/// Trait for service discovery
#[async_trait::async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// Register a node
    async fn register_node(&self, node_id: NodeId, address: String) -> Result<()>;
    
    /// Unregister a node
    async fn unregister_node(&self, node_id: NodeId) -> Result<()>;
    
    /// Discover nodes for a shard
    async fn discover_shard_nodes(&self, shard_id: ShardId) -> Result<Vec<NodeId>>;
    
    /// Health check for a node
    async fn health_check(&self, node_id: NodeId) -> Result<bool>;
}

/// Routing statistics
#[derive(Debug, Default, Clone)]
pub struct RoutingStats {
    /// Total routing requests
    pub total_requests: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Average routing latency
    pub avg_latency: f64,
    /// Failed routing attempts
    pub failed_routes: u64,
}

/// Rebalancing plan
#[derive(Debug)]
pub struct RebalancePlan {
    /// Data movements to execute
    pub movements: Vec<DataMovement>,
    /// Estimated completion time
    pub estimated_duration: Duration,
    /// Total data to move
    pub total_data_size: u64,
    /// Priority level
    pub priority: u32,
}

/// Rebalancing progress
#[derive(Debug, Clone)]
pub struct RebalanceProgress {
    /// Completed movements
    pub completed: u32,
    /// Total movements
    pub total: u32,
    /// Data transferred so far
    pub data_transferred: u64,
    /// Estimated remaining time
    pub eta: Duration,
    /// Current operation
    pub current_operation: Option<DataMovement>,
}

/// Sharding metrics for monitoring
#[derive(Debug, Default)]
pub struct ShardingMetrics {
    /// Requests per shard
    pub requests_per_shard: HashMap<ShardId, u64>,
    /// Cross-shard transactions
    pub cross_shard_txs: u64,
    /// Rebalancing operations
    pub rebalance_operations: u64,
    /// Data movement volume
    pub data_moved: u64,
    /// Node failures handled
    pub node_failures: u64,
    /// Average response time per shard
    pub avg_response_time: HashMap<ShardId, f64>,
}

impl ShardManager {
    /// Create new shard manager
    pub fn new(config: ClusterConfig) -> Self {
        let topology = Arc::new(ClusterTopology {
            shards: Arc::new(RwLock::new(HashMap::new())),
            strategy: config.sharding_strategy,
            routing_table: Arc::new(RwLock::new(RoutingTable {
                hash_ring: None,
                range_map: None,
                directory: None,
                default_shard: None,
                version: 0,
            })),
            node_shards: Arc::new(RwLock::new(HashMap::new())),
            config,
            version: Arc::new(RwLock::new(0)),
        });
        
        // Create strategy-specific router
        let router: Arc<dyn ShardRouter> = match topology.strategy {
            ShardingStrategy::Hash => Arc::new(strategies::HashBasedRouter::new()),
            ShardingStrategy::Range => Arc::new(strategies::RangeBasedRouter::new()),
            ShardingStrategy::Directory => Arc::new(strategies::DirectoryBasedRouter::new()),
            ShardingStrategy::Hybrid => Arc::new(strategies::HybridRouter::new()),
        };
        
        Self {
            topology,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            router,
            rebalancer: Arc::new(rebalancer::DefaultRebalancer::new()),
            coordinator: Arc::new(coordinator::TwoPhaseCommitCoordinator::new()),
            discovery: Arc::new(discovery::EtcdServiceDiscovery::new()),
            tasks: Vec::new(),
            metrics: Arc::new(ShardingMetrics::default()),
        }
    }
    
    /// Initialize cluster with initial shards
    pub async fn initialize_cluster(&mut self, initial_nodes: Vec<NodeId>) -> Result<()> {
        if initial_nodes.len() < 3 {
            return Err(Error::InvalidInput("Need at least 3 nodes for HA".to_string()));
        }
        
        let replication_factor = self.topology.config.default_replication_factor.min(initial_nodes.len() as u32);
        
        // Create initial shard
        let shard_info = ShardInfo {
            id: 1,
            nodes: initial_nodes[..replication_factor as usize].to_vec(),
            leader: Some(initial_nodes[0]),
            state: ShardState::Initializing,
            key_range: None,
            hash_range: Some(HashRange {
                start: 0,
                end: u64::MAX,
            }),
            created_at: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            modified_at: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            stats: ShardStats::default(),
            replication_factor,
            config: ShardConfig::default(),
        };
        
        // Add shard to topology
        self.topology.shards.write().insert(1, shard_info);
        
        // Update node mappings
        for node_id in &initial_nodes[..replication_factor as usize] {
            self.topology.node_shards.write()
                .entry(*node_id)
                .or_insert_with(Vec::new)
                .push(1);
        }
        
        // Initialize routing table
        self.router.update_routing(&self.topology)?;
        
        // Start background tasks
        self.start_background_tasks().await?;
        
        println!("Cluster initialized with {} nodes and shard 1", initial_nodes.len());
        Ok(())
    }
    
    /// Add new node to cluster
    pub async fn add_node(&mut self, node_id: NodeId, raft_node: Arc<RaftNode>) -> Result<()> {
        // Register node
        self.nodes.write().insert(node_id, raft_node);
        
        // Register with service discovery
        self.discovery.register_node(node_id, format!("node-{}", node_id)).await?;
        
        // Trigger rebalancing if needed
        if self.rebalancer.needs_rebalancing(&self.topology)? {
            let plan = self.rebalancer.generate_plan(&self.topology)?;
            tokio::spawn({
                let rebalancer = self.rebalancer.clone();
                async move {
                    if let Err(e) = rebalancer.execute_plan(plan).await {
                        eprintln!("Rebalancing failed: {}", e);
                    }
                }
            });
        }
        
        Ok(())
    }
    
    /// Remove node from cluster
    pub async fn remove_node(&mut self, node_id: NodeId) -> Result<()> {
        // Get shards affected by this node
        let affected_shards = self.topology.node_shards.read()
            .get(&node_id)
            .cloned()
            .unwrap_or_default();
        
        // For each affected shard, find replacement nodes
        for shard_id in affected_shards {
            let mut shards = self.topology.shards.write();
            if let Some(shard) = shards.get_mut(&shard_id) {
                // Remove failed node
                shard.nodes.retain(|&id| id != node_id);
                
                // Update leader if necessary
                if shard.leader == Some(node_id) {
                    shard.leader = shard.nodes.first().copied();
                }
                
                // Mark for rebalancing if below minimum nodes
                if shard.nodes.len() < shard.config.min_nodes as usize {
                    shard.state = ShardState::Rebalancing;
                }
            }
        }
        
        // Remove from node mappings
        self.topology.node_shards.write().remove(&node_id);
        self.nodes.write().remove(&node_id);
        
        // Unregister from service discovery
        self.discovery.unregister_node(node_id).await?;
        
        println!("Node {} removed from cluster", node_id);
        Ok(())
    }
    
    /// Route operation to appropriate shard
    pub async fn route_operation(&self, key: &PartitionKey, operation: TransactionOp) -> Result<Vec<u8>> {
        let shard_id = self.router.route(key)?;
        
        // Get shard info
        let shard_info = self.topology.shards.read()
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("Shard {} not found", shard_id)))?;
        
        // Get leader node
        let leader_id = shard_info.leader
            .ok_or_else(|| Error::NotFound("No leader for shard".to_string()))?;
        
        let leader_node = self.nodes.read()
            .get(&leader_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("Leader node {} not found", leader_id)))?;
        
        // Execute operation on leader
        match operation {
            TransactionOp::Read { key } => {
                leader_node.read_state_machine(&key)
                    .map(|opt| opt.unwrap_or_default())
            }
            TransactionOp::Write { key, value } => {
                let command = crate::raft::Command::Write { key, value };
                leader_node.execute_command_direct(command).await
            }
            TransactionOp::Delete { key } => {
                let command = crate::raft::Command::Delete { key };
                leader_node.execute_command_direct(command).await
            }
            TransactionOp::ConditionalWrite { key, value, .. } => {
                // TODO: Implement proper conditional write logic
                let command = crate::raft::Command::Write { key, value };
                leader_node.execute_command_direct(command).await
            }
        }
    }
    
    /// Execute cross-shard transaction
    pub async fn execute_cross_shard_transaction(&self, operations: HashMap<ShardId, Vec<TransactionOp>>) -> Result<()> {
        let tx_id = self.coordinator.begin_transaction(operations).await?;
        
        match self.coordinator.commit_transaction(&tx_id).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Attempt to abort on failure
                let _ = self.coordinator.abort_transaction(&tx_id).await;
                Err(e)
            }
        }
    }
    
    /// Get cluster statistics
    pub fn get_cluster_stats(&self) -> ClusterStats {
        let shards = self.topology.shards.read();
        
        let total_shards = shards.len();
        let active_shards = shards.values()
            .filter(|s| s.state == ShardState::Active)
            .count();
        
        let total_keys: u64 = shards.values()
            .map(|s| s.stats.key_count)
            .sum();
        
        let total_data_size: u64 = shards.values()
            .map(|s| s.stats.data_size)
            .sum();
        
        ClusterStats {
            total_shards,
            active_shards,
            total_keys,
            total_data_size,
            rebalancing_shards: shards.values()
                .filter(|s| s.state == ShardState::Rebalancing)
                .count(),
            cross_shard_transactions: self.metrics.cross_shard_txs,
        }
    }
    
    /// Start background maintenance tasks
    async fn start_background_tasks(&mut self) -> Result<()> {
        // Health checking task
        let topology = self.topology.clone();
        let discovery = self.discovery.clone();
        let health_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(topology.config.health_check_interval);
            loop {
                interval.tick().await;
                
                // Check health of all nodes
                let nodes: Vec<NodeId> = topology.node_shards.read().keys().copied().collect();
                for node_id in nodes {
                    if let Err(_) = discovery.health_check(node_id).await {
                        eprintln!("Node {} failed health check", node_id);
                        // TODO: Trigger failover
                    }
                }
            }
        });
        
        // Metrics collection task
        let topology = self.topology.clone();
        let metrics = self.metrics.clone();
        let metrics_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(topology.config.metrics_interval);
            loop {
                interval.tick().await;
                // TODO: Collect and aggregate metrics
            }
        });
        
        self.tasks.push(health_task);
        self.tasks.push(metrics_task);
        
        Ok(())
    }
}

/// Cluster statistics
#[derive(Debug)]
pub struct ClusterStats {
    pub total_shards: usize,
    pub active_shards: usize,
    pub total_keys: u64,
    pub total_data_size: u64,
    pub rebalancing_shards: usize,
    pub cross_shard_transactions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_partition_key_display() {
        let key = PartitionKey::String("test".to_string());
        assert_eq!(key.to_string(), "str:test");
        
        let key = PartitionKey::Integer(42);
        assert_eq!(key.to_string(), "int:42");
        
        let key = PartitionKey::Composite(vec![
            PartitionKey::String("user".to_string()),
            PartitionKey::Integer(123),
        ]);
        assert_eq!(key.to_string(), "comp:[str:user,int:123]");
    }
    
    #[test]
    fn test_shard_config_defaults() {
        let config = ShardConfig::default();
        assert_eq!(config.max_keys, 10_000_000);
        assert_eq!(config.rebalance_threshold, 20.0);
        assert!(config.auto_split);
        assert_eq!(config.min_nodes, 3);
    }
}
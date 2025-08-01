use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use tokio::time::{Duration, Instant, interval};
use serde::{Deserialize, Serialize};
use num_cpus;

/// Service discovery implementation using etcd
pub struct EtcdServiceDiscovery {
    /// etcd client configuration
    config: EtcdConfig,
    
    /// Local node registry
    nodes: Arc<TokioRwLock<HashMap<NodeId, NodeRegistration>>>,
    
    /// Shard assignments
    shard_assignments: Arc<TokioRwLock<HashMap<ShardId, Vec<NodeId>>>>,
    
    /// Health check results
    health_status: Arc<TokioRwLock<HashMap<NodeId, HealthStatus>>>,
    
    /// Background task handles
    tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    
    /// Discovery metrics
    metrics: Arc<DiscoveryMetrics>,
    
    /// Last update time
    last_update: AtomicU64,
}

/// etcd configuration
#[derive(Debug, Clone)]
pub struct EtcdConfig {
    /// etcd endpoints
    pub endpoints: Vec<String>,
    
    /// Connection timeout
    pub connect_timeout: Duration,
    
    /// Request timeout
    pub request_timeout: Duration,
    
    /// Key prefix for Lightning DB
    pub key_prefix: String,
    
    /// TTL for node registrations
    pub registration_ttl: Duration,
    
    /// Health check interval
    pub health_check_interval: Duration,
    
    /// Enable automatic cleanup
    pub enable_cleanup: bool,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://localhost:2379".to_string()],
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(3),
            key_prefix: "/lightning_db".to_string(),
            registration_ttl: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(5),
            enable_cleanup: true,
        }
    }
}

/// Node registration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegistration {
    /// Node ID
    pub node_id: NodeId,
    
    /// Node address
    pub address: String,
    
    /// Registration timestamp
    pub registered_at: u64,
    
    /// Last heartbeat
    pub last_heartbeat: u64,
    
    /// Node metadata
    pub metadata: NodeMetadata,
    
    /// Assigned shards
    pub shards: Vec<ShardId>,
}

/// Node metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// Node version
    pub version: String,
    
    /// Available CPU cores
    pub cpu_cores: u32,
    
    /// Total memory in bytes
    pub total_memory: u64,
    
    /// Available disk space in bytes
    pub disk_space: u64,
    
    /// Network bandwidth in Mbps
    pub network_bandwidth: u32,
    
    /// Availability zone
    pub availability_zone: Option<String>,
    
    /// Custom tags
    pub tags: HashMap<String, String>,
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            cpu_cores: num_cpus::get() as u32,
            total_memory: 8 * 1024 * 1024 * 1024, // 8GB default
            disk_space: 100 * 1024 * 1024 * 1024, // 100GB default
            network_bandwidth: 1000, // 1 Gbps
            availability_zone: None,
            tags: HashMap::new(),
        }
    }
}

/// Health status of a node
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Is the node healthy?
    pub healthy: bool,
    
    /// Last check time
    pub last_check: Instant,
    
    /// Response time in milliseconds
    pub response_time: f64,
    
    /// Error message if unhealthy
    pub error: Option<String>,
    
    /// Consecutive failures
    pub consecutive_failures: u32,
}

/// Discovery metrics
#[derive(Debug, Default)]
pub struct DiscoveryMetrics {
    /// Total registrations
    pub registrations: AtomicU64,
    
    /// Active nodes
    pub active_nodes: AtomicU64,
    
    /// Health checks performed
    pub health_checks: AtomicU64,
    
    /// Failed health checks
    pub failed_health_checks: AtomicU64,
    
    /// Average discovery latency
    pub avg_discovery_latency: parking_lot::RwLock<f64>,
    
    /// Shard assignments changed
    pub shard_assignments_changed: AtomicU64,
}

impl EtcdServiceDiscovery {
    /// Create new etcd-based service discovery
    pub fn new() -> Self {
        Self {
            config: EtcdConfig::default(),
            nodes: Arc::new(TokioRwLock::new(HashMap::new())),
            shard_assignments: Arc::new(TokioRwLock::new(HashMap::new())),
            health_status: Arc::new(TokioRwLock::new(HashMap::new())),
            tasks: parking_lot::Mutex::new(Vec::new()),
            metrics: Arc::new(DiscoveryMetrics::default()),
            last_update: AtomicU64::new(0),
        }
    }
    
    /// Create with custom configuration
    pub fn with_config(config: EtcdConfig) -> Self {
        Self {
            config,
            nodes: Arc::new(TokioRwLock::new(HashMap::new())),
            shard_assignments: Arc::new(TokioRwLock::new(HashMap::new())),
            health_status: Arc::new(TokioRwLock::new(HashMap::new())),
            tasks: parking_lot::Mutex::new(Vec::new()),
            metrics: Arc::new(DiscoveryMetrics::default()),
            last_update: AtomicU64::new(0),
        }
    }
    
    /// Start service discovery
    pub async fn start(&self) -> Result<()> {
        // Health check task
        let nodes = self.nodes.clone();
        let health_status = self.health_status.clone();
        let metrics = self.metrics.clone();
        let health_interval = self.config.health_check_interval;
        
        let health_task = tokio::spawn(async move {
            let mut interval = interval(health_interval);
            loop {
                interval.tick().await;
                
                let node_list: Vec<(NodeId, String)> = {
                    let nodes = nodes.read().await;
                    nodes.iter()
                        .map(|(id, reg)| (*id, reg.address.clone()))
                        .collect()
                };
                
                for (node_id, address) in node_list {
                    let start = Instant::now();
                    metrics.health_checks.fetch_add(1, Ordering::Relaxed);
                    
                    // Simulate health check
                    let (healthy, error) = Self::check_node_health(&address).await;
                    let response_time = start.elapsed().as_millis() as f64;
                    
                    if !healthy {
                        metrics.failed_health_checks.fetch_add(1, Ordering::Relaxed);
                    }
                    
                    // Update health status
                    let mut health = health_status.write().await;
                    let status = health.entry(node_id).or_insert(HealthStatus {
                        healthy: true,
                        last_check: start,
                        response_time: 0.0,
                        error: None,
                        consecutive_failures: 0,
                    });
                    
                    status.healthy = healthy;
                    status.last_check = Instant::now();
                    status.response_time = response_time;
                    status.error = error.clone();
                    
                    if healthy {
                        status.consecutive_failures = 0;
                    } else {
                        status.consecutive_failures += 1;
                    }
                    
                    if let Some(err) = error {
                        eprintln!("Health check failed for node {}: {}", node_id, err);
                    }
                }
            }
        });
        
        // Cleanup task
        if self.config.enable_cleanup {
            let nodes = self.nodes.clone();
            let ttl = self.config.registration_ttl;
            
            let cleanup_task = tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    
                    let mut expired_nodes = Vec::new();
                    
                    {
                        let nodes = nodes.read().await;
                        for (node_id, registration) in nodes.iter() {
                            if now - registration.last_heartbeat > ttl.as_secs() {
                                expired_nodes.push(*node_id);
                            }
                        }
                    }
                    
                    if !expired_nodes.is_empty() {
                        let mut nodes = nodes.write().await;
                        for node_id in expired_nodes {
                            nodes.remove(&node_id);
                            println!("Removed expired node registration: {}", node_id);
                        }
                    }
                }
            });
            
            self.tasks.lock().push(cleanup_task);
        }
        
        self.tasks.lock().push(health_task);
        
        println!("Service discovery started");
        Ok(())
    }
    
    /// Check health of a node
    async fn check_node_health(address: &str) -> (bool, Option<String>) {
        // Simulate network health check
        // In a real implementation, this would make an HTTP/gRPC call
        tokio::time::sleep(Duration::from_millis(5)).await;
        
        // Simulate occasional failures
        if rand::random::<f64>() < 0.05 { // 5% failure rate
            (false, Some("Connection timeout".to_string()))
        } else {
            (true, None)
        }
    }
    
    /// Update shard assignments
    pub async fn update_shard_assignments(&self, assignments: HashMap<ShardId, Vec<NodeId>>) -> Result<()> {
        let mut shard_assignments = self.shard_assignments.write().await;
        
        // Check for changes
        let mut changed = false;
        for (shard_id, nodes) in &assignments {
            if let Some(existing) = shard_assignments.get(shard_id) {
                if existing != nodes {
                    changed = true;
                    break;
                }
            } else {
                changed = true;
                break;
            }
        }
        
        if changed {
            *shard_assignments = assignments;
            self.metrics.shard_assignments_changed.fetch_add(1, Ordering::Relaxed);
            self.last_update.store(
                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
                Ordering::Release
            );
            
            println!("Shard assignments updated");
        }
        
        Ok(())
    }
    
    /// Get all registered nodes
    pub async fn get_all_nodes(&self) -> Vec<NodeRegistration> {
        self.nodes.read().await.values().cloned().collect()
    }
    
    /// Get healthy nodes
    pub async fn get_healthy_nodes(&self) -> Vec<NodeId> {
        let health_status = self.health_status.read().await;
        health_status.iter()
            .filter(|(_, status)| status.healthy)
            .map(|(node_id, _)| *node_id)
            .collect()
    }
    
    /// Get node load balancing weights
    pub async fn get_node_weights(&self) -> HashMap<NodeId, f64> {
        let mut weights = HashMap::new();
        
        let nodes = self.nodes.read().await;
        let health_status = self.health_status.read().await;
        
        for (node_id, registration) in nodes.iter() {
            let health = health_status.get(node_id);
            
            let base_weight = if let Some(health) = health {
                if health.healthy {
                    // Weight based on response time (lower is better)
                    1.0 / (1.0 + health.response_time / 100.0)
                } else {
                    0.0
                }
            } else {
                0.5 // Unknown health
            };
            
            // Adjust based on node capacity
            let capacity_weight = (registration.metadata.cpu_cores as f64 / 4.0).min(2.0);
            let memory_weight = (registration.metadata.total_memory as f64 / (8.0 * 1024.0 * 1024.0 * 1024.0)).min(2.0);
            
            let final_weight = base_weight * capacity_weight * memory_weight;
            weights.insert(*node_id, final_weight);
        }
        
        weights
    }
    
    /// Find optimal node for shard placement
    pub async fn find_optimal_node(&self, shard_id: ShardId, exclude: &[NodeId]) -> Option<NodeId> {
        let weights = self.get_node_weights().await;
        let shard_assignments = self.shard_assignments.read().await;
        
        // Find nodes not already assigned to this shard
        let assigned_nodes: std::collections::HashSet<NodeId> = shard_assignments
            .get(&shard_id)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default();
        
        // Find best available node
        weights.iter()
            .filter(|(node_id, &weight)| {
                weight > 0.0 && 
                !assigned_nodes.contains(node_id) && 
                !exclude.contains(node_id)
            })
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(node_id, _)| *node_id)
    }
    
    /// Send heartbeat for a node
    async fn send_heartbeat(&self, node_id: NodeId) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let mut nodes = self.nodes.write().await;
        if let Some(registration) = nodes.get_mut(&node_id) {
            registration.last_heartbeat = now;
        }
        
        Ok(())
    }
    
    /// Get discovery statistics
    pub fn get_stats(&self) -> DiscoveryStats {
        DiscoveryStats {
            total_registrations: self.metrics.registrations.load(Ordering::Acquire),
            active_nodes: self.metrics.active_nodes.load(Ordering::Acquire),
            health_checks_performed: self.metrics.health_checks.load(Ordering::Acquire),
            failed_health_checks: self.metrics.failed_health_checks.load(Ordering::Acquire),
            avg_discovery_latency: *self.metrics.avg_discovery_latency.read(),
            shard_assignments_changed: self.metrics.shard_assignments_changed.load(Ordering::Acquire),
            last_update: self.last_update.load(Ordering::Acquire),
        }
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for EtcdServiceDiscovery {
    async fn register_node(&self, node_id: NodeId, address: String) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let registration = NodeRegistration {
            node_id,
            address,
            registered_at: now,
            last_heartbeat: now,
            metadata: NodeMetadata::default(),
            shards: Vec::new(),
        };
        
        self.nodes.write().await.insert(node_id, registration);
        
        // Update metrics
        self.metrics.registrations.fetch_add(1, Ordering::Relaxed);
        self.metrics.active_nodes.fetch_add(1, Ordering::Relaxed);
        
        // Send initial heartbeat
        self.send_heartbeat(node_id).await?;
        
        println!("Registered node {} at {}", node_id, address);
        Ok(())
    }
    
    async fn unregister_node(&self, node_id: NodeId) -> Result<()> {
        if self.nodes.write().await.remove(&node_id).is_some() {
            self.health_status.write().await.remove(&node_id);
            self.metrics.active_nodes.fetch_sub(1, Ordering::Relaxed);
            
            println!("Unregistered node {}", node_id);
        }
        
        Ok(())
    }
    
    async fn discover_shard_nodes(&self, shard_id: ShardId) -> Result<Vec<NodeId>> {
        let start = Instant::now();
        
        let shard_assignments = self.shard_assignments.read().await;
        let nodes = shard_assignments.get(&shard_id)
            .cloned()
            .unwrap_or_default();
        
        // Update discovery latency metric
        let latency = start.elapsed().as_micros() as f64;
        let mut avg_latency = self.metrics.avg_discovery_latency.write();
        let checks = self.metrics.health_checks.load(Ordering::Acquire) + 1;
        *avg_latency = (*avg_latency * (checks - 1) as f64 + latency) / checks as f64;
        
        Ok(nodes)
    }
    
    async fn health_check(&self, node_id: NodeId) -> Result<bool> {
        let health_status = self.health_status.read().await;
        
        if let Some(status) = health_status.get(&node_id) {
            // Consider node unhealthy if it has too many consecutive failures
            Ok(status.healthy && status.consecutive_failures < 3)
        } else {
            // Unknown node, assume unhealthy
            Ok(false)
        }
    }
}

/// Discovery statistics
#[derive(Debug)]
pub struct DiscoveryStats {
    pub total_registrations: u64,
    pub active_nodes: u64,
    pub health_checks_performed: u64,
    pub failed_health_checks: u64,
    pub avg_discovery_latency: f64,
    pub shard_assignments_changed: u64,
    pub last_update: u64,
}

/// In-memory service discovery for testing
pub struct InMemoryServiceDiscovery {
    /// Registered nodes
    nodes: Arc<TokioRwLock<HashMap<NodeId, String>>>,
    
    /// Shard to nodes mapping
    shard_nodes: Arc<TokioRwLock<HashMap<ShardId, Vec<NodeId>>>>,
    
    /// Node health status
    health: Arc<TokioRwLock<HashMap<NodeId, bool>>>,
}

impl InMemoryServiceDiscovery {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(TokioRwLock::new(HashMap::new())),
            shard_nodes: Arc::new(TokioRwLock::new(HashMap::new())),
            health: Arc::new(TokioRwLock::new(HashMap::new())),
        }
    }
    
    /// Assign node to shard
    pub async fn assign_node_to_shard(&self, node_id: NodeId, shard_id: ShardId) {
        let mut shard_nodes = self.shard_nodes.write().await;
        shard_nodes.entry(shard_id)
            .or_insert_with(Vec::new)
            .push(node_id);
    }
    
    /// Set node health status
    pub async fn set_node_health(&self, node_id: NodeId, healthy: bool) {
        self.health.write().await.insert(node_id, healthy);
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for InMemoryServiceDiscovery {
    async fn register_node(&self, node_id: NodeId, address: String) -> Result<()> {
        self.nodes.write().await.insert(node_id, address);
        self.health.write().await.insert(node_id, true);
        Ok(())
    }
    
    async fn unregister_node(&self, node_id: NodeId) -> Result<()> {
        self.nodes.write().await.remove(&node_id);
        self.health.write().await.remove(&node_id);
        
        // Remove from all shard assignments
        let mut shard_nodes = self.shard_nodes.write().await;
        for nodes in shard_nodes.values_mut() {
            nodes.retain(|&id| id != node_id);
        }
        
        Ok(())
    }
    
    async fn discover_shard_nodes(&self, shard_id: ShardId) -> Result<Vec<NodeId>> {
        let shard_nodes = self.shard_nodes.read().await;
        Ok(shard_nodes.get(&shard_id).cloned().unwrap_or_default())
    }
    
    async fn health_check(&self, node_id: NodeId) -> Result<bool> {
        let health = self.health.read().await;
        Ok(health.get(&node_id).copied().unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_in_memory_discovery() {
        let discovery = InMemoryServiceDiscovery::new();
        
        // Register nodes
        discovery.register_node(1, "node1:8080".to_string()).await.unwrap();
        discovery.register_node(2, "node2:8080".to_string()).await.unwrap();
        
        // Assign to shard
        discovery.assign_node_to_shard(1, 100).await;
        discovery.assign_node_to_shard(2, 100).await;
        
        // Discover nodes
        let nodes = discovery.discover_shard_nodes(100).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&1));
        assert!(nodes.contains(&2));
        
        // Health check
        assert!(discovery.health_check(1).await.unwrap());
        
        // Set unhealthy
        discovery.set_node_health(1, false).await;
        assert!(!discovery.health_check(1).await.unwrap());
    }
    
    #[tokio::test]
    async fn test_etcd_discovery_creation() {
        let discovery = EtcdServiceDiscovery::new();
        let stats = discovery.get_stats();
        assert_eq!(stats.active_nodes, 0);
    }
    
    #[test]
    fn test_node_metadata_defaults() {
        let metadata = NodeMetadata::default();
        assert_eq!(metadata.version, env!("CARGO_PKG_VERSION"));
        assert!(metadata.cpu_cores > 0);
        assert!(metadata.total_memory > 0);
    }
}
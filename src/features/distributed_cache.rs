use crate::core::error::{Error, Result};
use parking_lot::{RwLock, Mutex};
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::net::{SocketAddr, IpAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{interval, timeout};
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, error};
use bytes::{Bytes, BytesMut, BufMut};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId {
    pub id: String,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
    pub ttl: Option<Duration>,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    None,
    Primary,
    PrimaryBackup { replicas: usize },
    Chain { replicas: usize },
    Quorum { replicas: usize, read_quorum: usize, write_quorum: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedCacheConfig {
    pub node_id: NodeId,
    pub capacity: usize,
    pub replication: ReplicationStrategy,
    pub hash_slots: usize,
    pub sync_interval: Duration,
    pub heartbeat_interval: Duration,
    pub failure_detection_timeout: Duration,
    pub enable_compression: bool,
    pub enable_encryption: bool,
}

impl Default for DistributedCacheConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId {
                id: uuid::Uuid::new_v4().to_string(),
                addr: "127.0.0.1:7000".parse().unwrap(),
            },
            capacity: 1_000_000,
            replication: ReplicationStrategy::PrimaryBackup { replicas: 2 },
            hash_slots: 16384,
            sync_interval: Duration::from_secs(1),
            heartbeat_interval: Duration::from_secs(5),
            failure_detection_timeout: Duration::from_secs(30),
            enable_compression: true,
            enable_encryption: false,
        }
    }
}

pub struct ConsistentHash {
    ring: BTreeMap<u64, NodeId>,
    virtual_nodes: usize,
    nodes: HashSet<NodeId>,
}

impl ConsistentHash {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
            nodes: HashSet::new(),
        }
    }
    
    pub fn add_node(&mut self, node: NodeId) {
        if self.nodes.insert(node.clone()) {
            for i in 0..self.virtual_nodes {
                let virtual_key = format!("{}#{}", node.id, i);
                let hash = self.hash_key(virtual_key.as_bytes());
                self.ring.insert(hash, node.clone());
            }
        }
    }
    
    pub fn remove_node(&mut self, node: &NodeId) {
        if self.nodes.remove(node) {
            let keys_to_remove: Vec<_> = self.ring
                .iter()
                .filter(|(_, n)| *n == node)
                .map(|(k, _)| *k)
                .collect();
            
            for key in keys_to_remove {
                self.ring.remove(&key);
            }
        }
    }
    
    pub fn get_node(&self, key: &[u8]) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }
        
        let hash = self.hash_key(key);
        
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| node.clone())
    }
    
    pub fn get_nodes(&self, key: &[u8], count: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return Vec::new();
        }
        
        let hash = self.hash_key(key);
        let mut nodes = Vec::new();
        let mut seen = HashSet::new();
        
        for (_, node) in self.ring.range(hash..).chain(self.ring.iter()) {
            if seen.insert(node.clone()) {
                nodes.push(node.clone());
                if nodes.len() >= count {
                    break;
                }
            }
        }
        
        nodes
    }
    
    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheMessage {
    Get { key: Vec<u8>, request_id: u64 },
    GetResponse { value: Option<Vec<u8>>, version: u64, request_id: u64 },
    Put { key: Vec<u8>, value: Vec<u8>, version: u64, ttl: Option<Duration> },
    PutResponse { success: bool, version: u64 },
    Delete { key: Vec<u8>, version: u64 },
    DeleteResponse { success: bool },
    Invalidate { key: Vec<u8>, version: u64 },
    Replicate { entries: Vec<CacheEntry> },
    Heartbeat { node_id: NodeId, load: f64, capacity: usize },
    Join { node_id: NodeId },
    Leave { node_id: NodeId },
    Gossip { nodes: Vec<NodeInfo> },
    Sync { keys: Vec<Vec<u8>> },
    SyncResponse { entries: Vec<CacheEntry> },
    Rebalance { migrations: Vec<Migration> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub last_seen: u64,
    pub load: f64,
    pub capacity: usize,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    Healthy,
    Suspected,
    Failed,
    Leaving,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub keys: Vec<Vec<u8>>,
    pub from_node: NodeId,
    pub to_node: NodeId,
}

pub struct DistributedCache {
    config: DistributedCacheConfig,
    local_cache: Arc<RwLock<HashMap<Vec<u8>, CacheEntry>>>,
    consistent_hash: Arc<RwLock<ConsistentHash>>,
    cluster_state: Arc<RwLock<ClusterState>>,
    message_sender: mpsc::Sender<CacheMessage>,
    message_receiver: Arc<Mutex<mpsc::Receiver<CacheMessage>>>,
    statistics: Arc<CacheStatistics>,
    shutdown: Arc<AtomicBool>,
}

struct ClusterState {
    nodes: HashMap<NodeId, NodeInfo>,
    pending_requests: HashMap<u64, oneshot::Sender<CacheMessage>>,
    next_request_id: u64,
    rebalancing: bool,
}

#[derive(Debug, Default)]
struct CacheStatistics {
    hits: AtomicU64,
    misses: AtomicU64,
    puts: AtomicU64,
    deletes: AtomicU64,
    evictions: AtomicU64,
    replication_lag: AtomicU64,
    network_bytes_sent: AtomicU64,
    network_bytes_received: AtomicU64,
}

impl DistributedCache {
    pub async fn new(config: DistributedCacheConfig) -> Result<Arc<Self>> {
        let (tx, rx) = mpsc::channel(10000);
        
        let mut consistent_hash = ConsistentHash::new(150);
        consistent_hash.add_node(config.node_id.clone());
        
        let cache = Arc::new(Self {
            config: config.clone(),
            local_cache: Arc::new(RwLock::new(HashMap::new())),
            consistent_hash: Arc::new(RwLock::new(consistent_hash)),
            cluster_state: Arc::new(RwLock::new(ClusterState {
                nodes: HashMap::new(),
                pending_requests: HashMap::new(),
                next_request_id: 1,
                rebalancing: false,
            })),
            message_sender: tx,
            message_receiver: Arc::new(Mutex::new(rx)),
            statistics: Arc::new(CacheStatistics::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        });
        
        cache.start_background_tasks().await?;
        
        Ok(cache)
    }
    
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let nodes = self.get_responsible_nodes(key);
        
        if nodes.is_empty() {
            return Ok(None);
        }
        
        if nodes[0] == self.config.node_id {
            let cache = self.local_cache.read();
            if let Some(entry) = cache.get(key) {
                if self.is_entry_valid(entry) {
                    self.statistics.hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(entry.value.clone()));
                }
            }
            self.statistics.misses.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        } else {
            self.remote_get(&nodes[0], key).await
        }
    }
    
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) -> Result<()> {
        let version = self.generate_version();
        let nodes = self.get_responsible_nodes(&key);
        
        if nodes.is_empty() {
            return Err(Error::Generic("No nodes available".to_string()));
        }
        
        let entry = CacheEntry {
            key: key.clone(),
            value: value.clone(),
            version,
            ttl,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
        };
        
        if nodes[0] == self.config.node_id {
            self.local_put(entry.clone())?;
            
            if let ReplicationStrategy::PrimaryBackup { replicas } = self.config.replication {
                for i in 1..=replicas.min(nodes.len() - 1) {
                    self.replicate_to(&nodes[i], vec![entry.clone()]).await?;
                }
            }
        } else {
            self.remote_put(&nodes[0], key, value, version, ttl).await?;
        }
        
        self.statistics.puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    pub async fn delete(&self, key: &[u8]) -> Result<bool> {
        let version = self.generate_version();
        let nodes = self.get_responsible_nodes(key);
        
        if nodes.is_empty() {
            return Ok(false);
        }
        
        let deleted = if nodes[0] == self.config.node_id {
            let mut cache = self.local_cache.write();
            let deleted = cache.remove(key).is_some();
            
            if deleted {
                self.invalidate_replicas(key, version, &nodes[1..]).await?;
            }
            
            deleted
        } else {
            self.remote_delete(&nodes[0], key, version).await?
        };
        
        if deleted {
            self.statistics.deletes.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(deleted)
    }
    
    pub async fn invalidate(&self, key: &[u8]) -> Result<()> {
        let version = self.generate_version();
        let nodes = self.get_responsible_nodes(key);
        
        for node in nodes {
            if node == self.config.node_id {
                let mut cache = self.local_cache.write();
                cache.remove(key);
            } else {
                let msg = CacheMessage::Invalidate {
                    key: key.to_vec(),
                    version,
                };
                self.send_to_node(&node, msg).await?;
            }
        }
        
        Ok(())
    }
    
    fn local_put(&self, entry: CacheEntry) -> Result<()> {
        let mut cache = self.local_cache.write();
        
        if cache.len() >= self.config.capacity {
            self.evict_lru(&mut cache);
        }
        
        cache.insert(entry.key.clone(), entry);
        Ok(())
    }
    
    fn evict_lru(&self, cache: &mut HashMap<Vec<u8>, CacheEntry>) {
        if let Some((key, _)) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            cache.remove(&key);
            self.statistics.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    fn is_entry_valid(&self, entry: &CacheEntry) -> bool {
        if let Some(ttl) = entry.ttl {
            entry.created_at.elapsed() < ttl
        } else {
            true
        }
    }
    
    fn get_responsible_nodes(&self, key: &[u8]) -> Vec<NodeId> {
        let hash_ring = self.consistent_hash.read();
        
        let replica_count = match self.config.replication {
            ReplicationStrategy::None => 1,
            ReplicationStrategy::Primary => 1,
            ReplicationStrategy::PrimaryBackup { replicas } => replicas + 1,
            ReplicationStrategy::Chain { replicas } => replicas + 1,
            ReplicationStrategy::Quorum { replicas, .. } => replicas,
        };
        
        hash_ring.get_nodes(key, replica_count)
    }
    
    async fn remote_get(&self, node: &NodeId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let request_id = self.next_request_id();
        let (tx, rx) = oneshot::channel();
        
        {
            let mut state = self.cluster_state.write();
            state.pending_requests.insert(request_id, tx);
        }
        
        let msg = CacheMessage::Get {
            key: key.to_vec(),
            request_id,
        };
        
        self.send_to_node(node, msg).await?;
        
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(CacheMessage::GetResponse { value, .. })) => Ok(value),
            _ => Ok(None),
        }
    }
    
    async fn remote_put(
        &self,
        node: &NodeId,
        key: Vec<u8>,
        value: Vec<u8>,
        version: u64,
        ttl: Option<Duration>,
    ) -> Result<()> {
        let msg = CacheMessage::Put { key, value, version, ttl };
        self.send_to_node(node, msg).await
    }
    
    async fn remote_delete(&self, node: &NodeId, key: &[u8], version: u64) -> Result<bool> {
        let msg = CacheMessage::Delete {
            key: key.to_vec(),
            version,
        };
        
        self.send_to_node(node, msg).await?;
        Ok(true)
    }
    
    async fn replicate_to(&self, node: &NodeId, entries: Vec<CacheEntry>) -> Result<()> {
        let msg = CacheMessage::Replicate { entries };
        self.send_to_node(node, msg).await
    }
    
    async fn invalidate_replicas(
        &self,
        key: &[u8],
        version: u64,
        nodes: &[NodeId],
    ) -> Result<()> {
        for node in nodes {
            let msg = CacheMessage::Invalidate {
                key: key.to_vec(),
                version,
            };
            self.send_to_node(node, msg).await?;
        }
        Ok(())
    }
    
    async fn send_to_node(&self, node: &NodeId, msg: CacheMessage) -> Result<()> {
        let serialized = bincode::encode_to_vec(&msg)
            .map_err(|e| Error::Generic(format!("Serialization failed: {}", e)))?;
        
        let mut stream = TcpStream::connect(&node.addr).await
            .map_err(|e| Error::Generic(format!("Connection failed: {}", e)))?;
        
        let len = serialized.len() as u32;
        stream.write_all(&len.to_be_bytes()).await
            .map_err(|e| Error::Generic(format!("Write failed: {}", e)))?;
        stream.write_all(&serialized).await
            .map_err(|e| Error::Generic(format!("Write failed: {}", e)))?;
        
        self.statistics.network_bytes_sent
            .fetch_add(serialized.len() as u64 + 4, Ordering::Relaxed);
        
        Ok(())
    }
    
    async fn start_background_tasks(self: &Arc<Self>) -> Result<()> {
        let cache = self.clone();
        tokio::spawn(async move {
            cache.run_server().await;
        });
        
        let cache = self.clone();
        tokio::spawn(async move {
            cache.run_heartbeat().await;
        });
        
        let cache = self.clone();
        tokio::spawn(async move {
            cache.run_sync().await;
        });
        
        let cache = self.clone();
        tokio::spawn(async move {
            cache.run_cleanup().await;
        });
        
        Ok(())
    }
    
    async fn run_server(&self) {
        let listener = match TcpListener::bind(&self.config.node_id.addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind listener: {}", e);
                return;
            }
        };
        
        info!("Cache server listening on {}", self.config.node_id.addr);
        
        while !self.shutdown.load(Ordering::Relaxed) {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let cache = self.clone();
                    tokio::spawn(async move {
                        cache.handle_connection(stream).await;
                    });
                }
                Err(e) => {
                    warn!("Failed to accept connection: {}", e);
                }
            }
        }
    }
    
    async fn handle_connection(&self, mut stream: TcpStream) {
        let mut len_buf = [0u8; 4];
        
        if stream.read_exact(&mut len_buf).await.is_err() {
            return;
        }
        
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        
        if stream.read_exact(&mut buf).await.is_err() {
            return;
        }
        
        self.statistics.network_bytes_received
            .fetch_add((len + 4) as u64, Ordering::Relaxed);
        
        if let Ok(msg) = bincode::deserialize::<CacheMessage>(&buf) {
            self.handle_message(msg).await;
        }
    }
    
    async fn handle_message(&self, msg: CacheMessage) {
        match msg {
            CacheMessage::Get { key, request_id } => {
                let value = self.get(&key).await.ok().flatten();
                let response = CacheMessage::GetResponse {
                    value,
                    version: 0,
                    request_id,
                };
                
                if let Some(tx) = self.cluster_state.write().pending_requests.remove(&request_id) {
                    let _ = tx.send(response);
                }
            }
            CacheMessage::Put { key, value, version, ttl } => {
                let entry = CacheEntry {
                    key: key.clone(),
                    value,
                    version,
                    ttl,
                    created_at: Instant::now(),
                    last_accessed: Instant::now(),
                    access_count: 0,
                };
                let _ = self.local_put(entry);
            }
            CacheMessage::Delete { key, .. } => {
                self.local_cache.write().remove(&key);
            }
            CacheMessage::Invalidate { key, .. } => {
                self.local_cache.write().remove(&key);
            }
            CacheMessage::Replicate { entries } => {
                let mut cache = self.local_cache.write();
                for entry in entries {
                    cache.insert(entry.key.clone(), entry);
                }
            }
            CacheMessage::Join { node_id } => {
                self.handle_node_join(node_id).await;
            }
            CacheMessage::Leave { node_id } => {
                self.handle_node_leave(node_id).await;
            }
            _ => {}
        }
    }
    
    async fn handle_node_join(&self, node_id: NodeId) {
        info!("Node {} joining cluster", node_id.id);
        
        self.consistent_hash.write().add_node(node_id.clone());
        
        let node_info = NodeInfo {
            node_id: node_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            load: 0.0,
            capacity: 0,
            status: NodeStatus::Healthy,
        };
        
        self.cluster_state.write().nodes.insert(node_id, node_info);
        
        self.trigger_rebalance().await;
    }
    
    async fn handle_node_leave(&self, node_id: NodeId) {
        info!("Node {} leaving cluster", node_id.id);
        
        self.consistent_hash.write().remove_node(&node_id);
        self.cluster_state.write().nodes.remove(&node_id);
        
        self.trigger_rebalance().await;
    }
    
    async fn trigger_rebalance(&self) {
        let mut state = self.cluster_state.write();
        if state.rebalancing {
            return;
        }
        
        state.rebalancing = true;
        drop(state);
        
        let cache = self.clone();
        tokio::spawn(async move {
            cache.perform_rebalance().await;
        });
    }
    
    async fn perform_rebalance(&self) {
        info!("Starting cache rebalance");
        
        let local_cache = self.local_cache.read();
        let mut migrations = Vec::new();
        
        for (key, entry) in local_cache.iter() {
            let nodes = self.get_responsible_nodes(key);
            
            if !nodes.is_empty() && nodes[0] != self.config.node_id {
                migrations.push((key.clone(), entry.clone(), nodes[0].clone()));
            }
        }
        
        drop(local_cache);
        
        for (key, entry, target_node) in migrations {
            if self.replicate_to(&target_node, vec![entry]).await.is_ok() {
                self.local_cache.write().remove(&key);
            }
        }
        
        self.cluster_state.write().rebalancing = false;
        
        info!("Cache rebalance completed");
    }
    
    async fn run_heartbeat(&self) {
        let mut ticker = interval(self.config.heartbeat_interval);
        
        while !self.shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            
            let load = self.calculate_load();
            let capacity = self.config.capacity;
            
            let msg = CacheMessage::Heartbeat {
                node_id: self.config.node_id.clone(),
                load,
                capacity,
            };
            
            for node in self.get_all_nodes() {
                if node != self.config.node_id {
                    let _ = self.send_to_node(&node, msg.clone()).await;
                }
            }
            
            self.detect_failed_nodes();
        }
    }
    
    async fn run_sync(&self) {
        let mut ticker = interval(self.config.sync_interval);
        
        while !self.shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            
            if let ReplicationStrategy::PrimaryBackup { .. } = self.config.replication {
                self.sync_replicas().await;
            }
        }
    }
    
    async fn sync_replicas(&self) {
        let local_cache = self.local_cache.read();
        let mut sync_map: HashMap<NodeId, Vec<Vec<u8>>> = HashMap::new();
        
        for key in local_cache.keys() {
            let nodes = self.get_responsible_nodes(key);
            
            for i in 1..nodes.len() {
                sync_map.entry(nodes[i].clone())
                    .or_insert_with(Vec::new)
                    .push(key.clone());
            }
        }
        
        drop(local_cache);
        
        for (node, keys) in sync_map {
            let msg = CacheMessage::Sync { keys };
            let _ = self.send_to_node(&node, msg).await;
        }
    }
    
    async fn run_cleanup(&self) {
        let mut ticker = interval(Duration::from_secs(60));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            ticker.tick().await;
            
            let mut cache = self.local_cache.write();
            let keys_to_remove: Vec<_> = cache
                .iter()
                .filter(|(_, entry)| !self.is_entry_valid(entry))
                .map(|(k, _)| k.clone())
                .collect();
            
            for key in keys_to_remove {
                cache.remove(&key);
            }
        }
    }
    
    fn calculate_load(&self) -> f64 {
        let cache_size = self.local_cache.read().len();
        (cache_size as f64) / (self.config.capacity as f64)
    }
    
    fn get_all_nodes(&self) -> Vec<NodeId> {
        self.cluster_state.read()
            .nodes
            .keys()
            .cloned()
            .collect()
    }
    
    fn detect_failed_nodes(&self) {
        let mut state = self.cluster_state.write();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let timeout = self.config.failure_detection_timeout.as_secs();
        
        for node_info in state.nodes.values_mut() {
            if now - node_info.last_seen > timeout {
                if node_info.status == NodeStatus::Healthy {
                    node_info.status = NodeStatus::Suspected;
                    warn!("Node {} suspected failed", node_info.node_id.id);
                } else if node_info.status == NodeStatus::Suspected {
                    node_info.status = NodeStatus::Failed;
                    error!("Node {} marked as failed", node_info.node_id.id);
                }
            }
        }
    }
    
    fn next_request_id(&self) -> u64 {
        let mut state = self.cluster_state.write();
        let id = state.next_request_id;
        state.next_request_id += 1;
        id
    }
    
    fn generate_version(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
    
    pub async fn join_cluster(&self, seed_nodes: Vec<NodeId>) -> Result<()> {
        let join_msg = CacheMessage::Join {
            node_id: self.config.node_id.clone(),
        };
        
        for node in seed_nodes {
            if let Ok(_) = self.send_to_node(&node, join_msg.clone()).await {
                info!("Joined cluster via {}", node.id);
                return Ok(());
            }
        }
        
        Err(Error::Generic("Failed to join cluster".to_string()))
    }
    
    pub async fn leave_cluster(&self) -> Result<()> {
        let leave_msg = CacheMessage::Leave {
            node_id: self.config.node_id.clone(),
        };
        
        for node in self.get_all_nodes() {
            if node != self.config.node_id {
                let _ = self.send_to_node(&node, leave_msg.clone()).await;
            }
        }
        
        self.shutdown.store(true, Ordering::Relaxed);
        
        info!("Left cluster");
        Ok(())
    }
    
    pub fn get_statistics(&self) -> CacheStats {
        let local_size = self.local_cache.read().len();
        
        CacheStats {
            hits: self.statistics.hits.load(Ordering::Relaxed),
            misses: self.statistics.misses.load(Ordering::Relaxed),
            puts: self.statistics.puts.load(Ordering::Relaxed),
            deletes: self.statistics.deletes.load(Ordering::Relaxed),
            evictions: self.statistics.evictions.load(Ordering::Relaxed),
            local_entries: local_size,
            cluster_nodes: self.get_all_nodes().len(),
            replication_lag: self.statistics.replication_lag.load(Ordering::Relaxed),
            network_bytes_sent: self.statistics.network_bytes_sent.load(Ordering::Relaxed),
            network_bytes_received: self.statistics.network_bytes_received.load(Ordering::Relaxed),
        }
    }
}

impl Clone for DistributedCache {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_cache: self.local_cache.clone(),
            consistent_hash: self.consistent_hash.clone(),
            cluster_state: self.cluster_state.clone(),
            message_sender: self.message_sender.clone(),
            message_receiver: self.message_receiver.clone(),
            statistics: self.statistics.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub deletes: u64,
    pub evictions: u64,
    pub local_entries: usize,
    pub cluster_nodes: usize,
    pub replication_lag: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

pub struct CacheClientBuilder {
    nodes: Vec<NodeId>,
    config: DistributedCacheConfig,
}

impl CacheClientBuilder {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            config: DistributedCacheConfig::default(),
        }
    }
    
    pub fn add_node(mut self, addr: &str) -> Result<Self> {
        let socket_addr: SocketAddr = addr.parse()
            .map_err(|e| Error::Generic(format!("Invalid address: {}", e)))?;
        
        let node_id = NodeId {
            id: uuid::Uuid::new_v4().to_string(),
            addr: socket_addr,
        };
        
        self.nodes.push(node_id);
        Ok(self)
    }
    
    pub fn with_replication(mut self, strategy: ReplicationStrategy) -> Self {
        self.config.replication = strategy;
        self
    }
    
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }
    
    pub async fn build(self) -> Result<Arc<DistributedCache>> {
        let cache = DistributedCache::new(self.config).await?;
        
        if !self.nodes.is_empty() {
            cache.join_cluster(self.nodes).await?;
        }
        
        Ok(cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_consistent_hash() {
        let mut hash = ConsistentHash::new(150);
        
        let node1 = NodeId {
            id: "node1".to_string(),
            addr: "127.0.0.1:7001".parse().unwrap(),
        };
        
        let node2 = NodeId {
            id: "node2".to_string(),
            addr: "127.0.0.1:7002".parse().unwrap(),
        };
        
        hash.add_node(node1.clone());
        hash.add_node(node2.clone());
        
        let key = b"test_key";
        let responsible = hash.get_node(key);
        assert!(responsible.is_some());
        
        let nodes = hash.get_nodes(key, 2);
        assert_eq!(nodes.len(), 2);
    }
    
    #[test]
    fn test_cache_entry_validity() {
        let entry = CacheEntry {
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
            version: 1,
            ttl: Some(Duration::from_secs(1)),
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
        };
        
        std::thread::sleep(Duration::from_millis(500));
        assert!(entry.created_at.elapsed() < entry.ttl.unwrap());
        
        std::thread::sleep(Duration::from_millis(600));
        assert!(entry.created_at.elapsed() > entry.ttl.unwrap());
    }
    
    #[tokio::test]
    async fn test_distributed_cache_creation() {
        let config = DistributedCacheConfig {
            node_id: NodeId {
                id: "test".to_string(),
                addr: "127.0.0.1:7010".parse().unwrap(),
            },
            ..Default::default()
        };
        
        let cache = DistributedCache::new(config).await;
        assert!(cache.is_ok());
    }
    
    #[test]
    fn test_replication_strategy() {
        let strat = ReplicationStrategy::PrimaryBackup { replicas: 2 };
        
        match strat {
            ReplicationStrategy::PrimaryBackup { replicas } => {
                assert_eq!(replicas, 2);
            }
            _ => panic!("Wrong strategy"),
        }
    }
}
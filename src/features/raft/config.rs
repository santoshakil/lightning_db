use serde::{Serialize, Deserialize};
use std::time::Duration;
use super::core::NodeId;
use super::membership::ClusterConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub initial_cluster: ClusterConfig,
    
    pub election_timeout: ElectionTimeout,
    pub heartbeat_interval: Duration,
    pub snapshot_threshold: usize,
    pub snapshot_chunk_size: usize,
    
    pub max_append_entries_batch: usize,
    pub max_in_flight_append_entries: usize,
    pub batch_optimization: bool,
    pub pipeline_requests: bool,
    
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub leader_lease: bool,
    pub read_only_lease_based: bool,
    
    pub auto_compaction: bool,
    pub compaction_overhead: usize,
    pub trailing_logs: usize,
    
    pub tick_interval: Duration,
    pub max_uncommitted_entries: usize,
    pub max_size_per_msg: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            initial_cluster: ClusterConfig::default(),
            
            election_timeout: ElectionTimeout::default(),
            heartbeat_interval: Duration::from_millis(150),
            snapshot_threshold: 10000,
            snapshot_chunk_size: 64 * 1024,
            
            max_append_entries_batch: 100,
            max_in_flight_append_entries: 256,
            batch_optimization: true,
            pipeline_requests: true,
            
            pre_vote: true,
            check_quorum: true,
            leader_lease: true,
            read_only_lease_based: true,
            
            auto_compaction: true,
            compaction_overhead: 1000,
            trailing_logs: 5000,
            
            tick_interval: Duration::from_millis(100),
            max_uncommitted_entries: 5000,
            max_size_per_msg: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ElectionTimeout {
    pub min: Duration,
    pub max: Duration,
}

impl Default for ElectionTimeout {
    fn default() -> Self {
        Self {
            min: Duration::from_millis(300),
            max: Duration::from_millis(600),
        }
    }
}

impl ElectionTimeout {
    pub fn random(&self) -> Duration {
        use rand::Rng;
        let min_ms = self.min.as_millis() as u64;
        let max_ms = self.max.as_millis() as u64;
        let timeout_ms = rand::thread_rng().gen_range(min_ms..=max_ms);
        Duration::from_millis(timeout_ms)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub bind_address: String,
    pub advertise_address: String,
    pub rpc_port: u16,
    pub client_port: u16,
    pub enable_tls: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub ca_file: Option<String>,
    pub connection_pool_size: usize,
    pub max_concurrent_streams: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            advertise_address: "127.0.0.1".to_string(),
            rpc_port: 5000,
            client_port: 5001,
            enable_tls: false,
            cert_file: None,
            key_file: None,
            ca_file: None,
            connection_pool_size: 10,
            max_concurrent_streams: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_dir: Option<String>,
    pub snapshot_dir: Option<String>,
    pub sync_writes: bool,
    pub cache_size: usize,
    pub compression: CompressionType,
    pub encryption: Option<EncryptionConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            wal_dir: None,
            snapshot_dir: None,
            sync_writes: true,
            cache_size: 128 * 1024 * 1024,
            compression: CompressionType::None,
            encryption: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub key_id: String,
    pub key_provider: KeyProvider,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    Aes256Gcm,
    ChaCha20Poly1305,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyProvider {
    File(String),
    Env(String),
    Kms(KmsConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsConfig {
    pub provider: String,
    pub key_id: String,
    pub region: String,
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enable: bool,
    pub prometheus_port: u16,
    pub collect_interval: Duration,
    pub namespace: String,
    pub subsystem: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            prometheus_port: 9090,
            collect_interval: Duration::from_secs(10),
            namespace: "raft".to_string(),
            subsystem: "node".to_string(),
        }
    }
}

pub struct ConfigBuilder {
    config: RaftConfig,
}

impl ConfigBuilder {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            config: RaftConfig {
                node_id,
                ..Default::default()
            },
        }
    }

    pub fn with_cluster(mut self, cluster: ClusterConfig) -> Self {
        self.config.initial_cluster = cluster;
        self
    }

    pub fn with_election_timeout(mut self, min: Duration, max: Duration) -> Self {
        self.config.election_timeout = ElectionTimeout { min, max };
        self
    }

    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    pub fn with_snapshot_threshold(mut self, threshold: usize) -> Self {
        self.config.snapshot_threshold = threshold;
        self
    }

    pub fn enable_pre_vote(mut self, enable: bool) -> Self {
        self.config.pre_vote = enable;
        self
    }

    pub fn enable_pipeline(mut self, enable: bool) -> Self {
        self.config.pipeline_requests = enable;
        self
    }

    pub fn build(self) -> RaftConfig {
        self.config
    }
}

pub fn validate_config(config: &RaftConfig) -> Result<(), String> {
    if config.node_id == 0 {
        return Err("Node ID must be non-zero".to_string());
    }

    if config.election_timeout.min >= config.election_timeout.max {
        return Err("Min election timeout must be less than max".to_string());
    }

    if config.heartbeat_interval >= config.election_timeout.min {
        return Err("Heartbeat interval must be less than min election timeout".to_string());
    }

    if config.max_append_entries_batch == 0 {
        return Err("Max append entries batch must be positive".to_string());
    }

    if config.snapshot_threshold == 0 {
        return Err("Snapshot threshold must be positive".to_string());
    }

    Ok(())
}
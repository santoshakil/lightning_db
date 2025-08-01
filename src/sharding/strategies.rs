use super::*;
use std::collections::{hash_map::DefaultHasher, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use regex;

/// Hash-based sharding router using consistent hashing
pub struct HashBasedRouter {
    /// Current topology version
    topology_version: AtomicU64,
    
    /// Cached consistent hash ring
    cached_ring: RwLock<Option<ConsistentHashRing>>,
    
    /// Routing statistics
    stats: RwLock<RoutingStats>,
}

impl HashBasedRouter {
    pub fn new() -> Self {
        Self {
            topology_version: AtomicU64::new(0),
            cached_ring: RwLock::new(None),
            stats: RwLock::new(RoutingStats::default()),
        }
    }
    
    /// Hash a partition key to a 64-bit value
    fn hash_key(&self, key: &PartitionKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        match key {
            PartitionKey::String(s) => s.hash(&mut hasher),
            PartitionKey::Integer(i) => i.hash(&mut hasher),
            PartitionKey::Binary(b) => b.hash(&mut hasher),
            PartitionKey::Composite(keys) => {
                for k in keys {
                    self.hash_key(k);
                    hasher.write_u8(0); // Separator
                }
            }
        }
        hasher.finish()
    }
    
    /// Build consistent hash ring from topology
    fn build_hash_ring(&self, topology: &ClusterTopology) -> ConsistentHashRing {
        let shards = topology.shards.read();
        let virtual_nodes_per_shard = 256; // Good balance between distribution and memory
        
        let mut virtual_nodes = BTreeMap::new();
        
        for (shard_id, shard_info) in shards.iter() {
            // Create virtual nodes for each shard
            for i in 0..virtual_nodes_per_shard {
                let virtual_key = format!("shard-{}-vnode-{}", shard_id, i);
                let mut hasher = DefaultHasher::new();
                virtual_key.hash(&mut hasher);
                let hash = hasher.finish();
                
                virtual_nodes.insert(hash, *shard_id);
            }
        }
        
        ConsistentHashRing {
            virtual_nodes,
            virtual_nodes_per_shard,
            total_capacity: u64::MAX,
        }
    }
    
    /// Find shard in consistent hash ring
    fn find_shard_in_ring(&self, hash: u64, ring: &ConsistentHashRing) -> Option<ShardId> {
        // Find the first virtual node >= hash
        if let Some((_, shard_id)) = ring.virtual_nodes.range(hash..).next() {
            Some(*shard_id)
        } else {
            // Wrap around to the first virtual node
            ring.virtual_nodes.iter().next().map(|(_, shard_id)| *shard_id)
        }
    }
}

impl ShardRouter for HashBasedRouter {
    fn route(&self, key: &PartitionKey) -> Result<ShardId> {
        let start = Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
        }
        
        // Get cached ring
        let ring = {
            let cached = self.cached_ring.read();
            cached.clone()
        };
        
        let shard_id = if let Some(ring) = ring {
            // Use cached ring
            let hash = self.hash_key(key);
            self.find_shard_in_ring(hash, &ring)
                .ok_or_else(|| Error::NotFound("No shards available".to_string()))?
        } else {
            // No cached ring
            {
                let mut stats = self.stats.write();
                stats.cache_misses += 1;
                stats.failed_routes += 1;
            }
            return Err(Error::NotFound("Routing table not initialized".to_string()));
        };
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.cache_hits += 1;
            let latency = start.elapsed().as_micros() as f64;
            stats.avg_latency = (stats.avg_latency * (stats.total_requests - 1) as f64 + latency) / stats.total_requests as f64;
        }
        
        Ok(shard_id)
    }
    
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>> {
        // Hash-based sharding doesn't support efficient range queries
        // We need to check all shards
        let ring = self.cached_ring.read();
        if let Some(ref ring) = *ring {
            let mut shards: Vec<ShardId> = ring.virtual_nodes.values().cloned().collect();
            shards.sort_unstable();
            shards.dedup();
            Ok(shards)
        } else {
            Err(Error::NotFound("Routing table not initialized".to_string()))
        }
    }
    
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()> {
        let new_version = *topology.version.read();
        let current_version = self.topology_version.load(Ordering::Acquire);
        
        if new_version > current_version {
            // Rebuild hash ring
            let new_ring = self.build_hash_ring(topology);
            
            // Update cached ring
            *self.cached_ring.write() = Some(new_ring);
            
            // Update version
            self.topology_version.store(new_version, Ordering::Release);
            
            println!("Hash-based routing updated to version {}", new_version);
        }
        
        Ok(())
    }
    
    fn get_stats(&self) -> RoutingStats {
        (*self.stats.read()).clone()
    }
}

/// Range-based sharding router for ordered data
pub struct RangeBasedRouter {
    /// Current topology version
    topology_version: AtomicU64,
    
    /// Cached range map
    cached_ranges: RwLock<Option<BTreeMap<PartitionKey, ShardId>>>,
    
    /// Routing statistics
    stats: RwLock<RoutingStats>,
}

impl RangeBasedRouter {
    pub fn new() -> Self {
        Self {
            topology_version: AtomicU64::new(0),
            cached_ranges: RwLock::new(None),
            stats: RwLock::new(RoutingStats::default()),
        }
    }
    
    /// Build range map from topology
    fn build_range_map(&self, topology: &ClusterTopology) -> BTreeMap<PartitionKey, ShardId> {
        let shards = topology.shards.read();
        let mut range_map = BTreeMap::new();
        
        for (shard_id, shard_info) in shards.iter() {
            if let Some(ref key_range) = shard_info.key_range {
                range_map.insert(key_range.end.clone(), *shard_id);
            }
        }
        
        range_map
    }
}

impl ShardRouter for RangeBasedRouter {
    fn route(&self, key: &PartitionKey) -> Result<ShardId> {
        let start = Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
        }
        
        // Get cached ranges
        let ranges = {
            let cached = self.cached_ranges.read();
            cached.clone()
        };
        
        let shard_id = if let Some(ranges) = ranges {
            // Find the first range where key <= end_key
            ranges.range(key..)
                .next()
                .map(|(_, shard_id)| *shard_id)
                .ok_or_else(|| Error::NotFound(format!("No shard found for key: {}", key)))?
        } else {
            {
                let mut stats = self.stats.write();
                stats.cache_misses += 1;
                stats.failed_routes += 1;
            }
            return Err(Error::NotFound("Routing table not initialized".to_string()));
        };
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.cache_hits += 1;
            let latency = start.elapsed().as_micros() as f64;
            stats.avg_latency = (stats.avg_latency * (stats.total_requests - 1) as f64 + latency) / stats.total_requests as f64;
        }
        
        Ok(shard_id)
    }
    
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>> {
        let ranges = self.cached_ranges.read();
        if let Some(ref ranges) = *ranges {
            let mut shards = Vec::new();
            
            // Find all ranges that overlap with [start, end)
            for (range_end, shard_id) in ranges.range(start..) {
                if range_end <= end {
                    shards.push(*shard_id);
                } else {
                    // Include this shard too since it contains the end key
                    shards.push(*shard_id);
                    break;
                }
            }
            
            shards.sort_unstable();
            shards.dedup();
            Ok(shards)
        } else {
            Err(Error::NotFound("Routing table not initialized".to_string()))
        }
    }
    
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()> {
        let new_version = *topology.version.read();
        let current_version = self.topology_version.load(Ordering::Acquire);
        
        if new_version > current_version {
            // Rebuild range map
            let new_ranges = self.build_range_map(topology);
            
            // Update cached ranges
            *self.cached_ranges.write() = Some(new_ranges);
            
            // Update version
            self.topology_version.store(new_version, Ordering::Release);
            
            println!("Range-based routing updated to version {}", new_version);
        }
        
        Ok(())
    }
    
    fn get_stats(&self) -> RoutingStats {
        (*self.stats.read()).clone()
    }
}

/// Directory-based sharding router with explicit key mapping
pub struct DirectoryBasedRouter {
    /// Current topology version
    topology_version: AtomicU64,
    
    /// Cached directory
    cached_directory: RwLock<Option<HashMap<PartitionKey, ShardId>>>,
    
    /// Default shard for unmapped keys
    default_shard: RwLock<Option<ShardId>>,
    
    /// Routing statistics
    stats: RwLock<RoutingStats>,
}

impl DirectoryBasedRouter {
    pub fn new() -> Self {
        Self {
            topology_version: AtomicU64::new(0),
            cached_directory: RwLock::new(None),
            default_shard: RwLock::new(None),
            stats: RwLock::new(RoutingStats::default()),
        }
    }
    
    /// Build directory from topology
    fn build_directory(&self, topology: &ClusterTopology) -> HashMap<PartitionKey, ShardId> {
        let routing_table = topology.routing_table.read();
        routing_table.directory.clone().unwrap_or_default()
    }
}

impl ShardRouter for DirectoryBasedRouter {
    fn route(&self, key: &PartitionKey) -> Result<ShardId> {
        let start = Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
        }
        
        // Get cached directory
        let directory = {
            let cached = self.cached_directory.read();
            cached.clone()
        };
        
        let shard_id = if let Some(directory) = directory {
            // Look up in directory
            if let Some(&shard_id) = directory.get(key) {
                shard_id
            } else {
                // Use default shard
                let default = self.default_shard.read();
                default.clone()
                    .ok_or_else(|| Error::NotFound(format!("No mapping found for key: {}", key)))?
            }
        } else {
            {
                let mut stats = self.stats.write();
                stats.cache_misses += 1;
                stats.failed_routes += 1;
            }
            return Err(Error::NotFound("Routing table not initialized".to_string()));
        };
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.cache_hits += 1;
            let latency = start.elapsed().as_micros() as f64;
            stats.avg_latency = (stats.avg_latency * (stats.total_requests - 1) as f64 + latency) / stats.total_requests as f64;
        }
        
        Ok(shard_id)
    }
    
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>> {
        let directory = self.cached_directory.read();
        if let Some(ref directory) = *directory {
            let mut shards = HashSet::new();
            
            // Find all keys in range
            for (key, shard_id) in directory {
                if key >= start && key < end {
                    shards.insert(*shard_id);
                }
            }
            
            let mut result: Vec<ShardId> = shards.into_iter().collect();
            result.sort_unstable();
            Ok(result)
        } else {
            Err(Error::NotFound("Routing table not initialized".to_string()))
        }
    }
    
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()> {
        let new_version = *topology.version.read();
        let current_version = self.topology_version.load(Ordering::Acquire);
        
        if new_version > current_version {
            // Rebuild directory
            let new_directory = self.build_directory(topology);
            
            // Update cached directory
            *self.cached_directory.write() = Some(new_directory);
            
            // Update default shard
            let routing_table = topology.routing_table.read();
            *self.default_shard.write() = routing_table.default_shard;
            
            // Update version
            self.topology_version.store(new_version, Ordering::Release);
            
            println!("Directory-based routing updated to version {}", new_version);
        }
        
        Ok(())
    }
    
    fn get_stats(&self) -> RoutingStats {
        (*self.stats.read()).clone()
    }
}

/// Hybrid router that combines multiple strategies
pub struct HybridRouter {
    /// Hash-based router for general purpose
    hash_router: HashBasedRouter,
    
    /// Range-based router for ordered queries
    range_router: RangeBasedRouter,
    
    /// Directory-based router for specific mappings
    directory_router: DirectoryBasedRouter,
    
    /// Configuration for hybrid routing
    config: HybridConfig,
    
    /// Routing statistics
    stats: RwLock<RoutingStats>,
}

/// Configuration for hybrid routing
#[derive(Debug, Clone)]
pub struct HybridConfig {
    /// Use directory for specific key patterns
    pub use_directory_for: Vec<String>, // regex patterns
    
    /// Use range for ordered key patterns  
    pub use_range_for: Vec<String>, // regex patterns
    
    /// Default to hash-based routing
    pub default_to_hash: bool,
    
    /// Enable adaptive routing based on access patterns
    pub adaptive_routing: bool,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            use_directory_for: vec![
                r"user:.*".to_string(),      // User-specific keys
                r"session:.*".to_string(),   // Session keys
            ],
            use_range_for: vec![
                r"ts:.*".to_string(),        // Timestamp-based keys
                r"log:.*".to_string(),       // Log entries
            ],
            default_to_hash: true,
            adaptive_routing: true,
        }
    }
}

impl HybridRouter {
    pub fn new() -> Self {
        Self {
            hash_router: HashBasedRouter::new(),
            range_router: RangeBasedRouter::new(),
            directory_router: DirectoryBasedRouter::new(),
            config: HybridConfig::default(),
            stats: RwLock::new(RoutingStats::default()),
        }
    }
    
    /// Determine which strategy to use for a key
    fn choose_strategy(&self, key: &PartitionKey) -> RoutingStrategy {
        let key_str = key.to_string();
        
        // Check directory patterns
        for pattern in &self.config.use_directory_for {
            if let Ok(regex) = regex::Regex::new(pattern) {
                if regex.is_match(&key_str) {
                    return RoutingStrategy::Directory;
                }
            }
        }
        
        // Check range patterns
        for pattern in &self.config.use_range_for {
            if let Ok(regex) = regex::Regex::new(pattern) {
                if regex.is_match(&key_str) {
                    return RoutingStrategy::Range;
                }
            }
        }
        
        // Default to hash
        RoutingStrategy::Hash
    }
}

#[derive(Debug, Clone, Copy)]
enum RoutingStrategy {
    Hash,
    Range,
    Directory,
}

impl ShardRouter for HybridRouter {
    fn route(&self, key: &PartitionKey) -> Result<ShardId> {
        let start = Instant::now();
        
        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_requests += 1;
        }
        
        // Choose strategy
        let strategy = self.choose_strategy(key);
        
        // Route using chosen strategy
        let result = match strategy {
            RoutingStrategy::Hash => self.hash_router.route(key),
            RoutingStrategy::Range => self.range_router.route(key),
            RoutingStrategy::Directory => self.directory_router.route(key),
        };
        
        // Update stats
        {
            let mut stats = self.stats.write();
            match result {
                Ok(_) => {
                    stats.cache_hits += 1;
                    let latency = start.elapsed().as_micros() as f64;
                    stats.avg_latency = (stats.avg_latency * (stats.total_requests - 1) as f64 + latency) / stats.total_requests as f64;
                }
                Err(_) => {
                    stats.failed_routes += 1;
                }
            }
        }
        
        result
    }
    
    fn route_range(&self, start: &PartitionKey, end: &PartitionKey) -> Result<Vec<ShardId>> {
        // For range queries, prefer range-based routing
        match self.range_router.route_range(start, end) {
            Ok(shards) => Ok(shards),
            Err(_) => {
                // Fallback to hash-based routing
                self.hash_router.route_range(start, end)
            }
        }
    }
    
    fn update_routing(&self, topology: &ClusterTopology) -> Result<()> {
        // Update all underlying routers
        self.hash_router.update_routing(topology)?;
        self.range_router.update_routing(topology)?;
        self.directory_router.update_routing(topology)?;
        
        Ok(())
    }
    
    fn get_stats(&self) -> RoutingStats {
        let mut combined_stats = self.stats.read().clone();
        
        // Combine stats from all routers
        let hash_stats = self.hash_router.get_stats();
        let range_stats = self.range_router.get_stats();
        let directory_stats = self.directory_router.get_stats();
        
        combined_stats.total_requests = hash_stats.total_requests + range_stats.total_requests + directory_stats.total_requests;
        combined_stats.cache_hits = hash_stats.cache_hits + range_stats.cache_hits + directory_stats.cache_hits;
        combined_stats.cache_misses = hash_stats.cache_misses + range_stats.cache_misses + directory_stats.cache_misses;
        combined_stats.failed_routes = hash_stats.failed_routes + range_stats.failed_routes + directory_stats.failed_routes;
        
        // Average latency
        if combined_stats.total_requests > 0 {
            combined_stats.avg_latency = (
                hash_stats.avg_latency * hash_stats.total_requests as f64 +
                range_stats.avg_latency * range_stats.total_requests as f64 +
                directory_stats.avg_latency * directory_stats.total_requests as f64
            ) / combined_stats.total_requests as f64;
        }
        
        combined_stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_router_key_distribution() {
        let router = HashBasedRouter::new();
        
        let keys = vec![
            PartitionKey::String("user:1".to_string()),
            PartitionKey::String("user:2".to_string()),
            PartitionKey::String("user:3".to_string()),
            PartitionKey::Integer(12345),
            PartitionKey::Binary(vec![1, 2, 3, 4]),
        ];
        
        let mut hashes = HashSet::new();
        for key in &keys {
            let hash = router.hash_key(key);
            hashes.insert(hash);
        }
        
        // All keys should have different hashes
        assert_eq!(hashes.len(), keys.len());
    }
    
    #[test]
    fn test_range_router_ordering() {
        let key1 = PartitionKey::String("aaa".to_string());
        let key2 = PartitionKey::String("bbb".to_string());
        let key3 = PartitionKey::String("ccc".to_string());
        
        assert!(key1 < key2);
        assert!(key2 < key3);
        assert!(key1 < key3);
    }
    
    #[test]
    fn test_partition_key_ordering() {
        let mut keys = vec![
            PartitionKey::String("zebra".to_string()),
            PartitionKey::String("apple".to_string()),
            PartitionKey::String("banana".to_string()),
        ];
        
        keys.sort();
        
        assert_eq!(keys[0], PartitionKey::String("apple".to_string()));
        assert_eq!(keys[1], PartitionKey::String("banana".to_string()));
        assert_eq!(keys[2], PartitionKey::String("zebra".to_string()));
    }
}
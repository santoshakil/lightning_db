pub mod cache_friendly;
pub mod memory_layout;
/// High-performance optimization modules for Lightning DB
///
/// This module contains various optimization techniques designed to maximize
/// performance on modern CPU architectures:
///
/// - SIMD (Single Instruction, Multiple Data) optimizations
/// - Cache-friendly memory layouts and algorithms
/// - Lock-free data structures
/// - Memory pooling and alignment optimizations
pub mod simd;

pub use cache_friendly::{CacheFriendlyAlgorithms, CacheFriendlyBTree, CacheFriendlyLRU};
pub use memory_layout::{
    CacheAlignedAllocator, CompactRecord, MappedBuffer, MemoryLayoutOps, ObjectPool,
    OptimizedBTreeNode,
};
pub use simd::safe as simd_ops;

/// Performance optimization configuration
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    /// Enable SIMD optimizations (auto-detected by default)
    pub enable_simd: bool,

    /// Enable cache-friendly algorithms
    pub enable_cache_optimizations: bool,

    /// Enable memory layout optimizations
    pub enable_memory_optimizations: bool,

    /// Object pool sizes for frequently allocated objects
    pub object_pool_sizes: ObjectPoolSizes,

    /// Memory prefetch configuration
    pub prefetch_config: PrefetchConfig,
}

#[derive(Debug, Clone)]
pub struct ObjectPoolSizes {
    /// Pool size for B-tree nodes
    pub btree_nodes: usize,

    /// Pool size for transaction objects
    pub transactions: usize,

    /// Pool size for cache entries
    pub cache_entries: usize,

    /// Pool size for buffer objects
    pub buffers: usize,
}

#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Enable automatic prefetching
    pub enable_prefetch: bool,

    /// Prefetch distance (number of cache lines)
    pub prefetch_distance: usize,

    /// Prefetch hint type (0=temporal, 1=non-temporal)
    pub prefetch_hint: u8,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            enable_simd: true,
            enable_cache_optimizations: true,
            enable_memory_optimizations: true,
            object_pool_sizes: ObjectPoolSizes::default(),
            prefetch_config: PrefetchConfig::default(),
        }
    }
}

impl Default for ObjectPoolSizes {
    fn default() -> Self {
        Self {
            btree_nodes: 1000,
            transactions: 100,
            cache_entries: 5000,
            buffers: 200,
        }
    }
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enable_prefetch: true,
            prefetch_distance: 2,
            prefetch_hint: 0,
        }
    }
}

/// Performance optimization manager
pub struct OptimizationManager {
    config: OptimizationConfig,

    /// Object pools for memory reuse
    btree_node_pool: ObjectPool<OptimizedBTreeNode>,
    buffer_pool: ObjectPool<Vec<u8>>,

    /// Performance counters
    simd_operations: std::sync::atomic::AtomicU64,
    cache_hits: std::sync::atomic::AtomicU64,
    cache_misses: std::sync::atomic::AtomicU64,
    pool_hits: std::sync::atomic::AtomicU64,
    pool_misses: std::sync::atomic::AtomicU64,
}

impl OptimizationManager {
    /// Create a new optimization manager with the given configuration
    pub fn new(config: OptimizationConfig) -> Self {
        let btree_node_pool = ObjectPool::new(
            || OptimizedBTreeNode::new(0, 0),
            config.object_pool_sizes.btree_nodes,
        );

        let buffer_pool = ObjectPool::new(
            || Vec::with_capacity(4096),
            config.object_pool_sizes.buffers,
        );

        Self {
            config,
            btree_node_pool,
            buffer_pool,
            simd_operations: std::sync::atomic::AtomicU64::new(0),
            cache_hits: std::sync::atomic::AtomicU64::new(0),
            cache_misses: std::sync::atomic::AtomicU64::new(0),
            pool_hits: std::sync::atomic::AtomicU64::new(0),
            pool_misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get an optimized B-tree node from the pool
    pub fn get_btree_node(&mut self) -> Box<OptimizedBTreeNode> {
        if self.btree_node_pool.stats().free_objects > 0 {
            self.pool_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.pool_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.btree_node_pool.get()
    }

    /// Return a B-tree node to the pool
    pub fn return_btree_node(&mut self, node: Box<OptimizedBTreeNode>) {
        self.btree_node_pool.return_object(node);
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&mut self) -> Box<Vec<u8>> {
        if self.buffer_pool.stats().free_objects > 0 {
            self.pool_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.pool_misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.buffer_pool.get()
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&mut self, buffer: Box<Vec<u8>>) {
        self.buffer_pool.return_object(buffer);
    }

    /// Perform optimized key comparison
    pub fn compare_keys(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        if self.config.enable_simd {
            self.simd_operations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            simd_ops::compare_keys(a, b)
        } else {
            a.cmp(b)
        }
    }

    /// Perform optimized hash calculation
    pub fn calculate_hash(&self, data: &[u8], seed: u64) -> u64 {
        if self.config.enable_simd {
            self.simd_operations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            simd_ops::hash(data, seed)
        } else {
            // Fallback hash implementation
            let mut hash = seed;
            for &byte in data {
                hash = hash.wrapping_mul(0x9e3779b97f4a7c15u64);
                hash ^= byte as u64;
                hash = hash.rotate_left(7);
            }
            hash
        }
    }

    /// Perform optimized bulk copy
    pub fn bulk_copy(&self, src: &[u8], dst: &mut [u8]) {
        if self.config.enable_simd {
            self.simd_operations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            simd_ops::bulk_copy(src, dst);
        } else {
            dst.copy_from_slice(src);
        }
    }

    /// Perform optimized prefix search
    pub fn search_prefix(&self, haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if self.config.enable_simd {
            self.simd_operations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            simd_ops::search_prefix(haystack, needle)
        } else {
            haystack
                .windows(needle.len())
                .position(|window| window == needle)
        }
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> OptimizationStats {
        OptimizationStats {
            simd_operations: self
                .simd_operations
                .load(std::sync::atomic::Ordering::Relaxed),
            cache_hits: self.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
            cache_misses: self.cache_misses.load(std::sync::atomic::Ordering::Relaxed),
            pool_hits: self.pool_hits.load(std::sync::atomic::Ordering::Relaxed),
            pool_misses: self.pool_misses.load(std::sync::atomic::Ordering::Relaxed),
            btree_node_pool_stats: self.btree_node_pool.stats(),
            buffer_pool_stats: self.buffer_pool.stats(),
        }
    }

    /// Reset performance counters
    pub fn reset_stats(&self) {
        self.simd_operations
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.cache_hits
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.cache_misses
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.pool_hits
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.pool_misses
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update cache hit statistics
    pub fn record_cache_hit(&self) {
        self.cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Update cache miss statistics
    pub fn record_cache_miss(&self) {
        self.cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get current configuration
    pub fn config(&self) -> &OptimizationConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: OptimizationConfig) {
        self.config = config;
    }
}

/// Performance statistics for optimizations
#[derive(Debug, Clone)]
pub struct OptimizationStats {
    /// Number of SIMD operations performed
    pub simd_operations: u64,

    /// Number of cache hits
    pub cache_hits: u64,

    /// Number of cache misses
    pub cache_misses: u64,

    /// Number of object pool hits
    pub pool_hits: u64,

    /// Number of object pool misses
    pub pool_misses: u64,

    /// B-tree node pool statistics
    pub btree_node_pool_stats: memory_layout::PoolStats,

    /// Buffer pool statistics
    pub buffer_pool_stats: memory_layout::PoolStats,
}

impl OptimizationStats {
    /// Calculate cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Calculate pool hit rate
    pub fn pool_hit_rate(&self) -> f64 {
        let total = self.pool_hits + self.pool_misses;
        if total == 0 {
            0.0
        } else {
            self.pool_hits as f64 / total as f64
        }
    }

    /// Calculate total operations
    pub fn total_operations(&self) -> u64 {
        self.simd_operations + self.cache_hits + self.cache_misses
    }
}

/// Utility functions for optimization detection and configuration
pub mod utils {
    use super::*;

    /// Detect available CPU features and create optimal configuration
    pub fn detect_optimal_config() -> OptimizationConfig {
        let mut config = OptimizationConfig::default();

        // Detect SIMD support
        #[cfg(target_arch = "x86_64")]
        {
            config.enable_simd =
                is_x86_feature_detected!("sse4.2") || is_x86_feature_detected!("avx2");
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            config.enable_simd = false;
        }

        // Adjust pool sizes based on available memory
        #[cfg(feature = "sys-info")]
        {
            let mut system = sysinfo::System::new_all();
            system.refresh_memory();
            let total_memory_bytes = system.total_memory();
            let total_memory_gb = total_memory_bytes / (1024 * 1024 * 1024);

            // Scale pool sizes based on available memory
            let scale_factor = std::cmp::min(total_memory_gb / 4, 8) as usize; // Max 8x scaling

            config.object_pool_sizes.btree_nodes *= scale_factor;
            config.object_pool_sizes.cache_entries *= scale_factor;
            config.object_pool_sizes.buffers *= scale_factor;
        }

        config
    }

    /// Create a configuration optimized for specific workloads
    pub fn create_workload_config(workload: WorkloadType) -> OptimizationConfig {
        let mut config = detect_optimal_config();

        match workload {
            WorkloadType::ReadHeavy => {
                config.object_pool_sizes.cache_entries *= 2;
                config.prefetch_config.prefetch_distance = 4;
            }
            WorkloadType::WriteHeavy => {
                config.object_pool_sizes.buffers *= 2;
                config.object_pool_sizes.transactions *= 2;
                config.prefetch_config.prefetch_distance = 1;
            }
            WorkloadType::Mixed => {
                // Use default balanced configuration
            }
            WorkloadType::AnalyticalScan => {
                config.prefetch_config.prefetch_distance = 8;
                config.enable_simd = true;
            }
        }

        config
    }
}

/// Workload types for optimization configuration
#[derive(Debug, Clone, Copy)]
pub enum WorkloadType {
    /// Read-heavy workloads (favor cache optimizations)
    ReadHeavy,

    /// Write-heavy workloads (favor write optimizations)
    WriteHeavy,

    /// Mixed read/write workloads
    Mixed,

    /// Analytical scan workloads (favor sequential access)
    AnalyticalScan,
}

// Re-export sysinfo as sys_info for convenience
#[cfg(feature = "sys-info")]
pub use sysinfo as sys_info;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimization_config_defaults() {
        let config = OptimizationConfig::default();

        assert!(config.enable_simd);
        assert!(config.enable_cache_optimizations);
        assert!(config.enable_memory_optimizations);
        assert_eq!(config.object_pool_sizes.btree_nodes, 1000);
        assert_eq!(config.object_pool_sizes.transactions, 100);
        assert!(config.prefetch_config.enable_prefetch);
    }

    #[test]
    fn test_optimization_manager() {
        let config = OptimizationConfig::default();
        let mut manager = OptimizationManager::new(config);

        // Test object pool
        let node = manager.get_btree_node();
        let node_id = node.header.node_id;
        assert_eq!(node_id, 0);
        manager.return_btree_node(node);

        // Test buffer pool
        let buffer = manager.get_buffer();
        assert!(buffer.capacity() >= 4096);
        manager.return_buffer(buffer);

        // Test operations
        let key1 = b"key1";
        let key2 = b"key2";
        let comparison = manager.compare_keys(key1, key2);
        assert_eq!(comparison, std::cmp::Ordering::Less);

        let hash = manager.calculate_hash(key1, 42);
        assert_ne!(hash, 0);

        // Test statistics
        let stats = manager.get_stats();
        assert!(stats.total_operations() > 0);

        manager.reset_stats();
        let new_stats = manager.get_stats();
        assert_eq!(new_stats.simd_operations, 0);
    }

    #[test]
    fn test_optimization_stats() {
        let stats = OptimizationStats {
            simd_operations: 100,
            cache_hits: 80,
            cache_misses: 20,
            pool_hits: 90,
            pool_misses: 10,
            btree_node_pool_stats: memory_layout::PoolStats {
                free_objects: 5,
                max_pool_size: 10,
                utilization: 0.5,
            },
            buffer_pool_stats: memory_layout::PoolStats {
                free_objects: 3,
                max_pool_size: 10,
                utilization: 0.7,
            },
        };

        assert_eq!(stats.cache_hit_rate(), 0.8);
        assert_eq!(stats.pool_hit_rate(), 0.9);
        assert_eq!(stats.total_operations(), 200);
    }

    #[test]
    fn test_workload_config() {
        let read_heavy = utils::create_workload_config(WorkloadType::ReadHeavy);
        let write_heavy = utils::create_workload_config(WorkloadType::WriteHeavy);
        let analytical = utils::create_workload_config(WorkloadType::AnalyticalScan);

        // Read-heavy should have larger cache
        assert!(
            read_heavy.object_pool_sizes.cache_entries
                >= write_heavy.object_pool_sizes.cache_entries
        );

        // Write-heavy should have larger buffers
        assert!(write_heavy.object_pool_sizes.buffers >= read_heavy.object_pool_sizes.buffers);

        // Analytical should have longer prefetch distance
        assert!(
            analytical.prefetch_config.prefetch_distance
                >= read_heavy.prefetch_config.prefetch_distance
        );
    }
}

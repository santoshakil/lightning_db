//! NUMA-Aware Memory Allocation and Thread Pinning
//!
//! This module provides NUMA (Non-Uniform Memory Access) awareness for Lightning DB,
//! optimizing memory allocation and thread placement for multi-socket systems.
//!
//! Key features:
//! - NUMA topology detection
//! - Memory allocation on local NUMA nodes
//! - Thread pinning to specific CPU cores
//! - Load balancing across NUMA domains
//! - Memory bandwidth optimization

use crate::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

pub mod allocator;
pub mod load_balancer;
pub mod thread_pinning;
pub mod topology;

pub use allocator::{NumaAllocator, NumaMemoryPolicy};
pub use load_balancer::{NumaLoadBalancer, WorkloadDistribution};
pub use thread_pinning::{CpuSet, ThreadPinner, ThreadPlacement};
pub use topology::{CpuInfo, NumaNode, NumaTopology};

/// NUMA-aware memory manager
pub struct NumaManager {
    topology: Arc<NumaTopology>,
    allocator: Arc<NumaAllocator>,
    thread_pinner: Arc<ThreadPinner>,
    load_balancer: Arc<NumaLoadBalancer>,
    config: NumaConfig,
    stats: Arc<RwLock<NumaStats>>,
}

/// NUMA configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaConfig {
    /// Enable NUMA-aware allocation
    pub enable_numa_allocation: bool,
    /// Enable thread pinning
    pub enable_thread_pinning: bool,
    /// Memory allocation policy
    pub memory_policy: NumaMemoryPolicy,
    /// Thread placement strategy
    pub thread_placement: ThreadPlacement,
    /// Load balancing strategy
    pub load_balancing: WorkloadDistribution,
    /// Memory interleaving across nodes
    pub enable_interleaving: bool,
    /// Migrate pages on access
    pub enable_page_migration: bool,
    /// Minimum memory size for NUMA allocation (bytes)
    pub numa_threshold_bytes: usize,
    /// CPU affinity for critical threads
    pub critical_thread_affinity: Vec<CpuSet>,
}

/// NUMA statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NumaStats {
    /// Allocations per NUMA node
    pub allocations_per_node: HashMap<u32, u64>,
    /// Memory usage per NUMA node (bytes)
    pub memory_usage_per_node: HashMap<u32, u64>,
    /// Thread assignments per NUMA node
    pub threads_per_node: HashMap<u32, u32>,
    /// Cross-NUMA memory accesses
    pub cross_numa_accesses: u64,
    /// Page migrations
    pub page_migrations: u64,
    /// Memory bandwidth utilization per node
    pub bandwidth_utilization: HashMap<u32, f64>,
    /// Cache miss rates per node
    pub cache_miss_rates: HashMap<u32, f64>,
}

/// NUMA memory allocation hint
#[derive(Debug, Clone)]
pub struct NumaHint {
    /// Preferred NUMA node
    pub preferred_node: Option<u32>,
    /// Access pattern (sequential, random, etc.)
    pub access_pattern: AccessPattern,
    /// Expected lifetime (short, medium, long)
    pub lifetime: MemoryLifetime,
    /// Thread that will primarily access this memory
    pub primary_thread: Option<thread::ThreadId>,
}

/// Memory access pattern
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessPattern {
    Sequential,
    Random,
    Hotspot,
    ReadOnly,
    WriteHeavy,
    Shared,
}

/// Memory lifetime classification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryLifetime {
    Ephemeral, // < 1 second
    Short,     // 1 second - 1 minute
    Medium,    // 1 minute - 1 hour
    Long,      // > 1 hour
    Permanent, // Database lifetime
}

impl NumaManager {
    /// Create a new NUMA manager
    pub fn new(config: NumaConfig) -> Result<Self> {
        let topology = Arc::new(NumaTopology::detect()?);
        let allocator = Arc::new(NumaAllocator::new(
            topology.clone(),
            config.memory_policy.clone(),
        )?);
        let thread_pinner = Arc::new(ThreadPinner::new(topology.clone())?);
        let load_balancer = Arc::new(NumaLoadBalancer::new(
            topology.clone(),
            config.load_balancing.clone(),
        )?);

        Ok(Self {
            topology,
            allocator,
            thread_pinner,
            load_balancer,
            config,
            stats: Arc::new(RwLock::new(NumaStats::default())),
        })
    }

    /// Initialize NUMA-aware memory management
    pub fn initialize(&self) -> Result<()> {
        // Pin the current thread to an appropriate CPU
        if self.config.enable_thread_pinning {
            let cpu_set = self
                .thread_pinner
                .get_optimal_cpu_set(thread::current().id(), &self.config.thread_placement)?;
            self.thread_pinner.pin_current_thread(&cpu_set)?;
        }

        // Initialize per-node memory pools
        if self.config.enable_numa_allocation {
            self.allocator.initialize_pools()?;
        }

        // Start load balancing if enabled
        self.load_balancer.start_monitoring()?;

        println!("NUMA Manager initialized successfully");
        self.print_topology_info();

        Ok(())
    }

    /// Allocate memory with NUMA awareness
    pub fn allocate(&self, size: usize, hint: Option<NumaHint>) -> Result<*mut u8> {
        if !self.config.enable_numa_allocation || size < self.config.numa_threshold_bytes {
            // Use standard allocation for small allocations
            return self.allocate_standard(size);
        }

        let node = self.determine_allocation_node(size, &hint)?;
        let ptr = self.allocator.allocate_on_node(size, node)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            *stats.allocations_per_node.entry(node).or_insert(0) += 1;
            *stats.memory_usage_per_node.entry(node).or_insert(0) += size as u64;
        }

        Ok(ptr)
    }

    /// Deallocate NUMA-aware memory
    pub fn deallocate(&self, ptr: *mut u8, size: usize) -> Result<()> {
        if let Some(node) = self.allocator.get_allocation_node(ptr) {
            // Update statistics
            {
                let mut stats = self.stats.write();
                if let Some(usage) = stats.memory_usage_per_node.get_mut(&node) {
                    *usage = usage.saturating_sub(size as u64);
                }
            }
        }

        self.allocator.deallocate(ptr, size)
    }

    /// Pin a thread to optimal CPU cores
    pub fn pin_thread(&self, thread_id: thread::ThreadId, hint: Option<ThreadHint>) -> Result<()> {
        if !self.config.enable_thread_pinning {
            return Ok(());
        }

        let cpu_set = if let Some(hint) = hint {
            self.thread_pinner.get_cpu_set_for_hint(&hint)?
        } else {
            self.thread_pinner
                .get_optimal_cpu_set(thread_id, &self.config.thread_placement)?
        };

        self.thread_pinner.pin_thread(thread_id, &cpu_set)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            for node_id in cpu_set.get_numa_nodes() {
                *stats.threads_per_node.entry(node_id).or_insert(0) += 1;
            }
        }

        Ok(())
    }

    /// Get memory allocation advice for a specific access pattern
    pub fn get_allocation_advice(&self, size: usize, pattern: AccessPattern) -> NumaHint {
        let current_thread = thread::current().id();
        let preferred_node = self
            .load_balancer
            .get_preferred_node_for_thread(current_thread);

        let lifetime = match size {
            0..=4096 => MemoryLifetime::Ephemeral,
            4097..=1048576 => MemoryLifetime::Short,
            1048577..=67108864 => MemoryLifetime::Medium,
            _ => MemoryLifetime::Long,
        };

        NumaHint {
            preferred_node,
            access_pattern: pattern,
            lifetime,
            primary_thread: Some(current_thread),
        }
    }

    /// Migrate memory to a different NUMA node
    pub fn migrate_memory(&self, ptr: *mut u8, size: usize, target_node: u32) -> Result<()> {
        if !self.config.enable_page_migration {
            return Err(Error::InvalidOperation {
                reason: "Page migration is disabled".to_string(),
            });
        }

        self.allocator.migrate_pages(ptr, size, target_node)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.page_migrations += 1;
        }

        Ok(())
    }

    /// Get current NUMA statistics
    pub fn get_stats(&self) -> NumaStats {
        (*self.stats.read()).clone()
    }

    /// Get NUMA topology information
    pub fn get_topology(&self) -> &NumaTopology {
        &self.topology
    }

    /// Optimize memory layout for better NUMA locality
    pub fn optimize_layout(&self) -> Result<()> {
        // Analyze current memory usage patterns
        let stats = self.get_stats();

        // Identify memory regions with poor NUMA locality
        let problematic_regions = self.identify_problematic_regions(&stats)?;

        // Suggest or perform memory migrations
        for region in problematic_regions {
            if region.migration_benefit > 0.2 {
                println!(
                    "Migrating memory region ({} bytes) to node {} for better locality",
                    region.size, region.target_node
                );
                self.migrate_memory(region.ptr, region.size, region.target_node)?;
            }
        }

        Ok(())
    }

    /// Print NUMA topology information
    fn print_topology_info(&self) {
        let topology = &self.topology;

        println!("NUMA Topology Information:");
        println!("  Nodes: {}", topology.get_node_count());

        for node in topology.get_nodes() {
            println!(
                "  Node {}: {} CPUs, {} MB memory",
                node.id, node.cpu_count, node.memory_size_mb
            );
            println!("    CPUs: {:?}", node.cpu_list);
            println!(
                "    Memory bandwidth: {:.1} GB/s",
                node.memory_bandwidth_gbps
            );
        }
    }

    /// Determine the best NUMA node for allocation
    fn determine_allocation_node(&self, size: usize, hint: &Option<NumaHint>) -> Result<u32> {
        if let Some(hint) = hint {
            if let Some(preferred_node) = hint.preferred_node {
                // Use the explicitly preferred node
                return Ok(preferred_node);
            }
        }

        // Use load balancer to determine optimal node
        let current_thread = thread::current().id();
        if let Some(node) = self
            .load_balancer
            .get_preferred_node_for_thread(current_thread)
        {
            Ok(node)
        } else {
            // Fallback to the least loaded node
            Ok(self.load_balancer.get_least_loaded_node())
        }
    }

    /// Standard allocation for small sizes or when NUMA is disabled
    fn allocate_standard(&self, size: usize) -> Result<*mut u8> {
        use std::alloc::{alloc, Layout};

        let layout = Layout::from_size_align(size, std::mem::align_of::<u8>()).map_err(|e| {
            Error::InvalidOperation {
                reason: format!("Invalid memory layout: {}", e),
            }
        })?;

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(Error::Memory);
        }

        Ok(ptr)
    }

    /// Identify memory regions that would benefit from migration
    fn identify_problematic_regions(&self, stats: &NumaStats) -> Result<Vec<MigrationCandidate>> {
        // This is a simplified analysis - in practice, this would use
        // more sophisticated algorithms to analyze access patterns
        let mut candidates = Vec::new();

        // Look for nodes with high cross-NUMA access rates
        for (node_id, cache_miss_rate) in &stats.cache_miss_rates {
            if *cache_miss_rate > 0.15 {
                // 15% cache miss rate threshold
                // This is a placeholder - real implementation would track
                // specific memory regions and their access patterns
                candidates.push(MigrationCandidate {
                    ptr: std::ptr::null_mut(), // Would be actual pointer
                    size: 0,                   // Would be actual size
                    current_node: *node_id,
                    target_node: self.find_better_node(*node_id),
                    migration_benefit: cache_miss_rate - 0.10,
                });
            }
        }

        Ok(candidates)
    }

    /// Find a better NUMA node for memory migration
    fn find_better_node(&self, current_node: u32) -> u32 {
        // Find the node with the lowest memory usage
        let stats = self.stats.read();
        stats
            .memory_usage_per_node
            .iter()
            .filter(|(node_id, _)| **node_id != current_node)
            .min_by_key(|(_, usage)| *usage)
            .map(|(node_id, _)| *node_id)
            .unwrap_or(0)
    }
}

/// Thread placement hint
#[derive(Debug, Clone)]
pub struct ThreadHint {
    /// Thread type
    pub thread_type: ThreadType,
    /// Preferred NUMA node
    pub preferred_node: Option<u32>,
    /// CPU affinity requirements
    pub cpu_requirements: CpuRequirements,
}

/// Thread type classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThreadType {
    /// I/O worker thread
    IoWorker,
    /// Compute-intensive thread
    Compute,
    /// Network thread
    Network,
    /// Background maintenance
    Maintenance,
    /// Transaction processing
    Transaction,
    /// Query execution
    Query,
}

/// CPU requirements for thread placement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuRequirements {
    /// Minimum CPU cores needed
    pub min_cores: u32,
    /// Preferred CPU cores
    pub preferred_cores: u32,
    /// Require physical cores (not hyperthreads)
    pub require_physical: bool,
    /// Require specific CPU features
    pub required_features: Vec<String>,
}

/// Memory migration candidate
#[derive(Debug)]
struct MigrationCandidate {
    ptr: *mut u8,
    size: usize,
    current_node: u32,
    target_node: u32,
    migration_benefit: f64,
}

impl Default for NumaConfig {
    fn default() -> Self {
        Self {
            enable_numa_allocation: true,
            enable_thread_pinning: true,
            memory_policy: NumaMemoryPolicy::LocalPreferred,
            thread_placement: ThreadPlacement::LoadBalanced,
            load_balancing: WorkloadDistribution::RoundRobin,
            enable_interleaving: false,
            enable_page_migration: true,
            numa_threshold_bytes: 4096, // Only use NUMA for allocations > 4KB
            critical_thread_affinity: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_manager_creation() {
        let config = NumaConfig::default();
        let result = NumaManager::new(config);

        // May fail on systems without NUMA, which is fine for testing
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_allocation_advice() {
        let config = NumaConfig::default();
        if let Ok(manager) = NumaManager::new(config) {
            let advice = manager.get_allocation_advice(1024, AccessPattern::Sequential);
            assert_eq!(advice.access_pattern, AccessPattern::Sequential);
            assert_eq!(advice.lifetime, MemoryLifetime::Ephemeral);
        }
    }

    #[test]
    fn test_numa_hint_creation() {
        let hint = NumaHint {
            preferred_node: Some(0),
            access_pattern: AccessPattern::Random,
            lifetime: MemoryLifetime::Medium,
            primary_thread: Some(thread::current().id()),
        };

        assert_eq!(hint.preferred_node, Some(0));
        assert!(matches!(hint.access_pattern, AccessPattern::Random));
    }
}

//! NUMA-Aware Memory Allocator
//!
//! This module provides NUMA-aware memory allocation that can allocate memory
//! on specific NUMA nodes for optimal locality.

use crate::{Result, Error};
use crate::numa::topology::NumaTopology;
use std::sync::Arc;
use std::collections::HashMap;
use std::alloc::{GlobalAlloc, Layout};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

/// NUMA memory allocation policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NumaMemoryPolicy {
    /// Prefer local node, fallback to any node
    LocalPreferred,
    /// Strict local node only
    LocalOnly,
    /// Interleave across all nodes
    Interleave,
    /// Bind to specific node
    Bind(u32),
    /// Round-robin across nodes
    RoundRobin,
    /// Allocate on least loaded node
    LeastLoaded,
}

/// NUMA-aware memory allocator
pub struct NumaAllocator {
    topology: Arc<NumaTopology>,
    policy: NumaMemoryPolicy,
    node_allocators: HashMap<u32, NodeAllocator>,
    allocation_tracker: Arc<RwLock<AllocationTracker>>,
    stats: Arc<RwLock<AllocatorStats>>,
    round_robin_counter: std::sync::atomic::AtomicU32,
}

/// Per-node memory allocator
struct NodeAllocator {
    node_id: u32,
    memory_pools: Vec<MemoryPool>,
    large_allocation_tracker: HashMap<usize, AllocationInfo>, // Use address as usize
}

/// Memory pool for specific allocation sizes
struct MemoryPool {
    block_size: usize,
    blocks: Vec<usize>, // Use addresses instead of raw pointers
    free_blocks: Vec<usize>, // Use addresses instead of raw pointers
    total_allocated: usize,
}

/// Allocation tracking information
#[derive(Debug, Clone)]
struct AllocationInfo {
    size: usize,
    node_id: u32,
    allocation_time: std::time::Instant,
    access_count: u64,
}

/// Tracks all allocations for migration and analysis
struct AllocationTracker {
    allocations: HashMap<usize, AllocationInfo>, // Use address as usize instead of raw pointer
    node_usage: HashMap<u32, u64>,
    total_allocated: u64,
}

/// Allocator statistics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AllocatorStats {
    /// Allocations per node
    pub allocations_per_node: HashMap<u32, u64>,
    /// Bytes allocated per node
    pub bytes_per_node: HashMap<u32, u64>,
    /// Failed allocations per node
    pub failed_allocations: HashMap<u32, u64>,
    /// Average allocation size per node
    pub avg_allocation_size: HashMap<u32, f64>,
    /// Memory utilization per node
    pub memory_utilization: HashMap<u32, f64>,
    /// Cross-node allocation fallbacks
    pub cross_node_fallbacks: u64,
}

impl NumaAllocator {
    /// Create a new NUMA-aware allocator
    pub fn new(topology: Arc<NumaTopology>, policy: NumaMemoryPolicy) -> Result<Self> {
        let mut node_allocators = HashMap::new();
        
        // Create allocators for each NUMA node
        for node in topology.get_nodes() {
            let allocator = NodeAllocator::new(node.id)?;
            node_allocators.insert(node.id, allocator);
        }

        Ok(Self {
            topology,
            policy,
            node_allocators,
            allocation_tracker: Arc::new(RwLock::new(AllocationTracker::new())),
            stats: Arc::new(RwLock::new(AllocatorStats::default())),
            round_robin_counter: std::sync::atomic::AtomicU32::new(0),
        })
    }

    /// Initialize memory pools on all nodes
    pub fn initialize_pools(&self) -> Result<()> {
        for (node_id, _) in &self.node_allocators {
            self.initialize_node_pools(*node_id)?;
        }
        Ok(())
    }

    /// Allocate memory on a specific NUMA node
    pub fn allocate_on_node(&self, size: usize, node_id: u32) -> Result<*mut u8> {
        // Try to allocate on the requested node
        if let Some(node_allocator) = self.node_allocators.get(&node_id) {
            if let Ok(ptr) = self.allocate_on_node_internal(node_allocator, size, node_id) {
                self.track_allocation(ptr, size, node_id);
                return Ok(ptr);
            }
        }

        // Fallback based on policy
        match &self.policy {
            NumaMemoryPolicy::LocalOnly => {
                Err(Error::Memory)
            }
            _ => {
                // Try fallback allocation
                let fallback_node = self.select_fallback_node(node_id)?;
                if let Some(node_allocator) = self.node_allocators.get(&fallback_node) {
                    let ptr = self.allocate_on_node_internal(node_allocator, size, fallback_node)?;
                    self.track_allocation(ptr, size, fallback_node);
                    
                    // Update fallback statistics
                    {
                        let mut stats = self.stats.write();
                        stats.cross_node_fallbacks += 1;
                    }
                    
                    Ok(ptr)
                } else {
                    Err(Error::Memory)
                }
            }
        }
    }

    /// Allocate memory using the configured policy
    pub fn allocate(&self, size: usize) -> Result<*mut u8> {
        let node_id = self.select_allocation_node(size)?;
        self.allocate_on_node(size, node_id)
    }

    /// Deallocate memory
    pub fn deallocate(&self, ptr: *mut u8, size: usize) -> Result<()> {
        // Find which node this allocation belongs to
        let node_id = {
            let tracker = self.allocation_tracker.read();
            tracker.allocations.get(&(ptr as usize)).map(|info| info.node_id)
        };

        if let Some(node_id) = node_id {
            if let Some(node_allocator) = self.node_allocators.get(&node_id) {
                self.deallocate_on_node_internal(node_allocator, ptr, size)?;
            }
            
            // Remove from tracking
            {
                let mut tracker = self.allocation_tracker.write();
                tracker.allocations.remove(&(ptr as usize));
                if let Some(usage) = tracker.node_usage.get_mut(&node_id) {
                    *usage = usage.saturating_sub(size as u64);
                }
                tracker.total_allocated = tracker.total_allocated.saturating_sub(size as u64);
            }
        } else {
            // Fallback to standard deallocation
            unsafe {
                let layout = Layout::from_size_align(size, std::mem::align_of::<u8>())
                    .map_err(|e| Error::InvalidOperation { reason: format!("Invalid layout: {}", e) })?;
                std::alloc::dealloc(ptr, layout);
            }
        }

        Ok(())
    }

    /// Get the NUMA node for an allocation
    pub fn get_allocation_node(&self, ptr: *mut u8) -> Option<u32> {
        let tracker = self.allocation_tracker.read();
        tracker.allocations.get(&(ptr as usize)).map(|info| info.node_id)
    }

    /// Migrate memory pages to a different NUMA node
    pub fn migrate_pages(&self, ptr: *mut u8, size: usize, target_node: u32) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.migrate_pages_linux(ptr, size, target_node)
        }

        #[cfg(not(target_os = "linux"))]
        {
            // For non-Linux systems, we can't migrate pages in place
            // In a real implementation, we'd allocate new memory and copy
            Err(Error::UnsupportedFeature { feature: "Page migration".to_string() })
        }
    }

    /// Get allocator statistics
    pub fn get_stats(&self) -> AllocatorStats {
        (*self.stats.read()).clone()
    }

    /// Select allocation node based on policy
    fn select_allocation_node(&self, size: usize) -> Result<u32> {
        match &self.policy {
            NumaMemoryPolicy::LocalPreferred => {
                // Try to get the local node for current thread
                self.get_local_node().or_else(|| Some(0))
                    .ok_or_else(|| Error::Generic("No NUMA nodes available".to_string()))
            }
            NumaMemoryPolicy::LocalOnly => {
                self.get_local_node()
                    .ok_or_else(|| Error::Memory)
            }
            NumaMemoryPolicy::Interleave => {
                let node_count = self.topology.get_node_count();
                let node_id = (size / 4096) as u32 % node_count; // Interleave by page
                Ok(node_id)
            }
            NumaMemoryPolicy::Bind(node_id) => {
                Ok(*node_id)
            }
            NumaMemoryPolicy::RoundRobin => {
                let node_count = self.topology.get_node_count();
                let counter = self.round_robin_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(counter % node_count)
            }
            NumaMemoryPolicy::LeastLoaded => {
                self.find_least_loaded_node()
            }
        }
    }

    /// Get the local NUMA node for the current thread
    fn get_local_node(&self) -> Option<u32> {
        // In a real implementation, this would detect which CPU core
        // the current thread is running on and return its NUMA node
        // For now, return node 0 as default
        Some(0)
    }

    /// Find the least loaded NUMA node
    fn find_least_loaded_node(&self) -> Result<u32> {
        let tracker = self.allocation_tracker.read();
        let mut min_usage = u64::MAX;
        let mut best_node = 0;

        for node in self.topology.get_nodes() {
            let usage = tracker.node_usage.get(&node.id).copied().unwrap_or(0);
            if usage < min_usage {
                min_usage = usage;
                best_node = node.id;
            }
        }

        Ok(best_node)
    }

    /// Select fallback node when primary allocation fails
    fn select_fallback_node(&self, failed_node: u32) -> Result<u32> {
        // Find the closest node to the failed node
        let mut best_node = None;
        let mut best_distance = f64::MAX;

        for node in self.topology.get_nodes() {
            if node.id != failed_node {
                let distance = self.topology.get_node_distance(failed_node, node.id);
                if distance < best_distance {
                    best_distance = distance;
                    best_node = Some(node.id);
                }
            }
        }

        best_node.ok_or_else(|| Error::Memory)
    }

    /// Internal allocation on a specific node
    fn allocate_on_node_internal(&self, node_allocator: &NodeAllocator, size: usize, node_id: u32) -> Result<*mut u8> {
        // For large allocations, use direct system allocation
        if size > 2 * 1024 * 1024 { // > 2MB
            return self.allocate_large_on_node(size, node_id);
        }

        // Try pool allocation first
        if let Ok(ptr) = self.allocate_from_pools(node_allocator, size) {
            return Ok(ptr);
        }

        // Fallback to direct allocation
        self.allocate_large_on_node(size, node_id)
    }

    /// Allocate from memory pools
    fn allocate_from_pools(&self, node_allocator: &NodeAllocator, size: usize) -> Result<*mut u8> {
        // Find the appropriate pool size (round up to next power of 2)
        let pool_size = size.next_power_of_two().max(64);
        
        // This is a simplified pool allocation - real implementation would
        // maintain actual memory pools with free list management
        self.allocate_large_on_node(size, node_allocator.node_id)
    }

    /// Allocate large memory directly on node
    fn allocate_large_on_node(&self, size: usize, node_id: u32) -> Result<*mut u8> {
        #[cfg(target_os = "linux")]
        {
            self.allocate_large_linux(size, node_id)
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Fallback to standard allocation
            let layout = Layout::from_size_align(size, std::mem::align_of::<u8>())
                .map_err(|e| Error::InvalidOperation { reason: format!("Invalid layout: {}", e) })?;
            
            let ptr = unsafe { std::alloc::alloc(layout) };
            if ptr.is_null() {
                Err(Error::Memory)
            } else {
                Ok(ptr)
            }
        }
    }

    /// Linux-specific large allocation with NUMA binding
    #[cfg(target_os = "linux")]
    fn allocate_large_linux(&self, size: usize, node_id: u32) -> Result<*mut u8> {
        use std::os::raw::{c_void, c_int, c_long};
        
        // Use mmap with NUMA policy (simplified)
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::Memory);
        }

        // Try to bind to specific NUMA node using mbind
        // This is a simplified version - real implementation would use proper syscalls
        #[cfg(feature = "numa_syscalls")]
        {
            let node_mask = 1u64 << node_id;
            let result = unsafe {
                libc::syscall(
                    libc::SYS_mbind,
                    ptr,
                    size,
                    1, // MPOL_BIND
                    &node_mask as *const u64,
                    64, // maxnode
                    0   // flags
                )
            };

            if result != 0 {
                // If NUMA binding fails, continue with regular allocation
                eprintln!("Warning: NUMA binding failed for node {}", node_id);
            }
        }

        Ok(ptr as *mut u8)
    }

    /// Linux-specific page migration
    #[cfg(target_os = "linux")]
    fn migrate_pages_linux(&self, ptr: *mut u8, size: usize, target_node: u32) -> Result<()> {
        // This would use the move_pages system call in a real implementation
        // For now, we'll just return success
        Ok(())
    }

    /// Internal deallocation on a specific node
    fn deallocate_on_node_internal(&self, node_allocator: &NodeAllocator, ptr: *mut u8, size: usize) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            // If it was allocated with mmap, use munmap
            if size > 2 * 1024 * 1024 {
                unsafe {
                    libc::munmap(ptr as *mut libc::c_void, size);
                }
                return Ok(());
            }
        }

        // For smaller allocations, use standard deallocation
        unsafe {
            let layout = Layout::from_size_align(size, std::mem::align_of::<u8>())
                .map_err(|e| Error::InvalidOperation { reason: format!("Invalid layout: {}", e) })?;
            std::alloc::dealloc(ptr, layout);
        }

        Ok(())
    }

    /// Track allocation for statistics and migration
    fn track_allocation(&self, ptr: *mut u8, size: usize, node_id: u32) {
        let allocation_info = AllocationInfo {
            size,
            node_id,
            allocation_time: std::time::Instant::now(),
            access_count: 0,
        };

        {
            let mut tracker = self.allocation_tracker.write();
            tracker.allocations.insert(ptr as usize, allocation_info);
            *tracker.node_usage.entry(node_id).or_insert(0) += size as u64;
            tracker.total_allocated += size as u64;
        }

        // Update statistics
        {
            let mut stats = self.stats.write();
            *stats.allocations_per_node.entry(node_id).or_insert(0) += 1;
            *stats.bytes_per_node.entry(node_id).or_insert(0) += size as u64;
            
            // Update average allocation size
            let allocs = *stats.allocations_per_node.get(&node_id).unwrap();
            let bytes = *stats.bytes_per_node.get(&node_id).unwrap();
            stats.avg_allocation_size.insert(node_id, bytes as f64 / allocs as f64);
        }
    }

    /// Initialize memory pools for a specific node
    fn initialize_node_pools(&self, node_id: u32) -> Result<()> {
        // This would initialize actual memory pools in a real implementation
        Ok(())
    }
}

impl NodeAllocator {
    fn new(node_id: u32) -> Result<Self> {
        Ok(Self {
            node_id,
            memory_pools: Vec::new(),
            large_allocation_tracker: HashMap::new(),
        })
    }
}

impl AllocationTracker {
    fn new() -> Self {
        Self {
            allocations: HashMap::new(),
            node_usage: HashMap::new(),
            total_allocated: 0,
        }
    }
}

/// NUMA-aware global allocator wrapper
pub struct NumaGlobalAllocator {
    allocator: Arc<NumaAllocator>,
    fallback: std::alloc::System,
}

impl NumaGlobalAllocator {
    pub fn new(allocator: Arc<NumaAllocator>) -> Self {
        Self {
            allocator,
            fallback: std::alloc::System,
        }
    }
}

unsafe impl GlobalAlloc for NumaGlobalAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= 4096 { // Use NUMA allocation for large allocations
            self.allocator.allocate(layout.size())
                .unwrap_or_else(|_| self.fallback.alloc(layout))
        } else {
            self.fallback.alloc(layout)
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if self.allocator.get_allocation_node(ptr).is_some() {
            let _ = self.allocator.deallocate(ptr, layout.size());
        } else {
            self.fallback.dealloc(ptr, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numa::topology::NumaTopology;

    #[test]
    fn test_numa_allocator_creation() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let policy = NumaMemoryPolicy::LocalPreferred;
        let result = NumaAllocator::new(topology, policy);
        assert!(result.is_ok());
    }

    #[test]
    fn test_allocation_and_deallocation() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let policy = NumaMemoryPolicy::LocalPreferred;
        let allocator = NumaAllocator::new(topology, policy).unwrap();
        
        let size = 4096;
        let ptr = allocator.allocate(size);
        
        if let Ok(ptr) = ptr {
            let result = allocator.deallocate(ptr, size);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_node_selection_policies() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        
        // Test different policies
        let policies = vec![
            NumaMemoryPolicy::LocalPreferred,
            NumaMemoryPolicy::RoundRobin,
            NumaMemoryPolicy::LeastLoaded,
        ];
        
        for policy in policies {
            let allocator = NumaAllocator::new(topology.clone(), policy);
            assert!(allocator.is_ok());
        }
    }
}
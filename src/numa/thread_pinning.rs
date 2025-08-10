//! Thread Pinning and CPU Affinity Management
//!
//! This module provides thread pinning capabilities to bind threads to specific
//! CPU cores for optimal NUMA locality and performance.

use crate::numa::topology::NumaTopology;
use crate::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::thread;

/// Thread pinner for managing CPU affinity
pub struct ThreadPinner {
    topology: Arc<NumaTopology>,
    thread_assignments: Arc<RwLock<HashMap<thread::ThreadId, CpuSet>>>,
    cpu_usage: Arc<RwLock<HashMap<u32, u32>>>, // CPU ID -> thread count
    numa_node_usage: Arc<RwLock<HashMap<u32, u32>>>, // Node ID -> thread count
}

/// CPU set for thread affinity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuSet {
    /// CPU IDs in the set
    pub cpu_ids: BTreeSet<u32>,
    /// Preferred NUMA node
    pub numa_node: Option<u32>,
    /// Is this an exclusive assignment?
    pub exclusive: bool,
}

/// Thread placement strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThreadPlacement {
    /// Automatic load balancing
    LoadBalanced,
    /// Prefer specific NUMA node
    NumaPreferred(u32),
    /// Bind to specific CPU cores
    CpuBound(Vec<u32>),
    /// Distribute across all nodes
    Distributed,
    /// Pack threads on minimal cores
    Compact,
    /// Spread threads across all cores
    Spread,
}

/// CPU affinity mask for different platforms
#[derive(Debug)]
pub struct AffinityMask {
    #[cfg(target_os = "linux")]
    pub mask: libc::cpu_set_t,
    #[cfg(target_os = "macos")]
    pub cpu_ids: Vec<u32>, // macOS doesn't support hard affinity
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    pub cpu_ids: Vec<u32>,
}

/// Thread type for specialized placement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThreadType {
    /// Database worker thread
    Worker,
    /// I/O thread
    Io,
    /// Network thread
    Network,
    /// Background maintenance
    Background,
    /// User query thread
    Query,
    /// System critical thread
    Critical,
}

impl ThreadPinner {
    /// Create a new thread pinner
    pub fn new(topology: Arc<NumaTopology>) -> Result<Self> {
        Ok(Self {
            topology,
            thread_assignments: Arc::new(RwLock::new(HashMap::new())),
            cpu_usage: Arc::new(RwLock::new(HashMap::new())),
            numa_node_usage: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Pin the current thread to a CPU set
    pub fn pin_current_thread(&self, cpu_set: &CpuSet) -> Result<()> {
        let thread_id = thread::current().id();
        self.pin_thread(thread_id, cpu_set)
    }

    /// Pin a specific thread to a CPU set
    pub fn pin_thread(&self, thread_id: thread::ThreadId, cpu_set: &CpuSet) -> Result<()> {
        // Apply the actual CPU affinity
        self.apply_cpu_affinity(cpu_set)?;

        // Track the assignment
        {
            let mut assignments = self.thread_assignments.write();
            assignments.insert(thread_id, cpu_set.clone());
        }

        // Update usage statistics
        self.update_usage_stats(cpu_set, 1);

        println!(
            "Pinned thread {:?} to CPUs: {:?}",
            thread_id, cpu_set.cpu_ids
        );
        Ok(())
    }

    /// Unpin a thread and restore default affinity
    pub fn unpin_thread(&self, thread_id: thread::ThreadId) -> Result<()> {
        let cpu_set = {
            let mut assignments = self.thread_assignments.write();
            assignments.remove(&thread_id)
        };

        if let Some(cpu_set) = cpu_set {
            // Restore full CPU affinity
            let all_cpus = self.get_all_cpu_set();
            self.apply_cpu_affinity(&all_cpus)?;

            // Update usage statistics
            self.update_usage_stats(&cpu_set, -1);
        }

        Ok(())
    }

    /// Get optimal CPU set for a thread based on placement strategy
    pub fn get_optimal_cpu_set(
        &self,
        thread_id: thread::ThreadId,
        placement: &ThreadPlacement,
    ) -> Result<CpuSet> {
        match placement {
            ThreadPlacement::LoadBalanced => self.get_load_balanced_cpu_set(),
            ThreadPlacement::NumaPreferred(node_id) => self.get_numa_preferred_cpu_set(*node_id),
            ThreadPlacement::CpuBound(cpu_ids) => Ok(CpuSet {
                cpu_ids: cpu_ids.iter().cloned().collect(),
                numa_node: self.get_numa_node_for_cpus(cpu_ids),
                exclusive: true,
            }),
            ThreadPlacement::Distributed => self.get_distributed_cpu_set(),
            ThreadPlacement::Compact => self.get_compact_cpu_set(),
            ThreadPlacement::Spread => self.get_spread_cpu_set(),
        }
    }

    /// Get CPU set based on thread type and hint
    pub fn get_cpu_set_for_hint(&self, hint: &crate::numa::ThreadHint) -> Result<CpuSet> {
        let base_set = if let Some(preferred_node) = hint.preferred_node {
            self.get_numa_preferred_cpu_set(preferred_node)?
        } else {
            self.get_load_balanced_cpu_set()?
        };

        // Modify based on thread type
        match hint.thread_type {
            crate::numa::ThreadType::IoWorker => {
                // I/O workers benefit from dedicated cores
                self.get_dedicated_cpu_set(&base_set, 1)
            }
            crate::numa::ThreadType::Compute => {
                // Compute threads need multiple cores
                self.get_dedicated_cpu_set(
                    &base_set,
                    hint.cpu_requirements.preferred_cores as usize,
                )
            }
            crate::numa::ThreadType::Network => {
                // Network threads prefer specific cores close to network hardware
                self.get_network_optimal_cpu_set()
            }
            _ => Ok(base_set),
        }
    }

    /// Get load-balanced CPU set
    fn get_load_balanced_cpu_set(&self) -> Result<CpuSet> {
        let cpu_usage = self.cpu_usage.read();
        let numa_usage = self.numa_node_usage.read();

        // Find the least loaded NUMA node
        let mut best_node = 0;
        let mut min_load = u32::MAX;

        for node in self.topology.get_nodes() {
            let load = numa_usage.get(&node.id).copied().unwrap_or(0);
            if load < min_load {
                min_load = load;
                best_node = node.id;
            }
        }

        // Find the least loaded CPU in that node
        let node = self
            .topology
            .get_nodes()
            .iter()
            .find(|n| n.id == best_node)
            .ok_or_else(|| Error::Generic("NUMA node not found".to_string()))?;

        let mut best_cpu = node.cpu_list[0];
        let mut min_cpu_load = u32::MAX;

        for &cpu_id in &node.cpu_list {
            let load = cpu_usage.get(&cpu_id).copied().unwrap_or(0);
            if load < min_cpu_load {
                min_cpu_load = load;
                best_cpu = cpu_id;
            }
        }

        Ok(CpuSet {
            cpu_ids: [best_cpu].iter().cloned().collect(),
            numa_node: Some(best_node),
            exclusive: false,
        })
    }

    /// Get CPU set for specific NUMA node
    fn get_numa_preferred_cpu_set(&self, node_id: u32) -> Result<CpuSet> {
        let node = self
            .topology
            .get_nodes()
            .iter()
            .find(|n| n.id == node_id)
            .ok_or_else(|| Error::Generic(format!("NUMA node {} not found", node_id)))?;

        let cpu_usage = self.cpu_usage.read();

        // Find the least loaded CPU in the node
        let mut best_cpu = node.cpu_list[0];
        let mut min_load = u32::MAX;

        for &cpu_id in &node.cpu_list {
            let load = cpu_usage.get(&cpu_id).copied().unwrap_or(0);
            if load < min_load {
                min_load = load;
                best_cpu = cpu_id;
            }
        }

        Ok(CpuSet {
            cpu_ids: [best_cpu].iter().cloned().collect(),
            numa_node: Some(node_id),
            exclusive: false,
        })
    }

    /// Get distributed CPU set across all nodes
    fn get_distributed_cpu_set(&self) -> Result<CpuSet> {
        let mut cpu_ids = BTreeSet::new();

        // Take one CPU from each NUMA node
        for node in self.topology.get_nodes() {
            if let Some(&cpu_id) = node.cpu_list.first() {
                cpu_ids.insert(cpu_id);
            }
        }

        Ok(CpuSet {
            cpu_ids,
            numa_node: None, // Spans multiple nodes
            exclusive: false,
        })
    }

    /// Get compact CPU set (pack on minimal cores)
    fn get_compact_cpu_set(&self) -> Result<CpuSet> {
        // Find the NUMA node with the most available CPUs
        let mut best_node = None;
        let mut max_available = 0;

        let cpu_usage = self.cpu_usage.read();

        for node in self.topology.get_nodes() {
            let available = node
                .cpu_list
                .iter()
                .filter(|&&cpu_id| cpu_usage.get(&cpu_id).copied().unwrap_or(0) == 0)
                .count();

            if available > max_available {
                max_available = available;
                best_node = Some(node);
            }
        }

        if let Some(node) = best_node {
            // Take the first available CPU
            for &cpu_id in &node.cpu_list {
                if cpu_usage.get(&cpu_id).copied().unwrap_or(0) == 0 {
                    return Ok(CpuSet {
                        cpu_ids: [cpu_id].iter().cloned().collect(),
                        numa_node: Some(node.id),
                        exclusive: false,
                    });
                }
            }
        }

        // Fallback to first CPU of first node
        let first_node = &self.topology.get_nodes()[0];
        Ok(CpuSet {
            cpu_ids: [first_node.cpu_list[0]].iter().cloned().collect(),
            numa_node: Some(first_node.id),
            exclusive: false,
        })
    }

    /// Get spread CPU set (spread across all cores)
    fn get_spread_cpu_set(&self) -> Result<CpuSet> {
        let mut cpu_ids = BTreeSet::new();

        // Take all available CPUs
        for node in self.topology.get_nodes() {
            cpu_ids.extend(node.cpu_list.iter());
        }

        Ok(CpuSet {
            cpu_ids,
            numa_node: None, // Spans all nodes
            exclusive: false,
        })
    }

    /// Get dedicated CPU set with specific number of cores
    fn get_dedicated_cpu_set(&self, base_set: &CpuSet, num_cores: usize) -> Result<CpuSet> {
        if let Some(numa_node) = base_set.numa_node {
            let node = self
                .topology
                .get_nodes()
                .iter()
                .find(|n| n.id == numa_node)
                .ok_or_else(|| Error::Generic("NUMA node not found".to_string()))?;

            let cpu_usage = self.cpu_usage.read();
            let mut available_cpus: Vec<_> = node
                .cpu_list
                .iter()
                .filter(|&&cpu_id| cpu_usage.get(&cpu_id).copied().unwrap_or(0) == 0)
                .cloned()
                .collect();

            available_cpus.sort();
            available_cpus.truncate(num_cores);

            if available_cpus.is_empty() {
                // Fallback to any CPU in the node
                available_cpus.push(node.cpu_list[0]);
            }

            Ok(CpuSet {
                cpu_ids: available_cpus.into_iter().collect(),
                numa_node: Some(numa_node),
                exclusive: true,
            })
        } else {
            Ok(base_set.clone())
        }
    }

    /// Get CPU set optimal for network operations
    fn get_network_optimal_cpu_set(&self) -> Result<CpuSet> {
        // In a real implementation, this would consider network hardware affinity
        // For now, prefer CPU 0 which often handles interrupts
        Ok(CpuSet {
            cpu_ids: [0].iter().cloned().collect(),
            numa_node: self.topology.get_node_for_cpu(0),
            exclusive: false,
        })
    }

    /// Get CPU set containing all CPUs
    fn get_all_cpu_set(&self) -> CpuSet {
        let mut cpu_ids = BTreeSet::new();

        for node in self.topology.get_nodes() {
            cpu_ids.extend(node.cpu_list.iter());
        }

        CpuSet {
            cpu_ids,
            numa_node: None,
            exclusive: false,
        }
    }

    /// Get NUMA node that contains the given CPUs
    fn get_numa_node_for_cpus(&self, cpu_ids: &[u32]) -> Option<u32> {
        if let Some(&first_cpu) = cpu_ids.first() {
            self.topology.get_node_for_cpu(first_cpu)
        } else {
            None
        }
    }

    /// Apply CPU affinity to the current thread
    fn apply_cpu_affinity(&self, cpu_set: &CpuSet) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.apply_affinity_linux(cpu_set)
        }

        #[cfg(target_os = "macos")]
        {
            self.apply_affinity_macos(cpu_set)
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            // No-op for unsupported platforms
            Ok(())
        }
    }

    /// Apply CPU affinity on Linux
    #[cfg(target_os = "linux")]
    fn apply_affinity_linux(&self, cpu_set: &CpuSet) -> Result<()> {
        use std::mem;

        let mut mask: libc::cpu_set_t = unsafe { mem::zeroed() };

        unsafe {
            libc::CPU_ZERO(&mut mask);

            for &cpu_id in &cpu_set.cpu_ids {
                libc::CPU_SET(cpu_id as usize, &mut mask);
            }

            let result = libc::sched_setaffinity(
                0, // Current thread
                mem::size_of::<libc::cpu_set_t>(),
                &mask,
            );

            if result != 0 {
                let errno = *libc::__errno_location();
                return Err(Error::Generic(format!(
                    "Failed to set CPU affinity: errno {}",
                    errno
                )));
            }
        }

        Ok(())
    }

    /// Apply CPU affinity on macOS
    #[cfg(target_os = "macos")]
    fn apply_affinity_macos(&self, cpu_set: &CpuSet) -> Result<()> {
        // macOS doesn't support hard CPU affinity like Linux
        // We can only provide hints via thread_policy_set

        if let Some(&cpu_id) = cpu_set.cpu_ids.iter().next() {
            // This is a simplified approach - real implementation would use
            // thread_policy_set with THREAD_AFFINITY_POLICY
            println!("Hint: prefer CPU {} on macOS", cpu_id);
        }

        Ok(())
    }

    /// Update usage statistics
    fn update_usage_stats(&self, cpu_set: &CpuSet, delta: i32) {
        // Update CPU usage
        {
            let mut cpu_usage = self.cpu_usage.write();
            for &cpu_id in &cpu_set.cpu_ids {
                let current = cpu_usage.entry(cpu_id).or_insert(0);
                *current = (*current as i32 + delta).max(0) as u32;
            }
        }

        // Update NUMA node usage
        if let Some(numa_node) = cpu_set.numa_node {
            let mut numa_usage = self.numa_node_usage.write();
            let current = numa_usage.entry(numa_node).or_insert(0);
            *current = (*current as i32 + delta).max(0) as u32;
        }
    }

    /// Get current thread assignments
    pub fn get_thread_assignments(&self) -> HashMap<thread::ThreadId, CpuSet> {
        self.thread_assignments.read().clone()
    }

    /// Get CPU usage statistics
    pub fn get_cpu_usage(&self) -> HashMap<u32, u32> {
        self.cpu_usage.read().clone()
    }

    /// Get NUMA node usage statistics
    pub fn get_numa_usage(&self) -> HashMap<u32, u32> {
        self.numa_node_usage.read().clone()
    }
}

impl CpuSet {
    /// Create a new CPU set
    pub fn new(cpu_ids: Vec<u32>) -> Self {
        Self {
            cpu_ids: cpu_ids.into_iter().collect(),
            numa_node: None,
            exclusive: false,
        }
    }

    /// Create a CPU set for a specific NUMA node
    pub fn for_numa_node(cpu_ids: Vec<u32>, numa_node: u32) -> Self {
        Self {
            cpu_ids: cpu_ids.into_iter().collect(),
            numa_node: Some(numa_node),
            exclusive: false,
        }
    }

    /// Check if the CPU set is empty
    pub fn is_empty(&self) -> bool {
        self.cpu_ids.is_empty()
    }

    /// Get the number of CPUs in the set
    pub fn size(&self) -> usize {
        self.cpu_ids.len()
    }

    /// Check if a CPU is in the set
    pub fn contains(&self, cpu_id: u32) -> bool {
        self.cpu_ids.contains(&cpu_id)
    }

    /// Get NUMA nodes covered by this CPU set
    pub fn get_numa_nodes(&self) -> Vec<u32> {
        if let Some(node) = self.numa_node {
            vec![node]
        } else {
            // This would need topology information to determine actual nodes
            vec![0] // Simplified
        }
    }

    /// Intersect with another CPU set
    pub fn intersect(&self, other: &CpuSet) -> CpuSet {
        let intersection: BTreeSet<u32> =
            self.cpu_ids.intersection(&other.cpu_ids).cloned().collect();

        CpuSet {
            cpu_ids: intersection,
            numa_node: if self.numa_node == other.numa_node {
                self.numa_node
            } else {
                None
            },
            exclusive: self.exclusive || other.exclusive,
        }
    }

    /// Union with another CPU set
    pub fn union(&self, other: &CpuSet) -> CpuSet {
        let union: BTreeSet<u32> = self.cpu_ids.union(&other.cpu_ids).cloned().collect();

        CpuSet {
            cpu_ids: union,
            numa_node: if self.numa_node == other.numa_node {
                self.numa_node
            } else {
                None
            },
            exclusive: self.exclusive && other.exclusive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numa::topology::NumaTopology;

    #[test]
    fn test_thread_pinner_creation() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let result = ThreadPinner::new(topology);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cpu_set_operations() {
        let set1 = CpuSet::new(vec![0, 1, 2]);
        let set2 = CpuSet::new(vec![1, 2, 3]);

        assert_eq!(set1.size(), 3);
        assert!(set1.contains(1));
        assert!(!set1.contains(3));

        let intersection = set1.intersect(&set2);
        assert_eq!(intersection.size(), 2);
        assert!(intersection.contains(1));
        assert!(intersection.contains(2));

        let union = set1.union(&set2);
        assert_eq!(union.size(), 4);
    }

    #[test]
    fn test_placement_strategies() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let pinner = ThreadPinner::new(topology).unwrap();

        let strategies = vec![
            ThreadPlacement::LoadBalanced,
            ThreadPlacement::Distributed,
            ThreadPlacement::Compact,
            ThreadPlacement::Spread,
        ];

        for strategy in strategies {
            let result = pinner.get_optimal_cpu_set(thread::current().id(), &strategy);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_numa_preferred_placement() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let pinner = ThreadPinner::new(topology).unwrap();

        if !pinner.topology.get_nodes().is_empty() {
            let node_id = pinner.topology.get_nodes()[0].id;
            let placement = ThreadPlacement::NumaPreferred(node_id);
            let result = pinner.get_optimal_cpu_set(thread::current().id(), &placement);
            assert!(result.is_ok());

            if let Ok(cpu_set) = result {
                assert_eq!(cpu_set.numa_node, Some(node_id));
            }
        }
    }
}

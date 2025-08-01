//! NUMA Load Balancer
//!
//! This module provides intelligent load balancing across NUMA nodes
//! to optimize memory locality and minimize cross-node traffic.

use crate::{Result, Error};
use crate::numa::topology::{NumaTopology, NumaNode};
use std::sync::Arc;
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

/// NUMA load balancer
pub struct NumaLoadBalancer {
    topology: Arc<NumaTopology>,
    distribution: WorkloadDistribution,
    node_metrics: Arc<RwLock<HashMap<u32, NodeMetrics>>>,
    thread_assignments: Arc<RwLock<HashMap<thread::ThreadId, u32>>>,
    monitoring_active: Arc<std::sync::atomic::AtomicBool>,
    rebalance_threshold: f64,
}

/// Workload distribution strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkloadDistribution {
    /// Round-robin assignment
    RoundRobin,
    /// Least loaded node
    LeastLoaded,
    /// Weighted by node capacity
    Weighted,
    /// Locality-aware assignment
    LocalityAware,
    /// Custom weights per node
    CustomWeights(HashMap<u32, f64>),
}

/// Metrics for a NUMA node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Current thread count
    pub thread_count: u32,
    /// Memory usage in bytes
    pub memory_usage: u64,
    /// CPU utilization (0.0 - 1.0)
    pub cpu_utilization: f64,
    /// Memory bandwidth utilization (0.0 - 1.0)
    pub memory_bandwidth_utilization: f64,
    /// Average latency in microseconds
    pub avg_latency_us: f64,
    /// Cache miss rate (0.0 - 1.0)
    pub cache_miss_rate: f64,
    /// Load score (lower is better)
    pub load_score: f64,
    /// Last update timestamp (as milliseconds since epoch)
    #[serde(with = "instant_serde")]
    pub last_update: Instant,
    /// Allocation requests per second
    pub allocation_rate: f64,
    /// Cross-node memory accesses per second
    pub cross_node_accesses: f64,
}

mod instant_serde {
    use std::time::{Instant, SystemTime, UNIX_EPOCH};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    
    pub fn serialize<S>(instant: &Instant, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert to milliseconds since epoch for serialization
        let elapsed = instant.elapsed().as_millis() as u64;
        elapsed.serialize(serializer)
    }
    
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Instant, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = u64::deserialize(deserializer)?;
        // Return current instant (we can't recreate past instants)
        Ok(Instant::now())
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            thread_count: 0,
            memory_usage: 0,
            cpu_utilization: 0.0,
            memory_bandwidth_utilization: 0.0,
            avg_latency_us: 0.0,
            cache_miss_rate: 0.0,
            load_score: 0.0,
            last_update: Instant::now(),
            allocation_rate: 0.0,
            cross_node_accesses: 0.0,
        }
    }
}

/// Load balancing statistics
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct LoadBalancingStats {
    /// Rebalancing events count
    pub rebalance_count: u64,
    /// Thread migrations
    pub thread_migrations: u64,
    /// Average load imbalance
    pub avg_load_imbalance: f64,
    /// Peak load imbalance
    pub peak_load_imbalance: f64,
    /// Load balancing overhead in milliseconds
    pub overhead_ms: f64,
}

impl NumaLoadBalancer {
    /// Create a new NUMA load balancer
    pub fn new(topology: Arc<NumaTopology>, distribution: WorkloadDistribution) -> Result<Self> {
        let mut node_metrics = HashMap::new();
        
        // Initialize metrics for each node
        for node in topology.get_nodes() {
            node_metrics.insert(node.id, NodeMetrics::default());
        }

        Ok(Self {
            topology,
            distribution,
            node_metrics: Arc::new(RwLock::new(node_metrics)),
            thread_assignments: Arc::new(RwLock::new(HashMap::new())),
            monitoring_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            rebalance_threshold: 0.15, // 15% imbalance threshold
        })
    }

    /// Start monitoring and load balancing
    pub fn start_monitoring(&self) -> Result<()> {
        self.monitoring_active.store(true, std::sync::atomic::Ordering::Relaxed);
        
        // Start background monitoring thread
        let node_metrics = Arc::clone(&self.node_metrics);
        let topology = Arc::clone(&self.topology);
        let monitoring_active = Arc::clone(&self.monitoring_active);
        
        thread::spawn(move || {
            let mut last_rebalance = Instant::now();
            
            while monitoring_active.load(std::sync::atomic::Ordering::Relaxed) {
                // Update node metrics
                Self::update_node_metrics_background(&node_metrics, &topology);
                
                // Check if rebalancing is needed
                if last_rebalance.elapsed() > Duration::from_secs(30) {
                    // Rebalance every 30 seconds if needed
                    last_rebalance = Instant::now();
                    // Rebalancing logic would go here
                }
                
                thread::sleep(Duration::from_secs(5));
            }
        });

        println!("NUMA load balancer monitoring started");
        Ok(())
    }

    /// Stop monitoring
    pub fn stop_monitoring(&self) {
        self.monitoring_active.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get the preferred NUMA node for a thread
    pub fn get_preferred_node_for_thread(&self, thread_id: thread::ThreadId) -> Option<u32> {
        // Check if thread already has an assignment
        {
            let assignments = self.thread_assignments.read();
            if let Some(&node_id) = assignments.get(&thread_id) {
                return Some(node_id);
            }
        }

        // Assign new node based on distribution strategy
        match self.select_optimal_node() {
            Ok(node_id) => {
                // Record the assignment
                {
                    let mut assignments = self.thread_assignments.write();
                    assignments.insert(thread_id, node_id);
                }
                Some(node_id)
            }
            Err(_) => None,
        }
    }

    /// Get the least loaded NUMA node
    pub fn get_least_loaded_node(&self) -> u32 {
        let metrics = self.node_metrics.read();
        
        let mut best_node = 0;
        let mut best_score = f64::MAX;

        for (node_id, node_metrics) in metrics.iter() {
            if node_metrics.load_score < best_score {
                best_score = node_metrics.load_score;
                best_node = *node_id;
            }
        }

        best_node
    }

    /// Select optimal node based on distribution strategy
    pub fn select_optimal_node(&self) -> Result<u32> {
        match &self.distribution {
            WorkloadDistribution::RoundRobin => {
                self.select_round_robin()
            }
            WorkloadDistribution::LeastLoaded => {
                Ok(self.get_least_loaded_node())
            }
            WorkloadDistribution::Weighted => {
                self.select_weighted()
            }
            WorkloadDistribution::LocalityAware => {
                self.select_locality_aware()
            }
            WorkloadDistribution::CustomWeights(weights) => {
                self.select_custom_weighted(weights)
            }
        }
    }

    /// Update node metrics
    pub fn update_node_metrics(&self, node_id: u32, metrics: NodeMetrics) {
        let mut node_metrics = self.node_metrics.write();
        node_metrics.insert(node_id, metrics);
    }

    /// Get current node metrics
    pub fn get_node_metrics(&self, node_id: u32) -> Option<NodeMetrics> {
        let metrics = self.node_metrics.read();
        metrics.get(&node_id).cloned()
    }

    /// Get all node metrics
    pub fn get_all_metrics(&self) -> HashMap<u32, NodeMetrics> {
        self.node_metrics.read().clone()
    }

    /// Calculate load imbalance across nodes
    pub fn calculate_load_imbalance(&self) -> f64 {
        let metrics = self.node_metrics.read();
        
        if metrics.is_empty() {
            return 0.0;
        }

        let scores: Vec<f64> = metrics.values().map(|m| m.load_score).collect();
        let avg_score = scores.iter().sum::<f64>() / scores.len() as f64;
        let max_score = scores.iter().cloned().fold(0.0f64, f64::max);
        
        if avg_score > 0.0 {
            (max_score - avg_score) / avg_score
        } else {
            0.0
        }
    }

    /// Trigger manual rebalancing
    pub fn rebalance(&self) -> Result<()> {
        let imbalance = self.calculate_load_imbalance();
        
        if imbalance > self.rebalance_threshold {
            println!("Rebalancing NUMA load (imbalance: {:.2}%)", imbalance * 100.0);
            
            // In a real implementation, this would migrate threads between nodes
            // For now, we'll just update the assignments
            self.redistribute_threads()?;
        }

        Ok(())
    }

    /// Select node using round-robin
    fn select_round_robin(&self) -> Result<u32> {
        // Simple round-robin based on current assignments
        let assignments = self.thread_assignments.read();
        let node_count = self.topology.get_node_count();
        
        // Count assignments per node
        let mut node_counts = HashMap::new();
        for &node_id in assignments.values() {
            *node_counts.entry(node_id).or_insert(0) += 1;
        }

        // Find node with minimum assignments
        let mut best_node = 0;
        let mut min_count = u32::MAX;

        for node_id in 0..node_count {
            let count = node_counts.get(&node_id).copied().unwrap_or(0);
            if count < min_count {
                min_count = count;
                best_node = node_id;
            }
        }

        Ok(best_node)
    }

    /// Select node using weighted strategy
    fn select_weighted(&self) -> Result<u32> {
        let metrics = self.node_metrics.read();
        
        // Calculate weights based on node capacity
        let mut best_node = 0;
        let mut best_weight = 0.0;

        for node in self.topology.get_nodes() {
            if let Some(node_metrics) = metrics.get(&node.id) {
                // Weight based on available capacity
                let cpu_weight = 1.0 - node_metrics.cpu_utilization;
                let memory_weight = 1.0 - (node_metrics.memory_usage as f64 / node.memory_size_mb as f64 / 1024.0 / 1024.0);
                let combined_weight = (cpu_weight + memory_weight) / 2.0;
                
                if combined_weight > best_weight {
                    best_weight = combined_weight;
                    best_node = node.id;
                }
            }
        }

        Ok(best_node)
    }

    /// Select node using locality-aware strategy
    fn select_locality_aware(&self) -> Result<u32> {
        // This would consider current thread's memory access patterns
        // For now, use least loaded as fallback
        Ok(self.get_least_loaded_node())
    }

    /// Select node using custom weights
    fn select_custom_weighted(&self, weights: &HashMap<u32, f64>) -> Result<u32> {
        let metrics = self.node_metrics.read();
        
        let mut best_node = 0;
        let mut best_score = f64::MIN;

        for (&node_id, &weight) in weights {
            if let Some(node_metrics) = metrics.get(&node_id) {
                // Combine custom weight with current load
                let load_factor = 1.0 - node_metrics.load_score / 100.0; // Normalize load score
                let score = weight * load_factor;
                
                if score > best_score {
                    best_score = score;
                    best_node = node_id;
                }
            }
        }

        Ok(best_node)
    }

    /// Redistribute threads for better balance
    fn redistribute_threads(&self) -> Result<()> {
        let metrics = self.node_metrics.read();
        let mut assignments = self.thread_assignments.write();
        
        // Identify overloaded and underloaded nodes
        let avg_load = metrics.values().map(|m| m.load_score).sum::<f64>() / metrics.len() as f64;
        
        let mut overloaded_nodes = Vec::new();
        let mut underloaded_nodes = Vec::new();
        
        for (&node_id, node_metrics) in metrics.iter() {
            if node_metrics.load_score > avg_load * 1.2 {
                overloaded_nodes.push(node_id);
            } else if node_metrics.load_score < avg_load * 0.8 {
                underloaded_nodes.push(node_id);
            }
        }

        // Move threads from overloaded to underloaded nodes
        let mut moved_count = 0;
        for &overloaded_node in &overloaded_nodes {
            if underloaded_nodes.is_empty() {
                break;
            }
            
            // Find threads on overloaded node
            let threads_to_move: Vec<_> = assignments.iter()
                .filter(|(_, &node_id)| node_id == overloaded_node)
                .map(|(&thread_id, _)| thread_id)
                .take(1) // Move one thread at a time
                .collect();
            
            for thread_id in threads_to_move {
                if let Some(&target_node) = underloaded_nodes.first() {
                    assignments.insert(thread_id, target_node);
                    moved_count += 1;
                    
                    // Remove from underloaded if it's getting busy
                    if moved_count >= 1 {
                        underloaded_nodes.remove(0);
                    }
                }
            }
        }

        if moved_count > 0 {
            println!("Redistributed {} threads for better NUMA balance", moved_count);
        }

        Ok(())
    }

    /// Background thread for updating node metrics
    fn update_node_metrics_background(
        node_metrics: &Arc<RwLock<HashMap<u32, NodeMetrics>>>,
        topology: &Arc<NumaTopology>
    ) {
        let mut metrics = node_metrics.write();
        
        for node in topology.get_nodes() {
            if let Some(node_metric) = metrics.get_mut(&node.id) {
                // Update metrics from system (simplified)
                node_metric.last_update = Instant::now();
                node_metric.cpu_utilization = Self::measure_cpu_utilization(node.id);
                node_metric.memory_usage = Self::measure_memory_usage(node.id);
                node_metric.cache_miss_rate = Self::measure_cache_miss_rate(node.id);
                node_metric.avg_latency_us = Self::measure_avg_latency(node.id);
                
                // Calculate composite load score
                node_metric.load_score = Self::calculate_load_score(node_metric);
            }
        }
    }

    /// Measure CPU utilization for a node (simplified)
    fn measure_cpu_utilization(node_id: u32) -> f64 {
        // In a real implementation, this would read from /proc/stat or similar
        // For now, return a simulated value
        0.3 + (node_id as f64 * 0.1) % 0.5
    }

    /// Measure memory usage for a node (simplified)
    fn measure_memory_usage(node_id: u32) -> u64 {
        // In a real implementation, this would read from /sys/devices/system/node/nodeX/meminfo
        // For now, return a simulated value
        (1024 * 1024 * 1024) + (node_id as u64 * 1024 * 1024 * 100) // ~1GB + variations
    }

    /// Measure cache miss rate for a node (simplified)
    fn measure_cache_miss_rate(node_id: u32) -> f64 {
        // In a real implementation, this would use performance counters
        0.05 + (node_id as f64 * 0.01) % 0.10
    }

    /// Measure average latency for a node (simplified)
    fn measure_avg_latency(node_id: u32) -> f64 {
        // In a real implementation, this would measure actual operation latencies
        10.0 + (node_id as f64 * 2.0) % 20.0
    }

    /// Calculate composite load score
    fn calculate_load_score(metrics: &NodeMetrics) -> f64 {
        // Weighted combination of different metrics
        let cpu_weight = 0.4;
        let memory_weight = 0.3;
        let latency_weight = 0.2;
        let cache_weight = 0.1;

        let cpu_score = metrics.cpu_utilization * 100.0;
        let memory_score = metrics.memory_bandwidth_utilization * 100.0;
        let latency_score = (metrics.avg_latency_us / 100.0).min(100.0); // Normalize to 0-100
        let cache_score = metrics.cache_miss_rate * 100.0;

        cpu_weight * cpu_score +
        memory_weight * memory_score +
        latency_weight * latency_score +
        cache_weight * cache_score
    }
}

impl Default for WorkloadDistribution {
    fn default() -> Self {
        WorkloadDistribution::LeastLoaded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numa::topology::NumaTopology;

    #[test]
    fn test_load_balancer_creation() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let distribution = WorkloadDistribution::LeastLoaded;
        let result = NumaLoadBalancer::new(topology, distribution);
        assert!(result.is_ok());
    }

    #[test]
    fn test_node_selection_strategies() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        
        let strategies = vec![
            WorkloadDistribution::RoundRobin,
            WorkloadDistribution::LeastLoaded,
            WorkloadDistribution::Weighted,
            WorkloadDistribution::LocalityAware,
        ];
        
        for distribution in strategies {
            let balancer = NumaLoadBalancer::new(topology.clone(), distribution).unwrap();
            let result = balancer.select_optimal_node();
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_load_imbalance_calculation() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let balancer = NumaLoadBalancer::new(topology, WorkloadDistribution::LeastLoaded).unwrap();
        
        // Add some test metrics
        for node in balancer.topology.get_nodes() {
            let metrics = NodeMetrics {
                load_score: node.id as f64 * 10.0,
                ..Default::default()
            };
            balancer.update_node_metrics(node.id, metrics);
        }
        
        let imbalance = balancer.calculate_load_imbalance();
        assert!(imbalance >= 0.0);
    }

    #[test]
    fn test_thread_assignment() {
        let topology = Arc::new(NumaTopology::detect().unwrap());
        let balancer = NumaLoadBalancer::new(topology, WorkloadDistribution::RoundRobin).unwrap();
        
        let thread_id = thread::current().id();
        let node = balancer.get_preferred_node_for_thread(thread_id);
        
        assert!(node.is_some());
        
        // Second call should return the same node
        let node2 = balancer.get_preferred_node_for_thread(thread_id);
        assert_eq!(node, node2);
    }
}
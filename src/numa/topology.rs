//! NUMA Topology Detection
//!
//! This module detects and maps the NUMA topology of the system,
//! providing information about NUMA nodes, CPU cores, and memory layout.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

/// NUMA topology information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaTopology {
    /// Number of NUMA nodes
    pub node_count: u32,
    /// NUMA nodes information
    pub nodes: Vec<NumaNode>,
    /// CPU to node mapping
    pub cpu_to_node: HashMap<u32, u32>,
    /// Node distances (latency matrix)
    pub node_distances: HashMap<(u32, u32), f64>,
    /// Total system memory in MB
    pub total_memory_mb: u64,
    /// Memory bandwidth between nodes
    pub memory_bandwidth: HashMap<(u32, u32), f64>,
}

/// NUMA node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaNode {
    /// Node ID
    pub id: u32,
    /// CPUs in this node
    pub cpu_list: Vec<u32>,
    /// Number of CPUs
    pub cpu_count: u32,
    /// Memory size in MB
    pub memory_size_mb: u64,
    /// Free memory in MB
    pub free_memory_mb: u64,
    /// CPU information
    pub cpus: Vec<CpuInfo>,
    /// Memory bandwidth in GB/s
    pub memory_bandwidth_gbps: f64,
    /// Cache sizes per level
    pub cache_sizes: HashMap<u32, u64>,
}

/// CPU core information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuInfo {
    /// CPU ID
    pub id: u32,
    /// Physical CPU ID
    pub physical_id: u32,
    /// Core ID within physical CPU
    pub core_id: u32,
    /// Thread ID within core (for hyperthreading)
    pub thread_id: u32,
    /// CPU frequency in MHz
    pub frequency_mhz: u32,
    /// CPU features/capabilities
    pub features: Vec<String>,
    /// Cache information
    pub cache_info: CacheInfo,
    /// Is this a hyperthread?
    pub is_hyperthread: bool,
}

/// CPU cache information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInfo {
    /// L1 data cache size in KB
    pub l1d_size_kb: u32,
    /// L1 instruction cache size in KB
    pub l1i_size_kb: u32,
    /// L2 cache size in KB
    pub l2_size_kb: u32,
    /// L3 cache size in KB (shared)
    pub l3_size_kb: u32,
    /// Cache line size in bytes
    pub line_size_bytes: u32,
}

impl NumaTopology {
    /// Detect NUMA topology from the system
    pub fn detect() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }

        #[cfg(target_os = "macos")]
        {
            Self::detect_macos()
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Self::detect_fallback()
        }
    }

    /// Detect topology on Linux systems
    #[cfg(target_os = "linux")]
    fn detect_linux() -> Result<Self> {
        let mut nodes = Vec::new();
        let mut cpu_to_node = HashMap::new();
        let mut node_distances = HashMap::new();

        // Check if NUMA is available
        if !Path::new("/sys/devices/system/node").exists() {
            return Self::detect_fallback();
        }

        // Discover NUMA nodes
        let node_dirs = fs::read_dir("/sys/devices/system/node/")?;
        let mut node_ids = Vec::new();

        for entry in node_dirs {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.starts_with("node") {
                if let Ok(node_id) = name_str[4..].parse::<u32>() {
                    node_ids.push(node_id);
                }
            }
        }

        node_ids.sort();

        // Process each NUMA node
        for node_id in &node_ids {
            let node = Self::detect_node_info_linux(*node_id)?;

            // Update CPU to node mapping
            for cpu_id in &node.cpu_list {
                cpu_to_node.insert(*cpu_id, *node_id);
            }

            nodes.push(node);
        }

        // Read node distances
        for &node_from in &node_ids {
            for &node_to in &node_ids {
                let distance_path = format!("/sys/devices/system/node/node{}/distance", node_from);
                if let Ok(distances) = fs::read_to_string(&distance_path) {
                    let distance_values: Vec<&str> = distances.trim().split_whitespace().collect();
                    if let Some(distance_str) = distance_values.get(node_to as usize) {
                        if let Ok(distance) = distance_str.parse::<f64>() {
                            // Convert NUMA distance to relative latency multiplier
                            let latency_multiplier = distance / 10.0;
                            node_distances.insert((node_from, node_to), latency_multiplier);
                        }
                    }
                }
            }
        }

        let total_memory_mb = nodes.iter().map(|n| n.memory_size_mb).sum();

        Ok(NumaTopology {
            node_count: nodes.len() as u32,
            nodes,
            cpu_to_node,
            node_distances,
            total_memory_mb,
            memory_bandwidth: Self::estimate_memory_bandwidth(&node_ids),
        })
    }

    /// Detect NUMA node information on Linux
    #[cfg(target_os = "linux")]
    fn detect_node_info_linux(node_id: u32) -> Result<NumaNode> {
        let node_path = format!("/sys/devices/system/node/node{}", node_id);

        // Read CPU list
        let cpulist_path = format!("{}/cpulist", node_path);
        let cpu_list = if let Ok(cpulist_str) = fs::read_to_string(&cpulist_path) {
            Self::parse_cpu_list(&cpulist_str.trim())?
        } else {
            Vec::new()
        };

        // Read memory information
        let meminfo_path = format!("{}/meminfo", node_path);
        let (memory_size_mb, free_memory_mb) =
            if let Ok(meminfo) = fs::read_to_string(&meminfo_path) {
                Self::parse_node_meminfo(&meminfo)?
            } else {
                (0, 0)
            };

        // Detect CPU information
        let mut cpus = Vec::new();
        for &cpu_id in &cpu_list {
            if let Ok(cpu_info) = Self::detect_cpu_info_linux(cpu_id) {
                cpus.push(cpu_info);
            }
        }

        // Estimate memory bandwidth (simplified)
        let memory_bandwidth_gbps = Self::estimate_node_bandwidth(node_id, &cpus);

        // Collect cache sizes
        let cache_sizes = Self::collect_cache_sizes(&cpus);

        Ok(NumaNode {
            id: node_id,
            cpu_list,
            cpu_count: cpus.len() as u32,
            memory_size_mb,
            free_memory_mb,
            cpus,
            memory_bandwidth_gbps,
            cache_sizes,
        })
    }

    /// Detect CPU information on Linux
    #[cfg(target_os = "linux")]
    fn detect_cpu_info_linux(cpu_id: u32) -> Result<CpuInfo> {
        let cpu_path = format!("/sys/devices/system/cpu/cpu{}", cpu_id);

        // Read topology information
        let topology_path = format!("{}/topology", cpu_path);
        let physical_id = Self::read_topology_value(&topology_path, "physical_package_id")?;
        let core_id = Self::read_topology_value(&topology_path, "core_id")?;
        let thread_siblings = Self::read_topology_value(&topology_path, "thread_siblings_list")
            .unwrap_or_else(|_| cpu_id.to_string());

        // Determine if this is a hyperthread
        let is_hyperthread = thread_siblings.contains(',') || thread_siblings.contains('-');
        let thread_id = if is_hyperthread { 1 } else { 0 };

        // Read CPU frequency
        let freq_path = format!("{}/cpufreq/scaling_cur_freq", cpu_path);
        let frequency_mhz = if let Ok(freq_str) = fs::read_to_string(&freq_path) {
            freq_str.trim().parse::<u32>().unwrap_or(0) / 1000 // Convert kHz to MHz
        } else {
            // Fallback to base frequency
            Self::read_cpu_frequency_fallback(cpu_id).unwrap_or(2400)
        };

        // Read CPU features from /proc/cpuinfo
        let features = Self::read_cpu_features(cpu_id)?;

        // Read cache information
        let cache_info = Self::read_cache_info_linux(cpu_id)?;

        Ok(CpuInfo {
            id: cpu_id,
            physical_id,
            core_id,
            thread_id,
            frequency_mhz,
            features,
            cache_info,
            is_hyperthread,
        })
    }

    /// Read cache information on Linux
    #[cfg(target_os = "linux")]
    fn read_cache_info_linux(cpu_id: u32) -> Result<CacheInfo> {
        let cache_path = format!("/sys/devices/system/cpu/cpu{}/cache", cpu_id);

        let mut l1d_size_kb = 32;
        let mut l1i_size_kb = 32;
        let mut l2_size_kb = 256;
        let mut l3_size_kb = 8192;
        let mut line_size_bytes = 64;

        // Try to read actual cache sizes
        for level in 0..4 {
            let index_path = format!("{}/index{}", cache_path, level);
            if Path::new(&index_path).exists() {
                let level_path = format!("{}/level", index_path);
                let type_path = format!("{}/type", index_path);
                let size_path = format!("{}/size", index_path);
                let linesize_path = format!("{}/coherency_line_size", index_path);

                if let (Ok(level_str), Ok(type_str), Ok(size_str)) = (
                    fs::read_to_string(&level_path),
                    fs::read_to_string(&type_path),
                    fs::read_to_string(&size_path),
                ) {
                    let cache_level = level_str.trim().parse::<u32>().unwrap_or(0);
                    let cache_type = type_str.trim();
                    let cache_size_str = size_str.trim().trim_end_matches('K');
                    let cache_size = cache_size_str.parse::<u32>().unwrap_or(0);

                    match (cache_level, cache_type) {
                        (1, "Data") => l1d_size_kb = cache_size,
                        (1, "Instruction") => l1i_size_kb = cache_size,
                        (2, _) => l2_size_kb = cache_size,
                        (3, _) => l3_size_kb = cache_size,
                        _ => {}
                    }

                    // Read line size
                    if let Ok(linesize_str) = fs::read_to_string(&linesize_path) {
                        if let Ok(linesize) = linesize_str.trim().parse::<u32>() {
                            line_size_bytes = linesize;
                        }
                    }
                }
            }
        }

        Ok(CacheInfo {
            l1d_size_kb,
            l1i_size_kb,
            l2_size_kb,
            l3_size_kb,
            line_size_bytes,
        })
    }

    /// Detect topology on macOS
    #[cfg(target_os = "macos")]
    fn detect_macos() -> Result<Self> {
        use std::process::Command;

        // macOS doesn't have traditional NUMA, but we can detect CPU topology
        let output = Command::new("sysctl")
            .args(&["-n", "hw.ncpu"])
            .output()
            .map_err(|e| Error::Generic(format!("Failed to get CPU count: {}", e)))?;

        let cpu_count = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u32>()
            .unwrap_or(1);

        // Get memory size
        let output = Command::new("sysctl")
            .args(&["-n", "hw.memsize"])
            .output()
            .map_err(|e| Error::Generic(format!("Failed to get memory size: {}", e)))?;

        let memory_bytes = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(8 * 1024 * 1024 * 1024); // Default 8GB

        let memory_mb = memory_bytes / (1024 * 1024);

        // Create a single NUMA node for macOS
        let mut cpus = Vec::new();
        let mut cpu_list = Vec::new();

        for cpu_id in 0..cpu_count {
            cpu_list.push(cpu_id);
            cpus.push(CpuInfo {
                id: cpu_id,
                physical_id: 0,
                core_id: cpu_id,
                thread_id: 0,
                frequency_mhz: Self::get_macos_cpu_frequency().unwrap_or(2400),
                features: Self::get_macos_cpu_features(),
                cache_info: Self::get_macos_cache_info(),
                is_hyperthread: false,
            });
        }

        let node = NumaNode {
            id: 0,
            cpu_list,
            cpu_count,
            memory_size_mb: memory_mb,
            free_memory_mb: memory_mb / 2, // Estimate
            cpus,
            memory_bandwidth_gbps: 50.0, // Estimate for modern Macs
            cache_sizes: HashMap::new(),
        };

        let mut cpu_to_node = HashMap::new();
        for cpu_id in 0..cpu_count {
            cpu_to_node.insert(cpu_id, 0);
        }

        Ok(NumaTopology {
            node_count: 1,
            nodes: vec![node],
            cpu_to_node,
            node_distances: [((0, 0), 1.0)].iter().cloned().collect(),
            total_memory_mb: memory_mb,
            memory_bandwidth: HashMap::new(),
        })
    }

    /// Fallback detection for unsupported systems
    fn detect_fallback() -> Result<Self> {
        // Create a fake single-node topology
        let cpu_count = std::thread::available_parallelism()
            .map(|p| p.get() as u32)
            .unwrap_or(1);

        let mut cpus = Vec::new();
        let mut cpu_list = Vec::new();

        for cpu_id in 0..cpu_count {
            cpu_list.push(cpu_id);
            cpus.push(CpuInfo {
                id: cpu_id,
                physical_id: 0,
                core_id: cpu_id,
                thread_id: 0,
                frequency_mhz: 2400,
                features: vec!["generic".to_string()],
                cache_info: CacheInfo {
                    l1d_size_kb: 32,
                    l1i_size_kb: 32,
                    l2_size_kb: 256,
                    l3_size_kb: 8192,
                    line_size_bytes: 64,
                },
                is_hyperthread: false,
            });
        }

        let node = NumaNode {
            id: 0,
            cpu_list,
            cpu_count,
            memory_size_mb: 8192, // 8GB default
            free_memory_mb: 4096,
            cpus,
            memory_bandwidth_gbps: 25.0,
            cache_sizes: HashMap::new(),
        };

        let mut cpu_to_node = HashMap::new();
        for cpu_id in 0..cpu_count {
            cpu_to_node.insert(cpu_id, 0);
        }

        Ok(NumaTopology {
            node_count: 1,
            nodes: vec![node],
            cpu_to_node,
            node_distances: [((0, 0), 1.0)].iter().cloned().collect(),
            total_memory_mb: 8192,
            memory_bandwidth: HashMap::new(),
        })
    }

    /// Parse CPU list string (e.g., "0-3,8-11")
    fn parse_cpu_list(cpulist: &str) -> Result<Vec<u32>> {
        let mut cpu_list = Vec::new();

        for range in cpulist.split(',') {
            if range.contains('-') {
                let parts: Vec<&str> = range.split('-').collect();
                if parts.len() == 2 {
                    let start = parts[0]
                        .parse::<u32>()
                        .map_err(|e| Error::ParseError(format!("Invalid CPU range: {}", e)))?;
                    let end = parts[1]
                        .parse::<u32>()
                        .map_err(|e| Error::ParseError(format!("Invalid CPU range: {}", e)))?;

                    for cpu_id in start..=end {
                        cpu_list.push(cpu_id);
                    }
                }
            } else {
                let cpu_id = range
                    .parse::<u32>()
                    .map_err(|e| Error::ParseError(format!("Invalid CPU ID: {}", e)))?;
                cpu_list.push(cpu_id);
            }
        }

        Ok(cpu_list)
    }

    /// Parse node memory information
    fn parse_node_meminfo(meminfo: &str) -> Result<(u64, u64)> {
        let mut total_kb = 0u64;
        let mut free_kb = 0u64;

        for line in meminfo.lines() {
            if line.starts_with("Node") && line.contains("MemTotal:") {
                if let Some(value) = line.split_whitespace().nth(2) {
                    total_kb = value.parse().unwrap_or(0);
                }
            } else if line.starts_with("Node") && line.contains("MemFree:") {
                if let Some(value) = line.split_whitespace().nth(2) {
                    free_kb = value.parse().unwrap_or(0);
                }
            }
        }

        Ok((total_kb / 1024, free_kb / 1024)) // Convert to MB
    }

    /// Helper methods for Linux topology detection
    #[cfg(target_os = "linux")]
    fn read_topology_value(topology_path: &str, filename: &str) -> Result<u32> {
        let path = format!("{}/{}", topology_path, filename);
        let content = fs::read_to_string(&path)
            .map_err(|e| Error::Generic(format!("Failed to read {}: {}", path, e)))?;

        content
            .trim()
            .parse::<u32>()
            .map_err(|e| Error::ParseError(format!("Invalid topology value: {}", e)))
    }

    /// Read CPU frequency fallback
    fn read_cpu_frequency_fallback(cpu_id: u32) -> Result<u32> {
        // Try to read from /proc/cpuinfo
        if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
            for line in cpuinfo.lines() {
                if line.starts_with("cpu MHz") {
                    if let Some(value) = line.split(':').nth(1) {
                        if let Ok(freq) = value.trim().parse::<f32>() {
                            return Ok(freq as u32);
                        }
                    }
                }
            }
        }

        Err(Error::Generic(
            "Could not determine CPU frequency".to_string(),
        ))
    }

    /// Read CPU features
    fn read_cpu_features(cpu_id: u32) -> Result<Vec<String>> {
        // This is a simplified version - real implementation would parse /proc/cpuinfo
        Ok(vec![
            "sse".to_string(),
            "sse2".to_string(),
            "sse4_1".to_string(),
            "sse4_2".to_string(),
            "avx".to_string(),
            "avx2".to_string(),
        ])
    }

    /// Estimate memory bandwidth between nodes
    fn estimate_memory_bandwidth(node_ids: &[u32]) -> HashMap<(u32, u32), f64> {
        let mut bandwidth = HashMap::new();

        for &node_from in node_ids {
            for &node_to in node_ids {
                let bw = if node_from == node_to {
                    100.0 // Local memory bandwidth in GB/s
                } else {
                    25.0 // Inter-node bandwidth in GB/s
                };
                bandwidth.insert((node_from, node_to), bw);
            }
        }

        bandwidth
    }

    /// Estimate node memory bandwidth
    fn estimate_node_bandwidth(node_id: u32, cpus: &[CpuInfo]) -> f64 {
        // Estimate based on CPU count and type
        let base_bandwidth = 25.0; // GB/s per memory channel
        let memory_channels = (cpus.len() / 4).max(1) as f64; // Estimate memory channels

        base_bandwidth * memory_channels
    }

    /// Collect cache sizes from CPUs
    fn collect_cache_sizes(cpus: &[CpuInfo]) -> HashMap<u32, u64> {
        let mut cache_sizes = HashMap::new();

        if let Some(cpu) = cpus.first() {
            cache_sizes.insert(1, cpu.cache_info.l1d_size_kb as u64 * 1024);
            cache_sizes.insert(2, cpu.cache_info.l2_size_kb as u64 * 1024);
            cache_sizes.insert(3, cpu.cache_info.l3_size_kb as u64 * 1024);
        }

        cache_sizes
    }

    /// Get macOS CPU frequency
    #[cfg(target_os = "macos")]
    fn get_macos_cpu_frequency() -> Result<u32> {
        use std::process::Command;

        let output = Command::new("sysctl")
            .args(&["-n", "hw.cpufrequency"])
            .output()
            .map_err(|e| Error::Generic(format!("Failed to get CPU frequency: {}", e)))?;

        let freq_hz = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(2400000000); // Default 2.4GHz

        Ok((freq_hz / 1000000) as u32) // Convert to MHz
    }

    /// Get macOS CPU features
    #[cfg(target_os = "macos")]
    fn get_macos_cpu_features() -> Vec<String> {
        vec![
            "sse".to_string(),
            "sse2".to_string(),
            "sse4_1".to_string(),
            "sse4_2".to_string(),
            "avx".to_string(),
            "avx2".to_string(),
        ]
    }

    /// Get macOS cache information
    #[cfg(target_os = "macos")]
    fn get_macos_cache_info() -> CacheInfo {
        // Try to get cache sizes from sysctl
        let l1d_size = Self::get_sysctl_value("hw.l1dcachesize").unwrap_or(32768) / 1024;
        let l1i_size = Self::get_sysctl_value("hw.l1icachesize").unwrap_or(32768) / 1024;
        let l2_size = Self::get_sysctl_value("hw.l2cachesize").unwrap_or(262144) / 1024;
        let l3_size = Self::get_sysctl_value("hw.l3cachesize").unwrap_or(8388608) / 1024;

        CacheInfo {
            l1d_size_kb: l1d_size as u32,
            l1i_size_kb: l1i_size as u32,
            l2_size_kb: l2_size as u32,
            l3_size_kb: l3_size as u32,
            line_size_bytes: 64,
        }
    }

    /// Get sysctl value on macOS
    #[cfg(target_os = "macos")]
    fn get_sysctl_value(key: &str) -> Option<u64> {
        use std::process::Command;

        let output = Command::new("sysctl").args(&["-n", key]).output().ok()?;

        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .ok()
    }

    /// Get all NUMA nodes
    pub fn get_nodes(&self) -> &[NumaNode] {
        &self.nodes
    }

    /// Get number of NUMA nodes
    pub fn get_node_count(&self) -> u32 {
        self.node_count
    }

    /// Get NUMA node for a CPU
    pub fn get_node_for_cpu(&self, cpu_id: u32) -> Option<u32> {
        self.cpu_to_node.get(&cpu_id).copied()
    }

    /// Get distance between two nodes
    pub fn get_node_distance(&self, from: u32, to: u32) -> f64 {
        self.node_distances.get(&(from, to)).copied().unwrap_or(1.0)
    }

    /// Get memory bandwidth between nodes
    pub fn get_memory_bandwidth(&self, from: u32, to: u32) -> f64 {
        self.memory_bandwidth
            .get(&(from, to))
            .copied()
            .unwrap_or(25.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_detection() {
        let result = NumaTopology::detect();
        assert!(result.is_ok());

        let topology = result.unwrap();
        assert!(topology.node_count > 0);
        assert!(!topology.nodes.is_empty());
    }

    #[test]
    fn test_cpu_list_parsing() {
        let result = NumaTopology::parse_cpu_list("0-3,8-11");
        assert!(result.is_ok());

        let cpu_list = result.unwrap();
        assert_eq!(cpu_list, vec![0, 1, 2, 3, 8, 9, 10, 11]);
    }

    #[test]
    fn test_single_cpu_parsing() {
        let result = NumaTopology::parse_cpu_list("5");
        assert!(result.is_ok());

        let cpu_list = result.unwrap();
        assert_eq!(cpu_list, vec![5]);
    }
}

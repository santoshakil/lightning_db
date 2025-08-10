//! Workload-Specific Configuration Profiles
//!
//! This module provides optimized configuration profiles for different database workloads:
//! - OLTP (Online Transaction Processing)
//! - OLAP (Online Analytical Processing)
//! - Mixed workloads
//! - Custom profiles based on workload analysis

use crate::performance_tuning::hardware_detector::{
    CacheSizes, HardwareInfo, PowerProfile, PowerSource, StorageType, ThermalState,
};
use crate::performance_tuning::workload_profiler::{WorkloadProfile, WorkloadType};
use crate::LightningDbConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Workload-specific configuration profiles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadProfiles {
    /// OLTP configuration profile
    pub oltp: WorkloadConfigProfile,
    /// OLAP configuration profile
    pub olap: WorkloadConfigProfile,
    /// Mixed workload configuration profile
    pub mixed: WorkloadConfigProfile,
    /// Custom profiles by name
    pub custom: HashMap<String, WorkloadConfigProfile>,
}

/// Configuration profile for a specific workload type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadConfigProfile {
    /// Profile name and description
    pub name: String,
    pub description: String,

    /// Cache configuration
    pub cache_size_mb: u64,
    pub cache_type: CacheType,
    pub cache_eviction_strategy: EvictionStrategy,

    /// B+Tree configuration
    pub btree_page_size: u32,
    pub btree_node_size: u32,
    pub btree_split_threshold: f64,

    /// Write buffer configuration
    pub write_buffer_size_mb: u64,
    pub write_buffer_flush_threshold: f64,
    pub batch_write_size: u32,

    /// Compression settings
    pub compression_enabled: bool,
    pub compression_algorithm: CompressionAlgorithm,
    pub compression_level: u8,

    /// Consistency settings
    pub consistency_level: ConsistencyLevel,
    pub sync_writes: bool,
    pub checkpoint_interval_ms: u64,

    /// Concurrency settings
    pub max_concurrent_transactions: u32,
    pub thread_pool_size: u32,
    pub lock_timeout_ms: u64,

    /// I/O settings
    pub io_queue_depth: u32,
    pub prefetch_size: u32,
    pub use_direct_io: bool,

    /// Memory allocation settings
    pub use_huge_pages: bool,
    pub numa_awareness: bool,
    pub memory_allocator: MemoryAllocator,
}

/// Cache type configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacheType {
    /// ARC (Adaptive Replacement Cache)
    ARC,
    /// LRU (Least Recently Used)
    LRU,
    /// Clock-Pro algorithm
    ClockPro,
    /// W-TinyLFU algorithm
    WTinyLFU,
}

/// Cache eviction strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionStrategy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// Time-based expiration
    TTL,
    /// Adaptive based on workload
    Adaptive,
}

/// Compression algorithm options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 - fast compression
    LZ4,
    /// Snappy - moderate compression
    Snappy,
    /// Zstd - high compression
    Zstd,
}

/// Consistency level options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Eventual consistency
    Eventual,
    /// Read committed
    ReadCommitted,
    /// Serializable
    Serializable,
    /// Linearizable
    Linearizable,
}

/// Memory allocator options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryAllocator {
    /// System default
    System,
    /// jemalloc
    Jemalloc,
    /// tcmalloc
    Tcmalloc,
    /// mimalloc
    Mimalloc,
}

/// Workload profile generator
pub struct WorkloadProfileGenerator {
    hardware_info: HardwareInfo,
}

impl WorkloadProfileGenerator {
    /// Create a new workload profile generator
    pub fn new(hardware_info: HardwareInfo) -> Self {
        Self { hardware_info }
    }

    /// Generate default workload profiles
    pub fn generate_default_profiles(&self) -> WorkloadProfiles {
        WorkloadProfiles {
            oltp: self.generate_oltp_profile(),
            olap: self.generate_olap_profile(),
            mixed: self.generate_mixed_profile(),
            custom: HashMap::new(),
        }
    }

    /// Generate OLTP (Online Transaction Processing) profile
    /// Optimized for: High concurrency, low latency, frequent small reads/writes
    pub fn generate_oltp_profile(&self) -> WorkloadConfigProfile {
        let memory_mb = self.hardware_info.total_memory_mb;

        WorkloadConfigProfile {
            name: "OLTP".to_string(),
            description: "Optimized for high-concurrency, low-latency transactional workloads"
                .to_string(),

            // Cache: Moderate size, optimized for frequent access patterns
            cache_size_mb: (memory_mb / 4).min(2048).max(128), // 25% of memory, 128MB-2GB
            cache_type: CacheType::ARC,                        // Adaptive for mixed patterns
            cache_eviction_strategy: EvictionStrategy::Adaptive,

            // B+Tree: Small pages for better concurrency
            btree_page_size: 4096,       // 4KB pages
            btree_node_size: 512,        // Smaller nodes for better cache locality
            btree_split_threshold: 0.75, // Split earlier to reduce lock contention

            // Write buffer: Small, frequent flushes
            write_buffer_size_mb: 64.min(memory_mb / 16), // Small buffer
            write_buffer_flush_threshold: 0.7,            // Flush more frequently
            batch_write_size: 100,                        // Small batches for low latency

            // Compression: Fast compression to reduce I/O
            compression_enabled: true,
            compression_algorithm: CompressionAlgorithm::LZ4,
            compression_level: 1, // Fastest compression

            // Consistency: Strong consistency for ACID compliance
            consistency_level: ConsistencyLevel::Serializable,
            sync_writes: true,            // Durability is critical
            checkpoint_interval_ms: 5000, // Frequent checkpoints

            // Concurrency: High concurrency settings
            max_concurrent_transactions: (self.hardware_info.cpu_count * 16) as u32,
            thread_pool_size: (self.hardware_info.cpu_count * 2) as u32,
            lock_timeout_ms: 100, // Short timeouts to detect deadlocks quickly

            // I/O: Low latency settings
            io_queue_depth: 32,
            prefetch_size: 16,    // Small prefetch for random access
            use_direct_io: false, // Let OS cache handle small I/O

            // Memory: Optimize for low latency
            use_huge_pages: true,
            numa_awareness: true,
            memory_allocator: MemoryAllocator::Jemalloc,
        }
    }

    /// Generate OLAP (Online Analytical Processing) profile
    /// Optimized for: Large scans, complex queries, read-heavy workloads
    pub fn generate_olap_profile(&self) -> WorkloadConfigProfile {
        let memory_mb = self.hardware_info.total_memory_mb;

        WorkloadConfigProfile {
            name: "OLAP".to_string(),
            description: "Optimized for analytical queries and large data scans".to_string(),

            // Cache: Large cache for data retention
            cache_size_mb: (memory_mb / 2).min(8192).max(512), // 50% of memory, 512MB-8GB
            cache_type: CacheType::WTinyLFU,                   // Better for scan resistance
            cache_eviction_strategy: EvictionStrategy::LFU,    // Keep frequently accessed data

            // B+Tree: Large pages for better scan performance
            btree_page_size: 16384,     // 16KB pages for better sequential I/O
            btree_node_size: 2048,      // Larger nodes for better scan performance
            btree_split_threshold: 0.9, // Split later to maximize space utilization

            // Write buffer: Large buffers for batch processing
            write_buffer_size_mb: 256.min(memory_mb / 8), // Larger buffer
            write_buffer_flush_threshold: 0.9,            // Flush less frequently
            batch_write_size: 1000,                       // Large batches for efficiency

            // Compression: High compression to save storage
            compression_enabled: true,
            compression_algorithm: CompressionAlgorithm::Zstd,
            compression_level: 6, // Balanced compression

            // Consistency: Relaxed consistency for read performance
            consistency_level: ConsistencyLevel::ReadCommitted,
            sync_writes: false, // Prioritize performance over durability
            checkpoint_interval_ms: 30000, // Less frequent checkpoints

            // Concurrency: Moderate concurrency, focus on throughput
            max_concurrent_transactions: (self.hardware_info.cpu_count * 4) as u32,
            thread_pool_size: self.hardware_info.cpu_count as u32,
            lock_timeout_ms: 1000, // Longer timeouts for complex queries

            // I/O: High throughput settings
            io_queue_depth: 128,
            prefetch_size: 256,  // Large prefetch for sequential access
            use_direct_io: true, // Bypass OS cache for large I/O

            // Memory: Optimize for throughput
            use_huge_pages: true,
            numa_awareness: true,
            memory_allocator: MemoryAllocator::Tcmalloc,
        }
    }

    /// Generate Mixed workload profile
    /// Balanced between OLTP and OLAP characteristics
    pub fn generate_mixed_profile(&self) -> WorkloadConfigProfile {
        let memory_mb = self.hardware_info.total_memory_mb;

        WorkloadConfigProfile {
            name: "Mixed".to_string(),
            description: "Balanced profile for mixed transactional and analytical workloads"
                .to_string(),

            // Cache: Balanced size and strategy
            cache_size_mb: (memory_mb / 3).min(4096).max(256), // 33% of memory
            cache_type: CacheType::ClockPro,                   // Good balance for mixed patterns
            cache_eviction_strategy: EvictionStrategy::Adaptive,

            // B+Tree: Medium pages
            btree_page_size: 8192,      // 8KB pages
            btree_node_size: 1024,      // Medium nodes
            btree_split_threshold: 0.8, // Balanced split threshold

            // Write buffer: Medium size
            write_buffer_size_mb: 128.min(memory_mb / 12), // Medium buffer
            write_buffer_flush_threshold: 0.8,             // Balanced flush threshold
            batch_write_size: 500,                         // Medium batches

            // Compression: Balanced compression
            compression_enabled: true,
            compression_algorithm: CompressionAlgorithm::Snappy,
            compression_level: 3, // Balanced compression

            // Consistency: Balanced consistency
            consistency_level: ConsistencyLevel::ReadCommitted,
            sync_writes: true,             // Moderate durability
            checkpoint_interval_ms: 15000, // Balanced checkpoint frequency

            // Concurrency: Balanced settings
            max_concurrent_transactions: (self.hardware_info.cpu_count * 8) as u32,
            thread_pool_size: ((self.hardware_info.cpu_count * 3) / 2) as u32,
            lock_timeout_ms: 500, // Balanced timeout

            // I/O: Balanced settings
            io_queue_depth: 64,
            prefetch_size: 64,    // Medium prefetch
            use_direct_io: false, // Let OS cache decide

            // Memory: Balanced allocation
            use_huge_pages: true,
            numa_awareness: true,
            memory_allocator: MemoryAllocator::Jemalloc,
        }
    }

    /// Generate custom profile based on workload analysis
    pub fn generate_custom_profile(
        &self,
        name: String,
        workload_profile: &WorkloadProfile,
    ) -> WorkloadConfigProfile {
        let memory_mb = self.hardware_info.total_memory_mb;

        // Analyze workload characteristics
        let read_ratio = workload_profile.read_ratio;
        let write_ratio = 1.0 - read_ratio;
        let is_read_heavy = read_ratio > 0.8;
        let is_write_heavy = write_ratio > 0.6;
        let is_scan_heavy = workload_profile.access_patterns.operation_mix.range_scans > 0.3;

        // Base configuration on workload type
        let base_config = match workload_profile.workload_type {
            WorkloadType::OLTP => self.generate_oltp_profile(),
            WorkloadType::OLAP => self.generate_olap_profile(),
            _ => self.generate_mixed_profile(),
        };

        // Customize based on specific characteristics
        let mut custom_config = base_config;
        custom_config.name = name;
        custom_config.description = format!(
            "Custom profile: {:.1}% reads, {:.1}% writes, range scans: {:.1}%",
            read_ratio * 100.0,
            write_ratio * 100.0,
            workload_profile.access_patterns.operation_mix.range_scans * 100.0
        );

        // Adjust cache size based on read patterns
        if is_read_heavy {
            custom_config.cache_size_mb = (memory_mb / 2).min(6144).max(512);
            custom_config.cache_type = CacheType::WTinyLFU;
        }

        // Adjust write buffer based on write patterns
        if is_write_heavy {
            custom_config.write_buffer_size_mb = 256.min(memory_mb / 6);
            custom_config.batch_write_size = 2000;
        }

        // Adjust for scan-heavy workloads
        if is_scan_heavy {
            custom_config.btree_page_size = 16384;
            custom_config.prefetch_size = 512;
            custom_config.use_direct_io = true;
        }

        // Adjust compression based on data characteristics
        if workload_profile.key_size_avg > 100 || workload_profile.value_size_avg > 1000 {
            custom_config.compression_algorithm = CompressionAlgorithm::Zstd;
            custom_config.compression_level = 4;
        }

        custom_config
    }

    /// Apply workload profile to Lightning DB configuration
    pub fn apply_profile_to_config(
        profile: &WorkloadConfigProfile,
        mut config: LightningDbConfig,
    ) -> LightningDbConfig {
        // Apply cache settings
        config.cache_size = profile.cache_size_mb * 1024 * 1024; // Convert to bytes

        // Apply consistency settings based on profile
        config.consistency_config.enable_read_repair = profile.sync_writes;

        // Apply compression settings
        config.compression_enabled = profile.compression_enabled;

        // Apply other settings based on profile
        config.max_active_transactions = profile.max_concurrent_transactions as usize;
        config.prefetch_enabled = profile.prefetch_size > 0;
        config.prefetch_distance = profile.prefetch_size as usize;

        // Map compression algorithm to compression_type
        config.compression_type = match profile.compression_algorithm {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::LZ4 => 2,
            CompressionAlgorithm::Snappy => 3,
            CompressionAlgorithm::Zstd => 1,
        };

        config
    }
}

impl Default for WorkloadProfiles {
    fn default() -> Self {
        // Create with default hardware info
        let default_hardware = HardwareInfo {
            cpu_count: 4,
            physical_cores: 4,
            cpu_arch: "Unknown".to_string(),
            cpu_model: "Unknown CPU".to_string(),
            cpu_base_freq_mhz: None,
            cpu_features: vec![],
            total_memory_mb: 8192,     // 8GB default
            available_memory_mb: 4096, // 4GB available
            memory_bandwidth_mb_per_sec: None,
            cache_sizes: CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            },
            storage_devices: vec![],
            storage_type: StorageType::SSD,
            storage_bandwidth_mb_per_sec: None,
            numa_nodes: 1,
            huge_pages_available: false,
            gpu_devices: vec![],
            network_interfaces: vec![],
            thermal_state: ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: PowerProfile {
                power_source: PowerSource::AC,
                cpu_governor: None,
                max_performance_available: true,
            },
        };

        let generator = WorkloadProfileGenerator::new(default_hardware);
        generator.generate_default_profiles()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::performance_tuning::workload_profiler::{
        AccessPatterns, BatchCharacteristics, KeyDistribution, OperationMix,
    };

    #[test]
    fn test_profile_generation() {
        let hardware = HardwareInfo {
            cpu_count: 8,
            physical_cores: 4,
            cpu_arch: "Test CPU".to_string(),
            cpu_model: "Intel Core i7".to_string(),
            cpu_base_freq_mhz: Some(2400),
            cpu_features: vec!["sse4.2".to_string(), "avx2".to_string()],
            total_memory_mb: 16384,    // 16GB
            available_memory_mb: 8192, // 8GB available
            memory_bandwidth_mb_per_sec: Some(25600.0),
            cache_sizes: CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            },
            storage_devices: vec![],
            storage_type: StorageType::SSD,
            storage_bandwidth_mb_per_sec: Some(500.0),
            numa_nodes: 1,
            huge_pages_available: false,
            gpu_devices: vec![],
            network_interfaces: vec![],
            thermal_state: ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: PowerProfile {
                power_source: PowerSource::AC,
                cpu_governor: Some("performance".to_string()),
                max_performance_available: true,
            },
        };

        let generator = WorkloadProfileGenerator::new(hardware);
        let profiles = generator.generate_default_profiles();

        // Test OLTP profile characteristics
        assert_eq!(profiles.oltp.name, "OLTP");
        assert!(profiles.oltp.cache_size_mb >= 128);
        assert!(profiles.oltp.cache_size_mb <= 2048);
        assert_eq!(profiles.oltp.btree_page_size, 4096);
        assert!(profiles.oltp.sync_writes);

        // Test OLAP profile characteristics
        assert_eq!(profiles.olap.name, "OLAP");
        assert!(profiles.olap.cache_size_mb >= 512);
        assert_eq!(profiles.olap.btree_page_size, 16384);
        assert!(!profiles.olap.sync_writes);

        // Test Mixed profile characteristics
        assert_eq!(profiles.mixed.name, "Mixed");
        assert_eq!(profiles.mixed.btree_page_size, 8192);
        assert!(profiles.mixed.sync_writes);
    }

    #[test]
    fn test_custom_profile_generation() {
        let hardware = HardwareInfo {
            cpu_count: 4,
            physical_cores: 2,
            cpu_arch: "Test CPU".to_string(),
            cpu_model: "Intel Core i5".to_string(),
            cpu_base_freq_mhz: Some(2000),
            cpu_features: vec![],
            total_memory_mb: 8192,
            available_memory_mb: 4096,
            memory_bandwidth_mb_per_sec: Some(19200.0),
            cache_sizes: CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            },
            storage_devices: vec![],
            storage_type: StorageType::SSD,
            storage_bandwidth_mb_per_sec: None,
            numa_nodes: 1,
            huge_pages_available: false,
            gpu_devices: vec![],
            network_interfaces: vec![],
            thermal_state: ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: PowerProfile {
                power_source: PowerSource::AC,
                cpu_governor: Some("performance".to_string()),
                max_performance_available: true,
            },
        };

        let generator = WorkloadProfileGenerator::new(hardware);

        // Test read-heavy workload
        let read_heavy_profile = WorkloadProfile {
            workload_type: WorkloadType::OLAP,
            read_ratio: 0.9,
            write_ratio: 0.1,
            key_size_avg: 32,
            value_size_avg: 256,
            hot_key_percentage: 0.1,
            sequential_access_ratio: 0.8,
            transaction_size_avg: 1,
            concurrent_operations_avg: 10.0,
            peak_ops_per_sec: 10000.0,
            access_patterns: AccessPatterns {
                temporal_locality: 0.7,
                spatial_locality: 0.8,
                key_distribution: KeyDistribution::Sequential,
                operation_mix: OperationMix {
                    point_reads: 0.2,
                    range_scans: 0.7,
                    inserts: 0.05,
                    updates: 0.04,
                    deletes: 0.01,
                },
                batch_characteristics: BatchCharacteristics {
                    avg_batch_size: 100,
                    max_batch_size: 1000,
                    batch_frequency: 0.3,
                },
            },
        };

        let custom =
            generator.generate_custom_profile("ReadHeavy".to_string(), &read_heavy_profile);

        assert_eq!(custom.name, "ReadHeavy");
        assert!(custom.description.contains("90.0% reads"));
        assert!(custom.cache_size_mb >= 512);
    }

    #[test]
    fn test_config_application() {
        let hardware = HardwareInfo {
            cpu_count: 4,
            physical_cores: 2,
            cpu_arch: "Test CPU".to_string(),
            cpu_model: "Intel Core i5".to_string(),
            cpu_base_freq_mhz: Some(2000),
            cpu_features: vec![],
            total_memory_mb: 8192,
            available_memory_mb: 4096,
            memory_bandwidth_mb_per_sec: Some(19200.0),
            cache_sizes: CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            },
            storage_devices: vec![],
            storage_type: StorageType::SSD,
            storage_bandwidth_mb_per_sec: None,
            numa_nodes: 1,
            huge_pages_available: false,
            gpu_devices: vec![],
            network_interfaces: vec![],
            thermal_state: ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: PowerProfile {
                power_source: PowerSource::AC,
                cpu_governor: Some("performance".to_string()),
                max_performance_available: true,
            },
        };

        let generator = WorkloadProfileGenerator::new(hardware);
        let oltp_profile = generator.generate_oltp_profile();

        let base_config = LightningDbConfig::default();
        let optimized_config =
            WorkloadProfileGenerator::apply_profile_to_config(&oltp_profile, base_config);

        assert!(optimized_config.compression_enabled);
        assert!(optimized_config.cache_size > 0);
    }
}

//! Configuration Optimizer
//!
//! Generates optimal Lightning DB configurations based on hardware capabilities,
//! workload characteristics, and optimization goals.

use super::{HardwareInfo, WorkloadProfile, WorkloadType};
use crate::{LightningDbConfig, Result, WalSyncMode};
use serde::{Deserialize, Serialize};

/// Configuration optimizer
pub struct ConfigOptimizer {
    optimization_goal: OptimizationGoal,
    constraints: OptimizationConstraints,
}

/// Optimization goal
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OptimizationGoal {
    Throughput,        // Maximize operations per second
    Latency,           // Minimize response time
    Balanced,          // Balance throughput and latency
    ResourceEfficient, // Minimize resource usage
    CostOptimized,     // Optimize for cloud/cost efficiency
}

/// Optimization constraints
#[derive(Debug, Clone)]
pub struct OptimizationConstraints {
    pub max_memory_mb: Option<u64>,
    pub max_cpu_percent: Option<f64>,
    pub max_disk_mb: Option<u64>,
    pub required_durability: DurabilityLevel,
    pub required_consistency: ConsistencyLevel,
}

/// Durability requirements
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DurabilityLevel {
    BestEffort, // May lose recent writes on crash
    Standard,   // Standard durability with WAL
    High,       // Synchronous WAL writes
    Maximum,    // Sync to disk on every operation
}

/// Consistency requirements
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsistencyLevel {
    Eventual,      // Allow temporary inconsistencies
    ReadYourWrite, // Session consistency
    Strong,        // Full ACID consistency
}

impl ConfigOptimizer {
    /// Create a new configuration optimizer
    pub fn new(goal: OptimizationGoal) -> Self {
        Self {
            optimization_goal: goal,
            constraints: OptimizationConstraints::default(),
        }
    }

    /// Set optimization constraints
    pub fn with_constraints(mut self, constraints: OptimizationConstraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Generate optimized configuration
    pub fn optimize(
        &self,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
        base_config: &LightningDbConfig,
    ) -> Result<LightningDbConfig> {
        let mut config = base_config.clone();

        // Apply hardware-based optimizations
        self.apply_hardware_optimizations(&mut config, hardware);

        // Apply workload-based optimizations
        self.apply_workload_optimizations(&mut config, workload);

        // Apply goal-specific optimizations
        self.apply_goal_optimizations(&mut config, hardware, workload);

        // Apply constraints
        self.apply_constraints(&mut config);

        // Validate configuration
        self.validate_config(&config, hardware)?;

        Ok(config)
    }

    /// Apply hardware-based optimizations
    fn apply_hardware_optimizations(
        &self,
        config: &mut LightningDbConfig,
        hardware: &HardwareInfo,
    ) {
        // Cache size: Use 25-50% of available memory depending on workload
        let cache_percentage = match self.optimization_goal {
            OptimizationGoal::Throughput => 0.5,
            OptimizationGoal::Latency => 0.4,
            OptimizationGoal::ResourceEfficient => 0.15,
            _ => 0.25,
        };

        config.cache_size =
            (hardware.available_memory_mb as f64 * cache_percentage * 1024.0 * 1024.0) as u64;

        // CPU optimizations
        if hardware.cpu_count >= 8 {
            config.prefetch_enabled = true;
            config.prefetch_workers = (hardware.cpu_count / 4).clamp(2, 8);
            config.use_optimized_transactions = true;
            config.use_optimized_page_manager = true;
        } else if hardware.cpu_count >= 4 {
            config.prefetch_enabled = true;
            config.prefetch_workers = 2;
        }

        // Storage optimizations
        match hardware.storage_type {
            super::hardware_detector::StorageType::NVME => {
                config.mmap_size = Some(config.cache_size * 2);
                config.prefetch_distance = 64;
            }
            super::hardware_detector::StorageType::SSD => {
                config.mmap_size = Some(config.cache_size);
                config.prefetch_distance = 32;
            }
            super::hardware_detector::StorageType::HDD => {
                config.compression_enabled = false; // Reduce CPU overhead
                config.prefetch_distance = 128; // Larger sequential reads
                config.write_batch_size = 10000; // Batch more writes
            }
            _ => {}
        }

        // Memory optimizations
        if hardware.huge_pages_available {
            // Enable huge pages support when implemented
        }

        // NUMA optimizations
        if hardware.numa_nodes > 1 {
            // Enable NUMA-aware allocation when implemented
        }

        // Cache line optimizations
        config.page_size = if hardware.cache_sizes.cache_line_size == 128 {
            8192 // Use larger pages for 128-byte cache lines
        } else {
            4096 // Standard 4KB pages
        };
    }

    /// Apply workload-based optimizations
    fn apply_workload_optimizations(
        &self,
        config: &mut LightningDbConfig,
        workload: &WorkloadProfile,
    ) {
        match workload.workload_type {
            WorkloadType::OLTP => {
                config.compression_enabled = false; // Low latency
                config.write_batch_size = 100;
                config.wal_sync_mode = WalSyncMode::Async;
                config.max_active_transactions = 10000;
                config.use_improved_wal = true;
            }
            WorkloadType::OLAP => {
                config.compression_enabled = true;
                config.compression_type = 1; // ZSTD for better compression
                config.write_batch_size = 10000;
                config.prefetch_distance = 128;
                config.wal_sync_mode = WalSyncMode::Async;
            }
            WorkloadType::WriteHeavy => {
                config.cache_size = config.cache_size / 2; // More memory for write buffers
                config.write_batch_size = 10000;
                config.compression_enabled = false;
                config.wal_sync_mode = WalSyncMode::Async;
                config.use_improved_wal = true;
            }
            WorkloadType::ReadHeavy => {
                config.cache_size = config.cache_size * 2; // Maximize cache
                config.prefetch_enabled = true;
                config.prefetch_distance = 64;
                config.compression_enabled = true;
                config.compression_type = 2; // LZ4 for fast decompression
            }
            WorkloadType::TimeSeries => {
                config.compression_enabled = true;
                config.compression_type = 1; // ZSTD for time-series data
                config.write_batch_size = 5000;
                config.prefetch_distance = 32;
            }
            _ => {} // Use defaults for other types
        }

        // Sequential access optimizations
        if workload.sequential_access_ratio > 0.7 {
            config.prefetch_enabled = true;
            config.prefetch_distance = config.prefetch_distance * 2;
        }

        // Hot key optimizations
        if workload.hot_key_percentage > 0.2 {
            // Increase cache size for hot key workloads
            config.cache_size = (config.cache_size as f64 * 1.5) as u64;
        }

        // Transaction size optimizations
        if workload.transaction_size_avg > 10 {
            config.max_active_transactions = config.max_active_transactions / 2;
        }

        // Concurrent access optimizations
        if workload.concurrent_operations_avg > 100.0 {
            config.use_optimized_transactions = true;
            config.use_optimized_page_manager = true;
        }
    }

    /// Apply goal-specific optimizations
    fn apply_goal_optimizations(
        &self,
        config: &mut LightningDbConfig,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) {
        match self.optimization_goal {
            OptimizationGoal::Throughput => {
                // Maximize throughput
                config.write_batch_size = config.write_batch_size * 2;
                config.wal_sync_mode = WalSyncMode::Async;
                config.compression_enabled = false; // Reduce CPU overhead
                config.use_optimized_transactions = true;
                config.use_optimized_page_manager = true;
            }
            OptimizationGoal::Latency => {
                // Minimize latency
                config.write_batch_size = config.write_batch_size / 2;
                config.prefetch_enabled = true;
                config.compression_enabled = false;
                config.cache_size = (config.cache_size as f64 * 1.2) as u64;
            }
            OptimizationGoal::ResourceEfficient => {
                // Minimize resource usage
                config.cache_size = config.cache_size / 2;
                config.prefetch_workers = config.prefetch_workers.min(2);
                config.compression_enabled = true;
                config.compression_type = 2; // LZ4 for balanced CPU usage
            }
            OptimizationGoal::CostOptimized => {
                // Optimize for cloud environments
                config.compression_enabled = true;
                config.compression_type = 1; // ZSTD for best compression
                config.cache_size = config.cache_size / 3;
                config.prefetch_enabled = false;
            }
            _ => {} // Balanced uses defaults
        }
    }

    /// Apply constraints to configuration
    fn apply_constraints(&self, config: &mut LightningDbConfig) {
        // Memory constraint
        if let Some(max_memory_mb) = self.constraints.max_memory_mb {
            let max_cache_size = (max_memory_mb * 1024 * 1024 * 3 / 4) as u64; // Use 75% for cache
            config.cache_size = config.cache_size.min(max_cache_size);
        }

        // Durability constraint
        match self.constraints.required_durability {
            DurabilityLevel::Maximum => {
                config.wal_sync_mode = WalSyncMode::Sync;
                config.write_batch_size = 1; // No batching
            }
            DurabilityLevel::High => {
                config.wal_sync_mode = WalSyncMode::Sync;
            }
            DurabilityLevel::Standard => {
                config.use_improved_wal = true;
            }
            _ => {} // BestEffort uses current settings
        }

        // Consistency constraint
        match self.constraints.required_consistency {
            ConsistencyLevel::Strong => {
                config.use_optimized_transactions = true;
            }
            _ => {} // Other levels use current settings
        }
    }

    /// Validate configuration
    fn validate_config(&self, config: &LightningDbConfig, hardware: &HardwareInfo) -> Result<()> {
        // Ensure cache size doesn't exceed available memory
        let max_cache = (hardware.available_memory_mb * 1024 * 1024 * 9 / 10) as u64; // 90% max
        if config.cache_size > max_cache {
            return Err(crate::Error::Config(format!(
                "Cache size {} exceeds available memory",
                config.cache_size
            )));
        }

        // Ensure prefetch workers don't exceed CPU count
        if config.prefetch_workers > hardware.cpu_count {
            return Err(crate::Error::Config(format!(
                "Prefetch workers {} exceeds CPU count {}",
                config.prefetch_workers, hardware.cpu_count
            )));
        }

        Ok(())
    }

    /// Get configuration score
    pub fn score_config(
        &self,
        config: &LightningDbConfig,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> f64 {
        let mut score = 0.0;

        // Cache size scoring
        let optimal_cache = (hardware.available_memory_mb * 1024 * 1024 / 4) as u64;
        let cache_score = 1.0
            - ((config.cache_size as f64 - optimal_cache as f64).abs() / optimal_cache as f64)
                .min(1.0);
        score += cache_score * 0.3;

        // Workload alignment scoring
        let workload_score = match (workload.workload_type, config.compression_enabled) {
            (WorkloadType::OLTP, false) => 1.0,
            (WorkloadType::OLTP, true) => 0.7,
            (WorkloadType::OLAP, true) => 1.0,
            (WorkloadType::OLAP, false) => 0.8,
            _ => 0.9,
        };
        score += workload_score * 0.3;

        // Hardware utilization scoring
        let cpu_score = if config.prefetch_enabled && hardware.cpu_count >= 8 {
            1.0
        } else {
            0.8
        };
        score += cpu_score * 0.2;

        // Goal alignment scoring
        let goal_score = match (self.optimization_goal, config.wal_sync_mode) {
            (OptimizationGoal::Throughput, WalSyncMode::Async) => 1.0,
            (OptimizationGoal::Latency, WalSyncMode::Sync) => 0.9,
            _ => 0.85,
        };
        score += goal_score * 0.2;

        score
    }
}

impl Default for OptimizationConstraints {
    fn default() -> Self {
        Self {
            max_memory_mb: None,
            max_cpu_percent: None,
            max_disk_mb: None,
            required_durability: DurabilityLevel::Standard,
            required_consistency: ConsistencyLevel::Strong,
        }
    }
}

impl ConfigOptimizer {
    /// Get explanation for configuration choices
    pub fn explain_config(
        &self,
        config: &LightningDbConfig,
        hardware: &HardwareInfo,
        workload: &WorkloadProfile,
    ) -> Vec<String> {
        let mut explanations = Vec::new();

        // Explain cache size
        explanations.push(format!(
            "Cache size set to {} MB ({:.0}% of available memory) for {:?} workload",
            config.cache_size / 1024 / 1024,
            (config.cache_size as f64 / (hardware.available_memory_mb as f64 * 1024.0 * 1024.0))
                * 100.0,
            workload.workload_type
        ));

        // Explain compression choice
        if config.compression_enabled {
            explanations.push(format!(
                "Compression enabled with {} to reduce storage and I/O",
                match config.compression_type {
                    1 => "ZSTD (best compression)",
                    2 => "LZ4 (balanced speed)",
                    3 => "Snappy (fastest)",
                    _ => "default",
                }
            ));
        } else {
            explanations.push("Compression disabled for lowest latency".to_string());
        }

        // Explain WAL mode
        explanations.push(format!(
            "WAL mode set to {:?} for {}",
            config.wal_sync_mode,
            match config.wal_sync_mode {
                WalSyncMode::Async => "maximum throughput",
                WalSyncMode::Sync => "maximum durability",
                WalSyncMode::Periodic { .. } => "balanced durability with periodic sync",
            }
        ));

        // Explain prefetch settings
        if config.prefetch_enabled {
            explanations.push(format!(
                "Prefetch enabled with {} workers and distance {} for {:.0}% sequential access",
                config.prefetch_workers,
                config.prefetch_distance,
                workload.sequential_access_ratio * 100.0
            ));
        }

        // Explain optimization features
        if config.use_optimized_transactions {
            explanations.push("Optimized transactions enabled for better concurrency".to_string());
        }

        explanations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_optimizer_creation() {
        let _optimizer = ConfigOptimizer::new(OptimizationGoal::Balanced);
    }

    #[test]
    fn test_hardware_optimizations() {
        let optimizer = ConfigOptimizer::new(OptimizationGoal::Throughput);
        let mut config = LightningDbConfig::default();

        let hardware = HardwareInfo {
            cpu_count: 16,
            physical_cores: 8,
            cpu_arch: "x86_64".to_string(),
            cpu_model: "Intel Core i9".to_string(),
            cpu_base_freq_mhz: Some(3200),
            cpu_features: vec!["AVX2".to_string()],
            total_memory_mb: 32768,
            available_memory_mb: 16384,
            memory_bandwidth_mb_per_sec: Some(51200.0),
            cache_sizes: super::super::hardware_detector::CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 16384,
                cache_line_size: 64,
            },
            storage_devices: vec![],
            storage_type: super::super::hardware_detector::StorageType::NVME,
            storage_bandwidth_mb_per_sec: Some(3500.0),
            numa_nodes: 1,
            huge_pages_available: false,
            gpu_devices: vec![],
            network_interfaces: vec![],
            thermal_state: super::super::hardware_detector::ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: super::super::hardware_detector::PowerProfile {
                power_source: super::super::hardware_detector::PowerSource::AC,
                cpu_governor: Some("performance".to_string()),
                max_performance_available: true,
            },
        };

        optimizer.apply_hardware_optimizations(&mut config, &hardware);

        assert!(config.cache_size > 0);
        assert!(config.prefetch_enabled);
        assert!(config.use_optimized_transactions);
    }

    #[test]
    fn test_workload_optimizations() {
        let optimizer = ConfigOptimizer::new(OptimizationGoal::Balanced);
        let mut config = LightningDbConfig::default();

        let workload = WorkloadProfile {
            workload_type: WorkloadType::OLTP,
            read_ratio: 0.7,
            write_ratio: 0.3,
            sequential_access_ratio: 0.1,
            ..Default::default()
        };

        optimizer.apply_workload_optimizations(&mut config, &workload);

        assert!(!config.compression_enabled); // OLTP disables compression
        assert_eq!(config.write_batch_size, 100);
    }
}

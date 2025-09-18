use crate::core::error::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum WorkloadType {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    Analytical,
    Sequential,
    Random,
    OLTP,
    OLAP,
    Cache,
    KeyValue,
    TimeSeries,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareInfo {
    pub cpu_cores: usize,
    pub total_memory: usize,
    pub total_memory_mb: usize,
    pub cache_sizes: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveSizingConfig {
    pub enabled: bool,
    pub adjustment_interval: Duration,
    pub min_cache_size: usize,
    pub max_cache_size: usize,
    pub memory_pressure_threshold: f64,
    pub performance_threshold: f64,
    pub max_adjustment_percent: f64,
    pub analysis_window_size: usize,
    pub enable_cache_warming: bool,
    pub enable_predictive_sizing: bool,
}

impl Default for AdaptiveSizingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            adjustment_interval: Duration::from_secs(60),
            min_cache_size: 64 * 1024 * 1024,
            max_cache_size: 8 * 1024 * 1024 * 1024,
            memory_pressure_threshold: 0.85,
            performance_threshold: 0.02,
            max_adjustment_percent: 0.15,
            analysis_window_size: 300,
            enable_cache_warming: false,
            enable_predictive_sizing: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachePerformanceMetrics {
    pub hit_rate: f64,
    pub eviction_rate: f64,
    pub avg_latency_us: f64,
    pub miss_penalty_ms: f64,
    pub memory_utilization: f64,
    pub average_latency_us: f64,
    pub throughput_ops_per_sec: f64,
    pub working_set_hit_rate: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum SizingReason {
    MemoryPressure,
    PerformanceImprovement,
    WorkloadChange,
    Manual,
}

#[derive(Debug)]
pub struct AdaptiveCacheSizer {
    config: AdaptiveSizingConfig,
    current_size: AtomicUsize,
    target_size: AtomicUsize,
    hardware_info: Arc<RwLock<Option<HardwareInfo>>>,
}

impl AdaptiveCacheSizer {
    pub fn new(config: AdaptiveSizingConfig) -> Result<Self> {
        let initial_size = config.min_cache_size;

        Ok(Self {
            config,
            current_size: AtomicUsize::new(initial_size),
            target_size: AtomicUsize::new(initial_size),
            hardware_info: Arc::new(RwLock::new(None)),
        })
    }

    pub fn initialize(&self) -> Result<()> {
        let total_mem = 8 * 1024 * 1024 * 1024;
        let hw_info = HardwareInfo {
            cpu_cores: num_cpus::get(),
            total_memory: total_mem,
            total_memory_mb: total_mem / (1024 * 1024),
            cache_sizes: vec![32768, 262144, 8388608],
        };

        *self.hardware_info.write() = Some(hw_info);
        Ok(())
    }

    pub fn record_access(&self, _page_id: u64, _cache_hit: bool, _latency_us: f64) {
    }

    pub fn update_performance(&self, _metrics: &CachePerformanceMetrics) -> Result<Option<usize>> {
        Ok(None)
    }

    pub fn analyze_workload(&self, _ops_per_sec: f64, _read_ratio: f64) -> Result<()> {
        Ok(())
    }

    pub fn get_current_size(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    pub fn get_target_size(&self) -> usize {
        self.target_size.load(Ordering::Relaxed)
    }

    pub fn adjust_cache_size(&self, new_size: usize, _reason: SizingReason) -> Result<()> {
        let clamped_size = new_size.max(self.config.min_cache_size)
            .min(self.config.max_cache_size);

        self.current_size.store(clamped_size, Ordering::Relaxed);
        self.target_size.store(clamped_size, Ordering::Relaxed);

        Ok(())
    }

    pub fn suggest_optimal_size(&self) -> Result<usize> {
        Ok(self.current_size.load(Ordering::Relaxed))
    }

    pub fn get_sizing_stats(&self) -> SizingStats {
        SizingStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            target_size: self.target_size.load(Ordering::Relaxed),
            min_size: self.config.min_cache_size,
            max_size: self.config.max_cache_size,
            adjustments_made: 0,
            successful_adjustments: 0,
            failed_adjustments: 0,
            last_adjustment_reason: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SizingStats {
    pub current_size: usize,
    pub target_size: usize,
    pub min_size: usize,
    pub max_size: usize,
    pub adjustments_made: u64,
    pub successful_adjustments: u64,
    pub failed_adjustments: u64,
    pub last_adjustment_reason: Option<SizingReason>,
}

pub type AdaptiveSizingStats = SizingStats;

#[derive(Debug, Clone)]
pub struct WorkloadPattern {
    pub workload_type: WorkloadType,
    pub read_write_ratio: f64,
    pub access_locality: f64,
    pub temporal_locality: f64,
    pub working_set_size: usize,
    pub scan_intensity: f64,
    pub burst_factor: f64,
}

#[derive(Debug, Clone)]
pub struct CacheAllocation {
    pub total_size: usize,
    pub tier_sizes: HashMap<CacheTier, usize>,
}

impl CacheAllocation {
    pub fn new(total: usize) -> Self {
        let mut tier_sizes = HashMap::new();
        tier_sizes.insert(CacheTier::Hot, total / 2);
        tier_sizes.insert(CacheTier::Warm, total / 4);
        tier_sizes.insert(CacheTier::Cold, total / 4);
        Self {
            total_size: total,
            tier_sizes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheTier {
    Hot,
    Warm,
    Cold,
}

#[derive(Debug, Clone)]
pub struct CacheWarmer;

#[derive(Debug, Clone)]
pub struct SizingDecision {
    pub new_size: usize,
    pub reason: SizingReason,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_adjustment() {
        let config = AdaptiveSizingConfig {
            min_cache_size: 1024,
            max_cache_size: 1024 * 1024,
            ..Default::default()
        };
        let sizer = AdaptiveCacheSizer::new(config).expect("Failed to create sizer");

        sizer.current_size.store(4096, Ordering::Relaxed);
        sizer.target_size.store(4096, Ordering::Relaxed);

        let initial_size = sizer.get_current_size();
        let new_size = initial_size * 2;

        let result = sizer.adjust_cache_size(new_size, SizingReason::PerformanceImprovement);
        assert!(result.is_ok());

        let current_size = sizer.get_current_size();
        assert_eq!(current_size, new_size);
    }

    #[test]
    fn test_size_clamping() {
        let config = AdaptiveSizingConfig {
            min_cache_size: 1024,
            max_cache_size: 8192,
            ..Default::default()
        };
        let sizer = AdaptiveCacheSizer::new(config).expect("Failed to create sizer");

        sizer.adjust_cache_size(512, SizingReason::Manual).unwrap();
        assert_eq!(sizer.get_current_size(), 1024);

        sizer.adjust_cache_size(16384, SizingReason::Manual).unwrap();
        assert_eq!(sizer.get_current_size(), 8192);
    }
}
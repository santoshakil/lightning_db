//! Performance Tuning System
//!
//! Provides hardware-aware auto-configuration, workload-specific optimization profiles,
//! and continuous performance tuning to achieve optimal Lightning DB performance.

use crate::{Database, LightningDbConfig, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::path::Path;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

pub mod hardware_detector;
pub mod workload_profiler;
pub mod workload_profiles;
pub mod auto_tuner;
pub mod config_optimizer;
pub mod validation;
pub mod ml_autotuner;
pub mod performance_validation;

pub use hardware_detector::{HardwareDetector, HardwareInfo};
pub use workload_profiler::{WorkloadProfiler, WorkloadProfile, WorkloadType};
pub use workload_profiles::{WorkloadProfiles, WorkloadConfigProfile, WorkloadProfileGenerator};
pub use auto_tuner::{AutoTuner, TuningStrategy};
pub use config_optimizer::{ConfigOptimizer, OptimizationGoal};
pub use validation::{PerformanceValidator, ValidationResult};
pub use ml_autotuner::{MLAutoTuner, AutoTuningConfig, TuningRecommendation, PerformanceDataPoint, SafetyLimits};
pub use performance_validation::{
    PerformanceValidationFramework, ValidationSession, DetailedPerformanceMetrics,
    PerformanceImprovement, ImprovementArea, ImprovementImpact, StatisticalAnalysis,
    ValidationResult as FrameworkValidationResult
};

/// Performance tuning coordinator
pub struct PerformanceTuner {
    hardware_detector: HardwareDetector,
    workload_profiler: WorkloadProfiler,
    auto_tuner: AutoTuner,
    config_optimizer: ConfigOptimizer,
    performance_validator: PerformanceValidator,
    current_config: Arc<RwLock<LightningDbConfig>>,
    tuning_history: Arc<RwLock<Vec<TuningResult>>>,
}

/// Tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningConfig {
    /// Enable auto-tuning
    pub auto_tune_enabled: bool,
    /// Tuning interval
    pub tuning_interval: Duration,
    /// Performance target (ops/sec)
    pub performance_target: Option<f64>,
    /// Maximum memory usage (MB)
    pub max_memory_mb: Option<u64>,
    /// Optimization goal
    pub optimization_goal: OptimizationGoal,
    /// Enable hardware detection
    pub detect_hardware: bool,
    /// Enable workload profiling
    pub profile_workload: bool,
    /// Validation sample size
    pub validation_sample_size: usize,
}

impl Default for TuningConfig {
    fn default() -> Self {
        Self {
            auto_tune_enabled: true,
            tuning_interval: Duration::from_secs(300), // 5 minutes
            performance_target: None,
            max_memory_mb: None,
            optimization_goal: OptimizationGoal::Balanced,
            detect_hardware: true,
            profile_workload: true,
            validation_sample_size: 10000,
        }
    }
}

/// Tuning result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningResult {
    pub timestamp: std::time::SystemTime,
    pub previous_config: LightningDbConfig,
    pub new_config: LightningDbConfig,
    pub performance_before: PerformanceMetrics,
    pub performance_after: PerformanceMetrics,
    pub improvement_percentage: f64,
    pub tuning_duration: Duration,
    pub success: bool,
    pub notes: Vec<String>,
}

/// Performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub read_ops_per_sec: f64,
    pub write_ops_per_sec: f64,
    pub mixed_ops_per_sec: f64,
    pub read_latency_us: LatencyMetrics,
    pub write_latency_us: LatencyMetrics,
    pub memory_usage_mb: u64,
    pub cpu_usage_percent: f64,
}

/// Latency metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetrics {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// Tuning parameters for ML optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningParameters {
    pub cache_size_mb: usize,
    pub write_buffer_size: usize,
    pub compaction_threads: usize,
    pub bloom_filter_bits_per_key: u32,
    pub prefetch_distance: usize,
    pub write_batch_size: usize,
}

impl Default for TuningParameters {
    fn default() -> Self {
        Self {
            cache_size_mb: 256,
            write_buffer_size: 4096,
            compaction_threads: 4,
            bloom_filter_bits_per_key: 10,
            prefetch_distance: 32,
            write_batch_size: 1000,
        }
    }
}

impl PerformanceTuner {
    /// Create a new performance tuner
    pub fn new(config: TuningConfig) -> Self {
        Self {
            hardware_detector: HardwareDetector::new(),
            workload_profiler: WorkloadProfiler::new(),
            auto_tuner: AutoTuner::new(config.clone()),
            config_optimizer: ConfigOptimizer::new(config.optimization_goal),
            performance_validator: PerformanceValidator::new(config.validation_sample_size),
            current_config: Arc::new(RwLock::new(LightningDbConfig::default())),
            tuning_history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Tune database configuration
    pub fn tune_database(&self, db_path: &Path) -> Result<TuningResult> {
        let start_time = Instant::now();
        let mut result = TuningResult {
            timestamp: std::time::SystemTime::now(),
            previous_config: self.current_config.read().clone(),
            new_config: LightningDbConfig::default(),
            performance_before: self.measure_current_performance(db_path)?,
            performance_after: PerformanceMetrics::default(),
            improvement_percentage: 0.0,
            tuning_duration: Duration::default(),
            success: false,
            notes: Vec::new(),
        };

        // Step 1: Detect hardware capabilities
        let hardware_info = self.hardware_detector.detect()?;
        result.notes.push(format!(
            "Detected hardware: {} CPUs, {}MB RAM, {} storage",
            hardware_info.cpu_count,
            hardware_info.total_memory_mb,
            hardware_info.storage_type
        ));

        // Step 2: Profile workload
        let workload_profile = self.workload_profiler.profile_database(db_path)?;
        result.notes.push(format!(
            "Workload profile: {:?} ({}% reads)",
            workload_profile.workload_type,
            (workload_profile.read_ratio * 100.0) as u32
        ));

        // Step 3: Generate optimized configuration
        let optimized_config = self.config_optimizer.optimize(
            &hardware_info,
            &workload_profile,
            &result.previous_config,
        )?;
        result.new_config = optimized_config;

        // Step 4: Validate new configuration
        let validation_result = self.performance_validator.validate_config(
            db_path,
            &result.new_config,
        )?;

        if validation_result.is_better {
            result.performance_after = validation_result.metrics;
            result.improvement_percentage = validation_result.improvement_percentage;
            result.success = true;
            result.notes.push(format!(
                "Performance improved by {:.1}%",
                result.improvement_percentage
            ));

            // Update current configuration
            *self.current_config.write() = result.new_config.clone();
        } else {
            result.notes.push("New configuration did not improve performance".to_string());
        }

        result.tuning_duration = start_time.elapsed();
        self.tuning_history.write().push(result.clone());

        Ok(result)
    }

    /// Get recommended configuration for specific workload
    pub fn get_recommended_config(&self, workload_type: WorkloadType) -> Result<LightningDbConfig> {
        let hardware_info = self.hardware_detector.detect()?;
        let workload_profile = WorkloadProfile::for_type(workload_type);
        
        self.config_optimizer.optimize(
            &hardware_info,
            &workload_profile,
            &LightningDbConfig::default(),
        )
    }

    /// Measure current performance
    fn measure_current_performance(&self, db_path: &Path) -> Result<PerformanceMetrics> {
        // Open database with current configuration
        let db = Arc::new(Database::open(db_path, self.current_config.read().clone())?);
        
        // Run performance measurements
        self.performance_validator.measure_performance(&db)
    }

    /// Get tuning history
    pub fn get_history(&self) -> Vec<TuningResult> {
        self.tuning_history.read().clone()
    }

    /// Export tuning report
    pub fn export_report(&self, output_path: &Path) -> Result<()> {
        let history = self.get_history();
        let report = TuningReport {
            generated_at: std::time::SystemTime::now(),
            total_tuning_sessions: history.len(),
            successful_tunings: history.iter().filter(|r| r.success).count(),
            average_improvement: history.iter()
                .filter(|r| r.success)
                .map(|r| r.improvement_percentage)
                .sum::<f64>() / history.len().max(1) as f64,
            best_configuration: history.iter()
                .filter(|r| r.success)
                .max_by(|a, b| {
                    a.performance_after.mixed_ops_per_sec
                        .partial_cmp(&b.performance_after.mixed_ops_per_sec)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|r| r.new_config.clone()),
            tuning_history: history,
        };

        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(output_path, json)?;
        Ok(())
    }
}

/// Tuning report
#[derive(Debug, Serialize, Deserialize)]
struct TuningReport {
    generated_at: std::time::SystemTime,
    total_tuning_sessions: usize,
    successful_tunings: usize,
    average_improvement: f64,
    best_configuration: Option<LightningDbConfig>,
    tuning_history: Vec<TuningResult>,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            read_ops_per_sec: 0.0,
            write_ops_per_sec: 0.0,
            mixed_ops_per_sec: 0.0,
            read_latency_us: LatencyMetrics::default(),
            write_latency_us: LatencyMetrics::default(),
            memory_usage_mb: 0,
            cpu_usage_percent: 0.0,
        }
    }
}

impl Default for LatencyMetrics {
    fn default() -> Self {
        Self {
            p50: 0.0,
            p95: 0.0,
            p99: 0.0,
            p999: 0.0,
            max: 0.0,
        }
    }
}

/// Predefined configurations for common workloads
pub mod presets {
    use super::*;

    /// Get configuration for OLTP workload
    pub fn oltp_config() -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 1024 * 1024 * 1024, // 1GB cache
            prefetch_enabled: true,
            prefetch_distance: 16,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            compression_enabled: false, // Disable for low latency
            wal_sync_mode: crate::WalSyncMode::Async,
            write_batch_size: 100,
            ..Default::default()
        }
    }

    /// Get configuration for OLAP workload
    pub fn olap_config() -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 4 * 1024 * 1024 * 1024, // 4GB cache
            prefetch_enabled: true,
            prefetch_distance: 64,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            compression_enabled: true,
            compression_type: 1, // ZSTD
            wal_sync_mode: crate::WalSyncMode::Async,
            write_batch_size: 10000,
            ..Default::default()
        }
    }

    /// Get configuration for mixed workload
    pub fn mixed_config() -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 2 * 1024 * 1024 * 1024, // 2GB cache
            prefetch_enabled: true,
            prefetch_distance: 32,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            compression_enabled: true,
            compression_type: 2, // LZ4 for balanced speed
            wal_sync_mode: crate::WalSyncMode::Async,
            write_batch_size: 1000,
            ..Default::default()
        }
    }

    /// Get configuration for write-heavy workload
    pub fn write_heavy_config() -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 512 * 1024 * 1024, // 512MB cache (more for write buffer)
            prefetch_enabled: false,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            compression_enabled: false, // Disable for write speed
            wal_sync_mode: crate::WalSyncMode::Async,
            write_batch_size: 10000,
            use_improved_wal: true,
            ..Default::default()
        }
    }

    /// Get configuration for read-heavy workload
    pub fn read_heavy_config() -> LightningDbConfig {
        LightningDbConfig {
            cache_size: 8 * 1024 * 1024 * 1024, // 8GB cache
            prefetch_enabled: true,
            prefetch_distance: 128,
            prefetch_workers: 4,
            use_optimized_transactions: true,
            use_optimized_page_manager: true,
            compression_enabled: true,
            compression_type: 1, // ZSTD for better compression
            wal_sync_mode: crate::WalSyncMode::Sync, // Can afford sync for read-heavy
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_tuner_creation() {
        let config = TuningConfig::default();
        let _tuner = PerformanceTuner::new(config);
    }

    #[test]
    fn test_preset_configs() {
        let _oltp = presets::oltp_config();
        let _olap = presets::olap_config();
        let _mixed = presets::mixed_config();
    }
}
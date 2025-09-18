//! Adaptive Cache Sizing System
//!
//! Dynamically adjusts cache sizes based on workload patterns, memory pressure,
//! and performance metrics to optimize database performance across different
//! usage scenarios.

// Simplified types to replace removed performance_tuning module
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WorkloadType {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    Analytical,
    Sequential,
    Random,
    OLTP,       // Online Transaction Processing
    OLAP,       // Online Analytical Processing
    Cache,      // Cache-oriented workload
    KeyValue,   // Key-value store workload
    TimeSeries, // Time-series workload
}

#[derive(Debug, Clone)]
pub struct HardwareInfo {
    pub cpu_cores: usize,
    pub total_memory: usize,
    pub total_memory_mb: usize, // Memory in MB for compatibility
    pub cache_sizes: Vec<usize>,
}
use crate::core::error::{Error, Result};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

// Statistical calculations (previously SIMD-optimized)
#[inline(always)]
fn simd_mean_f64(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

#[inline(always)]
fn simd_variance_f64(values: &[f64], mean: f64) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }

    let sum_sq_diff: f64 = values.iter().map(|&v| (v - mean).powi(2)).sum();

    sum_sq_diff / (values.len() - 1) as f64
}

/// Adaptive cache sizing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveSizingConfig {
    /// Enable adaptive sizing
    pub enabled: bool,
    /// Adjustment interval
    pub adjustment_interval: Duration,
    /// Minimum cache size (bytes)
    pub min_cache_size: usize,
    /// Maximum cache size (bytes)
    pub max_cache_size: usize,
    /// Memory pressure threshold (0.0-1.0)
    pub memory_pressure_threshold: f64,
    /// Performance improvement threshold for size changes
    pub performance_threshold: f64,
    /// Maximum adjustment per interval (as percentage)
    pub max_adjustment_percent: f64,
    /// Workload analysis window size
    pub analysis_window_size: usize,
    /// Cache warming enabled
    pub enable_cache_warming: bool,
    /// Predictive sizing enabled
    pub enable_predictive_sizing: bool,
}

impl Default for AdaptiveSizingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            adjustment_interval: Duration::from_secs(60), // 1 minute
            min_cache_size: 64 * 1024 * 1024,             // 64MB
            max_cache_size: 8 * 1024 * 1024 * 1024,       // 8GB
            memory_pressure_threshold: 0.85,              // 85%
            performance_threshold: 0.02,                  // 2%
            max_adjustment_percent: 0.15,                 // 15%
            analysis_window_size: 300,                    // 5 minutes of samples
            enable_cache_warming: true,
            enable_predictive_sizing: true,
        }
    }
}

/// Workload pattern characteristics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadPattern {
    pub workload_type: WorkloadType,
    pub read_write_ratio: f64,
    pub access_locality: f64,    // How localized are accesses (0.0-1.0)
    pub temporal_locality: f64,  // How repetitive are accesses (0.0-1.0)
    pub working_set_size: usize, // Estimated working set size
    pub scan_intensity: f64,     // How scan-heavy is the workload (0.0-1.0)
    pub burst_factor: f64,       // Traffic burstiness (1.0 = steady)
}

impl Default for WorkloadPattern {
    fn default() -> Self {
        Self {
            workload_type: WorkloadType::Mixed,
            read_write_ratio: 0.7,
            access_locality: 0.5,
            temporal_locality: 0.5,
            working_set_size: 100 * 1024 * 1024, // 100MB
            scan_intensity: 0.1,
            burst_factor: 1.2,
        }
    }
}

/// Cache performance metrics for adaptive sizing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePerformanceMetrics {
    pub hit_rate: f64,
    pub miss_penalty_ms: f64,
    pub eviction_rate: f64,
    pub memory_utilization: f64,
    pub average_latency_us: f64,
    pub throughput_ops_per_sec: f64,
    pub working_set_hit_rate: f64,
}

impl Default for CachePerformanceMetrics {
    fn default() -> Self {
        Self {
            hit_rate: 0.0,
            miss_penalty_ms: 0.0,
            eviction_rate: 0.0,
            memory_utilization: 0.0,
            average_latency_us: 0.0,
            throughput_ops_per_sec: 0.0,
            working_set_hit_rate: 0.0,
        }
    }
}

/// Cache sizing decision with rationale
#[derive(Debug, Clone)]
pub struct SizingDecision {
    pub timestamp: SystemTime,
    pub old_size: usize,
    pub new_size: usize,
    pub adjustment_reason: SizingReason,
    pub confidence: f64,
    pub expected_improvement: f64,
}

/// Reasons for cache size adjustments
#[derive(Debug, Clone, PartialEq)]
pub enum SizingReason {
    WorkloadChange(WorkloadType),
    MemoryPressure,
    PerformanceImprovement,
    LocalityImprovement,
    ScanResistance,
    BurstHandling,
    PredictiveAdjustment,
    ManualOverride,
}

/// Cache tier for hierarchical caching
#[derive(Debug, Clone, PartialEq)]
pub enum CacheTier {
    Hot,  // Frequently accessed data
    Warm, // Moderately accessed data
    Cold, // Infrequently accessed data
    Scan, // One-time scan data
}

/// Hierarchical cache allocation
#[derive(Debug, Clone)]
pub struct CacheAllocation {
    pub hot_size: usize,
    pub warm_size: usize,
    pub cold_size: usize,
    pub scan_size: usize,
    pub total_size: usize,
}

impl CacheAllocation {
    pub fn new(total_size: usize, pattern: &WorkloadPattern) -> Self {
        // Allocate based on workload characteristics
        let hot_ratio = match pattern.workload_type {
            WorkloadType::ReadHeavy => 0.6,  // More hot cache for reads
            WorkloadType::WriteHeavy => 0.3, // Less hot cache for writes
            WorkloadType::OLTP => 0.5,       // Balanced for OLTP
            WorkloadType::OLAP => 0.4,       // Less hot for analytics
            WorkloadType::Mixed => 0.45,     // Balanced allocation
            WorkloadType::KeyValue => 0.5,   // Similar to OLTP
            WorkloadType::TimeSeries => 0.3, // Write-heavy pattern
            WorkloadType::Cache => 0.7,      // Read-heavy pattern
            WorkloadType::Analytical => 0.4, // Similar to OLAP
            WorkloadType::Sequential => 0.5, // Balanced for sequential
            WorkloadType::Random => 0.6,     // More cache for random access
        };

        let warm_ratio = 0.3;
        let cold_ratio = 0.15;
        let scan_ratio = 0.05 + (pattern.scan_intensity * 0.15); // 5-20% for scans

        // Normalize ratios
        let total_ratio = hot_ratio + warm_ratio + cold_ratio + scan_ratio;

        Self {
            hot_size: ((total_size as f64 * hot_ratio / total_ratio) as usize).max(4096),
            warm_size: ((total_size as f64 * warm_ratio / total_ratio) as usize).max(4096),
            cold_size: ((total_size as f64 * cold_ratio / total_ratio) as usize).max(4096),
            scan_size: ((total_size as f64 * scan_ratio / total_ratio) as usize).max(4096),
            total_size,
        }
    }
}

/// Adaptive cache sizing engine
#[derive(Debug)]
pub struct AdaptiveCacheSizer {
    config: AdaptiveSizingConfig,
    current_size: AtomicUsize,
    target_size: AtomicUsize,
    // Removed with performance_tuning module
    // workload_profiler: WorkloadProfiler,
    // hardware_detector: HardwareDetector,
    hardware_info: Arc<RwLock<Option<HardwareInfo>>>,

    // Pattern analysis
    current_pattern: Arc<RwLock<WorkloadPattern>>,
    pattern_history: Arc<RwLock<VecDeque<WorkloadPattern>>>,

    // Performance tracking
    performance_history: Arc<RwLock<VecDeque<CachePerformanceMetrics>>>,
    sizing_decisions: Arc<RwLock<VecDeque<SizingDecision>>>,

    // Predictive models
    working_set_estimator: Arc<Mutex<WorkingSetEstimator>>,
    performance_predictor: Arc<Mutex<PerformancePredictor>>,

    // Control flags
    last_adjustment: Arc<RwLock<Instant>>,
    is_adjusting: AtomicBool,

    // Statistics
    adjustment_count: AtomicU64,
    successful_adjustments: AtomicU64,
}

/// Working set size estimation
#[derive(Debug)]
struct WorkingSetEstimator {
    access_frequency: HashMap<u64, u32>,
    temporal_windows: VecDeque<HashSet<u64>>,
    _window_size: Duration,
    max_windows: usize,
    estimated_size: usize,
}

use std::collections::HashSet;

impl WorkingSetEstimator {
    fn new(window_size: Duration, max_windows: usize) -> Self {
        Self {
            access_frequency: HashMap::new(),
            temporal_windows: VecDeque::new(),
            _window_size: window_size,
            max_windows,
            estimated_size: 0,
        }
    }

    fn record_access(&mut self, page_id: u64) {
        // Update frequency counter
        *self.access_frequency.entry(page_id).or_insert(0) += 1;

        // Add to current window or create new one
        if let Some(current_window) = self.temporal_windows.back_mut() {
            current_window.insert(page_id);
        } else {
            let mut new_window = HashSet::new();
            new_window.insert(page_id);
            self.temporal_windows.push_back(new_window);
        }

        // Maintain window limit
        while self.temporal_windows.len() > self.max_windows {
            if let Some(old_window) = self.temporal_windows.pop_front() {
                // Decay frequency counts for pages in old window
                for page_id in old_window {
                    if let Some(count) = self.access_frequency.get_mut(&page_id) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            self.access_frequency.remove(&page_id);
                        }
                    }
                }
            }
        }

        // Estimate working set size (pages accessed frequently)
        self.estimated_size = self
            .access_frequency
            .values()
            .filter(|&&count| count >= 2)
            .count()
            * 4096; // Assume 4KB pages
    }

    fn get_estimated_size(&self) -> usize {
        self.estimated_size
    }

    fn get_locality_score(&self) -> f64 {
        if self.access_frequency.is_empty() {
            return 0.5;
        }

        // Calculate how concentrated accesses are
        let total_accesses: u32 = self.access_frequency.values().sum();
        let unique_pages = self.access_frequency.len();

        if unique_pages == 0 {
            return 0.5;
        }

        // Higher scores mean more concentrated access patterns
        let average_accesses = total_accesses as f64 / unique_pages as f64;
        (average_accesses / 10.0).min(1.0)
    }
}

/// Performance prediction for cache size changes
#[derive(Debug)]
struct PerformancePredictor {
    size_performance_history: VecDeque<(usize, f64)>, // (size, hit_rate)
    max_history: usize,
    last_prediction: Option<f64>,
}

impl PerformancePredictor {
    fn new(max_history: usize) -> Self {
        Self {
            size_performance_history: VecDeque::new(),
            max_history,
            last_prediction: None,
        }
    }

    fn record_performance(&mut self, cache_size: usize, hit_rate: f64) {
        self.size_performance_history
            .push_back((cache_size, hit_rate));

        while self.size_performance_history.len() > self.max_history {
            self.size_performance_history.pop_front();
        }
    }

    fn predict_hit_rate(&mut self, target_size: usize) -> f64 {
        if self.size_performance_history.len() < 3 {
            return 0.8; // Default assumption
        }

        // Simple linear interpolation/extrapolation
        let mut size_sum = 0.0;
        let mut hit_rate_sum = 0.0;
        let mut size_sq_sum = 0.0;
        let mut size_hit_sum = 0.0;
        let n = self.size_performance_history.len() as f64;

        for (size, hit_rate) in &self.size_performance_history {
            let size_f = *size as f64;
            size_sum += size_f;
            hit_rate_sum += hit_rate;
            size_sq_sum += size_f * size_f;
            size_hit_sum += size_f * hit_rate;
        }

        // Linear regression: hit_rate = a * size + b
        let denominator = n * size_sq_sum - size_sum * size_sum;
        if denominator.abs() < 1e-10 {
            return hit_rate_sum / n; // Average if no variance
        }

        let a = (n * size_hit_sum - size_sum * hit_rate_sum) / denominator;
        let b = (hit_rate_sum - a * size_sum) / n;

        let predicted = a * (target_size as f64) + b;
        self.last_prediction = Some(predicted);

        // Clamp to reasonable range
        predicted.clamp(0.0, 1.0)
    }

    fn get_confidence(&self) -> f64 {
        if self.size_performance_history.len() < 5 {
            return 0.5;
        }

        // Calculate R-squared for confidence
        let n = self.size_performance_history.len();
        let mean_hit_rate: f64 = self
            .size_performance_history
            .iter()
            .map(|(_, hr)| hr)
            .sum::<f64>()
            / n as f64;

        let mut ss_tot = 0.0;
        let mut ss_res = 0.0;

        // Collect predictions first to avoid borrowing issues
        let predictions: Vec<f64> = self
            .size_performance_history
            .iter()
            .map(|(size, _)| {
                // Simple linear regression calculation without mutable borrow
                let mut size_sum = 0.0;
                let mut hit_rate_sum = 0.0;
                let mut size_sq_sum = 0.0;
                let mut size_hit_sum = 0.0;
                let n = self.size_performance_history.len() as f64;

                for (s, hr) in &self.size_performance_history {
                    let size_f = *s as f64;
                    size_sum += size_f;
                    hit_rate_sum += hr;
                    size_sq_sum += size_f * size_f;
                    size_hit_sum += size_f * hr;
                }

                let denominator = n * size_sq_sum - size_sum * size_sum;
                if denominator.abs() < 1e-10 {
                    return hit_rate_sum / n;
                }

                let a = (n * size_hit_sum - size_sum * hit_rate_sum) / denominator;
                let b = (hit_rate_sum - a * size_sum) / n;

                (a * (*size as f64) + b).clamp(0.0, 1.0)
            })
            .collect();

        for ((_, actual_hit_rate), predicted) in
            self.size_performance_history.iter().zip(predictions)
        {
            ss_tot += (actual_hit_rate - mean_hit_rate).powi(2);
            ss_res += (actual_hit_rate - predicted).powi(2);
        }

        if ss_tot < 1e-10 {
            return 1.0;
        }

        (1.0 - ss_res / ss_tot).clamp(0.0, 1.0)
    }
}

impl AdaptiveCacheSizer {
    /// Create a new adaptive cache sizer
    pub fn new(config: AdaptiveSizingConfig) -> Result<Self> {
        let initial_size = config.max_cache_size / 4; // Start conservatively

        Ok(Self {
            config: config.clone(),
            current_size: AtomicUsize::new(initial_size),
            target_size: AtomicUsize::new(initial_size),
            // Removed with performance_tuning module
            // workload_profiler: WorkloadProfiler::new(),
            // hardware_detector: HardwareDetector::new(),
            hardware_info: Arc::new(RwLock::new(None)),

            current_pattern: Arc::new(RwLock::new(WorkloadPattern::default())),
            pattern_history: Arc::new(RwLock::new(VecDeque::new())),

            performance_history: Arc::new(RwLock::new(VecDeque::new())),
            sizing_decisions: Arc::new(RwLock::new(VecDeque::new())),

            working_set_estimator: Arc::new(Mutex::new(WorkingSetEstimator::new(
                Duration::from_secs(60),
                10,
            ))),
            performance_predictor: Arc::new(Mutex::new(PerformancePredictor::new(50))),

            last_adjustment: Arc::new(RwLock::new(Instant::now())),
            is_adjusting: AtomicBool::new(false),

            adjustment_count: AtomicU64::new(0),
            successful_adjustments: AtomicU64::new(0),
        })
    }

    /// Initialize the sizer with hardware detection
    pub fn initialize(&self) -> Result<()> {
        // Hardware detection removed with performance_tuning module
        // Using default hardware info instead
        let total_mem = 8 * 1024 * 1024 * 1024; // Default 8GB
        let hw_info = HardwareInfo {
            cpu_cores: num_cpus::get(),
            total_memory: total_mem,
            total_memory_mb: total_mem / (1024 * 1024),
            cache_sizes: vec![32768, 262144, 8388608], // L1, L2, L3 defaults
        };
        {
            *self.hardware_info.write() = Some(hw_info.clone());

            // Set initial cache size based on available memory
            let recommended_size = self.calculate_initial_cache_size(&hw_info);
            self.current_size.store(recommended_size, Ordering::Relaxed);
            self.target_size.store(recommended_size, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Record cache access for pattern analysis
    pub fn record_access(&self, page_id: u64, _cache_hit: bool, _latency_us: f64) {
        self.working_set_estimator.lock().record_access(page_id);

        // Additional pattern analysis could be added here
        // For example, tracking sequential vs random access patterns
    }

    /// Update performance metrics and trigger adaptation if needed
    pub fn update_performance(&self, metrics: &CachePerformanceMetrics) -> Result<Option<usize>> {
        // Record performance history
        {
            let mut history = self.performance_history.write();
            history.push_back(metrics.clone());

            while history.len() > self.config.analysis_window_size {
                history.pop_front();
            }
        }

        // Update performance predictor
        let current_size = self.current_size.load(Ordering::Relaxed);
        self.performance_predictor
            .lock()
            .record_performance(current_size, metrics.hit_rate);

        // Check if adjustment is needed
        if self.should_adjust(metrics)? {
            return self.calculate_optimal_size(metrics);
        }

        Ok(None)
    }

    /// Analyze workload pattern and update internal state
    pub fn analyze_workload(&self, ops_per_sec: f64, read_ratio: f64) -> Result<()> {
        let working_set_size = self.working_set_estimator.lock().get_estimated_size();
        let locality_score = self.working_set_estimator.lock().get_locality_score();

        // Classify workload type
        let workload_type = if read_ratio > 0.8 {
            WorkloadType::ReadHeavy
        } else if read_ratio < 0.3 {
            WorkloadType::WriteHeavy
        } else if ops_per_sec > 10000.0 {
            WorkloadType::OLTP
        } else {
            WorkloadType::Mixed
        };

        // Calculate temporal locality
        let temporal_locality = self.calculate_temporal_locality();

        // Estimate scan intensity based on cache hit patterns
        let scan_intensity = self.estimate_scan_intensity();

        // Create workload pattern
        let pattern = WorkloadPattern {
            workload_type,
            read_write_ratio: read_ratio,
            access_locality: locality_score,
            temporal_locality,
            working_set_size,
            scan_intensity,
            burst_factor: self.calculate_burst_factor(),
        };

        // Update current pattern and history
        {
            *self.current_pattern.write() = pattern.clone();

            let mut history = self.pattern_history.write();
            history.push_back(pattern);

            while history.len() > 100 {
                history.pop_front();
            }
        }

        Ok(())
    }

    /// Get optimal cache allocation for current workload
    pub fn get_cache_allocation(&self) -> CacheAllocation {
        let total_size = self.current_size.load(Ordering::Relaxed);
        let pattern = self.current_pattern.read();
        CacheAllocation::new(total_size, &pattern)
    }

    /// Force cache size adjustment
    pub fn adjust_cache_size(&self, new_size: usize, reason: SizingReason) -> Result<bool> {
        let new_size = new_size
            .max(self.config.min_cache_size)
            .min(self.config.max_cache_size);

        let old_size = self.current_size.load(Ordering::Relaxed);

        if new_size == old_size {
            return Ok(false);
        }

        // Check memory pressure
        if let Some(hw_info) = self.hardware_info.read().as_ref() {
            let memory_usage_ratio =
                new_size as f64 / (hw_info.total_memory_mb * 1024 * 1024) as f64;
            if memory_usage_ratio > self.config.memory_pressure_threshold {
                return Err(Error::Config(
                    "Cache size would exceed memory pressure threshold".into(),
                ));
            }
        }

        // Record sizing decision
        let decision = SizingDecision {
            timestamp: SystemTime::now(),
            old_size,
            new_size,
            adjustment_reason: reason,
            confidence: self.performance_predictor.lock().get_confidence(),
            expected_improvement: self.estimate_improvement(old_size, new_size),
        };

        self.sizing_decisions.write().push_back(decision);

        // Update sizes
        self.current_size.store(new_size, Ordering::Relaxed);
        self.target_size.store(new_size, Ordering::Relaxed);
        *self.last_adjustment.write() = Instant::now();

        self.adjustment_count.fetch_add(1, Ordering::Relaxed);

        Ok(true)
    }

    /// Get current cache size
    pub fn get_current_size(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get sizing statistics
    pub fn get_statistics(&self) -> AdaptiveSizingStats {
        let performance_history = self.performance_history.read();
        let decisions = self.sizing_decisions.read();

        let average_hit_rate = if performance_history.is_empty() {
            0.0
        } else {
            performance_history.iter().map(|m| m.hit_rate).sum::<f64>()
                / performance_history.len() as f64
        };

        let recent_adjustments = decisions.len();
        let successful_count = self.successful_adjustments.load(Ordering::Relaxed);
        let total_count = self.adjustment_count.load(Ordering::Relaxed);

        AdaptiveSizingStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            target_size: self.target_size.load(Ordering::Relaxed),
            average_hit_rate,
            total_adjustments: total_count,
            successful_adjustments: successful_count,
            recent_adjustments,
            current_pattern: self.current_pattern.read().clone(),
            last_adjustment_time: *self.last_adjustment.read(),
        }
    }

    // Private helper methods

    fn calculate_initial_cache_size(&self, hw_info: &HardwareInfo) -> usize {
        // Use 25% of available memory by default, within configured limits
        let recommended = (hw_info.total_memory_mb * 1024 * 1024) / 4;
        recommended
            .max(self.config.min_cache_size)
            .min(self.config.max_cache_size)
    }

    fn should_adjust(&self, metrics: &CachePerformanceMetrics) -> Result<bool> {
        // Don't adjust too frequently
        if self.last_adjustment.read().elapsed() < self.config.adjustment_interval {
            return Ok(false);
        }

        // Don't adjust if already adjusting
        if self.is_adjusting.load(Ordering::Relaxed) {
            return Ok(false);
        }

        // Check if performance is below threshold
        if metrics.hit_rate < 0.7 && metrics.memory_utilization > 0.9 {
            return Ok(true);
        }

        // Check for workload pattern changes
        let pattern_history = self.pattern_history.read();
        if pattern_history.len() >= 3 {
            let recent_patterns: Vec<_> = pattern_history.iter().rev().take(3).collect();
            if self.has_workload_pattern_changed(&recent_patterns) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn calculate_optimal_size(&self, metrics: &CachePerformanceMetrics) -> Result<Option<usize>> {
        let current_size = self.current_size.load(Ordering::Relaxed);
        let pattern = self.current_pattern.read();

        // Base size calculation on workload characteristics
        let base_size = match pattern.workload_type {
            WorkloadType::ReadHeavy => {
                // Read-heavy workloads benefit from larger caches
                pattern.working_set_size.max(current_size * 12 / 10)
            }
            WorkloadType::WriteHeavy => {
                // Write-heavy workloads need less cache but more write buffers
                pattern.working_set_size.min(current_size * 8 / 10)
            }
            WorkloadType::OLTP => {
                // OLTP needs cache size based on hot working set
                (pattern.working_set_size as f64 * (1.0 + pattern.temporal_locality)) as usize
            }
            WorkloadType::OLAP => {
                // OLAP needs large cache for scan resistance
                pattern.working_set_size.max(current_size * 15 / 10)
            }
            WorkloadType::Mixed => {
                // Balanced approach
                pattern.working_set_size.max(current_size * 11 / 10)
            }
            WorkloadType::KeyValue => {
                // Similar to OLTP but potentially smaller working set
                (pattern.working_set_size as f64 * (1.0 + pattern.temporal_locality * 0.8)) as usize
            }
            WorkloadType::TimeSeries => {
                // Time-series often has sequential access, moderate cache
                pattern.working_set_size.max(current_size * 10 / 10)
            }
            WorkloadType::Cache => {
                // Cache workloads benefit significantly from larger cache
                pattern.working_set_size.max(current_size * 14 / 10)
            }
            WorkloadType::Analytical => {
                // Analytical workloads similar to OLAP
                pattern.working_set_size.max(current_size * 15 / 10)
            }
            WorkloadType::Sequential => {
                // Sequential access patterns need moderate cache
                pattern.working_set_size
            }
            WorkloadType::Random => {
                // Random access benefits from larger cache
                pattern.working_set_size.max(current_size * 13 / 10)
            }
        };

        // Adjust based on hit rate
        let hit_rate_factor = if metrics.hit_rate < 0.7 {
            1.3 // Increase cache size significantly
        } else if metrics.hit_rate < 0.85 {
            1.1 // Moderate increase
        } else if metrics.hit_rate > 0.95 && metrics.memory_utilization < 0.5 {
            0.9 // Slight decrease if very high hit rate and low utilization
        } else {
            1.0 // No change
        };

        let target_size = (base_size as f64 * hit_rate_factor) as usize;

        // Apply adjustment limits
        let max_increase =
            (current_size as f64 * (1.0 + self.config.max_adjustment_percent)) as usize;
        let max_decrease =
            (current_size as f64 * (1.0 - self.config.max_adjustment_percent)) as usize;

        let adjusted_size = target_size
            .max(max_decrease)
            .min(max_increase)
            .max(self.config.min_cache_size)
            .min(self.config.max_cache_size);

        if adjusted_size != current_size {
            // Determine the reason for adjustment
            let _reason = if pattern.workload_type != WorkloadType::Mixed {
                SizingReason::WorkloadChange(pattern.workload_type)
            } else if metrics.hit_rate < 0.7 {
                SizingReason::PerformanceImprovement
            } else {
                SizingReason::LocalityImprovement
            };

            Ok(Some(adjusted_size))
        } else {
            Ok(None)
        }
    }

    fn calculate_temporal_locality(&self) -> f64 {
        // Analyze recent performance history to estimate temporal locality
        let history = self.performance_history.read();
        if history.len() < 3 {
            return 0.5;
        }

        // Higher hit rates generally indicate better temporal locality
        let recent_hit_rates: Vec<f64> = history.iter().rev().take(5).map(|m| m.hit_rate).collect();

        // Use SIMD-optimized mean calculation
        // Convert hit rate to locality score
        simd_mean_f64(&recent_hit_rates)
    }

    fn estimate_scan_intensity(&self) -> f64 {
        let history = self.performance_history.read();
        if history.len() < 5 {
            return 0.1;
        }

        // Look for patterns indicating scan workloads
        let recent_metrics: Vec<_> = history.iter().rev().take(5).collect();

        // High eviction rates with low hit rates suggest scan workloads
        let eviction_rates: Vec<f64> = recent_metrics.iter().map(|m| m.eviction_rate).collect();
        let hit_rates: Vec<f64> = recent_metrics.iter().map(|m| m.hit_rate).collect();

        // Use SIMD-optimized calculations
        let avg_eviction_rate = simd_mean_f64(&eviction_rates);
        let avg_hit_rate = simd_mean_f64(&hit_rates);

        if avg_eviction_rate > 0.1 && avg_hit_rate < 0.6 {
            0.3 + (avg_eviction_rate * 0.7).min(0.5)
        } else {
            0.1
        }
    }

    fn calculate_burst_factor(&self) -> f64 {
        let history = self.performance_history.read();
        if history.len() < 10 {
            return 1.2;
        }

        // Calculate coefficient of variation in throughput
        let throughputs: Vec<f64> = history.iter().map(|m| m.throughput_ops_per_sec).collect();

        // Use SIMD-optimized statistical calculations
        let mean = simd_mean_f64(&throughputs);

        if mean < 1.0 {
            return 1.0;
        }

        let variance = simd_variance_f64(&throughputs, mean);
        let std_dev = variance.sqrt();
        let cv = std_dev / mean;

        1.0 + cv.min(2.0)
    }

    fn has_workload_pattern_changed(&self, recent_patterns: &[&WorkloadPattern]) -> bool {
        if recent_patterns.len() < 3 {
            return false;
        }

        let first = recent_patterns[0];
        let last = recent_patterns[recent_patterns.len() - 1];

        // Check for significant changes in key metrics
        (first.read_write_ratio - last.read_write_ratio).abs() > 0.2
            || first.workload_type != last.workload_type
            || (first.access_locality - last.access_locality).abs() > 0.3
            || (first.working_set_size as f64 / last.working_set_size as f64 - 1.0).abs() > 0.5
    }

    fn estimate_improvement(&self, old_size: usize, new_size: usize) -> f64 {
        if new_size <= old_size {
            return 0.0;
        }

        // Predict improvement based on historical data
        let mut predictor = self.performance_predictor.lock();
        let predicted_old = predictor.predict_hit_rate(old_size);
        let predicted_new = predictor.predict_hit_rate(new_size);
        drop(predictor);

        (predicted_new - predicted_old).max(0.0)
    }
}

/// Adaptive cache sizing statistics
#[derive(Debug, Clone)]
pub struct AdaptiveSizingStats {
    pub current_size: usize,
    pub target_size: usize,
    pub average_hit_rate: f64,
    pub total_adjustments: u64,
    pub successful_adjustments: u64,
    pub recent_adjustments: usize,
    pub current_pattern: WorkloadPattern,
    pub last_adjustment_time: Instant,
}

/// Cache warming strategy for new cache sizes
pub struct CacheWarmer {
    _warming_rate: usize, // Pages to warm per second
    _priority_pages: VecDeque<u64>,
    is_warming: AtomicBool,
}

impl CacheWarmer {
    pub fn new(warming_rate: usize) -> Self {
        Self {
            _warming_rate: warming_rate,
            _priority_pages: VecDeque::new(),
            is_warming: AtomicBool::new(false),
        }
    }

    pub fn start_warming(&self, _working_set: &[u64]) {
        if self.is_warming.swap(true, Ordering::Relaxed) {
            // Already warming
        }

        // Implementation would warm cache with priority pages
        // This is a placeholder for the actual warming logic
    }

    pub fn is_warming(&self) -> bool {
        self.is_warming.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_adaptive_sizer_creation() {
        let config = AdaptiveSizingConfig::default();
        let sizer = AdaptiveCacheSizer::new(config);
        assert!(sizer.is_ok());
    }

    #[test]
    fn test_workload_pattern_analysis() {
        let config = AdaptiveSizingConfig::default();
        let sizer = AdaptiveCacheSizer::new(config).expect("Failed to create adaptive cache sizer");

        // Simulate read-heavy workload
        sizer
            .analyze_workload(5000.0, 0.9)
            .expect("Failed to analyze workload");
        let pattern = sizer.current_pattern.read();
        assert_eq!(pattern.workload_type, WorkloadType::ReadHeavy);
    }

    #[test]
    fn test_cache_allocation() {
        let pattern = WorkloadPattern {
            workload_type: WorkloadType::OLTP,
            read_write_ratio: 0.7,
            access_locality: 0.8,
            temporal_locality: 0.9,
            working_set_size: 100 * 1024 * 1024,
            scan_intensity: 0.1,
            burst_factor: 1.2,
        };

        let allocation = CacheAllocation::new(1024 * 1024 * 1024, &pattern); // 1GB
        assert!(allocation.hot_size > 0);
        assert!(allocation.warm_size > 0);
        assert!(allocation.cold_size > 0);
        assert!(allocation.scan_size > 0);
        assert_eq!(allocation.total_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_working_set_estimator() {
        let mut estimator = WorkingSetEstimator::new(Duration::from_secs(60), 5);

        // Simulate access pattern
        for page_id in 0..100 {
            estimator.record_access(page_id);
        }

        // Access some pages multiple times
        for _ in 0..5 {
            for page_id in 0..20 {
                estimator.record_access(page_id);
            }
        }

        let estimated_size = estimator.get_estimated_size();
        assert!(estimated_size > 0);

        let locality_score = estimator.get_locality_score();
        assert!((0.0..=1.0).contains(&locality_score));
    }

    #[test]
    fn test_performance_predictor() {
        let mut predictor = PerformancePredictor::new(10);

        // Add some performance data points
        predictor.record_performance(64 * 1024 * 1024, 0.7); // 64MB, 70% hit rate
        predictor.record_performance(128 * 1024 * 1024, 0.8); // 128MB, 80% hit rate
        predictor.record_performance(256 * 1024 * 1024, 0.85); // 256MB, 85% hit rate

        let predicted = predictor.predict_hit_rate(512 * 1024 * 1024);
        assert!((0.0..=1.0).contains(&predicted));

        let confidence = predictor.get_confidence();
        assert!((0.0..=1.0).contains(&confidence));
    }

    #[test]
    fn test_size_adjustment() {
        let config = AdaptiveSizingConfig {
            min_cache_size: 1024,
            max_cache_size: 1024 * 1024,
            ..Default::default()
        };
        let sizer = AdaptiveCacheSizer::new(config).expect("Failed to create adaptive cache sizer");

        // Set initial size directly without full initialization
        sizer.current_size.store(4096, Ordering::Relaxed);
        sizer.target_size.store(4096, Ordering::Relaxed);

        let initial_size = sizer.get_current_size();
        let new_size = initial_size * 2;

        let result = sizer.adjust_cache_size(new_size, SizingReason::PerformanceImprovement);
        assert!(result.is_ok());

        let current_size = sizer.get_current_size();
        assert_eq!(current_size, new_size);
    }
}

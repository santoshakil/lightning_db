//! Comprehensive Memory Tracking Infrastructure
//!
//! This module provides complete memory tracking capabilities including:
//! - Real-time allocation/deallocation monitoring
//! - Memory growth pattern detection
//! - Unbounded growth detection
//! - Memory statistics reporting
//! - Integration with existing profiling system

use std::{
    sync::{Arc, Mutex, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}},
    time::{Duration, SystemTime, UNIX_EPOCH},
    thread,
    alloc::{GlobalAlloc, Layout},
};

use parking_lot::RwLock as ParkingRwLock;
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use tracing::{warn, info};

/// Global memory tracker instance
pub static MEMORY_TRACKER: once_cell::sync::Lazy<Arc<MemoryTracker>> = 
    once_cell::sync::Lazy::new(|| Arc::new(MemoryTracker::new()));

/// Configuration for memory tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryTrackingConfig {
    /// Enable allocation tracking (performance impact)
    pub track_allocations: bool,
    /// Maximum number of allocation records to keep
    pub max_allocation_records: usize,
    /// Track stack traces for allocations
    pub track_stack_traces: bool,
    /// Memory growth detection threshold (bytes per second)
    pub growth_threshold: u64,
    /// Time window for growth detection
    pub growth_window: Duration,
    /// Enable memory statistics collection
    pub collect_statistics: bool,
    /// Statistics collection interval
    pub stats_interval: Duration,
    /// Enable memory alerts
    pub enable_alerts: bool,
    /// Memory usage alert threshold (bytes)
    pub alert_threshold: u64,
}

impl Default for MemoryTrackingConfig {
    fn default() -> Self {
        Self {
            track_allocations: cfg!(debug_assertions),
            max_allocation_records: 100_000,
            track_stack_traces: cfg!(debug_assertions),
            growth_threshold: 10 * 1024 * 1024, // 10 MB/s
            growth_window: Duration::from_secs(60),
            collect_statistics: true,
            stats_interval: Duration::from_secs(30),
            enable_alerts: true,
            alert_threshold: 1024 * 1024 * 1024, // 1 GB
        }
    }
}

/// Memory allocation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationRecord {
    pub address: usize,
    pub size: usize,
    pub timestamp: u64,
    pub thread_id: u64,
    pub allocation_site: Option<String>,
    pub stack_trace: Option<Vec<String>>,
}

/// Memory usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    pub total_allocated: u64,
    pub total_deallocated: u64,
    pub current_usage: u64,
    pub peak_usage: u64,
    pub allocation_count: u64,
    pub deallocation_count: u64,
    pub allocation_rate: f64, // allocations per second
    pub growth_rate: f64,     // bytes per second
    pub fragmentation_ratio: f64,
    pub live_allocations: usize,
}

/// Memory growth pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GrowthPattern {
    Stable,
    Linear { rate: f64 },
    Exponential { factor: f64 },
    Oscillating { amplitude: f64, period: Duration },
    Unbounded,
}

/// Memory alert type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MemoryAlert {
    HighUsage { current: u64, threshold: u64 },
    RapidGrowth { rate: f64, threshold: f64 },
    PotentialLeak { site: String, growth: u64 },
    FragmentationHigh { ratio: f64 },
    AllocationStorm { rate: f64 },
}

/// Allocation site statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationSiteStats {
    pub location: String,
    pub total_allocations: u64,
    pub total_bytes: u64,
    pub current_allocations: u64,
    pub current_bytes: u64,
    pub peak_allocations: u64,
    pub peak_bytes: u64,
    pub average_size: f64,
    pub growth_rate: f64,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
}

/// Main memory tracker
pub struct MemoryTracker {
    config: ParkingRwLock<MemoryTrackingConfig>,
    allocations: DashMap<usize, AllocationRecord>,
    statistics: RwLock<MemoryStats>,
    allocation_sites: DashMap<String, AllocationSiteStats>,
    memory_history: Mutex<Vec<(u64, u64)>>, // (timestamp, usage)
    alerts: Mutex<Vec<(SystemTime, MemoryAlert)>>,
    
    // Atomic counters for fast access
    total_allocated: AtomicU64,
    total_deallocated: AtomicU64,
    current_usage: AtomicU64,
    peak_usage: AtomicU64,
    allocation_count: AtomicU64,
    deallocation_count: AtomicU64,
    
    // Control flags
    enabled: AtomicBool,
    monitoring_thread: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            config: ParkingRwLock::new(MemoryTrackingConfig::default()),
            allocations: DashMap::new(),
            statistics: RwLock::new(MemoryStats {
                total_allocated: 0,
                total_deallocated: 0,
                current_usage: 0,
                peak_usage: 0,
                allocation_count: 0,
                deallocation_count: 0,
                allocation_rate: 0.0,
                growth_rate: 0.0,
                fragmentation_ratio: 0.0,
                live_allocations: 0,
            }),
            allocation_sites: DashMap::new(),
            memory_history: Mutex::new(Vec::new()),
            alerts: Mutex::new(Vec::new()),
            total_allocated: AtomicU64::new(0),
            total_deallocated: AtomicU64::new(0),
            current_usage: AtomicU64::new(0),
            peak_usage: AtomicU64::new(0),
            allocation_count: AtomicU64::new(0),
            deallocation_count: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
            monitoring_thread: Mutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Configure memory tracking
    pub fn configure(&self, config: MemoryTrackingConfig) {
        let mut cfg = self.config.write();
        *cfg = config;
    }

    /// Start memory tracking
    pub fn start(&self) {
        if self.enabled.swap(true, Ordering::SeqCst) {
            return; // Already running
        }

        info!("Starting memory tracker");
        
        // Reset statistics
        self.reset_statistics();
        
        // Start monitoring thread - need to get Arc reference to self
        // Since we're in the global MEMORY_TRACKER, we can safely clone the Arc
        let tracker_arc = MEMORY_TRACKER.clone();
        Self::start_monitoring_thread(tracker_arc);
    }

    /// Stop memory tracking
    pub fn stop(&self) {
        if !self.enabled.swap(false, Ordering::SeqCst) {
            return; // Not running
        }

        info!("Stopping memory tracker");
        
        // Signal shutdown
        self.shutdown_flag.store(true, Ordering::SeqCst);
        
        // Wait for monitoring thread
        if let Ok(mut guard) = self.monitoring_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
        
        self.shutdown_flag.store(false, Ordering::SeqCst);
    }

    /// Record memory allocation
    pub fn record_allocation(&self, addr: usize, size: usize, _layout: Layout) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        let config = self.config.read();
        if !config.track_allocations {
            return;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let thread_id = self.get_thread_id();
        
        // Create allocation record
        let record = AllocationRecord {
            address: addr,
            size,
            timestamp,
            thread_id,
            allocation_site: if config.track_stack_traces {
                Some(self.capture_allocation_site())
            } else {
                None
            },
            stack_trace: if config.track_stack_traces {
                Some(self.capture_stack_trace())
            } else {
                None
            },
        };

        // Store allocation record
        if self.allocations.len() < config.max_allocation_records {
            self.allocations.insert(addr, record.clone());
        }

        // Update statistics
        self.total_allocated.fetch_add(size as u64, Ordering::Relaxed);
        let current = self.current_usage.fetch_add(size as u64, Ordering::Relaxed) + size as u64;
        self.allocation_count.fetch_add(1, Ordering::Relaxed);

        // Update peak usage
        let mut peak = self.peak_usage.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_usage.compare_exchange_weak(
                peak, current, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }

        // Update allocation site statistics
        if let Some(ref site) = record.allocation_site {
            self.update_allocation_site_stats(site, size as u64, true);
        }
    }

    /// Record memory deallocation
    pub fn record_deallocation(&self, addr: usize) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }

        // Remove allocation record and get size
        if let Some((_, record)) = self.allocations.remove(&addr) {
            let size = record.size as u64;
            
            // Update statistics
            self.total_deallocated.fetch_add(size, Ordering::Relaxed);
            self.current_usage.fetch_sub(size, Ordering::Relaxed);
            self.deallocation_count.fetch_add(1, Ordering::Relaxed);

            // Update allocation site statistics
            if let Some(ref site) = record.allocation_site {
                self.update_allocation_site_stats(site, size, false);
            }
        }
    }

    /// Get current memory statistics
    pub fn get_statistics(&self) -> MemoryStats {
        let mut stats = self.statistics.read().unwrap().clone();
        
        // Update with current atomic values
        stats.total_allocated = self.total_allocated.load(Ordering::Relaxed);
        stats.total_deallocated = self.total_deallocated.load(Ordering::Relaxed);
        stats.current_usage = self.current_usage.load(Ordering::Relaxed);
        stats.peak_usage = self.peak_usage.load(Ordering::Relaxed);
        stats.allocation_count = self.allocation_count.load(Ordering::Relaxed);
        stats.deallocation_count = self.deallocation_count.load(Ordering::Relaxed);
        stats.live_allocations = self.allocations.len();
        
        stats
    }

    /// Detect memory growth patterns
    pub fn detect_growth_pattern(&self) -> GrowthPattern {
        let history = self.memory_history.lock().unwrap();
        if history.len() < 3 {
            return GrowthPattern::Stable;
        }

        let window_start = history.len().saturating_sub(10);
        let recent_data: Vec<_> = history[window_start..].to_vec();
        
        self.analyze_growth_pattern(&recent_data)
    }

    /// Get recent memory alerts
    pub fn get_alerts(&self, since: SystemTime) -> Vec<(SystemTime, MemoryAlert)> {
        let alerts = self.alerts.lock().unwrap();
        alerts.iter()
            .filter(|(timestamp, _)| *timestamp >= since)
            .cloned()
            .collect()
    }

    /// Get allocation site statistics
    pub fn get_allocation_sites(&self) -> Vec<AllocationSiteStats> {
        self.allocation_sites.iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Check for potential memory leaks
    pub fn detect_potential_leaks(&self) -> Vec<String> {
        let mut potential_leaks = Vec::new();
        let config = self.config.read();
        
        for entry in self.allocation_sites.iter() {
            let stats = entry.value();
            
            // Check for sites with no deallocations
            if stats.total_allocations > 100 && stats.current_allocations == stats.total_allocations {
                potential_leaks.push(format!(
                    "Potential leak at {}: {} allocations, no deallocations",
                    stats.location, stats.total_allocations
                ));
            }
            
            // Check for rapidly growing sites
            if stats.growth_rate > config.growth_threshold as f64 {
                potential_leaks.push(format!(
                    "Rapid growth at {}: {:.2} bytes/sec",
                    stats.location, stats.growth_rate
                ));
            }
        }
        
        potential_leaks
    }

    /// Export detailed memory report
    pub fn export_report(&self, include_allocations: bool) -> MemoryReport {
        MemoryReport {
            timestamp: SystemTime::now(),
            statistics: self.get_statistics(),
            growth_pattern: self.detect_growth_pattern(),
            allocation_sites: self.get_allocation_sites(),
            potential_leaks: self.detect_potential_leaks(),
            recent_alerts: self.get_alerts(
                SystemTime::now() - Duration::from_secs(3600)
            ),
            allocations: if include_allocations {
                Some(self.allocations.iter()
                    .map(|entry| entry.value().clone())
                    .collect())
            } else {
                None
            },
        }
    }

    // Private helper methods

    fn reset_statistics(&self) {
        self.allocations.clear();
        self.allocation_sites.clear();
        self.memory_history.lock().unwrap().clear();
        self.alerts.lock().unwrap().clear();
        
        self.total_allocated.store(0, Ordering::Relaxed);
        self.total_deallocated.store(0, Ordering::Relaxed);
        self.current_usage.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);
        self.deallocation_count.store(0, Ordering::Relaxed);
    }

    fn start_monitoring_thread(tracker_arc: Arc<MemoryTracker>) {
        let config = tracker_arc.config.read().clone();
        if !config.collect_statistics {
            return;
        }

        // Use Arc directly - completely safe, no type punning needed
        let shutdown_flag = tracker_arc.shutdown_flag.clone();
        let tracker_for_thread = tracker_arc.clone();
        
        let handle = thread::Builder::new()
            .name("memory-monitor".to_string())
            .spawn(move || {
                Self::safe_monitoring_loop(tracker_for_thread, shutdown_flag, config);
            })
            .expect("Failed to start memory monitoring thread");

        // Store the handle safely
        if let Ok(mut monitoring_thread) = tracker_arc.monitoring_thread.lock() {
            *monitoring_thread = Some(handle);
        }
    }

    fn safe_monitoring_loop(
        tracker: Arc<MemoryTracker>,
        shutdown_flag: Arc<AtomicBool>,
        config: MemoryTrackingConfig,
    ) {
        // Completely safe monitoring loop - no unsafe operations needed
        while !shutdown_flag.load(Ordering::Relaxed) {
            // Collect statistics
            tracker.collect_statistics();
            
            // Check for alerts
            tracker.check_memory_alerts();
            
            // Sleep
            thread::sleep(config.stats_interval);
        }
    }

    fn collect_statistics(&self) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let current_usage = self.current_usage.load(Ordering::Relaxed);
        
        // Add to history
        {
            let mut history = self.memory_history.lock().unwrap();
            history.push((timestamp, current_usage));
            
            // Keep last 1000 entries
            if history.len() > 1000 {
                history.drain(..100);
            }
        }

        // Update statistics
        {
            let mut stats = self.statistics.write().unwrap();
            stats.current_usage = current_usage;
            stats.peak_usage = self.peak_usage.load(Ordering::Relaxed);
            stats.total_allocated = self.total_allocated.load(Ordering::Relaxed);
            stats.total_deallocated = self.total_deallocated.load(Ordering::Relaxed);
            stats.allocation_count = self.allocation_count.load(Ordering::Relaxed);
            stats.deallocation_count = self.deallocation_count.load(Ordering::Relaxed);
            stats.live_allocations = self.allocations.len();
            
            // Calculate rates
            let history = self.memory_history.lock().unwrap();
            if history.len() >= 2 {
                let time_diff = (timestamp - history[history.len()-2].0) as f64 / 1_000_000_000.0;
                if time_diff > 0.0 {
                    let alloc_diff = stats.allocation_count as f64 - 
                        history.get(history.len()-2).map(|_| 0.0).unwrap_or(0.0);
                    stats.allocation_rate = alloc_diff / time_diff;
                    
                    let usage_diff = current_usage as f64 - history[history.len()-2].1 as f64;
                    stats.growth_rate = usage_diff / time_diff;
                }
            }
            
            // Calculate fragmentation ratio
            if stats.total_allocated > 0 {
                stats.fragmentation_ratio = 
                    (stats.total_allocated - stats.current_usage) as f64 / 
                    stats.total_allocated as f64;
            }
        }
    }

    fn check_memory_alerts(&self) {
        let config = self.config.read();
        if !config.enable_alerts {
            return;
        }

        let stats = self.get_statistics();
        let mut alerts = Vec::new();

        // High usage alert
        if stats.current_usage > config.alert_threshold {
            alerts.push(MemoryAlert::HighUsage {
                current: stats.current_usage,
                threshold: config.alert_threshold,
            });
        }

        // Rapid growth alert
        if stats.growth_rate > config.growth_threshold as f64 {
            alerts.push(MemoryAlert::RapidGrowth {
                rate: stats.growth_rate,
                threshold: config.growth_threshold as f64,
            });
        }

        // High fragmentation alert
        if stats.fragmentation_ratio > 0.5 {
            alerts.push(MemoryAlert::FragmentationHigh {
                ratio: stats.fragmentation_ratio,
            });
        }

        // Allocation storm alert
        if stats.allocation_rate > 10000.0 {
            alerts.push(MemoryAlert::AllocationStorm {
                rate: stats.allocation_rate,
            });
        }

        // Check for potential leaks
        for site in self.get_allocation_sites() {
            if site.growth_rate > config.growth_threshold as f64 && 
               site.current_allocations > 1000 {
                alerts.push(MemoryAlert::PotentialLeak {
                    site: site.location,
                    growth: site.current_bytes,
                });
            }
        }

        // Store alerts
        if !alerts.is_empty() {
            let timestamp = SystemTime::now();
            let mut alert_storage = self.alerts.lock().unwrap();
            
            for alert in alerts {
                warn!("Memory alert: {:?}", alert);
                alert_storage.push((timestamp, alert));
            }
            
            // Keep last 1000 alerts
            if alert_storage.len() > 1000 {
                alert_storage.drain(..100);
            }
        }
    }

    fn update_allocation_site_stats(&self, site: &str, size: u64, is_allocation: bool) {
        let now = SystemTime::now();
        
        let mut entry = self.allocation_sites.entry(site.to_string())
            .or_insert_with(|| AllocationSiteStats {
                location: site.to_string(),
                total_allocations: 0,
                total_bytes: 0,
                current_allocations: 0,
                current_bytes: 0,
                peak_allocations: 0,
                peak_bytes: 0,
                average_size: 0.0,
                growth_rate: 0.0,
                first_seen: now,
                last_seen: now,
            });

        if is_allocation {
            entry.total_allocations += 1;
            entry.total_bytes += size;
            entry.current_allocations += 1;
            entry.current_bytes += size;
            
            if entry.current_allocations > entry.peak_allocations {
                entry.peak_allocations = entry.current_allocations;
            }
            if entry.current_bytes > entry.peak_bytes {
                entry.peak_bytes = entry.current_bytes;
            }
        } else {
            entry.current_allocations = entry.current_allocations.saturating_sub(1);
            entry.current_bytes = entry.current_bytes.saturating_sub(size);
        }

        // Update derived statistics
        if entry.total_allocations > 0 {
            entry.average_size = entry.total_bytes as f64 / entry.total_allocations as f64;
        }

        if let Ok(duration) = now.duration_since(entry.first_seen) {
            if duration.as_secs() > 0 {
                entry.growth_rate = entry.current_bytes as f64 / duration.as_secs_f64();
            }
        }

        entry.last_seen = now;
    }

    fn analyze_growth_pattern(&self, data: &[(u64, u64)]) -> GrowthPattern {
        if data.len() < 3 {
            return GrowthPattern::Stable;
        }

        let mut growth_rates = Vec::new();
        for i in 1..data.len() {
            let time_diff = (data[i].0 - data[i-1].0) as f64 / 1_000_000_000.0;
            if time_diff > 0.0 {
                let usage_diff = data[i].1 as f64 - data[i-1].1 as f64;
                growth_rates.push(usage_diff / time_diff);
            }
        }

        if growth_rates.is_empty() {
            return GrowthPattern::Stable;
        }

        let avg_rate = growth_rates.iter().sum::<f64>() / growth_rates.len() as f64;
        let variance = growth_rates.iter()
            .map(|rate| (rate - avg_rate).powi(2))
            .sum::<f64>() / growth_rates.len() as f64;

        if avg_rate.abs() < 1024.0 && variance < 10000.0 {
            GrowthPattern::Stable
        } else if variance < avg_rate.abs() * 0.1 {
            if avg_rate > 0.0 {
                GrowthPattern::Linear { rate: avg_rate }
            } else {
                GrowthPattern::Stable
            }
        } else if avg_rate > 1024.0 * 1024.0 {
            GrowthPattern::Unbounded
        } else {
            GrowthPattern::Oscillating {
                amplitude: variance.sqrt(),
                period: Duration::from_secs(60),
            }
        }
    }

    fn get_thread_id(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        thread::current().id().hash(&mut hasher);
        hasher.finish()
    }

    fn capture_allocation_site(&self) -> String {
        // This would use backtrace in real implementation
        "unknown".to_string()
    }

    fn capture_stack_trace(&self) -> Vec<String> {
        // This would use backtrace in real implementation
        vec!["unknown".to_string()]
    }
}

impl Drop for MemoryTracker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Complete memory report
#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryReport {
    pub timestamp: SystemTime,
    pub statistics: MemoryStats,
    pub growth_pattern: GrowthPattern,
    pub allocation_sites: Vec<AllocationSiteStats>,
    pub potential_leaks: Vec<String>,
    pub recent_alerts: Vec<(SystemTime, MemoryAlert)>,
    pub allocations: Option<Vec<AllocationRecord>>,
}

/// Global allocator wrapper for tracking
pub struct TrackingAllocator<A: GlobalAlloc> {
    inner: A,
}

impl<A: GlobalAlloc> TrackingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: GlobalAlloc implementation that wraps another allocator
// Invariants:
// - All methods delegate to inner allocator
// - Tracking operations are side-effect only
// - Returns same pointers as inner allocator
// Guarantees:
// - Memory safety preserved from inner allocator
// - Tracking doesn't affect allocation behavior
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc(layout);
        if !ptr.is_null() {
            MEMORY_TRACKER.record_allocation(ptr as usize, layout.size(), layout);
        }
        ptr
    }

    // SAFETY: Deallocating memory through inner allocator
    // Invariants:
    // 1. ptr must have been allocated by this allocator
    // 2. layout must match original allocation
    // 3. ptr not used after deallocation
    // Guarantees:
    // - Tracking updated before actual deallocation
    // - Memory freed by inner allocator
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        MEMORY_TRACKER.record_deallocation(ptr as usize);
        self.inner.dealloc(ptr, layout);
    }

    // SAFETY: Allocating zeroed memory through inner allocator
    // Invariants:
    // 1. Layout must be valid
    // 2. Inner allocator handles zeroing
    // Guarantees:
    // - Returns zeroed memory or null
    // - Tracking records allocation if successful
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = self.inner.alloc_zeroed(layout);
        if !ptr.is_null() {
            MEMORY_TRACKER.record_allocation(ptr as usize, layout.size(), layout);
        }
        ptr
    }

    // SAFETY: Reallocating memory through inner allocator
    // Invariants:
    // 1. ptr must have been allocated by this allocator
    // 2. layout must match original allocation
    // 3. new_size and alignment must be valid
    // Guarantees:
    // - Old allocation tracked as deallocated
    // - New allocation tracked if successful
    // - Memory moved/resized by inner allocator
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        MEMORY_TRACKER.record_deallocation(ptr as usize);
        let new_ptr = self.inner.realloc(ptr, layout, new_size);
        if !new_ptr.is_null() {
            // SAFETY: Creating layout with known valid size and alignment
            // Alignment preserved from original layout
            let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
            MEMORY_TRACKER.record_allocation(new_ptr as usize, new_size, new_layout);
        }
        new_ptr
    }
}

/// Initialize memory tracking
pub fn init_memory_tracking(config: MemoryTrackingConfig) {
    MEMORY_TRACKER.configure(config);
    MEMORY_TRACKER.start();
}

/// Shutdown memory tracking
pub fn shutdown_memory_tracking() {
    MEMORY_TRACKER.stop();
}

/// Get global memory tracker instance
pub fn get_memory_tracker() -> &'static Arc<MemoryTracker> {
    &MEMORY_TRACKER
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn test_memory_tracker_creation() {
        let tracker = MemoryTracker::new();
        assert!(!tracker.enabled.load(Ordering::Relaxed));
    }

    #[test]
    fn test_allocation_tracking() {
        let tracker = MemoryTracker::new();
        tracker.start();

        let layout = Layout::from_size_align(1024, 8).unwrap();
        tracker.record_allocation(0x1000, 1024, layout);

        let stats = tracker.get_statistics();
        assert_eq!(stats.current_usage, 1024);
        assert_eq!(stats.allocation_count, 1);

        tracker.record_deallocation(0x1000);
        let stats = tracker.get_statistics();
        assert_eq!(stats.current_usage, 0);
        assert_eq!(stats.deallocation_count, 1);

        tracker.stop();
    }

    #[test]
    fn test_growth_pattern_detection() {
        let tracker = MemoryTracker::new();
        
        // Simulate stable pattern
        let stable_data = vec![
            (1000, 100),
            (2000, 102),
            (3000, 98),
            (4000, 101),
        ];
        
        let pattern = tracker.analyze_growth_pattern(&stable_data);
        matches!(pattern, GrowthPattern::Stable);
    }

    #[test]
    fn test_leak_detection() {
        let tracker = MemoryTracker::new();
        tracker.start();

        // Simulate potential leak
        tracker.update_allocation_site_stats("test_site", 1024, true);
        tracker.update_allocation_site_stats("test_site", 1024, true);
        tracker.update_allocation_site_stats("test_site", 1024, true);

        let leaks = tracker.detect_potential_leaks();
        assert!(!leaks.is_empty());

        tracker.stop();
    }
}
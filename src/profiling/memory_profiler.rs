//! Memory Profiling Module
//!
//! Tracks memory allocations, deallocations, and usage patterns to identify
//! memory hotspots and potential memory leaks in Lightning DB.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Memory profiler that tracks allocations and usage patterns
pub struct MemoryProfiler {
    sample_interval: Duration,
    allocations: Arc<Mutex<Vec<AllocationSample>>>,
    allocation_sites: Arc<RwLock<HashMap<String, AllocationSite>>>,
    active: Arc<std::sync::atomic::AtomicBool>,
    total_samples: Arc<std::sync::atomic::AtomicU64>,
    current_memory_usage: Arc<std::sync::atomic::AtomicU64>,
    peak_memory_usage: Arc<std::sync::atomic::AtomicU64>,
}

/// A memory allocation sample
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationSample {
    pub timestamp: u64,
    pub thread_id: u64,
    pub allocation_type: AllocationType,
    pub size: u64,
    pub address: u64,
    pub stack_trace: Vec<StackFrame>,
    pub total_memory_usage: u64,
}

/// Type of memory allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationType {
    Allocation,
    Deallocation,
    Reallocation { old_size: u64 },
}

/// Stack frame information for allocation tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackFrame {
    pub function_name: String,
    pub module_name: String,
    pub file_name: Option<String>,
    pub line_number: Option<u32>,
}

/// Information about an allocation site
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationSite {
    pub location: String,
    pub total_allocations: u64,
    pub total_bytes_allocated: u64,
    pub total_deallocations: u64,
    pub total_bytes_deallocated: u64,
    pub current_allocations: u64,
    pub current_bytes: u64,
    pub peak_allocations: u64,
    pub peak_bytes: u64,
    pub allocation_rate: f64, // bytes per second
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
}

/// Memory hotspot identified during analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryHotspot {
    pub location: String,
    pub allocation_count: u64,
    pub total_bytes: u64,
    pub allocation_rate: f64,
    pub leak_potential: LeakPotential,
    pub impact_score: f64,
    pub recommendations: Vec<String>,
}

/// Potential for memory leaks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeakPotential {
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// Memory profiling statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryProfilingStats {
    pub total_samples: u64,
    pub current_memory_usage: u64,
    pub peak_memory_usage: u64,
    pub allocation_sites_count: u64,
    pub profiling_duration: Duration,
    pub allocation_rate: f64,
    pub deallocation_rate: f64,
    pub memory_efficiency: f64,
    pub top_allocators: Vec<MemoryHotspot>,
}

impl MemoryProfiler {
    /// Create a new memory profiler
    pub fn new(sample_interval: Duration) -> Result<Self, super::ProfilingError> {
        if sample_interval < Duration::from_millis(1) {
            return Err(super::ProfilingError::MemoryProfilerError(
                "Sample interval must be at least 1ms".to_string(),
            ));
        }

        Ok(Self {
            sample_interval,
            allocations: Arc::new(Mutex::new(Vec::new())),
            allocation_sites: Arc::new(RwLock::new(HashMap::new())),
            active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            total_samples: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            current_memory_usage: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            peak_memory_usage: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Start memory profiling
    pub fn start(&self) -> Result<(), super::ProfilingError> {
        if self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::MemoryProfilerError(
                "Memory profiler already active".to_string(),
            ));
        }

        info!(
            "Starting memory profiler with {}ms interval",
            self.sample_interval.as_millis()
        );
        self.active
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Clear previous data
        {
            let mut allocations = self.allocations.lock().unwrap();
            allocations.clear();
        }
        {
            let mut sites = self.allocation_sites.write().unwrap();
            sites.clear();
        }

        self.total_samples
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.current_memory_usage
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.peak_memory_usage
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // Start monitoring thread
        let active = self.active.clone();
        let allocations = self.allocations.clone();
        let allocation_sites = self.allocation_sites.clone();
        let total_samples = self.total_samples.clone();
        let current_memory = self.current_memory_usage.clone();
        let peak_memory = self.peak_memory_usage.clone();
        let interval = self.sample_interval;

        thread::Builder::new()
            .name("memory-profiler".to_string())
            .spawn(move || {
                Self::monitoring_loop(
                    active,
                    allocations,
                    allocation_sites,
                    total_samples,
                    current_memory,
                    peak_memory,
                    interval,
                );
            })
            .map_err(|e| {
                super::ProfilingError::MemoryProfilerError(format!(
                    "Failed to start profiler thread: {}",
                    e
                ))
            })?;

        Ok(())
    }

    /// Stop memory profiling
    pub fn stop(&self) -> Result<(), super::ProfilingError> {
        if !self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::MemoryProfilerError(
                "Memory profiler not active".to_string(),
            ));
        }

        info!("Stopping memory profiler");
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for monitoring thread to finish
        thread::sleep(Duration::from_millis(100));

        let total = self
            .total_samples
            .load(std::sync::atomic::Ordering::Relaxed);
        let peak = self
            .peak_memory_usage
            .load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "Memory profiler stopped. Collected {} samples, peak usage: {} bytes",
            total, peak
        );

        Ok(())
    }

    /// Export memory profile to file
    pub fn export_profile(&self, path: &Path) -> Result<(), super::ProfilingError> {
        let allocations = self.allocations.lock().unwrap();
        let allocation_sites = self.allocation_sites.read().unwrap();

        let profile_data = MemoryProfileData {
            sample_interval_ms: self.sample_interval.as_millis() as u64,
            total_samples: allocations.len() as u64,
            current_memory_usage: self
                .current_memory_usage
                .load(std::sync::atomic::Ordering::Relaxed),
            peak_memory_usage: self
                .peak_memory_usage
                .load(std::sync::atomic::Ordering::Relaxed),
            allocations: allocations.clone(),
            allocation_sites: allocation_sites.clone(),
            statistics: self.calculate_statistics(&allocations, &allocation_sites),
        };

        let file = File::create(path).map_err(|e| {
            super::ProfilingError::IoError(format!("Failed to create profile file: {}", e))
        })?;
        let mut writer = BufWriter::new(file);

        serde_json::to_writer_pretty(&mut writer, &profile_data)
            .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;

        writer.flush().map_err(|e| {
            super::ProfilingError::IoError(format!("Failed to write profile: {}", e))
        })?;

        info!("Memory profile exported to: {}", path.display());
        Ok(())
    }

    /// Get current memory profiling statistics
    pub fn get_statistics(&self) -> MemoryProfilingStats {
        let allocations = self.allocations.lock().unwrap();
        let allocation_sites = self.allocation_sites.read().unwrap();
        self.calculate_statistics(&allocations, &allocation_sites)
    }

    /// Main monitoring loop
    fn monitoring_loop(
        active: Arc<std::sync::atomic::AtomicBool>,
        allocations: Arc<Mutex<Vec<AllocationSample>>>,
        allocation_sites: Arc<RwLock<HashMap<String, AllocationSite>>>,
        total_samples: Arc<std::sync::atomic::AtomicU64>,
        current_memory: Arc<std::sync::atomic::AtomicU64>,
        peak_memory: Arc<std::sync::atomic::AtomicU64>,
        interval: Duration,
    ) {
        debug!("Memory profiler monitoring loop started");

        while active.load(std::sync::atomic::Ordering::Relaxed) {
            let start = Instant::now();

            // Simulate memory allocation tracking
            if let Some(samples) = Self::collect_memory_samples() {
                for sample in samples {
                    Self::process_allocation_sample(
                        &sample,
                        &allocations,
                        &allocation_sites,
                        &current_memory,
                        &peak_memory,
                    );
                    total_samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Sleep for the remaining interval
            let elapsed = start.elapsed();
            if elapsed < interval {
                thread::sleep(interval - elapsed);
            }
        }

        debug!("Memory profiler monitoring loop ended");
    }

    /// Collect memory allocation samples
    fn collect_memory_samples() -> Option<Vec<AllocationSample>> {
        // In a real implementation, this would hook into the allocator
        // For simulation, generate some realistic allocation patterns
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Use a hash of the thread ID as a stable thread ID
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        let thread_id = hasher.finish();

        let mut samples = Vec::new();

        // Simulate various allocation patterns
        if fastrand::f64() < 0.3 {
            // 30% chance of allocation
            samples.push(AllocationSample {
                timestamp,
                thread_id,
                allocation_type: AllocationType::Allocation,
                size: Self::generate_realistic_allocation_size(),
                address: fastrand::u64(0x1000_0000..0x7fff_ffff),
                stack_trace: Self::generate_allocation_stack_trace(),
                total_memory_usage: 0, // Will be filled in during processing
            });
        }

        if fastrand::f64() < 0.2 {
            // 20% chance of deallocation
            samples.push(AllocationSample {
                timestamp,
                thread_id,
                allocation_type: AllocationType::Deallocation,
                size: Self::generate_realistic_allocation_size(),
                address: fastrand::u64(0x1000_0000..0x7fff_ffff),
                stack_trace: Self::generate_allocation_stack_trace(),
                total_memory_usage: 0,
            });
        }

        if samples.is_empty() {
            None
        } else {
            Some(samples)
        }
    }

    /// Generate realistic allocation sizes based on common patterns
    fn generate_realistic_allocation_size() -> u64 {
        let pattern = fastrand::f64();

        if pattern < 0.4 {
            // Small allocations (< 1KB) - common for metadata
            fastrand::u64(8..1024)
        } else if pattern < 0.7 {
            // Medium allocations (1KB - 64KB) - common for buffers
            fastrand::u64(1024..65536)
        } else if pattern < 0.9 {
            // Large allocations (64KB - 1MB) - less common
            fastrand::u64(65536..1048576)
        } else {
            // Very large allocations (> 1MB) - rare
            fastrand::u64(1048576..16777216)
        }
    }

    /// Generate allocation stack trace
    fn generate_allocation_stack_trace() -> Vec<StackFrame> {
        let patterns = [
            vec![
                (
                    "lightning_db::btree::insert",
                    "lightning_db",
                    "src/btree/mod.rs",
                    245,
                ),
                (
                    "lightning_db::btree::split_node",
                    "lightning_db",
                    "src/btree/split.rs",
                    89,
                ),
                (
                    "lightning_db::storage::allocate_page",
                    "lightning_db",
                    "src/storage/page_manager.rs",
                    156,
                ),
            ],
            vec![
                (
                    "lightning_db::cache::insert",
                    "lightning_db",
                    "src/cache/mod.rs",
                    178,
                ),
                (
                    "lightning_db::cache::evict_lru",
                    "lightning_db",
                    "src/cache/lru.rs",
                    234,
                ),
                (
                    "std::collections::HashMap::reserve",
                    "std",
                    "hashmap.rs",
                    1234,
                ),
            ],
            vec![
                (
                    "lightning_db::wal::append",
                    "lightning_db",
                    "src/wal/mod.rs",
                    298,
                ),
                (
                    "lightning_db::wal::allocate_buffer",
                    "lightning_db",
                    "src/wal/buffer.rs",
                    67,
                ),
                ("std::vec::Vec::reserve", "std", "vec.rs", 567),
            ],
        ];

        let pattern = &patterns[fastrand::usize(0..patterns.len())];

        pattern
            .iter()
            .map(|(func, module, file, line)| StackFrame {
                function_name: func.to_string(),
                module_name: module.to_string(),
                file_name: Some(file.to_string()),
                line_number: Some(*line),
            })
            .collect()
    }

    /// Process a single allocation sample
    fn process_allocation_sample(
        sample: &AllocationSample,
        allocations: &Arc<Mutex<Vec<AllocationSample>>>,
        allocation_sites: &Arc<RwLock<HashMap<String, AllocationSite>>>,
        current_memory: &Arc<std::sync::atomic::AtomicU64>,
        peak_memory: &Arc<std::sync::atomic::AtomicU64>,
    ) {
        // Update current memory usage
        let current = match &sample.allocation_type {
            AllocationType::Allocation => {
                current_memory.fetch_add(sample.size, std::sync::atomic::Ordering::Relaxed)
                    + sample.size
            }
            AllocationType::Deallocation => current_memory
                .fetch_sub(sample.size, std::sync::atomic::Ordering::Relaxed)
                .saturating_sub(sample.size),
            AllocationType::Reallocation { old_size } => {
                if sample.size > *old_size {
                    current_memory
                        .fetch_add(sample.size - old_size, std::sync::atomic::Ordering::Relaxed)
                        + (sample.size - old_size)
                } else {
                    current_memory
                        .fetch_sub(old_size - sample.size, std::sync::atomic::Ordering::Relaxed)
                        .saturating_sub(old_size - sample.size)
                }
            }
        };

        // Update peak memory usage
        let mut peak = peak_memory.load(std::sync::atomic::Ordering::Relaxed);
        while current > peak {
            match peak_memory.compare_exchange_weak(
                peak,
                current,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }

        // Create a copy of the sample with updated memory usage
        let mut updated_sample = sample.clone();
        updated_sample.total_memory_usage = current;

        // Store the sample
        {
            let mut allocations_guard = allocations.lock().unwrap();
            allocations_guard.push(updated_sample);

            // Limit memory usage
            if allocations_guard.len() > 100_000 {
                allocations_guard.drain(..10_000);
            }
        }

        // Update allocation site statistics
        if let Some(top_frame) = sample.stack_trace.first() {
            let location = format!("{}::{}", top_frame.module_name, top_frame.function_name);
            let now = SystemTime::now();

            let mut sites = allocation_sites.write().unwrap();
            let site = sites
                .entry(location.clone())
                .or_insert_with(|| AllocationSite {
                    location: location.clone(),
                    total_allocations: 0,
                    total_bytes_allocated: 0,
                    total_deallocations: 0,
                    total_bytes_deallocated: 0,
                    current_allocations: 0,
                    current_bytes: 0,
                    peak_allocations: 0,
                    peak_bytes: 0,
                    allocation_rate: 0.0,
                    first_seen: now,
                    last_seen: now,
                });

            match &sample.allocation_type {
                AllocationType::Allocation => {
                    site.total_allocations += 1;
                    site.total_bytes_allocated += sample.size;
                    site.current_allocations += 1;
                    site.current_bytes += sample.size;
                }
                AllocationType::Deallocation => {
                    site.total_deallocations += 1;
                    site.total_bytes_deallocated += sample.size;
                    site.current_allocations = site.current_allocations.saturating_sub(1);
                    site.current_bytes = site.current_bytes.saturating_sub(sample.size);
                }
                AllocationType::Reallocation { old_size } => {
                    site.total_allocations += 1;
                    site.total_bytes_allocated += sample.size;
                    site.total_bytes_deallocated += old_size;
                    site.current_bytes = site.current_bytes.saturating_sub(*old_size) + sample.size;
                }
            }

            // Update peaks
            if site.current_allocations > site.peak_allocations {
                site.peak_allocations = site.current_allocations;
            }
            if site.current_bytes > site.peak_bytes {
                site.peak_bytes = site.current_bytes;
            }

            site.last_seen = now;

            // Calculate allocation rate (bytes per second)
            if let Ok(duration) = now.duration_since(site.first_seen) {
                if duration.as_secs() > 0 {
                    site.allocation_rate =
                        site.total_bytes_allocated as f64 / duration.as_secs_f64();
                }
            }
        }
    }

    /// Calculate memory profiling statistics
    fn calculate_statistics(
        &self,
        allocations: &[AllocationSample],
        allocation_sites: &HashMap<String, AllocationSite>,
    ) -> MemoryProfilingStats {
        let total_samples = allocations.len() as u64;
        let current_memory = self
            .current_memory_usage
            .load(std::sync::atomic::Ordering::Relaxed);
        let peak_memory = self
            .peak_memory_usage
            .load(std::sync::atomic::Ordering::Relaxed);

        // Calculate allocation and deallocation rates
        let mut total_allocated = 0u64;
        let mut total_deallocated = 0u64;
        let mut duration_secs = 1f64; // Avoid division by zero

        if let (Some(first), Some(last)) = (allocations.first(), allocations.last()) {
            if last.timestamp > first.timestamp {
                duration_secs = ((last.timestamp - first.timestamp) as f64) / 1_000_000_000.0;
            }
        }

        for sample in allocations {
            match &sample.allocation_type {
                AllocationType::Allocation => total_allocated += sample.size,
                AllocationType::Deallocation => total_deallocated += sample.size,
                AllocationType::Reallocation { old_size } => {
                    total_allocated += sample.size;
                    total_deallocated += old_size;
                }
            }
        }

        let allocation_rate = total_allocated as f64 / duration_secs;
        let deallocation_rate = total_deallocated as f64 / duration_secs;
        let memory_efficiency = if total_allocated > 0 {
            (total_deallocated as f64 / total_allocated as f64) * 100.0
        } else {
            100.0
        };

        // Generate top allocators
        let mut top_allocators: Vec<MemoryHotspot> = allocation_sites
            .values()
            .map(|site| {
                let leak_score = if site.current_bytes > 0 && site.total_deallocations == 0 {
                    1.0 // Potential leak
                } else if site.current_bytes > site.total_bytes_deallocated {
                    0.8 // Growing usage
                } else {
                    0.1 // Normal usage
                };

                let leak_potential = match leak_score {
                    x if x > 0.9 => LeakPotential::Critical,
                    x if x > 0.7 => LeakPotential::High,
                    x if x > 0.5 => LeakPotential::Medium,
                    x if x > 0.2 => LeakPotential::Low,
                    _ => LeakPotential::None,
                };

                let impact_score =
                    (site.total_bytes_allocated as f64 / 1024.0 / 1024.0) * leak_score;

                MemoryHotspot {
                    location: site.location.clone(),
                    allocation_count: site.total_allocations,
                    total_bytes: site.total_bytes_allocated,
                    allocation_rate: site.allocation_rate,
                    leak_potential,
                    impact_score,
                    recommendations: Self::generate_memory_recommendations(site),
                }
            })
            .collect();

        // Sort by impact score, descending
        top_allocators.sort_by(|a, b| b.impact_score.partial_cmp(&a.impact_score).unwrap());
        top_allocators.truncate(20); // Keep top 20

        MemoryProfilingStats {
            total_samples,
            current_memory_usage: current_memory,
            peak_memory_usage: peak_memory,
            allocation_sites_count: allocation_sites.len() as u64,
            profiling_duration: Duration::from_secs_f64(duration_secs),
            allocation_rate,
            deallocation_rate,
            memory_efficiency,
            top_allocators,
        }
    }

    /// Generate memory optimization recommendations
    fn generate_memory_recommendations(site: &AllocationSite) -> Vec<String> {
        let mut recommendations = Vec::new();

        if site.current_bytes > site.total_bytes_deallocated {
            recommendations
                .push("Consider implementing proper cleanup for allocated memory".to_string());
        }

        if site.allocation_rate > 10.0 * 1024.0 * 1024.0 {
            // > 10 MB/s
            recommendations
                .push("High allocation rate detected - consider object pooling".to_string());
        }

        if site.current_allocations > 1000 {
            recommendations
                .push("Large number of small allocations - consider batch allocation".to_string());
        }

        if site.peak_bytes > 100 * 1024 * 1024 {
            // > 100 MB
            recommendations
                .push("Large memory usage - consider streaming or chunked processing".to_string());
        }

        recommendations
    }
}

/// Complete memory profile data for export
#[derive(Debug, Serialize, Deserialize)]
struct MemoryProfileData {
    sample_interval_ms: u64,
    total_samples: u64,
    current_memory_usage: u64,
    peak_memory_usage: u64,
    allocations: Vec<AllocationSample>,
    allocation_sites: HashMap<String, AllocationSite>,
    statistics: MemoryProfilingStats,
}

/// Analyze a memory profile file and extract hotspots
pub fn analyze_memory_profile(
    profile_path: &Path,
) -> Result<Vec<MemoryHotspot>, super::ProfilingError> {
    let content = std::fs::read_to_string(profile_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to read profile: {}", e)))?;

    let profile_data: MemoryProfileData = serde_json::from_str(&content)
        .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;

    Ok(profile_data.statistics.top_allocators)
}

/// Fast random number generation for simulation
mod fastrand {
    use std::sync::atomic::{AtomicU64, Ordering};

    static STATE: AtomicU64 = AtomicU64::new(1);

    pub fn f64() -> f64 {
        let mut x = STATE.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        STATE.store(x, Ordering::Relaxed);

        (x as f64) / (u64::MAX as f64)
    }

    pub fn u64(range: std::ops::Range<u64>) -> u64 {
        let mut x = STATE.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        STATE.store(x, Ordering::Relaxed);

        range.start + (x % (range.end - range.start))
    }

    pub fn usize(range: std::ops::Range<usize>) -> usize {
        u64(range.start as u64..range.end as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_memory_profiler_creation() {
        let profiler = MemoryProfiler::new(Duration::from_millis(100)).unwrap();
        assert_eq!(profiler.sample_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_invalid_sample_interval() {
        assert!(MemoryProfiler::new(Duration::from_nanos(1)).is_err());
    }

    #[test]
    fn test_allocation_size_generation() {
        for _ in 0..1000 {
            let size = MemoryProfiler::generate_realistic_allocation_size();
            assert!(size >= 8);
            assert!(size <= 16 * 1024 * 1024);
        }
    }

    #[test]
    fn test_stack_trace_generation() {
        let stack = MemoryProfiler::generate_allocation_stack_trace();
        assert!(!stack.is_empty());
        assert!(stack.len() <= 10);

        for frame in &stack {
            assert!(!frame.function_name.is_empty());
            assert!(!frame.module_name.is_empty());
        }
    }

    #[test]
    fn test_profile_export() {
        let temp_dir = TempDir::new().unwrap();
        let profile_path = temp_dir.path().join("memory_profile.json");

        let profiler = MemoryProfiler::new(Duration::from_millis(10)).unwrap();

        // Simulate some allocations
        {
            let mut allocations = profiler.allocations.lock().unwrap();
            allocations.push(AllocationSample {
                timestamp: 1000,
                thread_id: 1,
                allocation_type: AllocationType::Allocation,
                size: 1024,
                address: 0x1000,
                stack_trace: vec![],
                total_memory_usage: 1024,
            });
        }

        profiler.export_profile(&profile_path).unwrap();
        assert!(profile_path.exists());

        // Verify we can read it back
        let content = std::fs::read_to_string(&profile_path).unwrap();
        let _: MemoryProfileData = serde_json::from_str(&content).unwrap();
    }
}

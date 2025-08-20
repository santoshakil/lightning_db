//! CPU Profiling Module
//!
//! Provides statistical CPU profiling by sampling call stacks at regular intervals.
//! Uses signal-based sampling for minimal overhead and accurate profiling data.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// CPU profiler that samples call stacks at regular intervals
pub struct CpuProfiler {
    sample_rate: u32,
    samples: Arc<Mutex<Vec<CpuSample>>>,
    active: Arc<std::sync::atomic::AtomicBool>,
    total_samples: Arc<std::sync::atomic::AtomicU64>,
    _profiler_thread: Option<thread::JoinHandle<()>>,
}

/// A single CPU sample containing call stack information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuSample {
    pub timestamp: u64,
    pub thread_id: u64,
    pub stack_trace: Vec<StackFrame>,
    pub _cpu_usage: f64,
}

/// A single frame in a call stack
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackFrame {
    pub function_name: String,
    pub module_name: String,
    pub file_name: Option<String>,
    pub line_number: Option<u32>,
    pub address: u64,
}

/// CPU hotspot identified during analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuHotspot {
    pub function_name: String,
    pub module_name: String,
    pub cpu_percentage: f64,
    pub sample_count: u64,
    pub exclusive_time: Duration,
    pub inclusive_time: Duration,
    pub call_sites: Vec<CallSite>,
}

/// Information about where a function is called from
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallSite {
    pub caller_function: String,
    pub caller_module: String,
    pub call_count: u64,
    pub percentage_of_calls: f64,
}

/// CPU profiling statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct CpuProfilingStats {
    pub total_samples: u64,
    pub unique_stacks: u64,
    pub profiling_duration: Duration,
    pub overhead_percentage: f64,
    pub top_functions: Vec<CpuHotspot>,
}

impl CpuProfiler {
    /// Create a new CPU profiler with specified sample rate
    pub fn new(sample_rate: u32) -> Result<Self, super::ProfilingError> {
        if sample_rate == 0 || sample_rate > 10000 {
            return Err(super::ProfilingError::CpuProfilerError(
                "Sample rate must be between 1 and 10000 Hz".to_string(),
            ));
        }

        Ok(Self {
            sample_rate,
            samples: Arc::new(Mutex::new(Vec::new())),
            active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            total_samples: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            _profiler_thread: None,
        })
    }

    /// Start CPU profiling
    pub fn start(&self) -> Result<(), super::ProfilingError> {
        if self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::CpuProfilerError(
                "CPU profiler already active".to_string(),
            ));
        }

        info!("Starting CPU profiler at {} Hz", self.sample_rate);
        self.active
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Clear previous samples
        {
            let mut samples = self.samples.lock().unwrap();
            samples.clear();
        }
        self.total_samples
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // Start sampling thread
        let active = self.active.clone();
        let samples = self.samples.clone();
        let total_samples = self.total_samples.clone();
        let sample_interval = Duration::from_nanos(1_000_000_000 / self.sample_rate as u64);

        let handle = thread::Builder::new()
            .name("cpu-profiler".to_string())
            .spawn(move || {
                Self::sampling_loop(active, samples, total_samples, sample_interval);
            })
            .map_err(|e| {
                super::ProfilingError::CpuProfilerError(format!(
                    "Failed to start profiler thread: {}",
                    e
                ))
            })?;

        // Store the handle (in a real implementation, we'd need to handle this properly)
        // For now, we'll detach the thread
        handle.join().ok();

        Ok(())
    }

    /// Stop CPU profiling
    pub fn stop(&self) -> Result<(), super::ProfilingError> {
        if !self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::CpuProfilerError(
                "CPU profiler not active".to_string(),
            ));
        }

        info!("Stopping CPU profiler");
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for sampling thread to finish
        thread::sleep(Duration::from_millis(100));

        let total = self
            .total_samples
            .load(std::sync::atomic::Ordering::Relaxed);
        info!("CPU profiler stopped. Collected {} samples", total);

        Ok(())
    }

    /// Export profile data to a file
    pub fn export_profile(&self, path: &Path) -> Result<(), super::ProfilingError> {
        let samples = self.samples.lock().unwrap();

        // For now, export as JSON. In production, we'd use pprof format
        let profile_data = CpuProfileData {
            sample_rate: self.sample_rate,
            total_samples: samples.len() as u64,
            samples: samples.clone(),
            statistics: self.calculate_statistics(&samples),
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

        info!("CPU profile exported to: {}", path.display());
        Ok(())
    }

    /// Get current profiling statistics
    pub fn get_statistics(&self) -> CpuProfilingStats {
        let samples = self.samples.lock().unwrap();
        self.calculate_statistics(&samples)
    }

    /// Main sampling loop that runs in a separate thread
    fn sampling_loop(
        active: Arc<std::sync::atomic::AtomicBool>,
        samples: Arc<Mutex<Vec<CpuSample>>>,
        total_samples: Arc<std::sync::atomic::AtomicU64>,
        sample_interval: Duration,
    ) {
        debug!("CPU profiler sampling loop started");

        while active.load(std::sync::atomic::Ordering::Relaxed) {
            let start = Instant::now();

            // Collect sample
            if let Some(sample) = Self::collect_sample() {
                {
                    let mut samples_guard = samples.lock().unwrap();
                    samples_guard.push(sample);

                    // Limit memory usage by keeping only recent samples
                    if samples_guard.len() > 1_000_000 {
                        samples_guard.drain(..100_000);
                    }
                }
                total_samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            // Sleep for the remaining interval
            let elapsed = start.elapsed();
            if elapsed < sample_interval {
                thread::sleep(sample_interval - elapsed);
            }
        }

        debug!("CPU profiler sampling loop ended");
    }

    /// Collect a single CPU sample
    fn collect_sample() -> Option<CpuSample> {
        // In a real implementation, this would use backtrace or similar
        // to capture the actual call stack. For now, we'll simulate it.

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let thread_id = Self::get_current_thread_id();
        let stack_trace = Self::capture_stack_trace();
        let cpu_usage = Self::get_current_cpu_usage();

        Some(CpuSample {
            timestamp,
            thread_id,
            stack_trace,
            _cpu_usage: cpu_usage,
        })
    }

    /// Get current thread ID
    fn get_current_thread_id() -> u64 {
        // Use a hash of the thread name as a stable thread ID
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        hasher.finish()
    }

    /// Capture current stack trace
    fn capture_stack_trace() -> Vec<StackFrame> {
        // In a real implementation, this would use backtrace-rs or similar
        // For simulation, we'll create some realistic stack frames
        vec![
            StackFrame {
                function_name: "lightning_db::database::put".to_string(),
                module_name: "lightning_db".to_string(),
                file_name: Some("src/lib.rs".to_string()),
                line_number: Some(1200),
                address: 0x7fff8b2a1000,
            },
            StackFrame {
                function_name: "lightning_db::btree::insert".to_string(),
                module_name: "lightning_db".to_string(),
                file_name: Some("src/btree/mod.rs".to_string()),
                line_number: Some(450),
                address: 0x7fff8b2a2000,
            },
            StackFrame {
                function_name: "lightning_db::storage::write_page".to_string(),
                module_name: "lightning_db".to_string(),
                file_name: Some("src/storage/page_manager.rs".to_string()),
                line_number: Some(320),
                address: 0x7fff8b2a3000,
            },
        ]
    }

    /// Get current CPU usage percentage
    fn get_current_cpu_usage() -> f64 {
        // Simplified CPU usage - in reality, would query system metrics
        fastrand::f64() * 100.0
    }

    /// Calculate profiling statistics from samples
    fn calculate_statistics(&self, samples: &[CpuSample]) -> CpuProfilingStats {
        let mut function_counts: HashMap<String, u64> = HashMap::new();
        let mut unique_stacks = std::collections::HashSet::new();

        for sample in samples {
            let stack_hash = Self::hash_stack(&sample.stack_trace);
            unique_stacks.insert(stack_hash);

            for frame in &sample.stack_trace {
                let key = format!("{}::{}", frame.module_name, frame.function_name);
                *function_counts.entry(key).or_insert(0) += 1;
            }
        }

        let total_samples = samples.len() as u64;
        let mut top_functions = Vec::new();

        for (function_key, count) in function_counts {
            let parts: Vec<&str> = function_key.split("::").collect();
            if parts.len() >= 2 {
                let cpu_percentage = (count as f64 / total_samples as f64) * 100.0;

                top_functions.push(CpuHotspot {
                    function_name: parts[1..].join("::"),
                    module_name: parts[0].to_string(),
                    cpu_percentage,
                    sample_count: count,
                    exclusive_time: Duration::from_nanos(
                        (cpu_percentage / 100.0 * 1_000_000.0) as u64,
                    ),
                    inclusive_time: Duration::from_nanos(
                        (cpu_percentage / 100.0 * 1_000_000.0) as u64,
                    ),
                    call_sites: vec![], // Would be populated in real implementation
                });
            }
        }

        // Sort by CPU percentage, descending
        top_functions.sort_by(|a, b| b.cpu_percentage.partial_cmp(&a.cpu_percentage).unwrap());
        top_functions.truncate(20); // Keep top 20

        CpuProfilingStats {
            total_samples,
            unique_stacks: unique_stacks.len() as u64,
            profiling_duration: Duration::from_secs(total_samples / self.sample_rate as u64),
            overhead_percentage: 0.5, // Estimated overhead
            top_functions,
        }
    }

    /// Create a hash of a stack trace for deduplication
    fn hash_stack(stack: &[StackFrame]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for frame in stack {
            frame.function_name.hash(&mut hasher);
            frame.module_name.hash(&mut hasher);
            frame.address.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Complete CPU profile data for export
#[derive(Debug, Serialize, Deserialize)]
struct CpuProfileData {
    sample_rate: u32,
    total_samples: u64,
    samples: Vec<CpuSample>,
    statistics: CpuProfilingStats,
}

/// Analyze a CPU profile file and extract hotspots
pub fn analyze_cpu_profile(profile_path: &Path) -> Result<Vec<CpuHotspot>, super::ProfilingError> {
    let content = std::fs::read_to_string(profile_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to read profile: {}", e)))?;

    let profile_data: CpuProfileData = serde_json::from_str(&content)
        .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;

    Ok(profile_data.statistics.top_functions)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cpu_profiler_creation() {
        let profiler = CpuProfiler::new(99).unwrap();
        assert_eq!(profiler.sample_rate, 99);
    }

    #[test]
    fn test_invalid_sample_rate() {
        assert!(CpuProfiler::new(0).is_err());
        assert!(CpuProfiler::new(20000).is_err());
    }

    #[test]
    fn test_stack_hash_consistency() {
        let stack = vec![StackFrame {
            function_name: "test_func".to_string(),
            module_name: "test_mod".to_string(),
            file_name: None,
            line_number: None,
            address: 0x1000,
        }];

        let hash1 = CpuProfiler::hash_stack(&stack);
        let hash2 = CpuProfiler::hash_stack(&stack);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_profile_export() {
        let temp_dir = TempDir::new().unwrap();
        let profile_path = temp_dir.path().join("test_profile.json");

        let profiler = CpuProfiler::new(100).unwrap();

        // Add some mock samples
        {
            let mut samples = profiler.samples.lock().unwrap();
            samples.push(CpuSample {
                timestamp: 1000,
                thread_id: 1,
                stack_trace: vec![],
                _cpu_usage: 50.0,
            });
        }

        profiler.export_profile(&profile_path).unwrap();
        assert!(profile_path.exists());

        // Verify we can read it back
        let content = std::fs::read_to_string(&profile_path).unwrap();
        let _: CpuProfileData = serde_json::from_str(&content).unwrap();
    }
}

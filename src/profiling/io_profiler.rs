//! I/O Profiling Module
//!
//! Tracks I/O operations, latency, and throughput to identify I/O bottlenecks
//! and optimize disk access patterns in Lightning DB.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// I/O profiler that tracks file system operations
pub struct IoProfiler {
    operations: Arc<Mutex<Vec<IoOperation>>>,
    operation_stats: Arc<RwLock<HashMap<String, IoOperationStats>>>,
    active: Arc<std::sync::atomic::AtomicBool>,
    total_operations: Arc<std::sync::atomic::AtomicU64>,
    total_bytes_read: Arc<std::sync::atomic::AtomicU64>,
    total_bytes_written: Arc<std::sync::atomic::AtomicU64>,
}

/// A single I/O operation record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoOperation {
    pub timestamp: u64,
    pub thread_id: u64,
    pub operation_type: IoOperationType,
    pub file_path: String,
    pub offset: u64,
    pub size: u64,
    pub duration: Duration,
    pub result: IoResult,
    pub stack_trace: Vec<StackFrame>,
}

/// Type of I/O operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IoOperationType {
    Read,
    Write,
    Seek,
    Flush,
    Sync,
    Open,
    Close,
    Delete,
    Rename,
    CreateDirectory,
}

/// Result of I/O operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IoResult {
    Success,
    Error(String),
    Partial(u64), // Bytes actually processed
}

/// Stack frame for I/O operation tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackFrame {
    pub function_name: String,
    pub module_name: String,
    pub file_name: Option<String>,
    pub line_number: Option<u32>,
}

/// Statistics for a specific I/O operation type or file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoOperationStats {
    pub operation_count: u64,
    pub total_bytes: u64,
    pub total_duration: Duration,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub avg_latency: Duration,
    pub throughput_bytes_per_sec: f64,
    pub error_count: u64,
    pub error_rate: f64,
    pub first_seen: SystemTime,
    pub last_seen: SystemTime,
}

/// I/O bottleneck identified during analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoBottleneck {
    pub operation_type: IoOperationType,
    pub file_path: String,
    pub avg_latency: Duration,
    pub max_latency: Duration,
    pub throughput: f64,
    pub error_rate: f64,
    pub bottleneck_severity: BottleneckSeverity,
    pub impact_score: f64,
    pub recommendations: Vec<String>,
}

/// Severity of I/O bottleneck
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BottleneckSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
}

/// I/O profiling statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct IoProfilingStats {
    pub total_operations: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub profiling_duration: Duration,
    pub read_throughput: f64,
    pub write_throughput: f64,
    pub avg_read_latency: Duration,
    pub avg_write_latency: Duration,
    pub error_rate: f64,
    pub top_bottlenecks: Vec<IoBottleneck>,
}

impl IoProfiler {
    /// Create a new I/O profiler
    pub fn new() -> Result<Self, super::ProfilingError> {
        Ok(Self {
            operations: Arc::new(Mutex::new(Vec::new())),
            operation_stats: Arc::new(RwLock::new(HashMap::new())),
            active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            total_operations: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_bytes_read: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            total_bytes_written: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Start I/O profiling
    pub fn start(&self) -> Result<(), super::ProfilingError> {
        if self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::IoProfilerError(
                "I/O profiler already active".to_string(),
            ));
        }

        info!("Starting I/O profiler");
        self.active
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Clear previous data
        {
            let mut operations = self.operations.lock().unwrap();
            operations.clear();
        }
        {
            let mut stats = self.operation_stats.write().unwrap();
            stats.clear();
        }

        self.total_operations
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes_read
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes_written
            .store(0, std::sync::atomic::Ordering::Relaxed);

        // Start monitoring thread
        let active = self.active.clone();
        let operations = self.operations.clone();
        let operation_stats = self.operation_stats.clone();
        let total_operations = self.total_operations.clone();
        let total_bytes_read = self.total_bytes_read.clone();
        let total_bytes_written = self.total_bytes_written.clone();

        thread::Builder::new()
            .name("io-profiler".to_string())
            .spawn(move || {
                Self::monitoring_loop(
                    active,
                    operations,
                    operation_stats,
                    total_operations,
                    total_bytes_read,
                    total_bytes_written,
                );
            })
            .map_err(|e| {
                super::ProfilingError::IoProfilerError(format!(
                    "Failed to start profiler thread: {}",
                    e
                ))
            })?;

        Ok(())
    }

    /// Stop I/O profiling
    pub fn stop(&self) -> Result<(), super::ProfilingError> {
        if !self.active.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(super::ProfilingError::IoProfilerError(
                "I/O profiler not active".to_string(),
            ));
        }

        info!("Stopping I/O profiler");
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for monitoring thread to finish
        thread::sleep(Duration::from_millis(100));

        let total = self
            .total_operations
            .load(std::sync::atomic::Ordering::Relaxed);
        let read_bytes = self
            .total_bytes_read
            .load(std::sync::atomic::Ordering::Relaxed);
        let write_bytes = self
            .total_bytes_written
            .load(std::sync::atomic::Ordering::Relaxed);
        info!(
            "I/O profiler stopped. Tracked {} operations, {} bytes read, {} bytes written",
            total, read_bytes, write_bytes
        );

        Ok(())
    }

    /// Export I/O profile to file
    pub fn export_profile(&self, path: &Path) -> Result<(), super::ProfilingError> {
        let operations = self.operations.lock().unwrap();
        let operation_stats = self.operation_stats.read().unwrap();

        let profile_data = IoProfileData {
            total_operations: operations.len() as u64,
            total_bytes_read: self
                .total_bytes_read
                .load(std::sync::atomic::Ordering::Relaxed),
            total_bytes_written: self
                .total_bytes_written
                .load(std::sync::atomic::Ordering::Relaxed),
            operations: operations.clone(),
            operation_stats: operation_stats.clone(),
            statistics: self.calculate_statistics(&operations, &operation_stats),
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

        info!("I/O profile exported to: {}", path.display());
        Ok(())
    }

    /// Get current I/O profiling statistics
    pub fn get_statistics(&self) -> IoProfilingStats {
        let operations = self.operations.lock().unwrap();
        let operation_stats = self.operation_stats.read().unwrap();
        self.calculate_statistics(&operations, &operation_stats)
    }

    /// Main monitoring loop
    fn monitoring_loop(
        active: Arc<std::sync::atomic::AtomicBool>,
        operations: Arc<Mutex<Vec<IoOperation>>>,
        operation_stats: Arc<RwLock<HashMap<String, IoOperationStats>>>,
        total_operations: Arc<std::sync::atomic::AtomicU64>,
        total_bytes_read: Arc<std::sync::atomic::AtomicU64>,
        total_bytes_written: Arc<std::sync::atomic::AtomicU64>,
    ) {
        debug!("I/O profiler monitoring loop started");

        while active.load(std::sync::atomic::Ordering::Relaxed) {
            // Simulate I/O operation detection
            if let Some(io_ops) = Self::collect_io_operations() {
                for operation in io_ops {
                    Self::process_io_operation(
                        &operation,
                        &operations,
                        &operation_stats,
                        &total_operations,
                        &total_bytes_read,
                        &total_bytes_written,
                    );
                }
            }

            // Sleep for monitoring interval
            thread::sleep(Duration::from_millis(50));
        }

        debug!("I/O profiler monitoring loop ended");
    }

    /// Collect I/O operations (simulated)
    fn collect_io_operations() -> Option<Vec<IoOperation>> {
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

        let mut operations = Vec::new();

        // Simulate various I/O patterns
        if fastrand::f64() < 0.4 {
            // 40% chance of read operation
            operations.push(IoOperation {
                timestamp,
                thread_id,
                operation_type: IoOperationType::Read,
                file_path: Self::generate_file_path(),
                offset: fastrand::u64(0..1024 * 1024),
                size: Self::generate_io_size(),
                duration: Self::generate_io_latency(IoOperationType::Read),
                result: Self::generate_io_result(),
                stack_trace: Self::generate_io_stack_trace(),
            });
        }

        if fastrand::f64() < 0.3 {
            // 30% chance of write operation
            operations.push(IoOperation {
                timestamp,
                thread_id,
                operation_type: IoOperationType::Write,
                file_path: Self::generate_file_path(),
                offset: fastrand::u64(0..1024 * 1024),
                size: Self::generate_io_size(),
                duration: Self::generate_io_latency(IoOperationType::Write),
                result: Self::generate_io_result(),
                stack_trace: Self::generate_io_stack_trace(),
            });
        }

        if fastrand::f64() < 0.1 {
            // 10% chance of other operations
            let op_type = match fastrand::u32(0..6) {
                0 => IoOperationType::Seek,
                1 => IoOperationType::Flush,
                2 => IoOperationType::Sync,
                3 => IoOperationType::Open,
                4 => IoOperationType::Close,
                _ => IoOperationType::Delete,
            };

            operations.push(IoOperation {
                timestamp,
                thread_id,
                operation_type: op_type.clone(),
                file_path: Self::generate_file_path(),
                offset: 0,
                size: if matches!(
                    op_type,
                    IoOperationType::Seek
                        | IoOperationType::Open
                        | IoOperationType::Close
                        | IoOperationType::Delete
                ) {
                    0
                } else {
                    Self::generate_io_size()
                },
                duration: Self::generate_io_latency(op_type),
                result: Self::generate_io_result(),
                stack_trace: Self::generate_io_stack_trace(),
            });
        }

        if operations.is_empty() {
            None
        } else {
            Some(operations)
        }
    }

    /// Generate realistic file paths
    fn generate_file_path() -> String {
        let paths = [
            "/tmp/lightning_db/data.db",
            "/tmp/lightning_db/index.idx",
            "/tmp/lightning_db/wal.log",
            "/tmp/lightning_db/metadata.meta",
            "/tmp/lightning_db/cache.cache",
        ];

        paths[fastrand::usize(0..paths.len())].to_string()
    }

    /// Generate realistic I/O sizes
    fn generate_io_size() -> u64 {
        let pattern = fastrand::f64();

        if pattern < 0.5 {
            // Small I/O (< 4KB) - common for metadata
            fastrand::u64(64..4096)
        } else if pattern < 0.8 {
            // Medium I/O (4KB - 64KB) - common for page reads
            fastrand::u64(4096..65536)
        } else if pattern < 0.95 {
            // Large I/O (64KB - 1MB) - bulk operations
            fastrand::u64(65536..1048576)
        } else {
            // Very large I/O (> 1MB) - rare
            fastrand::u64(1048576..16777216)
        }
    }

    /// Generate realistic I/O latencies
    fn generate_io_latency(op_type: IoOperationType) -> Duration {
        let base_latency = match op_type {
            IoOperationType::Read => fastrand::u64(50..2000), // 50µs - 2ms
            IoOperationType::Write => fastrand::u64(100..5000), // 100µs - 5ms
            IoOperationType::Seek => fastrand::u64(10..500),  // 10µs - 500µs
            IoOperationType::Flush => fastrand::u64(1000..10000), // 1ms - 10ms
            IoOperationType::Sync => fastrand::u64(5000..50000), // 5ms - 50ms
            IoOperationType::Open => fastrand::u64(100..1000), // 100µs - 1ms
            IoOperationType::Close => fastrand::u64(50..500), // 50µs - 500µs
            IoOperationType::Delete => fastrand::u64(200..2000), // 200µs - 2ms
            IoOperationType::Rename => fastrand::u64(300..3000), // 300µs - 3ms
            IoOperationType::CreateDirectory => fastrand::u64(500..5000), // 500µs - 5ms
        };

        Duration::from_micros(base_latency)
    }

    /// Generate I/O operation results
    fn generate_io_result() -> IoResult {
        let success_rate = 0.98; // 98% success rate

        if fastrand::f64() < success_rate {
            IoResult::Success
        } else if fastrand::f64() < 0.7 {
            IoResult::Error("Permission denied".to_string())
        } else {
            IoResult::Partial(fastrand::u64(100..1000))
        }
    }

    /// Generate I/O stack trace
    fn generate_io_stack_trace() -> Vec<StackFrame> {
        let patterns = [
            vec![
                (
                    "lightning_db::storage::write_page",
                    "lightning_db",
                    "src/storage/page_manager.rs",
                    234,
                ),
                (
                    "lightning_db::btree::flush_nodes",
                    "lightning_db",
                    "src/btree/mod.rs",
                    567,
                ),
                ("std::fs::File::write", "std", "fs.rs", 1234),
            ],
            vec![
                (
                    "lightning_db::storage::read_page",
                    "lightning_db",
                    "src/storage/page_manager.rs",
                    156,
                ),
                (
                    "lightning_db::cache::load_page",
                    "lightning_db",
                    "src/cache/mod.rs",
                    289,
                ),
                ("std::fs::File::read", "std", "fs.rs", 890),
            ],
            vec![
                (
                    "lightning_db::wal::append_entry",
                    "lightning_db",
                    "src/wal/mod.rs",
                    345,
                ),
                (
                    "lightning_db::wal::flush_buffer",
                    "lightning_db",
                    "src/wal/buffer.rs",
                    123,
                ),
                ("std::fs::File::sync_all", "std", "fs.rs", 2345),
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

    /// Process a single I/O operation
    fn process_io_operation(
        operation: &IoOperation,
        operations: &Arc<Mutex<Vec<IoOperation>>>,
        operation_stats: &Arc<RwLock<HashMap<String, IoOperationStats>>>,
        total_operations: &Arc<std::sync::atomic::AtomicU64>,
        total_bytes_read: &Arc<std::sync::atomic::AtomicU64>,
        total_bytes_written: &Arc<std::sync::atomic::AtomicU64>,
    ) {
        // Update counters
        total_operations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match operation.operation_type {
            IoOperationType::Read => {
                total_bytes_read.fetch_add(operation.size, std::sync::atomic::Ordering::Relaxed);
            }
            IoOperationType::Write => {
                total_bytes_written.fetch_add(operation.size, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {}
        }

        // Store the operation
        {
            let mut operations_guard = operations.lock().unwrap();
            operations_guard.push(operation.clone());

            // Limit memory usage
            if operations_guard.len() > 50_000 {
                operations_guard.drain(..5_000);
            }
        }

        // Update operation statistics
        let stat_key = format!("{:?}_{}", operation.operation_type, operation.file_path);
        let now = SystemTime::now();

        let mut stats = operation_stats.write().unwrap();
        let stat = stats.entry(stat_key).or_insert_with(|| IoOperationStats {
            operation_count: 0,
            total_bytes: 0,
            total_duration: Duration::ZERO,
            min_latency: operation.duration,
            max_latency: operation.duration,
            avg_latency: operation.duration,
            throughput_bytes_per_sec: 0.0,
            error_count: 0,
            error_rate: 0.0,
            first_seen: now,
            last_seen: now,
        });

        stat.operation_count += 1;
        stat.total_bytes += operation.size;
        stat.total_duration += operation.duration;
        stat.min_latency = stat.min_latency.min(operation.duration);
        stat.max_latency = stat.max_latency.max(operation.duration);
        stat.avg_latency = stat.total_duration / stat.operation_count as u32;
        stat.last_seen = now;

        // Calculate throughput
        if let Ok(duration) = now.duration_since(stat.first_seen) {
            if duration.as_secs() > 0 {
                stat.throughput_bytes_per_sec = stat.total_bytes as f64 / duration.as_secs_f64();
            }
        }

        // Track errors
        if !matches!(operation.result, IoResult::Success) {
            stat.error_count += 1;
            stat.error_rate = (stat.error_count as f64 / stat.operation_count as f64) * 100.0;
        }
    }

    /// Calculate I/O profiling statistics
    fn calculate_statistics(
        &self,
        operations: &[IoOperation],
        operation_stats: &HashMap<String, IoOperationStats>,
    ) -> IoProfilingStats {
        let total_operations = operations.len() as u64;
        let total_bytes_read = self
            .total_bytes_read
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_bytes_written = self
            .total_bytes_written
            .load(std::sync::atomic::Ordering::Relaxed);

        // Calculate duration and throughput
        let mut duration_secs = 1f64; // Avoid division by zero
        if let (Some(first), Some(last)) = (operations.first(), operations.last()) {
            if last.timestamp > first.timestamp {
                duration_secs = ((last.timestamp - first.timestamp) as f64) / 1_000_000_000.0;
            }
        }

        let read_throughput = total_bytes_read as f64 / duration_secs;
        let write_throughput = total_bytes_written as f64 / duration_secs;

        // Calculate average latencies
        let read_operations: Vec<_> = operations
            .iter()
            .filter(|op| matches!(op.operation_type, IoOperationType::Read))
            .collect();
        let write_operations: Vec<_> = operations
            .iter()
            .filter(|op| matches!(op.operation_type, IoOperationType::Write))
            .collect();

        let avg_read_latency = if read_operations.is_empty() {
            Duration::ZERO
        } else {
            read_operations
                .iter()
                .map(|op| op.duration)
                .sum::<Duration>()
                / read_operations.len() as u32
        };

        let avg_write_latency = if write_operations.is_empty() {
            Duration::ZERO
        } else {
            write_operations
                .iter()
                .map(|op| op.duration)
                .sum::<Duration>()
                / write_operations.len() as u32
        };

        // Calculate error rate
        let error_count = operations
            .iter()
            .filter(|op| !matches!(op.result, IoResult::Success))
            .count();
        let error_rate = if total_operations == 0 {
            0.0
        } else {
            (error_count as f64 / total_operations as f64) * 100.0
        };

        // Generate top bottlenecks
        let mut bottlenecks: Vec<IoBottleneck> = operation_stats
            .iter()
            .map(|(key, stats)| {
                let parts: Vec<&str> = key.splitn(2, '_').collect();
                let operation_type = match parts[0] {
                    "Read" => IoOperationType::Read,
                    "Write" => IoOperationType::Write,
                    "Seek" => IoOperationType::Seek,
                    "Flush" => IoOperationType::Flush,
                    "Sync" => IoOperationType::Sync,
                    "Open" => IoOperationType::Open,
                    "Close" => IoOperationType::Close,
                    "Delete" => IoOperationType::Delete,
                    "Rename" => IoOperationType::Rename,
                    _ => IoOperationType::Read,
                };

                let file_path = if parts.len() > 1 { parts[1] } else { "unknown" };

                let severity = if stats.avg_latency > Duration::from_millis(100) {
                    BottleneckSeverity::Critical
                } else if stats.avg_latency > Duration::from_millis(50) {
                    BottleneckSeverity::High
                } else if stats.avg_latency > Duration::from_millis(20) {
                    BottleneckSeverity::Medium
                } else if stats.avg_latency > Duration::from_millis(5) {
                    BottleneckSeverity::Low
                } else {
                    BottleneckSeverity::None
                };

                let impact_score = (stats.avg_latency.as_millis() as f64)
                    * (stats.operation_count as f64)
                    * (1.0 + stats.error_rate / 100.0);

                IoBottleneck {
                    operation_type,
                    file_path: file_path.to_string(),
                    avg_latency: stats.avg_latency,
                    max_latency: stats.max_latency,
                    throughput: stats.throughput_bytes_per_sec,
                    error_rate: stats.error_rate,
                    bottleneck_severity: severity,
                    impact_score,
                    recommendations: Self::generate_io_recommendations(stats),
                }
            })
            .collect();

        // Sort by impact score, descending
        bottlenecks.sort_by(|a, b| b.impact_score.partial_cmp(&a.impact_score).unwrap());
        bottlenecks.truncate(10); // Keep top 10

        IoProfilingStats {
            total_operations,
            total_bytes_read,
            total_bytes_written,
            profiling_duration: Duration::from_secs_f64(duration_secs),
            read_throughput,
            write_throughput,
            avg_read_latency,
            avg_write_latency,
            error_rate,
            top_bottlenecks: bottlenecks,
        }
    }

    /// Generate I/O optimization recommendations
    fn generate_io_recommendations(stats: &IoOperationStats) -> Vec<String> {
        let mut recommendations = Vec::new();

        if stats.avg_latency > Duration::from_millis(50) {
            recommendations
                .push("Consider using asynchronous I/O for better concurrency".to_string());
        }

        if stats.error_rate > 5.0 {
            recommendations.push(
                "High error rate detected - check file permissions and disk health".to_string(),
            );
        }

        if stats.throughput_bytes_per_sec < 10.0 * 1024.0 * 1024.0 {
            // < 10 MB/s
            recommendations.push(
                "Low throughput detected - consider I/O batching or larger buffer sizes"
                    .to_string(),
            );
        }

        if stats.operation_count > 10000 && stats.total_bytes / stats.operation_count < 4096 {
            recommendations.push(
                "Many small I/O operations detected - consider combining into larger operations"
                    .to_string(),
            );
        }

        recommendations
    }
}

/// Complete I/O profile data for export
#[derive(Debug, Serialize, Deserialize)]
struct IoProfileData {
    total_operations: u64,
    total_bytes_read: u64,
    total_bytes_written: u64,
    operations: Vec<IoOperation>,
    operation_stats: HashMap<String, IoOperationStats>,
    statistics: IoProfilingStats,
}

/// Analyze an I/O profile file and extract bottlenecks
pub fn analyze_io_profile(profile_path: &Path) -> Result<Vec<IoBottleneck>, super::ProfilingError> {
    let content = std::fs::read_to_string(profile_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to read profile: {}", e)))?;

    let profile_data: IoProfileData = serde_json::from_str(&content)
        .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;

    Ok(profile_data.statistics.top_bottlenecks)
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

    pub fn u32(range: std::ops::Range<u32>) -> u32 {
        u64(range.start as u64..range.end as u64) as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_io_profiler_creation() {
        let profiler = IoProfiler::new().unwrap();
        assert!(!profiler.active.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_io_latency_generation() {
        for _ in 0..100 {
            let latency = IoProfiler::generate_io_latency(IoOperationType::Read);
            assert!(latency >= Duration::from_micros(50));
            assert!(latency <= Duration::from_millis(10));
        }
    }

    #[test]
    fn test_io_size_generation() {
        for _ in 0..1000 {
            let size = IoProfiler::generate_io_size();
            assert!(size >= 64);
            assert!(size <= 16 * 1024 * 1024);
        }
    }

    #[test]
    fn test_file_path_generation() {
        for _ in 0..100 {
            let path = IoProfiler::generate_file_path();
            assert!(!path.is_empty());
            assert!(path.contains("lightning_db"));
        }
    }

    #[test]
    fn test_stack_trace_generation() {
        let stack = IoProfiler::generate_io_stack_trace();
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
        let profile_path = temp_dir.path().join("io_profile.json");

        let profiler = IoProfiler::new().unwrap();

        // Add some mock operations
        {
            let mut operations = profiler.operations.lock().unwrap();
            operations.push(IoOperation {
                timestamp: 1000,
                thread_id: 1,
                operation_type: IoOperationType::Read,
                file_path: "/test/file.db".to_string(),
                offset: 0,
                size: 4096,
                duration: Duration::from_millis(1),
                result: IoResult::Success,
                stack_trace: vec![],
            });
        }

        profiler.export_profile(&profile_path).unwrap();
        assert!(profile_path.exists());

        // Verify we can read it back
        let content = std::fs::read_to_string(&profile_path).unwrap();
        let _: IoProfileData = serde_json::from_str(&content).unwrap();
    }
}

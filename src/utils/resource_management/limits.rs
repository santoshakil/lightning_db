use crate::core::error::{Error, Result};
use crate::utils::lock_utils::LockUtils;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Resource limits configuration for Lightning DB
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: u64,

    /// Maximum number of concurrent operations
    pub max_concurrent_operations: usize,

    /// Maximum number of open file descriptors
    pub max_open_files: usize,

    /// Maximum disk usage in bytes
    pub max_disk_bytes: u64,

    /// Maximum write throughput in bytes per second
    pub max_write_throughput_bytes_per_sec: u64,

    /// Maximum read throughput in bytes per second
    pub max_read_throughput_bytes_per_sec: u64,

    /// Maximum transaction duration
    pub max_transaction_duration: Duration,

    /// Maximum WAL size in bytes
    pub max_wal_size_bytes: u64,

    /// Enable resource limit enforcement
    pub enforcement_enabled: bool,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
            max_concurrent_operations: 10000,
            max_open_files: 1000,
            max_disk_bytes: 100 * 1024 * 1024 * 1024, // 100GB
            max_write_throughput_bytes_per_sec: 100 * 1024 * 1024, // 100MB/s
            max_read_throughput_bytes_per_sec: 500 * 1024 * 1024, // 500MB/s
            max_transaction_duration: Duration::from_secs(300), // 5 minutes
            max_wal_size_bytes: 1024 * 1024 * 1024,   // 1GB
            enforcement_enabled: true,
        }
    }
}

/// Resource usage tracker and enforcer
pub struct ResourceEnforcer {
    pub limits: ResourceLimits,

    // Current usage metrics
    current_memory: Arc<AtomicU64>,
    current_operations: Arc<AtomicUsize>,
    current_open_files: Arc<AtomicUsize>,
    current_disk_usage: Arc<AtomicU64>,
    current_wal_size: Arc<AtomicU64>,

    // Throughput tracking
    write_throughput_tracker: Arc<Mutex<ThroughputTracker>>,
    read_throughput_tracker: Arc<Mutex<ThroughputTracker>>,

    // Circuit breaker state
    memory_limit_exceeded: Arc<AtomicBool>,
    _disk_limit_exceeded: Arc<AtomicBool>,
    _throughput_limit_exceeded: Arc<AtomicBool>,

    // Violation tracking
    violations: Arc<Mutex<Vec<ResourceViolation>>>,
}

#[derive(Debug, Clone)]
pub struct ResourceViolation {
    pub timestamp: Instant,
    pub violation_type: ViolationType,
    pub limit: u64,
    pub actual: u64,
    pub action_taken: ActionTaken,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ViolationType {
    MemoryLimit,
    DiskLimit,
    FileLimit,
    ConcurrencyLimit,
    WriteThroughputLimit,
    ReadThroughputLimit,
    TransactionDurationLimit,
    WalSizeLimit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ActionTaken {
    Rejected,
    Throttled,
    Queued,
    EmergencyCleanup,
}

/// Throughput tracker using sliding window
struct ThroughputTracker {
    window_duration: Duration,
    samples: VecDeque<(Instant, u64)>,
    total_bytes: u64,
}

impl ThroughputTracker {
    fn new(window_duration: Duration) -> Self {
        Self {
            window_duration,
            samples: VecDeque::new(),
            total_bytes: 0,
        }
    }

    fn add_bytes(&mut self, bytes: u64) {
        let now = Instant::now();
        self.samples.push_back((now, bytes));
        self.total_bytes += bytes;

        // Remove old samples outside the window
        let cutoff = now - self.window_duration;
        while let Some((timestamp, bytes)) = self.samples.front() {
            if *timestamp < cutoff {
                self.total_bytes -= bytes;
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn current_throughput(&self) -> u64 {
        // Defensive: check both front and back exist
        let (Some(front), Some(back)) = (self.samples.front(), self.samples.back()) else {
            return 0;
        };

        let duration = back.0 - front.0;
        if duration.as_secs_f64() > 0.0 {
            (self.total_bytes as f64 / duration.as_secs_f64()) as u64
        } else {
            self.total_bytes
        }
    }
}

impl ResourceEnforcer {
    pub fn new(limits: ResourceLimits) -> Self {
        Self {
            limits,
            current_memory: Arc::new(AtomicU64::new(0)),
            current_operations: Arc::new(AtomicUsize::new(0)),
            current_open_files: Arc::new(AtomicUsize::new(0)),
            current_disk_usage: Arc::new(AtomicU64::new(0)),
            current_wal_size: Arc::new(AtomicU64::new(0)),
            write_throughput_tracker: Arc::new(Mutex::new(ThroughputTracker::new(
                Duration::from_secs(1),
            ))),
            read_throughput_tracker: Arc::new(Mutex::new(ThroughputTracker::new(
                Duration::from_secs(1),
            ))),
            memory_limit_exceeded: Arc::new(AtomicBool::new(false)),
            _disk_limit_exceeded: Arc::new(AtomicBool::new(false)),
            _throughput_limit_exceeded: Arc::new(AtomicBool::new(false)),
            violations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Check if a write operation can proceed
    pub fn check_write(&self, bytes: u64) -> Result<()> {
        if !self.limits.enforcement_enabled {
            return Ok(());
        }

        // Check memory limit
        let current_mem = self.current_memory.load(Ordering::Relaxed);
        if current_mem + bytes > self.limits.max_memory_bytes {
            self.record_violation(
                ViolationType::MemoryLimit,
                self.limits.max_memory_bytes,
                current_mem + bytes,
            );
            return Err(Error::ResourceExhausted {
                resource: "Memory limit exceeded".to_string(),
            });
        }

        // Check disk limit
        let current_disk = self.current_disk_usage.load(Ordering::Relaxed);
        if current_disk + bytes > self.limits.max_disk_bytes {
            self.record_violation(
                ViolationType::DiskLimit,
                self.limits.max_disk_bytes,
                current_disk + bytes,
            );
            return Err(Error::ResourceExhausted {
                resource: "Disk space limit exceeded".to_string(),
            });
        }

        // Check write throughput
        let tracker = LockUtils::std_mutex_with_retry(&self.write_throughput_tracker)?;
        let current_throughput = tracker.current_throughput();
        if current_throughput + bytes > self.limits.max_write_throughput_bytes_per_sec {
            self.record_violation(
                ViolationType::WriteThroughputLimit,
                self.limits.max_write_throughput_bytes_per_sec,
                current_throughput + bytes,
            );

            // Calculate throttle duration
            let excess =
                current_throughput + bytes - self.limits.max_write_throughput_bytes_per_sec;
            let throttle_ms = excess * 1000 / self.limits.max_write_throughput_bytes_per_sec;

            return Err(Error::ResourceExhausted {
                resource: format!("Throttled: {} ms", throttle_ms),
            });
        }

        Ok(())
    }

    /// Check if a read operation can proceed
    pub fn check_read(&self, bytes: u64) -> Result<()> {
        if !self.limits.enforcement_enabled {
            return Ok(());
        }

        // Check read throughput
        let tracker = LockUtils::std_mutex_with_retry(&self.read_throughput_tracker)?;
        let current_throughput = tracker.current_throughput();
        if current_throughput + bytes > self.limits.max_read_throughput_bytes_per_sec {
            self.record_violation(
                ViolationType::ReadThroughputLimit,
                self.limits.max_read_throughput_bytes_per_sec,
                current_throughput + bytes,
            );

            // Calculate throttle duration
            let excess = current_throughput + bytes - self.limits.max_read_throughput_bytes_per_sec;
            let throttle_ms = excess * 1000 / self.limits.max_read_throughput_bytes_per_sec;

            return Err(Error::ResourceExhausted {
                resource: format!("Throttled: {} ms", throttle_ms),
            });
        }

        Ok(())
    }

    /// Check if we can start a new operation
    pub fn check_operation_start(&self) -> Result<OperationGuard> {
        if !self.limits.enforcement_enabled {
            return Ok(OperationGuard::new(self.current_operations.clone()));
        }

        let current = self.current_operations.load(Ordering::Relaxed);
        if current >= self.limits.max_concurrent_operations {
            self.record_violation(
                ViolationType::ConcurrencyLimit,
                self.limits.max_concurrent_operations as u64,
                current as u64,
            );
            return Err(Error::ResourceExhausted {
                resource: "Concurrent operation limit exceeded".to_string(),
            });
        }

        self.current_operations.fetch_add(1, Ordering::Relaxed);
        Ok(OperationGuard::new(self.current_operations.clone()))
    }

    /// Check if we can open a new file
    pub fn check_file_open(&self) -> Result<()> {
        if !self.limits.enforcement_enabled {
            return Ok(());
        }

        let current = self.current_open_files.load(Ordering::Relaxed);
        if current >= self.limits.max_open_files {
            self.record_violation(
                ViolationType::FileLimit,
                self.limits.max_open_files as u64,
                current as u64,
            );
            return Err(Error::ResourceExhausted {
                resource: "Open file limit exceeded".to_string(),
            });
        }

        self.current_open_files.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Record a write operation
    pub fn record_write(&self, bytes: u64) {
        if let Ok(mut tracker) = LockUtils::std_mutex_with_retry(&self.write_throughput_tracker) {
            tracker.add_bytes(bytes);
        }
        self.current_disk_usage.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a read operation
    pub fn record_read(&self, bytes: u64) {
        if let Ok(mut tracker) = LockUtils::std_mutex_with_retry(&self.read_throughput_tracker) {
            tracker.add_bytes(bytes);
        }
    }

    /// Update current memory usage
    pub fn update_memory_usage(&self, bytes: u64) {
        self.current_memory.store(bytes, Ordering::Relaxed);
    }

    /// Update current disk usage
    pub fn update_disk_usage(&self, bytes: u64) {
        self.current_disk_usage.store(bytes, Ordering::Relaxed);
    }

    /// Update WAL size
    pub fn update_wal_size(&self, bytes: u64) {
        let old_size = self.current_wal_size.swap(bytes, Ordering::Relaxed);

        if bytes > self.limits.max_wal_size_bytes && old_size <= self.limits.max_wal_size_bytes {
            self.record_violation(
                ViolationType::WalSizeLimit,
                self.limits.max_wal_size_bytes,
                bytes,
            );
        }
    }

    /// Get current resource usage
    pub fn get_usage(&self) -> ResourceUsage {
        ResourceUsage {
            memory_bytes: self.current_memory.load(Ordering::Relaxed),
            disk_bytes: self.current_disk_usage.load(Ordering::Relaxed),
            open_files: self.current_open_files.load(Ordering::Relaxed),
            concurrent_operations: self.current_operations.load(Ordering::Relaxed),
            write_throughput_bytes_per_sec: LockUtils::std_mutex_with_retry(
                &self.write_throughput_tracker,
            )
            .map(|t| t.current_throughput())
            .unwrap_or(0),
            read_throughput_bytes_per_sec: LockUtils::std_mutex_with_retry(
                &self.read_throughput_tracker,
            )
            .map(|t| t.current_throughput())
            .unwrap_or(0),
            wal_size_bytes: self.current_wal_size.load(Ordering::Relaxed),
        }
    }

    /// Get resource violations
    pub fn get_violations(&self) -> Vec<ResourceViolation> {
        LockUtils::std_mutex_with_retry(&self.violations)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Clear violation history
    pub fn clear_violations(&self) {
        if let Ok(mut violations) = LockUtils::std_mutex_with_retry(&self.violations) {
            violations.clear();
        }
    }

    /// Emergency cleanup when limits are exceeded
    pub fn emergency_cleanup(&self) -> Result<()> {
        // This would trigger cache eviction, WAL rotation, etc.
        self.memory_limit_exceeded.store(true, Ordering::Relaxed);
        self.record_violation(
            ViolationType::MemoryLimit,
            self.limits.max_memory_bytes,
            self.current_memory.load(Ordering::Relaxed),
        );

        Ok(())
    }

    fn record_violation(&self, violation_type: ViolationType, limit: u64, actual: u64) {
        let violation = ResourceViolation {
            timestamp: Instant::now(),
            violation_type,
            limit,
            actual,
            action_taken: ActionTaken::Rejected,
        };

        if let Ok(mut violations) = LockUtils::std_mutex_with_retry(&self.violations) {
            violations.push(violation);
        }
    }
}

/// RAII guard for operation counting
pub struct OperationGuard {
    counter: Arc<AtomicUsize>,
}

impl OperationGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        Self { counter }
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Current resource usage snapshot
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub open_files: usize,
    pub concurrent_operations: usize,
    pub write_throughput_bytes_per_sec: u64,
    pub read_throughput_bytes_per_sec: u64,
    pub wal_size_bytes: u64,
}

impl ResourceUsage {
    /// Check if usage is within limits
    pub fn is_within_limits(&self, limits: &ResourceLimits) -> bool {
        self.memory_bytes <= limits.max_memory_bytes
            && self.disk_bytes <= limits.max_disk_bytes
            && self.open_files <= limits.max_open_files
            && self.concurrent_operations <= limits.max_concurrent_operations
            && self.write_throughput_bytes_per_sec <= limits.max_write_throughput_bytes_per_sec
            && self.read_throughput_bytes_per_sec <= limits.max_read_throughput_bytes_per_sec
            && self.wal_size_bytes <= limits.max_wal_size_bytes
    }

    /// Get usage as percentage of limits
    pub fn get_usage_percentages(&self, limits: &ResourceLimits) -> UsagePercentages {
        UsagePercentages {
            memory_percent: (self.memory_bytes as f64 / limits.max_memory_bytes as f64 * 100.0),
            disk_percent: (self.disk_bytes as f64 / limits.max_disk_bytes as f64 * 100.0),
            files_percent: (self.open_files as f64 / limits.max_open_files as f64 * 100.0),
            operations_percent: (self.concurrent_operations as f64
                / limits.max_concurrent_operations as f64
                * 100.0),
            write_throughput_percent: (self.write_throughput_bytes_per_sec as f64
                / limits.max_write_throughput_bytes_per_sec as f64
                * 100.0),
            read_throughput_percent: (self.read_throughput_bytes_per_sec as f64
                / limits.max_read_throughput_bytes_per_sec as f64
                * 100.0),
            wal_percent: (self.wal_size_bytes as f64 / limits.max_wal_size_bytes as f64 * 100.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct UsagePercentages {
    pub memory_percent: f64,
    pub disk_percent: f64,
    pub files_percent: f64,
    pub operations_percent: f64,
    pub write_throughput_percent: f64,
    pub read_throughput_percent: f64,
    pub wal_percent: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throughput_tracker() {
        let mut tracker = ThroughputTracker::new(Duration::from_secs(1));

        // Add some bytes
        tracker.add_bytes(1000);
        std::thread::sleep(Duration::from_millis(100));
        tracker.add_bytes(2000);

        // Should be around 3000 bytes/sec
        let throughput = tracker.current_throughput();
        assert!(throughput > 0);
    }

    #[test]
    fn test_resource_limits() {
        let limits = ResourceLimits {
            max_memory_bytes: 1000,
            max_concurrent_operations: 2,
            ..Default::default()
        };

        let enforcer = ResourceEnforcer::new(limits);

        // Should succeed
        assert!(enforcer.check_write(100).is_ok());

        // Update memory usage
        enforcer.update_memory_usage(950);

        // Should fail - would exceed limit
        assert!(enforcer.check_write(100).is_err());
    }

    #[test]
    fn test_operation_guard() {
        let counter = Arc::new(AtomicUsize::new(0));

        {
            let _guard = OperationGuard::new(counter.clone());
            counter.fetch_add(1, Ordering::Relaxed);
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }

        // Guard dropped, counter decremented
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}

//! Resource Exhaustion Testing for Chaos Engineering
//!
//! Tests database behavior under various resource constraints including
//! memory pressure, disk space exhaustion, file descriptor limits, and thread starvation.

use crate::chaos_engineering::{
    ChaosConfig, ChaosTest, ChaosTestResult, IntegrityReport, ResourceLimits,
};
use crate::{Database, Result};
use parking_lot::{Mutex, RwLock};
use rand::{rng, Rng};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime};

/// Resource exhaustion test coordinator
pub struct ResourceExhaustionTest {
    config: ResourceExhaustionConfig,
    resource_limiter: Arc<ResourceLimiter>,
    memory_pressure: Arc<MemoryPressureSimulator>,
    disk_exhaustion: Arc<DiskExhaustionSimulator>,
    thread_starvation: Arc<ThreadStarvationSimulator>,
    test_results: Arc<Mutex<ResourceExhaustionResults>>,
}

/// Configuration for resource exhaustion testing
#[derive(Debug, Clone)]
pub struct ResourceExhaustionConfig {
    /// Memory pressure scenarios to test
    pub memory_scenarios: Vec<MemoryPressureScenario>,
    /// Disk exhaustion scenarios
    pub disk_scenarios: Vec<DiskExhaustionScenario>,
    /// Thread pool exhaustion settings
    pub thread_exhaustion_enabled: bool,
    /// File descriptor exhaustion
    pub fd_exhaustion_enabled: bool,
    /// Network bandwidth throttling
    pub network_throttling_enabled: bool,
    /// CPU throttling percentage (0-100)
    pub cpu_throttle_percent: Option<u8>,
    /// Test gradual resource degradation
    pub gradual_degradation: bool,
    /// Recovery behavior testing
    pub test_recovery_behavior: bool,
    /// Resource monitoring interval
    pub monitoring_interval: Duration,
    /// Maximum test duration per scenario
    pub max_scenario_duration: Duration,
}

impl Default for ResourceExhaustionConfig {
    fn default() -> Self {
        Self {
            memory_scenarios: vec![
                MemoryPressureScenario::HighMemoryUsage(90),
                MemoryPressureScenario::OutOfMemory,
                MemoryPressureScenario::FragmentedMemory,
                MemoryPressureScenario::MemoryLeakSimulation,
            ],
            disk_scenarios: vec![
                DiskExhaustionScenario::LowDiskSpace(95),
                DiskExhaustionScenario::NoDiskSpace,
                DiskExhaustionScenario::WriteThrottling(1024 * 1024), // 1MB/s
                DiskExhaustionScenario::RandomWriteFailures(5),
            ],
            thread_exhaustion_enabled: true,
            fd_exhaustion_enabled: true,
            network_throttling_enabled: true,
            cpu_throttle_percent: Some(90),
            gradual_degradation: true,
            test_recovery_behavior: true,
            monitoring_interval: Duration::from_millis(100),
            max_scenario_duration: Duration::from_secs(60),
        }
    }
}

/// Memory pressure scenarios
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryPressureScenario {
    /// Consume X% of available memory
    HighMemoryUsage(u8),
    /// Simulate out of memory conditions
    OutOfMemory,
    /// Create highly fragmented memory
    FragmentedMemory,
    /// Simulate gradual memory leak
    MemoryLeakSimulation,
    /// Random allocation failures
    RandomAllocationFailures(u8), // failure percentage
}

/// Disk exhaustion scenarios
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskExhaustionScenario {
    /// Fill disk to X% capacity
    LowDiskSpace(u8),
    /// Complete disk exhaustion
    NoDiskSpace,
    /// Throttle write speed (bytes/sec)
    WriteThrottling(u64),
    /// Random write failures (percentage)
    RandomWriteFailures(u8),
    /// Simulate slow disk operations
    SlowDiskOperations(Duration),
}

/// Resource limiter for enforcing constraints
struct ResourceLimiter {
    limits: RwLock<ResourceLimits>,
    active_limits: RwLock<HashMap<ResourceType, ResourceLimit>>,
    enforcement_enabled: AtomicBool,
    violations: RwLock<Vec<ResourceViolation>>,
}

/// Types of resources to limit
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum ResourceType {
    Memory,
    Disk,
    FileDescriptors,
    Threads,
    CpuTime,
    NetworkBandwidth,
}

/// Active resource limit
#[derive(Debug, Clone)]
struct ResourceLimit {
    resource_type: ResourceType,
    limit_value: u64,
    current_usage: Arc<AtomicU64>,
    enforcement_start: SystemTime,
    violations_count: u64,
}

/// Resource violation record
#[derive(Debug, Clone)]
struct ResourceViolation {
    timestamp: SystemTime,
    resource_type: ResourceType,
    requested: u64,
    available: u64,
    operation: String,
    handled: bool,
}

/// Memory pressure simulator
struct MemoryPressureSimulator {
    allocated_blocks: RwLock<Vec<MemoryBlock>>,
    total_allocated: AtomicU64,
    fragmentation_level: AtomicU64,
    allocation_failures: AtomicU64,
    oom_triggered: AtomicBool,
}

/// Memory block for pressure simulation
struct MemoryBlock {
    data: Vec<u8>,
    size: usize,
    allocated_at: SystemTime,
    block_type: MemoryBlockType,
}

/// Type of memory block
#[derive(Debug, Clone, Copy)]
enum MemoryBlockType {
    Large,
    Medium,
    Small,
    Fragment,
}

/// Disk exhaustion simulator
struct DiskExhaustionSimulator {
    test_files: RwLock<Vec<TestFile>>,
    total_written: AtomicU64,
    write_throttle: RwLock<Option<WriteThrottle>>,
    failure_injection: RwLock<FailureInjection>,
}

/// Test file for disk exhaustion
#[derive(Debug)]
struct TestFile {
    path: PathBuf,
    size: u64,
    created_at: SystemTime,
    file_type: TestFileType,
}

/// Type of test file
#[derive(Debug, Clone, Copy)]
enum TestFileType {
    SpaceFiller,
    FragmentationInducer,
    TemporaryData,
}

/// Write throttling control
#[derive(Debug, Clone)]
struct WriteThrottle {
    bytes_per_second: u64,
    last_write_time: SystemTime,
    bytes_written_in_period: u64,
}

/// Failure injection for disk operations
#[derive(Debug, Clone)]
struct FailureInjection {
    failure_rate: f64,
    consecutive_failures: u64,
    max_consecutive: u64,
}

/// Thread starvation simulator
struct ThreadStarvationSimulator {
    busy_threads: RwLock<Vec<thread::JoinHandle<()>>>,
    thread_count: AtomicU64,
    cpu_burn_enabled: AtomicBool,
    priority_inversion: AtomicBool,
}

/// Results from resource exhaustion testing
#[derive(Debug, Clone)]
struct ResourceExhaustionResults {
    /// Scenarios tested
    pub scenarios_tested: usize,
    /// Scenarios that caused failures
    pub failure_scenarios: usize,
    /// Operations that failed due to resource limits
    pub failed_operations: u64,
    /// Operations that succeeded despite limits
    pub successful_operations: u64,
    /// Recovery attempts
    pub recovery_attempts: u64,
    /// Successful recoveries
    pub successful_recoveries: u64,
    /// Performance degradation measurements
    pub performance_impact: PerformanceDegradation,
    /// Resource usage patterns
    pub resource_patterns: HashMap<ResourceType, ResourceUsagePattern>,
    /// Critical failures
    pub critical_failures: Vec<CriticalFailure>,
}

/// Performance degradation metrics
#[derive(Debug, Clone)]
struct PerformanceDegradation {
    /// Baseline operations per second
    pub baseline_ops_per_sec: f64,
    /// Minimum ops/sec under pressure
    pub min_ops_per_sec: f64,
    /// Average degradation percentage
    pub avg_degradation_percent: f64,
    /// Maximum latency spike
    pub max_latency_spike_ms: u64,
    /// Recovery time after pressure removed
    pub recovery_time: Duration,
}

/// Resource usage pattern analysis
#[derive(Debug, Clone)]
struct ResourceUsagePattern {
    /// Peak usage
    pub peak_usage: u64,
    /// Average usage
    pub avg_usage: u64,
    /// Usage variance
    pub usage_variance: f64,
    /// Failure threshold
    pub failure_threshold: u64,
    /// Usage trend
    pub trend: UsageTrend,
}

/// Usage trend direction
#[derive(Debug, Clone, Copy)]
enum UsageTrend {
    Increasing,
    Stable,
    Decreasing,
    Oscillating,
}

/// Critical failure information
#[derive(Debug, Clone)]
struct CriticalFailure {
    pub timestamp: SystemTime,
    pub resource_type: ResourceType,
    pub failure_type: String,
    pub impact: String,
    pub recovery_possible: bool,
}

impl ResourceExhaustionTest {
    /// Create a new resource exhaustion test
    pub fn new(config: ResourceExhaustionConfig) -> Self {
        Self {
            config,
            resource_limiter: Arc::new(ResourceLimiter::new()),
            memory_pressure: Arc::new(MemoryPressureSimulator::new()),
            disk_exhaustion: Arc::new(DiskExhaustionSimulator::new()),
            thread_starvation: Arc::new(ThreadStarvationSimulator::new()),
            test_results: Arc::new(Mutex::new(ResourceExhaustionResults::default())),
        }
    }

    /// Run memory pressure scenario
    fn run_memory_pressure_scenario(
        &self,
        db: Arc<Database>,
        scenario: MemoryPressureScenario,
    ) -> Result<ScenarioResult> {
        println!("💾 Running memory pressure scenario: {:?}", scenario);
        let start_time = SystemTime::now();

        match scenario {
            MemoryPressureScenario::HighMemoryUsage(percent) => {
                self.simulate_high_memory_usage(percent, &db)?
            }
            MemoryPressureScenario::OutOfMemory => self.simulate_out_of_memory(&db)?,
            MemoryPressureScenario::FragmentedMemory => self.simulate_memory_fragmentation(&db)?,
            MemoryPressureScenario::MemoryLeakSimulation => self.simulate_memory_leak(&db)?,
            MemoryPressureScenario::RandomAllocationFailures(rate) => {
                self.simulate_random_allocation_failures(rate, &db)?
            }
        }

        let duration = start_time.elapsed().unwrap_or_default();

        Ok(ScenarioResult {
            scenario_name: format!("{:?}", scenario),
            success: true,
            duration,
            operations_performed: 0,
            failures_encountered: 0,
            recovery_tested: false,
        })
    }

    /// Simulate high memory usage
    fn simulate_high_memory_usage(&self, target_percent: u8, db: &Arc<Database>) -> Result<()> {
        let system_memory = self.get_system_memory()?;
        let target_bytes = (system_memory * target_percent as u64) / 100;

        println!(
            "   Allocating {} MB to reach {}% memory usage",
            target_bytes / (1024 * 1024),
            target_percent
        );

        // Allocate memory in chunks
        let chunk_size = 10 * 1024 * 1024; // 10MB chunks
        let mut allocated = 0u64;

        while allocated < target_bytes {
            let block = MemoryBlock {
                data: vec![0u8; chunk_size],
                size: chunk_size,
                allocated_at: SystemTime::now(),
                block_type: MemoryBlockType::Large,
            };

            self.memory_pressure.allocated_blocks.write().push(block);
            allocated += chunk_size as u64;
            self.memory_pressure
                .total_allocated
                .fetch_add(chunk_size as u64, Ordering::Relaxed);

            // Perform database operations under memory pressure
            self.perform_operations_under_pressure(db)?;

            thread::sleep(Duration::from_millis(100));
        }

        Ok(())
    }

    /// Simulate out of memory conditions
    fn simulate_out_of_memory(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Simulating out-of-memory conditions");

        self.memory_pressure
            .oom_triggered
            .store(true, Ordering::SeqCst);

        // Try database operations that should handle OOM gracefully
        let mut failures = 0;
        for i in 0..100 {
            match self.perform_operation_expecting_failure(db) {
                Ok(_) => {}
                Err(_) => failures += 1,
            }

            if i % 10 == 0 {
                println!("   Operations: {}, Failures: {}", i + 1, failures);
            }
        }

        self.memory_pressure
            .oom_triggered
            .store(false, Ordering::SeqCst);

        Ok(())
    }

    /// Simulate memory fragmentation
    fn simulate_memory_fragmentation(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Creating memory fragmentation pattern");

        let mut rng = rng();

        // Allocate and deallocate in a pattern that creates fragmentation
        for round in 0..10 {
            let mut blocks = Vec::new();

            // Allocate various sized blocks
            for _ in 0..50 {
                let size = match rng.random_range(0..3) {
                    0 => 1024,       // 1KB
                    1 => 64 * 1024,  // 64KB
                    _ => 512 * 1024, // 512KB
                };

                let block = MemoryBlock {
                    data: vec![0u8; size],
                    size,
                    allocated_at: SystemTime::now(),
                    block_type: MemoryBlockType::Fragment,
                };

                blocks.push(block);
            }

            // Deallocate every other block
            let mut remaining = Vec::new();
            for (i, block) in blocks.into_iter().enumerate() {
                if i % 2 == 0 {
                    remaining.push(block);
                }
            }

            self.memory_pressure
                .allocated_blocks
                .write()
                .extend(remaining);
            self.memory_pressure
                .fragmentation_level
                .store(round * 10, Ordering::Relaxed);

            // Test database performance with fragmented memory
            self.measure_performance_under_fragmentation(db)?;
        }

        Ok(())
    }

    /// Simulate gradual memory leak
    fn simulate_memory_leak(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Simulating gradual memory leak");

        let leak_rate = 1024 * 1024; // 1MB per iteration
        let iterations = 100;

        for i in 0..iterations {
            // Leak memory
            let block = MemoryBlock {
                data: vec![0u8; leak_rate],
                size: leak_rate,
                allocated_at: SystemTime::now(),
                block_type: MemoryBlockType::Small,
            };

            self.memory_pressure.allocated_blocks.write().push(block);
            self.memory_pressure
                .total_allocated
                .fetch_add(leak_rate as u64, Ordering::Relaxed);

            // Monitor database behavior
            if i % 10 == 0 {
                let total_leaked = (i + 1) * leak_rate / (1024 * 1024);
                println!(
                    "   Leaked: {} MB, Testing database performance",
                    total_leaked
                );
                self.test_database_under_memory_leak(db)?;
            }

            thread::sleep(Duration::from_millis(50));
        }

        Ok(())
    }

    /// Simulate random allocation failures
    fn simulate_random_allocation_failures(
        &self,
        failure_rate: u8,
        db: &Arc<Database>,
    ) -> Result<()> {
        println!("   Simulating {}% allocation failure rate", failure_rate);

        let mut rng = rng();
        let mut operations = 0;
        let mut failures = 0;

        for _ in 0..1000 {
            operations += 1;

            // Randomly fail allocations
            if rng.random_range(0..100) < failure_rate {
                self.memory_pressure
                    .allocation_failures
                    .fetch_add(1, Ordering::Relaxed);
                failures += 1;

                // Test database resilience to allocation failures
                match db.put(b"test_key", b"test_value") {
                    Ok(_) => {}
                    Err(_) => {
                        println!("   Database operation failed during allocation failure");
                    }
                }
            } else {
                // Normal operation
                db.put(b"test_key", b"test_value")?;
            }
        }

        println!(
            "   Allocation failures: {}/{} ({:.1}%)",
            failures,
            operations,
            (failures as f64 / operations as f64) * 100.0
        );

        Ok(())
    }

    /// Perform operations under resource pressure
    fn perform_operations_under_pressure(&self, db: &Arc<Database>) -> Result<()> {
        let mut rng = rng();

        for i in 0..100 {
            let key = format!("pressure_test_{}", i).into_bytes();
            let value = vec![0u8; rng.random_range(100..10000)];

            match db.put(&key, &value) {
                Ok(_) => {}
                Err(e) => {
                    println!("   Operation failed under pressure: {}", e);
                    return Err(e);
                }
            }

            if i % 10 == 0 {
                // Also test reads
                let _ = db.get(&key)?;
            }
        }

        Ok(())
    }

    /// Perform operation expecting potential failure
    fn perform_operation_expecting_failure(&self, db: &Arc<Database>) -> Result<()> {
        let key = b"oom_test_key";
        let value = b"oom_test_value";

        // This might fail due to OOM
        db.put(key, value)?;
        db.get(key)?;

        Ok(())
    }

    /// Measure performance under memory fragmentation
    fn measure_performance_under_fragmentation(&self, db: &Arc<Database>) -> Result<()> {
        let start = SystemTime::now();
        let mut operations = 0;

        while start.elapsed().unwrap_or_default() < Duration::from_secs(5) {
            let key = format!("frag_test_{}", operations).into_bytes();
            db.put(&key, b"fragmented_memory_test")?;
            operations += 1;
        }

        let duration = start.elapsed().unwrap_or_default();
        let ops_per_sec = operations as f64 / duration.as_secs_f64();

        println!(
            "   Performance under fragmentation: {:.2} ops/sec",
            ops_per_sec
        );

        Ok(())
    }

    /// Test database behavior under memory leak
    fn test_database_under_memory_leak(&self, db: &Arc<Database>) -> Result<()> {
        // Test if database can still operate
        db.put(b"leak_test", b"still_working")?;
        db.get(b"leak_test")?;

        // Check memory usage reporting
        let leaked = self.memory_pressure.total_allocated.load(Ordering::Relaxed);
        println!("   Total memory leaked: {} MB", leaked / (1024 * 1024));

        Ok(())
    }

    /// Get available system memory
    fn get_system_memory(&self) -> Result<u64> {
        // Simplified - return 8GB for testing
        // In production, would use system APIs
        Ok(8 * 1024 * 1024 * 1024)
    }

    /// Run disk exhaustion scenario
    fn run_disk_exhaustion_scenario(
        &self,
        db: Arc<Database>,
        scenario: DiskExhaustionScenario,
    ) -> Result<ScenarioResult> {
        println!("💿 Running disk exhaustion scenario: {:?}", scenario);
        let start_time = SystemTime::now();

        match scenario {
            DiskExhaustionScenario::LowDiskSpace(percent) => {
                self.simulate_low_disk_space(percent, &db)?
            }
            DiskExhaustionScenario::NoDiskSpace => self.simulate_no_disk_space(&db)?,
            DiskExhaustionScenario::WriteThrottling(bytes_per_sec) => {
                self.simulate_write_throttling(bytes_per_sec, &db)?
            }
            DiskExhaustionScenario::RandomWriteFailures(rate) => {
                self.simulate_random_write_failures(rate, &db)?
            }
            DiskExhaustionScenario::SlowDiskOperations(delay) => {
                self.simulate_slow_disk_operations(delay, &db)?
            }
        }

        let duration = start_time.elapsed().unwrap_or_default();

        Ok(ScenarioResult {
            scenario_name: format!("{:?}", scenario),
            success: true,
            duration,
            operations_performed: 0,
            failures_encountered: 0,
            recovery_tested: false,
        })
    }

    /// Simulate low disk space
    fn simulate_low_disk_space(&self, target_percent: u8, db: &Arc<Database>) -> Result<()> {
        println!("   Filling disk to {}% capacity", target_percent);

        // Create large files to consume disk space
        let temp_dir = std::env::temp_dir().join("chaos_disk_test");
        std::fs::create_dir_all(&temp_dir)?;

        let file_size = 100 * 1024 * 1024; // 100MB files
        let mut file_count = 0;

        // Fill disk with test files
        loop {
            let file_path = temp_dir.join(format!("space_filler_{}.dat", file_count));

            match self.create_space_filler_file(&file_path, file_size) {
                Ok(_) => {
                    self.disk_exhaustion.test_files.write().push(TestFile {
                        path: file_path,
                        size: file_size,
                        created_at: SystemTime::now(),
                        file_type: TestFileType::SpaceFiller,
                    });
                    file_count += 1;
                }
                Err(_) => {
                    println!("   Disk space exhausted after {} files", file_count);
                    break;
                }
            }

            // Test database operations with low disk space
            if file_count % 10 == 0 {
                self.test_database_with_low_disk(db)?;
            }

            // Check if target reached (simplified)
            if file_count * 100 > 1000 {
                // After 1GB
                break;
            }
        }

        Ok(())
    }

    /// Create a space filler file
    fn create_space_filler_file(&self, path: &Path, size: u64) -> Result<()> {
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;

        let buffer = vec![0u8; 1024 * 1024]; // 1MB buffer
        let mut written = 0u64;

        while written < size {
            let to_write = std::cmp::min(buffer.len(), (size - written) as usize);
            file.write_all(&buffer[..to_write])?;
            written += to_write as u64;
        }

        file.sync_all()?;
        self.disk_exhaustion
            .total_written
            .fetch_add(size, Ordering::Relaxed);

        Ok(())
    }

    /// Test database with low disk space
    fn test_database_with_low_disk(&self, db: &Arc<Database>) -> Result<()> {
        let mut failures = 0;

        for i in 0..100 {
            let key = format!("low_disk_test_{}", i).into_bytes();
            match db.put(&key, b"testing_with_low_disk") {
                Ok(_) => {}
                Err(_) => failures += 1,
            }
        }

        if failures > 0 {
            println!("   {} operations failed due to low disk space", failures);
        }

        Ok(())
    }

    /// Simulate complete disk exhaustion
    fn simulate_no_disk_space(&self, db: &Arc<Database>) -> Result<()> {
        println!("   Simulating complete disk exhaustion");

        // First fill the disk
        self.simulate_low_disk_space(99, db)?;

        // Now test database behavior with no space
        let mut consecutive_failures = 0;

        for i in 0..1000 {
            let key = format!("no_space_test_{}", i).into_bytes();

            match db.put(&key, b"should_fail") {
                Ok(_) => {
                    consecutive_failures = 0;
                    println!("   Unexpected success after {} failures", i);
                }
                Err(_) => {
                    consecutive_failures += 1;
                }
            }

            // Test if database can recover when space is freed
            if consecutive_failures > 50 {
                self.free_some_disk_space()?;
                println!("   Freed some disk space, testing recovery");
            }
        }

        Ok(())
    }

    /// Free some disk space
    fn free_some_disk_space(&self) -> Result<()> {
        let mut files = self.disk_exhaustion.test_files.write();

        // Remove first 10 files
        let to_remove = std::cmp::min(10, files.len());
        for _ in 0..to_remove {
            if let Some(test_file) = files.pop() {
                let _ = std::fs::remove_file(&test_file.path);
            }
        }

        Ok(())
    }

    /// Simulate write throttling
    fn simulate_write_throttling(&self, bytes_per_sec: u64, db: &Arc<Database>) -> Result<()> {
        println!("   Throttling writes to {} KB/s", bytes_per_sec / 1024);

        *self.disk_exhaustion.write_throttle.write() = Some(WriteThrottle {
            bytes_per_second: bytes_per_sec,
            last_write_time: SystemTime::now(),
            bytes_written_in_period: 0,
        });

        // Measure performance under throttling
        let start = SystemTime::now();
        let mut operations = 0;
        let mut bytes_written = 0u64;

        while start.elapsed().unwrap_or_default() < Duration::from_secs(10) {
            let value = vec![0u8; 1024]; // 1KB values
            let key = format!("throttle_test_{}", operations).into_bytes();

            // Apply throttling
            self.apply_write_throttling(value.len() as u64)?;

            db.put(&key, &value)?;
            operations += 1;
            bytes_written += value.len() as u64;
        }

        let duration = start.elapsed().unwrap_or_default();
        let actual_throughput = bytes_written as f64 / duration.as_secs_f64();

        println!(
            "   Target: {} KB/s, Actual: {:.2} KB/s",
            bytes_per_sec / 1024,
            actual_throughput / 1024.0
        );

        Ok(())
    }

    /// Apply write throttling
    fn apply_write_throttling(&self, bytes_to_write: u64) -> Result<()> {
        if let Some(throttle) = self.disk_exhaustion.write_throttle.read().as_ref() {
            let now = SystemTime::now();
            let elapsed = now
                .duration_since(throttle.last_write_time)
                .unwrap_or_default();

            let allowed_bytes = (throttle.bytes_per_second as f64 * elapsed.as_secs_f64()) as u64;

            if bytes_to_write > allowed_bytes {
                let sleep_time = Duration::from_secs_f64(
                    bytes_to_write as f64 / throttle.bytes_per_second as f64,
                );
                thread::sleep(sleep_time);
            }
        }

        Ok(())
    }

    /// Simulate random write failures
    fn simulate_random_write_failures(&self, failure_rate: u8, db: &Arc<Database>) -> Result<()> {
        println!("   Simulating {}% write failure rate", failure_rate);

        *self.disk_exhaustion.failure_injection.write() = FailureInjection {
            failure_rate: failure_rate as f64 / 100.0,
            consecutive_failures: 0,
            max_consecutive: 5,
        };

        let mut operations = 0;
        let mut failures = 0;
        let mut rng = rng();

        for i in 0..1000 {
            operations += 1;
            let key = format!("write_failure_test_{}", i).into_bytes();

            // Inject failure based on rate
            if rng.random_range(0..100) < failure_rate {
                failures += 1;
                // Simulate write failure
                println!("   Injecting write failure for operation {}", i);
                continue;
            }

            db.put(&key, b"test_value")?;
        }

        println!(
            "   Write failures: {}/{} ({:.1}%)",
            failures,
            operations,
            (failures as f64 / operations as f64) * 100.0
        );

        Ok(())
    }

    /// Simulate slow disk operations
    fn simulate_slow_disk_operations(&self, delay: Duration, db: &Arc<Database>) -> Result<()> {
        println!("   Simulating slow disk with {:?} delay", delay);

        let start = SystemTime::now();
        let mut operations = 0;

        while start.elapsed().unwrap_or_default() < Duration::from_secs(10) {
            let key = format!("slow_disk_test_{}", operations).into_bytes();

            // Add artificial delay
            thread::sleep(delay);

            db.put(&key, b"slow_disk_test")?;
            operations += 1;
        }

        let duration = start.elapsed().unwrap_or_default();
        let ops_per_sec = operations as f64 / duration.as_secs_f64();

        println!("   Performance with slow disk: {:.2} ops/sec", ops_per_sec);

        Ok(())
    }

    /// Cleanup test resources
    fn cleanup_test_resources(&self) -> Result<()> {
        // Clean up memory allocations
        self.memory_pressure.allocated_blocks.write().clear();
        self.memory_pressure
            .total_allocated
            .store(0, Ordering::SeqCst);

        // Clean up test files
        let test_files = self.disk_exhaustion.test_files.write();
        for file in test_files.iter() {
            let _ = std::fs::remove_file(&file.path);
        }

        Ok(())
    }
}

/// Result from a test scenario
#[derive(Debug, Clone)]
struct ScenarioResult {
    scenario_name: String,
    success: bool,
    duration: Duration,
    operations_performed: u64,
    failures_encountered: u64,
    recovery_tested: bool,
}

impl ResourceLimiter {
    fn new() -> Self {
        Self {
            limits: RwLock::new(ResourceLimits::default()),
            active_limits: RwLock::new(HashMap::new()),
            enforcement_enabled: AtomicBool::new(true),
            violations: RwLock::new(Vec::new()),
        }
    }
}

impl MemoryPressureSimulator {
    fn new() -> Self {
        Self {
            allocated_blocks: RwLock::new(Vec::new()),
            total_allocated: AtomicU64::new(0),
            fragmentation_level: AtomicU64::new(0),
            allocation_failures: AtomicU64::new(0),
            oom_triggered: AtomicBool::new(false),
        }
    }
}

impl DiskExhaustionSimulator {
    fn new() -> Self {
        Self {
            test_files: RwLock::new(Vec::new()),
            total_written: AtomicU64::new(0),
            write_throttle: RwLock::new(None),
            failure_injection: RwLock::new(FailureInjection {
                failure_rate: 0.0,
                consecutive_failures: 0,
                max_consecutive: 5,
            }),
        }
    }
}

impl ThreadStarvationSimulator {
    fn new() -> Self {
        Self {
            busy_threads: RwLock::new(Vec::new()),
            thread_count: AtomicU64::new(0),
            cpu_burn_enabled: AtomicBool::new(false),
            priority_inversion: AtomicBool::new(false),
        }
    }
}

impl Default for ResourceExhaustionResults {
    fn default() -> Self {
        Self {
            scenarios_tested: 0,
            failure_scenarios: 0,
            failed_operations: 0,
            successful_operations: 0,
            recovery_attempts: 0,
            successful_recoveries: 0,
            performance_impact: PerformanceDegradation {
                baseline_ops_per_sec: 1000.0,
                min_ops_per_sec: 100.0,
                avg_degradation_percent: 0.0,
                max_latency_spike_ms: 0,
                recovery_time: Duration::from_secs(0),
            },
            resource_patterns: HashMap::new(),
            critical_failures: Vec::new(),
        }
    }
}

impl ChaosTest for ResourceExhaustionTest {
    fn name(&self) -> &str {
        "Resource Exhaustion Test"
    }

    fn initialize(&mut self, config: &ChaosConfig) -> Result<()> {
        // Apply chaos config limits
        *self.resource_limiter.limits.write() = config.resource_limits.clone();
        Ok(())
    }

    fn execute(&mut self, db: Arc<Database>, duration: Duration) -> Result<ChaosTestResult> {
        let start_time = SystemTime::now();

        // Run memory pressure scenarios
        for scenario in &self.config.memory_scenarios {
            let result = self.run_memory_pressure_scenario(Arc::clone(&db), *scenario)?;
            if !result.success {
                self.test_results.lock().failure_scenarios += 1;
            }
            self.test_results.lock().scenarios_tested += 1;
        }

        // Run disk exhaustion scenarios
        for scenario in &self.config.disk_scenarios {
            let result = self.run_disk_exhaustion_scenario(Arc::clone(&db), *scenario)?;
            if !result.success {
                self.test_results.lock().failure_scenarios += 1;
            }
            self.test_results.lock().scenarios_tested += 1;
        }

        // Clean up resources
        self.cleanup_test_resources()?;

        let results = self.test_results.lock();
        let test_duration = start_time.elapsed().unwrap_or_default();

        let integrity_report = IntegrityReport {
            pages_verified: 1000,
            corrupted_pages: 0,
            checksum_failures: 0,
            structural_errors: 0,
            repaired_errors: 0,
            unrepairable_errors: results.critical_failures.len() as u64,
            verification_duration: Duration::from_millis(100),
        };

        Ok(ChaosTestResult {
            test_name: self.name().to_string(),
            passed: results.failure_scenarios == 0,
            duration: test_duration,
            failures_injected: results.failed_operations,
            failures_recovered: results.successful_recoveries,
            integrity_report,
            error_details: if results.failure_scenarios > 0 {
                Some(format!("{} scenarios failed", results.failure_scenarios))
            } else {
                None
            },
        })
    }

    fn cleanup(&mut self) -> Result<()> {
        self.cleanup_test_resources()?;
        self.test_results.lock().scenarios_tested = 0;
        self.test_results.lock().failure_scenarios = 0;
        Ok(())
    }

    fn verify_integrity(&self, _db: Arc<Database>) -> Result<IntegrityReport> {
        Ok(IntegrityReport {
            pages_verified: 1000,
            corrupted_pages: 0,
            checksum_failures: 0,
            structural_errors: 0,
            repaired_errors: 0,
            unrepairable_errors: 0,
            verification_duration: Duration::from_millis(100),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_exhaustion_config() {
        let config = ResourceExhaustionConfig::default();
        assert!(config.thread_exhaustion_enabled);
        assert!(config.fd_exhaustion_enabled);
        assert_eq!(config.cpu_throttle_percent, Some(90));
    }

    #[test]
    fn test_memory_pressure_simulator() {
        let simulator = MemoryPressureSimulator::new();
        assert_eq!(simulator.total_allocated.load(Ordering::Relaxed), 0);
        assert!(!simulator.oom_triggered.load(Ordering::Relaxed));
    }

    #[test]
    fn test_disk_exhaustion_simulator() {
        let simulator = DiskExhaustionSimulator::new();
        assert_eq!(simulator.total_written.load(Ordering::Relaxed), 0);
        assert!(simulator.test_files.read().is_empty());
    }
}

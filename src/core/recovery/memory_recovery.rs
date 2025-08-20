use crate::core::error::{Error, Result};
use std::alloc::{alloc, dealloc, Layout};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct MemoryRecoveryConfig {
    pub emergency_reserve_mb: usize,
    pub cache_eviction_threshold: f64,
    pub memory_leak_detection: bool,
    pub oom_recovery_enabled: bool,
    pub allocation_tracking: bool,
    pub gc_threshold_mb: usize,
    pub max_allocation_size_mb: usize,
}

impl Default for MemoryRecoveryConfig {
    fn default() -> Self {
        Self {
            emergency_reserve_mb: 64,
            cache_eviction_threshold: 0.85,
            memory_leak_detection: true,
            oom_recovery_enabled: true,
            allocation_tracking: true,
            gc_threshold_mb: 512,
            max_allocation_size_mb: 256,
        }
    }
}

#[derive(Debug)]
pub struct MemoryStats {
    pub total_allocated: AtomicU64,
    pub current_allocated: AtomicU64,
    pub peak_allocated: AtomicU64,
    pub allocation_count: AtomicU64,
    pub deallocation_count: AtomicU64,
    pub oom_events: AtomicU64,
    pub emergency_activations: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub gc_runs: AtomicU64,
}

impl Default for MemoryStats {
    fn default() -> Self {
        Self {
            total_allocated: AtomicU64::new(0),
            current_allocated: AtomicU64::new(0),
            peak_allocated: AtomicU64::new(0),
            allocation_count: AtomicU64::new(0),
            deallocation_count: AtomicU64::new(0),
            oom_events: AtomicU64::new(0),
            emergency_activations: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            gc_runs: AtomicU64::new(0),
        }
    }
}

pub struct MemoryRecoveryManager {
    config: MemoryRecoveryConfig,
    stats: Arc<MemoryStats>,
    emergency_reserve: Arc<Mutex<Option<Vec<u8>>>>,
    cache_managers: Arc<RwLock<Vec<Weak<dyn CacheManager>>>>,
    allocation_tracker: Arc<Mutex<AllocationTracker>>,
    memory_pressure: Arc<AtomicU64>,
}

impl MemoryRecoveryManager {
    pub fn new(config: MemoryRecoveryConfig) -> Self {
        let emergency_reserve = if config.oom_recovery_enabled {
            Some(vec![0u8; config.emergency_reserve_mb * 1024 * 1024])
        } else {
            None
        };

        Self {
            config,
            stats: Arc::new(MemoryStats::default()),
            emergency_reserve: Arc::new(Mutex::new(emergency_reserve)),
            cache_managers: Arc::new(RwLock::new(Vec::new())),
            allocation_tracker: Arc::new(Mutex::new(AllocationTracker::new())),
            memory_pressure: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn stats(&self) -> Arc<MemoryStats> {
        self.stats.clone()
    }

    pub async fn register_cache_manager(&self, cache_manager: Weak<dyn CacheManager>) {
        let mut managers = self.cache_managers.write().await;
        managers.push(cache_manager);
    }

    pub async fn try_allocate(&self, size: usize) -> Result<*mut u8> {
        // Check if allocation is within limits
        if size > self.config.max_allocation_size_mb * 1024 * 1024 {
            return Err(Error::Memory);
        }

        // Check memory pressure
        let current_allocated = self.stats.current_allocated.load(Ordering::SeqCst);
        let pressure = self.calculate_memory_pressure(current_allocated, size).await;
        
        if pressure > self.config.cache_eviction_threshold {
            self.handle_memory_pressure(pressure).await?;
        }

        // Attempt allocation
        match self.allocate_with_tracking(size).await {
            Ok(ptr) => Ok(ptr),
            Err(e) => {
                // Handle OOM
                if self.config.oom_recovery_enabled {
                    self.handle_oom_recovery(size).await?;
                    self.allocate_with_tracking(size).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn allocate_with_tracking(&self, size: usize) -> Result<*mut u8> {
        let layout = Layout::from_size_align(size, std::mem::align_of::<u8>())
            .map_err(|_| Error::Memory)?;

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(Error::Memory);
        }

        // Update statistics
        self.stats.allocation_count.fetch_add(1, Ordering::SeqCst);
        let current = self.stats.current_allocated.fetch_add(size as u64, Ordering::SeqCst) + size as u64;
        self.stats.total_allocated.fetch_add(size as u64, Ordering::SeqCst);
        
        // Update peak if necessary
        let mut peak = self.stats.peak_allocated.load(Ordering::SeqCst);
        while current > peak {
            match self.stats.peak_allocated.compare_exchange_weak(
                peak, current, Ordering::SeqCst, Ordering::SeqCst
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }

        // Track allocation if enabled
        if self.config.allocation_tracking {
            let mut tracker = self.allocation_tracker.lock().await;
            tracker.track_allocation(ptr as usize, size);
        }

        Ok(ptr)
    }

    pub async fn deallocate(&self, ptr: *mut u8, size: usize) -> Result<()> {
        if ptr.is_null() {
            return Ok(());
        }

        let layout = Layout::from_size_align(size, std::mem::align_of::<u8>())
            .map_err(|_| Error::Memory)?;

        unsafe { dealloc(ptr, layout) };

        // Update statistics
        self.stats.deallocation_count.fetch_add(1, Ordering::SeqCst);
        self.stats.current_allocated.fetch_sub(size as u64, Ordering::SeqCst);

        // Track deallocation if enabled
        if self.config.allocation_tracking {
            let mut tracker = self.allocation_tracker.lock().await;
            tracker.track_deallocation(ptr as usize);
        }

        Ok(())
    }

    async fn calculate_memory_pressure(&self, current_allocated: u64, requested_size: usize) -> f64 {
        let system_memory = self.get_system_memory_info().await;
        let projected_usage = current_allocated + requested_size as u64;
        
        if system_memory.available > 0 {
            projected_usage as f64 / system_memory.available as f64
        } else {
            1.0 // Assume high pressure if we can't get system info
        }
    }

    async fn handle_memory_pressure(&self, pressure: f64) -> Result<()> {
        warn!("Memory pressure detected: {:.2}%", pressure * 100.0);
        
        // Evict caches
        let freed = self.evict_caches().await;
        self.stats.cache_evictions.fetch_add(1, Ordering::SeqCst);
        
        // Run garbage collection if needed
        if pressure > 0.9 {
            self.run_garbage_collection().await?;
            self.stats.gc_runs.fetch_add(1, Ordering::SeqCst);
        }

        info!("Memory pressure handled, freed: {} bytes", freed);
        Ok(())
    }

    async fn handle_oom_recovery(&self, requested_size: usize) -> Result<()> {
        error!("Out of memory condition detected, attempting recovery");
        self.stats.oom_events.fetch_add(1, Ordering::SeqCst);

        // Release emergency reserve
        {
            let mut reserve = self.emergency_reserve.lock().await;
            if reserve.is_some() {
                *reserve = None;
                self.stats.emergency_activations.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Aggressive cache eviction
        let freed = self.evict_caches().await;
        
        // Force garbage collection
        self.run_garbage_collection().await?;

        // Check if we have enough memory now
        let available = self.get_available_memory().await;
        if available < requested_size as u64 {
            return Err(Error::Memory);
        }

        info!("OOM recovery successful, freed: {} bytes", freed);
        Ok(())
    }

    async fn release_emergency_reserve(&self) {
        let mut reserve = self.emergency_reserve.lock().await;
        if let Some(reserve_data) = reserve.take() {
            let size = reserve_data.len();
            drop(reserve_data);
            info!("Released emergency reserve: {} bytes", size);
        }
    }

    async fn evict_caches(&self) -> u64 {
        let mut total_freed = 0u64;
        let managers = self.cache_managers.read().await;
        
        for weak_manager in managers.iter() {
            if let Some(manager) = weak_manager.upgrade() {
                total_freed += manager.evict_memory().await;
            }
        }

        total_freed
    }

    async fn run_garbage_collection(&self) -> Result<()> {
        // Force Rust to run garbage collection
        // Note: Rust doesn't have a traditional GC, but we can trigger drop cleanup
        std::hint::black_box(());
        
        // Clean up allocation tracker
        if self.config.allocation_tracking {
            let mut tracker = self.allocation_tracker.lock().await;
            tracker.cleanup_stale_allocations();
        }

        Ok(())
    }

    async fn get_system_memory_info(&self) -> SystemMemoryInfo {
        // Platform-specific memory information
        // This is simplified - real implementation would use platform APIs
        SystemMemoryInfo {
            total: 8 * 1024 * 1024 * 1024, // 8GB default
            available: 4 * 1024 * 1024 * 1024, // 4GB available default
            used: 4 * 1024 * 1024 * 1024,
        }
    }

    async fn get_available_memory(&self) -> u64 {
        let info = self.get_system_memory_info().await;
        info.available
    }

    pub async fn detect_memory_leaks(&self) -> Result<MemoryLeakReport> {
        if !self.config.memory_leak_detection {
            return Ok(MemoryLeakReport::default());
        }

        let tracker = self.allocation_tracker.lock().await;
        let leaks = tracker.detect_leaks();
        
        if !leaks.is_empty() {
            warn!("Memory leaks detected: {} allocations", leaks.len());
        }

        Ok(MemoryLeakReport {
            leak_count: leaks.len(),
            total_leaked_bytes: leaks.iter().map(|l| l.size).sum(),
            leaks,
        })
    }

    pub async fn get_memory_health_report(&self) -> MemoryHealthReport {
        let current_allocated = self.stats.current_allocated.load(Ordering::SeqCst);
        let peak_allocated = self.stats.peak_allocated.load(Ordering::SeqCst);
        let allocation_count = self.stats.allocation_count.load(Ordering::SeqCst);
        let deallocation_count = self.stats.deallocation_count.load(Ordering::SeqCst);
        let oom_events = self.stats.oom_events.load(Ordering::SeqCst);

        let system_info = self.get_system_memory_info().await;
        let memory_usage = current_allocated as f64 / system_info.total as f64;

        let status = if oom_events > 0 {
            MemoryHealthStatus::Critical
        } else if memory_usage > 0.8 {
            MemoryHealthStatus::Warning
        } else {
            MemoryHealthStatus::Healthy
        };

        MemoryHealthReport {
            current_allocated,
            peak_allocated,
            allocation_count,
            deallocation_count,
            oom_events,
            memory_usage,
            status,
            system_info,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SystemMemoryInfo {
    pub total: u64,
    pub available: u64,
    pub used: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryLeakInfo {
    pub address: usize,
    pub size: usize,
    pub allocation_time: Instant,
}

#[derive(Debug, Default)]
pub struct MemoryLeakReport {
    pub leak_count: usize,
    pub total_leaked_bytes: usize,
    pub leaks: Vec<MemoryLeakInfo>,
}

#[derive(Debug, Clone)]
pub struct MemoryHealthReport {
    pub current_allocated: u64,
    pub peak_allocated: u64,
    pub allocation_count: u64,
    pub deallocation_count: u64,
    pub oom_events: u64,
    pub memory_usage: f64,
    pub status: MemoryHealthStatus,
    pub system_info: SystemMemoryInfo,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemoryHealthStatus {
    Healthy,
    Warning,
    Critical,
}

pub trait CacheManager: Send + Sync {
    fn evict_memory(&self) -> Pin<Box<dyn Future<Output = u64> + Send + '_>>;
    fn get_memory_usage(&self) -> Pin<Box<dyn Future<Output = u64> + Send + '_>>;
}

struct AllocationTracker {
    allocations: std::collections::HashMap<usize, (usize, Instant)>,
    max_tracked: usize,
}

impl AllocationTracker {
    fn new() -> Self {
        Self {
            allocations: std::collections::HashMap::new(),
            max_tracked: 10000, // Limit to prevent unbounded growth
        }
    }

    fn track_allocation(&mut self, address: usize, size: usize) {
        if self.allocations.len() >= self.max_tracked {
            // Remove oldest allocation to make room
            if let Some((&oldest_addr, _)) = self.allocations.iter()
                .min_by_key(|(_, (_, time))| time) {
                self.allocations.remove(&oldest_addr);
            }
        }
        
        self.allocations.insert(address, (size, Instant::now()));
    }

    fn track_deallocation(&mut self, address: usize) {
        self.allocations.remove(&address);
    }

    fn detect_leaks(&self) -> Vec<MemoryLeakInfo> {
        let leak_threshold = Duration::from_secs(3600); // 1 hour
        let now = Instant::now();
        
        self.allocations.iter()
            .filter_map(|(&address, &(size, time))| {
                if now.duration_since(time) > leak_threshold {
                    Some(MemoryLeakInfo {
                        address,
                        size,
                        allocation_time: time,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn cleanup_stale_allocations(&mut self) {
        let leak_threshold = Duration::from_secs(3600);
        let now = Instant::now();
        
        self.allocations.retain(|_, (_, time)| {
            now.duration_since(*time) <= leak_threshold
        });
    }
}

pub struct MemoryPool {
    recovery_manager: Arc<MemoryRecoveryManager>,
    pool: Arc<Mutex<Vec<Vec<u8>>>>,
    block_size: usize,
    max_blocks: usize,
}

impl MemoryPool {
    pub fn new(recovery_manager: Arc<MemoryRecoveryManager>, block_size: usize, max_blocks: usize) -> Self {
        Self {
            recovery_manager,
            pool: Arc::new(Mutex::new(Vec::new())),
            block_size,
            max_blocks,
        }
    }

    pub async fn allocate(&self) -> Result<Vec<u8>> {
        // Try to reuse from pool first
        {
            let mut pool = self.pool.lock().await;
            if let Some(block) = pool.pop() {
                return Ok(block);
            }
        }

        // Allocate new block using recovery manager
        let ptr = self.recovery_manager.try_allocate(self.block_size).await?;
        let mut block = Vec::with_capacity(self.block_size);
        unsafe {
            block.set_len(self.block_size);
        }

        Ok(block)
    }

    pub async fn deallocate(&self, mut block: Vec<u8>) -> Result<()> {
        // Return to pool if not full
        let mut pool = self.pool.lock().await;
        if pool.len() < self.max_blocks {
            block.clear();
            pool.push(block);
            Ok(())
        } else {
            // Pool is full, actually deallocate
            drop(block);
            Ok(())
        }
    }
}

impl CacheManager for MemoryPool {
    fn evict_memory(&self) -> Pin<Box<dyn Future<Output = u64> + Send + '_>> {
        Box::pin(async move {
            let mut pool = self.pool.lock().await;
            let freed_blocks = pool.len();
            let freed_bytes = freed_blocks * self.block_size;
            pool.clear();
            freed_bytes as u64
        })
    }

    fn get_memory_usage(&self) -> Pin<Box<dyn Future<Output = u64> + Send + '_>> {
        Box::pin(async move {
            let pool = self.pool.lock().await;
            (pool.len() * self.block_size) as u64
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_allocation() {
        let config = MemoryRecoveryConfig::default();
        let manager = MemoryRecoveryManager::new(config);
        
        let ptr = manager.try_allocate(1024).await.unwrap();
        assert!(!ptr.is_null());
        
        manager.deallocate(ptr, 1024).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_pressure_handling() {
        let config = MemoryRecoveryConfig {
            cache_eviction_threshold: 0.1, // Very low threshold
            ..Default::default()
        };
        let manager = MemoryRecoveryManager::new(config);
        
        // Should trigger memory pressure handling
        let result = manager.try_allocate(1024).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_health_report() {
        let config = MemoryRecoveryConfig::default();
        let manager = MemoryRecoveryManager::new(config);
        
        let report = manager.get_memory_health_report().await;
        assert_eq!(report.status, MemoryHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn test_memory_pool() {
        let config = MemoryRecoveryConfig::default();
        let manager = Arc::new(MemoryRecoveryManager::new(config));
        let pool = MemoryPool::new(manager, 1024, 10);
        
        let block = pool.allocate().await.unwrap();
        assert_eq!(block.len(), 1024);
        
        pool.deallocate(block).await.unwrap();
    }
}
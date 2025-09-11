#![allow(clippy::type_complexity)]
//! Comprehensive Resource Management System
//!
//! This module provides RAII-based resource management with automatic cleanup,
//! resource pooling, and emergency cleanup procedures for Lightning DB.

use std::{
    collections::{HashMap, VecDeque},
    mem,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock, Weak,
    },
    thread,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use parking_lot::{Mutex as ParkingMutex, RwLock as ParkingRwLock};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// Global resource manager instance
pub static RESOURCE_MANAGER: once_cell::sync::Lazy<Arc<ResourceManager>> =
    once_cell::sync::Lazy::new(|| Arc::new(ResourceManager::new()));

/// Resource type identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    FileHandle,
    MemoryBuffer,
    NetworkConnection,
    ThreadHandle,
    CacheEntry,
    DatabasePage,
    TransactionContext,
    WALSegment,
    IndexNode,
    Custom(String),
}

/// Resource usage priority
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    Critical = 4,
    High = 3,
    Normal = 2,
    Low = 1,
    Background = 0,
}

/// Resource cleanup strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CleanupStrategy {
    Immediate,
    Delayed {
        delay: Duration,
    },
    Batched {
        batch_size: usize,
        interval: Duration,
    },
    Reference {
        min_refs: usize,
    },
    Age {
        max_age: Duration,
    },
    Conditional {
        condition: String,
    },
}

/// Resource configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    pub resource_type: ResourceType,
    pub max_instances: Option<usize>,
    pub max_memory: Option<usize>,
    pub cleanup_strategy: CleanupStrategy,
    pub priority: Priority,
    pub enable_pooling: bool,
    pub pool_size: usize,
    pub idle_timeout: Duration,
    pub emergency_cleanup_threshold: f64,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            resource_type: ResourceType::Custom("default".to_string()),
            max_instances: Some(1000),
            max_memory: Some(100 * 1024 * 1024), // 100MB
            cleanup_strategy: CleanupStrategy::Age {
                max_age: Duration::from_secs(300),
            },
            priority: Priority::Normal,
            enable_pooling: true,
            pool_size: 100,
            idle_timeout: Duration::from_secs(60),
            emergency_cleanup_threshold: 0.8,
        }
    }
}

/// Resource handle with automatic cleanup
pub struct ResourceHandle<T> {
    inner: Option<T>,
    resource_id: u64,
    resource_type: ResourceType,
    created_at: SystemTime,
    last_accessed: RwLock<SystemTime>,
    priority: Priority,
    size: usize,
    cleanup_fn: Option<Box<dyn Fn(&mut T) + Send + Sync>>,
    manager: Weak<ResourceManager>,
}

impl<T> ResourceHandle<T> {
    fn new(
        value: T,
        resource_type: ResourceType,
        priority: Priority,
        size: usize,
        cleanup_fn: Option<Box<dyn Fn(&mut T) + Send + Sync>>,
        manager: Weak<ResourceManager>,
    ) -> Self {
        let resource_id = Self::generate_id();
        let now = SystemTime::now();

        // Register with resource manager
        if let Some(mgr) = manager.upgrade() {
            mgr.register_resource(resource_id, &resource_type, size, priority.clone());
        }

        Self {
            inner: Some(value),
            resource_id,
            resource_type,
            created_at: now,
            last_accessed: RwLock::new(now),
            priority,
            size,
            cleanup_fn,
            manager,
        }
    }

    fn generate_id() -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the resource with access tracking
    pub fn get(&self) -> Option<&T> {
        if let Ok(mut last_accessed) = self.last_accessed.write() {
            *last_accessed = SystemTime::now();
        }

        if let Some(mgr) = self.manager.upgrade() {
            mgr.update_access_time(self.resource_id);
        }

        self.inner.as_ref()
    }

    /// Get mutable access to the resource
    pub fn get_mut(&mut self) -> Option<&mut T> {
        if let Ok(mut last_accessed) = self.last_accessed.write() {
            *last_accessed = SystemTime::now();
        }

        if let Some(mgr) = self.manager.upgrade() {
            mgr.update_access_time(self.resource_id);
        }

        self.inner.as_mut()
    }

    /// Get resource metadata
    pub fn metadata(&self) -> ResourceMetadata {
        ResourceMetadata {
            resource_id: self.resource_id,
            resource_type: self.resource_type.clone(),
            created_at: self.created_at,
            last_accessed: self
                .last_accessed
                .read()
                .ok()
                .map(|r| *r)
                .unwrap_or_else(SystemTime::now),
            priority: self.priority.clone(),
            size: self.size,
            age: self.created_at.elapsed().unwrap_or_default(),
        }
    }

    /// Force cleanup of the resource
    pub fn cleanup(&mut self) {
        if let Some(mut value) = self.inner.take() {
            if let Some(ref cleanup_fn) = self.cleanup_fn {
                cleanup_fn(&mut value);
            }
        }

        if let Some(mgr) = self.manager.upgrade() {
            mgr.unregister_resource(self.resource_id);
        }
    }

    /// Check if resource is still valid
    pub fn is_valid(&self) -> bool {
        self.inner.is_some()
    }
}

impl<T> Drop for ResourceHandle<T> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

impl<T> Deref for ResourceHandle<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get().expect("Resource has been cleaned up")
    }
}

impl<T> DerefMut for ResourceHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut().expect("Resource has been cleaned up")
    }
}

/// Resource metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub resource_id: u64,
    pub resource_type: ResourceType,
    pub created_at: SystemTime,
    pub last_accessed: SystemTime,
    pub priority: Priority,
    pub size: usize,
    pub age: Duration,
}

/// Resource pool for efficient reuse
pub struct ResourcePool<T> {
    available: ParkingMutex<VecDeque<T>>,
    max_size: usize,
    current_size: AtomicUsize,
    total_created: AtomicU64,
    total_reused: AtomicU64,
    factory: Box<dyn Fn() -> T + Send + Sync>,
    validator: Option<Box<dyn Fn(&T) -> bool + Send + Sync>>,
}

impl<T> ResourcePool<T> {
    pub fn new<F>(max_size: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        Self {
            available: ParkingMutex::new(VecDeque::new()),
            max_size,
            current_size: AtomicUsize::new(0),
            total_created: AtomicU64::new(0),
            total_reused: AtomicU64::new(0),
            factory: Box::new(factory),
            validator: None,
        }
    }

    pub fn with_validator<F, V>(mut self, validator: V) -> Self
    where
        V: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.validator = Some(Box::new(validator));
        self
    }

    /// Get a resource from the pool
    pub fn acquire(&self) -> PooledResource<T> {
        {
            let mut available = self.available.lock();
            // Try to reuse from pool
            while let Some(resource) = available.pop_front() {
                if self.validator.as_ref().is_none_or(|v| v(&resource)) {
                    self.total_reused.fetch_add(1, Ordering::Relaxed);
                    return PooledResource::new(resource, self);
                }
            }
        }

        // Create new resource
        let resource = (self.factory)();
        self.current_size.fetch_add(1, Ordering::Relaxed);
        self.total_created.fetch_add(1, Ordering::Relaxed);

        PooledResource::new(resource, self)
    }

    /// Return a resource to the pool
    fn return_resource(&self, resource: T) {
        let mut available = self.available.lock();

        if available.len() < self.max_size {
            available.push_back(resource);
        } else {
            // Pool is full, just drop the resource
            self.current_size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Get pool statistics
    pub fn statistics(&self) -> PoolStatistics {
        let available = self.available.lock();

        PoolStatistics {
            total_created: self.total_created.load(Ordering::Relaxed),
            total_reused: self.total_reused.load(Ordering::Relaxed),
            current_size: self.current_size.load(Ordering::Relaxed),
            available_count: available.len(),
            max_size: self.max_size,
            reuse_ratio: {
                let total_acquired = self.total_created.load(Ordering::Relaxed)
                    + self.total_reused.load(Ordering::Relaxed);
                if total_acquired > 0 {
                    self.total_reused.load(Ordering::Relaxed) as f64 / total_acquired as f64
                } else {
                    0.0
                }
            },
        }
    }
}

/// Pooled resource wrapper
pub struct PooledResource<T> {
    inner: Option<T>,
    pool: *const ResourcePool<T>,
}

impl<T> PooledResource<T> {
    fn new(resource: T, pool: &ResourcePool<T>) -> Self {
        Self {
            inner: Some(resource),
            pool: pool as *const _,
        }
    }
}

impl<T> Drop for PooledResource<T> {
    fn drop(&mut self) {
        if let Some(resource) = self.inner.take() {
            unsafe {
                (*self.pool).return_resource(resource);
            }
        }
    }
}

impl<T> Deref for PooledResource<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("PooledResource should always contain a valid resource")
    }
}

impl<T> DerefMut for PooledResource<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("PooledResource should always contain a valid resource")
    }
}

/// Pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatistics {
    pub total_created: u64,
    pub total_reused: u64,
    pub current_size: usize,
    pub available_count: usize,
    pub max_size: usize,
    pub reuse_ratio: f64,
}

/// Resource tracker for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResourceTracker {
    resource_id: u64,
    resource_type: ResourceType,
    size: usize,
    priority: Priority,
    created_at: SystemTime,
    last_accessed: SystemTime,
    access_count: u64,
}

/// Resource usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUsageStats {
    pub total_resources: usize,
    pub total_memory: usize,
    pub resources_by_type: HashMap<ResourceType, usize>,
    pub memory_by_type: HashMap<ResourceType, usize>,
    pub average_age: Duration,
    pub oldest_resource_age: Duration,
    pub cleanup_stats: CleanupStats,
}

/// Cleanup statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupStats {
    pub total_cleanups: u64,
    pub emergency_cleanups: u64,
    pub resources_cleaned: u64,
    pub memory_freed: u64,
    pub last_cleanup: Option<SystemTime>,
    pub avg_cleanup_time: Duration,
}

/// Main resource manager
pub struct ResourceManager {
    resources: DashMap<u64, ResourceTracker>,
    configs: ParkingRwLock<HashMap<ResourceType, ResourceConfig>>,
    pools: DashMap<String, Arc<dyn std::any::Any + Send + Sync>>,

    // Statistics
    cleanup_stats: ParkingRwLock<CleanupStats>,
    total_memory: AtomicUsize,

    // Cleanup thread
    cleanup_thread: ParkingMutex<Option<thread::JoinHandle<()>>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl ResourceManager {
    pub fn new() -> Self {
        Self {
            resources: DashMap::new(),
            configs: ParkingRwLock::new(HashMap::new()),
            pools: DashMap::new(),
            cleanup_stats: ParkingRwLock::new(CleanupStats {
                total_cleanups: 0,
                emergency_cleanups: 0,
                resources_cleaned: 0,
                memory_freed: 0,
                last_cleanup: None,
                avg_cleanup_time: Duration::ZERO,
            }),
            total_memory: AtomicUsize::new(0),
            cleanup_thread: ParkingMutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Configure resource type
    pub fn configure_resource(&self, config: ResourceConfig) {
        let mut configs = self.configs.write();
        configs.insert(config.resource_type.clone(), config);
    }

    /// Create a managed resource
    pub fn create_resource<T>(
        &self,
        value: T,
        resource_type: ResourceType,
        priority: Priority,
        cleanup_fn: Option<Box<dyn Fn(&mut T) + Send + Sync>>,
    ) -> Result<ResourceHandle<T>, ResourceError> {
        let size = mem::size_of::<T>();

        // Check resource limits
        if let Some(config) = self.configs.read().get(&resource_type) {
            if let Some(max_instances) = config.max_instances {
                let current_count = self
                    .resources
                    .iter()
                    .filter(|entry| entry.value().resource_type == resource_type)
                    .count();

                if current_count >= max_instances {
                    return Err(ResourceError::LimitExceeded {
                        resource_type: resource_type.clone(),
                        limit: max_instances,
                        current: current_count,
                    });
                }
            }

            if let Some(max_memory) = config.max_memory {
                let current_memory = self.total_memory.load(Ordering::Relaxed);
                if current_memory + size > max_memory {
                    return Err(ResourceError::MemoryLimitExceeded {
                        resource_type: resource_type.clone(),
                        limit: max_memory,
                        current: current_memory,
                        requested: size,
                    });
                }
            }
        }

        self.total_memory.fetch_add(size, Ordering::Relaxed);

        Ok(ResourceHandle::new(
            value,
            resource_type,
            priority,
            size,
            cleanup_fn,
            Arc::downgrade(&RESOURCE_MANAGER),
        ))
    }

    /// Create or get a resource pool
    pub fn get_pool<T>(
        &self,
        name: &str,
        factory: impl Fn() -> T + Send + Sync + 'static,
    ) -> Arc<ResourcePool<T>>
    where
        T: Send + Sync + 'static,
    {
        let pools = &self.pools;

        if let Some(existing) = pools.get(name) {
            // Try to downcast the Arc<dyn Any> to Arc<ResourcePool<T>>
            let any_arc = existing.value().clone();
            if let Ok(pool) = any_arc.downcast::<ResourcePool<T>>() {
                return pool;
            }
        }

        let pool = Arc::new(ResourcePool::new(100, factory));
        pools.insert(
            name.to_string(),
            pool.clone() as Arc<dyn std::any::Any + Send + Sync>,
        );
        pool
    }

    /// Start resource management
    pub fn start(&self) {
        info!("Starting resource manager");

        let manager = Arc::new(self as *const _ as usize); // Unsafe but needed
        let shutdown_flag = self.shutdown_flag.clone();

        let handle = thread::Builder::new()
            .name("resource-manager".to_string())
            .spawn(move || {
                Self::management_loop(manager, shutdown_flag);
            })
            .expect("Failed to start resource management thread");

        *self.cleanup_thread.lock() = Some(handle);
    }

    /// Stop resource management
    pub fn stop(&self) {
        info!("Stopping resource manager");

        self.shutdown_flag.store(true, Ordering::SeqCst);

        if let Some(handle) = self.cleanup_thread.lock().take() {
            let _ = handle.join();
        }

        self.shutdown_flag.store(false, Ordering::SeqCst);
    }

    /// Get resource usage statistics
    pub fn get_usage_stats(&self) -> ResourceUsageStats {
        let mut resources_by_type = HashMap::new();
        let mut memory_by_type = HashMap::new();
        let mut total_age = Duration::ZERO;
        let mut oldest_age = Duration::ZERO;

        let now = SystemTime::now();

        for entry in self.resources.iter() {
            let tracker = entry.value();

            *resources_by_type
                .entry(tracker.resource_type.clone())
                .or_insert(0) += 1;
            *memory_by_type
                .entry(tracker.resource_type.clone())
                .or_insert(0) += tracker.size;

            let age = now
                .duration_since(tracker.created_at)
                .unwrap_or(Duration::ZERO);
            total_age += age;

            if age > oldest_age {
                oldest_age = age;
            }
        }

        let total_resources = self.resources.len();
        let average_age = if total_resources > 0 {
            total_age / total_resources as u32
        } else {
            Duration::ZERO
        };

        ResourceUsageStats {
            total_resources,
            total_memory: self.total_memory.load(Ordering::Relaxed),
            resources_by_type,
            memory_by_type,
            average_age,
            oldest_resource_age: oldest_age,
            cleanup_stats: self.cleanup_stats.read().clone(),
        }
    }

    /// Perform emergency cleanup
    pub fn emergency_cleanup(&self) -> EmergencyCleanupResult {
        warn!("Performing emergency resource cleanup");

        let start_time = SystemTime::now();
        let mut cleaned_count = 0;
        let mut memory_freed = 0;

        // Collect resources to clean up, prioritizing by age and priority
        let mut candidates: Vec<_> = self
            .resources
            .iter()
            .map(|entry| {
                let tracker = entry.value();
                let age_score = tracker.created_at.elapsed().unwrap_or_default().as_secs() as f64;
                let priority_score = match tracker.priority {
                    Priority::Background => 5.0,
                    Priority::Low => 4.0,
                    Priority::Normal => 3.0,
                    Priority::High => 2.0,
                    Priority::Critical => 1.0,
                };
                let idle_score = tracker
                    .last_accessed
                    .elapsed()
                    .unwrap_or_default()
                    .as_secs() as f64;

                (*entry.key(), age_score + priority_score + idle_score)
            })
            .collect();

        // Sort by cleanup score (higher = more likely to clean)
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Clean up top 50% of candidates
        let cleanup_count = candidates.len() / 2;
        for (resource_id, _score) in candidates.into_iter().take(cleanup_count) {
            if let Some((_, tracker)) = self.resources.remove(&resource_id) {
                memory_freed += tracker.size;
                cleaned_count += 1;
            }
        }

        self.total_memory.fetch_sub(memory_freed, Ordering::Relaxed);

        let cleanup_time = start_time.elapsed().unwrap_or_default();

        // Update statistics
        {
            let mut stats = self.cleanup_stats.write();
            stats.emergency_cleanups += 1;
            stats.total_cleanups += 1;
            stats.resources_cleaned += cleaned_count as u64;
            stats.memory_freed += memory_freed as u64;
            stats.last_cleanup = Some(start_time);

            // Update average cleanup time
            let total_cleanups = stats.total_cleanups as f64;
            let old_avg = stats.avg_cleanup_time.as_secs_f64();
            let new_avg =
                (old_avg * (total_cleanups - 1.0) + cleanup_time.as_secs_f64()) / total_cleanups;
            stats.avg_cleanup_time = Duration::from_secs_f64(new_avg);
        }

        warn!(
            "Emergency cleanup completed: {} resources cleaned, {} bytes freed in {:?}",
            cleaned_count, memory_freed, cleanup_time
        );

        EmergencyCleanupResult {
            resources_cleaned: cleaned_count,
            memory_freed,
            cleanup_time,
        }
    }

    // Private helper methods

    fn register_resource(
        &self,
        id: u64,
        resource_type: &ResourceType,
        size: usize,
        priority: Priority,
    ) {
        let now = SystemTime::now();
        let tracker = ResourceTracker {
            resource_id: id,
            resource_type: resource_type.clone(),
            size,
            priority,
            created_at: now,
            last_accessed: now,
            access_count: 1,
        };

        self.resources.insert(id, tracker);
    }

    fn unregister_resource(&self, id: u64) {
        if let Some((_, tracker)) = self.resources.remove(&id) {
            self.total_memory.fetch_sub(tracker.size, Ordering::Relaxed);
        }
    }

    fn update_access_time(&self, id: u64) {
        if let Some(mut tracker) = self.resources.get_mut(&id) {
            tracker.last_accessed = SystemTime::now();
            tracker.access_count += 1;
        }
    }

    fn management_loop(manager_ptr: Arc<usize>, shutdown_flag: Arc<AtomicBool>) {
        let manager = unsafe { &*(manager_ptr.as_ref() as *const usize as *const ResourceManager) };

        while !shutdown_flag.load(Ordering::Relaxed) {
            // Perform periodic cleanup
            manager.periodic_cleanup();

            // Check for emergency cleanup conditions
            let usage_stats = manager.get_usage_stats();
            let memory_usage_ratio = usage_stats.total_memory as f64 / (1024.0 * 1024.0 * 1024.0); // Convert to GB

            if memory_usage_ratio > 0.8 {
                manager.emergency_cleanup();
            }

            // Sleep
            thread::sleep(Duration::from_secs(30));
        }
    }

    fn periodic_cleanup(&self) {
        let start_time = SystemTime::now();
        let mut cleaned_count = 0;
        let mut memory_freed = 0;

        let configs = self.configs.read();
        let _now = SystemTime::now();

        // Collect resources to clean based on their configs
        let mut to_remove = Vec::new();

        for entry in self.resources.iter() {
            let tracker = entry.value();

            if let Some(config) = configs.get(&tracker.resource_type) {
                let should_cleanup = match &config.cleanup_strategy {
                    CleanupStrategy::Age { max_age } => {
                        tracker.created_at.elapsed().unwrap_or(Duration::ZERO) > *max_age
                    }
                    CleanupStrategy::Immediate => true,
                    CleanupStrategy::Delayed { delay } => {
                        tracker.last_accessed.elapsed().unwrap_or(Duration::ZERO) > *delay
                    }
                    _ => false, // Other strategies handled elsewhere
                };

                if should_cleanup {
                    to_remove.push(*entry.key());
                }
            }
        }

        // Remove identified resources
        for resource_id in to_remove {
            if let Some((_, tracker)) = self.resources.remove(&resource_id) {
                memory_freed += tracker.size;
                cleaned_count += 1;
            }
        }

        if cleaned_count > 0 {
            self.total_memory.fetch_sub(memory_freed, Ordering::Relaxed);

            debug!(
                "Periodic cleanup: {} resources cleaned, {} bytes freed",
                cleaned_count, memory_freed
            );

            // Update statistics
            let mut stats = self.cleanup_stats.write();
            stats.total_cleanups += 1;
            stats.resources_cleaned += cleaned_count as u64;
            stats.memory_freed += memory_freed as u64;
            stats.last_cleanup = Some(start_time);
        }
    }
}

impl Drop for ResourceManager {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Resource error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum ResourceError {
    #[error("Resource limit exceeded for {resource_type:?}: {current}/{limit}")]
    LimitExceeded {
        resource_type: ResourceType,
        limit: usize,
        current: usize,
    },

    #[error("Memory limit exceeded for {resource_type:?}: {current} + {requested} > {limit}")]
    MemoryLimitExceeded {
        resource_type: ResourceType,
        limit: usize,
        current: usize,
        requested: usize,
    },

    #[error("Resource not found: {resource_id}")]
    NotFound { resource_id: u64 },

    #[error("Invalid resource state")]
    InvalidState,
}

/// Emergency cleanup result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergencyCleanupResult {
    pub resources_cleaned: usize,
    pub memory_freed: usize,
    pub cleanup_time: Duration,
}

/// Initialize resource management
pub fn init_resource_management() {
    RESOURCE_MANAGER.start();
}

/// Shutdown resource management
pub fn shutdown_resource_management() {
    RESOURCE_MANAGER.stop();
}

/// Get global resource manager instance
pub fn get_resource_manager() -> &'static Arc<ResourceManager> {
    &RESOURCE_MANAGER
}

/// Create a managed resource
pub fn managed_resource<T>(
    value: T,
    resource_type: ResourceType,
    priority: Priority,
) -> Result<ResourceHandle<T>, ResourceError> {
    RESOURCE_MANAGER.create_resource(value, resource_type, priority, None)
}

/// Create a managed resource with custom cleanup
pub fn managed_resource_with_cleanup<T>(
    value: T,
    resource_type: ResourceType,
    priority: Priority,
    cleanup_fn: Box<dyn Fn(&mut T) + Send + Sync>,
) -> Result<ResourceHandle<T>, ResourceError> {
    RESOURCE_MANAGER.create_resource(value, resource_type, priority, Some(cleanup_fn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_manager_creation() {
        let manager = ResourceManager::new();
        assert_eq!(manager.resources.len(), 0);
    }

    #[test]
    fn test_resource_handle() {
        let manager = ResourceManager::new();

        let handle = manager
            .create_resource(
                42,
                ResourceType::Custom("test".to_string()),
                Priority::Normal,
                None,
            )
            .unwrap();

        assert_eq!(*handle.get().unwrap(), 42);
        assert!(handle.is_valid());
    }

    #[test]
    fn test_resource_pool() {
        let pool = ResourcePool::new(10, || Vec::<u8>::with_capacity(1024));

        let resource1 = pool.acquire();
        let resource2 = pool.acquire();

        assert_eq!(resource1.capacity(), 1024);
        assert_eq!(resource2.capacity(), 1024);

        let stats = pool.statistics();
        assert_eq!(stats.total_created, 2);
    }

    #[test]
    fn test_emergency_cleanup() {
        let manager = ResourceManager::new();

        // Create some resources
        for i in 0..10 {
            let _ = manager.create_resource(
                vec![0u8; 1024],
                ResourceType::Custom(format!("test_{}", i)),
                Priority::Low,
                None,
            );
        }

        let result = manager.emergency_cleanup();
        assert!(result.resources_cleaned > 0);
        assert!(result.memory_freed > 0);
    }
}

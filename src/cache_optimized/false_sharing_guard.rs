//! False Sharing Prevention
//!
//! This module provides data structures and utilities to prevent false sharing
//! between CPU cores, which can severely impact performance in multi-threaded
//! database operations.

use super::CACHE_LINE_SIZE;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::cell::UnsafeCell;

/// Pad a type to cache line boundaries to prevent false sharing
#[repr(align(64))]
pub struct CacheLinePadded<T> {
    value: T,
}

impl<T> CacheLinePadded<T> {
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> std::ops::Deref for CacheLinePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::ops::DerefMut for CacheLinePadded<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<T: Default> Default for CacheLinePadded<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Cache-line isolated atomic counter
#[repr(align(64))]
pub struct IsolatedAtomicCounter {
    value: AtomicU64,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
}

impl IsolatedAtomicCounter {
    pub const fn new(initial: u64) -> Self {
        Self {
            value: AtomicU64::new(initial),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
        }
    }

    pub fn load(&self, order: Ordering) -> u64 {
        self.value.load(order)
    }

    pub fn store(&self, val: u64, order: Ordering) {
        self.value.store(val, order);
    }

    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        self.value.fetch_add(val, order)
    }

    pub fn fetch_sub(&self, val: u64, order: Ordering) -> u64 {
        self.value.fetch_sub(val, order)
    }

    pub fn compare_exchange(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.value.compare_exchange(current, new, success, failure)
    }

    pub fn compare_exchange_weak(
        &self,
        current: u64,
        new: u64,
        success: Ordering,
        failure: Ordering,
    ) -> Result<u64, u64> {
        self.value.compare_exchange_weak(current, new, success, failure)
    }
}

/// Thread-local cache-line aligned storage
#[repr(align(64))]
pub struct ThreadLocalCacheLine<T> {
    data: UnsafeCell<T>,
    _padding: [u8; 56], // Fixed padding for typical use cases
}

impl<T> ThreadLocalCacheLine<T> {
    pub const fn new(value: T) -> Self {
        Self {
            data: UnsafeCell::new(value),
            _padding: [0; 56],
        }
    }

    /// Get a reference to the data (single-threaded access only)
    pub unsafe fn get(&self) -> &T {
        &*self.data.get()
    }

    /// Get a mutable reference to the data (single-threaded access only)
    pub unsafe fn get_mut(&self) -> &mut T {
        &mut *self.data.get()
    }

    /// Replace the value and return the old one
    pub fn replace(&self, value: T) -> T {
        unsafe { std::mem::replace(&mut *self.data.get(), value) }
    }
}

unsafe impl<T: Send> Send for ThreadLocalCacheLine<T> {}
// Note: Not implementing Sync because this is for thread-local use

/// Per-thread statistics to avoid false sharing
pub struct PerThreadStats {
    pub operations: IsolatedAtomicCounter,
    pub cache_hits: IsolatedAtomicCounter,
    pub cache_misses: IsolatedAtomicCounter,
    pub locks_acquired: IsolatedAtomicCounter,
    pub contention_events: IsolatedAtomicCounter,
}

impl PerThreadStats {
    pub fn new() -> Self {
        Self {
            operations: IsolatedAtomicCounter::new(0),
            cache_hits: IsolatedAtomicCounter::new(0),
            cache_misses: IsolatedAtomicCounter::new(0),
            locks_acquired: IsolatedAtomicCounter::new(0),
            contention_events: IsolatedAtomicCounter::new(0),
        }
    }

    pub fn record_operation(&self) {
        self.operations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lock_acquired(&self) {
        self.locks_acquired.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_contention(&self) {
        self.contention_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_summary(&self) -> ThreadStatsSummary {
        ThreadStatsSummary {
            operations: self.operations.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            locks_acquired: self.locks_acquired.load(Ordering::Relaxed),
            contention_events: self.contention_events.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.operations.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.locks_acquired.store(0, Ordering::Relaxed);
        self.contention_events.store(0, Ordering::Relaxed);
    }
}

impl Default for PerThreadStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ThreadStatsSummary {
    pub operations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub locks_acquired: u64,
    pub contention_events: u64,
}

impl ThreadStatsSummary {
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total > 0 {
            self.cache_hits as f64 / total as f64
        } else {
            0.0
        }
    }

    pub fn contention_rate(&self) -> f64 {
        if self.locks_acquired > 0 {
            self.contention_events as f64 / self.locks_acquired as f64
        } else {
            0.0
        }
    }
}

/// Striped counter to reduce contention
pub struct StripedCounter {
    stripes: Vec<IsolatedAtomicCounter>,
    stripe_mask: usize,
}

impl StripedCounter {
    pub fn new(num_stripes: usize) -> Self {
        // Round up to next power of 2 for efficient modulo
        let stripe_count = num_stripes.next_power_of_two();
        let stripes = (0..stripe_count)
            .map(|_| IsolatedAtomicCounter::new(0))
            .collect();

        Self {
            stripes,
            stripe_mask: stripe_count - 1,
        }
    }

    pub fn increment(&self) {
        let stripe_idx = self.get_stripe_index();
        self.stripes[stripe_idx].fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, value: u64) {
        let stripe_idx = self.get_stripe_index();
        self.stripes[stripe_idx].fetch_add(value, Ordering::Relaxed);
    }

    pub fn get_total(&self) -> u64 {
        self.stripes
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .sum()
    }

    pub fn reset(&self) {
        for counter in &self.stripes {
            counter.store(0, Ordering::Relaxed);
        }
    }

    fn get_stripe_index(&self) -> usize {
        // Use thread ID hash to distribute load across stripes
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        hasher.finish() as usize & self.stripe_mask
    }
}

/// Cache-line aligned array to prevent false sharing between elements
pub struct CacheAlignedArray<T> {
    elements: Vec<CacheLinePadded<T>>,
}

impl<T> CacheAlignedArray<T> {
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: T) {
        self.elements.push(CacheLinePadded::new(value));
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.elements.get(index).map(|padded| padded.get())
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        self.elements.get_mut(index).map(|padded| padded.get_mut())
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter().map(|padded| padded.get())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.elements.iter_mut().map(|padded| padded.get_mut())
    }
}

impl<T> Default for CacheAlignedArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> std::ops::Index<usize> for CacheAlignedArray<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.elements[index].get()
    }
}

impl<T> std::ops::IndexMut<usize> for CacheAlignedArray<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.elements[index].get_mut()
    }
}

/// False sharing detector using performance counters
pub struct FalseSharingDetector {
    enabled: AtomicBool,
    detection_threshold: f64,
    sample_interval: std::time::Duration,
}

impl FalseSharingDetector {
    pub fn new(detection_threshold: f64) -> Self {
        Self {
            enabled: AtomicBool::new(true),
            detection_threshold,
            sample_interval: std::time::Duration::from_millis(100),
        }
    }

    pub fn start_monitoring(&self) -> FalseSharingMonitor {
        FalseSharingMonitor::new(self.detection_threshold, self.sample_interval)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Relaxed);
    }
}

pub struct FalseSharingMonitor {
    start_time: std::time::Instant,
    threshold: f64,
    interval: std::time::Duration,
    last_sample: std::time::Instant,
    cache_misses: u64,
    memory_stalls: u64,
}

impl FalseSharingMonitor {
    fn new(threshold: f64, interval: std::time::Duration) -> Self {
        let now = std::time::Instant::now();
        Self {
            start_time: now,
            threshold,
            interval,
            last_sample: now,
            cache_misses: 0,
            memory_stalls: 0,
        }
    }

    pub fn sample(&mut self) -> Option<FalseSharingReport> {
        let now = std::time::Instant::now();
        if now.duration_since(self.last_sample) < self.interval {
            return None;
        }

        // In a real implementation, this would read hardware performance counters
        // For demonstration, we simulate the values
        let cache_miss_rate = self.simulate_cache_miss_rate();
        let false_sharing_detected = cache_miss_rate > self.threshold;

        self.last_sample = now;
        
        Some(FalseSharingReport {
            timestamp: now,
            cache_miss_rate,
            false_sharing_detected,
            recommendations: if false_sharing_detected {
                vec![
                    "Consider using cache-line padded data structures".to_string(),
                    "Separate frequently accessed variables into different cache lines".to_string(),
                    "Use thread-local storage for per-thread counters".to_string(),
                ]
            } else {
                vec![]
            },
        })
    }

    fn simulate_cache_miss_rate(&mut self) -> f64 {
        // Simulate cache miss rate based on some heuristics
        // In real implementation, this would read performance counters
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        self.start_time.elapsed().as_nanos().hash(&mut hasher);
        let hash = hasher.finish();
        
        // Generate a value between 0.0 and 1.0
        (hash % 100) as f64 / 100.0
    }
}

#[derive(Debug)]
pub struct FalseSharingReport {
    pub timestamp: std::time::Instant,
    pub cache_miss_rate: f64,
    pub false_sharing_detected: bool,
    pub recommendations: Vec<String>,
}

/// Builder for creating cache-optimized data structures
pub struct CacheOptimizedBuilder;

impl CacheOptimizedBuilder {
    /// Create a new cache-line padded atomic counter
    pub fn atomic_counter(initial: u64) -> IsolatedAtomicCounter {
        IsolatedAtomicCounter::new(initial)
    }

    /// Create a new striped counter with the optimal number of stripes
    pub fn striped_counter() -> StripedCounter {
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8);
        StripedCounter::new(num_cpus * 2) // 2x to reduce contention
    }

    /// Create cache-aligned array
    pub fn cache_aligned_array<T>() -> CacheAlignedArray<T> {
        CacheAlignedArray::new()
    }

    /// Create per-thread statistics
    pub fn per_thread_stats() -> PerThreadStats {
        PerThreadStats::new()
    }

    /// Create false sharing detector
    pub fn false_sharing_detector(threshold: f64) -> FalseSharingDetector {
        FalseSharingDetector::new(threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_cache_line_padding() {
        assert_eq!(std::mem::align_of::<CacheLinePadded<u64>>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::size_of::<CacheLinePadded<u64>>(), CACHE_LINE_SIZE);
    }

    #[test]
    fn test_isolated_atomic_counter() {
        let counter = IsolatedAtomicCounter::new(0);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        
        counter.store(42, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 42);
        
        let old = counter.fetch_add(8, Ordering::Relaxed);
        assert_eq!(old, 42);
        assert_eq!(counter.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_striped_counter() {
        let counter = StripedCounter::new(4);
        
        counter.increment();
        counter.add(5);
        
        assert_eq!(counter.get_total(), 6);
        
        counter.reset();
        assert_eq!(counter.get_total(), 0);
    }

    #[test]
    fn test_cache_aligned_array() {
        let mut array = CacheAlignedArray::new();
        
        array.push(1);
        array.push(2);
        array.push(3);
        
        assert_eq!(array.len(), 3);
        assert_eq!(array.get(1), Some(&2));
        
        if let Some(value) = array.get_mut(1) {
            *value = 20;
        }
        
        assert_eq!(array[1], 20);
    }

    #[test]
    fn test_per_thread_stats() {
        let stats = PerThreadStats::new();
        
        stats.record_operation();
        stats.record_cache_hit();
        stats.record_cache_miss();
        
        let summary = stats.get_summary();
        assert_eq!(summary.operations, 1);
        assert_eq!(summary.cache_hits, 1);
        assert_eq!(summary.cache_misses, 1);
        assert_eq!(summary.hit_rate(), 0.5);
    }

    #[test]
    fn test_false_sharing_detector() {
        let detector = FalseSharingDetector::new(0.1);
        assert!(detector.is_enabled());
        
        detector.disable();
        assert!(!detector.is_enabled());
        
        detector.enable();
        assert!(detector.is_enabled());
    }

    #[test]
    fn test_multithread_striped_counter() {
        let counter = Arc::new(StripedCounter::new(8));
        let num_threads = 4;
        let ops_per_thread = 1000;
        
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let counter = Arc::clone(&counter);
                thread::spawn(move || {
                    for _ in 0..ops_per_thread {
                        counter.increment();
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(counter.get_total(), num_threads * ops_per_thread);
    }

    #[test]
    fn test_cache_optimized_builder() {
        let counter = CacheOptimizedBuilder::atomic_counter(100);
        assert_eq!(counter.load(Ordering::Relaxed), 100);
        
        let striped = CacheOptimizedBuilder::striped_counter();
        striped.increment();
        assert_eq!(striped.get_total(), 1);
        
        let array = CacheOptimizedBuilder::cache_aligned_array::<i32>();
        assert!(array.is_empty());
        
        let stats = CacheOptimizedBuilder::per_thread_stats();
        stats.record_operation();
        assert_eq!(stats.get_summary().operations, 1);
    }
}
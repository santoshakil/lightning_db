//! CPU Cache-Line Optimized Data Structures
//!
//! This module provides data structures specifically optimized for CPU cache performance:
//! - Cache-line aligned structures (64-byte alignment)
//! - False sharing prevention
//! - Sequential access optimization
//! - NUMA-aware memory layouts

pub mod aligned_btree;
pub mod cache_aligned_page;
pub mod false_sharing_guard;
pub mod prefetch_buffer;
pub mod simd_operations;

use std::sync::atomic::{AtomicUsize, Ordering};

/// CPU cache line size (typically 64 bytes on x86-64)
pub const CACHE_LINE_SIZE: usize = 64;

/// Cache-line aligned atomic counter (prevents false sharing)
#[repr(align(64))]
pub struct CacheAlignedCounter {
    value: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
}

impl CacheAlignedCounter {
    pub const fn new(initial: usize) -> Self {
        Self {
            value: AtomicUsize::new(initial),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
        }
    }

    pub fn load(&self, order: Ordering) -> usize {
        self.value.load(order)
    }

    pub fn store(&self, val: usize, order: Ordering) {
        self.value.store(val, order);
    }

    pub fn fetch_add(&self, val: usize, order: Ordering) -> usize {
        self.value.fetch_add(val, order)
    }

    pub fn compare_exchange(
        &self,
        current: usize,
        new: usize,
        success: Ordering,
        failure: Ordering,
    ) -> Result<usize, usize> {
        self.value.compare_exchange(current, new, success, failure)
    }
}

/// Memory prefetch hints for improved cache performance
pub struct PrefetchHints;

impl PrefetchHints {
    /// Prefetch data for reading (non-temporal)
    #[inline(always)]
    pub fn prefetch_read_nt(ptr: *const u8) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_NTA }>(
                ptr as *const i8
            );
        }
    }

    /// Prefetch data for reading (temporal, multiple uses expected)
    #[inline(always)]
    pub fn prefetch_read_t0(ptr: *const u8) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                ptr as *const i8
            );
        }
    }

    /// Prefetch data for writing
    #[inline(always)]
    pub fn prefetch_write(ptr: *const u8) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            // Use T0 hint for write prefetch
            std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                ptr as *const i8
            );
        }
    }

    /// Prefetch multiple cache lines sequentially
    #[inline(always)]
    pub fn prefetch_range(start: *const u8, len: usize) {
        let mut ptr = start;
        let end = unsafe { start.add(len) };
        
        while ptr < end {
            Self::prefetch_read_t0(ptr);
            ptr = unsafe { ptr.add(CACHE_LINE_SIZE) };
        }
    }
}

/// Cache-line optimized ring buffer for high-throughput operations
#[repr(align(64))]
pub struct CacheOptimizedRingBuffer<T> {
    buffer: Vec<T>,
    capacity: usize,
    head: CacheAlignedCounter,
    tail: CacheAlignedCounter,
    _padding: [u8; CACHE_LINE_SIZE],
}

impl<T> CacheOptimizedRingBuffer<T> 
where 
    T: Default + Clone,
{
    pub fn new(capacity: usize) -> Self {
        // Round capacity to cache line boundary for optimal access
        let aligned_capacity = (capacity + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        
        Self {
            buffer: vec![T::default(); aligned_capacity],
            capacity: aligned_capacity,
            head: CacheAlignedCounter::new(0),
            tail: CacheAlignedCounter::new(0),
            _padding: [0; CACHE_LINE_SIZE],
        }
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let current_tail = self.tail.load(Ordering::Relaxed);
        let next_tail = (current_tail + 1) % self.capacity;
        
        if next_tail == self.head.load(Ordering::Acquire) {
            return Err(item); // Buffer full
        }

        // Prefetch the next write location
        let next_ptr = &self.buffer[next_tail] as *const T as *const u8;
        PrefetchHints::prefetch_write(next_ptr);

        // SAFETY: We've verified the buffer isn't full
        unsafe {
            std::ptr::write(
                self.buffer.as_ptr().add(current_tail) as *mut T,
                item,
            );
        }

        self.tail.store(next_tail, Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let current_head = self.head.load(Ordering::Relaxed);
        
        if current_head == self.tail.load(Ordering::Acquire) {
            return None; // Buffer empty
        }

        let next_head = (current_head + 1) % self.capacity;
        
        // Prefetch the next read location
        let next_ptr = &self.buffer[next_head] as *const T as *const u8;
        PrefetchHints::prefetch_read_t0(next_ptr);

        // SAFETY: We've verified the buffer isn't empty
        let item = unsafe { std::ptr::read(self.buffer.as_ptr().add(current_head)) };

        self.head.store(next_head, Ordering::Release);
        Some(item)
    }

    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        
        if tail >= head {
            tail - head
        } else {
            self.capacity - head + tail
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        (tail + 1) % self.capacity == head
    }
}

unsafe impl<T: Send> Send for CacheOptimizedRingBuffer<T> {}
unsafe impl<T: Send> Sync for CacheOptimizedRingBuffer<T> {}

/// Statistics for cache performance monitoring
#[derive(Debug, Default)]
pub struct CachePerformanceStats {
    pub cache_hits: AtomicUsize,
    pub cache_misses: AtomicUsize,
    pub prefetch_hits: AtomicUsize,
    pub false_sharing_events: AtomicUsize,
    pub memory_stalls: AtomicUsize,
}

impl CachePerformanceStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prefetch_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_false_sharing(&self) {
        self.false_sharing_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_memory_stall(&self) {
        self.memory_stalls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }

    pub fn prefetch_effectiveness(&self) -> f64 {
        let prefetch_hits = self.prefetch_hits.load(Ordering::Relaxed) as f64;
        let total_hits = self.cache_hits.load(Ordering::Relaxed) as f64;
        
        if total_hits > 0.0 {
            prefetch_hits / total_hits
        } else {
            0.0
        }
    }

    pub fn false_sharing_rate(&self) -> f64 {
        let false_sharing = self.false_sharing_events.load(Ordering::Relaxed) as f64;
        let total_accesses = (self.cache_hits.load(Ordering::Relaxed) + 
                             self.cache_misses.load(Ordering::Relaxed)) as f64;
        
        if total_accesses > 0.0 {
            false_sharing / total_accesses
        } else {
            0.0
        }
    }

    pub fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.prefetch_hits.store(0, Ordering::Relaxed);
        self.false_sharing_events.store(0, Ordering::Relaxed);
        self.memory_stalls.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_aligned_counter() {
        let counter = CacheAlignedCounter::new(0);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        
        counter.store(42, Ordering::Relaxed);
        assert_eq!(counter.load(Ordering::Relaxed), 42);
        
        let old = counter.fetch_add(8, Ordering::Relaxed);
        assert_eq!(old, 42);
        assert_eq!(counter.load(Ordering::Relaxed), 50);
    }

    #[test]
    fn test_cache_optimized_ring_buffer() {
        let buffer = CacheOptimizedRingBuffer::new(4);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());

        // Test push
        assert!(buffer.push(1).is_ok());
        assert!(buffer.push(2).is_ok());
        assert!(buffer.push(3).is_ok());
        assert_eq!(buffer.len(), 3);

        // Test pop
        assert_eq!(buffer.pop(), Some(1));
        assert_eq!(buffer.pop(), Some(2));
        assert_eq!(buffer.len(), 1);

        // Test wrap around
        assert!(buffer.push(4).is_ok());
        assert!(buffer.push(5).is_ok());
        assert!(buffer.is_full());

        // Buffer should be full, push should fail
        assert!(buffer.push(6).is_err());
    }

    #[test]
    fn test_cache_performance_stats() {
        let stats = CachePerformanceStats::new();
        
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();
        
        assert_eq!(stats.hit_rate(), 2.0 / 3.0);
        
        stats.record_prefetch_hit();
        assert_eq!(stats.prefetch_effectiveness(), 1.0 / 2.0);
        
        stats.reset();
        assert_eq!(stats.hit_rate(), 0.0);
    }

    #[test]
    fn test_cache_line_alignment() {
        // Verify structures are properly aligned
        assert_eq!(std::mem::align_of::<CacheAlignedCounter>(), CACHE_LINE_SIZE);
        assert_eq!(std::mem::size_of::<CacheAlignedCounter>(), CACHE_LINE_SIZE);
    }
}
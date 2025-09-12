//! Optimized thread-local storage for high-performance database operations
//!
//! This module extends the existing thread-local cache with additional optimizations
//! for database-specific workloads, including transaction metadata caching,
//! page buffers, and SIMD-aligned data structures.

use crate::core::error::Result;
use crate::performance::optimizations::simd;
use crate::performance::optimizations::memory_layout::{CacheAlignedAllocator, CompactRecord};
use std::cell::{RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

// Thread-local storage constants
const MAX_CACHED_TRANSACTIONS: usize = 64;
const MAX_CACHED_PAGES: usize = 32;
const TLS_BUFFER_SIZE: usize = 8192; // 8KB per thread
const STATISTICS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// Thread-local transaction metadata cache
#[derive(Debug)]
pub struct ThreadLocalTransactionCache {
    active_transactions: FxHashMap<u64, CachedTransactionState>,
    free_tx_slots: VecDeque<u64>,
    last_gc_time: Instant,
    gc_interval: Duration,
}

/// Cached transaction state for fast access
#[derive(Debug, Clone)]
pub struct CachedTransactionState {
    pub tx_id: u64,
    pub read_timestamp: u64,
    pub write_count: usize,
    pub read_count: usize,
    pub last_access: Instant,
    pub isolation_level: IsolationLevel,
    pub priority: TransactionPriority,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IsolationLevel {
    ReadUncommitted = 0,
    ReadCommitted = 1,
    RepeatableRead = 2,
    Serializable = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum TransactionPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl ThreadLocalTransactionCache {
    fn new() -> Self {
        Self {
            active_transactions: FxHashMap::with_capacity_and_hasher(MAX_CACHED_TRANSACTIONS, Default::default()),
            free_tx_slots: VecDeque::with_capacity(MAX_CACHED_TRANSACTIONS),
            last_gc_time: Instant::now(),
            gc_interval: Duration::from_secs(30),
        }
    }

    /// Cache transaction state for quick access
    pub fn cache_transaction(&mut self, state: CachedTransactionState) -> bool {
        if self.active_transactions.len() >= MAX_CACHED_TRANSACTIONS {
            self.maybe_gc();
            if self.active_transactions.len() >= MAX_CACHED_TRANSACTIONS {
                return false; // Cache full
            }
        }

        self.active_transactions.insert(state.tx_id, state);
        true
    }

    /// Get cached transaction state
    pub fn get_transaction(&self, tx_id: u64) -> Option<&CachedTransactionState> {
        self.active_transactions.get(&tx_id)
    }

    /// Update transaction access time and counters
    pub fn touch_transaction(&mut self, tx_id: u64, is_write: bool) {
        if let Some(state) = self.active_transactions.get_mut(&tx_id) {
            state.last_access = Instant::now();
            if is_write {
                state.write_count += 1;
            } else {
                state.read_count += 1;
            }
        }
    }

    /// Remove transaction from cache
    pub fn remove_transaction(&mut self, tx_id: u64) -> Option<CachedTransactionState> {
        let removed = self.active_transactions.remove(&tx_id);
        if removed.is_some() {
            self.free_tx_slots.push_back(tx_id);
        }
        removed
    }

    /// Garbage collect old transactions
    fn maybe_gc(&mut self) {
        if self.last_gc_time.elapsed() < self.gc_interval {
            return;
        }

        let now = Instant::now();
        let timeout = Duration::from_secs(300); // 5 minutes
        
        self.active_transactions.retain(|_, state| {
            now.duration_since(state.last_access) < timeout
        });

        self.last_gc_time = now;
    }
}

/// Thread-local page cache for frequently accessed database pages
#[derive(Debug)]
pub struct ThreadLocalPageCache {
    pages: FxHashMap<u32, CachedPage>,
    lru_order: VecDeque<u32>,
    max_pages: usize,
    hit_count: usize,
    miss_count: usize,
}

/// Cached page data with metadata
#[derive(Debug, Clone)]
pub struct CachedPage {
    pub page_id: u32,
    pub data: Arc<Vec<u8>>,
    pub last_access: Instant,
    pub access_count: usize,
    pub is_dirty: bool,
}

impl ThreadLocalPageCache {
    fn new() -> Self {
        Self {
            pages: FxHashMap::with_capacity_and_hasher(MAX_CACHED_PAGES, Default::default()),
            lru_order: VecDeque::with_capacity(MAX_CACHED_PAGES),
            max_pages: MAX_CACHED_PAGES,
            hit_count: 0,
            miss_count: 0,
        }
    }

    /// Cache a page
    pub fn cache_page(&mut self, page: CachedPage) {
        if self.pages.len() >= self.max_pages {
            self.evict_lru();
        }

        self.lru_order.push_back(page.page_id);
        self.pages.insert(page.page_id, page);
    }

    /// Get a cached page
    pub fn get_page(&mut self, page_id: u32) -> Option<&CachedPage> {
        if let Some(page) = self.pages.get_mut(&page_id) {
            page.last_access = Instant::now();
            page.access_count += 1;
            self.hit_count += 1;
            
            // Move to end of LRU
            if let Some(pos) = self.lru_order.iter().position(|&id| id == page_id) {
                self.lru_order.remove(pos);
                self.lru_order.push_back(page_id);
            }
            
            Some(page)
        } else {
            self.miss_count += 1;
            None
        }
    }

    /// Mark a page as dirty
    pub fn mark_dirty(&mut self, page_id: u32) {
        if let Some(page) = self.pages.get_mut(&page_id) {
            page.is_dirty = true;
        }
    }

    /// Get cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.hit_count + self.miss_count;
        if total == 0 {
            0.0
        } else {
            self.hit_count as f64 / total as f64
        }
    }

    /// Evict least recently used page
    fn evict_lru(&mut self) {
        if let Some(page_id) = self.lru_order.pop_front() {
            self.pages.remove(&page_id);
        }
    }
}

/// SIMD-aligned working buffers for database operations
#[repr(C, align(64))] // Cache line aligned
pub struct SIMDWorkingBuffers {
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    comparison_buffer: Vec<u8>,
    hash_buffer: Vec<u64>,
    temp_buffer: Vec<u8>,
}

impl SIMDWorkingBuffers {
    fn new() -> Self {
        Self {
            key_buffer: Vec::with_capacity(1024),
            value_buffer: Vec::with_capacity(4096),
            comparison_buffer: Vec::with_capacity(2048),
            hash_buffer: Vec::with_capacity(256), // 256 * 8 = 2KB
            temp_buffer: Vec::with_capacity(8192),
        }
    }

    /// Get key buffer, resizing if necessary
    pub fn get_key_buffer(&mut self, min_size: usize) -> &mut Vec<u8> {
        if self.key_buffer.capacity() < min_size {
            self.key_buffer.reserve(min_size - self.key_buffer.capacity());
        }
        self.key_buffer.clear();
        &mut self.key_buffer
    }

    /// Get value buffer, resizing if necessary
    pub fn get_value_buffer(&mut self, min_size: usize) -> &mut Vec<u8> {
        if self.value_buffer.capacity() < min_size {
            self.value_buffer.reserve(min_size - self.value_buffer.capacity());
        }
        self.value_buffer.clear();
        &mut self.value_buffer
    }

    /// Get SIMD-aligned comparison buffer
    pub fn get_comparison_buffer(&mut self, size: usize) -> &mut Vec<u8> {
        self.comparison_buffer.clear();
        self.comparison_buffer.resize(size, 0);
        &mut self.comparison_buffer
    }

    /// Get hash computation buffer
    pub fn get_hash_buffer(&mut self, count: usize) -> &mut Vec<u64> {
        self.hash_buffer.clear();
        self.hash_buffer.resize(count, 0);
        &mut self.hash_buffer
    }

    /// Clear all buffers
    pub fn clear_all(&mut self) {
        self.key_buffer.clear();
        self.value_buffer.clear();
        self.comparison_buffer.clear();
        self.hash_buffer.clear();
        self.temp_buffer.clear();
    }
}

/// Thread-local statistics collector
#[derive(Debug)]
pub struct ThreadLocalStats {
    operations_count: FxHashMap<&'static str, u64>,
    latency_samples: FxHashMap<&'static str, VecDeque<Duration>>,
    last_update: Instant,
    update_interval: Duration,
    max_samples: usize,
}

impl ThreadLocalStats {
    fn new() -> Self {
        Self {
            operations_count: FxHashMap::default(),
            latency_samples: FxHashMap::default(),
            last_update: Instant::now(),
            update_interval: STATISTICS_UPDATE_INTERVAL,
            max_samples: 1000,
        }
    }

    /// Record an operation
    pub fn record_operation(&mut self, operation: &'static str, latency: Duration) {
        *self.operations_count.entry(operation).or_insert(0) += 1;
        
        let samples = self.latency_samples.entry(operation).or_insert_with(|| VecDeque::with_capacity(self.max_samples));
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(latency);
    }

    /// Get operation count
    pub fn get_operation_count(&self, operation: &str) -> u64 {
        self.operations_count.get(operation).copied().unwrap_or(0)
    }

    /// Get average latency
    pub fn get_avg_latency(&self, operation: &str) -> Option<Duration> {
        if let Some(samples) = self.latency_samples.get(operation) {
            if samples.is_empty() {
                return None;
            }
            
            let total: Duration = samples.iter().sum();
            Some(total / samples.len() as u32)
        } else {
            None
        }
    }

    /// Should update global stats
    pub fn should_update(&self) -> bool {
        self.last_update.elapsed() >= self.update_interval
    }

    /// Mark as updated
    pub fn mark_updated(&mut self) {
        self.last_update = Instant::now();
    }
}

thread_local! {
    /// Thread-local transaction cache
    static TRANSACTION_CACHE: RefCell<ThreadLocalTransactionCache> = RefCell::new(ThreadLocalTransactionCache::new());

    /// Thread-local page cache
    static PAGE_CACHE: RefCell<ThreadLocalPageCache> = RefCell::new(ThreadLocalPageCache::new());

    /// Thread-local SIMD working buffers
    static SIMD_BUFFERS: RefCell<SIMDWorkingBuffers> = RefCell::new(SIMDWorkingBuffers::new());

    /// Thread-local statistics
    static TL_STATS: RefCell<ThreadLocalStats> = RefCell::new(ThreadLocalStats::new());

    /// Thread-local memory allocator for compact records
    static COMPACT_ALLOCATOR: RefCell<CacheAlignedAllocator> = RefCell::new(CacheAlignedAllocator::new(64 * 1024)); // 64KB per thread
}

/// High-level API for thread-local storage operations
pub struct ThreadLocalStorage;

impl ThreadLocalStorage {
    /// Cache a transaction for quick access
    pub fn cache_transaction(state: CachedTransactionState) -> bool {
        TRANSACTION_CACHE.with(|cache| {
            cache.borrow_mut().cache_transaction(state)
        })
    }

    /// Get cached transaction state
    pub fn get_transaction(tx_id: u64) -> Option<CachedTransactionState> {
        TRANSACTION_CACHE.with(|cache| {
            cache.borrow().get_transaction(tx_id).cloned()
        })
    }

    /// Touch transaction (update access time and counters)
    pub fn touch_transaction(tx_id: u64, is_write: bool) {
        TRANSACTION_CACHE.with(|cache| {
            cache.borrow_mut().touch_transaction(tx_id, is_write)
        })
    }

    /// Remove transaction from cache
    pub fn remove_transaction(tx_id: u64) -> Option<CachedTransactionState> {
        TRANSACTION_CACHE.with(|cache| {
            cache.borrow_mut().remove_transaction(tx_id)
        })
    }

    /// Cache a page
    pub fn cache_page(page: CachedPage) {
        PAGE_CACHE.with(|cache| {
            cache.borrow_mut().cache_page(page)
        })
    }

    /// Get cached page
    pub fn get_cached_page(page_id: u32) -> Option<CachedPage> {
        PAGE_CACHE.with(|cache| {
            cache.borrow_mut().get_page(page_id).cloned()
        })
    }

    /// Mark page as dirty
    pub fn mark_page_dirty(page_id: u32) {
        PAGE_CACHE.with(|cache| {
            cache.borrow_mut().mark_dirty(page_id)
        })
    }

    /// Get page cache hit rate
    pub fn page_cache_hit_rate() -> f64 {
        PAGE_CACHE.with(|cache| {
            cache.borrow().hit_rate()
        })
    }

    /// Execute with SIMD working buffers
    pub fn with_simd_buffers<F, R>(f: F) -> R
    where
        F: FnOnce(&mut SIMDWorkingBuffers) -> R,
    {
        SIMD_BUFFERS.with(|buffers| {
            f(&mut buffers.borrow_mut())
        })
    }

    /// Record operation statistics
    pub fn record_operation(operation: &'static str, latency: Duration) {
        TL_STATS.with(|stats| {
            stats.borrow_mut().record_operation(operation, latency)
        })
    }

    /// Get operation statistics
    pub fn get_operation_stats(operation: &str) -> (u64, Option<Duration>) {
        TL_STATS.with(|stats| {
            let stats = stats.borrow();
            (stats.get_operation_count(operation), stats.get_avg_latency(operation))
        })
    }

    /// Check if should update global statistics
    pub fn should_update_stats() -> bool {
        TL_STATS.with(|stats| {
            stats.borrow().should_update()
        })
    }

    /// Mark statistics as updated
    pub fn mark_stats_updated() {
        TL_STATS.with(|stats| {
            stats.borrow_mut().mark_updated()
        })
    }

    /// Allocate compact record from thread-local allocator
    pub fn allocate_compact_record(size: usize) -> Option<CompactRecord> {
        COMPACT_ALLOCATOR.with(|allocator| {
            allocator.borrow_mut().allocate(size)
        })
    }

    /// Clear all thread-local caches (for testing/cleanup)
    pub fn clear_all_caches() {
        TRANSACTION_CACHE.with(|cache| cache.borrow_mut().active_transactions.clear());
        PAGE_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            cache.pages.clear();
            cache.lru_order.clear();
        });
        SIMD_BUFFERS.with(|buffers| buffers.borrow_mut().clear_all());
        TL_STATS.with(|stats| {
            let mut stats = stats.borrow_mut();
            stats.operations_count.clear();
            stats.latency_samples.clear();
        });
    }

    /// Get comprehensive thread-local statistics
    pub fn get_comprehensive_stats() -> ThreadLocalStorageStats {
        let (tx_cache_size, page_cache_size, page_hit_rate) = {
            let tx_size = TRANSACTION_CACHE.with(|cache| cache.borrow().active_transactions.len());
            let (page_size, hit_rate) = PAGE_CACHE.with(|cache| {
                let cache = cache.borrow();
                (cache.pages.len(), cache.hit_rate())
            });
            (tx_size, page_size, hit_rate)
        };

        let total_operations = TL_STATS.with(|stats| {
            stats.borrow().operations_count.values().sum()
        });

        ThreadLocalStorageStats {
            transaction_cache_size: tx_cache_size,
            page_cache_size: page_cache_size,
            page_cache_hit_rate: page_hit_rate,
            total_operations,
        }
    }
}

/// Comprehensive thread-local storage statistics
#[derive(Debug)]
pub struct ThreadLocalStorageStats {
    pub transaction_cache_size: usize,
    pub page_cache_size: usize,
    pub page_cache_hit_rate: f64,
    pub total_operations: u64,
}

/// Macro for easy operation timing with thread-local statistics
#[macro_export]
macro_rules! time_operation {
    ($operation:expr, $code:block) => {{
        let start = std::time::Instant::now();
        let result = $code;
        let duration = start.elapsed();
        $crate::performance::thread_local::optimized_storage::ThreadLocalStorage::record_operation($operation, duration);
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_cache() {
        ThreadLocalStorage::clear_all_caches();
        
        let state = CachedTransactionState {
            tx_id: 1,
            read_timestamp: 100,
            write_count: 0,
            read_count: 0,
            last_access: Instant::now(),
            isolation_level: IsolationLevel::ReadCommitted,
            priority: TransactionPriority::Normal,
        };

        assert!(ThreadLocalStorage::cache_transaction(state.clone()));
        assert_eq!(ThreadLocalStorage::get_transaction(1).unwrap().tx_id, 1);
        
        ThreadLocalStorage::touch_transaction(1, true);
        let updated = ThreadLocalStorage::get_transaction(1).unwrap();
        assert_eq!(updated.write_count, 1);
        
        assert!(ThreadLocalStorage::remove_transaction(1).is_some());
        assert!(ThreadLocalStorage::get_transaction(1).is_none());
    }

    #[test]
    fn test_page_cache() {
        ThreadLocalStorage::clear_all_caches();
        
        let page = CachedPage {
            page_id: 1,
            data: Arc::new(vec![1, 2, 3, 4]),
            last_access: Instant::now(),
            access_count: 0,
            is_dirty: false,
        };

        ThreadLocalStorage::cache_page(page);
        assert!(ThreadLocalStorage::get_cached_page(1).is_some());
        
        ThreadLocalStorage::mark_page_dirty(1);
        let cached_page = ThreadLocalStorage::get_cached_page(1).unwrap();
        assert!(cached_page.is_dirty);
    }

    #[test]
    fn test_simd_buffers() {
        let result = ThreadLocalStorage::with_simd_buffers(|buffers| {
            let key_buf = buffers.get_key_buffer(100);
            key_buf.extend_from_slice(b"test_key");
            key_buf.len()
        });
        
        assert_eq!(result, 8);
    }

    #[test]
    fn test_operation_stats() {
        ThreadLocalStorage::clear_all_caches();
        
        ThreadLocalStorage::record_operation("test_op", Duration::from_millis(10));
        ThreadLocalStorage::record_operation("test_op", Duration::from_millis(20));
        
        let (count, avg_latency) = ThreadLocalStorage::get_operation_stats("test_op");
        assert_eq!(count, 2);
        assert!(avg_latency.is_some());
        assert!(avg_latency.unwrap().as_millis() > 10);
    }

    #[test] 
    fn test_comprehensive_stats() {
        ThreadLocalStorage::clear_all_caches();
        
        // Add some test data
        let state = CachedTransactionState {
            tx_id: 1,
            read_timestamp: 100,
            write_count: 0,
            read_count: 0,
            last_access: Instant::now(),
            isolation_level: IsolationLevel::ReadCommitted,
            priority: TransactionPriority::Normal,
        };
        ThreadLocalStorage::cache_transaction(state);
        
        ThreadLocalStorage::record_operation("test", Duration::from_millis(1));
        
        let stats = ThreadLocalStorage::get_comprehensive_stats();
        assert_eq!(stats.transaction_cache_size, 1);
        assert_eq!(stats.total_operations, 1);
    }
}
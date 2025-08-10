pub mod adaptive_sizing;
pub mod advanced_cache;
pub mod arc_cache;
pub mod batch_eviction;
pub mod deadlock_free_arc;
pub mod lock_free_cache;
pub mod memory_pool;
pub mod optimized_arc;
pub mod prewarming;

use crate::storage::Page;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub use adaptive_sizing::{
    AdaptiveCacheSizer, AdaptiveSizingConfig, AdaptiveSizingStats, CacheAllocation,
    CachePerformanceMetrics, CacheTier, CacheWarmer, SizingDecision, SizingReason, WorkloadPattern,
};
pub use arc_cache::{ArcCache, ArcCacheStats};
pub use batch_eviction::{BatchEvictingArcCache, BatchEvictionConfig, BatchEvictionStats};
pub use deadlock_free_arc::DeadlockFreeArcCache;
pub use lock_free_cache::{LockFreeCache, SegmentedLockFreeCache};
pub use memory_pool::MemoryPool;
pub use prewarming::{
    AccessPattern, CachePrewarmer, PatternType, PrefetchRequest, PrewarmingConfig, WarmingPriority,
    WarmingStats,
};

#[derive(Debug, Clone)]
pub struct MemoryConfig {
    pub hot_cache_size: usize,    // Bytes in hot cache
    pub mmap_size: Option<usize>, // Total mmap size
    pub prefetch_size: usize,     // Pages to prefetch
    pub eviction_batch: usize,    // Batch eviction size
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            hot_cache_size: 100 * 1024 * 1024, // 100MB
            mmap_size: None,
            prefetch_size: 4,
            eviction_batch: 32,
        }
    }
}

pub struct CacheStats {
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
    pub evictions: AtomicUsize,
    pub prefetch_hits: AtomicUsize,
}

impl CacheStats {
    pub fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
            prefetch_hits: AtomicUsize::new(0),
        }
    }

    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_prefetch_hit(&self) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }

    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.prefetch_hits.store(0, Ordering::Relaxed);
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct CachedPage {
    pub page: Arc<Page>,
    pub access_count: AtomicUsize,
    pub last_access: AtomicUsize,
    pub is_dirty: AtomicUsize, // 0 = clean, 1 = dirty
}

impl CachedPage {
    pub fn new(page: Page) -> Self {
        Self {
            page: Arc::new(page),
            access_count: AtomicUsize::new(0),
            last_access: AtomicUsize::new(0),
            is_dirty: AtomicUsize::new(0),
        }
    }

    pub fn access(&self, timestamp: usize) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        self.last_access.store(timestamp, Ordering::Relaxed);
    }

    pub fn mark_dirty(&self) {
        self.is_dirty.store(1, Ordering::Relaxed);
    }

    pub fn mark_clean(&self) {
        self.is_dirty.store(0, Ordering::Relaxed);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed) == 1
    }

    pub fn get_access_count(&self) -> usize {
        self.access_count.load(Ordering::Relaxed)
    }

    pub fn get_last_access(&self) -> usize {
        self.last_access.load(Ordering::Relaxed)
    }
}

pub trait CachePolicy: Send + Sync {
    fn should_evict(&self, page: &CachedPage, current_time: usize) -> bool;
    fn should_prefetch(&self, page_id: u32) -> Vec<u32>;
    fn on_access(&self, page_id: u32);
}

pub struct LruPolicy {
    max_age: usize,
}

impl LruPolicy {
    pub fn new(max_age: usize) -> Self {
        Self { max_age }
    }
}

impl CachePolicy for LruPolicy {
    fn should_evict(&self, page: &CachedPage, current_time: usize) -> bool {
        let last_access = page.get_last_access();
        current_time.saturating_sub(last_access) > self.max_age && !page.is_dirty()
    }

    fn should_prefetch(&self, page_id: u32) -> Vec<u32> {
        // Simple sequential prefetch
        (page_id + 1..page_id + 5).collect()
    }

    fn on_access(&self, _page_id: u32) {
        // No special handling for LRU
    }
}

pub struct AdaptivePolicy {
    access_patterns: DashMap<u32, Vec<usize>>,
    prefetch_threshold: usize,
}

impl AdaptivePolicy {
    pub fn new(prefetch_threshold: usize) -> Self {
        Self {
            access_patterns: DashMap::new(),
            prefetch_threshold,
        }
    }

    fn predict_next_pages(&self, page_id: u32) -> Vec<u32> {
        if let Some(pattern) = self.access_patterns.get(&page_id) {
            if pattern.len() >= self.prefetch_threshold {
                // Analyze pattern for sequential or stride access
                let mut deltas = Vec::new();
                for i in 1..pattern.len() {
                    deltas.push(pattern[i] as i32 - pattern[i - 1] as i32);
                }

                // If mostly sequential, prefetch next few pages
                let sequential_count = deltas.iter().filter(|&&d| d == 1).count();
                if sequential_count > deltas.len() / 2 {
                    return (page_id + 1..page_id + 5).collect();
                }
            }
        }
        Vec::new()
    }
}

impl CachePolicy for AdaptivePolicy {
    fn should_evict(&self, page: &CachedPage, _current_time: usize) -> bool {
        // Evict pages with low access count that aren't dirty
        page.get_access_count() < 2 && !page.is_dirty()
    }

    fn should_prefetch(&self, page_id: u32) -> Vec<u32> {
        self.predict_next_pages(page_id)
    }

    fn on_access(&self, page_id: u32) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as usize;

        self.access_patterns
            .entry(page_id)
            .and_modify(|v| {
                v.push(timestamp);
                if v.len() > 10 {
                    v.remove(0);
                }
            })
            .or_insert_with(|| vec![timestamp]);
    }
}

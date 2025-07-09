use crate::cache::{CacheStats, CachedPage};
use crate::error::Result;
use crate::storage::Page;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// A lock-free cache implementation using segmented LRU with Clock eviction
///
/// This implementation avoids the complex list manipulations of ARC by using:
/// 1. DashMap for lock-free concurrent access
/// 2. Segmented design to reduce contention
/// 3. Clock algorithm for eviction (simpler than LRU but effective)
pub struct LockFreeCache {
    // Main storage - already lock-free
    cache: DashMap<u32, Arc<CachedPage>>,

    // Capacity and size tracking
    capacity: usize,
    size: AtomicUsize,

    // Clock hand for eviction
    clock_hand: AtomicUsize,

    // Access order tracking using a circular buffer
    access_order: Arc<RwLock<VecDeque<u32>>>,

    // Statistics
    stats: Arc<CacheStats>,
    timestamp: AtomicU64,

    // Eviction queue for batch evictions
    eviction_queue: ArrayQueue<u32>,
}

impl LockFreeCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: DashMap::with_capacity(capacity),
            capacity,
            size: AtomicUsize::new(0),
            clock_hand: AtomicUsize::new(0),
            access_order: Arc::new(RwLock::new(VecDeque::with_capacity(capacity))),
            stats: Arc::new(CacheStats::new()),
            timestamp: AtomicU64::new(0),
            eviction_queue: ArrayQueue::new(capacity / 10), // Batch eviction queue
        }
    }

    pub fn get(&self, page_id: u32) -> Option<Arc<CachedPage>> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);

        if let Some(entry) = self.cache.get(&page_id) {
            let cached_page = entry.value().clone();

            // Update access time and increment reference bit (for Clock algorithm)
            cached_page.access(timestamp as usize);
            cached_page.access_count.fetch_add(1, Ordering::Relaxed);

            self.stats.record_hit();
            Some(cached_page)
        } else {
            self.stats.record_miss();
            None
        }
    }

    pub fn insert(&self, page_id: u32, page: Page) -> Result<()> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);
        let cached_page = Arc::new(CachedPage::new(page));
        cached_page.access(timestamp as usize);

        // Check if we need to evict
        let current_size = self.size.load(Ordering::Relaxed);
        if current_size >= self.capacity {
            self.evict_one()?;
        }

        // Insert the new page
        if let Some(old) = self.cache.insert(page_id, cached_page) {
            // Page was already in cache (shouldn't happen in normal operation)
            drop(old);
        } else {
            // New insertion - increment size
            self.size.fetch_add(1, Ordering::Relaxed);

            // Track in access order
            let mut access_order = self.access_order.write();
            access_order.push_back(page_id);

            // Limit access order size
            while access_order.len() > self.capacity {
                access_order.pop_front();
            }
        }

        Ok(())
    }

    pub fn remove(&self, page_id: u32) {
        if self.cache.remove(&page_id).is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);

            // Remove from access order
            let mut access_order = self.access_order.write();
            access_order.retain(|&id| id != page_id);
        }
    }

    /// Evict one page using Clock algorithm
    fn evict_one(&self) -> Result<()> {
        // Try to get a page from the eviction queue first
        if let Some(victim_id) = self.eviction_queue.pop() {
            if self.cache.remove(&victim_id).is_some() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.stats.record_eviction();
                return Ok(());
            }
        }

        // Clock algorithm: scan through pages looking for one to evict
        let mut attempts = 0;
        let max_attempts = self.capacity * 2; // Prevent infinite loop

        while attempts < max_attempts {
            let access_order = self.access_order.read();
            if access_order.is_empty() {
                return Ok(());
            }

            let hand = self.clock_hand.fetch_add(1, Ordering::Relaxed) % access_order.len();

            if let Some(&page_id) = access_order.get(hand) {
                drop(access_order); // Release read lock

                // Use remove_if to atomically check and remove
                let removed = self.cache.remove_if(&page_id, |_key, cached_page| {
                    let access_count = cached_page.access_count.load(Ordering::Relaxed);
                    if access_count == 0 {
                        true // Remove this entry
                    } else {
                        // Give it a second chance - decrement reference count
                        cached_page.access_count.fetch_sub(1, Ordering::Relaxed);
                        false // Don't remove
                    }
                });

                if removed.is_some() {
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    self.stats.record_eviction();

                    // Remove from access order
                    let mut access_order_mut = self.access_order.write();
                    access_order_mut.retain(|&id| id != page_id);

                    return Ok(());
                }
            }

            attempts += 1;
        }

        // If we couldn't find a victim with Clock, just evict the oldest
        let mut access_order = self.access_order.write();
        if let Some(victim_id) = access_order.pop_front() {
            drop(access_order);

            if self.cache.remove(&victim_id).is_some() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                self.stats.record_eviction();
            }
        }

        Ok(())
    }

    /// Batch eviction for better performance
    pub fn evict_batch(&self, count: usize) -> Result<()> {
        for _ in 0..count {
            self.evict_one()?;
        }
        Ok(())
    }

    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        self.cache.clear();
        self.size.store(0, Ordering::Relaxed);
        self.access_order.write().clear();
        self.stats.reset();
    }
}

/// Segmented lock-free cache for even better scalability
/// Divides the cache into segments to reduce contention
pub struct SegmentedLockFreeCache {
    segments: Vec<Arc<LockFreeCache>>,
    num_segments: usize,
    total_capacity: usize,
    stats: Arc<CacheStats>,
}

impl SegmentedLockFreeCache {
    pub fn new(capacity: usize, num_segments: usize) -> Self {
        let segment_capacity = capacity / num_segments;
        let mut segments = Vec::with_capacity(num_segments);

        for _ in 0..num_segments {
            segments.push(Arc::new(LockFreeCache::new(segment_capacity)));
        }

        Self {
            segments,
            num_segments,
            total_capacity: capacity,
            stats: Arc::new(CacheStats::new()),
        }
    }

    fn get_segment(&self, page_id: u32) -> &Arc<LockFreeCache> {
        let segment_idx = (page_id as usize) % self.num_segments;
        &self.segments[segment_idx]
    }

    pub fn get(&self, page_id: u32) -> Option<Arc<CachedPage>> {
        let segment = self.get_segment(page_id);
        let result = segment.get(page_id);

        // Update global stats
        if result.is_some() {
            self.stats.record_hit();
        } else {
            self.stats.record_miss();
        }

        result
    }

    pub fn insert(&self, page_id: u32, page: Page) -> Result<()> {
        let segment = self.get_segment(page_id);
        segment.insert(page_id, page)
    }

    pub fn remove(&self, page_id: u32) {
        let segment = self.get_segment(page_id);
        segment.remove(page_id);
    }

    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }

    pub fn size(&self) -> usize {
        self.segments.iter().map(|s| s.size()).sum()
    }

    pub fn clear(&self) {
        for segment in &self.segments {
            segment.clear();
        }
        self.stats.reset();
    }

    pub fn total_capacity(&self) -> usize {
        self.total_capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_free_cache_basic() {
        let cache = LockFreeCache::new(100);

        // Test insert and get
        let page = Page::new(1);
        cache.insert(1, page.clone()).unwrap();

        let cached = cache.get(1).unwrap();
        assert_eq!(cached.page.id, 1);

        // Test miss
        assert!(cache.get(2).is_none());

        // Test stats
        assert_eq!(cache.stats().hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_lock_free_cache_eviction() {
        let cache = LockFreeCache::new(3);

        // Fill cache
        for i in 0..3 {
            cache.insert(i, Page::new(i)).unwrap();
        }

        assert_eq!(cache.size(), 3);

        // Insert one more - should trigger eviction
        cache.insert(3, Page::new(3)).unwrap();
        assert_eq!(cache.size(), 3);

        // At least one of the first 3 should have been evicted
        let mut found = 0;
        for i in 0..4 {
            if cache.get(i).is_some() {
                found += 1;
            }
        }
        assert_eq!(found, 3);
    }

    #[test]
    fn test_segmented_cache() {
        let cache = SegmentedLockFreeCache::new(100, 4);

        // Test basic operations
        cache.insert(1, Page::new(1)).unwrap();
        cache.insert(2, Page::new(2)).unwrap();

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_none());

        assert_eq!(cache.size(), 2);
    }
}

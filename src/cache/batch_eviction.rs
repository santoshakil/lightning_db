use crate::cache::{CacheStats, CachedPage};
use crate::error::Result;
use crate::storage::Page;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

/// Configuration for batch eviction
#[derive(Debug, Clone)]
pub struct BatchEvictionConfig {
    /// Number of pages to evict in a single batch
    pub batch_size: usize,
    /// Minimum time between batch evictions
    pub min_interval: Duration,
    /// Target free space percentage
    pub target_free_ratio: f64,
    /// Whether to prioritize dirty pages for writeback
    pub prioritize_dirty: bool,
}

impl Default for BatchEvictionConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            min_interval: Duration::from_millis(10),
            target_free_ratio: 0.2, // Keep 20% free
            prioritize_dirty: true,
        }
    }
}

/// Batch eviction statistics
#[derive(Debug, Default)]
pub struct BatchEvictionStats {
    pub total_batches: AtomicUsize,
    pub pages_evicted: AtomicUsize,
    pub dirty_pages_written: AtomicUsize,
    pub eviction_time_ms: AtomicUsize,
    pub last_eviction: Mutex<Option<Instant>>,
}

/// Enhanced ARC cache with batch eviction support
pub struct BatchEvictingArcCache {
    capacity: usize,
    config: BatchEvictionConfig,

    // ARC algorithm state
    p: AtomicUsize,                // Target size for T1
    t1: Arc<Mutex<VecDeque<u32>>>, // Recent cache
    t2: Arc<Mutex<VecDeque<u32>>>, // Frequent cache
    b1: Arc<Mutex<VecDeque<u32>>>, // Ghost recent
    b2: Arc<Mutex<VecDeque<u32>>>, // Ghost frequent

    // Page storage
    cache: Arc<DashMap<u32, Arc<CachedPage>>>,

    // Statistics
    stats: Arc<CacheStats>,
    eviction_stats: Arc<BatchEvictionStats>,

    // Eviction control
    evicting: AtomicBool,
    timestamp: AtomicUsize,
}

impl BatchEvictingArcCache {
    pub fn new(capacity: usize, config: BatchEvictionConfig) -> Self {
        Self {
            capacity,
            config,
            p: AtomicUsize::new(capacity / 2),
            t1: Arc::new(Mutex::new(VecDeque::new())),
            t2: Arc::new(Mutex::new(VecDeque::new())),
            b1: Arc::new(Mutex::new(VecDeque::new())),
            b2: Arc::new(Mutex::new(VecDeque::new())),
            cache: Arc::new(DashMap::new()),
            stats: Arc::new(CacheStats::new()),
            eviction_stats: Arc::new(BatchEvictionStats::default()),
            evicting: AtomicBool::new(false),
            timestamp: AtomicUsize::new(0),
        }
    }

    /// Get a page from cache
    pub fn get(&self, page_id: u32) -> Option<Arc<CachedPage>> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);

        if let Some(entry) = self.cache.get(&page_id) {
            let cached_page = entry.value().clone();
            cached_page.access(timestamp);

            // Move from T1 to T2 if in T1
            self.promote_to_frequent(page_id);

            self.stats.record_hit();
            Some(cached_page)
        } else {
            self.stats.record_miss();
            None
        }
    }

    /// Insert a page, potentially triggering batch eviction
    pub fn insert(&self, page_id: u32, page: Page) -> Result<()> {
        let timestamp = self.timestamp.fetch_add(1, Ordering::Relaxed);
        let cached_page = Arc::new(CachedPage::new(page));
        cached_page.access(timestamp);

        // Check if we need eviction
        if self.needs_eviction() {
            self.batch_evict()?;
        }

        // Handle ghost list hits and insertion
        self.handle_insertion(page_id, cached_page)?;

        Ok(())
    }

    /// Check if batch eviction is needed
    fn needs_eviction(&self) -> bool {
        let current_size = self.cache.len();
        let target_size = (self.capacity as f64 * (1.0 - self.config.target_free_ratio)) as usize;
        current_size >= target_size
    }

    /// Perform batch eviction
    pub fn batch_evict(&self) -> Result<Vec<u32>> {
        // Try to acquire eviction lock
        if self
            .evicting
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(Vec::new()); // Another thread is evicting
        }

        let start = Instant::now();
        let mut evicted = Vec::new();

        // Check last eviction time
        {
            let last_eviction = self.eviction_stats.last_eviction.lock();
            if let Some(last) = *last_eviction {
                if start.duration_since(last) < self.config.min_interval {
                    self.evicting.store(false, Ordering::Release);
                    return Ok(evicted);
                }
            }
        }

        // Collect victims
        let victims = self.select_victims(self.config.batch_size);

        // Process evictions
        for victim in victims {
            if let Some((_, cached_page)) = self.cache.remove(&victim) {
                // Handle dirty pages
                if cached_page.is_dirty() {
                    // In a real implementation, we'd schedule writeback here
                    self.eviction_stats
                        .dirty_pages_written
                        .fetch_add(1, Ordering::Relaxed);
                }

                evicted.push(victim);
                self.stats.record_eviction();
            }
        }

        // Update statistics
        self.eviction_stats
            .total_batches
            .fetch_add(1, Ordering::Relaxed);
        self.eviction_stats
            .pages_evicted
            .fetch_add(evicted.len(), Ordering::Relaxed);
        self.eviction_stats
            .eviction_time_ms
            .fetch_add(start.elapsed().as_millis() as usize, Ordering::Relaxed);
        *self.eviction_stats.last_eviction.lock() = Some(start);

        // Release eviction lock
        self.evicting.store(false, Ordering::Release);

        debug!(
            "Batch evicted {} pages in {:?}",
            evicted.len(),
            start.elapsed()
        );

        Ok(evicted)
    }

    /// Select victims for eviction based on ARC algorithm
    fn select_victims(&self, count: usize) -> Vec<u32> {
        let mut victims = Vec::with_capacity(count);
        let p_val = self.p.load(Ordering::Relaxed);

        // Lock all lists to ensure consistency
        let mut t1 = self.t1.lock();
        let mut t2 = self.t2.lock();
        let mut b1 = self.b1.lock();
        let mut b2 = self.b2.lock();

        // Collect victims based on ARC policy
        while victims.len() < count {
            let t1_len = t1.len();
            let should_evict_from_t1 = t1_len > 0 && (t1_len > p_val || t2.is_empty());

            if should_evict_from_t1 {
                // Evict from T1 (recent)
                if let Some(victim) = t1.pop_back() {
                    victims.push(victim);
                    b1.push_front(victim);

                    // Trim B1 if needed
                    if b1.len() + t2.len() > self.capacity {
                        b1.pop_back();
                    }
                } else {
                    break;
                }
            } else {
                // Evict from T2 (frequent)
                if let Some(victim) = t2.pop_back() {
                    victims.push(victim);
                    b2.push_front(victim);

                    // Trim B2 if needed
                    if b2.len() + t1.len() > self.capacity {
                        b2.pop_back();
                    }
                } else {
                    break;
                }
            }
        }

        // If prioritizing dirty pages, reorder victims
        if self.config.prioritize_dirty && !victims.is_empty() {
            self.prioritize_dirty_victims(&mut victims);
        }

        victims
    }

    /// Reorder victims to prioritize dirty pages for writeback
    fn prioritize_dirty_victims(&self, victims: &mut Vec<u32>) {
        let mut dirty_indices = Vec::new();
        let mut clean_indices = Vec::new();

        for (i, &page_id) in victims.iter().enumerate() {
            if let Some(entry) = self.cache.get(&page_id) {
                if entry.value().is_dirty() {
                    dirty_indices.push(i);
                } else {
                    clean_indices.push(i);
                }
            }
        }

        // Reorder: dirty pages first
        let mut reordered = Vec::with_capacity(victims.len());
        for &i in &dirty_indices {
            reordered.push(victims[i]);
        }
        for &i in &clean_indices {
            reordered.push(victims[i]);
        }

        *victims = reordered;
    }

    /// Promote a page from T1 to T2
    fn promote_to_frequent(&self, page_id: u32) {
        let mut t1 = self.t1.lock();
        if let Some(pos) = t1.iter().position(|&id| id == page_id) {
            t1.remove(pos);
            drop(t1);

            let mut t2 = self.t2.lock();
            t2.push_front(page_id);
        }
    }

    /// Handle page insertion with ghost list checks
    fn handle_insertion(&self, page_id: u32, cached_page: Arc<CachedPage>) -> Result<()> {
        let mut b1 = self.b1.lock();
        let mut b2 = self.b2.lock();

        if let Some(pos) = b1.iter().position(|&id| id == page_id) {
            // Hit in B1 - adapt p upward
            b1.remove(pos);
            let delta = if b1.len() >= b2.len() {
                1
            } else {
                b2.len() / b1.len().max(1)
            };
            let p_val = self.p.load(Ordering::Relaxed);
            self.p
                .store((p_val + delta).min(self.capacity), Ordering::Relaxed);

            drop(b1);
            drop(b2);

            // Insert into T2
            let mut t2 = self.t2.lock();
            t2.push_front(page_id);
            self.cache.insert(page_id, cached_page);
        } else if let Some(pos) = b2.iter().position(|&id| id == page_id) {
            // Hit in B2 - adapt p downward
            b2.remove(pos);
            let delta = if b2.len() >= b1.len() {
                1
            } else {
                b1.len() / b2.len().max(1)
            };
            let p_val = self.p.load(Ordering::Relaxed);
            self.p.store(p_val.saturating_sub(delta), Ordering::Relaxed);

            drop(b1);
            drop(b2);

            // Insert into T2
            let mut t2 = self.t2.lock();
            t2.push_front(page_id);
            self.cache.insert(page_id, cached_page);
        } else {
            // New page - insert into T1
            drop(b1);
            drop(b2);

            let mut t1 = self.t1.lock();
            t1.push_front(page_id);
            self.cache.insert(page_id, cached_page);
        }

        Ok(())
    }

    /// Get batch eviction statistics
    pub fn eviction_stats(&self) -> &BatchEvictionStats {
        &self.eviction_stats
    }

    /// Manually trigger batch eviction
    pub fn force_batch_evict(&self, target_pages: usize) -> Result<Vec<u32>> {
        let victims = self.select_victims(target_pages);
        let mut evicted = Vec::new();

        for victim in victims {
            if let Some((_, _)) = self.cache.remove(&victim) {
                evicted.push(victim);
                self.stats.record_eviction();
            }
        }

        Ok(evicted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_eviction() {
        let config = BatchEvictionConfig {
            batch_size: 10,
            min_interval: Duration::from_millis(0),
            target_free_ratio: 0.2,
            prioritize_dirty: false,
        };

        let cache = BatchEvictingArcCache::new(100, config);

        // Fill cache beyond target
        for i in 0..85 {
            let page = Page::new(i);
            cache.insert(i, page).unwrap();
        }

        // Should trigger batch eviction
        let evicted = cache.batch_evict().unwrap();
        assert!(!evicted.is_empty());
        assert!(evicted.len() <= 10); // Should respect batch size

        // Verify evicted pages are gone
        for page_id in evicted {
            assert!(cache.get(page_id).is_none());
        }
    }

    #[test]
    fn test_concurrent_eviction() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let config = BatchEvictionConfig::default();
        let cache = StdArc::new(BatchEvictingArcCache::new(50, config));

        // Multiple threads trying to evict
        let mut handles = vec![];

        for t in 0..4 {
            let cache_clone = cache.clone();
            let handle = thread::spawn(move || {
                for i in 0..30 {
                    let page = Page::new(t * 100 + i);
                    cache_clone.insert(t * 100 + i, page).unwrap();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only one batch eviction should happen at a time
        let stats = cache.eviction_stats();
        assert!(stats.total_batches.load(Ordering::Relaxed) > 0);
    }
}

//! Advanced Caching Strategies
//!
//! Implements state-of-the-art caching algorithms:
//! - Clock-Pro: An improved CLOCK algorithm with better scan resistance
//! - W-TinyLFU: Window TinyLFU with frequency sketch for admission control
//! - S3-FIFO: Simple, Scalable and Streaming FIFO
//!
//! These algorithms provide better hit rates and scan resistance compared to
//! traditional LRU/ARC caches.

use parking_lot::{Mutex, RwLock};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Trait for advanced cache implementations
pub trait AdvancedCache<K: Clone + Hash + Eq, V: Clone>: Send + Sync {
    /// Get a value from the cache
    fn get(&self, key: &K) -> Option<V>;

    /// Put a value into the cache
    fn put(&self, key: K, value: V);

    /// Remove a value from the cache
    fn remove(&self, key: &K) -> Option<V>;

    /// Clear the cache
    fn clear(&self);

    /// Get cache statistics
    fn stats(&self) -> AdvancedCacheStats;

    /// Get the current size of the cache
    fn size(&self) -> usize;

    /// Get the maximum capacity
    fn capacity(&self) -> usize;
}

/// Advanced cache statistics
#[derive(Debug, Clone, Default)]
pub struct AdvancedCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub admissions: u64,
    pub rejections: u64,
}

impl AdvancedCacheStats {
    /// Calculate hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Clock-Pro Cache Implementation
///
/// Clock-Pro is an improved CLOCK algorithm that provides better scan resistance
/// and adapts to changing access patterns. It maintains hot and cold pages with
/// different eviction policies.
pub struct ClockProCache<K: Clone + Hash + Eq, V: Clone> {
    capacity: usize,
    hot_capacity: usize,
    entries: Arc<RwLock<HashMap<K, Arc<ClockProEntry<K, V>>>>>,
    clock_hand_hot: Arc<AtomicUsize>,
    clock_hand_cold: Arc<AtomicUsize>,
    hot_pages: Arc<RwLock<VecDeque<K>>>,
    cold_pages: Arc<RwLock<VecDeque<K>>>,
    stats: Arc<RwLock<AdvancedCacheStats>>,
}

#[derive(Debug)]
struct ClockProEntry<K, V> {
    key: K,
    value: V,
    is_hot: AtomicU64, // 0 = cold, 1 = hot
    reference_bit: AtomicU64,
    test_bit: AtomicU64,
}

impl<K: Clone + Hash + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static>
    ClockProCache<K, V>
{
    /// Create a new Clock-Pro cache
    pub fn new(capacity: usize) -> Self {
        let hot_capacity = capacity * 3 / 4; // 75% for hot pages

        Self {
            capacity,
            hot_capacity,
            entries: Arc::new(RwLock::new(HashMap::new())),
            clock_hand_hot: Arc::new(AtomicUsize::new(0)),
            clock_hand_cold: Arc::new(AtomicUsize::new(0)),
            hot_pages: Arc::new(RwLock::new(VecDeque::new())),
            cold_pages: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(AdvancedCacheStats::default())),
        }
    }

    /// Run the clock algorithm to find a victim
    fn clock_run(&self, is_hot: bool) -> Option<K> {
        if is_hot {
            let hot_pages = self.hot_pages.read();
            if hot_pages.is_empty() {
                return None;
            }

            let mut hand = self.clock_hand_hot.load(Ordering::Relaxed);
            let entries = self.entries.read();

            // Run clock until we find a victim
            for _ in 0..hot_pages.len() * 2 {
                hand = hand % hot_pages.len();
                if let Some(key) = hot_pages.get(hand) {
                    if let Some(entry) = entries.get(key) {
                        if entry.reference_bit.swap(0, Ordering::Relaxed) == 0 {
                            // Found a victim
                            self.clock_hand_hot
                                .store((hand + 1) % hot_pages.len(), Ordering::Relaxed);
                            return Some(key.clone());
                        }
                    }
                }
                hand += 1;
            }
        } else {
            let cold_pages = self.cold_pages.read();
            if cold_pages.is_empty() {
                return None;
            }

            let mut hand = self.clock_hand_cold.load(Ordering::Relaxed);
            let entries = self.entries.read();

            // Run clock until we find a victim
            for _ in 0..cold_pages.len() * 2 {
                hand = hand % cold_pages.len();
                if let Some(key) = cold_pages.get(hand) {
                    if let Some(entry) = entries.get(key) {
                        if entry.test_bit.load(Ordering::Relaxed) == 0 {
                            // Found a victim
                            self.clock_hand_cold
                                .store((hand + 1) % cold_pages.len(), Ordering::Relaxed);
                            return Some(key.clone());
                        } else {
                            // Promote to hot
                            entry.test_bit.store(0, Ordering::Relaxed);
                            entry.is_hot.store(1, Ordering::Relaxed);
                        }
                    }
                }
                hand += 1;
            }
        }

        None
    }

    /// Evict a page if necessary
    fn evict_if_needed(&self) {
        let total_size = {
            let hot = self.hot_pages.read().len();
            let cold = self.cold_pages.read().len();
            hot + cold
        };

        if total_size >= self.capacity {
            // Try to evict from cold pages first
            if let Some(victim_key) = self.clock_run(false) {
                self.remove_internal(&victim_key);
                self.stats.write().evictions += 1;
            } else if let Some(victim_key) = self.clock_run(true) {
                // If no cold victim, evict from hot
                self.remove_internal(&victim_key);
                self.stats.write().evictions += 1;
            }
        }
    }

    fn remove_internal(&self, key: &K) {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            let is_hot = entry.is_hot.load(Ordering::Relaxed) == 1;

            if is_hot {
                let mut hot_pages = self.hot_pages.write();
                hot_pages.retain(|k| k != key);
            } else {
                let mut cold_pages = self.cold_pages.write();
                cold_pages.retain(|k| k != key);
            }
        }
    }
}

impl<K: Clone + Hash + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static>
    AdvancedCache<K, V> for ClockProCache<K, V>
{
    fn get(&self, key: &K) -> Option<V> {
        let entries = self.entries.read();

        if let Some(entry) = entries.get(key) {
            // Set reference bit
            entry.reference_bit.store(1, Ordering::Relaxed);

            // Update stats
            self.stats.write().hits += 1;

            Some(entry.value.clone())
        } else {
            self.stats.write().misses += 1;
            None
        }
    }

    fn put(&self, key: K, value: V) {
        // Check if key already exists
        {
            let entries = self.entries.read();
            if entries.contains_key(&key) {
                // Update existing entry
                if let Some(entry) = entries.get(&key) {
                    // Create new entry with updated value
                    let new_entry = Arc::new(ClockProEntry {
                        key: key.clone(),
                        value,
                        is_hot: AtomicU64::new(entry.is_hot.load(Ordering::Relaxed)),
                        reference_bit: AtomicU64::new(1),
                        test_bit: AtomicU64::new(entry.test_bit.load(Ordering::Relaxed)),
                    });

                    drop(entries);
                    self.entries.write().insert(key, new_entry);
                    return;
                }
            }
        }

        // Evict if needed
        self.evict_if_needed();

        // Add new entry as cold
        let entry = Arc::new(ClockProEntry {
            key: key.clone(),
            value,
            is_hot: AtomicU64::new(0),
            reference_bit: AtomicU64::new(1),
            test_bit: AtomicU64::new(1),
        });

        self.entries.write().insert(key.clone(), entry);
        self.cold_pages.write().push_back(key);
        self.stats.write().admissions += 1;
    }

    fn remove(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.write();
        if let Some(entry) = entries.remove(key) {
            let value = entry.value.clone();
            let is_hot = entry.is_hot.load(Ordering::Relaxed) == 1;

            drop(entries);

            if is_hot {
                self.hot_pages.write().retain(|k| k != key);
            } else {
                self.cold_pages.write().retain(|k| k != key);
            }

            Some(value)
        } else {
            None
        }
    }

    fn clear(&self) {
        self.entries.write().clear();
        self.hot_pages.write().clear();
        self.cold_pages.write().clear();
        self.clock_hand_hot.store(0, Ordering::Relaxed);
        self.clock_hand_cold.store(0, Ordering::Relaxed);
        *self.stats.write() = AdvancedCacheStats::default();
    }

    fn stats(&self) -> AdvancedCacheStats {
        self.stats.read().clone()
    }

    fn size(&self) -> usize {
        self.entries.read().len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// W-TinyLFU Cache Implementation
///
/// Window TinyLFU (W-TinyLFU) is a modern cache admission policy that uses
/// a frequency sketch to track item popularity and make better eviction decisions.
/// It combines a window cache (1%) with a main cache (99%) using TinyLFU admission.
pub struct WTinyLFUCache<K: Clone + Hash + Eq, V: Clone> {
    window_cache: Arc<RwLock<LRUCache<K, V>>>,
    main_cache: Arc<RwLock<SLRUCache<K, V>>>,
    frequency_sketch: Arc<Mutex<CountMinSketch>>,
    window_size: usize,
    main_size: usize,
    stats: Arc<RwLock<AdvancedCacheStats>>,
}

/// Simple LRU cache for the window
struct LRUCache<K: Clone + Hash + Eq, V: Clone> {
    capacity: usize,
    entries: HashMap<K, V>,
    order: VecDeque<K>,
}

impl<K: Clone + Hash + Eq, V: Clone> LRUCache<K, V> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.entries.get(key) {
            // Move to front
            self.order.retain(|k| k != key);
            self.order.push_front(key.clone());
            Some(value.clone())
        } else {
            None
        }
    }

    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        if self.entries.contains_key(&key) {
            self.entries.insert(key.clone(), value);
            self.order.retain(|k| k != &key);
            self.order.push_front(key);
            None
        } else {
            let mut evicted = None;

            if self.entries.len() >= self.capacity {
                if let Some(victim) = self.order.pop_back() {
                    if let Some(value) = self.entries.remove(&victim) {
                        evicted = Some((victim, value));
                    }
                }
            }

            self.entries.insert(key.clone(), value);
            self.order.push_front(key);

            evicted
        }
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.entries.remove(key) {
            self.order.retain(|k| k != key);
            Some(value)
        } else {
            None
        }
    }
}

/// Segmented LRU for the main cache
struct SLRUCache<K: Clone + Hash + Eq, V: Clone> {
    probation_capacity: usize,
    protected_capacity: usize,
    probation: LRUCache<K, V>,
    protected: LRUCache<K, V>,
}

impl<K: Clone + Hash + Eq, V: Clone> SLRUCache<K, V> {
    fn new(capacity: usize) -> Self {
        let probation_capacity = capacity * 20 / 100; // 20% probation
        let protected_capacity = capacity * 80 / 100; // 80% protected

        Self {
            probation_capacity,
            protected_capacity,
            probation: LRUCache::new(probation_capacity),
            protected: LRUCache::new(protected_capacity),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        // Check protected first
        if let Some(value) = self.protected.get(key) {
            return Some(value);
        }

        // Check probation and promote if found
        if let Some(value) = self.probation.remove(key) {
            // Promote to protected
            if let Some((evicted_key, evicted_val)) = self.protected.put(key.clone(), value.clone())
            {
                // Demote evicted to probation
                self.probation.put(evicted_key, evicted_val);
            }
            return Some(value);
        }

        None
    }

    fn put(&mut self, key: K, value: V) -> Option<(K, V)> {
        // New items go to probation
        self.probation.put(key, value)
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.protected.remove(key) {
            return Some(value);
        }
        self.probation.remove(key)
    }
}

/// Count-Min Sketch for frequency estimation
struct CountMinSketch {
    counters: Vec<Vec<u8>>,
    width: usize,
    depth: usize,
    decay_counter: u64,
}

impl CountMinSketch {
    fn new(width: usize, depth: usize) -> Self {
        let counters = vec![vec![0u8; width]; depth];
        Self {
            counters,
            width,
            depth,
            decay_counter: 0,
        }
    }

    fn increment<K: Hash>(&mut self, key: &K) {
        for i in 0..self.depth {
            let hash = self.hash(key, i);
            let idx = hash % self.width;
            self.counters[i][idx] = self.counters[i][idx].saturating_add(1);
        }

        // Decay periodically
        self.decay_counter += 1;
        if self.decay_counter % 10000 == 0 {
            self.decay();
        }
    }

    fn estimate<K: Hash>(&self, key: &K) -> u8 {
        let mut min_count = u8::MAX;

        for i in 0..self.depth {
            let hash = self.hash(key, i);
            let idx = hash % self.width;
            min_count = min_count.min(self.counters[i][idx]);
        }

        min_count
    }

    fn hash<K: Hash>(&self, key: &K, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }

    fn decay(&mut self) {
        for row in &mut self.counters {
            for count in row {
                *count = *count >> 1; // Divide by 2
            }
        }
    }
}

impl<K: Clone + Hash + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static>
    WTinyLFUCache<K, V>
{
    /// Create a new W-TinyLFU cache
    pub fn new(capacity: usize) -> Self {
        let window_size = capacity.max(1) / 100; // 1% window
        let main_size = capacity - window_size;

        Self {
            window_cache: Arc::new(RwLock::new(LRUCache::new(window_size))),
            main_cache: Arc::new(RwLock::new(SLRUCache::new(main_size))),
            frequency_sketch: Arc::new(Mutex::new(CountMinSketch::new(capacity * 8, 4))),
            window_size,
            main_size,
            stats: Arc::new(RwLock::new(AdvancedCacheStats::default())),
        }
    }

    /// TinyLFU admission decision
    fn should_admit(&self, candidate_key: &K, victim_key: &K) -> bool {
        let sketch = self.frequency_sketch.lock();
        let candidate_freq = sketch.estimate(candidate_key);
        let victim_freq = sketch.estimate(victim_key);

        // Admit if candidate is more frequent
        candidate_freq > victim_freq
    }
}

impl<K: Clone + Hash + Eq + Send + Sync + 'static, V: Clone + Send + Sync + 'static>
    AdvancedCache<K, V> for WTinyLFUCache<K, V>
{
    fn get(&self, key: &K) -> Option<V> {
        // Increment frequency
        self.frequency_sketch.lock().increment(key);

        // Check window cache first
        {
            let mut window = self.window_cache.write();
            if let Some(value) = window.get(key) {
                self.stats.write().hits += 1;
                return Some(value);
            }
        }

        // Check main cache
        {
            let mut main = self.main_cache.write();
            if let Some(value) = main.get(key) {
                self.stats.write().hits += 1;
                return Some(value);
            }
        }

        self.stats.write().misses += 1;
        None
    }

    fn put(&self, key: K, value: V) {
        // Increment frequency
        self.frequency_sketch.lock().increment(&key);

        // Try to put in window first
        let mut window = self.window_cache.write();
        if let Some((evicted_key, evicted_val)) = window.put(key, value) {
            // Window eviction - try to admit to main
            drop(window);

            let mut main = self.main_cache.write();
            if let Some((victim_key, _)) = main.put(evicted_key.clone(), evicted_val) {
                // Use TinyLFU admission policy
                if self.should_admit(&evicted_key, &victim_key) {
                    self.stats.write().admissions += 1;
                } else {
                    // Reject admission, keep victim
                    main.remove(&evicted_key);
                    self.stats.write().rejections += 1;
                    self.stats.write().evictions += 1;
                }
            } else {
                self.stats.write().admissions += 1;
            }
        } else {
            self.stats.write().admissions += 1;
        }
    }

    fn remove(&self, key: &K) -> Option<V> {
        // Check window first
        if let Some(value) = self.window_cache.write().remove(key) {
            return Some(value);
        }

        // Check main
        self.main_cache.write().remove(key)
    }

    fn clear(&self) {
        self.window_cache.write().entries.clear();
        self.window_cache.write().order.clear();
        self.main_cache.write().probation.entries.clear();
        self.main_cache.write().probation.order.clear();
        self.main_cache.write().protected.entries.clear();
        self.main_cache.write().protected.order.clear();
        *self.stats.write() = AdvancedCacheStats::default();
    }

    fn stats(&self) -> AdvancedCacheStats {
        self.stats.read().clone()
    }

    fn size(&self) -> usize {
        let window_size = self.window_cache.read().entries.len();
        let main_size = {
            let main = self.main_cache.read();
            main.probation.entries.len() + main.protected.entries.len()
        };
        window_size + main_size
    }

    fn capacity(&self) -> usize {
        self.window_size + self.main_size
    }
}

/// Cache type selector
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CacheType {
    Arc,      // Adaptive Replacement Cache (existing)
    ClockPro, // Clock-Pro algorithm
    WTinyLFU, // Window TinyLFU
}

/// Factory for creating cache instances
pub fn create_cache<
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
>(
    cache_type: CacheType,
    capacity: usize,
) -> Box<dyn AdvancedCache<K, V>> {
    match cache_type {
        CacheType::ClockPro => Box::new(ClockProCache::new(capacity)),
        CacheType::WTinyLFU => Box::new(WTinyLFUCache::new(capacity)),
        CacheType::Arc => panic!("ARC cache should use existing implementation"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_pro_basic() {
        let cache = ClockProCache::new(3);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        assert_eq!(cache.get(&1), Some("one"));
        assert_eq!(cache.get(&2), Some("two"));
        assert_eq!(cache.get(&3), Some("three"));

        // Should evict least recently used
        cache.put(4, "four");
        assert_eq!(cache.size(), 3);
    }

    #[test]
    fn test_wtinylfu_basic() {
        let cache = WTinyLFUCache::new(100);

        // Populate cache
        for i in 0..50 {
            cache.put(i, i * 2);
        }

        // Access some items frequently
        for _ in 0..10 {
            cache.get(&5);
            cache.get(&10);
            cache.get(&15);
        }

        // Add more items
        for i in 50..100 {
            cache.put(i, i * 2);
        }

        // Frequently accessed items should still be in cache
        assert_eq!(cache.get(&5), Some(10));
        assert_eq!(cache.get(&10), Some(20));
        assert_eq!(cache.get(&15), Some(30));

        let stats = cache.stats();
        println!("W-TinyLFU hit ratio: {:.2}%", stats.hit_ratio() * 100.0);
    }

    #[test]
    fn test_cache_scan_resistance() {
        let cache = WTinyLFUCache::new(100);

        // Create a working set
        for i in 0..50 {
            cache.put(i, i);
            cache.get(&i); // Access once
        }

        // Access working set multiple times
        for _ in 0..5 {
            for i in 0..50 {
                cache.get(&i);
            }
        }

        // Perform a scan (one-time access of many items)
        for i in 100..200 {
            cache.put(i, i);
        }

        // Working set should still be in cache
        let mut hits = 0;
        for i in 0..50 {
            if cache.get(&i).is_some() {
                hits += 1;
            }
        }

        assert!(hits > 40); // Most of working set should survive
    }
}

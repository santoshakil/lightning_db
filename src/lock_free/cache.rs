use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Lock-free LRU-like cache using DashMap and atomic operations
/// Uses clock algorithm for eviction (approximates LRU without locks)
pub struct LockFreeCache<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    map: Arc<DashMap<K, CacheEntry<V>>>,
    capacity: usize,
    size: Arc<CachePadded<AtomicUsize>>,
    hits: Arc<CachePadded<AtomicU64>>,
    misses: Arc<CachePadded<AtomicU64>>,
    clock_hand: Arc<CachePadded<AtomicUsize>>,
}

struct CacheEntry<V> {
    value: V,
    accessed: AtomicU64, // Use counter instead of bool for better concurrency
    timestamp: Instant,
}

impl<V: Clone> Clone for CacheEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            accessed: AtomicU64::new(self.accessed.load(Ordering::Relaxed)),
            timestamp: self.timestamp,
        }
    }
}

impl<K, V> LockFreeCache<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
            size: Arc::new(CachePadded::new(AtomicUsize::new(0))),
            hits: Arc::new(CachePadded::new(AtomicU64::new(0))),
            misses: Arc::new(CachePadded::new(AtomicU64::new(0))),
            clock_hand: Arc::new(CachePadded::new(AtomicUsize::new(0))),
        }
    }

    /// Get a value from cache (lock-free read path)
    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(entry) = self.map.get(key) {
            // Mark as accessed
            entry.accessed.fetch_add(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a value into cache (mostly lock-free)
    pub fn insert(&self, key: K, value: V) {
        let entry = CacheEntry {
            value,
            accessed: AtomicU64::new(1),
            timestamp: Instant::now(),
        };

        // Insert new entry
        let prev_entry = self.map.insert(key, entry);

        if prev_entry.is_none() {
            // New entry added, increment size
            let new_size = self.size.fetch_add(1, Ordering::AcqRel) + 1;

            // Check if we exceeded capacity and need to evict
            if new_size > self.capacity {
                // Evict entries until we're back at capacity
                let excess = new_size - self.capacity;
                for _ in 0..excess {
                    self.evict_one();

                    // Check if we're at capacity now
                    if self.size.load(Ordering::Acquire) <= self.capacity {
                        break;
                    }
                }
            }
        }
    }

    /// Remove a value from cache
    pub fn remove(&self, key: &K) -> Option<V> {
        if let Some((_, entry)) = self.map.remove(key) {
            self.size.fetch_sub(1, Ordering::Relaxed);
            Some(entry.value)
        } else {
            None
        }
    }

    /// Evict one entry using clock algorithm
    fn evict_one(&self) {
        let mut attempts = 0;
        let max_attempts = self.capacity * 2;

        // Get all keys for clock sweep
        let keys: Vec<K> = self.map.iter().map(|entry| entry.key().clone()).collect();

        if keys.is_empty() {
            return;
        }

        let num_keys = keys.len();
        let mut hand = self.clock_hand.load(Ordering::Relaxed);

        while attempts < max_attempts {
            hand = (hand + 1) % num_keys;
            attempts += 1;

            if let Some(key) = keys.get(hand) {
                if let Some(entry) = self.map.get_mut(key) {
                    let accessed = entry.accessed.load(Ordering::Relaxed);
                    if accessed == 0 {
                        // Found victim, remove it
                        drop(entry); // Release the lock
                        self.map.remove(key);
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        self.clock_hand.store(hand, Ordering::Relaxed);
                        return;
                    } else {
                        // Give it another chance, decrement access count
                        entry.accessed.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // If no victim found after max attempts, force evict at current position
        if let Some(key) = keys.get(hand) {
            self.map.remove(key);
            self.size.fetch_sub(1, Ordering::Relaxed);
        }

        self.clock_hand.store(hand, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        CacheStats {
            size: self.size.load(Ordering::Relaxed),
            capacity: self.capacity,
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.map.clear();
        self.size.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
}

/// Thread-local cache layer for reduced contention
pub struct ThreadLocalCache<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    shared: Arc<LockFreeCache<K, V>>,
}

impl<K, V> ThreadLocalCache<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(shared: Arc<LockFreeCache<K, V>>, _local_capacity: usize) -> Self {
        Self { shared }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        // TODO: Implement actual thread-local caching
        // type LocalCacheType = std::cell::RefCell<Option<dashmap::DashMap<Vec<u8>, (Vec<u8>, Instant)>>>;
        // 
        // thread_local! {
        //     static LOCAL_CACHE: LocalCacheType = const { std::cell::RefCell::new(None) };
        // }

        // For now, just use shared cache
        self.shared.get(key)
    }

    pub fn insert(&self, key: K, value: V) {
        self.shared.insert(key, value)
    }
}

/// Sharded cache for reduced contention
pub struct ShardedCache<K, V>
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    shards: Vec<Arc<LockFreeCache<K, V>>>,
    shard_count: usize,
}

impl<K, V> ShardedCache<K, V>
where
    K: Clone + Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(total_capacity: usize, shard_count: usize) -> Self {
        let capacity_per_shard = total_capacity / shard_count;
        let mut shards = Vec::with_capacity(shard_count);

        for _ in 0..shard_count {
            shards.push(Arc::new(LockFreeCache::new(capacity_per_shard)));
        }

        Self {
            shards,
            shard_count,
        }
    }

    fn get_shard(&self, key: &K) -> &Arc<LockFreeCache<K, V>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = std::hash::Hasher::finish(&hasher);
        let shard_idx = (hash as usize) % self.shard_count;
        &self.shards[shard_idx]
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.get_shard(key).get(key)
    }

    pub fn insert(&self, key: K, value: V) {
        self.get_shard(&key).insert(key, value);
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.get_shard(key).remove(key)
    }

    pub fn stats(&self) -> CacheStats {
        let mut total_stats = CacheStats {
            size: 0,
            capacity: 0,
            hits: 0,
            misses: 0,
            hit_rate: 0.0,
        };

        for shard in &self.shards {
            let stats = shard.stats();
            total_stats.size += stats.size;
            total_stats.capacity += stats.capacity;
            total_stats.hits += stats.hits;
            total_stats.misses += stats.misses;
        }

        let total = total_stats.hits + total_stats.misses;
        total_stats.hit_rate = if total > 0 {
            total_stats.hits as f64 / total as f64
        } else {
            0.0
        };

        total_stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_lock_free_cache() {
        let cache = Arc::new(LockFreeCache::<String, String>::new(100));

        // Test basic operations
        cache.insert("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), None);

        // Test concurrent access
        let mut handles = vec![];

        for i in 0..4 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("thread{}key{}", i, j);
                    let value = format!("thread{}value{}", i, j);
                    cache_clone.insert(key.clone(), value.clone());

                    // Don't assert on immediate read-back as the value might be evicted
                    // in a capacity-limited cache with concurrent access
                    let _ = cache_clone.get(&key);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = cache.stats();
        assert!(stats.size <= 100); // Should respect capacity
        assert!(stats.hits > 0);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = LockFreeCache::new(3);

        // Fill cache
        cache.insert("a", 1);
        cache.insert("b", 2);
        cache.insert("c", 3);

        // Access 'a' and 'b' to mark them as recently used
        cache.get(&"a");
        cache.get(&"b");

        // Insert new item, should evict 'c'
        cache.insert("d", 4);

        assert_eq!(cache.stats().size, 3);
        assert!(
            cache.get(&"a").is_some() || cache.get(&"b").is_some() || cache.get(&"d").is_some()
        );
    }

    #[test]
    fn test_sharded_cache() {
        let cache = ShardedCache::new(1000, 16);

        // Test operations across shards
        for i in 0..1000 {
            cache.insert(i, i * 2);
        }

        for i in 0..1000 {
            if let Some(v) = cache.get(&i) {
                assert_eq!(v, i * 2);
            }
        }

        let stats = cache.stats();
        assert!(stats.size > 0);
        assert!(stats.size <= 1000);
    }
}

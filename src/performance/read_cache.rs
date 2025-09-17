use std::sync::Arc;
use lru::LruCache;
use parking_lot::RwLock;

const DEFAULT_CACHE_SIZE: usize = 10000;

pub struct ReadCache {
    cache: Arc<RwLock<LruCache<Vec<u8>, Vec<u8>>>>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl ReadCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity).unwrap_or(
                    std::num::NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap()
                )
            ))),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut cache = self.cache.write();
        if let Some(value) = cache.get(key) {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(value.clone())
        } else {
            self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut cache = self.cache.write();
        cache.put(key, value);
    }

    pub fn invalidate(&self, key: &[u8]) {
        let mut cache = self.cache.write();
        cache.pop(key);
    }

    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    pub fn hit_rate(&self) -> f64 {
        use std::sync::atomic::Ordering;
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let misses = self.misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
}
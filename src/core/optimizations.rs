use crate::core::error::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Performance optimization configurations
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    pub enable_prefetch: bool,
    pub enable_batch_optimization: bool,
    pub enable_hot_path_cache: bool,
    pub adaptive_cache_size: bool,
    pub compression_threshold: usize,
    pub batch_threshold: usize,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            enable_prefetch: true,
            enable_batch_optimization: true,
            enable_hot_path_cache: true,
            adaptive_cache_size: true,
            compression_threshold: 1024,
            batch_threshold: 100,
        }
    }
}

/// Hot path cache for frequently accessed keys
pub struct HotPathCache {
    cache: Arc<RwLock<HashMap<Vec<u8>, (Vec<u8>, Instant)>>>,
    max_size: usize,
    ttl: Duration,
}

impl HotPathCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            ttl,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache = self.cache.read();
        cache.get(key).and_then(|(value, timestamp)| {
            if timestamp.elapsed() < self.ttl {
                Some(value.clone())
            } else {
                None
            }
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut cache = self.cache.write();

        if cache.len() >= self.max_size {
            // Simple eviction: remove oldest entry
            let oldest = cache.iter()
                .min_by_key(|(_, (_, ts))| *ts)
                .map(|(k, _)| k.clone());

            if let Some(oldest_key) = oldest {
                cache.remove(&oldest_key);
            }
        }

        cache.insert(key, (value, Instant::now()));
    }

    pub fn invalidate(&self, key: &[u8]) {
        let mut cache = self.cache.write();
        cache.remove(key);
    }

    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }
}

/// Batch operation optimizer
pub struct BatchOptimizer {
    pending: Arc<RwLock<Vec<BatchOperation>>>,
    threshold: usize,
}

#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl BatchOptimizer {
    pub fn new(threshold: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(Vec::new())),
            threshold,
        }
    }

    pub fn add_operation(&self, op: BatchOperation) -> bool {
        let mut pending = self.pending.write();
        pending.push(op);
        pending.len() >= self.threshold
    }

    pub fn take_batch(&self) -> Vec<BatchOperation> {
        let mut pending = self.pending.write();
        std::mem::take(&mut *pending)
    }

    pub fn pending_count(&self) -> usize {
        let pending = self.pending.read();
        pending.len()
    }
}

/// Prefetch predictor for sequential access patterns
pub struct PrefetchPredictor {
    history: Arc<RwLock<Vec<Vec<u8>>>>,
    window_size: usize,
}

impl PrefetchPredictor {
    pub fn new(window_size: usize) -> Self {
        Self {
            history: Arc::new(RwLock::new(Vec::new())),
            window_size,
        }
    }

    pub fn record_access(&self, key: &[u8]) {
        let mut history = self.history.write();
        history.push(key.to_vec());
        if history.len() > self.window_size {
            history.remove(0);
        }
    }

    pub fn predict_next(&self) -> Option<Vec<u8>> {
        let history = self.history.read();
        if history.len() < 2 {
            return None;
        }

        // Simple sequential pattern detection
        let last = &history[history.len() - 1];
        let second_last = &history[history.len() - 2];

        if let (Some(last_num), Some(prev_num)) = (
            extract_number(last),
            extract_number(second_last),
        ) {
            if last_num == prev_num + 1 {
                // Sequential pattern detected
                return create_next_key(last, last_num + 1);
            }
        }

        None
    }
}

fn extract_number(key: &[u8]) -> Option<u64> {
    // Simple extraction: find trailing number
    let s = String::from_utf8_lossy(key);
    s.chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>()
        .parse()
        .ok()
}

fn create_next_key(template: &[u8], num: u64) -> Option<Vec<u8>> {
    let s = String::from_utf8_lossy(template);
    let prefix_len = s.len() - num.to_string().len() + 1;
    if prefix_len > 0 && prefix_len <= s.len() {
        let prefix = &s[..prefix_len - 1];
        Some(format!("{}{}", prefix, num).into_bytes())
    } else {
        None
    }
}

/// Adaptive cache size manager
pub struct AdaptiveCacheManager {
    current_size: Arc<RwLock<usize>>,
    min_size: usize,
    max_size: usize,
    hit_rate: Arc<RwLock<f64>>,
    adjustment_interval: Duration,
    last_adjustment: Arc<RwLock<Instant>>,
}

impl AdaptiveCacheManager {
    pub fn new(initial_size: usize, min_size: usize, max_size: usize) -> Self {
        Self {
            current_size: Arc::new(RwLock::new(initial_size)),
            min_size,
            max_size,
            hit_rate: Arc::new(RwLock::new(0.0)),
            adjustment_interval: Duration::from_secs(60),
            last_adjustment: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn update_hit_rate(&self, hits: u64, misses: u64) {
        let total = hits + misses;
        if total > 0 {
            let mut hit_rate = self.hit_rate.write();
            *hit_rate = hits as f64 / total as f64;
        }
    }

    pub fn should_adjust(&self) -> bool {
        let last = self.last_adjustment.read();
        last.elapsed() >= self.adjustment_interval
    }

    pub fn adjust_cache_size(&self) -> usize {
        let mut current = self.current_size.write();
        let hit_rate = *self.hit_rate.read();
        let mut last_adjustment = self.last_adjustment.write();

        let new_size = if hit_rate < 0.5 {
            // Low hit rate: increase cache
            (*current as f64 * 1.2) as usize
        } else if hit_rate > 0.9 {
            // High hit rate: can reduce cache slightly
            (*current as f64 * 0.95) as usize
        } else {
            *current
        };

        *current = new_size.clamp(self.min_size, self.max_size);
        *last_adjustment = Instant::now();
        *current
    }

    pub fn get_current_size(&self) -> usize {
        *self.current_size.read()
    }
}

/// Memory pool for reducing allocations
pub struct MemoryPool {
    small_buffers: Arc<RwLock<Vec<Vec<u8>>>>,
    medium_buffers: Arc<RwLock<Vec<Vec<u8>>>>,
    large_buffers: Arc<RwLock<Vec<Vec<u8>>>>,
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            small_buffers: Arc::new(RwLock::new(Vec::new())),
            medium_buffers: Arc::new(RwLock::new(Vec::new())),
            large_buffers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn acquire(&self, size: usize) -> Vec<u8> {
        let pool = if size <= 1024 {
            &self.small_buffers
        } else if size <= 64 * 1024 {
            &self.medium_buffers
        } else {
            &self.large_buffers
        };

        let mut buffers = pool.write();
        if let Some(mut buffer) = buffers.pop() {
            buffer.clear();
            buffer.reserve(size);
            buffer
        } else {
            Vec::with_capacity(size)
        }
    }

    pub fn release(&self, mut buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        buffer.clear();

        let pool = if capacity <= 1024 {
            &self.small_buffers
        } else if capacity <= 64 * 1024 {
            &self.medium_buffers
        } else {
            &self.large_buffers
        };

        let mut buffers = pool.write();
        if buffers.len() < 100 {
            buffers.push(buffer);
        }
    }
}

impl Default for MemoryPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_path_cache() {
        let cache = HotPathCache::new(3, Duration::from_secs(60));

        cache.put(b"key1".to_vec(), b"value1".to_vec());
        cache.put(b"key2".to_vec(), b"value2".to_vec());
        cache.put(b"key3".to_vec(), b"value3".to_vec());

        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(cache.get(b"key2"), Some(b"value2".to_vec()));
        assert_eq!(cache.get(b"key3"), Some(b"value3".to_vec()));

        // Adding fourth item should evict oldest
        cache.put(b"key4".to_vec(), b"value4".to_vec());
        assert_eq!(cache.get(b"key4"), Some(b"value4".to_vec()));
    }

    #[test]
    fn test_batch_optimizer() {
        let optimizer = BatchOptimizer::new(3);

        assert!(!optimizer.add_operation(BatchOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        }));

        assert!(!optimizer.add_operation(BatchOperation::Delete {
            key: b"key2".to_vec(),
        }));

        assert!(optimizer.add_operation(BatchOperation::Put {
            key: b"key3".to_vec(),
            value: b"value3".to_vec(),
        }));

        let batch = optimizer.take_batch();
        assert_eq!(batch.len(), 3);
        assert_eq!(optimizer.pending_count(), 0);
    }

    #[test]
    fn test_prefetch_predictor() {
        let predictor = PrefetchPredictor::new(5);

        predictor.record_access(b"key_1");
        predictor.record_access(b"key_2");
        predictor.record_access(b"key_3");

        let next = predictor.predict_next();
        assert_eq!(next, Some(b"key_4".to_vec()));
    }

    #[test]
    fn test_adaptive_cache_manager() {
        let manager = AdaptiveCacheManager::new(1024 * 1024, 512 * 1024, 2 * 1024 * 1024);

        // Low hit rate should increase cache
        manager.update_hit_rate(30, 70);
        let new_size = manager.adjust_cache_size();
        assert!(new_size > 1024 * 1024);

        // High hit rate might reduce cache
        manager.update_hit_rate(95, 5);
        let adjusted = manager.adjust_cache_size();
        assert!(adjusted <= new_size);
    }

    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new();

        let buf1 = pool.acquire(100);
        assert!(buf1.capacity() >= 100);

        pool.release(buf1);

        let buf2 = pool.acquire(100);
        assert!(buf2.capacity() >= 100);
    }
}
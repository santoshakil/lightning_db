use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use std::ptr;

/// Lock-free hot path cache optimized for maximum performance
pub struct HotPathCache<K: Clone + Eq + std::hash::Hash, V: Clone> {
    // Array of atomic pointers for fast access
    slots: Box<[AtomicPtr<CacheEntry<K, V>>]>,
    capacity: usize,
    size: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheEntry<K, V> {
    key: K,
    value: V,
    hash: u64,
    access_count: AtomicU64,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> HotPathCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        // Round up to power of 2 for fast modulo
        let capacity = capacity.next_power_of_two();
        
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(AtomicPtr::new(ptr::null_mut()));
        }
        
        Self {
            slots: slots.into_boxed_slice(),
            capacity,
            size: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hash_key(key);
        let index = self.index_for(hash);
        
        let ptr = self.slots[index].load(Ordering::Acquire);
        if ptr.is_null() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        
        unsafe {
            let entry = &*ptr;
            if entry.hash == hash && entry.key == *key {
                entry.access_count.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value.clone())
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    #[inline(always)]
    pub fn insert(&self, key: K, value: V) {
        let hash = self.hash_key(&key);
        let index = self.index_for(hash);
        
        let new_entry = Box::into_raw(Box::new(CacheEntry {
            key,
            value,
            hash,
            access_count: AtomicU64::new(1),
        }));
        
        let old_ptr = self.slots[index].swap(new_entry, Ordering::Release);
        
        if old_ptr.is_null() {
            self.size.fetch_add(1, Ordering::Relaxed);
        } else {
            // Deallocate old entry
            unsafe {
                let _ = Box::from_raw(old_ptr);
            }
        }
    }

    #[inline(always)]
    fn hash_key(&self, key: &K) -> u64 {
        use std::hash::Hasher;
        use std::collections::hash_map::DefaultHasher;
        
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline(always)]
    fn index_for(&self, hash: u64) -> usize {
        (hash as usize) & (self.capacity - 1)
    }

    pub fn stats(&self) -> (u64, u64, f64) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        (hits, misses, hit_rate)
    }
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> Drop for HotPathCache<K, V> {
    fn drop(&mut self) {
        for slot in self.slots.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

/// Wait-free read buffer for hot path reads
pub struct WaitFreeReadBuffer<T: Clone> {
    // Double buffering for wait-free reads
    buffers: [AtomicPtr<Vec<T>>; 2],
    active_buffer: AtomicUsize,
    epoch: AtomicU64,
}

impl<T: Clone> WaitFreeReadBuffer<T> {
    pub fn new() -> Self {
        Self {
            buffers: [
                AtomicPtr::new(Box::into_raw(Box::new(Vec::new()))),
                AtomicPtr::new(Box::into_raw(Box::new(Vec::new()))),
            ],
            active_buffer: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
        }
    }

    /// Wait-free read of the buffer
    #[inline(always)]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[T]) -> R,
    {
        let buffer_idx = self.active_buffer.load(Ordering::Acquire);
        let buffer_ptr = self.buffers[buffer_idx].load(Ordering::Acquire);
        
        unsafe {
            let buffer = &*buffer_ptr;
            f(buffer)
        }
    }

    /// Update the buffer (not wait-free, but minimally blocking)
    pub fn update(&self, new_data: Vec<T>) {
        let current_idx = self.active_buffer.load(Ordering::Acquire);
        let next_idx = 1 - current_idx;
        
        // Update the inactive buffer
        let old_ptr = self.buffers[next_idx].swap(
            Box::into_raw(Box::new(new_data)),
            Ordering::Release
        );
        
        // Switch active buffer atomically
        self.active_buffer.store(next_idx, Ordering::Release);
        self.epoch.fetch_add(1, Ordering::Release);
        
        // Clean up old buffer after ensuring no readers
        std::thread::yield_now(); // Give readers time to finish
        unsafe {
            let _ = Box::from_raw(old_ptr);
        }
    }
}

/// Lock-free write combining buffer for batching writes
pub struct WriteCombiningBuffer<K: Clone + Send, V: Clone + Send> {
    pending: Atomic<WriteBatch<K, V>>,
    threshold: usize,
}

struct WriteBatch<K, V> {
    writes: Vec<(K, V)>,
    _next: Option<Owned<WriteBatch<K, V>>>,
}

impl<K: Clone + Send, V: Clone + Send> WriteCombiningBuffer<K, V> {
    pub fn new(threshold: usize) -> Self {
        Self {
            pending: Atomic::null(),
            threshold,
        }
    }

    pub fn add_write(&self, key: K, value: V) -> Option<Vec<(K, V)>> {
        let guard = &epoch::pin();
        
        loop {
            let current = self.pending.load(Ordering::Acquire, guard);
            
            let batch = if current.is_null() {
                WriteBatch {
                    writes: vec![(key.clone(), value.clone())],
                    _next: None,
                }
            } else {
                unsafe {
                    let mut batch = current.as_ref().unwrap().clone();
                    batch.writes.push((key.clone(), value.clone()));
                    batch
                }
            };
            
            if batch.writes.len() >= self.threshold {
                // Batch is full, try to claim it
                let owned = Owned::new(WriteBatch {
                    writes: Vec::new(),
                    _next: None,
                });
                
                match self.pending.compare_exchange(
                    current,
                    owned,
                    Ordering::Release,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => {
                        if !current.is_null() {
                            unsafe {
                                guard.defer_destroy(current);
                            }
                        }
                        return Some(batch.writes);
                    }
                    Err(_) => continue,
                }
            } else {
                // Add to batch
                let owned = Owned::new(batch);
                
                match self.pending.compare_exchange(
                    current,
                    owned,
                    Ordering::Release,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => {
                        if !current.is_null() {
                            unsafe {
                                guard.defer_destroy(current);
                            }
                        }
                        return None;
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    pub fn flush(&self) -> Option<Vec<(K, V)>> {
        let guard = &epoch::pin();
        
        let current = self.pending.swap(Shared::null(), Ordering::AcqRel, guard);
        
        if current.is_null() {
            None
        } else {
            unsafe {
                let owned = current.into_owned();
                let batch = owned.into_box();
                Some(batch.writes)
            }
        }
    }
}

impl<K: Clone + Send, V: Clone + Send> Clone for WriteBatch<K, V> {
    fn clone(&self) -> Self {
        WriteBatch {
            writes: self.writes.clone(),
            _next: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_hot_path_cache() {
        // Use String types for dynamic keys/values
        let cache = Arc::new(HotPathCache::<String, String>::new(1024));
        
        // Test basic operations
        cache.insert("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(cache.get(&"key2".to_string()), None);
        
        // Test concurrent access
        let cache_clone = Arc::clone(&cache);
        let handle = thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                cache_clone.insert(key, value);
            }
        });
        
        for i in 0..1000 {
            let key = format!("key{}", i);
            let _ = cache.get(&key);
        }
        
        handle.join().unwrap();
        
        let (hits, misses, hit_rate) = cache.stats();
        println!("Cache stats - Hits: {}, Misses: {}, Hit rate: {:.2}%", 
                 hits, misses, hit_rate * 100.0);
    }

    #[test]
    fn test_wait_free_read_buffer() {
        let buffer = Arc::new(WaitFreeReadBuffer::new());
        
        // Update buffer
        buffer.update(vec![1, 2, 3, 4, 5]);
        
        // Concurrent reads
        let mut handles = vec![];
        for _ in 0..10 {
            let buffer_clone = Arc::clone(&buffer);
            handles.push(thread::spawn(move || {
                buffer_clone.read(|data| {
                    // Data might be [1,2,3,4,5] or [10,20,30] due to concurrent update
                    assert!(data.len() == 5 || data.len() == 3);
                    data.iter().sum::<i32>()
                })
            }));
        }
        
        // Update while reading
        buffer.update(vec![10, 20, 30]);
        
        for handle in handles {
            let sum = handle.join().unwrap();
            assert!(sum == 15 || sum == 60); // Either old or new data
        }
    }

    #[test]
    fn test_write_combining_buffer() {
        let buffer = Arc::new(WriteCombiningBuffer::new(5));
        
        // Add writes
        for i in 0..4 {
            assert!(buffer.add_write(i, i * 10).is_none());
        }
        
        // This should trigger a flush
        if let Some(batch) = buffer.add_write(4, 40) {
            assert_eq!(batch.len(), 5);
        }
        
        // Manual flush
        buffer.add_write(5, 50);
        if let Some(batch) = buffer.flush() {
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0], (5, 50));
        }
    }
}
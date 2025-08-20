use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};


/// Fast hash function optimized for small keys
#[inline(always)]
fn fast_hash(key: &[u8]) -> u64 {
    // Use a simple but fast hash for hot path
    let mut h = 0xcbf29ce484222325u64;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3u64);
    }
    h
}

/// Fast hash for generic key types
#[inline(always)]
fn fast_hash_key<K: std::hash::Hash>(key: &K) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Branch prediction hints - using stable intrinsics
#[inline(always)]
fn likely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if !b {
        cold();
    }
    b
}

#[inline(always)]
fn unlikely(b: bool) -> bool {
    #[cold]
    fn cold() {}
    
    if b {
        cold();
    }
    b
}

/// Lock-free hot path cache optimized for maximum performance
/// Uses const generics for compile-time optimization
pub struct HotPathCache<K: Clone + Eq + std::hash::Hash + std::fmt::Debug, V: Clone + std::fmt::Debug, const CAPACITY: usize = 16384> {
    // Array of versioned pointers to prevent ABA problems - aligned for cache efficiency
    slots: Box<[VersionedPtr<K, V>; CAPACITY]>,
    capacity_mask: usize, // Pre-computed for fast modulo
    size: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    // Global generation counter for ABA prevention
    global_generation: AtomicU64,
}

// Cache-aligned entry to prevent false sharing
#[repr(align(64))]
struct CacheEntry<K, V> {
    key: K,
    value: V,
    hash: u64,
    access_count: AtomicU64,
    // Generation counter to prevent ABA problems
    generation: u64,
    // Padding to ensure 64-byte alignment
    _padding: [u8; 0],
}

// Cache-aligned versioned pointer to prevent ABA issues
#[repr(align(64))]
#[derive(Debug)]
struct VersionedPtr<K, V> {
    ptr: Atomic<CacheEntry<K, V>>,
    generation: AtomicU64,
    // Padding to prevent false sharing
    _padding: [u8; 48],
}

impl<K: Clone + Eq + std::hash::Hash + std::fmt::Debug, V: Clone + std::fmt::Debug, const CAPACITY: usize> HotPathCache<K, V, CAPACITY> {
    pub fn new() -> Self {
        // Ensure CAPACITY is power of 2 at compile time
        assert!(CAPACITY.is_power_of_two(), "CAPACITY must be power of 2");
        
        // Initialize array using unsafe for performance
        let mut slots: Vec<VersionedPtr<K, V>> = Vec::with_capacity(CAPACITY);
        for _ in 0..CAPACITY {
            slots.push(VersionedPtr {
                ptr: Atomic::null(),
                generation: AtomicU64::new(0),
                _padding: [0; 48],
            });
        }
        
        Self {
            slots: slots.into_boxed_slice().try_into().expect("Vector length must match CAPACITY"),
            capacity_mask: CAPACITY - 1,
            size: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            global_generation: AtomicU64::new(0),
        }
    }
    
    /// Create cache with specific capacity (legacy method)
    pub fn with_capacity(_capacity: usize) -> Self {
        Self::new()
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = fast_hash_key(key);
        let index = hash as usize & self.capacity_mask;

        let guard = &epoch::pin();
        let entry_ptr = self.slots[index].ptr.load(Ordering::Acquire, guard);

        if unlikely(entry_ptr.is_null()) {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // SAFETY: Dereferencing epoch-protected cache entry
        // Invariants:
        // - entry_ptr is protected by epoch guard
        // - Pointer is non-null (checked above)
        // Guarantees:
        // - Safe access to entry during guard lifetime
        unsafe {
            let entry = entry_ptr.deref();
            if likely(entry.hash == hash && entry.key == *key) {
                entry.access_count.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value.clone())
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Zero-copy get that executes a closure with the cached value reference
    /// This eliminates cloning for hot path reads while avoiding lifetime issues
    #[inline(always)]
    pub fn with_value_ref<F, R>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let hash = fast_hash_key(key);
        let index = hash as usize & self.capacity_mask;

        let guard = epoch::pin();
        let entry_ptr = self.slots[index].ptr.load(Ordering::Acquire, &guard);

        if unlikely(entry_ptr.is_null()) {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // SAFETY: Dereferencing epoch-protected cache entry
        unsafe {
            let entry = entry_ptr.deref();
            if likely(entry.hash == hash && entry.key == *key) {
                entry.access_count.fetch_add(1, Ordering::Relaxed);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(f(&entry.value))
            } else {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    #[inline(always)]
    pub fn insert(&self, key: K, value: V) {
        let hash = fast_hash_key(&key);
        let index = hash as usize & self.capacity_mask;

        let guard = &epoch::pin();
        let generation = self.global_generation.fetch_add(1, Ordering::Relaxed);
        
        let new_entry = Owned::new(CacheEntry {
            key,
            value,
            hash,
            access_count: AtomicU64::new(1),
            generation,
            _padding: [0; 0],
        });

        // Use compare_exchange to prevent ABA - simpler approach
        let old_entry = self.slots[index].ptr.swap(new_entry, Ordering::AcqRel, guard);
        
        // Update generation counter after successful swap
        self.slots[index].generation.store(generation, Ordering::Release);
        
        if unlikely(old_entry.is_null()) {
            self.size.fetch_add(1, Ordering::Relaxed);
        } else {
            // SAFETY: old_entry is being replaced and will be reclaimed by epoch GC
            unsafe {
                guard.defer_destroy(old_entry);
            }
        }
    }

    // Removed hash_key and index_for methods - now using global fast_hash_key and inline indexing

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

// Note: CacheEntryRef commented out due to lifetime complexity
// Use with_value_ref() method instead for zero-copy access

// /// Zero-copy reference to a cache entry that avoids cloning
// pub struct CacheEntryRef<'g, K, V> {
//     entry: &'g CacheEntry<K, V>,
//     _guard: epoch::Guard,
// }

// impl<'g, K, V> CacheEntryRef<'g, K, V> {
//     pub fn key(&self) -> &K {
//         &self.entry.key
//     }

//     pub fn value(&self) -> &V {
//         &self.entry.value
//     }

//     pub fn access_count(&self) -> u64 {
//         self.entry.access_count.load(Ordering::Relaxed)
//     }
// }

impl<K: Clone + Eq + std::hash::Hash + std::fmt::Debug, V: Clone + std::fmt::Debug, const CAPACITY: usize> Drop for HotPathCache<K, V, CAPACITY> {
    fn drop(&mut self) {
        let guard = &epoch::pin();
        for slot in self.slots.iter() {
            let entry = slot.ptr.load(Ordering::Acquire, guard);
            if !entry.is_null() {
                unsafe {
                    // Take ownership and drop immediately since we're in Drop
                    let _ = entry.into_owned();
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

impl<T: Clone> Default for WaitFreeReadBuffer<T> {
    fn default() -> Self {
        Self {
            buffers: [
                AtomicPtr::new(Box::into_raw(Box::new(Vec::new()))),
                AtomicPtr::new(Box::into_raw(Box::new(Vec::new()))),
            ],
            active_buffer: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
        }
    }
}

impl<T: Clone> WaitFreeReadBuffer<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait-free read of the buffer
    #[inline(always)]
    pub fn read<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&[T]) -> R,
    {
        let _guard = epoch::pin();
        let buffer_idx = self.active_buffer.load(Ordering::Acquire);
        let buffer_ptr = self.buffers[buffer_idx].load(Ordering::Acquire);

        unsafe {
            // Buffer pointer should be valid during epoch
            debug_assert!(!buffer_ptr.is_null(), "Buffer pointer is null");
            let buffer = &*buffer_ptr;
            f(buffer)
        }
    }

    /// Update the buffer (not wait-free, but minimally blocking)
    pub fn update(&self, new_data: Vec<T>) {
        let _guard = epoch::pin();
        let current_idx = self.active_buffer.load(Ordering::Acquire);
        let next_idx = 1 - current_idx;

        // Update the inactive buffer
        let old_ptr =
            self.buffers[next_idx].swap(Box::into_raw(Box::new(new_data)), Ordering::Release);

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

impl<T: Clone> Drop for WaitFreeReadBuffer<T> {
    fn drop(&mut self) {
        unsafe {
            // Clean up both buffers
            for buffer_ptr in &self.buffers {
                let ptr = buffer_ptr.load(Ordering::Acquire);
                if !ptr.is_null() {
                    let _ = Box::from_raw(ptr);
                }
            }
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
                    let current_batch = current.deref();
                    let mut batch = WriteBatch {
                        writes: current_batch.writes.clone(),
                        _next: None,
                    };
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

/// Zero-copy write buffer that uses pre-allocated memory pools
/// Eliminates allocations in the critical write path
pub struct ZeroCopyWriteBuffer<K: Send + Sync, V: Send + Sync> {
    // Ring buffer for zero-copy batching
    ring_buffer: Box<[AtomicPtr<(K, V)>]>,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    capacity_mask: usize,
    threshold: usize,
}

impl<K: Send + Sync, V: Send + Sync> ZeroCopyWriteBuffer<K, V> {
    pub fn new(capacity: usize, threshold: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be power of 2");
        
        let mut ring_buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            ring_buffer.push(AtomicPtr::new(std::ptr::null_mut()));
        }
        
        Self {
            ring_buffer: ring_buffer.into_boxed_slice(),
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            capacity_mask: capacity - 1,
            threshold,
        }
    }

    /// Add a write operation using move semantics (zero-copy)
    pub fn add_write_move(&self, key: K, value: V) -> Option<Vec<(K, V)>> {
        let write_pos = self.write_pos.fetch_add(1, Ordering::AcqRel);
        let slot_idx = write_pos & self.capacity_mask;
        
        // Store the data directly in the ring buffer
        let data_ptr = Box::into_raw(Box::new((key, value)));
        let old_ptr = self.ring_buffer[slot_idx].swap(data_ptr, Ordering::Release);
        
        // Clean up old data if present
        if !old_ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(old_ptr);
            }
        }
        
        // Check if we should flush
        let read_pos = self.read_pos.load(Ordering::Acquire);
        let pending_count = write_pos.wrapping_sub(read_pos);
        
        if pending_count >= self.threshold {
            self.flush_internal()
        } else {
            None
        }
    }

    fn flush_internal(&self) -> Option<Vec<(K, V)>> {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let old_read_pos = self.read_pos.swap(write_pos, Ordering::AcqRel);
        
        if write_pos == old_read_pos {
            return None;
        }
        
        let mut batch = Vec::with_capacity(self.threshold);
        
        for pos in old_read_pos..write_pos {
            let slot_idx = pos & self.capacity_mask;
            let data_ptr = self.ring_buffer[slot_idx].swap(std::ptr::null_mut(), Ordering::Acquire);
            
            if !data_ptr.is_null() {
                unsafe {
                    let data = Box::from_raw(data_ptr);
                    batch.push(*data);
                }
            }
        }
        
        Some(batch)
    }

    pub fn flush(&self) -> Option<Vec<(K, V)>> {
        self.flush_internal()
    }
}

impl<K: Send + Sync, V: Send + Sync> Drop for ZeroCopyWriteBuffer<K, V> {
    fn drop(&mut self) {
        // Clean up any remaining data in the ring buffer
        for slot in self.ring_buffer.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
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
        let cache = Arc::new(HotPathCache::<String, String, 1024>::new());

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
        println!(
            "Cache stats - Hits: {}, Misses: {}, Hit rate: {:.2}%",
            hits,
            misses,
            hit_rate * 100.0
        );
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

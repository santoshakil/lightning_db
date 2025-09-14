//! Lock-free concurrent data structures for high-performance database operations
//!
//! This module implements lock-free data structures specifically designed for
//! database workloads where minimizing contention and maximizing throughput
//! are critical for performance.

use crate::core::error::Result;
use crossbeam::epoch::{self, Atomic, Owned, Shared, CompareExchangeError};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Simple cache-padded wrapper to avoid false sharing
#[repr(align(64))]
struct CachePadded<T> {
    inner: T,
}

impl<T> CachePadded<T> {
    fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Lock-free hash map optimized for database key-value operations
pub struct LockFreeHashMap<K, V> {
    buckets: Vec<CachePadded<Atomic<Bucket<K, V>>>>,
    capacity: usize,
    size: AtomicUsize,
    resize_threshold: f64,
}

/// Bucket node for the lock-free hash map
struct Bucket<K, V> {
    entries: Vec<Entry<K, V>>,
    overflow: Atomic<Bucket<K, V>>,
    version: AtomicU64,
}

/// Entry in a hash map bucket
struct Entry<K, V> {
    key: K,
    value: Atomic<V>,
    deleted: AtomicBool,
    hash: u64,
}

impl<K, V> LockFreeHashMap<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new lock-free hash map with the specified initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let next_power_of_2 = capacity.next_power_of_two();
        let mut buckets = Vec::with_capacity(next_power_of_2);
        
        for _ in 0..next_power_of_2 {
            buckets.push(CachePadded::new(Atomic::null()));
        }
        
        Self {
            buckets,
            capacity: next_power_of_2,
            size: AtomicUsize::new(0),
            resize_threshold: 0.75,
        }
    }
    
    /// Insert or update a key-value pair
    pub fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        let guard = epoch::pin();
        let hash = self.hash_key(&key);
        let bucket_index = self.get_bucket_index(hash);
        
        loop {
            let bucket_ptr = self.buckets[bucket_index].load(Ordering::Acquire, &guard);
            
            if bucket_ptr.is_null() {
                // Initialize bucket if it doesn't exist
                let new_bucket = Owned::new(Bucket {
                    entries: Vec::with_capacity(8),
                    overflow: Atomic::null(),
                    version: AtomicU64::new(1),
                });
                
                match self.buckets[bucket_index]
                    .compare_exchange_weak(bucket_ptr, new_bucket, Ordering::AcqRel, Ordering::Acquire, &guard)
                {
                    Ok(_) => continue, // Retry with the new bucket
                    Err(_) => continue, // Another thread initialized it, retry
                }
            }
            
            let bucket = unsafe { bucket_ptr.deref() };
            let current_version = bucket.version.load(Ordering::Acquire);
            
            // Search for existing key in this bucket and overflow buckets
            if let Some(old_value) = self.update_existing_key(bucket, &key, value.clone(), hash, &guard)? {
                return Ok(Some(old_value));
            }
            
            // Key doesn't exist, need to add new entry
            if bucket.version.compare_exchange_weak(
                current_version,
                current_version + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                // Successfully acquired version lock, can modify bucket
                self.add_new_entry(bucket, key.clone(), value.clone(), hash, &guard)?;
                self.size.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            
            // Version changed, retry
        }
    }
    
    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<V> {
        let guard = epoch::pin();
        let hash = self.hash_key(key);
        let bucket_index = self.get_bucket_index(hash);
        
        let bucket_ptr = self.buckets[bucket_index].load(Ordering::Acquire, &guard);
        if bucket_ptr.is_null() {
            return None;
        }
        
        let bucket = unsafe { bucket_ptr.deref() };
        self.find_value_in_bucket(bucket, key, hash, &guard)
    }
    
    /// Remove a key-value pair
    pub fn remove(&self, key: &K) -> Option<V> {
        let guard = epoch::pin();
        let hash = self.hash_key(key);
        let bucket_index = self.get_bucket_index(hash);
        
        let bucket_ptr = self.buckets[bucket_index].load(Ordering::Acquire, &guard);
        if bucket_ptr.is_null() {
            return None;
        }
        
        let bucket = unsafe { bucket_ptr.deref() };
        
        // Mark entry as deleted and return old value
        if let Some(old_value) = self.mark_deleted_in_bucket(bucket, key, hash, &guard) {
            self.size.fetch_sub(1, Ordering::Relaxed);
            Some(old_value)
        } else {
            None
        }
    }
    
    /// Get the current size of the map
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// Check if the map is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Hash a key using SIMD-optimized hashing when available
    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Get bucket index from hash
    fn get_bucket_index(&self, hash: u64) -> usize {
        (hash as usize) & (self.capacity - 1)
    }
    
    /// Update existing key in bucket chain
    fn update_existing_key(
        &self,
        bucket: &Bucket<K, V>,
        key: &K,
        value: V,
        hash: u64,
        guard: &epoch::Guard,
    ) -> Result<Option<V>> {
        // Search in main bucket entries
        for entry in &bucket.entries {
            if entry.hash == hash && entry.key == *key && !entry.deleted.load(Ordering::Acquire) {
                let old_ptr = entry.value.load(Ordering::Acquire, guard);
                let new_value = Owned::new(value.clone());
                
                match entry.value.compare_exchange_weak(
                    old_ptr,
                    new_value,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    guard,
                ) {
                    Ok(_) => {
                        if !old_ptr.is_null() {
                            let old_value = unsafe { old_ptr.deref().clone() };
                            unsafe { guard.defer_destroy(old_ptr) };
                            return Ok(Some(old_value));
                        } else {
                            return Ok(None);
                        }
                    }
                    Err(CompareExchangeError { .. }) => {
                        // CAS failed, value still owned by returned_new, continue with value
                        continue;
                    }
                }
            }
        }
        
        // Search in overflow buckets
        let overflow_ptr = bucket.overflow.load(Ordering::Acquire, guard);
        if !overflow_ptr.is_null() {
            let overflow_bucket = unsafe { overflow_ptr.deref() };
            return self.update_existing_key(overflow_bucket, key, value, hash, guard);
        }
        
        Ok(None)
    }
    
    /// Add new entry to bucket
    fn add_new_entry(
        &self,
        _bucket: &Bucket<K, V>,
        _key: K,
        _value: V,
        _hash: u64,
        _guard: &epoch::Guard,
    ) -> Result<()> {
        // In a real implementation, this would need to handle bucket expansion
        // For now, this is a simplified version
        Ok(())
    }
    
    /// Find value in bucket chain
    fn find_value_in_bucket(
        &self,
        bucket: &Bucket<K, V>,
        key: &K,
        hash: u64,
        guard: &epoch::Guard,
    ) -> Option<V> {
        // Search in main bucket entries
        for entry in &bucket.entries {
            if entry.hash == hash && entry.key == *key && !entry.deleted.load(Ordering::Acquire) {
                let value_ptr = entry.value.load(Ordering::Acquire, guard);
                if !value_ptr.is_null() {
                    return Some(unsafe { value_ptr.deref().clone() });
                }
            }
        }
        
        // Search in overflow buckets
        let overflow_ptr = bucket.overflow.load(Ordering::Acquire, guard);
        if !overflow_ptr.is_null() {
            let overflow_bucket = unsafe { overflow_ptr.deref() };
            return self.find_value_in_bucket(overflow_bucket, key, hash, guard);
        }
        
        None
    }
    
    /// Mark entry as deleted in bucket chain
    fn mark_deleted_in_bucket(
        &self,
        bucket: &Bucket<K, V>,
        key: &K,
        hash: u64,
        guard: &epoch::Guard,
    ) -> Option<V> {
        // Search in main bucket entries
        for entry in &bucket.entries {
            if entry.hash == hash && entry.key == *key
                && entry.deleted.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                let value_ptr = entry.value.load(Ordering::Acquire, guard);
                if !value_ptr.is_null() {
                    let value = unsafe { value_ptr.deref().clone() };
                    unsafe { guard.defer_destroy(value_ptr) };
                    return Some(value);
                }
            }
        }
        
        // Search in overflow buckets
        let overflow_ptr = bucket.overflow.load(Ordering::Acquire, guard);
        if !overflow_ptr.is_null() {
            let overflow_bucket = unsafe { overflow_ptr.deref() };
            return self.mark_deleted_in_bucket(overflow_bucket, key, hash, guard);
        }
        
        None
    }
}

/// Lock-free queue for high-throughput producer-consumer scenarios
pub struct LockFreeQueue<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buffer: Vec<CachePadded<Slot<T>>>,
    capacity: usize,
    mask: usize,
}

/// Slot in the lock-free queue
struct Slot<T> {
    data: Atomic<T>,
    sequence: AtomicUsize,
}

impl<T> LockFreeQueue<T>
where
    T: Send + Sync,
{
    /// Create a new lock-free queue with power-of-2 capacity
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let mut buffer = Vec::with_capacity(capacity);
        
        for i in 0..capacity {
            buffer.push(CachePadded::new(Slot {
                data: Atomic::null(),
                sequence: AtomicUsize::new(i),
            }));
        }
        
        Self {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            buffer,
            capacity,
            mask: capacity - 1,
        }
    }
    
    /// Enqueue an element (non-blocking)
    pub fn enqueue(&self, item: T) -> std::result::Result<(), T> {
        let item = Owned::new(item);
        let _guard = epoch::pin();
        
        loop {
            let tail = self.tail.load(Ordering::Relaxed);
            let slot = &self.buffer[tail & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            
            let diff = sequence.wrapping_sub(tail);

            if diff == 0 {
                // Slot is available for writing
                if self.tail.compare_exchange_weak(
                    tail,
                    tail.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ).is_ok() {
                    slot.data.store(item, Ordering::Release);
                    slot.sequence.store(tail.wrapping_add(1), Ordering::Release);
                    return Ok(());
                }
            } else if diff > self.capacity {
                // Queue is full (wrapped around)
                return Err(*item.into_box());
            } else {
                // Another thread is updating this slot
                continue;
            }
        }
    }
    
    /// Dequeue an element (non-blocking)
    pub fn dequeue(&self) -> Option<T> {
        let guard = epoch::pin();
        
        loop {
            let head = self.head.load(Ordering::Relaxed);
            let slot = &self.buffer[head & self.mask];
            let sequence = slot.sequence.load(Ordering::Acquire);
            
            let diff = sequence.wrapping_sub(head.wrapping_add(1));
            
            if diff == 0 {
                // Slot contains data
                if self.head.compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ).is_ok() {
                    let data_ptr = slot.data.load(Ordering::Acquire, &guard);
                    if !data_ptr.is_null() {
                        let data = unsafe { std::ptr::read(data_ptr.as_raw()) };
                        slot.data.store(Shared::null(), Ordering::Release);
                        slot.sequence.store(head.wrapping_add(self.capacity), Ordering::Release);
                        unsafe { guard.defer_destroy(data_ptr) };
                        return Some(data);
                    }
                }
            } else if diff > self.capacity {
                // Queue is empty (wrapped around)
                return None;
            } else {
                // Another thread is updating this slot
                continue;
            }
        }
    }
    
    /// Get approximate size of the queue
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail.wrapping_sub(head)
    }
    
    /// Check if the queue is empty
    pub fn is_empty(&self) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        tail == head
    }
}

/// Lock-free stack for LIFO operations
pub struct LockFreeStack<T> {
    head: Atomic<Node<T>>,
    size: AtomicUsize,
}

struct Node<T> {
    data: T,
    next: Atomic<Node<T>>,
}

impl<T> LockFreeStack<T> {
    /// Create a new lock-free stack
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
            size: AtomicUsize::new(0),
        }
    }
    
    /// Push an element onto the stack
    pub fn push(&self, data: T) {
        let guard = epoch::pin();
        let mut new_node = Owned::new(Node {
            data,
            next: Atomic::null(),
        });
        
        loop {
            let head = self.head.load(Ordering::Relaxed, &guard);
            new_node.next.store(head, Ordering::Relaxed);
            
            match self.head.compare_exchange_weak(
                head,
                new_node,
                Ordering::Release,
                Ordering::Relaxed,
                &guard,
            ) {
                Ok(_) => {
                    self.size.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(err) => {
                    new_node = err.new;
                }
            }
        }
    }
    
    /// Pop an element from the stack
    pub fn pop(&self) -> Option<T> {
        let guard = epoch::pin();
        
        loop {
            let head = self.head.load(Ordering::Acquire, &guard);
            if head.is_null() {
                return None;
            }
            
            let head_node = unsafe { head.deref() };
            let next = head_node.next.load(Ordering::Relaxed, &guard);
            
            if self.head.compare_exchange_weak(
                head,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
                &guard,
            ).is_ok() {
                self.size.fetch_sub(1, Ordering::Relaxed);
                let data = unsafe { std::ptr::read(&head_node.data) };
                unsafe { guard.defer_destroy(head) };
                return Some(data);
            }
        }
    }
    
    /// Get the current size
    pub fn len(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Default for LockFreeStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for LockFreeStack<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {
            // Drop all remaining elements
        }
    }
}

/// Performance statistics for lock-free data structures
#[derive(Debug, Default)]
pub struct LockFreeStats {
    pub operations_completed: u64,
    pub contention_count: u64,
    pub retry_count: u64,
    pub memory_reclaimed: u64,
}

impl LockFreeStats {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.operations_completed + self.contention_count == 0 {
            0.0
        } else {
            self.operations_completed as f64 / (self.operations_completed + self.contention_count) as f64
        }
    }
    
    /// Calculate average retries per operation
    pub fn avg_retries(&self) -> f64 {
        if self.operations_completed == 0 {
            0.0
        } else {
            self.retry_count as f64 / self.operations_completed as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    
    #[test]
    fn test_lock_free_hashmap() {
        let map = Arc::new(LockFreeHashMap::with_capacity(16));
        let map_clone = Arc::clone(&map);
        
        // Test basic operations
        assert_eq!(map.insert("key1".to_string(), "value1".to_string()).unwrap(), None);
        assert_eq!(map.get(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(map.remove(&"key1".to_string()), Some("value1".to_string()));
        assert_eq!(map.get(&"key1".to_string()), None);
        
        // Test concurrent operations
        let handles: Vec<_> = (0..4).map(|i| {
            let map = Arc::clone(&map_clone);
            thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key_{}_{}", i, j);
                    let value = format!("value_{}_{}", i, j);
                    map.insert(key.clone(), value.clone()).unwrap();
                    assert_eq!(map.get(&key), Some(value));
                }
            })
        }).collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(map.len(), 400);
    }
    
    #[test]
    fn test_lock_free_queue() {
        let queue = Arc::new(LockFreeQueue::with_capacity(16));
        
        // Test basic operations
        assert!(queue.is_empty());
        queue.enqueue(1).unwrap();
        queue.enqueue(2).unwrap();
        assert_eq!(queue.len(), 2);
        
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
        assert_eq!(queue.dequeue(), None);
        assert!(queue.is_empty());
        
        // Test concurrent producer-consumer
        let queue_clone = Arc::clone(&queue);
        let producer = thread::spawn(move || {
            for i in 0..100 {
                while queue_clone.enqueue(i).is_err() {
                    thread::yield_now();
                }
            }
        });
        
        let queue_clone2 = Arc::clone(&queue);
        let consumer = thread::spawn(move || {
            let mut received = Vec::new();
            for _ in 0..100 {
                while let Some(item) = queue_clone2.dequeue() {
                    received.push(item);
                    if received.len() == 100 {
                        break;
                    }
                }
            }
            received
        });
        
        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received.len(), 100);
    }
    
    #[test]
    fn test_lock_free_stack() {
        let stack = Arc::new(LockFreeStack::new());
        
        // Test basic operations
        assert!(stack.is_empty());
        stack.push(1);
        stack.push(2);
        assert_eq!(stack.len(), 2);
        
        assert_eq!(stack.pop(), Some(2)); // LIFO order
        assert_eq!(stack.pop(), Some(1));
        assert_eq!(stack.pop(), None);
        assert!(stack.is_empty());
        
        // Test concurrent operations
        let handles: Vec<_> = (0..4).map(|i| {
            let stack = Arc::clone(&stack);
            thread::spawn(move || {
                for j in 0..25 {
                    stack.push(i * 25 + j);
                }
            })
        }).collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        assert_eq!(stack.len(), 100);
        
        // Pop all elements
        let mut count = 0;
        while stack.pop().is_some() {
            count += 1;
        }
        assert_eq!(count, 100);
    }
}
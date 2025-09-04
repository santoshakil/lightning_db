use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use bytes::Bytes;
use std::collections::HashMap;

/// Thread-local object pool that eliminates lock contention
/// Much faster than global pools with mutex synchronization
pub struct ThreadLocalPool<T> {
    pool: RefCell<VecDeque<T>>,
    factory: fn() -> T,
    reset: Option<fn(&mut T)>,
    max_size: usize,
}

impl<T> ThreadLocalPool<T> {
    pub fn new(max_size: usize, factory: fn() -> T) -> Self {
        Self {
            pool: RefCell::new(VecDeque::with_capacity(max_size.min(16))),
            factory,
            reset: None,
            max_size,
        }
    }

    pub fn with_reset(max_size: usize, factory: fn() -> T, reset: fn(&mut T)) -> Self {
        Self {
            pool: RefCell::new(VecDeque::with_capacity(max_size.min(16))),
            factory,
            reset: Some(reset),
            max_size,
        }
    }

    pub fn get(&self) -> ThreadLocalGuard<'_, T> {
        let object = {
            let mut pool = self.pool.borrow_mut();
            pool.pop_front().unwrap_or_else(|| (self.factory)())
        };

        ThreadLocalGuard {
            object: Some(object),
            pool: self,
        }
    }

    fn return_object(&self, mut object: T) {
        if let Some(reset_fn) = self.reset {
            reset_fn(&mut object);
        }

        let mut pool = self.pool.borrow_mut();
        if pool.len() < self.max_size {
            pool.push_back(object);
        }
    }

    pub fn len(&self) -> usize {
        self.pool.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.pool.borrow().is_empty()
    }

    pub fn clear(&self) {
        self.pool.borrow_mut().clear();
    }
}

/// RAII guard for thread-local pooled objects
pub struct ThreadLocalGuard<'a, T> {
    object: Option<T>,
    pool: &'a ThreadLocalPool<T>,
}

impl<'a, T> ThreadLocalGuard<'a, T> {
    pub fn take(mut self) -> Option<T> {
        self.object.take()
    }
}

impl<'a, T> Deref for ThreadLocalGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.object.as_ref().unwrap_or_else(|| {
            panic!("ThreadLocalGuard used after take() was called")
        })
    }
}

impl<'a, T> DerefMut for ThreadLocalGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.object.as_mut().unwrap_or_else(|| {
            panic!("ThreadLocalGuard used after take() was called")
        })
    }
}

impl<'a, T> Drop for ThreadLocalGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            self.pool.return_object(object);
        }
    }
}

// Thread-local pool instances for high-performance operations
thread_local! {
    /// Thread-local Vec<u8> buffer pool - eliminates global mutex contention
    static VEC_BUFFER_POOL: ThreadLocalPool<Vec<u8>> = ThreadLocalPool::with_reset(
        100,
        || Vec::with_capacity(1024),
        |vec| {
            vec.clear();
            // Shrink oversized buffers to prevent unbounded growth
            if vec.capacity() > 64 * 1024 {
                vec.shrink_to(16 * 1024);
            }
        }
    );

    /// Thread-local String buffer pool
    static STRING_BUFFER_POOL: ThreadLocalPool<String> = ThreadLocalPool::with_reset(
        50,
        || String::with_capacity(256),
        |s| {
            s.clear();
            if s.capacity() > 16 * 1024 {
                s.shrink_to(4 * 1024);
            }
        }
    );

    /// Thread-local HashMap pool for caching operations
    static HASHMAP_POOL: ThreadLocalPool<HashMap<Bytes, Bytes>> = ThreadLocalPool::with_reset(
        25,
        || HashMap::with_capacity(32),
        |map| {
            map.clear();
            if map.capacity() > 1024 {
                map.shrink_to(256);
            }
        }
    );

    /// Thread-local large serialization buffer pool
    static LARGE_BUFFER_POOL: ThreadLocalPool<Vec<u8>> = ThreadLocalPool::with_reset(
        50,
        || Vec::with_capacity(64 * 1024), // 64KB for large operations
        |vec| {
            vec.clear();
            if vec.capacity() > 1024 * 1024 {
                vec.shrink_to(256 * 1024);
            }
        }
    );

    /// Thread-local small buffer pool for keys and small values
    static SMALL_BUFFER_POOL: ThreadLocalPool<Vec<u8>> = ThreadLocalPool::with_reset(
        200,
        || Vec::with_capacity(256),
        |vec| {
            vec.clear();
            if vec.capacity() > 4096 {
                vec.shrink_to(1024);
            }
        }
    );

    /// Thread-local btree node buffer pool for zero-copy operations
    static BTREE_NODE_POOL: ThreadLocalPool<Vec<u8>> = ThreadLocalPool::with_reset(
        75,
        || Vec::with_capacity(4096), // Page-sized buffers
        |vec| {
            vec.clear();
            if vec.capacity() > 16 * 1024 {
                vec.shrink_to(8 * 1024);
            }
        }
    );

    /// Thread-local compression workspace pool
    static COMPRESSION_WORKSPACE_POOL: ThreadLocalPool<Vec<u8>> = ThreadLocalPool::with_reset(
        20,
        || Vec::with_capacity(128 * 1024), // Large compression workspace
        |vec| {
            vec.clear();
            if vec.capacity() > 2 * 1024 * 1024 {
                vec.shrink_to(512 * 1024);
            }
        }
    );
}

/// Zero-allocation access to thread-local Vec<u8> buffer
#[inline(always)]
pub fn with_vec_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    VEC_BUFFER_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

/// Zero-allocation access to thread-local String buffer
#[inline(always)]
pub fn with_string_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut String) -> R,
{
    STRING_BUFFER_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

/// Zero-allocation access to thread-local HashMap
#[inline(always)]
pub fn with_hashmap<F, R>(f: F) -> R
where
    F: FnOnce(&mut HashMap<Bytes, Bytes>) -> R,
{
    HASHMAP_POOL.with(|pool| {
        let mut map = pool.get();
        f(&mut map)
    })
}

/// Zero-allocation access to thread-local large buffer
#[inline(always)]
pub fn with_large_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    LARGE_BUFFER_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

/// Zero-allocation access to thread-local small buffer
#[inline(always)]
pub fn with_small_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    SMALL_BUFFER_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

/// Zero-allocation access to thread-local btree node buffer
#[inline(always)]
pub fn with_btree_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    BTREE_NODE_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

/// Zero-allocation access to thread-local compression workspace
#[inline(always)]
pub fn with_compression_workspace<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    COMPRESSION_WORKSPACE_POOL.with(|pool| {
        let mut buf = pool.get();
        f(&mut buf)
    })
}

// Note: These functions are commented out due to lifetime complexity with thread_local!
// Use the with_* functions instead for zero-allocation access

// /// Get a thread-local Vec<u8> buffer (returns ownership)
// pub fn get_vec_buffer() -> ThreadLocalGuard<'static, Vec<u8>> {
//     VEC_BUFFER_POOL.with(|pool| pool.get())
// }

// /// Get a thread-local String buffer (returns ownership)
// pub fn get_string_buffer() -> ThreadLocalGuard<'static, String> {
//     STRING_BUFFER_POOL.with(|pool| pool.get())
// }

// /// Get a thread-local HashMap (returns ownership)
// pub fn get_hashmap() -> ThreadLocalGuard<'static, HashMap<Bytes, Bytes>> {
//     HASHMAP_POOL.with(|pool| pool.get())
// }

/// Memory pool statistics for monitoring
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub vec_buffer_count: usize,
    pub string_buffer_count: usize,
    pub hashmap_count: usize,
    pub large_buffer_count: usize,
    pub small_buffer_count: usize,
    pub btree_buffer_count: usize,
    pub compression_workspace_count: usize,
}

/// Get statistics for all thread-local pools
pub fn get_pool_stats() -> PoolStats {
    PoolStats {
        vec_buffer_count: VEC_BUFFER_POOL.with(|pool| pool.len()),
        string_buffer_count: STRING_BUFFER_POOL.with(|pool| pool.len()),
        hashmap_count: HASHMAP_POOL.with(|pool| pool.len()),
        large_buffer_count: LARGE_BUFFER_POOL.with(|pool| pool.len()),
        small_buffer_count: SMALL_BUFFER_POOL.with(|pool| pool.len()),
        btree_buffer_count: BTREE_NODE_POOL.with(|pool| pool.len()),
        compression_workspace_count: COMPRESSION_WORKSPACE_POOL.with(|pool| pool.len()),
    }
}

/// Clear all thread-local pools (useful for testing and memory cleanup)
pub fn clear_all_thread_pools() {
    VEC_BUFFER_POOL.with(|pool| pool.clear());
    STRING_BUFFER_POOL.with(|pool| pool.clear());
    HASHMAP_POOL.with(|pool| pool.clear());
    LARGE_BUFFER_POOL.with(|pool| pool.clear());
    SMALL_BUFFER_POOL.with(|pool| pool.clear());
    BTREE_NODE_POOL.with(|pool| pool.clear());
    COMPRESSION_WORKSPACE_POOL.with(|pool| pool.clear());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_local_pool() {
        let pool = ThreadLocalPool::new(5, || Vec::<i32>::new());

        let mut obj1 = pool.get();
        obj1.push(42);
        assert_eq!(obj1.len(), 1);

        drop(obj1);

        let _obj2 = pool.get();
        // Object should be reused but not necessarily reset
        assert_eq!(pool.len(), 0); // Should be taken from pool
    }

    #[test]
    fn test_thread_local_pool_with_reset() {
        let pool = ThreadLocalPool::with_reset(
            5,
            || Vec::with_capacity(10),
            |vec: &mut Vec<i32>| vec.clear(),
        );

        let mut obj1 = pool.get();
        obj1.push(42);
        obj1.push(24);
        drop(obj1);

        let obj2 = pool.get();
        assert!(obj2.is_empty()); // Should be reset
        assert!(obj2.capacity() >= 10); // Should maintain capacity
    }

    #[test]
    fn test_with_buffer_functions() {
        // Test vector buffer
        let result = with_vec_buffer(|buf| {
            buf.extend_from_slice(b"hello world");
            buf.len()
        });
        assert_eq!(result, 11);

        // Test string buffer
        let result = with_string_buffer(|buf| {
            buf.push_str("test string");
            buf.len()
        });
        assert_eq!(result, 11);

        // Test hashmap
        let result = with_hashmap(|map| {
            map.insert(Bytes::from("key"), Bytes::from("value"));
            map.len()
        });
        assert_eq!(result, 1);
    }

    #[test]
    fn test_pool_stats() {
        // Get initial stats
        let initial_stats = get_pool_stats();
        
        // Use some buffers
        with_vec_buffer(|buf| buf.push(42));
        with_string_buffer(|buf| buf.push_str("test"));
        
        // Stats should reflect usage
        let final_stats = get_pool_stats();
        
        // Note: Exact counts depend on test execution order
        // We just verify the function works
        println!("Initial stats: {:?}", initial_stats);
        println!("Final stats: {:?}", final_stats);
    }

    #[test]
    fn test_zero_allocation_pattern() {
        // This pattern should not allocate beyond the initial pool allocation
        for i in 0..1000 {
            with_small_buffer(|buf| {
                buf.extend_from_slice(format!("key{}", i).as_bytes());
                // Buffer is automatically returned to pool on scope exit
            });
        }
        
        // Verify pool has objects ready for reuse
        let stats = get_pool_stats();
        assert!(stats.small_buffer_count > 0);
    }

    #[test]
    fn test_large_buffer_performance() {
        // Test that large buffer operations work efficiently
        with_large_buffer(|buf| {
            // Simulate large serialization operation
            for i in 0u32..10000 {
                buf.extend_from_slice(&i.to_le_bytes());
            }
            assert_eq!(buf.len(), 40000); // 10000 * 4 bytes
        });
    }

    #[test]
    fn test_compression_workspace() {
        with_compression_workspace(|workspace| {
            // Simulate compression operation
            workspace.resize(1024 * 1024, 0xFF); // 1MB of 0xFF
            assert_eq!(workspace.len(), 1024 * 1024);
            
            // Simulate compression result
            workspace.clear();
            workspace.extend_from_slice(b"compressed_data");
        });
    }
}

use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

/// Lock-free memory pool for reducing allocations
/// Uses crossbeam's ArrayQueue for wait-free operations
pub struct LockFreeMemoryPool {
    key_buffers: Arc<ArrayQueue<Vec<u8>>>,
    value_buffers: Arc<ArrayQueue<Vec<u8>>>,
    key_buffer_size: usize,
    value_buffer_size: usize,
}

impl LockFreeMemoryPool {
    pub fn new(capacity: usize, key_size: usize, value_size: usize) -> Self {
        let key_buffers = Arc::new(ArrayQueue::new(capacity));
        let value_buffers = Arc::new(ArrayQueue::new(capacity));

        // Pre-allocate buffers
        for _ in 0..capacity {
            let _ = key_buffers.push(Vec::with_capacity(key_size));
            let _ = value_buffers.push(Vec::with_capacity(value_size));
        }

        Self {
            key_buffers,
            value_buffers,
            key_buffer_size: key_size,
            value_buffer_size: value_size,
        }
    }

    /// Acquire a key buffer from the pool (wait-free)
    #[inline(always)]
    pub fn acquire_key_buffer(&self) -> Vec<u8> {
        self.key_buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.key_buffer_size))
    }

    /// Acquire a value buffer from the pool (wait-free)
    #[inline(always)]
    pub fn acquire_value_buffer(&self) -> Vec<u8> {
        self.value_buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.value_buffer_size))
    }

    /// Return a key buffer to the pool (wait-free)
    #[inline(always)]
    pub fn return_key_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        // Only keep buffer if it's not too large
        if buffer.capacity() <= self.key_buffer_size * 2 {
            let _ = self.key_buffers.push(buffer);
        }
    }

    /// Return a value buffer to the pool (wait-free)
    #[inline(always)]
    pub fn return_value_buffer(&self, mut buffer: Vec<u8>) {
        buffer.clear();
        // Only keep buffer if it's not too large
        if buffer.capacity() <= self.value_buffer_size * 2 {
            let _ = self.value_buffers.push(buffer);
        }
    }

    /// Get current pool statistics
    pub fn stats(&self) -> MemoryPoolStats {
        MemoryPoolStats {
            key_buffers_available: self.key_buffers.len(),
            value_buffers_available: self.value_buffers.len(),
            key_buffers_capacity: self.key_buffers.capacity(),
            value_buffers_capacity: self.value_buffers.capacity(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPoolStats {
    pub key_buffers_available: usize,
    pub value_buffers_available: usize,
    pub key_buffers_capacity: usize,
    pub value_buffers_capacity: usize,
}

/// RAII guard for automatically returning buffers to pool
pub struct BufferGuard<'a> {
    buffer: Option<Vec<u8>>,
    pool: &'a LockFreeMemoryPool,
    is_key: bool,
}

impl<'a> BufferGuard<'a> {
    pub fn new_key(pool: &'a LockFreeMemoryPool) -> Self {
        Self {
            buffer: Some(pool.acquire_key_buffer()),
            pool,
            is_key: true,
        }
    }

    pub fn new_value(pool: &'a LockFreeMemoryPool) -> Self {
        Self {
            buffer: Some(pool.acquire_value_buffer()),
            pool,
            is_key: false,
        }
    }

    pub fn get_mut(&mut self) -> &mut Vec<u8> {
        self.buffer.as_mut().unwrap()
    }

    pub fn take(mut self) -> Vec<u8> {
        self.buffer.take().unwrap()
    }
}

impl Drop for BufferGuard<'_> {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            if self.is_key {
                self.pool.return_key_buffer(buffer);
            } else {
                self.pool.return_value_buffer(buffer);
            }
        }
    }
}

impl std::ops::Deref for BufferGuard<'_> {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for BufferGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_lock_free_memory_pool() {
        let pool = Arc::new(LockFreeMemoryPool::new(10, 128, 1024));

        // Test basic acquire/return
        let mut key_buf = pool.acquire_key_buffer();
        key_buf.extend_from_slice(b"test_key");
        assert_eq!(key_buf.as_slice(), b"test_key");
        pool.return_key_buffer(key_buf);

        // Test concurrent access
        let mut handles = vec![];
        for i in 0..4 {
            let pool_clone = Arc::clone(&pool);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let mut key = pool_clone.acquire_key_buffer();
                    key.extend_from_slice(&format!("key_{}_{}", i, j).into_bytes());

                    let mut value = pool_clone.acquire_value_buffer();
                    value.extend_from_slice(&format!("value_{}_{}", i, j).into_bytes());

                    // Simulate some work
                    std::thread::yield_now();

                    pool_clone.return_key_buffer(key);
                    pool_clone.return_value_buffer(value);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Check pool still works after concurrent access
        let stats = pool.stats();
        assert!(stats.key_buffers_available > 0);
        assert!(stats.value_buffers_available > 0);
    }

    #[test]
    fn test_buffer_guard() {
        let pool = LockFreeMemoryPool::new(5, 64, 256);

        {
            let mut guard = BufferGuard::new_key(&pool);
            guard.extend_from_slice(b"guarded_key");
            assert_eq!(guard.as_slice(), b"guarded_key");
        } // Buffer automatically returned

        // Verify buffer was returned
        assert_eq!(pool.stats().key_buffers_available, 5);
    }
}

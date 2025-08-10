//! Zero-Copy Buffer Management
//!
//! This module provides efficient buffer management for zero-copy I/O operations.
//! It includes buffer pools, alignment management, and direct memory mapping.

use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::io::{Error, ErrorKind, Result};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Page size for aligned allocations (typically 4KB)
pub const PAGE_SIZE: usize = 4096;

/// Direct I/O alignment requirement (typically 512 bytes)
pub const DIRECT_IO_ALIGN: usize = 512;

/// Buffer alignment for optimal performance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferAlignment {
    /// No alignment requirement
    None,
    /// Align to cache line (64 bytes)
    CacheLine,
    /// Align to page boundary (4KB)
    Page,
    /// Align for direct I/O (512 bytes)
    DirectIo,
    /// Custom alignment
    Custom(usize),
}

impl BufferAlignment {
    pub fn size(&self) -> usize {
        match self {
            BufferAlignment::None => 1,
            BufferAlignment::CacheLine => 64,
            BufferAlignment::Page => PAGE_SIZE,
            BufferAlignment::DirectIo => DIRECT_IO_ALIGN,
            BufferAlignment::Custom(size) => *size,
        }
    }
}

/// Zero-copy buffer with automatic alignment
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
    alignment: BufferAlignment,
    owned: bool,
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Create a new aligned buffer
    pub fn new(size: usize, alignment: BufferAlignment) -> Result<Self> {
        let align_size = alignment.size();
        let aligned_size = (size + align_size - 1) & !(align_size - 1);

        let layout = Layout::from_size_align(aligned_size, align_size)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid layout"))?;

        let ptr = unsafe { alloc(layout) };

        if ptr.is_null() {
            return Err(Error::new(
                ErrorKind::OutOfMemory,
                "Failed to allocate buffer",
            ));
        }

        Ok(AlignedBuffer {
            ptr: NonNull::new(ptr).unwrap(),
            len: size,
            cap: aligned_size,
            alignment,
            owned: true,
        })
    }

    /// Create a buffer from existing memory (non-owning)
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize, alignment: BufferAlignment) -> Self {
        AlignedBuffer {
            ptr: NonNull::new_unchecked(ptr),
            len,
            cap: len,
            alignment,
            owned: false,
        }
    }

    /// Get a pointer to the buffer
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Get a mutable pointer to the buffer
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Get buffer alignment
    pub fn alignment(&self) -> BufferAlignment {
        self.alignment
    }

    /// Check if buffer is aligned for direct I/O
    pub fn is_directio_aligned(&self) -> bool {
        let ptr_addr = self.ptr.as_ptr() as usize;
        ptr_addr % DIRECT_IO_ALIGN == 0 && self.len % DIRECT_IO_ALIGN == 0
    }

    /// Split buffer at index
    pub fn split_at(&mut self, mid: usize) -> Result<AlignedBuffer> {
        if mid > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Split index out of bounds",
            ));
        }

        let second_ptr = unsafe { self.ptr.as_ptr().add(mid) };
        let second_len = self.len - mid;

        self.len = mid;

        Ok(unsafe { AlignedBuffer::from_raw_parts(second_ptr, second_len, self.alignment) })
    }

    /// Zero the buffer
    pub fn zero(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.len);
        }
    }

    /// Get buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get buffer as a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.owned {
            let align_size = self.alignment.size();
            let layout = Layout::from_size_align(self.cap, align_size).unwrap();
            unsafe {
                dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

/// Buffer pool for efficient buffer reuse
pub struct BufferPool {
    pools: Vec<Mutex<BufferSizePool>>,
    stats: BufferPoolStats,
}

struct BufferSizePool {
    size: usize,
    alignment: BufferAlignment,
    free_buffers: VecDeque<AlignedBuffer>,
    max_buffers: usize,
}

#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub allocations: AtomicUsize,
    pub deallocations: AtomicUsize,
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
    pub current_buffers: AtomicUsize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new() -> Self {
        let mut pools = Vec::new();

        // Create pools for common sizes
        let sizes = [
            (512, BufferAlignment::DirectIo),
            (4096, BufferAlignment::Page),
            (8192, BufferAlignment::Page),
            (16384, BufferAlignment::Page),
            (65536, BufferAlignment::Page),
            (131072, BufferAlignment::Page),
            (262144, BufferAlignment::Page),
            (1048576, BufferAlignment::Page),
        ];

        for (size, alignment) in sizes {
            pools.push(Mutex::new(BufferSizePool {
                size,
                alignment,
                free_buffers: VecDeque::new(),
                max_buffers: 100, // Max buffers per size
            }));
        }

        BufferPool {
            pools,
            stats: BufferPoolStats::default(),
        }
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self, size: usize, alignment: BufferAlignment) -> Result<AlignedBuffer> {
        // Find the appropriate pool
        let pool_idx = self.find_pool_index(size, alignment);

        if let Some(idx) = pool_idx {
            let mut pool = self.pools[idx].lock().unwrap();

            if let Some(mut buffer) = pool.free_buffers.pop_front() {
                self.stats.hits.fetch_add(1, Ordering::Relaxed);
                buffer.len = size; // Adjust size if needed
                buffer.zero(); // Clear buffer
                return Ok(buffer);
            }
        }

        // No suitable buffer in pool, allocate new one
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        self.stats.current_buffers.fetch_add(1, Ordering::Relaxed);

        AlignedBuffer::new(size, alignment)
    }

    /// Release a buffer back to the pool
    pub fn release(&self, buffer: AlignedBuffer) {
        if !buffer.owned {
            return; // Don't pool non-owned buffers
        }

        let pool_idx = self.find_pool_index(buffer.cap, buffer.alignment);

        if let Some(idx) = pool_idx {
            let mut pool = self.pools[idx].lock().unwrap();

            if pool.free_buffers.len() < pool.max_buffers {
                pool.free_buffers.push_back(buffer);
                self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // Pool is full or no suitable pool, let buffer drop
        self.stats.current_buffers.fetch_sub(1, Ordering::Relaxed);
    }

    fn find_pool_index(&self, size: usize, alignment: BufferAlignment) -> Option<usize> {
        for (idx, pool) in self.pools.iter().enumerate() {
            let pool = pool.lock().unwrap();
            if pool.size >= size && pool.alignment == alignment {
                return Some(idx);
            }
        }
        None
    }

    /// Get pool statistics
    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
    }

    /// Clear all buffers from pools
    pub fn clear(&self) {
        for pool in &self.pools {
            let mut pool = pool.lock().unwrap();
            let count = pool.free_buffers.len();
            pool.free_buffers.clear();
            self.stats
                .current_buffers
                .fetch_sub(count, Ordering::Relaxed);
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Registered buffer for io_uring fixed buffers
pub struct RegisteredBuffer {
    buffer: AlignedBuffer,
    index: u32,
    in_use: AtomicBool,
}

impl RegisteredBuffer {
    pub fn new(buffer: AlignedBuffer, index: u32) -> Self {
        RegisteredBuffer {
            buffer,
            index,
            in_use: AtomicBool::new(false),
        }
    }

    /// Try to acquire this buffer for use
    pub fn try_acquire(&self) -> Option<RegisteredBufferGuard> {
        if self
            .in_use
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(RegisteredBufferGuard { buffer: self })
        } else {
            None
        }
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}

/// Guard for registered buffer usage
pub struct RegisteredBufferGuard<'a> {
    buffer: &'a RegisteredBuffer,
}

impl<'a> Drop for RegisteredBufferGuard<'a> {
    fn drop(&mut self) {
        self.buffer.in_use.store(false, Ordering::Release);
    }
}

impl<'a> Deref for RegisteredBufferGuard<'a> {
    type Target = AlignedBuffer;

    fn deref(&self) -> &Self::Target {
        &self.buffer.buffer
    }
}

/// Manager for registered buffers
pub struct RegisteredBufferManager {
    buffers: Vec<Arc<RegisteredBuffer>>,
}

impl RegisteredBufferManager {
    pub fn new() -> Self {
        RegisteredBufferManager {
            buffers: Vec::new(),
        }
    }

    /// Register buffers for io_uring fixed buffer operations
    pub fn register_buffers(&mut self, buffers: Vec<AlignedBuffer>) -> Vec<Arc<RegisteredBuffer>> {
        self.buffers.clear();

        for (idx, buffer) in buffers.into_iter().enumerate() {
            self.buffers
                .push(Arc::new(RegisteredBuffer::new(buffer, idx as u32)));
        }

        self.buffers.clone()
    }

    /// Try to acquire a free registered buffer
    pub fn try_acquire(&self, min_size: usize) -> Option<RegisteredBufferGuard> {
        for buffer in &self.buffers {
            if buffer.buffer.len() >= min_size {
                if let Some(guard) = buffer.try_acquire() {
                    return Some(guard);
                }
            }
        }
        None
    }

    /// Get all registered buffers
    pub fn buffers(&self) -> &[Arc<RegisteredBuffer>] {
        &self.buffers
    }
}

/// Zero-copy buffer view for vectored I/O
pub struct IoVecBuffer {
    vecs: Vec<super::IoVec>,
    _buffers: Vec<AlignedBuffer>, // Keep buffers alive
}

impl IoVecBuffer {
    pub fn new() -> Self {
        IoVecBuffer {
            vecs: Vec::new(),
            _buffers: Vec::new(),
        }
    }

    /// Add a buffer to the vectored I/O
    pub fn add_buffer(&mut self, mut buffer: AlignedBuffer) {
        self.vecs.push(super::IoVec {
            base: buffer.as_mut_ptr(),
            len: buffer.len(),
        });
        self._buffers.push(buffer);
    }

    /// Get the I/O vectors
    pub fn vecs(&self) -> &[super::IoVec] {
        &self.vecs
    }

    /// Get total length of all buffers
    pub fn total_len(&self) -> usize {
        self.vecs.iter().map(|v| v.len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer_creation() {
        let buffer = AlignedBuffer::new(4096, BufferAlignment::Page).unwrap();
        assert_eq!(buffer.len(), 4096);
        assert_eq!(buffer.capacity(), 4096);
        assert!(buffer.is_directio_aligned());

        let ptr_addr = buffer.as_ptr() as usize;
        assert_eq!(ptr_addr % PAGE_SIZE, 0);
    }

    #[test]
    fn test_buffer_split() {
        let mut buffer = AlignedBuffer::new(8192, BufferAlignment::Page).unwrap();
        let second = buffer.split_at(4096).unwrap();

        assert_eq!(buffer.len(), 4096);
        assert_eq!(second.len(), 4096);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new();

        // Acquire buffer
        let buffer1 = pool.acquire(4096, BufferAlignment::Page).unwrap();
        assert_eq!(pool.stats().misses.load(Ordering::Relaxed), 1);

        // Release buffer
        pool.release(buffer1);
        assert_eq!(pool.stats().deallocations.load(Ordering::Relaxed), 1);

        // Acquire again - should hit pool
        let buffer2 = pool.acquire(4096, BufferAlignment::Page).unwrap();
        assert_eq!(pool.stats().hits.load(Ordering::Relaxed), 1);

        pool.release(buffer2);
    }

    #[test]
    fn test_registered_buffer_manager() {
        let mut manager = RegisteredBufferManager::new();

        let buffers = vec![
            AlignedBuffer::new(4096, BufferAlignment::Page).unwrap(),
            AlignedBuffer::new(8192, BufferAlignment::Page).unwrap(),
        ];

        let registered = manager.register_buffers(buffers);
        assert_eq!(registered.len(), 2);
        assert_eq!(registered[0].index(), 0);
        assert_eq!(registered[1].index(), 1);

        // Test acquisition
        let guard1 = manager.try_acquire(4096).unwrap();
        let guard2 = manager.try_acquire(4096);
        assert!(guard2.is_some()); // Should get second buffer

        let guard3 = manager.try_acquire(4096);
        assert!(guard3.is_none()); // All buffers in use

        drop(guard1);
        let guard4 = manager.try_acquire(4096);
        assert!(guard4.is_some()); // First buffer available again
    }

    #[test]
    fn test_iovec_buffer() {
        let mut iovec = IoVecBuffer::new();

        iovec.add_buffer(AlignedBuffer::new(1024, BufferAlignment::None).unwrap());
        iovec.add_buffer(AlignedBuffer::new(2048, BufferAlignment::None).unwrap());

        assert_eq!(iovec.vecs().len(), 2);
        assert_eq!(iovec.total_len(), 3072);
    }
}

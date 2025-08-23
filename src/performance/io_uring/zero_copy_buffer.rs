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

/// Standard page size (4KB)
pub const PAGE_SIZE_4K: usize = 4096;
/// Large page size for better I/O throughput (16KB)
pub const PAGE_SIZE_16K: usize = 16384;
/// Extra large page size for high-throughput scenarios (32KB)
pub const PAGE_SIZE_32K: usize = 32768;
/// Huge page size (2MB)
pub const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024;
/// Default optimized page size for lightning_db
pub const PAGE_SIZE: usize = PAGE_SIZE_16K;

/// Direct I/O alignment requirement (typically 512 bytes)
pub const DIRECT_IO_ALIGN: usize = 512;
/// Optimized alignment for NVMe SSDs (4KB)
pub const NVME_ALIGN: usize = 4096;

/// Buffer alignment for optimal performance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferAlignment {
    /// No alignment requirement
    None,
    /// Align to cache line (64 bytes)
    CacheLine,
    /// Align to page boundary (4KB)
    Page,
    /// Align to large page boundary (16KB)
    LargePage,
    /// Align to extra large page boundary (32KB)
    ExtraLargePage,
    /// Align to huge page boundary (2MB)
    HugePage,
    /// Optimized alignment for NVMe SSDs
    NvmeOptimized,
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
            BufferAlignment::Page => PAGE_SIZE_4K,
            BufferAlignment::LargePage => PAGE_SIZE_16K,
            BufferAlignment::ExtraLargePage => PAGE_SIZE_32K,
            BufferAlignment::HugePage => HUGE_PAGE_SIZE,
            BufferAlignment::DirectIo => DIRECT_IO_ALIGN,
            BufferAlignment::NvmeOptimized => NVME_ALIGN,
            BufferAlignment::Custom(size) => *size,
        }
    }
}

/// Zero-copy buffer with automatic alignment
#[derive(Debug)]
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
    alignment: BufferAlignment,
    owned: bool,
}

// SAFETY: AlignedBuffer is safe to send/sync when:
// 1. The ptr field is owned and exclusively managed by this instance
// 2. The owned flag ensures proper ownership semantics
// 3. NonNull<u8> is safe to send/sync as it's just a pointer wrapper
// 4. All other fields are basic types that are naturally Send/Sync
// RISK: MEDIUM - Incorrect ownership could lead to data races
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl Clone for AlignedBuffer {
    fn clone(&self) -> Self {
        // Create a new buffer with the same content
        match AlignedBuffer::new(self.len, self.alignment) {
            Ok(new_buffer) => {
                // Copy the data
                // SAFETY: Copying data between aligned buffers
                // Invariants:
                // 1. Source and destination pointers are valid and aligned
                // 2. Buffers are non-overlapping (new allocation)
                // 3. Length is validated to fit in both buffers
                // 4. Both pointers are properly owned
                // Guarantees:
                // - Data is copied without aliasing
                // - Alignment is preserved
                // SAFETY: Copying data between aligned buffers with existing comments
                // RISK: LOW - Safe copy between owned buffers
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.ptr.as_ptr(),
                        new_buffer.ptr.as_ptr(),
                        self.len,
                    );
                }
                new_buffer
            }
            Err(_) => {
                // Fallback: create a minimal buffer
                AlignedBuffer {
                    ptr: NonNull::dangling(),
                    len: 0,
                    cap: 0,
                    alignment: self.alignment,
                    owned: false,
                }
            }
        }
    }
}

impl AlignedBuffer {
    /// Create a new aligned buffer
    pub fn new(size: usize, alignment: BufferAlignment) -> Result<Self> {
        let align_size = alignment.size();
        let aligned_size = (size + align_size - 1) & !(align_size - 1);

        let layout = Layout::from_size_align(aligned_size, align_size)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid layout"))?;

        // SAFETY: Allocating aligned memory with valid layout
        // Invariants:
        // 1. Layout is validated above with from_size_align
        // 2. Size and alignment are powers of two
        // 3. Allocation follows system alignment requirements
        // Guarantees:
        // - Returns aligned memory or null on failure
        // - Memory is uninitialized but owned
        // SAFETY: Allocating aligned memory with valid layout
        // Invariants:
        // 1. Layout is validated above with from_size_align
        // 2. Size and alignment are powers of two
        // 3. Allocation follows system alignment requirements
        // Guarantees:
        // - Returns aligned memory or null on failure
        // - Memory is uninitialized but owned
        // RISK: LOW - Standard allocation pattern with proper validation
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
    /// 
    /// # Safety
    /// - ptr must be valid for reads/writes of len bytes
    /// - ptr must be properly aligned according to alignment parameter
    /// - ptr must remain valid for the lifetime of the AlignedBuffer
    /// - Caller must ensure no data races occur
    /// Create a buffer from existing memory (non-owning)
    /// 
    /// # Safety
    /// - ptr must be valid for reads/writes of len bytes
    /// - ptr must be properly aligned according to alignment parameter
    /// - ptr must remain valid for the lifetime of the AlignedBuffer
    /// - Caller must ensure no data races occur
    /// 
    /// # Risk Assessment: HIGH
    /// - Can cause use-after-free if ptr becomes invalid
    /// - Can cause data races if multiple mutable references exist
    /// - Can cause undefined behavior if alignment is incorrect
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize, alignment: BufferAlignment) -> Self {
        debug_assert!(!ptr.is_null(), "Cannot create AlignedBuffer from null pointer");
        debug_assert!(len > 0, "Cannot create AlignedBuffer with zero length");
        debug_assert_eq!(ptr as usize % alignment.size(), 0, "Pointer not properly aligned");
        
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

    /// Check if buffer is aligned for NVMe optimization
    pub fn is_nvme_aligned(&self) -> bool {
        let ptr_addr = self.ptr.as_ptr() as usize;
        ptr_addr % NVME_ALIGN == 0 && self.len % NVME_ALIGN == 0
    }

    /// Prefault pages to minimize page faults during I/O
    pub fn prefault(&mut self) {
        let page_size = PAGE_SIZE_4K;
        // Touch each page to fault it in
        for offset in (0..self.len).step_by(page_size) {
            // SAFETY: Pre-faulting pages within buffer bounds
            // Invariants:
            // 1. offset is always < self.len due to step_by iteration
            // 2. ptr.add(offset) stays within allocated buffer
            // 3. Buffer is exclusively owned (mutable reference)
            // 4. Volatile operations prevent optimization
            // Guarantees:
            // - Forces OS page allocation
            // - Read-modify-write preserves data
            // - No data races due to exclusive ownership
            // SAFETY: Pre-faulting pages within buffer bounds
            // Invariants:
            // 1. offset is always < self.len due to step_by iteration
            // 2. ptr.add(offset) stays within allocated buffer
            // 3. Buffer is exclusively owned (mutable reference)
            // 4. Volatile operations prevent optimization
            // Guarantees:
            // - Forces OS page allocation
            // - Read-modify-write preserves data
            // - No data races due to exclusive ownership
            // RISK: LOW - Bounded access within owned buffer
            unsafe {
                let ptr = self.ptr.as_ptr().add(offset);
                std::ptr::read_volatile(ptr);
                std::ptr::write_volatile(ptr, *ptr);
            }
        }
    }

    /// Split buffer at index
    pub fn split_at(&mut self, mid: usize) -> Result<AlignedBuffer> {
        if mid > self.len {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Split index out of bounds",
            ));
        }

        // SAFETY: Creating pointer to split point
        // Invariants:
        // 1. mid <= self.len (checked above)
        // 2. Resulting pointer stays within allocation
        // 3. Original buffer remains valid for [0..mid]
        // Guarantees:
        // - Split creates non-overlapping regions
        // - Both halves remain valid
        // SAFETY: Creating pointer to split point
        // Invariants:
        // 1. mid <= self.len (checked above)
        // 2. Resulting pointer stays within allocation
        // 3. Original buffer remains valid for [0..mid]
        // Guarantees:
        // - Split creates non-overlapping regions
        // - Both halves remain valid
        // RISK: LOW - Bounds checked pointer arithmetic
        let second_ptr = unsafe { self.ptr.as_ptr().add(mid) };
        let second_len = self.len - mid;

        self.len = mid;

        // SAFETY: Creating non-owning buffer from split region
        // Invariants:
        // 1. second_ptr points to valid memory within original allocation
        // 2. second_len doesn't exceed remaining buffer size
        // 3. Alignment is preserved from parent buffer
        // 4. Parent buffer lifetime exceeds split buffer
        // Guarantees:
        // - Split buffer is non-owning (owned: false)
        // - No double-free as only parent owns memory
        // SAFETY: Creating non-owning buffer from split region
        // Invariants:
        // 1. second_ptr points to valid memory within original allocation
        // 2. second_len doesn't exceed remaining buffer size
        // 3. Alignment is preserved from parent buffer
        // 4. Parent buffer lifetime exceeds split buffer
        // Guarantees:
        // - Split buffer is non-owning (owned: false)
        // - No double-free as only parent owns memory
        // RISK: MEDIUM - Caller must ensure parent buffer outlives split
        Ok(unsafe { AlignedBuffer::from_raw_parts(second_ptr, second_len, self.alignment) })
    }

    /// Zero the buffer
    pub fn zero(&mut self) {
        // SAFETY: Zeroing owned buffer memory
        // Invariants:
        // 1. ptr points to valid allocated memory
        // 2. len is within allocated capacity
        // 3. Exclusive access via mutable reference
        // Guarantees:
        // - All bytes set to zero
        // - No data races
        // SAFETY: Zeroing owned buffer memory
        // Invariants:
        // 1. ptr points to valid allocated memory
        // 2. len is within allocated capacity
        // 3. Exclusive access via mutable reference
        // Guarantees:
        // - All bytes set to zero
        // - No data races
        // RISK: LOW - Safe operation on owned memory
        unsafe {
            std::ptr::write_bytes(self.ptr.as_ptr(), 0, self.len);
        }
    }

    /// Get buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: Creating slice from owned buffer
        // Invariants:
        // 1. ptr is valid and properly aligned
        // 2. len bytes are allocated and initialized
        // 3. Lifetime tied to &self reference
        // Guarantees:
        // - Slice is valid for lifetime of borrow
        // - No mutable aliasing possible
        // RISK: LOW - Standard slice creation from owned memory
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get buffer as a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: Creating mutable slice from owned buffer
        // Invariants:
        // 1. ptr is valid and properly aligned
        // 2. len bytes are allocated
        // 3. Exclusive access via &mut self
        // 4. Lifetime tied to mutable borrow
        // Guarantees:
        // - No aliasing due to exclusive access
        // - Slice valid for lifetime of mutable borrow
        // RISK: LOW - Safe mutable slice creation
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.owned {
            let align_size = self.alignment.size();
            let layout = Layout::from_size_align(self.cap, align_size).unwrap();
            // SAFETY: Deallocating owned aligned memory
            // Invariants:
            // 1. Memory was allocated with same layout (cap, align_size)
            // 2. owned flag ensures we only free our allocations
            // 3. Drop is called exactly once
            // 4. No other references exist (Drop impl)
            // Guarantees:
            // - Memory is returned to allocator
            // - No use-after-free as Drop invalidates buffer
            // SAFETY: Deallocating owned aligned memory
            // Invariants:
            // 1. Memory was allocated with same layout (cap, align_size)
            // 2. owned flag ensures we only free our allocations
            // 3. Drop is called exactly once
            // 4. No other references exist (Drop impl)
            // Guarantees:
            // - Memory is returned to allocator
            // - No use-after-free as Drop invalidates buffer
            // RISK: LOW - Standard deallocation pattern
            unsafe {
                dealloc(self.ptr.as_ptr(), layout);
            }
        }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: Creating slice for Deref implementation
        // Same invariants as as_slice() method
        // RISK: LOW - Standard deref pattern
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Creating mutable slice for DerefMut implementation
        // Same invariants as as_mut_slice() method
        // RISK: LOW - Standard mutable deref pattern
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

        // Create pools for optimized sizes for maximum I/O throughput
        let sizes = [
            (512, BufferAlignment::DirectIo),
            (PAGE_SIZE_4K, BufferAlignment::Page),
            (PAGE_SIZE_16K, BufferAlignment::LargePage),
            (PAGE_SIZE_32K, BufferAlignment::ExtraLargePage),
            (64 * 1024, BufferAlignment::LargePage),
            (128 * 1024, BufferAlignment::ExtraLargePage),
            (256 * 1024, BufferAlignment::ExtraLargePage),
            (512 * 1024, BufferAlignment::ExtraLargePage),
            (1024 * 1024, BufferAlignment::ExtraLargePage),
            (2 * 1024 * 1024, BufferAlignment::HugePage),
            (4 * 1024 * 1024, BufferAlignment::HugePage),
        ];

        for (size, alignment) in sizes {
            pools.push(Mutex::new(BufferSizePool {
                size,
                alignment,
                free_buffers: VecDeque::new(),
                max_buffers: match size {
                    s if s <= PAGE_SIZE_16K => 200,    // More small buffers
                    s if s <= 256 * 1024 => 100,       // Medium buffers
                    s if s <= 1024 * 1024 => 50,       // Large buffers
                    _ => 20,                            // Huge buffers
                },
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
            let mut pool = match self.pools[idx].lock() {
                Ok(p) => p,
                Err(_) => return Err(Error::new(ErrorKind::Other, "Pool lock poisoned")),
            };

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
            let mut pool = match self.pools[idx].lock() {
                Ok(p) => p,
                Err(_) => {
                    // If lock is poisoned, just let buffer drop
                    self.stats.current_buffers.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };

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
            let pool = match pool.lock() {
                Ok(p) => p,
                Err(_) => continue, // Skip if lock is poisoned
            };
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
            let mut pool = match pool.lock() {
                Ok(p) => p,
                Err(_) => continue, // Skip if lock is poisoned
            };
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

/// High-performance buffer manager with pre-allocated pools and parallel access
pub struct HighPerformanceBufferManager {
    pools: Vec<Arc<Mutex<BufferSizePool>>>,
    stats: Arc<BufferPoolStats>,
    prefault_on_acquire: bool,
}

impl HighPerformanceBufferManager {
    pub fn new() -> Self {
        Self::with_prefault(true)
    }

    pub fn with_prefault(prefault: bool) -> Self {
        let mut pools = Vec::new();
        
        // Optimized pool sizes for high-throughput I/O
        let pool_configs = [
            (PAGE_SIZE_4K, BufferAlignment::Page, 500),
            (PAGE_SIZE_16K, BufferAlignment::LargePage, 300),
            (PAGE_SIZE_32K, BufferAlignment::ExtraLargePage, 200),
            (64 * 1024, BufferAlignment::ExtraLargePage, 150),
            (128 * 1024, BufferAlignment::ExtraLargePage, 100),
            (256 * 1024, BufferAlignment::ExtraLargePage, 80),
            (512 * 1024, BufferAlignment::ExtraLargePage, 60),
            (1024 * 1024, BufferAlignment::ExtraLargePage, 40),
            (2 * 1024 * 1024, BufferAlignment::HugePage, 20),
            (4 * 1024 * 1024, BufferAlignment::HugePage, 10),
        ];

        for (size, alignment, max_buffers) in pool_configs {
            pools.push(Arc::new(Mutex::new(BufferSizePool {
                size,
                alignment,
                free_buffers: VecDeque::new(),
                max_buffers,
            })));
        }

        let manager = Self {
            pools,
            stats: Arc::new(BufferPoolStats::default()),
            prefault_on_acquire: prefault,
        };

        // Pre-allocate some buffers for common sizes
        manager.prewarm();
        manager
    }

    fn prewarm(&self) {
        let prewarm_configs = [
            (PAGE_SIZE_16K, BufferAlignment::LargePage, 50),
            (PAGE_SIZE_32K, BufferAlignment::ExtraLargePage, 30),
            (256 * 1024, BufferAlignment::ExtraLargePage, 20),
            (1024 * 1024, BufferAlignment::ExtraLargePage, 10),
        ];

        for (size, alignment, count) in prewarm_configs {
            for _ in 0..count {
                if let Ok(mut buffer) = AlignedBuffer::new(size, alignment) {
                    if self.prefault_on_acquire {
                        buffer.prefault();
                    }
                    self.release_internal(buffer);
                }
            }
        }
    }

    pub fn acquire_optimized(&self, size: usize) -> Result<AlignedBuffer> {
        // Choose optimal alignment based on size
        let alignment = match size {
            s if s <= PAGE_SIZE_4K => BufferAlignment::Page,
            s if s <= PAGE_SIZE_16K => BufferAlignment::LargePage,
            s if s <= PAGE_SIZE_32K => BufferAlignment::ExtraLargePage,
            s if s <= 1024 * 1024 => BufferAlignment::ExtraLargePage,
            _ => BufferAlignment::HugePage,
        };

        self.acquire(size, alignment)
    }

    pub fn acquire(&self, size: usize, alignment: BufferAlignment) -> Result<AlignedBuffer> {
        // Find the best matching pool
        for pool in &self.pools {
            let mut pool_guard = match pool.lock() {
                Ok(p) => p,
                Err(_) => continue,
            };

            if pool_guard.size >= size && pool_guard.alignment == alignment {
                if let Some(mut buffer) = pool_guard.free_buffers.pop_front() {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    buffer.len = size;
                    if self.prefault_on_acquire {
                        buffer.prefault();
                    }
                    return Ok(buffer);
                }
            }
        }

        // No buffer available, allocate new
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        self.stats.current_buffers.fetch_add(1, Ordering::Relaxed);

        let mut buffer = AlignedBuffer::new(size, alignment)?;
        if self.prefault_on_acquire {
            buffer.prefault();
        }
        Ok(buffer)
    }

    pub fn release(&self, buffer: AlignedBuffer) {
        self.release_internal(buffer);
    }

    fn release_internal(&self, buffer: AlignedBuffer) {
        if !buffer.owned {
            return;
        }

        for pool in &self.pools {
            let mut pool_guard = match pool.lock() {
                Ok(p) => p,
                Err(_) => continue,
            };

            if pool_guard.size >= buffer.cap && pool_guard.alignment == buffer.alignment {
                if pool_guard.free_buffers.len() < pool_guard.max_buffers {
                    pool_guard.free_buffers.push_back(buffer);
                    self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }

        // No suitable pool or pool full
        self.stats.current_buffers.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> &BufferPoolStats {
        &self.stats
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

    #[test]
    fn test_high_performance_buffer_manager() {
        let manager = HighPerformanceBufferManager::new();
        
        // Test optimized acquisition
        let buffer = manager.acquire_optimized(PAGE_SIZE_16K).unwrap();
        assert!(buffer.len() >= PAGE_SIZE_16K);
        assert_eq!(buffer.alignment(), BufferAlignment::LargePage);
        
        manager.release(buffer);
        
        // Should hit the pool now
        let buffer2 = manager.acquire_optimized(PAGE_SIZE_16K).unwrap();
        assert_eq!(manager.stats().hits.load(Ordering::Relaxed), 1);
        
        manager.release(buffer2);
    }

    #[test]
    fn test_buffer_prefaulting() {
        let mut buffer = AlignedBuffer::new(PAGE_SIZE_32K, BufferAlignment::ExtraLargePage).unwrap();
        
        // Prefault should not crash
        buffer.prefault();
        
        // Buffer should still be usable
        buffer.zero();
        assert_eq!(buffer.as_slice()[0], 0);
    }
}

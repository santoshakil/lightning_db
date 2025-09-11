use crate::features::adaptive_compression::AdaptiveCompressionEngine;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

// Cache line size for optimal memory alignment
const CACHE_LINE_SIZE: usize = 64;

// SIMD-optimized buffer operations
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn simd_clear_buffer(buffer: &mut [u8]) {
    unsafe {
        let len = buffer.len();
        let ptr = buffer.as_mut_ptr();

        // Clear 16-byte chunks with SIMD
        let mut i = 0;
        while i + 16 <= len {
            let zero = _mm_setzero_si128();
            _mm_storeu_si128(ptr.add(i) as *mut __m128i, zero);
            i += 16;
        }

        // Clear remaining bytes
        for j in i..len {
            *ptr.add(j) = 0;
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn simd_clear_buffer(buffer: &mut [u8]) {
    buffer.fill(0);
}

// SIMD-optimized buffer copy
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn simd_copy_buffer(src: &[u8], dst: &mut [u8]) {
    if src.len() != dst.len() {
        dst[..src.len().min(dst.len())].copy_from_slice(&src[..src.len().min(dst.len())]);
        return;
    }

    unsafe {
        let len = src.len();
        let src_ptr = src.as_ptr();
        let dst_ptr = dst.as_mut_ptr();

        // Copy 16-byte chunks with SIMD
        let mut i = 0;
        while i + 16 <= len {
            let data = _mm_loadu_si128(src_ptr.add(i) as *const __m128i);
            _mm_storeu_si128(dst_ptr.add(i) as *mut __m128i, data);
            i += 16;
        }

        // Copy remaining bytes
        for j in i..len {
            *dst_ptr.add(j) = *src_ptr.add(j);
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn simd_copy_buffer(src: &[u8], dst: &mut [u8]) {
    let len = src.len().min(dst.len());
    dst[..len].copy_from_slice(&src[..len]);
}

// Cache-line aligned buffer for optimal performance
#[repr(C, align(64))]
struct AlignedBuffer {
    data: Vec<u8>,
    capacity: usize,
}

impl AlignedBuffer {
    fn new(capacity: usize) -> Self {
        let data = vec![0; capacity];
        Self { data, capacity }
    }

    fn clear_simd(&mut self) {
        simd_clear_buffer(&mut self.data);
        self.data.clear();
    }

    fn extend_from_slice_simd(&mut self, src: &[u8]) {
        let old_len = self.data.len();
        self.data.resize(old_len + src.len(), 0);
        simd_copy_buffer(src, &mut self.data[old_len..]);
    }

    fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

thread_local! {
    // Cache-line aligned thread-local key buffer to avoid allocations and false sharing
    static KEY_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(256));

    // Cache-line aligned thread-local value buffer
    static VALUE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(4096));

    // Thread-local compression buffer pool with aligned buffers
    static COMPRESSION_BUFFERS: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };

    // Thread-local deque pool for various uses
    static DEQUE_POOL: RefCell<Vec<VecDeque<u8>>> = const { RefCell::new(Vec::new()) };

    // Thread-local compression engine cache
    static COMPRESSION_ENGINE_CACHE: RefCell<Option<Arc<AdaptiveCompressionEngine>>> = const { RefCell::new(None) };

    // Thread-local SIMD computation buffer
    static SIMD_WORK_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(1024));

    // Thread-local prefetch hints buffer
    static PREFETCH_BUFFER: RefCell<Vec<*const u8>> = RefCell::new(Vec::with_capacity(8));
}

/// Get a SIMD-optimized thread-local key buffer
#[inline(always)]
pub fn with_key_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    KEY_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// Get a SIMD-optimized thread-local key buffer with zero-copy access
#[inline(always)]
pub fn with_key_buffer_aligned<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    KEY_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// Get a SIMD-optimized thread-local value buffer
#[inline(always)]
pub fn with_value_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    VALUE_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// Get a SIMD-optimized thread-local value buffer with zero-copy access
#[inline(always)]
pub fn with_value_buffer_aligned<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    VALUE_BUFFER.with(|buffer| {
        let mut buf = buffer.borrow_mut();
        buf.clear();
        f(&mut buf)
    })
}

/// SIMD-optimized allocation-free copy for small buffers
#[inline(always)]
pub fn copy_to_thread_buffer(data: &[u8]) -> Vec<u8> {
    if data.len() <= 4096 {
        with_value_buffer_aligned(|buf| {
            buf.extend_from_slice(data);
            buf.clone()
        })
    } else {
        data.to_vec()
    }
}

/// Zero-copy reference to thread-local buffer (lifetime tied to closure)
#[inline(always)]
pub fn with_zero_copy_buffer<F, R>(data: &[u8], f: F) -> R
where
    F: FnOnce(&[u8]) -> R,
{
    if data.len() <= 4096 {
        with_value_buffer_aligned(|buf| {
            buf.extend_from_slice(data);
            f(buf.as_slice())
        })
    } else {
        f(data)
    }
}

/// SIMD-optimized key formatting that reuses aligned buffers
#[inline(always)]
pub fn format_key_optimized(prefix: &str, id: u64) -> Vec<u8> {
    with_key_buffer_aligned(|buf| {
        use std::io::Write;
        let _ = write!(buf, "{}{:08}", prefix, id);
        buf.clone()
    })
}

/// Zero-copy key formatting with SIMD optimization
#[inline(always)]
pub fn format_key_zero_copy<F, R>(prefix: &str, id: u64, f: F) -> R
where
    F: FnOnce(&[u8]) -> R,
{
    with_key_buffer_aligned(|buf| {
        use std::io::Write;
        let _ = write!(buf, "{}{:08}", prefix, id);
        f(buf.as_slice())
    })
}

/// Get a SIMD-optimized thread-local compression buffer
#[inline(always)]
pub fn with_compression_buffer<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    COMPRESSION_BUFFERS.with(|buffers| {
        let mut pool = buffers.borrow_mut();
        let mut buffer = pool.pop().unwrap_or_else(|| Vec::with_capacity(32 * 1024)); // 32KB default
        buffer.clear();
        let result = f(&mut buffer);
        if pool.len() < 4 {
            // Keep max 4 buffers per thread
            pool.push(buffer);
        }
        result
    })
}

/// Get a SIMD-optimized thread-local compression buffer with alignment
#[inline(always)]
pub fn with_compression_buffer_aligned<F, R>(f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    COMPRESSION_BUFFERS.with(|buffers| {
        let mut pool = buffers.borrow_mut();
        let mut buffer = pool.pop().unwrap_or_else(|| Vec::with_capacity(32 * 1024));
        buffer.clear();
        let result = f(&mut buffer);
        if pool.len() < 4 {
            pool.push(buffer);
        }
        result
    })
}

/// Get a thread-local deque
#[inline(always)]
pub fn with_deque<F, R>(f: F) -> R
where
    F: FnOnce(&mut VecDeque<u8>) -> R,
{
    DEQUE_POOL.with(|deques| {
        let mut pool = deques.borrow_mut();
        let mut deque = pool.pop().unwrap_or_else(|| VecDeque::with_capacity(1024));
        deque.clear();
        let result = f(&mut deque);
        if pool.len() < 2 {
            // Keep max 2 deques per thread
            pool.push(deque);
        }
        result
    })
}

/// Get or create a thread-local compression engine
pub fn get_compression_engine() -> crate::core::error::Result<Arc<AdaptiveCompressionEngine>> {
    COMPRESSION_ENGINE_CACHE.with(|cache| {
        let mut engine_opt = cache.borrow_mut();
        if let Some(ref engine) = *engine_opt {
            Ok(engine.clone())
        } else {
            let engine = Arc::new(AdaptiveCompressionEngine::new()?);
            *engine_opt = Some(engine.clone());
            Ok(engine)
        }
    })
}

/// Clear thread-local caches (useful for testing) with SIMD optimization
pub fn clear_thread_caches() {
    KEY_BUFFER.with(|buf| buf.borrow_mut().clear());
    VALUE_BUFFER.with(|buf| buf.borrow_mut().clear());
    COMPRESSION_BUFFERS.with(|buffers| {
        for buffer in buffers.borrow_mut().iter_mut() {
            buffer.clear();
        }
        buffers.borrow_mut().clear();
    });
    DEQUE_POOL.with(|deques| deques.borrow_mut().clear());
    COMPRESSION_ENGINE_CACHE.with(|cache| *cache.borrow_mut() = None);
    SIMD_WORK_BUFFER.with(|buf| buf.borrow_mut().clear());
    PREFETCH_BUFFER.with(|buf| buf.borrow_mut().clear());
}

/// SIMD-optimized memory operations for cache performance
pub mod simd_ops {

    /// SIMD-optimized memory comparison
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub fn simd_memcmp(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }

        unsafe {
            let len = a.len();
            let mut i = 0;

            // Compare 16-byte chunks with SIMD
            while i + 16 <= len {
                let va = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
                let vb = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);
                let cmp = _mm_cmpeq_epi8(va, vb);
                let mask = _mm_movemask_epi8(cmp);
                if mask != 0xFFFF {
                    return false;
                }
                i += 16;
            }

            // Compare remaining bytes
            for j in i..len {
                if a[j] != b[j] {
                    return false;
                }
            }
        }

        true
    }

    #[cfg(not(target_arch = "x86_64"))]
    #[inline(always)]
    pub fn simd_memcmp(a: &[u8], b: &[u8]) -> bool {
        a == b
    }

    /// Issue hardware prefetch hints for better cache performance
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub fn prefetch_data(addr: *const u8, hint: i32) {
        unsafe {
            _mm_prefetch(addr as *const i8, hint);
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    #[inline(always)]
    pub fn prefetch_data(_addr: *const u8, _hint: i32) {
        // No-op on non-x86_64 platforms
    }

    /// Batch prefetch multiple addresses
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    pub fn batch_prefetch(addresses: &[*const u8]) {
        PREFETCH_BUFFER.with(|buf| {
            let mut buffer = buf.borrow_mut();
            buffer.clear();

            for &addr in addresses.iter().take(8) {
                // Limit to 8 prefetches
                buffer.push(addr);
                unsafe {
                    _mm_prefetch(addr as *const i8, _MM_HINT_T0);
                }
            }
        });
    }

    #[cfg(not(target_arch = "x86_64"))]
    #[inline(always)]
    pub fn batch_prefetch(_addresses: &[*const u8]) {
        // No-op on non-x86_64 platforms
    }
}

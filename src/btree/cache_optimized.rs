use crate::error::{Error, Result};
use std::mem;

/// Cache line size for modern CPUs
const CACHE_LINE_SIZE: usize = 64;

/// Optimized B+Tree node layout for CPU cache efficiency
#[repr(C, align(64))]
pub struct CacheOptimizedNode {
    /// First cache line: hot metadata
    num_keys: u16,
    node_type: u8, // 0 = leaf, 1 = internal
    flags: u8,
    parent_id: u32,
    right_sibling: u32,
    _padding1: [u8; 48], // Pad to 64 bytes
    
    /// Second cache line: first few keys for fast binary search
    first_keys: [u32; 16], // Hashes of first 16 keys for quick filtering
    
    /// Remaining data: packed keys and values
    data: [u8; 4096 - 128], // Rest of the 4KB page
}

impl CacheOptimizedNode {
    /// Create a new empty node
    pub fn new(node_type: u8) -> Self {
        Self {
            num_keys: 0,
            node_type,
            flags: 0,
            parent_id: 0,
            right_sibling: 0,
            _padding1: [0; 48],
            first_keys: [0; 16],
            data: [0; 4096 - 128],
        }
    }
    
    /// Binary search using SIMD when available
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    pub unsafe fn find_key_simd(&self, key_hash: u32) -> Option<usize> {
        use std::arch::x86_64::*;
        
        // Load key hash into all lanes
        let target = _mm256_set1_epi32(key_hash as i32);
        
        // Process 8 keys at a time with AVX2
        for i in (0..self.num_keys as usize).step_by(8) {
            let keys = _mm256_loadu_si256(self.first_keys[i..].as_ptr() as *const __m256i);
            let cmp = _mm256_cmpeq_epi32(keys, target);
            let mask = _mm256_movemask_epi8(cmp);
            
            if mask != 0 {
                // Found a match, calculate exact position
                let pos = mask.trailing_zeros() / 4;
                return Some(i + pos as usize);
            }
        }
        
        None
    }
    
    /// Fallback non-SIMD search
    pub fn find_key(&self, key_hash: u32) -> Option<usize> {
        self.first_keys[..self.num_keys as usize]
            .iter()
            .position(|&h| h == key_hash)
    }
    
    /// Check if this node would benefit from splitting based on cache pressure
    pub fn should_split(&self) -> bool {
        // Split when we're using more than 3/4 of the data section
        let used_bytes = self.estimate_used_bytes();
        used_bytes > (self.data.len() * 3 / 4)
    }
    
    fn estimate_used_bytes(&self) -> usize {
        // Rough estimate: each entry is ~32 bytes on average
        self.num_keys as usize * 32
    }
}

/// Fast key comparison using SIMD
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
pub unsafe fn compare_keys_simd(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use std::arch::x86_64::*;
    
    let len = a.len().min(b.len());
    let chunks = len / 16;
    
    for i in 0..chunks {
        let a_chunk = _mm_loadu_si128(a[i*16..].as_ptr() as *const __m128i);
        let b_chunk = _mm_loadu_si128(b[i*16..].as_ptr() as *const __m128i);
        
        let cmp = _mm_cmpestri(
            a_chunk, 16,
            b_chunk, 16,
            _SIDD_CMP_EQUAL_EACH | _SIDD_NEGATIVE_POLARITY
        );
        
        if cmp != 16 {
            return a[i*16 + cmp as usize].cmp(&b[i*16 + cmp as usize]);
        }
    }
    
    // Compare remaining bytes
    a[chunks*16..len].cmp(&b[chunks*16..len])
        .then(a.len().cmp(&b.len()))
}

/// Prefetch next node for better performance
#[inline(always)]
pub fn prefetch_node(addr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::_mm_prefetch;
        _mm_prefetch(addr as *const i8, 3); // Prefetch to all cache levels
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cache_aligned() {
        // Verify the node is cache-line aligned
        assert_eq!(mem::align_of::<CacheOptimizedNode>(), CACHE_LINE_SIZE);
        assert_eq!(mem::size_of::<CacheOptimizedNode>(), 4096);
    }
    
    #[test]
    fn test_node_creation() {
        let node = CacheOptimizedNode::new(0);
        assert_eq!(node.num_keys, 0);
        assert_eq!(node.node_type, 0);
    }
}
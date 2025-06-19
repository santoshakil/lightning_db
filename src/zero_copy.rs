use std::mem::MaybeUninit;

/// Small key optimization - avoid heap allocation for keys <= 32 bytes
#[derive(Clone)]
pub enum SmallKey {
    Inline([u8; 32], u8), // data + length
    Heap(Vec<u8>),
}

impl SmallKey {
    #[inline(always)]
    pub fn new(key: &[u8]) -> Self {
        if key.len() <= 32 {
            let mut data = [0u8; 32];
            data[..key.len()].copy_from_slice(key);
            SmallKey::Inline(data, key.len() as u8)
        } else {
            SmallKey::Heap(key.to_vec())
        }
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            SmallKey::Inline(data, len) => &data[..*len as usize],
            SmallKey::Heap(vec) => vec.as_slice(),
        }
    }
}

/// Stack-allocated buffer for temporary operations
pub struct StackBuffer<const N: usize> {
    data: [MaybeUninit<u8>; N],
    len: usize,
}

impl<const N: usize> StackBuffer<N> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            data: unsafe { MaybeUninit::uninit().assume_init() },
            len: 0,
        }
    }

    #[inline(always)]
    pub fn from_slice(slice: &[u8]) -> Option<Self> {
        if slice.len() > N {
            return None;
        }
        
        let mut buffer = Self::new();
        unsafe {
            std::ptr::copy_nonoverlapping(
                slice.as_ptr(),
                buffer.data.as_mut_ptr() as *mut u8,
                slice.len(),
            );
        }
        buffer.len = slice.len();
        Some(buffer)
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const u8,
                self.len,
            )
        }
    }
}

/// Fast hash function for keys
#[inline(always)]
pub fn fast_hash(key: &[u8]) -> u64 {
    if key.len() >= 8 {
        // Use first 8 bytes as hash for speed
        u64::from_le_bytes(key[..8].try_into().unwrap())
    } else {
        // Simple hash for small keys
        let mut hash = 0u64;
        for (i, &byte) in key.iter().enumerate() {
            hash |= (byte as u64) << (i * 8);
        }
        hash
    }
}

/// SIMD-optimized key comparison
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn fast_key_compare(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    // For small keys, use simple comparison
    if a.len() < 16 {
        return a == b;
    }

    // For larger keys, use SIMD if available
    #[cfg(target_feature = "sse2")]
    unsafe {
        use std::arch::x86_64::*;
        
        let chunks = a.len() / 16;
        let remainder = a.len() % 16;
        
        for i in 0..chunks {
            let offset = i * 16;
            let a_vec = _mm_loadu_si128(a[offset..].as_ptr() as *const __m128i);
            let b_vec = _mm_loadu_si128(b[offset..].as_ptr() as *const __m128i);
            let cmp = _mm_cmpeq_epi8(a_vec, b_vec);
            let mask = _mm_movemask_epi8(cmp);
            if mask != 0xFFFF {
                return false;
            }
        }
        
        // Compare remainder
        let offset = chunks * 16;
        a[offset..] == b[offset..]
    }
    
    #[cfg(not(target_feature = "sse2"))]
    {
        a == b
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub fn fast_key_compare(a: &[u8], b: &[u8]) -> bool {
    a == b
}
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use std::cmp::Ordering;

/// SIMD-optimized key comparison for x86_64 and ARM
pub struct SimdKeyComparator;

impl SimdKeyComparator {
    /// Compare two byte slices using SIMD instructions
    #[cfg(target_arch = "x86_64")]
    pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
        unsafe {
            if is_x86_feature_detected!("avx2") {
                Self::compare_avx2(a, b)
            } else if is_x86_feature_detected!("sse2") {
                Self::compare_sse2(a, b)
            } else {
                Self::compare_scalar(a, b)
            }
        }
    }

    #[cfg(target_arch = "aarch64")]
    pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
        unsafe {
            if std::arch::is_aarch64_feature_detected!("neon") {
                Self::compare_neon(a, b)
            } else {
                Self::compare_scalar(a, b)
            }
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    pub fn compare(a: &[u8], b: &[u8]) -> Ordering {
        Self::compare_scalar(a, b)
    }

    /// AVX2 implementation (32 bytes at a time)
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn compare_avx2(a: &[u8], b: &[u8]) -> Ordering {
        let len = a.len().min(b.len());
        let chunks = len / 32;

        for i in 0..chunks {
            let offset = i * 32;
            let a_vec = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
            let b_vec = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);

            let cmp = _mm256_cmpeq_epi8(a_vec, b_vec);
            let mask = _mm256_movemask_epi8(cmp);

            if mask != -1 {
                // Found difference, compare byte by byte in this chunk
                for j in 0..32 {
                    let idx = offset + j;
                    match a[idx].cmp(&b[idx]) {
                        Ordering::Equal => continue,
                        other => return other,
                    }
                }
            }
        }

        // Compare remaining bytes
        let remainder_start = chunks * 32;
        Self::compare_scalar(&a[remainder_start..], &b[remainder_start..])
    }

    /// SSE2 implementation (16 bytes at a time)
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    unsafe fn compare_sse2(a: &[u8], b: &[u8]) -> Ordering {
        let len = a.len().min(b.len());
        let chunks = len / 16;

        for i in 0..chunks {
            let offset = i * 16;
            let a_vec = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
            let b_vec = _mm_loadu_si128(b.as_ptr().add(offset) as *const __m128i);

            let cmp = _mm_cmpeq_epi8(a_vec, b_vec);
            let mask = _mm_movemask_epi8(cmp);

            if mask != 0xFFFF {
                // Found difference
                for j in 0..16 {
                    let idx = offset + j;
                    match a[idx].cmp(&b[idx]) {
                        Ordering::Equal => continue,
                        other => return other,
                    }
                }
            }
        }

        let remainder_start = chunks * 16;
        Self::compare_scalar(&a[remainder_start..], &b[remainder_start..])
    }

    /// NEON implementation for ARM
    #[cfg(target_arch = "aarch64")]
    unsafe fn compare_neon(a: &[u8], b: &[u8]) -> Ordering {
        use std::arch::aarch64::*;

        let len = a.len().min(b.len());
        let chunks = len / 16;

        for i in 0..chunks {
            let offset = i * 16;
            let a_vec = vld1q_u8(a.as_ptr().add(offset));
            let b_vec = vld1q_u8(b.as_ptr().add(offset));

            let cmp = vceqq_u8(a_vec, b_vec);
            let reduced = vminvq_u8(cmp);

            if reduced != 0xFF {
                // Found difference
                for j in 0..16 {
                    let idx = offset + j;
                    match a[idx].cmp(&b[idx]) {
                        Ordering::Equal => continue,
                        other => return other,
                    }
                }
            }
        }

        let remainder_start = chunks * 16;
        Self::compare_scalar(&a[remainder_start..], &b[remainder_start..])
    }

    /// Fallback scalar implementation
    fn compare_scalar(a: &[u8], b: &[u8]) -> Ordering {
        let len = a.len().min(b.len());

        // Compare common prefix
        for i in 0..len {
            match a[i].cmp(&b[i]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }

        // If all bytes equal, compare lengths
        a.len().cmp(&b.len())
    }
}

/// SIMD-accelerated key search in sorted array
pub fn simd_find_key(keys: &[Vec<u8>], target: &[u8]) -> Option<usize> {
    if keys.is_empty() {
        return None;
    }

    // Binary search with SIMD comparison
    let mut left = 0;
    let mut right = keys.len();

    while left < right {
        let mid = left + (right - left) / 2;

        match simd_compare_keys(&keys[mid], target) {
            Ordering::Less => left = mid + 1,
            Ordering::Greater => right = mid,
            Ordering::Equal => return Some(mid),
        }
    }

    None
}

/// Public wrapper for SIMD key comparison
#[inline]
pub fn simd_compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    SimdKeyComparator::compare(a, b)
}

/// Batch key comparison using SIMD
#[cfg(target_arch = "x86_64")]
pub fn simd_batch_compare_avx2(keys: &[&[u8]], target: &[u8]) -> Vec<bool> {
    if !is_x86_feature_detected!("avx2") {
        return keys.iter().map(|k| k == &target).collect();
    }

    unsafe { simd_batch_compare_avx2_impl(keys, target) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_batch_compare_avx2_impl(keys: &[&[u8]], target: &[u8]) -> Vec<bool> {
    keys.iter()
        .map(|key| {
            if key.len() != target.len() {
                false
            } else {
                simd_compare_keys(key, target) == Ordering::Equal
            }
        })
        .collect()
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_batch_compare_avx2(keys: &[&[u8]], target: &[u8]) -> Vec<bool> {
    keys.iter().map(|k| k == &target).collect()
}

/// Fast memcmp using SIMD
#[cfg(target_arch = "x86_64")]
pub fn simd_memeq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    unsafe {
        if is_x86_feature_detected!("avx2") {
            simd_memeq_avx2(a, b)
        } else if is_x86_feature_detected!("sse2") {
            simd_memeq_sse2(a, b)
        } else {
            a == b
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_memeq_avx2(a: &[u8], b: &[u8]) -> bool {
    let len = a.len();
    let chunks = len / 32;

    for i in 0..chunks {
        let offset = i * 32;
        let a_vec = _mm256_loadu_si256(a.as_ptr().add(offset) as *const __m256i);
        let b_vec = _mm256_loadu_si256(b.as_ptr().add(offset) as *const __m256i);

        let cmp = _mm256_cmpeq_epi8(a_vec, b_vec);
        let mask = _mm256_movemask_epi8(cmp);

        if mask != -1 {
            return false;
        }
    }

    let remainder_start = chunks * 32;
    &a[remainder_start..] == &b[remainder_start..]
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn simd_memeq_sse2(a: &[u8], b: &[u8]) -> bool {
    let len = a.len();
    let chunks = len / 16;

    for i in 0..chunks {
        let offset = i * 16;
        let a_vec = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
        let b_vec = _mm_loadu_si128(b.as_ptr().add(offset) as *const __m128i);

        let cmp = _mm_cmpeq_epi8(a_vec, b_vec);
        let mask = _mm_movemask_epi8(cmp);

        if mask != 0xFFFF {
            return false;
        }
    }

    let remainder_start = chunks * 16;
    &a[remainder_start..] == &b[remainder_start..]
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_memeq(a: &[u8], b: &[u8]) -> bool {
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_compare() {
        let a = b"hello world this is a test";
        let b = b"hello world this is a test";
        let c = b"hello world this is b test";
        let d = b"hello world";

        assert_eq!(simd_compare_keys(a, b), Ordering::Equal);
        assert_eq!(simd_compare_keys(a, c), Ordering::Less);
        assert_eq!(simd_compare_keys(c, a), Ordering::Greater);
        assert_eq!(simd_compare_keys(a, d), Ordering::Greater);
        assert_eq!(simd_compare_keys(d, a), Ordering::Less);
    }

    #[test]
    fn test_simd_find() {
        let keys = vec![
            b"aaa".to_vec(),
            b"bbb".to_vec(),
            b"ccc".to_vec(),
            b"ddd".to_vec(),
            b"eee".to_vec(),
        ];

        assert_eq!(simd_find_key(&keys, b"ccc"), Some(2));
        assert_eq!(simd_find_key(&keys, b"fff"), None);
        assert_eq!(simd_find_key(&keys, b"aaa"), Some(0));
        assert_eq!(simd_find_key(&keys, b"eee"), Some(4));
    }

    #[test]
    fn test_simd_memeq() {
        let a = vec![0u8; 1000];
        let mut b = vec![0u8; 1000];

        assert!(simd_memeq(&a, &b));

        b[500] = 1;
        assert!(!simd_memeq(&a, &b));

        let c = vec![0u8; 999];
        assert!(!simd_memeq(&a, &c));
    }

    #[test]
    fn test_batch_compare() {
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3", b"key2", b"key4"];
        let target = b"key2";

        let results = simd_batch_compare_avx2(&keys, target);
        assert_eq!(results, vec![false, true, false, true, false]);
    }
}

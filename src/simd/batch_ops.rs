#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD batch validation for keys
pub fn simd_batch_validate(keys: &[&[u8]], min_len: usize, max_len: usize) -> Vec<bool> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { simd_batch_validate_avx2(keys, min_len, max_len) }
        } else {
            batch_validate_scalar(keys, min_len, max_len)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        batch_validate_scalar(keys, min_len, max_len)
    }
}

fn batch_validate_scalar(keys: &[&[u8]], min_len: usize, max_len: usize) -> Vec<bool> {
    keys.iter()
        .map(|k| k.len() >= min_len && k.len() <= max_len)
        .collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_batch_validate_avx2(keys: &[&[u8]], min_len: usize, max_len: usize) -> Vec<bool> {
    // For now, just validate lengths
    // Could be extended to validate content patterns
    batch_validate_scalar(keys, min_len, max_len)
}

/// SIMD batch hash computation
pub fn simd_batch_hash(keys: &[&[u8]]) -> Vec<u64> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { simd_batch_hash_avx2(keys) }
        } else if is_x86_feature_detected!("sse4.2") {
            unsafe { simd_batch_hash_sse42(keys) }
        } else {
            batch_hash_scalar(keys)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        batch_hash_scalar(keys)
    }
}

fn batch_hash_scalar(keys: &[&[u8]]) -> Vec<u64> {
    keys.iter()
        .map(|k| {
            // Simple FNV-1a hash
            let mut hash = 14695981039346656037u64;
            for &byte in k.iter() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(1099511628211);
            }
            hash
        })
        .collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn simd_batch_hash_sse42(keys: &[&[u8]]) -> Vec<u64> {
    keys.iter()
        .map(|k| {
            let mut hash = 0u64;
            let chunks = k.len() / 8;

            // Process 8-byte chunks with CRC32
            for i in 0..chunks {
                let offset = i * 8;
                let chunk = *(k.as_ptr().add(offset) as *const u64);
                hash = _mm_crc32_u64(hash, chunk);
            }

            // Process remaining bytes
            let remainder_start = chunks * 8;
            for &byte in &k[remainder_start..] {
                hash = _mm_crc32_u8(hash as u32, byte) as u64;
            }

            hash
        })
        .collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_batch_hash_avx2(keys: &[&[u8]]) -> Vec<u64> {
    // AVX2 doesn't have CRC instructions, fall back to SSE4.2 if available
    if is_x86_feature_detected!("sse4.2") {
        simd_batch_hash_sse42(keys)
    } else {
        batch_hash_scalar(keys)
    }
}

/// SIMD prefix matching
#[cfg(target_arch = "x86_64")]
pub fn simd_prefix_match(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    if is_x86_feature_detected!("avx2") {
        unsafe { simd_prefix_match_avx2(keys, prefix) }
    } else if is_x86_feature_detected!("sse2") {
        unsafe { simd_prefix_match_sse2(keys, prefix) }
    } else {
        prefix_match_scalar(keys, prefix)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_prefix_match(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    prefix_match_scalar(keys, prefix)
}

fn prefix_match_scalar(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    keys.iter().map(|k| k.starts_with(prefix)).collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn simd_prefix_match_sse2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    let prefix_len = prefix.len();
    if prefix_len == 0 {
        return vec![true; keys.len()];
    }

    keys.iter()
        .map(|key| {
            if key.len() < prefix_len {
                return false;
            }

            let chunks = prefix_len / 16;

            for i in 0..chunks {
                let offset = i * 16;
                let key_vec = _mm_loadu_si128(key.as_ptr().add(offset) as *const __m128i);
                let prefix_vec = _mm_loadu_si128(prefix.as_ptr().add(offset) as *const __m128i);

                let cmp = _mm_cmpeq_epi8(key_vec, prefix_vec);
                let mask = _mm_movemask_epi8(cmp);

                if mask != 0xFFFF {
                    return false;
                }
            }

            let remainder_start = chunks * 16;
            &key[remainder_start..prefix_len] == &prefix[remainder_start..]
        })
        .collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_prefix_match_avx2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    let prefix_len = prefix.len();
    if prefix_len == 0 {
        return vec![true; keys.len()];
    }

    keys.iter()
        .map(|key| {
            if key.len() < prefix_len {
                return false;
            }

            let chunks = prefix_len / 32;

            for i in 0..chunks {
                let offset = i * 32;
                let key_vec = _mm256_loadu_si256(key.as_ptr().add(offset) as *const __m256i);
                let prefix_vec = _mm256_loadu_si256(prefix.as_ptr().add(offset) as *const __m256i);

                let cmp = _mm256_cmpeq_epi8(key_vec, prefix_vec);
                let mask = _mm256_movemask_epi8(cmp);

                if mask != -1 {
                    return false;
                }
            }

            let remainder_start = chunks * 32;
            &key[remainder_start..prefix_len] == &prefix[remainder_start..]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_validate() {
        let keys: Vec<&[u8]> = vec![b"a", b"ab", b"abc", b"abcd", b"abcde"];

        let results = simd_batch_validate(&keys, 2, 4);
        assert_eq!(results, vec![false, true, true, true, false]);
    }

    #[test]
    fn test_batch_hash() {
        let keys: Vec<&[u8]> = vec![b"key1", b"key2", b"key3"];
        let hashes = simd_batch_hash(&keys);

        // Verify hashes are different
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[1], hashes[2]);
        assert_ne!(hashes[0], hashes[2]);

        // Verify consistency
        let hashes2 = simd_batch_hash(&keys);
        assert_eq!(hashes, hashes2);
    }

    #[test]
    fn test_prefix_match() {
        let keys: Vec<&[u8]> = vec![
            b"prefix_test",
            b"prefix_other",
            b"different",
            b"pref",
            b"prefix",
        ];

        let results = simd_prefix_match(&keys, b"prefix");
        assert_eq!(results, vec![true, true, false, false, true]);
    }
}

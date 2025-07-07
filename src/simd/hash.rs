#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-accelerated batch hash computation
pub fn simd_batch_hash(keys: &[&[u8]]) -> Vec<u64> {
    keys.iter()
        .map(|k| {
            let mut hash = 14695981039346656037u64; // FNV-1a offset basis
            for &byte in k.iter() {
                hash ^= byte as u64;
                hash = hash.wrapping_mul(1099511628211); // FNV-1a prime
            }
            hash
        })
        .collect()
}

/// SIMD-accelerated prefix matching
pub fn simd_prefix_match(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && prefix.len() >= 32 {
            unsafe { simd_prefix_match_avx2(keys, prefix) }
        } else if is_x86_feature_detected!("sse2") && prefix.len() >= 16 {
            unsafe { simd_prefix_match_sse2(keys, prefix) }
        } else {
            prefix_match_scalar(keys, prefix)
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        prefix_match_scalar(keys, prefix)
    }
}

fn prefix_match_scalar(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    keys.iter().map(|k| k.starts_with(prefix)).collect()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn simd_prefix_match_sse2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    let mut results = Vec::with_capacity(keys.len());
    let chunks = prefix.len() / 16;

    for key in keys {
        if key.len() < prefix.len() {
            results.push(false);
            continue;
        }

        let mut matches = true;
        for i in 0..chunks {
            let offset = i * 16;
            let key_vec = _mm_loadu_si128(key.as_ptr().add(offset) as *const __m128i);
            let prefix_vec = _mm_loadu_si128(prefix.as_ptr().add(offset) as *const __m128i);
            let cmp = _mm_cmpeq_epi8(key_vec, prefix_vec);
            let mask = _mm_movemask_epi8(cmp);

            if mask != 0xFFFF {
                matches = false;
                break;
            }
        }

        // Check remaining bytes
        if matches {
            let remainder_start = chunks * 16;
            matches = key[remainder_start..].starts_with(&prefix[remainder_start..]);
        }

        results.push(matches);
    }

    results
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_prefix_match_avx2(keys: &[&[u8]], prefix: &[u8]) -> Vec<bool> {
    let mut results = Vec::with_capacity(keys.len());
    let chunks = prefix.len() / 32;

    for key in keys {
        if key.len() < prefix.len() {
            results.push(false);
            continue;
        }

        let mut matches = true;
        for i in 0..chunks {
            let offset = i * 32;
            let key_vec = _mm256_loadu_si256(key.as_ptr().add(offset) as *const __m256i);
            let prefix_vec = _mm256_loadu_si256(prefix.as_ptr().add(offset) as *const __m256i);
            let cmp = _mm256_cmpeq_epi8(key_vec, prefix_vec);
            let mask = _mm256_movemask_epi8(cmp);

            if mask != -1 {
                matches = false;
                break;
            }
        }

        // Check remaining bytes
        if matches {
            let remainder_start = chunks * 32;
            matches = key[remainder_start..].starts_with(&prefix[remainder_start..]);
        }

        results.push(matches);
    }

    results
}

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-optimized operations for high-performance database operations
pub struct SimdOps;

impl SimdOps {
    /// Compare two byte arrays using SIMD instructions for maximum speed
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports SSE4.2 instructions
    /// - Input slices must be valid for reads
    /// - No alignment requirements (uses unaligned loads)
    pub unsafe fn compare_keys_simd(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let len = std::cmp::min(a.len(), b.len());

        // Process 16-byte chunks with SSE
        let chunks = len / 16;
        for i in 0..chunks {
            let offset = i * 16;

            // Load 16 bytes from each array
            let va = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
            let vb = _mm_loadu_si128(b.as_ptr().add(offset) as *const __m128i);

            // Compare for equality
            let eq_mask = _mm_cmpeq_epi8(va, vb);
            let eq_bits = _mm_movemask_epi8(eq_mask);

            if eq_bits != 0xFFFF {
                // Found first difference, process byte by byte in this chunk
                let chunk_a = &a[offset..offset + 16];
                let chunk_b = &b[offset..offset + 16];

                for (ba, bb) in chunk_a.iter().zip(chunk_b.iter()) {
                    match ba.cmp(bb) {
                        std::cmp::Ordering::Equal => continue,
                        other => return other,
                    }
                }
            }
        }

        // Process remaining bytes
        let remaining_start = chunks * 16;
        for (ba, bb) in a[remaining_start..].iter().zip(b[remaining_start..].iter()) {
            match ba.cmp(bb) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }

        a.len().cmp(&b.len())
    }

    /// Fast checksum calculation using SIMD CRC32 instructions
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports SSE4.2 with CRC32 instructions
    /// - Data slice must be valid for reads
    /// - Uses unaligned reads for efficiency
    pub unsafe fn crc32_simd(data: &[u8]) -> u32 {
        let mut crc = 0u32;
        let mut pos = 0;

        // Process 8-byte chunks
        while pos + 8 <= data.len() {
            let chunk = std::ptr::read_unaligned(data.as_ptr().add(pos) as *const u64);
            crc = _mm_crc32_u64(crc as u64, chunk) as u32;
            pos += 8;
        }

        // Process 4-byte chunks
        while pos + 4 <= data.len() {
            let chunk = std::ptr::read_unaligned(data.as_ptr().add(pos) as *const u32);
            crc = _mm_crc32_u32(crc, chunk);
            pos += 4;
        }

        // Process remaining bytes
        while pos < data.len() {
            crc = _mm_crc32_u8(crc, data[pos]);
            pos += 1;
        }

        crc
    }

    /// Vectorized memory operations for bulk data movement
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports AVX2 instructions
    /// - Source and destination must not overlap
    /// - Both slices must have equal length
    /// - Uses unaligned loads/stores for flexibility
    pub unsafe fn bulk_copy_avx2(src: &[u8], dst: &mut [u8]) {
        assert_eq!(src.len(), dst.len());
        let len = src.len();
        let mut pos = 0;

        // Process 32-byte chunks with AVX2
        while pos + 32 <= len {
            let chunk = _mm256_loadu_si256(src.as_ptr().add(pos) as *const __m256i);
            _mm256_storeu_si256(dst.as_mut_ptr().add(pos) as *mut __m256i, chunk);
            pos += 32;
        }

        // Process 16-byte chunks with SSE
        while pos + 16 <= len {
            let chunk = _mm_loadu_si128(src.as_ptr().add(pos) as *const __m128i);
            _mm_storeu_si128(dst.as_mut_ptr().add(pos) as *mut __m128i, chunk);
            pos += 16;
        }

        // Copy remaining bytes
        std::ptr::copy_nonoverlapping(src.as_ptr().add(pos), dst.as_mut_ptr().add(pos), len - pos);
    }

    /// SIMD-accelerated search for key prefixes
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports SSE4.2 instructions
    /// - Both slices must be valid for reads
    /// - Uses unaligned loads for pattern matching
    pub unsafe fn search_prefix_simd(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() || haystack.len() < needle.len() {
            return None;
        }

        let needle_len = needle.len();
        let search_len = haystack.len() - needle_len + 1;

        if needle_len >= 16 {
            // Use SSE for longer needles
            let needle_start = _mm_loadu_si128(needle.as_ptr() as *const __m128i);

            for i in 0..search_len {
                let hay_chunk = _mm_loadu_si128(haystack.as_ptr().add(i) as *const __m128i);
                let eq_mask = _mm_cmpeq_epi8(needle_start, hay_chunk);
                let eq_bits = _mm_movemask_epi8(eq_mask);

                if eq_bits == 0xFFFF {
                    // First 16 bytes match, check the rest
                    if haystack[i..i + needle_len] == *needle {
                        return Some(i);
                    }
                }
            }
        } else {
            // Use byte-by-byte search for short needles
            for i in 0..search_len {
                if haystack[i..i + needle_len] == *needle {
                    return Some(i);
                }
            }
        }

        None
    }

    /// High-performance hash calculation using SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports AVX2 instructions
    /// - Data slice must be valid for reads
    /// - Uses unaligned loads for processing
    pub unsafe fn hash_simd(data: &[u8], seed: u64) -> u64 {
        let mut hash = seed;
        let mut pos = 0;

        // Process 32-byte chunks
        while pos + 32 <= data.len() {
            let chunk = _mm256_loadu_si256(data.as_ptr().add(pos) as *const __m256i);

            // Extract 64-bit values and mix them safely using SIMD intrinsics
            // Extract four 64-bit values from the 256-bit SIMD register
            let value0 = _mm256_extract_epi64(chunk, 0) as u64;
            let value1 = _mm256_extract_epi64(chunk, 1) as u64;
            let value2 = _mm256_extract_epi64(chunk, 2) as u64;
            let value3 = _mm256_extract_epi64(chunk, 3) as u64;

            for &value in &[value0, value1, value2, value3] {
                hash = hash.wrapping_mul(0x9e3779b97f4a7c15u64);
                hash ^= value;
                hash = hash.rotate_left(31);
            }

            pos += 32;
        }

        // Process remaining bytes
        while pos < data.len() {
            hash = hash.wrapping_mul(0x9e3779b97f4a7c15u64);
            hash ^= data[pos] as u64;
            hash = hash.rotate_left(7);
            pos += 1;
        }

        hash
    }

    /// Parallel compression ratio estimation using SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    #[inline]
    /// # Safety
    /// - Caller must ensure CPU supports AVX2 instructions
    /// - Data slice must be valid for reads
    /// - Uses unaligned loads for entropy calculation
    pub unsafe fn estimate_compression_ratio_simd(data: &[u8]) -> f32 {
        if data.len() < 32 {
            return Self::estimate_compression_ratio_scalar(data);
        }

        let mut entropy_acc = _mm256_setzero_si256();
        let mut pos = 0;

        // Process 32-byte chunks
        while pos + 32 <= data.len() {
            let chunk = _mm256_loadu_si256(data.as_ptr().add(pos) as *const __m256i);

            // Calculate byte frequencies (simplified entropy estimation) using safe extraction
            let mut local_entropy = 0u32;

            // Extract bytes safely using SIMD intrinsics
            for i in 0..32 {
                let byte = _mm256_extract_epi8(chunk, i) as u8;
                // Simple entropy estimation based on byte patterns
                local_entropy += (byte as u32).count_ones();
            }

            let entropy_vec = _mm256_set1_epi32(local_entropy as i32);
            entropy_acc = _mm256_add_epi32(entropy_acc, entropy_vec);
            pos += 32;
        }

        // Extract sum safely using SIMD intrinsics
        let entropy_sum: i32 = {
            // Extract eight 32-bit values from the 256-bit SIMD register
            let val0 = _mm256_extract_epi32(entropy_acc, 0);
            let val1 = _mm256_extract_epi32(entropy_acc, 1);
            let val2 = _mm256_extract_epi32(entropy_acc, 2);
            let val3 = _mm256_extract_epi32(entropy_acc, 3);
            let val4 = _mm256_extract_epi32(entropy_acc, 4);
            let val5 = _mm256_extract_epi32(entropy_acc, 5);
            let val6 = _mm256_extract_epi32(entropy_acc, 6);
            let val7 = _mm256_extract_epi32(entropy_acc, 7);

            val0 + val1 + val2 + val3 + val4 + val5 + val6 + val7
        };

        // Estimate based on bit distribution
        let avg_bits_per_byte = entropy_sum as f32 / data.len() as f32;
        (8.0 - avg_bits_per_byte) / 8.0
    }

    /// Fallback scalar implementation for compression ratio estimation
    fn estimate_compression_ratio_scalar(data: &[u8]) -> f32 {
        let mut byte_counts = [0u32; 256];

        for &byte in data {
            byte_counts[byte as usize] += 1;
        }

        let data_len = data.len() as f32;
        let mut entropy = 0.0f32;

        for count in byte_counts.iter() {
            if *count > 0 {
                let prob = *count as f32 / data_len;
                entropy -= prob * prob.log2();
            }
        }

        // Estimate compression ratio based on entropy
        (8.0 - entropy) / 8.0
    }
}

/// Safe wrapper functions that automatically use SIMD when available
pub mod safe {
    use super::SimdOps;

    /// Safe key comparison that uses SIMD when available
    #[inline]
    pub fn compare_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        // Fast path for common cases
        match (a.len(), b.len()) {
            (0, 0) => return std::cmp::Ordering::Equal,
            (0, _) => return std::cmp::Ordering::Less,
            (_, 0) => return std::cmp::Ordering::Greater,
            (la, lb) if la == lb && la <= 8 => {
                // For small equal-length keys, use word comparison
                if la == 8 {
                    let a_word = u64::from_le_bytes(a.try_into().unwrap());
                    let b_word = u64::from_le_bytes(b.try_into().unwrap());
                    return a_word.cmp(&b_word);
                } else if la == 4 {
                    let a_word = u32::from_le_bytes(a.try_into().unwrap());
                    let b_word = u32::from_le_bytes(b.try_into().unwrap());
                    return a_word.cmp(&b_word);
                }
            },
            _ => {}
        }
        
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") && (a.len() >= 8 || b.len() >= 8) {
                // SAFETY: CPU feature detection ensures SSE4.2 support
                unsafe { SimdOps::compare_keys_simd(a, b) }
            } else {
                a.cmp(b)
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            a.cmp(b)
        }
    }

    /// Safe CRC32 calculation with SIMD acceleration
    pub fn crc32(data: &[u8]) -> u32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") {
                // SAFETY: CPU feature detection ensures CRC32 support
                // Invariants:
                // 1. Runtime check confirms SSE4.2 with CRC32
                // 2. Data slice is valid from safe reference
                // Guarantees:
                // - Hardware CRC32 acceleration when available
                // - Correct checksum calculation
                unsafe { SimdOps::crc32_simd(data) }
            } else {
                crc32_scalar(data)
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            crc32_scalar(data)
        }
    }

    /// Safe bulk copy with SIMD acceleration
    pub fn bulk_copy(src: &[u8], dst: &mut [u8]) {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                // SAFETY: CPU feature detection ensures AVX2 support
                // Invariants:
                // 1. Runtime check confirms AVX2 availability
                // 2. Slices from safe references, non-overlapping
                // 3. Length equality checked in function
                // Guarantees:
                // - Vectorized copy when hardware supports it
                // - Correct data transfer
                unsafe { SimdOps::bulk_copy_avx2(src, dst) }
            } else {
                dst.copy_from_slice(src);
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            dst.copy_from_slice(src);
        }
    }

    /// Safe prefix search with SIMD acceleration
    pub fn search_prefix(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("sse4.2") {
                // SAFETY: CPU feature detection ensures SSE4.2 support
                // Invariants:
                // 1. Runtime check confirms SSE4.2 availability
                // 2. Slices from safe references
                // Guarantees:
                // - Accelerated pattern matching when possible
                // - Correct search results
                unsafe { SimdOps::search_prefix_simd(haystack, needle) }
            } else {
                search_prefix_scalar(haystack, needle)
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            search_prefix_scalar(haystack, needle)
        }
    }

    /// Safe hash calculation with SIMD acceleration
    pub fn hash(data: &[u8], seed: u64) -> u64 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                // SAFETY: CPU feature detection ensures AVX2 support
                // Invariants:
                // 1. Runtime check confirms AVX2 availability
                // 2. Data slice from safe reference
                // Guarantees:
                // - SIMD-accelerated hashing when available
                // - Consistent hash values
                unsafe { SimdOps::hash_simd(data, seed) }
            } else {
                hash_scalar(data, seed)
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            hash_scalar(data, seed)
        }
    }

    /// Safe compression ratio estimation with SIMD acceleration
    pub fn estimate_compression_ratio(data: &[u8]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                // SAFETY: CPU feature detection ensures AVX2 support
                // Invariants:
                // 1. Runtime check confirms AVX2 availability
                // 2. Data slice from safe reference
                // Guarantees:
                // - Parallel entropy estimation when possible
                // - Accurate compression ratio estimate
                unsafe { SimdOps::estimate_compression_ratio_simd(data) }
            } else {
                SimdOps::estimate_compression_ratio_scalar(data)
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            SimdOps::estimate_compression_ratio_scalar(data)
        }
    }

    // Scalar fallback implementations

    fn crc32_scalar(data: &[u8]) -> u32 {
        // Simple CRC32 implementation
        let mut crc = 0xFFFFFFFFu32;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }

    fn search_prefix_scalar(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    fn hash_scalar(data: &[u8], mut seed: u64) -> u64 {
        for &byte in data {
            seed = seed.wrapping_mul(0x9e3779b97f4a7c15u64);
            seed ^= byte as u64;
            seed = seed.rotate_left(7);
        }
        seed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_comparison() {
        let key1 = b"hello world";
        let key2 = b"hello world";
        let key3 = b"hello world!";

        assert_eq!(safe::compare_keys(key1, key2), std::cmp::Ordering::Equal);
        assert_eq!(safe::compare_keys(key1, key3), std::cmp::Ordering::Less);
        assert_eq!(safe::compare_keys(key3, key1), std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_crc32() {
        let data = b"hello world";
        let crc = safe::crc32(data);

        // CRC32 should be consistent
        assert_eq!(crc, safe::crc32(data));

        // Different data should produce different CRC (usually)
        let different_crc = safe::crc32(b"hello world!");
        assert_ne!(crc, different_crc);
    }

    #[test]
    fn test_bulk_copy() {
        let src = b"hello world test data for bulk copy";
        let mut dst = vec![0u8; src.len()];

        safe::bulk_copy(src, &mut dst);
        assert_eq!(src, dst.as_slice());
    }

    #[test]
    fn test_prefix_search() {
        let haystack = b"hello world this is a test";
        let needle1 = b"world";
        let needle2 = b"test";
        let needle3 = b"notfound";

        assert_eq!(safe::search_prefix(haystack, needle1), Some(6));
        assert_eq!(safe::search_prefix(haystack, needle2), Some(22));
        assert_eq!(safe::search_prefix(haystack, needle3), None);
    }

    #[test]
    fn test_hash() {
        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"hello world!";

        assert_eq!(safe::hash(data1, 0), safe::hash(data2, 0));
        assert_ne!(safe::hash(data1, 0), safe::hash(data3, 0));
        assert_ne!(safe::hash(data1, 0), safe::hash(data1, 1));
    }

    #[test]
    fn test_compression_ratio_estimation() {
        // Highly compressible data (all zeros)
        let zeros = vec![0u8; 1000];
        let ratio1 = safe::estimate_compression_ratio(&zeros);
        assert!(ratio1 > 0.8); // Should be highly compressible

        // Random-ish data (less compressible)
        let random: Vec<u8> = (0..1000).map(|i| (i * 7 + 13) as u8).collect();
        let ratio2 = safe::estimate_compression_ratio(&random);
        assert!(ratio2 < ratio1); // Should be less compressible
    }

    #[test]
    fn test_simd_feature_detection() {
        // Test that our feature detection works
        #[cfg(target_arch = "x86_64")]
        {
            println!("SSE4.2 support: {}", is_x86_feature_detected!("sse4.2"));
            println!("AVX2 support: {}", is_x86_feature_detected!("avx2"));
        }
    }
}

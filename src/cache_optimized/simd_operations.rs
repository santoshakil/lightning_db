//! SIMD-Optimized Operations
//!
//! High-performance SIMD implementations for common database operations:
//! - Vectorized key comparisons
//! - Bulk data processing
//! - Checksum calculations
//! - String operations

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-optimized key comparison operations
pub struct SimdKeyOperations;

impl SimdKeyOperations {
    /// Compare multiple 32-bit keys with a search key using SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn compare_keys_u32(keys: &[u32], search_key: u32) -> Vec<bool> {
        let mut results = vec![false; keys.len()];
        let search_vec = _mm_set1_epi32(search_key as i32);
        
        let chunks = keys.chunks_exact(4);
        let remainder = chunks.remainder();
        
        for (chunk_idx, chunk) in chunks.enumerate() {
            let keys_vec = _mm_loadu_si128(chunk.as_ptr() as *const __m128i);
            let cmp_result = _mm_cmpeq_epi32(keys_vec, search_vec);
            let mask = _mm_movemask_epi8(cmp_result);
            
            // Extract comparison results
            for i in 0..4 {
                results[chunk_idx * 4 + i] = (mask & (0x000F << (i * 4))) != 0;
            }
        }
        
        // Handle remainder
        for (i, &key) in remainder.iter().enumerate() {
            results[chunks.len() * 4 + i] = key == search_key;
        }
        
        results
    }

    /// Find first key greater than or equal to search key
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn find_gte_u32(keys: &[u32], search_key: u32) -> Option<usize> {
        let search_vec = _mm_set1_epi32(search_key as i32);
        
        let chunks = keys.chunks_exact(4);
        let remainder = chunks.remainder();
        
        for (chunk_idx, chunk) in chunks.enumerate() {
            let keys_vec = _mm_loadu_si128(chunk.as_ptr() as *const __m128i);
            
            // Compare greater than or equal (not less than)
            let cmp_result = _mm_cmplt_epi32(keys_vec, search_vec);
            let mask = _mm_movemask_epi8(cmp_result);
            
            // Find first position where key >= search_key (not less than)
            for i in 0..4 {
                let is_less = (mask & (0x000F << (i * 4))) != 0;
                if !is_less {
                    return Some(chunk_idx * 4 + i);
                }
            }
        }
        
        // Check remainder
        for (i, &key) in remainder.iter().enumerate() {
            if key >= search_key {
                return Some(chunks.len() * 4 + i);
            }
        }
        
        None
    }

    /// Count keys in a range using SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn count_keys_in_range_u32(keys: &[u32], min_key: u32, max_key: u32) -> usize {
        let min_vec = _mm_set1_epi32(min_key as i32);
        let max_vec = _mm_set1_epi32(max_key as i32);
        let mut count = 0;
        
        let chunks = keys.chunks_exact(4);
        let remainder = chunks.remainder();
        
        for chunk in chunks {
            let keys_vec = _mm_loadu_si128(chunk.as_ptr() as *const __m128i);
            
            // Check if key >= min_key
            let gte_min = _mm_cmplt_epi32(min_vec, keys_vec);
            let eq_min = _mm_cmpeq_epi32(min_vec, keys_vec);
            let gte_min_final = _mm_or_si128(gte_min, eq_min);
            
            // Check if key <= max_key
            let lte_max = _mm_cmplt_epi32(keys_vec, max_vec);
            let eq_max = _mm_cmpeq_epi32(keys_vec, max_vec);
            let lte_max_final = _mm_or_si128(lte_max, eq_max);
            
            // Combine conditions
            let in_range = _mm_and_si128(gte_min_final, lte_max_final);
            let mask = _mm_movemask_epi8(in_range);
            
            // Count set bits
            for i in 0..4 {
                if (mask & (0x000F << (i * 4))) != 0 {
                    count += 1;
                }
            }
        }
        
        // Handle remainder
        for &key in remainder {
            if key >= min_key && key <= max_key {
                count += 1;
            }
        }
        
        count
    }

    /// Fallback implementations for non-x86_64 architectures
    #[cfg(not(target_arch = "x86_64"))]
    pub fn compare_keys_u32(keys: &[u32], search_key: u32) -> Vec<bool> {
        keys.iter().map(|&key| key == search_key).collect()
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn find_gte_u32(keys: &[u32], search_key: u32) -> Option<usize> {
        keys.iter().position(|&key| key >= search_key)
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn count_keys_in_range_u32(keys: &[u32], min_key: u32, max_key: u32) -> usize {
        keys.iter().filter(|&&key| key >= min_key && key <= max_key).count()
    }
}

/// SIMD-optimized bulk data operations
pub struct SimdBulkOperations;

impl SimdBulkOperations {
    /// Vectorized memory copy with prefetching
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    pub unsafe fn vectorized_copy(src: &[u8], dst: &mut [u8]) {
        assert_eq!(src.len(), dst.len());
        
        let len = src.len();
        let simd_len = len & !15; // Align to 16-byte boundaries
        
        // Prefetch source data
        for i in (0..len).step_by(64) {
            _mm_prefetch(src.as_ptr().add(i) as *const i8, _MM_HINT_T0);
        }
        
        // Vectorized copy for aligned portion
        for i in (0..simd_len).step_by(16) {
            let src_vec = _mm_loadu_si128(src.as_ptr().add(i) as *const __m128i);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, src_vec);
        }
        
        // Handle remainder
        for i in simd_len..len {
            dst[i] = src[i];
        }
    }

    /// Vectorized memory comparison
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn vectorized_compare(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        
        let len = a.len();
        let simd_len = len & !15;
        
        // Compare 16 bytes at a time
        for i in (0..simd_len).step_by(16) {
            let a_vec = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
            let b_vec = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);
            let cmp_result = _mm_cmpeq_epi8(a_vec, b_vec);
            
            // Check if all bytes are equal
            let mask = _mm_movemask_epi8(cmp_result);
            if mask != 0xFFFF {
                return false;
            }
        }
        
        // Compare remainder
        for i in simd_len..len {
            if a[i] != b[i] {
                return false;
            }
        }
        
        true
    }

    /// Vectorized byte search
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
        let needle_vec = _mm_set1_epi8(needle as i8);
        let len = haystack.len();
        let simd_len = len & !15;
        
        // Search 16 bytes at a time
        for i in (0..simd_len).step_by(16) {
            let data_vec = _mm_loadu_si128(haystack.as_ptr().add(i) as *const __m128i);
            let cmp_result = _mm_cmpeq_epi8(data_vec, needle_vec);
            let mask = _mm_movemask_epi8(cmp_result);
            
            if mask != 0 {
                // Found at least one match, find the first one
                let first_match = mask.trailing_zeros() as usize;
                return Some(i + first_match);
            }
        }
        
        // Search remainder
        for i in simd_len..len {
            if haystack[i] == needle {
                return Some(i);
            }
        }
        
        None
    }

    /// Fallback implementations
    #[cfg(not(target_arch = "x86_64"))]
    pub fn vectorized_copy(src: &[u8], dst: &mut [u8]) {
        dst.copy_from_slice(src);
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn vectorized_compare(a: &[u8], b: &[u8]) -> bool {
        a == b
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn find_byte(haystack: &[u8], needle: u8) -> Option<usize> {
        haystack.iter().position(|&b| b == needle)
    }
}

/// SIMD-optimized checksum calculations
pub struct SimdChecksumOperations;

impl SimdChecksumOperations {
    /// Fast CRC32 calculation using hardware acceleration
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    pub unsafe fn hardware_crc32(data: &[u8]) -> u32 {
        let mut crc = 0xFFFFFFFF_u32;
        let len = data.len();
        let mut pos = 0;
        
        // Process 8 bytes at a time when possible
        while pos + 8 <= len {
            let chunk = std::ptr::read_unaligned(data.as_ptr().add(pos) as *const u64);
            crc = _mm_crc32_u64(crc as u64, chunk) as u32;
            pos += 8;
        }
        
        // Process 4 bytes at a time
        while pos + 4 <= len {
            let chunk = std::ptr::read_unaligned(data.as_ptr().add(pos) as *const u32);
            crc = _mm_crc32_u32(crc, chunk);
            pos += 4;
        }
        
        // Process remaining bytes
        while pos < len {
            crc = _mm_crc32_u8(crc, data[pos]);
            pos += 1;
        }
        
        !crc
    }

    /// Vectorized XOR checksum
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse2")]
    pub unsafe fn vectorized_xor_checksum(data: &[u8]) -> u64 {
        let mut checksum_vec = _mm_setzero_si128();
        let len = data.len();
        let simd_len = len & !15;
        
        // XOR 16 bytes at a time
        for i in (0..simd_len).step_by(16) {
            let data_vec = _mm_loadu_si128(data.as_ptr().add(i) as *const __m128i);
            checksum_vec = _mm_xor_si128(checksum_vec, data_vec);
        }
        
        // Extract the result
        let mut result = [0u64; 2];
        _mm_storeu_si128(result.as_mut_ptr() as *mut __m128i, checksum_vec);
        let mut checksum = result[0] ^ result[1];
        
        // XOR remaining bytes
        for i in simd_len..len {
            checksum ^= data[i] as u64;
        }
        
        checksum
    }

    /// Fallback implementations
    #[cfg(not(target_arch = "x86_64"))]
    pub fn hardware_crc32(data: &[u8]) -> u32 {
        // Use software CRC32 implementation
        crc32fast::hash(data)
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn vectorized_xor_checksum(data: &[u8]) -> u64 {
        data.iter().fold(0u64, |acc, &byte| acc ^ (byte as u64))
    }
}

/// SIMD-optimized string operations
pub struct SimdStringOperations;

impl SimdStringOperations {
    /// Fast string comparison with SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn compare_strings(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        
        let min_len = a.len().min(b.len());
        let simd_len = min_len & !15;
        
        // Compare 16 bytes at a time
        for i in (0..simd_len).step_by(16) {
            let a_vec = _mm_loadu_si128(a.as_ptr().add(i) as *const __m128i);
            let b_vec = _mm_loadu_si128(b.as_ptr().add(i) as *const __m128i);
            let cmp_result = _mm_cmpeq_epi8(a_vec, b_vec);
            let mask = _mm_movemask_epi8(cmp_result);
            
            if mask != 0xFFFF {
                // Found difference, find first differing byte
                let diff_pos = (!mask).trailing_zeros() as usize;
                let byte_a = a[i + diff_pos];
                let byte_b = b[i + diff_pos];
                return byte_a.cmp(&byte_b);
            }
        }
        
        // Compare remainder byte by byte
        for i in simd_len..min_len {
            match a[i].cmp(&b[i]) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        
        // All compared bytes are equal, compare lengths
        a.len().cmp(&b.len())
    }

    /// Find substring using SIMD
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.1")]
    pub unsafe fn find_substring(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        if needle.len() > haystack.len() {
            return None;
        }
        
        let first_byte = needle[0];
        let first_byte_vec = _mm_set1_epi8(first_byte as i8);
        let haystack_len = haystack.len();
        let needle_len = needle.len();
        let search_len = haystack_len - needle_len + 1;
        let simd_len = search_len & !15;
        
        // Search for first byte using SIMD
        for i in (0..simd_len).step_by(16) {
            let data_vec = _mm_loadu_si128(haystack.as_ptr().add(i) as *const __m128i);
            let cmp_result = _mm_cmpeq_epi8(data_vec, first_byte_vec);
            let mask = _mm_movemask_epi8(cmp_result);
            
            if mask != 0 {
                // Found potential matches, check each one
                for bit in 0..16 {
                    if (mask & (1 << bit)) != 0 {
                        let pos = i + bit;
                        if pos + needle_len <= haystack_len {
                            if &haystack[pos..pos + needle_len] == needle {
                                return Some(pos);
                            }
                        }
                    }
                }
            }
        }
        
        // Search remainder
        for i in simd_len..search_len {
            if &haystack[i..i + needle_len] == needle {
                return Some(i);
            }
        }
        
        None
    }

    /// Fallback implementations
    #[cfg(not(target_arch = "x86_64"))]
    pub fn compare_strings(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn find_substring(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() {
            return Some(0);
        }
        if needle.len() > haystack.len() {
            return None;
        }
        
        for i in 0..=(haystack.len() - needle.len()) {
            if &haystack[i..i + needle.len()] == needle {
                return Some(i);
            }
        }
        None
    }
}

/// SIMD capability detection
pub struct SimdCapabilities;

impl SimdCapabilities {
    /// Check if SSE4.1 is available
    pub fn has_sse41() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("sse4.1")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }

    /// Check if SSE4.2 is available
    pub fn has_sse42() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("sse4.2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }

    /// Check if AVX2 is available
    pub fn has_avx2() -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    }

    /// Get SIMD capabilities summary
    pub fn get_capabilities() -> SimdCapabilitiesInfo {
        SimdCapabilitiesInfo {
            sse41: Self::has_sse41(),
            sse42: Self::has_sse42(),
            avx2: Self::has_avx2(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimdCapabilitiesInfo {
    pub sse41: bool,
    pub sse42: bool,
    pub avx2: bool,
}

impl SimdCapabilitiesInfo {
    pub fn has_hardware_crc32(&self) -> bool {
        self.sse42
    }

    pub fn has_vectorized_ops(&self) -> bool {
        self.sse41
    }

    pub fn optimal_vector_width(&self) -> usize {
        if self.avx2 {
            32 // 256-bit vectors
        } else if self.sse41 {
            16 // 128-bit vectors
        } else {
            8  // 64-bit fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_key_operations() {
        let keys = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let search_key = 30;
        
        let results = SimdKeyOperations::compare_keys_u32(&keys, search_key);
        assert_eq!(results[2], true);
        assert_eq!(results[0], false);
        
        let gte_pos = SimdKeyOperations::find_gte_u32(&keys, 35);
        assert_eq!(gte_pos, Some(3)); // 40 is first key >= 35
        
        let count = SimdKeyOperations::count_keys_in_range_u32(&keys, 25, 65);
        assert_eq!(count, 4); // 30, 40, 50, 60
    }

    #[test]
    fn test_simd_bulk_operations() {
        let src = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut dst = vec![0; src.len()];
        
        SimdBulkOperations::vectorized_copy(&src, &mut dst);
        assert_eq!(src, dst);
        
        let equal = SimdBulkOperations::vectorized_compare(&src, &dst);
        assert!(equal);
        
        let pos = SimdBulkOperations::find_byte(&src, 10);
        assert_eq!(pos, Some(9));
    }

    #[test]
    fn test_simd_checksum() {
        let data = b"Hello, SIMD world! This is a test string for checksum calculation.";
        
        let crc = SimdChecksumOperations::hardware_crc32(data);
        assert!(crc != 0);
        
        let xor_checksum = SimdChecksumOperations::vectorized_xor_checksum(data);
        assert!(xor_checksum != 0);
    }

    #[test]
    fn test_simd_string_operations() {
        let a = b"Hello, World!";
        let b = b"Hello, World!";
        let c = b"Hello, SIMD!";
        
        let cmp1 = SimdStringOperations::compare_strings(a, b);
        assert_eq!(cmp1, std::cmp::Ordering::Equal);
        
        let cmp2 = SimdStringOperations::compare_strings(a, c);
        assert_ne!(cmp2, std::cmp::Ordering::Equal);
        
        let haystack = b"This is a test string with some text";
        let needle = b"test";
        let pos = SimdStringOperations::find_substring(haystack, needle);
        assert_eq!(pos, Some(10));
    }

    #[test]
    fn test_simd_capabilities() {
        let caps = SimdCapabilities::get_capabilities();
        
        // These tests will pass regardless of hardware
        // as they test the capability detection functions
        println!("SSE4.1: {}", caps.sse41);
        println!("SSE4.2: {}", caps.sse42);
        println!("AVX2: {}", caps.avx2);
        println!("Optimal vector width: {} bytes", caps.optimal_vector_width());
    }

    #[test]
    fn test_edge_cases() {
        // Test empty arrays
        let empty_keys: Vec<u32> = vec![];
        let results = SimdKeyOperations::compare_keys_u32(&empty_keys, 42);
        assert!(results.is_empty());
        
        // Test single element
        let single_key = vec![42];
        let results = SimdKeyOperations::compare_keys_u32(&single_key, 42);
        assert_eq!(results, vec![true]);
        
        // Test unaligned sizes
        let odd_keys = vec![1, 2, 3, 4, 5];
        let results = SimdKeyOperations::compare_keys_u32(&odd_keys, 3);
        assert_eq!(results[2], true);
    }

    #[test]
    fn test_large_data_performance() {
        // Test with larger datasets to verify SIMD benefits
        let large_keys: Vec<u32> = (0..10000).collect();
        let search_key = 5000;
        
        let start = std::time::Instant::now();
        let results = SimdKeyOperations::compare_keys_u32(&large_keys, search_key);
        let simd_time = start.elapsed();
        
        let start = std::time::Instant::now();
        let scalar_results: Vec<bool> = large_keys.iter().map(|&k| k == search_key).collect();
        let scalar_time = start.elapsed();
        
        assert_eq!(results, scalar_results);
        println!("SIMD time: {:?}, Scalar time: {:?}", simd_time, scalar_time);
        
        // SIMD should find the correct result
        assert_eq!(results[search_key as usize], true);
    }
}
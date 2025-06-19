#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// SIMD-accelerated CRC32 checksum
#[cfg(target_arch = "x86_64")]
pub fn simd_crc32(data: &[u8], initial: u32) -> u32 {
    if is_x86_feature_detected!("sse4.2") {
        unsafe { simd_crc32_sse42(data, initial) }
    } else {
        crc32_scalar(data, initial)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_crc32(data: &[u8], initial: u32) -> u32 {
    crc32_scalar(data, initial)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn simd_crc32_sse42(data: &[u8], initial: u32) -> u32 {
    let mut crc = initial;
    let len = data.len();
    let mut offset = 0;
    
    // Process 8-byte chunks
    while offset + 8 <= len {
        let chunk = *(data.as_ptr().add(offset) as *const u64);
        crc = _mm_crc32_u64(crc as u64, chunk) as u32;
        offset += 8;
    }
    
    // Process 4-byte chunk if available
    if offset + 4 <= len {
        let chunk = *(data.as_ptr().add(offset) as *const u32);
        crc = _mm_crc32_u32(crc, chunk);
        offset += 4;
    }
    
    // Process remaining bytes
    while offset < len {
        crc = _mm_crc32_u8(crc, data[offset]);
        offset += 1;
    }
    
    crc
}

fn crc32_scalar(data: &[u8], initial: u32) -> u32 {
    // Use crc32fast for scalar implementation
    let mut hasher = crc32fast::Hasher::new_with_initial(initial);
    hasher.update(data);
    hasher.finalize()
}

/// Batch checksum verification using SIMD
pub fn simd_checksum_verify(data_chunks: &[(&[u8], u32)]) -> Vec<bool> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse4.2") {
            data_chunks.iter()
                .map(|(data, expected)| {
                    let computed = simd_crc32(data, 0);
                    computed == *expected
                })
                .collect()
        } else {
            checksum_verify_scalar(data_chunks)
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    {
        checksum_verify_scalar(data_chunks)
    }
}

fn checksum_verify_scalar(data_chunks: &[(&[u8], u32)]) -> Vec<bool> {
    data_chunks.iter()
        .map(|(data, expected)| {
            let computed = crc32_scalar(data, 0);
            computed == *expected
        })
        .collect()
}

/// Fast zero detection using SIMD
#[cfg(target_arch = "x86_64")]
pub fn simd_is_zero(data: &[u8]) -> bool {
    if is_x86_feature_detected!("avx2") {
        unsafe { simd_is_zero_avx2(data) }
    } else if is_x86_feature_detected!("sse2") {
        unsafe { simd_is_zero_sse2(data) }
    } else {
        is_zero_scalar(data)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_is_zero(data: &[u8]) -> bool {
    is_zero_scalar(data)
}

fn is_zero_scalar(data: &[u8]) -> bool {
    data.iter().all(|&b| b == 0)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn simd_is_zero_sse2(data: &[u8]) -> bool {
    let len = data.len();
    let chunks = len / 16;
    let zero = _mm_setzero_si128();
    
    for i in 0..chunks {
        let offset = i * 16;
        let vec = _mm_loadu_si128(data.as_ptr().add(offset) as *const __m128i);
        let cmp = _mm_cmpeq_epi8(vec, zero);
        let mask = _mm_movemask_epi8(cmp);
        
        if mask != 0xFFFF {
            return false;
        }
    }
    
    let remainder_start = chunks * 16;
    is_zero_scalar(&data[remainder_start..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_is_zero_avx2(data: &[u8]) -> bool {
    let len = data.len();
    let chunks = len / 32;
    let zero = _mm256_setzero_si256();
    
    for i in 0..chunks {
        let offset = i * 32;
        let vec = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
        let cmp = _mm256_cmpeq_epi8(vec, zero);
        let mask = _mm256_movemask_epi8(cmp);
        
        if mask != -1 {
            return false;
        }
    }
    
    let remainder_start = chunks * 32;
    is_zero_scalar(&data[remainder_start..])
}

/// Count leading zeros in byte array using SIMD
#[cfg(target_arch = "x86_64")]
pub fn simd_count_leading_zeros(data: &[u8]) -> usize {
    if is_x86_feature_detected!("avx2") {
        unsafe { simd_count_leading_zeros_avx2(data) }
    } else {
        count_leading_zeros_scalar(data)
    }
}

#[cfg(not(target_arch = "x86_64"))]
pub fn simd_count_leading_zeros(data: &[u8]) -> usize {
    count_leading_zeros_scalar(data)
}

fn count_leading_zeros_scalar(data: &[u8]) -> usize {
    data.iter().take_while(|&&b| b == 0).count()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_count_leading_zeros_avx2(data: &[u8]) -> usize {
    let len = data.len();
    let chunks = len / 32;
    let zero = _mm256_setzero_si256();
    let mut count = 0;
    
    for i in 0..chunks {
        let offset = i * 32;
        let vec = _mm256_loadu_si256(data.as_ptr().add(offset) as *const __m256i);
        let cmp = _mm256_cmpeq_epi8(vec, zero);
        let mask = _mm256_movemask_epi8(cmp);
        
        if mask == -1 {
            count += 32;
        } else {
            // Found non-zero, count zeros in this chunk
            count += (!mask).leading_zeros() as usize;
            return count;
        }
    }
    
    let remainder_start = chunks * 32;
    count + count_leading_zeros_scalar(&data[remainder_start..])
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simd_crc32() {
        let data = b"Hello, World!";
        let crc = simd_crc32(data, 0);
        
        // Verify it matches the scalar implementation
        let expected = crc32_scalar(data, 0);
        assert_eq!(crc, expected);
        
        // Test with initial value
        let crc2 = simd_crc32(data, 0x12345678);
        let expected2 = crc32_scalar(data, 0x12345678);
        assert_eq!(crc2, expected2);
    }
    
    #[test]
    fn test_checksum_verify() {
        let data1 = b"test data 1";
        let data2 = b"test data 2";
        
        let crc1 = simd_crc32(data1, 0);
        let crc2 = simd_crc32(data2, 0);
        
        let chunks = vec![
            (data1.as_ref(), crc1),
            (data2.as_ref(), crc2),
            (data1.as_ref(), crc2), // Wrong checksum
        ];
        
        let results = simd_checksum_verify(&chunks);
        assert_eq!(results, vec![true, true, false]);
    }
    
    #[test]
    fn test_is_zero() {
        let zeros = vec![0u8; 1000];
        assert!(simd_is_zero(&zeros));
        
        let mut mostly_zeros = vec![0u8; 1000];
        mostly_zeros[500] = 1;
        assert!(!simd_is_zero(&mostly_zeros));
        
        let empty = vec![];
        assert!(simd_is_zero(&empty));
    }
    
    #[test]
    fn test_count_leading_zeros() {
        let data1 = vec![0u8; 100];
        assert_eq!(simd_count_leading_zeros(&data1), 100);
        
        let mut data2 = vec![0u8; 100];
        data2[50] = 1;
        assert_eq!(simd_count_leading_zeros(&data2), 50);
        
        let data3 = b"hello";
        assert_eq!(simd_count_leading_zeros(data3), 0);
    }
}
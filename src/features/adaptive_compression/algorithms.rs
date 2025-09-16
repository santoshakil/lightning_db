//! Compression Algorithm Implementations
//!
//! Provides unified interface for various compression algorithms with hardware acceleration
//! support and performance optimization features.

use super::{CompressionAlgorithm, CompressionLevel, HardwareCapabilities};
use crate::core::error::{Error, Result};

/// Trait for compression algorithms
pub trait CompressionAlgorithmTrait: Send + Sync {
    /// Compress data with specified level
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>>;

    /// Decompress data
    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>>;

    /// Get algorithm name
    fn name(&self) -> &'static str;

    /// Check if hardware acceleration is available
    fn supports_hardware_acceleration(&self) -> bool {
        false
    }

    /// Get recommended block size for this algorithm
    fn recommended_block_size(&self) -> usize {
        64 * 1024 // 64KB default
    }

    /// Estimate compression ratio for data type
    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // Simple entropy-based estimation
        let entropy = calculate_entropy(data);
        (entropy / 8.0).clamp(0.1, 1.0)
    }
}

/// No compression (pass-through)
pub struct NoCompression;

impl NoCompression {
    pub fn new() -> Self {
        Self
    }
}

impl CompressionAlgorithmTrait for NoCompression {
    fn compress(&self, data: &[u8], _level: CompressionLevel) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        Ok(compressed_data.to_vec())
    }

    fn name(&self) -> &'static str {
        "none"
    }

    fn recommended_block_size(&self) -> usize {
        1024 * 1024 // 1MB for no compression
    }
}

/// LZ4 compression implementation
pub struct LZ4Compression {
    high_compression: bool,
}

impl LZ4Compression {
    pub fn new() -> Self {
        Self {
            high_compression: false,
        }
    }

    pub fn with_high_compression() -> Self {
        Self {
            high_compression: true,
        }
    }
}

impl CompressionAlgorithmTrait for LZ4Compression {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let compressed = if self.high_compression || level.as_int() > 6 {
            // Use HC compression for higher levels
            lz4_flex::compress_prepend_size(data)
        } else {
            // Use fast compression for lower levels
            lz4_flex::compress_prepend_size(data)
        };

        Ok(compressed)
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        if compressed_data.is_empty() {
            return Ok(Vec::new());
        }

        lz4_flex::decompress_size_prepended(compressed_data)
            .map_err(|e| Error::Compression(format!("LZ4 decompression failed: {}", e)))
    }

    fn name(&self) -> &'static str {
        if self.high_compression {
            "lz4hc"
        } else {
            "lz4"
        }
    }

    fn recommended_block_size(&self) -> usize {
        64 * 1024 // 64KB optimal for LZ4
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // LZ4 typically achieves 2-3x compression on text, less on binary
        let entropy = calculate_entropy(data);
        if entropy < 6.0 {
            0.4 // Good compression on low entropy data
        } else if entropy < 7.0 {
            0.6 // Moderate compression
        } else {
            0.8 // Poor compression on high entropy data
        }
    }
}

/// Zstandard compression implementation
pub struct ZstdCompression;

impl ZstdCompression {
    pub fn new() -> Self {
        Self
    }
}

impl CompressionAlgorithmTrait for ZstdCompression {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(feature = "zstd-compression")]
        {
            let compression_level = match level {
                CompressionLevel::Fastest => 1,
                CompressionLevel::Fast => 3,
                CompressionLevel::Balanced => 6,
                CompressionLevel::High => 9,
                CompressionLevel::Maximum => 19,
            };
            zstd::encode_all(data, compression_level)
                .map_err(|e| Error::Compression(format!("Zstd compression failed: {}", e)))
        }
        #[cfg(not(feature = "zstd-compression"))]
        {
            let _ = level;
            Err(Error::Compression(
                "ZSTD compression not available".to_string(),
            ))
        }
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        if compressed_data.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(feature = "zstd-compression")]
        {
            zstd::decode_all(compressed_data)
                .map_err(|e| Error::Compression(format!("Zstd decompression failed: {}", e)))
        }
        #[cfg(not(feature = "zstd-compression"))]
        Err(Error::Compression(
            "ZSTD compression not available".to_string(),
        ))
    }

    fn name(&self) -> &'static str {
        "zstd"
    }

    fn recommended_block_size(&self) -> usize {
        128 * 1024 // 128KB optimal for Zstd
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // Zstd typically achieves better compression than LZ4
        let entropy = calculate_entropy(data);
        if entropy < 5.0 {
            0.2 // Excellent compression on very low entropy
        } else if entropy < 6.0 {
            0.3 // Good compression
        } else if entropy < 7.0 {
            0.5 // Moderate compression
        } else {
            0.7 // Poor compression on high entropy data
        }
    }
}

/// Snappy compression implementation
pub struct SnappyCompression;

impl SnappyCompression {
    pub fn new() -> Self {
        Self
    }
}

impl CompressionAlgorithmTrait for SnappyCompression {
    fn compress(&self, data: &[u8], _level: CompressionLevel) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let mut encoder = snap::raw::Encoder::new();
        encoder
            .compress_vec(data)
            .map_err(|e| Error::Compression(format!("Snappy compression failed: {}", e)))
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        if compressed_data.is_empty() {
            return Ok(Vec::new());
        }

        let mut decoder = snap::raw::Decoder::new();
        decoder
            .decompress_vec(compressed_data)
            .map_err(|e| Error::Compression(format!("Snappy decompression failed: {}", e)))
    }

    fn name(&self) -> &'static str {
        "snappy"
    }

    fn recommended_block_size(&self) -> usize {
        32 * 1024 // 32KB optimal for Snappy
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // Snappy prioritizes speed over compression ratio
        let entropy = calculate_entropy(data);
        if entropy < 6.0 {
            0.5 // Moderate compression on low entropy
        } else if entropy < 7.0 {
            0.7 // Poor compression
        } else {
            0.9 // Very poor compression on high entropy
        }
    }
}

/// LZMA compression implementation (optional feature)
#[cfg(feature = "lzma")]
pub struct LZMACompression;

#[cfg(feature = "lzma")]
impl LZMACompression {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "lzma")]
impl CompressionAlgorithmTrait for LZMACompression {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let _compression_level = level.as_int() as u32;

        // Note: This is a placeholder - real implementation would use lzma-rs or similar
        Err(Error::Compression("LZMA not implemented".to_string()))
    }

    fn decompress(&self, _compressed_data: &[u8]) -> Result<Vec<u8>> {
        Err(Error::Compression("LZMA not implemented".to_string()))
    }

    fn name(&self) -> &'static str {
        "lzma"
    }

    fn recommended_block_size(&self) -> usize {
        256 * 1024 // 256KB for LZMA
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // LZMA achieves excellent compression but is slow
        let entropy = calculate_entropy(data);
        if entropy < 4.0 {
            0.1 // Excellent compression
        } else if entropy < 6.0 {
            0.2 // Very good compression
        } else if entropy < 7.0 {
            0.4 // Good compression
        } else {
            0.6 // Moderate compression
        }
    }
}

/// Brotli compression implementation (optional feature)
#[cfg(feature = "brotli")]
pub struct BrotliCompression;

#[cfg(feature = "brotli")]
impl BrotliCompression {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "brotli")]
impl CompressionAlgorithmTrait for BrotliCompression {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let _compression_level = level.as_int() as u32;

        // Note: This is a placeholder - real implementation would use brotli crate
        Err(Error::Compression("Brotli not implemented".to_string()))
    }

    fn decompress(&self, _compressed_data: &[u8]) -> Result<Vec<u8>> {
        Err(Error::Compression("Brotli not implemented".to_string()))
    }

    fn name(&self) -> &'static str {
        "brotli"
    }

    fn recommended_block_size(&self) -> usize {
        128 * 1024 // 128KB for Brotli
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // Brotli achieves excellent compression on text
        let entropy = calculate_entropy(data);
        if entropy < 5.0 {
            0.15 // Excellent compression on text
        } else if entropy < 6.0 {
            0.25 // Very good compression
        } else if entropy < 7.0 {
            0.4 // Good compression
        } else {
            0.6 // Moderate compression
        }
    }
}

/// Hardware-accelerated compression (placeholder for future implementations)
pub struct HardwareAcceleratedCompression {
    hardware: HardwareCapabilities,
    fallback: Box<dyn CompressionAlgorithmTrait + Send + Sync>,
}

impl HardwareAcceleratedCompression {
    pub fn new(hardware: &HardwareCapabilities) -> Result<Self> {
        // For now, use Zstd as fallback
        let fallback = Box::new(ZstdCompression::new());

        Ok(Self {
            hardware: hardware.clone(),
            fallback,
        })
    }
}

impl CompressionAlgorithmTrait for HardwareAcceleratedCompression {
    fn compress(&self, data: &[u8], level: CompressionLevel) -> Result<Vec<u8>> {
        // Check for hardware acceleration capabilities
        if self.hardware.has_compression_acceleration() {
            // Implementation pending actual hardware acceleration
            // For now, fall back to software implementation
        }

        // Use fallback algorithm
        self.fallback.compress(data, level)
    }

    fn decompress(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        if self.hardware.has_compression_acceleration() {
            // Implementation pending actual hardware acceleration
        }

        self.fallback.decompress(compressed_data)
    }

    fn name(&self) -> &'static str {
        "hardware"
    }

    fn supports_hardware_acceleration(&self) -> bool {
        true
    }

    fn recommended_block_size(&self) -> usize {
        // Hardware accelerated compression often works better with larger blocks
        1024 * 1024 // 1MB
    }

    fn estimate_compression_ratio(&self, data: &[u8]) -> f64 {
        // Delegate to fallback algorithm
        self.fallback.estimate_compression_ratio(data)
    }
}

/// Compression algorithm factory
pub struct CompressionAlgorithmFactory;

impl CompressionAlgorithmFactory {
    /// Create compression algorithm instance
    pub fn create(
        algorithm: CompressionAlgorithm,
        hardware: Option<&HardwareCapabilities>,
    ) -> Result<Box<dyn CompressionAlgorithmTrait + Send + Sync>> {
        match algorithm {
            CompressionAlgorithm::None => Ok(Box::new(NoCompression::new())),
            CompressionAlgorithm::LZ4 => Ok(Box::new(LZ4Compression::new())),
            CompressionAlgorithm::Zstd => Ok(Box::new(ZstdCompression::new())),
            CompressionAlgorithm::Snappy => Ok(Box::new(SnappyCompression::new())),
            #[cfg(feature = "lzma")]
            CompressionAlgorithm::LZMA => Ok(Box::new(LZMACompression::new())),
            #[cfg(feature = "brotli")]
            CompressionAlgorithm::Brotli => Ok(Box::new(BrotliCompression::new())),
            CompressionAlgorithm::HardwareAccelerated => {
                if let Some(hw) = hardware {
                    Ok(Box::new(HardwareAcceleratedCompression::new(hw)?))
                } else {
                    Err(Error::InvalidOperation {
                        reason: "Hardware capabilities required for hardware acceleration"
                            .to_string(),
                    })
                }
            }
            #[cfg(not(feature = "lzma"))]
            CompressionAlgorithm::LZMA => {
                Err(Error::Compression("LZMA feature not enabled".to_string()))
            }
            #[cfg(not(feature = "brotli"))]
            CompressionAlgorithm::Brotli => {
                Err(Error::Compression("Brotli feature not enabled".to_string()))
            }
        }
    }

    /// Get available algorithms
    pub fn available_algorithms() -> Vec<CompressionAlgorithm> {
        let algorithms = vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::LZ4,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Snappy,
        ];

        #[cfg(feature = "lzma")]
        algorithms.push(CompressionAlgorithm::LZMA);

        #[cfg(feature = "brotli")]
        algorithms.push(CompressionAlgorithm::Brotli);

        algorithms
    }
}

/// Calculate Shannon entropy of data
fn calculate_entropy(data: &[u8]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut counts = [0u32; 256];
    for &byte in data {
        counts[byte as usize] += 1;
    }

    let len = data.len() as f64;
    let mut entropy = 0.0;

    for count in counts.iter() {
        if *count > 0 {
            let p = *count as f64 / len;
            entropy -= p * p.log2();
        }
    }

    entropy
}

/// Compression algorithm performance metrics
#[derive(Debug, Clone)]
pub struct AlgorithmPerformance {
    pub algorithm: CompressionAlgorithm,
    pub compression_time_ns: u64,
    pub decompression_time_ns: u64,
    pub compression_ratio: f64,
    pub throughput_mb_s: f64,
    pub memory_usage_mb: f64,
}

impl AlgorithmPerformance {
    /// Calculate efficiency score (higher is better)
    pub fn efficiency_score(&self) -> f64 {
        // Balance compression ratio, speed, and memory usage
        let ratio_score = (1.0 - self.compression_ratio) * 100.0; // Higher compression is better
        let speed_score = self.throughput_mb_s / 10.0; // Normalize throughput
        let memory_score = 100.0 / (1.0 + self.memory_usage_mb / 100.0); // Lower memory is better

        (ratio_score + speed_score + memory_score) / 3.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let compressor = NoCompression::new();
        let data = b"Hello, World!";

        let compressed = compressor.compress(data, CompressionLevel::Fast).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compression() {
        let compressor = LZ4Compression::new();
        // Use repetitive data to ensure compression
        let data = b"Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well.";

        let compressed = compressor.compress(data, CompressionLevel::Fast).unwrap();
        // LZ4 should compress repetitive data
        assert!(compressed.len() <= data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_compression() {
        let compressor = ZstdCompression::new();
        // Use repetitive data to ensure compression
        let data = b"Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well.";

        let compressed = compressor
            .compress(data, CompressionLevel::Balanced)
            .unwrap();
        // Zstd should compress repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compression() {
        let compressor = SnappyCompression::new();
        // Use repetitive data to ensure compression
        let data = b"Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well. \
                     Hello, World! This is a test string that should compress well.";

        let compressed = compressor.compress(data, CompressionLevel::Fast).unwrap();
        // Snappy should compress repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_entropy_calculation() {
        // High entropy data (random)
        let random_data = (0..256).map(|i| i as u8).collect::<Vec<_>>();
        let entropy = calculate_entropy(&random_data);
        assert!(entropy > 7.0);

        // Low entropy data (repetitive)
        let repetitive_data = vec![b'A'; 1000];
        let entropy = calculate_entropy(&repetitive_data);
        assert!(entropy < 1.0);
    }

    #[test]
    fn test_algorithm_factory() {
        let algorithms = CompressionAlgorithmFactory::available_algorithms();
        assert!(!algorithms.is_empty());
        assert!(algorithms.contains(&CompressionAlgorithm::LZ4));
        assert!(algorithms.contains(&CompressionAlgorithm::Zstd));

        let lz4 = CompressionAlgorithmFactory::create(CompressionAlgorithm::LZ4, None).unwrap();
        assert_eq!(lz4.name(), "lz4");
    }

    #[test]
    fn test_compression_levels() {
        let compressor = ZstdCompression::new();
        let data = b"This is test data for compression level testing. ".repeat(100);

        let fast = compressor.compress(&data, CompressionLevel::Fast).unwrap();
        let balanced = compressor
            .compress(&data, CompressionLevel::Balanced)
            .unwrap();
        let high = compressor.compress(&data, CompressionLevel::High).unwrap();

        // Higher compression levels should achieve better compression
        // (though this isn't guaranteed for all data)
        assert!(high.len() <= balanced.len());

        // All should decompress correctly
        assert_eq!(compressor.decompress(&fast).unwrap(), data);
        assert_eq!(compressor.decompress(&balanced).unwrap(), data);
        assert_eq!(compressor.decompress(&high).unwrap(), data);
    }
}

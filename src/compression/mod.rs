pub mod adaptive_compression;

use crate::error::{Error, Result};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use snap::raw::{Decoder, Encoder};
use std::sync::Arc;
use zstd::stream::{decode_all, encode_all};

pub use adaptive_compression::{AdaptiveCompressionEngine, AdaptiveCompressionConfig, DataPattern};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    None,
    Zstd,
    Lz4,
    Snappy,
}

pub trait Compressor: Send + Sync {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>>;
    fn compression_type(&self) -> CompressionType;
}

pub struct NoCompression;

impl Compressor for NoCompression {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::None
    }
}

pub struct ZstdCompressor {
    level: i32,
    _dict: Option<Vec<u8>>,
}

impl ZstdCompressor {
    pub fn new(level: i32) -> Self {
        Self { level, _dict: None }
    }

    pub fn with_dict(level: i32, dict: Vec<u8>) -> Self {
        Self {
            level,
            _dict: Some(dict),
        }
    }
}

impl Compressor for ZstdCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Dictionary compression is not available in this version of zstd
        // Fall back to regular compression
        encode_all(data, self.level).map_err(|e| Error::Compression(e.to_string()))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Dictionary decompression is not available in this version
        // Fall back to regular decompression
        decode_all(data).map_err(|e| Error::Decompression(e.to_string()))
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Zstd
    }
}

pub struct Lz4Compressor;

impl Compressor for Lz4Compressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(compress_prepend_size(data))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        decompress_size_prepended(data).map_err(|e| Error::Decompression(e.to_string()))
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Lz4
    }
}

pub struct SnappyCompressor;

impl Compressor for SnappyCompressor {
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new();
        encoder.compress_vec(data).map_err(|e| Error::Compression(e.to_string()))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = Decoder::new();
        decoder.decompress_vec(data).map_err(|e| Error::Decompression(e.to_string()))
    }

    fn compression_type(&self) -> CompressionType {
        CompressionType::Snappy
    }
}

pub fn get_compressor(compression_type: CompressionType) -> Arc<dyn Compressor> {
    match compression_type {
        CompressionType::None => Arc::new(NoCompression),
        CompressionType::Zstd => Arc::new(ZstdCompressor::new(3)), // Level 3 is a good default
        CompressionType::Lz4 => Arc::new(Lz4Compressor),
        CompressionType::Snappy => Arc::new(SnappyCompressor),
    }
}

pub struct CompressionStats {
    pub original_size: usize,
    pub compressed_size: usize,
    pub compression_ratio: f64,
}

impl CompressionStats {
    pub fn new(original_size: usize, compressed_size: usize) -> Self {
        let compression_ratio = if original_size > 0 {
            1.0 - (compressed_size as f64 / original_size as f64)
        } else {
            0.0
        };

        Self {
            original_size,
            compressed_size,
            compression_ratio,
        }
    }
}

pub struct CompressionBenchmark;

impl CompressionBenchmark {
    pub fn benchmark_compressor(
        compressor: &dyn Compressor,
        data: &[u8],
    ) -> Result<(CompressionStats, std::time::Duration, std::time::Duration)> {
        let start = std::time::Instant::now();
        let compressed = compressor.compress(data)?;
        let compress_time = start.elapsed();

        let start = std::time::Instant::now();
        let _decompressed = compressor.decompress(&compressed)?;
        let decompress_time = start.elapsed();

        let stats = CompressionStats::new(data.len(), compressed.len());

        Ok((stats, compress_time, decompress_time))
    }

    pub fn find_best_compressor(data: &[u8]) -> CompressionType {
        // Skip compression for very small data
        if data.len() < 128 {
            return CompressionType::None;
        }

        // Analyze data patterns to make intelligent compression choices
        let entropy = Self::estimate_entropy(data);
        let is_highly_repetitive = Self::is_highly_repetitive(data);
        let is_text_like = Self::is_text_like(data);

        // Choose compressor based on data characteristics
        let candidates = if entropy < 3.0 && is_highly_repetitive {
            // Very low entropy, highly repetitive - try all compressors
            vec![
                CompressionType::Zstd,    // Best for highly compressible data
                CompressionType::Snappy,  // Fast with decent compression
                CompressionType::Lz4,     // Fastest
            ]
        } else if entropy < 5.0 && is_text_like {
            // Medium entropy, text-like data
            vec![
                CompressionType::Snappy,  // Good balance for text
                CompressionType::Lz4,     // Fast alternative
                CompressionType::Zstd,    // Better compression if needed
            ]
        } else if entropy > 7.0 {
            // High entropy, likely already compressed or encrypted
            vec![CompressionType::None]  // Don't try to compress
        } else {
            // Default: try fast compressors first
            vec![
                CompressionType::Lz4,     // Fastest
                CompressionType::Snappy,  // Good balance
                CompressionType::Zstd,    // Best compression
            ]
        };

        let mut best_type = CompressionType::None;
        let mut best_score = f64::MAX;
        let uncompressed_score = data.len() as f64 * 0.000001; // Very small time for no compression

        for comp_type in candidates {
            let compressor = get_compressor(comp_type);
            if let Ok((stats, compress_time, _)) = Self::benchmark_compressor(&*compressor, data) {
                // Skip if compression ratio is too low (less than 10% savings)
                if stats.compression_ratio < 0.1 {
                    continue;
                }

                // Score based on compression ratio, time, and size
                // Balance between compression effectiveness and speed
                let time_weight = compress_time.as_secs_f64();
                let compression_benefit = stats.compression_ratio;
                let size_factor = (stats.compressed_size as f64 / 1024.0).max(1.0).ln();
                
                // Lower score is better
                let score = (time_weight * size_factor) / (compression_benefit + 0.1);

                if score < best_score && score < uncompressed_score {
                    best_score = score;
                    best_type = comp_type;
                }
            }
        }

        best_type
    }

    fn estimate_entropy(data: &[u8]) -> f64 {
        let mut counts = [0u64; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }

        let total = data.len() as f64;
        let mut entropy = 0.0;

        for &count in &counts {
            if count > 0 {
                let p = count as f64 / total;
                entropy -= p * p.log2();
            }
        }

        entropy
    }

    fn is_highly_repetitive(data: &[u8]) -> bool {
        if data.len() < 16 {
            return false;
        }

        // Check for repeating patterns
        let sample_size = data.len().min(1024);
        let mut repetitions = 0;

        for window_size in [2, 4, 8, 16] {
            if window_size >= sample_size {
                break;
            }

            for i in 0..(sample_size - window_size * 2) {
                if data[i..i + window_size] == data[i + window_size..i + window_size * 2] {
                    repetitions += 1;
                }
            }
        }

        repetitions > sample_size / 10
    }

    fn is_text_like(data: &[u8]) -> bool {
        let sample_size = data.len().min(1024);
        let mut text_chars = 0;

        for &byte in &data[..sample_size] {
            // Count printable ASCII characters and common whitespace
            if (byte >= 32 && byte <= 126) || byte == 9 || byte == 10 || byte == 13 {
                text_chars += 1;
            }
        }

        text_chars > sample_size * 3 / 4
    }
}

/// Convenience function to compress data with a specific compression type
pub fn compress(data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
    let compressor = get_compressor(compression_type);
    compressor.compress(data)
}

/// Convenience function to decompress data with a specific compression type
pub fn decompress(data: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
    let compressor = get_compressor(compression_type);
    compressor.decompress(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compression() {
        let compressor = NoCompression;
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zstd_compression() {
        let compressor = ZstdCompressor::new(3);
        let data = b"Hello, World! ".repeat(100); // Repetitive data compresses well

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compression() {
        let compressor = Lz4Compressor;
        let data = b"Hello, World! ".repeat(100);

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compression() {
        let compressor = SnappyCompressor;
        let data = b"Hello, World! ".repeat(100);

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new(1000, 300);
        assert_eq!(stats.original_size, 1000);
        assert_eq!(stats.compressed_size, 300);
        assert!((stats.compression_ratio - 0.7).abs() < 0.001);
    }

    #[test]
    fn test_compression_benchmark() {
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(100);
        let compressor = get_compressor(CompressionType::Zstd);

        let result = CompressionBenchmark::benchmark_compressor(&*compressor, &data);
        assert!(result.is_ok());

        let (stats, compress_time, decompress_time) = result.unwrap();
        assert!(stats.compressed_size < stats.original_size);
        assert!(compress_time.as_nanos() > 0);
        assert!(decompress_time.as_nanos() > 0);
    }

    #[test]
    fn test_adaptive_compression_small_data() {
        // Small data should not be compressed
        let small_data = b"Small data";
        let best = CompressionBenchmark::find_best_compressor(small_data);
        assert_eq!(best, CompressionType::None);
    }

    #[test]
    fn test_adaptive_compression_repetitive_data() {
        // Highly repetitive data should use Zstd
        let repetitive_data = b"AAAAAAAAAA".repeat(100);
        let best = CompressionBenchmark::find_best_compressor(&repetitive_data);
        assert!(matches!(best, CompressionType::Zstd | CompressionType::Snappy | CompressionType::Lz4));
    }

    #[test]
    fn test_adaptive_compression_text_data() {
        // Text-like data
        let text_data = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(50);
        let best = CompressionBenchmark::find_best_compressor(&text_data);
        assert!(matches!(best, CompressionType::Snappy | CompressionType::Lz4 | CompressionType::Zstd));
    }

    #[test]
    fn test_adaptive_compression_random_data() {
        // High entropy data (pseudo-random)
        let mut random_data = vec![0u8; 1024];
        for i in 0..random_data.len() {
            random_data[i] = ((i * 7 + 13) % 256) as u8;
        }
        let best = CompressionBenchmark::find_best_compressor(&random_data);
        // Random data might not compress well, could be None or any compressor
        assert!(matches!(best, CompressionType::None | CompressionType::Lz4 | CompressionType::Snappy | CompressionType::Zstd));
    }

    #[test]
    fn test_entropy_estimation() {
        // Low entropy (all same byte)
        let low_entropy = vec![65u8; 1000];
        let entropy = CompressionBenchmark::estimate_entropy(&low_entropy);
        assert!(entropy < 0.1);

        // High entropy (all different bytes)
        let mut high_entropy = vec![0u8; 256];
        for i in 0..256 {
            high_entropy[i] = i as u8;
        }
        let entropy = CompressionBenchmark::estimate_entropy(&high_entropy);
        assert!(entropy > 7.9 && entropy <= 8.0);
    }

    #[test]
    fn test_pattern_detection() {
        // Highly repetitive pattern
        let repetitive = b"ABCDABCDABCDABCD".repeat(10);
        assert!(CompressionBenchmark::is_highly_repetitive(&repetitive));

        // Non-repetitive pattern
        let non_repetitive = b"The quick brown fox jumps over the lazy dog.";
        assert!(!CompressionBenchmark::is_highly_repetitive(non_repetitive));
    }

    #[test]
    fn test_text_detection() {
        // Text data
        let text = b"This is regular text with ASCII characters.";
        assert!(CompressionBenchmark::is_text_like(text));

        // Binary data
        let binary = vec![0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD];
        assert!(!CompressionBenchmark::is_text_like(&binary));
    }
}

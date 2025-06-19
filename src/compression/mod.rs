use crate::error::{Error, Result};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::sync::Arc;
use zstd::stream::{decode_all, encode_all};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompressionType {
    None,
    Zstd,
    Lz4,
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

pub fn get_compressor(compression_type: CompressionType) -> Arc<dyn Compressor> {
    match compression_type {
        CompressionType::None => Arc::new(NoCompression),
        CompressionType::Zstd => Arc::new(ZstdCompressor::new(3)), // Level 3 is a good default
        CompressionType::Lz4 => Arc::new(Lz4Compressor),
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
        let compressors = vec![
            (CompressionType::None, get_compressor(CompressionType::None)),
            (CompressionType::Lz4, get_compressor(CompressionType::Lz4)),
            (CompressionType::Zstd, get_compressor(CompressionType::Zstd)),
        ];

        let mut best_type = CompressionType::None;
        let mut best_score = f64::MAX;

        for (comp_type, compressor) in compressors {
            if let Ok((stats, compress_time, _)) = Self::benchmark_compressor(&*compressor, data) {
                // Score based on compression ratio and time
                // Lower is better
                let score = (1.0 - stats.compression_ratio) * compress_time.as_secs_f64();

                if score < best_score {
                    best_score = score;
                    best_type = comp_type;
                }
            }
        }

        best_type
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
}

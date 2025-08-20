// iOS-specific compression module that excludes zstd
pub mod adaptive_compression;

use crate::core::error::{Error, Result};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use snap::raw::{Decoder, Encoder};
use std::sync::Arc;

pub use adaptive_compression::{AdaptiveCompressionEngine, AdaptiveCompressionConfig, DataPattern};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    None,
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
        CompressionType::Lz4 => Arc::new(Lz4Compressor),
        CompressionType::Snappy => Arc::new(SnappyCompressor),
    }
}

// Re-export the rest of the module content
pub use super::{CompressionStats, CompressionBenchmark, compress, decompress};
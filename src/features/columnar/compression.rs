use bytes::Bytes;
use crate::core::error::Error;
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnCompression {
    None,
    Snappy,
    Zstd,
    Lz4,
    Gzip,
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionStrategy {
    Speed,
    Ratio,
    Balanced,
}

pub struct Compressor {
    strategy: CompressionStrategy,
    compression_threshold: f64,
}

impl Compressor {
    pub fn new(strategy: CompressionStrategy) -> Self {
        Self { 
            strategy,
            compression_threshold: 0.9,
        }
    }

    pub fn compress(&self, data: &Bytes, compression: ColumnCompression) -> Result<Bytes, Error> {
        if data.is_empty() {
            return Ok(data.clone());
        }

        match compression {
            ColumnCompression::None => Ok(data.clone()),
            ColumnCompression::Snappy => self.compress_snappy(data),
            ColumnCompression::Lz4 => self.compress_lz4(data),
            ColumnCompression::Zstd => self.compress_zstd(data),
            ColumnCompression::Gzip => self.compress_gzip(data),
        }
    }

    pub fn decompress(&self, data: &Bytes, compression: ColumnCompression) -> Result<Bytes, Error> {
        if data.is_empty() {
            return Ok(data.clone());
        }

        match compression {
            ColumnCompression::None => Ok(data.clone()),
            ColumnCompression::Snappy => self.decompress_snappy(data),
            ColumnCompression::Lz4 => self.decompress_lz4(data),
            ColumnCompression::Zstd => self.decompress_zstd(data),
            ColumnCompression::Gzip => self.decompress_gzip(data),
        }
    }

    fn compress_snappy(&self, data: &Bytes) -> Result<Bytes, Error> {
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder.compress_vec(data.as_ref())
            .map_err(|e| Error::CompressionError(format!("Snappy compression failed: {}", e)))?;
        
        let compression_ratio = compressed.len() as f64 / data.len() as f64;
        if compression_ratio > self.compression_threshold {
            return Ok(data.clone());
        }
        
        Ok(Bytes::from(compressed))
    }

    fn decompress_snappy(&self, data: &Bytes) -> Result<Bytes, Error> {
        let mut decoder = snap::raw::Decoder::new();
        let decompressed = decoder.decompress_vec(data.as_ref())
            .map_err(|e| Error::CompressionError(format!("Snappy decompression failed: {}", e)))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_lz4(&self, data: &Bytes) -> Result<Bytes, Error> {
        let compressed = lz4_flex::compress_prepend_size(data.as_ref());
        
        let compression_ratio = compressed.len() as f64 / data.len() as f64;
        if compression_ratio > self.compression_threshold {
            return Ok(data.clone());
        }
        
        Ok(Bytes::from(compressed))
    }

    fn decompress_lz4(&self, data: &Bytes) -> Result<Bytes, Error> {
        let decompressed = lz4_flex::decompress_size_prepended(data.as_ref())
            .map_err(|e| Error::CompressionError(format!("LZ4 decompression failed: {}", e)))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_zstd(&self, data: &Bytes) -> Result<Bytes, Error> {
        let level = match self.strategy {
            CompressionStrategy::Speed => 1,
            CompressionStrategy::Balanced => 3,
            CompressionStrategy::Ratio => 9,
        };
        
        let compressed = zstd::bulk::compress(data.as_ref(), level)
            .map_err(|e| Error::CompressionError(format!("Zstd compression failed: {}", e)))?;
        
        let compression_ratio = compressed.len() as f64 / data.len() as f64;
        if compression_ratio > self.compression_threshold {
            return Ok(data.clone());
        }
        
        Ok(Bytes::from(compressed))
    }

    fn decompress_zstd(&self, data: &Bytes) -> Result<Bytes, Error> {
        let decompressed = zstd::bulk::decompress(data.as_ref(), 1024 * 1024 * 100)
            .map_err(|e| Error::CompressionError(format!("Zstd decompression failed: {}", e)))?;
        Ok(Bytes::from(decompressed))
    }

    fn compress_gzip(&self, data: &Bytes) -> Result<Bytes, Error> {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data.as_ref())
            .map_err(|e| Error::CompressionError(format!("Gzip compression failed: {}", e)))?;
        
        let compressed = encoder.finish()
            .map_err(|e| Error::CompressionError(format!("Gzip compression failed: {}", e)))?;
        
        let compression_ratio = compressed.len() as f64 / data.len() as f64;
        if compression_ratio > self.compression_threshold {
            return Ok(data.clone());
        }
        
        Ok(Bytes::from(compressed))
    }

    fn decompress_gzip(&self, data: &Bytes) -> Result<Bytes, Error> {
        use flate2::read::GzDecoder;
        
        let mut decoder = GzDecoder::new(data.as_ref());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| Error::CompressionError(format!("Gzip decompression failed: {}", e)))?;
        
        Ok(Bytes::from(decompressed))
    }

    pub fn estimate_compression_ratio(&self, data: &Bytes, compression: ColumnCompression) -> Result<f64, Error> {
        if data.is_empty() {
            return Ok(1.0);
        }

        let sample_size = (data.len() / 10).max(1024).min(data.len());
        let sample = &data[..sample_size];
        let sample_bytes = Bytes::from(sample.to_vec());
        
        let compressed = self.compress(&sample_bytes, compression)?;
        Ok(compressed.len() as f64 / sample.len() as f64)
    }
}
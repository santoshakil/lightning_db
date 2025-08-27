use bytes::Bytes;
use crate::core::error::Error;

#[derive(Debug, Clone, Copy)]
pub enum ColumnCompression {
    None,
    Snappy,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionStrategy {
    Speed,
    Ratio,
    Balanced,
}

pub struct Compressor {
    strategy: CompressionStrategy,
}

impl Compressor {
    pub fn new(strategy: CompressionStrategy) -> Self {
        Self { strategy }
    }

    pub fn compress(&self, data: &Bytes, compression: ColumnCompression) -> Result<Bytes, Error> {
        match compression {
            ColumnCompression::None => Ok(data.clone()),
            _ => Ok(data.clone()),
        }
    }
}
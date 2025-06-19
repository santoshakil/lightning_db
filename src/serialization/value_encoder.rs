use crate::compression::{CompressionType, compress, decompress};
use std::borrow::Cow;

/// Trait for efficient value encoding/decoding
pub trait ValueEncoder: Send + Sync {
    /// Encode a value for storage
    fn encode<'a>(&self, value: &'a [u8]) -> Cow<'a, [u8]>;
    
    /// Decode a value from storage
    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]>;
}

/// Default value encoder (no transformation)
#[derive(Debug, Clone, Default)]
pub struct IdentityValueEncoder;

impl ValueEncoder for IdentityValueEncoder {
    fn encode<'a>(&self, value: &'a [u8]) -> Cow<'a, [u8]> {
        Cow::Borrowed(value)
    }
    
    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        Cow::Borrowed(encoded)
    }
}

/// Compression-based value encoder
#[derive(Debug, Clone)]
pub struct CompressionValueEncoder {
    compression_type: CompressionType,
    min_size: usize,
}

impl CompressionValueEncoder {
    pub fn new(compression_type: CompressionType, min_size: usize) -> Self {
        Self {
            compression_type,
            min_size,
        }
    }
}

impl ValueEncoder for CompressionValueEncoder {
    fn encode<'a>(&self, value: &'a [u8]) -> Cow<'a, [u8]> {
        if value.len() < self.min_size {
            return Cow::Borrowed(value);
        }
        
        match compress(value, self.compression_type) {
            Ok(compressed) => {
                if compressed.len() < value.len() {
                    // Add header to indicate compression
                    let mut result = vec![self.compression_type as u8];
                    result.extend_from_slice(&compressed);
                    Cow::Owned(result)
                } else {
                    Cow::Borrowed(value)
                }
            }
            Err(_) => Cow::Borrowed(value),
        }
    }
    
    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        if encoded.is_empty() {
            return Cow::Borrowed(encoded);
        }
        
        // Check if value is compressed
        let first_byte = encoded[0];
        if first_byte <= CompressionType::Lz4 as u8 {
            let compression_type = match first_byte {
                0 => CompressionType::None,
                1 => CompressionType::Zstd,
                2 => CompressionType::Lz4,
                _ => return Cow::Borrowed(encoded),
            };
            
            if compression_type != CompressionType::None {
                if let Ok(decompressed) = decompress(&encoded[1..], compression_type) {
                    return Cow::Owned(decompressed);
                }
            }
        }
        
        Cow::Borrowed(encoded)
    }
}

/// Encryption-based value encoder (placeholder for future implementation)
#[derive(Debug, Clone)]
pub struct EncryptionValueEncoder {
    _key: Vec<u8>,
}

impl EncryptionValueEncoder {
    pub fn new(key: Vec<u8>) -> Self {
        Self { _key: key }
    }
}

impl ValueEncoder for EncryptionValueEncoder {
    fn encode<'a>(&self, value: &'a [u8]) -> Cow<'a, [u8]> {
        // TODO: Implement encryption
        Cow::Borrowed(value)
    }
    
    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        // TODO: Implement decryption
        Cow::Borrowed(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_identity_encoder() {
        let encoder = IdentityValueEncoder;
        let value = b"test value";
        
        let encoded = encoder.encode(value);
        assert_eq!(&*encoded, value);
        
        let decoded = encoder.decode(&encoded);
        assert_eq!(&*decoded, value);
    }
    
    #[test]
    fn test_compression_encoder() {
        let encoder = CompressionValueEncoder::new(CompressionType::Zstd, 10);
        
        // Small value (not compressed)
        let small_value = b"small";
        let encoded = encoder.encode(small_value);
        assert_eq!(&*encoded, small_value);
        
        // Large repetitive value (should compress)
        let large_value = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let encoded = encoder.encode(large_value);
        assert!(encoded.len() < large_value.len());
        
        let decoded = encoder.decode(&encoded);
        assert_eq!(&*decoded, large_value);
    }
}
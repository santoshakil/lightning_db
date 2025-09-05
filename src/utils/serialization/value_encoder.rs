use crate::features::adaptive_compression::{compress, decompress, CompressionType};
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
                    // Add header to indicate compression with fixed encoding
                    let compression_byte = match self.compression_type {
                        CompressionType::None => 0u8,
                        CompressionType::Zstd => 1u8,
                        CompressionType::LZ4 => 2u8,
                        CompressionType::Snappy => 3u8,
                        _ => 0u8, // Default to no compression for unsupported types
                    };
                    let mut result = vec![compression_byte];
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
        // Fixed encoding values for backward compatibility
        let compression_type = match first_byte {
            0 => CompressionType::None,
            1 => CompressionType::Zstd,
            2 => CompressionType::LZ4,
            3 => CompressionType::Snappy,
            _ => return Cow::Borrowed(encoded),
        };

        if compression_type != CompressionType::None {
            if let Ok(decompressed) = decompress(&encoded[1..], compression_type) {
                return Cow::Owned(decompressed);
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
        // Encryption implementation requires cryptographic library integration
        // Currently returns plaintext - NOT for production use
        Cow::Borrowed(value)
    }

    fn decode<'a>(&self, encoded: &'a [u8]) -> Cow<'a, [u8]> {
        // Decryption implementation requires cryptographic library integration
        // Currently returns plaintext - NOT for production use  
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
        let encoder = CompressionValueEncoder::new(CompressionType::LZ4, 10);

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

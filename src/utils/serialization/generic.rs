use crate::core::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Generic serialization utilities for consistent data encoding
pub struct SerializationUtils;

impl SerializationUtils {
    /// Serialize a value to bytes using serde_json for simplicity
    pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| Error::SerializationError(format!("Failed to serialize: {}", e)))
    }

    /// Deserialize bytes to a value using serde_json
    pub fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| Error::DeserializationError(format!("Failed to deserialize: {}", e)))
    }

    /// Serialize a value and write to a writer
    pub fn serialize_to_writer<T: Serialize, W: Write>(value: &T, writer: W) -> Result<()> {
        serde_json::to_writer(writer, value).map_err(|e| Error::SerializationError(format!("Failed to serialize to writer: {}", e)))
    }

    /// Read from a reader and deserialize to a value
    pub fn deserialize_from_reader<T: for<'de> Deserialize<'de>, R: Read>(reader: R) -> Result<T> {
        serde_json::from_reader(reader).map_err(|e| Error::DeserializationError(format!("Failed to deserialize from reader: {}", e)))
    }

    /// Serialize with size prefix (4-byte little-endian length + data)
    pub fn serialize_with_size<T: Serialize>(value: &T) -> Result<Vec<u8>> {
        let data = Self::serialize(value)?;
        let size = data.len() as u32;
        let mut result = Vec::with_capacity(4 + data.len());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend_from_slice(&data);
        Ok(result)
    }

    /// Deserialize with size prefix
    pub fn deserialize_with_size<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
        if bytes.len() < 4 {
            return Err(Error::DeserializationError("Insufficient data for size prefix".to_string()));
        }
        
        let size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + size {
            return Err(Error::DeserializationError(format!("Insufficient data: expected {} bytes, got {}", 4 + size, bytes.len())));
        }
        
        Self::deserialize(&bytes[4..4 + size])
    }

    /// Get serialized size of a value without actually serializing
    pub fn serialized_size<T: Serialize>(value: &T) -> Result<u64> {
        let data = Self::serialize(value)?;
        Ok(data.len() as u64)
    }

    /// Serialize to a fixed-size buffer
    pub fn serialize_to_buffer<T: Serialize>(value: &T, buffer: &mut [u8]) -> Result<usize> {
        let serialized = Self::serialize(value)?;
        if serialized.len() > buffer.len() {
            return Err(Error::SerializationError(format!("Buffer too small: need {}, got {}", serialized.len(), buffer.len())));
        }
        buffer[..serialized.len()].copy_from_slice(&serialized);
        Ok(serialized.len())
    }

    /// Deserialize from a buffer with known length
    pub fn deserialize_from_buffer<T: for<'de> Deserialize<'de>>(buffer: &[u8], length: usize) -> Result<T> {
        if length > buffer.len() {
            return Err(Error::DeserializationError(format!("Buffer too small: need {}, got {}", length, buffer.len())));
        }
        Self::deserialize(&buffer[..length])
    }
}

/// Configuration for serialization behavior
#[derive(Debug, Clone)]
pub struct SerializationConfig {
    /// Use compression for large values
    pub use_compression: bool,
    /// Compression threshold in bytes
    pub compression_threshold: usize,
    /// Enable checksums for integrity verification
    pub enable_checksums: bool,
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            use_compression: true,
            compression_threshold: 1024,
            enable_checksums: true,
        }
    }
}

/// Advanced serialization with compression and checksums
pub struct AdvancedSerialization {
    config: SerializationConfig,
}

impl AdvancedSerialization {
    pub fn new(config: SerializationConfig) -> Self {
        Self { config }
    }

    /// Serialize with optional compression and checksums
    pub fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
        let mut data = SerializationUtils::serialize(value)?;
        
        // Apply compression if enabled and data is large enough
        if self.config.use_compression && data.len() >= self.config.compression_threshold {
            data = self.compress(&data)?;
        }
        
        // Add checksum if enabled
        if self.config.enable_checksums {
            data = self.add_checksum(data)?;
        }
        
        Ok(data)
    }

    /// Deserialize with checksum verification and decompression
    pub fn deserialize<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<T> {
        // Verify and remove checksum if enabled
        let data = if self.config.enable_checksums {
            self.verify_and_remove_checksum(data)?
        } else {
            data
        };
        
        // Decompress if compression was used
        let decompressed_data = if self.is_compressed(data)? {
            self.decompress(data)?
        } else {
            data.to_vec()
        };
        
        SerializationUtils::deserialize(&decompressed_data)
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Simple compression marker + data for now
        // In production, use zstd, lz4, or similar
        let mut result = Vec::with_capacity(data.len() + 1);
        result.push(1); // Compression marker
        result.extend_from_slice(data);
        Ok(result)
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() || data[0] != 1 {
            return Err(Error::DeserializationError("Invalid compression marker".to_string()));
        }
        Ok(data[1..].to_vec())
    }

    fn is_compressed(&self, data: &[u8]) -> Result<bool> {
        Ok(!data.is_empty() && data[0] == 1)
    }

    fn add_checksum(&self, mut data: Vec<u8>) -> Result<Vec<u8>> {
        // Simple CRC32 checksum
        let checksum = crc32fast::hash(&data);
        data.extend_from_slice(&checksum.to_le_bytes());
        Ok(data)
    }

    fn verify_and_remove_checksum<'a>(&self, data: &'a [u8]) -> Result<&'a [u8]> {
        if data.len() < 4 {
            return Err(Error::DeserializationError("Data too short for checksum".to_string()));
        }
        
        let data_len = data.len() - 4;
        let data_part = &data[..data_len];
        let checksum_bytes = &data[data_len..];
        
        let expected_checksum = crc32fast::hash(data_part);
        let actual_checksum = u32::from_le_bytes([checksum_bytes[0], checksum_bytes[1], checksum_bytes[2], checksum_bytes[3]]);
        
        if expected_checksum != actual_checksum {
            return Err(Error::CorruptedData(format!(
                "Checksum mismatch: expected {}, got {}",
                expected_checksum, actual_checksum
            )));
        }
        
        Ok(data_part)
    }
}

/// Trait for custom serialization implementations
pub trait CustomSerializable {
    /// Serialize to bytes
    fn to_bytes(&self) -> Result<Vec<u8>>;
    
    /// Deserialize from bytes
    fn from_bytes(bytes: &[u8]) -> Result<Self> where Self: Sized;
    
    /// Get the serialized size hint
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

/// Batch serialization for multiple values
pub struct BatchSerializer<T> {
    items: Vec<T>,
    config: SerializationConfig,
}

impl<T: Serialize> BatchSerializer<T> {
    pub fn new(config: SerializationConfig) -> Self {
        Self {
            items: Vec::new(),
            config,
        }
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn serialize_batch(&self) -> Result<Vec<u8>> {
        let advanced = AdvancedSerialization::new(self.config.clone());
        advanced.serialize(&self.items)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}

pub struct BatchDeserializer {
    config: SerializationConfig,
}

impl BatchDeserializer {
    pub fn new(config: SerializationConfig) -> Self {
        Self { config }
    }

    pub fn deserialize_batch<T: for<'de> Deserialize<'de>>(&self, data: &[u8]) -> Result<Vec<T>> {
        let advanced = AdvancedSerialization::new(self.config.clone());
        advanced.deserialize(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u32,
        name: String,
        data: Vec<u8>,
    }

    #[test]
    fn test_basic_serialization() {
        let test_data = TestStruct {
            id: 42,
            name: "test".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        let serialized = SerializationUtils::serialize(&test_data).unwrap();
        let deserialized: TestStruct = SerializationUtils::deserialize(&serialized).unwrap();

        assert_eq!(test_data, deserialized);
    }

    #[test]
    fn test_size_prefixed_serialization() {
        let test_data = TestStruct {
            id: 123,
            name: "size_test".to_string(),
            data: vec![10, 20, 30],
        };

        let serialized = SerializationUtils::serialize_with_size(&test_data).unwrap();
        let deserialized: TestStruct = SerializationUtils::deserialize_with_size(&serialized).unwrap();

        assert_eq!(test_data, deserialized);
    }

    #[test]
    fn test_advanced_serialization() {
        let config = SerializationConfig::default();
        let advanced = AdvancedSerialization::new(config);

        let test_data = TestStruct {
            id: 456,
            name: "advanced_test".to_string(),
            data: vec![100; 2000], // Large data to trigger compression
        };

        let serialized = advanced.serialize(&test_data).unwrap();
        let deserialized: TestStruct = advanced.deserialize(&serialized).unwrap();

        assert_eq!(test_data, deserialized);
    }

    #[test]
    fn test_batch_serialization() {
        let config = SerializationConfig::default();
        let mut batch = BatchSerializer::new(config.clone());

        let items = vec![
            TestStruct { id: 1, name: "item1".to_string(), data: vec![1] },
            TestStruct { id: 2, name: "item2".to_string(), data: vec![2] },
            TestStruct { id: 3, name: "item3".to_string(), data: vec![3] },
        ];

        for item in &items {
            batch.add(item.clone());
        }

        let serialized = batch.serialize_batch().unwrap();
        
        let deserializer = BatchDeserializer::new(config);
        let deserialized: Vec<TestStruct> = deserializer.deserialize_batch(&serialized).unwrap();

        assert_eq!(items, deserialized);
    }
}
use crate::error::{Error, Result};
use std::collections::HashMap;

/// Key compression schemes for reducing storage overhead
#[derive(Debug, Clone, PartialEq)]
pub enum KeyCompressionType {
    /// No compression
    None,
    /// Prefix compression - remove common prefixes
    Prefix,
    /// Dictionary compression - replace common patterns with shorter codes
    Dictionary,
    /// Delta compression - store differences between consecutive keys
    Delta,
    /// Hybrid compression - combination of prefix and dictionary
    Hybrid,
}

pub trait KeyCompressor: Send + Sync {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock>;
    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>>;
    fn compression_ratio(&self, original_size: usize, compressed_size: usize) -> f64 {
        if original_size == 0 {
            1.0
        } else {
            compressed_size as f64 / original_size as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressedKeyBlock {
    pub compression_type: KeyCompressionType,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub key_count: u32,
    pub original_size: u32,
    pub compressed_size: u32,
}

/// Prefix compression - removes common prefixes from keys
pub struct PrefixCompressor {
    min_prefix_length: usize,
}

impl PrefixCompressor {
    pub fn new(min_prefix_length: usize) -> Self {
        Self { min_prefix_length }
    }

    fn find_common_prefix(keys: &[Vec<u8>]) -> Vec<u8> {
        if keys.is_empty() {
            return Vec::new();
        }

        let first_key = &keys[0];
        let mut prefix_len = first_key.len();

        for key in keys.iter().skip(1) {
            let common_len = first_key
                .iter()
                .zip(key.iter())
                .take_while(|(a, b)| a == b)
                .count();
            prefix_len = prefix_len.min(common_len);
            
            if prefix_len == 0 {
                break;
            }
        }

        first_key[..prefix_len].to_vec()
    }
}

impl KeyCompressor for PrefixCompressor {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        if keys.is_empty() {
            return Ok(CompressedKeyBlock {
                compression_type: KeyCompressionType::Prefix,
                data: Vec::new(),
                metadata: Vec::new(),
                key_count: 0,
                original_size: 0,
                compressed_size: 0,
            });
        }

        let common_prefix = Self::find_common_prefix(keys);
        
        if common_prefix.len() < self.min_prefix_length {
            // Not worth compressing
            return self.compress_uncompressed(keys);
        }

        let mut compressed_data = Vec::new();
        let mut original_size = 0;

        // Write prefix length and prefix
        compressed_data.extend_from_slice(&(common_prefix.len() as u32).to_le_bytes());
        compressed_data.extend_from_slice(&common_prefix);

        // Write compressed keys (suffixes)
        for key in keys {
            original_size += key.len();
            
            let suffix = &key[common_prefix.len()..];
            compressed_data.extend_from_slice(&(suffix.len() as u32).to_le_bytes());
            compressed_data.extend_from_slice(suffix);
        }

        Ok(CompressedKeyBlock {
            compression_type: KeyCompressionType::Prefix,
            data: compressed_data,
            metadata: Vec::new(),
            key_count: keys.len() as u32,
            original_size: original_size as u32,
            compressed_size: compressed_data.len() as u32,
        })
    }

    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>> {
        if compressed.key_count == 0 {
            return Ok(Vec::new());
        }

        if compressed.compression_type != KeyCompressionType::Prefix {
            return Err(Error::Storage("Invalid compression type for prefix decompressor".to_string()));
        }

        let mut data = compressed.data.as_slice();
        let mut keys = Vec::new();

        // Read prefix
        if data.len() < 4 {
            return Err(Error::Storage("Corrupted compressed key block".to_string()));
        }
        
        let prefix_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        data = &data[4..];
        
        if data.len() < prefix_len {
            return Err(Error::Storage("Corrupted compressed key block".to_string()));
        }
        
        let prefix = &data[..prefix_len];
        data = &data[prefix_len..];

        // Read suffixes and reconstruct keys
        for _ in 0..compressed.key_count {
            if data.len() < 4 {
                return Err(Error::Storage("Corrupted compressed key block".to_string()));
            }
            
            let suffix_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[4..];
            
            if data.len() < suffix_len {
                return Err(Error::Storage("Corrupted compressed key block".to_string()));
            }
            
            let suffix = &data[..suffix_len];
            data = &data[suffix_len..];

            let mut key = Vec::with_capacity(prefix.len() + suffix.len());
            key.extend_from_slice(prefix);
            key.extend_from_slice(suffix);
            keys.push(key);
        }

        Ok(keys)
    }
}

impl PrefixCompressor {
    fn compress_uncompressed(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        let mut compressed_data = Vec::new();
        let mut original_size = 0;

        // Write that prefix length is 0 (no compression)
        compressed_data.extend_from_slice(&0u32.to_le_bytes());

        // Write all keys as-is
        for key in keys {
            original_size += key.len();
            compressed_data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            compressed_data.extend_from_slice(key);
        }

        Ok(CompressedKeyBlock {
            compression_type: KeyCompressionType::Prefix,
            data: compressed_data,
            metadata: Vec::new(),
            key_count: keys.len() as u32,
            original_size: original_size as u32,
            compressed_size: compressed_data.len() as u32,
        })
    }
}

/// Dictionary compression - replaces common patterns with codes
pub struct DictionaryCompressor {
    dictionary: HashMap<Vec<u8>, u16>,
    reverse_dictionary: HashMap<u16, Vec<u8>>,
    next_code: u16,
    min_pattern_length: usize,
    max_dictionary_size: usize,
}

impl DictionaryCompressor {
    pub fn new(min_pattern_length: usize, max_dictionary_size: usize) -> Self {
        Self {
            dictionary: HashMap::new(),
            reverse_dictionary: HashMap::new(),
            next_code: 1, // Reserve 0 for special use
            min_pattern_length,
            max_dictionary_size,
        }
    }

    fn build_dictionary(&mut self, keys: &[Vec<u8>]) {
        self.dictionary.clear();
        self.reverse_dictionary.clear();
        self.next_code = 1;

        let mut pattern_counts: HashMap<Vec<u8>, u32> = HashMap::new();

        // Count pattern frequencies
        for key in keys {
            for i in 0..key.len() {
                for j in (i + self.min_pattern_length)..=key.len() {
                    let pattern = key[i..j].to_vec();
                    *pattern_counts.entry(pattern).or_insert(0) += 1;
                }
            }
        }

        // Sort patterns by frequency and benefit
        let mut patterns: Vec<_> = pattern_counts
            .into_iter()
            .filter(|(pattern, count)| {
                *count > 1 && pattern.len() >= self.min_pattern_length
            })
            .map(|(pattern, count)| {
                let benefit = (pattern.len() as u32 - 2) * count; // -2 for the code bytes
                (pattern, count, benefit)
            })
            .collect();

        patterns.sort_by(|a, b| b.2.cmp(&a.2)); // Sort by benefit (descending)

        // Add most beneficial patterns to dictionary
        for (pattern, _count, _benefit) in patterns.into_iter().take(self.max_dictionary_size) {
            self.dictionary.insert(pattern.clone(), self.next_code);
            self.reverse_dictionary.insert(self.next_code, pattern);
            self.next_code += 1;
        }
    }

    fn compress_key(&self, key: &[u8]) -> Vec<u8> {
        let mut compressed = Vec::new();
        let mut i = 0;

        while i < key.len() {
            let mut best_match_len = 0;
            let mut best_code = 0u16;

            // Find longest matching pattern
            for j in (i + self.min_pattern_length)..=key.len() {
                let pattern = &key[i..j];
                if let Some(&code) = self.dictionary.get(pattern) {
                    if pattern.len() > best_match_len {
                        best_match_len = pattern.len();
                        best_code = code;
                    }
                }
            }

            if best_match_len > 0 {
                // Write code (2 bytes)
                compressed.extend_from_slice(&best_code.to_le_bytes());
                i += best_match_len;
            } else {
                // Write literal byte with escape code (0)
                compressed.extend_from_slice(&0u16.to_le_bytes());
                compressed.push(key[i]);
                i += 1;
            }
        }

        compressed
    }
}

impl KeyCompressor for DictionaryCompressor {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        if keys.is_empty() {
            return Ok(CompressedKeyBlock {
                compression_type: KeyCompressionType::Dictionary,
                data: Vec::new(),
                metadata: Vec::new(),
                key_count: 0,
                original_size: 0,
                compressed_size: 0,
            });
        }

        // Clone self to build dictionary without mutating
        let mut compressor = Self::new(self.min_pattern_length, self.max_dictionary_size);
        compressor.build_dictionary(keys);

        let mut compressed_data = Vec::new();
        let mut original_size = 0;

        // Serialize dictionary as metadata
        let mut metadata = Vec::new();
        metadata.extend_from_slice(&(compressor.dictionary.len() as u32).to_le_bytes());
        
        for (pattern, code) in &compressor.dictionary {
            metadata.extend_from_slice(&code.to_le_bytes());
            metadata.extend_from_slice(&(pattern.len() as u32).to_le_bytes());
            metadata.extend_from_slice(pattern);
        }

        // Compress each key
        for key in keys {
            original_size += key.len();
            let compressed_key = compressor.compress_key(key);
            compressed_data.extend_from_slice(&(compressed_key.len() as u32).to_le_bytes());
            compressed_data.extend_from_slice(&compressed_key);
        }

        Ok(CompressedKeyBlock {
            compression_type: KeyCompressionType::Dictionary,
            data: compressed_data,
            metadata,
            key_count: keys.len() as u32,
            original_size: original_size as u32,
            compressed_size: compressed_data.len() as u32,
        })
    }

    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>> {
        if compressed.key_count == 0 {
            return Ok(Vec::new());
        }

        if compressed.compression_type != KeyCompressionType::Dictionary {
            return Err(Error::Storage("Invalid compression type for dictionary decompressor".to_string()));
        }

        // Rebuild dictionary from metadata
        let mut dictionary = HashMap::new();
        let mut metadata = compressed.metadata.as_slice();

        if metadata.len() < 4 {
            return Err(Error::Storage("Corrupted dictionary metadata".to_string()));
        }

        let dict_size = u32::from_le_bytes([metadata[0], metadata[1], metadata[2], metadata[3]]) as usize;
        metadata = &metadata[4..];

        for _ in 0..dict_size {
            if metadata.len() < 6 {
                return Err(Error::Storage("Corrupted dictionary metadata".to_string()));
            }

            let code = u16::from_le_bytes([metadata[0], metadata[1]]);
            let pattern_len = u32::from_le_bytes([metadata[2], metadata[3], metadata[4], metadata[5]]) as usize;
            metadata = &metadata[6..];

            if metadata.len() < pattern_len {
                return Err(Error::Storage("Corrupted dictionary metadata".to_string()));
            }

            let pattern = metadata[..pattern_len].to_vec();
            metadata = &metadata[pattern_len..];

            dictionary.insert(code, pattern);
        }

        // Decompress keys
        let mut data = compressed.data.as_slice();
        let mut keys = Vec::new();

        for _ in 0..compressed.key_count {
            if data.len() < 4 {
                return Err(Error::Storage("Corrupted compressed key data".to_string()));
            }

            let compressed_key_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[4..];

            if data.len() < compressed_key_len {
                return Err(Error::Storage("Corrupted compressed key data".to_string()));
            }

            let compressed_key = &data[..compressed_key_len];
            data = &data[compressed_key_len..];

            let key = self.decompress_key(compressed_key, &dictionary)?;
            keys.push(key);
        }

        Ok(keys)
    }
}

impl DictionaryCompressor {
    fn decompress_key(&self, compressed: &[u8], dictionary: &HashMap<u16, Vec<u8>>) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        let mut i = 0;

        while i + 1 < compressed.len() {
            let code = u16::from_le_bytes([compressed[i], compressed[i + 1]]);
            i += 2;

            if code == 0 {
                // Literal byte follows
                if i >= compressed.len() {
                    return Err(Error::Storage("Corrupted compressed key".to_string()));
                }
                key.push(compressed[i]);
                i += 1;
            } else {
                // Dictionary pattern
                if let Some(pattern) = dictionary.get(&code) {
                    key.extend_from_slice(pattern);
                } else {
                    return Err(Error::Storage("Unknown dictionary code".to_string()));
                }
            }
        }

        Ok(key)
    }
}

/// Factory for creating key compressors
pub struct KeyCompressionFactory;

impl KeyCompressionFactory {
    pub fn create_compressor(compression_type: KeyCompressionType) -> Box<dyn KeyCompressor> {
        match compression_type {
            KeyCompressionType::None => Box::new(NoCompressionCompressor),
            KeyCompressionType::Prefix => Box::new(PrefixCompressor::new(3)),
            KeyCompressionType::Dictionary => Box::new(DictionaryCompressor::new(3, 256)),
            KeyCompressionType::Delta => Box::new(DeltaCompressor::new()),
            KeyCompressionType::Hybrid => Box::new(HybridCompressor::new()),
        }
    }
}

/// No compression - pass through
struct NoCompressionCompressor;

impl KeyCompressor for NoCompressionCompressor {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        let mut data = Vec::new();
        let mut original_size = 0;

        for key in keys {
            original_size += key.len();
            data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            data.extend_from_slice(key);
        }

        Ok(CompressedKeyBlock {
            compression_type: KeyCompressionType::None,
            data,
            metadata: Vec::new(),
            key_count: keys.len() as u32,
            original_size: original_size as u32,
            compressed_size: data.len() as u32,
        })
    }

    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>> {
        let mut data = compressed.data.as_slice();
        let mut keys = Vec::new();

        for _ in 0..compressed.key_count {
            if data.len() < 4 {
                return Err(Error::Storage("Corrupted uncompressed key data".to_string()));
            }

            let key_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[4..];

            if data.len() < key_len {
                return Err(Error::Storage("Corrupted uncompressed key data".to_string()));
            }

            let key = data[..key_len].to_vec();
            data = &data[key_len..];
            keys.push(key);
        }

        Ok(keys)
    }
}

/// Delta compression - store differences between consecutive keys
struct DeltaCompressor;

impl DeltaCompressor {
    fn new() -> Self {
        Self
    }
}

impl KeyCompressor for DeltaCompressor {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        if keys.is_empty() {
            return Ok(CompressedKeyBlock {
                compression_type: KeyCompressionType::Delta,
                data: Vec::new(),
                metadata: Vec::new(),
                key_count: 0,
                original_size: 0,
                compressed_size: 0,
            });
        }

        let mut compressed_data = Vec::new();
        let mut original_size = 0;

        // Store first key as-is
        let first_key = &keys[0];
        original_size += first_key.len();
        compressed_data.extend_from_slice(&(first_key.len() as u32).to_le_bytes());
        compressed_data.extend_from_slice(first_key);

        // Store deltas for subsequent keys
        for i in 1..keys.len() {
            let prev_key = &keys[i - 1];
            let curr_key = &keys[i];
            original_size += curr_key.len();

            let delta = self.compute_delta(prev_key, curr_key);
            compressed_data.extend_from_slice(&(delta.len() as u32).to_le_bytes());
            compressed_data.extend_from_slice(&delta);
        }

        Ok(CompressedKeyBlock {
            compression_type: KeyCompressionType::Delta,
            data: compressed_data,
            metadata: Vec::new(),
            key_count: keys.len() as u32,
            original_size: original_size as u32,
            compressed_size: compressed_data.len() as u32,
        })
    }

    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>> {
        if compressed.key_count == 0 {
            return Ok(Vec::new());
        }

        let mut data = compressed.data.as_slice();
        let mut keys = Vec::new();

        // Read first key
        if data.len() < 4 {
            return Err(Error::Storage("Corrupted delta compressed data".to_string()));
        }

        let first_key_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        data = &data[4..];

        if data.len() < first_key_len {
            return Err(Error::Storage("Corrupted delta compressed data".to_string()));
        }

        let first_key = data[..first_key_len].to_vec();
        data = &data[first_key_len..];
        keys.push(first_key.clone());

        // Reconstruct subsequent keys from deltas
        let mut prev_key = first_key;
        for _ in 1..compressed.key_count {
            if data.len() < 4 {
                return Err(Error::Storage("Corrupted delta compressed data".to_string()));
            }

            let delta_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
            data = &data[4..];

            if data.len() < delta_len {
                return Err(Error::Storage("Corrupted delta compressed data".to_string()));
            }

            let delta = &data[..delta_len];
            data = &data[delta_len..];

            let curr_key = self.apply_delta(&prev_key, delta)?;
            keys.push(curr_key.clone());
            prev_key = curr_key;
        }

        Ok(keys)
    }
}

impl DeltaCompressor {
    fn compute_delta(&self, prev_key: &[u8], curr_key: &[u8]) -> Vec<u8> {
        // Simple delta: common prefix length + suffix
        let common_prefix_len = prev_key
            .iter()
            .zip(curr_key.iter())
            .take_while(|(a, b)| a == b)
            .count();

        let mut delta = Vec::new();
        delta.extend_from_slice(&(common_prefix_len as u32).to_le_bytes());
        delta.extend_from_slice(&((curr_key.len() - common_prefix_len) as u32).to_le_bytes());
        delta.extend_from_slice(&curr_key[common_prefix_len..]);
        delta
    }

    fn apply_delta(&self, prev_key: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
        if delta.len() < 8 {
            return Err(Error::Storage("Invalid delta format".to_string()));
        }

        let common_prefix_len = u32::from_le_bytes([delta[0], delta[1], delta[2], delta[3]]) as usize;
        let suffix_len = u32::from_le_bytes([delta[4], delta[5], delta[6], delta[7]]) as usize;

        if delta.len() < 8 + suffix_len {
            return Err(Error::Storage("Invalid delta format".to_string()));
        }

        if common_prefix_len > prev_key.len() {
            return Err(Error::Storage("Invalid delta: common prefix too long".to_string()));
        }

        let mut result = Vec::with_capacity(common_prefix_len + suffix_len);
        result.extend_from_slice(&prev_key[..common_prefix_len]);
        result.extend_from_slice(&delta[8..8 + suffix_len]);

        Ok(result)
    }
}

/// Hybrid compressor - combines prefix and dictionary compression
struct HybridCompressor {
    prefix_compressor: PrefixCompressor,
    dictionary_compressor: DictionaryCompressor,
}

impl HybridCompressor {
    fn new() -> Self {
        Self {
            prefix_compressor: PrefixCompressor::new(3),
            dictionary_compressor: DictionaryCompressor::new(3, 128),
        }
    }
}

impl KeyCompressor for HybridCompressor {
    fn compress(&self, keys: &[Vec<u8>]) -> Result<CompressedKeyBlock> {
        // Try both compression methods and pick the best one
        let prefix_result = self.prefix_compressor.compress(keys)?;
        let dict_result = self.dictionary_compressor.compress(keys)?;

        if prefix_result.compressed_size <= dict_result.compressed_size {
            Ok(prefix_result)
        } else {
            Ok(dict_result)
        }
    }

    fn decompress(&self, compressed: &CompressedKeyBlock) -> Result<Vec<Vec<u8>>> {
        match compressed.compression_type {
            KeyCompressionType::Prefix => self.prefix_compressor.decompress(compressed),
            KeyCompressionType::Dictionary => self.dictionary_compressor.decompress(compressed),
            _ => Err(Error::Storage("Unsupported compression type for hybrid decompressor".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_compression() {
        let compressor = PrefixCompressor::new(2);
        
        let keys = vec![
            b"user:123:name".to_vec(),
            b"user:123:email".to_vec(),
            b"user:123:age".to_vec(),
        ];

        let compressed = compressor.compress(&keys).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(keys, decompressed);
        assert!(compressed.compressed_size < compressed.original_size);
    }

    #[test]
    fn test_dictionary_compression() {
        let compressor = DictionaryCompressor::new(3, 256);
        
        let keys = vec![
            b"prefix_123_suffix".to_vec(),
            b"prefix_456_suffix".to_vec(),
            b"prefix_789_suffix".to_vec(),
        ];

        let compressed = compressor.compress(&keys).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(keys, decompressed);
    }

    #[test]
    fn test_delta_compression() {
        let compressor = DeltaCompressor::new();
        
        let keys = vec![
            b"user001".to_vec(),
            b"user002".to_vec(),
            b"user003".to_vec(),
        ];

        let compressed = compressor.compress(&keys).unwrap();
        let decompressed = compressor.decompress(&compressed).unwrap();

        assert_eq!(keys, decompressed);
        assert!(compressed.compressed_size < compressed.original_size);
    }
}
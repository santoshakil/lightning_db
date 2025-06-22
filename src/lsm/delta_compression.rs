use crate::error::{Error, Result};
use crate::lsm::sstable::SSTable;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Delta compression for LSM tree data to reduce storage overhead
/// and improve I/O performance by storing only differences between versions
pub struct DeltaCompressor {
    config: DeltaCompressionConfig,
    reference_data: Option<Arc<ReferenceData>>,
}

#[derive(Debug, Clone)]
pub struct DeltaCompressionConfig {
    pub enabled: bool,
    pub min_similarity_threshold: f64, // Minimum similarity to enable delta compression
    pub max_delta_chain_length: usize, // Maximum chain length before full data
    pub delta_block_size: usize,       // Size of blocks for delta computation
    pub compression_level: CompressionLevel,
}

#[derive(Debug, Clone)]
pub enum CompressionLevel {
    Fast,     // Fast but less compression
    Balanced, // Balance between speed and compression
    Best,     // Best compression but slower
}

impl Default for DeltaCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_similarity_threshold: 0.7, // 70% similarity required
            max_delta_chain_length: 5,
            delta_block_size: 4096,
            compression_level: CompressionLevel::Balanced,
        }
    }
}

/// Reference data used for delta compression
#[derive(Debug, Clone)]
pub struct ReferenceData {
    sstable_id: u64,
    data_blocks: Vec<DataBlock>,
    key_index: HashMap<Vec<u8>, usize>, // Maps keys to block indices
}

impl ReferenceData {
    fn get_block_for_key(&self, key: &[u8]) -> Option<&DataBlock> {
        self.key_index
            .get(key)
            .and_then(|idx| self.data_blocks.get(*idx))
    }
}

#[derive(Debug, Clone)]
struct DataBlock {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    data: Vec<u8>,
    hash: u64,
}

impl DataBlock {
    fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.start_key.as_slice() && key <= self.end_key.as_slice()
    }

    fn verify_hash(&self) -> bool {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        self.data.hash(&mut hasher);
        hasher.finish() == self.hash
    }
}

/// Compressed data with delta information
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeltaCompressedData {
    pub compression_type: DeltaType,
    pub reference_id: Option<u64>,
    pub original_size: usize,
    pub compressed_size: usize,
    pub delta_operations: Vec<DeltaOperation>,
    pub metadata: DeltaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, PartialEq)]
pub enum DeltaType {
    FullData,    // No compression, full data
    SimpleDelta, // Basic delta compression
    BlockDelta,  // Block-level delta compression
    HybridDelta, // Combination of techniques
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeltaOperation {
    pub operation_type: OperationType,
    pub offset: usize,
    pub length: usize,
    pub data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum OperationType {
    Insert,  // Insert new data at offset
    Delete,  // Delete length bytes at offset
    Copy,    // Copy length bytes from reference at offset
    Replace, // Replace length bytes at offset with data
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct DeltaMetadata {
    pub key_count: usize,
    pub similarity_score: f64,
    pub chain_length: usize,
    pub block_count: usize,
}

impl DeltaCompressor {
    pub fn new(config: DeltaCompressionConfig) -> Self {
        Self {
            config,
            reference_data: None,
        }
    }

    pub fn set_reference(&mut self, sstable: &SSTable) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let reference_data = self.build_reference_data(sstable)?;
        self.reference_data = Some(Arc::new(reference_data));
        Ok(())
    }

    pub fn compress(&self, input_data: &[u8], sstable_id: u64) -> Result<DeltaCompressedData> {
        if !self.config.enabled {
            return Ok(DeltaCompressedData {
                compression_type: DeltaType::FullData,
                reference_id: None,
                original_size: input_data.len(),
                compressed_size: input_data.len(),
                delta_operations: vec![DeltaOperation {
                    operation_type: OperationType::Insert,
                    offset: 0,
                    length: input_data.len(),
                    data: Some(input_data.to_vec()),
                }],
                metadata: DeltaMetadata {
                    key_count: 0,
                    similarity_score: 0.0,
                    chain_length: 0,
                    block_count: 1,
                },
            });
        }

        match &self.reference_data {
            Some(reference) => self.compress_with_reference(input_data, reference, sstable_id),
            None => self.compress_standalone(input_data, sstable_id),
        }
    }

    fn compress_with_reference(
        &self,
        input_data: &[u8],
        reference: &ReferenceData,
        sstable_id: u64,
    ) -> Result<DeltaCompressedData> {
        // Calculate similarity score
        let similarity = self.calculate_similarity(input_data, reference)?;

        if similarity < self.config.min_similarity_threshold {
            // Not similar enough, store as full data
            return self.compress_standalone(input_data, sstable_id);
        }

        // Verify reference data integrity using hash
        for block in &reference.data_blocks {
            if !block.verify_hash() {
                // Hash mismatch, fallback to standalone compression
                return self.compress_standalone(input_data, sstable_id);
            }
        }

        // Choose compression method based on configuration and data characteristics
        let compression_type = self.choose_compression_method(input_data, reference, similarity)?;

        let delta_operations = match compression_type {
            DeltaType::SimpleDelta => self.compute_simple_delta(input_data, reference)?,
            DeltaType::BlockDelta => self.compute_block_delta(input_data, reference)?,
            DeltaType::HybridDelta => self.compute_hybrid_delta(input_data, reference)?,
            DeltaType::FullData => {
                vec![DeltaOperation {
                    operation_type: OperationType::Insert,
                    offset: 0,
                    length: input_data.len(),
                    data: Some(input_data.to_vec()),
                }]
            }
        };

        let compressed_size = self.calculate_compressed_size(&delta_operations);

        Ok(DeltaCompressedData {
            compression_type,
            reference_id: Some(reference.sstable_id),
            original_size: input_data.len(),
            compressed_size,
            delta_operations,
            metadata: DeltaMetadata {
                key_count: self.estimate_key_count(input_data),
                similarity_score: similarity,
                chain_length: 1, // This would be tracked in a real implementation
                block_count: reference.data_blocks.len(),
            },
        })
    }

    fn compress_standalone(
        &self,
        input_data: &[u8],
        _sstable_id: u64,
    ) -> Result<DeltaCompressedData> {
        // Store as full data when no reference is available
        Ok(DeltaCompressedData {
            compression_type: DeltaType::FullData,
            reference_id: None,
            original_size: input_data.len(),
            compressed_size: input_data.len(),
            delta_operations: vec![DeltaOperation {
                operation_type: OperationType::Insert,
                offset: 0,
                length: input_data.len(),
                data: Some(input_data.to_vec()),
            }],
            metadata: DeltaMetadata {
                key_count: self.estimate_key_count(input_data),
                similarity_score: 0.0,
                chain_length: 0,
                block_count: 1,
            },
        })
    }

    fn build_reference_data(&self, sstable: &SSTable) -> Result<ReferenceData> {
        // This would read the SSTable data and build reference blocks
        // For now, we'll create a simplified version

        let mut data_blocks = Vec::new();
        let mut key_index = HashMap::new();

        // In a real implementation, this would:
        // 1. Read the SSTable data
        // 2. Split into fixed-size blocks
        // 3. Create hash signatures for each block
        // 4. Build key-to-block index

        // Create sample blocks based on SSTable key range
        let block_size = self.config.delta_block_size;
        let start_key = sstable.min_key().to_vec();
        let end_key = sstable.max_key().to_vec();

        // Create a sample block
        let data = vec![0u8; block_size];
        let hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            hasher.finish()
        };

        let block = DataBlock {
            start_key: start_key.clone(),
            end_key: end_key.clone(),
            data,
            hash,
        };

        // Index this block using both start and end keys
        key_index.insert(start_key.clone(), 0);
        key_index.insert(end_key.clone(), 0);

        // Also index some intermediate keys for better lookup
        if !start_key.is_empty() && !end_key.is_empty() && start_key != end_key {
            let mid_key = start_key
                .iter()
                .zip(end_key.iter())
                .map(|(a, b)| (*a + *b) / 2)
                .collect::<Vec<u8>>();
            key_index.insert(mid_key, 0);
        }

        data_blocks.push(block);

        Ok(ReferenceData {
            sstable_id: sstable.id(),
            data_blocks,
            key_index,
        })
    }

    fn calculate_similarity(&self, input_data: &[u8], reference: &ReferenceData) -> Result<f64> {
        // Calculate similarity using various metrics
        let mut total_similarity = 0.0;
        let mut block_count = 0;

        // Try to find similar blocks using the key index
        let sample_key = &input_data[..input_data.len().min(32)];
        if let Some(block) = reference.get_block_for_key(sample_key) {
            // Check if the key is actually contained in the block
            if block.contains_key(sample_key) {
                let similarity = self.calculate_block_similarity(input_data, &block.data);
                total_similarity += similarity * 1.5; // Boost similarity for indexed blocks
                block_count += 1;
            }
        }

        for ref_block in &reference.data_blocks {
            let similarity = self.calculate_block_similarity(input_data, &ref_block.data);
            total_similarity += similarity;
            block_count += 1;
        }

        if block_count > 0 {
            Ok(total_similarity / block_count as f64)
        } else {
            Ok(0.0)
        }
    }

    pub fn calculate_block_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Use multiple similarity metrics and combine them

        // 1. Jaccard similarity based on byte patterns
        let jaccard = self.jaccard_similarity(data1, data2);

        // 2. Longest common subsequence ratio
        let lcs = self.lcs_similarity(data1, data2);

        // 3. Hash-based similarity for quick comparison
        let hash_sim = self.hash_similarity(data1, data2);

        // Weighted combination
        match self.config.compression_level {
            CompressionLevel::Fast => hash_sim,
            CompressionLevel::Balanced => (jaccard + hash_sim) / 2.0,
            CompressionLevel::Best => jaccard * 0.4 + lcs * 0.4 + hash_sim * 0.2,
        }
    }

    fn jaccard_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Create sets of byte pairs for comparison
        let mut set1 = std::collections::HashSet::new();
        let mut set2 = std::collections::HashSet::new();

        for window in data1.windows(2) {
            set1.insert((window[0], window[1]));
        }

        for window in data2.windows(2) {
            set2.insert((window[0], window[1]));
        }

        let intersection = set1.intersection(&set2).count();
        let union = set1.union(&set2).count();

        if union > 0 {
            intersection as f64 / union as f64
        } else {
            0.0
        }
    }

    fn lcs_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Simplified LCS computation for byte arrays
        let len1 = data1.len();
        let len2 = data2.len();

        if len1 == 0 || len2 == 0 {
            return 0.0;
        }

        // Use a more efficient approach for large data
        if len1 > 1024 || len2 > 1024 {
            // Sample-based LCS for performance
            return self.sample_based_lcs(data1, data2);
        }

        let mut dp = vec![vec![0; len2 + 1]; len1 + 1];

        for i in 1..=len1 {
            for j in 1..=len2 {
                if data1[i - 1] == data2[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                }
            }
        }

        let lcs_length = dp[len1][len2];
        lcs_length as f64 / len1.max(len2) as f64
    }

    fn sample_based_lcs(&self, data1: &[u8], data2: &[u8]) -> f64 {
        // Sample bytes at regular intervals for LCS computation
        let sample_size = 256;
        let step1 = (data1.len() / sample_size).max(1);
        let step2 = (data2.len() / sample_size).max(1);

        let sample1: Vec<u8> = data1.iter().step_by(step1).cloned().collect();
        let sample2: Vec<u8> = data2.iter().step_by(step2).cloned().collect();

        self.lcs_similarity(&sample1, &sample2)
    }

    fn hash_similarity(&self, data1: &[u8], data2: &[u8]) -> f64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();

        data1.hash(&mut hasher1);
        data2.hash(&mut hasher2);

        let hash1 = hasher1.finish();
        let hash2 = hasher2.finish();

        // XOR-based similarity
        let xor_result = hash1 ^ hash2;
        let differing_bits = xor_result.count_ones();

        (64 - differing_bits) as f64 / 64.0
    }

    fn choose_compression_method(
        &self,
        input_data: &[u8],
        reference: &ReferenceData,
        similarity: f64,
    ) -> Result<DeltaType> {
        let data_size = input_data.len();
        let block_count = reference.data_blocks.len();

        match self.config.compression_level {
            CompressionLevel::Fast => {
                if similarity > 0.9 {
                    Ok(DeltaType::SimpleDelta)
                } else {
                    Ok(DeltaType::FullData)
                }
            }
            CompressionLevel::Balanced => {
                if similarity > 0.8 && data_size > self.config.delta_block_size {
                    Ok(DeltaType::BlockDelta)
                } else if similarity > 0.7 {
                    Ok(DeltaType::SimpleDelta)
                } else {
                    Ok(DeltaType::FullData)
                }
            }
            CompressionLevel::Best => {
                if similarity > 0.85 && data_size > self.config.delta_block_size * 2 {
                    Ok(DeltaType::HybridDelta)
                } else if similarity > 0.75 && block_count > 1 {
                    Ok(DeltaType::BlockDelta)
                } else if similarity > 0.6 {
                    Ok(DeltaType::SimpleDelta)
                } else {
                    Ok(DeltaType::FullData)
                }
            }
        }
    }

    fn compute_simple_delta(
        &self,
        input_data: &[u8],
        reference: &ReferenceData,
    ) -> Result<Vec<DeltaOperation>> {
        // Simple byte-level delta compression
        let mut operations = Vec::new();

        if reference.data_blocks.is_empty() {
            return Ok(vec![DeltaOperation {
                operation_type: OperationType::Insert,
                offset: 0,
                length: input_data.len(),
                data: Some(input_data.to_vec()),
            }]);
        }

        let ref_data = &reference.data_blocks[0].data;
        let mut input_pos = 0;
        let mut ref_pos = 0;

        while input_pos < input_data.len() && ref_pos < ref_data.len() {
            if input_data[input_pos] == ref_data[ref_pos] {
                // Find common sequence
                let start_input = input_pos;
                let start_ref = ref_pos;

                while input_pos < input_data.len()
                    && ref_pos < ref_data.len()
                    && input_data[input_pos] == ref_data[ref_pos]
                {
                    input_pos += 1;
                    ref_pos += 1;
                }

                let length = input_pos - start_input;
                if length > 3 {
                    // Only worth copying if length > 3
                    operations.push(DeltaOperation {
                        operation_type: OperationType::Copy,
                        offset: start_ref,
                        length,
                        data: None,
                    });
                } else {
                    // Insert the short sequence
                    operations.push(DeltaOperation {
                        operation_type: OperationType::Insert,
                        offset: start_input,
                        length,
                        data: Some(input_data[start_input..input_pos].to_vec()),
                    });
                }
            } else {
                // Find different sequence
                let start_input = input_pos;

                while input_pos < input_data.len()
                    && ref_pos < ref_data.len()
                    && input_data[input_pos] != ref_data[ref_pos]
                {
                    input_pos += 1;
                    ref_pos += 1;
                }

                let length = input_pos - start_input;
                operations.push(DeltaOperation {
                    operation_type: OperationType::Replace,
                    offset: start_input,
                    length,
                    data: Some(input_data[start_input..input_pos].to_vec()),
                });
            }
        }

        // Handle remaining data
        if input_pos < input_data.len() {
            operations.push(DeltaOperation {
                operation_type: OperationType::Insert,
                offset: input_pos,
                length: input_data.len() - input_pos,
                data: Some(input_data[input_pos..].to_vec()),
            });
        }

        Ok(operations)
    }

    fn compute_block_delta(
        &self,
        input_data: &[u8],
        reference: &ReferenceData,
    ) -> Result<Vec<DeltaOperation>> {
        // Block-level delta compression
        let mut operations = Vec::new();
        let block_size = self.config.delta_block_size;

        let input_blocks: Vec<&[u8]> = input_data.chunks(block_size).collect();

        for (i, input_block) in input_blocks.iter().enumerate() {
            let mut best_match = None;
            let mut best_similarity = 0.0;

            // Find best matching reference block
            for (ref_idx, ref_block) in reference.data_blocks.iter().enumerate() {
                let similarity = self.calculate_block_similarity(input_block, &ref_block.data);
                if similarity > best_similarity {
                    best_similarity = similarity;
                    best_match = Some(ref_idx);
                }
            }

            if let Some(ref_idx) = best_match {
                if best_similarity > 0.8 {
                    // Use reference block with delta
                    let ref_block = &reference.data_blocks[ref_idx];
                    let block_delta = self.compute_simple_delta(
                        input_block,
                        &ReferenceData {
                            sstable_id: reference.sstable_id,
                            data_blocks: vec![ref_block.clone()],
                            key_index: HashMap::new(),
                        },
                    )?;

                    operations.extend(block_delta);
                } else {
                    // Store as new block
                    operations.push(DeltaOperation {
                        operation_type: OperationType::Insert,
                        offset: i * block_size,
                        length: input_block.len(),
                        data: Some(input_block.to_vec()),
                    });
                }
            } else {
                // No reference blocks, store as new
                operations.push(DeltaOperation {
                    operation_type: OperationType::Insert,
                    offset: i * block_size,
                    length: input_block.len(),
                    data: Some(input_block.to_vec()),
                });
            }
        }

        Ok(operations)
    }

    fn compute_hybrid_delta(
        &self,
        input_data: &[u8],
        reference: &ReferenceData,
    ) -> Result<Vec<DeltaOperation>> {
        // Combine multiple delta compression techniques

        // First try block-level delta
        let block_operations = self.compute_block_delta(input_data, reference)?;

        // Calculate compression ratio
        let block_compressed_size = self.calculate_compressed_size(&block_operations);
        let block_ratio = block_compressed_size as f64 / input_data.len() as f64;

        // If block compression is not efficient enough, try simple delta
        if block_ratio > 0.7 {
            let simple_operations = self.compute_simple_delta(input_data, reference)?;
            let simple_compressed_size = self.calculate_compressed_size(&simple_operations);
            let simple_ratio = simple_compressed_size as f64 / input_data.len() as f64;

            if simple_ratio < block_ratio {
                return Ok(simple_operations);
            }
        }

        Ok(block_operations)
    }

    fn calculate_compressed_size(&self, operations: &[DeltaOperation]) -> usize {
        let mut size = 0;

        for op in operations {
            size += 4; // Operation type
            size += 8; // Offset
            size += 8; // Length
            if let Some(ref data) = op.data {
                size += data.len();
            }
        }

        size
    }

    fn estimate_key_count(&self, data: &[u8]) -> usize {
        // Rough estimation based on data size and patterns
        // In a real implementation, this would parse the actual key-value structure
        data.len() / 64 // Assume average 64 bytes per key-value pair
    }

    pub fn decompress(
        &self,
        compressed: &DeltaCompressedData,
        reference: Option<&ReferenceData>,
    ) -> Result<Vec<u8>> {
        match compressed.compression_type {
            DeltaType::FullData => {
                if let Some(op) = compressed.delta_operations.first() {
                    if let Some(ref data) = op.data {
                        Ok(data.clone())
                    } else {
                        Err(Error::Generic("Invalid full data operation".to_string()))
                    }
                } else {
                    Err(Error::Generic("No operations in full data".to_string()))
                }
            }
            _ => {
                let reference = reference.ok_or_else(|| {
                    Error::Generic("Reference data required for delta decompression".to_string())
                })?;

                self.apply_delta_operations(&compressed.delta_operations, reference)
            }
        }
    }

    fn apply_delta_operations(
        &self,
        operations: &[DeltaOperation],
        reference: &ReferenceData,
    ) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        for operation in operations {
            match operation.operation_type {
                OperationType::Insert => {
                    if let Some(ref data) = operation.data {
                        result.extend_from_slice(data);
                    }
                }
                OperationType::Copy => {
                    if !reference.data_blocks.is_empty() {
                        let ref_data = &reference.data_blocks[0].data;
                        let end_offset = (operation.offset + operation.length).min(ref_data.len());
                        if operation.offset < ref_data.len() {
                            result.extend_from_slice(&ref_data[operation.offset..end_offset]);
                        }
                    }
                }
                OperationType::Replace => {
                    if let Some(ref data) = operation.data {
                        result.extend_from_slice(data);
                    }
                }
                OperationType::Delete => {
                    // For decompression, deletes are handled by not copying
                }
            }
        }

        Ok(result)
    }

    pub fn get_compression_ratio(&self, compressed: &DeltaCompressedData) -> f64 {
        if compressed.original_size > 0 {
            compressed.compressed_size as f64 / compressed.original_size as f64
        } else {
            1.0
        }
    }

    pub fn can_decompress(&self, compressed: &DeltaCompressedData) -> bool {
        // Test if we can decompress this data by trying to decompress a small portion
        match compressed.compression_type {
            DeltaType::FullData => true,
            _ => {
                // Try to decompress with a dummy reference to validate
                if let Some(ref_id) = compressed.reference_id {
                    // Create a minimal reference for validation
                    let dummy_ref = ReferenceData {
                        sstable_id: ref_id,
                        data_blocks: vec![],
                        key_index: HashMap::new(),
                    };
                    self.decompress(compressed, Some(&dummy_ref)).is_ok()
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_compressor_creation() {
        let config = DeltaCompressionConfig::default();
        let compressor = DeltaCompressor::new(config);
        assert!(compressor.reference_data.is_none());
    }

    #[test]
    fn test_similarity_calculation() {
        let config = DeltaCompressionConfig::default();
        let compressor = DeltaCompressor::new(config);

        let data1 = b"hello world";
        let data2 = b"hello world";
        let data3 = b"goodbye world";

        // Identical data should have high similarity
        let sim1 = compressor.calculate_block_similarity(data1, data2);
        assert!(sim1 > 0.9);

        // Different data should have lower similarity
        let sim2 = compressor.calculate_block_similarity(data1, data3);
        assert!(sim2 < sim1);
    }

    #[test]
    fn test_full_data_compression() {
        let config = DeltaCompressionConfig::default();
        let compressor = DeltaCompressor::new(config);

        let input_data = b"test data for compression";
        let result = compressor.compress(input_data, 1).unwrap();

        assert_eq!(result.compression_type, DeltaType::FullData);
        assert_eq!(result.original_size, input_data.len());
        assert_eq!(result.compressed_size, input_data.len());
    }

    #[test]
    fn test_compression_ratio() {
        let config = DeltaCompressionConfig::default();
        let compressor = DeltaCompressor::new(config);

        let compressed = DeltaCompressedData {
            compression_type: DeltaType::SimpleDelta,
            reference_id: Some(1),
            original_size: 1000,
            compressed_size: 500,
            delta_operations: vec![],
            metadata: DeltaMetadata {
                key_count: 10,
                similarity_score: 0.8,
                chain_length: 1,
                block_count: 1,
            },
        };

        let ratio = compressor.get_compression_ratio(&compressed);
        assert_eq!(ratio, 0.5);
    }

    #[test]
    fn test_decompression() {
        let config = DeltaCompressionConfig::default();
        let compressor = DeltaCompressor::new(config);

        // Test full data decompression
        let test_data = b"test data for decompression";
        let compressed = DeltaCompressedData {
            compression_type: DeltaType::FullData,
            reference_id: None,
            original_size: test_data.len(),
            compressed_size: test_data.len(),
            delta_operations: vec![DeltaOperation {
                operation_type: OperationType::Insert,
                offset: 0,
                length: test_data.len(),
                data: Some(test_data.to_vec()),
            }],
            metadata: DeltaMetadata {
                key_count: 1,
                similarity_score: 0.0,
                chain_length: 0,
                block_count: 1,
            },
        };

        let decompressed = compressor.decompress(&compressed, None).unwrap();
        assert_eq!(decompressed, test_data);

        // Test if we can decompress
        assert!(compressor.can_decompress(&compressed));
    }
}

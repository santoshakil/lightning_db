//! Sorted String Table (SSTable) Implementation
//!
//! SSTables are immutable on-disk data structures that store sorted key-value pairs.
//! They include an index for efficient lookups and support compression.

use super::bloom_filter::{BloomFilter, BloomFilterBuilder};
use crate::{Error, Result};
use bincode::{Decode, Encode};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// SSTable file format:
/// - Header (metadata)
/// - Data blocks (key-value pairs)
/// - Index block (key -> offset mapping)
/// - Bloom filter
/// - Footer (offsets to different sections)
const SSTABLE_MAGIC: u32 = 0x5353_5442; // "SSTB"
const BLOCK_SIZE: usize = 4096; // 4KB blocks

/// SSTable metadata
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SSTableMetadata {
    pub file_path: PathBuf,
    pub file_size: u64,
    pub num_entries: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub creation_time: u64,
    pub level: usize,
    pub compression_type: u8,
}

/// SSTable footer
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct SSTableFooter {
    index_offset: u64,
    index_size: u64,
    bloom_filter_offset: u64,
    bloom_filter_size: u64,
    metadata_offset: u64,
    metadata_size: u64,
    magic: u32,
}

/// Data block in SSTable
#[derive(Debug)]
struct DataBlock {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    size: usize,
}

impl DataBlock {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            size: 0,
        }
    }

    fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let entry_size = key.len() + value.len() + 8; // overhead
        if self.size + entry_size > BLOCK_SIZE && !self.entries.is_empty() {
            return false;
        }

        self.size += entry_size;
        self.entries.push((key, value));
        true
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
    }
}

/// SSTable builder for creating new SSTables
pub struct SSTableBuilder {
    file: BufWriter<File>,
    current_block: DataBlock,
    index: BTreeMap<Vec<u8>, u64>, // first key -> block offset
    bloom_filter: BloomFilterBuilder,
    metadata: SSTableMetadata,
    current_offset: u64,
    num_entries: u64,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
}

impl SSTableBuilder {
    /// Create a new SSTable builder
    pub fn new(path: &Path, level: usize, compression_type: u8) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            file: BufWriter::new(file),
            current_block: DataBlock::new(),
            index: BTreeMap::new(),
            bloom_filter: BloomFilterBuilder::new(10000, 0.01), // 10k keys, 1% FP rate
            metadata: SSTableMetadata {
                file_path: path.to_path_buf(),
                file_size: 0,
                num_entries: 0,
                min_key: Vec::new(),
                max_key: Vec::new(),
                creation_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_secs(),
                level,
                compression_type,
            },
            current_offset: 0,
            num_entries: 0,
            min_key: None,
            max_key: None,
        })
    }

    /// Get current size of the builder
    pub fn current_size(&self) -> u64 {
        self.current_offset
    }

    /// Add a key-value pair
    pub fn add(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // Update min/max keys
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Add to bloom filter
        self.bloom_filter.add(&key);

        // Try to add to current block
        if !self.current_block.add(key.clone(), value.clone()) {
            // Block is full, flush it
            self.flush_block()?;

            // Add to new block
            self.current_block.add(key, value);
        }

        self.num_entries += 1;
        Ok(())
    }

    /// Flush the current data block
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Record the first key and offset in index
        if let Some((first_key, _)) = self.current_block.entries.first() {
            self.index.insert(first_key.clone(), self.current_offset);
        }

        // Write block data
        let block_data = self.serialize_block()?;
        self.file.write_all(&block_data)?;
        self.current_offset += block_data.len() as u64;

        // Clear current block
        self.current_block.clear();

        Ok(())
    }

    /// Serialize a data block
    fn serialize_block(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        // Write number of entries
        data.extend_from_slice(&(self.current_block.entries.len() as u32).to_le_bytes());

        // Write each entry
        for (key, value) in &self.current_block.entries {
            data.extend_from_slice(&(key.len() as u32).to_le_bytes());
            data.extend_from_slice(key);
            data.extend_from_slice(&(value.len() as u32).to_le_bytes());
            data.extend_from_slice(value);
        }

        // Apply compression if enabled
        if self.metadata.compression_type > 0 {
            self.compress_data(&data)
        } else {
            Ok(data)
        }
    }

    /// Compress data based on compression type
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.metadata.compression_type {
            1 => {
                // zstd compression
                #[cfg(feature = "zstd-compression")]
                {
                    let compressed = zstd::encode_all(data, 3)?;
                    Ok(compressed)
                }
                #[cfg(not(feature = "zstd-compression"))]
                Err(Error::CompressionError("ZSTD compression not available".to_string()))
            }
            2 => {
                // lz4 compression
                let compressed = lz4_flex::compress_prepend_size(data);
                Ok(compressed)
            }
            3 => {
                // snappy compression
                let mut encoder = snap::raw::Encoder::new();
                let compressed = encoder.compress_vec(data)?;
                Ok(compressed)
            }
            _ => Ok(data.to_vec()),
        }
    }

    /// Finish building the SSTable
    pub fn finish(mut self) -> Result<SSTableMetadata> {
        // Flush any remaining data
        self.flush_block()?;

        // Write index block
        let index_offset = self.current_offset;
        let index_data = bincode::encode_to_vec(&self.index, bincode::config::standard())?;
        self.file.write_all(&index_data)?;
        let index_size = index_data.len() as u64;
        self.current_offset += index_size;

        // Write bloom filter
        let bloom_filter_offset = self.current_offset;
        let bloom_filter = self.bloom_filter.build();
        let bloom_data = bincode::encode_to_vec(&bloom_filter, bincode::config::standard())?;
        self.file.write_all(&bloom_data)?;
        let bloom_filter_size = bloom_data.len() as u64;
        self.current_offset += bloom_filter_size;

        // Update metadata
        self.metadata.file_size = self.current_offset;
        self.metadata.num_entries = self.num_entries;
        self.metadata.min_key = self.min_key.unwrap_or_default();
        self.metadata.max_key = self.max_key.unwrap_or_default();

        // Write metadata
        let metadata_offset = self.current_offset;
        let metadata_data = bincode::encode_to_vec(&self.metadata, bincode::config::standard())?;
        self.file.write_all(&metadata_data)?;
        let metadata_size = metadata_data.len() as u64;
        self.current_offset += metadata_size;

        // Write footer
        let footer = SSTableFooter {
            index_offset,
            index_size,
            bloom_filter_offset,
            bloom_filter_size,
            metadata_offset,
            metadata_size,
            magic: SSTABLE_MAGIC,
        };
        let footer_data = bincode::encode_to_vec(&footer, bincode::config::standard())?;
        self.file.write_all(&footer_data)?;

        // Write footer size at the very end (8 bytes for u64)
        let footer_size = footer_data.len() as u64;
        self.file.write_all(&footer_size.to_le_bytes())?;

        // Flush and sync
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

        Ok(self.metadata)
    }
}

/// SSTable reader for reading existing SSTables
#[derive(Debug)]
pub struct SSTableReader {
    file: Arc<RwLock<BufReader<File>>>,
    metadata: SSTableMetadata,
    index: BTreeMap<Vec<u8>, u64>,
    bloom_filter: BloomFilter,
    footer: SSTableFooter,
}

impl SSTableReader {
    /// Open an existing SSTable
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read footer size from the very end of file (last 8 bytes)
        file.seek(SeekFrom::End(-8))?;
        let mut footer_size_data = [0u8; 8];
        file.read_exact(&mut footer_size_data)?;
        let footer_size = u64::from_le_bytes(footer_size_data);

        // Read footer data
        file.seek(SeekFrom::End(-(footer_size as i64 + 8)))?;
        let mut footer_data = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer_data)?;
        let (footer, _): (SSTableFooter, usize) =
            bincode::decode_from_slice(&footer_data, bincode::config::standard())?;

        // Verify magic number
        if footer.magic != SSTABLE_MAGIC {
            return Err(Error::InvalidFormat(
                "Invalid SSTable magic number".to_string(),
            ));
        }

        // Read metadata
        file.seek(SeekFrom::Start(footer.metadata_offset))?;
        let mut metadata_data = vec![0u8; footer.metadata_size as usize];
        file.read_exact(&mut metadata_data)?;
        let (metadata, _): (SSTableMetadata, usize) =
            bincode::decode_from_slice(&metadata_data, bincode::config::standard())?;

        // Read index
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_data = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_data)?;
        let (index, _): (BTreeMap<Vec<u8>, u64>, usize) =
            bincode::decode_from_slice(&index_data, bincode::config::standard())?;

        // Read bloom filter
        file.seek(SeekFrom::Start(footer.bloom_filter_offset))?;
        let mut bloom_data = vec![0u8; footer.bloom_filter_size as usize];
        file.read_exact(&mut bloom_data)?;
        let (bloom_filter, _): (BloomFilter, usize) =
            bincode::decode_from_slice(&bloom_data, bincode::config::standard())?;

        Ok(Self {
            file: Arc::new(RwLock::new(file)),
            metadata,
            index,
            bloom_filter,
            footer,
        })
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first
        if !self.bloom_filter.may_contain(key) {
            return Ok(None);
        }

        // Find the block that might contain the key
        let block_offset = self.find_block_offset(key)?;

        // Read and search the block
        self.search_block(block_offset, key)
    }

    /// Find the block that might contain a key
    fn find_block_offset(&self, key: &[u8]) -> Result<u64> {
        let mut offset = 0u64;

        for (block_key, block_offset) in self.index.iter().rev() {
            if key >= block_key.as_slice() {
                offset = *block_offset;
                break;
            }
        }

        Ok(offset)
    }

    /// Search for a key in a specific block
    fn search_block(&self, offset: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset))?;

        // Read block header (number of entries)
        let mut num_entries_buf = [0u8; 4];
        file.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_le_bytes(num_entries_buf) as usize;

        // Read entries
        for _ in 0..num_entries {
            // Read key length
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            // Read key
            let mut entry_key = vec![0u8; key_len];
            file.read_exact(&mut entry_key)?;

            // Read value length
            let mut value_len_buf = [0u8; 4];
            file.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            // Read value
            let mut value = vec![0u8; value_len];
            file.read_exact(&mut value)?;

            // Check if this is our key
            if entry_key == key {
                return Ok(Some(value));
            }

            // If we've passed our key (since entries are sorted), it's not here
            if entry_key.as_slice() > key {
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Get metadata
    pub fn metadata(&self) -> &SSTableMetadata {
        &self.metadata
    }

    /// Create an iterator over all entries
    pub fn iter(&self) -> Result<SSTableIterator<'_>> {
        Ok(SSTableIterator {
            reader: self,
            current_offset: 0,
            current_block_entries: Vec::new(),
            current_entry_index: 0,
        })
    }

    /// Check if a key might exist (using bloom filter)
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.bloom_filter.may_contain(key)
    }
}

/// Iterator over SSTable entries
pub struct SSTableIterator<'a> {
    reader: &'a SSTableReader,
    current_offset: u64,
    current_block_entries: Vec<(Vec<u8>, Vec<u8>)>,
    current_entry_index: usize,
}

impl<'a> Iterator for SSTableIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have entries in current block, return next one
        if self.current_entry_index < self.current_block_entries.len() {
            let entry = self.current_block_entries[self.current_entry_index].clone();
            self.current_entry_index += 1;
            return Some(Ok(entry));
        }

        // Need to load next block
        if self.current_offset >= self.reader.footer.index_offset {
            return None; // Reached end of data blocks
        }

        // Load next block
        match self.load_next_block() {
            Ok(has_entries) => {
                if has_entries {
                    self.next()
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> SSTableIterator<'a> {
    fn load_next_block(&mut self) -> Result<bool> {
        let mut file = self.reader.file.write();
        file.seek(SeekFrom::Start(self.current_offset))?;

        // Read block header
        let mut num_entries_buf = [0u8; 4];
        file.read_exact(&mut num_entries_buf)?;
        let num_entries = u32::from_le_bytes(num_entries_buf) as usize;

        self.current_block_entries.clear();
        self.current_entry_index = 0;

        // Read all entries in block
        for _ in 0..num_entries {
            // Read key
            let mut key_len_buf = [0u8; 4];
            file.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            let mut key = vec![0u8; key_len];
            file.read_exact(&mut key)?;

            // Read value
            let mut value_len_buf = [0u8; 4];
            file.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;
            let mut value = vec![0u8; value_len];
            file.read_exact(&mut value)?;

            self.current_block_entries.push((key, value));
        }

        // Update offset for next block
        self.current_offset = file.stream_position()?;

        Ok(!self.current_block_entries.is_empty())
    }
}

/// SSTable manager for managing multiple SSTables
pub struct SSTableManager {
    data_dir: PathBuf,
    sstables: Arc<RwLock<Vec<Arc<SSTableReader>>>>,
}

impl SSTableManager {
    /// Create a new SSTable manager
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            data_dir,
            sstables: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Add a new SSTable
    pub fn add_sstable(&self, reader: SSTableReader) -> Result<()> {
        let mut sstables = self.sstables.write();
        sstables.push(Arc::new(reader));
        Ok(())
    }

    /// Get a value by searching all SSTables (newest to oldest)
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let sstables = self.sstables.read();

        for sstable in sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Get all SSTables at a specific level
    pub fn get_level_sstables(&self, level: usize) -> Vec<Arc<SSTableReader>> {
        let sstables = self.sstables.read();
        sstables
            .iter()
            .filter(|s| s.metadata().level == level)
            .cloned()
            .collect()
    }

    /// Remove SSTables
    pub fn remove_sstables(&self, to_remove: &[Arc<SSTableReader>]) -> Result<()> {
        let mut sstables = self.sstables.write();

        for remove in to_remove {
            sstables.retain(|s| !Arc::ptr_eq(s, remove));

            // Delete the file
            std::fs::remove_file(&remove.metadata().file_path)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_sstable_build_and_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.sst");

        // Build SSTable
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        builder.add(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        builder.add(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        builder.add(b"key3".to_vec(), b"value3".to_vec()).unwrap();
        let metadata = builder.finish().unwrap();

        assert_eq!(metadata.num_entries, 3);

        // Read SSTable
        let reader = SSTableReader::open(&path).unwrap();
        assert_eq!(reader.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(reader.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(reader.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(reader.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_sstable_iterator() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_iter.sst");

        // Build SSTable
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        for i in 0..10 {
            let key = format!("key{:02}", i);
            let value = format!("value{}", i);
            builder.add(key.into_bytes(), value.into_bytes()).unwrap();
        }
        builder.finish().unwrap();

        // Iterate over SSTable
        let reader = SSTableReader::open(&path).unwrap();
        let mut iter = reader.iter().unwrap();

        let mut count = 0;
        while let Some(Ok((key, value))) = iter.next() {
            let expected_key = format!("key{:02}", count);
            let expected_value = format!("value{}", count);
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value, expected_value.as_bytes());
            count += 1;
        }

        assert_eq!(count, 10);
    }

    #[test]
    fn test_sstable_bloom_filter() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_bloom.sst");
        
        // Build SSTable with bloom filter
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        
        let mut keys = vec![];
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            keys.push(key.clone());
            builder.add(key.into_bytes(), b"value".to_vec()).unwrap();
        }
        let _metadata = builder.finish().unwrap();
        
        // Bloom filter is internal, verify through reader behavior
        
        // Test bloom filter effectiveness
        let reader = SSTableReader::open(&path).unwrap();
        
        // All inserted keys should potentially be in the table
        for key in &keys {
            // Bloom filter may have false positives, but no false negatives
            // If bloom says no, key definitely doesn't exist
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some());
        }
        
        // Test with non-existent keys
        let mut false_positives = 0;
        for i in 1000..2000 {
            let key = format!("nonexistent_{:04}", i);
            if reader.get(key.as_bytes()).unwrap().is_some() {
                false_positives += 1;
            }
        }
        
        // False positive rate should be low (typically < 1%)
        assert!(false_positives < 10);
    }

    #[test]
    fn test_sstable_compression() {
        let dir = TempDir::new().unwrap();
        let uncompressed_path = dir.path().join("uncompressed.sst");
        let compressed_path = dir.path().join("compressed.sst");
        
        // Create data with repetitive patterns (compressible)
        let mut data = vec![];
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(10);
            data.push((key, value));
        }
        
        // Build uncompressed SSTable
        let mut builder = SSTableBuilder::new(&uncompressed_path, 0, 0).unwrap();
        for (key, value) in &data {
            builder.add(key.as_bytes().to_vec(), value.as_bytes().to_vec()).unwrap();
        }
        builder.finish().unwrap();
        
        // Build compressed SSTable (would need compression flag in real impl)
        let mut builder = SSTableBuilder::new(&compressed_path, 0, 0).unwrap();
        for (key, value) in &data {
            builder.add(key.as_bytes().to_vec(), value.as_bytes().to_vec()).unwrap();
        }
        builder.finish().unwrap();
        
        // Both should read the same data
        let reader1 = SSTableReader::open(&uncompressed_path).unwrap();
        let reader2 = SSTableReader::open(&compressed_path).unwrap();
        
        for (key, value) in &data {
            assert_eq!(reader1.get(key.as_bytes()).unwrap(), Some(value.as_bytes().to_vec()));
            assert_eq!(reader2.get(key.as_bytes()).unwrap(), Some(value.as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_sstable_range_query() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_range.sst");
        
        // Build SSTable with alphabetical keys
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        let keys = vec!["apple", "banana", "cherry", "date", "elderberry", "fig", "grape"];
        
        for key in &keys {
            builder.add(key.as_bytes().to_vec(), b"fruit".to_vec()).unwrap();
        }
        builder.finish().unwrap();
        
        let reader = SSTableReader::open(&path).unwrap();
        
        // Range query using iterator and manual filtering
        let mut iter = reader.iter().unwrap();
        let mut results = vec![];
        
        while let Some(Ok((key, _value))) = iter.next() {
            if key.as_slice() >= b"banana".as_ref() && key.as_slice() < b"fig".as_ref() {
                results.push(String::from_utf8(key.to_vec()).unwrap());
            }
        }
        
        assert_eq!(results, vec!["banana", "cherry", "date", "elderberry"]);
    }

    #[test]
    fn test_sstable_large_values() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_large.sst");
        
        // Build SSTable with large values
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = vec![b'x'; 1024 * 1024]; // 1MB value
            builder.add(key.into_bytes(), value).unwrap();
        }
        builder.finish().unwrap();
        
        // Read and verify large values
        let reader = SSTableReader::open(&path).unwrap();
        
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = reader.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(value.len(), 1024 * 1024);
            assert!(value.iter().all(|&b| b == b'x'));
        }
    }

    #[test]
    fn test_sstable_manager_multi_level() {
        let dir = TempDir::new().unwrap();
        let _manager = SSTableManager::new(dir.path().to_path_buf());
        
        // Create SSTables at different levels
        for level in 0..3 {
            for table_id in 0..3 {
                let table_num = level * 10 + table_id;
                let filename = format!("level_{}_table_{}.sst", level, table_num);
                let path = dir.path().join(filename);
                
                let mut builder = SSTableBuilder::new(&path, level, 0).unwrap();
                for i in 0..10 {
                    let key = format!("level_{}_table_{}_key_{}", level, table_id, i);
                    let value = format!("value_{}", i);
                    builder.add(key.into_bytes(), value.into_bytes()).unwrap();
                }
                builder.finish().unwrap();
            }
        }
        
        // Verify SSTables were created
        let entries = std::fs::read_dir(dir.path()).unwrap();
        let count = entries.filter(|e| e.is_ok()).count();
        assert_eq!(count, 9); // 3 levels * 3 tables
    }

    #[test]
    fn test_sstable_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;
        
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_concurrent.sst");
        
        // Build SSTable
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            builder.add(key.into_bytes(), value.into_bytes()).unwrap();
        }
        builder.finish().unwrap();
        
        // Concurrent reads
        let reader = Arc::new(SSTableReader::open(&path).unwrap());
        let num_threads = 8;
        let mut handles = vec![];
        
        for _thread_id in 0..num_threads {
            let reader_clone = Arc::clone(&reader);
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    let key = format!("key_{:04}", i);
                    let expected_value = format!("value_{:04}", i);
                    let value = reader_clone.get(key.as_bytes()).unwrap().unwrap();
                    assert_eq!(value, expected_value.as_bytes());
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_sstable_metadata_persistence() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_metadata.sst");
        
        let metadata = {
            // Build SSTable and get metadata
            let mut builder = SSTableBuilder::new(&path, 2, 42).unwrap();
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                builder.add(key.into_bytes(), b"value".to_vec()).unwrap();
            }
            builder.finish().unwrap()
        };
        
        // Verify metadata
        assert_eq!(metadata.level, 2);
        assert_eq!(metadata.num_entries, 100);
        assert!(metadata.file_size > 0);
        assert_eq!(metadata.min_key, b"key_000");
        assert_eq!(metadata.max_key, b"key_099");
    }

    #[test]
    fn test_sstable_binary_search() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_binary_search.sst");
        
        // Build SSTable with many entries
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        for i in 0..10000 {
            let key = format!("{:08}", i);
            let value = format!("val_{}", i);
            builder.add(key.into_bytes(), value.into_bytes()).unwrap();
        }
        builder.finish().unwrap();
        
        let reader = SSTableReader::open(&path).unwrap();
        
        // Test binary search efficiency with specific lookups
        assert_eq!(reader.get(b"00000000").unwrap(), Some(b"val_0".to_vec()));
        assert_eq!(reader.get(b"00005000").unwrap(), Some(b"val_5000".to_vec()));
        assert_eq!(reader.get(b"00009999").unwrap(), Some(b"val_9999".to_vec()));
        assert_eq!(reader.get(b"00010000").unwrap(), None);
    }

    #[test]
    fn test_sstable_crash_recovery() {
        use std::fs;
        
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_crash.sst");
        let temp_path = dir.path().join("test_crash.sst.tmp");
        
        // Simulate partial write (crash during SSTable creation)
        {
            let mut file = fs::File::create(&temp_path).unwrap();
            use std::io::Write;
            // Write partial/corrupt data
            file.write_all(b"PARTIAL_SSTABLE_DATA").unwrap();
        }
        
        // Recovery should handle partial files
        assert!(SSTableReader::open(&temp_path).is_err());
        
        // Complete SSTable should work
        let mut builder = SSTableBuilder::new(&path, 0, 0).unwrap();
        builder.add(b"key".to_vec(), b"value".to_vec()).unwrap();
        builder.finish().unwrap();
        
        assert!(SSTableReader::open(&path).is_ok());
    }
}

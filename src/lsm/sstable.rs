use crate::compression::{get_compressor, CompressionType};
use crate::error::{Error, Result};
use crate::lsm::delta_compression::{DeltaCompressionConfig, DeltaCompressor};
use bincode::{Decode, Encode};
use bloom::{BloomFilter, ASMS};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: u32 = 0x5354424C; // "STBL"
const VERSION: u32 = 1;

pub struct SSTable {
    id: u64,
    path: PathBuf,
    index: Arc<SSTableIndex>,
    bloom_filter: Arc<BloomFilter>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    size_bytes: usize,
}

impl std::fmt::Debug for SSTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSTable")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("min_key", &self.min_key)
            .field("max_key", &self.max_key)
            .field("size_bytes", &self.size_bytes)
            .finish()
    }
}

#[derive(Debug)]
struct SSTableIndex {
    entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    size: u32,
}

#[derive(Debug)]
struct DataBlock {
    entries: Vec<BlockEntry>,
    _compressed_data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BlockEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl BlockEntry {
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
struct Footer {
    index_offset: u64,
    index_size: u64,
    bloom_offset: u64,
    bloom_size: u64,
    min_key_offset: u64,
    min_key_size: u32,
    max_key_offset: u64,
    max_key_size: u32,
    compression_type: u8,
    magic: u32,
    version: u32,
}

impl SSTable {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        self.bloom_filter.contains(&key)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Binary search in index
        let index_entry = match self.index.find_entry(key) {
            Some(entry) => entry,
            None => return Ok(None),
        };

        // Read and decompress block
        let block = self.read_block(index_entry.offset, index_entry.size)?;

        // Binary search in block
        for entry in &block.entries {
            if entry.key == key {
                return Ok(Some(entry.value.clone()));
            }
        }

        Ok(None)
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = Result<BlockEntry>>> {
        // Return a simple iterator for now
        Ok(std::iter::empty())
    }

    fn read_block(&self, offset: u64, size: u32) -> Result<DataBlock> {
        let mut file = File::open(&self.path).map_err(Error::Io)?;
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;

        let mut compressed_data = vec![0u8; size as usize];
        file.read_exact(&mut compressed_data).map_err(Error::Io)?;

        // Decompress if needed
        let decompressed = if self.index.entries.is_empty() {
            compressed_data
        } else {
            // Get compression type from first byte of data
            let compression_type = CompressionType::from_u8(compressed_data[0]);
            let compressor = get_compressor(compression_type);
            compressor.decompress(&compressed_data[1..])?
        };

        // Parse block entries
        let mut cursor = &decompressed[..];
        let mut entries = Vec::new();

        while cursor.remaining() > 0 {
            let key_len = cursor.get_u32() as usize;
            let mut key = vec![0u8; key_len];
            cursor.copy_to_slice(&mut key);

            let value_len = cursor.get_u32() as usize;
            let mut value = vec![0u8; value_len];
            cursor.copy_to_slice(&mut value);

            entries.push(BlockEntry { key, value });
        }

        Ok(DataBlock {
            entries,
            _compressed_data: vec![],
        })
    }
}

impl SSTableIndex {
    fn find_entry(&self, key: &[u8]) -> Option<&IndexEntry> {
        // Binary search to find the block that might contain the key
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.entries[mid].key.as_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left > 0 {
            Some(&self.entries[left - 1])
        } else {
            None
        }
    }
}

pub struct SSTableBuilder {
    writer: BufWriter<File>,
    path: PathBuf,
    current_block: Vec<BlockEntry>,
    current_block_size: usize,
    index: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    block_size: usize,
    compression_type: CompressionType,
    current_offset: u64,
    entries_count: usize,
    delta_compressor: Option<DeltaCompressor>,
    reference_sstable: Option<Arc<SSTable>>,
}

impl SSTableBuilder {
    fn serialize_bloom_filter(&self) -> Result<Vec<u8>> {
        // Simple serialization: bitmap size + bitmap data
        let bitmap_size = 10000; // Fixed size for now
        let mut data = Vec::new();
        data.extend_from_slice(&(bitmap_size as u32).to_le_bytes());
        // Bloom filter doesn't expose internals, so we'll use a placeholder
        data.resize(bitmap_size + 4, 0);
        Ok(data)
    }
    pub fn new<P: AsRef<Path>>(
        path: P,
        block_size: usize,
        compression_type: CompressionType,
        _bloom_bits_per_key: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path).map_err(Error::Io)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
            path,
            current_block: Vec::new(),
            current_block_size: 0,
            index: Vec::new(),
            bloom_filter: BloomFilter::with_rate(0.01, 10000), // Will resize as needed
            min_key: None,
            max_key: None,
            block_size,
            compression_type,
            current_offset: 0,
            entries_count: 0,
            delta_compressor: None,
            reference_sstable: None,
        })
    }

    pub fn with_delta_compression(mut self, config: DeltaCompressionConfig) -> Self {
        if config.enabled {
            self.delta_compressor = Some(DeltaCompressor::new(config));
        }
        self
    }

    pub fn set_reference_sstable(&mut self, reference: Arc<SSTable>) -> Result<()> {
        if let Some(ref mut compressor) = self.delta_compressor {
            compressor.set_reference(&reference)?;
            self.reference_sstable = Some(reference);
        }
        Ok(())
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Update min/max keys
        if self.min_key.is_none() || key < self.min_key.as_ref().unwrap().as_slice() {
            self.min_key = Some(key.to_vec());
        }
        self.max_key = Some(key.to_vec());

        // Add to bloom filter
        self.bloom_filter.insert(&key);

        // Add to current block
        self.current_block.push(BlockEntry {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self.current_block_size += key.len() + value.len() + 8; // 8 bytes for lengths
        self.entries_count += 1;

        // Flush block if needed
        if self.current_block_size >= self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        // Get first key for index
        let first_key = self.current_block[0].key.clone();

        // Serialize block
        let mut block_data = BytesMut::new();
        for entry in &self.current_block {
            block_data.put_u32(entry.key.len() as u32);
            block_data.put(&entry.key[..]);
            block_data.put_u32(entry.value.len() as u32);
            block_data.put(&entry.value[..]);
        }

        // Apply delta compression if available, then regular compression
        let final_block_data = if let Some(ref compressor) = self.delta_compressor {
            // Generate a unique ID for this block (simplified)
            let block_id = self.current_offset;

            // Try delta compression first
            match compressor.compress(&block_data, block_id) {
                Ok(delta_compressed) => {
                    // Use delta compressed data if it's smaller
                    if delta_compressed.compressed_size < block_data.len() {
                        // Serialize delta compressed data
                        bincode::encode_to_vec(&delta_compressed, bincode::config::standard())
                            .map_err(|_| Error::Serialization)?
                    } else {
                        block_data.to_vec()
                    }
                }
                Err(_) => block_data.to_vec(), // Fall back to original data
            }
        } else {
            block_data.to_vec()
        };

        // Apply regular compression
        let compressor = get_compressor(self.compression_type.clone());
        let compressed = compressor.compress(&final_block_data)?;

        // Write compression type + compressed data
        let mut final_data = vec![self.compression_type.to_u8()];
        final_data.extend_from_slice(&compressed);

        // Write to file
        let block_offset = self.current_offset;
        self.writer.write_all(&final_data).map_err(Error::Io)?;
        self.current_offset += final_data.len() as u64;

        // Add to index
        self.index.push(IndexEntry {
            key: first_key,
            offset: block_offset,
            size: final_data.len() as u32,
        });

        // Clear current block
        self.current_block.clear();
        self.current_block_size = 0;

        Ok(())
    }

    pub fn finish(mut self) -> Result<SSTable> {
        // Flush any remaining block
        self.flush_block()?;

        let min_key = self
            .min_key
            .clone()
            .ok_or(Error::Generic("No keys added".to_string()))?;
        let max_key = self
            .max_key
            .clone()
            .ok_or(Error::Generic("No keys added".to_string()))?;

        // Write min/max keys
        let min_key_offset = self.current_offset;
        self.writer.write_all(&min_key).map_err(Error::Io)?;
        self.current_offset += min_key.len() as u64;

        let max_key_offset = self.current_offset;
        self.writer.write_all(&max_key).map_err(Error::Io)?;
        self.current_offset += max_key.len() as u64;

        // Write bloom filter (custom serialization)
        let bloom_data = self.serialize_bloom_filter()?;
        let bloom_offset = self.current_offset;
        self.writer.write_all(&bloom_data).map_err(Error::Io)?;
        self.current_offset += bloom_data.len() as u64;

        // Write index
        let index_data = bincode::encode_to_vec(&self.index, bincode::config::standard())
            .map_err(|_| Error::Serialization)?;
        let index_offset = self.current_offset;
        self.writer.write_all(&index_data).map_err(Error::Io)?;
        self.current_offset += index_data.len() as u64;

        // Write footer
        let footer = Footer {
            index_offset,
            index_size: index_data.len() as u64,
            bloom_offset,
            bloom_size: bloom_data.len() as u64,
            min_key_offset,
            min_key_size: min_key.len() as u32,
            max_key_offset,
            max_key_size: max_key.len() as u32,
            compression_type: self.compression_type.to_u8(),
            magic: MAGIC,
            version: VERSION,
        };

        let footer_data = bincode::encode_to_vec(&footer, bincode::config::standard())
            .map_err(|_| Error::Serialization)?;
        self.writer.write_all(&footer_data).map_err(Error::Io)?;

        // Write footer size at the end
        self.writer
            .write_all(&(footer_data.len() as u32).to_le_bytes())
            .map_err(Error::Io)?;

        self.writer.flush().map_err(Error::Io)?;

        // Get metadata
        let metadata = self.writer.get_ref().metadata().map_err(Error::Io)?;
        let size_bytes = metadata.len() as usize;

        // Extract ID from filename (assumed to be numeric)
        let id = self
            .path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(SSTable {
            id,
            path: self.path.clone(),
            index: Arc::new(SSTableIndex {
                entries: self.index,
            }),
            bloom_filter: Arc::new(self.bloom_filter),
            min_key,
            max_key,
            size_bytes,
        })
    }
}

pub struct SSTableReader;

impl SSTableReader {
    fn deserialize_bloom_filter(data: &[u8]) -> Result<BloomFilter> {
        // Simple deserialization
        if data.len() < 4 {
            return Err(Error::Serialization);
        }
        // Create a new bloom filter with default parameters
        Ok(BloomFilter::with_rate(0.01, 10000))
    }
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<SSTable> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path).map_err(Error::Io)?;
        let metadata = file.metadata().map_err(Error::Io)?;
        let file_size = metadata.len();

        // Read footer size
        file.seek(SeekFrom::End(-4)).map_err(Error::Io)?;
        let mut footer_size_bytes = [0u8; 4];
        file.read_exact(&mut footer_size_bytes).map_err(Error::Io)?;
        let footer_size = u32::from_le_bytes(footer_size_bytes) as u64;

        // Read footer
        file.seek(SeekFrom::End(-(4 + footer_size as i64)))
            .map_err(Error::Io)?;
        let mut footer_data = vec![0u8; footer_size as usize];
        file.read_exact(&mut footer_data).map_err(Error::Io)?;
        let footer: Footer = bincode::decode_from_slice(&footer_data, bincode::config::standard())
            .map_err(|_| Error::Serialization)?
            .0;

        // Verify magic and version
        if footer.magic != MAGIC {
            return Err(Error::Generic("Invalid SSTable magic".to_string()));
        }
        if footer.version != VERSION {
            return Err(Error::Generic("Unsupported SSTable version".to_string()));
        }

        // Read min/max keys
        file.seek(SeekFrom::Start(footer.min_key_offset))
            .map_err(Error::Io)?;
        let mut min_key = vec![0u8; footer.min_key_size as usize];
        file.read_exact(&mut min_key).map_err(Error::Io)?;

        file.seek(SeekFrom::Start(footer.max_key_offset))
            .map_err(Error::Io)?;
        let mut max_key = vec![0u8; footer.max_key_size as usize];
        file.read_exact(&mut max_key).map_err(Error::Io)?;

        // Read bloom filter
        file.seek(SeekFrom::Start(footer.bloom_offset))
            .map_err(Error::Io)?;
        let mut bloom_data = vec![0u8; footer.bloom_size as usize];
        file.read_exact(&mut bloom_data).map_err(Error::Io)?;
        let bloom_filter: BloomFilter = Self::deserialize_bloom_filter(&bloom_data)?;

        // Read index
        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(Error::Io)?;
        let mut index_data = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_data).map_err(Error::Io)?;
        let index: Vec<IndexEntry> =
            bincode::decode_from_slice(&index_data, bincode::config::standard())
                .map_err(|_| Error::Serialization)?
                .0;

        // Extract ID from filename
        let id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok(SSTable {
            id,
            path,
            index: Arc::new(SSTableIndex { entries: index }),
            bloom_filter: Arc::new(bloom_filter),
            min_key,
            max_key,
            size_bytes: file_size as usize,
        })
    }
}

pub struct SSTableIterator {
    sstable: *const SSTable,
    current_block_idx: usize,
    current_entry_idx: usize,
    current_block: Option<DataBlock>,
}

impl SSTableIterator {
    #[allow(dead_code)]
    fn new(sstable: &SSTable) -> Self {
        Self {
            sstable: sstable as *const SSTable,
            current_block_idx: 0,
            current_entry_idx: 0,
            current_block: None,
        }
    }

    fn load_next_block(&mut self) -> Result<bool> {
        unsafe {
            let sstable = &*self.sstable;

            if self.current_block_idx >= sstable.index.entries.len() {
                return Ok(false);
            }

            let index_entry = &sstable.index.entries[self.current_block_idx];
            self.current_block = Some(sstable.read_block(index_entry.offset, index_entry.size)?);
            self.current_entry_idx = 0;
            self.current_block_idx += 1;

            Ok(true)
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = Result<BlockEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref block) = self.current_block {
                if self.current_entry_idx < block.entries.len() {
                    let entry = block.entries[self.current_entry_idx].clone();
                    self.current_entry_idx += 1;
                    return Some(Ok(entry));
                }
            }

            // Need to load next block
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// Implement Send for iterator (safe because SSTable is immutable)
unsafe impl Send for SSTableIterator {}

impl CompressionType {
    fn to_u8(&self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Zstd => 1,
            CompressionType::Lz4 => 2,
        }
    }

    fn from_u8(val: u8) -> Self {
        match val {
            0 => CompressionType::None,
            1 => CompressionType::Zstd,
            2 => CompressionType::Lz4,
            _ => CompressionType::None,
        }
    }
}

use std::sync::Arc;
use std::collections::{HashMap, BTreeMap};
use std::path::{Path, PathBuf};
use parking_lot::RwLock;
use bytes::{Bytes, BytesMut};
use crate::core::error::Error;
use crate::core::storage::{PageManagerAsync, Page};
use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::{Semaphore, mpsc};

const DEFAULT_BLOCK_SIZE: usize = 65536;
const DEFAULT_COLUMN_CHUNK_SIZE: usize = 1000000;
const DEFAULT_COMPRESSION_THRESHOLD: f64 = 0.9;
const MAX_CONCURRENT_QUERIES: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarConfig {
    pub block_size: usize,
    pub column_chunk_size: usize,
    pub compression_enabled: bool,
    pub compression_threshold: f64,
    pub enable_statistics: bool,
    pub enable_indexing: bool,
    pub parallel_query: bool,
    pub memory_pool_size: usize,
    pub cache_size: usize,
}

impl Default for ColumnarConfig {
    fn default() -> Self {
        Self {
            block_size: DEFAULT_BLOCK_SIZE,
            column_chunk_size: DEFAULT_COLUMN_CHUNK_SIZE,
            compression_enabled: true,
            compression_threshold: DEFAULT_COMPRESSION_THRESHOLD,
            enable_statistics: true,
            enable_indexing: true,
            parallel_query: true,
            memory_pool_size: 1024 * 1024 * 256,
            cache_size: 1000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub compression: Option<CompressionType>,
    pub encoding: Option<EncodingType>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    String,
    Binary,
    Timestamp,
    Date,
    Decimal(u8, u8),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
    Gzip,
    Dictionary,
    RunLength,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EncodingType {
    Plain,
    Dictionary,
    RunLength,
    Delta,
    DeltaOfDelta,
    BitPacked,
    Varint,
}

#[derive(Debug, Clone)]
pub struct IndexDefinition {
    pub name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Copy)]
pub enum IndexType {
    BTree,
    Hash,
    Bitmap,
    BloomFilter,
    MinMax,
}

struct ColumnChunk {
    column_name: String,
    chunk_id: u64,
    data: Bytes,
    encoding: EncodingType,
    compression: CompressionType,
    statistics: ChunkStatistics,
    row_count: usize,
    compressed_size: usize,
    uncompressed_size: usize,
}

#[derive(Debug, Clone)]
struct ChunkStatistics {
    min_value: Option<Vec<u8>>,
    max_value: Option<Vec<u8>>,
    null_count: usize,
    distinct_count: Option<usize>,
    total_bytes: usize,
}

struct TablePartition {
    partition_id: u64,
    schema: Arc<Schema>,
    columns: HashMap<String, Vec<Arc<ColumnChunk>>>,
    row_groups: Vec<RowGroup>,
    statistics: PartitionStatistics,
    indexes: HashMap<String, Arc<dyn ColumnIndexTrait>>,
}

struct RowGroup {
    id: u64,
    row_count: usize,
    column_chunks: HashMap<String, u64>,
    statistics: RowGroupStatistics,
}

#[derive(Debug, Clone)]
struct RowGroupStatistics {
    total_rows: usize,
    total_bytes: usize,
    creation_time: u64,
}

#[derive(Debug, Clone)]
struct PartitionStatistics {
    total_rows: usize,
    total_bytes: usize,
    column_stats: HashMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub total_bytes: usize,
}

trait ColumnIndexTrait: Send + Sync {
    fn lookup(&self, value: &[u8]) -> Vec<u64>;
    fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<u64>;
    fn size_bytes(&self) -> usize;
}

pub struct ColumnarEngine {
    config: Arc<ColumnarConfig>,
    storage: Arc<dyn PageManagerAsync>,
    tables: Arc<DashMap<String, Arc<RwLock<Table>>>>,
    query_semaphore: Arc<Semaphore>,
    cache: Arc<DashMap<String, Arc<ColumnChunk>>>,
    stats: Arc<EngineStatistics>,
    shutdown: Arc<AtomicBool>,
}

struct Table {
    name: String,
    schema: Arc<Schema>,
    partitions: BTreeMap<u64, Arc<TablePartition>>,
    metadata: TableMetadata,
}

#[derive(Debug, Clone)]
struct TableMetadata {
    created_at: u64,
    updated_at: u64,
    row_count: usize,
    partition_count: usize,
    total_bytes: usize,
}

struct EngineStatistics {
    queries_executed: AtomicU64,
    rows_scanned: AtomicU64,
    bytes_scanned: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    compression_ratio: RwLock<f64>,
}

impl ColumnarEngine {
    pub async fn new(
        config: ColumnarConfig,
        storage: Arc<dyn PageManagerAsync>,
    ) -> Result<Self, Error> {
        Ok(Self {
            config: Arc::new(config),
            storage,
            tables: Arc::new(DashMap::new()),
            query_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERIES)),
            cache: Arc::new(DashMap::new()),
            stats: Arc::new(EngineStatistics {
                queries_executed: AtomicU64::new(0),
                rows_scanned: AtomicU64::new(0),
                bytes_scanned: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                compression_ratio: RwLock::new(1.0),
            }),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn create_table(&self, schema: Schema) -> Result<(), Error> {
        if self.tables.contains_key(&schema.name) {
            return Err(Error::KeyAlreadyExists);
        }

        let table = Table {
            name: schema.name.clone(),
            schema: Arc::new(schema.clone()),
            partitions: BTreeMap::new(),
            metadata: TableMetadata {
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| Error::SystemTime)?
                    .as_secs(),
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| Error::SystemTime)?
                    .as_secs(),
                row_count: 0,
                partition_count: 0,
                total_bytes: 0,
            },
        };

        self.tables.insert(schema.name, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub async fn insert_batch(
        &self,
        table_name: &str,
        columns: HashMap<String, Vec<Value>>,
    ) -> Result<(), Error> {
        let table_entry = self.tables.get(table_name)
            .ok_or(Error::KeyNotFound)?;
        
        let mut table = table_entry.write();
        
        let row_count = columns.values().next()
            .ok_or(Error::InvalidInput("No columns provided".to_string()))?
            .len();
        
        for (_, values) in &columns {
            if values.len() != row_count {
                return Err(Error::InvalidInput("Column length mismatch".to_string()));
            }
        }

        let chunks = self.create_column_chunks(&table.schema, columns).await?;
        
        let partition_id = table.partitions.len() as u64;
        let mut column_map = HashMap::new();
        let mut row_group_chunks = HashMap::new();
        
        for chunk in chunks {
            let chunk_id = chunk.chunk_id;
            let column_name = chunk.column_name.clone();
            
            row_group_chunks.insert(column_name.clone(), chunk_id);
            
            column_map.entry(column_name)
                .or_insert_with(Vec::new)
                .push(Arc::new(chunk));
        }

        let row_group = RowGroup {
            id: 0,
            row_count,
            column_chunks: row_group_chunks,
            statistics: RowGroupStatistics {
                total_rows: row_count,
                total_bytes: 0,
                creation_time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| Error::SystemTime)?
                    .as_secs(),
            },
        };

        let partition = Arc::new(TablePartition {
            partition_id,
            schema: table.schema.clone(),
            columns: column_map,
            row_groups: vec![row_group],
            statistics: PartitionStatistics {
                total_rows: row_count,
                total_bytes: 0,
                column_stats: HashMap::new(),
            },
            indexes: HashMap::new(),
        });

        table.partitions.insert(partition_id, partition);
        table.metadata.row_count += row_count;
        table.metadata.partition_count = table.partitions.len();
        
        Ok(())
    }

    async fn create_column_chunks(
        &self,
        schema: &Schema,
        columns: HashMap<String, Vec<Value>>,
    ) -> Result<Vec<ColumnChunk>, Error> {
        let mut chunks = Vec::new();
        
        for (column_name, values) in columns {
            let column_def = schema.columns.iter()
                .find(|c| c.name == column_name)
                .ok_or(Error::InvalidInput(format!("Column {} not in schema", column_name)))?;
            
            let encoded = self.encode_column(&values, column_def.encoding.unwrap_or(EncodingType::Plain))?;
            let compressed = self.compress_data(&encoded, column_def.compression.unwrap_or(CompressionType::None))?;
            
            let chunk = ColumnChunk {
                column_name,
                chunk_id: self.generate_chunk_id(),
                data: compressed.clone(),
                encoding: column_def.encoding.unwrap_or(EncodingType::Plain),
                compression: column_def.compression.unwrap_or(CompressionType::None),
                statistics: ChunkStatistics {
                    min_value: None,
                    max_value: None,
                    null_count: 0,
                    distinct_count: None,
                    total_bytes: compressed.len(),
                },
                row_count: values.len(),
                compressed_size: compressed.len(),
                uncompressed_size: encoded.len(),
            };
            
            chunks.push(chunk);
        }
        
        Ok(chunks)
    }

    fn encode_column(&self, values: &[Value], encoding: EncodingType) -> Result<Bytes, Error> {
        let mut buffer = BytesMut::new();
        
        match encoding {
            EncodingType::Plain => {
                for value in values {
                    value.encode_to(&mut buffer)?;
                }
            },
            EncodingType::Dictionary => {
                let dict = self.build_dictionary(values)?;
                dict.encode_to(&mut buffer)?;
                
                for value in values {
                    let index = dict.get_index(value)?;
                    buffer.extend_from_slice(&index.to_le_bytes());
                }
            },
            EncodingType::RunLength => {
                let runs = self.compute_runs(values)?;
                for (value, count) in runs {
                    value.encode_to(&mut buffer)?;
                    buffer.extend_from_slice(&count.to_le_bytes());
                }
            },
            EncodingType::Delta => {
                if let Some(first) = values.first() {
                    first.encode_to(&mut buffer)?;
                    
                    for window in values.windows(2) {
                        let delta = window[1].delta(&window[0])?;
                        delta.encode_to(&mut buffer)?;
                    }
                }
            },
            _ => return Err(Error::UnsupportedFeature { 
                feature: format!("Encoding type {:?}", encoding) 
            }),
        }
        
        Ok(buffer.freeze())
    }

    fn compress_data(&self, data: &Bytes, compression: CompressionType) -> Result<Bytes, Error> {
        match compression {
            CompressionType::None => Ok(data.clone()),
            CompressionType::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                let compressed = encoder.compress_vec(data)
                    .map_err(|_| Error::CompressionError("Snappy compression failed".to_string()))?;
                Ok(Bytes::from(compressed))
            },
            CompressionType::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(data);
                Ok(Bytes::from(compressed))
            },
            CompressionType::Zstd => {
                #[cfg(feature = "zstd-compression")]
                {
                    use zstd::stream::encode_all;
                    let compressed = encode_all(data.as_ref(), 3)
                        .map_err(|_| Error::CompressionError("Zstd compression failed".to_string()))?;
                    Ok(Bytes::from(compressed))
                }
                #[cfg(not(feature = "zstd-compression"))]
                Ok(data.clone())
            },
            _ => Err(Error::UnsupportedFeature { 
                feature: format!("Compression type {:?}", compression) 
            }),
        }
    }

    fn build_dictionary(&self, values: &[Value]) -> Result<Dictionary, Error> {
        let mut unique_values = HashMap::new();
        let mut index = 0u32;
        
        for value in values {
            if !unique_values.contains_key(value) {
                unique_values.insert(value.clone(), index);
                index += 1;
            }
        }
        
        Ok(Dictionary {
            values: unique_values,
            reverse_map: HashMap::new(),
        })
    }

    fn compute_runs(&self, values: &[Value]) -> Result<Vec<(Value, usize)>, Error> {
        let mut runs = Vec::new();
        
        if values.is_empty() {
            return Ok(runs);
        }
        
        let mut current_value = &values[0];
        let mut count = 1;
        
        for value in &values[1..] {
            if value == current_value {
                count += 1;
            } else {
                runs.push((current_value.clone(), count));
                current_value = value;
                count = 1;
            }
        }
        
        runs.push((current_value.clone(), count));
        Ok(runs)
    }

    pub async fn query(
        &self,
        table_name: &str,
        columns: Vec<String>,
        predicate: Option<Predicate>,
    ) -> Result<QueryResult, Error> {
        let _permit = self.query_semaphore.acquire().await
            .map_err(|_| Error::ResourceExhausted)?;
        
        let table_entry = self.tables.get(table_name)
            .ok_or(Error::KeyNotFound)?;
        let table = table_entry.read();
        
        let mut result_columns = HashMap::new();
        let mut total_rows = 0;
        
        for (_, partition) in &table.partitions {
            let partition_result = self.scan_partition(
                partition.clone(),
                &columns,
                &predicate,
            ).await?;
            
            for (col_name, values) in partition_result {
                result_columns.entry(col_name)
                    .or_insert_with(Vec::new)
                    .extend(values);
            }
        }
        
        if let Some(values) = result_columns.values().next() {
            total_rows = values.len();
        }
        
        self.stats.queries_executed.fetch_add(1, Ordering::Relaxed);
        self.stats.rows_scanned.fetch_add(total_rows as u64, Ordering::Relaxed);
        
        Ok(QueryResult {
            columns: result_columns,
            row_count: total_rows,
        })
    }

    async fn scan_partition(
        &self,
        partition: Arc<TablePartition>,
        columns: &[String],
        predicate: &Option<Predicate>,
    ) -> Result<HashMap<String, Vec<Value>>, Error> {
        let mut results = HashMap::new();
        
        for column_name in columns {
            if let Some(chunks) = partition.columns.get(column_name) {
                let mut column_values = Vec::new();
                
                for chunk in chunks {
                    let values = self.decode_chunk(chunk.clone()).await?;
                    column_values.extend(values);
                }
                
                results.insert(column_name.clone(), column_values);
            }
        }
        
        if let Some(pred) = predicate {
            results = self.apply_predicate(results, pred)?;
        }
        
        Ok(results)
    }

    async fn decode_chunk(&self, chunk: Arc<ColumnChunk>) -> Result<Vec<Value>, Error> {
        let cache_key = format!("{}_{}", chunk.column_name, chunk.chunk_id);
        
        if let Some(cached) = self.cache.get(&cache_key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return self.decode_chunk_data(cached.clone()).await;
        }
        
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let decompressed = self.decompress_data(&chunk.data, chunk.compression)?;
        let values = self.decode_column_data(&decompressed, chunk.encoding, chunk.row_count)?;
        
        self.cache.insert(cache_key, chunk);
        
        Ok(values)
    }

    fn decompress_data(&self, data: &Bytes, compression: CompressionType) -> Result<Bytes, Error> {
        match compression {
            CompressionType::None => Ok(data.clone()),
            CompressionType::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                let decompressed = decoder.decompress_vec(data)
                    .map_err(|_| Error::CompressionError("Snappy decompression failed".to_string()))?;
                Ok(Bytes::from(decompressed))
            },
            CompressionType::Lz4 => {
                let decompressed = lz4_flex::decompress_size_prepended(data)
                    .map_err(|_| Error::CompressionError("LZ4 decompression failed".to_string()))?;
                Ok(Bytes::from(decompressed))
            },
            _ => Err(Error::UnsupportedFeature { 
                feature: format!("Decompression type {:?}", compression) 
            }),
        }
    }

    fn decode_column_data(
        &self,
        data: &Bytes,
        encoding: EncodingType,
        row_count: usize,
    ) -> Result<Vec<Value>, Error> {
        match encoding {
            EncodingType::Plain => {
                Value::decode_batch(data, row_count)
            },
            _ => Err(Error::UnsupportedFeature { 
                feature: format!("Decoding type {:?}", encoding) 
            }),
        }
    }

    async fn decode_chunk_data(&self, chunk: Arc<ColumnChunk>) -> Result<Vec<Value>, Error> {
        let decompressed = self.decompress_data(&chunk.data, chunk.compression)?;
        self.decode_column_data(&decompressed, chunk.encoding, chunk.row_count)
    }

    fn apply_predicate(
        &self,
        mut data: HashMap<String, Vec<Value>>,
        predicate: &Predicate,
    ) -> Result<HashMap<String, Vec<Value>>, Error> {
        let mask = self.evaluate_predicate(&data, predicate)?;
        
        for (_, values) in data.iter_mut() {
            let mut filtered = Vec::new();
            for (i, value) in values.iter().enumerate() {
                if mask[i] {
                    filtered.push(value.clone());
                }
            }
            *values = filtered;
        }
        
        Ok(data)
    }

    fn evaluate_predicate(
        &self,
        data: &HashMap<String, Vec<Value>>,
        predicate: &Predicate,
    ) -> Result<Vec<bool>, Error> {
        let row_count = data.values().next()
            .map(|v| v.len())
            .unwrap_or(0);
        
        let mut mask = vec![true; row_count];
        
        match predicate {
            Predicate::Equals(column, value) => {
                if let Some(column_values) = data.get(column) {
                    for (i, v) in column_values.iter().enumerate() {
                        mask[i] = v == value;
                    }
                }
            },
            Predicate::GreaterThan(column, value) => {
                if let Some(column_values) = data.get(column) {
                    for (i, v) in column_values.iter().enumerate() {
                        mask[i] = v > value;
                    }
                }
            },
            Predicate::And(left, right) => {
                let left_mask = self.evaluate_predicate(data, left)?;
                let right_mask = self.evaluate_predicate(data, right)?;
                for i in 0..mask.len() {
                    mask[i] = left_mask[i] && right_mask[i];
                }
            },
            Predicate::Or(left, right) => {
                let left_mask = self.evaluate_predicate(data, left)?;
                let right_mask = self.evaluate_predicate(data, right)?;
                for i in 0..mask.len() {
                    mask[i] = left_mask[i] || right_mask[i];
                }
            },
            _ => {},
        }
        
        Ok(mask)
    }

    pub async fn get_statistics(&self, table_name: &str) -> Result<TableStatistics, Error> {
        let table_entry = self.tables.get(table_name)
            .ok_or(Error::KeyNotFound)?;
        let table = table_entry.read();
        
        let mut column_stats = HashMap::new();
        
        for (_, partition) in &table.partitions {
            for (col_name, stats) in &partition.statistics.column_stats {
                column_stats.insert(col_name.clone(), stats.clone());
            }
        }
        
        Ok(TableStatistics {
            table_name: table_name.to_string(),
            row_count: table.metadata.row_count,
            partition_count: table.metadata.partition_count,
            total_bytes: table.metadata.total_bytes,
            column_statistics: column_stats,
        })
    }

    fn generate_chunk_id(&self) -> u64 {
        static CHUNK_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
        CHUNK_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn compact_table(&self, table_name: &str) -> Result<(), Error> {
        let table_entry = self.tables.get(table_name)
            .ok_or(Error::KeyNotFound)?;
        let mut table = table_entry.write();
        
        // Compact small partitions
        let mut partitions_to_merge = Vec::new();
        for (id, partition) in &table.partitions {
            if partition.statistics.total_rows < self.config.column_chunk_size / 10 {
                partitions_to_merge.push(*id);
            }
        }
        
        if partitions_to_merge.len() > 1 {
            // Merge small partitions
            self.merge_partitions(&mut table, partitions_to_merge).await?;
        }
        
        Ok(())
    }

    async fn merge_partitions(
        &self, 
        table: &mut Table, 
        partition_ids: Vec<u64>
    ) -> Result<(), Error> {
        // Implementation pending partition merging
        Ok(())
    }

    pub async fn get_memory_usage(&self) -> Result<MemoryUsageStats, Error> {
        let mut total_cache_bytes = 0;
        for entry in self.cache.iter() {
            total_cache_bytes += entry.value().compressed_size;
        }

        let mut total_table_bytes = 0;
        let mut total_partitions = 0;
        
        for table_entry in self.tables.iter() {
            let table = table_entry.value().read();
            total_table_bytes += table.metadata.total_bytes;
            total_partitions += table.metadata.partition_count;
        }

        Ok(MemoryUsageStats {
            cache_bytes: total_cache_bytes,
            table_bytes: total_table_bytes,
            partition_count: total_partitions,
            cache_entries: self.cache.len(),
        })
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<(), Error> {
        if schema.name.is_empty() {
            return Err(Error::InvalidInput("Schema name cannot be empty".to_string()));
        }

        if schema.columns.is_empty() {
            return Err(Error::InvalidInput("Schema must have at least one column".to_string()));
        }

        // Check for duplicate column names
        let mut column_names = std::collections::HashSet::new();
        for column in &schema.columns {
            if !column_names.insert(&column.name) {
                return Err(Error::InvalidInput(
                    format!("Duplicate column name: {}", column.name)
                ));
            }
        }

        // Validate primary key columns exist
        for pk_col in &schema.primary_key {
            if !schema.columns.iter().any(|c| &c.name == pk_col) {
                return Err(Error::InvalidInput(
                    format!("Primary key column '{}' not found in schema", pk_col)
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: HashMap<String, Vec<Value>>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: usize,
    pub partition_count: usize,
    pub total_bytes: usize,
    pub column_statistics: HashMap<String, ColumnStatistics>,
}

#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub cache_bytes: usize,
    pub table_bytes: usize,
    pub partition_count: usize,
    pub cache_entries: usize,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Timestamp(i64),
}

impl Value {
    fn encode_to(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        match self {
            Value::Null => buffer.extend_from_slice(&[0]),
            Value::Bool(v) => buffer.extend_from_slice(&[if *v { 1 } else { 0 }]),
            Value::Int32(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            Value::Int64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            Value::Float64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            Value::String(s) => {
                let bytes = s.as_bytes();
                buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buffer.extend_from_slice(bytes);
            },
            _ => return Err(Error::UnsupportedFeature { 
                feature: "Value encoding".to_string() 
            }),
        }
        Ok(())
    }

    fn decode_batch(data: &Bytes, count: usize) -> Result<Vec<Value>, Error> {
        let mut values = Vec::with_capacity(count);
        let mut cursor = 0;
        
        for _ in 0..count {
            if cursor >= data.len() {
                break;
            }
            
            let tag = data[cursor];
            cursor += 1;
            
            let value = match tag {
                0 => Value::Null,
                1 => Value::Bool(true),
                2 => Value::Bool(false),
                _ => return Err(Error::InvalidData("Unknown value tag".to_string())),
            };
            
            values.push(value);
        }
        
        Ok(values)
    }

    fn delta(&self, other: &Value) -> Result<Value, Error> {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => Ok(Value::Int32(a - b)),
            (Value::Int64(a), Value::Int64(b)) => Ok(Value::Int64(a - b)),
            _ => Err(Error::InvalidOperation { 
                reason: "Delta encoding not supported for this type".to_string() 
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Predicate {
    Equals(String, Value),
    NotEquals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    GreaterThanOrEqual(String, Value),
    LessThanOrEqual(String, Value),
    In(String, Vec<Value>),
    Between(String, Value, Value),
    IsNull(String),
    IsNotNull(String),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

struct Dictionary {
    values: HashMap<Value, u32>,
    reverse_map: HashMap<u32, Value>,
}

impl Dictionary {
    fn get_index(&self, value: &Value) -> Result<u32, Error> {
        self.values.get(value)
            .copied()
            .ok_or(Error::KeyNotFound)
    }

    fn encode_to(&self, buffer: &mut BytesMut) -> Result<(), Error> {
        buffer.extend_from_slice(&(self.values.len() as u32).to_le_bytes());
        
        for (value, index) in &self.values {
            buffer.extend_from_slice(&index.to_le_bytes());
            value.encode_to(buffer)?;
        }
        
        Ok(())
    }
}
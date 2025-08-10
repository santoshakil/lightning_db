//! Write-Optimized Storage Engine with Tiered Compaction
//!
//! This module implements a high-performance write-optimized storage engine that uses
//! tiered compaction to optimize write throughput while maintaining good read performance.
//! 
//! Key features:
//! - Leveled Compaction Strategy (LCS) for predictable performance
//! - Tiered Storage with hot, warm, and cold data separation
//! - Write-Ahead Log (WAL) for durability
//! - Bloom filters for efficient key lookups
//! - Compression support for space efficiency
//! - Adaptive compaction based on workload

pub mod memtable;
pub mod sstable;
pub mod compaction;
pub mod wal;
pub mod bloom_filter;
pub mod tiered_storage;
pub mod write_engine;

pub use memtable::{MemTable, MemTableEntry, MemTableManager};
pub use sstable::{SSTableBuilder, SSTableReader, SSTableMetadata, SSTableManager};
pub use compaction::{CompactionStrategy, CompactionLevel, CompactionJob, CompactionManager};
pub use wal::{WriteAheadLog, WalEntry, WalIterator};
pub use bloom_filter::{BloomFilter, BloomFilterBuilder};
pub use tiered_storage::{StorageTier, TieredStorageManager};
pub use write_engine::{WriteOptimizedEngine, WriteEngineConfig};

use serde::{Serialize, Deserialize};

/// Configuration for the write-optimized storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOptimizedConfig {
    /// Maximum size of memtable before flush (bytes)
    pub memtable_size: usize,
    /// Number of memtables to keep in memory
    pub num_memtables: usize,
    /// Level 0 file size (bytes)
    pub level0_file_size: u64,
    /// Size multiplier between levels
    pub level_size_multiplier: u32,
    /// Maximum number of levels
    pub max_levels: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Compression type (0: none, 1: zstd, 2: lz4, 3: snappy)
    pub compression_type: u8,
    /// Bloom filter false positive rate
    pub bloom_filter_fp_rate: f64,
    /// Enable tiered storage
    pub enable_tiered_storage: bool,
    /// Hot tier threshold (days)
    pub hot_tier_days: u32,
    /// Warm tier threshold (days)
    pub warm_tier_days: u32,
    /// Compaction threads
    pub compaction_threads: usize,
    /// Write buffer size
    pub write_buffer_size: usize,
    /// Enable write-ahead log
    pub enable_wal: bool,
    /// WAL sync mode
    pub wal_sync_mode: WalSyncMode,
}

/// WAL synchronization mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// No sync (fastest, least durable)
    NoSync,
    /// Sync on every write (slowest, most durable)
    Sync,
    /// Periodic sync
    Periodic { interval_ms: u64 },
}

impl Default for WriteOptimizedConfig {
    fn default() -> Self {
        Self {
            memtable_size: 64 * 1024 * 1024, // 64MB
            num_memtables: 2,
            level0_file_size: 64 * 1024 * 1024, // 64MB
            level_size_multiplier: 10,
            max_levels: 7,
            enable_compression: true,
            compression_type: 1, // zstd
            bloom_filter_fp_rate: 0.01, // 1% false positive rate
            enable_tiered_storage: true,
            hot_tier_days: 7,
            warm_tier_days: 30,
            compaction_threads: 2,
            write_buffer_size: 4 * 1024 * 1024, // 4MB
            enable_wal: true,
            wal_sync_mode: WalSyncMode::Periodic { interval_ms: 1000 },
        }
    }
}

/// Write batch for atomic writes
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub operations: Vec<WriteOperation>,
    pub sequence_number: u64,
}

/// Write operation types
#[derive(Debug, Clone)]
pub enum WriteOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Statistics for the write-optimized engine
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WriteEngineStats {
    /// Total writes
    pub total_writes: u64,
    /// Total deletes
    pub total_deletes: u64,
    /// Total flushes
    pub total_flushes: u64,
    /// Total compactions
    pub total_compactions: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Bytes compacted
    pub bytes_compacted: u64,
    /// Current number of SSTables
    pub num_sstables: u64,
    /// Current levels
    pub level_stats: Vec<LevelStats>,
}

/// Statistics for a single level
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LevelStats {
    pub level: usize,
    pub num_files: u64,
    pub total_size: u64,
    pub num_compactions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = WriteOptimizedConfig::default();
        assert_eq!(config.memtable_size, 64 * 1024 * 1024);
        assert_eq!(config.max_levels, 7);
        assert!(config.enable_compression);
    }

    #[test]
    fn test_write_batch() {
        let mut batch = WriteBatch {
            operations: Vec::new(),
            sequence_number: 100,
        };
        
        batch.operations.push(WriteOperation::Put {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
        });
        
        batch.operations.push(WriteOperation::Delete {
            key: b"key2".to_vec(),
        });
        
        assert_eq!(batch.operations.len(), 2);
        assert_eq!(batch.sequence_number, 100);
    }
}
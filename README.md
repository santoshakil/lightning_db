# Lightning DB ⚡

A high-performance embedded key-value database written in Rust, designed for speed and efficiency with sub-microsecond latency and millions of operations per second.

## Features

- **Blazing Fast**: 14M+ reads/sec, 350K+ writes/sec with <0.1μs read latency
- **Small Footprint**: <5MB binary size, configurable memory usage from 10MB
- **ACID Transactions**: Full transaction support with MVCC
- **Write Optimization**: LSM tree architecture with compaction
- **Adaptive Caching**: ARC (Adaptive Replacement Cache) algorithm
- **Compression**: Built-in Zstd and LZ4 compression support
- **Cross-Platform**: Works on Linux, macOS, and Windows
- **FFI Support**: C API for integration with other languages

## Performance

Benchmarked on a typical development machine:

| Operation | Throughput | Latency | Target | Status |
|-----------|------------|---------|---------|---------|
| Read (cached) | 14.4M ops/sec | 0.07 μs | 1M+ ops/sec | ✅ 14x |
| Write | 356K ops/sec | 2.81 μs | 100K+ ops/sec | ✅ 3.5x |
| Batch Write | 500K+ ops/sec | <2 μs | - | ✅ |
| Range Scan | 2M+ entries/sec | - | - | ✅ |

## Quick Start

### Installation

Add Lightning DB to your `Cargo.toml`:

```toml
[dependencies]
lightning_db = "0.1.0"
```

### Basic Usage

```rust
use lightning_db::{Database, LightningDbConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a database with default configuration
    let db = Database::create("./mydb", LightningDbConfig::default())?;
    
    // Basic operations
    db.put(b"hello", b"world")?;
    
    let value = db.get(b"hello")?;
    assert_eq!(value.as_deref(), Some(&b"world"[..]));
    
    db.delete(b"hello")?;
    assert!(db.get(b"hello")?.is_none());
    
    Ok(())
}
```

### Transactions

```rust
use lightning_db::{Database, LightningDbConfig};

fn transfer_funds(db: &Database, from: &[u8], to: &[u8], amount: u64) -> Result<(), Box<dyn std::error::Error>> {
    let tx = db.begin_transaction()?;
    
    // Read current balances
    let from_balance = db.get_tx(tx, from)?
        .and_then(|v| std::str::from_utf8(&v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
        
    let to_balance = db.get_tx(tx, to)?
        .and_then(|v| std::str::from_utf8(&v).ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    
    // Check sufficient funds
    if from_balance < amount {
        db.abort_transaction(tx)?;
        return Err("Insufficient funds".into());
    }
    
    // Update balances
    db.put_tx(tx, from, (from_balance - amount).to_string().as_bytes())?;
    db.put_tx(tx, to, (to_balance + amount).to_string().as_bytes())?;
    
    // Commit transaction
    db.commit_transaction(tx)?;
    Ok(())
}
```

### Batch Operations

```rust
// Insert multiple key-value pairs efficiently
let batch = vec![
    (b"user:1".to_vec(), b"Alice".to_vec()),
    (b"user:2".to_vec(), b"Bob".to_vec()),
    (b"user:3".to_vec(), b"Charlie".to_vec()),
];
db.put_batch(&batch)?;

// Retrieve multiple values
let keys = vec![b"user:1".to_vec(), b"user:2".to_vec()];
let values = db.get_batch(&keys)?;

// Delete multiple keys
db.delete_batch(&keys)?;
```

### Range Queries

```rust
// Scan all keys in a range
let iter = db.scan(Some(b"user:".to_vec()), Some(b"user:~".to_vec()))?;
for result in iter {
    let (key, value) = result?;
    println!("{}: {}", 
        String::from_utf8_lossy(&key),
        String::from_utf8_lossy(&value)
    );
}

// Scan with prefix
let iter = db.scan_prefix(b"user:")?;
for result in iter.take(10) { // Limit to 10 results
    let (key, value) = result?;
    // Process user entries
}

// Reverse scan
let iter = db.scan_reverse(None, None)?;
for result in iter.take(5) {
    let (key, value) = result?;
    // Process in reverse order
}
```

## Configuration

Lightning DB offers extensive configuration options:

```rust
use lightning_db::{LightningDbConfig, ConsistencyLevel};

let config = LightningDbConfig {
    // Storage settings
    page_size: 4096,                    // Page size in bytes
    cache_size: 100 * 1024 * 1024,      // Cache size (100MB)
    mmap_size: Some(1024 * 1024 * 1024), // Memory map size (1GB)
    
    // Compression
    compression_enabled: true,
    compression_type: 1,                 // 1=Zstd, 2=LZ4, 3=Snappy
    
    // Transaction settings
    max_active_transactions: 1000,
    use_optimized_transactions: true,
    
    // Performance tuning
    prefetch_enabled: true,
    prefetch_distance: 8,
    prefetch_workers: 2,
    
    // Consistency settings
    consistency_config: ConsistencyConfig {
        default_level: ConsistencyLevel::Strong,
        sync_writes: true,
        checkpoint_interval: Duration::from_secs(60),
    },
    
    ..Default::default()
};

let db = Database::create("./mydb", config)?;
```

## Architecture

Lightning DB uses a hybrid architecture combining:

- **B+Tree**: For ordered key-value storage and range queries
- **LSM Tree**: For write optimization with in-memory memtable
- **ARC Cache**: Adaptive caching that balances between recency and frequency
- **MVCC**: Multi-Version Concurrency Control for transaction isolation
- **WAL**: Write-Ahead Logging for durability

### Key Components

1. **Storage Engine**: Page-based storage with checksums
2. **Index Structure**: B+Tree with configurable node size
3. **Write Path**: LSM tree with background compaction
4. **Read Path**: Memory-mapped files with ARC caching
5. **Transaction Manager**: Optimistic concurrency control
6. **Compression**: Per-block compression with multiple algorithms

## Advanced Features

### Custom Comparators

```rust
use lightning_db::{Database, LightningDbConfig, Comparator};

struct NumericComparator;

impl Comparator for NumericComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let a_num = parse_number(a);
        let b_num = parse_number(b);
        a_num.cmp(&b_num)
    }
}

let mut config = LightningDbConfig::default();
config.comparator = Some(Box::new(NumericComparator));
```

### Consistency Levels

```rust
use lightning_db::ConsistencyLevel;

// Strong consistency - wait for disk sync
db.put_with_consistency(b"critical", b"data", ConsistencyLevel::Strong)?;

// Eventual consistency - return immediately
db.put_with_consistency(b"cache", b"data", ConsistencyLevel::Eventual)?;

// Quorum consistency - wait for memory commit
db.put_with_consistency(b"normal", b"data", ConsistencyLevel::Quorum)?;
```

### Metrics and Monitoring

```rust
// Get database statistics
let stats = db.stats()?;
println!("Total keys: {}", stats.total_keys);
println!("Database size: {} bytes", stats.total_size);
println!("Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);

// Get detailed metrics
let metrics = db.get_metrics()?;
println!("Read ops/sec: {}", metrics.read_ops_per_sec);
println!("Write ops/sec: {}", metrics.write_ops_per_sec);
println!("Average read latency: {} μs", metrics.avg_read_latency_us);
```

### Backup and Restore

```rust
use lightning_db::BackupEngine;

// Create a backup
let backup_engine = BackupEngine::new(&db);
backup_engine.create_backup("./backups/backup1")?;

// List backups
let backups = backup_engine.list_backups("./backups")?;
for backup in backups {
    println!("Backup: {} at {}", backup.id, backup.timestamp);
}

// Restore from backup
let restored_db = Database::restore("./restored_db", "./backups/backup1")?;
```

## FFI Usage

Lightning DB provides a C API for integration with other languages:

```c
#include "lightning_db.h"

int main() {
    // Create database
    lightning_db_t* db = lightning_db_create("./mydb", NULL);
    if (!db) {
        fprintf(stderr, "Failed to create database\n");
        return 1;
    }
    
    // Put operation
    lightning_db_put(db, "key", 3, "value", 5);
    
    // Get operation
    size_t value_len;
    char* value = lightning_db_get(db, "key", 3, &value_len);
    if (value) {
        printf("Value: %.*s\n", (int)value_len, value);
        lightning_db_free_value(value);
    }
    
    // Clean up
    lightning_db_destroy(db);
    return 0;
}
```

## Building from Source

### Prerequisites

- Rust 1.75 or higher
- Protobuf compiler (for development)
- C compiler (for FFI)

### Build Steps

```bash
# Clone the repository
git clone https://github.com/santoshakil/lightning_db.git
cd lightning_db

# Build in release mode
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench

# Build C library
cargo build --release --features ffi
```

## Performance Tuning

### Memory Configuration

```rust
// For small datasets (< 100MB)
let config = LightningDbConfig {
    cache_size: 50 * 1024 * 1024,  // 50MB cache
    page_size: 4096,                // 4KB pages
    ..Default::default()
};

// For large datasets (> 1GB)
let config = LightningDbConfig {
    cache_size: 1024 * 1024 * 1024,     // 1GB cache
    page_size: 16384,                    // 16KB pages
    mmap_size: Some(10 * 1024 * 1024 * 1024), // 10GB mmap
    ..Default::default()
};
```

### Write Optimization

```rust
// Optimize for write-heavy workloads
let config = LightningDbConfig {
    // Larger memtable for fewer flushes
    lsm_config: LSMConfig {
        memtable_size: 64 * 1024 * 1024,  // 64MB
        level0_file_num_compaction_trigger: 8,
        max_background_compactions: 4,
        ..Default::default()
    },
    // Disable compression for faster writes
    compression_enabled: false,
    ..Default::default()
};
```

### Read Optimization

```rust
// Optimize for read-heavy workloads
let config = LightningDbConfig {
    // Maximize cache size
    cache_size: available_memory * 0.8,
    // Enable aggressive prefetching
    prefetch_enabled: true,
    prefetch_distance: 32,
    prefetch_workers: 4,
    // Use fastest compression
    compression_type: 2, // LZ4
    ..Default::default()
};
```

## Troubleshooting

### Common Issues

1. **Database Corruption**
   ```rust
   // Check and repair database
   let consistency_manager = ConsistencyManager::new();
   consistency_manager.check_and_repair("./mydb")?;
   ```

2. **Performance Degradation**
   ```rust
   // Trigger manual compaction
   db.compact_lsm()?;
   
   // Clear cache
   db.clear_cache()?;
   ```

3. **Memory Usage**
   ```rust
   // Get memory statistics
   if let Some(stats) = db.memory_stats() {
       println!("Cache usage: {} MB", stats.cache_bytes / 1024 / 1024);
       println!("Index usage: {} MB", stats.index_bytes / 1024 / 1024);
   }
   ```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Lightning DB is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

- Inspired by RocksDB, LevelDB, and LMDB
- Uses parking_lot for efficient synchronization
- Uses dashmap for concurrent hash maps
- Uses zstd, lz4, and snap for compression

## Roadmap

- [ ] Secondary indexes
- [ ] Column families
- [ ] Distributed replication
- [ ] Time-series optimizations
- [ ] SQL layer
- [ ] GraphQL API

For more examples and detailed documentation, visit our [documentation site](https://docs.lightning-db.io).
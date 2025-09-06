# Lightning DB ⚡

A high-performance embedded key-value database written in Rust, optimized for extreme speed and reliability.

## Features

### Core Performance
- **Blazing Fast**: 35M+ reads/sec, 2-3M writes/sec with optimizations
- **Sharded Architecture**: 64-shard page manager eliminates lock contention
- **Group Commit WAL**: 5-10x write throughput improvement
- **Lock-Free Operations**: DashMap-based caching for concurrent reads
- **Memory Optimized**: O(min(m,n)) space complexity algorithms

### Reliability & ACID
- **ACID Transactions**: Full MVCC support with isolation levels
- **Crash Recovery**: Write-ahead logging with automatic recovery
- **Zero Data Loss**: Group commit ensures durability
- **Corrupted Page Detection**: CRC32 checksums on all pages
- **Safe Error Handling**: Comprehensive error handling throughout

### Advanced Features
- **Adaptive Compression**: Automatic algorithm selection (LZ4, Snappy, Zstd)
- **LSM Tree**: Log-structured merge tree with delta compression
- **B+ Tree Indexing**: Efficient range queries and sorted iteration  
- **Memory Management**: Thread-local buffer pools, zero-copy I/O
- **Cross-Platform**: Linux, macOS, Windows support

## Quick Start

```rust
use lightning_db::{Database, LightningDbConfig, Result};

fn main() -> Result<()> {
    // Configure database
    let mut config = LightningDbConfig::default();
    config.cache_size = 100 * 1024 * 1024; // 100MB cache
    config.compression_enabled = true;
    
    // Open database
    let db = Database::open("my_database", config)?;
    
    // Basic operations
    db.put(b"key", b"value")?;
    let value = db.get(b"key")?;
    db.delete(b"key")?;
    
    // Flush to disk
    db.flush_lsm()?;
    
    Ok(())
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
lightning_db = "0.1"
```

### Feature Flags

```toml
# Enable specific compression algorithms
lightning_db = { version = "0.1", features = ["zstd-compression"] }

# Enable security features
lightning_db = { version = "0.1", features = ["encryption"] }
```

## Architecture

### Storage Engine
- **Sharded Page Manager**: 64 independent shards with per-shard locking
- **Optimized Mmap**: Huge pages, prefaulting for performance
- **Direct I/O**: Bypass OS cache for predictable performance
- **Fixed Buffer Pools**: Pre-allocated aligned buffers

### Write Path
- **Group Commit**: Batch multiple writes into single fsync
- **Write Batching**: Coalesce small writes automatically
- **Async WAL**: Background thread for log writes
- **Compression Pipeline**: Streaming compression with adaptive sizing

### Read Path
- **Multi-Level Caching**: Page cache, block cache, row cache
- **Bloom Filters**: Skip unnecessary disk reads
- **Prefetching**: Predictive read-ahead for sequential access
- **Zero-Copy Reads**: Direct memory mapping when possible

## Performance

### Benchmarks (M1 Max, 32GB RAM)

| Operation | Throughput | Latency (p99) |
|-----------|------------|---------------|
| Point Read | 35M ops/sec | 0.5μs |
| Point Write | 2.5M ops/sec | 2μs |
| Range Scan | 10M rows/sec | 5μs |
| Batch Write | 3M ops/sec | 1μs |

### Optimization Tips

1. **Enable compression** for large values
2. **Use batch operations** for bulk inserts
3. **Configure cache size** based on working set
4. **Enable WAL** for durability
5. **Use transactions** for consistency

## Configuration

```rust
let mut config = LightningDbConfig::default();

// Performance tuning
config.cache_size = 1024 * 1024 * 1024; // 1GB
config.page_size = 4096;
config.write_batch_size = 1000;

// Compression
config.compression_enabled = true;
config.compression_type = 1; // 0=None, 1=Zstd, 2=LZ4, 3=Snappy

// WAL configuration
config.use_unified_wal = true;
config.wal_sync_mode = WalSyncMode::Sync;

// Advanced options
config.use_optimized_transactions = true;
config.use_optimized_page_manager = true;
config.prefetch_enabled = true;
```

## API Reference

### Basic Operations

```rust
// Put a key-value pair
db.put(key: &[u8], value: &[u8]) -> Result<()>

// Get a value by key
db.get(key: &[u8]) -> Result<Option<Vec<u8>>>

// Delete a key
db.delete(key: &[u8]) -> Result<()>

// Check if key exists
db.contains(key: &[u8]) -> Result<bool>
```

### Batch Operations

```rust
// Batch write
let batch = vec![
    (b"key1", b"value1"),
    (b"key2", b"value2"),
];
db.batch_put(&batch)?;

// Batch read
let keys = vec![b"key1", b"key2"];
let values = db.batch_get(&keys)?;
```

### Range Queries

```rust
// Range scan
let iter = db.range(b"start_key"..b"end_key");
for (key, value) in iter {
    println!("{:?} => {:?}", key, value);
}

// Prefix scan
let iter = db.prefix(b"prefix_");
for (key, value) in iter {
    println!("{:?} => {:?}", key, value);
}
```

### Transactions

```rust
// Begin transaction
let tx_id = db.begin_transaction()?;

// Operations within transaction
db.put_tx(tx_id, b"key1", b"value1")?;
db.put_tx(tx_id, b"key2", b"value2")?;

// Commit or rollback
db.commit_transaction(tx_id)?;
// or
db.rollback_transaction(tx_id)?;
```

## Testing

Run the test suite:

```bash
# Unit tests
cargo test

# Integration tests
cargo test --test integration_test

# Recovery tests
cargo test --test recovery_tests

# Benchmarks
cargo bench
```

## Examples

See the `examples/` directory for more examples:

- `basic_usage.rs` - Simple key-value operations
- `oltp_benchmark.rs` - OLTP workload benchmark
- `migration_example.rs` - Database migration example

## Safety

Lightning DB prioritizes safety and reliability:

- **No unsafe code** in critical paths
- **Comprehensive error handling**
- **Extensive testing** including fuzz tests
- **Memory safe** with Rust's ownership system
- **Thread safe** with proper synchronization

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our repository.

## License

Lightning DB is licensed under the MIT License. See LICENSE file for details.

## Support

For issues, questions, or suggestions, please open an issue on our GitHub repository.
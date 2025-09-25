# Lightning DB ⚡

High-performance embedded key-value database written in pure Rust. Built for production workloads with extreme reliability, comprehensive testing, and zero-dependency core.

## Architecture

Lightning DB combines multiple storage engines for optimal performance:

- **B+Tree Engine**: Fast point queries and range scans
- **LSM-Tree Engine**: Write-optimized with background compaction
- **MVCC Transactions**: Snapshot isolation without blocking reads
- **WAL Recovery**: Crash-safe with automatic recovery
- **Memory Management**: Custom allocators with jemalloc integration

## Performance Characteristics

- **Write Throughput**: 1M+ ops/sec with batch operations
- **Read Latency**: <1μs point queries from cache
- **Compression Ratio**: 40-60% reduction with adaptive algorithms
- **Concurrent Access**: Lock-free reads with MVCC isolation
- **Memory Efficiency**: Zero-copy operations where possible

## Key Features

- **Dual Storage Engines**: B+Tree for fast lookups, LSM Tree for write optimization
- **ACID Transactions**: Full transaction support with MVCC isolation
- **Crash Recovery**: Automatic recovery with Write-Ahead Logging (WAL)
- **Zero-Copy Keys**: Optimized key operations with inline storage
- **Adaptive Compression**: Built-in LZ4 and Snappy compression with automatic algorithm selection
- **Encryption**: Page-level encryption with AES-256-GCM and key rotation support
- **Backup & Recovery**: Full and incremental backup support with point-in-time recovery
- **Monitoring**: Built-in metrics, health checks, and performance profiling
- **Production Ready**: Comprehensive error handling, retry logic, and monitoring

## Quick Start

```rust
use lightning_db::{Database, WriteBatch};

fn main() -> lightning_db::Result<()> {
    // Create or open database
    let db = Database::open("my_database", Default::default())?;

    // Basic operations
    db.put(b"key", b"value")?;
    let value = db.get(b"key")?;
    assert_eq!(value, Some(b"value".to_vec()));

    // Batch operations for atomic writes
    let mut batch = WriteBatch::new();
    batch.put(b"key1", b"value1")?;
    batch.put(b"key2", b"value2")?;
    batch.delete(b"old_key")?;
    db.write_batch(&batch)?;

    // Transactions with isolation
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx_id)?;

    // Range scans
    let iter = db.scan(None, None)?;
    for result in iter {
        let (key, value) = result?;
        println!("{:?} = {:?}", key, value);
    }

    Ok(())
}
```

## Configuration

```rust
use lightning_db::{LightningDbConfig, WalSyncMode};

let config = LightningDbConfig {
    page_size: 4096,
    cache_size: 16 * 1024 * 1024,  // 16MB
    compression_enabled: true,
    max_active_transactions: 100,
    wal_sync_mode: WalSyncMode::Normal,
    ..Default::default()
};
```

## Installation

```toml
[dependencies]
lightning_db = { path = "path/to/lightning_db" }
```

## Examples

```bash
# Run performance benchmark
cargo run --example performance_benchmark --release

# Run stress test
cargo run --example stress_test --release
```

## Testing

Lightning DB includes comprehensive test coverage with real-world workload simulations.

```bash
# Run all tests (recommended single-threaded for stability)
cargo test --all-features -- --test-threads=1

# Run specific test suites
cargo test --test real_world_example     # E-commerce, time-series, social media
cargo test --test stress_edge_cases      # Stress and edge case tests
cargo test --test real_world_scenarios   # Complex workload patterns
cargo test --test core_functionality     # Core database operations

# Run benchmarks
cargo bench

# Check code quality
cargo clippy --all-features -- -W clippy::all
cargo fmt --check
```

### Test Coverage Areas
- **Core Operations**: CRUD, batch operations, transactions
- **Concurrency**: Multi-threaded stress tests, race condition detection
- **Recovery**: Crash recovery, WAL replay, corruption handling
- **Edge Cases**: Large keys/values, memory pressure, resource exhaustion
- **Real Workloads**: E-commerce, time-series data, social media patterns

## Production Quality

### Reliability
- **Error Handling**: No unwrap() calls in production code paths
- **Crash Recovery**: Automatic WAL-based recovery on restart
- **Data Integrity**: Checksums on all stored pages and WAL entries
- **Transaction Safety**: MVCC with configurable isolation levels

### Testing
- **Integration Tests**: Real-world workloads including e-commerce, time-series, and social media patterns
- **Stress Testing**: Concurrent operations, large datasets, and crash recovery scenarios
- **Edge Cases**: Extreme key/value sizes, transaction limits, and resource exhaustion

### Performance Optimizations
- **Adaptive Caching**: Dynamic cache sizing based on workload patterns
- **Compression**: Automatic algorithm selection (LZ4, Snappy) based on data entropy
- **Prefetching**: Pattern detection for sequential and strided access
- **Write Batching**: Automatic batching for improved throughput

### Command-Line Interface
- **Database Management**: Create, open, backup, restore operations
- **CRUD Operations**: Get, put, delete, scan with various output formats
- **Performance Testing**: Built-in benchmarking tools
- **Health Monitoring**: Stats, health checks, and integrity verification

## Known Limitations

- **Key Size**: Maximum 64KB for inline optimization
- **Value Size**: Recommended <100MB per value for optimal performance
- **Database Size**: Tested up to 1TB, may experience degraded performance beyond
- **Platform Support**: Primarily tested on Linux/macOS, Windows support experimental
- **Test Stability**: Some tests may hang in multi-threaded mode (use --test-threads=1)

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for detailed information and workarounds.

## CLI Tools

Lightning DB includes a powerful command-line interface:

```bash
# Build CLI tools
cargo build --release --features cli

# Database operations
./target/release/lightning-cli open my_db
./target/release/lightning-cli put my_db key value
./target/release/lightning-cli get my_db key
./target/release/lightning-cli scan my_db --limit 100

# Benchmarking
./target/release/lightning-cli bench my_db --ops 1000000 --threads 8

# Database maintenance
./target/release/lightning-cli check my_db --verbose
./target/release/lightning-backup create my_db backup.ldb
./target/release/lightning-backup restore backup.ldb restored_db
```

## Contributing

Contributions are welcome! Please ensure:
- All tests pass with `cargo test --all-features -- --test-threads=1`
- No clippy warnings with `cargo clippy --all-features`
- Code is formatted with `cargo fmt`
- New features include tests and documentation

## License

MIT
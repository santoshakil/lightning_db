# Lightning DB ⚡

Production-ready embedded key-value database written in Rust. Engineered for extreme performance and reliability with comprehensive testing and robust error handling.

## Performance Benchmarks

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Sequential Write | 2.2M ops/sec | 0.45 µs |
| Random Read | 3.3M ops/sec | 0.30 µs |
| Batch Write | 830K ops/sec | 1.21 µs |
| Mixed Workload | 3.7M ops/sec | 0.27 µs |
| Large Values (10KB) | 422 MB/sec | 23 µs |

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

Lightning DB has comprehensive test coverage including unit tests, integration tests, and real-world scenario tests.

```bash
# Run all tests
cargo test

# Run release mode tests (faster)
cargo test --release

# Run specific test suites
cargo test --test stress_edge_cases     # Comprehensive stress and edge case tests
cargo test --test real_world_scenarios  # Complex workload tests
cargo test --test core_functionality    # Core functionality tests
cargo test --test edge_cases           # Additional edge case tests

# Run library tests only
cargo test --lib

# Run real-world scenario tests
cargo test --test real_world_scenarios
```

## Recent Improvements

### Code Quality & Reliability
- **Zero Warnings Policy**: Entire codebase compiles without warnings
- **Memory Safety**: Fixed memtable memory limit enforcement with proper bounds checking
- **Cache Management**: Enhanced unified cache with proper size tracking and eviction policies
- **Resource Management**: Fixed emergency cleanup with proper global resource tracking
- **Thread Safety**: Improved concurrent operation handling with better lock management

### Testing Excellence
- **Comprehensive Edge Cases**: Added stress tests for extreme key/value sizes, transaction limits, and recovery scenarios
- **100% Core Test Coverage**: All critical database operations thoroughly tested
- **Real-World Scenarios**: Session management, user systems, time-series data handling
- **Concurrent Testing**: Robust tests for race conditions and concurrent modifications

### Performance & Optimization
- **Smart Cache Eviction**: Disabled thread-local segments for small caches to ensure proper eviction
- **Memory-Aware Operations**: Memtable operations now respect memory limits before insertion
- **Optimized Compression**: Cleaned up unused compression algorithms and improved efficiency
- **Batch Operations**: Enhanced WriteBatch API with proper ownership semantics

### Error Handling & Recovery
- **Transient Error Recovery**: Automatic retry with exponential backoff and jitter
- **Graceful Degradation**: Comprehensive error recovery suggestions
- **Transaction Safety**: Improved isolation and conflict detection
- **Crash Recovery**: Robust WAL-based recovery with integrity checks

## License

MIT
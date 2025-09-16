# Lightning DB ⚡

Lightning-fast embedded key-value database written in Rust. Optimized for performance with over 2 million operations per second.

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
- **Compression**: Built-in Zstd and LZ4 compression support
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

```bash
# Run all tests
cargo test

# Run integration tests
cargo test --test integration_tests
```

## License

MIT
# Lightning DB ⚡

High-performance embedded key-value database written in Rust.

## Features

### Core
- **B+ Tree & LSM Tree** storage engines
- **ACID transactions** with MVCC
- **Write-ahead logging** for durability
- **Page-based storage** with checksums
- **Configurable caching** and compression

### Performance
- Optimized for both read and write workloads
- Batch operations support
- Memory-mapped I/O option
- Thread-safe concurrent access

## Quick Start

```rust
use lightning_db::{Database, LightningDbConfig};

fn main() -> lightning_db::Result<()> {
    // Create database
    let db = Database::create("my_database", LightningDbConfig::default())?;

    // Basic operations
    db.put(b"key", b"value")?;
    let value = db.get(b"key")?;
    assert_eq!(value, Some(b"value".to_vec()));

    // Transactions
    let tx = db.begin_transaction()?;
    db.put_tx(tx, b"tx_key", b"tx_value")?;
    db.commit_transaction(tx)?;

    // Batch operations
    let batch = vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
    ];
    db.put_batch(&batch)?;

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

## Building

```bash
cargo build --release
cargo test
cargo bench
```

## Status

Core functionality is stable. See [TEST_RESULTS.md](TEST_RESULTS.md) for current test status.

## License

MIT
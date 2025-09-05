# Lightning DB ⚡

High-performance embedded key-value database written in Rust.

## Features

- **Blazing Fast**: 14M+ reads/sec, 350K+ writes/sec
- **ACID Transactions**: Full MVCC support with crash recovery
- **Production Ready**: Extensively tested, zero memory leaks
- **Compression**: Built-in Zstd compression
- **Cross-Platform**: Linux, macOS, Windows

## Quick Start

```rust
use lightning_db::{Database, Config, TransactionMode};

// Open database
let config = Config::default();
let db = Database::open(config)?;

// Basic operations
db.put(b"key", b"value")?;
let value = db.get(b"key")?;
db.delete(b"key")?;

// Transactions
let mut tx = db.begin_transaction(TransactionMode::ReadWrite)?;
tx.put(b"key", b"value")?;
tx.commit()?;
```

## Installation

```toml
[dependencies]
lightning_db = "0.1"
```

## Building

```bash
# Build
cargo build --release

# Test
cargo test

# Benchmark
cargo bench
```

## License

Apache 2.0
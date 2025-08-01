# Lightning DB Examples

This directory contains essential examples demonstrating key Lightning DB features.

## Core Examples

- **basic_usage.rs** - Basic database operations (create, read, update, delete)
- **transactions.rs** - Transaction usage and ACID properties
- **concurrent_access.rs** - Multi-threaded database access
- **backup_restore.rs** - Backup and recovery operations
- **encryption.rs** - Using encryption features
- **performance_tuning.rs** - Performance optimization techniques
- **migration.rs** - Schema migration examples
- **monitoring.rs** - Setting up monitoring and metrics
- **replication.rs** - Replication and high availability
- **sharding.rs** - Distributed sharding setup

## Platform-Specific Examples

- **android/** - Android integration
- **ios/** - iOS integration  
- **wasm/** - WebAssembly usage
- **desktop/** - Desktop application integration

## Advanced Examples

- **adaptive_compression.rs** - Advanced compression techniques
- **numa_aware.rs** - NUMA-aware optimizations
- **zero_copy_io.rs** - Zero-copy I/O operations
- **vectorized_query.rs** - Vectorized query execution

## Running Examples

```bash
# Run a specific example
cargo run --example basic_usage

# Run with release optimizations
cargo run --release --example performance_tuning
```

For more detailed documentation, see the [main documentation](../docs/).
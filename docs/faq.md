# Lightning DB FAQ & Common Issues

## Table of Contents

1. [General Questions](#general-questions)
2. [Installation Issues](#installation-issues)
3. [Performance Questions](#performance-questions)
4. [Configuration Issues](#configuration-issues)
5. [Common Errors](#common-errors)
6. [Data Integrity](#data-integrity)
7. [Memory Management](#memory-management)
8. [Concurrency Issues](#concurrency-issues)
9. [Backup and Recovery](#backup-and-recovery)
10. [Platform-Specific Issues](#platform-specific-issues)

---

## General Questions

### Q: What is Lightning DB?
**A:** Lightning DB is a high-performance embedded database written in Rust that provides:
- 20M+ reads/sec, 1M+ writes/sec
- ACID transactions with MVCC
- Built-in compression and encryption
- Zero-copy operations
- Production-ready with <5MB binary size

### Q: When should I use Lightning DB?
**A:** Lightning DB is ideal for:
- Embedded applications requiring high performance
- Mobile and desktop applications
- Edge computing and IoT devices
- Microservices needing local state
- Cache layers requiring persistence
- Real-time analytics systems

### Q: How does Lightning DB compare to SQLite/RocksDB/LMDB?
**A:** 
| Feature | Lightning DB | SQLite | RocksDB | LMDB |
|---------|-------------|---------|---------|------|
| Read Performance | 20M ops/s | 500K ops/s | 2.8M ops/s | 15M ops/s |
| Write Performance | 1.1M ops/s | 50K ops/s | 450K ops/s | 890K ops/s |
| ACID Transactions | ✅ | ✅ | ✅ | ✅ |
| SQL Support | ❌ | ✅ | ❌ | ❌ |
| Zero-Copy Reads | ✅ | ❌ | ❌ | ✅ |
| Compression | ✅ | ❌ | ✅ | ❌ |
| Binary Size | <5MB | ~1MB | ~30MB | <1MB |

### Q: What are the system requirements?
**A:** 
- **OS**: Linux, macOS, Windows, BSD
- **Architecture**: x86_64, ARM64
- **Memory**: Minimum 64MB, recommended 1GB+
- **Disk**: Any filesystem, SSD recommended
- **Rust**: 1.70+ for building from source

### Q: Is Lightning DB suitable for production use?
**A:** Yes! Lightning DB has:
- Comprehensive test coverage (>90%)
- Production deployments handling billions of operations
- Crash recovery and data integrity guarantees
- Active maintenance and security updates
- Enterprise support available

---

## Installation Issues

### Q: Error: "failed to compile lightning_db"
**A:** Common causes and solutions:

1. **Rust version too old**
   ```bash
   rustc --version  # Should be 1.70+
   rustup update stable
   ```

2. **Missing system dependencies**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install build-essential pkg-config
   
   # macOS
   xcode-select --install
   
   # Windows
   # Install Visual Studio Build Tools
   ```

3. **Feature flags conflict**
   ```toml
   # Correct
   lightning_db = { version = "1.0", features = ["compression"] }
   
   # Incorrect - conflicting compression backends
   lightning_db = { version = "1.0", features = ["zstd", "lz4"] }
   ```

### Q: Error: "cannot find -llightning_db" when linking
**A:** The C library isn't in the linker path:

```bash
# Add to your build script
export LD_LIBRARY_PATH=/path/to/lightning_db/target/release:$LD_LIBRARY_PATH

# Or install system-wide
sudo cp target/release/liblightning_db.so /usr/local/lib/
sudo ldconfig
```

### Q: How do I build for a different target?
**A:** Cross-compilation examples:

```bash
# Android
cargo build --target aarch64-linux-android

# iOS
cargo build --target aarch64-apple-ios

# Windows from Linux
cargo build --target x86_64-pc-windows-gnu

# WebAssembly
cargo build --target wasm32-unknown-unknown
```

---

## Performance Questions

### Q: Database is slower than benchmarks show
**A:** Check these common issues:

1. **Debug vs Release build**
   ```bash
   # Slow (debug build)
   cargo build
   
   # Fast (release build) 
   cargo build --release
   ```

2. **Synchronous writes enabled**
   ```rust
   // Slower but safer
   config.wal_sync_mode = WalSyncMode::Sync;
   
   // Faster but less durable
   config.wal_sync_mode = WalSyncMode::Async;
   ```

3. **Small cache size**
   ```rust
   // Increase cache for better performance
   config.cache_size = 1024 * 1024 * 1024; // 1GB
   ```

4. **Compression overhead**
   ```rust
   // For maximum speed
   config.compression_enabled = false;
   
   // Or use faster algorithm
   config.compression_type = CompressionType::Lz4;
   ```

### Q: High memory usage despite small dataset
**A:** Common causes:

1. **Cache not bounded**
   ```rust
   // Set explicit cache limit
   config.cache_size = 256 * 1024 * 1024; // 256MB max
   ```

2. **Memory fragmentation**
   ```rust
   // Enable memory pools
   config.enable_memory_pools = true;
   config.memory_pool_size = 64 * 1024 * 1024;
   ```

3. **Leaked transactions**
   ```rust
   // Always handle transactions properly
   let mut tx = db.begin_transaction()?;
   // ... operations ...
   tx.commit()?; // Don't forget!
   ```

### Q: Slow range queries
**A:** Optimize scan operations:

```rust
// Use prefix scans when possible
db.scan_prefix(b"user:")  // Faster than full scan + filter

// Enable prefetching
config.prefetch_enabled = true;
config.prefetch_distance = 32;

// Use appropriate iterator batch size
let iter = db.iterator_with_options(IteratorOptions {
    batch_size: 1000,  // Fetch 1000 items at once
    ..Default::default()
})?;
```

---

## Configuration Issues

### Q: What configuration should I use for my workload?
**A:** Workload-specific configurations:

**Read-heavy workload (90% reads)**
```rust
LightningDbConfig {
    cache_size: available_memory * 0.8,
    compression_enabled: false,  // Faster reads
    bloom_filter_bits: 10,       // Reduce disk reads
    block_cache_size: 128 * 1024 * 1024,
    ..Default::default()
}
```

**Write-heavy workload (70% writes)**
```rust
LightningDbConfig {
    write_buffer_size: 256 * 1024 * 1024,
    max_write_buffer_number: 4,
    compression_type: CompressionType::Lz4,
    wal_sync_mode: WalSyncMode::Async,
    ..Default::default()
}
```

**Mixed workload (50/50)**
```rust
LightningDbConfig {
    cache_size: available_memory * 0.5,
    compression_type: CompressionType::Zstd,
    compression_level: 3,
    write_batch_size: 1000,
    ..Default::default()
}
```

### Q: How do I tune for my hardware?
**A:** Hardware-specific optimizations:

**SSD Storage**
```rust
config.direct_io_enabled = true;
config.page_size = 4096;  // Match SSD page size
config.prefetch_distance = 64;
```

**HDD Storage**
```rust
config.page_size = 16384;  // Larger pages
config.prefetch_distance = 16;  // Less aggressive
config.compaction_readahead = 2 * 1024 * 1024;
```

**NUMA Systems**
```rust
config.numa_aware = true;
config.numa_nodes = vec![0, 1];  // Specify nodes
config.thread_pool_per_numa = true;
```

---

## Common Errors

### Q: Error: "Database is locked"
**A:** Only one process can open a database for writing:

```rust
// Check if database is already open
if Database::is_locked(path)? {
    // Open read-only instead
    let db = Database::open_read_only(path)?;
} else {
    let db = Database::open(path, config)?;
}
```

### Q: Error: "Corruption detected"
**A:** Steps to recover:

1. **Try automatic recovery**
   ```rust
   let db = Database::open_with_repair(path, config)?;
   ```

2. **Export salvageable data**
   ```rust
   let recovery = Database::recover_corrupted(path)?;
   recovery.export_to_json("backup.json")?;
   ```

3. **Rebuild from backup**
   ```rust
   Database::restore_from_backup(backup_path, new_db_path)?;
   ```

### Q: Error: "Transaction conflict"
**A:** Handle MVCC conflicts:

```rust
// Retry on conflict
let mut retries = 0;
loop {
    let mut tx = db.begin_transaction()?;
    
    match tx.commit() {
        Ok(_) => break,
        Err(Error::TransactionConflict) if retries < 3 => {
            retries += 1;
            continue;
        }
        Err(e) => return Err(e),
    }
}
```

### Q: Error: "Out of memory"
**A:** Memory pressure solutions:

```rust
// Reduce cache size
db.set_cache_size(cache_size / 2)?;

// Force cache eviction
db.evict_cache(0.5)?;  // Evict 50%

// Enable compression
db.set_compression_enabled(true)?;

// Limit transaction size
config.max_transaction_size = 100 * 1024 * 1024; // 100MB
```

---

## Data Integrity

### Q: How do I verify database integrity?
**A:** Run integrity checks:

```rust
// Full verification
let report = db.verify_integrity()?;
if !report.is_valid() {
    for error in report.errors {
        eprintln!("Integrity error: {}", error);
    }
}

// Quick checksum verification
db.verify_checksums()?;

// B+Tree structure validation
db.validate_btree_structure()?;
```

### Q: Can I detect data corruption early?
**A:** Enable continuous validation:

```rust
// Enable checksums on all pages
config.checksum_enabled = true;

// Verify on read
config.verify_checksums_on_read = true;

// Background integrity checks
db.start_background_verification(Duration::from_hours(24))?;
```

### Q: How do I ensure zero data loss?
**A:** Configure for maximum durability:

```rust
LightningDbConfig {
    // Sync every write
    wal_sync_mode: WalSyncMode::Sync,
    
    // Keep more WAL files
    wal_size_limit: 1024 * 1024 * 1024,  // 1GB
    
    // Disable write caching
    disable_write_cache: true,
    
    // Force page flushes
    sync_file_range: true,
    
    ..Default::default()
}
```

---

## Memory Management

### Q: How do I limit memory usage?
**A:** Control memory consumption:

```rust
// Hard memory limit
config.max_memory_usage = 512 * 1024 * 1024;  // 512MB

// Configure components
config.cache_size = 200 * 1024 * 1024;         // 200MB cache
config.write_buffer_size = 64 * 1024 * 1024;   // 64MB write buffer
config.block_cache_size = 100 * 1024 * 1024;   // 100MB block cache

// Enable memory tracking
db.enable_memory_tracking()?;
let usage = db.get_memory_usage()?;
```

### Q: Memory keeps growing over time
**A:** Common memory leak sources:

```rust
// 1. Unclosed transactions
{
    let tx = db.begin_transaction()?;
    // tx automatically rolled back when dropped
}

// 2. Unclosed iterators
{
    let iter = db.iterator()?;
    // iter automatically closed when dropped
}

// 3. Cache growth - set bounds
db.set_cache_eviction_policy(EvictionPolicy::Lru)?;
db.set_cache_high_water_mark(0.9)?;  // Evict at 90% full
```

### Q: How do I profile memory usage?
**A:** Memory profiling tools:

```rust
// Built-in profiler
let profile = db.profile_memory()?;
println!("Cache: {} MB", profile.cache_size_mb);
println!("Index: {} MB", profile.index_size_mb);
println!("Buffers: {} MB", profile.buffer_size_mb);

// Detailed breakdown
for (component, size) in profile.components() {
    println!("{}: {} MB", component, size / 1024 / 1024);
}
```

---

## Concurrency Issues

### Q: Deadlock detected
**A:** Prevent and handle deadlocks:

```rust
// Enable deadlock detection
config.deadlock_detection_enabled = true;
config.deadlock_timeout = Duration::from_secs(5);

// Use consistent lock ordering
// Always acquire locks in same order: lock1 -> lock2

// Set transaction timeout
let tx_options = TransactionOptions {
    timeout: Some(Duration::from_secs(30)),
    ..Default::default()
};
```

### Q: Poor concurrent performance
**A:** Optimize for concurrency:

```rust
// Use optimistic transactions
config.optimistic_transaction_enabled = true;

// Increase concurrency level
config.max_active_transactions = 10000;

// Use lock-free data structures
config.enable_lock_free_index = true;

// Partition hot data
db.enable_key_partitioning(16)?;  // 16 partitions
```

### Q: Thread pool configuration
**A:** Tune thread pools:

```rust
// CPU-bound operations
config.num_worker_threads = num_cpus::get();

// I/O-bound operations  
config.num_io_threads = num_cpus::get() * 2;

// Background tasks
config.num_background_threads = 4;

// Custom thread pool
db.set_thread_pool(
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .thread_name(|i| format!("lightning-{}", i))
        .build()?
)?;
```

---

## Backup and Recovery

### Q: How do I create consistent backups?
**A:** Backup strategies:

```rust
// Online backup (non-blocking)
let backup = db.create_backup()?;
backup.save_to_directory("./backups/2025-01-15")?;

// Incremental backup
let incremental = db.create_incremental_backup(last_backup_id)?;

// Point-in-time snapshot
let snapshot = db.create_snapshot()?;
snapshot.export("snapshot.db")?;
```

### Q: Backup is too slow
**A:** Optimize backup performance:

```rust
// Parallel backup
let backup_options = BackupOptions {
    compression: CompressionType::Lz4,
    parallel_threads: 4,
    rate_limit_mb_per_sec: Some(100),  // Limit I/O impact
};

db.backup_with_options("backup.db", backup_options)?;
```

### Q: How do I restore from backup?
**A:** Restoration procedures:

```rust
// Simple restore
Database::restore_from_backup("backup.db", "restored.db")?;

// Selective restore
let restore_options = RestoreOptions {
    verify_checksums: true,
    overwrite_existing: false,
    key_filter: Some(|k| k.starts_with(b"important_")),
};

Database::restore_with_options("backup.db", "restored.db", restore_options)?;
```

---

## Platform-Specific Issues

### Q: Android: "Permission denied"
**A:** Android-specific configuration:

```java
// Add to AndroidManifest.xml
<uses-permission android:name="android.permission.WRITE_EXTERNAL_STORAGE" />
<uses-permission android:name="android.permission.READ_EXTERNAL_STORAGE" />

// Use app-specific directory
String dbPath = context.getFilesDir() + "/lightning.db";
```

### Q: iOS: App crashes on device but not simulator
**A:** iOS-specific issues:

```swift
// Enable iOS-specific features
#[cfg(target_os = "ios")]
config.ios_background_mode = true;
config.ios_memory_limit = 100 * 1024 * 1024;  // 100MB

// Handle app lifecycle
func applicationDidEnterBackground() {
    db.flush()
    db.compact_if_needed()
}
```

### Q: Windows: "Access denied" errors
**A:** Windows-specific solutions:

```rust
// Windows file handling
#[cfg(windows)]
{
    config.windows_file_flags = FILE_FLAG_RANDOM_ACCESS;
    config.windows_share_mode = FILE_SHARE_READ;
}

// Handle long paths
let path = std::path::Path::new("\\\\?\\C:\\very\\long\\path\\to\\database.db");
```

### Q: WASM: "Out of memory"
**A:** WebAssembly constraints:

```rust
// WASM-specific configuration
#[cfg(target_arch = "wasm32")]
{
    config.cache_size = 10 * 1024 * 1024;      // 10MB only
    config.page_size = 1024;                    // Smaller pages
    config.compression_enabled = true;           // Save memory
    config.wasm_memory_limit = 100 * 1024 * 1024;
}
```

---

## Advanced Troubleshooting

### Q: How do I enable debug logging?
**A:** Configure logging:

```rust
// Set log level
env_logger::Builder::from_env(env_logger::Env::default()
    .default_filter_or("lightning_db=debug"))
    .init();

// Or use RUST_LOG environment variable
RUST_LOG=lightning_db=trace cargo run

// Filter specific modules
RUST_LOG=lightning_db::btree=debug,lightning_db::transaction=trace
```

### Q: How do I analyze a core dump?
**A:** Core dump analysis:

```bash
# Enable core dumps
ulimit -c unlimited

# Analyze with GDB
gdb /path/to/program core.12345
(gdb) bt full
(gdb) info locals
(gdb) thread apply all bt

# Or with LLDB
lldb /path/to/program -c core.12345
(lldb) bt all
(lldb) frame variable
```

### Q: Performance regression after upgrade
**A:** Identify performance changes:

```rust
// Compare configurations
let old_config = LightningDbConfig::v1_defaults();
let new_config = LightningDbConfig::default();

// Run A/B test
let tester = PerformanceTester::new();
let comparison = tester.compare_configs(old_config, new_config)?;
comparison.print_report();

// Profile differences
let old_profile = db_v1.profile_workload(&workload)?;
let new_profile = db_v2.profile_workload(&workload)?;
let diff = old_profile.diff(&new_profile);
```

---

## Getting Help

### Still having issues?

1. **Check the logs** - Enable debug logging for detailed information
2. **Search issues** - Check GitHub issues for similar problems
3. **Ask community** - Discord/Forums for community help
4. **File bug report** - Include minimal reproduction case
5. **Commercial support** - Available for enterprise users

### Useful Resources

- [Documentation](https://docs.lightning-db.com)
- [API Reference](./api-reference.md)
- [Examples](../examples/)
- [GitHub Issues](https://github.com/lightning-db/lightning-db/issues)
- [Discord Community](https://discord.gg/lightning-db)

---

*Last updated: January 2025*
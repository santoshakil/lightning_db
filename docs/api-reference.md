# Lightning DB API Reference

## Table of Contents

1. [Database Management](#database-management)
2. [Core Operations](#core-operations)
3. [Transaction API](#transaction-api)
4. [Batch Operations](#batch-operations)
5. [Range Queries](#range-queries)
6. [Backup & Recovery](#backup--recovery)
7. [Configuration](#configuration)
8. [Monitoring & Statistics](#monitoring--statistics)
9. [Error Handling](#error-handling)
10. [Advanced Features](#advanced-features)

---

## Database Management

### Creating a Database

```rust
use lightning_db::{Database, LightningDbConfig, Result};

// Create with default configuration
let db = Database::create("./mydb", LightningDbConfig::default())?;

// Create with custom configuration
let config = LightningDbConfig {
    cache_size: 100 * 1024 * 1024,  // 100MB cache
    compression_enabled: true,
    wal_sync_mode: WalSyncMode::Sync,
    max_active_transactions: 1000,
    ..Default::default()
};
let db = Database::create("./mydb", config)?;
```

### Opening an Existing Database

```rust
// Open existing database
let db = Database::open("./mydb", config)?;

// Open read-only
let db = Database::open_read_only("./mydb")?;

// Open with repair if corrupted
let db = Database::open_with_repair("./mydb", config)?;
```

### Database Lifecycle

```rust
// Create checkpoint (flush all data to disk)
db.checkpoint()?;

// Sync pending writes
db.sync()?;

// Get database statistics
let stats = db.get_stats()?;
println!("Total keys: {}", stats.total_keys);
println!("Database size: {} bytes", stats.total_size);

// Graceful shutdown (automatic on drop)
db.shutdown()?;
```

---

## Core Operations

### Basic CRUD Operations

```rust
// Put operation
db.put(b"key", b"value")?;

// Put with options
use lightning_db::PutOptions;
db.put_with_options(
    b"key", 
    b"value",
    PutOptions {
        sync: true,
        ttl: Some(Duration::from_secs(3600)), // 1 hour TTL
    }
)?;

// Get operation
if let Some(value) = db.get(b"key")? {
    println!("Value: {:?}", String::from_utf8_lossy(&value));
}

// Delete operation
let existed = db.delete(b"key")?;

// Check existence
if db.contains_key(b"key")? {
    println!("Key exists");
}

// Conditional operations
db.put_if_absent(b"key", b"value")?;
db.compare_and_swap(b"key", Some(b"old_value"), b"new_value")?;
```

---

## Transaction API

### MVCC Transactions

```rust
use lightning_db::Transaction;

// Begin transaction
let mut tx = db.begin_transaction()?;

// Transactional operations
tx.put(b"key1", b"value1")?;
tx.put(b"key2", b"value2")?;
tx.delete(b"key3")?;

// Read within transaction
if let Some(value) = tx.get(b"key1")? {
    // Sees uncommitted changes
}

// Commit transaction
tx.commit()?;

// Or rollback
// tx.rollback()?;
```

### Transaction Isolation

```rust
use lightning_db::{IsolationLevel, TransactionOptions};

// Create transaction with specific isolation level
let tx = db.begin_transaction_with_options(
    TransactionOptions {
        isolation_level: IsolationLevel::Serializable,
        read_only: false,
        timeout: Some(Duration::from_secs(30)),
    }
)?;
```

### Optimistic Concurrency Control

```rust
// Read with version
let (value, version) = db.get_with_version(b"key")?;

// Update only if version matches
db.put_if_version(b"key", b"new_value", version)?;
```

---

## Batch Operations

### Batch Writes

```rust
use lightning_db::WriteBatch;

// Create batch
let mut batch = WriteBatch::new();

// Add operations to batch
batch.put(b"key1", b"value1");
batch.put(b"key2", b"value2");
batch.delete(b"key3");

// Apply batch atomically
db.write_batch(&batch)?;

// Batch with options
db.write_batch_with_options(
    &batch,
    WriteOptions {
        sync: true,
        disable_wal: false,
    }
)?;
```

### Bulk Import

```rust
use lightning_db::BulkLoader;

// Create bulk loader for fast imports
let loader = db.create_bulk_loader()?;

// Load data (optimized for sequential writes)
for (key, value) in large_dataset {
    loader.add(key, value)?;
}

// Finish loading
loader.finish()?;
```

---

## Range Queries

### Range Scans

```rust
// Scan all keys
for result in db.scan()? {
    let (key, value) = result?;
    println!("{:?} = {:?}", key, value);
}

// Scan with prefix
for result in db.scan_prefix(b"user:")? {
    let (key, value) = result?;
    // Process user entries
}

// Scan range
for result in db.scan_range(b"a"..b"z")? {
    let (key, value) = result?;
    // Process entries between "a" and "z"
}

// Reverse scan
for result in db.scan_reverse()? {
    let (key, value) = result?;
    // Process in reverse order
}
```

### Seek Operations

```rust
// Create iterator
let mut iter = db.iterator()?;

// Seek to specific key
iter.seek(b"start_key")?;

// Seek to last
iter.seek_to_last()?;

// Navigate
if let Some((key, value)) = iter.next()? {
    // Process entry
}

if let Some((key, value)) = iter.prev()? {
    // Process previous entry
}
```

---

## Backup & Recovery

### Creating Backups

```rust
use lightning_db::BackupEngine;

// Create backup engine
let backup_engine = BackupEngine::new("./backups")?;

// Create full backup
let backup_id = backup_engine.create_backup(&db)?;

// Create incremental backup
let incremental_id = backup_engine.create_incremental_backup(&db)?;

// List backups
let backups = backup_engine.list_backups()?;
for backup in backups {
    println!("Backup {}: {} bytes", backup.id, backup.size);
}
```

### Restoring from Backup

```rust
// Restore latest backup
backup_engine.restore_latest("./restored_db")?;

// Restore specific backup
backup_engine.restore_backup(backup_id, "./restored_db")?;

// Verify backup integrity
let is_valid = backup_engine.verify_backup(backup_id)?;
```

### Point-in-Time Recovery

```rust
use lightning_db::PointInTimeRecovery;

// Enable PITR
let pitr = PointInTimeRecovery::new(&db)?;

// Restore to specific timestamp
let target_time = SystemTime::now() - Duration::from_hours(2);
pitr.restore_to_time("./restored_db", target_time)?;
```

---

## Configuration

### Database Configuration

```rust
use lightning_db::{LightningDbConfig, WalSyncMode, CompressionType};

let config = LightningDbConfig {
    // Storage settings
    page_size: 4096,
    cache_size: 1024 * 1024 * 1024, // 1GB
    
    // Write-ahead log
    wal_sync_mode: WalSyncMode::Sync,
    use_improved_wal: true,
    
    // Compression
    compression_enabled: true,
    compression_type: CompressionType::Zstd,
    compression_level: 3,
    
    // Performance
    prefetch_enabled: true,
    prefetch_distance: 32,
    write_batch_size: 1000,
    
    // Transactions
    max_active_transactions: 10000,
    transaction_timeout: Duration::from_secs(300),
    
    // Memory management
    enable_memory_pools: true,
    memory_pool_size: 64 * 1024 * 1024,
    
    ..Default::default()
};
```

### Runtime Configuration

```rust
// Update configuration at runtime
db.set_cache_size(2 * 1024 * 1024 * 1024)?; // 2GB

// Tune performance
db.set_prefetch_distance(64)?;
db.set_write_batch_size(5000)?;

// Enable/disable features
db.set_compression_enabled(true)?;
db.set_statistics_enabled(true)?;
```

---

## Monitoring & Statistics

### Database Statistics

```rust
// Get overall statistics
let stats = db.get_stats()?;
println!("Operations: {}", stats.total_operations);
println!("Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
println!("Write amplification: {:.2}", stats.write_amplification);

// Get detailed statistics
let detailed = db.get_detailed_stats()?;
println!("B+Tree height: {}", detailed.btree_height);
println!("Page count: {}", detailed.page_count);
println!("Free pages: {}", detailed.free_pages);
```

### Performance Metrics

```rust
use lightning_db::Metrics;

// Enable metrics collection
db.enable_metrics()?;

// Get current metrics
let metrics = db.get_metrics()?;
println!("Read QPS: {}", metrics.read_qps);
println!("Write QPS: {}", metrics.write_qps);
println!("P99 latency: {:?}", metrics.p99_latency);

// Export metrics in Prometheus format
let prometheus_metrics = db.export_metrics_prometheus()?;
```

### Health Checks

```rust
// Perform health check
let health = db.health_check()?;
if health.is_healthy() {
    println!("Database is healthy");
} else {
    println!("Issues found: {:?}", health.issues);
}

// Verify data integrity
let integrity_report = db.verify_integrity()?;
if !integrity_report.errors.is_empty() {
    println!("Integrity errors: {:?}", integrity_report.errors);
}
```

---

## Error Handling

### Error Types

```rust
use lightning_db::{Error, Result};

match db.get(b"key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Key not found"),
    Err(Error::Io(e)) => eprintln!("I/O error: {}", e),
    Err(Error::Corruption { message, .. }) => eprintln!("Corruption: {}", message),
    Err(Error::Transaction(msg)) => eprintln!("Transaction error: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}
```

### Retry Logic

```rust
use lightning_db::RetryPolicy;

// Configure retry policy
let retry_policy = RetryPolicy {
    max_attempts: 3,
    initial_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(5),
    exponential_base: 2.0,
};

// Apply retry policy to operations
db.put_with_retry(b"key", b"value", &retry_policy)?;
```

---

## Advanced Features

### Encryption

```rust
use lightning_db::{EncryptionConfig, KeyProvider};

// Configure encryption
let encryption = EncryptionConfig {
    enabled: true,
    algorithm: EncryptionAlgorithm::AES256_GCM,
    key_provider: Box::new(MyKeyProvider),
    key_rotation_period: Duration::from_days(90),
};

// Create encrypted database
let db = Database::create_encrypted("./mydb", config, encryption)?;
```

### Sharding

```rust
use lightning_db::{ShardedDatabase, ShardingStrategy};

// Create sharded database
let sharded_db = ShardedDatabase::new(
    vec!["shard1", "shard2", "shard3"],
    ShardingStrategy::ConsistentHashing,
)?;

// Operations automatically routed to correct shard
sharded_db.put(b"key", b"value")?;
```

### Async API

```rust
use lightning_db::AsyncDatabase;

// Async operations
let async_db = AsyncDatabase::open("./mydb").await?;

// Async put
async_db.put(b"key", b"value").await?;

// Async get
if let Some(value) = async_db.get(b"key").await? {
    println!("Value: {:?}", value);
}

// Async transaction
let mut tx = async_db.begin_transaction().await?;
tx.put(b"key", b"value").await?;
tx.commit().await?;
```

### Custom Comparators

```rust
use lightning_db::{Comparator, Database};

// Define custom key comparator
struct ReverseComparator;

impl Comparator for ReverseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a) // Reverse order
    }
}

// Use custom comparator
let db = Database::create_with_comparator(
    "./mydb",
    config,
    Box::new(ReverseComparator),
)?;
```

---

## Examples

### Complete Example

```rust
use lightning_db::{Database, LightningDbConfig, WriteBatch, Result};
use std::time::Duration;

fn main() -> Result<()> {
    // Configure database
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        compression_enabled: true,
        ..Default::default()
    };
    
    // Open database
    let db = Database::open_or_create("./mydb", config)?;
    
    // Single operations
    db.put(b"user:1", b"Alice")?;
    db.put(b"user:2", b"Bob")?;
    
    // Batch operations
    let mut batch = WriteBatch::new();
    for i in 3..10 {
        batch.put(format!("user:{}", i).as_bytes(), b"User");
    }
    db.write_batch(&batch)?;
    
    // Transaction
    let mut tx = db.begin_transaction()?;
    tx.put(b"counter", b"0")?;
    tx.commit()?;
    
    // Range scan
    println!("All users:");
    for result in db.scan_prefix(b"user:")? {
        let (key, value) = result?;
        println!("  {} = {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }
    
    // Cleanup
    db.checkpoint()?;
    
    Ok(())
}
```

---

## Best Practices

1. **Always handle errors** - Lightning DB operations return `Result<T>`
2. **Use transactions for consistency** - Group related operations
3. **Enable compression for large values** - Reduces storage and I/O
4. **Monitor cache hit rate** - Tune cache size for workload
5. **Use batch operations** - More efficient than individual puts
6. **Regular checkpoints** - Ensures durability and bounds recovery time
7. **Verify backups** - Test restore procedures regularly

---

## Version Compatibility

- Lightning DB follows semantic versioning
- Database format is forward-compatible within major versions
- Breaking changes documented in [CHANGELOG.md](../CHANGELOG.md)

For more examples, see the [examples directory](../examples/).
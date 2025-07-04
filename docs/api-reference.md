# Lightning DB API Reference

## Table of Contents

1. [Database Management](#database-management)
2. [Core Operations](#core-operations)
3. [Transaction API](#transaction-api)
4. [Batch Operations](#batch-operations)
5. [Range Queries](#range-queries)
6. [Secondary Indexes](#secondary-indexes)
7. [Configuration](#configuration)
8. [Monitoring & Statistics](#monitoring--statistics)
9. [Administrative Operations](#administrative-operations)
10. [Error Handling](#error-handling)

## Database Management

### Creating a Database

```rust
use lightning_db::{Database, LightningDbConfig};

// Create with default configuration
let db = Database::create("./mydb", LightningDbConfig::default())?;

// Create with custom configuration
let config = LightningDbConfig {
    cache_size: 100 * 1024 * 1024,  // 100MB cache
    compression_enabled: true,
    wal_sync_mode: WalSyncMode::Sync,
    ..Default::default()
};
let db = Database::create("./mydb", config)?;
```

### Opening an Existing Database

```rust
// Open with same configuration used when created
let db = Database::open("./mydb", LightningDbConfig::default())?;
```

### Database Lifecycle

```rust
// Graceful shutdown (automatic on drop, but can be explicit)
db.shutdown()?;

// Create checkpoint (flush all data to disk)
db.checkpoint()?;

// Sync pending writes
db.sync()?;
```

## Core Operations

### Basic CRUD Operations

```rust
// Put operation
db.put(b"key", b"value")?;
db.put("user:123".as_bytes(), "Alice".as_bytes())?;

// Get operation
if let Some(value) = db.get(b"key")? {
    println!("Value: {:?}", value);
}

// Delete operation
let existed = db.delete(b"key")?;
println!("Key existed: {}", existed);
```

### Consistency Levels

```rust
use lightning_db::ConsistencyLevel;

// Put with specific consistency level
db.put_with_consistency(b"key", b"value", ConsistencyLevel::Strong)?;

// Get with specific consistency level
let value = db.get_with_consistency(b"key", ConsistencyLevel::Eventual)?;
```

## Transaction API

### Basic Transactions

```rust
// Begin transaction
let tx_id = db.begin_transaction()?;

// Transactional operations
db.put_tx(tx_id, b"key1", b"value1")?;
db.put_tx(tx_id, b"key2", b"value2")?;

// Read within transaction (sees uncommitted changes)
if let Some(value) = db.get_tx(tx_id, b"key1")? {
    println!("Value in tx: {:?}", value);
}

// Delete within transaction
db.delete_tx(tx_id, b"old_key")?;

// Commit transaction
db.commit_transaction(tx_id)?;

// OR abort transaction
// db.abort_transaction(tx_id)?;
```

### Transaction Isolation

```rust
// Start two concurrent transactions
let tx1 = db.begin_transaction()?;
let tx2 = db.begin_transaction()?;

// Each transaction sees its own snapshot
db.put_tx(tx1, b"key", b"value1")?;
db.put_tx(tx2, b"key", b"value2")?;

// Reads are isolated
assert_eq!(db.get_tx(tx1, b"key")?, Some(b"value1".to_vec()));
assert_eq!(db.get_tx(tx2, b"key")?, Some(b"value2".to_vec()));

// First to commit wins
db.commit_transaction(tx1)?;
// This will fail with conflict error
// db.commit_transaction(tx2)?;
```

## Batch Operations

### Batch Writes

```rust
// Prepare batch data
let pairs = vec![
    (b"user:1".to_vec(), b"Alice".to_vec()),
    (b"user:2".to_vec(), b"Bob".to_vec()),
    (b"user:3".to_vec(), b"Charlie".to_vec()),
];

// Batch put (atomic - all succeed or all fail)
db.put_batch(&pairs)?;
```

### Batch Reads

```rust
let keys = vec![
    b"user:1".to_vec(),
    b"user:2".to_vec(),
    b"user:3".to_vec(),
];

let results = db.get_batch(&keys)?;
for (i, result) in results.iter().enumerate() {
    match result {
        Some(value) => println!("Key {}: {:?}", i, value),
        None => println!("Key {} not found", i),
    }
}
```

### Batch Deletes

```rust
let keys_to_delete = vec![
    b"temp:1".to_vec(),
    b"temp:2".to_vec(),
    b"temp:3".to_vec(),
];

let results = db.delete_batch(&keys_to_delete)?;
// Returns Vec<bool> indicating which keys existed
```

## Range Queries

### Basic Range Scans

```rust
// Scan all keys
let iter = db.scan(None, None)?;
for result in iter {
    let (key, value) = result?;
    println!("{:?} = {:?}", key, value);
}

// Scan with bounds
let iter = db.scan(
    Some(b"user:".to_vec()),      // start key (inclusive)
    Some(b"user:~".to_vec())      // end key (exclusive)
)?;
```

### Prefix Scans

```rust
// Scan all keys with prefix "user:"
let iter = db.scan_prefix(b"user:")?;
for result in iter {
    let (key, value) = result?;
    println!("{:?} = {:?}", key, value);
}
```

### Reverse Scans

```rust
// Scan in reverse order
let iter = db.scan_reverse(
    Some(b"user:999".to_vec()),   // start (highest)
    Some(b"user:000".to_vec())    // end (lowest)
)?;
```

### Limited Scans

```rust
// Scan with limit
let iter = db.scan_limit(Some(b"user:".to_vec()), 10)?;
// Returns at most 10 entries
```

### Transactional Scans

```rust
let tx_id = db.begin_transaction()?;

// Add some data in transaction
db.put_tx(tx_id, b"tx:1", b"value1")?;
db.put_tx(tx_id, b"tx:2", b"value2")?;

// Scan sees uncommitted changes
let iter = db.scan_tx(tx_id, None, None)?;
for result in iter {
    let (key, value) = result?;
    // Will see both committed data and uncommitted changes
}

db.commit_transaction(tx_id)?;
```

### Convenience Methods

```rust
// Get all keys as iterator
let keys_iter = db.keys()?;
for key_result in keys_iter {
    let key = key_result?;
    println!("Key: {:?}", key);
}

// Get all values as iterator
let values_iter = db.values()?;

// Count entries in range
let count = db.count_range(
    Some(b"user:".to_vec()),
    Some(b"user:~".to_vec())
)?;

// Get range as vector (for small result sets)
let results = db.range(Some(b"user:"), Some(b"user:~"))?;
```

## Secondary Indexes

### Creating Indexes

```rust
use lightning_db::{IndexConfig, IndexType};

// Simple index
db.create_index("name_index", vec!["name".to_string()])?;

// Complex index configuration
let config = IndexConfig {
    name: "user_email_index".to_string(),
    columns: vec!["email".to_string()],
    unique: true,
    index_type: IndexType::BTree,
};
db.create_index_with_config(config)?;
```

### Querying Indexes

```rust
use lightning_db::IndexKey;

// Query by index
let index_key = IndexKey::single(b"alice@example.com".to_vec());
let results = db.query_index("user_email_index", index_key.as_bytes())?;

// Range query on index
let results = db.range_index(
    "name_index",
    Some(b"A"),  // start
    Some(b"B")   // end (exclusive)
)?;
```

### Indexed Operations

```rust
use lightning_db::SimpleRecord;

// Create a record that implements IndexableRecord
let user_record = SimpleRecord::new()
    .field("name", b"Alice")
    .field("email", b"alice@example.com")
    .field("age", b"25");

// Put with automatic index updates
db.put_indexed(b"user:123", b"user_data", &user_record)?;

// Update with index maintenance
let new_record = SimpleRecord::new()
    .field("name", b"Alice Smith")
    .field("email", b"alice.smith@example.com")
    .field("age", b"26");

db.update_indexed(
    b"user:123",
    b"new_user_data",
    &user_record,    // old record
    &new_record      // new record
)?;

// Delete with index cleanup
db.delete_indexed(b"user:123", &new_record)?;
```

### Advanced Queries

```rust
use lightning_db::{JoinQuery, JoinType, QueryCondition};

// Inner join between two indexes
let results = db.inner_join(
    "user_department_index",
    IndexKey::single(b"engineering".to_vec()),
    "department_budget_index", 
    IndexKey::single(b"engineering".to_vec())
)?;

// Complex query with conditions
let conditions = vec![
    ("age", ">", b"18"),
    ("department", "=", b"engineering"),
];
let plan = db.plan_query(&conditions)?;
let results = db.execute_planned_query(&plan)?;
```

## Configuration

### Configuration Options

```rust
use lightning_db::{LightningDbConfig, WalSyncMode, ConsistencyConfig, ConsistencyLevel};

let config = LightningDbConfig {
    // Performance
    page_size: 4096,                    // Page size in bytes
    cache_size: 256 * 1024 * 1024,     // Cache size (256MB)
    mmap_size: Some(1024 * 1024 * 1024), // Memory map size (1GB)
    
    // Compression
    compression_enabled: true,
    compression_type: 1,                // 0=None, 1=Zstd, 2=LZ4
    
    // Transactions
    max_active_transactions: 1000,
    use_optimized_transactions: false,  // Disable if stability issues
    
    // WAL (Write-Ahead Log)
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Async,  // Sync | Periodic | Async
    
    // Consistency
    consistency_config: ConsistencyConfig {
        default_level: ConsistencyLevel::Eventual,
        enable_checksums: true,
        verify_writes: true,
        ..Default::default()
    },
    
    // Performance features
    prefetch_enabled: false,            // Disable for predictable latency
    prefetch_distance: 8,
    prefetch_workers: 2,
    
    // Write optimization
    write_batch_size: 1000,
    use_optimized_page_manager: false,  // Disable if deadlock issues
    
    ..Default::default()
};
```

### WAL Sync Modes

```rust
// Maximum durability (slowest)
config.wal_sync_mode = WalSyncMode::Sync;

// Periodic sync (balanced)
config.wal_sync_mode = WalSyncMode::Periodic { interval_ms: 100 };

// No automatic sync (fastest, least durable)
config.wal_sync_mode = WalSyncMode::Async;
```

### Memory Configuration

```rust
use lightning_db::storage::MmapConfig;

config.mmap_config = Some(MmapConfig {
    max_mmap_size: 2 * 1024 * 1024 * 1024,  // 2GB
    enable_huge_pages: false,                 // OS-dependent
    prefault_pages: false,                    // Eager loading
    sync_on_write: true,                      // Immediate persistence
});
```

## Monitoring & Statistics

### Database Statistics

```rust
// Get basic statistics
let stats = db.stats();
println!("Pages: {}", stats.page_count);
println!("Free pages: {}", stats.free_page_count);
println!("Tree height: {}", stats.tree_height);
println!("Active transactions: {}", stats.active_transactions);
```

### Performance Metrics

```rust
// Get comprehensive metrics
let metrics = db.get_metrics();
println!("Read ops/sec: {}", metrics.read_ops_per_sec);
println!("Write ops/sec: {}", metrics.write_ops_per_sec);
println!("Cache hit rate: {}%", metrics.cache_hit_rate * 100.0);

// Get formatted metrics report
let reporter = db.get_metrics_reporter();
println!("{}", reporter.format_summary());
println!("{}", reporter.format_detailed());
```

### Cache Statistics

```rust
if let Some(cache_stats) = db.cache_stats() {
    println!("Cache statistics: {}", cache_stats);
}
```

### LSM Tree Statistics

```rust
if let Some(lsm_stats) = db.lsm_stats() {
    println!("LSM levels: {}", lsm_stats.level_count);
    println!("Total size: {} bytes", lsm_stats.total_size);
    println!("Compaction ratio: {:.2}", lsm_stats.compaction_ratio);
}
```

### Production Health Checks

```rust
use lightning_db::monitoring::production_hooks::{HealthStatus, OperationType};
use std::time::Duration;

// Set performance thresholds
db.set_operation_threshold(OperationType::Read, Duration::from_micros(100));
db.set_operation_threshold(OperationType::Write, Duration::from_millis(1));

// Health check
match db.production_health_check() {
    HealthStatus::Healthy => println!("Database is healthy"),
    HealthStatus::Warning(issues) => println!("Warning: {:?}", issues),
    HealthStatus::Critical(issues) => println!("Critical: {:?}", issues),
}

// Collect production metrics
let prod_metrics = db.get_production_metrics();
for (metric, value) in prod_metrics {
    println!("{}: {}", metric, value);
}
```

## Administrative Operations

### Data Integrity

```rust
use lightning_db::integrity::{VerificationConfig, IntegrityReport};

// Basic integrity check
let report = db.verify_integrity()?;
if !report.errors.is_empty() {
    println!("Integrity errors found: {:?}", report.errors);
}

// Integrity check with custom config
let config = VerificationConfig {
    verify_checksums: true,
    verify_key_order: true,
    verify_references: true,
    max_errors: 100,
};
let report = db.verify_integrity_with_config(config)?;

// Attempt repair if issues found
if !report.errors.is_empty() {
    let repair_report = db.repair_integrity(&report)?;
    println!("Repaired {} issues", repair_report.fixed_count);
}
```

### Compaction

```rust
// Manual LSM compaction
db.compact_lsm()?;

// Flush in-memory data
db.flush_lsm()?;
db.flush_write_buffer()?;
```

### Cleanup Operations

```rust
// Clean up old transaction data
db.cleanup_old_transactions(30 * 60 * 1000); // 30 minutes in ms

// Clean up old MVCC versions
let cutoff_timestamp = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)?
    .as_millis() as u64 - (60 * 60 * 1000); // 1 hour ago
db.cleanup_old_versions(cutoff_timestamp);
```

### Index Management

```rust
// List all indexes
let indexes = db.list_indexes();
for index_name in indexes {
    println!("Index: {}", index_name);
}

// Drop an index
db.drop_index("old_index")?;

// Analyze indexes for query optimization
db.analyze_indexes()?;
```

## Write Batchers

### Auto Batcher (Recommended)

```rust
use std::sync::Arc;

let db = Arc::new(Database::create("./db", LightningDbConfig::default())?);

// Create auto batcher for optimal performance
let batcher = Database::create_auto_batcher(db.clone());

// Writes are automatically batched for efficiency
for i in 0..10000 {
    let key = format!("key:{}", i);
    let value = format!("value:{}", i);
    batcher.put(key.as_bytes(), value.as_bytes())?;
}

// Batcher automatically flushes based on size/time
// Can force flush if needed
batcher.flush()?;
```

### Fast Auto Batcher (Lower Latency)

```rust
// For latency-sensitive applications
let fast_batcher = Database::create_fast_auto_batcher(db.clone());

// Lower latency but potentially lower throughput
fast_batcher.put(b"urgent_key", b"urgent_value")?;
```

## Error Handling

### Error Types

```rust
use lightning_db::{Error, ErrorContext};

match db.get(b"key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Key not found"),
    Err(Error::Io(io_error)) => {
        eprintln!("I/O error: {}", io_error);
    },
    Err(Error::Transaction(tx_error)) => {
        eprintln!("Transaction error: {}", tx_error);
    },
    Err(Error::Corruption(corruption_error)) => {
        eprintln!("Data corruption: {}", corruption_error);
        // Consider running integrity check
    },
    Err(Error::Generic(msg)) => {
        eprintln!("Generic error: {}", msg);
    },
}
```

### Error Context

```rust
// Errors include context for debugging
if let Err(e) = db.put(b"key", b"value") {
    eprintln!("Error: {}", e);
    
    // Print full error chain
    let mut source = e.source();
    while let Some(err) = source {
        eprintln!("Caused by: {}", err);
        source = err.source();
    }
}
```

## Best Practices

### Performance Tips

```rust
// 1. Use appropriate configuration for workload
let config = LightningDbConfig {
    // For read-heavy workloads
    cache_size: 1024 * 1024 * 1024,  // Large cache
    compression_enabled: false,       // Less CPU usage
    
    // For write-heavy workloads  
    wal_sync_mode: WalSyncMode::Async, // Faster writes
    write_batch_size: 5000,            // Larger batches
    
    ..Default::default()
};

// 2. Use batchers for high-throughput writes
let batcher = Database::create_auto_batcher(Arc::new(db));

// 3. Use transactions for consistency
let tx_id = db.begin_transaction()?;
// ... multiple operations ...
db.commit_transaction(tx_id)?;

// 4. Use appropriate consistency levels
db.put_with_consistency(b"key", b"value", ConsistencyLevel::Eventual)?;
```

### Memory Management

```rust
// Set resource limits
let config = LightningDbConfig {
    cache_size: 512 * 1024 * 1024,    // 512MB cache limit
    max_active_transactions: 100,      // Limit concurrent transactions
    ..Default::default()
};

// Monitor memory usage
let metrics = db.get_metrics();
if metrics.memory_usage > 0.8 * config.cache_size as f64 {
    // Consider reducing cache size or clearing old data
}
```

### Error Recovery

```rust
// Handle transient errors with retry
fn put_with_retry(db: &Database, key: &[u8], value: &[u8]) -> Result<(), Error> {
    const MAX_RETRIES: usize = 3;
    
    for attempt in 0..MAX_RETRIES {
        match db.put(key, value) {
            Ok(()) => return Ok(()),
            Err(Error::Transaction(_)) if attempt < MAX_RETRIES - 1 => {
                // Retry transaction conflicts
                std::thread::sleep(std::time::Duration::from_millis(10));
                continue;
            },
            Err(e) => return Err(e),
        }
    }
    
    unreachable!()
}
```

---

This API reference covers the complete Lightning DB interface. For more examples and advanced usage patterns, see the examples directory in the repository.

*Last Updated: API Reference v1.0*  
*Next Review: After API changes*  
*Owner: Lightning DB API Team*
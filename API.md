# Lightning DB API Reference

## Core Database Operations

### Database Management

#### `Database::create(path: &str, config: LightningDbConfig) -> Result<Database>`
Creates a new database at the specified path.

```rust
let db = Database::create("./mydb", LightningDbConfig::default())?;
```

#### `Database::open(path: &str, config: LightningDbConfig) -> Result<Database>`
Opens an existing database or creates a new one if it doesn't exist.

```rust
let db = Database::open("./mydb", LightningDbConfig::default())?;
```

### Basic Operations

#### `put(key: &[u8], value: &[u8]) -> Result<()>`
Stores a key-value pair in the database.

```rust
db.put(b"key", b"value")?;
```

#### `get(key: &[u8]) -> Result<Option<Vec<u8>>>`
Retrieves a value by key.

```rust
if let Some(value) = db.get(b"key")? {
    println!("Value: {:?}", value);
}
```

#### `delete(key: &[u8]) -> Result<bool>`
Deletes a key-value pair. Returns true if the key existed.

```rust
let existed = db.delete(b"key")?;
```

### Batch Operations

#### `put_batch(pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()>`
Inserts multiple key-value pairs atomically.

```rust
let batch = vec![
    (b"key1".to_vec(), b"value1".to_vec()),
    (b"key2".to_vec(), b"value2".to_vec()),
];
db.put_batch(&batch)?;
```

#### `delete_batch(keys: &[Vec<u8>]) -> Result<()>`
Deletes multiple keys atomically.

```rust
let keys = vec![b"key1".to_vec(), b"key2".to_vec()];
db.delete_batch(&keys)?;
```

### Range Queries

#### `range_query(start: &[u8], end: &[u8], limit: usize) -> Result<Vec<KeyEntry>>`
Performs a range query between start and end keys.

```rust
let results = db.range_query(b"a", b"z", 100)?;
for entry in results {
    println!("{:?} => {:?}", entry.key, entry.value);
}
```

#### `scan(start: Option<&[u8]>, end: Option<&[u8]>, limit: Option<usize>) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`
Scans the database with optional bounds.

```rust
// Scan all entries
let all = db.scan(None, None, None)?;

// Scan with prefix
let prefix_scan = db.scan(Some(b"prefix"), Some(b"prefix\xff"), None)?;
```

## Transactions

### Transaction Management

#### `begin_transaction() -> Result<u64>`
Begins a new transaction and returns its ID.

```rust
let tx_id = db.begin_transaction()?;
```

#### `commit_transaction(tx_id: u64) -> Result<()>`
Commits a transaction, making all changes permanent.

```rust
db.commit_transaction(tx_id)?;
```

#### `rollback_transaction(tx_id: u64) -> Result<()>`
Rolls back a transaction, discarding all changes.

```rust
db.rollback_transaction(tx_id)?;
```

### Transactional Operations

#### `put_tx(tx_id: u64, key: &[u8], value: &[u8]) -> Result<()>`
Puts a key-value pair within a transaction.

```rust
db.put_tx(tx_id, b"key", b"value")?;
```

#### `get_tx(tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>>`
Gets a value within a transaction, seeing uncommitted changes.

```rust
let value = db.get_tx(tx_id, b"key")?;
```

#### `delete_tx(tx_id: u64, key: &[u8]) -> Result<()>`
Deletes a key within a transaction.

```rust
db.delete_tx(tx_id, b"key")?;
```

## Configuration

### LightningDbConfig

```rust
pub struct LightningDbConfig {
    // Page size for B+Tree nodes (default: 4096)
    pub page_size: u64,
    
    // Cache size in bytes (0 = disabled, default: 0)
    pub cache_size: u64,
    
    // Memory map size (None = auto, default: None)
    pub mmap_size: Option<u64>,
    
    // Enable compression (default: true)
    pub compression_enabled: bool,
    
    // Compression type: 0=None, 1=Zstd, 2=LZ4 (default: 1)
    pub compression_type: i32,
    
    // Maximum active transactions (default: 1000)
    pub max_active_transactions: usize,
    
    // Enable prefetching (default: false)
    pub prefetch_enabled: bool,
    
    // Prefetch distance (default: 8)
    pub prefetch_distance: usize,
    
    // Prefetch worker threads (default: 2)
    pub prefetch_workers: usize,
    
    // Use improved WAL (default: true)
    pub use_improved_wal: bool,
    
    // WAL sync mode (default: Async)
    pub wal_sync_mode: WalSyncMode,
    
    // Write batch size (default: 1000)
    pub write_batch_size: usize,
}
```

### WalSyncMode

```rust
pub enum WalSyncMode {
    // Sync after every write (safest, slowest)
    Sync,
    
    // Sync periodically
    Periodic { interval_ms: u64 },
    
    // Never sync automatically (fastest, least safe)
    Async,
}
```

## Advanced Features

### Secondary Indexes

#### `create_index(name: &str, extractor: F) -> Result<()>`
Creates a secondary index with a key extractor function.

```rust
db.create_index("email_index", |record| {
    // Extract email field from record
    record.get_field("email").map(|v| v.to_vec())
})?;
```

#### `query_index(index_name: &str, key: &[u8]) -> Result<Vec<Vec<u8>>>`
Queries a secondary index.

```rust
let user_ids = db.query_index("email_index", b"user@example.com")?;
```

### Monitoring and Metrics

#### `export_metrics() -> Result<String>`
Exports Prometheus-compatible metrics.

```rust
let metrics = db.export_metrics()?;
println!("{}", metrics);
```

#### `get_stats() -> DatabaseStats`
Gets current database statistics.

```rust
let stats = db.get_stats();
println!("Pages: {}, Free: {}", stats.page_count, stats.free_page_count);
```

### Consistency Levels

#### `put_with_consistency(key: &[u8], value: &[u8], level: ConsistencyLevel) -> Result<()>`
Puts a value with a specific consistency level.

```rust
use lightning_db::ConsistencyLevel;

// Wait for replication
db.put_with_consistency(b"key", b"value", ConsistencyLevel::Quorum)?;

// Local write only
db.put_with_consistency(b"key", b"value", ConsistencyLevel::Local)?;
```

## Error Handling

All operations return `Result<T, Error>` where `Error` is an enum of possible error conditions:

```rust
use lightning_db::error::{Error, ErrorContext};

match db.get(b"key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Not found"),
    Err(Error::Io(msg)) => eprintln!("I/O error: {}", msg),
    Err(Error::Transaction(msg)) => eprintln!("Transaction error: {}", msg),
    Err(Error::Timeout(msg)) => eprintln!("Operation timed out: {}", msg),
    Err(e) => eprintln!("Other error: {}", e),
}

// Add context to errors
let result = db.get(b"user:123")
    .context("Failed to load user profile")?;
```

## Thread Safety

The `Database` struct is thread-safe and can be shared across threads:

```rust
use std::sync::Arc;
use std::thread;

let db = Arc::new(Database::open("./mydb", config)?);

let mut handles = vec![];
for i in 0..10 {
    let db_clone = Arc::clone(&db);
    handles.push(thread::spawn(move || {
        db_clone.put(format!("key{}", i).as_bytes(), b"value").unwrap();
    }));
}

for handle in handles {
    handle.join().unwrap();
}
```

## C API

For C/C++ integration, include `lightning_db.h`:

```c
// Open database
LightningDB* db = lightning_db_open("./mydb", NULL);

// Basic operations
lightning_db_put(db, "key", 3, "value", 5);

size_t value_len;
char* value = lightning_db_get(db, "key", 3, &value_len);

// Transactions
uint64_t tx_id = lightning_db_begin_transaction(db);
lightning_db_put_tx(db, tx_id, "key2", 4, "value2", 6);
lightning_db_commit_transaction(db, tx_id);

// Clean up
lightning_db_free_value(value);
lightning_db_close(db);
```
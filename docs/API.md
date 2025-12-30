# Lightning DB API Reference

Complete API reference for the Lightning DB Rust library.

## Table of Contents

- [Database Creation](#database-creation)
- [Basic Operations](#basic-operations)
- [Transactions](#transactions)
- [Batch Operations](#batch-operations)
- [Range Scans](#range-scans)
- [Index Operations](#index-operations)
- [Maintenance](#maintenance)
- [Statistics & Monitoring](#statistics--monitoring)
- [Error Handling](#error-handling)

---

## Database Creation

### `Database::create`

Create a new database at the specified path.

```rust
pub fn create<P: AsRef<Path>>(path: P, config: LightningDbConfig) -> Result<Self>
```

**Parameters:**
- `path` - Path where the database will be created
- `config` - Configuration options

**Returns:** `Result<Database>`

**Example:**
```rust
use lightning_db::{Database, LightningDbConfig};

let db = Database::create("./my_db", LightningDbConfig::default())?;
```

---

### `Database::open`

Open an existing database or create if it doesn't exist.

```rust
pub fn open<P: AsRef<Path>>(path: P, config: LightningDbConfig) -> Result<Self>
```

**Example:**
```rust
let db = Database::open("./my_db", LightningDbConfig::default())?;
```

---

## Basic Operations

### `put`

Store a key-value pair.

```rust
pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>
```

**Constraints:**
- Key: 1 byte to 4KB
- Value: Up to 1MB

**Example:**
```rust
db.put(b"user:1001", b"John Doe")?;
db.put(b"config", b"{\"debug\": true}")?;
```

---

### `get`

Retrieve a value by key.

```rust
pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>
```

**Returns:** `Some(value)` if found, `None` if not found

**Example:**
```rust
if let Some(value) = db.get(b"user:1001")? {
    println!("Found: {:?}", String::from_utf8_lossy(&value));
}
```

---

### `delete`

Delete a key from the database.

```rust
pub fn delete(&self, key: &[u8]) -> Result<bool>
```

**Returns:** `true` if key existed and was deleted, `false` if key didn't exist

**Example:**
```rust
let deleted = db.delete(b"user:1001")?;
```

---

### `contains_key`

Check if a key exists.

```rust
pub fn contains_key(&self, key: &[u8]) -> Result<bool>
```

**Example:**
```rust
if db.contains_key(b"user:1001")? {
    println!("User exists");
}
```

---

## Transactions

### `begin_transaction`

Start a new transaction.

```rust
pub fn begin_transaction(&self) -> Result<u64>
```

**Returns:** Transaction ID

**Example:**
```rust
let tx_id = db.begin_transaction()?;
```

---

### `begin_transaction_with_isolation`

Start a transaction with specific isolation level.

```rust
pub fn begin_transaction_with_isolation(
    &self,
    isolation_level: IsolationLevel,
) -> Result<u64>
```

**Isolation Levels:**
- `ReadCommitted` - See only committed data
- `RepeatableRead` - Consistent reads within transaction
- `Serializable` - Full isolation (default)
- `Snapshot` - Point-in-time snapshot

**Example:**
```rust
use lightning_db::features::transactions::isolation::IsolationLevel;

let tx_id = db.begin_transaction_with_isolation(IsolationLevel::Snapshot)?;
```

---

### `commit_transaction`

Commit a transaction, making all changes permanent.

```rust
pub fn commit_transaction(&self, tx_id: u64) -> Result<()>
```

**Example:**
```rust
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"key", b"value")?;
db.commit_transaction(tx_id)?;  // Changes are now permanent
```

---

### `abort_transaction`

Abort a transaction, discarding all changes.

```rust
pub fn abort_transaction(&self, tx_id: u64) -> Result<()>
```

**Example:**
```rust
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"key", b"value")?;
db.abort_transaction(tx_id)?;  // Changes are discarded
```

---

### `put_tx`

Store a key-value pair within a transaction.

```rust
pub fn put_tx(&self, tx_id: u64, key: &[u8], value: &[u8]) -> Result<()>
```

**Example:**
```rust
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"account:1", b"1000")?;
db.put_tx(tx_id, b"account:2", b"500")?;
db.commit_transaction(tx_id)?;
```

---

### `get_tx`

Read a value within a transaction (sees uncommitted changes).

```rust
pub fn get_tx(&self, tx_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>>
```

**Example:**
```rust
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, b"key", b"new_value")?;

// Sees the uncommitted "new_value"
let value = db.get_tx(tx_id, b"key")?;
```

---

### `delete_tx`

Delete a key within a transaction.

```rust
pub fn delete_tx(&self, tx_id: u64, key: &[u8]) -> Result<()>
```

---

## Batch Operations

### `WriteBatch`

Atomic batch of operations.

```rust
use lightning_db::WriteBatch;

let mut batch = WriteBatch::new();
batch.put(b"key1", b"value1")?;
batch.put(b"key2", b"value2")?;
batch.delete(b"old_key")?;

db.write_batch(&batch)?;  // All operations are atomic
```

---

### `write_batch`

Execute a batch atomically.

```rust
pub fn write_batch(&self, batch: &WriteBatch) -> Result<()>
```

---

### `batch_put`

Put multiple key-value pairs efficiently.

```rust
pub fn batch_put(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()>
```

**Example:**
```rust
let pairs = vec![
    (b"key1".to_vec(), b"value1".to_vec()),
    (b"key2".to_vec(), b"value2".to_vec()),
];
db.batch_put(&pairs)?;
```

---

## Range Scans

### `scan`

Scan keys in a range.

```rust
pub fn scan(
    &self,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
```

**Parameters:**
- `start_key` - Start of range (inclusive), None = beginning
- `end_key` - End of range (exclusive), None = end

**Example:**
```rust
// Scan all keys
let all = db.scan(None, None)?;

// Scan range
let range = db.scan(Some(b"user:1000"), Some(b"user:2000"))?;

for (key, value) in range {
    println!("{:?} = {:?}", key, value);
}
```

---

### `scan_prefix`

Scan all keys with a given prefix.

```rust
pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
```

**Example:**
```rust
// Get all user keys
let users = db.scan_prefix(b"user:")?;
```

---

### `RangeIterator`

Efficient iterator for large range scans.

```rust
use lightning_db::IteratorBuilder;

let iter = IteratorBuilder::new(&db)
    .start_key(b"user:")
    .end_key(b"user:\xFF")
    .build()?;

for result in iter {
    let (key, value) = result?;
    // Process each entry
}
```

---

## Index Operations

### `create_index_with_config`

Create a secondary index.

```rust
pub fn create_index_with_config(
    &self,
    name: &str,
    config: IndexConfig,
) -> Result<()>
```

**Example:**
```rust
use lightning_db::{IndexConfig, IndexType};

let config = IndexConfig {
    name: "email_index".to_string(),
    index_type: IndexType::BTree,
    unique: true,
    ..Default::default()
};

db.create_index_with_config("email_index", config)?;
```

---

### `query_index_advanced`

Query an index with advanced options.

```rust
pub fn query_index_advanced(
    &self,
    index_name: &str,
    query: &IndexQuery,
    options: QueryOptions,
) -> Result<Vec<Vec<u8>>>
```

---

## Maintenance

### `checkpoint`

Force a checkpoint to persist all changes.

```rust
pub fn checkpoint(&self) -> Result<()>
```

---

### `compact`

Trigger compaction to reclaim space.

```rust
pub fn compact(&self) -> Result<()>
```

---

### `flush`

Flush all pending writes to disk.

```rust
pub fn flush(&self) -> Result<()>
```

---

### `verify_integrity`

Verify database integrity.

```rust
pub fn verify_integrity(&self) -> Result<bool>
```

**Returns:** `true` if database is healthy

---

## Statistics & Monitoring

### `stats`

Get database statistics.

```rust
pub fn stats(&self) -> DatabaseStats
```

**Example:**
```rust
let stats = db.stats();
println!("Page count: {}", stats.page_count);
println!("Tree height: {}", stats.tree_height);
println!("Cache hit rate: {:?}", stats.cache_hit_rate);
```

---

### `DatabaseStats`

```rust
pub struct DatabaseStats {
    pub page_count: u32,
    pub free_page_count: usize,
    pub tree_height: u32,
    pub active_transactions: usize,
    pub cache_hit_rate: Option<f64>,
    pub memory_usage_bytes: u64,
    pub disk_usage_bytes: u64,
    pub active_connections: u64,
}
```

---

## Error Handling

### Error Types

```rust
pub enum Error {
    /// I/O operation failed
    Io(String),

    /// Page not found
    PageNotFound(u32),

    /// Transaction error
    Transaction(String),

    /// Key size out of bounds
    InvalidKeySize { size: usize, min: usize, max: usize },

    /// Value size out of bounds
    InvalidValueSize { size: usize, max: usize },

    /// Encryption error
    Encryption(String),

    /// Resource quota exceeded
    QuotaExceeded(String),

    /// Generic error
    Generic(String),

    // ... and more
}
```

### Result Type

```rust
pub type Result<T> = std::result::Result<T, Error>;
```

### Error Handling Example

```rust
use lightning_db::{Database, Error};

match db.put(b"key", b"value") {
    Ok(()) => println!("Success"),
    Err(Error::InvalidKeySize { size, min, max }) => {
        eprintln!("Key size {} not in range {}-{}", size, min, max);
    }
    Err(Error::QuotaExceeded(msg)) => {
        eprintln!("Quota exceeded: {}", msg);
    }
    Err(e) => {
        eprintln!("Error: {}", e);
    }
}
```

---

## Async API

Lightning DB provides an async wrapper for use with Tokio.

```rust
use lightning_db::AsyncDatabase;

let db = AsyncDatabase::open("./my_db", config).await?;

db.put(b"key", b"value").await?;
let value = db.get(b"key").await?;
```

---

## Backup & Restore

### BackupManager

```rust
use lightning_db::{BackupManager, BackupConfig};

let backup_manager = BackupManager::new(&db);

// Create backup
backup_manager.create_backup("./backup", BackupConfig::default())?;

// Restore backup
BackupManager::restore_backup("./backup", "./restored_db")?;
```

---

## Type Aliases

| Type | Description |
|------|-------------|
| `Result<T>` | `std::result::Result<T, Error>` |
| `Key` | Efficient key type with inline storage |
| `SmallKey` | Fixed-size small key (up to 64 bytes) |
| `Transaction` | MVCC transaction handle |

---

## Thread Safety

`Database` is fully thread-safe and can be shared across threads using `Arc`:

```rust
use std::sync::Arc;
use std::thread;

let db = Arc::new(Database::create("./my_db", config)?);

let handles: Vec<_> = (0..4).map(|i| {
    let db = Arc::clone(&db);
    thread::spawn(move || {
        let key = format!("key:{}", i);
        db.put(key.as_bytes(), b"value").unwrap();
    })
}).collect();

for handle in handles {
    handle.join().unwrap();
}
```

---

## Performance Tips

1. **Use batch operations** for multiple writes:
   ```rust
   let mut batch = WriteBatch::new();
   for i in 0..1000 {
       batch.put(format!("key:{}", i).as_bytes(), b"value")?;
   }
   db.write_batch(&batch)?;
   ```

2. **Use transactions** for atomicity:
   ```rust
   let tx = db.begin_transaction()?;
   // Multiple operations...
   db.commit_transaction(tx)?;
   ```

3. **Use iterators** for large scans instead of loading all into memory.

4. **Configure cache size** appropriately for your workload.

5. **Use async API** for I/O-bound applications.

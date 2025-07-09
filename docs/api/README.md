# Lightning DB API Reference

## Overview

Lightning DB provides a high-performance embedded database with full ACID compliance. This API reference covers all public interfaces and their usage patterns.

## Quick Links

- [Core API](#core-api) - Main database operations
- [Transaction API](#transaction-api) - ACID transaction management
- [Configuration API](#configuration-api) - Database configuration options
- [Error Handling](#error-handling) - Error types and handling patterns
- [Examples](#examples) - Common usage patterns

## Core API

### Database Operations

#### `Database::create(path, config) -> Result<Database>`

Creates a new database instance at the specified path with the given configuration.

**Parameters:**
- `path: impl AsRef<Path>` - Database directory path
- `config: LightningDbConfig` - Database configuration

**Returns:**
- `Result<Database, LightningDbError>` - Database instance or error

**Example:**
```rust
use lightning_db::{Database, LightningDbConfig};

let config = LightningDbConfig {
    cache_size: 100 * 1024 * 1024, // 100MB
    compression_enabled: true,
    ..Default::default()
};

let db = Database::create("./my_database", config)?;
```

#### `Database::put(key, value) -> Result<()>`

Stores a key-value pair in the database.

**Parameters:**
- `key: impl AsRef<[u8]>` - The key to store
- `value: impl AsRef<[u8]>` - The value to store

**Returns:**
- `Result<(), LightningDbError>` - Success or error

**Example:**
```rust
db.put("user:123", b"John Doe")?;
db.put(b"counter", b"42")?;
```

#### `Database::get(key) -> Result<Option<Vec<u8>>>`

Retrieves a value by its key.

**Parameters:**
- `key: impl AsRef<[u8]>` - The key to retrieve

**Returns:**
- `Result<Option<Vec<u8>>, LightningDbError>` - Value if exists, None if not found, or error

**Example:**
```rust
let value = db.get("user:123")?;
if let Some(data) = value {
    let name = String::from_utf8(data)?;
    println!("User name: {}", name);
}
```

#### `Database::delete(key) -> Result<bool>`

Deletes a key-value pair from the database.

**Parameters:**
- `key: impl AsRef<[u8]>` - The key to delete

**Returns:**
- `Result<bool, LightningDbError>` - True if key existed and was deleted, false if key didn't exist

**Example:**
```rust
let existed = db.delete("user:123")?;
if existed {
    println!("User deleted successfully");
}
```

#### `Database::scan(prefix) -> Result<Vec<(Vec<u8>, Vec<u8>)>>`

Scans for all key-value pairs with the given prefix.

**Parameters:**
- `prefix: impl AsRef<[u8]>` - The prefix to scan for

**Returns:**
- `Result<Vec<(Vec<u8>, Vec<u8>)>, LightningDbError>` - Vector of key-value pairs

**Example:**
```rust
let users = db.scan("user:")?;
for (key, value) in users {
    let key_str = String::from_utf8(key)?;
    let name = String::from_utf8(value)?;
    println!("{}: {}", key_str, name);
}
```

## Transaction API

### Transaction Management

#### `Database::begin_transaction() -> Result<TransactionId>`

Begins a new ACID transaction.

**Returns:**
- `Result<TransactionId, LightningDbError>` - Transaction ID or error

**Example:**
```rust
let tx_id = db.begin_transaction()?;
```

#### `Database::put_tx(tx_id, key, value) -> Result<()>`

Stores a key-value pair within a transaction.

**Parameters:**
- `tx_id: TransactionId` - Transaction ID
- `key: impl AsRef<[u8]>` - The key to store
- `value: impl AsRef<[u8]>` - The value to store

**Example:**
```rust
let tx_id = db.begin_transaction()?;
db.put_tx(tx_id, "user:123", b"John Doe")?;
db.put_tx(tx_id, "user:456", b"Jane Smith")?;
```

#### `Database::get_tx(tx_id, key) -> Result<Option<Vec<u8>>>`

Retrieves a value by its key within a transaction context.

**Parameters:**
- `tx_id: TransactionId` - Transaction ID
- `key: impl AsRef<[u8]>` - The key to retrieve

**Example:**
```rust
let value = db.get_tx(tx_id, "user:123")?;
```

#### `Database::delete_tx(tx_id, key) -> Result<bool>`

Deletes a key-value pair within a transaction.

**Parameters:**
- `tx_id: TransactionId` - Transaction ID  
- `key: impl AsRef<[u8]>` - The key to delete

**Example:**
```rust
db.delete_tx(tx_id, "user:123")?;
```

#### `Database::commit_transaction(tx_id) -> Result<()>`

Commits a transaction, making all changes permanent.

**Parameters:**
- `tx_id: TransactionId` - Transaction ID to commit

**Example:**
```rust
db.commit_transaction(tx_id)?;
println!("Transaction committed successfully");
```

#### `Database::rollback_transaction(tx_id) -> Result<()>`

Rolls back a transaction, discarding all changes.

**Parameters:**
- `tx_id: TransactionId` - Transaction ID to rollback

**Example:**
```rust
match db.commit_transaction(tx_id) {
    Ok(_) => println!("Transaction committed"),
    Err(e) => {
        db.rollback_transaction(tx_id)?;
        println!("Transaction rolled back due to error: {}", e);
    }
}
```

## Configuration API

### LightningDbConfig

Configuration struct for database initialization.

```rust
pub struct LightningDbConfig {
    /// Cache size in bytes (default: 64MB)
    pub cache_size: usize,
    
    /// Enable compression (default: true)
    pub compression_enabled: bool,
    
    /// Use optimized page manager (default: true)
    pub use_optimized_page_manager: bool,
    
    /// Use improved WAL (default: true)
    pub use_improved_wal: bool,
    
    /// WAL sync mode (default: Async)
    pub wal_sync_mode: WalSyncMode,
    
    /// Maximum concurrent transactions (default: 1000)
    pub max_concurrent_transactions: usize,
    
    /// Enable prefetching (default: true)
    pub prefetch_enabled: bool,
    
    /// Memory mapping configuration
    pub mmap_config: Option<MmapConfig>,
}
```

#### Default Configuration

```rust
let config = LightningDbConfig::default();
// Equivalent to:
let config = LightningDbConfig {
    cache_size: 64 * 1024 * 1024, // 64MB
    compression_enabled: true,
    use_optimized_page_manager: true,
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Async,
    max_concurrent_transactions: 1000,
    prefetch_enabled: true,
    mmap_config: None,
};
```

#### High-Performance Configuration

```rust
let config = LightningDbConfig {
    cache_size: 1024 * 1024 * 1024, // 1GB
    compression_enabled: true,
    use_optimized_page_manager: true,
    use_improved_wal: true,
    wal_sync_mode: WalSyncMode::Async,
    max_concurrent_transactions: 10000,
    prefetch_enabled: true,
    mmap_config: Some(MmapConfig {
        enable_huge_pages: true,
        enable_prefault: true,
        enable_async_msync: true,
        max_mapped_regions: 64,
        region_size: 64 * 1024 * 1024, // 64MB regions
        ..Default::default()
    }),
};
```

### WalSyncMode

Write-Ahead Log synchronization modes:

```rust
pub enum WalSyncMode {
    /// Synchronous writes (safest, slowest)
    Sync,
    /// Asynchronous writes (faster, small risk)
    Async,
    /// No explicit sync (fastest, higher risk)
    None,
}
```

### MmapConfig

Memory mapping configuration for advanced performance tuning:

```rust
pub struct MmapConfig {
    /// Enable huge pages support
    pub enable_huge_pages: bool,
    
    /// Enable page prefaulting
    pub enable_prefault: bool,
    
    /// Enable asynchronous msync
    pub enable_async_msync: bool,
    
    /// Maximum number of mapped regions
    pub max_mapped_regions: usize,
    
    /// Size of each mapped region
    pub region_size: usize,
    
    /// Use madvise for access patterns
    pub use_madvise: bool,
}
```

## Error Handling

### Error Types

Lightning DB uses a comprehensive error system:

```rust
#[derive(Debug, thiserror::Error)]
pub enum LightningDbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    #[error("Lock timeout: {0}")]
    LockTimeout(String),
    
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}
```

### Error Handling Patterns

#### Basic Error Handling

```rust
use lightning_db::{Database, LightningDbError};

fn database_operation() -> Result<(), LightningDbError> {
    let db = Database::create("./db", Default::default())?;
    db.put("key", "value")?;
    Ok(())
}

match database_operation() {
    Ok(_) => println!("Success"),
    Err(e) => println!("Error: {}", e),
}
```

#### Specific Error Handling

```rust
use lightning_db::LightningDbError;

match db.get("key") {
    Ok(Some(value)) => println!("Found: {:?}", value),
    Ok(None) => println!("Key not found"),
    Err(LightningDbError::Io(io_err)) => {
        println!("IO error: {}", io_err);
    }
    Err(LightningDbError::Corruption(msg)) => {
        println!("Database corruption: {}", msg);
        // Handle corruption recovery
    }
    Err(e) => println!("Other error: {}", e),
}
```

#### Transaction Error Handling

```rust
fn safe_transaction(db: &Database) -> Result<(), LightningDbError> {
    let tx_id = db.begin_transaction()?;
    
    // Ensure rollback on any error
    let result = (|| {
        db.put_tx(tx_id, "key1", "value1")?;
        db.put_tx(tx_id, "key2", "value2")?;
        db.delete_tx(tx_id, "old_key")?;
        Ok(())
    })();
    
    match result {
        Ok(_) => {
            db.commit_transaction(tx_id)?;
            Ok(())
        }
        Err(e) => {
            db.rollback_transaction(tx_id)?;
            Err(e)
        }
    }
}
```

## Examples

### Complete CRUD Application

```rust
use lightning_db::{Database, LightningDbConfig, LightningDbError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct User {
    id: u64,
    name: String,
    email: String,
}

struct UserRepository {
    db: Database,
}

impl UserRepository {
    fn new(db_path: &str) -> Result<Self, LightningDbError> {
        let config = LightningDbConfig {
            cache_size: 100 * 1024 * 1024, // 100MB
            compression_enabled: true,
            ..Default::default()
        };
        
        let db = Database::create(db_path, config)?;
        Ok(Self { db })
    }
    
    fn save_user(&self, user: &User) -> Result<(), LightningDbError> {
        let key = format!("user:{}", user.id);
        let value = serde_json::to_vec(user).map_err(|e| {
            LightningDbError::Serialization(e.to_string())
        })?;
        
        self.db.put(key.as_bytes(), &value)
    }
    
    fn get_user(&self, id: u64) -> Result<Option<User>, LightningDbError> {
        let key = format!("user:{}", id);
        
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let user: User = serde_json::from_slice(&data).map_err(|e| {
                    LightningDbError::Serialization(e.to_string())
                })?;
                Ok(Some(user))
            }
            None => Ok(None),
        }
    }
    
    fn delete_user(&self, id: u64) -> Result<bool, LightningDbError> {
        let key = format!("user:{}", id);
        self.db.delete(key.as_bytes())
    }
    
    fn list_all_users(&self) -> Result<Vec<User>, LightningDbError> {
        let pairs = self.db.scan("user:")?;
        let mut users = Vec::new();
        
        for (_, data) in pairs {
            let user: User = serde_json::from_slice(&data).map_err(|e| {
                LightningDbError::Serialization(e.to_string())
            })?;
            users.push(user);
        }
        
        Ok(users)
    }
    
    fn transfer_users(&self, from_prefix: &str, to_prefix: &str) -> Result<(), LightningDbError> {
        let tx_id = self.db.begin_transaction()?;
        
        let result = (|| {
            let users = self.db.scan(from_prefix)?;
            
            for (old_key, data) in users {
                // Create new key with different prefix
                let old_key_str = String::from_utf8_lossy(&old_key);
                let new_key = old_key_str.replace(from_prefix, to_prefix);
                
                // Move data to new key
                self.db.put_tx(tx_id, new_key.as_bytes(), &data)?;
                self.db.delete_tx(tx_id, &old_key)?;
            }
            
            Ok(())
        })();
        
        match result {
            Ok(_) => {
                self.db.commit_transaction(tx_id)?;
                Ok(())
            }
            Err(e) => {
                self.db.rollback_transaction(tx_id)?;
                Err(e)
            }
        }
    }
}

fn main() -> Result<(), LightningDbError> {
    let repo = UserRepository::new("./user_db")?;
    
    // Create users
    let user1 = User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string() };
    let user2 = User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string() };
    
    repo.save_user(&user1)?;
    repo.save_user(&user2)?;
    
    // Read user
    if let Some(user) = repo.get_user(1)? {
        println!("Found user: {:?}", user);
    }
    
    // List all users
    let all_users = repo.list_all_users()?;
    println!("All users: {:?}", all_users);
    
    // Delete user
    if repo.delete_user(2)? {
        println!("User 2 deleted successfully");
    }
    
    Ok(())
}
```

### High-Performance Batch Processing

```rust
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn batch_insert_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = LightningDbConfig {
        cache_size: 512 * 1024 * 1024, // 512MB
        compression_enabled: true,
        use_optimized_page_manager: true,
        max_concurrent_transactions: 100,
        ..Default::default()
    };
    
    let db = Database::create("./batch_db", config)?;
    
    let start = Instant::now();
    let batch_size = 10000;
    let total_items = 1_000_000;
    
    for batch_start in (0..total_items).step_by(batch_size) {
        let tx_id = db.begin_transaction()?;
        
        for i in batch_start..std::cmp::min(batch_start + batch_size, total_items) {
            let key = format!("item:{:08}", i);
            let value = format!("value_for_item_{}", i);
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        
        db.commit_transaction(tx_id)?;
        
        if batch_start % (batch_size * 10) == 0 {
            println!("Processed {} items", batch_start + batch_size);
        }
    }
    
    let duration = start.elapsed();
    println!("Inserted {} items in {:?}", total_items, duration);
    println!("Rate: {:.2} items/sec", total_items as f64 / duration.as_secs_f64());
    
    Ok(())
}
```

### Concurrent Access Example

```rust
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn concurrent_access_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB
        max_concurrent_transactions: 1000,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create("./concurrent_db", config)?);
    let num_threads = 8;
    let operations_per_thread = 10000;
    
    let start = Instant::now();
    
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..operations_per_thread {
                    let key = format!("thread_{}:item_{}", thread_id, i);
                    let value = format!("data_from_thread_{}_item_{}", thread_id, i);
                    
                    if let Err(e) = db.put(key.as_bytes(), value.as_bytes()) {
                        eprintln!("Error in thread {}: {}", thread_id, e);
                    }
                }
            })
        })
        .collect();
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = num_threads * operations_per_thread;
    
    println!("Completed {} operations across {} threads in {:?}", 
             total_ops, num_threads, duration);
    println!("Rate: {:.2} ops/sec", total_ops as f64 / duration.as_secs_f64());
    
    // Verify data integrity
    let mut verified = 0;
    for thread_id in 0..num_threads {
        for i in 0..operations_per_thread {
            let key = format!("thread_{}:item_{}", thread_id, i);
            if db.get(key.as_bytes())?.is_some() {
                verified += 1;
            }
        }
    }
    
    println!("Verified {} out of {} operations", verified, total_ops);
    
    Ok(())
}
```

## Performance Tips

1. **Use Transactions for Bulk Operations**: Group multiple operations in transactions for better performance.

2. **Configure Cache Size Appropriately**: Set cache size to ~25% of available memory for optimal performance.

3. **Enable Compression**: Compression reduces I/O and often improves performance despite CPU overhead.

4. **Use Async WAL Mode**: For applications that can tolerate small data loss risk in case of power failure.

5. **Batch Related Operations**: Group related puts/deletes in single transactions.

6. **Monitor Memory Usage**: Use appropriate cache sizes to avoid excessive memory usage.

7. **Regular Maintenance**: Perform periodic compaction and checkpointing for long-running applications.

For more examples and advanced usage patterns, see the [examples directory](../../examples/) in the repository.

## Generated Documentation

The complete API documentation is automatically generated from the source code:

```bash
cargo doc --all-features --no-deps --open
```

This will generate and open the complete API documentation in your browser.
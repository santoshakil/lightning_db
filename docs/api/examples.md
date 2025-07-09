# Lightning DB Examples

This document provides comprehensive examples demonstrating various Lightning DB features and use cases.

## Table of Contents

1. [Basic Operations](#basic-operations)
2. [Transaction Management](#transaction-management)
3. [Data Serialization](#data-serialization)
4. [Performance Optimization](#performance-optimization)
5. [Error Handling](#error-handling)
6. [Advanced Patterns](#advanced-patterns)

## Basic Operations

### Simple Key-Value Storage

```rust
use lightning_db::{Database, LightningDbConfig};

fn basic_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create database with default configuration
    let db = Database::create("./basic_db", LightningDbConfig::default())?;
    
    // Store data
    db.put("name", b"Lightning DB")?;
    db.put("version", b"0.1.0")?;
    db.put("language", b"Rust")?;
    
    // Retrieve data
    if let Some(value) = db.get("name")? {
        println!("Database name: {}", String::from_utf8_lossy(&value));
    }
    
    // Check if key exists
    match db.get("unknown_key")? {
        Some(value) => println!("Found: {:?}", value),
        None => println!("Key not found"),
    }
    
    // Delete data
    let existed = db.delete("version")?;
    println!("Version key deleted: {}", existed);
    
    Ok(())
}
```

### Prefix Scanning

```rust
use lightning_db::{Database, LightningDbConfig};

fn scanning_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./scan_db", LightningDbConfig::default())?;
    
    // Store multiple items with similar prefixes
    let users = [
        ("user:001", "Alice"),
        ("user:002", "Bob"),
        ("user:003", "Charlie"),
        ("config:theme", "dark"),
        ("config:lang", "en"),
    ];
    
    for (key, value) in users.iter() {
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Scan for all users
    let user_entries = db.scan("user:")?;
    println!("Users found:");
    for (key, value) in user_entries {
        let key_str = String::from_utf8_lossy(&key);
        let value_str = String::from_utf8_lossy(&value);
        println!("  {}: {}", key_str, value_str);
    }
    
    // Scan for configuration
    let config_entries = db.scan("config:")?;
    println!("Configuration:");
    for (key, value) in config_entries {
        let key_str = String::from_utf8_lossy(&key);
        let value_str = String::from_utf8_lossy(&value);
        println!("  {}: {}", key_str, value_str);
    }
    
    Ok(())
}
```

## Transaction Management

### Basic Transactions

```rust
use lightning_db::{Database, LightningDbConfig};

fn transaction_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./tx_db", LightningDbConfig::default())?;
    
    // Start a transaction
    let tx_id = db.begin_transaction()?;
    
    // Perform multiple operations
    db.put_tx(tx_id, "account:alice", b"1000")?;
    db.put_tx(tx_id, "account:bob", b"500")?;
    db.put_tx(tx_id, "last_update", b"2024-01-01")?;
    
    // Commit the transaction
    db.commit_transaction(tx_id)?;
    println!("Transaction committed successfully");
    
    // Verify data was saved
    if let Some(balance) = db.get("account:alice")? {
        println!("Alice's balance: {}", String::from_utf8_lossy(&balance));
    }
    
    Ok(())
}
```

### Rollback Example

```rust
use lightning_db::{Database, LightningDbConfig};

fn rollback_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./rollback_db", LightningDbConfig::default())?;
    
    // Set initial state
    db.put("counter", b"100")?;
    
    let tx_id = db.begin_transaction()?;
    
    // Simulate a series of operations that might fail
    db.put_tx(tx_id, "counter", b"150")?;
    db.put_tx(tx_id, "operation", b"increment")?;
    
    // Simulate an error condition
    let should_fail = true;
    
    if should_fail {
        db.rollback_transaction(tx_id)?;
        println!("Transaction rolled back due to error");
        
        // Verify original value is preserved
        if let Some(value) = db.get("counter")? {
            println!("Counter value after rollback: {}", String::from_utf8_lossy(&value));
        }
    } else {
        db.commit_transaction(tx_id)?;
        println!("Transaction committed");
    }
    
    Ok(())
}
```

### Money Transfer Example

```rust
use lightning_db::{Database, LightningDbConfig, LightningDbError};

fn transfer_money(
    db: &Database,
    from_account: &str,
    to_account: &str,
    amount: i32,
) -> Result<(), LightningDbError> {
    let tx_id = db.begin_transaction()?;
    
    // Get current balances
    let from_key = format!("account:{}", from_account);
    let to_key = format!("account:{}", to_account);
    
    let from_balance = match db.get_tx(tx_id, &from_key)? {
        Some(data) => String::from_utf8_lossy(&data).parse::<i32>().unwrap_or(0),
        None => return Err(LightningDbError::KeyNotFound(from_account.to_string())),
    };
    
    let to_balance = match db.get_tx(tx_id, &to_key)? {
        Some(data) => String::from_utf8_lossy(&data).parse::<i32>().unwrap_or(0),
        None => 0,
    };
    
    // Check sufficient funds
    if from_balance < amount {
        db.rollback_transaction(tx_id)?;
        return Err(LightningDbError::InvalidOperation(
            "Insufficient funds".to_string()
        ));
    }
    
    // Perform transfer
    let new_from_balance = from_balance - amount;
    let new_to_balance = to_balance + amount;
    
    db.put_tx(tx_id, &from_key, new_from_balance.to_string().as_bytes())?;
    db.put_tx(tx_id, &to_key, new_to_balance.to_string().as_bytes())?;
    
    // Log transaction
    let log_key = format!("transfer:{}:{}", chrono::Utc::now().timestamp(), tx_id);
    let log_value = format!("{}->{}:{}", from_account, to_account, amount);
    db.put_tx(tx_id, &log_key, log_value.as_bytes())?;
    
    db.commit_transaction(tx_id)?;
    Ok(())
}

fn money_transfer_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./transfer_db", LightningDbConfig::default())?;
    
    // Initialize accounts
    db.put("account:alice", b"1000")?;
    db.put("account:bob", b"500")?;
    
    // Transfer money
    match transfer_money(&db, "alice", "bob", 200) {
        Ok(_) => println!("Transfer successful"),
        Err(e) => println!("Transfer failed: {}", e),
    }
    
    // Check final balances
    if let Some(balance) = db.get("account:alice")? {
        println!("Alice's final balance: {}", String::from_utf8_lossy(&balance));
    }
    if let Some(balance) = db.get("account:bob")? {
        println!("Bob's final balance: {}", String::from_utf8_lossy(&balance));
    }
    
    Ok(())
}
```

## Data Serialization

### JSON Serialization

```rust
use lightning_db::{Database, LightningDbConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    id: u64,
    name: String,
    email: String,
    age: u32,
    is_active: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct Post {
    id: u64,
    author_id: u64,
    title: String,
    content: String,
    tags: Vec<String>,
    created_at: String,
}

fn json_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./json_db", LightningDbConfig::default())?;
    
    // Create and store users
    let users = vec![
        User {
            id: 1,
            name: "Alice Johnson".to_string(),
            email: "alice@example.com".to_string(),
            age: 28,
            is_active: true,
        },
        User {
            id: 2,
            name: "Bob Smith".to_string(),
            email: "bob@example.com".to_string(),
            age: 35,
            is_active: false,
        },
    ];
    
    for user in &users {
        let key = format!("user:{}", user.id);
        let value = serde_json::to_vec(user)?;
        db.put(&key, &value)?;
    }
    
    // Create and store posts
    let posts = vec![
        Post {
            id: 1,
            author_id: 1,
            title: "Hello World".to_string(),
            content: "This is my first post!".to_string(),
            tags: vec!["intro".to_string(), "first".to_string()],
            created_at: "2024-01-01T10:00:00Z".to_string(),
        },
        Post {
            id: 2,
            author_id: 1,
            title: "Lightning DB Tutorial".to_string(),
            content: "How to use Lightning DB effectively".to_string(),
            tags: vec!["tutorial".to_string(), "database".to_string()],
            created_at: "2024-01-02T14:30:00Z".to_string(),
        },
    ];
    
    for post in &posts {
        let key = format!("post:{}", post.id);
        let value = serde_json::to_vec(post)?;
        db.put(&key, &value)?;
    }
    
    // Retrieve and display users
    println!("Users:");
    let user_entries = db.scan("user:")?;
    for (key, value) in user_entries {
        let user: User = serde_json::from_slice(&value)?;
        println!("  {}: {} ({}) - Active: {}", 
                 String::from_utf8_lossy(&key), user.name, user.email, user.is_active);
    }
    
    // Retrieve and display posts
    println!("\nPosts:");
    let post_entries = db.scan("post:")?;
    for (key, value) in post_entries {
        let post: Post = serde_json::from_slice(&value)?;
        println!("  {}: \"{}\" by user {} - Tags: {:?}", 
                 String::from_utf8_lossy(&key), post.title, post.author_id, post.tags);
    }
    
    Ok(())
}
```

### Binary Serialization with Bincode

```rust
use lightning_db::{Database, LightningDbConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Metrics {
    timestamp: u64,
    cpu_usage: f64,
    memory_usage: f64,
    disk_io: u64,
    network_io: u64,
}

fn binary_serialization_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./binary_db", LightningDbConfig::default())?;
    
    // Generate sample metrics
    let metrics = vec![
        Metrics {
            timestamp: 1704067200, // 2024-01-01 00:00:00
            cpu_usage: 45.2,
            memory_usage: 67.8,
            disk_io: 1024000,
            network_io: 2048000,
        },
        Metrics {
            timestamp: 1704067260, // 2024-01-01 00:01:00
            cpu_usage: 52.1,
            memory_usage: 71.2,
            disk_io: 1536000,
            network_io: 1792000,
        },
    ];
    
    // Store metrics using bincode (more efficient than JSON)
    for metric in &metrics {
        let key = format!("metrics:{}", metric.timestamp);
        let value = bincode::serialize(metric)?;
        db.put(&key, &value)?;
    }
    
    // Retrieve and analyze metrics
    let stored_metrics = db.scan("metrics:")?;
    let mut total_cpu = 0.0;
    let mut count = 0;
    
    for (key, value) in stored_metrics {
        let metric: Metrics = bincode::deserialize(&value)?;
        total_cpu += metric.cpu_usage;
        count += 1;
        
        println!("Timestamp {}: CPU {:.1}%, Memory {:.1}%", 
                 metric.timestamp, metric.cpu_usage, metric.memory_usage);
    }
    
    if count > 0 {
        println!("Average CPU usage: {:.1}%", total_cpu / count as f64);
    }
    
    Ok(())
}
```

## Performance Optimization

### Batch Processing

```rust
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn batch_processing_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB cache
        compression_enabled: true,
        use_optimized_page_manager: true,
        max_concurrent_transactions: 100,
        ..Default::default()
    };
    
    let db = Database::create("./batch_db", config)?;
    
    let total_items = 100_000;
    let batch_size = 1_000;
    
    println!("Inserting {} items in batches of {}", total_items, batch_size);
    let start_time = Instant::now();
    
    for batch_start in (0..total_items).step_by(batch_size) {
        let tx_id = db.begin_transaction()?;
        
        for i in batch_start..std::cmp::min(batch_start + batch_size, total_items) {
            let key = format!("item:{:08}", i);
            let value = format!("data_for_item_{}_with_timestamp_{}", i, 
                              std::time::SystemTime::now()
                                  .duration_since(std::time::UNIX_EPOCH)?
                                  .as_millis());
            
            db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
        }
        
        db.commit_transaction(tx_id)?;
        
        // Progress reporting
        if batch_start % (batch_size * 10) == 0 {
            let elapsed = start_time.elapsed();
            let items_processed = batch_start + batch_size;
            let rate = items_processed as f64 / elapsed.as_secs_f64();
            println!("Processed {} items ({:.1} items/sec)", items_processed, rate);
        }
    }
    
    let total_time = start_time.elapsed();
    let final_rate = total_items as f64 / total_time.as_secs_f64();
    
    println!("Completed in {:?}", total_time);
    println!("Final rate: {:.1} items/sec", final_rate);
    
    // Verify a few random items
    println!("Verifying random items...");
    for i in [100, 50000, 99999] {
        let key = format!("item:{:08}", i);
        if let Some(value) = db.get(key.as_bytes())? {
            let value_str = String::from_utf8_lossy(&value);
            if value_str.contains(&format!("item_{}", i)) {
                println!("  Item {} verified", i);
            }
        }
    }
    
    Ok(())
}
```

### Cache Optimization

```rust
use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::time::Instant;

fn cache_optimization_example() -> Result<(), Box<dyn std::error::Error>> {
    // Test different cache sizes
    let cache_sizes = vec![
        16 * 1024 * 1024,  // 16MB
        64 * 1024 * 1024,  // 64MB
        256 * 1024 * 1024, // 256MB
    ];
    
    let data_size = 10_000;
    
    for cache_size in cache_sizes {
        println!("Testing with cache size: {}MB", cache_size / (1024 * 1024));
        
        let config = LightningDbConfig {
            cache_size,
            compression_enabled: true,
            ..Default::default()
        };
        
        let db_path = format!("./cache_test_{}", cache_size);
        let db = Database::create(&db_path, config)?;
        
        // Write phase
        let start_write = Instant::now();
        for i in 0..data_size {
            let key = format!("data:{:06}", i);
            let value = format!("value_{}_with_padding_{}", i, "x".repeat(100));
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        let write_time = start_write.elapsed();
        
        // Read phase (sequential)
        let start_read_seq = Instant::now();
        for i in 0..data_size {
            let key = format!("data:{:06}", i);
            let _ = db.get(key.as_bytes())?;
        }
        let read_seq_time = start_read_seq.elapsed();
        
        // Read phase (random)
        let start_read_rand = Instant::now();
        for i in 0..data_size {
            let random_id = (i * 7919) % data_size; // Simple pseudo-random
            let key = format!("data:{:06}", random_id);
            let _ = db.get(key.as_bytes())?;
        }
        let read_rand_time = start_read_rand.elapsed();
        
        println!("  Write time: {:?} ({:.1} ops/sec)", 
                 write_time, data_size as f64 / write_time.as_secs_f64());
        println!("  Sequential read: {:?} ({:.1} ops/sec)", 
                 read_seq_time, data_size as f64 / read_seq_time.as_secs_f64());
        println!("  Random read: {:?} ({:.1} ops/sec)", 
                 read_rand_time, data_size as f64 / read_rand_time.as_secs_f64());
        println!();
        
        // Cleanup
        std::fs::remove_dir_all(&db_path)?;
    }
    
    Ok(())
}
```

## Error Handling

### Comprehensive Error Handling

```rust
use lightning_db::{Database, LightningDbConfig, LightningDbError};

fn error_handling_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./error_test_db", LightningDbConfig::default())?;
    
    // Example 1: Handle key not found
    match db.get("nonexistent_key") {
        Ok(Some(value)) => println!("Found: {:?}", value),
        Ok(None) => println!("Key not found (this is normal)"),
        Err(e) => println!("Unexpected error: {}", e),
    }
    
    // Example 2: Handle transaction errors
    let result = simulate_transaction_with_error(&db);
    match result {
        Ok(_) => println!("Transaction completed successfully"),
        Err(LightningDbError::InvalidOperation(msg)) => {
            println!("Business logic error: {}", msg);
        },
        Err(LightningDbError::Transaction(msg)) => {
            println!("Transaction system error: {}", msg);
        },
        Err(e) => println!("Other error: {}", e),
    }
    
    // Example 3: Retry on transient errors
    match retry_operation(&db, "retry_key", "retry_value", 3) {
        Ok(_) => println!("Operation succeeded after retries"),
        Err(e) => println!("Operation failed after all retries: {}", e),
    }
    
    Ok(())
}

fn simulate_transaction_with_error(db: &Database) -> Result<(), LightningDbError> {
    let tx_id = db.begin_transaction()?;
    
    // Simulate some operations
    db.put_tx(tx_id, "temp_key1", b"temp_value1")?;
    db.put_tx(tx_id, "temp_key2", b"temp_value2")?;
    
    // Simulate a business logic error
    let should_fail = true;
    if should_fail {
        db.rollback_transaction(tx_id)?;
        return Err(LightningDbError::InvalidOperation(
            "Simulated business logic failure".to_string()
        ));
    }
    
    db.commit_transaction(tx_id)?;
    Ok(())
}

fn retry_operation(
    db: &Database, 
    key: &str, 
    value: &str, 
    max_attempts: u32
) -> Result<(), LightningDbError> {
    let mut attempts = 0;
    
    loop {
        attempts += 1;
        
        match db.put(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                println!("Operation succeeded on attempt {}", attempts);
                return Ok(());
            }
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(e);
                }
                
                println!("Attempt {} failed: {}. Retrying...", attempts, e);
                std::thread::sleep(std::time::Duration::from_millis(100 * attempts as u64));
            }
        }
    }
}
```

### Recovery Patterns

```rust
use lightning_db::{Database, LightningDbConfig, LightningDbError};
use std::path::Path;

fn recovery_example() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "./recovery_db";
    
    // Attempt to open existing database, create if it doesn't exist
    let db = match Database::open(db_path) {
        Ok(db) => {
            println!("Opened existing database");
            db
        }
        Err(LightningDbError::Io(_)) => {
            println!("Database doesn't exist, creating new one");
            Database::create(db_path, LightningDbConfig::default())?
        }
        Err(e) => {
            println!("Error opening database: {}. Attempting recovery...", e);
            
            // Try to backup corrupted data
            if Path::new(db_path).exists() {
                let backup_path = format!("{}_corrupted_{}", db_path, 
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs());
                std::fs::rename(db_path, backup_path)?;
                println!("Corrupted database backed up");
            }
            
            // Create fresh database
            Database::create(db_path, LightningDbConfig::default())?
        }
    };
    
    // Verify database is working
    db.put("health_check", b"ok")?;
    match db.get("health_check")? {
        Some(value) if value == b"ok" => println!("Database health check passed"),
        _ => println!("Database health check failed"),
    }
    
    Ok(())
}
```

## Advanced Patterns

### Repository Pattern with Generic Types

```rust
use lightning_db::{Database, LightningDbError};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

trait Identifiable {
    fn id(&self) -> String;
}

trait Repository<T: Identifiable + Serialize + for<'de> Deserialize<'de>> {
    fn save(&self, entity: &T) -> Result<(), LightningDbError>;
    fn find_by_id(&self, id: &str) -> Result<Option<T>, LightningDbError>;
    fn find_all(&self) -> Result<Vec<T>, LightningDbError>;
    fn delete(&self, id: &str) -> Result<bool, LightningDbError>;
}

struct GenericRepository<T> {
    db: Database,
    prefix: String,
    _phantom: PhantomData<T>,
}

impl<T> GenericRepository<T>
where
    T: Identifiable + Serialize + for<'de> Deserialize<'de>,
{
    fn new(db: Database, prefix: &str) -> Self {
        Self {
            db,
            prefix: prefix.to_string(),
            _phantom: PhantomData,
        }
    }
    
    fn make_key(&self, id: &str) -> String {
        format!("{}:{}", self.prefix, id)
    }
}

impl<T> Repository<T> for GenericRepository<T>
where
    T: Identifiable + Serialize + for<'de> Deserialize<'de>,
{
    fn save(&self, entity: &T) -> Result<(), LightningDbError> {
        let key = self.make_key(&entity.id());
        let value = serde_json::to_vec(entity).map_err(|e| {
            LightningDbError::Serialization(e.to_string())
        })?;
        self.db.put(key.as_bytes(), &value)
    }
    
    fn find_by_id(&self, id: &str) -> Result<Option<T>, LightningDbError> {
        let key = self.make_key(id);
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let entity: T = serde_json::from_slice(&data).map_err(|e| {
                    LightningDbError::Serialization(e.to_string())
                })?;
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }
    
    fn find_all(&self) -> Result<Vec<T>, LightningDbError> {
        let prefix = format!("{}:", self.prefix);
        let entries = self.db.scan(prefix.as_bytes())?;
        let mut results = Vec::new();
        
        for (_, data) in entries {
            let entity: T = serde_json::from_slice(&data).map_err(|e| {
                LightningDbError::Serialization(e.to_string())
            })?;
            results.push(entity);
        }
        
        Ok(results)
    }
    
    fn delete(&self, id: &str) -> Result<bool, LightningDbError> {
        let key = self.make_key(id);
        self.db.delete(key.as_bytes())
    }
}

// Example usage
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Product {
    id: String,
    name: String,
    price: f64,
    category: String,
}

impl Identifiable for Product {
    fn id(&self) -> String {
        self.id.clone()
    }
}

fn repository_pattern_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./repository_db", LightningDbConfig::default())?;
    let product_repo = GenericRepository::new(db, "product");
    
    // Create products
    let products = vec![
        Product {
            id: "1".to_string(),
            name: "Laptop".to_string(),
            price: 999.99,
            category: "Electronics".to_string(),
        },
        Product {
            id: "2".to_string(),
            name: "Coffee Mug".to_string(),
            price: 12.99,
            category: "Kitchen".to_string(),
        },
    ];
    
    // Save products
    for product in &products {
        product_repo.save(product)?;
    }
    
    // Find by ID
    if let Some(laptop) = product_repo.find_by_id("1")? {
        println!("Found laptop: {} - ${}", laptop.name, laptop.price);
    }
    
    // Find all
    let all_products = product_repo.find_all()?;
    println!("All products:");
    for product in all_products {
        println!("  {}: {} (${}) - {}", product.id, product.name, product.price, product.category);
    }
    
    // Delete
    if product_repo.delete("2")? {
        println!("Coffee mug deleted");
    }
    
    Ok(())
}
```

### Event Sourcing Pattern

```rust
use lightning_db::{Database, LightningDbConfig, LightningDbError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Event {
    id: String,
    aggregate_id: String,
    event_type: String,
    data: serde_json::Value,
    timestamp: u64,
    version: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BankAccount {
    id: String,
    balance: f64,
    version: u64,
}

struct EventStore {
    db: Database,
}

impl EventStore {
    fn new(db: Database) -> Self {
        Self { db }
    }
    
    fn append_event(&self, event: &Event) -> Result<(), LightningDbError> {
        let key = format!("event:{}:{:010}", event.aggregate_id, event.version);
        let value = serde_json::to_vec(event).map_err(|e| {
            LightningDbError::Serialization(e.to_string())
        })?;
        self.db.put(key.as_bytes(), &value)
    }
    
    fn get_events(&self, aggregate_id: &str) -> Result<Vec<Event>, LightningDbError> {
        let prefix = format!("event:{}:", aggregate_id);
        let entries = self.db.scan(prefix.as_bytes())?;
        let mut events = Vec::new();
        
        for (_, data) in entries {
            let event: Event = serde_json::from_slice(&data).map_err(|e| {
                LightningDbError::Serialization(e.to_string())
            })?;
            events.push(event);
        }
        
        // Sort by version to ensure correct order
        events.sort_by_key(|e| e.version);
        Ok(events)
    }
    
    fn create_snapshot(&self, aggregate_id: &str, state: &BankAccount) -> Result<(), LightningDbError> {
        let key = format!("snapshot:{}", aggregate_id);
        let value = serde_json::to_vec(state).map_err(|e| {
            LightningDbError::Serialization(e.to_string())
        })?;
        self.db.put(key.as_bytes(), &value)
    }
    
    fn get_snapshot(&self, aggregate_id: &str) -> Result<Option<BankAccount>, LightningDbError> {
        let key = format!("snapshot:{}", aggregate_id);
        match self.db.get(key.as_bytes())? {
            Some(data) => {
                let account: BankAccount = serde_json::from_slice(&data).map_err(|e| {
                    LightningDbError::Serialization(e.to_string())
                })?;
                Ok(Some(account))
            }
            None => Ok(None),
        }
    }
}

fn event_sourcing_example() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::create("./event_store_db", LightningDbConfig::default())?;
    let event_store = EventStore::new(db);
    
    let account_id = "account_123";
    
    // Create account
    let created_event = Event {
        id: "1".to_string(),
        aggregate_id: account_id.to_string(),
        event_type: "AccountCreated".to_string(),
        data: serde_json::json!({"initial_balance": 1000.0}),
        timestamp: 1704067200,
        version: 1,
    };
    event_store.append_event(&created_event)?;
    
    // Deposit money
    let deposit_event = Event {
        id: "2".to_string(),
        aggregate_id: account_id.to_string(),
        event_type: "MoneyDeposited".to_string(),
        data: serde_json::json!({"amount": 500.0}),
        timestamp: 1704067260,
        version: 2,
    };
    event_store.append_event(&deposit_event)?;
    
    // Withdraw money
    let withdraw_event = Event {
        id: "3".to_string(),
        aggregate_id: account_id.to_string(),
        event_type: "MoneyWithdrawn".to_string(),
        data: serde_json::json!({"amount": 200.0}),
        timestamp: 1704067320,
        version: 3,
    };
    event_store.append_event(&withdraw_event)?;
    
    // Rebuild state from events
    let events = event_store.get_events(account_id)?;
    let mut account = BankAccount {
        id: account_id.to_string(),
        balance: 0.0,
        version: 0,
    };
    
    for event in events {
        match event.event_type.as_str() {
            "AccountCreated" => {
                account.balance = event.data["initial_balance"].as_f64().unwrap_or(0.0);
            }
            "MoneyDeposited" => {
                account.balance += event.data["amount"].as_f64().unwrap_or(0.0);
            }
            "MoneyWithdrawn" => {
                account.balance -= event.data["amount"].as_f64().unwrap_or(0.0);
            }
            _ => {}
        }
        account.version = event.version;
    }
    
    println!("Final account state: {:?}", account);
    
    // Create snapshot for faster future rebuilds
    event_store.create_snapshot(account_id, &account)?;
    
    // Verify snapshot works
    if let Some(snapshot) = event_store.get_snapshot(account_id)? {
        println!("Snapshot retrieved: {:?}", snapshot);
    }
    
    Ok(())
}
```

This comprehensive examples document covers various usage patterns and demonstrates the flexibility and power of Lightning DB across different use cases.
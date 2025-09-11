use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::error::Error;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Lightning DB Basic Usage Example\n");
    
    // Create temporary directory for demo
    let dir = tempdir()?;
    
    // 1. Basic database creation
    basic_operations(&dir)?;
    
    // 2. Transactions
    transaction_example(&dir)?;
    
    // 3. Range queries
    range_query_example(&dir)?;
    
    // 4. Custom configuration
    custom_config_example(&dir)?;
    
    println!("\nAll examples completed successfully!");
    Ok(())
}

fn basic_operations(dir: &tempfile::TempDir) -> Result<(), Box<dyn Error>> {
    println!("=== Basic Operations ===");
    
    let db = Database::create(dir.path().join("basic"), LightningDbConfig::default())?;
    
    // Insert data
    db.put(b"key1", b"value1")?;
    db.put(b"key2", b"value2")?;
    db.put(b"key3", b"value3")?;
    println!("Inserted 3 key-value pairs");
    
    // Read data
    if let Some(value) = db.get(b"key1")? {
        println!("Retrieved: key1 = {}", String::from_utf8_lossy(&value));
    }
    
    // Update data
    db.put(b"key1", b"updated_value1")?;
    if let Some(value) = db.get(b"key1")? {
        println!("Updated: key1 = {}", String::from_utf8_lossy(&value));
    }
    
    // Delete data
    db.delete(b"key3")?;
    if db.get(b"key3")?.is_none() {
        println!("Deleted: key3");
    }
    
    Ok(())
}

fn transaction_example(dir: &tempfile::TempDir) -> Result<(), Box<dyn Error>> {
    println!("\n=== Transaction Example ===");
    
    let db = Database::create(dir.path().join("transactions"), LightningDbConfig::default())?;
    
    // Initialize accounts
    db.put(b"account:alice", b"1000")?;
    db.put(b"account:bob", b"500")?;
    println!("Initial balances: Alice=1000, Bob=500");
    
    // Transfer money in a transaction
    let tx = db.begin_transaction()?;
    
    // Read current balances
    let alice_data = db.get_tx(tx, b"account:alice")?.unwrap();
    let bob_data = db.get_tx(tx, b"account:bob")?.unwrap();
    
    let alice_balance = String::from_utf8_lossy(&alice_data);
    let bob_balance = String::from_utf8_lossy(&bob_data);
    
    let alice_amount: i32 = alice_balance.parse()?;
    let bob_amount: i32 = bob_balance.parse()?;
    
    // Transfer 200 from Alice to Bob
    let transfer = 200;
    db.put_tx(tx, b"account:alice", (alice_amount - transfer).to_string().as_bytes())?;
    db.put_tx(tx, b"account:bob", (bob_amount + transfer).to_string().as_bytes())?;
    
    // Commit transaction
    db.commit_transaction(tx)?;
    
    // Verify final balances
    let alice_final_data = db.get(b"account:alice")?.unwrap();
    let bob_final_data = db.get(b"account:bob")?.unwrap();
    let alice_final = String::from_utf8_lossy(&alice_final_data);
    let bob_final = String::from_utf8_lossy(&bob_final_data);
    println!("After transfer: Alice={}, Bob={}", alice_final, bob_final);
    
    Ok(())
}

fn range_query_example(dir: &tempfile::TempDir) -> Result<(), Box<dyn Error>> {
    println!("\n=== Range Query Example ===");
    
    let db = Database::create(dir.path().join("range"), LightningDbConfig::default())?;
    
    // Insert sorted data
    for i in 0..10 {
        let key = format!("item:{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    println!("Inserted 10 items (item:000 to item:009)");
    
    // Query range [item:003, item:007)
    let results = db.range(Some(b"item:003"), Some(b"item:007"))?;
    println!("\nRange query [item:003, item:007):");
    for (key, value) in results {
        println!("  {} = {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }
    
    // Query all items
    let all_items = db.range(None, None)?;
    println!("\nTotal items in database: {}", all_items.len());
    
    Ok(())
}

fn custom_config_example(dir: &tempfile::TempDir) -> Result<(), Box<dyn Error>> {
    println!("\n=== Custom Configuration Example ===");
    
    let mut config = LightningDbConfig::default();
    
    // Configure cache
    config.cache_size = 32 * 1024 * 1024; // 32MB cache
    config.prefetch_enabled = true;
    
    // Configure compression
    config.compression_enabled = true;
    config.compression_type = 2; // LZ4
    
    // Configure write-ahead log
    config.wal_sync_mode = WalSyncMode::Periodic { interval_ms: 100 };
    
    // Configure transactions
    config.max_active_transactions = 100;
    
    let db = Database::create(dir.path().join("custom"), config)?;
    
    // Test with compressed data
    let large_value = vec![42u8; 10000]; // 10KB of repeated data
    db.put(b"compressed_key", &large_value)?;
    
    if let Some(value) = db.get(b"compressed_key")? {
        println!("Stored and retrieved {} bytes (compressed internally)", value.len());
    }
    
    // Show configuration
    println!("\nConfiguration:");
    println!("  Cache: 32MB");
    println!("  Compression: LZ4");
    println!("  WAL sync: Periodic (100ms)");
    println!("  Max transactions: 100");
    
    Ok(())
}
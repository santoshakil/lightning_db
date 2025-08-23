//! End-to-End Database Operation Tests
//! 
//! Tests complete lifecycle operations and complex workflows

use super::{TestEnvironment, setup_test_data, verify_test_data, generate_workload_data};
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::collections::HashMap;
use rayon::prelude::*;

#[test]
fn test_complete_database_lifecycle() {
    let mut env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Test initial empty state
    assert!(db.get(b"nonexistent").unwrap().is_none());
    
    // Test basic CRUD operations
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap().unwrap(), b"value1");
    
    db.put(b"key1", b"updated_value1").unwrap(); // Update
    assert_eq!(db.get(b"key1").unwrap().unwrap(), b"updated_value1");
    
    db.delete(b"key1").unwrap();
    assert!(db.get(b"key1").unwrap().is_none());
    
    // Test batch operations
    let batch_data = generate_workload_data(1000);
    for (key, value) in &batch_data {
        db.put(key, value).unwrap();
    }
    
    // Verify batch data
    for (key, expected_value) in &batch_data {
        let actual_value = db.get(key).unwrap().unwrap();
        assert_eq!(actual_value, *expected_value);
    }
    
    // Test range operations (if available)
    let range_keys: Vec<_> = batch_data.iter()
        .take(10)
        .map(|(k, _)| k.clone())
        .collect();
    
    for key in &range_keys {
        assert!(db.get(key).unwrap().is_some());
    }
}

#[test]
fn test_transaction_workflows() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Test successful transaction
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"tx_key1", b"tx_value1").unwrap();
    db.put_tx(tx_id, b"tx_key2", b"tx_value2").unwrap();
    db.commit_transaction(tx_id).unwrap();
    
    assert_eq!(db.get(b"tx_key1").unwrap().unwrap(), b"tx_value1");
    assert_eq!(db.get(b"tx_key2").unwrap().unwrap(), b"tx_value2");
    
    // Test transaction rollback
    let tx_id = db.begin_transaction().unwrap();
    db.put_tx(tx_id, b"rollback_key1", b"rollback_value1").unwrap();
    db.put_tx(tx_id, b"rollback_key2", b"rollback_value2").unwrap();
    db.rollback_transaction(tx_id).unwrap();
    
    assert!(db.get(b"rollback_key1").unwrap().is_none());
    assert!(db.get(b"rollback_key2").unwrap().is_none());
    
    // Test nested transactions (if supported)
    let outer_tx = db.begin_transaction().unwrap();
    db.put_tx(outer_tx, b"outer_key", b"outer_value").unwrap();
    
    let inner_tx = db.begin_transaction().unwrap();
    db.put_tx(inner_tx, b"inner_key", b"inner_value").unwrap();
    db.commit_transaction(inner_tx).unwrap();
    db.commit_transaction(outer_tx).unwrap();
    
    assert_eq!(db.get(b"outer_key").unwrap().unwrap(), b"outer_value");
    assert_eq!(db.get(b"inner_key").unwrap().unwrap(), b"inner_value");
}

#[test]
fn test_complex_query_patterns() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup test data with structured keys
    let test_data = vec![
        ("user:1:name", "Alice"),
        ("user:1:email", "alice@example.com"),
        ("user:1:age", "25"),
        ("user:2:name", "Bob"),
        ("user:2:email", "bob@example.com"),
        ("user:2:age", "30"),
        ("product:1:name", "Laptop"),
        ("product:1:price", "1200"),
        ("product:2:name", "Mouse"),
        ("product:2:price", "25"),
    ];
    
    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Test prefix-based queries (simulated)
    let user_keys: Vec<_> = test_data.iter()
        .filter(|(k, _)| k.starts_with("user:"))
        .collect();
    
    for (key, expected_value) in user_keys {
        let actual_value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Test specific user data retrieval
    assert_eq!(db.get(b"user:1:name").unwrap().unwrap(), b"Alice");
    assert_eq!(db.get(b"user:2:email").unwrap().unwrap(), b"bob@example.com");
    
    // Test product data retrieval
    assert_eq!(db.get(b"product:1:price").unwrap().unwrap(), b"1200");
    assert_eq!(db.get(b"product:2:name").unwrap().unwrap(), b"Mouse");
}

#[test]
fn test_bulk_operations() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Test bulk insert
    let bulk_data = generate_workload_data(10000);
    
    // Batch insert using transactions
    let batch_size = 1000;
    for chunk in bulk_data.chunks(batch_size) {
        let tx_id = db.begin_transaction().unwrap();
        for (key, value) in chunk {
            db.put_tx(tx_id, key, value).unwrap();
        }
        db.commit_transaction(tx_id).unwrap();
    }
    
    // Verify all data was inserted
    for (key, expected_value) in &bulk_data {
        let actual_value = db.get(key).unwrap().unwrap();
        assert_eq!(actual_value, *expected_value);
    }
    
    // Test bulk update
    let tx_id = db.begin_transaction().unwrap();
    for (key, _) in bulk_data.iter().take(5000) {
        let new_value = format!("updated_{}", String::from_utf8_lossy(key));
        db.put_tx(tx_id, key, new_value.as_bytes()).unwrap();
    }
    db.commit_transaction(tx_id).unwrap();
    
    // Verify updates
    for (key, _) in bulk_data.iter().take(5000) {
        let expected_value = format!("updated_{}", String::from_utf8_lossy(key));
        let actual_value = db.get(key).unwrap().unwrap();
        assert_eq!(actual_value, expected_value.as_bytes());
    }
    
    // Test bulk delete
    let tx_id = db.begin_transaction().unwrap();
    for (key, _) in bulk_data.iter().take(2500) {
        db.delete_tx(tx_id, key).unwrap();
    }
    db.commit_transaction(tx_id).unwrap();
    
    // Verify deletions
    for (key, _) in bulk_data.iter().take(2500) {
        assert!(db.get(key).unwrap().is_none());
    }
}

#[test]
fn test_multi_table_simulation() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Simulate multiple tables using key prefixes
    let users = vec![
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com"),
    ];
    
    let orders = vec![
        (101, 1, "2024-01-15", 250.00),
        (102, 2, "2024-01-16", 150.00),
        (103, 1, "2024-01-17", 300.00),
    ];
    
    // Insert users
    for (id, name, email) in &users {
        db.put(format!("users:{}:name", id).as_bytes(), name.as_bytes()).unwrap();
        db.put(format!("users:{}:email", id).as_bytes(), email.as_bytes()).unwrap();
    }
    
    // Insert orders
    for (order_id, user_id, date, amount) in &orders {
        db.put(format!("orders:{}:user_id", order_id).as_bytes(), user_id.to_string().as_bytes()).unwrap();
        db.put(format!("orders:{}:date", order_id).as_bytes(), date.as_bytes()).unwrap();
        db.put(format!("orders:{}:amount", order_id).as_bytes(), amount.to_string().as_bytes()).unwrap();
    }
    
    // Test "join" operation - find orders for user 1
    let user_1_orders: Vec<_> = orders.iter()
        .filter(|(_, user_id, _, _)| *user_id == 1)
        .collect();
    
    for (order_id, _, _, _) in user_1_orders {
        let stored_user_id = db.get(format!("orders:{}:user_id", order_id).as_bytes()).unwrap().unwrap();
        assert_eq!(stored_user_id, b"1");
    }
    
    // Test data consistency across "tables"
    for (order_id, user_id, _, _) in &orders {
        let stored_user_id = String::from_utf8(
            db.get(format!("orders:{}:user_id", order_id).as_bytes()).unwrap().unwrap()
        ).unwrap().parse::<i32>().unwrap();
        
        let user_name = db.get(format!("users:{}:name", stored_user_id).as_bytes()).unwrap();
        assert!(user_name.is_some());
    }
}

#[test]
fn test_concurrent_transactions() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Setup initial data
    for i in 0..100 {
        db.put(format!("counter:{}", i).as_bytes(), b"0").unwrap();
    }
    
    // Run concurrent transactions
    let handles: Vec<_> = (0..10).map(|thread_id| {
        let db = db.clone();
        std::thread::spawn(move || {
            for i in 0..10 {
                let tx_id = db.begin_transaction().unwrap();
                
                // Read current value
                let key = format!("counter:{}", thread_id * 10 + i);
                let current = db.get_tx(tx_id, key.as_bytes()).unwrap()
                    .map(|v| String::from_utf8(v).unwrap().parse::<i32>().unwrap())
                    .unwrap_or(0);
                
                // Increment and write back
                let new_value = current + 1;
                db.put_tx(tx_id, key.as_bytes(), new_value.to_string().as_bytes()).unwrap();
                
                db.commit_transaction(tx_id).unwrap();
            }
        })
    }).collect();
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Verify final state
    for i in 0..100 {
        let value = db.get(format!("counter:{}", i).as_bytes()).unwrap().unwrap();
        let count = String::from_utf8(value).unwrap().parse::<i32>().unwrap();
        assert_eq!(count, 1);
    }
}

#[test]
fn test_database_reopen() {
    let mut env = TestEnvironment::new().expect("Failed to create test environment");
    let db_path = env.db_path.clone();
    let config = env.config.clone();
    
    // Insert test data
    setup_test_data(&env.db()).unwrap();
    
    // Close database (drop reference)
    drop(env.db);
    
    // Reopen database
    let reopened_db = Database::open(db_path, config).unwrap();
    
    // Verify data persistence
    assert!(verify_test_data(&reopened_db).unwrap());
    
    // Test continued operations
    reopened_db.put(b"new_key_after_reopen", b"new_value").unwrap();
    assert_eq!(reopened_db.get(b"new_key_after_reopen").unwrap().unwrap(), b"new_value");
}

#[test]
fn test_large_value_handling() {
    let env = TestEnvironment::new().expect("Failed to create test environment");
    let db = env.db();
    
    // Test various value sizes
    let sizes = vec![1, 100, 1_000, 10_000, 100_000, 1_000_000];
    
    for size in sizes {
        let key = format!("large_value_{}", size);
        let value = vec![b'x'; size];
        
        db.put(key.as_bytes(), &value).unwrap();
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        
        assert_eq!(retrieved.len(), size);
        assert_eq!(retrieved, value);
    }
    
    // Test very large transaction
    let tx_id = db.begin_transaction().unwrap();
    for i in 0..100 {
        let key = format!("tx_large_{}", i);
        let value = vec![b'y'; 10_000];
        db.put_tx(tx_id, key.as_bytes(), &value).unwrap();
    }
    db.commit_transaction(tx_id).unwrap();
    
    // Verify large transaction data
    for i in 0..100 {
        let key = format!("tx_large_{}", i);
        let retrieved = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(retrieved.len(), 10_000);
        assert!(retrieved.iter().all(|&b| b == b'y'));
    }
}
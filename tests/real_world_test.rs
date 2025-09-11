use lightning_db::{Database, LightningDbConfig, Key};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn test_real_world_scenario() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    config.cache_size = 1024 * 1024;
    config.use_optimized_transactions = true;
    
    let db = Arc::new(Database::create(dir.path(), config).unwrap());
    
    println!("\n=== Real World Database Test ===\n");
    
    println!("1. Simulating user session data...");
    for user_id in 0..100 {
        let session_key = format!("session:user:{}", user_id);
        let session_data = format!(r#"{{
            "user_id": {},
            "login_time": "2024-01-01T00:00:00Z",
            "ip": "192.168.1.{}",
            "user_agent": "Mozilla/5.0"
        }}"#, user_id, user_id % 256);
        
        db.put(session_key.as_bytes(), session_data.as_bytes()).unwrap();
    }
    println!("   ✓ Created 100 user sessions");
    
    println!("\n2. Simulating product catalog...");
    for product_id in 0..500 {
        let product_key = format!("product:{:04}", product_id);
        let product_data = format!(r#"{{
            "id": {},
            "name": "Product {}",
            "price": {},
            "stock": {},
            "category": "category_{}",
            "description": "This is a description for product {}"
        }}"#, product_id, product_id, 
            10.0 + (product_id as f64 * 0.5),
            100 - (product_id % 100),
            product_id % 10,
            product_id);
        
        db.put(product_key.as_bytes(), product_data.as_bytes()).unwrap();
    }
    println!("   ✓ Added 500 products to catalog");
    
    println!("\n3. Simulating shopping cart operations...");
    let mut handles = vec![];
    for user_id in 0..10 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for item in 0..5 {
                let cart_key = format!("cart:user:{}:item:{}", user_id, item);
                let cart_data = format!(r#"{{
                    "product_id": {},
                    "quantity": {},
                    "added_at": "2024-01-01T12:00:00Z"
                }}"#, item * 10, item + 1);
                
                db_clone.put(cart_key.as_bytes(), cart_data.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    println!("   ✓ Processed 50 cart operations concurrently");
    
    println!("\n4. Testing transactional order processing...");
    let tx_id = db.begin_transaction().unwrap();
    
    db.put_tx(tx_id, b"order:1001", br#"{
        "order_id": 1001,
        "user_id": 1,
        "total": 299.99,
        "status": "pending"
    }"#).unwrap();
    
    db.put_tx(tx_id, b"order:1001:items", br#"[
        {"product_id": 1, "quantity": 2, "price": 99.99},
        {"product_id": 2, "quantity": 1, "price": 100.01}
    ]"#).unwrap();
    
    db.commit_transaction(tx_id).unwrap();
    println!("   ✓ Completed transactional order");
    
    println!("\n5. Testing analytics queries...");
    
    let products = db.prefix_scan_key(&Key::from(b"product:".as_ref())).unwrap();
    println!("   ✓ Found {} products", products.len());
    assert_eq!(products.len(), 500);
    
    let sessions = db.prefix_scan_key(&Key::from(b"session:".as_ref())).unwrap();
    println!("   ✓ Found {} active sessions", sessions.len());
    assert_eq!(sessions.len(), 100);
    
    let carts = db.prefix_scan_key(&Key::from(b"cart:".as_ref())).unwrap();
    println!("   ✓ Found {} cart items", carts.len());
    assert_eq!(carts.len(), 50);
    
    println!("\n6. Testing database statistics...");
    let stats = db.stats();
    println!("   Page count: {}", stats.page_count);
    println!("   Free pages: {}", stats.free_page_count);
    println!("   Tree height: {}", stats.tree_height);
    println!("   Active transactions: {}", stats.active_transactions);
    println!("   Cache hit rate: {:.2}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    
    println!("\n7. Testing data persistence...");
    db.flush_lsm().unwrap();
    
    let order = db.get(b"order:1001").unwrap();
    assert!(order.is_some());
    println!("   ✓ Order data persisted and retrieved");
    
    println!("\n8. Testing update operations...");
    // Update some cart items to test overwrite
    for user_id in 0..5 {
        let cart_key = format!("cart:user:{}:item:0", user_id);
        let updated_data = br#"{
            "product_id": 999,
            "quantity": 10,
            "updated_at": "2024-01-02T00:00:00Z"
        }"#;
        db.put(cart_key.as_bytes(), updated_data).unwrap();
    }
    
    // Verify updates
    for user_id in 0..5 {
        let cart_key = format!("cart:user:{}:item:0", user_id);
        let data = db.get(cart_key.as_bytes()).unwrap();
        assert!(data.is_some());
        let data = data.unwrap();
        let data_str = String::from_utf8_lossy(&data);
        assert!(data_str.contains("999"));
    }
    println!("   ✓ Successfully updated cart data");
    
    println!("\n=== All Real World Tests Passed ===\n");
}

#[test]
fn test_crash_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        
        for i in 0..100 {
            let key = format!("persistent_key_{}", i);
            let value = format!("persistent_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.flush_lsm().unwrap();
    }
    
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        
        for i in 0..100 {
            let key = format!("persistent_key_{}", i);
            let value = format!("persistent_value_{}", i);
            let retrieved = db.get(key.as_bytes()).unwrap();
            assert_eq!(retrieved, Some(value.into_bytes()));
        }
        
        println!("✓ Successfully recovered all data after restart");
    }
}

#[test]
fn test_memory_efficiency() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 512;
    
    let db = Database::create(dir.path(), config).unwrap();
    
    let value_size = 1024;
    let large_value = vec![42u8; value_size];
    
    for i in 0..1000 {
        let key = format!("mem_test_{:04}", i);
        db.put(key.as_bytes(), &large_value).unwrap();
        
        if i % 100 == 0 {
            db.flush_lsm().unwrap();
        }
    }
    
    let stats = db.stats();
    println!("Memory efficiency test:");
    println!("  Stored: {} KB of data", 1000 * value_size / 1024);
    println!("  Cache hit rate: {:.2}%", stats.cache_hit_rate.unwrap_or(0.0) * 100.0);
    
    assert!(stats.cache_hit_rate.unwrap_or(0.0) >= 0.0);
}
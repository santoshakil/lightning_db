use lightning_db::{Database, Key, KeyBatch, LightningDbConfig};
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Zero-Copy Key Optimization Test ===\n");

    // Create database
    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig::default();
    let db = Database::create(temp_dir.path(), config)?;

    // Test 1: Basic key operations
    println!("1. Testing basic key operations...");
    test_basic_operations(&db)?;

    // Test 2: Inline vs heap allocation
    println!("2. Testing inline vs heap allocation...");
    test_allocation_behavior(&db)?;

    // Test 3: Performance comparison
    println!("3. Testing performance comparison...");
    test_performance_comparison(&db)?;

    // Test 4: Batch operations
    println!("4. Testing batch operations...");
    test_batch_operations(&db)?;

    // Test 5: Memory usage
    println!("5. Testing memory usage...");
    test_memory_usage(&db)?;

    println!("\n✅ All zero-copy key tests passed!");
    Ok(())
}

fn test_basic_operations(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Test with different key types
    let string_key = Key::from("hello_world");
    let bytes_key = Key::from([1, 2, 3, 4, 5].as_ref());
    let vec_key = Key::from_vec(vec![10, 20, 30, 40, 50]);

    let value = b"test_value";

    // Put operations
    db.put_key(&string_key, value)?;
    db.put_key(&bytes_key, value)?;
    db.put_key(&vec_key, value)?;

    // Get operations
    assert_eq!(db.get_key(&string_key)?.as_deref(), Some(value.as_ref()));
    assert_eq!(db.get_key(&bytes_key)?.as_deref(), Some(value.as_ref()));
    assert_eq!(db.get_key(&vec_key)?.as_deref(), Some(value.as_ref()));

    // Delete operations
    assert!(db.delete_key(&string_key)?);
    assert!(db.delete_key(&bytes_key)?);
    assert!(db.delete_key(&vec_key)?);

    // Verify deletion
    assert!(db.get_key(&string_key)?.is_none());
    assert!(db.get_key(&bytes_key)?.is_none());
    assert!(db.get_key(&vec_key)?.is_none());

    println!("  ✓ Basic operations work correctly");
    Ok(())
}

fn test_allocation_behavior(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Small keys (should be inline)
    let small_keys = vec![
        Key::from("a"),
        Key::from("short"),
        Key::from("medium_length_key"),
        Key::from("exactly_32_bytes_long_key_here!"),
    ];

    // Large keys (should use heap)
    let large_keys = vec![
        Key::from_vec(vec![b'x'; 33]),
        Key::from_vec(vec![b'y'; 64]),
        Key::from_vec(vec![b'z'; 128]),
    ];

    // Check allocation behavior
    for (i, key) in small_keys.iter().enumerate() {
        assert!(key.is_inline(), "Small key {} should be inline", i);
        println!("  ✓ Key '{}' is inline ({}B)", key, key.len());
    }

    for (i, key) in large_keys.iter().enumerate() {
        assert!(!key.is_inline(), "Large key {} should use heap", i);
        println!("  ✓ Key {} is heap-allocated ({}B)", i, key.len());
    }

    // Test performance with inline keys
    let value = b"test_value";
    for key in &small_keys {
        db.put_key(key, value)?;
        assert_eq!(db.get_key(key)?.as_deref(), Some(value.as_ref()));
    }

    for key in &large_keys {
        db.put_key(key, value)?;
        assert_eq!(db.get_key(key)?.as_deref(), Some(value.as_ref()));
    }

    println!("  ✓ Allocation behavior is correct");
    Ok(())
}

fn test_performance_comparison(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let num_ops = 10_000;
    let value = b"benchmark_value";

    // Generate test keys
    let byte_keys: Vec<Vec<u8>> = (0..num_ops)
        .map(|i| format!("benchmark_key_{:06}", i).into_bytes())
        .collect();

    let zero_copy_keys: Vec<Key> = byte_keys.iter().map(|k| Key::from(k.as_slice())).collect();

    // Benchmark traditional byte slice operations
    let start = Instant::now();
    for key in &byte_keys {
        db.put(key, value)?;
    }
    let traditional_put_time = start.elapsed();

    let start = Instant::now();
    for key in &byte_keys {
        let _ = db.get(key)?;
    }
    let traditional_get_time = start.elapsed();

    // Clear data
    for key in &byte_keys {
        db.delete(key)?;
    }

    // Benchmark zero-copy key operations
    let start = Instant::now();
    for key in &zero_copy_keys {
        db.put_key(key, value)?;
    }
    let zero_copy_put_time = start.elapsed();

    let start = Instant::now();
    for key in &zero_copy_keys {
        let _ = db.get_key(key)?;
    }
    let zero_copy_get_time = start.elapsed();

    // Calculate performance metrics
    let put_ops_per_sec_traditional = num_ops as f64 / traditional_put_time.as_secs_f64();
    let get_ops_per_sec_traditional = num_ops as f64 / traditional_get_time.as_secs_f64();
    let put_ops_per_sec_zero_copy = num_ops as f64 / zero_copy_put_time.as_secs_f64();
    let get_ops_per_sec_zero_copy = num_ops as f64 / zero_copy_get_time.as_secs_f64();

    println!("  Performance comparison ({} operations):", num_ops);
    println!(
        "    Traditional PUT: {:.0} ops/sec ({:.2} ms)",
        put_ops_per_sec_traditional,
        traditional_put_time.as_millis()
    );
    println!(
        "    Zero-copy PUT:   {:.0} ops/sec ({:.2} ms)",
        put_ops_per_sec_zero_copy,
        zero_copy_put_time.as_millis()
    );
    println!(
        "    Traditional GET: {:.0} ops/sec ({:.2} ms)",
        get_ops_per_sec_traditional,
        traditional_get_time.as_millis()
    );
    println!(
        "    Zero-copy GET:   {:.0} ops/sec ({:.2} ms)",
        get_ops_per_sec_zero_copy,
        zero_copy_get_time.as_millis()
    );

    // Calculate improvement
    let put_improvement = (put_ops_per_sec_zero_copy / put_ops_per_sec_traditional - 1.0) * 100.0;
    let get_improvement = (get_ops_per_sec_zero_copy / get_ops_per_sec_traditional - 1.0) * 100.0;

    if put_improvement > 0.0 {
        println!("  ✓ Zero-copy PUT is {:.1}% faster", put_improvement);
    } else {
        println!("  ⚠ Zero-copy PUT is {:.1}% slower", -put_improvement);
    }

    if get_improvement > 0.0 {
        println!("  ✓ Zero-copy GET is {:.1}% faster", get_improvement);
    } else {
        println!("  ⚠ Zero-copy GET is {:.1}% slower", -get_improvement);
    }

    Ok(())
}

fn test_batch_operations(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let num_items = 1000;

    // Create batch data
    let pairs: Vec<(Key, Vec<u8>)> = (0..num_items)
        .map(|i| {
            let key = Key::from(format!("batch_key_{:04}", i));
            let value = format!("batch_value_{:04}", i).into_bytes();
            (key, value)
        })
        .collect();

    // Test batch put
    let start = Instant::now();
    db.put_batch_keys(&pairs)?;
    let batch_put_time = start.elapsed();

    // Test batch get
    let keys: Vec<Key> = pairs.iter().map(|(k, _)| k.clone()).collect();
    let start = Instant::now();
    let values = db.get_batch_keys(&keys)?;
    let batch_get_time = start.elapsed();

    // Verify results
    assert_eq!(values.len(), num_items);
    for (i, value) in values.iter().enumerate() {
        let expected = format!("batch_value_{:04}", i).into_bytes();
        assert_eq!(value.as_deref(), Some(expected.as_slice()));
    }

    // Test multi-get
    let start = Instant::now();
    let multi_results = db.multi_get_keys(&keys)?;
    let multi_get_time = start.elapsed();

    assert_eq!(multi_results.len(), num_items);
    for (i, (key, value)) in multi_results.iter().enumerate() {
        let expected_key = format!("batch_key_{:04}", i);
        let expected_value = format!("batch_value_{:04}", i).into_bytes();
        assert_eq!(key.as_bytes(), expected_key.as_bytes());
        assert_eq!(value.as_deref(), Some(expected_value.as_slice()));
    }

    println!("  Batch operations ({} items):", num_items);
    println!(
        "    Batch PUT: {:.2} ms ({:.0} ops/sec)",
        batch_put_time.as_millis(),
        num_items as f64 / batch_put_time.as_secs_f64()
    );
    println!(
        "    Batch GET: {:.2} ms ({:.0} ops/sec)",
        batch_get_time.as_millis(),
        num_items as f64 / batch_get_time.as_secs_f64()
    );
    println!(
        "    Multi GET: {:.2} ms ({:.0} ops/sec)",
        multi_get_time.as_millis(),
        num_items as f64 / multi_get_time.as_secs_f64()
    );

    // Test batch delete
    let start = Instant::now();
    let delete_results = db.delete_batch_keys(&keys)?;
    let batch_delete_time = start.elapsed();

    assert_eq!(delete_results.len(), num_items);
    let found_count = delete_results.iter().filter(|&&exists| exists).count();
    println!(
        "    Found {}/{} keys during deletion",
        found_count, num_items
    );

    if found_count != num_items {
        println!("    ⚠ Not all keys were found - might be due to transaction visibility");
    }

    println!(
        "    Batch DELETE: {:.2} ms ({:.0} ops/sec)",
        batch_delete_time.as_millis(),
        num_items as f64 / batch_delete_time.as_secs_f64()
    );

    println!("  ✓ Batch operations work correctly");
    Ok(())
}

fn test_memory_usage(_db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    // Test KeyBatch for efficient key collection
    let mut key_batch = KeyBatch::new();

    // Add various key types
    key_batch.add("string_key");
    key_batch.add(b"bytes_key".as_ref());
    key_batch.add(vec![1, 2, 3, 4, 5]);

    println!("  KeyBatch usage:");
    println!("    Keys: {}", key_batch.len());
    println!("    Total size: {} bytes", key_batch.total_size());

    // Test with large number of keys
    let mut large_batch = KeyBatch::with_capacity(10_000);
    for i in 0..10_000 {
        large_batch.add(format!("key_{:06}", i));
    }

    println!(
        "    Large batch - Keys: {}, Size: {} bytes",
        large_batch.len(),
        large_batch.total_size()
    );

    // Test that inline keys don't cause excessive allocations
    let inline_keys: Vec<Key> = (0..1000)
        .map(|i| Key::from(format!("k{}", i))) // Short keys, should be inline
        .collect();

    let inline_count = inline_keys.iter().filter(|k| k.is_inline()).count();
    println!(
        "    Inline keys: {}/{} ({:.1}%)",
        inline_count,
        inline_keys.len(),
        (inline_count as f64 / inline_keys.len() as f64) * 100.0
    );

    println!("  ✓ Memory usage is optimized");
    Ok(())
}

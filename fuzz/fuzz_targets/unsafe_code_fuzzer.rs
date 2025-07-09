#![no_main]
use libfuzzer_sys::fuzz_target;
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::TempDir;

/// Fuzzing target for unsafe code paths
/// 
/// This fuzzer specifically targets:
/// 1. SIMD operations with various input sizes
/// 2. Memory-mapped file operations
/// 3. Lock-free data structures
/// 4. Transaction boundaries

fuzz_target!(|data: &[u8]| {
    // Skip if data is too small
    if data.len() < 4 {
        return;
    }
    
    // Use first byte to determine operation type
    let op_type = data[0] % 10;
    let remaining_data = &data[1..];
    
    match op_type {
        0..=2 => fuzz_database_operations(remaining_data),
        3..=5 => fuzz_simd_operations(remaining_data),
        6..=7 => fuzz_cache_operations(remaining_data),
        8..=9 => fuzz_concurrent_operations(remaining_data),
        _ => unreachable!(),
    }
});

/// Fuzz database operations with malformed inputs
fn fuzz_database_operations(data: &[u8]) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    
    // Try to create database with fuzzed config
    let mut config = LightningDbConfig::default();
    if data.len() >= 8 {
        config.cache_size = u64::from_le_bytes([
            data[0], data[1], data[2], data[3],
            data[4], data[5], data[6], data[7],
        ]) % (1024 * 1024 * 1024); // Cap at 1GB
    }
    
    let db = match Database::create(db_path, config) {
        Ok(db) => db,
        Err(_) => return, // Config rejected, that's fine
    };
    
    // Try various operations with fuzzed data
    let chunk_size = 3;
    for chunk in data.chunks(chunk_size) {
        if chunk.is_empty() {
            continue;
        }
        
        let operation = chunk[0] % 4;
        
        match operation {
            0 => {
                // Put operation
                if chunk.len() >= 3 {
                    let key_len = (chunk[1] as usize) % 64 + 1;
                    let val_len = (chunk[2] as usize) % 256 + 1;
                    
                    let key = generate_key(data, key_len);
                    let value = generate_value(data, val_len);
                    
                    if let Ok(tx) = db.begin_transaction() {
                        let _ = tx.put(&key, &value);
                        let _ = tx.commit();
                    }
                }
            }
            1 => {
                // Get operation
                if chunk.len() >= 2 {
                    let key_len = (chunk[1] as usize) % 64 + 1;
                    let key = generate_key(data, key_len);
                    
                    if let Ok(tx) = db.begin_transaction() {
                        let _ = tx.get(&key);
                    }
                }
            }
            2 => {
                // Delete operation
                if chunk.len() >= 2 {
                    let key_len = (chunk[1] as usize) % 64 + 1;
                    let key = generate_key(data, key_len);
                    
                    if let Ok(tx) = db.begin_transaction() {
                        let _ = tx.delete(&key);
                        let _ = tx.commit();
                    }
                }
            }
            3 => {
                // Scan operation
                if chunk.len() >= 3 {
                    let start_len = (chunk[1] as usize) % 32 + 1;
                    let limit = chunk[2] as usize;
                    
                    let start_key = generate_key(data, start_len);
                    
                    if let Ok(tx) = db.begin_transaction() {
                        if let Ok(mut iter) = tx.scan(&start_key, None, false) {
                            for _ in 0..limit {
                                if iter.next().is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }
}

/// Fuzz SIMD operations with various input sizes
fn fuzz_simd_operations(data: &[u8]) {
    use lightning_db::simd::{simd_compare_keys, simd_hash_keys, simd_checksum};
    
    // Generate keys of various sizes
    let mut keys = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() && keys.len() < 100 {
        let key_len = if offset < data.len() {
            (data[offset] as usize % 128) + 1
        } else {
            break;
        };
        offset += 1;
        
        let end = (offset + key_len).min(data.len());
        if offset < end {
            keys.push(&data[offset..end]);
            offset = end;
        }
    }
    
    if keys.is_empty() {
        return;
    }
    
    // Test SIMD hash with various batch sizes
    for batch_size in [1, 2, 4, 8, 16, 32, 64, 128] {
        let batch: Vec<_> = keys.iter().take(batch_size).copied().collect();
        let _ = simd_hash_keys(&batch);
    }
    
    // Test SIMD comparison
    if keys.len() >= 2 {
        for i in 0..keys.len().saturating_sub(1) {
            let _ = simd_compare_keys(keys[i], keys[i + 1]);
        }
    }
    
    // Test SIMD checksum
    for key in &keys {
        let _ = simd_checksum(key);
    }
    
    // Test with aligned and unaligned data
    for alignment_offset in 0..8 {
        if alignment_offset < data.len() {
            let unaligned_data = &data[alignment_offset..];
            let _ = simd_checksum(unaligned_data);
        }
    }
}

/// Fuzz cache operations
fn fuzz_cache_operations(data: &[u8]) {
    use lightning_db::cache::LockFreeCache;
    use lightning_db::storage::Page;
    
    let cache = LockFreeCache::new(100); // Small cache for fuzzing
    
    // Generate operations from fuzz data
    for chunk in data.chunks(4) {
        if chunk.len() < 2 {
            continue;
        }
        
        let operation = chunk[0] % 3;
        let page_id = u32::from_le_bytes([
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
            chunk.get(3).copied().unwrap_or(0),
            0,
        ]) % 1000; // Limit page IDs
        
        match operation {
            0 => {
                // Insert
                let page = Page::new(page_id);
                let _ = cache.insert(page_id, Arc::new(page));
            }
            1 => {
                // Get
                let _ = cache.get(page_id);
            }
            2 => {
                // Remove
                cache.remove(page_id);
            }
            _ => unreachable!(),
        }
    }
    
    // Force eviction by filling cache
    for i in 1000..1200 {
        let page = Page::new(i);
        let _ = cache.insert(i, Arc::new(page));
    }
}

/// Fuzz concurrent operations
fn fuzz_concurrent_operations(data: &[u8]) {
    use std::thread;
    use std::sync::atomic::{AtomicBool, Ordering};
    
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();
    
    let db = match Database::create(db_path, LightningDbConfig::default()) {
        Ok(db) => Arc::new(db),
        Err(_) => return,
    };
    
    let stop = Arc::new(AtomicBool::new(false));
    let data = Arc::new(data.to_vec());
    
    // Spawn multiple threads with different operations
    let mut handles = vec![];
    
    for thread_id in 0..4 {
        let db = db.clone();
        let stop = stop.clone();
        let data = data.clone();
        
        let handle = thread::spawn(move || {
            let mut offset = thread_id;
            
            while !stop.load(Ordering::Relaxed) && offset < data.len() {
                let op = data[offset] % 4;
                offset += 4;
                
                if offset >= data.len() {
                    break;
                }
                
                match op {
                    0 => {
                        // Concurrent writes
                        if let Ok(tx) = db.begin_transaction() {
                            let key = format!("t{}_k{}", thread_id, offset);
                            let _ = tx.put(key.as_bytes(), &data[..offset.min(data.len())]);
                            let _ = tx.commit();
                        }
                    }
                    1 => {
                        // Concurrent reads
                        if let Ok(tx) = db.begin_transaction() {
                            let key = format!("t{}_k{}", thread_id % 4, offset % 100);
                            let _ = tx.get(key.as_bytes());
                        }
                    }
                    2 => {
                        // Concurrent scans
                        if let Ok(tx) = db.begin_transaction() {
                            let start = format!("t{}_", thread_id);
                            if let Ok(mut iter) = tx.scan(start.as_bytes(), None, false) {
                                for _ in 0..10 {
                                    if iter.next().is_none() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    3 => {
                        // Transaction conflicts
                        if let Ok(tx1) = db.begin_transaction() {
                            if let Ok(tx2) = db.begin_transaction() {
                                let key = b"conflict_key";
                                let _ = tx1.put(key, b"value1");
                                let _ = tx2.put(key, b"value2");
                                let _ = tx1.commit();
                                let _ = tx2.commit(); // Should fail
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
        });
        
        handles.push(handle);
    }
    
    // Let threads run briefly
    thread::sleep(std::time::Duration::from_millis(10));
    
    // Stop threads
    stop.store(true, Ordering::Relaxed);
    
    // Wait for completion
    for handle in handles {
        let _ = handle.join();
    }
}

/// Generate a key from fuzz data
fn generate_key(data: &[u8], len: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(len);
    for i in 0..len {
        key.push(data[i % data.len()]);
    }
    key
}

/// Generate a value from fuzz data
fn generate_value(data: &[u8], len: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(len);
    for i in 0..len {
        value.push(data[(i + 7) % data.len()].wrapping_add(i as u8));
    }
    value
}
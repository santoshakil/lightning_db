use lightning_db::{Database, LightningDbConfig};
use lightning_db::storage::MmapConfig;
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_btree_with_standard_page_manager() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_standard_db");
    
    let config = LightningDbConfig {
        use_optimized_page_manager: false,
        prefetch_enabled: false, // Disable prefetch for standard mode
        ..Default::default()
    };
    
    let db = Database::create(&db_path, config).unwrap();
    
    // Test basic operations
    const NUM_KEYS: usize = 100;
    
    // Write keys
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Read keys back
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(value, format!("value_{}", i).as_bytes());
    }
    
    // Delete some keys
    for i in (0..NUM_KEYS).step_by(2) {
        let key = format!("key_{:04}", i);
        assert!(db.delete(key.as_bytes()).unwrap());
    }
    
    // Verify deletions
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(value.is_none());
        } else {
            assert_eq!(value.unwrap(), format!("value_{}", i).as_bytes());
        }
    }
    
    println!("Standard page manager test passed!");
}

#[test]
fn test_btree_with_optimized_page_manager() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_optimized_db");
    
    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        mmap_config: Some(MmapConfig {
            enable_prefault: true,
            enable_async_msync: true,
            region_size: 4 * 1024 * 1024, // 4MB regions
            max_mapped_regions: 16,
            ..Default::default()
        }),
        ..Default::default()
    };
    
    let db = Database::create(&db_path, config).unwrap();
    
    // Test basic operations
    const NUM_KEYS: usize = 100;
    
    // Write keys
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Read keys back
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(value, format!("value_{}", i).as_bytes());
    }
    
    // Delete some keys
    for i in (0..NUM_KEYS).step_by(2) {
        let key = format!("key_{:04}", i);
        assert!(db.delete(key.as_bytes()).unwrap());
    }
    
    // Verify deletions
    for i in 0..NUM_KEYS {
        let key = format!("key_{:04}", i);
        let value = db.get(key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(value.is_none());
        } else {
            assert_eq!(value.unwrap(), format!("value_{}", i).as_bytes());
        }
    }
    
    // Sync to disk
    db.sync().unwrap();
    
    println!("Optimized page manager test passed!");
}

#[test]
fn test_performance_comparison() {
    let dir = tempdir().unwrap();
    const NUM_KEYS: usize = 10000;
    
    // Test with standard page manager
    {
        let db_path = dir.path().join("perf_standard_db");
        let config = LightningDbConfig {
            use_optimized_page_manager: false,
            prefetch_enabled: false,
            ..Default::default()
        };
        
        let db = Database::create(&db_path, config).unwrap();
        
        let start = Instant::now();
        for i in 0..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let write_duration = start.elapsed();
        
        let start = Instant::now();
        for i in 0..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let _value = db.get(key.as_bytes()).unwrap().unwrap();
        }
        let read_duration = start.elapsed();
        
        println!("\nStandard Page Manager Performance:");
        println!("  Write {} keys: {:?} ({:.2} ops/sec)", 
            NUM_KEYS, write_duration, NUM_KEYS as f64 / write_duration.as_secs_f64());
        println!("  Read {} keys: {:?} ({:.2} ops/sec)", 
            NUM_KEYS, read_duration, NUM_KEYS as f64 / read_duration.as_secs_f64());
    }
    
    // Test with optimized page manager
    {
        let db_path = dir.path().join("perf_optimized_db");
        let config = LightningDbConfig {
            use_optimized_page_manager: true,
            mmap_config: Some(MmapConfig {
                enable_prefault: true,
                enable_async_msync: true,
                region_size: 16 * 1024 * 1024, // 16MB regions
                max_mapped_regions: 32,
                ..Default::default()
            }),
            ..Default::default()
        };
        
        let db = Database::create(&db_path, config).unwrap();
        
        let start = Instant::now();
        for i in 0..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let value = format!("value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        let write_duration = start.elapsed();
        
        let start = Instant::now();
        for i in 0..NUM_KEYS {
            let key = format!("key_{:08}", i);
            let _value = db.get(key.as_bytes()).unwrap().unwrap();
        }
        let read_duration = start.elapsed();
        
        println!("\nOptimized Page Manager Performance:");
        println!("  Write {} keys: {:?} ({:.2} ops/sec)", 
            NUM_KEYS, write_duration, NUM_KEYS as f64 / write_duration.as_secs_f64());
        println!("  Read {} keys: {:?} ({:.2} ops/sec)", 
            NUM_KEYS, read_duration, NUM_KEYS as f64 / read_duration.as_secs_f64());
    }
}

#[test]
fn test_persistence_with_optimized_page_manager() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("persist_db");
    
    let config = LightningDbConfig {
        use_optimized_page_manager: true,
        mmap_config: Some(MmapConfig {
            enable_async_msync: false, // Sync immediately for test
            ..Default::default()
        }),
        ..Default::default()
    };
    
    // Create and populate database
    {
        let db = Database::create(&db_path, config.clone()).unwrap();
        
        for i in 0..50 {
            let key = format!("persist_key_{:02}", i);
            let value = format!("persist_value_{}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        db.sync().unwrap();
    }
    
    // Reopen and verify data
    {
        let db = Database::open(&db_path, config).unwrap();
        
        for i in 0..50 {
            let key = format!("persist_key_{:02}", i);
            let value = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(value, format!("persist_value_{}", i).as_bytes());
        }
        
        println!("Persistence test with optimized page manager passed!");
    }
}
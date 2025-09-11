use lightning_db::{Database, LightningDbConfig, Key};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn test_basic_database_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    db.put(b"key1", b"value1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    
    db.put(b"key1", b"value2").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value2".to_vec()));
    
    db.delete(b"key1").unwrap();
    assert_eq!(db.get(b"key1").unwrap(), None);
}

#[test]
fn test_batch_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    let keys = vec![b"k1", b"k2", b"k3"];
    let values = vec![b"v1", b"v2", b"v3"];
    
    for (key, value) in keys.iter().zip(values.iter()) {
        db.put(*key, *value).unwrap();
    }
    
    for (key, value) in keys.iter().zip(values.iter()) {
        assert_eq!(db.get(*key).unwrap(), Some(value.to_vec()));
    }
    
    for key in &keys {
        db.delete(*key).unwrap();
    }
    
    for key in keys {
        assert_eq!(db.get(key).unwrap(), None);
    }
}

#[test]
fn test_range_queries() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.put(b"d", b"4").unwrap();
    
    let results = db.range(Some(b"b"), Some(b"d")).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"b");
    assert_eq!(results[0].1, b"2");
    assert_eq!(results[1].0, b"c");
    assert_eq!(results[1].1, b"3");
}

#[test]
fn test_concurrent_access() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    
    let mut handles = vec![];
    
    for i in 0..10 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            
            let retrieved = db_clone.get(key.as_bytes()).unwrap();
            assert_eq!(retrieved, Some(value.into_bytes()));
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    for i in 0..10 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);
        assert_eq!(db.get(key.as_bytes()).unwrap(), Some(value.into_bytes()));
    }
}

#[test]
fn test_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    {
        let db = Database::create(&path, LightningDbConfig::default()).unwrap();
        db.put(b"persistent", b"data").unwrap();
        db.flush_lsm().unwrap();
    }
    
    {
        let db = Database::open(&path, LightningDbConfig::default()).unwrap();
        assert_eq!(db.get(b"persistent").unwrap(), Some(b"data".to_vec()));
    }
}

#[test]
fn test_large_values() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    let large_value = vec![42u8; 1024 * 1024];
    db.put(b"large", &large_value).unwrap();
    
    let retrieved = db.get(b"large").unwrap();
    assert_eq!(retrieved, Some(large_value));
}

#[test]
fn test_with_compression() {
    let dir = tempdir().unwrap();
    let mut config = LightningDbConfig::default();
    config.compression_enabled = true;
    config.compression_type = 2;
    
    let db = Database::create(dir.path(), config).unwrap();
    
    let data = b"compress me please, this is repetitive data data data data data";
    db.put(b"compressed", data).unwrap();
    
    assert_eq!(db.get(b"compressed").unwrap(), Some(data.to_vec()));
}

#[test]
fn test_prefix_scan() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    db.put(b"user:1", b"Alice").unwrap();
    db.put(b"user:2", b"Bob").unwrap();
    db.put(b"user:3", b"Charlie").unwrap();
    db.put(b"admin:1", b"Admin").unwrap();
    
    let users = db.prefix_scan_key(&Key::from(b"user:".as_ref())).unwrap();
    assert_eq!(users.len(), 3);
    
    let admins = db.prefix_scan_key(&Key::from(b"admin:".as_ref())).unwrap();
    assert_eq!(admins.len(), 1);
}

#[test]
fn test_database_stats() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    db.put(b"test", b"data").unwrap();
    
    let stats = db.stats();
    assert!(stats.page_count > 0);
}

#[test]
fn test_clear_database() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    
    db.put(b"key1", b"value1").unwrap();
    db.put(b"key2", b"value2").unwrap();
    
    assert_eq!(db.get(b"key1").unwrap(), Some(b"value1".to_vec()));
    
    db.delete(b"key1").unwrap();
    db.delete(b"key2").unwrap();
    
    assert_eq!(db.get(b"key1").unwrap(), None);
    assert_eq!(db.get(b"key2").unwrap(), None);
}
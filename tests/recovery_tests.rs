use lightning_db::{Database, LightningDbConfig, Result, WalSyncMode};
use lightning_db::core::storage::{Page, PageId};
use lightning_db::utils::integrity::{calculate_checksum};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_checksum_calculation() -> Result<()> {
    let empty_data = &[];
    let empty_checksum = calculate_checksum(empty_data);
    assert_eq!(empty_checksum, 0);
    
    let single_byte = &[0x42];
    let single_checksum = calculate_checksum(single_byte);
    assert_ne!(single_checksum, 0);
    
    let test_data = b"Hello, World!";
    let test_checksum = calculate_checksum(test_data);
    let duplicate_checksum = calculate_checksum(test_data);
    assert_eq!(test_checksum, duplicate_checksum);
    
    let different_data = b"Goodbye, World!";
    let different_checksum = calculate_checksum(different_data);
    assert_ne!(test_checksum, different_checksum);
    
    Ok(())
}

#[test]
fn test_page_basic_operations() -> Result<()> {
    let page_id: PageId = 1;
    let mut page = Page::new(page_id);
    
    assert_eq!(page.id, page_id);
    assert!(!page.dirty);
    
    let test_data = b"Test page data";
    page.set_data(test_data)?;
    assert!(page.dirty);
    
    let checksum = page.calculate_checksum();
    assert_ne!(checksum, 0);
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config)?;
    
    // Test basic put and get within implicit transactions
    db.put(b"tx_key1", b"tx_value1")?;
    db.put(b"tx_key2", b"tx_value2")?;
    
    let value1 = db.get(b"tx_key1")?;
    assert_eq!(value1, Some(b"tx_value1".to_vec()));
    
    let value2 = db.get(b"tx_key2")?;
    assert_eq!(value2, Some(b"tx_value2".to_vec()));
    
    // Test delete
    db.delete(b"tx_key1")?;
    let value1_after = db.get(b"tx_key1")?;
    assert_eq!(value1_after, None);
    
    Ok(())
}

#[tokio::test]
async fn test_wal_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    config.wal_sync_mode = WalSyncMode::Sync;
    
    let db = Database::create(&db_path, config.clone())?;
    
    db.put(b"wal_key1", b"wal_value1")?;
    db.put(b"wal_key2", b"wal_value2")?;
    db.flush_lsm()?;
    
    drop(db);
    
    let recovered_db = Database::open(&db_path, config)?;
    
    let value1 = recovered_db.get(b"wal_key1")?;
    assert_eq!(value1, Some(b"wal_value1".to_vec()));
    
    let value2 = recovered_db.get(b"wal_key2")?;
    assert_eq!(value2, Some(b"wal_value2".to_vec()));
    
    Ok(())
}

#[tokio::test]
async fn test_database_crash_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    
    {
        let db = Database::create(&db_path, config.clone())?;
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let value = format!("value_{:04}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        db.flush_lsm()?;
    }
    
    let recovered_db = Database::open(&db_path, config)?;
    
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{:04}", i);
        let result = recovered_db.get(key.as_bytes())?;
        assert_eq!(result, Some(value.into_bytes()));
    }
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_recovery() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(&db_path, config)?);
    
    let mut handles = vec![];
    
    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let key = format!("thread_{}_key_{:03}", thread_id, i);
                let value = format!("thread_{}_value_{:03}", thread_id, i);
                db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        let _ = handle.await;
    }
    
    for thread_id in 0..10 {
        for i in 0..50 {
            let key = format!("thread_{}_key_{:03}", thread_id, i);
            let value = format!("thread_{}_value_{:03}", thread_id, i);
            let result = db.get(key.as_bytes())?;
            assert_eq!(result, Some(value.into_bytes()));
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_recovery_with_compression() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.use_unified_wal = true;
    config.compression_enabled = true;
    config.compression_type = 1; // ZSTD
    
    let db = Database::create(&db_path, config.clone())?;
    
    db.put(b"compressed_key1", b"compressed_value1")?;
    db.put(b"compressed_key2", b"compressed_value2")?;
    db.flush_lsm()?;
    
    drop(db);
    
    let recovered_db = Database::open(&db_path, config)?;
    
    let value1 = recovered_db.get(b"compressed_key1")?;
    assert_eq!(value1, Some(b"compressed_value1".to_vec()));
    
    let value2 = recovered_db.get(b"compressed_key2")?;
    assert_eq!(value2, Some(b"compressed_value2".to_vec()));
    
    Ok(())
}

#[tokio::test]
async fn test_recovery_performance() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test.db");
    
    let mut config = LightningDbConfig::default();
    config.cache_size = 1024 * 1024 * 10; // 10MB cache
    
    let start = std::time::Instant::now();
    
    {
        let db = Database::create(&db_path, config.clone())?;
        
        for i in 0..1000 {
            let key = format!("perf_key_{:06}", i);
            let value = vec![i as u8; 1024]; // 1KB per value
            db.put(key.as_bytes(), &value)?;
        }
        
        db.flush_lsm()?;
    }
    
    let write_duration = start.elapsed();
    
    let recovery_start = std::time::Instant::now();
    let recovered_db = Database::open(&db_path, config)?;
    let recovery_duration = recovery_start.elapsed();
    
    for i in (0..1000).step_by(100) {
        let key = format!("perf_key_{:06}", i);
        let result = recovered_db.get(key.as_bytes())?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1024);
    }
    
    println!("Write duration: {:?}", write_duration);
    println!("Recovery duration: {:?}", recovery_duration);
    
    assert!(recovery_duration < Duration::from_secs(5));
    
    Ok(())
}
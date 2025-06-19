use lightning_db::{Database, LightningDbConfig, BackupManager, BackupConfig};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_backup_and_restore() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("original_db");
    let backup_path = dir.path().join("backup");
    let restore_path = dir.path().join("restored_db");
    
    // Create and populate database
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config.clone()).unwrap();
    
    // Insert test data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Sync to ensure data is persisted
    db.sync().unwrap();
    drop(db); // Close database
    
    // Create backup
    let backup_manager = BackupManager::new(BackupConfig::default());
    let metadata = backup_manager.create_backup(&db_path, &backup_path).unwrap();
    
    println!("Backup created:");
    println!("  Type: {:?}", metadata.backup_type);
    println!("  Files: {}", metadata.file_count);
    println!("  Size: {} bytes", metadata.total_size);
    println!("  Compressed: {}", metadata.compressed);
    
    // Restore to new location
    backup_manager.restore_backup(&backup_path, &restore_path).unwrap();
    
    // Verify restored database
    let restored_db = Database::open(&restore_path, config).unwrap();
    
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = restored_db.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(format!("value_{}", i).into_bytes()));
    }
    
    println!("Restore verification successful!");
}

#[test]
fn test_incremental_backup() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("incremental_db");
    let full_backup_path = dir.path().join("full_backup");
    let incr_backup_path = dir.path().join("incremental_backup");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).unwrap();
    
    // Initial data
    for i in 0..50 {
        let key = format!("initial_key_{:03}", i);
        let value = format!("initial_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.sync().unwrap();
    
    // Create full backup
    let backup_config = BackupConfig {
        include_wal: true,
        compress: false, // Disable compression for easier debugging
        verify: true,
        max_incremental_size: 10 * 1024 * 1024,
    };
    let backup_manager = BackupManager::new(backup_config);
    let full_metadata = backup_manager.create_backup(&db_path, &full_backup_path).unwrap();
    
    println!("Full backup created: {} files, {} bytes", 
             full_metadata.file_count, full_metadata.total_size);
    
    // Wait a bit to ensure file modification times are different
    thread::sleep(Duration::from_millis(1100));
    
    // Add more data
    for i in 50..100 {
        let key = format!("new_key_{:03}", i);
        let value = format!("new_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.sync().unwrap();
    
    // Create incremental backup
    let incr_metadata = backup_manager.create_incremental_backup(
        &db_path,
        &incr_backup_path,
        full_metadata.timestamp,
    ).unwrap();
    
    println!("Incremental backup created: {} files, {} bytes", 
             incr_metadata.file_count, incr_metadata.total_size);
    
    // Incremental backup should be smaller
    assert!(incr_metadata.total_size <= full_metadata.total_size);
}

#[test]
fn test_compressed_backup() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("compress_test_db");
    let uncompressed_backup = dir.path().join("uncompressed");
    let compressed_backup = dir.path().join("compressed");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config.clone()).unwrap();
    
    // Insert repetitive data that compresses well
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = "This is a repetitive value that should compress well. ".repeat(10);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.sync().unwrap();
    drop(db);
    
    // Create uncompressed backup
    let uncompressed_config = BackupConfig {
        compress: false,
        ..Default::default()
    };
    let manager = BackupManager::new(uncompressed_config);
    let uncompressed_metadata = manager.create_backup(&db_path, &uncompressed_backup).unwrap();
    
    // Create compressed backup
    let compressed_config = BackupConfig {
        compress: true,
        ..Default::default()
    };
    let manager = BackupManager::new(compressed_config);
    let compressed_metadata = manager.create_backup(&db_path, &compressed_backup).unwrap();
    
    println!("Uncompressed size: {} bytes", uncompressed_metadata.total_size);
    println!("Compressed size: {} bytes", compressed_metadata.total_size);
    println!("Compression ratio: {:.2}%", 
             (compressed_metadata.total_size as f64 / uncompressed_metadata.total_size as f64) * 100.0);
    
    // Compressed backup should be smaller
    assert!(compressed_metadata.total_size < uncompressed_metadata.total_size);
    
    // Restore and verify compressed backup
    let restore_path = dir.path().join("restored_from_compressed");
    manager.restore_backup(&compressed_backup, &restore_path).unwrap();
    
    let restored_db = Database::open(&restore_path, config).unwrap();
    let value = restored_db.get(b"key_000").unwrap();
    assert!(value.is_some());
}

#[test]
fn test_backup_listing() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("list_test_db");
    let backup_root = dir.path().join("backups");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).unwrap();
    
    // Create some data
    db.put(b"test_key", b"test_value").unwrap();
    db.sync().unwrap();
    
    let manager = BackupManager::new(BackupConfig::default());
    
    // Create multiple backups
    for i in 0..3 {
        let backup_dir = backup_root.join(format!("backup_{}", i));
        manager.create_backup(&db_path, &backup_dir).unwrap();
        
        // Add more data between backups
        let key = format!("key_{}", i);
        db.put(key.as_bytes(), b"value").unwrap();
        db.sync().unwrap();
        
        // Small delay to ensure different timestamps
        thread::sleep(Duration::from_millis(100));
    }
    
    // List backups
    let backups = BackupManager::list_backups(&backup_root).unwrap();
    
    assert_eq!(backups.len(), 3);
    
    // Verify backups are sorted by timestamp (newest first)
    for i in 1..backups.len() {
        assert!(backups[i-1].timestamp >= backups[i].timestamp);
    }
    
    println!("Found {} backups:", backups.len());
    for (i, backup) in backups.iter().enumerate() {
        println!("  {}: {} files, {} bytes, created at {}", 
                 i, backup.file_count, backup.total_size, backup.timestamp);
    }
}

#[test]
fn test_backup_verification() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("verify_test_db");
    let backup_path = dir.path().join("backup_to_verify");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).unwrap();
    
    // Add test data
    for i in 0..10 {
        let key = format!("verify_key_{}", i);
        let value = format!("verify_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.sync().unwrap();
    drop(db);
    
    // Create backup with verification enabled
    let backup_config = BackupConfig {
        verify: true,
        ..Default::default()
    };
    let manager = BackupManager::new(backup_config);
    
    // This should succeed
    let metadata = manager.create_backup(&db_path, &backup_path).unwrap();
    assert!(metadata.checksum.is_some());
    
    println!("Backup verification passed!");
    println!("  Checksum: {}", metadata.checksum.unwrap());
}
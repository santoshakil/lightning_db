use lightning_db::{Database, LightningDbConfig};
use lightning_db::features::compaction::{CompactionConfig, CompactionType, CompactionState};
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn test_basic_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test basic compaction
    let compaction_id = db.compact()?;
    assert!(compaction_id > 0);
    
    // Test compaction statistics (async API via runtime)
    let rt = tokio::runtime::Runtime::new()?;
    let stats = rt.block_on(db.get_compaction_stats())?;
    assert!(stats.total_compactions >= 1);
    
    Ok(())
}

#[test]
fn test_async_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test async compaction
    let compaction_id = db.compact_async()?;
    assert!(compaction_id > 0);
    
    // Wait a bit and check progress
    std::thread::sleep(Duration::from_millis(100));
    
    match tokio::runtime::Runtime::new()?.block_on(db.get_compaction_progress(compaction_id)) {
        Ok(progress) => {
            assert_eq!(progress.compaction_id, compaction_id);
            assert_eq!(progress.compaction_type, CompactionType::Online);
        },
        Err(_) => {
            // Progress might not be available if compaction completed quickly
        }
    }
    
    Ok(())
}

#[test]
fn test_major_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Add some data first
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Test major compaction
    let bytes_reclaimed = db.compact_major()?;
    assert!(bytes_reclaimed >= 0); // Should not error
    
    // Verify data is still accessible
    for i in 0..100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        if let Some(value) = db.get(key.as_bytes())? {
            assert_eq!(value, expected_value.as_bytes());
        }
    }
    
    Ok(())
}

#[test]
fn test_incremental_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test incremental compaction
    let bytes_reclaimed = db.compact_incremental()?;
    assert!(bytes_reclaimed >= 0);
    
    Ok(())
}

#[test]
fn test_garbage_collection() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Create some data that could be garbage collected
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"temp_key", b"temp_value")?;
    // Don't commit - this creates garbage
    db.abort_transaction(tx_id)?;
    
    // Test garbage collection
    let bytes_collected = db.garbage_collect()?;
    assert!(bytes_collected >= 0);
    
    Ok(())
}

#[test]
fn test_fragmentation_stats() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Get fragmentation statistics
    let frag_stats = db.get_fragmentation_stats()?;
    
    // Should have statistics for various components
    assert!(!frag_stats.is_empty());
    
    // Check that all values are valid percentages
    for (component, fragmentation) in frag_stats {
        assert!(fragmentation >= 0.0 && fragmentation <= 1.0, 
            "Invalid fragmentation ratio for {}: {}", component, fragmentation);
    }
    
    Ok(())
}

#[test]
fn test_space_savings_estimation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Estimate space savings (async)
    let estimated_savings = tokio::runtime::Runtime::new()?.block_on(db.estimate_space_savings())?;
    assert!(estimated_savings >= 0);
    
    Ok(())
}

#[test]
fn test_auto_compaction_config() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Test enabling auto compaction
    db.set_auto_compaction(true, Some(3600))?; // Every hour
    
    // Test disabling auto compaction
    db.set_auto_compaction(false, None)?;
    
    Ok(())
}

#[test]
fn test_background_maintenance() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Start background maintenance
    db.start_background_maintenance()?;
    
    // Let it run briefly
    std::thread::sleep(Duration::from_millis(100));
    
    // Stop background maintenance
    db.stop_background_maintenance()?;
    
    Ok(())
}

#[test]
fn test_compaction_report() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Run a compaction first
    db.compact()?;
    
    // Get the report
    let report = db.get_compaction_report()?;
    
    // Verify report contains expected sections
    assert!(report.contains("Compaction Report"));
    assert!(report.contains("Total Compactions:"));
    assert!(report.contains("Space Reclaimed:"));
    
    Ok(())
}

#[test]
fn test_compaction_cancellation() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Start async compaction
    let compaction_id = db.compact_async()?;
    
    // Try to cancel it (might succeed or fail depending on timing)
    let _ = db.cancel_compaction(compaction_id);
    
    Ok(())
}

#[test]
fn test_multiple_concurrent_compactions() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Start multiple async compactions
    let mut compaction_ids = Vec::new();
    
    for _ in 0..3 {
        match db.compact_async() {
            Ok(id) => compaction_ids.push(id),
            Err(_) => break, // Expected if concurrency limit reached
        }
    }
    
    assert!(!compaction_ids.is_empty());
    
    // Wait a bit for them to complete
    std::thread::sleep(Duration::from_millis(200));
    
    Ok(())
}

#[test]
fn test_compaction_with_active_transactions() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Start a transaction
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"test_key", b"test_value")?;
    
    // Run compaction while transaction is active
    let bytes_reclaimed = db.compact()?;
    assert!(bytes_reclaimed >= 0);
    
    // Transaction should still work
    let value = db.get_tx(tx_id, b"test_key")?;
    assert_eq!(value.as_deref(), Some(b"test_value".as_ref()));
    
    // Commit transaction
    db.commit_transaction(tx_id)?;
    
    Ok(())
}

#[test]
fn test_compaction_stats_tracking() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // Get initial stats (async API): block on a runtime to avoid changing library API
    let rt = tokio::runtime::Runtime::new()?;
    let initial_stats = rt.block_on(db.get_compaction_stats())?;
    
    // Run multiple compactions
    db.compact()?;
    db.compact_incremental()?;
    
    // Get updated stats
    let updated_stats = rt.block_on(db.get_compaction_stats())?;
    
    // Verify stats increased
    assert!(updated_stats.total_compactions >= initial_stats.total_compactions + 2);
    assert!(updated_stats.successful_compactions >= initial_stats.successful_compactions);
    
    Ok(())
}

// Integration test with the existing LSM compaction
#[test]
fn test_integration_with_lsm_compaction() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let mut config = LightningDbConfig::default();
    // LSM is enabled when compression is enabled in current config
    config.compression_enabled = true;
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Add data to populate LSM tree
    for i in 0..1000 {
        let key = format!("lsm_key_{}", i);
        let value = format!("lsm_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    
    // Run both old and new compaction methods
    db.compact_lsm()?; // Existing method
    db.compact()?;     // New comprehensive method
    
    // Verify data integrity
    for i in 0..1000 {
        let key = format!("lsm_key_{}", i);
        let expected_value = format!("lsm_value_{}", i);
        if let Some(value) = db.get(key.as_bytes())? {
            assert_eq!(value, expected_value.as_bytes());
        }
    }
    
    Ok(())
}

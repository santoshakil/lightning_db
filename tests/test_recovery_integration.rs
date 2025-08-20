use lightning_db::core::recovery::*;
use lightning_db::{LightningDbConfig, RecoveryConfiguration};
use tempfile::TempDir;

#[tokio::test]
async fn test_comprehensive_recovery_manager_creation() {
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config).unwrap();
    
    // Test that recovery manager was created successfully
    assert!(!recovery_manager.needs_recovery().await.unwrap());
}

#[tokio::test]
async fn test_recovery_health_monitoring() {
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    
    let recovery_config = RecoveryConfiguration {
        enable_crash_recovery: false,
        enable_io_recovery: true,
        enable_memory_recovery: true,
        enable_transaction_recovery: true,
        enable_corruption_recovery: true,
        auto_recovery: false,
        ..Default::default()
    };
    
    let recovery_manager = ComprehensiveRecoveryManager::with_config(
        temp_dir.path(), 
        config, 
        recovery_config
    ).unwrap();
    
    let health_report = recovery_manager.get_health_report().await;
    
    // Initially should be unknown status since no operations have occurred
    assert_eq!(health_report.overall_status, OverallHealthStatus::Unknown);
}

#[tokio::test]
async fn test_individual_recovery_managers() {
    let temp_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config).unwrap();
    
    // Test accessing individual recovery managers
    let _io_manager = recovery_manager.io_recovery();
    let _memory_manager = recovery_manager.memory_recovery();
    let _transaction_manager = recovery_manager.transaction_recovery();
    let _corruption_manager = recovery_manager.corruption_recovery();
    
    // These should all be accessible without panicking
}

#[tokio::test]
async fn test_io_recovery_basic_operations() {
    let io_config = IoRecoveryConfig::default();
    let io_manager = IoRecoveryManager::new(io_config);
    
    let health_report = io_manager.monitor_disk_health().await.unwrap();
    assert_eq!(health_report.status, DiskHealthStatus::Healthy);
}

#[tokio::test]
async fn test_memory_recovery_basic_operations() {
    let memory_config = MemoryRecoveryConfig::default();
    let memory_manager = MemoryRecoveryManager::new(memory_config);
    
    let health_report = memory_manager.get_memory_health_report().await;
    assert_eq!(health_report.status, MemoryHealthStatus::Healthy);
    
    // Test basic allocation
    let ptr = memory_manager.try_allocate(1024).await.unwrap();
    assert!(!ptr.is_null());
    
    memory_manager.deallocate(ptr, 1024).await.unwrap();
}

#[tokio::test]
async fn test_transaction_recovery_basic_operations() {
    let tx_config = TransactionRecoveryConfig::default();
    let tx_manager = TransactionRecoveryManager::new(tx_config);
    
    let health_report = tx_manager.get_transaction_health_report().await;
    assert_eq!(health_report.status, TransactionHealthStatus::Healthy);
    
    // Test transaction lifecycle
    let tx_id = tx_manager.begin_transaction().await.unwrap();
    assert!(tx_id > 0);
    
    tx_manager.commit_transaction(tx_id).await.unwrap();
}

#[tokio::test]
async fn test_corruption_recovery_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let corruption_config = CorruptionRecoveryConfig::default();
    let corruption_manager = CorruptionRecoveryManager::new(
        corruption_config, 
        temp_dir.path()
    ).unwrap();
    
    let health_report = corruption_manager.get_corruption_health_report().await;
    assert_eq!(health_report.status, CorruptionHealthStatus::Healthy);
    
    // Test database scan
    let scan_report = corruption_manager.scan_database_corruption(temp_dir.path()).await.unwrap();
    assert_eq!(scan_report.files_scanned, 0); // No database files in empty directory
}
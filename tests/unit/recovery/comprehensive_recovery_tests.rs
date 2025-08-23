use lightning_db::core::recovery::{
    ComprehensiveRecoveryManager, RecoveryConfiguration, OverallHealthStatus,
    IoRecoveryManager, MemoryRecoveryManager, TransactionRecoveryManager, 
    CorruptionRecoveryManager, IoRecoveryConfig, MemoryRecoveryConfig,
    TransactionRecoveryConfig, CorruptionRecoveryConfig, ValidationLevel,
    DiskHealthStatus, MemoryHealthStatus, TransactionHealthStatus, CorruptionHealthStatus,
};
use lightning_db::{LightningDbConfig, Database};
use std::time::Duration;
use tempfile::TempDir;
use tokio::fs;

/// Test ComprehensiveRecoveryManager initialization and configuration
#[tokio::test]
async fn test_comprehensive_recovery_manager_new() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    // Verify all components are initialized
    assert!(recovery_manager.io_recovery().is_some());
    assert!(recovery_manager.memory_recovery().is_some());
    assert!(recovery_manager.transaction_recovery().is_some());
    assert!(recovery_manager.corruption_recovery().is_some());
    assert!(recovery_manager.crash_recovery().is_some());
    
    // Test progress tracker
    let progress = recovery_manager.progress();
    assert!(!progress.phase().is_empty());
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test ComprehensiveRecoveryManager with custom configuration
#[tokio::test]
async fn test_comprehensive_recovery_manager_with_config() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let custom_config = RecoveryConfiguration {
        enable_crash_recovery: false,
        enable_io_recovery: true,
        enable_memory_recovery: false,
        enable_transaction_recovery: true,
        enable_corruption_recovery: true,
        auto_recovery: false,
        recovery_timeout: Duration::from_secs(300),
        health_check_interval: Duration::from_secs(60),
    };
    
    let recovery_manager = ComprehensiveRecoveryManager::with_config(
        temp_dir.path(), 
        config, 
        custom_config
    ).expect("Failed to create recovery manager with config");
    
    // Verify crash recovery is disabled
    assert!(recovery_manager.crash_recovery().is_none());
    
    // Verify other components are initialized
    assert!(recovery_manager.io_recovery().is_some());
    assert!(recovery_manager.memory_recovery().is_some());
    assert!(recovery_manager.transaction_recovery().is_some());
    assert!(recovery_manager.corruption_recovery().is_some());
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test needs_recovery logic for fresh databases
#[tokio::test]
async fn test_needs_recovery_fresh_database() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    // Fresh directory should not need recovery
    let needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    assert!(!needs_recovery, "Fresh database should not need recovery");
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test needs_recovery logic with explicit recovery marker
#[tokio::test]
async fn test_needs_recovery_with_marker() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    // Create recovery marker file
    let recovery_marker = temp_dir.path().join(".recovery_needed");
    fs::write(&recovery_marker, "recovery needed").await
        .expect("Failed to write recovery marker");
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    let needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    assert!(needs_recovery, "Database with recovery marker should need recovery");
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test needs_recovery logic with missing shutdown marker
#[tokio::test]
async fn test_needs_recovery_missing_shutdown_marker() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    // Create data file to simulate existing database
    let data_file = temp_dir.path().join("data.db");
    fs::write(&data_file, "database data").await
        .expect("Failed to write data file");
    
    // Don't create shutdown marker
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    let needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    assert!(needs_recovery, "Database without shutdown marker should need recovery");
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test needs_recovery logic with WAL files
#[tokio::test]
async fn test_needs_recovery_with_wal_files() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    // Create data file and shutdown marker
    let data_file = temp_dir.path().join("data.db");
    fs::write(&data_file, "database data").await
        .expect("Failed to write data file");
    
    let shutdown_marker = temp_dir.path().join(".clean_shutdown");
    fs::write(&shutdown_marker, "clean").await
        .expect("Failed to write shutdown marker");
    
    // Create WAL directory with files
    let wal_dir = temp_dir.path().join("wal");
    fs::create_dir(&wal_dir).await
        .expect("Failed to create WAL directory");
    
    let wal_file = wal_dir.join("transaction.wal");
    fs::write(&wal_file, "wal data").await
        .expect("Failed to write WAL file");
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    let needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    assert!(needs_recovery, "Database with WAL files should need recovery");
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test health monitoring task management
#[tokio::test]
async fn test_health_monitoring_lifecycle() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let custom_config = RecoveryConfiguration {
        health_check_interval: Duration::from_millis(100), // Fast interval for testing
        ..Default::default()
    };
    
    let recovery_manager = ComprehensiveRecoveryManager::with_config(
        temp_dir.path(), 
        config, 
        custom_config
    ).expect("Failed to create recovery manager");
    
    // Start health monitoring by calling get_health_report (which triggers monitoring)
    let initial_report = recovery_manager.get_health_report().await;
    assert_eq!(initial_report.overall_status, OverallHealthStatus::Unknown);
    
    // Wait a bit for monitoring to start
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Stop health monitoring
    recovery_manager.stop_health_monitoring().await;
    
    // Verify we can still get health reports after stopping
    let final_report = recovery_manager.get_health_report().await;
    assert!(matches!(final_report.overall_status, OverallHealthStatus::Unknown | OverallHealthStatus::Healthy));
}

/// Test shutdown and cleanup procedures
#[tokio::test]
async fn test_shutdown_cleanup() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    // Get initial health report to start monitoring
    let _initial_report = recovery_manager.get_health_report().await;
    
    // Wait a bit for monitoring to initialize
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Test graceful shutdown
    recovery_manager.stop_health_monitoring().await;
    
    // Should be able to call multiple times without issues
    recovery_manager.stop_health_monitoring().await;
    recovery_manager.stop_health_monitoring().await;
}

/// Test recovery with all systems disabled
#[tokio::test]
async fn test_recovery_all_disabled() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_config = RecoveryConfiguration {
        enable_crash_recovery: false,
        enable_io_recovery: false,
        enable_memory_recovery: false,
        enable_transaction_recovery: false,
        enable_corruption_recovery: false,
        auto_recovery: false,
        ..Default::default()
    };
    
    let recovery_manager = ComprehensiveRecoveryManager::with_config(
        temp_dir.path(), 
        config, 
        recovery_config
    ).expect("Failed to create recovery manager");
    
    // Should still work with all systems disabled
    let needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    assert!(!needs_recovery, "Fresh database should not need recovery");
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test access to individual recovery managers
#[tokio::test]
async fn test_recovery_manager_access() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    // Test access to individual managers
    let _io_manager = recovery_manager.io_recovery();
    let _memory_manager = recovery_manager.memory_recovery();
    let _transaction_manager = recovery_manager.transaction_recovery();
    let _corruption_manager = recovery_manager.corruption_recovery();
    let _crash_manager = recovery_manager.crash_recovery();
    
    // All should be accessible
    assert!(recovery_manager.crash_recovery().is_some());
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test configuration validation
#[tokio::test]
async fn test_configuration_validation() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    // Test extreme timeout values
    let recovery_config = RecoveryConfiguration {
        recovery_timeout: Duration::from_millis(1), // Very short timeout
        health_check_interval: Duration::from_millis(1), // Very short interval
        ..Default::default()
    };
    
    let recovery_manager = ComprehensiveRecoveryManager::with_config(
        temp_dir.path(), 
        config, 
        recovery_config
    ).expect("Failed to create recovery manager with extreme config");
    
    // Should still initialize successfully
    assert!(recovery_manager.io_recovery().is_some());
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}

/// Test default configuration values
#[tokio::test]
async fn test_default_configuration() {
    let default_config = RecoveryConfiguration::default();
    
    assert!(default_config.enable_crash_recovery);
    assert!(default_config.enable_io_recovery);
    assert!(default_config.enable_memory_recovery);
    assert!(default_config.enable_transaction_recovery);
    assert!(default_config.enable_corruption_recovery);
    assert!(default_config.auto_recovery);
    assert_eq!(default_config.recovery_timeout, Duration::from_secs(600));
    assert_eq!(default_config.health_check_interval, Duration::from_secs(30));
}

/// Mock test for recovery state transitions
#[tokio::test]
async fn test_recovery_state_transitions() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = LightningDbConfig::default();
    
    let recovery_manager = ComprehensiveRecoveryManager::new(temp_dir.path(), config)
        .expect("Failed to create recovery manager");
    
    // Test progress tracking
    let progress = recovery_manager.progress();
    
    // Initial state should be set
    let initial_phase = progress.phase();
    assert!(!initial_phase.is_empty());
    
    // Simulate phase transitions by checking needs_recovery
    let _needs_recovery = recovery_manager.needs_recovery().await
        .expect("Failed to check recovery needs");
    
    // Progress should still be accessible
    let phase_after_check = progress.phase();
    assert!(!phase_after_check.is_empty());
    
    // Clean up
    recovery_manager.stop_health_monitoring().await;
}
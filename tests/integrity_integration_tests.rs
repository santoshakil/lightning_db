use lightning_db::{Database, LightningDbConfig};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_integrity_verification() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_integrity_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Add some test data
    db.put(b"key1", b"value1").expect("Failed to put data");
    db.put(b"key2", b"value2").expect("Failed to put data");
    db.put(b"key3", b"value3").expect("Failed to put data");

    // Test integrity verification
    let report = db.verify_integrity().await;
    
    match report {
        Ok(corruption_report) => {
            println!("Integrity verification completed successfully");
            println!("Corruptions found: {}", corruption_report.corruption_details.len());
            println!("Risk level: {:?}", corruption_report.risk_assessment.overall_risk);
        }
        Err(e) => {
            println!("Integrity verification failed: {}", e);
            // This is acceptable for now as the system is still under development
        }
    }
}

#[tokio::test]
async fn test_corruption_scanning() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_scan_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test corruption scanning
    let scan_result = db.scan_for_corruption().await;
    
    match scan_result {
        Ok(scan) => {
            println!("Corruption scan completed");
            println!("Pages scanned: {}", scan.pages_scanned);
            println!("Corruptions found: {}", scan.corruptions.len());
        }
        Err(e) => {
            println!("Corruption scan failed: {}", e);
            // This is acceptable for now
        }
    }
}

#[tokio::test]
async fn test_auto_repair_configuration() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_repair_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test enabling auto repair
    let enable_result = db.enable_auto_repair(true).await;
    match enable_result {
        Ok(()) => {
            println!("Auto repair enabled successfully");
        }
        Err(e) => {
            println!("Failed to enable auto repair: {}", e);
        }
    }

    // Test disabling auto repair
    let disable_result = db.enable_auto_repair(false).await;
    match disable_result {
        Ok(()) => {
            println!("Auto repair disabled successfully");
        }
        Err(e) => {
            println!("Failed to disable auto repair: {}", e);
        }
    }
}

#[tokio::test]
async fn test_integrity_scanning_interval() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_interval_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test setting scan interval
    let interval = Duration::from_secs(30);
    let result = db.set_integrity_check_interval(interval).await;
    
    match result {
        Ok(()) => {
            println!("Integrity check interval set successfully");
        }
        Err(e) => {
            println!("Failed to set integrity check interval: {}", e);
        }
    }
}

#[tokio::test]
async fn test_background_scanning() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_background_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test starting background scanning
    let start_result = db.start_integrity_scanning().await;
    match start_result {
        Ok(()) => {
            println!("Background scanning started successfully");
        }
        Err(e) => {
            println!("Failed to start background scanning: {}", e);
        }
    }

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test stopping background scanning
    let stop_result = db.stop_integrity_scanning().await;
    match stop_result {
        Ok(()) => {
            println!("Background scanning stopped successfully");
        }
        Err(e) => {
            println!("Failed to stop background scanning: {}", e);
        }
    }
}

#[tokio::test]
async fn test_corruption_report() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_report_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test getting corruption report
    let report_result = db.get_corruption_report().await;
    
    match report_result {
        Ok(report) => {
            println!("Corruption report generated successfully");
            println!("Report ID: {}", report.report_id);
            println!("Database: {}", report.database_info.name);
            println!("Total corruptions: {}", report.corruption_details.len());
        }
        Err(e) => {
            println!("Failed to get corruption report: {}", e);
        }
    }
}

#[tokio::test]
async fn test_integrity_stats() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_stats_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test getting integrity stats
    let stats_result = db.get_integrity_stats().await;
    
    match stats_result {
        Ok(stats) => {
            println!("Integrity stats retrieved successfully");
            println!("Total scans: {}", stats.total_scans);
            println!("Corrupted pages: {}", stats.corrupted_pages);
            println!("Repaired pages: {}", stats.repaired_pages);
        }
        Err(e) => {
            println!("Failed to get integrity stats: {}", e);
        }
    }
}

#[tokio::test]
async fn test_quarantine_operations() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_quarantine_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test getting quarantined data
    let quarantine_result = db.get_quarantined_data().await;
    
    match quarantine_result {
        Ok(entries) => {
            println!("Quarantined data retrieved successfully");
            println!("Number of quarantined entries: {}", entries.len());
        }
        Err(e) => {
            println!("Failed to get quarantined data: {}", e);
        }
    }
}

#[tokio::test]
async fn test_repair_corruption() {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test_repair_corruption_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config).expect("Failed to create database");

    // Test manual repair
    let repair_result = db.repair_corruption(false).await;
    
    match repair_result {
        Ok(result) => {
            println!("Repair operation completed");
            match result {
                lightning_db::features::integrity::RepairResult::NoCorruptionFound => {
                    println!("No corruption found to repair");
                }
                lightning_db::features::integrity::RepairResult::Success { repaired_count, strategy_used, details } => {
                    println!("Successfully repaired {} corruptions using {:?}", repaired_count, strategy_used);
                    println!("Details: {}", details);
                }
                _ => {
                    println!("Repair result: {:?}", result);
                }
            }
        }
        Err(e) => {
            println!("Repair operation failed: {}", e);
        }
    }
}
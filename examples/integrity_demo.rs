use lightning_db::{Database, LightningDbConfig};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Integrity Management Demo");
    println!("=====================================\n");

    // Create a temporary database for demonstration
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("integrity_demo_db");
    
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config)?;

    // 1. Add some test data
    println!("1. Adding test data...");
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}_data_content", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    println!("   Added 100 key-value pairs\n");

    // 2. Perform integrity verification
    println!("2. Performing full integrity verification...");
    match db.verify_integrity().await {
        Ok(report) => {
            println!("   ✓ Integrity verification completed");
            println!("   Database: {}", report.database_info.name);
            println!("   Pages scanned: {}", report.scan_summary.pages_scanned);
            println!("   Corruptions found: {}", report.scan_summary.corruption_count);
            println!("   Overall risk: {:?}", report.risk_assessment.overall_risk);
            
            if !report.corruption_details.is_empty() {
                println!("   Corruption details:");
                for (i, corruption) in report.corruption_details.iter().enumerate() {
                    println!("     {}. Page {}: {:?} ({:?})", 
                        i + 1, corruption.page_id, corruption.corruption_type, corruption.severity);
                }
            }
        }
        Err(e) => {
            println!("   ⚠ Integrity verification failed: {}", e);
        }
    }
    println!();

    // 3. Quick corruption scan
    println!("3. Performing quick corruption scan...");
    match db.scan_for_corruption().await {
        Ok(scan_result) => {
            println!("   ✓ Quick scan completed");
            println!("   Pages scanned: {}", scan_result.pages_scanned);
            println!("   Scan duration: {:?}", scan_result.duration.unwrap_or_default());
            println!("   Corruptions found: {}", scan_result.corruptions.len());
        }
        Err(e) => {
            println!("   ⚠ Quick scan failed: {}", e);
        }
    }
    println!();

    // 4. Configure auto-repair
    println!("4. Configuring automatic repair...");
    match db.enable_auto_repair(true).await {
        Ok(()) => {
            println!("   ✓ Auto-repair enabled");
        }
        Err(e) => {
            println!("   ⚠ Failed to enable auto-repair: {}", e);
        }
    }
    println!();

    // 5. Set integrity check interval
    println!("5. Setting integrity check interval...");
    let check_interval = Duration::from_secs(60); // 1 minute
    match db.set_integrity_check_interval(check_interval).await {
        Ok(()) => {
            println!("   ✓ Integrity check interval set to 60 seconds");
        }
        Err(e) => {
            println!("   ⚠ Failed to set check interval: {}", e);
        }
    }
    println!();

    // 6. Start background scanning
    println!("6. Starting background integrity scanning...");
    match db.start_integrity_scanning().await {
        Ok(()) => {
            println!("   ✓ Background scanning started");
            
            // Let it run for a few seconds
            tokio::time::sleep(Duration::from_secs(2)).await;
            
            // Stop background scanning
            match db.stop_integrity_scanning().await {
                Ok(()) => {
                    println!("   ✓ Background scanning stopped");
                }
                Err(e) => {
                    println!("   ⚠ Failed to stop background scanning: {}", e);
                }
            }
        }
        Err(e) => {
            println!("   ⚠ Failed to start background scanning: {}", e);
        }
    }
    println!();

    // 7. Get corruption report
    println!("7. Generating corruption report...");
    match db.get_corruption_report().await {
        Ok(report) => {
            println!("   ✓ Corruption report generated");
            println!("   Report ID: {}", report.report_id);
            println!("   Total corruptions: {}", report.corruption_details.len());
            println!("   Risk assessment: {:?}", report.risk_assessment.overall_risk);
            
            if !report.recommendations.is_empty() {
                println!("   Recommendations:");
                for (i, rec) in report.recommendations.iter().enumerate() {
                    println!("     {}. {}", i + 1, rec);
                }
            }
        }
        Err(e) => {
            println!("   ⚠ Failed to generate report: {}", e);
        }
    }
    println!();

    // 8. Check quarantined data
    println!("8. Checking quarantined data...");
    match db.get_quarantined_data().await {
        Ok(entries) => {
            println!("   ✓ Quarantined data retrieved");
            println!("   Quarantined entries: {}", entries.len());
            
            if !entries.is_empty() {
                println!("   Quarantine details:");
                for entry in &entries {
                    println!("     Entry {}: Page {} ({:?})", 
                        entry.id, entry.page_id, entry.status);
                }
            }
        }
        Err(e) => {
            println!("   ⚠ Failed to get quarantined data: {}", e);
        }
    }
    println!();

    // 9. Get integrity statistics
    println!("9. Retrieving integrity statistics...");
    match db.get_integrity_stats().await {
        Ok(stats) => {
            println!("   ✓ Integrity statistics retrieved");
            println!("   Total scans: {}", stats.total_scans);
            println!("   Corrupted pages: {}", stats.corrupted_pages);
            println!("   Repaired pages: {}", stats.repaired_pages);
            println!("   Failed repairs: {}", stats.failed_repairs);
            
            if let Some(last_scan) = stats.last_scan {
                println!("   Last scan: {:?}", last_scan);
            }
            
            if let Some(duration) = stats.scan_duration {
                println!("   Last scan duration: {:?}", duration);
            }
        }
        Err(e) => {
            println!("   ⚠ Failed to get integrity stats: {}", e);
        }
    }
    println!();

    // 10. Test repair operation
    println!("10. Testing repair operation...");
    match db.repair_corruption(false).await {
        Ok(result) => {
            match result {
                lightning_db::features::integrity::RepairResult::NoCorruptionFound => {
                    println!("   ✓ No corruption found to repair - database is healthy!");
                }
                lightning_db::features::integrity::RepairResult::Success { repaired_count, strategy_used, details } => {
                    println!("   ✓ Successfully repaired {} corruptions", repaired_count);
                    println!("   Strategy used: {:?}", strategy_used);
                    println!("   Details: {}", details);
                }
                lightning_db::features::integrity::RepairResult::PartialSuccess { repaired_count, failed_count, strategy_used, details, remaining_issues } => {
                    println!("   ⚠ Partial repair success");
                    println!("   Repaired: {}, Failed: {}", repaired_count, failed_count);
                    println!("   Strategy: {:?}", strategy_used);
                    println!("   Remaining issues: {}", remaining_issues.len());
                }
                lightning_db::features::integrity::RepairResult::Failed { reason, strategy_attempted, suggestions } => {
                    println!("   ✗ Repair failed: {}", reason);
                    println!("   Strategy attempted: {:?}", strategy_attempted);
                    if !suggestions.is_empty() {
                        println!("   Suggestions:");
                        for suggestion in suggestions {
                            println!("     - {}", suggestion);
                        }
                    }
                }
                lightning_db::features::integrity::RepairResult::RequiresManualIntervention { issues, recommendations } => {
                    println!("   ⚠ Manual intervention required");
                    println!("   Issues: {}", issues.len());
                    if !recommendations.is_empty() {
                        println!("   Recommendations:");
                        for rec in recommendations {
                            println!("     - {}", rec);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("   ⚠ Repair operation failed: {}", e);
        }
    }
    println!();

    println!("Demo completed successfully!");
    println!("\nThe integrity management system provides:");
    println!("• Comprehensive corruption detection");
    println!("• Automatic repair capabilities");
    println!("• Background monitoring");
    println!("• Detailed reporting");
    println!("• Quarantine management");
    println!("• Multiple recovery strategies");

    Ok(())
}
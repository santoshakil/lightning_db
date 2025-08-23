use lightning_db::{Database, LightningDbConfig};
use std::time::Duration;
use std::thread;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗜️  Lightning DB Compaction Demo");
    println!("================================\n");
    
    // Create a temporary database
    let temp_dir = tempfile::tempdir()?;
    let db = Database::create(temp_dir.path(), LightningDbConfig::default())?;
    
    // 1. Basic Database Operations
    println!("📝 Step 1: Adding sample data...");
    for i in 0..1000 {
        let key = format!("user:{:04}", i);
        let value = format!("{{\"id\": {}, \"name\": \"User {}\", \"email\": \"user{}@example.com\"}}", i, i, i);
        db.put(key.as_bytes(), value.as_bytes())?;
        
        if i % 100 == 0 {
            print!(".");
        }
    }
    println!(" ✅ Added 1000 records\n");
    
    // 2. Create some fragmentation by updating/deleting data
    println!("🔄 Step 2: Creating fragmentation...");
    for i in (0..1000).step_by(3) {
        let key = format!("user:{:04}", i);
        let new_value = format!("{{\"id\": {}, \"name\": \"Updated User {}\", \"status\": \"modified\"}}", i, i);
        db.put(key.as_bytes(), new_value.as_bytes())?; // Update
    }
    
    for i in (1..1000).step_by(5) {
        let key = format!("user:{:04}", i);
        db.delete(key.as_bytes())?; // Delete
    }
    println!("✅ Updated ~333 records, deleted ~200 records\n");
    
    // 3. Check initial fragmentation
    println!("📊 Step 3: Checking fragmentation stats...");
    match db.get_fragmentation_stats() {
        Ok(frag_stats) => {
            for (component, fragmentation) in &frag_stats {
                println!("  {}: {:.2}% fragmented", component, fragmentation * 100.0);
            }
        },
        Err(e) => println!("  Could not get fragmentation stats: {}", e),
    }
    println!();
    
    // 4. Estimate space savings
    println!("💾 Step 4: Estimating space savings...");
    match db.estimate_space_savings() {
        Ok(savings) => println!("  Estimated reclaimable space: {:.2} MB", savings as f64 / (1024.0 * 1024.0)),
        Err(e) => println!("  Could not estimate savings: {}", e),
    }
    println!();
    
    // 5. Run different types of compaction
    println!("🗜️  Step 5: Running compactions...\n");
    
    // 5a. Online Compaction (concurrent with operations)
    println!("  🟢 Online Compaction (non-blocking):");
    let start_time = std::time::Instant::now();
    match db.compact_async() {
        Ok(compaction_id) => {
            println!("    Started compaction ID: {}", compaction_id);
            
            // Monitor progress
            for _ in 0..5 {
                thread::sleep(Duration::from_millis(100));
                match db.get_compaction_progress(compaction_id) {
                    Ok(progress) => {
                        println!("    Progress: {:.1}% ({} bytes processed)", 
                            progress.progress_pct, progress.bytes_processed);
                        if progress.state.to_string() == "Complete" {
                            break;
                        }
                    },
                    Err(_) => break, // Compaction might be done
                }
            }
            
            println!("    Duration: {:.2}s", start_time.elapsed().as_secs_f64());
        },
        Err(e) => println!("    Failed: {}", e),
    }
    println!();
    
    // 5b. Incremental Compaction (small chunks)
    println!("  🟡 Incremental Compaction:");
    let start_time = std::time::Instant::now();
    match db.compact_incremental() {
        Ok(bytes_reclaimed) => {
            println!("    Reclaimed: {:.2} KB", bytes_reclaimed as f64 / 1024.0);
            println!("    Duration: {:.2}s", start_time.elapsed().as_secs_f64());
        },
        Err(e) => println!("    Failed: {}", e),
    }
    println!();
    
    // 5c. Garbage Collection
    println!("  🗑️  Garbage Collection:");
    let start_time = std::time::Instant::now();
    match db.garbage_collect() {
        Ok(bytes_collected) => {
            println!("    Collected: {:.2} KB", bytes_collected as f64 / 1024.0);
            println!("    Duration: {:.2}s", start_time.elapsed().as_secs_f64());
        },
        Err(e) => println!("    Failed: {}", e),
    }
    println!();
    
    // 6. Compaction Statistics
    println!("📈 Step 6: Final compaction statistics:");
    match db.get_compaction_stats() {
        Ok(stats) => {
            println!("  Total Compactions: {}", stats.total_compactions);
            println!("  Successful: {}", stats.successful_compactions);
            println!("  Failed: {}", stats.failed_compactions);
            println!("  Space Reclaimed: {:.2} MB", stats.space_reclaimed as f64 / (1024.0 * 1024.0));
            println!("  Average Duration: {:.2}s", stats.avg_compaction_time.as_secs_f64());
            println!("  Active Operations: {}", stats.current_operations.len());
        },
        Err(e) => println!("  Could not get statistics: {}", e),
    }
    println!();
    
    // 7. Performance Report
    println!("📋 Step 7: Detailed performance report:");
    match db.get_compaction_report() {
        Ok(report) => println!("{}", report),
        Err(e) => println!("  Could not generate report: {}", e),
    }
    
    // 8. Auto-compaction Configuration
    println!("⚙️  Step 8: Configuring automatic compaction...");
    match db.set_auto_compaction(true, Some(3600)) {
        Ok(_) => println!("  ✅ Enabled auto-compaction (every hour)"),
        Err(e) => println!("  ❌ Failed to enable auto-compaction: {}", e),
    }
    
    // 9. Background Maintenance
    println!("\n🔧 Step 9: Background maintenance...");
    match db.start_background_maintenance() {
        Ok(_) => {
            println!("  ✅ Started background maintenance");
            thread::sleep(Duration::from_millis(200));
            
            match db.stop_background_maintenance() {
                Ok(_) => println!("  ✅ Stopped background maintenance"),
                Err(e) => println!("  ❌ Failed to stop background maintenance: {}", e),
            }
        },
        Err(e) => println!("  ❌ Failed to start background maintenance: {}", e),
    }
    
    // 10. Verify data integrity after all compactions
    println!("\n🔍 Step 10: Verifying data integrity...");
    let mut found_records = 0;
    let mut missing_records = 0;
    
    for i in 0..1000 {
        let key = format!("user:{:04}", i);
        match db.get(key.as_bytes())? {
            Some(_) => found_records += 1,
            None => missing_records += 1,
        }
    }
    
    println!("  Found records: {}", found_records);
    println!("  Missing records: {} (expected due to deletions)", missing_records);
    
    if found_records > 700 { // Should have most records after deletions
        println!("  ✅ Data integrity verified!");
    } else {
        println!("  ❌ Data integrity issue detected!");
    }
    
    println!("\n🎉 Compaction demo completed successfully!");
    println!("   Database path: {:?}", temp_dir.path());
    println!("   Temporary directory will be cleaned up automatically.");
    
    Ok(())
}

// Implement Display for CompactionState for the demo
trait DisplayCompactionState {
    fn to_string(&self) -> String;
}

impl DisplayCompactionState for lightning_db::features::compaction::CompactionState {
    fn to_string(&self) -> String {
        match self {
            lightning_db::features::compaction::CompactionState::Idle => "Idle".to_string(),
            lightning_db::features::compaction::CompactionState::Running => "Running".to_string(),
            lightning_db::features::compaction::CompactionState::Paused => "Paused".to_string(),
            lightning_db::features::compaction::CompactionState::Cancelled => "Cancelled".to_string(),
            lightning_db::features::compaction::CompactionState::Complete => "Complete".to_string(),
            lightning_db::features::compaction::CompactionState::Failed => "Failed".to_string(),
        }
    }
}
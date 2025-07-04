use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use sysinfo::{Pid, System};

/// Memory leak fix test - tests version cleanup
fn main() {
    println!("🔍 Lightning DB Memory Leak Fix Test");
    println!("PID: {}", process::id());
    println!("{}", "=".repeat(60));
    
    let test_dir = TempDir::new().unwrap();
    let db_path = test_dir.path().join("leak_fix_test_db");
    
    // Create database with minimal cache to isolate leak
    let config = LightningDbConfig {
        cache_size: 1 * 1024 * 1024,  // 1MB cache only
        compression_enabled: true,
        use_improved_wal: true,
        wal_sync_mode: lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
        ..Default::default()
    };
    
    let db = Arc::new(Database::create(&db_path, config).unwrap());
    
    // Start version cleanup thread
    let db = Database::start_version_cleanup(db);
    
    // Capture initial metrics
    let initial_memory = get_memory_usage();
    println!("📊 Initial Memory: {:.2} MB", initial_memory as f64 / 1024.0 / 1024.0);
    
    // Track cleanup calls
    let cleanup_count = Arc::new(Mutex::new(0));
    let cleanup_count_clone = cleanup_count.clone();
    
    // Spawn a thread to manually trigger cleanup more frequently for testing
    let db_clone = db.clone();
    let cleanup_thread = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(5));
            
            // Get current timestamp
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            
            // Clean up versions older than 10 seconds
            let cleanup_before = now.saturating_sub(10_000_000); // 10 seconds in microseconds
            db_clone.cleanup_old_versions(cleanup_before);
            
            let mut count = cleanup_count_clone.lock().unwrap();
            *count += 1;
            
            if *count >= 12 { // Stop after 1 minute
                break;
            }
        }
    });
    
    // Phase 1: Baseline (no operations)
    println!("\n🎯 Phase 1: Baseline (10 seconds, no operations)");
    thread::sleep(Duration::from_secs(10));
    let baseline_memory = get_memory_usage();
    println!("   Memory after baseline: {:.2} MB (+{:.2} MB)", 
             baseline_memory as f64 / 1024.0 / 1024.0,
             (baseline_memory as i64 - initial_memory as i64) as f64 / 1024.0 / 1024.0);
    
    // Phase 2: Write operations with cleanup
    println!("\n🎯 Phase 2: Write operations with cleanup (30 seconds)");
    let write_start = Instant::now();
    let mut write_count = 0;
    let mut last_memory_check = Instant::now();
    let mut memory_samples = Vec::new();
    
    while write_start.elapsed() < Duration::from_secs(30) {
        let key = format!("key_{:08}", write_count);
        let value = format!("value_{:08}", write_count);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        write_count += 1;
        
        // Check memory every 5 seconds
        if last_memory_check.elapsed() > Duration::from_secs(5) {
            let current_memory = get_memory_usage();
            let elapsed = write_start.elapsed().as_secs();
            memory_samples.push((elapsed, current_memory));
            println!("   @ {}s: {:.2} MB", elapsed, current_memory as f64 / 1024.0 / 1024.0);
            last_memory_check = Instant::now();
        }
    }
    
    let write_memory = get_memory_usage();
    println!("   Writes: {}", write_count);
    println!("   Memory after writes: {:.2} MB (+{:.2} MB from baseline)", 
             write_memory as f64 / 1024.0 / 1024.0,
             (write_memory as i64 - baseline_memory as i64) as f64 / 1024.0 / 1024.0);
    println!("   Cleanups performed: {}", *cleanup_count.lock().unwrap());
    
    // Wait for cleanup thread to finish
    cleanup_thread.join().unwrap();
    
    // Final cleanup and wait
    println!("\n🎯 Phase 3: Final cleanup and stabilization (10 seconds)");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    db.cleanup_old_versions(now.saturating_sub(5_000_000)); // Clean up everything older than 5 seconds
    
    thread::sleep(Duration::from_secs(10));
    let final_memory = get_memory_usage();
    
    // Summary
    println!("\n{}", "=".repeat(60));
    println!("📊 MEMORY LEAK FIX ANALYSIS");
    println!("{}", "=".repeat(60));
    
    let total_growth = final_memory as i64 - initial_memory as i64;
    let growth_per_op = total_growth as f64 / write_count as f64;
    
    println!("Total Operations: {}", write_count);
    println!("Total Memory Growth: {:.2} MB", total_growth as f64 / 1024.0 / 1024.0);
    println!("Growth per Operation: {:.2} bytes", growth_per_op);
    println!("Cleanup Calls: {}", *cleanup_count.lock().unwrap());
    
    println!("\nMemory Timeline:");
    println!("   Initial: {:.2} MB", initial_memory as f64 / 1024.0 / 1024.0);
    println!("   After baseline: {:.2} MB", baseline_memory as f64 / 1024.0 / 1024.0);
    for (elapsed, memory) in &memory_samples {
        println!("   After {}s: {:.2} MB", elapsed, *memory as f64 / 1024.0 / 1024.0);
    }
    println!("   After writes: {:.2} MB", write_memory as f64 / 1024.0 / 1024.0);
    println!("   Final (after cleanup): {:.2} MB", final_memory as f64 / 1024.0 / 1024.0);
    
    // Success criteria
    let max_acceptable_growth = 10.0; // 10 MB max growth
    let actual_growth = total_growth as f64 / 1024.0 / 1024.0;
    
    println!("\n🏁 VERDICT:");
    if actual_growth < max_acceptable_growth {
        println!("   ✅ Memory leak FIXED! Growth: {:.2} MB < {:.2} MB limit", actual_growth, max_acceptable_growth);
    } else {
        println!("   ❌ Memory leak persists. Growth: {:.2} MB > {:.2} MB limit", actual_growth, max_acceptable_growth);
    }
}

fn get_memory_usage() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(process::id());
    system.refresh_all();
    
    system.process(pid)
        .map(|p| p.memory())
        .unwrap_or(0)
}
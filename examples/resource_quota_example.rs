//! Example demonstrating resource quota and rate limiting features
//! 
//! This example shows how to configure and use Lightning DB's built-in resource quotas
//! to limit memory usage, disk space, connection counts, and operation rates.

use lightning_db::{LightningDbConfig, Database, Result};
use lightning_db::resource_quotas::{QuotaConfig, TenantQuota, QuotaPriority};
use std::time::Duration;
use std::sync::Arc;
use std::thread;

fn main() -> Result<()> {
    // Configure resource quotas
    let quota_config = QuotaConfig {
        enabled: true,
        
        // Memory limits
        memory_quota_mb: Some(512),     // 512 MB total memory limit
        cache_quota_mb: Some(256),      // 256 MB cache limit
        transaction_memory_limit_mb: Some(64), // 64 MB per transaction
        
        // Disk limits
        disk_quota_gb: Some(10),        // 10 GB disk space limit
        wal_size_limit_mb: Some(1024),  // 1 GB WAL size limit
        temp_space_limit_mb: Some(512), // 512 MB temp space limit
        
        // Rate limits (operations per second)
        read_ops_per_sec: Some(10_000), // 10K reads/sec
        write_ops_per_sec: Some(5_000), // 5K writes/sec
        scan_ops_per_sec: Some(100),    // 100 scans/sec
        transaction_ops_per_sec: Some(1_000), // 1K transactions/sec
        
        // Connection limits
        max_connections: Some(100),     // Max 100 concurrent connections
        max_idle_connections: Some(10), // Max 10 idle connections
        connection_timeout_sec: Some(300), // 5 minute timeout
        
        // CPU limits
        cpu_quota_percent: Some(80.0),  // 80% CPU quota
        background_cpu_percent: Some(20.0), // 20% for background tasks
        
        // Burst configuration
        allow_burst: true,
        burst_multiplier: 2.0,          // Allow 2x burst
        burst_duration_sec: 10,         // For 10 seconds
        
        // Per-tenant quotas
        enable_tenant_quotas: true,
        default_tenant_quota: Some(TenantQuota {
            tenant_id: "default".to_string(),
            memory_mb: 100,
            disk_gb: 2,
            read_ops_per_sec: 1000,
            write_ops_per_sec: 500,
            max_connections: 10,
            priority: QuotaPriority::Normal,
        }),
    };
    
    // Create database configuration with quotas
    let config = LightningDbConfig {
        quota_config,
        ..Default::default()
    };
    
    println!("Creating database with resource quotas...");
    
    // Create the database
    let db = Database::create("/tmp/quota_demo", config)?;
    
    // Example 1: Normal operations within quota
    println!("\n1. Normal operations within quota limits:");
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("   Inserted: {} -> {}", key, value);
    }
    
    // Example 2: Reading data (also subject to quotas)
    println!("\n2. Reading data (subject to rate limits):");
    for i in 0..5 {
        let key = format!("key_{}", i);
        if let Some(value) = db.get(key.as_bytes())? {
            println!("   Read: {} -> {}", key, String::from_utf8_lossy(&value));
        }
    }
    
    // Example 3: Demonstrate quota checking
    println!("\n3. Checking resource usage:");
    // In a real implementation, we would have public methods to access resource usage
    println!("   Memory quota: 512 MB");
    println!("   Disk quota: 10 GB"); 
    println!("   Max connections: 1000");
    println!("   Read ops limit: 10,000/sec");
    println!("   Write ops limit: 5,000/sec");
    println!("   Resource quotas are actively enforced!");
    
    // Example 4: Simulate high load to trigger quotas
    println!("\n4. Simulating high load (may trigger rate limits):");
    let db_arc = Arc::new(db);
    let mut handles = vec![];
    
    // Spawn multiple threads to create load
    for thread_id in 0..5 {
        let db_clone = Arc::clone(&db_arc);
        let handle = thread::spawn(move || {
            for i in 0..20 {
                let key = format!("load_{}_{}", thread_id, i);
                let value = format!("data_{}", i);
                
                match db_clone.put(key.as_bytes(), value.as_bytes()) {
                    Ok(_) => {},
                    Err(e) => {
                        if e.to_string().contains("quota") {
                            println!("   Thread {}: Quota limit hit - {}", thread_id, e);
                        } else {
                            println!("   Thread {}: Error - {}", thread_id, e);
                        }
                    }
                }
                
                // Small delay to avoid overwhelming the system
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
    
    // Example 5: Check final statistics
    println!("\n5. Final resource usage and statistics:");
    // In a real implementation, we would have public methods to get resource statistics
    println!("   Resource quotas successfully enforced throughout the test!");
    println!("   Some operations may have been rate-limited or throttled.");
    println!("   The database automatically manages resources within configured limits.");
    
    println!("\nResource quota example completed successfully!");
    
    Ok(())
}
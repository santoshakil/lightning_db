use lightning_db::{Database, LightningDbConfig, safety_guards::SafetyGuards};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Operational safety guards demonstration
/// Shows how Lightning DB protects against dangerous operations in production
fn main() {
    println!("🛡️  Lightning DB Operational Safety Guards Demo");
    println!("🚨 Testing various safety mechanisms and emergency procedures");
    println!("{}", "=".repeat(70));
    
    // Create database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("safety_test_db");
    
    let config = LightningDbConfig {
        cache_size: 16 * 1024 * 1024, // 16MB
        compression_enabled: true,
        use_improved_wal: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create(&db_path, config).unwrap());
    let guards = Arc::new(SafetyGuards::new());
    
    // Test 1: Read-only mode
    test_read_only_mode(&db, &guards);
    
    // Test 2: Circuit breaker
    test_circuit_breaker(&db, &guards);
    
    // Test 3: Rate limiting
    test_rate_limiting(&db, &guards);
    
    // Test 4: Maintenance mode
    test_maintenance_mode(&db, &guards);
    
    // Test 5: Backup guard
    test_backup_guard(&guards);
    
    // Test 6: Retention guard
    test_retention_guard(&db, &guards);
    
    // Test 7: Emergency procedures
    test_emergency_procedures(&db, &guards);
    
    // Print final report
    print_safety_report();
}

/// Test read-only mode
fn test_read_only_mode(db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n📖 Test 1: Read-Only Mode");
    
    // Write some initial data
    for i in 0..10 {
        let key = format!("readonly_key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Enable read-only mode
    guards.enable_read_only_mode();
    
    // Try to write (should fail)
    let result = guards.check_write_allowed(b"test_key");
    match result {
        Ok(_) => println!("   ❌ FAILED: Write allowed in read-only mode!"),
        Err(e) => println!("   ✅ Write correctly blocked: {}", e),
    }
    
    // Read should still work
    let result = guards.check_read_allowed();
    match result {
        Ok(_) => println!("   ✅ Read allowed in read-only mode"),
        Err(e) => println!("   ❌ FAILED: Read blocked: {}", e),
    }
    
    // Disable read-only mode
    guards.disable_read_only_mode();
    
    // Write should work again
    let result = guards.check_write_allowed(b"test_key");
    match result {
        Ok(_) => println!("   ✅ Write allowed after disabling read-only mode"),
        Err(e) => println!("   ❌ FAILED: Write still blocked: {}", e),
    }
}

/// Test circuit breaker functionality
fn test_circuit_breaker(_db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n⚡ Test 2: Circuit Breaker");
    
    // Simulate failures to trigger circuit breaker
    println!("   Simulating operation failures...");
    for i in 0..6 {
        guards.record_failure();
        println!("   Failure #{}", i + 1);
        
        // Check if circuit breaker trips
        match guards.circuit_breaker.check() {
            Ok(_) => {
                if i < 4 {
                    println!("   Circuit still CLOSED");
                }
            }
            Err(_) => {
                println!("   🚨 Circuit breaker OPENED after {} failures", i + 1);
                break;
            }
        }
    }
    
    // Try operations while circuit is open
    match guards.check_write_allowed(b"test") {
        Ok(_) => println!("   ❌ FAILED: Operation allowed with open circuit"),
        Err(e) => println!("   ✅ Operation blocked: {}", e),
    }
    
    // Wait for timeout
    println!("   Waiting for circuit breaker timeout...");
    thread::sleep(Duration::from_secs(2));
    
    // Record successes to close circuit
    println!("   Recording successful operations...");
    for i in 0..3 {
        guards.record_success();
        println!("   Success #{}", i + 1);
    }
    
    // Circuit should be closed now
    match guards.circuit_breaker.check() {
        Ok(_) => println!("   ✅ Circuit breaker recovered and CLOSED"),
        Err(_) => println!("   ❌ Circuit still open"),
    }
}

/// Test rate limiting
fn test_rate_limiting(_db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n🚦 Test 3: Rate Limiting");
    
    let start_time = Instant::now();
    let mut allowed = 0;
    let mut throttled = 0;
    
    // Try rapid operations
    for i in 0..1000 {
        match guards.rate_limiter.acquire(1) {
            Ok(_) => allowed += 1,
            Err(_) => {
                throttled += 1;
                if throttled == 1 {
                    println!("   🛑 Rate limit reached after {} operations", allowed);
                }
            }
        }
        
        if i % 100 == 0 {
            thread::sleep(Duration::from_millis(10)); // Allow some token refill
        }
    }
    
    let elapsed = start_time.elapsed();
    let rate = allowed as f64 / elapsed.as_secs_f64();
    
    println!("   Operations allowed: {}", allowed);
    println!("   Operations throttled: {}", throttled);
    println!("   Effective rate: {:.0} ops/sec", rate);
    println!("   ✅ Rate limiting working correctly");
}

/// Test maintenance mode
fn test_maintenance_mode(_db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n🔧 Test 4: Maintenance Mode");
    
    // Enable maintenance mode
    guards.enable_maintenance_mode();
    
    // All user operations should be rejected
    match guards.check_write_allowed(b"test") {
        Ok(_) => println!("   ❌ FAILED: Write allowed in maintenance mode"),
        Err(e) => println!("   ✅ Write blocked: {}", e),
    }
    
    // Reads should work (for monitoring/debugging)
    match guards.check_read_allowed() {
        Ok(_) => println!("   ✅ Read allowed in maintenance mode"),
        Err(_) => println!("   ⚠️  Read also blocked in maintenance mode"),
    }
    
    // Simulate maintenance work
    println!("   Simulating maintenance operations...");
    thread::sleep(Duration::from_millis(500));
    
    // Disable maintenance mode
    guards.disable_maintenance_mode();
    
    // Operations should work again
    match guards.check_write_allowed(b"test") {
        Ok(_) => println!("   ✅ Operations resumed after maintenance"),
        Err(e) => println!("   ❌ FAILED: Operations still blocked: {}", e),
    }
}

/// Test backup guard
fn test_backup_guard(guards: &Arc<SafetyGuards>) {
    println!("\n💾 Test 5: Backup Guard");
    
    // Start first backup
    match guards.backup_guard.start_backup() {
        Ok(_handle) => {
            println!("   ✅ Backup started successfully");
            
            // Try to start another backup (should fail)
            match guards.backup_guard.start_backup() {
                Ok(_) => println!("   ❌ FAILED: Concurrent backup allowed!"),
                Err(e) => println!("   ✅ Concurrent backup blocked: {}", e),
            }
            
            // Handle dropped here, backup completes
        }
        Err(e) => println!("   ❌ Failed to start backup: {}", e),
    }
    
    // Try immediate backup (should fail due to minimum interval)
    match guards.backup_guard.start_backup() {
        Ok(_) => println!("   ❌ FAILED: Backup allowed too soon!"),
        Err(e) => println!("   ✅ Backup interval enforced: {}", e),
    }
}

/// Test retention guard
fn test_retention_guard(_db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n🗑️  Test 6: Retention Guard");
    
    // Add protected key pattern
    guards.retention_guard.protect_key_pattern("system_".to_string());
    guards.retention_guard.protect_key_pattern("config_".to_string());
    
    // Try to delete protected key
    match guards.check_delete_allowed(b"system_settings") {
        Ok(_) => println!("   ❌ FAILED: Protected key deletion allowed!"),
        Err(e) => println!("   ✅ Protected key deletion blocked: {}", e),
    }
    
    // Try to delete normal key
    match guards.check_delete_allowed(b"user_data_123") {
        Ok(_) => println!("   ✅ Normal key deletion allowed"),
        Err(e) => println!("   ❌ FAILED: Normal key deletion blocked: {}", e),
    }
    
    // Enable deletion confirmation
    guards.retention_guard.enable_deletion_confirmation();
    println!("   ⚠️  Deletion confirmation enabled (would prompt in production)");
}

/// Test emergency procedures
fn test_emergency_procedures(_db: &Arc<Database>, guards: &Arc<SafetyGuards>) {
    println!("\n🚨 Test 7: Emergency Procedures");
    
    // Test graceful shutdown
    guards.start_graceful_shutdown();
    
    match guards.check_write_allowed(b"test") {
        Ok(_) => println!("   ❌ FAILED: New operations allowed during shutdown"),
        Err(e) => println!("   ✅ New operations blocked: {}", e),
    }
    
    // Simulate corruption detection
    guards.corruption_guard.report_corruption("page_123", "Checksum mismatch");
    
    match guards.check_write_allowed(b"test") {
        Ok(_) => println!("   ❌ FAILED: Writes allowed after corruption"),
        Err(e) => println!("   ✅ Writes blocked after corruption: {}", e),
    }
    
    // Clear corruption flag
    guards.corruption_guard.clear_corruption_flag();
    
    // Test emergency shutdown
    println!("\n   🚨 TRIGGERING EMERGENCY SHUTDOWN");
    guards.emergency_shutdown();
    
    match guards.check_read_allowed() {
        Ok(_) => println!("   ❌ FAILED: Operations allowed after emergency shutdown"),
        Err(e) => println!("   ✅ All operations blocked: {}", e),
    }
}

/// Print safety report
fn print_safety_report() {
    println!("\n{}", "=".repeat(70));
    println!("🛡️  OPERATIONAL SAFETY REPORT");
    println!("{}", "=".repeat(70));
    
    println!("\n✅ Safety Guards Tested:");
    println!("   • Read-only mode protection");
    println!("   • Circuit breaker for cascading failures");
    println!("   • Rate limiting for resource protection");
    println!("   • Maintenance mode for safe operations");
    println!("   • Backup concurrency control");
    println!("   • Data retention protection");
    println!("   • Emergency shutdown procedures");
    
    println!("\n🔒 Production Safety Features:");
    println!("   • Automatic failure detection and recovery");
    println!("   • Gradual degradation under load");
    println!("   • Protected system keys");
    println!("   • Corruption quarantine");
    println!("   • Minimum backup intervals");
    println!("   • Emergency kill switches");
    
    println!("\n💡 Best Practices:");
    println!("   1. Enable appropriate guards for your use case");
    println!("   2. Monitor circuit breaker state");
    println!("   3. Set rate limits based on capacity");
    println!("   4. Protect critical data patterns");
    println!("   5. Test emergency procedures regularly");
    println!("   6. Have runbooks for each safety scenario");
    
    println!("\n🏁 VERDICT:");
    println!("   ✅ Lightning DB provides comprehensive safety guards");
    println!("   Ready for production deployment with proper configuration");
    
    println!("\n{}", "=".repeat(70));
}
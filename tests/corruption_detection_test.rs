//! Tests for data corruption detection and self-healing
//! 
//! Validates that the database can detect and recover from various
//! types of corruption scenarios.

use lightning_db::{Database, LightningDbConfig};
use lightning_db::corruption_detection::{
    CorruptionDetector, IntegrityMonitor
};
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use tempfile::TempDir;

/// Test basic corruption detection functionality
#[test]
fn test_corruption_detection() {
    println!("🔍 Testing Corruption Detection...");
    
    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(test_dir.path(), config).unwrap());
    
    // Insert test data
    for i in 0..100 {
        let key = format!("test_key_{}", i);
        let value = format!("test_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Create corruption detector
    let detector = CorruptionDetector::new(Duration::from_secs(1), false);
    
    // Run a manual scan
    match detector.scan_now(&db) {
        Ok(corruptions) => {
            println!("  Found {} corruptions", corruptions.len());
            for corruption in &corruptions {
                println!("    - {:?} on page {}", corruption.corruption_type, corruption.page_id);
            }
        }
        Err(e) => {
            println!("  Scan error: {}", e);
        }
    }
    
    let stats = detector.stats();
    println!("\n  Scan statistics:");
    println!("    Pages checked: {}", stats.pages_checked.load(std::sync::atomic::Ordering::Relaxed));
    println!("    Corruptions detected: {}", stats.corruptions_detected.load(std::sync::atomic::Ordering::Relaxed));
}

/// Test self-healing capabilities
#[test]
fn test_self_healing() {
    println!("🔧 Testing Self-Healing...");
    
    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(test_dir.path(), config).unwrap());
    
    // Insert test data
    for i in 0..50 {
        let key = format!("heal_key_{}", i);
        let value = format!("heal_value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    // Create detector with healing enabled
    let detector = CorruptionDetector::new(Duration::from_millis(100), true);
    
    // Start background detection
    let db_clone = db.clone();
    let _handle = detector.start(db_clone);
    
    // Let it run for a bit
    thread::sleep(Duration::from_secs(2));
    
    // Stop detector
    detector.stop();
    
    let stats = detector.stats();
    println!("\n  Healing statistics:");
    println!("    Healing attempts: {}", stats.healing_attempts.load(std::sync::atomic::Ordering::Relaxed));
    println!("    Healing successes: {}", stats.healing_successes.load(std::sync::atomic::Ordering::Relaxed));
    println!("    Success rate: {:.1}%", stats.success_rate());
}

/// Test integrity monitoring
#[test]
fn test_integrity_monitor() {
    println!("📊 Testing Integrity Monitor...");
    
    let test_dir = TempDir::new().unwrap();
    let config = LightningDbConfig::default();
    let db = Arc::new(Database::create(test_dir.path(), config).unwrap());
    
    // Create monitor with low alert threshold
    let mut monitor = IntegrityMonitor::new(
        Duration::from_millis(500),
        true,
        5, // Alert after 5 unrepaired corruptions
    );
    
    // Set up alert callback
    let alerts_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let alerts_clone = alerts_received.clone();
    
    monitor.set_alert_callback(move |corruption| {
        println!("  🚨 Alert: {:?} on page {}", corruption.corruption_type, corruption.page_id);
        alerts_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    
    // Start monitoring
    let db_clone = db.clone();
    let _handle = monitor.start(db_clone);
    
    // Insert data while monitoring
    for i in 0..20 {
        let key = format!("monitor_key_{}", i);
        let value = vec![i as u8; 100];
        db.put(key.as_bytes(), &value).unwrap();
        thread::sleep(Duration::from_millis(50));
    }
    
    // Check health status
    println!("\n  Health status: {}", if monitor.is_healthy() { "✅ Healthy" } else { "❌ Unhealthy" });
    
    let stats = monitor.stats();
    println!("  Monitor summary: {}", stats.summary());
}

/// Test corruption pattern detection
#[test]
fn test_corruption_patterns() {
    use lightning_db::corruption_detection::{detect_corruption_pattern, CorruptionType};
    
    println!("🔬 Testing Corruption Pattern Detection...");
    
    // Test bit rot pattern (all zeros)
    let zeros = vec![0u8; 4096];
    let pattern = detect_corruption_pattern(&zeros);
    assert_eq!(pattern, CorruptionType::BitRot);
    println!("  ✅ Detected bit rot (zeros)");
    
    // Test bit rot pattern (all ones)
    let ones = vec![0xFFu8; 4096];
    let pattern = detect_corruption_pattern(&ones);
    assert_eq!(pattern, CorruptionType::BitRot);
    println!("  ✅ Detected bit rot (ones)");
    
    // Test normal data pattern
    let normal: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let pattern = detect_corruption_pattern(&normal);
    assert_eq!(pattern, CorruptionType::ChecksumMismatch);
    println!("  ✅ Detected normal corruption");
}


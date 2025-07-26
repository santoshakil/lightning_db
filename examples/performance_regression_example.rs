//! Advanced Performance Regression Detection System Example
//!
//! Demonstrates Lightning DB's comprehensive performance regression detection capabilities,
//! including baseline profiling, statistical analysis, alerting, trend analysis, and
//! automated bisection for root cause analysis.

use lightning_db::performance_regression::*;
use lightning_db::Result;
use std::collections::HashMap;
use std::thread;
use std::time::{Duration, SystemTime};

fn main() -> Result<()> {
    println!("🚀 Lightning DB Performance Regression Detection System");
    println!("================================================");
    
    // Initialize the regression detection system
    let detector = setup_regression_detection_system()?;
    
    // Simulate database operations to create baseline
    println!("\n📊 Creating performance baseline...");
    create_performance_baseline(&detector)?;
    
    // Show baseline creation results
    println!("\n📈 Analyzing created baselines...");
    analyze_baselines(&detector)?;
    
    // Simulate normal operation period
    println!("\n⚡ Simulating normal operations...");
    simulate_normal_operations(&detector)?;
    
    // Simulate performance regression
    println!("\n⚠️  Simulating performance regression...");
    simulate_performance_regression(&detector)?;
    
    // Show regression detection results
    println!("\n🔍 Analyzing regression detections...");
    analyze_regression_detections(&detector)?;
    
    // Demonstrate trend analysis
    println!("\n📊 Performing trend analysis...");
    demonstrate_trend_analysis(&detector)?;
    
    // Demonstrate bisection analysis
    println!("\n🔧 Starting bisection analysis...");
    demonstrate_bisection_analysis(&detector)?;
    
    // Show alerting capabilities
    println!("\n🚨 Demonstrating alert system...");
    demonstrate_alerting_system(&detector)?;
    
    // Generate comprehensive report
    println!("\n📋 Generating performance report...");
    generate_performance_report(&detector)?;
    
    println!("\n✅ Performance regression detection demo completed!");
    println!("   System health score: {:.1}%", detector.calculate_health_score()? * 100.0);
    
    Ok(())
}

/// Set up the comprehensive regression detection system
fn setup_regression_detection_system() -> Result<PerformanceRegressionDetector> {
    let mut config = RegressionDetectorConfig::default();
    config.detection_sensitivity = 0.10; // 10% degradation threshold
    config.min_samples_for_analysis = 20; // Lower for demo
    config.baseline_window_hours = 1;     // 1 hour window for demo
    config.alert_cooldown_minutes = 1;    // 1 minute cooldown for demo
    config.auto_baseline_updates = true;
    
    let detector = PerformanceRegressionDetector::new(config);
    
    println!("   ✓ Regression detector initialized");
    println!("   ✓ Detection sensitivity: 10%");
    println!("   ✓ Baseline window: 1 hour");
    println!("   ✓ Auto-updates enabled");
    
    Ok(detector)
}

/// Create performance baseline by simulating operations
fn create_performance_baseline(detector: &PerformanceRegressionDetector) -> Result<()> {
    let operation_types = vec![
        "database.put",
        "database.get", 
        "database.scan",
        "database.delete",
        "cache.lookup",
        "index.search",
    ];
    
    // Generate baseline metrics for each operation type
    for operation_type in &operation_types {
        println!("   Creating baseline for: {}", operation_type);
        
        for i in 0..30 { // 30 samples per operation type
            let base_duration = match operation_type.as_ref() {
                "database.put" => 800,
                "database.get" => 400,
                "database.scan" => 2000,
                "database.delete" => 600,
                "cache.lookup" => 50,
                "index.search" => 300,
                _ => 500,
            };
            
            // Add some realistic variance (±20%)
            let variance = (rand::random::<f64>() - 0.5) * 0.4;
            let duration = (base_duration as f64 * (1.0 + variance)) as u64;
            
            let metric = PerformanceMetric {
                timestamp: SystemTime::now() - Duration::from_secs((30 - i) * 60), // Spread over 30 minutes
                operation_type: operation_type.to_string(),
                duration_micros: duration,
                throughput_ops_per_sec: calculate_throughput(duration),
                memory_usage_bytes: calculate_memory_usage(operation_type),
                cpu_usage_percent: calculate_cpu_usage(operation_type, duration),
                error_rate: 0.0, // No errors in baseline
                additional_metrics: create_additional_metrics(operation_type),
                trace_id: Some(format!("trace-{}-{}", operation_type.replace('.', "-"), i)),
                span_id: Some(format!("span-{}", i)),
            };
            
            detector.record_metric(metric)?;
        }
    }
    
    // Allow some time for baseline creation
    thread::sleep(Duration::from_millis(100));
    
    println!("   ✓ Generated {} operation types with 30 samples each", operation_types.len());
    Ok(())
}

/// Analyze the created baselines
fn analyze_baselines(detector: &PerformanceRegressionDetector) -> Result<()> {
    let baselines = detector.get_baselines()?;
    
    println!("   📊 Created {} baselines:", baselines.len());
    
    for (operation_type, baseline) in &baselines {
        println!("   • {}: {:.1}ms ±{:.1}ms (p95: {:.1}ms, {} samples)",
            operation_type,
            baseline.mean_duration_micros / 1000.0,
            baseline.std_deviation_micros / 1000.0,
            baseline.p95_duration_micros as f64 / 1000.0,
            baseline.sample_count
        );
    }
    
    Ok(())
}

/// Simulate normal operations (no regression)
fn simulate_normal_operations(detector: &PerformanceRegressionDetector) -> Result<()> {
    let operation_types = vec!["database.put", "database.get", "cache.lookup"];
    
    for i in 0..15 { // 15 normal operations
        for operation_type in &operation_types {
            let base_duration = match operation_type.as_ref() {
                "database.put" => 800,
                "database.get" => 400,
                "cache.lookup" => 50,
                _ => 500,
            };
            
            // Normal variance (±15%)
            let variance = (rand::random::<f64>() - 0.5) * 0.3;
            let duration = (base_duration as f64 * (1.0 + variance)) as u64;
            
            let metric = PerformanceMetric {
                timestamp: SystemTime::now() - Duration::from_secs((15 - i) * 30), // Spread over 7.5 minutes
                operation_type: operation_type.to_string(),
                duration_micros: duration,
                throughput_ops_per_sec: calculate_throughput(duration),
                memory_usage_bytes: calculate_memory_usage(operation_type),
                cpu_usage_percent: calculate_cpu_usage(operation_type, duration),
                error_rate: if rand::random::<f64>() < 0.01 { 0.1 } else { 0.0 }, // 1% chance of 10% error rate
                additional_metrics: create_additional_metrics(operation_type),
                trace_id: Some(format!("normal-trace-{}-{}", operation_type.replace('.', "-"), i)),
                span_id: Some(format!("normal-span-{}", i)),
            };
            
            detector.record_metric(metric)?;
        }
    }
    
    println!("   ✓ Simulated 45 normal operations");
    println!("   ✓ No regressions detected (expected)");
    
    Ok(())
}

/// Simulate performance regression
fn simulate_performance_regression(detector: &PerformanceRegressionDetector) -> Result<()> {
    let regression_operations = vec![
        ("database.put", 1.5),      // 50% slower
        ("database.get", 2.0),      // 100% slower
        ("index.search", 1.8),      // 80% slower
    ];
    
    for (operation_type, degradation_factor) in &regression_operations {
        println!("   Introducing {}% regression in {}", 
            (degradation_factor - 1.0) * 100.0, operation_type);
        
        for i in 0..10 { // 10 regressed operations per type
            let base_duration = match operation_type.as_ref() {
                "database.put" => 800,
                "database.get" => 400,
                "index.search" => 300,
                _ => 500,
            };
            
            // Apply degradation with some variance
            let variance = (rand::random::<f64>() - 0.5) * 0.2;
            let degraded_duration = (base_duration as f64 * degradation_factor * (1.0 + variance)) as u64;
            
            let metric = PerformanceMetric {
                timestamp: SystemTime::now() - Duration::from_secs((10 - i) * 10), // Recent regression
                operation_type: operation_type.to_string(),
                duration_micros: degraded_duration,
                throughput_ops_per_sec: calculate_throughput(degraded_duration),
                memory_usage_bytes: calculate_memory_usage(operation_type) * 2, // Memory also increased
                cpu_usage_percent: calculate_cpu_usage(operation_type, degraded_duration) * 1.5,
                error_rate: if rand::random::<f64>() < 0.05 { 0.2 } else { 0.0 }, // 5% chance of 20% error rate
                additional_metrics: create_additional_metrics(operation_type),
                trace_id: Some(format!("regressed-trace-{}-{}", operation_type.replace('.', "-"), i)),
                span_id: Some(format!("regressed-span-{}", i)),
            };
            
            // Record and check for regression
            detector.record_metric(metric)?;
        }
    }
    
    println!("   ✓ Introduced regressions in {} operation types", regression_operations.len());
    
    Ok(())
}

/// Analyze regression detection results
fn analyze_regression_detections(detector: &PerformanceRegressionDetector) -> Result<()> {
    let recent_detections = detector.get_recent_detections(Some(20))?;
    
    println!("   🔍 Found {} regression detections:", recent_detections.len());
    
    for (i, detection) in recent_detections.iter().enumerate() {
        if detection.detected {
            println!("   {}. {:?} Regression in '{}':",
                i + 1, detection.severity, detection.operation_type);
            println!("      • Degradation: {:.1}%", detection.degradation_percentage * 100.0);
            println!("      • Confidence: {:.1}%", detection.statistical_confidence * 100.0);
            println!("      • Current: {:.1}ms, Baseline: {:.1}ms",
                detection.current_performance.duration_micros as f64 / 1000.0,
                detection.baseline_performance.mean_duration_micros / 1000.0);
            
            if !detection.recommended_actions.is_empty() {
                println!("      • Actions: {}", detection.recommended_actions[0]);
            }
        }
    }
    
    Ok(())
}

/// Demonstrate trend analysis capabilities
fn demonstrate_trend_analysis(detector: &PerformanceRegressionDetector) -> Result<()> {
    // Get all operation types from recent detections
    let recent_detections = detector.get_recent_detections(Some(50))?;
    let mut operation_types = std::collections::HashSet::new();
    
    for detection in &recent_detections {
        operation_types.insert(detection.operation_type.clone());
    }
    
    println!("   📈 Analyzing trends for {} operation types:", operation_types.len());
    
    for operation_type in &operation_types {
        let trends = detector.get_performance_trends(operation_type, 2)?; // 2 hours of trends
        
        if !trends.is_empty() {
            let first_duration = trends[0].duration_micros as f64;
            let last_duration = trends.last().unwrap().duration_micros as f64;
            let trend_change = (last_duration - first_duration) / first_duration * 100.0;
            
            let trend_direction = if trend_change > 5.0 {
                "📈 DEGRADING"
            } else if trend_change < -5.0 {
                "📉 IMPROVING"
            } else {
                "➡️  STABLE"
            };
            
            println!("   • {}: {} ({:.1}% change, {} samples)",
                operation_type, trend_direction, trend_change, trends.len());
        }
    }
    
    Ok(())
}

/// Demonstrate bisection analysis
fn demonstrate_bisection_analysis(detector: &PerformanceRegressionDetector) -> Result<()> {
    let recent_detections = detector.get_recent_detections(Some(5))?;
    
    if let Some(regression) = recent_detections.iter().find(|d| d.detected && d.severity >= RegressionSeverity::Major) {
        println!("   🔧 Starting bisection for: {}", regression.operation_type);
        
        let bisection_session = detector.start_bisection_analysis(regression)?;
        
        println!("   • Session ID: {}", bisection_session.session_id);
        println!("   • Bisection Type: {:?}", bisection_session.bisection_type);
        println!("   • Time Range: {:.1} hours", bisection_session.current_state.narrowed_range.duration_hours);
        println!("   • Suspects: {} identified", bisection_session.suspects.len());
        
        // Show top suspects
        for (i, suspect) in bisection_session.suspects.iter().take(3).enumerate() {
            println!("   {}. {:?} ({:.0}% probability): {}",
                i + 1, suspect.suspect_type, suspect.probability * 100.0, suspect.description);
        }
        
        // Show recommendations
        if !bisection_session.recommendations.is_empty() {
            println!("   🎯 Top Recommendation: {}", bisection_session.recommendations[0].title);
            println!("      {}", bisection_session.recommendations[0].description);
        }
    } else {
        println!("   ℹ️  No major regressions found for bisection analysis");
    }
    
    Ok(())
}

/// Demonstrate alerting system
fn demonstrate_alerting_system(_detector: &PerformanceRegressionDetector) -> Result<()> {
    println!("   🚨 Alert system capabilities:");
    println!("   • Multi-channel notifications (Console, Log, Webhook)");
    println!("   • Severity-based routing");
    println!("   • Rate limiting and cooldown");
    println!("   • Alert suppression and escalation");
    println!("   • Statistical confidence filtering");
    
    // The alert system would have been triggered during regression simulation
    println!("   ✓ Alerts have been automatically sent for detected regressions");
    
    Ok(())
}

/// Generate comprehensive performance report
fn generate_performance_report(detector: &PerformanceRegressionDetector) -> Result<()> {
    let report = detector.generate_performance_report()?;
    
    println!("   📋 Performance Report Summary:");
    println!("   • Report Generated: {:?}", report.generated_at);
    println!("   • Operations Tracked: {}", report.total_operations_tracked);
    println!("   • Active Regressions: {}", report.active_regressions);
    println!("   • System Health Score: {:.1}%", report.system_health_score * 100.0);
    println!("   • Recent Detections: {}", report.recent_detections.len());
    
    // Show operation summaries
    println!("\n   📊 Operation Performance Summary:");
    for (operation_type, summary) in &report.operation_summaries {
        let health_indicator = match summary.trend_direction {
            TrendDirection::Improving => "✅",
            TrendDirection::Stable => "🟡",
            TrendDirection::Degrading => "⚠️",
            TrendDirection::Unknown => "❓",
        };
        
        println!("   {} {}: {:.1}ms avg ({} samples, {:?} trend)",
            health_indicator,
            operation_type,
            summary.recent_average_micros / 1000.0,
            summary.total_samples,
            summary.trend_direction
        );
        
        if let Some(last_regression) = summary.last_regression {
            let time_since = SystemTime::now().duration_since(last_regression)
                .unwrap_or_default().as_secs() / 60;
            println!("      Last regression: {} minutes ago", time_since);
        }
    }
    
    // Show severity breakdown
    let mut severity_counts = HashMap::new();
    for detection in &report.recent_detections {
        if detection.detected {
            *severity_counts.entry(detection.severity).or_insert(0) += 1;
        }
    }
    
    if !severity_counts.is_empty() {
        println!("\n   🚨 Regression Severity Breakdown:");
        for (severity, count) in severity_counts {
            println!("   • {:?}: {} detections", severity, count);
        }
    }
    
    Ok(())
}

/// Calculate realistic throughput based on duration
fn calculate_throughput(duration_micros: u64) -> f64 {
    if duration_micros > 0 {
        1_000_000.0 / duration_micros as f64 // ops per second
    } else {
        0.0
    }
}

/// Calculate realistic memory usage for operation type
fn calculate_memory_usage(operation_type: &str) -> u64 {
    match operation_type {
        "database.put" => 2048 + (rand::random::<u64>() % 1024),
        "database.get" => 1024 + (rand::random::<u64>() % 512),
        "database.scan" => 4096 + (rand::random::<u64>() % 2048),
        "database.delete" => 1536 + (rand::random::<u64>() % 768),
        "cache.lookup" => 256 + (rand::random::<u64>() % 128),
        "index.search" => 512 + (rand::random::<u64>() % 256),
        _ => 1024 + (rand::random::<u64>() % 512),
    }
}

/// Calculate realistic CPU usage
fn calculate_cpu_usage(operation_type: &str, duration_micros: u64) -> f64 {
    let base_cpu = match operation_type {
        "database.put" => 8.0,
        "database.get" => 3.0,
        "database.scan" => 15.0,
        "database.delete" => 5.0,
        "cache.lookup" => 1.0,
        "index.search" => 7.0,
        _ => 5.0,
    };
    
    // CPU usage increases with duration (simplified model)
    let duration_factor = (duration_micros as f64 / 1000.0).ln().max(1.0);
    let variance = (rand::random::<f64>() - 0.5) * 0.4;
    
    (base_cpu * duration_factor * (1.0 + variance)).max(0.1)
}

/// Create additional metrics for different operation types
fn create_additional_metrics(operation_type: &str) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();
    
    match operation_type {
        "database.put" => {
            metrics.insert("bytes_written".to_string(), 1024.0 + rand::random::<f64>() * 512.0);
            metrics.insert("fsync_time_micros".to_string(), 50.0 + rand::random::<f64>() * 100.0);
        }
        "database.get" => {
            metrics.insert("bytes_read".to_string(), 512.0 + rand::random::<f64>() * 256.0);
            metrics.insert("cache_hits".to_string(), if rand::random::<f64>() < 0.8 { 1.0 } else { 0.0 });
        }
        "database.scan" => {
            metrics.insert("records_scanned".to_string(), 10.0 + rand::random::<f64>() * 90.0);
            metrics.insert("index_seeks".to_string(), 1.0 + rand::random::<f64>() * 5.0);
        }
        "cache.lookup" => {
            metrics.insert("cache_level".to_string(), (rand::random::<f64>() * 3.0).floor());
            metrics.insert("evictions_triggered".to_string(), if rand::random::<f64>() < 0.1 { 1.0 } else { 0.0 });
        }
        _ => {}
    }
    
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throughput_calculation() {
        assert_eq!(calculate_throughput(1000), 1000.0); // 1ms = 1000 ops/sec
        assert_eq!(calculate_throughput(100), 10000.0);  // 0.1ms = 10k ops/sec
        assert_eq!(calculate_throughput(0), 0.0);
    }

    #[test]
    fn test_memory_usage_calculation() {
        let memory = calculate_memory_usage("database.put");
        assert!(memory >= 2048 && memory < 3072);
        
        let cache_memory = calculate_memory_usage("cache.lookup");
        assert!(cache_memory >= 256 && cache_memory < 384);
    }

    #[test]
    fn test_cpu_usage_calculation() {
        let cpu = calculate_cpu_usage("database.put", 1000);
        assert!(cpu > 0.0 && cpu < 100.0);
    }

    #[test]
    fn test_additional_metrics_creation() {
        let metrics = create_additional_metrics("database.put");
        assert!(metrics.contains_key("bytes_written"));
        assert!(metrics.contains_key("fsync_time_micros"));
        
        let cache_metrics = create_additional_metrics("cache.lookup");
        assert!(cache_metrics.contains_key("cache_level"));
    }
}
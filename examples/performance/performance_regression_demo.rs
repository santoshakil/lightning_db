use lightning_db::performance_regression::performance_tracker::*;
use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Demonstrate the performance regression tracking and reporting framework
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lightning DB Performance Regression Tracking Demo");
    println!("==================================================");
    println!();

    // Create a performance tracker
    let mut tracker = PerformanceTracker::new("performance_regression_demo.json");

    // Simulate running various performance tests
    simulate_read_performance_test(&mut tracker)?;
    simulate_write_performance_test(&mut tracker)?;
    simulate_mixed_workload_test(&mut tracker)?;
    simulate_concurrent_test(&mut tracker)?;

    // Analyze results and generate comprehensive report
    println!("\n📊 PERFORMANCE REGRESSION ANALYSIS");
    println!("==================================");

    // Check each test for regressions
    let tests = ["read_performance", "write_performance", "mixed_workload", "concurrent_performance"];
    let mut regressions_found = 0;

    for test_name in &tests {
        if let Some(analysis) = tracker.analyze_regression(test_name) {
            let status = if analysis.is_regression { "❌ REGRESSION" } else { "✅ PASS" };
            
            println!("\n🔍 {}", analysis.test_name);
            println!("   Status:           {}", status);
            println!("   Current:          {:>12.0} ops/sec", analysis.current.ops_per_sec);
            println!("   Baseline:         {:>12.0} ops/sec", analysis.baseline.expected_ops_per_sec);
            println!("   Performance:      {:>12.1}% of baseline", analysis.performance_ratio * 100.0);
            println!("   Confidence:       {:>12.1}%", analysis.confidence * 100.0);
            println!("   Trend:            {}", analysis.trend_analysis.direction);
            
            if analysis.is_regression {
                println!("   Severity:         {:>12.1}% below threshold", analysis.regression_severity * 100.0);
                regressions_found += 1;
            }
        }
    }

    // Generate and display comprehensive report
    println!("\n📋 COMPREHENSIVE PERFORMANCE REPORT");
    println!("===================================");
    let report = tracker.generate_report();
    println!("{}", report);

    // Export performance data to CSV for external analysis
    println!("\n📈 EXPORTING PERFORMANCE DATA");
    println!("=============================");
    for test_name in &tests {
        let csv_path = format!("{}_performance_data.csv", test_name);
        match tracker.export_to_csv(test_name, &csv_path) {
            Ok(()) => println!("✅ Exported {} data to {}", test_name, csv_path),
            Err(e) => println!("⚠️ Could not export {} data: {}", test_name, e),
        }
    }

    // Display summary statistics
    println!("\n📊 SUMMARY STATISTICS");
    println!("====================");
    let stats = tracker.get_summary_stats();
    for (test_name, (mean, std_dev, count)) in stats {
        println!("{:20} | Mean: {:>10.0} ops/sec | StdDev: {:>8.0} | Samples: {:>4}",
            test_name, mean, std_dev, count);
    }

    // Final summary
    println!("\n🎯 FINAL SUMMARY");
    println!("===============");
    if regressions_found == 0 {
        println!("🎉 ALL PERFORMANCE TESTS PASSED!");
        println!("No significant performance regressions detected.");
        println!("Lightning DB maintains its documented performance characteristics.");
    } else {
        println!("⚠️ {} PERFORMANCE REGRESSION(S) DETECTED!", regressions_found);
        println!("Some performance metrics are below acceptable thresholds.");
        println!("Review the failing tests and consider optimizations.");
    }

    println!("\n💡 RECOMMENDATIONS");
    println!("==================");
    if regressions_found == 0 {
        println!("✅ Continue monitoring performance in CI/CD");
        println!("✅ Consider raising performance targets based on current results");
        println!("✅ Document current performance characteristics");
        println!("✅ Set up automated regression alerts");
    } else {
        println!("🔍 Investigate root causes of performance regressions");
        println!("📊 Profile bottlenecks in failing tests");
        println!("🔧 Apply targeted optimizations");  
        println!("🧪 Re-run tests after improvements");
    }

    println!("\nDemo completed! Check generated files:");
    println!("- performance_regression_demo.json (tracking data)");
    println!("- *_performance_data.csv (raw data for analysis)");

    Ok(())
}

/// Simulate a read performance test
fn simulate_read_performance_test(tracker: &mut PerformanceTracker) -> Result<(), Box<dyn std::error::Error>> {
    println!("📖 Running read performance simulation...");

    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 1024 * 1024 * 1024, // 1GB cache
        compression_enabled: false,
        prefetch_enabled: true,
        ..Default::default()
    };
    let db = Database::create(temp_dir.path(), config)?;

    // Pre-populate with test data
    let num_records = 50_000;
    let value = vec![0u8; 1024]; // 1KB values
    for i in 0..num_records {
        let key = format!("read_key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }

    // Measure read performance
    let test_duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut operations = 0;
    let mut latencies = Vec::new();

    while start.elapsed() < test_duration {
        let key = format!("read_key_{:08}", operations % num_records);
        let op_start = Instant::now();
        let _result = db.get(key.as_bytes())?;
        let latency = op_start.elapsed();
        latencies.push(latency);
        operations += 1;
    }

    let total_duration = start.elapsed();
    let ops_per_sec = operations as f64 / total_duration.as_secs_f64();

    // Calculate latencies
    latencies.sort();
    let p50_latency = latencies[latencies.len() / 2].as_nanos() as f64 / 1000.0; // microseconds
    let p99_latency = latencies[(latencies.len() as f64 * 0.99) as usize].as_nanos() as f64 / 1000.0;

    // Record measurement
    let test_config = TestConfiguration {
        test_name: "read_performance".to_string(),
        cache_size_mb: 1024,
        key_size: 32,
        value_size: 1024,
        dataset_size: num_records,
        compression_enabled: false,
        options: HashMap::new(),
    };

    let measurement = create_measurement(
        "read_performance",
        ops_per_sec,
        p50_latency,
        p99_latency,
        test_config,
    );

    tracker.record_measurement(measurement);

    println!("   Results: {:.2}M ops/sec, {:.3}μs P50, {:.3}μs P99", 
        ops_per_sec / 1_000_000.0, p50_latency, p99_latency);

    Ok(())
}

/// Simulate a write performance test
fn simulate_write_performance_test(tracker: &mut PerformanceTracker) -> Result<(), Box<dyn std::error::Error>> {
    println!("✏️ Running write performance simulation...");

    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 1024 * 1024 * 1024,
        compression_enabled: false,
        ..Default::default()
    };
    let db = Database::create(temp_dir.path(), config)?;

    let value = vec![0u8; 1024];
    let test_duration = Duration::from_secs(5);
    let start = Instant::now();
    let mut operations = 0;
    let mut latencies = Vec::new();

    while start.elapsed() < test_duration {
        let key = format!("write_key_{:08}", operations);
        let op_start = Instant::now();
        db.put(key.as_bytes(), &value)?;
        let latency = op_start.elapsed();
        latencies.push(latency);
        operations += 1;
    }

    let total_duration = start.elapsed();
    let ops_per_sec = operations as f64 / total_duration.as_secs_f64();

    latencies.sort();
    let p50_latency = latencies[latencies.len() / 2].as_nanos() as f64 / 1000.0;
    let p99_latency = latencies[(latencies.len() as f64 * 0.99) as usize].as_nanos() as f64 / 1000.0;

    let test_config = TestConfiguration {
        test_name: "write_performance".to_string(),
        cache_size_mb: 1024,
        key_size: 32,
        value_size: 1024,
        dataset_size: operations,
        compression_enabled: false,
        options: HashMap::new(),
    };

    let measurement = create_measurement(
        "write_performance",
        ops_per_sec,
        p50_latency,
        p99_latency,
        test_config,
    );

    tracker.record_measurement(measurement);

    println!("   Results: {:.2}K ops/sec, {:.3}μs P50, {:.3}μs P99", 
        ops_per_sec / 1_000.0, p50_latency, p99_latency);

    Ok(())
}

/// Simulate a mixed workload test
fn simulate_mixed_workload_test(tracker: &mut PerformanceTracker) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔄 Running mixed workload simulation...");

    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 512 * 1024 * 1024, // 512MB for mixed workload
        ..Default::default()
    };
    let db = Database::create(temp_dir.path(), config)?;

    // Pre-populate base data
    let num_initial_keys = 25_000;
    let value = vec![0u8; 1024];
    for i in 0..num_initial_keys {
        let key = format!("mixed_base_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }

    let test_duration = Duration::from_secs(8);
    let start = Instant::now();
    let mut operations = 0;

    while start.elapsed() < test_duration {
        let op_type = operations % 10;

        if op_type < 8 {
            // 80% reads
            let key = format!("mixed_base_{:08}", operations % num_initial_keys);
            let _result = db.get(key.as_bytes())?;
        } else {
            // 20% writes
            let key = format!("mixed_new_{:08}", operations);
            db.put(key.as_bytes(), &value)?;
        }

        operations += 1;
    }

    let total_duration = start.elapsed();
    let ops_per_sec = operations as f64 / total_duration.as_secs_f64();

    let test_config = TestConfiguration {
        test_name: "mixed_workload".to_string(),
        cache_size_mb: 512,
        key_size: 32,
        value_size: 1024,
        dataset_size: num_initial_keys,
        compression_enabled: false,
        options: {
            let mut opts = HashMap::new();
            opts.insert("read_ratio".to_string(), "80".to_string());
            opts.insert("write_ratio".to_string(), "20".to_string());
            opts
        },
    };

    let measurement = create_measurement(
        "mixed_workload",
        ops_per_sec,
        2.0, // Estimated mixed latency
        4.0, // Estimated mixed latency
        test_config,
    );

    tracker.record_measurement(measurement);

    println!("   Results: {:.2}K ops/sec (80% reads, 20% writes)", 
        ops_per_sec / 1_000.0);

    Ok(())
}

/// Simulate a concurrent performance test
fn simulate_concurrent_test(tracker: &mut PerformanceTracker) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔀 Running concurrent performance simulation...");

    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 1024 * 1024 * 1024,
        ..Default::default()
    };
    let db = std::sync::Arc::new(Database::create(temp_dir.path(), config)?);

    // Pre-populate base data
    let value = vec![0u8; 512];
    for i in 0..10_000 {
        let key = format!("concurrent_base_{:06}", i);
        db.put(key.as_bytes(), &value)?;
    }

    let thread_count = 4; // Use 4 threads for demo
    let test_duration = Duration::from_secs(6);
    let start_time = Instant::now();

    let handles: Vec<_> = (0..thread_count).map(|thread_id| {
        let db_clone = std::sync::Arc::clone(&db);
        let value_clone = value.clone();
        let start_time = start_time;
        let test_duration = test_duration;

        std::thread::spawn(move || {
            let mut operations = 0;
            while start_time.elapsed() < test_duration {
                let op_type = operations % 4;

                if op_type == 0 {
                    // Write operation (25%)
                    let key = format!("concurrent_{}_{:08}", thread_id, operations);
                    let _ = db_clone.put(key.as_bytes(), &value_clone);
                } else {
                    // Read operation (75%)
                    let key = format!("concurrent_base_{:06}", operations % 10_000);
                    let _ = db_clone.get(key.as_bytes());
                }

                operations += 1;
            }
            operations
        })
    }).collect();

    let mut total_operations = 0;
    for handle in handles {
        total_operations += handle.join().unwrap();
    }

    let total_duration = start_time.elapsed();
    let ops_per_sec = total_operations as f64 / total_duration.as_secs_f64();

    let test_config = TestConfiguration {
        test_name: "concurrent_performance".to_string(),
        cache_size_mb: 1024,
        key_size: 32,
        value_size: 512,
        dataset_size: 10_000,
        compression_enabled: false,
        options: {
            let mut opts = HashMap::new();
            opts.insert("thread_count".to_string(), thread_count.to_string());
            opts.insert("read_ratio".to_string(), "75".to_string());
            opts.insert("write_ratio".to_string(), "25".to_string());
            opts
        },
    };

    let mut measurement = create_measurement(
        "concurrent_performance",
        ops_per_sec,
        3.0, // Estimated concurrent latency
        6.0, // Estimated concurrent latency
        test_config,
    );
    measurement.thread_count = thread_count;

    tracker.record_measurement(measurement);

    println!("   Results: {:.2}K ops/sec with {} threads", 
        ops_per_sec / 1_000.0, thread_count);

    Ok(())
}
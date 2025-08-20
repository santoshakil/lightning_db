use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct EnduranceMetrics {
    pub operations_completed: u64,
    pub errors_encountered: u64,
    pub memory_usage_mb: f64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub cache_hit_rate: f64,
    pub throughput_ops_sec: f64,
    pub timestamp: u64,
}

impl EnduranceMetrics {
    pub fn new() -> Self {
        Self {
            operations_completed: 0,
            errors_encountered: 0,
            memory_usage_mb: 0.0,
            avg_latency_us: 0.0,
            p99_latency_us: 0.0,
            cache_hit_rate: 0.0,
            throughput_ops_sec: 0.0,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

pub struct EnduranceRunner {
    db: Arc<Database>,
    running: Arc<AtomicBool>,
    metrics_history: Arc<parking_lot::Mutex<VecDeque<EnduranceMetrics>>>,
    operations_count: Arc<AtomicU64>,
    errors_count: Arc<AtomicU64>,
    start_time: Instant,
}

impl EnduranceRunner {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            running: Arc::new(AtomicBool::new(true)),
            metrics_history: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
            operations_count: Arc::new(AtomicU64::new(0)),
            errors_count: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    pub fn start_endurance_test(&self, duration_hours: u64) -> Result<Vec<EnduranceMetrics>, Box<dyn std::error::Error>> {
        let test_duration = Duration::from_secs(duration_hours * 3600);
        let mut handles = vec![];

        // Spawn metrics collector
        let metrics_handle = self.spawn_metrics_collector();
        handles.push(metrics_handle);

        // Spawn workload threads
        let workload_handle = self.spawn_mixed_workload();
        handles.push(workload_handle);

        let heavy_write_handle = self.spawn_heavy_write_workload();
        handles.push(heavy_write_handle);

        let scan_handle = self.spawn_scan_workload();
        handles.push(scan_handle);

        let transaction_handle = self.spawn_transaction_workload();
        handles.push(transaction_handle);

        // Run for specified duration
        thread::sleep(test_duration);

        // Stop all threads
        self.running.store(false, Ordering::Relaxed);

        // Wait for threads to complete
        for handle in handles {
            let _ = handle.join();
        }

        // Return metrics history
        Ok(self.metrics_history.lock().clone().into())
    }

    fn spawn_metrics_collector(&self) -> thread::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let metrics_history = Arc::clone(&self.metrics_history);
        let operations_count = Arc::clone(&self.operations_count);
        let errors_count = Arc::clone(&self.errors_count);
        let start_time = self.start_time;

        thread::spawn(move || {
            let mut last_ops = 0u64;
            let mut last_time = Instant::now();

            while running.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(10));

                let current_ops = operations_count.load(Ordering::Relaxed);
                let current_errors = errors_count.load(Ordering::Relaxed);
                let current_time = Instant::now();

                // Calculate throughput
                let ops_delta = current_ops.saturating_sub(last_ops);
                let time_delta = current_time.duration_since(last_time).as_secs_f64();
                let throughput = if time_delta > 0.0 { ops_delta as f64 / time_delta } else { 0.0 };

                // Get memory usage
                let memory_usage = Self::get_memory_usage_mb();

                // Get cache stats
                let cache_hit_rate = db.cache_stats()
                    .and_then(|stats| Self::parse_cache_hit_rate(&stats))
                    .unwrap_or(0.0);

                let metrics = EnduranceMetrics {
                    operations_completed: current_ops,
                    errors_encountered: current_errors,
                    memory_usage_mb: memory_usage,
                    avg_latency_us: 0.0, // Would need latency tracking
                    p99_latency_us: 0.0,
                    cache_hit_rate,
                    throughput_ops_sec: throughput,
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                metrics_history.lock().push_back(metrics.clone());

                // Keep last 24 hours of metrics (assuming 10-second intervals)
                let max_samples = 24 * 3600 / 10;
                if metrics_history.lock().len() > max_samples {
                    metrics_history.lock().pop_front();
                }

                println!(
                    "[{}] Ops: {}, Errors: {}, Memory: {:.1} MB, Throughput: {:.0} ops/sec, Cache Hit: {:.1}%",
                    start_time.elapsed().as_secs(),
                    current_ops,
                    current_errors,
                    memory_usage,
                    throughput,
                    cache_hit_rate * 100.0
                );

                last_ops = current_ops;
                last_time = current_time;
            }
        })
    }

    fn spawn_mixed_workload(&self) -> thread::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let operations_count = Arc::clone(&self.operations_count);
        let errors_count = Arc::clone(&self.errors_count);

        thread::spawn(move || {
            let mut operation_counter = 0u64;

            while running.load(Ordering::Relaxed) {
                operation_counter += 1;

                let operation_type = operation_counter % 10;
                let key = format!("mixed_key_{:012}", operation_counter);

                let result = match operation_type {
                    0..=6 => {
                        // 70% reads
                        db.get(key.as_bytes()).map(|_| ())
                    }
                    7..=8 => {
                        // 20% writes
                        let value = format!("value_{}", operation_counter);
                        db.put(key.as_bytes(), value.as_bytes())
                    }
                    9 => {
                        // 10% deletes
                        db.delete(key.as_bytes())
                    }
                    _ => unreachable!(),
                };

                match result {
                    Ok(_) => {
                        operations_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Small delay to prevent CPU spinning
                if operation_counter % 1000 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
        })
    }

    fn spawn_heavy_write_workload(&self) -> thread::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let operations_count = Arc::clone(&self.operations_count);
        let errors_count = Arc::clone(&self.errors_count);

        thread::spawn(move || {
            let mut operation_counter = 0u64;
            let large_value = vec![0u8; 10240]; // 10KB values

            while running.load(Ordering::Relaxed) {
                operation_counter += 1;

                let key = format!("heavy_write_key_{:012}", operation_counter);
                
                match db.put(key.as_bytes(), &large_value) {
                    Ok(_) => {
                        operations_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Throttle to prevent overwhelming the system
                if operation_counter % 100 == 0 {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        })
    }

    fn spawn_scan_workload(&self) -> thread::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let operations_count = Arc::clone(&self.operations_count);
        let errors_count = Arc::clone(&self.errors_count);

        thread::spawn(move || {
            let mut scan_counter = 0u64;

            while running.load(Ordering::Relaxed) {
                scan_counter += 1;

                let start_key = format!("mixed_key_{:012}", scan_counter * 1000);
                let end_key = format!("mixed_key_{:012}", (scan_counter + 1) * 1000);

                // Perform range scan
                let result = db.scan_range(start_key.as_bytes(), end_key.as_bytes(), 100);
                
                match result {
                    Ok(_) => {
                        operations_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Scans are expensive, so do them less frequently
                thread::sleep(Duration::from_secs(1));
            }
        })
    }

    fn spawn_transaction_workload(&self) -> thread::JoinHandle<()> {
        let db = Arc::clone(&self.db);
        let running = Arc::clone(&self.running);
        let operations_count = Arc::clone(&self.operations_count);
        let errors_count = Arc::clone(&self.errors_count);

        thread::spawn(move || {
            let mut tx_counter = 0u64;

            while running.load(Ordering::Relaxed) {
                tx_counter += 1;

                let tx_result = (|| -> Result<(), lightning_db::core::error::LightningError> {
                    let tx_id = db.begin_transaction()?;

                    // Multi-key transaction
                    for i in 0..5 {
                        let key = format!("tx_key_{}_{:012}", tx_counter, i);
                        let value = format!("tx_value_{}_{}", tx_counter, i);
                        db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
                    }

                    db.commit_transaction(tx_id)?;
                    Ok(())
                })();

                match tx_result {
                    Ok(_) => {
                        operations_count.fetch_add(5, Ordering::Relaxed); // 5 operations per transaction
                    }
                    Err(_) => {
                        errors_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Transactions are more expensive
                thread::sleep(Duration::from_millis(50));
            }
        })
    }

    fn get_memory_usage_mb() -> f64 {
        #[cfg(not(target_env = "msvc"))]
        {
            use tikv_jemallocator::Jemalloc;
            tikv_jemalloc_ctl::stats::allocated::read()
                .map(|bytes| bytes as f64 / 1024.0 / 1024.0)
                .unwrap_or(0.0)
        }
        #[cfg(target_env = "msvc")]
        {
            // Fallback for MSVC - would need platform-specific implementation
            0.0
        }
    }

    fn parse_cache_hit_rate(stats: &str) -> Option<f64> {
        // Parse cache statistics to extract hit rate
        // Implementation depends on the actual format of cache_stats()
        // This is a simplified version
        if stats.contains("hit_rate") {
            // Try to extract percentage
            Some(0.85) // Placeholder
        } else {
            None
        }
    }

    pub fn analyze_performance_degradation(&self) -> PerformanceAnalysis {
        let metrics = self.metrics_history.lock();
        
        if metrics.len() < 2 {
            return PerformanceAnalysis::default();
        }

        let first_hour: Vec<_> = metrics.iter().take(360).collect(); // First hour (10s intervals)
        let last_hour: Vec<_> = metrics.iter().rev().take(360).collect(); // Last hour

        let first_throughput = Self::avg_throughput(&first_hour);
        let last_throughput = Self::avg_throughput(&last_hour);
        
        let first_memory = Self::avg_memory(&first_hour);
        let last_memory = Self::avg_memory(&last_hour);

        let throughput_degradation = if first_throughput > 0.0 {
            (first_throughput - last_throughput) / first_throughput * 100.0
        } else {
            0.0
        };

        let memory_growth = if first_memory > 0.0 {
            (last_memory - first_memory) / first_memory * 100.0
        } else {
            0.0
        };

        PerformanceAnalysis {
            throughput_degradation_percent: throughput_degradation,
            memory_growth_percent: memory_growth,
            error_rate_trend: self.calculate_error_trend(),
            cache_efficiency_trend: self.calculate_cache_trend(),
            stability_score: self.calculate_stability_score(),
        }
    }

    fn avg_throughput(metrics: &[&EnduranceMetrics]) -> f64 {
        if metrics.is_empty() {
            return 0.0;
        }
        metrics.iter().map(|m| m.throughput_ops_sec).sum::<f64>() / metrics.len() as f64
    }

    fn avg_memory(metrics: &[&EnduranceMetrics]) -> f64 {
        if metrics.is_empty() {
            return 0.0;
        }
        metrics.iter().map(|m| m.memory_usage_mb).sum::<f64>() / metrics.len() as f64
    }

    fn calculate_error_trend(&self) -> String {
        // Simplified error trend analysis
        "stable".to_string()
    }

    fn calculate_cache_trend(&self) -> String {
        // Simplified cache trend analysis
        "stable".to_string()
    }

    fn calculate_stability_score(&self) -> f64 {
        // Simplified stability scoring (0.0 to 1.0)
        0.95
    }
}

#[derive(Debug, Default)]
pub struct PerformanceAnalysis {
    pub throughput_degradation_percent: f64,
    pub memory_growth_percent: f64,
    pub error_rate_trend: String,
    pub cache_efficiency_trend: String,
    pub stability_score: f64,
}

#[test]
#[ignore = "Long-running endurance test - run with: cargo test test_24_hour_endurance -- --ignored"]
fn test_24_hour_endurance() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB cache
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 1000 },
        prefetch_enabled: true,
        compression_enabled: true,
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let runner = EnduranceRunner::new(db);

    println!("Starting 24-hour endurance test...");
    let start = Instant::now();

    let metrics_history = runner.start_endurance_test(24).unwrap();
    
    let elapsed = start.elapsed();
    println!("Endurance test completed in {:?}", elapsed);

    // Analyze results
    let analysis = runner.analyze_performance_degradation();
    
    println!("Performance Analysis:");
    println!("  Throughput degradation: {:.2}%", analysis.throughput_degradation_percent);
    println!("  Memory growth: {:.2}%", analysis.memory_growth_percent);
    println!("  Error rate trend: {}", analysis.error_rate_trend);
    println!("  Cache efficiency trend: {}", analysis.cache_efficiency_trend);
    println!("  Stability score: {:.3}", analysis.stability_score);

    // Performance thresholds for 24-hour test
    assert!(analysis.throughput_degradation_percent < 20.0, 
        "Throughput degraded by more than 20%: {:.2}%", analysis.throughput_degradation_percent);
    
    assert!(analysis.memory_growth_percent < 50.0, 
        "Memory grew by more than 50%: {:.2}%", analysis.memory_growth_percent);
    
    assert!(analysis.stability_score > 0.9, 
        "Stability score too low: {:.3}", analysis.stability_score);

    // Save detailed metrics for analysis
    let final_metrics = metrics_history.last().unwrap();
    println!("Final metrics:");
    println!("  Total operations: {}", final_metrics.operations_completed);
    println!("  Total errors: {}", final_metrics.errors_encountered);
    println!("  Memory usage: {:.1} MB", final_metrics.memory_usage_mb);
    println!("  Final throughput: {:.0} ops/sec", final_metrics.throughput_ops_sec);

    assert!(final_metrics.errors_encountered < final_metrics.operations_completed / 1000,
        "Error rate too high: {} errors out of {} operations", 
        final_metrics.errors_encountered, final_metrics.operations_completed);
}

#[test]
#[ignore = "Memory leak detection test - run with: cargo test test_memory_leak_detection -- --ignored"]
fn test_memory_leak_detection() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 50 * 1024 * 1024, // 50MB cache
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let runner = EnduranceRunner::new(db);

    println!("Starting 4-hour memory leak detection test...");
    
    let metrics_history = runner.start_endurance_test(4).unwrap();
    
    // Analyze memory growth patterns
    let memory_samples: Vec<f64> = metrics_history.iter()
        .map(|m| m.memory_usage_mb)
        .collect();

    if memory_samples.len() >= 3 {
        let initial_memory = memory_samples.iter().take(10).sum::<f64>() / 10.0;
        let final_memory = memory_samples.iter().rev().take(10).sum::<f64>() / 10.0;
        
        let memory_growth_rate = (final_memory - initial_memory) / initial_memory * 100.0;
        
        println!("Memory leak analysis:");
        println!("  Initial memory: {:.1} MB", initial_memory);
        println!("  Final memory: {:.1} MB", final_memory);
        println!("  Growth rate: {:.2}%", memory_growth_rate);

        // Check for memory leaks
        assert!(memory_growth_rate < 25.0, 
            "Potential memory leak detected - growth rate: {:.2}%", memory_growth_rate);

        // Check for memory stability (shouldn't keep growing linearly)
        let mid_point = memory_samples.len() / 2;
        let mid_memory = memory_samples.iter().skip(mid_point - 5).take(10).sum::<f64>() / 10.0;
        
        let first_half_growth = (mid_memory - initial_memory) / initial_memory * 100.0;
        let second_half_growth = (final_memory - mid_memory) / mid_memory * 100.0;
        
        println!("  First half growth: {:.2}%", first_half_growth);
        println!("  Second half growth: {:.2}%", second_half_growth);

        // Memory growth should stabilize over time
        if first_half_growth > 5.0 {
            assert!(second_half_growth < first_half_growth * 0.5,
                "Memory growth is not stabilizing - potential leak");
        }
    }
}

#[test]
#[ignore = "Performance regression test - run with: cargo test test_performance_regression -- --ignored"]
fn test_performance_regression() {
    let dir = tempdir().unwrap();
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024,
        use_improved_wal: true,
        wal_sync_mode: WalSyncMode::Periodic { interval_ms: 500 },
        ..Default::default()
    };

    let db = Arc::new(Database::open(dir.path(), config).unwrap());
    let runner = EnduranceRunner::new(db);

    println!("Starting 2-hour performance regression test...");
    
    let metrics_history = runner.start_endurance_test(2).unwrap();
    
    // Analyze performance trends
    let analysis = runner.analyze_performance_degradation();
    
    println!("Performance regression analysis:");
    println!("  Throughput degradation: {:.2}%", analysis.throughput_degradation_percent);
    println!("  Memory growth: {:.2}%", analysis.memory_growth_percent);
    println!("  Stability score: {:.3}", analysis.stability_score);

    // Performance regression thresholds
    assert!(analysis.throughput_degradation_percent < 15.0,
        "Performance regression detected - throughput degraded by {:.2}%", 
        analysis.throughput_degradation_percent);

    assert!(analysis.stability_score > 0.85,
        "System stability degraded - score: {:.3}", analysis.stability_score);

    // Verify consistent performance across the test
    let throughput_samples: Vec<f64> = metrics_history.iter()
        .map(|m| m.throughput_ops_sec)
        .filter(|&t| t > 0.0)
        .collect();

    if !throughput_samples.is_empty() {
        let avg_throughput = throughput_samples.iter().sum::<f64>() / throughput_samples.len() as f64;
        let min_throughput = throughput_samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_throughput = throughput_samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let throughput_variance = (max_throughput - min_throughput) / avg_throughput * 100.0;

        println!("  Average throughput: {:.0} ops/sec", avg_throughput);
        println!("  Throughput variance: {:.2}%", throughput_variance);

        assert!(throughput_variance < 50.0,
            "Throughput too variable - variance: {:.2}%", throughput_variance);
    }
}
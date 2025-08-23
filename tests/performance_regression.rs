use lightning_db::{Database, LightningDbConfig};
use std::time::{Duration, Instant};
use std::fs;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaseline {
    pub version: String,
    pub timestamp: DateTime<Utc>,
    pub system_info: SystemInfo,
    pub test_results: Vec<TestResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub os: String,
    pub cpu_cores: usize,
    pub memory_gb: f64,
    pub rust_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub test_name: String,
    pub operations: u64,
    pub duration_ms: u64,
    pub ops_per_sec: f64,
    pub latency_us: f64,
    pub memory_used_mb: f64,
    pub percentiles: Percentiles,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Percentiles {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
}

pub struct RegressionDetector {
    baseline: Option<PerformanceBaseline>,
    threshold_percent: f64,
}

impl RegressionDetector {
    pub fn new(threshold_percent: f64) -> Self {
        Self {
            baseline: None,
            threshold_percent,
        }
    }
    
    pub fn load_baseline(&mut self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        self.baseline = Some(serde_json::from_str(&contents)?);
        Ok(())
    }
    
    pub fn check_regression(&self, current: &TestResult) -> RegressionResult {
        if let Some(baseline) = &self.baseline {
            if let Some(baseline_test) = baseline.test_results.iter()
                .find(|t| t.test_name == current.test_name) {
                
                let ops_change = (current.ops_per_sec - baseline_test.ops_per_sec) 
                    / baseline_test.ops_per_sec * 100.0;
                
                let latency_change = (current.latency_us - baseline_test.latency_us)
                    / baseline_test.latency_us * 100.0;
                
                let memory_change = (current.memory_used_mb - baseline_test.memory_used_mb)
                    / baseline_test.memory_used_mb * 100.0;
                
                RegressionResult {
                    test_name: current.test_name.clone(),
                    has_regression: ops_change < -self.threshold_percent 
                        || latency_change > self.threshold_percent
                        || memory_change > self.threshold_percent * 2.0,
                    ops_change_percent: ops_change,
                    latency_change_percent: latency_change,
                    memory_change_percent: memory_change,
                }
            } else {
                RegressionResult::new_test(current.test_name.clone())
            }
        } else {
            RegressionResult::no_baseline(current.test_name.clone())
        }
    }
}

#[derive(Debug)]
pub struct RegressionResult {
    pub test_name: String,
    pub has_regression: bool,
    pub ops_change_percent: f64,
    pub latency_change_percent: f64,
    pub memory_change_percent: f64,
}

impl RegressionResult {
    fn new_test(test_name: String) -> Self {
        Self {
            test_name,
            has_regression: false,
            ops_change_percent: 0.0,
            latency_change_percent: 0.0,
            memory_change_percent: 0.0,
        }
    }
    
    fn no_baseline(test_name: String) -> Self {
        Self {
            test_name,
            has_regression: false,
            ops_change_percent: 0.0,
            latency_change_percent: 0.0,
            memory_change_percent: 0.0,
        }
    }
}

pub struct PerformanceTestSuite {
    db_path: PathBuf,
    config: LightningDbConfig,
    results: Vec<TestResult>,
}

impl PerformanceTestSuite {
    pub fn new(db_path: PathBuf) -> Self {
        let config = LightningDbConfig {
            cache_size: 256 * 1024 * 1024,
            prefetch_enabled: true,
            use_optimized_transactions: true,
            ..Default::default()
        };
        
        Self {
            db_path,
            config,
            results: Vec::new(),
        }
    }
    
    pub fn run_all_tests(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.test_sequential_writes()?;
        self.test_random_writes()?;
        self.test_sequential_reads()?;
        self.test_random_reads()?;
        self.test_mixed_workload()?;
        self.test_batch_operations()?;
        self.test_range_scans()?;
        self.test_concurrent_access()?;
        self.test_large_values()?;
        self.test_transactions()?;
        Ok(())
    }
    
    fn test_sequential_writes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "sequential_writes";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for i in 0..n {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            
            let op_start = Instant::now();
            db.put(key.as_bytes(), value.as_bytes())?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_random_writes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "random_writes";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for _ in 0..n {
            let key = format!("key_{:08}", rand::rng().random::<u32>());
            let value = format!("value_{:08}", rand::rng().random::<u32>());
            
            let op_start = Instant::now();
            db.put(key.as_bytes(), value.as_bytes())?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_sequential_reads(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "sequential_reads";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        for i in 0..n {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for i in 0..n {
            let key = format!("key_{:08}", i);
            
            let op_start = Instant::now();
            let _ = db.get(key.as_bytes())?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_random_reads(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "random_reads";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        for i in 0..n {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for _ in 0..n {
            let key = format!("key_{:08}", rand::rng().random::<u32>() % n as u32);
            
            let op_start = Instant::now();
            let _ = db.get(key.as_bytes())?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_mixed_workload(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "mixed_workload";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for i in 0..n {
            let op_type = rand::rng().random::<f64>();
            
            let op_start = Instant::now();
            if op_type < 0.5 {
                let key = format!("key_{:08}", i);
                let value = format!("value_{:08}", i);
                db.put(key.as_bytes(), value.as_bytes())?;
            } else {
                let key = format!("key_{:08}", rand::rng().random::<u32>() % (i + 1) as u32);
                let _ = db.get(key.as_bytes())?;
            }
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_batch_operations(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "batch_operations";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n_batches = 1000;
        let batch_size = 100;
        let mut latencies = Vec::with_capacity(n_batches);
        
        let start = Instant::now();
        for batch in 0..n_batches {
            let batch_start = Instant::now();
            
            let mut batch_data = Vec::with_capacity(batch_size);
            for i in 0..batch_size {
                let key = format!("key_{:08}_{:04}", batch, i);
                let value = format!("value_{:08}_{:04}", batch, i);
                batch_data.push((key.into_bytes(), value.into_bytes()));
            }
            
            for (key, value) in batch_data {
                db.put(&key, &value)?;
            }
            
            latencies.push(batch_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let total_ops = (n_batches * batch_size) as u64;
        let result = self.create_test_result(test_name, total_ops, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_range_scans(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "range_scans";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 100_000;
        for i in 0..n {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        let n_scans = 1000;
        let scan_size = 100;
        let mut latencies = Vec::with_capacity(n_scans);
        
        let start = Instant::now();
        for _ in 0..n_scans {
            let start_key = format!("key_{:08}", rand::rng().random::<u32>() % (n - scan_size) as u32);
            let end_key = format!("key_{:08}", (n - 1) as u32);
            
            let scan_start = Instant::now();
            let _ = db.range_scan(start_key.as_bytes(), end_key.as_bytes(), scan_size)?;
            latencies.push(scan_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n_scans as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_concurrent_access(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "concurrent_access";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = std::sync::Arc::new(Database::create(&db_path, self.config.clone())?);
        
        let n_threads = 8;
        let ops_per_thread = 10_000;
        let mut handles = vec![];
        
        let start = Instant::now();
        for thread_id in 0..n_threads {
            let db_clone = db.clone();
            let handle = std::thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = format!("thread_{}_key_{:06}", thread_id, i);
                    let value = format!("thread_{}_value_{:06}", thread_id, i);
                    db_clone.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        let duration = start.elapsed();
        
        let total_ops = (n_threads * ops_per_thread) as u64;
        let result = TestResult {
            test_name: test_name.to_string(),
            operations: total_ops,
            duration_ms: duration.as_millis() as u64,
            ops_per_sec: total_ops as f64 / duration.as_secs_f64(),
            latency_us: duration.as_micros() as f64 / total_ops as f64,
            memory_used_mb: 0.0,
            percentiles: Percentiles {
                p50: 0.0,
                p90: 0.0,
                p95: 0.0,
                p99: 0.0,
                p999: 0.0,
            },
        };
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_large_values(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "large_values";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n = 1000;
        let value_size = 100_000;
        let large_value = vec![b'X'; value_size];
        let mut latencies = Vec::with_capacity(n);
        
        let start = Instant::now();
        for i in 0..n {
            let key = format!("large_key_{:06}", i);
            
            let op_start = Instant::now();
            db.put(key.as_bytes(), &large_value)?;
            latencies.push(op_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn test_transactions(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let test_name = "transactions";
        println!("Running {}...", test_name);
        
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join(test_name);
        let db = Database::create(&db_path, self.config.clone())?;
        
        let n_transactions = 1000;
        let ops_per_tx = 10;
        let mut latencies = Vec::with_capacity(n_transactions);
        
        let start = Instant::now();
        for tx_id in 0..n_transactions {
            let tx_start = Instant::now();
            
            let tx = db.begin_transaction()?;
            for i in 0..ops_per_tx {
                let key = format!("tx_{}_key_{}", tx_id, i);
                let value = format!("tx_{}_value_{}", tx_id, i);
                db.put_tx(tx, key.as_bytes(), value.as_bytes())?;
            }
            db.commit_transaction(tx)?;
            
            latencies.push(tx_start.elapsed().as_micros() as f64);
        }
        let duration = start.elapsed();
        
        let result = self.create_test_result(test_name, n_transactions as u64, duration, latencies);
        self.results.push(result);
        
        Ok(())
    }
    
    fn create_test_result(
        &self,
        test_name: &str,
        operations: u64,
        duration: Duration,
        mut latencies: Vec<f64>,
    ) -> TestResult {
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let percentiles = Percentiles {
            p50: self.percentile(&latencies, 0.50),
            p90: self.percentile(&latencies, 0.90),
            p95: self.percentile(&latencies, 0.95),
            p99: self.percentile(&latencies, 0.99),
            p999: self.percentile(&latencies, 0.999),
        };
        
        TestResult {
            test_name: test_name.to_string(),
            operations,
            duration_ms: duration.as_millis() as u64,
            ops_per_sec: operations as f64 / duration.as_secs_f64(),
            latency_us: duration.as_micros() as f64 / operations as f64,
            memory_used_mb: 0.0,
            percentiles,
        }
    }
    
    fn percentile(&self, sorted_values: &[f64], p: f64) -> f64 {
        let index = ((sorted_values.len() as f64 - 1.0) * p) as usize;
        sorted_values[index]
    }
    
    pub fn generate_report(&self) -> PerformanceBaseline {
        PerformanceBaseline {
            version: env!("CARGO_PKG_VERSION").to_string(),
            timestamp: Utc::now(),
            system_info: SystemInfo {
                os: std::env::consts::OS.to_string(),
                cpu_cores: num_cpus::get(),
                memory_gb: 0.0,
                rust_version: "1.70.0".to_string(),
            },
            test_results: self.results.clone(),
        }
    }
    
    pub fn save_results(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let report = self.generate_report();
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(path, json)?;
        Ok(())
    }
}

#[test]
fn test_performance_regression_suite() {
    let mut suite = PerformanceTestSuite::new(PathBuf::from("./test_db"));
    suite.run_all_tests().unwrap();
    
    let report = suite.generate_report();
    println!("\nPerformance Test Results:");
    println!("========================");
    
    for result in &report.test_results {
        println!("\n{}", result.test_name);
        println!("  Operations: {}", result.operations);
        println!("  Duration: {} ms", result.duration_ms);
        println!("  Throughput: {:.0} ops/sec", result.ops_per_sec);
        println!("  Avg Latency: {:.2} μs", result.latency_us);
        println!("  Percentiles:");
        println!("    P50: {:.2} μs", result.percentiles.p50);
        println!("    P90: {:.2} μs", result.percentiles.p90);
        println!("    P95: {:.2} μs", result.percentiles.p95);
        println!("    P99: {:.2} μs", result.percentiles.p99);
        println!("    P99.9: {:.2} μs", result.percentiles.p999);
    }
    
    suite.save_results("performance_baseline.json").unwrap();
    
    let mut detector = RegressionDetector::new(10.0);
    if detector.load_baseline("performance_baseline.json").is_ok() {
        let mut has_regression = false;
        
        for result in &report.test_results {
            let regression = detector.check_regression(result);
            if regression.has_regression {
                has_regression = true;
                println!("\n⚠️ Performance regression detected in {}:", regression.test_name);
                println!("  Throughput change: {:.1}%", regression.ops_change_percent);
                println!("  Latency change: {:.1}%", regression.latency_change_percent);
                println!("  Memory change: {:.1}%", regression.memory_change_percent);
            }
        }
        
        assert!(!has_regression, "Performance regressions detected!");
    }
}
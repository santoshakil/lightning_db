use lightning_db::core::btree::BPlusTree;
use lightning_db::core::transaction::unified_manager::UnifiedTransactionManager;
use lightning_db::core::storage::page_manager_async::AsyncPageManager;
use lightning_db::performance::cache::unified_cache::UnifiedCache;
use lightning_db::utils::config::DatabaseConfig;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::runtime::Runtime;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

struct BenchmarkMetrics {
    operation: String,
    ops_per_sec: f64,
    p50_latency_us: u64,
    p95_latency_us: u64,
    p99_latency_us: u64,
    total_ops: u64,
    duration_ms: u64,
}

struct OltpBenchmark {
    tree: Arc<RwLock<BPlusTree>>,
    tx_manager: Arc<UnifiedTransactionManager>,
    runtime: Runtime,
    metrics: Vec<BenchmarkMetrics>,
}

impl OltpBenchmark {
    fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        let config = DatabaseConfig::default();
        
        let page_manager = runtime.block_on(async {
            AsyncPageManager::new(":memory:", 4096).await
                .expect("Failed to create page manager")
        });
        
        let cache = Arc::new(UnifiedCache::new(100_000));
        let tree = Arc::new(RwLock::new(BPlusTree::new(10)));
        let tx_manager = Arc::new(UnifiedTransactionManager::new(
            Arc::clone(&tree),
            cache,
        ));
        
        Self {
            tree,
            tx_manager,
            runtime,
            metrics: Vec::new(),
        }
    }
    
    fn benchmark_single_inserts(&mut self, num_ops: u64) {
        println!("Benchmarking single inserts ({} ops)...", num_ops);
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        
        for i in 0..num_ops {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:016}", rng.gen::<u64>());
            
            let op_start = Instant::now();
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            self.tx_manager.insert(tx_id, key, value).unwrap();
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("single_insert", latencies, num_ops, duration);
    }
    
    fn benchmark_batch_inserts(&mut self, num_batches: u64, batch_size: u64) {
        println!("Benchmarking batch inserts ({} batches x {} ops)...", num_batches, batch_size);
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(43);
        
        for batch in 0..num_batches {
            let op_start = Instant::now();
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            
            for i in 0..batch_size {
                let key = format!("batch_{:06}_{:04}", batch, i);
                let value = format!("value_{:016}", rng.gen::<u64>());
                self.tx_manager.insert(tx_id, key, value).unwrap();
            }
            
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("batch_insert", latencies, num_batches * batch_size, duration);
    }
    
    fn benchmark_point_reads(&mut self, num_ops: u64) {
        println!("Benchmarking point reads ({} ops)...", num_ops);
        
        // First insert data
        let tx_id = self.tx_manager.begin_transaction().unwrap();
        for i in 0..num_ops {
            let key = format!("read_key_{:08}", i);
            let value = format!("read_value_{:08}", i);
            self.tx_manager.insert(tx_id, key, value).unwrap();
        }
        self.tx_manager.commit_transaction(tx_id).unwrap();
        
        // Now benchmark reads
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(44);
        
        for _ in 0..num_ops {
            let key_idx = rng.gen_range(0..num_ops);
            let key = format!("read_key_{:08}", key_idx);
            
            let op_start = Instant::now();
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            let _ = self.tx_manager.get(tx_id, &key);
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("point_read", latencies, num_ops, duration);
    }
    
    fn benchmark_range_scans(&mut self, num_ops: u64, scan_size: u64) {
        println!("Benchmarking range scans ({} ops, {} items per scan)...", num_ops, scan_size);
        
        // First insert data
        let tx_id = self.tx_manager.begin_transaction().unwrap();
        for i in 0..(num_ops * scan_size) {
            let key = format!("scan_{:010}", i);
            let value = format!("value_{:08}", i);
            self.tx_manager.insert(tx_id, key, value).unwrap();
        }
        self.tx_manager.commit_transaction(tx_id).unwrap();
        
        // Benchmark range scans
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(45);
        
        for _ in 0..num_ops {
            let start_idx = rng.gen_range(0..(num_ops * scan_size - scan_size));
            let start_key = format!("scan_{:010}", start_idx);
            let end_key = format!("scan_{:010}", start_idx + scan_size);
            
            let op_start = Instant::now();
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            let tree = self.tree.read();
            let _ = tree.range(&start_key, &end_key);
            drop(tree);
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("range_scan", latencies, num_ops, duration);
    }
    
    fn benchmark_updates(&mut self, num_ops: u64) {
        println!("Benchmarking updates ({} ops)...", num_ops);
        
        // Insert initial data
        let tx_id = self.tx_manager.begin_transaction().unwrap();
        for i in 0..num_ops {
            let key = format!("update_key_{:08}", i);
            let value = format!("initial_value_{:08}", i);
            self.tx_manager.insert(tx_id, key, value).unwrap();
        }
        self.tx_manager.commit_transaction(tx_id).unwrap();
        
        // Benchmark updates
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(46);
        
        for _ in 0..num_ops {
            let key_idx = rng.gen_range(0..num_ops);
            let key = format!("update_key_{:08}", key_idx);
            let value = format!("updated_value_{:016}", rng.gen::<u64>());
            
            let op_start = Instant::now();
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            self.tx_manager.update(tx_id, key, value).unwrap();
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("update", latencies, num_ops, duration);
    }
    
    fn benchmark_mixed_workload(&mut self, num_ops: u64) {
        println!("Benchmarking mixed workload ({} ops)...", num_ops);
        
        // Pre-populate some data
        let tx_id = self.tx_manager.begin_transaction().unwrap();
        for i in 0..1000 {
            let key = format!("mixed_key_{:08}", i);
            let value = format!("mixed_value_{:08}", i);
            self.tx_manager.insert(tx_id, key, value).unwrap();
        }
        self.tx_manager.commit_transaction(tx_id).unwrap();
        
        let mut latencies = Vec::new();
        let start = Instant::now();
        let mut rng = ChaCha8Rng::seed_from_u64(47);
        
        for i in 0..num_ops {
            let op_type = rng.gen_range(0..100);
            let op_start = Instant::now();
            
            let tx_id = self.tx_manager.begin_transaction().unwrap();
            
            if op_type < 30 {
                // 30% inserts
                let key = format!("mixed_new_{:08}", i);
                let value = format!("value_{:016}", rng.gen::<u64>());
                self.tx_manager.insert(tx_id, key, value).unwrap();
            } else if op_type < 80 {
                // 50% reads
                let key_idx = rng.gen_range(0..1000);
                let key = format!("mixed_key_{:08}", key_idx);
                let _ = self.tx_manager.get(tx_id, &key);
            } else {
                // 20% updates
                let key_idx = rng.gen_range(0..1000);
                let key = format!("mixed_key_{:08}", key_idx);
                let value = format!("updated_{:016}", rng.gen::<u64>());
                self.tx_manager.update(tx_id, key, value).unwrap();
            }
            
            self.tx_manager.commit_transaction(tx_id).unwrap();
            latencies.push(op_start.elapsed().as_micros() as u64);
        }
        
        let duration = start.elapsed();
        self.record_metrics("mixed_workload", latencies, num_ops, duration);
    }
    
    fn record_metrics(&mut self, operation: &str, mut latencies: Vec<u64>, total_ops: u64, duration: Duration) {
        latencies.sort_unstable();
        
        let p50 = latencies[latencies.len() / 2];
        let p95 = latencies[latencies.len() * 95 / 100];
        let p99 = latencies[latencies.len() * 99 / 100];
        
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        
        let metrics = BenchmarkMetrics {
            operation: operation.to_string(),
            ops_per_sec,
            p50_latency_us: p50,
            p95_latency_us: p95,
            p99_latency_us: p99,
            total_ops,
            duration_ms: duration.as_millis() as u64,
        };
        
        println!("  {} results:", operation);
        println!("    Throughput: {:.0} ops/sec", ops_per_sec);
        println!("    Latency p50: {} μs", p50);
        println!("    Latency p95: {} μs", p95);
        println!("    Latency p99: {} μs", p99);
        println!();
        
        self.metrics.push(metrics);
    }
    
    fn generate_report(&self) {
        println!("\n" + "=".repeat(60));
        println!("OLTP PERFORMANCE BENCHMARK REPORT");
        println!("=".repeat(60));
        println!();
        
        println!("| Operation       | Throughput (ops/s) | p50 (μs) | p95 (μs) | p99 (μs) |");
        println!("|-----------------|-------------------|----------|----------|----------|");
        
        for metric in &self.metrics {
            println!("| {:15} | {:17.0} | {:8} | {:8} | {:8} |",
                metric.operation,
                metric.ops_per_sec,
                metric.p50_latency_us,
                metric.p95_latency_us,
                metric.p99_latency_us
            );
        }
        
        println!();
        println!("Summary:");
        println!("--------");
        
        let avg_throughput: f64 = self.metrics.iter()
            .map(|m| m.ops_per_sec)
            .sum::<f64>() / self.metrics.len() as f64;
        
        let avg_p50: u64 = self.metrics.iter()
            .map(|m| m.p50_latency_us)
            .sum::<u64>() / self.metrics.len() as u64;
        
        let avg_p99: u64 = self.metrics.iter()
            .map(|m| m.p99_latency_us)
            .sum::<u64>() / self.metrics.len() as u64;
        
        println!("Average Throughput: {:.0} ops/sec", avg_throughput);
        println!("Average p50 Latency: {} μs", avg_p50);
        println!("Average p99 Latency: {} μs", avg_p99);
        
        // Check against SLOs
        println!();
        println!("SLO Compliance:");
        println!("--------------");
        
        let p50_target = 1000; // 1ms in microseconds
        let p95_target = 10000; // 10ms
        let p99_target = 50000; // 50ms
        let tps_target = 100000.0;
        
        let p50_pass = avg_p50 < p50_target;
        let p99_pass = avg_p99 < p99_target;
        let tps_pass = avg_throughput > tps_target;
        
        println!("p50 < 1ms: {} ({} μs)", 
            if p50_pass { "✅ PASS" } else { "❌ FAIL" }, 
            avg_p50
        );
        println!("p99 < 50ms: {} ({} μs)", 
            if p99_pass { "✅ PASS" } else { "❌ FAIL" },
            avg_p99
        );
        println!("TPS > 100K: {} ({:.0} ops/s)",
            if tps_pass { "✅ PASS" } else { "❌ FAIL" },
            avg_throughput
        );
        
        let all_pass = p50_pass && p99_pass && tps_pass;
        println!();
        println!("Overall: {}", if all_pass { "✅ ALL SLOs MET" } else { "⚠️ SLOs NOT MET" });
    }
}

fn main() {
    println!("Lightning DB OLTP Performance Benchmark");
    println!("========================================");
    println!();
    
    let mut benchmark = OltpBenchmark::new();
    
    // Run benchmarks with realistic workload sizes
    benchmark.benchmark_single_inserts(10000);
    benchmark.benchmark_batch_inserts(100, 100);
    benchmark.benchmark_point_reads(10000);
    benchmark.benchmark_range_scans(1000, 100);
    benchmark.benchmark_updates(5000);
    benchmark.benchmark_mixed_workload(10000);
    
    // Generate comprehensive report
    benchmark.generate_report();
}
use lightning_db::{Database, LightningDbConfig};
use std::collections::HashMap;
use std::path::Path;
use tempfile::tempdir;

// Example of using the regression benchmark suite programmatically
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB Regression Benchmark Demo");
    println!("=====================================");

    // Example 1: Basic performance comparison
    println!("\n1. Running basic performance comparison...");
    let baseline_performance = run_baseline_benchmark()?;
    println!("Baseline performance: {:.2} ops/sec", baseline_performance);

    let current_performance = run_current_benchmark()?;
    println!("Current performance: {:.2} ops/sec", current_performance);

    let change_percent = ((current_performance - baseline_performance) / baseline_performance) * 100.0;
    println!("Performance change: {:+.2}%", change_percent);

    if change_percent < -5.0 {
        println!("⚠️  Potential regression detected!");
    } else if change_percent > 5.0 {
        println!("🚀 Performance improvement detected!");
    } else {
        println!("✅ Performance stable");
    }

    // Example 2: Memory usage analysis
    println!("\n2. Analyzing memory usage patterns...");
    analyze_memory_usage()?;

    // Example 3: Concurrent performance testing
    println!("\n3. Testing concurrent performance...");
    test_concurrent_performance()?;

    // Example 4: Cache performance analysis  
    println!("\n4. Analyzing cache performance...");
    test_cache_performance()?;

    println!("\n✅ Demo completed successfully!");
    println!("\nTo run the full regression suite:");
    println!("  cargo bench comprehensive_regression_suite");
    println!("  ./scripts/run_regression_benchmarks.sh --quick");

    Ok(())
}

fn run_baseline_benchmark() -> Result<f64, Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB cache
        prefetch_enabled: true,
        compression_enabled: false,
        ..Default::default()
    };
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Populate test data
    let value = vec![0u8; 1024];
    for i in 0..10_000 {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }
    
    // Measure read performance
    let start = std::time::Instant::now();
    let iterations = 100_000;
    
    for i in 0..iterations {
        let key = format!("key_{:08}", i % 10_000);
        let _result = db.get(key.as_bytes())?;
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    
    Ok(ops_per_sec)
}

fn run_current_benchmark() -> Result<f64, Box<dyn std::error::Error>> {
    // Simulate current implementation (in real scenario, this would test the latest code)
    let temp_dir = tempdir()?;
    let config = LightningDbConfig {
        cache_size: 256 * 1024 * 1024, // 256MB cache
        prefetch_enabled: true,
        compression_enabled: false,
        ..Default::default()
    };
    
    let db = Database::create(temp_dir.path(), config)?;
    
    // Populate test data
    let value = vec![0u8; 1024];
    for i in 0..10_000 {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &value)?;
    }
    
    // Measure read performance with slight variation to simulate real scenarios
    let start = std::time::Instant::now();
    let iterations = 100_000;
    
    for i in 0..iterations {
        let key = format!("key_{:08}", i % 10_000);
        let _result = db.get(key.as_bytes())?;
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
    
    // Simulate small performance variation (in real testing, this would be actual measured difference)
    let variation = 1.0 + (rand::random::<f64>() - 0.5) * 0.1; // ±5% variation
    
    Ok(ops_per_sec * variation)
}

fn analyze_memory_usage() -> Result<(), Box<dyn std::error::Error>> {
    let memory_scenarios = vec![
        ("Small values (64B)", 64, 50_000),
        ("Medium values (1KB)", 1024, 20_000),
        ("Large values (8KB)", 8192, 5_000),
    ];
    
    for (name, value_size, count) in memory_scenarios {
        let temp_dir = tempdir()?;
        let config = LightningDbConfig {
            cache_size: 128 * 1024 * 1024, // 128MB cache
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config)?;
        let value = vec![0u8; value_size];
        
        let start_time = std::time::Instant::now();
        
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        let write_time = start_time.elapsed();
        let write_throughput = count as f64 / write_time.as_secs_f64();
        
        println!("  {}: {:.0} ops/sec", name, write_throughput);
    }
    
    Ok(())
}

fn test_concurrent_performance() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;
    use std::thread;
    
    let thread_counts = vec![1, 2, 4, 8];
    
    for thread_count in thread_counts {
        let temp_dir = tempdir()?;
        let config = LightningDbConfig {
            cache_size: 512 * 1024 * 1024, // 512MB cache
            ..Default::default()
        };
        
        let db = Arc::new(Database::create(temp_dir.path(), config)?);
        
        // Pre-populate data
        let value = vec![0u8; 512];
        for i in 0..10_000 {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        let start_time = std::time::Instant::now();
        let ops_per_thread = 10_000;
        
        let handles: Vec<_> = (0..thread_count)
            .map(|thread_id| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..ops_per_thread {
                        let key = format!("key_{:08}", (thread_id * 1000 + i) % 10_000);
                        let _result = db.get(key.as_bytes());
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let elapsed = start_time.elapsed();
        let total_ops = thread_count * ops_per_thread;
        let throughput = total_ops as f64 / elapsed.as_secs_f64();
        
        println!("  {} threads: {:.0} ops/sec", thread_count, throughput);
    }
    
    Ok(())
}

fn test_cache_performance() -> Result<(), Box<dyn std::error::Error>> {
    // Test cache hit vs cache miss performance
    let cache_scenarios = vec![
        ("Hot data (100% cache hit)", 1000, 128 * 1024 * 1024),
        ("Cold data (cache miss)", 100_000, 64 * 1024 * 1024),
    ];
    
    for (name, data_size, cache_size) in cache_scenarios {
        let temp_dir = tempdir()?;
        let config = LightningDbConfig {
            cache_size,
            prefetch_enabled: true,
            ..Default::default()
        };
        
        let db = Database::create(temp_dir.path(), config)?;
        let value = vec![0u8; 1024];
        
        // Populate data
        for i in 0..data_size {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), &value)?;
        }
        
        // Prime cache for hot data scenario
        if data_size <= 1000 {
            for i in 0..data_size {
                let key = format!("key_{:08}", i);
                let _ = db.get(key.as_bytes());
            }
        }
        
        // Measure performance
        let start_time = std::time::Instant::now();
        let test_ops = 50_000;
        
        for i in 0..test_ops {
            let key = format!("key_{:08}", i % data_size);
            let _result = db.get(key.as_bytes())?;
        }
        
        let elapsed = start_time.elapsed();
        let throughput = test_ops as f64 / elapsed.as_secs_f64();
        
        println!("  {}: {:.0} ops/sec", name, throughput);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_demo() {
        // Simple test to ensure the demo functions work
        assert!(run_baseline_benchmark().is_ok());
        assert!(run_current_benchmark().is_ok());
        assert!(analyze_memory_usage().is_ok());
        assert!(test_concurrent_performance().is_ok());
        assert!(test_cache_performance().is_ok());
    }
    
    #[test] 
    fn test_performance_comparison() {
        let baseline = run_baseline_benchmark().unwrap();
        let current = run_current_benchmark().unwrap();
        
        // Both should be positive and reasonable
        assert!(baseline > 0.0);
        assert!(current > 0.0);
        
        // Should be in expected range (rough sanity check)
        assert!(baseline > 1000.0); // At least 1K ops/sec
        assert!(current > 1000.0);
        
        // Difference should be reasonable (not more than 10x)
        let ratio = current / baseline;
        assert!(ratio > 0.1 && ratio < 10.0);
    }
}
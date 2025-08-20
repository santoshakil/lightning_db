//! Lightning DB I/O Performance Demo
//!
//! This example demonstrates the I/O optimizations implemented in Lightning DB,
//! including direct I/O, memory-mapped files, buffer management, and read-ahead.

use lightning_db::io_uring::{
    benchmarks::{BenchmarkConfig, IoBenchmarkSuite},
    zero_copy_buffer::{HighPerformanceBufferManager, BufferAlignment},
};
use std::time::Duration;

fn main() {
    println!("🌩️  Lightning DB I/O Performance Demo");
    println!("=====================================\n");
    
    // Create and run benchmarks
    match run_performance_demo() {
        Ok(_) => println!("\n✅ Demo completed successfully!"),
        Err(e) => eprintln!("\n❌ Demo failed: {}", e),
    }
}

fn run_performance_demo() -> Result<(), Box<dyn std::error::Error>> {
    // Demonstrate buffer manager performance
    demonstrate_buffer_management()?;
    
    // Run I/O benchmarks
    run_io_benchmarks()?;
    
    Ok(())
}

fn demonstrate_buffer_management() -> Result<(), Box<dyn std::error::Error>> {
    println!("📦 Buffer Management Demo");
    println!("------------------------");
    
    let buffer_manager = HighPerformanceBufferManager::new();
    
    // Allocate buffers of different sizes
    let sizes = [4096, 16384, 32768, 65536, 262144];
    let mut buffers = Vec::new();
    
    let start = std::time::Instant::now();
    
    // Allocate buffers
    for &size in &sizes {
        for _ in 0..10 {
            let buffer = buffer_manager.acquire_optimized(size)
                .map_err(|e| format!("Buffer allocation failed: {}", e))?;
            buffers.push(buffer);
        }
    }
    
    let alloc_time = start.elapsed();
    println!("  ✓ Allocated {} buffers in {:?}", buffers.len(), alloc_time);
    
    // Release buffers
    let start = std::time::Instant::now();
    for buffer in buffers {
        buffer_manager.release(buffer);
    }
    let release_time = start.elapsed();
    println!("  ✓ Released all buffers in {:?}", release_time);
    
    // Show statistics
    let stats = buffer_manager.stats();
    println!("  📊 Stats: {} hits, {} misses, {:.1}% hit rate",
        stats.hits.load(std::sync::atomic::Ordering::Relaxed),
        stats.misses.load(std::sync::atomic::Ordering::Relaxed),
        if stats.hits.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            stats.hits.load(std::sync::atomic::Ordering::Relaxed) as f64 / 
            (stats.hits.load(std::sync::atomic::Ordering::Relaxed) + 
             stats.misses.load(std::sync::atomic::Ordering::Relaxed)) as f64 * 100.0
        } else { 0.0 }
    );
    
    println!();
    Ok(())
}

fn run_io_benchmarks() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ I/O Performance Benchmarks");
    println!("----------------------------");
    
    let mut suite = IoBenchmarkSuite::new()?;
    
    // Run a subset of benchmarks for the demo
    let configs = vec![
        BenchmarkConfig {
            file_size: 100 * 1024 * 1024, // 100MB for quick demo
            io_size: 16 * 1024,           // 16KB I/O
            num_operations: 1000,         // Reduced for quick demo
            num_threads: 2,               // Reduced for quick demo
            test_duration: Duration::from_secs(10),
            warmup_duration: Duration::from_secs(1),
            use_direct_io: true,
            use_mmap: false,
            use_readahead: true,
            sequential_ratio: 0.8,
            write_ratio: 0.3,
        },
        BenchmarkConfig {
            file_size: 100 * 1024 * 1024,
            io_size: 32 * 1024,
            num_operations: 1000,
            num_threads: 2,
            test_duration: Duration::from_secs(10),
            warmup_duration: Duration::from_secs(1),
            use_direct_io: false,
            use_mmap: true,
            use_readahead: false,
            sequential_ratio: 0.8,
            write_ratio: 0.3,
        },
    ];
    
    for (i, config) in configs.iter().enumerate() {
        println!("  🏃 Running benchmark {} of {}...", i + 1, configs.len());
        
        match suite.run_benchmark(config) {
            Ok(results) => {
                println!("    ✓ Completed: {:.1} MB/s, {:.0} IOPS, {:.1}μs avg latency",
                    results.throughput_mb_per_second,
                    results.operations_per_second,
                    results.average_latency_us
                );
            }
            Err(e) => {
                println!("    ❌ Failed: {}", e);
            }
        }
    }
    
    // Generate and display report
    let report = suite.generate_report();
    println!("\n📈 Performance Report");
    println!("====================");
    println!("{}", report);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_buffer_management() {
        demonstrate_buffer_management().unwrap();
    }
}
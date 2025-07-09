use lightning_db::optimizations::*;
use lightning_db::optimizations::simd_ops;
use std::time::Instant;
use rand::Rng;

/// Comprehensive performance benchmark for Lightning DB optimizations
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Lightning DB Performance Optimizations Benchmark");
    println!("==================================================");
    
    // Test SIMD operations
    benchmark_simd_operations();
    
    // Test memory layout optimizations
    benchmark_memory_layout();
    
    // Test cache-friendly algorithms
    benchmark_cache_friendly_algorithms();
    
    // Test optimization manager
    benchmark_optimization_manager();
    
    // Test workload-specific configurations
    benchmark_workload_configurations();
    
    println!("\n✅ All optimization benchmarks completed successfully!");
    
    Ok(())
}

fn benchmark_simd_operations() {
    println!("\n🔥 SIMD Operations Benchmark");
    println!("----------------------------");
    
    let data_sizes = vec![1024, 4096, 16384, 65536];
    
    for size in data_sizes {
        println!("\nTesting with {} bytes of data:", size);
        
        // Generate test data
        let mut rng = rand::thread_rng();
        let data1: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        let data2: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        let mut dst = vec![0u8; size];
        
        // Benchmark key comparison
        let start = Instant::now();
        for _ in 0..10000 {
            simd_ops::compare_keys(&data1, &data2);
        }
        let comparison_time = start.elapsed();
        
        // Benchmark CRC32 calculation
        let start = Instant::now();
        for _ in 0..10000 {
            simd_ops::crc32(&data1);
        }
        let crc32_time = start.elapsed();
        
        // Benchmark bulk copy
        let start = Instant::now();
        for _ in 0..10000 {
            simd_ops::bulk_copy(&data1, &mut dst);
        }
        let bulk_copy_time = start.elapsed();
        
        // Benchmark hash calculation
        let start = Instant::now();
        for _ in 0..10000 {
            simd_ops::hash(&data1, 42);
        }
        let hash_time = start.elapsed();
        
        println!("  Key comparison: {:?} (10k ops)", comparison_time);
        println!("  CRC32 calculation: {:?} (10k ops)", crc32_time);
        println!("  Bulk copy: {:?} (10k ops)", bulk_copy_time);
        println!("  Hash calculation: {:?} (10k ops)", hash_time);
        
        // Calculate throughput
        let throughput = (10000.0 * size as f64) / comparison_time.as_secs_f64();
        println!("  Comparison throughput: {:.2} MB/s", throughput / 1_000_000.0);
    }
}

fn benchmark_memory_layout() {
    println!("\n💾 Memory Layout Optimization Benchmark");
    println!("---------------------------------------");
    
    // Test cache-aligned allocation
    let start = Instant::now();
    let mut ptrs = Vec::new();
    for _ in 0..10000 {
        let ptr = CacheAlignedAllocator::allocate(1024).unwrap();
        ptrs.push(ptr);
    }
    let alloc_time = start.elapsed();
    
    // Clean up
    for ptr in ptrs {
        unsafe {
            CacheAlignedAllocator::deallocate(ptr, 1024);
        }
    }
    
    println!("Cache-aligned allocation: {:?} (10k allocations)", alloc_time);
    
    // Test optimized B-tree node operations
    let mut node = OptimizedBTreeNode::new(0, 1);
    let start = Instant::now();
    
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let _ = node.insert_key_value(key.as_bytes(), i as u64);
    }
    let insert_time = start.elapsed();
    
    println!("B-tree node insertions: {:?} (1k ops)", insert_time);
    
    // Test object pool
    let mut pool = ObjectPool::new(|| Vec::with_capacity(1024), 100);
    let start = Instant::now();
    
    for _ in 0..10000 {
        let obj = pool.get();
        pool.return_object(obj);
    }
    let pool_time = start.elapsed();
    
    println!("Object pool operations: {:?} (10k get/return cycles)", pool_time);
    
    // Test memory-mapped buffer
    let mut buffer = MappedBuffer::new(1024 * 1024).unwrap();
    let start = Instant::now();
    
    for i in 0..1000 {
        buffer.prefetch(i * 1024, 1024);
    }
    let prefetch_time = start.elapsed();
    
    println!("Memory prefetch operations: {:?} (1k ops)", prefetch_time);
    
    // Test compact records
    let key = b"benchmark_key";
    let value = b"benchmark_value_with_some_data";
    let record_size = CompactRecord::calculate_size(key.len(), value.len());
    
    let start = Instant::now();
    for _ in 0..10000 {
        let mut buffer = vec![0u8; record_size];
        unsafe {
            let _record = CompactRecord::create_in_buffer(&mut buffer, key, value).unwrap();
        }
    }
    let record_time = start.elapsed();
    
    println!("Compact record creation: {:?} (10k ops)", record_time);
}

fn benchmark_cache_friendly_algorithms() {
    println!("\n🏃 Cache-Friendly Algorithms Benchmark");
    println!("--------------------------------------");
    
    // Test cache-friendly merge sort
    let sizes = vec![1000, 10000, 100000];
    
    for size in sizes {
        let mut data: Vec<u32> = (0..size).map(|_| rand::thread_rng().gen()).collect();
        
        let start = Instant::now();
        CacheFriendlyAlgorithms::merge_sort_keys(&mut data);
        let sort_time = start.elapsed();
        
        println!("Cache-friendly sort ({} items): {:?}", size, sort_time);
        
        // Verify sorting
        assert!(data.windows(2).all(|w| w[0] <= w[1]));
    }
    
    // Test binary search with prefetching
    let data: Vec<u32> = (0..100000).map(|i| i * 2).collect();
    let start = Instant::now();
    
    for i in 0..10000 {
        let target = i * 2;
        let _ = CacheFriendlyAlgorithms::binary_search_with_prefetch(&data, &target);
    }
    let search_time = start.elapsed();
    
    println!("Binary search with prefetch: {:?} (10k searches)", search_time);
    
    // Test sequential scan
    let data: Vec<u32> = (0..100000).collect();
    let start = Instant::now();
    
    let results = CacheFriendlyAlgorithms::sequential_scan_optimized(&data, |x| *x % 100 == 0);
    let scan_time = start.elapsed();
    
    println!("Sequential scan: {:?} (found {} matches)", scan_time, results.len());
    
    // Test Boyer-Moore string search
    let text = "the quick brown fox jumps over the lazy dog the quick brown fox".repeat(1000);
    let pattern = b"fox";
    let start = Instant::now();
    
    let matches = CacheFriendlyAlgorithms::boyer_moore_optimized(text.as_bytes(), pattern);
    let search_time = start.elapsed();
    
    println!("Boyer-Moore search: {:?} (found {} matches)", search_time, matches.len());
    
    // Test cache-friendly LRU
    let mut lru = CacheFriendlyAlgorithms::CacheFriendlyLRU::new(1000);
    let start = Instant::now();
    
    for i in 0..10000 {
        lru.put(i, format!("value_{}", i));
    }
    let lru_time = start.elapsed();
    
    println!("Cache-friendly LRU: {:?} (10k insertions)", lru_time);
    
    // Test cache-friendly B-tree
    let mut btree = CacheFriendlyBTree::new(100);
    let start = Instant::now();
    
    for i in 0..10000 {
        btree.insert(i, format!("value_{}", i));
    }
    let btree_time = start.elapsed();
    
    println!("Cache-friendly B-tree: {:?} (10k insertions)", btree_time);
}

fn benchmark_optimization_manager() {
    println!("\n⚡ Optimization Manager Benchmark");
    println!("--------------------------------");
    
    let config = OptimizationConfig::default();
    let mut manager = OptimizationManager::new(config);
    
    // Test object pool performance
    let start = Instant::now();
    let mut nodes = Vec::new();
    
    for _ in 0..1000 {
        let node = manager.get_btree_node();
        nodes.push(node);
    }
    
    for node in nodes {
        manager.return_btree_node(node);
    }
    let pool_time = start.elapsed();
    
    println!("Object pool operations: {:?} (1k get/return cycles)", pool_time);
    
    // Test optimized operations
    let data1 = b"benchmark_key_1";
    let data2 = b"benchmark_key_2";
    let haystack = b"this is a test string for searching benchmark";
    let needle = b"search";
    
    let start = Instant::now();
    for _ in 0..10000 {
        let _ = manager.compare_keys(data1, data2);
        let _ = manager.calculate_hash(data1, 42);
        let _ = manager.search_prefix(haystack, needle);
    }
    let ops_time = start.elapsed();
    
    println!("Optimized operations: {:?} (30k ops total)", ops_time);
    
    // Print statistics
    let stats = manager.get_stats();
    println!("Performance statistics:");
    println!("  SIMD operations: {}", stats.simd_operations);
    println!("  Cache hit rate: {:.2}%", stats.cache_hit_rate() * 100.0);
    println!("  Pool hit rate: {:.2}%", stats.pool_hit_rate() * 100.0);
    println!("  Total operations: {}", stats.total_operations());
}

fn benchmark_workload_configurations() {
    println!("\n🎯 Workload-Specific Configuration Benchmark");
    println!("--------------------------------------------");
    
    let workloads = vec![
        ("Read Heavy", WorkloadType::ReadHeavy),
        ("Write Heavy", WorkloadType::WriteHeavy),
        ("Mixed", WorkloadType::Mixed),
        ("Analytical Scan", WorkloadType::AnalyticalScan),
    ];
    
    for (name, workload_type) in workloads {
        println!("\nTesting {} workload:", name);
        
        let config = utils::create_workload_config(workload_type);
        let mut manager = OptimizationManager::new(config);
        
        // Simulate workload-specific operations
        let start = Instant::now();
        
        match workload_type {
            WorkloadType::ReadHeavy => {
                // Simulate read-heavy workload
                for i in 0..10000 {
                    let key = format!("key_{}", i % 1000);
                    let _ = manager.calculate_hash(key.as_bytes(), 42);
                }
            }
            WorkloadType::WriteHeavy => {
                // Simulate write-heavy workload
                for i in 0..1000 {
                    let node = manager.get_btree_node();
                    let key = format!("key_{}", i);
                    let _ = node.insert_key_value(key.as_bytes(), i as u64);
                    manager.return_btree_node(node);
                }
            }
            WorkloadType::Mixed => {
                // Simulate mixed workload
                for i in 0..5000 {
                    let key = format!("key_{}", i);
                    let _ = manager.calculate_hash(key.as_bytes(), 42);
                    
                    if i % 10 == 0 {
                        let node = manager.get_btree_node();
                        let _ = node.insert_key_value(key.as_bytes(), i as u64);
                        manager.return_btree_node(node);
                    }
                }
            }
            WorkloadType::AnalyticalScan => {
                // Simulate analytical scan workload
                let data: Vec<u32> = (0..100000).collect();
                let _results = CacheFriendlyAlgorithms::sequential_scan_optimized(&data, |x| *x % 1000 == 0);
            }
        }
        
        let workload_time = start.elapsed();
        println!("  Execution time: {:?}", workload_time);
        
        let stats = manager.get_stats();
        println!("  Operations: {}", stats.total_operations());
        println!("  Pool utilization: {:.2}%", stats.btree_node_pool_stats.utilization * 100.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_optimization_benchmarks() {
        // Run a subset of benchmarks for testing
        benchmark_simd_operations();
        benchmark_memory_layout();
        benchmark_cache_friendly_algorithms();
        benchmark_optimization_manager();
        benchmark_workload_configurations();
    }
}
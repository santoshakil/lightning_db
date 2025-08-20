use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use lightning_db::performance::{
    lock_free::{HotPathCache, CacheEntryRef, ZeroCopyWriteBuffer},
    memory::thread_local_pools::{with_vec_buffer, with_large_buffer, get_pool_stats},
    optimizations::simd::safe::{compare_keys, crc32, bulk_copy},
    async_checksum::{AsyncChecksumPipeline, BatchChecksumCalculator},
    zero_copy_batch::{ZeroCopyWriteBatch, PooledZeroCopyBatcher, ArenaZeroCopyBatcher},
};
use bytes::Bytes;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::runtime::Runtime;

/// Benchmark the zero-copy hot path cache vs cloning cache
fn bench_hot_path_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_path_cache");
    
    // Test data
    let keys: Vec<String> = (0..1000).map(|i| format!("key_{:06}", i)).collect();
    let values: Vec<String> = (0..1000).map(|i| format!("value_{:06}", i)).collect();
    
    // Benchmark cloning cache (original)
    group.bench_function("cloning_cache", |b| {
        let cache = Arc::new(HotPathCache::<String, String, 1024>::new());
        
        // Pre-populate cache
        for (key, value) in keys.iter().zip(values.iter()) {
            cache.insert(key.clone(), value.clone());
        }
        
        b.iter(|| {
            for key in &keys {
                if let Some(value) = cache.get(key) {
                    black_box(value);
                }
            }
        });
    });
    
    // Benchmark zero-copy cache
    group.bench_function("zero_copy_cache", |b| {
        let cache = Arc::new(HotPathCache::<String, String, 1024>::new());
        
        // Pre-populate cache
        for (key, value) in keys.iter().zip(values.iter()) {
            cache.insert(key.clone(), value.clone());
        }
        
        b.iter(|| {
            for key in &keys {
                if let Some(entry_ref) = cache.get_ref(key) {
                    black_box(entry_ref.value());
                }
            }
        });
    });
    
    group.finish();
}

/// Benchmark thread-local memory pools vs global pools
fn bench_memory_pools(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pools");
    group.throughput(Throughput::Elements(1000));
    
    // Benchmark global pool with mutex contention
    group.bench_function("global_pool", |b| {
        use lightning_db::performance::memory::pool::{get_vec_buffer};
        
        b.iter(|| {
            for _ in 0..1000 {
                let mut buf = get_vec_buffer();
                buf.extend_from_slice(b"test data for benchmarking");
                black_box(&*buf);
            }
        });
    });
    
    // Benchmark thread-local pools (no contention)
    group.bench_function("thread_local_pool", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                with_vec_buffer(|buf| {
                    buf.extend_from_slice(b"test data for benchmarking");
                    black_box(&*buf);
                });
            }
        });
    });
    
    group.finish();
}

/// Benchmark SIMD vs scalar operations
fn bench_simd_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_operations");
    
    // Test data
    let key1 = vec![0xAA; 1024];
    let key2 = vec![0xBB; 1024];
    let data = vec![0xCC; 64 * 1024];
    
    // Benchmark key comparison
    group.bench_function("key_compare_simd", |b| {
        b.iter(|| {
            black_box(compare_keys(&key1, &key2));
        });
    });
    
    group.bench_function("key_compare_scalar", |b| {
        b.iter(|| {
            black_box(key1.cmp(&key2));
        });
    });
    
    // Benchmark CRC32 calculation
    group.bench_function("crc32_simd", |b| {
        b.iter(|| {
            black_box(crc32(&data));
        });
    });
    
    group.bench_function("crc32_scalar", |b| {
        b.iter(|| {
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&data);
            black_box(hasher.finalize());
        });
    });
    
    // Benchmark bulk copy
    group.bench_function("bulk_copy_simd", |b| {
        let mut dst = vec![0; data.len()];
        b.iter(|| {
            bulk_copy(&data, &mut dst);
            black_box(&dst);
        });
    });
    
    group.bench_function("bulk_copy_scalar", |b| {
        let mut dst = vec![0; data.len()];
        b.iter(|| {
            dst.copy_from_slice(&data);
            black_box(&dst);
        });
    });
    
    group.finish();
}

/// Benchmark async checksum pipeline
fn bench_async_checksum(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("async_checksum");
    group.throughput(Throughput::Elements(100));
    
    // Test data
    let pages: Vec<(u32, Bytes)> = (0..100)
        .map(|i| (i, Bytes::from(vec![i as u8; 4096])))
        .collect();
    
    // Benchmark synchronous checksum calculation
    group.bench_function("sync_checksum", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for (page_id, data) in &pages {
                let checksum = crc32(data);
                results.push((*page_id, checksum));
            }
            black_box(results);
        });
    });
    
    // Benchmark async pipelined checksum calculation
    group.bench_function("async_pipeline", |b| {
        b.to_async(&rt).iter(|| async {
            let mut pipeline = AsyncChecksumPipeline::new(4);
            let results = pipeline.process_pages_pipelined(pages.clone()).await.unwrap();
            pipeline.shutdown().await.unwrap();
            black_box(results);
        });
    });
    
    // Benchmark batch checksum calculator
    group.bench_function("batch_calculator", |b| {
        b.to_async(&rt).iter(|| async {
            let mut calculator = BatchChecksumCalculator::new(4, 25);
            
            for (page_id, data) in &pages {
                if let Some(results) = calculator.add_page(*page_id, data.clone()).await.unwrap() {
                    black_box(results);
                }
            }
            
            let final_results = calculator.finish().await.unwrap();
            black_box(final_results);
        });
    });
    
    group.finish();
}

/// Benchmark zero-copy write batching
fn bench_zero_copy_batching(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batching");
    group.throughput(Throughput::Elements(1000));
    
    // Test data
    let operations: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key_{}", i).into_bytes(), format!("value_{}", i).into_bytes()))
        .collect();
    
    // Benchmark traditional write batch (with cloning)
    group.bench_function("traditional_batch", |b| {
        use lightning_db::utils::batching::WriteBatch;
        
        b.iter(|| {
            let mut batch = WriteBatch::new();
            for (key, value) in &operations {
                batch.put(key.clone(), value.clone()).unwrap();
            }
            black_box(batch.operations());
        });
    });
    
    // Benchmark zero-copy write batch
    group.bench_function("zero_copy_batch", |b| {
        b.iter(|| {
            let mut batch = ZeroCopyWriteBatch::new(1000);
            unsafe {
                for (key, value) in &operations {
                    batch.put_zero_copy(key, value).unwrap();
                }
            }
            let serialized = batch.serialize_to_buffer();
            black_box(serialized);
        });
    });
    
    // Benchmark pooled zero-copy batcher
    group.bench_function("pooled_zero_copy", |b| {
        b.iter(|| {
            let mut batcher = PooledZeroCopyBatcher::new(4, 1000, 64 * 1024);
            unsafe {
                for (key, value) in &operations {
                    batcher.put(key, value).unwrap();
                }
            }
            if let Some(batch) = batcher.take_batch() {
                let serialized = batch.serialize_to_buffer();
                black_box(serialized);
                batcher.return_batch(batch);
            }
        });
    });
    
    // Benchmark arena-based batcher
    group.bench_function("arena_batch", |b| {
        b.iter(|| {
            let mut batcher = ArenaZeroCopyBatcher::new(1024 * 1024, 1000);
            for (key, value) in &operations {
                batcher.put_arena(key, value).unwrap();
            }
            
            batcher.execute_arena(|_op_type, _key, _value| {
                Ok(())
            }).unwrap();
            
            black_box(batcher.len());
        });
    });
    
    group.finish();
}

/// Comprehensive performance comparison benchmark
fn bench_comprehensive_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("comprehensive_performance");
    group.sample_size(50); // Reduce sample size for comprehensive test
    
    // Simulate a complete database operation with all optimizations
    group.bench_function("optimized_operations", |b| {
        b.iter(|| {
            // Use thread-local pools for buffers
            with_vec_buffer(|key_buf| {
                key_buf.extend_from_slice(b"benchmark_key");
                
                with_large_buffer(|value_buf| {
                    value_buf.extend_from_slice(&vec![0xAA; 4096]);
                    
                    // Use SIMD for key comparison
                    let result = compare_keys(key_buf, b"other_key");
                    
                    // Use SIMD for checksum
                    let checksum = crc32(value_buf);
                    
                    black_box((result, checksum));
                });
            });
        });
    });
    
    // Compare against traditional approach
    group.bench_function("traditional_operations", |b| {
        b.iter(|| {
            // Traditional allocations
            let key_buf = b"benchmark_key".to_vec();
            let value_buf = vec![0xAA; 4096];
            
            // Traditional comparison
            let result = key_buf.cmp(b"other_key");
            
            // Traditional checksum
            use crc32fast::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(&value_buf);
            let checksum = hasher.finalize();
            
            black_box((result, checksum));
        });
    });
    
    group.finish();
}

/// Memory allocation benchmark to show reduction in allocations
fn bench_memory_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocations");
    group.throughput(Throughput::Elements(10000));
    
    // Benchmark many small allocations (traditional)
    group.bench_function("many_allocations", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for i in 0..10000 {
                let data = vec![i as u8; 64];
                results.push(data);
            }
            black_box(results);
        });
    });
    
    // Benchmark pooled allocations
    group.bench_function("pooled_allocations", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for i in 0..10000 {
                with_vec_buffer(|buf| {
                    buf.resize(64, i as u8);
                    results.push(buf.clone());
                });
            }
            black_box(results);
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_hot_path_cache,
    bench_memory_pools,
    bench_simd_operations,
    bench_async_checksum,
    bench_zero_copy_batching,
    bench_comprehensive_performance,
    bench_memory_allocations
);

criterion_main!(benches);
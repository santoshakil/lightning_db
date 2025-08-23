use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use lightning_db::features::distributed_cache::{
    DistributedCache, DistributedCacheConfig, ReplicationStrategy
};
use std::time::Duration;
use tokio::runtime::Runtime;

fn setup_cache(capacity: usize, replication: ReplicationStrategy) -> DistributedCache {
    let config = DistributedCacheConfig {
        capacity,
        replication,
        replication_factor: match replication {
            ReplicationStrategy::None => 0,
            ReplicationStrategy::Primary => 1,
            ReplicationStrategy::PrimaryBackup => 2,
            ReplicationStrategy::Chain => 3,
            ReplicationStrategy::Quorum => 3,
        },
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        DistributedCache::new(config).await.unwrap()
    })
}

fn bench_put_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_put");
    let rt = Runtime::new().unwrap();
    
    for size in [10, 100, 1000, 10000].iter() {
        let cache = setup_cache(100000, ReplicationStrategy::None);
        let value = vec![0u8; *size];
        
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let key = format!("key_{}", rand::rng().random::<u64>());
                    cache.put(
                        black_box(key.as_bytes()),
                        black_box(&value),
                        None
                    ).await.unwrap()
                });
            }
        );
    }
    group.finish();
}

fn bench_get_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_get");
    let rt = Runtime::new().unwrap();
    
    for size in [10, 100, 1000, 10000].iter() {
        let cache = setup_cache(100000, ReplicationStrategy::None);
        let value = vec![0u8; *size];
        
        rt.block_on(async {
            for i in 0..1000 {
                let key = format!("key_{}", i);
                cache.put(key.as_bytes(), &value, None).await.unwrap();
            }
        });
        
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(size),
            size,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let key = format!("key_{}", rand::rng().random::<u32>() % 1000);
                    black_box(cache.get(black_box(key.as_bytes())).await.unwrap())
                });
            }
        );
    }
    group.finish();
}

fn bench_replication_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_replication");
    let rt = Runtime::new().unwrap();
    
    let strategies = vec![
        ("None", ReplicationStrategy::None),
        ("Primary", ReplicationStrategy::Primary),
        ("PrimaryBackup", ReplicationStrategy::PrimaryBackup),
        ("Chain", ReplicationStrategy::Chain),
        ("Quorum", ReplicationStrategy::Quorum),
    ];
    
    for (name, strategy) in strategies {
        let cache = setup_cache(10000, strategy);
        let value = vec![0u8; 100];
        
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                let key = format!("key_{}", rand::rng().random::<u64>());
                cache.put(
                    black_box(key.as_bytes()),
                    black_box(&value),
                    None
                ).await.unwrap()
            });
        });
    }
    group.finish();
}

fn bench_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_concurrent");
    let rt = Runtime::new().unwrap();
    
    for threads in [1, 2, 4, 8, 16].iter() {
        let cache = setup_cache(100000, ReplicationStrategy::None);
        
        rt.block_on(async {
            for i in 0..10000 {
                let key = format!("key_{}", i);
                cache.put(key.as_bytes(), b"value", None).await.unwrap();
            }
        });
        
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            threads,
            |b, &num_threads| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = vec![];
                    
                    for _ in 0..num_threads {
                        let cache_ref = &cache;
                        let handle = tokio::spawn(async move {
                            for _ in 0..100 {
                                let key = format!("key_{}", rand::rng().random::<u32>() % 10000);
                                cache_ref.get(key.as_bytes()).await.unwrap();
                            }
                        });
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            }
        );
    }
    group.finish();
}

fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_compression");
    let rt = Runtime::new().unwrap();
    
    let config_uncompressed = DistributedCacheConfig {
        capacity: 10000,
        replication: ReplicationStrategy::None,
        replication_factor: 0,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let config_compressed = DistributedCacheConfig {
        compression_enabled: true,
        compression_threshold: 100,
        ..config_uncompressed.clone()
    };
    
    let cache_uncompressed = rt.block_on(async {
        DistributedCache::new(config_uncompressed).await.unwrap()
    });
    
    let cache_compressed = rt.block_on(async {
        DistributedCache::new(config_compressed).await.unwrap()
    });
    
    for size in [100, 1000, 10000].iter() {
        let value = vec![b'A'; *size];
        
        group.throughput(Throughput::Bytes(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("uncompressed", size),
            size,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let key = format!("key_{}", rand::rng().random::<u64>());
                    cache_uncompressed.put(
                        black_box(key.as_bytes()),
                        black_box(&value),
                        None
                    ).await.unwrap()
                });
            }
        );
        
        group.bench_with_input(
            BenchmarkId::new("compressed", size),
            size,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let key = format!("key_{}", rand::rng().random::<u64>());
                    cache_compressed.put(
                        black_box(key.as_bytes()),
                        black_box(&value),
                        None
                    ).await.unwrap()
                });
            }
        );
    }
    group.finish();
}

fn bench_eviction_policies(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_eviction");
    let rt = Runtime::new().unwrap();
    
    let cache = setup_cache(100, ReplicationStrategy::None);
    
    rt.block_on(async {
        for i in 0..100 {
            let key = format!("initial_key_{}", i);
            cache.put(key.as_bytes(), b"value", None).await.unwrap();
        }
    });
    
    group.bench_function("lru_eviction", |b| {
        b.to_async(&rt).iter(|| async {
            let key = format!("key_{}", rand::rng().random::<u64>());
            cache.put(
                black_box(key.as_bytes()),
                black_box(b"new_value"),
                None
            ).await.unwrap()
        });
    });
    
    group.finish();
}

fn bench_ttl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_ttl");
    let rt = Runtime::new().unwrap();
    
    let cache = setup_cache(10000, ReplicationStrategy::None);
    
    for ttl_ms in [100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(ttl_ms),
            ttl_ms,
            |b, &ttl| {
                b.to_async(&rt).iter(|| async {
                    let key = format!("key_{}", rand::rng().random::<u64>());
                    cache.put(
                        black_box(key.as_bytes()),
                        black_box(b"value"),
                        Some(Duration::from_millis(ttl))
                    ).await.unwrap()
                });
            }
        );
    }
    group.finish();
}

fn bench_invalidation(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_invalidation");
    let rt = Runtime::new().unwrap();
    
    let cache = setup_cache(10000, ReplicationStrategy::PrimaryBackup);
    
    rt.block_on(async {
        for i in 0..1000 {
            let key = format!("prefix_{}_key", i);
            cache.put(key.as_bytes(), b"value", None).await.unwrap();
        }
    });
    
    group.bench_function("single_invalidation", |b| {
        b.to_async(&rt).iter(|| async {
            let key = format!("prefix_{}_key", rand::rng().random::<u32>() % 1000);
            cache.invalidate(black_box(key.as_bytes())).await.unwrap()
        });
    });
    
    group.bench_function("pattern_invalidation", |b| {
        b.to_async(&rt).iter(|| async {
            cache.invalidate_pattern(black_box(b"prefix_*")).await.unwrap()
        });
    });
    
    group.finish();
}

fn bench_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("distributed_cache_hit_rate");
    let rt = Runtime::new().unwrap();
    
    let cache = setup_cache(1000, ReplicationStrategy::None);
    
    rt.block_on(async {
        for i in 0..500 {
            let key = format!("key_{}", i);
            cache.put(key.as_bytes(), b"value", None).await.unwrap();
        }
    });
    
    for hit_rate in [0.2, 0.5, 0.8, 0.95].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}%", (hit_rate * 100.0) as u32)),
            hit_rate,
            |b, &rate| {
                b.to_async(&rt).iter(|| async {
                    let key = if rand::rng().random::<f64>() < rate {
                        format!("key_{}", rand::rng().random::<u32>() % 500)
                    } else {
                        format!("missing_key_{}", rand::rng().random::<u32>())
                    };
                    black_box(cache.get(black_box(key.as_bytes())).await.unwrap())
                });
            }
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put_operations,
    bench_get_operations,
    bench_replication_strategies,
    bench_concurrent_access,
    bench_compression,
    bench_eviction_policies,
    bench_ttl_operations,
    bench_invalidation,
    bench_hit_rate
);

criterion_main!(benches);
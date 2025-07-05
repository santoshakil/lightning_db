use lightning_db::cache::{ArcCache, LockFreeCache, SegmentedLockFreeCache};
use lightning_db::storage::Page;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use parking_lot::RwLock;

fn benchmark_cache<F>(name: &str, cache_fn: F, num_threads: usize, operations: usize)
where
    F: Fn(u32) -> Option<()> + Send + Sync + 'static,
{
    let cache_fn = Arc::new(cache_fn);
    let start = Instant::now();
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let cache_fn = cache_fn.clone();
        let handle = thread::spawn(move || {
            let ops_per_thread = operations / num_threads;
            let start_idx = thread_id * ops_per_thread;
            
            for i in 0..ops_per_thread {
                let key = ((start_idx + i) % 10000) as u32;
                cache_fn(key);
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let ops_per_sec = operations as f64 / duration.as_secs_f64();
    
    println!("{}: {:.0} ops/sec ({:.2} ms total)", name, ops_per_sec, duration.as_millis());
}

fn main() {
    println!("=== Cache Performance Benchmark ===\n");
    
    let cache_size = 1000;
    let num_threads = vec![1, 2, 4, 8];
    let operations = 1_000_000;
    
    // Pre-populate test pages
    let test_pages: Vec<_> = (0..10000)
        .map(|i| (i as u32, Page::new(i as u32)))
        .collect();
    
    for &threads in &num_threads {
        println!("Testing with {} threads:", threads);
        
        // Test 1: ArcCache (current implementation)
        {
            let arc_cache = Arc::new(ArcCache::new(cache_size));
            
            // Pre-populate cache
            for i in 0..cache_size {
                let _ = arc_cache.insert(i as u32, test_pages[i].1.clone());
            }
            
            let cache = arc_cache.clone();
            benchmark_cache("  ArcCache", move |key| {
                if key % 10 < 8 {
                    // 80% reads
                    cache.get(key).map(|_| ())
                } else {
                    // 20% writes
                    let _ = cache.insert(key, test_pages[key as usize].1.clone());
                    Some(())
                }
            }, threads, operations);
        }
        
        // Test 2: LockFreeCache
        {
            let lock_free_cache = Arc::new(LockFreeCache::new(cache_size));
            
            // Pre-populate cache
            for i in 0..cache_size {
                let _ = lock_free_cache.insert(i as u32, test_pages[i].1.clone());
            }
            
            let cache = lock_free_cache.clone();
            benchmark_cache("  LockFreeCache", move |key| {
                if key % 10 < 8 {
                    // 80% reads
                    cache.get(key).map(|_| ())
                } else {
                    // 20% writes
                    let _ = cache.insert(key, test_pages[key as usize].1.clone());
                    Some(())
                }
            }, threads, operations);
        }
        
        // Test 3: SegmentedLockFreeCache
        {
            let segmented_cache = Arc::new(SegmentedLockFreeCache::new(cache_size, 16));
            
            // Pre-populate cache
            for i in 0..cache_size {
                let _ = segmented_cache.insert(i as u32, test_pages[i].1.clone());
            }
            
            let cache = segmented_cache.clone();
            benchmark_cache("  SegmentedCache", move |key| {
                if key % 10 < 8 {
                    // 80% reads
                    cache.get(key).map(|_| ())
                } else {
                    // 20% writes
                    let _ = cache.insert(key, test_pages[key as usize].1.clone());
                    Some(())
                }
            }, threads, operations);
        }
        
        println!();
    }
    
    // Test cache hit rates
    println!("Cache hit rate test (1M operations, single thread):");
    
    // Test with different access patterns
    let patterns = vec![
        ("Sequential", Box::new(|i: usize| i as u32) as Box<dyn Fn(usize) -> u32>),
        ("Random", Box::new(|i: usize| ((i * 7919) % 10000) as u32)),
        ("Zipfian", Box::new(|i: usize| {
            // Simple zipfian-like distribution
            let r = (i % 100) as f64 / 100.0;
            if r < 0.8 {
                (i % 100) as u32  // 80% of accesses to first 100 keys
            } else {
                ((i * 7919) % 10000) as u32
            }
        })),
    ];
    
    for (pattern_name, pattern_fn) in patterns {
        println!("\n{} access pattern:", pattern_name);
        
        // Test each cache type
        let caches: Vec<(&str, Box<dyn Fn(u32) -> Option<Arc<_>> + Send + Sync>)> = vec![
            ("ArcCache", {
                let cache = Arc::new(ArcCache::new(cache_size));
                for i in 0..cache_size {
                    let _ = cache.insert(i as u32, test_pages[i].1.clone());
                }
                Box::new(move |key| cache.get(key))
            }),
            ("LockFreeCache", {
                let cache = Arc::new(LockFreeCache::new(cache_size));
                for i in 0..cache_size {
                    let _ = cache.insert(i as u32, test_pages[i].1.clone());
                }
                Box::new(move |key| cache.get(key))
            }),
            ("SegmentedCache", {
                let cache = Arc::new(SegmentedLockFreeCache::new(cache_size, 16));
                for i in 0..cache_size {
                    let _ = cache.insert(i as u32, test_pages[i].1.clone());
                }
                Box::new(move |key| cache.get(key))
            }),
        ];
        
        for (cache_name, cache_fn) in caches {
            let mut hits = 0;
            let total = 100_000;
            
            for i in 0..total {
                let key = pattern_fn(i);
                if cache_fn(key).is_some() {
                    hits += 1;
                }
            }
            
            let hit_rate = (hits as f64 / total as f64) * 100.0;
            println!("  {}: {:.1}% hit rate", cache_name, hit_rate);
        }
    }
}
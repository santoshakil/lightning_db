use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("stress_test.db");
    
    println!("🚀 Lightning DB Stress Test");
    println!("============================");
    println!("Database path: {:?}\n", db_path);
    
    let config = LightningDbConfig {
        page_size: 8192,
        cache_size: 50 * 1024 * 1024,
        compression_enabled: true,
        enable_statistics: true,
        max_active_transactions: 100,
        prefetch_enabled: true,
        prefetch_distance: 10,
        prefetch_workers: 4,
        use_optimized_transactions: true,
        use_optimized_page_manager: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::open(&db_path, config)?);
    
    println!("📝 Sequential Write Test");
    println!("------------------------");
    let start = Instant::now();
    let num_records = 10000;
    
    for i in 0..num_records {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}_data", i);
        db.put(key.as_bytes(), value.as_bytes())?;
        
        if i % 1000 == 0 && i > 0 {
            println!("  Written {} records", i);
        }
    }
    
    let write_duration = start.elapsed();
    let writes_per_sec = num_records as f64 / write_duration.as_secs_f64();
    println!("  ✅ Wrote {} records in {:.2?}", num_records, write_duration);
    println!("  Speed: {:.0} writes/sec\n", writes_per_sec);
    
    println!("📖 Sequential Read Test");
    println!("----------------------");
    let start = Instant::now();
    let mut successful_reads = 0;
    
    for i in 0..num_records {
        let key = format!("key_{:08}", i);
        let expected_value = format!("value_{:08}_data", i);
        
        let value = db.get(key.as_bytes())?;
        if let Some(v) = value {
            assert_eq!(v, expected_value.as_bytes());
            successful_reads += 1;
        }
        
        if i % 1000 == 0 && i > 0 {
            println!("  Read {} records", i);
        }
    }
    
    let read_duration = start.elapsed();
    let reads_per_sec = successful_reads as f64 / read_duration.as_secs_f64();
    println!("  ✅ Read {} records in {:.2?}", successful_reads, read_duration);
    println!("  Speed: {:.0} reads/sec\n", reads_per_sec);
    
    println!("🔀 Random Access Test");
    println!("--------------------");
    let start = Instant::now();
    let random_accesses = 5000;
    
    for i in 0..random_accesses {
        let key_num = (i * 7919) % num_records;
        let key = format!("key_{:08}", key_num);
        let _ = db.get(key.as_bytes())?;
    }
    
    let random_duration = start.elapsed();
    let random_per_sec = random_accesses as f64 / random_duration.as_secs_f64();
    println!("  ✅ {} random accesses in {:.2?}", random_accesses, random_duration);
    println!("  Speed: {:.0} accesses/sec\n", random_per_sec);
    
    println!("🔄 Concurrent Operations Test");
    println!("----------------------------");
    let start = Instant::now();
    let num_tasks = 10;
    let ops_per_task = 500;
    
    let mut handles = vec![];
    for task_id in 0..num_tasks {
        let db_clone = db.clone();
        let handle = task::spawn(async move {
            let mut local_success = 0;
            for i in 0..ops_per_task {
                let key = format!("task_{}_key_{:04}", task_id, i);
                let value = format!("task_{}_value_{:04}", task_id, i);
                
                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    local_success += 1;
                }
            }
            local_success
        });
        handles.push(handle);
    }
    
    let mut total_success = 0;
    for handle in handles {
        total_success += handle.await?;
    }
    
    let concurrent_duration = start.elapsed();
    let concurrent_per_sec = total_success as f64 / concurrent_duration.as_secs_f64();
    println!("  ✅ {} concurrent operations in {:.2?}", total_success, concurrent_duration);
    println!("  Speed: {:.0} ops/sec\n", concurrent_per_sec);
    
    println!("🔍 Range Query Test");
    println!("------------------");
    let start = Instant::now();
    let mut range_count = 0;
    
    for (_, _) in db.range(Some(b"key_00000000"), Some(b"key_00001000"))? {
        range_count += 1;
    }
    
    let range_duration = start.elapsed();
    println!("  ✅ Found {} keys in range in {:.2?}\n", range_count, range_duration);
    
    println!("🗑️  Delete Test");
    println!("--------------");
    let start = Instant::now();
    let delete_count = 1000;
    
    for i in 0..delete_count {
        let key = format!("key_{:08}", i);
        db.delete(key.as_bytes())?;
    }
    
    let delete_duration = start.elapsed();
    let deletes_per_sec = delete_count as f64 / delete_duration.as_secs_f64();
    println!("  ✅ Deleted {} records in {:.2?}", delete_count, delete_duration);
    println!("  Speed: {:.0} deletes/sec\n", deletes_per_sec);
    
    println!("📊 Final Statistics");
    println!("-----------------");
    let stats = db.stats();
    println!("  Page count: {}", stats.page_count);
    println!("  Free pages: {}", stats.free_page_count);
    println!("  Tree height: {}", stats.tree_height);
    println!("  Active transactions: {}", stats.active_transactions);
    println!("  Memory usage: {:.2} MB", stats.memory_usage_bytes as f64 / 1024.0 / 1024.0);
    println!("  Disk usage: {:.2} MB", stats.disk_usage_bytes as f64 / 1024.0 / 1024.0);
    
    if let Some(hit_rate) = stats.cache_hit_rate {
        println!("  Cache hit rate: {:.1}%", hit_rate * 100.0);
    }
    
    println!("\n✅ All stress tests completed successfully!");
    
    Ok(())
}
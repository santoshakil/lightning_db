/// Debug threading issue
use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 DEBUG THREADING TEST");
    println!("========================\n");

    println!("Creating database...");
    let temp_dir = TempDir::new()?;
    let mut config = LightningDbConfig::default();
    config.cache_size = 10 * 1024 * 1024; // 10MB
    let db = Arc::new(Database::create(temp_dir.path(), config)?);

    println!("Starting single thread test...");
    let start = Instant::now();

    // Single thread test
    let db_clone = db.clone();
    let handle = thread::spawn(move || {
        println!("Thread started!");
        for i in 0..10 {
            let key = format!("thread1:key:{}", i);
            let value = format!("value:{}", i);
            if let Err(e) = db_clone.put(key.as_bytes(), value.as_bytes()) {
                println!("Error writing: {:?}", e);
            }
        }
        println!("Thread finished!");
    });

    println!("Waiting for thread...");
    handle.join().unwrap();
    println!("Thread joined in {:.2}ms", start.elapsed().as_millis());

    // Multi-thread test
    println!("\nStarting multi-thread test...");
    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..2 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            println!("Thread {} started!", thread_id);
            for i in 0..10 {
                let key = format!("thread{}:key:{}", thread_id, i);
                let value = format!("value:{}", i);
                if let Err(e) = db_clone.put(key.as_bytes(), value.as_bytes()) {
                    println!("Thread {} error writing: {:?}", thread_id, e);
                }
            }
            println!("Thread {} finished!", thread_id);
        });
        handles.push(handle);
    }

    println!("Waiting for {} threads...", handles.len());
    for (i, handle) in handles.into_iter().enumerate() {
        println!("Joining thread {}...", i);
        handle.join().unwrap();
        println!("Thread {} joined", i);
    }
    println!("All threads joined in {:.2}ms", start.elapsed().as_millis());

    println!("\n✅ Test completed successfully!");

    Ok(())
}

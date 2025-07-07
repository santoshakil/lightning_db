/// Test just the cache pattern
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing cache pattern...");

    let temp_dir = TempDir::new()?;
    let config = LightningDbConfig {
        cache_size: 10 * 1024 * 1024,
        ..Default::default()
    };

    println!("Creating database...");
    let db = Database::create(temp_dir.path(), config)?;

    println!("Pre-populating data...");
    for i in 0..10 {
        let key = format!("cache:key:{}", i);
        let value = format!("value:{}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    println!("Starting operations...");
    let start = Instant::now();
    let mut hits = 0;
    let mut misses = 0;

    for i in 0..100 {
        let key = format!("cache:key:{}", i % 20);
        match db.get(key.as_bytes())? {
            Some(_) => hits += 1,
            None => misses += 1,
        }
    }

    let duration = start.elapsed();
    println!("Duration: {:.2}ms", duration.as_millis());
    println!("Hits: {}, Misses: {}", hits, misses);
    println!("Done!");

    Ok(())
}

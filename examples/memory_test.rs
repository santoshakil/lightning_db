use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Memory Management and Caching Test");

    // Create database
    let db_path = "data/memory_test_db";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }

    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB
        compression_enabled: true,
        compression_type: 1, // Zstd
        ..Default::default()
    };

    let db = Database::create(&db_path, config)?;

    // 1. Initial Cache State
    println!("\n1. Initial Cache Statistics:");
    display_cache_stats(&db)?;

    // 2. Sequential Access (Cache-Friendly)
    println!("\n2. Sequential Access Pattern:");
    let start = Instant::now();

    for i in 0..100 {
        let key = format!("cache_test_{:03}", i);
        let value = format!("Cache test value {}", i);
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Read same keys sequentially
    for i in 0..100 {
        let key = format!("cache_test_{:03}", i);
        let _ = db.get(key.as_bytes())?;
    }

    let duration = start.elapsed();
    println!(
        "  Sequential access completed in {:.2}ms",
        duration.as_secs_f64() * 1000.0
    );

    println!("\n3. Cache Statistics After Sequential Access:");
    display_cache_stats(&db)?;

    // 3. Random Access (Cache Test)
    println!("\n4. Random Access Pattern:");
    use rand::{rngs::StdRng, Rng, SeedableRng};
    let mut rng = StdRng::seed_from_u64(42);
    let start = Instant::now();
    let mut cache_hits = 0;

    for _ in 0..1000 {
        let key_num = rng.random_range(0..1000);
        let key = format!("cache_test_{:03}", key_num);
        if db.get(key.as_bytes())?.is_some() {
            cache_hits += 1;
        }
    }

    let duration = start.elapsed();
    println!(
        "  1000 random reads completed in {:.2}ms",
        duration.as_secs_f64() * 1000.0
    );
    println!("  Hit rate: {:.1}%", (cache_hits as f64 / 1000.0) * 100.0);

    println!("\n5. Cache Statistics After Random Access:");
    display_cache_stats(&db)?;

    Ok(())
}

fn display_cache_stats(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(cache_stats) = db.cache_stats() {
        println!("  {}", cache_stats);
    } else {
        println!("  No cache statistics available");
    }
    Ok(())
}

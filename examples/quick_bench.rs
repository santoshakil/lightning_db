use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Quick Performance Test\n");

    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Test with fully optimized config
    let config = LightningDbConfig::default();
    let db = Database::create(&db_path, config)?;

    // Test parameters (reduced for quick test)
    let write_count = 10_000;
    let read_count = 100_000;
    let value_size = 100;

    let test_value = vec![b'x'; value_size];

    // Warmup
    for i in 0..100 {
        let key = format!("warmup_{:08}", i);
        db.put(key.as_bytes(), &test_value)?;
    }

    // Write benchmark
    println!("Testing writes...");
    let start = Instant::now();
    for i in 0..write_count {
        let key = format!("key_{:08}", i);
        db.put(key.as_bytes(), &test_value)?;
    }
    let write_duration = start.elapsed();
    let write_ops_per_sec = write_count as f64 / write_duration.as_secs_f64();
    let write_us_per_op = write_duration.as_micros() as f64 / write_count as f64;

    println!("Write Performance:");
    println!("  • {:.0} ops/sec", write_ops_per_sec);
    println!("  • {:.2} μs/op", write_us_per_op);
    println!("  • Target: 100K+ ops/sec, <10μs/op");
    println!(
        "  • Status: {}",
        if write_ops_per_sec >= 100_000.0 {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );

    // Read benchmark (cache hit)
    println!("\nTesting reads (cached)...");
    let test_key = b"cached_key";
    db.put(test_key, &test_value)?;
    let _ = db.get(test_key)?; // Prime cache

    let start = Instant::now();
    for _ in 0..read_count {
        let _ = db.get(test_key)?;
    }
    let read_duration = start.elapsed();
    let read_ops_per_sec = read_count as f64 / read_duration.as_secs_f64();
    let read_us_per_op = read_duration.as_micros() as f64 / read_count as f64;

    println!("Read Performance (cached):");
    println!("  • {:.0} ops/sec", read_ops_per_sec);
    println!("  • {:.2} μs/op", read_us_per_op);
    println!("  • Target: 1M+ ops/sec, <1μs/op");
    println!(
        "  • Status: {}",
        if read_ops_per_sec >= 1_000_000.0 {
            "✅ PASS"
        } else {
            "❌ FAIL"
        }
    );

    // Print summary
    println!("\n📊 Summary:");
    if write_ops_per_sec >= 100_000.0 && read_ops_per_sec >= 1_000_000.0 {
        println!("🎉 All performance targets met!");
    } else {
        println!("🔧 Need optimization:");
        if write_ops_per_sec < 100_000.0 {
            let needed_improvement = 100_000.0 / write_ops_per_sec;
            println!("  • Writes need {:.1}x improvement", needed_improvement);
        }
        if read_ops_per_sec < 1_000_000.0 {
            let needed_improvement = 1_000_000.0 / read_ops_per_sec;
            println!("  • Reads need {:.1}x improvement", needed_improvement);
        }
    }

    Ok(())
}

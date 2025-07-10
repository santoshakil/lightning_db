use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Debugging WAL Performance\n");

    let count = 100;

    // Test 1: No WAL
    {
        println!("Test 1: No WAL");
        let temp_dir = tempfile::tempdir()?;

        let mut config = LightningDbConfig {
            use_optimized_transactions: false,
            ..Default::default()
        };
        config.use_improved_wal = false;
        config.compression_enabled = false;
        config.cache_size = 0;
        config.prefetch_enabled = false;

        let db = Database::create(temp_dir.path(), config)?;

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        let elapsed = start.elapsed();
        println!("  • {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());
    }

    // Test 2: Improved WAL without group commit
    {
        println!("\nTest 2: Improved WAL (no group commit - manual test)");
        // This would require modifying the WAL creation code
        println!("  • Skipped - would need code modification");
    }

    // Test 3: Standard WAL
    {
        println!("\nTest 3: Standard WAL (old implementation)");
        let temp_dir = tempfile::tempdir()?;

        let mut config = LightningDbConfig {
            use_optimized_transactions: false,
            ..Default::default()
        };
        config.use_improved_wal = false; // Use old WAL
        config.compression_enabled = false;
        config.cache_size = 0;
        config.prefetch_enabled = false;

        let db = Database::create(temp_dir.path(), config)?;

        let start = Instant::now();
        for i in 0..count {
            let key = format!("key_{:08}", i);
            db.put(key.as_bytes(), b"value")?;
        }
        let elapsed = start.elapsed();
        println!("  • {:.0} ops/sec", count as f64 / elapsed.as_secs_f64());
    }

    Ok(())
}

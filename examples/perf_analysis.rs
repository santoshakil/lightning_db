use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Lightning DB Performance Analysis\n");

    let dir = tempdir()?;
    let test_count = 1000;
    let value = vec![0u8; 100];

    // Test different configurations
    let configs = vec![
        ("Sync WAL", WalSyncMode::Sync, false),
        ("Async WAL", WalSyncMode::Async, false),
        (
            "Periodic WAL (10ms)",
            WalSyncMode::Periodic { interval_ms: 10 },
            false,
        ),
        ("Async + No compression", WalSyncMode::Async, true),
    ];

    println!("Testing individual puts ({} operations):\n", test_count);
    println!(
        "{:<25} {:>15} {:>15} {:>15}",
        "Configuration", "Writes/sec", "μs/op", "Status"
    );
    println!("{}", "-".repeat(75));

    for (name, sync_mode, disable_compression) in configs {
        let db_path = dir.path().join(format!("{}.db", name.replace(" ", "_")));
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = sync_mode;
        if disable_compression {
            config.compression_enabled = false;
        }

        let db = Database::create(&db_path, config)?;

        // Warmup
        for i in 0..10 {
            db.put(format!("warmup{}", i).as_bytes(), &value)?;
        }

        // Test
        let start = Instant::now();
        for i in 0..test_count {
            db.put(format!("key{:06}", i).as_bytes(), &value)?;
        }
        let duration = start.elapsed();

        let ops_sec = test_count as f64 / duration.as_secs_f64();
        let us_per_op = duration.as_micros() as f64 / test_count as f64;
        let status = if ops_sec >= 100_000.0 {
            "✅"
        } else if ops_sec >= 50_000.0 {
            "⚠️"
        } else {
            "❌"
        };

        println!(
            "{:<25} {:>15.0} {:>15.2} {:>15}",
            name, ops_sec, us_per_op, status
        );
    }

    // Test AutoBatcher
    println!("\n\nTesting AutoBatcher ({} operations):\n", test_count);
    println!(
        "{:<25} {:>15} {:>15} {:>15}",
        "Configuration", "Writes/sec", "μs/op", "Status"
    );
    println!("{}", "-".repeat(75));

    let db_path = dir.path().join("auto_batcher.db");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;
    let db = Arc::new(Database::create(&db_path, config)?);
    let batcher = Database::create_auto_batcher(db);

    let start = Instant::now();
    for i in 0..test_count {
        batcher.put(format!("key{:06}", i).into_bytes(), value.clone())?;
    }
    batcher.flush()?;
    let duration = start.elapsed();

    let ops_sec = test_count as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / test_count as f64;
    let status = if ops_sec >= 100_000.0 {
        "✅"
    } else if ops_sec >= 50_000.0 {
        "⚠️"
    } else {
        "❌"
    };

    println!(
        "{:<25} {:>15.0} {:>15.2} {:>15}",
        "AutoBatcher", ops_sec, us_per_op, status
    );

    // Read performance test
    println!("\n\nTesting read performance (100K reads):\n");

    let db_path = dir.path().join("read_test.db");
    let mut config = LightningDbConfig::default();
    config.wal_sync_mode = WalSyncMode::Async;
    config.compression_enabled = false; // Disable compression for direct B+Tree access
    let db = Database::create(&db_path, config)?;

    // Insert test key
    let test_key = b"read_test_key";
    db.put(test_key, &value)?;

    // Warmup cache
    for _ in 0..100 {
        let _ = db.get(test_key)?;
    }

    // Test cached reads
    let read_count = 100_000;
    let start = Instant::now();
    for _ in 0..read_count {
        let _ = db.get(test_key)?;
    }
    let duration = start.elapsed();

    let ops_sec = read_count as f64 / duration.as_secs_f64();
    let us_per_op = duration.as_micros() as f64 / read_count as f64;
    let status = if ops_sec >= 1_000_000.0 {
        "✅"
    } else if ops_sec >= 500_000.0 {
        "⚠️"
    } else {
        "❌"
    };

    println!(
        "{:<25} {:>15} {:>15} {:>15}",
        "Configuration", "Reads/sec", "μs/op", "Status"
    );
    println!("{}", "-".repeat(75));
    println!(
        "{:<25} {:>15.0} {:>15.2} {:>15}",
        "Cached reads", ops_sec, us_per_op, status
    );

    // Summary
    println!("\n📊 Performance Summary:");
    println!("  • Target: 100K+ writes/sec, 1M+ reads/sec");
    println!("  • Async WAL mode recommended for best performance");
    println!("  • AutoBatcher provides best write throughput");
    println!("  • Sync mode is slow due to fsync on every operation");

    Ok(())
}

use lightning_db::{Database, LightningDbConfig};
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};
use tempfile::TempDir;

/// Memory leak debugging test
fn main() {
    println!("🔍 Lightning DB Memory Leak Debug Test");
    println!("PID: {}", process::id());
    println!("{}", "=".repeat(60));

    let test_dir = TempDir::new().unwrap();
    let db_path = test_dir.path().join("leak_test_db");

    // Create database with minimal cache to isolate leak
    let config = LightningDbConfig {
        cache_size: 1024 * 1024, // 1MB cache only
        compression_enabled: true,
        use_improved_wal: true,
        wal_sync_mode: lightning_db::WalSyncMode::Periodic { interval_ms: 1000 },
        ..Default::default()
    };

    let db = Arc::new(Database::create(&db_path, config).unwrap());

    // Capture initial metrics
    let initial_memory = get_memory_usage();
    println!(
        "📊 Initial Memory: {:.2} MB",
        initial_memory as f64 / 1024.0 / 1024.0
    );

    // Phase 1: Baseline (no operations)
    println!("\n🎯 Phase 1: Baseline (10 seconds, no operations)");
    thread::sleep(Duration::from_secs(10));
    let baseline_memory = get_memory_usage();
    println!(
        "   Memory after baseline: {:.2} MB (+{:.2} MB)",
        baseline_memory as f64 / 1024.0 / 1024.0,
        (baseline_memory as i64 - initial_memory as i64) as f64 / 1024.0 / 1024.0
    );

    // Phase 2: Write operations only
    println!("\n🎯 Phase 2: Write operations (10 seconds)");
    let write_start = Instant::now();
    let mut write_count = 0;
    while write_start.elapsed() < Duration::from_secs(10) {
        let key = format!("key_{:08}", write_count);
        let value = format!("value_{:08}", write_count);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
        write_count += 1;
    }
    let write_memory = get_memory_usage();
    println!("   Writes: {}", write_count);
    println!(
        "   Memory after writes: {:.2} MB (+{:.2} MB from baseline)",
        write_memory as f64 / 1024.0 / 1024.0,
        (write_memory as i64 - baseline_memory as i64) as f64 / 1024.0 / 1024.0
    );

    // Phase 3: Read operations only
    println!("\n🎯 Phase 3: Read operations (10 seconds)");
    let read_start = Instant::now();
    let mut read_count = 0;
    while read_start.elapsed() < Duration::from_secs(10) {
        let key = format!("key_{:08}", read_count % write_count);
        let _ = db.get(key.as_bytes());
        read_count += 1;
    }
    let read_memory = get_memory_usage();
    println!("   Reads: {}", read_count);
    println!(
        "   Memory after reads: {:.2} MB (+{:.2} MB from writes)",
        read_memory as f64 / 1024.0 / 1024.0,
        (read_memory as i64 - write_memory as i64) as f64 / 1024.0 / 1024.0
    );

    // Phase 4: Mixed operations
    println!("\n🎯 Phase 4: Mixed operations (10 seconds)");
    let mixed_start = Instant::now();
    let mut mixed_ops = 0;
    while mixed_start.elapsed() < Duration::from_secs(10) {
        if mixed_ops % 3 == 0 {
            let key = format!("mixed_key_{:08}", mixed_ops);
            let value = format!("mixed_value_{:08}", mixed_ops);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else {
            let key = format!("key_{:08}", mixed_ops % write_count);
            let _ = db.get(key.as_bytes());
        }
        mixed_ops += 1;
    }
    let mixed_memory = get_memory_usage();
    println!("   Mixed ops: {}", mixed_ops);
    println!(
        "   Memory after mixed: {:.2} MB (+{:.2} MB from reads)",
        mixed_memory as f64 / 1024.0 / 1024.0,
        (mixed_memory as i64 - read_memory as i64) as f64 / 1024.0 / 1024.0
    );

    // Summary
    println!("\n{}", "=".repeat(60));
    println!("📊 MEMORY LEAK ANALYSIS");
    println!("{}", "=".repeat(60));

    let total_growth = mixed_memory as i64 - initial_memory as i64;
    let growth_per_op = total_growth as f64 / (write_count + read_count + mixed_ops) as f64;

    println!("Total Operations: {}", write_count + read_count + mixed_ops);
    println!(
        "Total Memory Growth: {:.2} MB",
        total_growth as f64 / 1024.0 / 1024.0
    );
    println!("Growth per Operation: {:.2} bytes", growth_per_op);

    println!("\nBreakdown:");
    println!(
        "   Baseline growth: {:.2} MB",
        (baseline_memory as i64 - initial_memory as i64) as f64 / 1024.0 / 1024.0
    );
    println!(
        "   Write phase growth: {:.2} MB",
        (write_memory as i64 - baseline_memory as i64) as f64 / 1024.0 / 1024.0
    );
    println!(
        "   Read phase growth: {:.2} MB",
        (read_memory as i64 - write_memory as i64) as f64 / 1024.0 / 1024.0
    );
    println!(
        "   Mixed phase growth: {:.2} MB",
        (mixed_memory as i64 - read_memory as i64) as f64 / 1024.0 / 1024.0
    );

    // Keep process alive for external analysis
    println!("\n⏸️  Process will stay alive for 30 seconds for analysis...");
    println!("   Run: leaks {} to analyze", process::id());
    println!("   Or: vmmap {} for detailed memory map", process::id());

    thread::sleep(Duration::from_secs(30));

    println!("\n✅ Test complete");
}

fn get_memory_usage() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(process::id());
    system.refresh_all();

    system.process(pid).map(|p| p.memory()).unwrap_or(0)
}

//! Lightning DB Admin CLI Tool
//!
//! Production-grade operational tooling for database administration,
//! monitoring, and maintenance.

use clap::{Parser, Subcommand};
use lightning_db::{Database, LightningDbConfig};
#[cfg(feature = "cli-tools")]
use prettytable::format;
#[cfg(feature = "cli-tools")]
use prettytable::{row, Table};
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "lightning-admin")]
#[command(about = "Lightning DB administrative tools", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify database integrity
    Verify {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Enable verbose output
        #[arg(short, long)]
        verbose: bool,
    },

    /// Show database statistics
    Stats {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Output format (table, json, csv)
        #[arg(short, long, default_value = "table")]
        format: String,
    },

    /// Backup database
    Backup {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Backup destination
        #[arg(short, long)]
        output: PathBuf,

        /// Compression level (0-9)
        #[arg(short, long, default_value = "6")]
        compression: u8,
    },

    /// Restore database from backup
    Restore {
        /// Backup file path
        #[arg(short, long)]
        backup: PathBuf,

        /// Restore destination
        #[arg(short, long)]
        output: PathBuf,

        /// Force overwrite existing database
        #[arg(short, long)]
        force: bool,
    },

    /// Compact database to reclaim space
    Compact {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Target space reduction percentage
        #[arg(short, long, default_value = "20")]
        target: u8,
    },

    /// Monitor database performance
    Monitor {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Update interval in seconds
        #[arg(short, long, default_value = "1")]
        interval: u64,

        /// Enable detailed metrics
        #[arg(short, long)]
        detailed: bool,
    },

    /// Analyze database for optimization opportunities
    Analyze {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Generate optimization report
        #[arg(short, long)]
        report: Option<PathBuf>,
    },

    /// Repair corrupted database
    Repair {
        /// Path to database
        #[arg(short, long)]
        path: PathBuf,

        /// Backup before repair
        #[arg(short, long)]
        backup: Option<PathBuf>,

        /// Aggressive repair mode
        #[arg(short, long)]
        aggressive: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Verify { path, verbose } => verify_database(path, verbose),
        Commands::Stats { path, format } => show_stats(path, &format),
        Commands::Backup {
            path,
            output,
            compression,
        } => backup_database(path, output, compression),
        Commands::Restore {
            backup,
            output,
            force,
        } => restore_database(backup, output, force),
        Commands::Compact { path, target } => compact_database(path, target),
        Commands::Monitor {
            path,
            interval,
            detailed,
        } => monitor_database(path, interval, detailed),
        Commands::Analyze { path, report } => analyze_database(path, report),
        Commands::Repair {
            path,
            backup,
            aggressive,
        } => repair_database(path, backup, aggressive),
    }
}

fn verify_database(path: PathBuf, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Verifying database integrity...\n");

    let start = Instant::now();
    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;

    // Check basic stats
    let stats = db.stats();
    println!("📊 Database Statistics:");
    println!("   Pages: {}", stats.page_count);
    println!("   Free pages: {}", stats.free_page_count);
    println!("   B+Tree height: {}", stats.tree_height);

    // Verify data integrity
    println!("\n🔎 Checking data integrity...");
    let mut _checked = 0;
    let mut errors = 0;

    // In a real implementation, we would iterate through all keys
    // For now, we'll simulate the check
    let check_result = db.run_integrity_check()?;

    if check_result.is_valid {
        println!("✅ Data integrity check passed");
    } else {
        println!("❌ Data integrity issues found:");
        for error in &check_result.errors {
            println!("   - {}", error);
            errors += 1;
        }
    }

    if verbose {
        println!("\n📝 Detailed verification:");
        println!("   Page checksums verified: {}", check_result.pages_checked);
        println!(
            "   Index consistency: {}",
            if check_result.index_consistent {
                "OK"
            } else {
                "FAILED"
            }
        );
        println!(
            "   Transaction log: {}",
            if check_result.wal_consistent {
                "OK"
            } else {
                "FAILED"
            }
        );
    }

    let duration = start.elapsed();
    println!(
        "\n⏱️  Verification completed in {:.2}s",
        duration.as_secs_f64()
    );

    if errors > 0 {
        Err(format!("Found {} integrity errors", errors).into())
    } else {
        Ok(())
    }
}

fn show_stats(path: PathBuf, format: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;
    let stats = db.stats();
    let metrics = db.metrics()?;

    match format {
        "json" => {
            println!("{{");
            println!("  \"page_count\": {},", stats.page_count);
            println!("  \"free_pages\": {},", stats.free_page_count);
            println!("  \"btree_height\": {},", stats.tree_height);
            println!("  \"active_transactions\": {},", stats.active_transactions);
            println!("  \"cache_size\": {},", metrics.cache_size);
            println!("  \"cache_hits\": {},", metrics.cache_hits);
            println!("  \"cache_misses\": {},", metrics.cache_misses);
            println!("  \"total_reads\": {},", metrics.total_reads);
            println!("  \"total_writes\": {},", metrics.total_writes);
            println!("  \"wal_size\": {}", metrics.wal_size);
            println!("}}");
        }
        "csv" => {
            println!("metric,value");
            println!("page_count,{}", stats.page_count);
            println!("free_pages,{}", stats.free_page_count);
            println!("btree_height,{}", stats.tree_height);
            println!("active_transactions,{}", stats.active_transactions);
            println!("cache_size,{}", metrics.cache_size);
            println!("cache_hits,{}", metrics.cache_hits);
            println!("cache_misses,{}", metrics.cache_misses);
            println!("total_reads,{}", metrics.total_reads);
            println!("total_writes,{}", metrics.total_writes);
            println!("wal_size,{}", metrics.wal_size);
        }
        _ => {
            #[cfg(feature = "cli-tools")]
            {
                let mut table = Table::new();
                table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                table.set_titles(row!["Metric", "Value"]);

                table.add_row(row!["Page Count", stats.page_count]);
                table.add_row(row!["Free Pages", stats.free_page_count]);
                table.add_row(row!["B+Tree Height", stats.tree_height]);
                table.add_row(row!["Active Transactions", stats.active_transactions]);
                table.add_row(row!["Cache Size", format_bytes(metrics.cache_size)]);
                table.add_row(row!["Cache Hits", metrics.cache_hits]);
                table.add_row(row!["Cache Misses", metrics.cache_misses]);
                table.add_row(row![
                    "Cache Hit Rate",
                    format!(
                        "{:.1}%",
                        metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64
                            * 100.0
                    )
                ]);
                table.add_row(row!["Total Reads", metrics.total_reads]);
                table.add_row(row!["Total Writes", metrics.total_writes]);
                table.add_row(row!["WAL Size", format_bytes(metrics.wal_size)]);

                table.printstd();
            }
            
            #[cfg(not(feature = "cli-tools"))]
            {
                // Fallback to simple text output when prettytable is not available
                println!("📊 Database Statistics:");
                println!("   Page Count: {}", stats.page_count);
                println!("   Free Pages: {}", stats.free_page_count);
                println!("   B+Tree Height: {}", stats.tree_height);
                println!("   Active Transactions: {}", stats.active_transactions);
                println!("   Cache Size: {}", format_bytes(metrics.cache_size));
                println!("   Cache Hits: {}", metrics.cache_hits);
                println!("   Cache Misses: {}", metrics.cache_misses);
                println!(
                    "   Cache Hit Rate: {:.1}%",
                    metrics.cache_hits as f64 / (metrics.cache_hits + metrics.cache_misses) as f64
                        * 100.0
                );
                println!("   Total Reads: {}", metrics.total_reads);
                println!("   Total Writes: {}", metrics.total_writes);
                println!("   WAL Size: {}", format_bytes(metrics.wal_size));
            }
        }
    }

    Ok(())
}

fn backup_database(
    path: PathBuf,
    output: PathBuf,
    _compression: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("💾 Creating database backup...\n");

    let start = Instant::now();

    // Ensure output directory exists
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;

    // Create backup
    let backup_stats = db.backup_to(&output, _compression)?;

    let duration = start.elapsed();

    println!("✅ Backup completed successfully!");
    println!("\n📊 Backup Statistics:");
    println!(
        "   Original size: {}",
        format_bytes(backup_stats.original_size)
    );
    println!("   Backup size: {}", format_bytes(backup_stats.backup_size));
    println!(
        "   Compression ratio: {:.1}%",
        (1.0 - backup_stats.backup_size as f64 / backup_stats.original_size as f64) * 100.0
    );
    println!("   Duration: {:.2}s", duration.as_secs_f64());
    println!(
        "   Throughput: {}/s",
        format_bytes((backup_stats.original_size as f64 / duration.as_secs_f64()) as u64)
    );

    Ok(())
}

fn restore_database(
    _backup: PathBuf,
    output: PathBuf,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📥 Restoring database from backup...\n");

    // Check if output exists
    if output.exists() && !force {
        return Err("Output path already exists. Use --force to overwrite.".into());
    }

    let start = Instant::now();

    // Ensure output directory exists
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    // Restore from backup
    let restore_stats = Database::restore_from(&_backup, &output)?;

    let duration = start.elapsed();

    println!("✅ Restore completed successfully!");
    println!("\n📊 Restore Statistics:");
    println!(
        "   Restored size: {}",
        format_bytes(restore_stats.restored_size)
    );
    println!("   Duration: {:.2}s", duration.as_secs_f64());
    println!(
        "   Throughput: {}/s",
        format_bytes((restore_stats.restored_size as f64 / duration.as_secs_f64()) as u64)
    );

    // Verify restored database
    println!("\n🔍 Verifying restored database...");
    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&output, config)?;
    let stats = db.stats();

    println!("   Pages: {}", stats.page_count);
    println!("   B+Tree height: {}", stats.tree_height);
    println!("✅ Database restored and verified");

    Ok(())
}

fn compact_database(path: PathBuf, _target: u8) -> Result<(), Box<dyn std::error::Error>> {
    println!("🗜️  Compacting database...\n");

    let start = Instant::now();

    // Open database
    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;

    // Get initial stats
    let initial_stats = db.stats();
    let initial_size = get_directory_size(&path)?;

    println!("📊 Initial state:");
    println!("   Size: {}", format_bytes(initial_size));
    println!(
        "   Pages: {} ({} free)",
        initial_stats.page_count, initial_stats.free_page_count
    );

    // Run compaction
    println!(
        "\n🔄 Running compaction (target: {}% reduction)...",
        _target
    );
    let compact_stats = db.compact(_target)?;

    // Get final stats
    let final_stats = db.stats();
    let final_size = get_directory_size(&path)?;

    let duration = start.elapsed();
    let reduction = (1.0 - final_size as f64 / initial_size as f64) * 100.0;

    println!("\n✅ Compaction completed!");
    println!("\n📊 Results:");
    println!(
        "   Size: {} → {} ({:.1}% reduction)",
        format_bytes(initial_size),
        format_bytes(final_size),
        reduction
    );
    println!(
        "   Pages: {} → {} ({} reclaimed)",
        initial_stats.page_count, final_stats.page_count, compact_stats.pages_reclaimed
    );
    println!("   Duration: {:.2}s", duration.as_secs_f64());

    if reduction < _target as f64 {
        println!(
            "\n⚠️  Warning: Target reduction not achieved (got {:.1}% vs target {}%)",
            reduction, _target
        );
    }

    Ok(())
}

fn monitor_database(
    path: PathBuf,
    interval: u64,
    detailed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Monitoring database (press Ctrl+C to stop)...\n");

    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;
    let mut prev_metrics = db.metrics()?;

    loop {
        thread::sleep(Duration::from_secs(interval));

        // Clear screen (platform-specific)
        print!("\x1B[2J\x1B[1;1H");

        let metrics = db.metrics()?;
        let stats = db.stats();

        // Calculate rates
        let read_rate = (metrics.total_reads - prev_metrics.total_reads) / interval;
        let write_rate = (metrics.total_writes - prev_metrics.total_writes) / interval;
        let cache_hit_rate = if metrics.cache_hits > prev_metrics.cache_hits {
            let hits = metrics.cache_hits - prev_metrics.cache_hits;
            let misses = metrics.cache_misses - prev_metrics.cache_misses;
            hits as f64 / (hits + misses) as f64 * 100.0
        } else {
            0.0
        };

        println!(
            "🚀 Lightning DB Monitor - {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        println!("{}", "=".repeat(60));

        println!("\n📈 Operations/sec:");
        println!("   Reads:  {}/s", read_rate);
        println!("   Writes: {}/s", write_rate);

        println!("\n💾 Cache Performance:");
        println!("   Size: {}", format_bytes(metrics.cache_size));
        println!("   Hit rate: {:.1}%", cache_hit_rate);

        println!("\n🗄️  Storage:");
        println!(
            "   Pages: {} ({} free)",
            stats.page_count, stats.free_page_count
        );
        println!("   WAL size: {}", format_bytes(metrics.wal_size));

        if detailed {
            println!("\n🔍 Detailed Metrics:");
            println!("   Active transactions: {}", stats.active_transactions);
            println!("   B+Tree height: {}", stats.tree_height);
            println!(
                "   Total operations: {} reads, {} writes",
                metrics.total_reads, metrics.total_writes
            );
        }

        prev_metrics = metrics;

        // Flush output
        io::stdout().flush()?;
    }
}

fn analyze_database(
    path: PathBuf,
    report: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Analyzing database for optimization opportunities...\n");

    let start = Instant::now();
    let config = LightningDbConfig {
        ..Default::default()
    };

    let db = Database::open(&path, config)?;
    let analysis = db.analyze()?;

    let mut output = String::new();
    output.push_str("Lightning DB Analysis Report\n");
    output.push_str(&"=".repeat(50));
    output.push_str("\n\n");

    // Performance analysis
    output.push_str("🚀 Performance Analysis:\n");
    if analysis.avg_key_size > 1024 {
        output.push_str("   ⚠️  Large average key size detected\n");
        output.push_str(&format!("      Current: {} bytes\n", analysis.avg_key_size));
        output.push_str("      Recommendation: Consider using hash-based keys\n\n");
    }

    if analysis.avg_value_size > 1024 * 1024 {
        output.push_str("   ⚠️  Large average value size detected\n");
        output.push_str(&format!(
            "      Current: {}\n",
            format_bytes(analysis.avg_value_size)
        ));
        output.push_str("      Recommendation: Consider external blob storage\n\n");
    }

    // Storage analysis
    output.push_str("💾 Storage Analysis:\n");
    let fragmentation = analysis.fragmentation_percentage;
    if fragmentation > 20.0 {
        output.push_str(&format!(
            "   ⚠️  High fragmentation: {:.1}%\n",
            fragmentation
        ));
        output.push_str("      Recommendation: Run compaction\n\n");
    } else {
        output.push_str(&format!(
            "   ✅ Low fragmentation: {:.1}%\n\n",
            fragmentation
        ));
    }

    // Cache analysis
    output.push_str("🧠 Cache Analysis:\n");
    let cache_efficiency = analysis.cache_hit_rate;
    if cache_efficiency < 80.0 {
        output.push_str(&format!(
            "   ⚠️  Low cache hit rate: {:.1}%\n",
            cache_efficiency
        ));
        output.push_str("      Recommendation: Increase cache size\n\n");
    } else {
        output.push_str(&format!(
            "   ✅ Good cache hit rate: {:.1}%\n\n",
            cache_efficiency
        ));
    }

    // Index analysis
    output.push_str("🔍 Index Analysis:\n");
    if analysis.btree_height > 5 {
        output.push_str(&format!(
            "   ⚠️  Deep B+Tree: {} levels\n",
            analysis.btree_height
        ));
        output.push_str("      Recommendation: Consider rebalancing\n\n");
    } else {
        output.push_str(&format!(
            "   ✅ Optimal B+Tree height: {} levels\n\n",
            analysis.btree_height
        ));
    }

    // Summary
    output.push_str(&"=".repeat(50));
    output.push_str(&format!(
        "\n\nAnalysis completed in {:.2}s\n",
        start.elapsed().as_secs_f64()
    ));

    let optimization_count = if fragmentation > 20.0 { 1 } else { 0 }
        + if cache_efficiency < 80.0 { 1 } else { 0 }
        + if analysis.btree_height > 5 { 1 } else { 0 }
        + if analysis.avg_key_size > 1024 { 1 } else { 0 }
        + if analysis.avg_value_size > 1024 * 1024 {
            1
        } else {
            0
        };

    output.push_str(&format!(
        "Found {} optimization opportunities\n",
        optimization_count
    ));

    // Print or save report
    if let Some(report_path) = report {
        fs::write(&report_path, &output)?;
        println!("📄 Report saved to: {}", report_path.display());
    } else {
        print!("{}", output);
    }

    Ok(())
}

fn repair_database(
    path: PathBuf,
    backup: Option<PathBuf>,
    aggressive: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 Repairing database...\n");

    // Create backup if requested
    if let Some(backup_path) = backup {
        println!("💾 Creating backup before repair...");
        backup_database(path.clone(), backup_path, 6)?;
        println!("✅ Backup created\n");
    }

    let start = Instant::now();

    // Open database in repair mode
    let config = LightningDbConfig {
        ..Default::default()
    };

    match Database::open(&path, config) {
        Ok(db) => {
            println!("🔍 Running repair process...");

            let repair_stats = db.repair()?;

            println!("\n✅ Repair completed!");
            println!("\n📊 Repair Statistics:");
            println!("   Pages checked: {}", repair_stats.pages_checked);
            println!("   Pages repaired: {}", repair_stats.pages_repaired);
            println!("   Orphaned pages: {}", repair_stats.orphaned_pages);
            println!("   Index errors fixed: {}", repair_stats.index_errors_fixed);

            if repair_stats.data_loss {
                println!("\n⚠️  Warning: Some data loss occurred during repair");
            }

            // Verify repaired database
            println!("\n🔍 Verifying repaired database...");
            let verify_result = db.run_integrity_check()?;

            if verify_result.is_valid {
                println!("✅ Database integrity verified");
            } else {
                println!("❌ Database still has issues after repair");
                return Err("Repair incomplete".into());
            }
        }
        Err(e) => {
            println!("❌ Failed to open database for repair: {}", e);

            if aggressive {
                println!("\n🚨 Attempting aggressive repair...");
                // In aggressive mode, we might try to salvage what we can
                // This is a placeholder for more sophisticated recovery
                return Err("Aggressive repair not yet implemented".into());
            }

            return Err(e.into());
        }
    }

    let duration = start.elapsed();
    println!("\n⏱️  Repair completed in {:.2}s", duration.as_secs_f64());

    Ok(())
}

// Helper functions

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", size as u64, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

fn get_directory_size(path: &PathBuf) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total_size = 0;

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_file() {
            total_size += metadata.len();
        }
    }

    Ok(total_size)
}

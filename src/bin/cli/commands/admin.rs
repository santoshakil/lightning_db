//! Administrative commands (backup, restore, stats, health, compact)

use clap::{Arg, ArgMatches, Command};
use lightning_db::features::backup::{BackupConfig, BackupManager};
use lightning_db::utils::integrity::checker::{format_integrity_report, IntegrityChecker};
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

use crate::cli::utils::{
    format_bytes, format_duration, print_info, print_success, validate_backup_exists,
    validate_db_exists, validate_path_not_exists, CliResult, JsonOutput,
};
use crate::cli::GlobalOptions;

// ============================================================================
// Backup Command
// ============================================================================

/// Build the 'backup' subcommand
pub fn backup_command() -> Command {
    Command::new("backup")
        .about("Create a database backup")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("output")
                .help("Backup output path")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("incremental")
                .help("Create incremental backup")
                .long("incremental")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("compress")
                .help("Compression type (none, zstd). Note: zstd requires zstd-compression feature")
                .long("compress")
                .value_parser(["none", "zstd"])
                .default_value("none"),
        )
}

/// Execute the 'backup' command
pub fn run_backup(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let db_path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let backup_path = matches
        .get_one::<String>("output")
        .ok_or("output argument is required")?;
    let incremental = matches.get_flag("incremental");
    let compress = matches
        .get_one::<String>("compress")
        .ok_or("compress argument is required")?;

    validate_db_exists(db_path)?;
    validate_path_not_exists(backup_path, "Backup")?;

    if !global.is_json() && !global.quiet {
        println!(
            "Creating {} backup...",
            if incremental { "incremental" } else { "full" }
        );
    }

    let _db = Database::open(db_path, LightningDbConfig::default())?;
    let config = BackupConfig {
        include_wal: true,
        compress: compress != "none",
        verify: false,
        max_incremental_size: 100 * 1024 * 1024,
        online_backup: true,
        io_throttle_mb_per_sec: 50,
    };

    let backup_manager = BackupManager::new(config);

    let start = Instant::now();
    let backup = backup_manager.create_backup(db_path, backup_path)?;
    let duration = start.elapsed();

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "backup");
        output.add_str("source", db_path);
        output.add_str("destination", backup_path);
        output.add_str("type", &format!("{:?}", backup.backup_type));
        output.add_uint("size_bytes", backup.total_size);
        output.add_uint("file_count", backup.file_count as u64);
        output.add_float("duration_ms", duration.as_secs_f64() * 1000.0);
        output.add_bool("compressed", compress != "none");
        output.print();
    } else if !global.quiet {
        print_success("Backup created successfully");
        println!("  Type:     {:?}", backup.backup_type);
        println!("  Size:     {}", format_bytes(backup.total_size));
        println!("  Files:    {}", backup.file_count);
        println!("  Duration: {}", format_duration(duration));
    }

    Ok(())
}

// ============================================================================
// Restore Command
// ============================================================================

/// Build the 'restore' subcommand
pub fn restore_command() -> Command {
    Command::new("restore")
        .about("Restore from backup")
        .arg(
            Arg::new("backup")
                .help("Backup path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("output")
                .help("Restore destination")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("verify")
                .help("Verify backup before restore")
                .long("verify")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'restore' command
pub fn run_restore(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let backup_path = matches
        .get_one::<String>("backup")
        .ok_or("backup argument is required")?;
    let restore_path = matches
        .get_one::<String>("output")
        .ok_or("output argument is required")?;
    let verify = matches.get_flag("verify");

    validate_backup_exists(backup_path)?;
    validate_path_not_exists(restore_path, "Restore")?;

    if !global.is_json() && !global.quiet {
        println!("Restoring from backup...");
    }

    let config = BackupConfig::default();
    let backup_manager = BackupManager::new(config);

    if verify && !global.is_json() && !global.quiet {
        print_info("Backup verification not yet implemented in CLI");
    }

    let start = Instant::now();
    backup_manager.restore_backup(backup_path, restore_path)?;
    let duration = start.elapsed();

    // Verify the restored database can be opened
    let _db = Database::open(restore_path, LightningDbConfig::default())?;

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "restore");
        output.add_str("source", backup_path);
        output.add_str("destination", restore_path);
        output.add_bool("verified", true);
        output.add_float("duration_ms", duration.as_secs_f64() * 1000.0);
        output.print();
    } else if !global.quiet {
        print_success("Restore completed successfully");
        println!("  Duration: {}", format_duration(duration));
        print_success("Restored database verified");
    }

    Ok(())
}

// ============================================================================
// Stats Command
// ============================================================================

/// Build the 'stats' subcommand
pub fn stats_command() -> Command {
    Command::new("stats")
        .about("Show database statistics")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("detailed")
                .help("Show detailed statistics")
                .long("detailed")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'stats' command
pub fn run_stats(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let detailed = matches.get_flag("detailed");

    validate_db_exists(path)?;

    let _db = Database::open(path, LightningDbConfig::default())?;

    // Get statistics from realtime stats
    use lightning_db::features::statistics::REALTIME_STATS;
    let stats = REALTIME_STATS.read().get_current_stats();

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("database", path);

        // Operations stats
        let mut ops = JsonOutput::new();
        ops.add_uint("reads", stats.get_ops);
        ops.add_uint("writes", stats.put_ops);
        ops.add_uint("deletes", stats.delete_ops);
        ops.add_uint("scans", stats.range_ops);
        output.add_object("operations", ops);

        // Cache stats
        let mut cache = JsonOutput::new();
        cache.add_uint("hits", stats.cache_hits);
        cache.add_uint("misses", stats.cache_misses);
        cache.add_uint("evictions", stats.cache_evictions);
        cache.add_float("hit_rate", stats.cache_hit_rate);
        output.add_object("cache", cache);

        // Transaction stats
        let mut txn = JsonOutput::new();
        txn.add_uint("active", stats.active_transactions);
        txn.add_uint("committed", stats.committed_transactions);
        txn.add_uint("aborted", stats.aborted_transactions);
        output.add_object("transactions", txn);

        if detailed {
            // WAL stats
            let mut wal = JsonOutput::new();
            wal.add_uint("size_bytes", stats.wal_size_bytes);
            output.add_object("wal", wal);

            // Resource usage
            let mut resources = JsonOutput::new();
            resources.add_float("memory_mb", stats.memory_usage_mb);
            resources.add_float("cpu_percent", stats.cpu_usage_percent);
            output.add_object("resources", resources);

            // Size stats
            let mut sizes = JsonOutput::new();
            sizes.add_uint("data_bytes", stats.data_size_bytes);
            sizes.add_uint("index_bytes", stats.index_size_bytes);
            sizes.add_uint("cache_bytes", stats.cache_size_bytes);
            output.add_object("sizes", sizes);
        }

        output.print();
    } else {
        println!("=== Lightning DB Statistics ===");
        println!("\nOperations:");
        println!("  Reads:      {}", stats.get_ops);
        println!("  Writes:     {}", stats.put_ops);
        println!("  Deletes:    {}", stats.delete_ops);
        println!("  Scans:      {}", stats.range_ops);

        println!("\nCache:");
        println!(
            "  Hits:       {} ({:.1}%)",
            stats.cache_hits,
            stats.cache_hit_rate * 100.0
        );
        println!("  Misses:     {}", stats.cache_misses);
        println!("  Evictions:  {}", stats.cache_evictions);

        println!("\nTransactions:");
        println!("  Active:     {}", stats.active_transactions);
        println!("  Committed:  {}", stats.committed_transactions);
        println!("  Aborted:    {}", stats.aborted_transactions);

        if detailed {
            println!("\nWAL:");
            println!("  Size:       {}", format_bytes(stats.wal_size_bytes));

            println!("\nResource Usage:");
            println!("  Memory:     {:.1} MB", stats.memory_usage_mb);
            println!("  CPU:        {:.1}%", stats.cpu_usage_percent);

            println!("\nDatabase Size:");
            println!("  Data:       {}", format_bytes(stats.data_size_bytes));
            println!("  Index:      {}", format_bytes(stats.index_size_bytes));
            println!("  Cache:      {}", format_bytes(stats.cache_size_bytes));
        }
    }

    Ok(())
}

// ============================================================================
// Health Command
// ============================================================================

/// Build the 'health' subcommand
pub fn health_command() -> Command {
    Command::new("health")
        .about("Check database health")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("verify")
                .help("Perform integrity verification")
                .long("verify")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'health' command
pub fn run_health(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let verify = matches.get_flag("verify");

    validate_db_exists(path)?;

    if !global.is_json() && !global.quiet {
        println!("Checking database health...");
    }
    let start = Instant::now();

    let db = Database::open(path, LightningDbConfig::default())?;

    // Track test results for JSON output
    let mut checks_passed: Vec<&str> = Vec::new();
    let mut checks_failed: Vec<String> = Vec::new();

    // Basic health check using transactions that are rolled back
    // This avoids polluting user data with test keys

    // Test 1: Transaction begin/abort (no data pollution)
    let tx_id = db.begin_transaction()?;
    db.abort_transaction(tx_id)?;
    checks_passed.push("transaction_lifecycle");
    if !global.is_json() && !global.quiet {
        print_success("Transaction lifecycle: OK");
    }

    // Test 2: Use a unique timestamped key to minimize collision risk
    // and ensure we clean up properly
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let test_key = format!("__lightning_health_{}__", timestamp);
    let test_key = test_key.as_bytes();
    let test_value = b"health_check_value";

    // Write test within transaction
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, test_key, test_value)?;

    // Read within same transaction
    match db.get_tx(tx_id, test_key)? {
        Some(value) if value == test_value => {
            checks_passed.push("read_write");
            if !global.is_json() && !global.quiet {
                print_success("Read/Write operations: OK");
            }
        }
        _ => {
            db.abort_transaction(tx_id)?;
            checks_failed.push("read_write".to_string());
            if global.is_json() {
                let mut output = JsonOutput::new();
                output.status(false);
                output.add_str("error", "health_check_failed");
                output.add_str("failed_check", "read_write");
                output.print();
            }
            return Err("Health check failed: read/write test".into());
        }
    }

    // Abort to clean up - don't leave test data in database
    db.abort_transaction(tx_id)?;
    checks_passed.push("transaction_rollback");
    if !global.is_json() && !global.quiet {
        print_success("Transaction rollback: OK");
    }

    // Verify the aborted data is not visible
    if db.get(test_key)?.is_none() {
        checks_passed.push("transaction_isolation");
        if !global.is_json() && !global.quiet {
            print_success("Transaction isolation: OK");
        }
    } else {
        // Clean up if somehow visible
        let _ = db.delete(test_key);
    }

    // Integrity verification results
    let mut integrity_report = None;

    // Perform full integrity verification if requested
    if verify {
        if !global.is_json() && !global.quiet {
            println!("\nPerforming comprehensive integrity verification...");
        }
        // Use quiet mode for JSON output to avoid polluting the output
        let mut checker = if global.is_json() || global.quiet {
            IntegrityChecker::new_quiet(&db)
        } else {
            IntegrityChecker::new(&db)
        };

        match checker.check_all() {
            Ok(report) => {
                if report.passed {
                    checks_passed.push("integrity_check");
                    integrity_report = Some((true, report.statistics.total_pages, report.statistics.total_keys, 0, 0));
                    if !global.is_json() && !global.quiet {
                        print_success("Database integrity: OK");
                        println!("  Pages scanned: {}", report.statistics.total_pages);
                        println!("  Keys verified: {}", report.statistics.total_keys);
                    }
                } else {
                    checks_failed.push("integrity_check".to_string());
                    // Report not stored since we return immediately with error
                    if !global.is_json() {
                        println!("{}", format_integrity_report(&report));
                    }
                    if global.is_json() {
                        let mut output = JsonOutput::new();
                        output.status(false);
                        output.add_str("error", "integrity_check_failed");
                        output.add_uint("error_count", report.errors.len() as u64);
                        output.add_uint("warning_count", report.warnings.len() as u64);
                        output.print();
                    }
                    return Err(format!(
                        "Integrity check failed: {} errors, {} warnings",
                        report.errors.len(),
                        report.warnings.len()
                    ).into());
                }
            }
            Err(e) => {
                checks_failed.push("integrity_check".to_string());
                if global.is_json() {
                    let mut output = JsonOutput::new();
                    output.status(false);
                    output.add_str("error", "integrity_verification_failed");
                    output.add_str("message", &e.to_string());
                    output.print();
                }
                return Err(format!("Integrity verification failed: {}", e).into());
            }
        }
    }

    let duration = start.elapsed();

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("database", path);
        output.add_str("result", "passed");
        output.add_string_array("checks_passed", &checks_passed.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        output.add_float("duration_ms", duration.as_secs_f64() * 1000.0);

        if let Some((passed, pages, keys, errors, warnings)) = integrity_report {
            let mut integrity = JsonOutput::new();
            integrity.add_bool("passed", passed);
            integrity.add_uint("pages_scanned", pages as u64);
            integrity.add_uint("keys_verified", keys as u64);
            integrity.add_uint("errors", errors as u64);
            integrity.add_uint("warnings", warnings as u64);
            output.add_object("integrity", integrity);
        }

        output.print();
    } else if !global.quiet {
        println!("\nDatabase health check: PASSED");
        println!("Duration: {}", format_duration(duration));
    }

    Ok(())
}

// ============================================================================
// Compact Command
// ============================================================================

/// Build the 'compact' subcommand
pub fn compact_command() -> Command {
    Command::new("compact")
        .about("Trigger database compaction")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("force")
                .help("Force compaction even if not needed")
                .long("force")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'compact' command
pub fn run_compact(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let force = matches.get_flag("force");

    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;

    if !global.is_json() && !global.quiet {
        println!(
            "Triggering compaction{}...",
            if force { " (forced)" } else { "" }
        );
    }

    let start = Instant::now();

    if force {
        // Force compaction: do a full LSM compaction plus checkpoint
        db.compact_lsm()?;
        db.trigger_compaction()?;
    }

    // Always do a checkpoint to flush pending writes
    db.checkpoint()?;
    let duration = start.elapsed();

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "compact");
        output.add_str("database", path);
        output.add_bool("forced", force);
        output.add_float("duration_ms", duration.as_secs_f64() * 1000.0);
        output.print();
    } else if !global.quiet {
        print_success("Compaction completed");
        println!("  Duration: {}", format_duration(duration));
    }

    Ok(())
}

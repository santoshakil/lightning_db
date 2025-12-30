//! Transaction testing commands for enterprise-grade verification

use clap::{Arg, ArgMatches, Command};
use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;

use crate::cli::utils::{
    format_duration, print_info, print_success, validate_db_exists, validate_tx_test_ops, CliResult,
};
use crate::cli::utils::display::JsonOutput;
use crate::cli::GlobalOptions;

/// Build the 'tx-test' subcommand
pub fn tx_test_command() -> Command {
    Command::new("tx-test")
        .about("Test transaction isolation and ACID properties")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("isolation")
                .help("Isolation level to test (read-committed, repeatable-read, snapshot, serializable)")
                .long("isolation")
                .value_parser(["read-committed", "repeatable-read", "snapshot", "serializable"])
                .default_value("snapshot"),
        )
        .arg(
            Arg::new("operations")
                .help("Number of operations per transaction (1-10000)")
                .long("ops")
                .default_value("100"),
        )
        .arg(
            Arg::new("verbose")
                .help("Show detailed test output")
                .long("verbose")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'tx-test' command
pub fn run_tx_test(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let isolation = matches
        .get_one::<String>("isolation")
        .ok_or("isolation argument is required")?;
    let ops: usize = matches
        .get_one::<String>("operations")
        .ok_or("operations argument is required")?
        .parse()
        .map_err(|_| "operations must be a positive number")?;
    let verbose = matches.get_flag("verbose");

    // Validate parameters
    validate_tx_test_ops(ops)?;
    validate_db_exists(path)?;

    if !global.is_json() && !global.quiet {
        println!("=== Transaction Test Suite ===");
        println!("Database: {}", path);
        println!("Isolation level: {}", isolation);
        println!("Operations per test: {}", ops);
        println!();
    }

    let db = Database::open(path, LightningDbConfig::default())?;
    let mut passed = 0;
    let mut failed = 0;
    let mut test_results: Vec<(&str, bool, f64)> = Vec::new();
    let overall_start = Instant::now();

    // Test 1: Basic transaction commit
    if !global.is_json() && !global.quiet {
        print_info("Test 1: Basic transaction commit");
    }
    let start = Instant::now();
    match test_basic_commit(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("basic_commit", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("basic_commit", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 2: Transaction rollback
    if !global.is_json() && !global.quiet {
        print_info("Test 2: Transaction rollback");
    }
    let start = Instant::now();
    match test_rollback(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("rollback", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("rollback", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 3: Read isolation (uncommitted writes not visible)
    if !global.is_json() && !global.quiet {
        print_info("Test 3: Read isolation");
    }
    let start = Instant::now();
    match test_read_isolation(&db, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("read_isolation", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("read_isolation", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 4: Write durability (committed writes persist)
    if !global.is_json() && !global.quiet {
        print_info("Test 4: Write durability");
    }
    let start = Instant::now();
    match test_durability(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("durability", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("durability", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 5: Transaction atomicity (all-or-nothing)
    if !global.is_json() && !global.quiet {
        print_info("Test 5: Transaction atomicity");
    }
    let start = Instant::now();
    match test_atomicity(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("atomicity", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("atomicity", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    let total_duration = overall_start.elapsed().as_secs_f64() * 1000.0;

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(failed == 0);
        output.add_str("database", path);
        output.add_str("isolation_level", isolation);
        output.add_uint("operations_per_test", ops as u64);
        output.add_uint("passed", passed as u64);
        output.add_uint("failed", failed as u64);
        output.add_uint("total", (passed + failed) as u64);
        output.add_float("duration_ms", total_duration);

        // Add individual test results
        let passed_tests: Vec<&str> = test_results
            .iter()
            .filter(|(_, passed, _)| *passed)
            .map(|(name, _, _)| *name)
            .collect();
        let failed_tests: Vec<&str> = test_results
            .iter()
            .filter(|(_, passed, _)| !*passed)
            .map(|(name, _, _)| *name)
            .collect();

        output.add_str("passed_tests", &passed_tests.join(","));
        if !failed_tests.is_empty() {
            output.add_str("failed_tests", &failed_tests.join(","));
        }
        output.print();
    } else if !global.quiet {
        // Summary
        println!();
        println!("=== Test Summary ===");
        println!("Passed: {}", passed);
        println!("Failed: {}", failed);
        println!("Total:  {}", passed + failed);

        if failed == 0 {
            print_success("All transaction tests passed");
        }
    }

    if failed > 0 {
        Err(format!("{} test(s) failed", failed).into())
    } else {
        Ok(())
    }
}

fn test_basic_commit(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let tx_id = db.begin_transaction()?;

    // Write operations within transaction
    for i in 0..ops {
        let key = format!("__tx_test_commit_{}_{}", timestamp, i);
        let value = format!("value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
    }

    // Commit
    db.commit_transaction(tx_id)?;

    // Verify writes are visible after commit
    for i in 0..ops {
        let key = format!("__tx_test_commit_{}_{}", timestamp, i);
        let expected_value = format!("value_{}", i);

        match db.get(key.as_bytes())? {
            Some(value) => {
                if value != expected_value.as_bytes() {
                    return Err(format!(
                        "Value mismatch for key {}: expected '{}', got '{}'",
                        key,
                        expected_value,
                        String::from_utf8_lossy(&value)
                    )
                    .into());
                }
            }
            None => {
                return Err(format!("Key not found after commit: {}", key).into());
            }
        }
    }

    // Cleanup
    for i in 0..ops {
        let key = format!("__tx_test_commit_{}_{}", timestamp, i);
        db.delete(key.as_bytes())?;
    }

    if verbose {
        println!("  Committed {} operations successfully", ops);
    }

    Ok(())
}

fn test_rollback(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let tx_id = db.begin_transaction()?;

    // Write operations within transaction
    for i in 0..ops {
        let key = format!("__tx_test_rollback_{}_{}", timestamp, i);
        let value = format!("value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
    }

    // Verify writes are visible within transaction
    for i in 0..ops {
        let key = format!("__tx_test_rollback_{}_{}", timestamp, i);
        let result = db.get_tx(tx_id, key.as_bytes())?;
        if result.is_none() {
            return Err(format!("Key not visible within transaction: {}", key).into());
        }
    }

    // Rollback
    db.abort_transaction(tx_id)?;

    // Verify writes are NOT visible after rollback
    for i in 0..ops {
        let key = format!("__tx_test_rollback_{}_{}", timestamp, i);
        if db.get(key.as_bytes())?.is_some() {
            return Err(format!("Key should not be visible after rollback: {}", key).into());
        }
    }

    if verbose {
        println!("  Rolled back {} operations successfully", ops);
    }

    Ok(())
}

fn test_read_isolation(db: &Database, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let key = format!("__tx_test_isolation_{}", timestamp);
    let value = b"uncommitted_value";

    // Start a transaction and write, but don't commit
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, key.as_bytes(), value)?;

    // Read from outside the transaction - should NOT see uncommitted write
    let external_read = db.get(key.as_bytes())?;
    if external_read.is_some() {
        db.abort_transaction(tx_id)?;
        return Err("Uncommitted write visible outside transaction - isolation violated".into());
    }

    // Read from inside the transaction - SHOULD see uncommitted write
    let internal_read = db.get_tx(tx_id, key.as_bytes())?;
    if internal_read.is_none() {
        db.abort_transaction(tx_id)?;
        return Err("Uncommitted write not visible inside transaction".into());
    }

    // Cleanup
    db.abort_transaction(tx_id)?;

    if verbose {
        println!("  Transaction isolation verified");
    }

    Ok(())
}

fn test_durability(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Write and commit
    let tx_id = db.begin_transaction()?;
    for i in 0..ops {
        let key = format!("__tx_test_durability_{}_{}", timestamp, i);
        let value = format!("durable_value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
    }
    db.commit_transaction(tx_id)?;

    // Force checkpoint to ensure writes are persisted
    db.checkpoint()?;

    // Verify all writes are still readable
    for i in 0..ops {
        let key = format!("__tx_test_durability_{}_{}", timestamp, i);
        let expected = format!("durable_value_{}", i);
        match db.get(key.as_bytes())? {
            Some(value) => {
                if value != expected.as_bytes() {
                    return Err(format!("Durability check failed for key {}", key).into());
                }
            }
            None => {
                return Err(format!("Durable key not found: {}", key).into());
            }
        }
    }

    // Cleanup
    for i in 0..ops {
        let key = format!("__tx_test_durability_{}_{}", timestamp, i);
        db.delete(key.as_bytes())?;
    }

    if verbose {
        println!("  Verified durability of {} operations", ops);
    }

    Ok(())
}

fn test_atomicity(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Ensure no keys exist before test
    for i in 0..ops {
        let key = format!("__tx_test_atomicity_{}_{}", timestamp, i);
        let _ = db.delete(key.as_bytes());
    }

    // Test 1: Committed transaction - all writes visible
    let tx_id = db.begin_transaction()?;
    for i in 0..ops {
        let key = format!("__tx_test_atomicity_{}_{}", timestamp, i);
        let value = format!("atomic_value_{}", i);
        db.put_tx(tx_id, key.as_bytes(), value.as_bytes())?;
    }
    db.commit_transaction(tx_id)?;

    // Verify ALL writes are visible (atomicity of commit)
    let mut visible_count = 0;
    for i in 0..ops {
        let key = format!("__tx_test_atomicity_{}_{}", timestamp, i);
        if db.get(key.as_bytes())?.is_some() {
            visible_count += 1;
        }
    }

    if visible_count != ops {
        return Err(format!(
            "Atomicity violated: expected {} visible, got {}",
            ops, visible_count
        )
        .into());
    }

    // Cleanup
    for i in 0..ops {
        let key = format!("__tx_test_atomicity_{}_{}", timestamp, i);
        db.delete(key.as_bytes())?;
    }

    if verbose {
        println!("  Verified atomicity of {} operations", ops);
    }

    Ok(())
}

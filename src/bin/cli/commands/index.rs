//! Index testing and management commands

use clap::{Arg, ArgMatches, Command};
use lightning_db::{Database, IndexConfig, IndexKey, IndexType, LightningDbConfig, SimpleRecord};
use std::time::Instant;

use crate::cli::utils::{
    format_duration, print_info, print_success, validate_db_exists, CliResult,
};
use crate::cli::utils::display::JsonOutput;
use crate::cli::GlobalOptions;

/// Build the 'index-test' subcommand
pub fn index_test_command() -> Command {
    Command::new("index-test")
        .about("Test index operations and performance")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("operations")
                .help("Number of operations (1-50, limited by page size)")
                .long("ops")
                .default_value("50"),
        )
        .arg(
            Arg::new("verbose")
                .help("Show detailed test output")
                .long("verbose")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Build the 'index-list' subcommand
pub fn index_list_command() -> Command {
    Command::new("index-list")
        .about("List all indexes in the database")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
}

/// Execute the 'index-test' command
pub fn run_index_test(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let ops: usize = matches
        .get_one::<String>("operations")
        .ok_or("operations argument is required")?
        .parse()
        .map_err(|_| "operations must be a positive number")?;
    let verbose = matches.get_flag("verbose");

    // Validate parameters - limited to 50 due to page size constraints
    if ops == 0 || ops > 50 {
        return Err("operations must be between 1 and 50 (limited by page size)".into());
    }

    validate_db_exists(path)?;

    if !global.is_json() && !global.quiet {
        println!("=== Index Test Suite ===");
        println!("Database: {}", path);
        println!("Operations per test: {}", ops);
        println!();
    }

    let db = Database::open(path, LightningDbConfig::default())?;
    let mut passed = 0;
    let mut failed = 0;
    let mut test_results: Vec<(&str, bool, f64)> = Vec::new();
    let overall_start = Instant::now();

    // Test 1: Index creation
    if !global.is_json() && !global.quiet {
        print_info("Test 1: Index creation");
    }
    let start = Instant::now();
    match test_index_creation(&db, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_creation", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_creation", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 2: Index lookup
    if !global.is_json() && !global.quiet {
        print_info("Test 2: Index lookup performance");
    }
    let start = Instant::now();
    match test_index_lookup(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_lookup", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_lookup", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 3: Indexed updates
    if !global.is_json() && !global.quiet {
        print_info("Test 3: Indexed updates");
    }
    let start = Instant::now();
    match test_indexed_updates(&db, ops, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("indexed_updates", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("indexed_updates", false, duration));
            if !global.is_json() {
                eprintln!("[FAIL] {}", e);
            }
            failed += 1;
        }
    }

    // Test 4: Index listing
    if !global.is_json() && !global.quiet {
        print_info("Test 4: Index listing");
    }
    let start = Instant::now();
    match test_index_listing(&db, verbose && !global.is_json()) {
        Ok(_) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_listing", true, duration));
            if !global.is_json() && !global.quiet {
                print_success(&format!("PASSED ({})", format_duration(start.elapsed())));
            }
            passed += 1;
        }
        Err(e) => {
            let duration = start.elapsed().as_secs_f64() * 1000.0;
            test_results.push(("index_listing", false, duration));
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
        output.add_uint("operations_per_test", ops as u64);
        output.add_uint("passed", passed as u64);
        output.add_uint("failed", failed as u64);
        output.add_uint("total", (passed + failed) as u64);
        output.add_float("duration_ms", total_duration);

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
            print_success("All index tests passed");
        }
    }

    if failed > 0 {
        Err(format!("{} test(s) failed", failed).into())
    } else {
        Ok(())
    }
}

/// Execute the 'index-list' command
pub fn run_index_list(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;

    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;
    let indexes = db.list_indexes();

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("database", path);
        output.add_uint("count", indexes.len() as u64);
        output.add_str("indexes", &indexes.join(","));
        output.print();
    } else if !global.quiet {
        if indexes.is_empty() {
            println!("No indexes found in database.");
        } else {
            println!("Indexes ({}):", indexes.len());
            for index in &indexes {
                println!("  - {}", index);
            }
        }
    }

    Ok(())
}

/// Create a SimpleRecord with a single field
fn create_record(field_name: &str, value: &[u8]) -> SimpleRecord {
    let mut record = SimpleRecord::new();
    record.set_field(field_name.to_string(), value.to_vec());
    record
}

fn test_index_creation(db: &Database, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let index_name = format!("__test_idx_{}", timestamp);

    // Create index
    let config = IndexConfig {
        name: index_name.clone(),
        columns: vec!["id".to_string()],
        index_type: IndexType::BTree,
        unique: false,
    };

    db.create_index_with_config(config)?;

    // Verify index exists
    let indexes = db.list_indexes();
    if !indexes.contains(&index_name) {
        return Err("Created index not found in list".into());
    }

    // Clean up
    db.drop_index(&index_name)?;

    // Verify index was dropped
    let indexes = db.list_indexes();
    if indexes.contains(&index_name) {
        return Err("Dropped index still in list".into());
    }

    if verbose {
        println!("  Index creation/drop cycle completed");
    }

    Ok(())
}

fn test_index_lookup(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let index_name = format!("__test_lookup_idx_{}", timestamp);

    // Create index
    let config = IndexConfig {
        name: index_name.clone(),
        columns: vec!["id".to_string()],
        index_type: IndexType::BTree,
        unique: false,
    };
    db.create_index_with_config(config)?;

    // Insert indexed data
    let start = Instant::now();
    for i in 0..ops {
        let key = format!("__idx_test_{}_{}", timestamp, i);
        let value = format!(r#"{{"id":{},"name":"user{}"}}"#, i, i);
        let id_bytes = i.to_string().as_bytes().to_vec();
        let record = create_record("id", &id_bytes);

        db.put_indexed(key.as_bytes(), value.as_bytes(), &record)?;
    }
    let insert_duration = start.elapsed();

    // Lookup via index
    let start = Instant::now();
    for i in 0..ops {
        let id_bytes = i.to_string().as_bytes().to_vec();
        let index_key = IndexKey::single(id_bytes);
        let _result = db.get_by_index(&index_name, &index_key)?;
    }
    let lookup_duration = start.elapsed();

    // Cleanup
    for i in 0..ops {
        let key = format!("__idx_test_{}_{}", timestamp, i);
        let id_bytes = i.to_string().as_bytes().to_vec();
        let record = create_record("id", &id_bytes);
        db.delete_indexed(key.as_bytes(), &record)?;
    }
    db.drop_index(&index_name)?;

    if verbose {
        let insert_rate = ops as f64 / insert_duration.as_secs_f64();
        let lookup_rate = ops as f64 / lookup_duration.as_secs_f64();
        println!("  Insert rate: {:.0} ops/sec", insert_rate);
        println!("  Lookup rate: {:.0} ops/sec", lookup_rate);
    }

    Ok(())
}

fn test_indexed_updates(db: &Database, ops: usize, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let index_name = format!("__test_update_idx_{}", timestamp);

    // Create index
    let config = IndexConfig {
        name: index_name.clone(),
        columns: vec!["id".to_string()],
        index_type: IndexType::BTree,
        unique: false,
    };
    db.create_index_with_config(config)?;

    // Insert initial data
    for i in 0..ops {
        let key = format!("__idx_update_test_{}_{}", timestamp, i);
        let value = format!(r#"{{"id":{},"version":1}}"#, i);
        let id_bytes = i.to_string().as_bytes().to_vec();
        let record = create_record("id", &id_bytes);

        db.put_indexed(key.as_bytes(), value.as_bytes(), &record)?;
    }

    // Update all entries
    let start = Instant::now();
    for i in 0..ops {
        let key = format!("__idx_update_test_{}_{}", timestamp, i);
        let id_bytes = i.to_string().as_bytes().to_vec();
        let old_record = create_record("id", &id_bytes);
        let new_value = format!(r#"{{"id":{},"version":2}}"#, i);
        let new_record = create_record("id", &id_bytes);

        db.update_indexed(key.as_bytes(), &old_record, new_value.as_bytes(), &new_record)?;
    }
    let update_duration = start.elapsed();

    // Cleanup
    for i in 0..ops {
        let key = format!("__idx_update_test_{}_{}", timestamp, i);
        let id_bytes = i.to_string().as_bytes().to_vec();
        let record = create_record("id", &id_bytes);
        db.delete_indexed(key.as_bytes(), &record)?;
    }
    db.drop_index(&index_name)?;

    if verbose {
        let update_rate = ops as f64 / update_duration.as_secs_f64();
        println!("  Update rate: {:.0} ops/sec", update_rate);
    }

    Ok(())
}

fn test_index_listing(db: &Database, verbose: bool) -> CliResult<()> {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    // Create multiple indexes
    let index_names: Vec<String> = (0..5)
        .map(|i| format!("__test_list_idx_{}_{}", timestamp, i))
        .collect();

    for name in &index_names {
        let config = IndexConfig {
            name: name.clone(),
            columns: vec!["field".to_string()],
            index_type: IndexType::BTree,
            unique: false,
        };
        db.create_index_with_config(config)?;
    }

    // List and verify
    let indexes = db.list_indexes();
    for name in &index_names {
        if !indexes.contains(name) {
            // Cleanup before returning error
            for n in &index_names {
                let _ = db.drop_index(n);
            }
            return Err(format!("Index {} not found in listing", name).into());
        }
    }

    // Cleanup
    for name in &index_names {
        db.drop_index(name)?;
    }

    if verbose {
        println!("  Created and listed {} indexes successfully", index_names.len());
    }

    Ok(())
}

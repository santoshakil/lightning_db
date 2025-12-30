//! Database integrity check command

use clap::{Arg, ArgMatches, Command};
use lightning_db::utils::integrity::checker::{format_integrity_report, IntegrityChecker};
use lightning_db::{Database, LightningDbConfig};

use crate::cli::utils::{print_separator, validate_checksum_sample, validate_db_exists, CliResult, JsonOutput};
use crate::cli::GlobalOptions;

/// Build the 'check' subcommand
pub fn check_command() -> Command {
    Command::new("check")
        .about("Check database integrity")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("checksums")
                .help("Verify checksums (sample size)")
                .long("checksums")
                .value_name("SAMPLE_SIZE")
                .default_value("100"),
        )
        .arg(
            Arg::new("verbose")
                .help("Verbose output")
                .long("verbose")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'check' command
pub fn run_check(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let checksum_sample: usize = matches
        .get_one::<String>("checksums")
        .ok_or("checksums argument is required")?
        .parse()
        .map_err(|_| "checksums must be a non-negative number")?;
    let verbose = matches.get_flag("verbose");

    validate_checksum_sample(checksum_sample)?;
    validate_db_exists(path)?;

    if !global.is_json() && !global.quiet {
        println!("Checking database integrity: {}", path);
        print_separator(60);
    }

    let db = Database::open(path, LightningDbConfig::default())?;

    // Run integrity check with optional checksum verification
    // Use quiet mode for JSON output to avoid polluting the output
    let mut checker = if global.is_json() {
        IntegrityChecker::new_quiet(&db)
    } else {
        IntegrityChecker::new(&db)
    };

    // Run all integrity checks
    let final_report = checker.check_all()?;

    // Optionally verify checksums (note: this doesn't change the report)
    if checksum_sample > 0 {
        checker.verify_checksums(checksum_sample)?;
    }

    // Output results
    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(final_report.passed);
        output.add_str("database", path);
        output.add_bool("passed", final_report.passed);

        // Statistics
        let mut stats = JsonOutput::new();
        stats.add_uint("total_pages", final_report.statistics.total_pages as u64);
        stats.add_uint("total_keys", final_report.statistics.total_keys as u64);
        stats.add_uint("total_versions", final_report.statistics.total_versions as u64);
        stats.add_uint("btree_depth", final_report.statistics.btree_depth as u64);
        stats.add_uint("btree_nodes", final_report.statistics.btree_nodes as u64);
        stats.add_uint("lsm_levels", final_report.statistics.lsm_levels as u64);
        output.add_object("statistics", stats);

        // Errors summary
        output.add_uint("error_count", final_report.errors.len() as u64);
        output.add_uint("warning_count", final_report.warnings.len() as u64);

        // Include first few errors in JSON output
        if !final_report.errors.is_empty() {
            let error_strings: Vec<String> = final_report
                .errors
                .iter()
                .take(10)
                .map(|e| format!("[{}] {}: {}", e.component, e.error_type, e.details))
                .collect();
            output.add_string_array("errors", &error_strings);
        }

        output.print();
    } else if verbose {
        println!("{}", format_integrity_report(&final_report));
    } else {
        // Summary only
        if final_report.passed {
            println!("[OK] Integrity check PASSED");
            println!("  Pages:    {}", final_report.statistics.total_pages);
            println!("  Keys:     {}", final_report.statistics.total_keys);
            println!("  Versions: {}", final_report.statistics.total_versions);
        } else {
            println!("[FAIL] Integrity check FAILED");
            println!("  Errors:   {}", final_report.errors.len());
            println!("  Warnings: {}", final_report.warnings.len());

            // Show first few errors
            for (i, error) in final_report.errors.iter().take(5).enumerate() {
                println!("  Error {}: [{}] {}", i + 1, error.component, error.details);
            }

            if final_report.errors.len() > 5 {
                println!("  ... and {} more errors", final_report.errors.len() - 5);
            }
        }
    }

    if !final_report.passed {
        return Err(format!(
            "Integrity check failed with {} errors and {} warnings",
            final_report.errors.len(),
            final_report.warnings.len()
        ).into());
    }

    Ok(())
}

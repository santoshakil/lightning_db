//! Database lifecycle commands

use clap::{Arg, ArgMatches, Command};
use lightning_db::{Database, LightningDbConfig};

use crate::cli::utils::{print_success, validate_cache_size, validate_db_not_exists, CliResult, JsonOutput};
use crate::cli::GlobalOptions;

/// Build the 'create' subcommand
pub fn create_command() -> Command {
    Command::new("create")
        .about("Create a new database")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("cache-size")
                .help("Cache size in MB (1-102400)")
                .long("cache-size")
                .default_value("100"),
        )
        .arg(
            Arg::new("compression")
                .help("Enable compression")
                .long("compression")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'create' command
pub fn run_create(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;

    // Validate cache-size
    let cache_size_mb: u64 = matches
        .get_one::<String>("cache-size")
        .ok_or("cache-size argument is required")?
        .parse::<u64>()
        .map_err(|_| "cache-size must be a positive number")?;

    let cache_size = validate_cache_size(cache_size_mb)?;
    let compression = matches.get_flag("compression");

    // Check if path already exists
    validate_db_not_exists(path)?;

    let config = LightningDbConfig {
        cache_size,
        compression_enabled: compression,
        ..Default::default()
    };

    if !global.is_json() && !global.quiet {
        println!("Creating database at: {}", path);
    }

    let _db = Database::create(path, config)?;

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "create");
        output.add_str("path", path);
        output.add_uint("cache_size_mb", cache_size_mb);
        output.add_bool("compression", compression);
        output.print();
    } else if !global.quiet {
        print_success("Database created successfully");
    }

    Ok(())
}

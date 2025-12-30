//! Key-value operation commands

use clap::{Arg, ArgMatches, Command};
use lightning_db::{Database, LightningDbConfig};

use crate::cli::utils::{
    format_kv, format_value, print_success, validate_db_exists, validate_key,
    validate_scan_limit, validate_value, CliResult, JsonOutput,
};
use crate::cli::GlobalOptions;

/// Build the 'get' subcommand
pub fn get_command() -> Command {
    Command::new("get")
        .about("Get a value by key")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("key")
                .help("Key to retrieve")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("format")
                .help("Output format (text, hex, json)")
                .long("format")
                .value_parser(["text", "hex", "json"])
                .default_value("text"),
        )
}

/// Execute the 'get' command
pub fn run_get(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let key = matches
        .get_one::<String>("key")
        .ok_or("key argument is required")?;
    let format = matches
        .get_one::<String>("format")
        .ok_or("format argument is required")?;

    validate_key(key)?;
    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;

    match db.get(key.as_bytes())? {
        Some(value) => {
            if global.is_json() {
                let mut output = JsonOutput::new();
                output.status(true);
                output.add_str("key", key);
                // Handle binary values in JSON output
                match String::from_utf8(value.clone()) {
                    Ok(s) => output.add_str("value", &s),
                    Err(_) => output.add_str("value_hex", &hex::encode(&value)),
                };
                output.add_uint("size", value.len() as u64);
                output.print();
            } else {
                println!("{}", format_value(&value, format));
            }
            Ok(())
        }
        None => {
            if global.is_json() {
                let mut output = JsonOutput::new();
                output.status(false);
                output.add_str("error", "key_not_found");
                output.add_str("key", key);
                output.print();
            }
            Err(format!("Key not found: {}", key).into())
        }
    }
}

/// Build the 'put' subcommand
pub fn put_command() -> Command {
    Command::new("put")
        .about("Store a key-value pair")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("key")
                .help("Key to store")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("value")
                .help("Value to store")
                .required(true)
                .index(3),
        )
}

/// Execute the 'put' command
pub fn run_put(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let key = matches
        .get_one::<String>("key")
        .ok_or("key argument is required")?;
    let value = matches
        .get_one::<String>("value")
        .ok_or("value argument is required")?;

    validate_key(key)?;
    validate_value(value)?;
    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;
    db.put(key.as_bytes(), value.as_bytes())?;

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "put");
        output.add_str("key", key);
        output.add_uint("value_size", value.len() as u64);
        output.print();
    } else if !global.quiet {
        print_success(&format!("Stored key: {}", key));
    }
    Ok(())
}

/// Build the 'delete' subcommand
pub fn delete_command() -> Command {
    Command::new("delete")
        .about("Delete a key")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("key")
                .help("Key to delete")
                .required(true)
                .index(2),
        )
}

/// Execute the 'delete' command
pub fn run_delete(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let key = matches
        .get_one::<String>("key")
        .ok_or("key argument is required")?;

    validate_key(key)?;
    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;
    db.delete(key.as_bytes())?;

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "delete");
        output.add_str("key", key);
        output.print();
    } else if !global.quiet {
        print_success(&format!("Deleted key: {}", key));
    }
    Ok(())
}

/// Build the 'scan' subcommand
pub fn scan_command() -> Command {
    Command::new("scan")
        .about("Scan keys in range")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("start")
                .help("Start key (inclusive)")
                .long("start")
                .default_value(""),
        )
        .arg(
            Arg::new("end")
                .help("End key (exclusive)")
                .long("end"),
        )
        .arg(
            Arg::new("limit")
                .help("Maximum number of results (1-1000000)")
                .long("limit")
                .default_value("100"),
        )
        .arg(
            Arg::new("reverse")
                .help("Scan in reverse order")
                .long("reverse")
                .action(clap::ArgAction::SetTrue),
        )
}

/// Execute the 'scan' command
pub fn run_scan(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let start = matches
        .get_one::<String>("start")
        .ok_or("start argument is required")?;
    let end = matches.get_one::<String>("end");
    let limit: usize = matches
        .get_one::<String>("limit")
        .ok_or("limit argument is required")?
        .parse()
        .map_err(|_| "limit must be a positive number")?;
    let reverse = matches.get_flag("reverse");

    validate_scan_limit(limit)?;
    validate_db_exists(path)?;

    let db = Database::open(path, LightningDbConfig::default())?;

    // Create appropriate range iterator
    let start_bytes = if start.is_empty() {
        None
    } else {
        Some(start.as_bytes())
    };

    let end_bytes = end.map(|s| s.as_bytes());

    // Use appropriate scan direction - scan_reverse is more efficient than
    // collecting all items and reversing
    let iterator = if reverse {
        db.scan_reverse(start_bytes, end_bytes)?
    } else {
        db.scan(start_bytes, end_bytes)?
    };

    // Collect results with limit
    let mut items = Vec::new();
    for item in iterator {
        items.push(item?);
        if items.len() >= limit {
            break;
        }
    }

    // Output results
    if global.is_json() {
        let entries: Vec<(String, String)> = items
            .iter()
            .map(|(k, v)| {
                let key_str = String::from_utf8_lossy(k).to_string();
                let value_str = match String::from_utf8(v.clone()) {
                    Ok(s) => s,
                    Err(_) => format!("hex:{}", hex::encode(v)),
                };
                (key_str, value_str)
            })
            .collect();

        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("operation", "scan");
        output.add_bool("reverse", reverse);
        output.add_uint("count", items.len() as u64);
        output.add_uint("limit", limit as u64);
        output.add_kv_array("entries", &entries);
        output.print();
    } else {
        println!("Scanning entries{}:", if reverse { " (reverse)" } else { "" });
        for (key, value) in &items {
            let (key_str, value_str) = format_kv(key, value);
            println!("{}: {}", key_str, value_str);
        }
        println!("\nTotal entries found: {}", items.len());
    }

    Ok(())
}

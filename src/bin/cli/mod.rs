//! Lightning DB CLI - Enterprise-grade command-line interface
//!
//! This module provides a modular, maintainable CLI for database administration.
//!
//! # Output Formats
//!
//! The CLI supports two output formats:
//! - `text` (default): Human-readable output with formatting
//! - `json`: Machine-readable JSON output for scripting and automation
//!
//! Use the `--output` or `-o` flag to specify the format.
//! Use `--quiet` or `-q` to suppress informational messages.

pub mod commands;
pub mod utils;

use clap::{Arg, Command};

/// Output format for CLI commands
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(OutputFormat::Text),
            "json" => Ok(OutputFormat::Json),
            _ => Err(format!("Invalid output format: {}. Use 'text' or 'json'.", s)),
        }
    }
}

/// Global CLI options that apply to all commands
#[derive(Debug, Clone)]
pub struct GlobalOptions {
    pub output_format: OutputFormat,
    pub quiet: bool,
}

impl GlobalOptions {
    /// Extract global options from argument matches
    pub fn from_matches(matches: &clap::ArgMatches) -> Self {
        let output_format = matches
            .get_one::<String>("format")
            .map(|s| s.parse().unwrap_or_default())
            .unwrap_or_default();

        let quiet = matches.get_flag("quiet");

        GlobalOptions {
            output_format,
            quiet,
        }
    }

    /// Check if output should be JSON
    pub fn is_json(&self) -> bool {
        self.output_format == OutputFormat::Json
    }
}

/// Build the CLI command structure
pub fn build_cli() -> Command {
    Command::new("lightning-cli")
        .about("Lightning DB Administrative CLI - Enterprise-grade database management")
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        // Global options
        .arg(
            Arg::new("format")
                .help("Output format: text (default) or json")
                .short('o')
                .long("format")
                .global(true)
                .value_parser(["text", "json"])
                .default_value("text"),
        )
        .arg(
            Arg::new("quiet")
                .help("Suppress informational output (errors still shown)")
                .short('q')
                .long("quiet")
                .global(true)
                .action(clap::ArgAction::SetTrue),
        )
        // Database commands
        .subcommand(commands::database::create_command())
        // Key-Value commands
        .subcommand(commands::kv::get_command())
        .subcommand(commands::kv::put_command())
        .subcommand(commands::kv::delete_command())
        .subcommand(commands::kv::scan_command())
        // Admin commands
        .subcommand(commands::admin::backup_command())
        .subcommand(commands::admin::restore_command())
        .subcommand(commands::admin::stats_command())
        .subcommand(commands::admin::health_command())
        .subcommand(commands::admin::compact_command())
        // Benchmark and check
        .subcommand(commands::bench::bench_command())
        .subcommand(commands::check::check_command())
        // Transaction testing
        .subcommand(commands::transaction::tx_test_command())
        // Index management
        .subcommand(commands::index::index_test_command())
        .subcommand(commands::index::index_list_command())
}

/// Dispatch to appropriate command handler
pub fn run(matches: clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    match matches.subcommand() {
        Some(("create", sub)) => commands::database::run_create(sub),
        Some(("get", sub)) => commands::kv::run_get(sub),
        Some(("put", sub)) => commands::kv::run_put(sub),
        Some(("delete", sub)) => commands::kv::run_delete(sub),
        Some(("scan", sub)) => commands::kv::run_scan(sub),
        Some(("backup", sub)) => commands::admin::run_backup(sub),
        Some(("restore", sub)) => commands::admin::run_restore(sub),
        Some(("stats", sub)) => commands::admin::run_stats(sub),
        Some(("health", sub)) => commands::admin::run_health(sub),
        Some(("compact", sub)) => commands::admin::run_compact(sub),
        Some(("bench", sub)) => commands::bench::run_bench(sub),
        Some(("check", sub)) => commands::check::run_check(sub),
        Some(("tx-test", sub)) => commands::transaction::run_tx_test(sub),
        Some(("index-test", sub)) => commands::index::run_index_test(sub),
        Some(("index-list", sub)) => commands::index::run_index_list(sub),
        _ => {
            eprintln!("Error: Unknown command. Use --help for available commands.");
            std::process::exit(1);
        }
    }
}

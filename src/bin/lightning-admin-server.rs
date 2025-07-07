use clap::{Arg, Command};
use lightning_db::{simple_http_admin::start_admin_server, Database, LightningDbConfig};
use std::path::Path;
use std::sync::Arc;

/// Lightning DB HTTP Admin Server
///
/// Provides HTTP endpoints for database administration

fn main() {
    let matches = Command::new("lightning-admin-server")
        .about("Lightning DB HTTP Admin Server")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::new("database")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("port")
                .help("Port to listen on")
                .short('p')
                .long("port")
                .default_value("8080"),
        )
        .arg(
            Arg::new("create")
                .help("Create database if it doesn't exist")
                .long("create")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    let db_path = matches.get_one::<String>("database").unwrap();
    let port: u16 = matches
        .get_one::<String>("port")
        .unwrap()
        .parse()
        .expect("Invalid port number");
    let create = matches.get_flag("create");

    // Open or create database
    let db = if create || !Path::new(db_path).exists() {
        println!("Creating database at: {}", db_path);
        match Database::create(db_path, LightningDbConfig::default()) {
            Ok(db) => Arc::new(db),
            Err(e) => {
                eprintln!("Failed to create database: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        println!("Opening database at: {}", db_path);
        match Database::open(db_path, LightningDbConfig::default()) {
            Ok(db) => Arc::new(db),
            Err(e) => {
                eprintln!("Failed to open database: {}", e);
                std::process::exit(1);
            }
        }
    };

    // Start admin server
    if let Err(e) = start_admin_server(db, port) {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

use lightning_db::features::migration::cli;

fn main() {
    if let Err(e) = cli::run_cli() {
        eprintln!("Migration tool error: {}", e);
        std::process::exit(1);
    }
}

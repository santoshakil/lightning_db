//! Lightning DB Administrative CLI
//!
//! Enterprise-grade command-line interface providing:
//! - Database management (create)
//! - Key-value operations (get, put, delete, scan)
//! - Backup and restore
//! - Performance monitoring and statistics
//! - Health checks and integrity verification
//! - Compaction control
//! - Performance benchmarking
//!
//! Exit codes:
//! - 0: Success
//! - 1: General error
//! - 2: Usage error (invalid command line)
//! - 3: Database not found
//! - 4: Database already exists
//! - 5: Validation error
//! - 6: Permission denied
//! - 7: IO error
//! - 8: Database operation error
//! - 9: Transaction error
//! - 10: Integrity check failed
//! - 11: Test failure
//! - 99: Internal error

mod cli;

use cli::utils::error::{exit_codes, CliError};

fn main() {
    let matches = cli::build_cli().get_matches();

    if let Err(e) = cli::run(matches) {
        // Try to downcast to CliError for proper exit code
        let exit_code = if let Some(cli_err) = e.downcast_ref::<CliError>() {
            eprintln!("Error: {}", cli_err);
            cli_err.exit_code()
        } else {
            // Attempt to categorize standard errors
            let msg = e.to_string();
            let (code, category) = categorize_error(&msg);
            eprintln!("Error [{}]: {}", category, msg);
            code
        };
        std::process::exit(exit_code);
    }
}

/// Categorize error messages for appropriate exit codes
fn categorize_error(msg: &str) -> (i32, &'static str) {
    let msg_lower = msg.to_lowercase();

    if msg_lower.contains("not found") || msg_lower.contains("does not exist") {
        (exit_codes::NOT_FOUND, "NOT_FOUND")
    } else if msg_lower.contains("already exists") {
        (exit_codes::ALREADY_EXISTS, "ALREADY_EXISTS")
    } else if msg_lower.contains("permission") || msg_lower.contains("access denied") {
        (exit_codes::PERMISSION_DENIED, "PERMISSION")
    } else if msg_lower.contains("transaction") {
        (exit_codes::TRANSACTION_ERROR, "TRANSACTION")
    } else if msg_lower.contains("integrity") || msg_lower.contains("corrupt") {
        (exit_codes::INTEGRITY_FAILED, "INTEGRITY")
    } else if msg_lower.contains("validation") || msg_lower.contains("invalid")
           || msg_lower.contains("must be") || msg_lower.contains("cannot exceed") {
        (exit_codes::VALIDATION_ERROR, "VALIDATION")
    } else if msg_lower.contains("i/o") || msg_lower.contains("read") || msg_lower.contains("write") {
        (exit_codes::IO_ERROR, "IO")
    } else if msg_lower.contains("test") && msg_lower.contains("fail") {
        (exit_codes::TEST_FAILED, "TEST")
    } else {
        (exit_codes::GENERAL_ERROR, "ERROR")
    }
}

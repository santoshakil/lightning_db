//! CLI command modules
//!
//! Organized by domain for maintainability:
//! - database: Database lifecycle (create)
//! - kv: Key-value operations (get, put, delete, scan)
//! - admin: Administrative operations (backup, restore, stats, health, compact)
//! - bench: Performance benchmarking
//! - check: Integrity checking
//! - transaction: Transaction testing and validation
//! - index: Index management and testing

pub mod admin;
pub mod bench;
pub mod check;
pub mod database;
pub mod index;
pub mod kv;
pub mod transaction;

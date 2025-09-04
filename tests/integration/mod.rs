#![cfg(feature = "integration_tests")]
//! Lightning DB Integration Test Suite
//! 
//! Comprehensive integration tests for validating the entire system
//! working together under realistic conditions.

// Heavy integration suites are gated behind the `integration_tests` feature
// to keep default test runs fast and reliable during stabilization.
#[cfg(feature = "integration_tests")] pub mod end_to_end_tests;
#[cfg(feature = "integration_tests")] pub mod concurrency_tests;
#[cfg(feature = "integration_tests")] pub mod recovery_integration_tests;
#[cfg(feature = "integration_tests")] pub mod performance_integration_tests;
#[cfg(feature = "integration_tests")] pub mod ha_tests;
#[cfg(feature = "integration_tests")] pub mod security_integration_tests;
#[cfg(feature = "integration_tests")] pub mod system_integration_tests;
#[cfg(feature = "integration_tests")] pub mod chaos_tests;
#[cfg(feature = "integration_tests")] pub mod test_orchestrator;

use std::path::PathBuf;
use std::sync::Arc;
use lightning_db::{Database, LightningDbConfig};
use tempfile::TempDir;
use rand;
use num_cpus;
use chrono;
use base64;
use md5;

pub struct TestEnvironment {
    pub temp_dir: TempDir,
    pub db_path: PathBuf,
    pub db: Arc<Database>,
    pub config: LightningDbConfig,
}

impl TestEnvironment {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::new_with_config(LightningDbConfig::default())
    }

    pub fn new_with_config(config: LightningDbConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db");
        let db = Arc::new(Database::create(&db_path, config.clone())?);
        
        Ok(TestEnvironment {
            temp_dir,
            db_path,
            db,
            config,
        })
    }

    pub fn db(&self) -> Arc<Database> {
        self.db.clone()
    }

    pub fn recreate_db(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let db_path = self.temp_dir.path().join("test_db_new");
        self.db = Arc::new(Database::create(&db_path, self.config.clone())?);
        self.db_path = db_path;
        Ok(())
    }
}

pub fn setup_test_data(db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    for i in 0..1000 {
        let key = format!("test_key_{:04}", i);
        let value = format!("test_value_{:04}_{}", i, "x".repeat(i % 100));
        db.put(key.as_bytes(), value.as_bytes())?;
    }
    Ok(())
}

pub fn verify_test_data(db: &Database) -> Result<bool, Box<dyn std::error::Error>> {
    for i in 0..1000 {
        let key = format!("test_key_{:04}", i);
        let expected_value = format!("test_value_{:04}_{}", i, "x".repeat(i % 100));
        
        match db.get(key.as_bytes())? {
            Some(value) => {
                if value != expected_value.as_bytes() {
                    return Ok(false);
                }
            }
            None => return Ok(false),
        }
    }
    Ok(true)
}

pub fn generate_workload_data(size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..size)
        .map(|i| {
            let key = format!("workload_key_{:08}", i).into_bytes();
            let value = format!("workload_value_{:08}_{}", i, "data".repeat(i % 50)).into_bytes();
            (key, value)
        })
        .collect()
}

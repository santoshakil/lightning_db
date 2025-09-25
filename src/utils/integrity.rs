use crate::{Database, Result, Error};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IntegrityReport {
    pub total_keys: u64,
    pub total_values: u64,
    pub total_size: u64,
    pub corrupted_entries: Vec<CorruptedEntry>,
    pub orphaned_pages: Vec<u32>,
    pub duplicate_keys: Vec<Vec<u8>>,
    pub check_duration: std::time::Duration,
    pub is_healthy: bool,
}

#[derive(Debug, Clone)]
pub struct CorruptedEntry {
    pub key: Vec<u8>,
    pub error: String,
    pub page_id: Option<u32>,
}

pub struct IntegrityChecker {
    checked_keys: Arc<AtomicU64>,
    errors_found: Arc<AtomicU64>,
    total_size: Arc<AtomicU64>,
}

impl IntegrityChecker {
    pub fn new() -> Self {
        Self {
            checked_keys: Arc::new(AtomicU64::new(0)),
            errors_found: Arc::new(AtomicU64::new(0)),
            total_size: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn check_database(&self, db: &Database) -> Result<IntegrityReport> {
        let start_time = std::time::Instant::now();
        let mut corrupted_entries = Vec::new();
        let mut seen_keys = HashSet::new();
        let mut duplicate_keys = Vec::new();

        // Scan entire database
        let iter = db.scan(None, None)?;

        for entry_result in iter {
            match entry_result {
                Ok((key, value)) => {
                    self.checked_keys.fetch_add(1, Ordering::Relaxed);
                    self.total_size.fetch_add(
                        (key.len() + value.len()) as u64,
                        Ordering::Relaxed
                    );

                    // Check for duplicates
                    if !seen_keys.insert(key.clone()) {
                        duplicate_keys.push(key.clone());
                        self.errors_found.fetch_add(1, Ordering::Relaxed);
                    }

                    // Verify we can read back the value
                    match db.get(&key) {
                        Ok(Some(read_value)) => {
                            if read_value != value {
                                corrupted_entries.push(CorruptedEntry {
                                    key: key.clone(),
                                    error: "Value mismatch on read-back".to_string(),
                                    page_id: None,
                                });
                                self.errors_found.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Ok(None) => {
                            corrupted_entries.push(CorruptedEntry {
                                key: key.clone(),
                                error: "Key disappeared during check".to_string(),
                                page_id: None,
                            });
                            self.errors_found.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            corrupted_entries.push(CorruptedEntry {
                                key: key.clone(),
                                error: format!("Read error: {}", e),
                                page_id: None,
                            });
                            self.errors_found.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Err(e) => {
                    self.errors_found.fetch_add(1, Ordering::Relaxed);
                    corrupted_entries.push(CorruptedEntry {
                        key: Vec::new(),
                        error: format!("Scan error: {}", e),
                        page_id: None,
                    });
                }
            }
        }

        let total_keys = self.checked_keys.load(Ordering::Relaxed);
        let total_errors = self.errors_found.load(Ordering::Relaxed);

        Ok(IntegrityReport {
            total_keys,
            total_values: total_keys, // Same as keys for KV store
            total_size: self.total_size.load(Ordering::Relaxed),
            corrupted_entries,
            orphaned_pages: Vec::new(), // Would need page manager access
            duplicate_keys,
            check_duration: start_time.elapsed(),
            is_healthy: total_errors == 0,
        })
    }

    pub fn verify_transaction_consistency(&self, db: &Database) -> Result<bool> {
        // Start a transaction and verify ACID properties
        let tx_id = db.begin_transaction()?;

        let test_key = b"__integrity_test_key__";
        let test_value = b"test_value";

        // Write in transaction
        db.put_tx(tx_id, test_key, test_value)?;

        // Verify not visible outside transaction
        match db.get(test_key)? {
            None => {}, // Good - isolation works
            Some(_) => {
                db.abort_transaction(tx_id)?;
                return Ok(false);
            }
        }

        // Verify visible inside transaction
        match db.get_tx(tx_id, test_key)? {
            Some(v) if v == test_value => {}, // Good
            _ => {
                db.abort_transaction(tx_id)?;
                return Ok(false);
            }
        }

        // Abort and verify rollback
        db.abort_transaction(tx_id)?;

        // Should not exist after abort
        match db.get(test_key)? {
            None => Ok(true), // Good - rollback works
            Some(_) => Ok(false),
        }
    }

    pub fn check_compression_integrity(&self, db: &Database) -> Result<bool> {
        // Test that compression/decompression preserves data
        let test_data = vec![
            (b"small".to_vec(), b"value".to_vec()),
            (b"medium".to_vec(), vec![0xAB; 1000]),
            (b"large".to_vec(), vec![0xCD; 100_000]),
            (b"repetitive".to_vec(), vec![0x42; 10_000]),
        ];

        for (key, value) in test_data {
            db.put(&key, &value)?;
            match db.get(&key)? {
                Some(retrieved) if retrieved == value => {},
                _ => return Ok(false),
            }
            db.delete(&key)?;
        }

        Ok(true)
    }
}

impl Database {
    pub fn check_integrity(&self) -> Result<IntegrityReport> {
        let checker = IntegrityChecker::new();
        checker.check_database(self)
    }

    pub fn verify_consistency(&self) -> Result<bool> {
        let checker = IntegrityChecker::new();
        checker.verify_transaction_consistency(self)
    }

    pub fn repair(&self) -> Result<RepairReport> {
        // Basic repair operations
        let mut report = RepairReport {
            errors_found: 0,
            errors_fixed: 0,
            unfixable_errors: 0,
            repair_duration: std::time::Duration::default(),
        };

        let start = std::time::Instant::now();

        // Force flush all pending writes
        if let Some(ref lsm) = self.lsm_tree {
            lsm.flush()?;
        }

        // Sync page manager
        self.page_manager.sync()?;

        // Compact if using LSM
        if let Some(ref lsm) = self.lsm_tree {
            lsm.compact()?;
        }

        report.repair_duration = start.elapsed();
        Ok(report)
    }
}

#[derive(Debug, Clone)]
pub struct RepairReport {
    pub errors_found: u32,
    pub errors_fixed: u32,
    pub unfixable_errors: u32,
    pub repair_duration: std::time::Duration,
}
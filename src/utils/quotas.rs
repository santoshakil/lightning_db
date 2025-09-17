use crate::{Error, Result};
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    pub enabled: bool,
    pub max_database_size: Option<u64>,
    pub max_transaction_size: Option<u64>,
    pub max_concurrent_transactions: Option<usize>,
    pub max_connections: Option<usize>,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_database_size: None,
            max_transaction_size: None,
            max_concurrent_transactions: None,
            max_connections: None,
        }
    }
}

#[derive(Debug)]
pub struct QuotaManager {
    config: QuotaConfig,
    current_size: AtomicU64,
    transaction_count: AtomicU64,
}

impl QuotaManager {
    pub fn new(config: QuotaConfig) -> Result<Self> {
        Ok(Self {
            config,
            current_size: AtomicU64::new(0),
            transaction_count: AtomicU64::new(0),
        })
    }

    pub fn check_database_quota(&self, additional_bytes: u64) -> Result<()> {
        if let Some(max_size) = self.config.max_database_size {
            let current = self.current_size.load(Ordering::Relaxed);
            if current + additional_bytes > max_size {
                return Err(Error::QuotaExceeded(
                    format!("Database size quota exceeded: limit={}, requested={}", max_size, current + additional_bytes)
                ));
            }
        }
        Ok(())
    }

    pub fn check_transaction_quota(&self, size: u64) -> Result<()> {
        if let Some(max_size) = self.config.max_transaction_size {
            if size > max_size {
                return Err(Error::QuotaExceeded(
                    format!("Transaction size quota exceeded: limit={}, requested={}", max_size, size)
                ));
            }
        }
        Ok(())
    }

    pub fn acquire_transaction_slot(&self) -> Result<()> {
        if let Some(max_concurrent) = self.config.max_concurrent_transactions {
            let current = self.transaction_count.fetch_add(1, Ordering::AcqRel);
            if current >= max_concurrent as u64 {
                self.transaction_count.fetch_sub(1, Ordering::AcqRel);
                return Err(Error::TransactionLimitReached {
                    limit: max_concurrent,
                });
            }
        }
        Ok(())
    }

    pub fn release_transaction_slot(&self) {
        self.transaction_count.fetch_sub(1, Ordering::AcqRel);
    }

    pub fn update_database_size(&self, new_size: u64) {
        self.current_size.store(new_size, Ordering::Relaxed);
    }

    pub fn get_current_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    pub fn get_transaction_count(&self) -> u64 {
        self.transaction_count.load(Ordering::Relaxed)
    }

    pub fn check_write_allowed(&self, size: u64) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        self.check_database_quota(size)?;
        self.check_transaction_quota(size)
    }

    pub fn check_read_allowed(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        Ok(())
    }

    pub fn check_connection_allowed(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        if let Some(max_connections) = self.config.max_connections {
            let current = self.transaction_count.load(Ordering::Relaxed);
            if current >= max_connections as u64 {
                return Err(Error::QuotaExceeded(
                    format!("Connection limit exceeded: limit={}, current={}", max_connections, current)
                ));
            }
        }
        Ok(())
    }
}
//! Memory quota management for Lightning DB

use crate::core::error::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

/// Memory quota manager
pub struct MemoryQuotaManager {
    // Total memory limits
    total_memory_limit: Option<u64>,
    cache_memory_limit: Option<u64>,
    transaction_memory_limit: Option<u64>,

    // Current usage
    total_memory_used: AtomicU64,
    cache_memory_used: AtomicU64,
    transaction_memory: RwLock<HashMap<u64, u64>>, // tx_id -> memory_used

    // Memory pressure threshold (percentage)
    pressure_threshold: f64,

    // Callbacks for memory pressure
    pressure_callbacks: RwLock<Vec<Box<dyn Fn() + Send + Sync>>>,
}

impl std::fmt::Debug for MemoryQuotaManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryQuotaManager")
            .field("total_memory_limit", &self.total_memory_limit)
            .field("cache_memory_limit", &self.cache_memory_limit)
            .field("transaction_memory_limit", &self.transaction_memory_limit)
            .field("total_memory_used", &self.total_memory_used)
            .field("cache_memory_used", &self.cache_memory_used)
            .field("pressure_threshold", &self.pressure_threshold)
            .field(
                "pressure_callbacks_count",
                &self.pressure_callbacks.read().len(),
            )
            .finish()
    }
}

impl MemoryQuotaManager {
    pub fn new(
        memory_quota_mb: Option<u64>,
        cache_quota_mb: Option<u64>,
        transaction_memory_limit_mb: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            total_memory_limit: memory_quota_mb.map(|mb| mb * 1024 * 1024),
            cache_memory_limit: cache_quota_mb.map(|mb| mb * 1024 * 1024),
            transaction_memory_limit: transaction_memory_limit_mb.map(|mb| mb * 1024 * 1024),
            total_memory_used: AtomicU64::new(0),
            cache_memory_used: AtomicU64::new(0),
            transaction_memory: RwLock::new(HashMap::new()),
            pressure_threshold: 0.9, // 90% threshold
            pressure_callbacks: RwLock::new(Vec::new()),
        })
    }

    /// Check if a memory allocation is allowed
    pub fn check_allocation(&self, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.total_memory_limit {
            let current = self.total_memory_used.load(Ordering::Relaxed);
            if current + size_bytes > limit {
                warn!(
                    "Memory allocation denied: {} + {} > {}",
                    current, size_bytes, limit
                );
                return Ok(false);
            }

            // Check memory pressure
            let usage_ratio = (current + size_bytes) as f64 / limit as f64;
            if usage_ratio > self.pressure_threshold {
                self.trigger_memory_pressure();
            }
        }

        Ok(true)
    }

    /// Check if a cache allocation is allowed
    pub fn check_cache_allocation(&self, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.cache_memory_limit {
            let current = self.cache_memory_used.load(Ordering::Relaxed);
            if current + size_bytes > limit {
                debug!(
                    "Cache allocation denied: {} + {} > {}",
                    current, size_bytes, limit
                );
                return Ok(false);
            }
        }

        // Also check against total memory limit
        self.check_allocation(size_bytes)
    }

    /// Check if a transaction allocation is allowed
    pub fn check_transaction_allocation(&self, tx_id: u64, size_bytes: u64) -> Result<bool> {
        if let Some(limit) = self.transaction_memory_limit {
            let tx_memory = self.transaction_memory.write();
            let current = tx_memory.get(&tx_id).copied().unwrap_or(0);

            if current + size_bytes > limit {
                warn!(
                    "Transaction {} memory allocation denied: {} + {} > {}",
                    tx_id, current, size_bytes, limit
                );
                return Ok(false);
            }
        }

        // Also check against total memory limit
        self.check_allocation(size_bytes)
    }

    /// Record memory allocation
    pub fn allocate(&self, size_bytes: u64) -> Result<()> {
        self.total_memory_used
            .fetch_add(size_bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Record cache memory allocation
    pub fn allocate_cache(&self, size_bytes: u64) -> Result<()> {
        self.cache_memory_used
            .fetch_add(size_bytes, Ordering::Relaxed);
        self.allocate(size_bytes)
    }

    /// Record transaction memory allocation
    pub fn allocate_transaction(&self, tx_id: u64, size_bytes: u64) -> Result<()> {
        let mut tx_memory = self.transaction_memory.write();
        let current = tx_memory.entry(tx_id).or_insert(0);
        *current += size_bytes;
        drop(tx_memory);

        self.allocate(size_bytes)
    }

    /// Record memory deallocation
    pub fn deallocate(&self, size_bytes: u64) -> Result<()> {
        self.total_memory_used
            .fetch_sub(size_bytes, Ordering::Relaxed);
        Ok(())
    }

    /// Record cache memory deallocation
    pub fn deallocate_cache(&self, size_bytes: u64) -> Result<()> {
        self.cache_memory_used
            .fetch_sub(size_bytes, Ordering::Relaxed);
        self.deallocate(size_bytes)
    }

    /// Record transaction memory deallocation
    pub fn deallocate_transaction(&self, tx_id: u64, size_bytes: u64) -> Result<()> {
        let mut tx_memory = self.transaction_memory.write();
        if let Some(current) = tx_memory.get_mut(&tx_id) {
            *current = current.saturating_sub(size_bytes);
            if *current == 0 {
                tx_memory.remove(&tx_id);
            }
        }
        drop(tx_memory);

        self.deallocate(size_bytes)
    }

    /// Release all memory for a transaction
    pub fn release_transaction(&self, tx_id: u64) -> Result<()> {
        let mut tx_memory = self.transaction_memory.write();
        if let Some(memory_used) = tx_memory.remove(&tx_id) {
            drop(tx_memory);
            self.deallocate(memory_used)?;
        }
        Ok(())
    }

    /// Register a callback for memory pressure events
    pub fn register_pressure_callback<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.pressure_callbacks.write().push(Box::new(callback));
    }

    /// Get current memory usage statistics
    pub fn get_usage_stats(&self) -> MemoryUsageStats {
        MemoryUsageStats {
            total_used: self.total_memory_used.load(Ordering::Relaxed),
            total_limit: self.total_memory_limit,
            cache_used: self.cache_memory_used.load(Ordering::Relaxed),
            cache_limit: self.cache_memory_limit,
            transaction_count: self.transaction_memory.read().len(),
            transaction_memory_total: self.transaction_memory.read().values().sum(),
        }
    }

    /// Force memory reclamation
    pub fn reclaim_memory(&self, target_bytes: u64) -> u64 {
        // This would trigger cache eviction, transaction rollback, etc.
        // For now, just trigger pressure callbacks
        self.trigger_memory_pressure();

        // Return amount reclaimed (simulated)
        let cache_used = self.cache_memory_used.load(Ordering::Relaxed);
        let reclaimed = target_bytes.min(cache_used / 2);

        if reclaimed > 0 {
            self.cache_memory_used
                .fetch_sub(reclaimed, Ordering::Relaxed);
            self.total_memory_used
                .fetch_sub(reclaimed, Ordering::Relaxed);
        }

        reclaimed
    }

    fn trigger_memory_pressure(&self) {
        warn!("Memory pressure detected, triggering callbacks");
        let callbacks = self.pressure_callbacks.read();
        for callback in callbacks.iter() {
            callback();
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub total_used: u64,
    pub total_limit: Option<u64>,
    pub cache_used: u64,
    pub cache_limit: Option<u64>,
    pub transaction_count: usize,
    pub transaction_memory_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_memory_allocation_within_limit() {
        let manager = MemoryQuotaManager::new(Some(100), None, None).unwrap();

        // Should allow allocation within limit
        assert!(manager.check_allocation(50 * 1024 * 1024).unwrap());
        manager.allocate(50 * 1024 * 1024).unwrap();

        // Should allow more allocation up to limit
        assert!(manager.check_allocation(40 * 1024 * 1024).unwrap());

        // Should deny allocation exceeding limit
        assert!(!manager.check_allocation(60 * 1024 * 1024).unwrap());
    }

    #[test]
    fn test_transaction_memory_tracking() {
        let manager = MemoryQuotaManager::new(None, None, Some(10)).unwrap();

        // Allocate memory for transaction
        assert!(manager
            .check_transaction_allocation(1, 5 * 1024 * 1024)
            .unwrap());
        manager.allocate_transaction(1, 5 * 1024 * 1024).unwrap();

        // Should allow more allocation within limit
        assert!(manager
            .check_transaction_allocation(1, 4 * 1024 * 1024)
            .unwrap());

        // Should deny allocation exceeding limit
        assert!(!manager
            .check_transaction_allocation(1, 6 * 1024 * 1024)
            .unwrap());

        // Release transaction memory
        manager.release_transaction(1).unwrap();

        // Should allow allocation again
        assert!(manager
            .check_transaction_allocation(1, 10 * 1024 * 1024)
            .unwrap());
    }

    #[test]
    fn test_memory_pressure_callback() {
        let manager = MemoryQuotaManager::new(Some(100), None, None).unwrap();

        let triggered = Arc::new(AtomicU64::new(0));
        let triggered_clone = triggered.clone();

        manager.register_pressure_callback(move || {
            triggered_clone.fetch_add(1, Ordering::Relaxed);
        });

        // Allocate close to limit (should trigger pressure)
        manager.allocate(91 * 1024 * 1024).unwrap();
        manager.check_allocation(1024).unwrap();

        assert!(triggered.load(Ordering::Relaxed) > 0);
    }
}

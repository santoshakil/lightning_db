use crate::error::{Error, Result};
use crate::transaction::{Transaction, TransactionManager, VersionStore};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Thread-safe transaction manager that prevents race conditions
pub struct SafeTransactionManager {
    inner: Arc<Mutex<TransactionManagerInner>>,
    version_store: Arc<VersionStore>,
    next_tx_id: AtomicU64,
}

struct TransactionManagerInner {
    active_transactions: std::collections::HashMap<u64, Arc<RwLock<Transaction>>>,
    max_active_transactions: usize,
}

impl SafeTransactionManager {
    pub fn new(max_active_transactions: usize, version_store: Arc<VersionStore>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionManagerInner {
                active_transactions: std::collections::HashMap::new(),
                max_active_transactions,
            })),
            version_store,
            next_tx_id: AtomicU64::new(1),
        }
    }
    
    /// Begin a new transaction with proper synchronization
    pub fn begin(&self) -> Result<u64> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        
        let mut inner = self.inner.lock();
        
        // Check limit while holding lock
        if inner.active_transactions.len() >= inner.max_active_transactions {
            return Err(Error::TooManyTransactions);
        }
        
        let transaction = Arc::new(RwLock::new(Transaction::new(
            tx_id,
            self.version_store.get_current_version(),
        )));
        
        inner.active_transactions.insert(tx_id, transaction);
        
        Ok(tx_id)
    }
    
    /// Get transaction with proper error handling
    pub fn get_transaction(&self, tx_id: u64) -> Result<Arc<RwLock<Transaction>>> {
        let inner = self.inner.lock();
        inner.active_transactions
            .get(&tx_id)
            .cloned()
            .ok_or(Error::TransactionNotFound)
    }
    
    /// Commit transaction with atomic version update
    pub fn commit(&self, tx_id: u64) -> Result<()> {
        let transaction = {
            let inner = self.inner.lock();
            inner.active_transactions
                .get(&tx_id)
                .cloned()
                .ok_or(Error::TransactionNotFound)?
        };
        
        // Lock transaction for commit
        let tx = transaction.write();
        
        // Validate read set
        for read_op in &tx.read_set {
            if !self.version_store.validate_read(&read_op.key, read_op.version, tx.read_timestamp) {
                return Err(Error::TransactionConflict);
            }
        }
        
        // Apply write set atomically
        let commit_version = self.version_store.get_next_version();
        for write_op in &tx.write_set {
            self.version_store.put(
                write_op.key.clone(),
                write_op.value.clone(),
                commit_version,
                tx_id,
            );
        }
        
        // Mark as committed
        drop(tx);
        
        // Remove from active transactions
        let mut inner = self.inner.lock();
        inner.active_transactions.remove(&tx_id);
        
        Ok(())
    }
    
    /// Abort transaction safely
    pub fn abort(&self, tx_id: u64) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.active_transactions.remove(&tx_id);
        Ok(())
    }
    
    /// Cleanup old transactions with proper locking
    pub fn cleanup_old_transactions(&self, max_age: std::time::Duration) {
        let mut inner = self.inner.lock();
        let now = std::time::SystemTime::now();
        
        inner.active_transactions.retain(|_, tx_arc| {
            let tx = tx_arc.read();
            match now.duration_since(tx.start_time) {
                Ok(age) => age < max_age,
                Err(_) => true, // Keep if time went backwards
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_concurrent_begin() {
        let version_store = Arc::new(VersionStore::new());
        let manager = Arc::new(SafeTransactionManager::new(10, version_store));
        
        let handles: Vec<_> = (0..5).map(|_| {
            let manager_clone = manager.clone();
            std::thread::spawn(move || {
                manager_clone.begin()
            })
        }).collect();
        
        let results: Vec<_> = handles.into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        
        // All transactions should have unique IDs
        let mut ids: Vec<_> = results.into_iter()
            .map(|r| r.unwrap())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5);
    }
}
use crate::transaction::VersionStore;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Automatic version cleanup for MVCC
#[derive(Debug)]
pub struct VersionCleanupThread {
    store: Arc<VersionStore>,
    should_stop: Arc<AtomicBool>,
    cleanup_interval: Duration,
    retention_duration: Duration,
    last_cleanup: AtomicU64,
}

impl VersionCleanupThread {
    pub fn new(
        store: Arc<VersionStore>,
        cleanup_interval: Duration,
        retention_duration: Duration,
    ) -> Self {
        Self {
            store,
            should_stop: Arc::new(AtomicBool::new(false)),
            cleanup_interval,
            retention_duration,
            last_cleanup: AtomicU64::new(0),
        }
    }

    pub fn start(self: Arc<Self>) -> thread::JoinHandle<()> {
        let cleanup_thread = self.clone();
        thread::spawn(move || {
            cleanup_thread.run();
        })
    }

    fn run(&self) {
        while !self.should_stop.load(Ordering::Acquire) {
            thread::sleep(self.cleanup_interval);

            if self.should_stop.load(Ordering::Acquire) {
                break;
            }

            self.perform_cleanup();
        }
    }

    fn perform_cleanup(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Failed to get system time for version cleanup: {}",
                    e
                );
                // Return last cleanup time + interval as fallback
                self.last_cleanup.load(Ordering::Acquire) + self.cleanup_interval.as_micros() as u64
            });

        // Clean up versions older than retention_duration
        let cleanup_before = now.saturating_sub(self.retention_duration.as_micros() as u64);

        // Keep at least 2 versions per key for MVCC correctness
        self.store.cleanup_old_versions(cleanup_before, 2);

        self.last_cleanup.store(now, Ordering::Relaxed);

        #[cfg(debug_assertions)]
        println!(
            "Version cleanup: removed versions before timestamp {}",
            cleanup_before
        );
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Release);
    }
}

/// Cleanup for committed transactions in MVCC
pub struct TransactionCleanup {
    max_retained_transactions: usize,
    _cleanup_interval: Duration,
}

impl TransactionCleanup {
    pub fn new(max_retained_transactions: usize, cleanup_interval: Duration) -> Self {
        Self {
            max_retained_transactions,
            _cleanup_interval: cleanup_interval,
        }
    }

    pub fn cleanup_old_transactions(
        &self,
        committed_transactions: &mut std::collections::BTreeMap<u64, u64>,
    ) {
        if committed_transactions.len() <= self.max_retained_transactions {
            return;
        }

        // Keep only the most recent transactions
        let remove_count = committed_transactions.len() - self.max_retained_transactions;
        let mut keys_to_remove = Vec::with_capacity(remove_count);

        for (tx_id, _) in committed_transactions.iter().take(remove_count) {
            keys_to_remove.push(*tx_id);
        }

        for key in keys_to_remove {
            committed_transactions.remove(&key);
        }

        #[cfg(debug_assertions)]
        println!(
            "Transaction cleanup: removed {} old transactions",
            remove_count
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::VersionStore;

    #[test]
    fn test_version_cleanup() {
        let store = Arc::new(VersionStore::new());

        // Add some versions
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch in test")
            .as_micros() as u64;

        for i in 0..100 {
            store.put(
                format!("key_{}", i).into_bytes(),
                Some(format!("value_{}", i).into_bytes()),
                now + i,
                i,
            );
        }

        // Cleanup versions older than now + 50
        store.cleanup_old_versions(now + 50, 1);

        // Verify old versions are cleaned up
        // (Would need to add a method to check version count)
    }

    #[test]
    fn test_transaction_cleanup() {
        use std::collections::BTreeMap;

        let mut transactions = BTreeMap::new();
        for i in 0..1000 {
            transactions.insert(i, i * 10);
        }

        let cleanup = TransactionCleanup::new(100, Duration::from_secs(1));
        cleanup.cleanup_old_transactions(&mut transactions);

        assert_eq!(transactions.len(), 100);
        assert!(transactions.contains_key(&900)); // Recent transaction kept
        assert!(!transactions.contains_key(&0)); // Old transaction removed
    }
}

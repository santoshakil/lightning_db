use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use std::collections::HashMap;

/// Core metrics for database operations
#[derive(Debug, Default)]
pub struct DatabaseMetrics {
    // Operation counters
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub deletes: AtomicU64,
    pub transactions: AtomicU64,
    
    // Operation latencies (in microseconds)
    pub read_latency_sum: AtomicU64,
    pub write_latency_sum: AtomicU64,
    pub delete_latency_sum: AtomicU64,
    pub transaction_latency_sum: AtomicU64,
    
    // Cache metrics
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,
    
    // Page metrics
    pub pages_read: AtomicU64,
    pub pages_written: AtomicU64,
    pub page_faults: AtomicU64,
    
    // Compaction metrics
    pub compactions: AtomicU64,
    pub compaction_bytes_read: AtomicU64,
    pub compaction_bytes_written: AtomicU64,
    
    // Error counters
    pub read_errors: AtomicU64,
    pub write_errors: AtomicU64,
    pub transaction_aborts: AtomicU64,
    
    // Size metrics
    pub database_size_bytes: AtomicU64,
    pub wal_size_bytes: AtomicU64,
    pub active_transactions: AtomicUsize,
}

impl DatabaseMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn record_read(&self, latency: Duration) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.read_latency_sum.fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }
    
    pub fn record_write(&self, latency: Duration) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.write_latency_sum.fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }
    
    pub fn record_delete(&self, latency: Duration) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.delete_latency_sum.fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }
    
    pub fn record_transaction(&self, latency: Duration) {
        self.transactions.fetch_add(1, Ordering::Relaxed);
        self.transaction_latency_sum.fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }
    
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_eviction(&self) {
        self.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_page_read(&self) {
        self.pages_read.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_page_write(&self) {
        self.pages_written.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_page_fault(&self) {
        self.page_faults.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_compaction(&self, bytes_read: u64, bytes_written: u64) {
        self.compactions.fetch_add(1, Ordering::Relaxed);
        self.compaction_bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.compaction_bytes_written.fetch_add(bytes_written, Ordering::Relaxed);
    }
    
    pub fn record_read_error(&self) {
        self.read_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_transaction_abort(&self) {
        self.transaction_aborts.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn update_database_size(&self, size_bytes: u64) {
        self.database_size_bytes.store(size_bytes, Ordering::Relaxed);
    }
    
    pub fn update_wal_size(&self, size_bytes: u64) {
        self.wal_size_bytes.store(size_bytes, Ordering::Relaxed);
    }
    
    pub fn increment_active_transactions(&self) {
        self.active_transactions.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn decrement_active_transactions(&self) {
        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let reads = self.reads.load(Ordering::Relaxed);
        let writes = self.writes.load(Ordering::Relaxed);
        let deletes = self.deletes.load(Ordering::Relaxed);
        let transactions = self.transactions.load(Ordering::Relaxed);
        
        let read_latency_sum = self.read_latency_sum.load(Ordering::Relaxed);
        let write_latency_sum = self.write_latency_sum.load(Ordering::Relaxed);
        let delete_latency_sum = self.delete_latency_sum.load(Ordering::Relaxed);
        let transaction_latency_sum = self.transaction_latency_sum.load(Ordering::Relaxed);
        
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        
        MetricsSnapshot {
            reads,
            writes,
            deletes,
            transactions,
            avg_read_latency_us: if reads > 0 { read_latency_sum / reads } else { 0 },
            avg_write_latency_us: if writes > 0 { write_latency_sum / writes } else { 0 },
            avg_delete_latency_us: if deletes > 0 { delete_latency_sum / deletes } else { 0 },
            avg_transaction_latency_us: if transactions > 0 { transaction_latency_sum / transactions } else { 0 },
            cache_hits,
            cache_misses,
            cache_hit_rate: if cache_hits + cache_misses > 0 {
                (cache_hits as f64) / ((cache_hits + cache_misses) as f64)
            } else {
                0.0
            },
            cache_evictions: self.cache_evictions.load(Ordering::Relaxed),
            pages_read: self.pages_read.load(Ordering::Relaxed),
            pages_written: self.pages_written.load(Ordering::Relaxed),
            page_faults: self.page_faults.load(Ordering::Relaxed),
            compactions: self.compactions.load(Ordering::Relaxed),
            compaction_bytes_read: self.compaction_bytes_read.load(Ordering::Relaxed),
            compaction_bytes_written: self.compaction_bytes_written.load(Ordering::Relaxed),
            read_errors: self.read_errors.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            transaction_aborts: self.transaction_aborts.load(Ordering::Relaxed),
            database_size_bytes: self.database_size_bytes.load(Ordering::Relaxed),
            wal_size_bytes: self.wal_size_bytes.load(Ordering::Relaxed),
            active_transactions: self.active_transactions.load(Ordering::Relaxed),
        }
    }
    
    pub fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.writes.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
        self.transactions.store(0, Ordering::Relaxed);
        self.read_latency_sum.store(0, Ordering::Relaxed);
        self.write_latency_sum.store(0, Ordering::Relaxed);
        self.delete_latency_sum.store(0, Ordering::Relaxed);
        self.transaction_latency_sum.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.cache_evictions.store(0, Ordering::Relaxed);
        self.pages_read.store(0, Ordering::Relaxed);
        self.pages_written.store(0, Ordering::Relaxed);
        self.page_faults.store(0, Ordering::Relaxed);
        self.compactions.store(0, Ordering::Relaxed);
        self.compaction_bytes_read.store(0, Ordering::Relaxed);
        self.compaction_bytes_written.store(0, Ordering::Relaxed);
        self.read_errors.store(0, Ordering::Relaxed);
        self.write_errors.store(0, Ordering::Relaxed);
        self.transaction_aborts.store(0, Ordering::Relaxed);
        // Don't reset size metrics or active transactions
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub transactions: u64,
    pub avg_read_latency_us: u64,
    pub avg_write_latency_us: u64,
    pub avg_delete_latency_us: u64,
    pub avg_transaction_latency_us: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub cache_evictions: u64,
    pub pages_read: u64,
    pub pages_written: u64,
    pub page_faults: u64,
    pub compactions: u64,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,
    pub read_errors: u64,
    pub write_errors: u64,
    pub transaction_aborts: u64,
    pub database_size_bytes: u64,
    pub wal_size_bytes: u64,
    pub active_transactions: usize,
}

/// Component-specific metrics
#[derive(Debug)]
pub struct ComponentMetrics {
    pub name: String,
    pub custom_metrics: Arc<RwLock<HashMap<String, f64>>>,
    pub last_updated: Arc<RwLock<Instant>>,
}

impl ComponentMetrics {
    pub fn new(name: String) -> Self {
        Self {
            name,
            custom_metrics: Arc::new(RwLock::new(HashMap::new())),
            last_updated: Arc::new(RwLock::new(Instant::now())),
        }
    }
    
    pub fn update_metric(&self, key: &str, value: f64) {
        let mut metrics = self.custom_metrics.write();
        metrics.insert(key.to_string(), value);
        *self.last_updated.write() = Instant::now();
    }
    
    pub fn get_metrics(&self) -> HashMap<String, f64> {
        self.custom_metrics.read().clone()
    }
}
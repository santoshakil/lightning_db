use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Lock-free metrics collector using atomic operations and DashMap
pub struct LockFreeMetricsCollector {
    // Basic counters using atomics
    reads: AtomicU64,
    writes: AtomicU64,
    deletes: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    
    // Latency tracking using atomic nanoseconds
    total_read_ns: AtomicU64,
    total_write_ns: AtomicU64,
    total_delete_ns: AtomicU64,
    
    // Custom metrics using DashMap (lock-free concurrent hashmap)
    custom_metrics: Arc<DashMap<String, AtomicU64>>,
    
    // Component metrics
    component_metrics: Arc<DashMap<String, Arc<LockFreeComponentMetrics>>>,
    
    // Start time for rate calculations
    start_time: Instant,
}

impl LockFreeMetricsCollector {
    pub fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            total_read_ns: AtomicU64::new(0),
            total_write_ns: AtomicU64::new(0),
            total_delete_ns: AtomicU64::new(0),
            custom_metrics: Arc::new(DashMap::new()),
            component_metrics: Arc::new(DashMap::new()),
            start_time: Instant::now(),
        }
    }
    
    /// Record a read operation (lock-free)
    #[inline]
    pub fn record_read(&self, duration: Duration) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.total_read_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a write operation (lock-free)
    #[inline]
    pub fn record_write(&self, duration: Duration) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.total_write_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record a delete operation (lock-free)
    #[inline]
    pub fn record_delete(&self, duration: Duration) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.total_delete_ns.fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }
    
    /// Record cache hit (lock-free)
    #[inline]
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Record cache miss (lock-free)
    #[inline]
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Update custom metric (lock-free)
    pub fn update_custom_metric(&self, name: &str, value: u64) {
        self.custom_metrics
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .store(value, Ordering::Relaxed);
    }
    
    /// Increment custom metric (lock-free)
    pub fn increment_custom_metric(&self, name: &str, delta: u64) {
        self.custom_metrics
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }
    
    /// Register a component for metrics tracking
    pub fn register_component(&self, name: &str) -> Arc<LockFreeComponentMetrics> {
        let metrics = Arc::new(LockFreeComponentMetrics::new(name));
        self.component_metrics.insert(name.to_string(), metrics.clone());
        metrics
    }
    
    /// Get current metrics snapshot
    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        
        let reads = self.reads.load(Ordering::Relaxed);
        let writes = self.writes.load(Ordering::Relaxed);
        let deletes = self.deletes.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        
        let total_ops = reads + writes + deletes;
        let cache_total = cache_hits + cache_misses;
        
        MetricsSnapshot {
            reads,
            writes,
            deletes,
            total_ops,
            reads_per_sec: reads as f64 / elapsed_secs,
            writes_per_sec: writes as f64 / elapsed_secs,
            deletes_per_sec: deletes as f64 / elapsed_secs,
            ops_per_sec: total_ops as f64 / elapsed_secs,
            avg_read_latency_us: if reads > 0 {
                (self.total_read_ns.load(Ordering::Relaxed) / reads) as f64 / 1000.0
            } else {
                0.0
            },
            avg_write_latency_us: if writes > 0 {
                (self.total_write_ns.load(Ordering::Relaxed) / writes) as f64 / 1000.0
            } else {
                0.0
            },
            avg_delete_latency_us: if deletes > 0 {
                (self.total_delete_ns.load(Ordering::Relaxed) / deletes) as f64 / 1000.0
            } else {
                0.0
            },
            cache_hit_rate: if cache_total > 0 {
                cache_hits as f64 / cache_total as f64
            } else {
                0.0
            },
            uptime: elapsed,
        }
    }
}

/// Lock-free component-specific metrics
pub struct LockFreeComponentMetrics {
    operations: AtomicU64,
    bytes_processed: AtomicU64,
    errors: AtomicU64,
    custom_counters: DashMap<String, AtomicU64>,
}

impl LockFreeComponentMetrics {
    fn new(_name: &str) -> Self {
        Self {
            operations: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            custom_counters: DashMap::new(),
        }
    }
    
    #[inline]
    pub fn record_operation(&self) {
        self.operations.fetch_add(1, Ordering::Relaxed);
    }
    
    #[inline]
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }
    
    #[inline]
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn increment_counter(&self, name: &str, delta: u64) {
        self.custom_counters
            .entry(name.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(delta, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub reads: u64,
    pub writes: u64,
    pub deletes: u64,
    pub total_ops: u64,
    pub reads_per_sec: f64,
    pub writes_per_sec: f64,
    pub deletes_per_sec: f64,
    pub ops_per_sec: f64,
    pub avg_read_latency_us: f64,
    pub avg_write_latency_us: f64,
    pub avg_delete_latency_us: f64,
    pub cache_hit_rate: f64,
    pub uptime: Duration,
}

/// Scoped operation timer that automatically records duration
pub struct OperationTimer<'a> {
    collector: &'a LockFreeMetricsCollector,
    operation: OperationType,
    start: Instant,
}

#[derive(Debug, Clone, Copy)]
pub enum OperationType {
    Read,
    Write,
    Delete,
}

impl<'a> OperationTimer<'a> {
    pub fn new(collector: &'a LockFreeMetricsCollector, operation: OperationType) -> Self {
        Self {
            collector,
            operation,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for OperationTimer<'a> {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        match self.operation {
            OperationType::Read => self.collector.record_read(duration),
            OperationType::Write => self.collector.record_write(duration),
            OperationType::Delete => self.collector.record_delete(duration),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;
    
    #[test]
    fn test_lock_free_metrics() {
        let collector = Arc::new(LockFreeMetricsCollector::new());
        
        // Test concurrent updates
        let mut handles = vec![];
        
        for i in 0..4 {
            let collector_clone = Arc::clone(&collector);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    if i % 3 == 0 {
                        collector_clone.record_read(Duration::from_micros(100));
                    } else if i % 3 == 1 {
                        collector_clone.record_write(Duration::from_micros(200));
                    } else {
                        collector_clone.record_delete(Duration::from_micros(150));
                    }
                    
                    if i % 2 == 0 {
                        collector_clone.record_cache_hit();
                    } else {
                        collector_clone.record_cache_miss();
                    }
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let snapshot = collector.get_snapshot();
        assert_eq!(snapshot.total_ops, 4000);
        assert!(snapshot.cache_hit_rate > 0.0);
        assert!(snapshot.avg_read_latency_us > 0.0);
    }
    
    #[test]
    fn test_operation_timer() {
        let collector = LockFreeMetricsCollector::new();
        
        {
            let _timer = OperationTimer::new(&collector, OperationType::Write);
            std::thread::sleep(Duration::from_millis(10));
        } // Timer dropped, duration recorded
        
        let snapshot = collector.get_snapshot();
        assert_eq!(snapshot.writes, 1);
        assert!(snapshot.avg_write_latency_us >= 10_000.0); // At least 10ms
    }
}
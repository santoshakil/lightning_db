pub mod cache;
pub mod optimizations;
pub mod prefetch;
pub mod read_cache;
pub mod small_alloc;


// Performance metrics aggregation
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global performance metrics collector
pub struct GlobalPerformanceMetrics {
    total_operations: AtomicU64,
    total_cache_hits: AtomicU64,
    total_cache_misses: AtomicU64,
    total_batch_operations: AtomicU64,
    total_lock_free_operations: AtomicU64,
}

impl GlobalPerformanceMetrics {
    pub fn new() -> Self {
        Self {
            total_operations: AtomicU64::new(0),
            total_cache_hits: AtomicU64::new(0),
            total_cache_misses: AtomicU64::new(0),
            total_batch_operations: AtomicU64::new(0),
            total_lock_free_operations: AtomicU64::new(0),
        }
    }

    pub fn record_operation(&self) {
        self.total_operations.fetch_add(1, Ordering::Relaxed);
    }


    pub fn record_cache_hit(&self) {
        self.total_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.total_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_batch_operation(&self) {
        self.total_batch_operations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lock_free_operation(&self) {
        self.total_lock_free_operations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> PerformanceStats {
        PerformanceStats {
            total_operations: self.total_operations.load(Ordering::Relaxed),
            cache_hit_rate: {
                let hits = self.total_cache_hits.load(Ordering::Relaxed);
                let misses = self.total_cache_misses.load(Ordering::Relaxed);
                if hits + misses > 0 {
                    hits as f64 / (hits + misses) as f64
                } else {
                    0.0
                }
            },
            total_batch_operations: self.total_batch_operations.load(Ordering::Relaxed),
            total_lock_free_operations: self.total_lock_free_operations.load(Ordering::Relaxed),
        }
    }
}

impl Default for GlobalPerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregated performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub total_operations: u64,
    pub cache_hit_rate: f64,
    pub total_batch_operations: u64,
    pub total_lock_free_operations: u64,
}

// Global metrics instance
static GLOBAL_METRICS: std::sync::OnceLock<Arc<GlobalPerformanceMetrics>> = std::sync::OnceLock::new();

/// Get global performance metrics instance
pub fn get_global_metrics() -> &'static Arc<GlobalPerformanceMetrics> {
    GLOBAL_METRICS.get_or_init(|| Arc::new(GlobalPerformanceMetrics::new()))
}
use crate::cache::CacheStats;
use crate::metrics::METRICS;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RealtimeStats {
    // Operation counters
    pub total_ops: u64,
    pub get_ops: u64,
    pub put_ops: u64,
    pub delete_ops: u64,
    pub range_ops: u64,
    
    // Performance metrics
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub p95_latency_us: f64,
    pub throughput_ops_sec: f64,
    
    // Size metrics
    pub data_size_bytes: u64,
    pub index_size_bytes: u64,
    pub wal_size_bytes: u64,
    pub cache_size_bytes: u64,
    
    // Cache stats
    pub cache_hit_rate: f64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    
    // Transaction stats
    pub active_transactions: u64,
    pub committed_transactions: u64,
    pub aborted_transactions: u64,
    
    // Resource usage
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub disk_io_mb_sec: f64,
    
    // Time window
    pub window_start: Instant,
    pub window_duration: Duration,
}

#[derive(Debug, Clone)]
struct LatencySample {
    timestamp: Instant,
    latency_us: u64,
    _operation: String,
}

pub struct RealtimeStatsCollector {
    enabled: bool,
    current_stats: Arc<RwLock<RealtimeStats>>,
    latency_samples: Arc<RwLock<VecDeque<LatencySample>>>,
    sample_window: Duration,
    update_interval: Duration,
    
    // Atomic counters for high-frequency updates
    get_counter: Arc<AtomicU64>,
    put_counter: Arc<AtomicU64>,
    delete_counter: Arc<AtomicU64>,
    range_counter: Arc<AtomicU64>,
}

impl Default for RealtimeStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl RealtimeStatsCollector {
    pub fn new() -> Self {
        Self {
            enabled: true,
            current_stats: Arc::new(RwLock::new(RealtimeStats {
                total_ops: 0,
                get_ops: 0,
                put_ops: 0,
                delete_ops: 0,
                range_ops: 0,
                avg_latency_us: 0.0,
                p99_latency_us: 0.0,
                p95_latency_us: 0.0,
                throughput_ops_sec: 0.0,
                data_size_bytes: 0,
                index_size_bytes: 0,
                wal_size_bytes: 0,
                cache_size_bytes: 0,
                cache_hit_rate: 0.0,
                cache_hits: 0,
                cache_misses: 0,
                cache_evictions: 0,
                active_transactions: 0,
                committed_transactions: 0,
                aborted_transactions: 0,
                memory_usage_mb: 0.0,
                cpu_usage_percent: 0.0,
                disk_io_mb_sec: 0.0,
                window_start: Instant::now(),
                window_duration: Duration::from_secs(60),
            })),
            latency_samples: Arc::new(RwLock::new(VecDeque::new())),
            sample_window: Duration::from_secs(60),
            update_interval: Duration::from_secs(1),
            get_counter: Arc::new(AtomicU64::new(0)),
            put_counter: Arc::new(AtomicU64::new(0)),
            delete_counter: Arc::new(AtomicU64::new(0)),
            range_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn start(&self) {
        if !self.enabled {
            return;
        }

        let current_stats = self.current_stats.clone();
        let latency_samples = self.latency_samples.clone();
        let update_interval = self.update_interval;
        let sample_window = self.sample_window;
        
        let get_counter = self.get_counter.clone();
        let put_counter = self.put_counter.clone();
        let delete_counter = self.delete_counter.clone();
        let range_counter = self.range_counter.clone();

        thread::spawn(move || {
            loop {
                thread::sleep(update_interval);
                
                // Update counters
                let get_ops = get_counter.swap(0, Ordering::AcqRel);
                let put_ops = put_counter.swap(0, Ordering::AcqRel);
                let delete_ops = delete_counter.swap(0, Ordering::AcqRel);
                let range_ops = range_counter.swap(0, Ordering::AcqRel);
                let total_ops = get_ops + put_ops + delete_ops + range_ops;
                
                // Calculate latency percentiles
                let mut samples = latency_samples.write();
                let now = Instant::now();
                
                // Remove old samples
                while let Some(front) = samples.front() {
                    if now.duration_since(front.timestamp) > sample_window {
                        samples.pop_front();
                    } else {
                        break;
                    }
                }
                
                // Calculate percentiles
                let (avg_latency, p95_latency, p99_latency) = if !samples.is_empty() {
                    let mut latencies: Vec<_> = samples.iter().map(|s| s.latency_us).collect();
                    latencies.sort_unstable();
                    
                    let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
                    let p95_idx = (latencies.len() as f64 * 0.95) as usize;
                    let p99_idx = (latencies.len() as f64 * 0.99) as usize;
                    
                    (avg, latencies[p95_idx] as f64, latencies[p99_idx] as f64)
                } else {
                    (0.0, 0.0, 0.0)
                };
                
                // Update stats
                let mut stats = current_stats.write();
                stats.get_ops += get_ops;
                stats.put_ops += put_ops;
                stats.delete_ops += delete_ops;
                stats.range_ops += range_ops;
                stats.total_ops += total_ops;
                
                stats.avg_latency_us = avg_latency;
                stats.p95_latency_us = p95_latency;
                stats.p99_latency_us = p99_latency;
                stats.throughput_ops_sec = total_ops as f64 / update_interval.as_secs_f64();
                
                // Update metrics
                METRICS.read().update_database_size(
                    stats.data_size_bytes,
                    stats.index_size_bytes,
                    stats.wal_size_bytes,
                );
            }
        });
    }

    pub fn record_operation(&self, operation: &str, latency: Duration) {
        if !self.enabled {
            return;
        }

        // Update counters
        match operation {
            "get" => self.get_counter.fetch_add(1, Ordering::AcqRel),
            "put" => self.put_counter.fetch_add(1, Ordering::AcqRel),
            "delete" => self.delete_counter.fetch_add(1, Ordering::AcqRel),
            "range" => self.range_counter.fetch_add(1, Ordering::AcqRel),
            _ => 0,
        };

        // Record latency sample
        let sample = LatencySample {
            timestamp: Instant::now(),
            latency_us: latency.as_micros() as u64,
            _operation: operation.to_string(),
        };

        self.latency_samples.write().push_back(sample);
    }

    pub fn update_cache_stats(&self, stats: &CacheStats) {
        if !self.enabled {
            return;
        }

        let hits = stats.hits.load(Ordering::Acquire) as u64;
        let misses = stats.misses.load(Ordering::Acquire) as u64;
        let evictions = stats.evictions.load(Ordering::Acquire) as u64;

        let mut current = self.current_stats.write();
        current.cache_hits = hits;
        current.cache_misses = misses;
        current.cache_evictions = evictions;
        current.cache_hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };
    }

    pub fn update_transaction_stats(&self, active: u64, committed: u64, aborted: u64) {
        if !self.enabled {
            return;
        }

        let mut current = self.current_stats.write();
        current.active_transactions = active;
        current.committed_transactions = committed;
        current.aborted_transactions = aborted;
    }

    pub fn update_size_metrics(&self, data_size: u64, index_size: u64, wal_size: u64) {
        if !self.enabled {
            return;
        }

        let mut current = self.current_stats.write();
        current.data_size_bytes = data_size;
        current.index_size_bytes = index_size;
        current.wal_size_bytes = wal_size;
    }

    pub fn update_resource_usage(&self, memory_mb: f64, cpu_percent: f64, disk_io_mb_sec: f64) {
        if !self.enabled {
            return;
        }

        let mut current = self.current_stats.write();
        current.memory_usage_mb = memory_mb;
        current.cpu_usage_percent = cpu_percent;
        current.disk_io_mb_sec = disk_io_mb_sec;
    }

    pub fn get_current_stats(&self) -> RealtimeStats {
        self.current_stats.read().clone()
    }

    pub fn reset_window(&self) {
        let mut stats = self.current_stats.write();
        stats.window_start = Instant::now();
        stats.total_ops = 0;
        stats.get_ops = 0;
        stats.put_ops = 0;
        stats.delete_ops = 0;
        stats.range_ops = 0;
        
        self.latency_samples.write().clear();
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

impl RealtimeStats {
    pub fn print_summary(&self) {
        println!("\n=== Real-time Database Statistics ===");
        println!("Window: {:?} (started {:?} ago)", 
                 self.window_duration, 
                 self.window_start.elapsed());
        
        println!("\nThroughput:");
        println!("  Total: {:.0} ops/sec", self.throughput_ops_sec);
        println!("  Operations: {} total ({} gets, {} puts, {} deletes, {} ranges)",
                 self.total_ops, self.get_ops, self.put_ops, self.delete_ops, self.range_ops);
        
        println!("\nLatency:");
        println!("  Average: {:.1} μs", self.avg_latency_us);
        println!("  P95: {:.1} μs", self.p95_latency_us);
        println!("  P99: {:.1} μs", self.p99_latency_us);
        
        println!("\nCache Performance:");
        println!("  Hit Rate: {:.1}%", self.cache_hit_rate * 100.0);
        println!("  Hits: {} | Misses: {} | Evictions: {}", 
                 self.cache_hits, self.cache_misses, self.cache_evictions);
        
        println!("\nDatabase Size:");
        println!("  Data: {} MB", self.data_size_bytes / 1024 / 1024);
        println!("  Index: {} MB", self.index_size_bytes / 1024 / 1024);
        println!("  WAL: {} MB", self.wal_size_bytes / 1024 / 1024);
        println!("  Cache: {} MB", self.cache_size_bytes / 1024 / 1024);
        
        println!("\nTransactions:");
        println!("  Active: {} | Committed: {} | Aborted: {}",
                 self.active_transactions, self.committed_transactions, self.aborted_transactions);
        
        println!("\nResource Usage:");
        println!("  Memory: {:.1} MB", self.memory_usage_mb);
        println!("  CPU: {:.1}%", self.cpu_usage_percent);
        println!("  Disk I/O: {:.1} MB/s", self.disk_io_mb_sec);
    }
}

// Global realtime stats collector
lazy_static::lazy_static! {
    pub static ref REALTIME_STATS: Arc<RwLock<RealtimeStatsCollector>> = 
        Arc::new(RwLock::new(RealtimeStatsCollector::new()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_collection() {
        let collector = RealtimeStatsCollector::new();
        
        // Record some operations
        collector.record_operation("get", Duration::from_micros(100));
        collector.record_operation("put", Duration::from_micros(200));
        collector.record_operation("get", Duration::from_micros(150));
        
        // Wait a bit for counters to be read
        thread::sleep(Duration::from_millis(10));
        
        // Check that operations were recorded
        assert!(collector.get_counter.load(Ordering::Relaxed) > 0 || 
                collector.put_counter.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn test_cache_stats_update() {
        use std::sync::atomic::AtomicUsize;
        
        let collector = RealtimeStatsCollector::new();
        
        let cache_stats = CacheStats {
            hits: AtomicUsize::new(1000),
            misses: AtomicUsize::new(200),
            evictions: AtomicUsize::new(50),
            prefetch_hits: AtomicUsize::new(10),
        };
        
        collector.update_cache_stats(&cache_stats);
        
        let stats = collector.get_current_stats();
        assert_eq!(stats.cache_hits, 1000);
        assert_eq!(stats.cache_misses, 200);
        assert_eq!(stats.cache_evictions, 50);
        assert!((stats.cache_hit_rate - 0.833).abs() < 0.001);
    }

    #[test]
    fn test_window_reset() {
        let collector = RealtimeStatsCollector::new();
        
        // Record operations
        collector.record_operation("get", Duration::from_micros(100));
        collector.record_operation("put", Duration::from_micros(200));
        
        // Reset window
        collector.reset_window();
        
        let stats = collector.get_current_stats();
        assert_eq!(stats.total_ops, 0);
        assert_eq!(stats.get_ops, 0);
        assert_eq!(stats.put_ops, 0);
    }
}
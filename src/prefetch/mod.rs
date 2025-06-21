use crate::error::Result;
use crate::storage::Page;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Read-ahead prefetching system for sequential scans
pub struct PrefetchManager {
    // Configuration
    config: PrefetchConfig,

    // Prefetch queue and workers
    prefetch_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,
    worker_threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    running: Arc<AtomicBool>,

    // Access pattern tracking
    access_patterns: Arc<RwLock<HashMap<String, AccessPattern>>>,
    sequential_threshold: usize,

    // Statistics
    total_prefetches: Arc<AtomicU64>,
    successful_prefetches: Arc<AtomicU64>,
    cache_hits_from_prefetch: Arc<AtomicU64>,
    prefetch_latency_total: Arc<AtomicU64>,

    // Page cache integration
    page_cache: Option<Arc<dyn PageCache>>,
}

#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    pub enabled: bool,
    pub max_prefetch_distance: usize, // How far ahead to prefetch
    pub prefetch_batch_size: usize,   // Pages to prefetch in one batch
    pub worker_threads: usize,        // Number of prefetch worker threads
    pub sequential_threshold: usize,  // Min sequential accesses to trigger prefetch
    pub max_queue_size: usize,        // Max pending prefetch requests
    pub adaptive_distance: bool,      // Adjust prefetch distance based on hit rate
    pub prefetch_on_miss_only: bool,  // Only prefetch on cache misses
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_prefetch_distance: 8,
            prefetch_batch_size: 4,
            worker_threads: 2,
            sequential_threshold: 3,
            max_queue_size: 100,
            adaptive_distance: true,
            prefetch_on_miss_only: false,
        }
    }
}

#[derive(Debug, Clone)]
struct PrefetchRequest {
    table_id: String,
    start_page: u32,
    page_count: usize,
    priority: PrefetchPriority,
    created_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PrefetchPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Clone)]
struct AccessPattern {
    table_id: String,
    recent_accesses: VecDeque<u32>, // Recent page accesses
    last_access_time: Instant,
    sequential_count: usize,     // Count of sequential accesses
    stride_pattern: Option<i32>, // Detected stride pattern
    hit_rate: f64,               // Hit rate for prefetched pages
    total_prefetches: usize,
    successful_prefetches: usize,
}

impl AccessPattern {
    fn new(table_id: String) -> Self {
        Self {
            table_id,
            recent_accesses: VecDeque::with_capacity(16),
            last_access_time: Instant::now(),
            sequential_count: 0,
            stride_pattern: None,
            hit_rate: 0.0,
            total_prefetches: 0,
            successful_prefetches: 0,
        }
    }

    fn table_id(&self) -> &str {
        &self.table_id
    }

    fn record_access(&mut self, page_id: u32) {
        self.recent_accesses.push_back(page_id);
        if self.recent_accesses.len() > 16 {
            self.recent_accesses.pop_front();
        }
        self.last_access_time = Instant::now();
        self.analyze_pattern();
    }

    fn analyze_pattern(&mut self) {
        if self.recent_accesses.len() < 2 {
            return;
        }

        let mut sequential = 0;
        let mut strides = Vec::new();

        for i in 1..self.recent_accesses.len() {
            let prev = self.recent_accesses[i - 1];
            let curr = self.recent_accesses[i];
            let stride = curr as i32 - prev as i32;

            strides.push(stride);

            if stride == 1 {
                sequential += 1;
            }
        }

        self.sequential_count = sequential;

        // Detect stride pattern
        if strides.len() >= 3 {
            let common_stride = strides
                .iter()
                .fold(HashMap::new(), |mut acc, &stride| {
                    *acc.entry(stride).or_insert(0) += 1;
                    acc
                })
                .into_iter()
                .max_by_key(|(_, count)| *count)
                .map(|(stride, _)| stride);

            if let Some(stride) = common_stride {
                if stride > 0 && stride <= 16 {
                    self.stride_pattern = Some(stride);
                }
            }
        }
    }

    fn should_prefetch(&self, threshold: usize) -> bool {
        self.sequential_count >= threshold
            || self.stride_pattern.is_some()
            || (self.hit_rate > 0.7 && self.total_prefetches > 10)
    }

    fn get_prefetch_distance(&self, max_distance: usize, adaptive: bool) -> usize {
        if !adaptive {
            return max_distance;
        }

        // Adapt based on hit rate
        if self.hit_rate > 0.8 {
            (max_distance * 2).min(16)
        } else if self.hit_rate > 0.6 {
            max_distance
        } else if self.hit_rate > 0.3 {
            max_distance / 2
        } else {
            1
        }
    }

    fn record_prefetch_result(&mut self, successful: bool) {
        self.total_prefetches += 1;
        if successful {
            self.successful_prefetches += 1;
        }

        if self.total_prefetches > 0 {
            self.hit_rate = self.successful_prefetches as f64 / self.total_prefetches as f64;
        }
    }
}

pub trait PageCache: Send + Sync {
    fn get_page(&self, table_id: &str, page_id: u32) -> Result<Option<Arc<Page>>>;
    fn prefetch_page(&self, table_id: &str, page_id: u32) -> Result<()>;
    fn is_page_cached(&self, table_id: &str, page_id: u32) -> bool;
}

impl PrefetchManager {
    pub fn new(config: PrefetchConfig) -> Self {
        Self {
            sequential_threshold: config.sequential_threshold,
            config,
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
            worker_threads: Arc::new(Mutex::new(Vec::new())),
            running: Arc::new(AtomicBool::new(false)),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
            total_prefetches: Arc::new(AtomicU64::new(0)),
            successful_prefetches: Arc::new(AtomicU64::new(0)),
            cache_hits_from_prefetch: Arc::new(AtomicU64::new(0)),
            prefetch_latency_total: Arc::new(AtomicU64::new(0)),
            page_cache: None,
        }
    }

    pub fn set_page_cache(&mut self, cache: Arc<dyn PageCache>) {
        self.page_cache = Some(cache);
    }

    pub fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        info!(
            "Starting prefetch manager with {} workers",
            self.config.worker_threads
        );

        let mut threads = self.worker_threads.lock();

        // Start worker threads
        for i in 0..self.config.worker_threads {
            let handle = self.start_worker_thread(i);
            threads.push(handle);
        }

        // Start cleanup thread
        let cleanup_handle = self.start_cleanup_thread();
        threads.push(cleanup_handle);

        Ok(())
    }

    pub fn stop(&self) {
        if !self.running.swap(false, Ordering::SeqCst) {
            return; // Already stopped
        }

        info!("Stopping prefetch manager");

        // Wait for all threads to finish
        let mut threads = self.worker_threads.lock();
        while let Some(handle) = threads.pop() {
            if let Err(e) = handle.join() {
                error!("Error joining prefetch thread: {:?}", e);
            }
        }
    }

    pub fn record_access(&self, table_id: &str, page_id: u32, was_cache_hit: bool) {
        if !self.config.enabled {
            return;
        }

        // Update access pattern
        {
            let mut patterns = self.access_patterns.write();
            let pattern = patterns
                .entry(table_id.to_string())
                .or_insert_with(|| AccessPattern::new(table_id.to_string()));

            pattern.record_access(page_id);

            // Check if we should trigger prefetch
            if pattern.should_prefetch(self.sequential_threshold) && (!self.config.prefetch_on_miss_only || !was_cache_hit) {
                self.trigger_prefetch(table_id, page_id, pattern);
            }
        }
    }

    fn trigger_prefetch(&self, table_id: &str, current_page: u32, pattern: &AccessPattern) {
        // Verify pattern is for the correct table
        if pattern.table_id() != table_id {
            warn!(
                "Pattern table mismatch: expected {}, got {}",
                table_id,
                pattern.table_id()
            );
            return;
        }

        let distance = pattern.get_prefetch_distance(
            self.config.max_prefetch_distance,
            self.config.adaptive_distance,
        );

        let stride = pattern.stride_pattern.unwrap_or(1).max(1) as u32;
        let start_page = current_page + stride;
        let page_count = (distance / stride as usize)
            .max(1)
            .min(self.config.prefetch_batch_size);

        let request = PrefetchRequest {
            table_id: table_id.to_string(),
            start_page,
            page_count,
            priority: match (pattern.sequential_count, pattern.hit_rate) {
                (count, _) if count > self.sequential_threshold * 3 => PrefetchPriority::Critical,
                (count, _) if count > self.sequential_threshold * 2 => PrefetchPriority::High,
                (_, rate) if rate < 0.3 => PrefetchPriority::Low,
                _ => PrefetchPriority::Normal,
            },
            created_at: Instant::now(),
        };

        // Add to queue if not full
        {
            let mut queue = self.prefetch_queue.write();
            if queue.len() < self.config.max_queue_size {
                queue.push_back(request);

                // Sort by priority (highest first)
                let mut sorted_queue: Vec<_> = queue.drain(..).collect();
                sorted_queue.sort_by(|a, b| b.priority.cmp(&a.priority));
                queue.extend(sorted_queue);
            }
        }
    }

    fn start_worker_thread(&self, worker_id: usize) -> JoinHandle<()> {
        let running = Arc::clone(&self.running);
        let queue = Arc::clone(&self.prefetch_queue);
        let cache = self.page_cache.clone();
        let patterns = Arc::clone(&self.access_patterns);
        let total_prefetches = Arc::clone(&self.total_prefetches);
        let successful_prefetches = Arc::clone(&self.successful_prefetches);
        let latency_total = Arc::clone(&self.prefetch_latency_total);

        thread::spawn(move || {
            debug!("Prefetch worker {} started", worker_id);

            while running.load(Ordering::SeqCst) {
                let request = {
                    let mut queue = queue.write();
                    queue.pop_front()
                };

                if let Some(request) = request {
                    let start_time = Instant::now();

                    let queue_time = request.created_at.elapsed();
                    debug!(
                        "Worker {} prefetching pages {}..{} for table {} (queued for {:?})",
                        worker_id,
                        request.start_page,
                        request.start_page + request.page_count as u32,
                        request.table_id,
                        queue_time
                    );

                    let mut successful = 0;

                    if let Some(ref cache) = cache {
                        for i in 0..request.page_count {
                            let page_id = request.start_page + i as u32;

                            // Skip if already cached
                            if cache.is_page_cached(&request.table_id, page_id) {
                                continue;
                            }

                            match cache.prefetch_page(&request.table_id, page_id) {
                                Ok(()) => {
                                    successful += 1;
                                    total_prefetches.fetch_add(1, Ordering::Relaxed);
                                    successful_prefetches.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    debug!("Prefetch failed for page {}: {}", page_id, e);
                                    total_prefetches.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }

                    let duration = start_time.elapsed();
                    latency_total.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);

                    // Update pattern statistics
                    {
                        let mut patterns_guard = patterns.write();
                        if let Some(pattern) = patterns_guard.get_mut(&request.table_id) {
                            pattern.record_prefetch_result(successful > 0);
                        }
                    }

                    debug!(
                        "Worker {} completed prefetch: {}/{} pages successful in {:?}",
                        worker_id, successful, request.page_count, duration
                    );
                } else {
                    // No requests available, sleep briefly
                    thread::sleep(Duration::from_millis(10));
                }
            }

            debug!("Prefetch worker {} stopped", worker_id);
        })
    }

    fn start_cleanup_thread(&self) -> JoinHandle<()> {
        let running = Arc::clone(&self.running);
        let patterns = Arc::clone(&self.access_patterns);

        thread::spawn(move || {
            debug!("Prefetch cleanup thread started");

            let mut last_cleanup = Instant::now();
            
            while running.load(Ordering::SeqCst) {
                // Sleep in smaller increments to be more responsive to shutdown
                thread::sleep(Duration::from_millis(100));
                
                // Only do cleanup every 30 seconds
                if last_cleanup.elapsed() >= Duration::from_secs(30) {
                    // Clean up old access patterns
                    {
                        let mut patterns_guard = patterns.write();
                        let cutoff = Instant::now() - Duration::from_secs(300); // 5 minutes

                        patterns_guard.retain(|_, pattern| pattern.last_access_time > cutoff);
                    }
                    last_cleanup = Instant::now();
                }
            }

            debug!("Prefetch cleanup thread stopped");
        })
    }

    pub fn get_statistics(&self) -> PrefetchStatistics {
        let patterns = self.access_patterns.read();

        PrefetchStatistics {
            total_prefetches: self.total_prefetches.load(Ordering::Relaxed),
            successful_prefetches: self.successful_prefetches.load(Ordering::Relaxed),
            cache_hits_from_prefetch: self.cache_hits_from_prefetch.load(Ordering::Relaxed),
            average_latency_micros: {
                let total_latency = self.prefetch_latency_total.load(Ordering::Relaxed);
                let total_requests = self.total_prefetches.load(Ordering::Relaxed);
                if total_requests > 0 {
                    total_latency / total_requests
                } else {
                    0
                }
            },
            active_patterns: patterns.len(),
            queue_size: self.prefetch_queue.read().len(),
            hit_rate: {
                let total = self.total_prefetches.load(Ordering::Relaxed);
                let successful = self.successful_prefetches.load(Ordering::Relaxed);
                if total > 0 {
                    successful as f64 / total as f64
                } else {
                    0.0
                }
            },
        }
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn record_cache_hit_from_prefetch(&self) {
        self.cache_hits_from_prefetch
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn clear_patterns(&self) {
        self.access_patterns.write().clear();
    }

    pub fn get_pattern_count(&self) -> usize {
        self.access_patterns.read().len()
    }
}

#[derive(Debug, Clone)]
pub struct PrefetchStatistics {
    pub total_prefetches: u64,
    pub successful_prefetches: u64,
    pub cache_hits_from_prefetch: u64,
    pub average_latency_micros: u64,
    pub active_patterns: usize,
    pub queue_size: usize,
    pub hit_rate: f64,
}

impl Drop for PrefetchManager {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockPageCache {
        cached_pages: Arc<RwLock<std::collections::HashSet<(String, u32)>>>,
    }

    impl MockPageCache {
        fn new() -> Self {
            Self {
                cached_pages: Arc::new(RwLock::new(std::collections::HashSet::new())),
            }
        }
    }

    impl PageCache for MockPageCache {
        fn get_page(&self, table_id: &str, page_id: u32) -> Result<Option<Arc<Page>>> {
            if self
                .cached_pages
                .read()
                .contains(&(table_id.to_string(), page_id))
            {
                // Return a dummy page
                Ok(Some(Arc::new(Page::new(page_id))))
            } else {
                Ok(None)
            }
        }

        fn prefetch_page(&self, table_id: &str, page_id: u32) -> Result<()> {
            self.cached_pages
                .write()
                .insert((table_id.to_string(), page_id));
            Ok(())
        }

        fn is_page_cached(&self, table_id: &str, page_id: u32) -> bool {
            self.cached_pages
                .read()
                .contains(&(table_id.to_string(), page_id))
        }
    }

    #[test]
    fn test_access_pattern_detection() {
        let mut pattern = AccessPattern::new("test_table".to_string());

        // Simulate sequential access
        for i in 1..=10 {
            pattern.record_access(i);
        }

        assert!(pattern.should_prefetch(3));
        assert!(pattern.sequential_count >= 3);
    }

    #[test]
    fn test_stride_pattern_detection() {
        let mut pattern = AccessPattern::new("test_table".to_string());

        // Simulate stride-2 access pattern
        for i in (1..=20).step_by(2) {
            pattern.record_access(i);
        }

        assert_eq!(pattern.stride_pattern, Some(2));
        assert!(pattern.should_prefetch(1));
    }

    #[test]
    fn test_prefetch_manager_lifecycle() {
        let config = PrefetchConfig {
            worker_threads: 1,
            ..Default::default()
        };

        let mut manager = PrefetchManager::new(config);
        let cache = Arc::new(MockPageCache::new());
        manager.set_page_cache(cache.clone());

        assert!(manager.start().is_ok());
        assert!(manager.is_running());

        // Simulate sequential access to trigger prefetch
        for i in 1..=5 {
            manager.record_access("test_table", i, false);
        }

        // Give workers time to process
        thread::sleep(Duration::from_millis(100));

        let stats = manager.get_statistics();
        assert!(stats.total_prefetches > 0);

        manager.stop();
        assert!(!manager.is_running());
    }

    #[test]
    fn test_adaptive_prefetch_distance() {
        let mut pattern = AccessPattern::new("test_table".to_string());

        // Simulate high hit rate
        pattern.hit_rate = 0.9;
        pattern.total_prefetches = 20;

        let distance = pattern.get_prefetch_distance(4, true);
        assert!(distance > 4); // Should increase distance for high hit rate

        // Simulate low hit rate
        pattern.hit_rate = 0.2;
        let distance = pattern.get_prefetch_distance(4, true);
        assert!(distance < 4); // Should decrease distance for low hit rate
    }
}

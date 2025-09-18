//! Cache Prewarming and Predictive Prefetching System
//!
//! Implements intelligent cache warming strategies and predictive prefetching
//! to minimize cache misses and optimize database performance by proactively
//! loading data that is likely to be accessed.

use crate::core::error::{Error, Result};
use crate::performance::cache::adaptive_sizing::{CacheAllocation, WorkloadPattern, WorkloadType};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

/// Cache prewarming configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrewarmingConfig {
    /// Enable cache prewarming
    pub enabled: bool,
    /// Prewarming rate (pages per second)
    pub warming_rate: usize,
    /// Maximum prewarming time
    pub max_warming_duration: Duration,
    /// Prewarming priority levels
    pub priority_levels: usize,
    /// Enable predictive prefetching
    pub enable_predictive_prefetch: bool,
    /// Prefetch lookahead distance
    pub prefetch_distance: usize,
    /// Maximum prefetch queue size
    pub max_prefetch_queue: usize,
    /// Pattern learning window size
    pub pattern_learning_window: usize,
    /// Confidence threshold for predictions
    pub prediction_confidence_threshold: f64,
}

impl Default for PrewarmingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            warming_rate: 1000,                             // 1000 pages/sec
            max_warming_duration: Duration::from_secs(300), // 5 minutes
            priority_levels: 4,                             // Hot, Warm, Normal, Low
            enable_predictive_prefetch: true,
            prefetch_distance: 32,                // Look ahead 32 pages
            max_prefetch_queue: 1024,             // Max 1024 pending prefetches
            pattern_learning_window: 10000,       // Learn from 10k accesses
            prediction_confidence_threshold: 0.7, // 70% confidence required
        }
    }
}

/// Prewarming priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WarmingPriority {
    Critical = 0, // Hot working set
    High = 1,     // Frequently accessed
    Normal = 2,   // Regular access
    Low = 3,      // Background warming
}

/// Access pattern for predictive prefetching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPattern {
    pub pattern_type: PatternType,
    pub confidence: f64,
    pub next_pages: Vec<u64>,
    pub temporal_distance: Duration,
    pub access_frequency: f64,
}

/// Types of access patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PatternType {
    Sequential, // Sequential page access
    Strided,    // Fixed stride access
    Clustered,  // Clustered around certain pages
    Random,     // Random access pattern
    Temporal,   // Time-based pattern
    Hotspot,    // Hotspot access pattern
}

/// Prefetch request
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    pub page_id: u64,
    pub priority: WarmingPriority,
    pub predicted_access_time: Instant,
    pub confidence: f64,
    pub pattern_type: PatternType,
    pub requester: String,
}

/// Cache warming statistics
#[derive(Debug, Clone, Default)]
pub struct WarmingStats {
    pub total_pages_warmed: u64,
    pub warming_hit_rate: f64,
    pub prefetch_accuracy: f64,
    pub pattern_detection_accuracy: f64,
    pub warming_time_total: Duration,
    pub prefetch_queue_size: usize,
    pub active_patterns: usize,
}

/// Access history entry
#[derive(Debug, Clone)]
struct AccessHistoryEntry {
    page_id: u64,
    timestamp: Instant,
    _access_type: AccessType,
    _context: AccessContext,
}

/// Type of access operation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccessType {
    Read,
    Write,
    Scan,
    Index,
}

/// Context information for access
#[derive(Debug, Clone)]
struct AccessContext {
    _workload_type: WorkloadType,
    _transaction_id: Option<u64>,
    _query_type: QueryType,
    _user_session: Option<String>,
}

/// Query type classification
#[derive(Debug, Clone, Copy, PartialEq)]
enum QueryType {
    PointLookup,
    RangeScan,
    IndexScan,
    FullTableScan,
    _Aggregation,
    _Join,
    Update,
    Insert,
    _Delete,
}

/// Pattern detector for identifying access patterns
struct PatternDetector {
    access_history: VecDeque<AccessHistoryEntry>,
    detected_patterns: HashMap<PatternType, Vec<AccessPattern>>,
    pattern_confidence: HashMap<PatternType, f64>,
    learning_window_size: usize,
    min_pattern_length: usize,
}

impl PatternDetector {
    fn new(learning_window_size: usize) -> Self {
        Self {
            access_history: VecDeque::new(),
            detected_patterns: HashMap::new(),
            pattern_confidence: HashMap::new(),
            learning_window_size,
            min_pattern_length: 3,
        }
    }

    fn record_access(&mut self, entry: AccessHistoryEntry) {
        self.access_history.push_back(entry);

        // Maintain window size
        while self.access_history.len() > self.learning_window_size {
            self.access_history.pop_front();
        }

        // Analyze patterns periodically
        if self.access_history.len() % 100 == 0 {
            self.analyze_patterns();
        }
    }

    fn analyze_patterns(&mut self) {
        if self.access_history.len() < self.min_pattern_length {
            return;
        }

        // Detect different types of patterns
        self.detect_sequential_patterns();
        self.detect_strided_patterns();
        self.detect_clustered_patterns();
        self.detect_temporal_patterns();
        self.detect_hotspot_patterns();
    }

    fn detect_sequential_patterns(&mut self) {
        let mut sequential_runs = Vec::new();
        let mut current_run = Vec::new();

        for i in 0..self.access_history.len().saturating_sub(1) {
            let page_diff =
                self.access_history[i + 1].page_id as i64 - self.access_history[i].page_id as i64;

            if page_diff == 1 {
                if current_run.is_empty() {
                    current_run.push(self.access_history[i].page_id);
                }
                current_run.push(self.access_history[i + 1].page_id);
            } else {
                if current_run.len() >= self.min_pattern_length {
                    sequential_runs.push(current_run.clone());
                }
                current_run.clear();
            }
        }

        // Finalize last run
        if current_run.len() >= self.min_pattern_length {
            sequential_runs.push(current_run);
        }

        // Create patterns from sequential runs
        let mut patterns = Vec::new();
        for run in &sequential_runs {
            if let (Some(&_first), Some(&last)) = (run.first(), run.last()) {
                let next_pages: Vec<u64> = (last + 1..=last + 8).collect();
                let confidence = (run.len() as f64 / 10.0).min(1.0);

                patterns.push(AccessPattern {
                    pattern_type: PatternType::Sequential,
                    confidence,
                    next_pages,
                    temporal_distance: Duration::from_millis(100),
                    access_frequency: run.len() as f64,
                });
            }
        }

        self.detected_patterns
            .insert(PatternType::Sequential, patterns);

        // Update confidence
        let total_sequential_pages: usize = sequential_runs.iter().map(|r| r.len()).sum();
        let confidence = total_sequential_pages as f64 / self.access_history.len() as f64;
        self.pattern_confidence
            .insert(PatternType::Sequential, confidence);
    }

    fn detect_strided_patterns(&mut self) {
        let mut stride_patterns = HashMap::<i64, Vec<u64>>::new();

        for i in 0..self.access_history.len().saturating_sub(2) {
            let stride1 =
                self.access_history[i + 1].page_id as i64 - self.access_history[i].page_id as i64;
            let stride2 = self.access_history[i + 2].page_id as i64
                - self.access_history[i + 1].page_id as i64;

            if stride1 == stride2 && stride1 > 1 {
                stride_patterns
                    .entry(stride1)
                    .or_default()
                    .extend_from_slice(&[
                        self.access_history[i].page_id,
                        self.access_history[i + 1].page_id,
                        self.access_history[i + 2].page_id,
                    ]);
            }
        }

        let mut patterns = Vec::new();
        for (stride, pages) in &stride_patterns {
            if pages.len() >= self.min_pattern_length {
                let last_page = pages.last().copied().unwrap_or(0);
                let next_pages: Vec<u64> = (1..=5)
                    .map(|i| (last_page as i64 + stride * i) as u64)
                    .filter(|&p| p > 0)
                    .collect();

                let confidence = (pages.len() as f64 / 15.0).min(1.0);

                patterns.push(AccessPattern {
                    pattern_type: PatternType::Strided,
                    confidence,
                    next_pages,
                    temporal_distance: Duration::from_millis(200),
                    access_frequency: pages.len() as f64,
                });
            }
        }

        self.detected_patterns
            .insert(PatternType::Strided, patterns);

        // Update confidence
        let total_strided_pages: usize = stride_patterns.values().map(|p| p.len()).sum();
        let confidence = total_strided_pages as f64 / self.access_history.len() as f64;
        self.pattern_confidence
            .insert(PatternType::Strided, confidence);
    }

    fn detect_clustered_patterns(&mut self) {
        // Group pages by proximity
        let mut clusters = HashMap::<u64, Vec<u64>>::new();
        const CLUSTER_RADIUS: u64 = 16;

        for entry in &self.access_history {
            let cluster_center = (entry.page_id / CLUSTER_RADIUS) * CLUSTER_RADIUS;
            clusters
                .entry(cluster_center)
                .or_default()
                .push(entry.page_id);
        }

        let mut patterns = Vec::new();
        for (center, pages) in &clusters {
            if pages.len() >= 5 {
                // Sort pages and find gaps
                let mut sorted_pages = pages.clone();
                sorted_pages.sort_unstable();
                sorted_pages.dedup();

                // Predict next pages in cluster
                let min_page = sorted_pages.first().copied().unwrap_or(0);
                let max_page = sorted_pages.last().copied().unwrap_or(min_page);
                let next_pages: Vec<u64> = (max_page + 1..=max_page + CLUSTER_RADIUS)
                    .filter(|&p| p < center + CLUSTER_RADIUS)
                    .collect();

                let density = sorted_pages.len() as f64 / CLUSTER_RADIUS as f64;
                let confidence = (density * 0.8).min(1.0);

                patterns.push(AccessPattern {
                    pattern_type: PatternType::Clustered,
                    confidence,
                    next_pages,
                    temporal_distance: Duration::from_millis(150),
                    access_frequency: sorted_pages.len() as f64,
                });
            }
        }

        self.detected_patterns
            .insert(PatternType::Clustered, patterns);

        // Update confidence
        let total_clustered_pages: usize = clusters.values().map(|p| p.len()).sum();
        let confidence = total_clustered_pages as f64 / self.access_history.len() as f64;
        self.pattern_confidence
            .insert(PatternType::Clustered, confidence);
    }

    fn detect_temporal_patterns(&mut self) {
        // Group accesses by time intervals
        let mut time_buckets = BTreeMap::<u64, Vec<u64>>::new();
        const TIME_BUCKET_SIZE_MS: u64 = 1000; // 1 second buckets

        for entry in &self.access_history {
            let bucket = entry.timestamp.elapsed().as_millis() as u64 / TIME_BUCKET_SIZE_MS;
            time_buckets
                .entry(bucket)
                .or_default()
                .push(entry.page_id);
        }

        let mut patterns = Vec::new();

        // Find repeating temporal patterns
        let buckets: Vec<_> = time_buckets.values().collect();
        for i in 0..buckets.len().saturating_sub(2) {
            if buckets[i] == buckets[i + 2] && !buckets[i].is_empty() {
                // Found repeating pattern
                let next_pages = buckets[i].clone();
                let confidence = 0.6; // Temporal patterns are less certain

                patterns.push(AccessPattern {
                    pattern_type: PatternType::Temporal,
                    confidence,
                    next_pages,
                    temporal_distance: Duration::from_millis(TIME_BUCKET_SIZE_MS),
                    access_frequency: buckets[i].len() as f64,
                });
            }
        }

        // Update confidence
        let repeating_patterns = patterns.len();
        let confidence = (repeating_patterns as f64 / time_buckets.len() as f64).min(1.0);
        self.pattern_confidence
            .insert(PatternType::Temporal, confidence);

        self.detected_patterns
            .insert(PatternType::Temporal, patterns);
    }

    fn detect_hotspot_patterns(&mut self) {
        // Count page access frequencies
        let mut page_frequencies = HashMap::<u64, usize>::new();
        for entry in &self.access_history {
            *page_frequencies.entry(entry.page_id).or_insert(0) += 1;
        }

        // Identify hotspots (top 20% most accessed pages)
        let mut sorted_pages: Vec<_> = page_frequencies.into_iter().collect();
        sorted_pages.sort_by(|a, b| b.1.cmp(&a.1));

        let hotspot_count = (sorted_pages.len() as f64 * 0.2) as usize;
        let hotspots: Vec<u64> = sorted_pages
            .into_iter()
            .take(hotspot_count)
            .map(|(page_id, _)| page_id)
            .collect();

        if !hotspots.is_empty() {
            let confidence =
                (hotspot_count as f64 / self.access_history.len() as f64 * 5.0).min(1.0);

            let pattern = AccessPattern {
                pattern_type: PatternType::Hotspot,
                confidence,
                next_pages: hotspots.clone(),
                temporal_distance: Duration::from_millis(50),
                access_frequency: hotspots.len() as f64,
            };

            self.detected_patterns
                .insert(PatternType::Hotspot, vec![pattern]);
            self.pattern_confidence
                .insert(PatternType::Hotspot, confidence);
        }
    }

    fn predict_next_accesses(
        &self,
        _current_page: u64,
        max_predictions: usize,
    ) -> Vec<PrefetchRequest> {
        let mut predictions = Vec::new();
        let now = Instant::now();

        // Combine predictions from all pattern types
        for (pattern_type, patterns) in &self.detected_patterns {
            for pattern in patterns {
                if pattern.confidence >= 0.5 {
                    for &next_page in pattern.next_pages.iter().take(max_predictions) {
                        let request = PrefetchRequest {
                            page_id: next_page,
                            priority: self.determine_priority(pattern),
                            predicted_access_time: now + pattern.temporal_distance,
                            confidence: pattern.confidence,
                            pattern_type: *pattern_type,
                            requester: format!("{:?}_predictor", pattern_type),
                        };
                        predictions.push(request);
                    }
                }
            }
        }

        // Sort by confidence and take top predictions
        predictions.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        predictions.truncate(max_predictions);

        predictions
    }

    fn determine_priority(&self, pattern: &AccessPattern) -> WarmingPriority {
        match pattern.pattern_type {
            PatternType::Hotspot => WarmingPriority::Critical,
            PatternType::Sequential if pattern.confidence > 0.8 => WarmingPriority::High,
            PatternType::Sequential => WarmingPriority::Normal,
            PatternType::Strided if pattern.confidence > 0.7 => WarmingPriority::High,
            PatternType::Strided => WarmingPriority::Normal,
            PatternType::Clustered => WarmingPriority::Normal,
            PatternType::Temporal => WarmingPriority::Low,
            PatternType::Random => WarmingPriority::Low,
        }
    }
}

/// Cache prewarming and predictive prefetching engine
pub struct CachePrewarmer {
    config: PrewarmingConfig,
    pattern_detector: Arc<Mutex<PatternDetector>>,
    prefetch_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,
    warming_stats: Arc<RwLock<WarmingStats>>,
    active_warmers: Arc<RwLock<HashMap<String, thread::JoinHandle<()>>>>,
    is_running: Arc<AtomicBool>,

    // Priority queues for different warming levels
    critical_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,
    high_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,
    normal_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,
    low_queue: Arc<RwLock<VecDeque<PrefetchRequest>>>,

    // Performance tracking
    prefetch_hits: AtomicU64,
    prefetch_misses: AtomicU64,
    total_predictions: AtomicU64,
    accurate_predictions: AtomicU64,
}

impl CachePrewarmer {
    /// Create a new cache prewarmer
    pub fn new(config: PrewarmingConfig) -> Self {
        Self {
            pattern_detector: Arc::new(Mutex::new(PatternDetector::new(
                config.pattern_learning_window,
            ))),
            prefetch_queue: Arc::new(RwLock::new(VecDeque::new())),
            warming_stats: Arc::new(RwLock::new(WarmingStats::default())),
            active_warmers: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),

            critical_queue: Arc::new(RwLock::new(VecDeque::new())),
            high_queue: Arc::new(RwLock::new(VecDeque::new())),
            normal_queue: Arc::new(RwLock::new(VecDeque::new())),
            low_queue: Arc::new(RwLock::new(VecDeque::new())),

            prefetch_hits: AtomicU64::new(0),
            prefetch_misses: AtomicU64::new(0),
            total_predictions: AtomicU64::new(0),
            accurate_predictions: AtomicU64::new(0),

            config,
        }
    }

    /// Start the prewarming engine
    pub fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::Relaxed) {
            return Ok(()); // Already running
        }

        if self.config.enabled {
            self.start_warming_workers()?;
            self.start_pattern_analyzer()?;
            self.start_prefetch_scheduler()?;
        }

        Ok(())
    }

    /// Stop the prewarming engine
    pub fn stop(&self) -> Result<()> {
        self.is_running.store(false, Ordering::Relaxed);

        // Wait for all workers to finish
        let mut warmers = self.active_warmers.write();
        for (name, handle) in warmers.drain() {
            if let Err(e) = handle.join() {
                eprintln!("Warning: Failed to join warmer thread {}: {:?}", name, e);
            }
        }

        Ok(())
    }

    /// Record an access for pattern learning
    pub fn record_access(
        &self,
        page_id: u64,
        access_type: AccessType,
        workload_type: WorkloadType,
        transaction_id: Option<u64>,
    ) {
        let entry = AccessHistoryEntry {
            page_id,
            timestamp: Instant::now(),
            _access_type: access_type,
            _context: AccessContext {
                _workload_type: workload_type,
                _transaction_id: transaction_id,
                _query_type: self.infer_query_type(access_type, workload_type),
                _user_session: None,
            },
        };

        self.pattern_detector.lock().record_access(entry);

        // Trigger predictive prefetching if enabled
        if self.config.enable_predictive_prefetch {
            self.trigger_predictive_prefetch(page_id);
        }
    }

    /// Record successful cache hit for prefetched page
    pub fn record_prefetch_hit(&self, _page_id: u64) {
        self.prefetch_hits.fetch_add(1, Ordering::Relaxed);

        // Update accuracy metrics
        self.accurate_predictions.fetch_add(1, Ordering::Relaxed);

        // Update warming stats
        let mut stats = self.warming_stats.write();
        let total_hits = self.prefetch_hits.load(Ordering::Relaxed);
        let total_misses = self.prefetch_misses.load(Ordering::Relaxed);
        stats.prefetch_accuracy = total_hits as f64 / (total_hits + total_misses).max(1) as f64;
    }

    /// Record cache miss for prefetched page
    pub fn record_prefetch_miss(&self, _page_id: u64) {
        self.prefetch_misses.fetch_add(1, Ordering::Relaxed);

        // Update accuracy metrics
        let mut stats = self.warming_stats.write();
        let total_hits = self.prefetch_hits.load(Ordering::Relaxed);
        let total_misses = self.prefetch_misses.load(Ordering::Relaxed);
        stats.prefetch_accuracy = total_hits as f64 / (total_hits + total_misses).max(1) as f64;
    }

    /// Warm cache for specific workload pattern
    pub fn warm_for_workload(
        &self,
        workload_pattern: &WorkloadPattern,
        allocation: &CacheAllocation,
    ) -> Result<()> {
        let priority_pages = self.generate_warming_strategy(workload_pattern, allocation)?;

        for (priority, pages) in priority_pages {
            for page_id in pages {
                let request = PrefetchRequest {
                    page_id,
                    priority,
                    predicted_access_time: Instant::now(),
                    confidence: 0.8, // High confidence for workload-based warming
                    pattern_type: PatternType::Hotspot,
                    requester: "workload_warmer".to_string(),
                };

                self.enqueue_prefetch_request(request)?;
            }
        }

        Ok(())
    }

    /// Get current warming statistics
    pub fn get_stats(&self) -> WarmingStats {
        let mut stats = self.warming_stats.read().clone();

        // Update real-time metrics
        let total_predictions = self.total_predictions.load(Ordering::Relaxed);
        let accurate_predictions = self.accurate_predictions.load(Ordering::Relaxed);
        stats.pattern_detection_accuracy = if total_predictions > 0 {
            accurate_predictions as f64 / total_predictions as f64
        } else {
            0.0
        };

        // Update queue sizes
        stats.prefetch_queue_size = self.get_total_queue_size();
        stats.active_patterns = self.pattern_detector.lock().detected_patterns.len();

        stats
    }

    // Private helper methods

    fn start_warming_workers(&self) -> Result<()> {
        let worker_count = 4; // Configurable number of workers

        for i in 0..worker_count {
            let worker_name = format!("warmer_{}", i);
            let priority = match i {
                0 => WarmingPriority::Critical,
                1 => WarmingPriority::High,
                2 => WarmingPriority::Normal,
                _ => WarmingPriority::Low,
            };

            let handle = self.spawn_warming_worker(worker_name.clone(), priority)?;
            self.active_warmers.write().insert(worker_name, handle);
        }

        Ok(())
    }

    fn spawn_warming_worker(
        &self,
        name: String,
        priority: WarmingPriority,
    ) -> Result<thread::JoinHandle<()>> {
        let is_running = Arc::clone(&self.is_running);
        let queue = self.get_queue_for_priority(priority);
        let warming_rate = self.config.warming_rate / 4; // Distribute rate among workers
        let stats = Arc::clone(&self.warming_stats);

        let handle = thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                let mut pages_warmed = 0u64;
                let mut last_rate_check = Instant::now();
                let rate_interval = Duration::from_secs(1);

                while is_running.load(Ordering::Relaxed) {
                    // Check if we need to throttle warming rate
                    if last_rate_check.elapsed() >= rate_interval {
                        if pages_warmed > warming_rate as u64 {
                            thread::sleep(Duration::from_millis(100));
                        }
                        pages_warmed = 0;
                        last_rate_check = Instant::now();
                    }

                    // Process prefetch requests from queue
                    if let Some(_request) = queue.write().pop_front() {
                        // Simulate cache warming (in real implementation, this would load the page)
                        thread::sleep(Duration::from_micros(100)); // Simulated I/O time

                        pages_warmed += 1;

                        // Update statistics
                        let mut warming_stats = stats.write();
                        warming_stats.total_pages_warmed += 1;
                    } else {
                        // No requests, sleep briefly
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            })
            .map_err(|e| Error::Generic(format!("Failed to spawn warming worker: {}", e)))?;

        Ok(handle)
    }

    fn start_pattern_analyzer(&self) -> Result<()> {
        let pattern_detector = Arc::clone(&self.pattern_detector);
        let is_running = Arc::clone(&self.is_running);

        let handle = thread::Builder::new()
            .name("pattern_analyzer".to_string())
            .spawn(move || {
                while is_running.load(Ordering::Relaxed) {
                    // Analyze patterns periodically
                    pattern_detector.lock().analyze_patterns();

                    // Sleep for analysis interval
                    thread::sleep(Duration::from_secs(5));
                }
            })
            .map_err(|e| Error::Generic(format!("Failed to spawn pattern analyzer: {}", e)))?;

        self.active_warmers
            .write()
            .insert("pattern_analyzer".to_string(), handle);
        Ok(())
    }

    fn start_prefetch_scheduler(&self) -> Result<()> {
        let critical_queue = Arc::clone(&self.critical_queue);
        let high_queue = Arc::clone(&self.high_queue);
        let normal_queue = Arc::clone(&self.normal_queue);
        let low_queue = Arc::clone(&self.low_queue);
        let prefetch_queue = Arc::clone(&self.prefetch_queue);
        let is_running = Arc::clone(&self.is_running);
        let max_queue_size = self.config.max_prefetch_queue;

        let handle = thread::Builder::new()
            .name("prefetch_scheduler".to_string())
            .spawn(move || {
                while is_running.load(Ordering::Relaxed) {
                    // Move requests from main queue to priority queues
                    while let Some(request) = prefetch_queue.write().pop_front() {
                        let target_queue = match request.priority {
                            WarmingPriority::Critical => &critical_queue,
                            WarmingPriority::High => &high_queue,
                            WarmingPriority::Normal => &normal_queue,
                            WarmingPriority::Low => &low_queue,
                        };

                        let mut queue = target_queue.write();
                        if queue.len() < max_queue_size / 4 {
                            queue.push_back(request);
                        }
                        // Drop request if queue is full
                    }

                    // Sleep for scheduling interval
                    thread::sleep(Duration::from_millis(100));
                }
            })
            .map_err(|e| Error::Generic(format!("Failed to spawn prefetch scheduler: {}", e)))?;

        self.active_warmers
            .write()
            .insert("prefetch_scheduler".to_string(), handle);
        Ok(())
    }

    fn trigger_predictive_prefetch(&self, current_page: u64) {
        let predictions = self
            .pattern_detector
            .lock()
            .predict_next_accesses(current_page, self.config.prefetch_distance);

        for prediction in predictions {
            if prediction.confidence >= self.config.prediction_confidence_threshold
                && self.enqueue_prefetch_request(prediction).is_err()
            {
                // Queue might be full, skip this prediction
            }
        }
    }

    fn enqueue_prefetch_request(&self, request: PrefetchRequest) -> Result<()> {
        let mut queue = self.prefetch_queue.write();

        if queue.len() >= self.config.max_prefetch_queue {
            return Err(Error::Generic("Prefetch queue is full".to_string()));
        }

        queue.push_back(request);
        self.total_predictions.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn get_queue_for_priority(
        &self,
        priority: WarmingPriority,
    ) -> Arc<RwLock<VecDeque<PrefetchRequest>>> {
        match priority {
            WarmingPriority::Critical => Arc::clone(&self.critical_queue),
            WarmingPriority::High => Arc::clone(&self.high_queue),
            WarmingPriority::Normal => Arc::clone(&self.normal_queue),
            WarmingPriority::Low => Arc::clone(&self.low_queue),
        }
    }

    fn get_total_queue_size(&self) -> usize {
        self.critical_queue.read().len()
            + self.high_queue.read().len()
            + self.normal_queue.read().len()
            + self.low_queue.read().len()
            + self.prefetch_queue.read().len()
    }

    fn generate_warming_strategy(
        &self,
        workload_pattern: &WorkloadPattern,
        _allocation: &CacheAllocation,
    ) -> Result<Vec<(WarmingPriority, Vec<u64>)>> {
        let mut strategy = Vec::new();

        match workload_pattern.workload_type {
            WorkloadType::OLTP => {
                // For OLTP, warm hot working set first
                let critical_pages = self.generate_hot_working_set(1000);
                let high_pages = self.generate_sequential_pages(5000, 10000);
                strategy.push((WarmingPriority::Critical, critical_pages));
                strategy.push((WarmingPriority::High, high_pages));
            }
            WorkloadType::OLAP => {
                // For OLAP, prepare for large scans
                let high_pages = self.generate_sequential_pages(1, 50000);
                let normal_pages = self.generate_clustered_pages(100000, 10);
                strategy.push((WarmingPriority::High, high_pages));
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
            WorkloadType::ReadHeavy => {
                // Warm popular read pages
                let critical_pages = self.generate_popular_pages(2000);
                let high_pages = self.generate_hot_working_set(5000);
                strategy.push((WarmingPriority::Critical, critical_pages));
                strategy.push((WarmingPriority::High, high_pages));
            }
            WorkloadType::WriteHeavy => {
                // Warm index and metadata pages
                let high_pages = self.generate_index_pages(1000);
                let normal_pages = self.generate_metadata_pages(500);
                strategy.push((WarmingPriority::High, high_pages));
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
            WorkloadType::Mixed => {
                // Balanced warming strategy
                let critical_pages = self.generate_hot_working_set(1500);
                let high_pages = self.generate_popular_pages(3000);
                let normal_pages = self.generate_sequential_pages(10000, 20000);
                strategy.push((WarmingPriority::Critical, critical_pages));
                strategy.push((WarmingPriority::High, high_pages));
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
            WorkloadType::KeyValue => {
                // Similar to OLTP but with focus on key locality
                let critical_pages = self.generate_key_locality_pages(1200);
                let high_pages = self.generate_hash_bucket_pages(2000);
                strategy.push((WarmingPriority::Critical, critical_pages));
                strategy.push((WarmingPriority::High, high_pages));
            }
            WorkloadType::TimeSeries => {
                // Sequential time-based warming
                let high_pages = self.generate_time_series_pages(10000);
                let normal_pages = self.generate_recent_time_pages(5000);
                strategy.push((WarmingPriority::High, high_pages));
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
            WorkloadType::Cache => {
                // Aggressive warming for cache workloads
                let critical_pages = self.generate_hot_working_set(3000);
                let high_pages = self.generate_popular_pages(7000);
                strategy.push((WarmingPriority::Critical, critical_pages));
                strategy.push((WarmingPriority::High, high_pages));
            }
            WorkloadType::Analytical => {
                // Similar to OLAP
                let high_pages = self.generate_sequential_pages(1, 40000);
                let normal_pages = self.generate_clustered_pages(80000, 10);
                strategy.push((WarmingPriority::High, high_pages));
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
            WorkloadType::Sequential => {
                // Sequential access warming
                let high_pages = self.generate_sequential_pages(1, 30000);
                strategy.push((WarmingPriority::High, high_pages));
            }
            WorkloadType::Random => {
                // Random access needs broad warming
                let normal_pages = self.generate_random_pages(20000);
                strategy.push((WarmingPriority::Normal, normal_pages));
            }
        }

        Ok(strategy)
    }

    fn generate_hot_working_set(&self, size: usize) -> Vec<u64> {
        // Generate a simulated hot working set
        (1..=size as u64).collect()
    }

    fn generate_sequential_pages(&self, start: u64, count: usize) -> Vec<u64> {
        (start..start + count as u64).collect()
    }

    fn generate_clustered_pages(&self, base: u64, cluster_count: usize) -> Vec<u64> {
        let mut pages = Vec::new();
        for i in 0..cluster_count {
            let cluster_base = base + (i as u64 * 1000);
            pages.extend(cluster_base..cluster_base + 100);
        }
        pages
    }

    fn generate_popular_pages(&self, size: usize) -> Vec<u64> {
        // Generate pages based on popularity (simulated)
        let mut pages = Vec::new();
        for i in 0..size {
            // Simulate zipfian distribution
            let page_id = 1 + (i as f64).powf(0.8) as u64;
            pages.push(page_id);
        }
        pages.sort_unstable();
        pages.dedup();
        pages
    }

    fn generate_index_pages(&self, size: usize) -> Vec<u64> {
        // Generate index pages (typically at the beginning)
        (1..=size as u64).collect()
    }

    fn generate_metadata_pages(&self, size: usize) -> Vec<u64> {
        // Generate metadata pages
        (1000000..1000000 + size as u64).collect()
    }

    fn generate_key_locality_pages(&self, size: usize) -> Vec<u64> {
        // Generate pages with key locality
        let mut pages = Vec::new();
        for i in 0..size / 10 {
            let base = (i as u64) * 100;
            pages.extend(base..base + 10);
        }
        pages
    }

    fn generate_hash_bucket_pages(&self, size: usize) -> Vec<u64> {
        // Generate hash bucket pages
        (0..size as u64).map(|i| i * 64).collect()
    }

    fn generate_time_series_pages(&self, size: usize) -> Vec<u64> {
        // Generate time series pages (recent data first)
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        (0..size as u64).map(|i| now.saturating_sub(i)).collect()
    }

    fn generate_recent_time_pages(&self, size: usize) -> Vec<u64> {
        // Generate recent time-based pages
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        (0..size as u64)
            .map(|i| now.saturating_sub(i * 3600))
            .collect() // Last N hours
    }

    fn generate_random_pages(&self, size: usize) -> Vec<u64> {
        // Generate random page IDs for random access patterns
        use rand::prelude::*;
        let mut rng = rand::rng();
        (0..size).map(|_| rng.random::<u64>() % 1000000).collect()
    }

    fn infer_query_type(&self, access_type: AccessType, workload_type: WorkloadType) -> QueryType {
        match (access_type, workload_type) {
            (AccessType::Read, WorkloadType::OLTP) => QueryType::PointLookup,
            (AccessType::Read, WorkloadType::OLAP) => QueryType::RangeScan,
            (AccessType::Scan, _) => QueryType::FullTableScan,
            (AccessType::Index, _) => QueryType::IndexScan,
            (AccessType::Write, WorkloadType::OLTP) => QueryType::Update,
            (AccessType::Write, _) => QueryType::Insert,
            _ => QueryType::PointLookup,
        }
    }
}

impl Drop for CachePrewarmer {
    fn drop(&mut self) {
        if self.is_running.load(Ordering::Relaxed) {
            let _ = self.stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prewarmer_creation() {
        let config = PrewarmingConfig::default();
        let _prewarmer = CachePrewarmer::new(config);
    }

    #[test]
    fn test_pattern_detector() {
        let mut detector = PatternDetector::new(1000);

        // Add sequential access pattern
        for i in 0..10 {
            let entry = AccessHistoryEntry {
                page_id: i,
                timestamp: Instant::now(),
                _access_type: AccessType::Read,
                _context: AccessContext {
                    _workload_type: WorkloadType::OLTP,
                    _transaction_id: Some(1),
                    _query_type: QueryType::PointLookup,
                    _user_session: None,
                },
            };
            detector.record_access(entry);
        }

        detector.analyze_patterns();
        assert!(detector
            .detected_patterns
            .contains_key(&PatternType::Sequential));
    }

    #[test]
    fn test_prefetch_queue() {
        let config = PrewarmingConfig::default();
        let prewarmer = CachePrewarmer::new(config);

        let request = PrefetchRequest {
            page_id: 100,
            priority: WarmingPriority::High,
            predicted_access_time: Instant::now(),
            confidence: 0.8,
            pattern_type: PatternType::Sequential,
            requester: "test".to_string(),
        };

        assert!(prewarmer.enqueue_prefetch_request(request).is_ok());
    }

    #[test]
    fn test_warming_strategies() {
        let config = PrewarmingConfig::default();
        let prewarmer = CachePrewarmer::new(config);

        let workload_pattern = WorkloadPattern {
            workload_type: WorkloadType::OLTP,
            read_write_ratio: 0.8,
            access_locality: 0.9,
            temporal_locality: 0.8,
            working_set_size: 100 * 1024 * 1024,
            scan_intensity: 0.1,
            burst_factor: 1.2,
        };

        let allocation = crate::performance::cache::adaptive_sizing::CacheAllocation::new(
            512 * 1024 * 1024
        );

        let strategy = prewarmer.generate_warming_strategy(&workload_pattern, &allocation);
        assert!(strategy.is_ok());

        let strategy = strategy.unwrap_or_else(|_| Vec::new());
        assert!(!strategy.is_empty());
    }
}

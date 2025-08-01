//! Prefetch Buffer Implementation
//!
//! Advanced prefetching system that predicts memory access patterns and
//! pre-loads data into CPU cache for optimal performance.

use super::{CACHE_LINE_SIZE, PrefetchHints, CachePerformanceStats};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Prefetch request priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PrefetchPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Types of prefetch patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrefetchPattern {
    Sequential,     // Linear access pattern
    Strided,        // Fixed stride access pattern
    Indirect,       // Pointer chasing
    Spatial,        // Access nearby memory
    Temporal,       // Re-access recent memory
    Random,         // Unpredictable access
}

/// Prefetch request
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    pub address: usize,
    pub size: usize,
    pub priority: PrefetchPriority,
    pub pattern: PrefetchPattern,
    pub timestamp: Instant,
    pub prediction_confidence: f64,
}

impl PrefetchRequest {
    pub fn new(address: usize, size: usize, priority: PrefetchPriority) -> Self {
        Self {
            address,
            size,
            priority,
            pattern: PrefetchPattern::Sequential,
            timestamp: Instant::now(),
            prediction_confidence: 0.5,
        }
    }

    pub fn with_pattern(mut self, pattern: PrefetchPattern) -> Self {
        self.pattern = pattern;
        self
    }

    pub fn with_confidence(mut self, confidence: f64) -> Self {
        self.prediction_confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Calculate prefetch urgency based on priority, age, and confidence
    pub fn urgency_score(&self) -> f64 {
        let priority_weight = match self.priority {
            PrefetchPriority::Low => 0.25,
            PrefetchPriority::Normal => 0.5,
            PrefetchPriority::High => 0.75,
            PrefetchPriority::Critical => 1.0,
        };

        let age_penalty = self.timestamp.elapsed().as_millis() as f64 / 1000.0; // Seconds
        let base_score = priority_weight * self.prediction_confidence;
        
        // Reduce score for old requests
        base_score * (-age_penalty / 10.0).exp()
    }
}

/// Access pattern tracker for learning prefetch strategies
#[derive(Debug)]
pub struct AccessPatternTracker {
    access_history: VecDeque<(usize, Instant)>,
    pattern_stats: HashMap<PrefetchPattern, PatternStats>,
    max_history_size: usize,
    stride_detection_window: usize,
}

#[derive(Debug)]
struct PatternStats {
    successful_predictions: u64,
    total_predictions: u64,
    average_accuracy: f64,
    last_update: Instant,
}

impl Default for PatternStats {
    fn default() -> Self {
        Self {
            successful_predictions: 0,
            total_predictions: 0,
            average_accuracy: 0.0,
            last_update: Instant::now(),
        }
    }
}

impl AccessPatternTracker {
    pub fn new(max_history_size: usize) -> Self {
        Self {
            access_history: VecDeque::with_capacity(max_history_size),
            pattern_stats: HashMap::new(),
            max_history_size,
            stride_detection_window: 8,
        }
    }

    /// Record a memory access
    pub fn record_access(&mut self, address: usize) {
        let now = Instant::now();
        
        // Add to history
        self.access_history.push_back((address, now));
        
        // Trim history if too large
        while self.access_history.len() > self.max_history_size {
            self.access_history.pop_front();
        }
    }

    /// Detect access pattern and predict next accesses
    pub fn predict_next_accesses(&self, count: usize) -> Vec<PrefetchRequest> {
        if self.access_history.len() < 2 {
            return Vec::new();
        }

        let mut predictions = Vec::new();

        // Try different pattern detection strategies
        predictions.extend(self.detect_sequential_pattern(count));
        predictions.extend(self.detect_strided_pattern(count));
        predictions.extend(self.detect_spatial_pattern(count));
        predictions.extend(self.detect_temporal_pattern(count));

        // Sort by urgency and take the best predictions
        predictions.sort_by(|a, b| b.urgency_score().partial_cmp(&a.urgency_score()).unwrap());
        predictions.truncate(count);
        
        predictions
    }

    fn detect_sequential_pattern(&self, count: usize) -> Vec<PrefetchRequest> {
        let recent_accesses: Vec<_> = self.access_history
            .iter()
            .rev()
            .take(4)
            .map(|&(addr, _)| addr)
            .collect();

        if recent_accesses.len() < 2 {
            return Vec::new();
        }

        // Check if addresses are increasing sequentially
        let mut is_sequential = true;
        let mut stride = 0;
        
        for i in 1..recent_accesses.len() {
            let current_stride = if recent_accesses[i-1] > recent_accesses[i] {
                recent_accesses[i-1] - recent_accesses[i]
            } else {
                recent_accesses[i] - recent_accesses[i-1]
            };

            if stride == 0 {
                stride = current_stride;
            } else if current_stride != stride {
                is_sequential = false;
                break;
            }
        }

        if !is_sequential || stride == 0 {
            return Vec::new();
        }

        // Generate predictions
        let last_addr = recent_accesses[0];
        let accuracy = self.get_pattern_accuracy(PrefetchPattern::Sequential);
        
        (1..=count)
            .map(|i| {
                let next_addr = last_addr + (stride * i);
                PrefetchRequest::new(next_addr, CACHE_LINE_SIZE, PrefetchPriority::Normal)
                    .with_pattern(PrefetchPattern::Sequential)
                    .with_confidence(accuracy * 0.9_f64.powi(i as i32)) // Decrease confidence with distance
            })
            .collect()
    }

    fn detect_strided_pattern(&self, count: usize) -> Vec<PrefetchRequest> {
        if self.access_history.len() < self.stride_detection_window {
            return Vec::new();
        }

        let recent: Vec<_> = self.access_history
            .iter()
            .rev()
            .take(self.stride_detection_window)
            .map(|&(addr, _)| addr)
            .collect();

        // Calculate strides
        let mut strides = Vec::new();
        for i in 1..recent.len() {
            if recent[i-1] >= recent[i] {
                strides.push(recent[i-1] - recent[i]);
            } else {
                strides.push(recent[i] - recent[i-1]);
            }
        }

        // Find most common stride
        let mut stride_counts = HashMap::new();
        for &stride in &strides {
            if stride > 0 && stride <= 4096 { // Reasonable stride limit
                *stride_counts.entry(stride).or_insert(0) += 1;
            }
        }

        if let Some((&best_stride, &count_val)) = stride_counts.iter()
            .max_by_key(|&(_, count)| count) 
        {
            if count_val >= strides.len() / 2 { // At least 50% consistency
                let last_addr = recent[0];
                let accuracy = self.get_pattern_accuracy(PrefetchPattern::Strided);
                
                return (1..=count)
                    .map(|i| {
                        let next_addr = last_addr + (best_stride * i);
                        PrefetchRequest::new(next_addr, CACHE_LINE_SIZE, PrefetchPriority::Normal)
                            .with_pattern(PrefetchPattern::Strided)
                            .with_confidence(accuracy * 0.95_f64.powi(i as i32))
                    })
                    .collect();
            }
        }

        Vec::new()
    }

    fn detect_spatial_pattern(&self, count: usize) -> Vec<PrefetchRequest> {
        if self.access_history.is_empty() {
            return Vec::new();
        }

        let (last_addr, _) = self.access_history.back().unwrap();
        let cache_line_base = last_addr & !(CACHE_LINE_SIZE - 1);
        let accuracy = self.get_pattern_accuracy(PrefetchPattern::Spatial);

        // Prefetch adjacent cache lines
        (1..=count)
            .map(|i| {
                let next_addr = cache_line_base + (CACHE_LINE_SIZE * i);
                PrefetchRequest::new(next_addr, CACHE_LINE_SIZE, PrefetchPriority::Low)
                    .with_pattern(PrefetchPattern::Spatial)
                    .with_confidence(accuracy * 0.8_f64.powi(i as i32))
            })
            .collect()
    }

    fn detect_temporal_pattern(&self, count: usize) -> Vec<PrefetchRequest> {
        if self.access_history.len() < count * 2 {
            return Vec::new();
        }

        // Look for recently accessed addresses that might be accessed again
        let cutoff_time = Instant::now() - Duration::from_millis(100);
        let recent_addrs: Vec<_> = self.access_history
            .iter()
            .rev()
            .take_while(|(_, time)| *time >= cutoff_time)
            .map(|&(addr, _)| addr)
            .collect();

        if recent_addrs.len() < count {
            return Vec::new();
        }

        let accuracy = self.get_pattern_accuracy(PrefetchPattern::Temporal);
        
        recent_addrs
            .into_iter()
            .take(count)
            .enumerate()
            .map(|(i, addr)| {
                PrefetchRequest::new(addr, CACHE_LINE_SIZE, PrefetchPriority::Low)
                    .with_pattern(PrefetchPattern::Temporal)
                    .with_confidence(accuracy * 0.9_f64.powi(i as i32))
            })
            .collect()
    }

    fn get_pattern_accuracy(&self, pattern: PrefetchPattern) -> f64 {
        self.pattern_stats
            .get(&pattern)
            .map(|stats| stats.average_accuracy)
            .unwrap_or(0.5) // Default 50% confidence
    }

    /// Update pattern statistics based on prefetch success
    pub fn update_pattern_accuracy(&mut self, pattern: PrefetchPattern, was_successful: bool) {
        let stats = self.pattern_stats.entry(pattern).or_insert_with(Default::default);
        
        stats.total_predictions += 1;
        if was_successful {
            stats.successful_predictions += 1;
        }
        
        stats.average_accuracy = stats.successful_predictions as f64 / stats.total_predictions as f64;
        stats.last_update = Instant::now();
    }
}

/// High-performance prefetch buffer with adaptive learning
pub struct PrefetchBuffer {
    pending_requests: Arc<Mutex<Vec<PrefetchRequest>>>,
    access_tracker: Arc<Mutex<AccessPatternTracker>>,
    stats: Arc<CachePerformanceStats>,
    config: PrefetchConfig,
    worker_active: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    pub max_pending_requests: usize,
    pub prefetch_distance: usize,
    pub min_confidence_threshold: f64,
    pub background_prefetch_enabled: bool,
    pub adaptive_learning_enabled: bool,
    pub max_prefetch_size: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            max_pending_requests: 1024,
            prefetch_distance: 8,
            min_confidence_threshold: 0.3,
            background_prefetch_enabled: true,
            adaptive_learning_enabled: true,
            max_prefetch_size: 64 * 1024, // 64KB
        }
    }
}

impl PrefetchBuffer {
    pub fn new(config: PrefetchConfig) -> Self {
        let buffer = Self {
            pending_requests: Arc::new(Mutex::new(Vec::with_capacity(config.max_pending_requests))),
            access_tracker: Arc::new(Mutex::new(AccessPatternTracker::new(1000))),
            stats: Arc::new(CachePerformanceStats::new()),
            worker_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config,
        };

        if buffer.config.background_prefetch_enabled {
            buffer.start_background_worker();
        }

        buffer
    }

    /// Record a memory access and trigger adaptive prefetching
    pub fn record_access(&self, address: usize) {
        if let Ok(mut tracker) = self.access_tracker.lock() {
            tracker.record_access(address);
            
            if self.config.adaptive_learning_enabled {
                let predictions = tracker.predict_next_accesses(self.config.prefetch_distance);
                drop(tracker); // Release lock early
                
                for prediction in predictions {
                    if prediction.prediction_confidence >= self.config.min_confidence_threshold {
                        self.submit_prefetch_request(prediction);
                    }
                }
            }
        }
    }

    /// Submit a prefetch request
    pub fn submit_prefetch_request(&self, request: PrefetchRequest) {
        if request.size > self.config.max_prefetch_size {
            return; // Request too large
        }

        if let Ok(mut requests) = self.pending_requests.lock() {
            if requests.len() < self.config.max_pending_requests {
                requests.push(request);
                
                // Sort by urgency to prioritize important requests
                requests.sort_by(|a, b| b.urgency_score().partial_cmp(&a.urgency_score()).unwrap());
            }
        }
    }

    /// Execute pending prefetch requests
    pub fn execute_prefetches(&self, max_count: usize) -> usize {
        let mut executed = 0;
        
        if let Ok(mut requests) = self.pending_requests.lock() {
            let to_execute = requests.len().min(max_count);
            
            for _ in 0..to_execute {
                if let Some(request) = requests.pop() {
                    self.execute_single_prefetch(&request);
                    executed += 1;
                }
            }
        }
        
        executed
    }

    fn execute_single_prefetch(&self, request: &PrefetchRequest) {
        match request.pattern {
            PrefetchPattern::Sequential | PrefetchPattern::Strided => {
                // Use temporal prefetch hint for sequential patterns
                PrefetchHints::prefetch_read_t0(request.address as *const u8);
            },
            PrefetchPattern::Spatial => {
                // Prefetch multiple cache lines for spatial locality
                let cache_lines = (request.size + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
                for i in 0..cache_lines {
                    let addr = request.address + (i * CACHE_LINE_SIZE);
                    PrefetchHints::prefetch_read_t0(addr as *const u8);
                }
            },
            PrefetchPattern::Temporal => {
                // Use temporal hint for recently accessed data
                PrefetchHints::prefetch_read_t0(request.address as *const u8);
            },
            PrefetchPattern::Indirect | PrefetchPattern::Random => {
                // Use non-temporal hint for irregular patterns
                PrefetchHints::prefetch_read_nt(request.address as *const u8);
            },
        }

        self.stats.record_prefetch_hit();
    }

    /// Start background worker thread for automatic prefetching
    fn start_background_worker(&self) {
        let requests = Arc::clone(&self.pending_requests);
        let active = Arc::clone(&self.worker_active);
        let stats = Arc::clone(&self.stats);
        
        active.store(true, std::sync::atomic::Ordering::Relaxed);
        
        std::thread::spawn(move || {
            while active.load(std::sync::atomic::Ordering::Relaxed) {
                // Execute up to 16 prefetches per cycle
                if let Ok(mut reqs) = requests.lock() {
                    let to_execute = reqs.len().min(16);
                    for _ in 0..to_execute {
                        if let Some(request) = reqs.pop() {
                            match request.pattern {
                                PrefetchPattern::Sequential | PrefetchPattern::Strided => {
                                    PrefetchHints::prefetch_read_t0(request.address as *const u8);
                                },
                                _ => {
                                    PrefetchHints::prefetch_read_nt(request.address as *const u8);
                                },
                            }
                            stats.record_prefetch_hit();
                        }
                    }
                }
                
                // Sleep briefly to avoid consuming too much CPU
                std::thread::sleep(Duration::from_micros(100));
            }
        });
    }

    /// Stop background worker
    pub fn stop_background_worker(&self) {
        self.worker_active.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get buffer statistics
    pub fn get_stats(&self) -> PrefetchBufferStats {
        let pending_count = self.pending_requests.lock()
            .map(|reqs| reqs.len())
            .unwrap_or(0);

        PrefetchBufferStats {
            pending_requests: pending_count,
            total_prefetches: self.stats.prefetch_hits.load(Ordering::Relaxed),
            cache_hit_rate: self.stats.hit_rate(),
            background_worker_active: self.worker_active.load(Ordering::Relaxed),
        }
    }

    /// Update learning model based on access success
    pub fn update_learning(&self, address: usize, pattern: PrefetchPattern, was_hit: bool) {
        if let Ok(mut tracker) = self.access_tracker.lock() {
            tracker.update_pattern_accuracy(pattern, was_hit);
        }
        
        if was_hit {
            self.stats.record_hit();
        } else {
            self.stats.record_miss();
        }
    }

    /// Clear all pending requests
    pub fn clear_pending(&self) {
        if let Ok(mut requests) = self.pending_requests.lock() {
            requests.clear();
        }
    }

    /// Get pattern accuracy statistics
    pub fn get_pattern_accuracy(&self) -> HashMap<PrefetchPattern, f64> {
        self.access_tracker.lock()
            .map(|tracker| {
                tracker.pattern_stats.iter()
                    .map(|(&pattern, stats)| (pattern, stats.average_accuracy))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Drop for PrefetchBuffer {
    fn drop(&mut self) {
        self.stop_background_worker();
    }
}

#[derive(Debug)]
pub struct PrefetchBufferStats {
    pub pending_requests: usize,
    pub total_prefetches: usize,
    pub cache_hit_rate: f64,
    pub background_worker_active: bool,
}

/// Specialized prefetch buffers for different access patterns
pub struct SpecializedPrefetchers {
    sequential: PrefetchBuffer,
    random: PrefetchBuffer,
    spatial: PrefetchBuffer,
}

impl SpecializedPrefetchers {
    pub fn new() -> Self {
        Self {
            sequential: PrefetchBuffer::new(PrefetchConfig {
                prefetch_distance: 16,
                min_confidence_threshold: 0.8,
                ..Default::default()
            }),
            random: PrefetchBuffer::new(PrefetchConfig {
                prefetch_distance: 4,
                min_confidence_threshold: 0.2,
                ..Default::default()
            }),
            spatial: PrefetchBuffer::new(PrefetchConfig {
                prefetch_distance: 8,
                min_confidence_threshold: 0.5,
                ..Default::default()
            }),
        }
    }

    pub fn get_prefetcher(&self, pattern: PrefetchPattern) -> &PrefetchBuffer {
        match pattern {
            PrefetchPattern::Sequential | PrefetchPattern::Strided => &self.sequential,
            PrefetchPattern::Spatial | PrefetchPattern::Temporal => &self.spatial,
            PrefetchPattern::Random | PrefetchPattern::Indirect => &self.random,
        }
    }

    pub fn record_access(&self, address: usize, pattern: PrefetchPattern) {
        self.get_prefetcher(pattern).record_access(address);
    }
}

impl Default for SpecializedPrefetchers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_request_urgency() {
        let req = PrefetchRequest::new(0x1000, 64, PrefetchPriority::High)
            .with_confidence(0.9);
        
        let urgency = req.urgency_score();
        assert!(urgency > 0.6); // High priority with high confidence
        assert!(urgency <= 1.0);
    }

    #[test]
    fn test_access_pattern_tracker() {
        let mut tracker = AccessPatternTracker::new(100);
        
        // Record sequential accesses
        for i in 0..10 {
            tracker.record_access(0x1000 + i * 64);
        }
        
        let predictions = tracker.predict_next_accesses(3);
        assert!(!predictions.is_empty());
        
        // Check if predictions follow sequential pattern
        if let Some(first_pred) = predictions.first() {
            assert_eq!(first_pred.pattern, PrefetchPattern::Sequential);
        }
    }

    #[test]
    fn test_prefetch_buffer() {
        let config = PrefetchConfig {
            background_prefetch_enabled: false,
            ..Default::default()
        };
        let buffer = PrefetchBuffer::new(config);
        
        // Record sequential accesses
        for i in 0..5 {
            buffer.record_access(0x1000 + i * 64);
        }
        
        // Execute prefetches
        let executed = buffer.execute_prefetches(10);
        assert!(executed > 0);
        
        let stats = buffer.get_stats();
        assert_eq!(stats.total_prefetches, executed);
    }

    #[test]
    fn test_sequential_pattern_detection() {
        let mut tracker = AccessPatternTracker::new(100);
        
        // Record perfect sequential pattern
        for i in 0..8 {
            tracker.record_access(0x2000 + i * 128);
        }
        
        let predictions = tracker.detect_sequential_pattern(4);
        assert_eq!(predictions.len(), 4);
        
        for (i, pred) in predictions.iter().enumerate() {
            let expected_addr = 0x2000 + (8 + i + 1) * 128;
            assert_eq!(pred.address, expected_addr);
            assert_eq!(pred.pattern, PrefetchPattern::Sequential);
        }
    }

    #[test]
    fn test_strided_pattern_detection() {
        let mut tracker = AccessPatternTracker::new(100);
        
        // Record strided pattern (stride = 256)
        for i in 0..8 {
            tracker.record_access(0x3000 + i * 256);
        }
        
        let predictions = tracker.detect_strided_pattern(3);
        assert!(!predictions.is_empty());
        
        if let Some(first_pred) = predictions.first() {
            assert_eq!(first_pred.pattern, PrefetchPattern::Strided);
        }
    }

    #[test]
    fn test_specialized_prefetchers() {
        let prefetchers = SpecializedPrefetchers::new();
        
        prefetchers.record_access(0x1000, PrefetchPattern::Sequential);
        prefetchers.record_access(0x2000, PrefetchPattern::Random);
        prefetchers.record_access(0x3000, PrefetchPattern::Spatial);
        
        // Verify different prefetchers are used for different patterns
        let seq_prefetcher = prefetchers.get_prefetcher(PrefetchPattern::Sequential);
        let rand_prefetcher = prefetchers.get_prefetcher(PrefetchPattern::Random);
        
        assert!(!std::ptr::eq(seq_prefetcher, rand_prefetcher));
    }

    #[test]
    fn test_pattern_learning() {
        let config = PrefetchConfig {
            background_prefetch_enabled: false,
            adaptive_learning_enabled: true,
            ..Default::default()
        };
        let buffer = PrefetchBuffer::new(config);
        
        // Simulate successful prefetch
        buffer.update_learning(0x1000, PrefetchPattern::Sequential, true);
        buffer.update_learning(0x1000, PrefetchPattern::Sequential, true);
        buffer.update_learning(0x1000, PrefetchPattern::Sequential, false);
        
        let accuracy = buffer.get_pattern_accuracy();
        if let Some(&seq_accuracy) = accuracy.get(&PrefetchPattern::Sequential) {
            assert!((seq_accuracy - 0.6667).abs() < 0.01); // 2/3 success rate
        }
    }
}
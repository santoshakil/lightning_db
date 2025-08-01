//! I/O Request Scheduler
//!
//! This module provides intelligent scheduling of I/O requests to maximize
//! throughput and minimize latency. It includes request merging, prioritization,
//! and adaptive batching.

use super::*;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::{Ordering, Reverse};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

/// I/O request priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IoPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Scheduled I/O request with metadata
#[derive(Debug, Clone)]
pub struct ScheduledRequest {
    pub request: IoRequest,
    pub priority: IoPriority,
    pub submit_time: Instant,
    pub deadline: Option<Instant>,
    pub dependencies: Vec<u64>, // User data of requests this depends on
    pub mergeable: bool,
}

impl ScheduledRequest {
    pub fn new(request: IoRequest, priority: IoPriority) -> Self {
        Self {
            request,
            priority,
            submit_time: Instant::now(),
            deadline: None,
            dependencies: Vec::new(),
            mergeable: true,
        }
    }
    
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }
    
    pub fn with_dependencies(mut self, deps: Vec<u64>) -> Self {
        self.dependencies = deps;
        self
    }
    
    pub fn non_mergeable(mut self) -> Self {
        self.mergeable = false;
        self
    }
    
    /// Calculate request urgency score
    pub fn urgency_score(&self) -> i64 {
        let mut score = match self.priority {
            IoPriority::Low => 1000,
            IoPriority::Normal => 2000,
            IoPriority::High => 3000,
            IoPriority::Critical => 4000,
        };
        
        // Add deadline urgency
        if let Some(deadline) = self.deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining < Duration::from_millis(10) {
                score += 2000; // Very urgent
            } else if remaining < Duration::from_millis(100) {
                score += 1000; // Urgent
            }
        }
        
        // Add age factor (older requests get higher priority)
        let age_ms = self.submit_time.elapsed().as_millis() as i64;
        score += age_ms.min(1000); // Cap age bonus at 1000
        
        score
    }
}

impl PartialEq for ScheduledRequest {
    fn eq(&self, other: &Self) -> bool {
        self.request.user_data == other.request.user_data
    }
}

impl Eq for ScheduledRequest {}

impl PartialOrd for ScheduledRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher urgency score = higher priority
        self.urgency_score().cmp(&other.urgency_score())
    }
}

/// Request merging strategy
pub struct RequestMerger {
    merge_distance: u64, // Max distance between requests to merge
    max_merge_size: usize, // Max size of merged request
}

impl RequestMerger {
    pub fn new(merge_distance: u64, max_merge_size: usize) -> Self {
        Self {
            merge_distance,
            max_merge_size,
        }
    }
    
    /// Try to merge two requests
    pub fn try_merge(&self, req1: &ScheduledRequest, req2: &ScheduledRequest) -> Option<ScheduledRequest> {
        // Only merge same type, same fd, and mergeable requests
        if !req1.mergeable || !req2.mergeable {
            return None;
        }
        
        if req1.request.op_type != req2.request.op_type || req1.request.fd != req2.request.fd {
            return None;
        }
        
        // Check if requests are adjacent or close enough
        let (first, second) = if req1.request.offset <= req2.request.offset {
            (req1, req2)
        } else {
            (req2, req1)
        };
        
        let first_end = match &first.request.buffer {
            Some(IoBuffer::Standard(buf)) => first.request.offset + buf.len() as u64,
            _ => return None, // Can only merge standard buffers
        };
        
        if second.request.offset > first_end + self.merge_distance {
            return None; // Too far apart
        }
        
        // Create merged request
        let gap = second.request.offset.saturating_sub(first_end) as usize;
        
        match (&first.request.buffer, &second.request.buffer) {
            (Some(IoBuffer::Standard(buf1)), Some(IoBuffer::Standard(buf2))) => {
                let total_size = buf1.len() + gap + buf2.len();
                if total_size > self.max_merge_size {
                    return None; // Merged request too large
                }
                
                let mut merged_buffer = Vec::with_capacity(total_size);
                merged_buffer.extend_from_slice(buf1);
                merged_buffer.resize(buf1.len() + gap, 0); // Fill gap with zeros
                merged_buffer.extend_from_slice(buf2);
                
                let mut merged = ScheduledRequest::new(
                    IoRequest {
                        op_type: first.request.op_type,
                        fd: first.request.fd,
                        offset: first.request.offset,
                        buffer: Some(IoBuffer::Standard(merged_buffer)),
                        flags: first.request.flags,
                        user_data: first.request.user_data, // Keep first request's user data
                    },
                    std::cmp::max(first.priority, second.priority),
                );
                
                // Merge deadlines (use earliest)
                merged.deadline = match (first.deadline, second.deadline) {
                    (Some(d1), Some(d2)) => Some(std::cmp::min(d1, d2)),
                    (Some(d), None) | (None, Some(d)) => Some(d),
                    (None, None) => None,
                };
                
                Some(merged)
            },
            _ => None,
        }
    }
}

/// I/O request scheduler
pub struct IoScheduler {
    pending_queue: Arc<Mutex<BinaryHeap<ScheduledRequest>>>,
    dependency_map: Arc<Mutex<HashMap<u64, Vec<ScheduledRequest>>>>,
    completed_requests: Arc<Mutex<HashMap<u64, CompletionEntry>>>,
    merger: RequestMerger,
    stats: SchedulerStats,
    config: SchedulerConfig,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub max_batch_size: usize,
    pub batch_timeout: Duration,
    pub merge_requests: bool,
    pub merge_distance: u64,
    pub max_merge_size: usize,
    pub adaptive_batching: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 32,
            batch_timeout: Duration::from_micros(100),
            merge_requests: true,
            merge_distance: 4096, // 4KB
            max_merge_size: 1024 * 1024, // 1MB
            adaptive_batching: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct SchedulerStats {
    pub total_requests: AtomicU64,
    pub merged_requests: AtomicU64,
    pub dependency_waits: AtomicU64,
    pub batch_count: AtomicU64,
    pub avg_batch_size: AtomicUsize,
    pub deadline_misses: AtomicU64,
}

impl IoScheduler {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            pending_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            dependency_map: Arc::new(Mutex::new(HashMap::new())),
            completed_requests: Arc::new(Mutex::new(HashMap::new())),
            merger: RequestMerger::new(config.merge_distance, config.max_merge_size),
            stats: SchedulerStats::default(),
            config,
        }
    }
    
    /// Schedule a request
    pub fn schedule(&self, request: ScheduledRequest) {
        self.stats.total_requests.fetch_add(1, AtomicOrdering::Relaxed);
        
        // Check dependencies
        if !request.dependencies.is_empty() {
            let mut completed = self.completed_requests.lock().unwrap();
            let mut pending_deps = Vec::new();
            
            for dep in &request.dependencies {
                if !completed.contains_key(dep) {
                    pending_deps.push(*dep);
                }
            }
            
            if !pending_deps.is_empty() {
                // Has pending dependencies, add to dependency map
                let mut dep_map = self.dependency_map.lock().unwrap();
                for dep in pending_deps {
                    dep_map.entry(dep).or_insert_with(Vec::new).push(request.clone());
                }
                self.stats.dependency_waits.fetch_add(1, AtomicOrdering::Relaxed);
                return;
            }
        }
        
        // No dependencies or all satisfied, add to pending queue
        let mut queue = self.pending_queue.lock().unwrap();
        
        // Try to merge with existing requests if enabled
        if self.config.merge_requests && request.mergeable {
            let mut merged = false;
            let mut temp_queue = BinaryHeap::new();
            
            while let Some(existing) = queue.pop() {
                if !merged {
                    if let Some(merged_req) = self.merger.try_merge(&existing, &request) {
                        temp_queue.push(merged_req);
                        self.stats.merged_requests.fetch_add(1, AtomicOrdering::Relaxed);
                        merged = true;
                        continue;
                    }
                }
                temp_queue.push(existing);
            }
            
            if !merged {
                temp_queue.push(request);
            }
            
            *queue = temp_queue;
        } else {
            queue.push(request);
        }
    }
    
    /// Get next batch of requests to submit
    pub fn get_batch(&self) -> Vec<IoRequest> {
        let mut queue = self.pending_queue.lock().unwrap();
        let mut batch = Vec::with_capacity(self.config.max_batch_size);
        let batch_start = Instant::now();
        
        while batch.len() < self.config.max_batch_size {
            if let Some(scheduled) = queue.pop() {
                // Check deadline
                if let Some(deadline) = scheduled.deadline {
                    if Instant::now() > deadline {
                        self.stats.deadline_misses.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                }
                
                batch.push(scheduled.request);
            } else {
                break;
            }
            
            // Check batch timeout
            if batch_start.elapsed() >= self.config.batch_timeout {
                break;
            }
        }
        
        // Update stats
        if !batch.is_empty() {
            self.stats.batch_count.fetch_add(1, AtomicOrdering::Relaxed);
            
            // Update average batch size
            let old_avg = self.stats.avg_batch_size.load(AtomicOrdering::Relaxed);
            let batch_count = self.stats.batch_count.load(AtomicOrdering::Relaxed) as usize;
            let new_avg = (old_avg * (batch_count - 1) + batch.len()) / batch_count;
            self.stats.avg_batch_size.store(new_avg, AtomicOrdering::Relaxed);
        }
        
        batch
    }
    
    /// Notify completion of a request
    pub fn complete(&self, completion: CompletionEntry) {
        let user_data = completion.user_data;
        
        // Store completion
        self.completed_requests.lock().unwrap().insert(user_data, completion);
        
        // Check if any requests were waiting on this
        let mut dep_map = self.dependency_map.lock().unwrap();
        if let Some(waiting) = dep_map.remove(&user_data) {
            let mut queue = self.pending_queue.lock().unwrap();
            for mut req in waiting {
                // Remove this dependency
                req.dependencies.retain(|&d| d != user_data);
                
                // Check if all dependencies are now satisfied
                if req.dependencies.is_empty() {
                    queue.push(req);
                } else {
                    // Re-add to dependency map for remaining dependencies
                    drop(queue);
                    for dep in &req.dependencies {
                        dep_map.entry(*dep).or_insert_with(Vec::new).push(req.clone());
                    }
                    queue = self.pending_queue.lock().unwrap();
                }
            }
        }
    }
    
    /// Adapt scheduler parameters based on workload
    pub fn adapt_parameters(&mut self) {
        if !self.config.adaptive_batching {
            return;
        }
        
        let avg_batch_size = self.stats.avg_batch_size.load(AtomicOrdering::Relaxed);
        let merge_rate = if self.stats.total_requests.load(AtomicOrdering::Relaxed) > 0 {
            self.stats.merged_requests.load(AtomicOrdering::Relaxed) as f64 /
            self.stats.total_requests.load(AtomicOrdering::Relaxed) as f64
        } else {
            0.0
        };
        
        // Adjust batch size based on average
        if avg_batch_size > self.config.max_batch_size * 90 / 100 {
            // Consistently hitting batch limit, increase it
            self.config.max_batch_size = (self.config.max_batch_size * 125 / 100).min(128);
        } else if avg_batch_size < self.config.max_batch_size * 25 / 100 {
            // Batches too small, decrease limit
            self.config.max_batch_size = (self.config.max_batch_size * 75 / 100).max(8);
        }
        
        // Adjust merge distance based on merge rate
        if merge_rate > 0.2 {
            // High merge rate, increase distance
            self.config.merge_distance = (self.config.merge_distance * 125 / 100).min(64 * 1024);
        } else if merge_rate < 0.05 {
            // Low merge rate, decrease distance
            self.config.merge_distance = (self.config.merge_distance * 75 / 100).max(512);
        }
    }
    
    /// Get scheduler statistics
    pub fn stats(&self) -> &SchedulerStats {
        &self.stats
    }
    
    /// Clear completed requests older than specified duration
    pub fn cleanup_completed(&self, max_age: Duration) {
        let mut completed = self.completed_requests.lock().unwrap();
        let cutoff = Instant::now() - max_age;
        
        // In real implementation, would track completion times
        // For now, just clear if too many
        if completed.len() > 10000 {
            completed.clear();
        }
    }
}

/// Request batcher for optimal submission
pub struct RequestBatcher {
    requests: Vec<IoRequest>,
    max_size: usize,
    timeout: Duration,
    last_submit: Instant,
}

impl RequestBatcher {
    pub fn new(max_size: usize, timeout: Duration) -> Self {
        Self {
            requests: Vec::with_capacity(max_size),
            max_size,
            timeout,
            last_submit: Instant::now(),
        }
    }
    
    /// Add request to batch
    pub fn add(&mut self, request: IoRequest) -> bool {
        self.requests.push(request);
        self.should_submit()
    }
    
    /// Check if batch should be submitted
    pub fn should_submit(&self) -> bool {
        self.requests.len() >= self.max_size ||
        (self.requests.len() > 0 && self.last_submit.elapsed() >= self.timeout)
    }
    
    /// Take all requests and reset
    pub fn take(&mut self) -> Vec<IoRequest> {
        self.last_submit = Instant::now();
        std::mem::take(&mut self.requests)
    }
    
    /// Get current batch size
    pub fn len(&self) -> usize {
        self.requests.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_scheduled_request_priority() {
        let req1 = ScheduledRequest::new(
            IoRequest {
                op_type: OpType::Read,
                fd: 1,
                offset: 0,
                buffer: None,
                flags: SqeFlags::default(),
                user_data: 1,
            },
            IoPriority::Normal,
        );
        
        let req2 = ScheduledRequest::new(
            IoRequest {
                op_type: OpType::Read,
                fd: 1,
                offset: 0,
                buffer: None,
                flags: SqeFlags::default(),
                user_data: 2,
            },
            IoPriority::High,
        );
        
        assert!(req2.urgency_score() > req1.urgency_score());
    }
    
    #[test]
    fn test_request_merger() {
        let merger = RequestMerger::new(1024, 8192);
        
        let req1 = ScheduledRequest::new(
            IoRequest {
                op_type: OpType::Read,
                fd: 1,
                offset: 0,
                buffer: Some(IoBuffer::Standard(vec![0u8; 1024])),
                flags: SqeFlags::default(),
                user_data: 1,
            },
            IoPriority::Normal,
        );
        
        let req2 = ScheduledRequest::new(
            IoRequest {
                op_type: OpType::Read,
                fd: 1,
                offset: 1024,
                buffer: Some(IoBuffer::Standard(vec![0u8; 1024])),
                flags: SqeFlags::default(),
                user_data: 2,
            },
            IoPriority::Normal,
        );
        
        let merged = merger.try_merge(&req1, &req2);
        assert!(merged.is_some());
        
        let merged = merged.unwrap();
        match &merged.request.buffer {
            Some(IoBuffer::Standard(buf)) => assert_eq!(buf.len(), 2048),
            _ => panic!("Expected standard buffer"),
        }
    }
    
    #[test]
    fn test_scheduler_basic() {
        let scheduler = IoScheduler::new(SchedulerConfig::default());
        
        // Schedule some requests
        for i in 0..5 {
            let req = ScheduledRequest::new(
                IoRequest {
                    op_type: OpType::Read,
                    fd: 1,
                    offset: i * 1024,
                    buffer: Some(IoBuffer::Standard(vec![0u8; 1024])),
                    flags: SqeFlags::default(),
                    user_data: i,
                },
                IoPriority::Normal,
            );
            scheduler.schedule(req);
        }
        
        let batch = scheduler.get_batch();
        assert_eq!(batch.len(), 5);
        assert_eq!(scheduler.stats().total_requests.load(AtomicOrdering::Relaxed), 5);
    }
    
    #[test]
    fn test_request_batcher() {
        let mut batcher = RequestBatcher::new(3, Duration::from_millis(100));
        
        let req = IoRequest {
            op_type: OpType::Read,
            fd: 1,
            offset: 0,
            buffer: None,
            flags: SqeFlags::default(),
            user_data: 1,
        };
        
        assert!(!batcher.add(req.clone()));
        assert!(!batcher.add(req.clone()));
        assert!(batcher.add(req)); // Should trigger submission
        
        let batch = batcher.take();
        assert_eq!(batch.len(), 3);
        assert_eq!(batcher.len(), 0);
    }
}
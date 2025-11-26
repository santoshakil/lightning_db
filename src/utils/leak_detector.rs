//! Advanced Memory Leak Detection Tools
//!
//! This module provides sophisticated leak detection capabilities including:
//! - Reference counting validation
//! - Circular reference detection
//! - Orphaned resource detection  
//! - Object lifetime tracking
//! - Weak reference analysis

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    thread,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Global leak detector instance
pub static LEAK_DETECTOR: once_cell::sync::Lazy<Arc<LeakDetector>> =
    once_cell::sync::Lazy::new(|| Arc::new(LeakDetector::new()));

/// Configuration for leak detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeakDetectionConfig {
    /// Enable reference counting validation
    pub validate_ref_counts: bool,
    /// Enable circular reference detection
    pub detect_cycles: bool,
    /// Enable orphaned resource detection
    pub detect_orphans: bool,
    /// Maximum object age before considering it potentially leaked
    pub max_object_age: Duration,
    /// Minimum reference count threshold for validation
    pub min_ref_count_threshold: usize,
    /// Scan interval for detection algorithms
    pub scan_interval: Duration,
    /// Maximum objects to track simultaneously
    pub max_tracked_objects: usize,
    /// Enable detailed object lifetime tracking
    pub track_object_lifetimes: bool,
}

impl Default for LeakDetectionConfig {
    fn default() -> Self {
        Self {
            validate_ref_counts: true,
            detect_cycles: true,
            detect_orphans: true,
            max_object_age: Duration::from_secs(300), // 5 minutes
            min_ref_count_threshold: 10,
            scan_interval: Duration::from_secs(30),
            max_tracked_objects: 100_000,
            track_object_lifetimes: cfg!(debug_assertions),
        }
    }
}

/// Unique object identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId(u64);

impl ObjectId {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// Object type classification
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ObjectType {
    BTreeNode,
    CacheEntry,
    WALEntry,
    Transaction,
    PageBuffer,
    IndexEntry,
    Custom(String),
}

/// Reference type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferenceType {
    Strong,
    Weak,
    Raw,
    Shared,
}

/// Object reference information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectReference {
    pub from_object: ObjectId,
    pub to_object: ObjectId,
    pub reference_type: ReferenceType,
    pub created_at: SystemTime,
    pub location: Option<String>,
}

/// Tracked object information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedObject {
    pub id: ObjectId,
    pub object_type: ObjectType,
    pub size: usize,
    pub created_at: SystemTime,
    pub last_accessed: SystemTime,
    pub strong_ref_count: usize,
    pub weak_ref_count: usize,
    pub creation_location: Option<String>,
    pub references_to: Vec<ObjectReference>,
    pub references_from: Vec<ObjectReference>,
}

/// Leak detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeakType {
    CircularReference {
        cycle: Vec<ObjectId>,
        total_size: usize,
    },
    OrphanedObject {
        object: ObjectId,
        age: Duration,
        size: usize,
    },
    RefCountMismatch {
        object: ObjectId,
        expected: usize,
        actual: usize,
    },
    UnreleasedResource {
        object: ObjectId,
        resource_type: String,
        age: Duration,
    },
    WeakReferenceIssue {
        object: ObjectId,
        weak_refs: usize,
        strong_refs: usize,
    },
}

/// Leak detection report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeakReport {
    pub timestamp: SystemTime,
    pub scan_duration: Duration,
    pub objects_scanned: usize,
    pub leaks_detected: Vec<LeakType>,
    pub object_summary: ObjectSummary,
    pub recommendations: Vec<String>,
}

/// Object summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSummary {
    pub total_objects: usize,
    pub total_memory: usize,
    pub objects_by_type: HashMap<ObjectType, usize>,
    pub average_age: Duration,
    pub oldest_object_age: Duration,
    pub reference_density: f64,
}

/// Smart pointer with leak detection
pub struct TrackedArc<T> {
    inner: Arc<T>,
    object_id: ObjectId,
    _phantom: PhantomData<T>,
}

impl<T> TrackedArc<T> {
    pub fn new(value: T, object_type: ObjectType) -> Self {
        let inner = Arc::new(value);
        let object_id = ObjectId::new();

        // Register with leak detector
        LEAK_DETECTOR.register_object(object_id, object_type, std::mem::size_of::<T>(), None);

        Self {
            inner,
            object_id,
            _phantom: PhantomData,
        }
    }

    pub fn object_id(&self) -> ObjectId {
        self.object_id
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    pub fn weak_count(&self) -> usize {
        Arc::weak_count(&self.inner)
    }
}

impl<T> Clone for TrackedArc<T> {
    fn clone(&self) -> Self {
        LEAK_DETECTOR.update_ref_count(self.object_id, self.strong_count() + 1, self.weak_count());

        Self {
            inner: self.inner.clone(),
            object_id: self.object_id,
            _phantom: PhantomData,
        }
    }
}

impl<T> Drop for TrackedArc<T> {
    fn drop(&mut self) {
        let new_strong_count = Arc::strong_count(&self.inner) - 1;
        LEAK_DETECTOR.update_ref_count(self.object_id, new_strong_count, self.weak_count());

        if new_strong_count == 0 {
            LEAK_DETECTOR.unregister_object(self.object_id);
        }
    }
}

impl<T> std::ops::Deref for TrackedArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Weak reference with leak detection
pub struct TrackedWeak<T> {
    inner: Weak<T>,
    object_id: ObjectId,
    _phantom: PhantomData<T>,
}

impl<T> TrackedWeak<T> {
    pub fn upgrade(&self) -> Option<TrackedArc<T>> {
        self.inner.upgrade().map(|inner| {
            LEAK_DETECTOR.update_ref_count(
                self.object_id,
                Arc::strong_count(&inner),
                Arc::weak_count(&inner),
            );

            TrackedArc {
                inner,
                object_id: self.object_id,
                _phantom: PhantomData,
            }
        })
    }
}

impl<T> Clone for TrackedWeak<T> {
    fn clone(&self) -> Self {
        let weak_count = self.inner.weak_count() + 1;
        LEAK_DETECTOR.update_ref_count(self.object_id, self.inner.strong_count(), weak_count);

        Self {
            inner: self.inner.clone(),
            object_id: self.object_id,
            _phantom: PhantomData,
        }
    }
}

impl<T> Drop for TrackedWeak<T> {
    fn drop(&mut self) {
        let weak_count = self.inner.weak_count().saturating_sub(1);
        LEAK_DETECTOR.update_ref_count(self.object_id, self.inner.strong_count(), weak_count);
    }
}

/// Main leak detector
pub struct LeakDetector {
    config: RwLock<LeakDetectionConfig>,
    tracked_objects: DashMap<ObjectId, TrackedObject>,
    object_references: DashMap<ObjectId, Vec<ObjectReference>>,

    // Scanning state
    scan_thread: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown_flag: Arc<AtomicBool>,
    last_scan: Mutex<Option<SystemTime>>,

    // Statistics
    total_scans: AtomicU64,
    total_leaks_detected: AtomicU64,
    total_objects_tracked: AtomicUsize,
}

impl Default for LeakDetector {
    fn default() -> Self {
        Self {
            config: RwLock::new(LeakDetectionConfig::default()),
            tracked_objects: DashMap::new(),
            object_references: DashMap::new(),
            scan_thread: Mutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            last_scan: Mutex::new(None),
            total_scans: AtomicU64::new(0),
            total_leaks_detected: AtomicU64::new(0),
            total_objects_tracked: AtomicUsize::new(0),
        }
    }
}

impl LeakDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure leak detection
    pub fn configure(&self, config: LeakDetectionConfig) {
        let mut cfg = self.config.write().unwrap();
        *cfg = config;
    }

    /// Start leak detection using the global LEAK_DETECTOR instance
    pub fn start(&self) {
        info!("Starting leak detector");

        let config = self.config.read().unwrap().clone();
        let shutdown_flag = self.shutdown_flag.clone();

        let handle = thread::Builder::new()
            .name("leak-detector".to_string())
            .spawn(move || {
                Self::scanning_loop(shutdown_flag, config);
            })
            .expect("Failed to start leak detection thread");

        *self.scan_thread.lock().unwrap() = Some(handle);
    }

    /// Stop leak detection
    pub fn stop(&self) {
        info!("Stopping leak detector");

        self.shutdown_flag.store(true, Ordering::SeqCst);

        if let Ok(mut guard) = self.scan_thread.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }

        self.shutdown_flag.store(false, Ordering::SeqCst);
    }

    /// Register a new object for tracking
    pub fn register_object(
        &self,
        id: ObjectId,
        object_type: ObjectType,
        size: usize,
        location: Option<String>,
    ) {
        let config = self.config.read().unwrap();

        // Check capacity
        if self.tracked_objects.len() >= config.max_tracked_objects {
            self.cleanup_old_objects();
        }

        let now = SystemTime::now();
        let tracked_object = TrackedObject {
            id,
            object_type,
            size,
            created_at: now,
            last_accessed: now,
            strong_ref_count: 1,
            weak_ref_count: 0,
            creation_location: location,
            references_to: Vec::new(),
            references_from: Vec::new(),
        };

        self.tracked_objects.insert(id, tracked_object);
        self.total_objects_tracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Unregister an object
    pub fn unregister_object(&self, id: ObjectId) {
        if self.tracked_objects.remove(&id).is_some() {
            self.object_references.remove(&id);
            self.total_objects_tracked.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Update reference count for an object
    pub fn update_ref_count(&self, id: ObjectId, strong_count: usize, weak_count: usize) {
        if let Some(mut object) = self.tracked_objects.get_mut(&id) {
            object.strong_ref_count = strong_count;
            object.weak_ref_count = weak_count;
            object.last_accessed = SystemTime::now();
        }
    }

    /// Add a reference between objects
    pub fn add_reference(
        &self,
        from: ObjectId,
        to: ObjectId,
        ref_type: ReferenceType,
        location: Option<String>,
    ) {
        let reference = ObjectReference {
            from_object: from,
            to_object: to,
            reference_type: ref_type,
            created_at: SystemTime::now(),
            location,
        };

        // Add to from object's references_to
        if let Some(mut from_obj) = self.tracked_objects.get_mut(&from) {
            from_obj.references_to.push(reference.clone());
        }

        // Add to to object's references_from
        if let Some(mut to_obj) = self.tracked_objects.get_mut(&to) {
            to_obj.references_from.push(reference);
        }
    }

    /// Remove a reference between objects
    pub fn remove_reference(&self, from: ObjectId, to: ObjectId) {
        if let Some(mut from_obj) = self.tracked_objects.get_mut(&from) {
            from_obj.references_to.retain(|r| r.to_object != to);
        }

        if let Some(mut to_obj) = self.tracked_objects.get_mut(&to) {
            to_obj.references_from.retain(|r| r.from_object != from);
        }
    }

    /// Perform a comprehensive leak detection scan
    pub fn scan_for_leaks(&self) -> LeakReport {
        let start_time = SystemTime::now();
        let scan_start = SystemTime::now();

        info!("Starting leak detection scan");

        let mut leaks = Vec::new();
        let objects_scanned = self.tracked_objects.len();

        let config = self.config.read().unwrap();

        // 1. Detect circular references
        if config.detect_cycles {
            leaks.extend(self.detect_circular_references());
        }

        // 2. Detect orphaned objects
        if config.detect_orphans {
            leaks.extend(self.detect_orphaned_objects(&config));
        }

        // 3. Validate reference counts
        if config.validate_ref_counts {
            leaks.extend(self.validate_reference_counts(&config));
        }

        // 4. Check for weak reference issues
        leaks.extend(self.detect_weak_reference_issues());

        let scan_duration = start_time.elapsed();
        let object_summary = self.generate_object_summary();
        let recommendations = self.generate_recommendations(&leaks);

        self.total_scans.fetch_add(1, Ordering::Relaxed);
        self.total_leaks_detected
            .fetch_add(leaks.len() as u64, Ordering::Relaxed);
        *self.last_scan.lock().unwrap() = Some(start_time);

        info!(
            "Leak detection scan completed in {:?}, found {} potential leaks",
            scan_duration,
            leaks.len()
        );

        if !leaks.is_empty() {
            warn!("Detected {} potential memory leaks", leaks.len());
            for leak in &leaks {
                warn!("Leak detected: {:?}", leak);
            }
        }

        LeakReport {
            timestamp: scan_start,
            scan_duration: scan_start.elapsed().unwrap_or_default(),
            objects_scanned,
            leaks_detected: leaks,
            object_summary,
            recommendations,
        }
    }

    /// Get detection statistics
    pub fn get_statistics(&self) -> LeakDetectionStats {
        LeakDetectionStats {
            total_objects_tracked: self.total_objects_tracked.load(Ordering::Relaxed),
            total_scans_performed: self.total_scans.load(Ordering::Relaxed),
            total_leaks_detected: self.total_leaks_detected.load(Ordering::Relaxed),
            last_scan_time: *self.last_scan.lock().unwrap(),
            objects_by_type: self.get_objects_by_type(),
        }
    }

    // Private implementation methods

    fn scanning_loop(shutdown_flag: Arc<AtomicBool>, config: LeakDetectionConfig) {
        while !shutdown_flag.load(Ordering::Relaxed) {
            let _report = LEAK_DETECTOR.scan_for_leaks();
            thread::sleep(config.scan_interval);
        }
    }

    fn detect_circular_references(&self) -> Vec<LeakType> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut stack = Vec::new();

        for entry in self.tracked_objects.iter() {
            let object_id = *entry.key();
            if !visited.contains(&object_id) {
                if let Some(cycle) =
                    self.dfs_cycle_detection(object_id, &mut visited, &mut in_stack, &mut stack)
                {
                    let total_size = cycle
                        .iter()
                        .filter_map(|id| self.tracked_objects.get(id))
                        .map(|obj| obj.size)
                        .sum();

                    cycles.push(LeakType::CircularReference { cycle, total_size });
                }
            }
        }

        cycles
    }

    fn dfs_cycle_detection(
        &self,
        current: ObjectId,
        visited: &mut HashSet<ObjectId>,
        in_stack: &mut HashSet<ObjectId>,
        stack: &mut Vec<ObjectId>,
    ) -> Option<Vec<ObjectId>> {
        visited.insert(current);
        in_stack.insert(current);
        stack.push(current);

        if let Some(object) = self.tracked_objects.get(&current) {
            for reference in &object.references_to {
                let next = reference.to_object;

                if in_stack.contains(&next) {
                    // Found cycle - extract it from stack
                    let cycle_start = stack.iter().position(|&id| id == next)?;
                    return Some(stack[cycle_start..].to_vec());
                }

                if !visited.contains(&next) {
                    if let Some(cycle) = self.dfs_cycle_detection(next, visited, in_stack, stack) {
                        return Some(cycle);
                    }
                }
            }
        }

        in_stack.remove(&current);
        stack.pop();
        None
    }

    fn detect_orphaned_objects(&self, config: &LeakDetectionConfig) -> Vec<LeakType> {
        let mut orphans = Vec::new();
        let now = SystemTime::now();

        for entry in self.tracked_objects.iter() {
            let object = entry.value();

            if let Ok(age) = now.duration_since(object.created_at) {
                if age > config.max_object_age
                    && object.references_from.is_empty()
                    && object.strong_ref_count <= 1
                {
                    orphans.push(LeakType::OrphanedObject {
                        object: object.id,
                        age,
                        size: object.size,
                    });
                }
            }
        }

        orphans
    }

    fn validate_reference_counts(&self, config: &LeakDetectionConfig) -> Vec<LeakType> {
        let mut mismatches = Vec::new();

        for entry in self.tracked_objects.iter() {
            let object = entry.value();

            if object.strong_ref_count >= config.min_ref_count_threshold {
                let expected_refs = object
                    .references_from
                    .iter()
                    .filter(|r| {
                        matches!(
                            r.reference_type,
                            ReferenceType::Strong | ReferenceType::Shared
                        )
                    })
                    .count();

                if expected_refs != object.strong_ref_count && expected_refs > 0 {
                    mismatches.push(LeakType::RefCountMismatch {
                        object: object.id,
                        expected: expected_refs,
                        actual: object.strong_ref_count,
                    });
                }
            }
        }

        mismatches
    }

    fn detect_weak_reference_issues(&self) -> Vec<LeakType> {
        let mut issues = Vec::new();

        for entry in self.tracked_objects.iter() {
            let object = entry.value();

            // Check for objects with many weak references but no strong references
            if object.weak_ref_count > 10 && object.strong_ref_count == 0 {
                issues.push(LeakType::WeakReferenceIssue {
                    object: object.id,
                    weak_refs: object.weak_ref_count,
                    strong_refs: object.strong_ref_count,
                });
            }
        }

        issues
    }

    fn generate_object_summary(&self) -> ObjectSummary {
        let mut total_memory = 0;
        let mut total_age = Duration::ZERO;
        let mut oldest_age = Duration::ZERO;
        let mut objects_by_type = HashMap::new();
        let mut reference_count = 0;

        let now = SystemTime::now();
        let object_count = self.tracked_objects.len();

        for entry in self.tracked_objects.iter() {
            let object = entry.value();

            total_memory += object.size;
            reference_count += object.references_to.len();

            *objects_by_type
                .entry(object.object_type.clone())
                .or_insert(0) += 1;

            if let Ok(age) = now.duration_since(object.created_at) {
                total_age += age;
                if age > oldest_age {
                    oldest_age = age;
                }
            }
        }

        let average_age = if object_count > 0 {
            total_age / object_count as u32
        } else {
            Duration::ZERO
        };

        let reference_density = if object_count > 0 {
            reference_count as f64 / object_count as f64
        } else {
            0.0
        };

        ObjectSummary {
            total_objects: object_count,
            total_memory,
            objects_by_type,
            average_age,
            oldest_object_age: oldest_age,
            reference_density,
        }
    }

    fn generate_recommendations(&self, leaks: &[LeakType]) -> Vec<String> {
        let mut recommendations = Vec::new();

        let cycle_count = leaks
            .iter()
            .filter(|l| matches!(l, LeakType::CircularReference { .. }))
            .count();
        let orphan_count = leaks
            .iter()
            .filter(|l| matches!(l, LeakType::OrphanedObject { .. }))
            .count();
        let ref_count_issues = leaks
            .iter()
            .filter(|l| matches!(l, LeakType::RefCountMismatch { .. }))
            .count();

        if cycle_count > 0 {
            recommendations.push(format!(
                "Found {} circular references. Consider using weak references to break cycles.",
                cycle_count
            ));
        }

        if orphan_count > 0 {
            recommendations.push(format!(
                "Found {} orphaned objects. Review object lifetime management and cleanup procedures.",
                orphan_count
            ));
        }

        if ref_count_issues > 0 {
            recommendations.push(format!(
                "Found {} reference count mismatches. Audit reference handling code.",
                ref_count_issues
            ));
        }

        if recommendations.is_empty() {
            recommendations
                .push("No significant memory leaks detected. Continue monitoring.".to_string());
        }

        recommendations
    }

    fn cleanup_old_objects(&self) {
        let now = SystemTime::now();
        let mut to_remove = Vec::new();

        // Remove objects older than 1 hour that have no references
        for entry in self.tracked_objects.iter() {
            let object = entry.value();

            if let Ok(age) = now.duration_since(object.created_at) {
                if age > Duration::from_secs(3600)
                    && object.references_from.is_empty()
                    && object.strong_ref_count == 0
                {
                    to_remove.push(object.id);
                }
            }
        }

        for id in to_remove {
            self.tracked_objects.remove(&id);
            self.object_references.remove(&id);
        }
    }

    fn get_objects_by_type(&self) -> HashMap<ObjectType, usize> {
        let mut counts = HashMap::new();

        for entry in self.tracked_objects.iter() {
            *counts.entry(entry.value().object_type.clone()).or_insert(0) += 1;
        }

        counts
    }
}

impl Drop for LeakDetector {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Leak detection statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct LeakDetectionStats {
    pub total_objects_tracked: usize,
    pub total_scans_performed: u64,
    pub total_leaks_detected: u64,
    pub last_scan_time: Option<SystemTime>,
    pub objects_by_type: HashMap<ObjectType, usize>,
}

/// Initialize leak detection
pub fn init_leak_detection(config: LeakDetectionConfig) {
    LEAK_DETECTOR.configure(config);
    LEAK_DETECTOR.start();
}

/// Shutdown leak detection
pub fn shutdown_leak_detection() {
    LEAK_DETECTOR.stop();
}

/// Get global leak detector instance
pub fn get_leak_detector() -> &'static Arc<LeakDetector> {
    &LEAK_DETECTOR
}

/// Create a tracked Arc
pub fn tracked_arc<T>(value: T, object_type: ObjectType) -> TrackedArc<T> {
    TrackedArc::new(value, object_type)
}

/// Create a tracked weak reference
pub fn tracked_weak<T>(arc: &TrackedArc<T>) -> TrackedWeak<T> {
    TrackedWeak {
        inner: Arc::downgrade(&arc.inner),
        object_id: arc.object_id,
        _phantom: PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leak_detector_creation() {
        let detector = LeakDetector::new();
        assert_eq!(detector.tracked_objects.len(), 0);
    }

    #[test]
    fn test_object_registration() {
        let detector = LeakDetector::new();
        let id = ObjectId::new();

        detector.register_object(id, ObjectType::BTreeNode, 1024, None);
        assert!(detector.tracked_objects.contains_key(&id));

        detector.unregister_object(id);
        assert!(!detector.tracked_objects.contains_key(&id));
    }

    #[test]
    fn test_circular_reference_detection() {
        let detector = LeakDetector::new();

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();

        detector.register_object(id1, ObjectType::BTreeNode, 1024, None);
        detector.register_object(id2, ObjectType::BTreeNode, 1024, None);

        // Create circular reference
        detector.add_reference(id1, id2, ReferenceType::Strong, None);
        detector.add_reference(id2, id1, ReferenceType::Strong, None);

        let leaks = detector.detect_circular_references();
        assert!(!leaks.is_empty());

        if let LeakType::CircularReference { cycle, .. } = &leaks[0] {
            assert!(cycle.contains(&id1) || cycle.contains(&id2));
        }
    }

    #[test]
    fn test_tracked_arc() {
        let arc = tracked_arc(42, ObjectType::Custom("test".to_string()));
        assert_eq!(*arc, 42);
        assert_eq!(arc.strong_count(), 1);

        let weak = tracked_weak(&arc);
        assert!(weak.upgrade().is_some());

        drop(arc);
        assert!(weak.upgrade().is_none());
    }
}

use crate::core::error::{Error, Result};
use parking_lot::{RwLock, Mutex};
use std::collections::{HashMap, BTreeMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::mem;
use std::ptr;
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn, error};

const MAX_SERIES_POINTS: usize = 10000;
const MAX_LABEL_CACHE_SIZE: usize = 1000;
const COMPRESSION_THRESHOLD: usize = 100;
const MEMORY_POOL_SIZE: usize = 1024 * 1024; // 1MB chunks

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CompactMetricPoint {
    pub timestamp: u32,
    pub value: f32,
    pub label_id: u16,
}

impl CompactMetricPoint {
    pub fn new(timestamp: u64, value: f64, label_id: u16) -> Self {
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (365 * 24 * 60 * 60);
        
        Self {
            timestamp: (timestamp - base_timestamp) as u32,
            value: value as f32,
            label_id,
        }
    }
    
    pub fn get_timestamp(&self, base_timestamp: u64) -> u64 {
        base_timestamp + self.timestamp as u64
    }
}

pub struct CircularBuffer<T> {
    buffer: Vec<T>,
    head: AtomicUsize,
    tail: AtomicUsize,
    capacity: usize,
}

impl<T: Clone + Default> CircularBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![T::default(); capacity],
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            capacity,
        }
    }
    
    pub fn push(&self, item: T) {
        let tail = self.tail.fetch_add(1, Ordering::AcqRel) % self.capacity;
        unsafe {
            let ptr = self.buffer.as_ptr() as *mut T;
            ptr::write(ptr.add(tail), item);
        }
        
        let head = self.head.load(Ordering::Acquire);
        if tail == head {
            self.head.fetch_add(1, Ordering::Release);
        }
    }
    
    pub fn iter(&self) -> CircularBufferIterator<T> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        
        CircularBufferIterator {
            buffer: &self.buffer,
            current: head,
            end: tail,
            capacity: self.capacity,
        }
    }
    
    pub fn clear(&self) {
        self.head.store(0, Ordering::Release);
        self.tail.store(0, Ordering::Release);
    }
    
    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        
        if tail >= head {
            tail - head
        } else {
            self.capacity - head + tail
        }
    }
}

pub struct CircularBufferIterator<'a, T> {
    buffer: &'a Vec<T>,
    current: usize,
    end: usize,
    capacity: usize,
}

impl<'a, T: Clone> Iterator for CircularBufferIterator<'a, T> {
    type Item = T;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }
        
        let item = self.buffer[self.current].clone();
        self.current = (self.current + 1) % self.capacity;
        Some(item)
    }
}

pub struct LabelCache {
    labels_to_id: Arc<RwLock<HashMap<Vec<(String, String)>, u16>>>,
    id_to_labels: Arc<RwLock<Vec<Vec<(String, String)>>>>,
    next_id: AtomicU16,
}

impl LabelCache {
    pub fn new() -> Self {
        Self {
            labels_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_labels: Arc::new(RwLock::new(Vec::new())),
            next_id: AtomicU16::new(0),
        }
    }
    
    pub fn get_or_create_id(&self, labels: HashMap<String, String>) -> u16 {
        let mut sorted_labels: Vec<(String, String)> = labels.into_iter().collect();
        sorted_labels.sort_by(|a, b| a.0.cmp(&b.0));
        
        {
            let cache = self.labels_to_id.read();
            if let Some(&id) = cache.get(&sorted_labels) {
                return id;
            }
        }
        
        let mut cache = self.labels_to_id.write();
        let mut id_cache = self.id_to_labels.write();
        
        if cache.len() >= MAX_LABEL_CACHE_SIZE {
            cache.clear();
            id_cache.clear();
            self.next_id.store(0, Ordering::Release);
        }
        
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        cache.insert(sorted_labels.clone(), id);
        id_cache.push(sorted_labels);
        
        id
    }
    
    pub fn get_labels(&self, id: u16) -> Option<Vec<(String, String)>> {
        let cache = self.id_to_labels.read();
        cache.get(id as usize).cloned()
    }
}

pub struct CompressedTimeSeries {
    name: String,
    points: CircularBuffer<CompactMetricPoint>,
    base_timestamp: u64,
    delta_encoder: DeltaEncoder,
    aggregation: AggregationType,
}

impl CompressedTimeSeries {
    pub fn new(name: String, capacity: usize, aggregation: AggregationType) -> Self {
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - (365 * 24 * 60 * 60);
        
        Self {
            name,
            points: CircularBuffer::new(capacity),
            base_timestamp,
            delta_encoder: DeltaEncoder::new(),
            aggregation,
        }
    }
    
    pub fn add_point(&self, timestamp: u64, value: f64, label_id: u16) {
        let point = CompactMetricPoint::new(timestamp, value, label_id);
        self.points.push(point);
    }
    
    pub fn get_compressed_data(&self) -> Vec<u8> {
        self.delta_encoder.encode_series(&self.points)
    }
    
    pub fn decompress_range(&self, start: u64, end: u64) -> Vec<(u64, f64, u16)> {
        let mut result = Vec::new();
        
        for point in self.points.iter() {
            let timestamp = point.get_timestamp(self.base_timestamp);
            if timestamp >= start && timestamp <= end {
                result.push((timestamp, point.value as f64, point.label_id));
            }
        }
        
        result
    }
    
    pub fn aggregate(&self, window: Duration) -> Vec<(u64, f64)> {
        let mut aggregated = Vec::new();
        let window_secs = window.as_secs();
        let mut current_window = 0u64;
        let mut window_values = Vec::new();
        
        for point in self.points.iter() {
            let timestamp = point.get_timestamp(self.base_timestamp);
            let window_id = timestamp / window_secs;
            
            if window_id != current_window && !window_values.is_empty() {
                let agg_value = self.apply_aggregation(&window_values);
                aggregated.push((current_window * window_secs, agg_value));
                window_values.clear();
            }
            
            current_window = window_id;
            window_values.push(point.value as f64);
        }
        
        if !window_values.is_empty() {
            let agg_value = self.apply_aggregation(&window_values);
            aggregated.push((current_window * window_secs, agg_value));
        }
        
        aggregated
    }
    
    fn apply_aggregation(&self, values: &[f64]) -> f64 {
        match self.aggregation {
            AggregationType::Sum => values.iter().sum(),
            AggregationType::Average => values.iter().sum::<f64>() / values.len() as f64,
            AggregationType::Min => values.iter().fold(f64::MAX, |a, &b| a.min(b)),
            AggregationType::Max => values.iter().fold(f64::MIN, |a, &b| a.max(b)),
            AggregationType::Count => values.len() as f64,
            AggregationType::Percentile(p) => {
                let mut sorted = values.to_vec();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let idx = ((sorted.len() as f64 - 1.0) * p / 100.0) as usize;
                sorted[idx]
            }
        }
    }
}

struct DeltaEncoder {
    last_timestamp: AtomicU32,
    last_value: AtomicU32,
}

impl DeltaEncoder {
    fn new() -> Self {
        Self {
            last_timestamp: AtomicU32::new(0),
            last_value: AtomicU32::new(0),
        }
    }
    
    fn encode_series(&self, buffer: &CircularBuffer<CompactMetricPoint>) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut last_timestamp = 0u32;
        let mut last_value = 0f32;
        
        for point in buffer.iter() {
            let timestamp_delta = point.timestamp.wrapping_sub(last_timestamp);
            let value_delta = point.value - last_value;
            
            self.encode_varint(timestamp_delta, &mut encoded);
            self.encode_float_delta(value_delta, &mut encoded);
            encoded.extend_from_slice(&point.label_id.to_le_bytes());
            
            last_timestamp = point.timestamp;
            last_value = point.value;
        }
        
        encoded
    }
    
    fn encode_varint(&self, mut value: u32, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }
    
    fn encode_float_delta(&self, delta: f32, output: &mut Vec<u8>) {
        let bits = delta.to_bits();
        if bits == 0 {
            output.push(0);
        } else {
            output.push(1);
            output.extend_from_slice(&bits.to_le_bytes());
        }
    }
}

pub struct MemoryPool {
    chunks: Arc<RwLock<Vec<Vec<u8>>>>,
    available: Arc<RwLock<Vec<usize>>>,
    chunk_size: usize,
}

impl MemoryPool {
    pub fn new(num_chunks: usize, chunk_size: usize) -> Self {
        let mut chunks = Vec::with_capacity(num_chunks);
        let mut available = Vec::with_capacity(num_chunks);
        
        for i in 0..num_chunks {
            chunks.push(vec![0u8; chunk_size]);
            available.push(i);
        }
        
        Self {
            chunks: Arc::new(RwLock::new(chunks)),
            available: Arc::new(RwLock::new(available)),
            chunk_size,
        }
    }
    
    pub fn allocate(&self) -> Option<PooledBuffer> {
        let mut available = self.available.write();
        
        if let Some(idx) = available.pop() {
            Some(PooledBuffer {
                pool: self.chunks.clone(),
                available: self.available.clone(),
                index: idx,
                size: self.chunk_size,
            })
        } else {
            None
        }
    }
}

pub struct PooledBuffer {
    pool: Arc<RwLock<Vec<Vec<u8>>>>,
    available: Arc<RwLock<Vec<usize>>>,
    index: usize,
    size: usize,
}

impl PooledBuffer {
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            let pool = self.pool.read();
            std::slice::from_raw_parts(pool[self.index].as_ptr(), self.size)
        }
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            let pool = self.pool.read();
            std::slice::from_raw_parts_mut(pool[self.index].as_ptr() as *mut u8, self.size)
        }
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        let mut available = self.available.write();
        available.push(self.index);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AggregationType {
    Sum,
    Average,
    Min,
    Max,
    Count,
    Percentile(f64),
}

pub struct OptimizedMetricsCollector {
    series: Arc<RwLock<HashMap<String, Arc<CompressedTimeSeries>>>>,
    label_cache: Arc<LabelCache>,
    memory_pool: Arc<MemoryPool>,
    retention: Duration,
    flush_interval: Duration,
    last_flush: Arc<Mutex<Instant>>,
    memory_usage: Arc<AtomicUsize>,
    max_memory: usize,
}

impl OptimizedMetricsCollector {
    pub fn new(retention: Duration, flush_interval: Duration, max_memory: usize) -> Self {
        let num_chunks = max_memory / MEMORY_POOL_SIZE;
        
        Self {
            series: Arc::new(RwLock::new(HashMap::new())),
            label_cache: Arc::new(LabelCache::new()),
            memory_pool: Arc::new(MemoryPool::new(num_chunks, MEMORY_POOL_SIZE)),
            retention,
            flush_interval,
            last_flush: Arc::new(Mutex::new(Instant::now())),
            memory_usage: Arc::new(AtomicUsize::new(0)),
            max_memory,
        }
    }
    
    pub fn record_metric(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let label_id = self.label_cache.get_or_create_id(labels);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let mut series_map = self.series.write();
        let series = series_map.entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(CompressedTimeSeries::new(
                    name.to_string(),
                    MAX_SERIES_POINTS,
                    AggregationType::Average,
                ))
            });
        
        series.add_point(timestamp, value, label_id);
        
        self.check_memory_usage();
    }
    
    fn check_memory_usage(&self) {
        let current_usage = self.memory_usage.load(Ordering::Acquire);
        
        if current_usage > self.max_memory {
            self.evict_old_data();
        }
    }
    
    fn evict_old_data(&self) {
        let mut series_map = self.series.write();
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() - self.retention.as_secs();
        
        series_map.retain(|_, series| {
            let points_count = series.points.len();
            points_count > 0
        });
        
        self.label_cache.labels_to_id.write().clear();
        self.label_cache.id_to_labels.write().clear();
        self.label_cache.next_id.store(0, Ordering::Release);
    }
    
    pub fn query_range(
        &self,
        metric_name: &str,
        start: u64,
        end: u64,
    ) -> Vec<(u64, f64, HashMap<String, String>)> {
        let series_map = self.series.read();
        
        if let Some(series) = series_map.get(metric_name) {
            let mut result = Vec::new();
            
            for (timestamp, value, label_id) in series.decompress_range(start, end) {
                let labels = self.label_cache.get_labels(label_id)
                    .map(|l| l.into_iter().collect())
                    .unwrap_or_default();
                
                result.push((timestamp, value, labels));
            }
            
            result
        } else {
            Vec::new()
        }
    }
    
    pub fn aggregate_metric(
        &self,
        metric_name: &str,
        window: Duration,
    ) -> Vec<(u64, f64)> {
        let series_map = self.series.read();
        
        if let Some(series) = series_map.get(metric_name) {
            series.aggregate(window)
        } else {
            Vec::new()
        }
    }
    
    pub fn get_memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Acquire)
    }
    
    pub fn flush_to_disk(&self, path: &str) -> Result<()> {
        let series_map = self.series.read();
        let mut compressed_data = Vec::new();
        
        for (name, series) in series_map.iter() {
            let data = series.get_compressed_data();
            compressed_data.extend_from_slice(name.as_bytes());
            compressed_data.extend_from_slice(&(data.len() as u32).to_le_bytes());
            compressed_data.extend_from_slice(&data);
        }
        
        std::fs::write(path, compressed_data)
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        *self.last_flush.lock() = Instant::now();
        
        Ok(())
    }
}

pub struct OptimizedDashboardManager {
    dashboards: Arc<RwLock<HashMap<String, Dashboard>>>,
    metrics_collector: Arc<OptimizedMetricsCollector>,
    alert_manager: Arc<AlertManager>,
    update_channel: broadcast::Sender<DashboardUpdate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub title: String,
    pub panels: Vec<Panel>,
    pub refresh_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Panel {
    pub id: String,
    pub title: String,
    pub metric: String,
    pub aggregation: AggregationType,
    pub window: Duration,
}

#[derive(Debug, Clone)]
pub struct DashboardUpdate {
    pub dashboard_id: String,
    pub panel_id: String,
    pub data: Vec<(u64, f64)>,
}

struct AlertManager {
    alerts: RwLock<Vec<Alert>>,
}

#[derive(Debug, Clone)]
struct Alert {
    metric: String,
    threshold: f64,
    condition: AlertCondition,
}

#[derive(Debug, Clone)]
enum AlertCondition {
    Above,
    Below,
    Equal,
}

use std::sync::atomic::AtomicU16;

impl OptimizedDashboardManager {
    pub fn new(max_memory: usize) -> Self {
        let (tx, _rx) = broadcast::channel(1000);
        
        Self {
            dashboards: Arc::new(RwLock::new(HashMap::new())),
            metrics_collector: Arc::new(OptimizedMetricsCollector::new(
                Duration::from_secs(3600),
                Duration::from_secs(60),
                max_memory,
            )),
            alert_manager: Arc::new(AlertManager {
                alerts: RwLock::new(Vec::new()),
            }),
            update_channel: tx,
        }
    }
    
    pub fn create_dashboard(&self, dashboard: Dashboard) -> Result<()> {
        let mut dashboards = self.dashboards.write();
        dashboards.insert(dashboard.id.clone(), dashboard);
        Ok(())
    }
    
    pub fn update_dashboard(&self, dashboard_id: &str) -> Result<()> {
        let dashboards = self.dashboards.read();
        
        if let Some(dashboard) = dashboards.get(dashboard_id) {
            for panel in &dashboard.panels {
                let data = self.metrics_collector.aggregate_metric(
                    &panel.metric,
                    panel.window,
                );
                
                let update = DashboardUpdate {
                    dashboard_id: dashboard_id.to_string(),
                    panel_id: panel.id.clone(),
                    data,
                };
                
                let _ = self.update_channel.send(update);
            }
        }
        
        Ok(())
    }
    
    pub fn subscribe_to_updates(&self) -> broadcast::Receiver<DashboardUpdate> {
        self.update_channel.subscribe()
    }
    
    pub fn record_metric(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        self.metrics_collector.record_metric(name, value, labels);
    }
    
    pub fn get_memory_usage(&self) -> usize {
        self.metrics_collector.get_memory_usage()
    }
}

impl Default for CompactMetricPoint {
    fn default() -> Self {
        Self {
            timestamp: 0,
            value: 0.0,
            label_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_circular_buffer() {
        let buffer: CircularBuffer<i32> = CircularBuffer::new(5);
        
        for i in 0..10 {
            buffer.push(i);
        }
        
        let items: Vec<i32> = buffer.iter().collect();
        assert_eq!(items, vec![5, 6, 7, 8, 9]);
    }
    
    #[test]
    fn test_label_cache() {
        let cache = LabelCache::new();
        
        let mut labels1 = HashMap::new();
        labels1.insert("host".to_string(), "server1".to_string());
        labels1.insert("env".to_string(), "prod".to_string());
        
        let id1 = cache.get_or_create_id(labels1.clone());
        let id2 = cache.get_or_create_id(labels1);
        
        assert_eq!(id1, id2);
        
        let retrieved = cache.get_labels(id1).unwrap();
        assert_eq!(retrieved.len(), 2);
    }
    
    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new(10, 1024);
        
        let mut buffers = Vec::new();
        for _ in 0..10 {
            if let Some(buffer) = pool.allocate() {
                buffers.push(buffer);
            }
        }
        
        assert_eq!(buffers.len(), 10);
        assert!(pool.allocate().is_none());
        
        drop(buffers);
        
        assert!(pool.allocate().is_some());
    }
}
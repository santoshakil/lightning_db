use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use parking_lot::{RwLock, Mutex};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::interval;
use crossbeam_queue::ArrayQueue;
use crate::core::error::Error;
use super::engine::{DataPoint, TimeSeriesEngine};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const CHANNEL_BUFFER_SIZE: usize = 100000;
const MAX_CONCURRENT_FLUSHES: usize = 4;

#[derive(Debug, Clone)]
pub struct IngestionConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub channel_buffer_size: usize,
    pub max_concurrent_flushes: usize,
    pub enable_deduplication: bool,
    pub enable_validation: bool,
    pub max_out_of_order_delay: Duration,
    pub enable_backpressure: bool,
    pub backpressure_threshold: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            channel_buffer_size: CHANNEL_BUFFER_SIZE,
            max_concurrent_flushes: MAX_CONCURRENT_FLUSHES,
            enable_deduplication: true,
            enable_validation: true,
            max_out_of_order_delay: Duration::from_secs(60),
            enable_backpressure: true,
            backpressure_threshold: CHANNEL_BUFFER_SIZE / 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestionPoint {
    pub series_id: String,
    pub timestamp: u64,
    pub value: f64,
    pub tags: HashMap<String, String>,
}

impl IngestionPoint {
    pub fn new(series_id: String, timestamp: u64, value: f64) -> Self {
        Self {
            series_id,
            timestamp,
            value,
            tags: HashMap::new(),
        }
    }

    pub fn with_tags(series_id: String, timestamp: u64, value: f64, tags: HashMap<String, String>) -> Self {
        Self {
            series_id,
            timestamp,
            value,
            tags,
        }
    }

    fn to_data_point(&self) -> DataPoint {
        let tag_hash = self.hash_tags();
        DataPoint::with_tags(self.timestamp, self.value, tag_hash)
    }

    fn hash_tags(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        for (k, v) in &self.tags {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

struct SeriesBatch {
    series_id: String,
    points: Vec<DataPoint>,
    last_update: Instant,
    size_bytes: usize,
}

impl SeriesBatch {
    fn new(series_id: String) -> Self {
        Self {
            series_id,
            points: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            last_update: Instant::now(),
            size_bytes: 0,
        }
    }

    fn add_point(&mut self, point: DataPoint) {
        self.points.push(point);
        self.size_bytes += std::mem::size_of::<DataPoint>();
        self.last_update = Instant::now();
    }

    fn should_flush(&self, config: &IngestionConfig) -> bool {
        self.points.len() >= config.batch_size ||
        self.last_update.elapsed() >= config.flush_interval
    }

    fn clear(&mut self) {
        self.points.clear();
        self.size_bytes = 0;
        self.last_update = Instant::now();
    }
}

pub struct IngestionPipeline {
    config: Arc<IngestionConfig>,
    engine: Arc<TimeSeriesEngine>,
    sender: mpsc::Sender<IngestionPoint>,
    batches: Arc<DashMap<String, Arc<Mutex<SeriesBatch>>>>,
    stats: Arc<IngestionStatistics>,
    shutdown: Arc<AtomicBool>,
    flush_semaphore: Arc<Semaphore>,
    dedup_cache: Arc<DashMap<(String, u64), Instant>>,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
}

struct IngestionStatistics {
    total_points: AtomicU64,
    total_batches: AtomicU64,
    duplicate_points: AtomicU64,
    invalid_points: AtomicU64,
    out_of_order_points: AtomicU64,
    flush_errors: AtomicU64,
    bytes_ingested: AtomicU64,
    last_flush_time: RwLock<Instant>,
}

impl IngestionPipeline {
    pub async fn new(
        config: IngestionConfig,
        engine: Arc<TimeSeriesEngine>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::channel(config.channel_buffer_size);
        
        let pipeline = Self {
            config: Arc::new(config.clone()),
            engine: engine.clone(),
            sender,
            batches: Arc::new(DashMap::new()),
            stats: Arc::new(IngestionStatistics {
                total_points: AtomicU64::new(0),
                total_batches: AtomicU64::new(0),
                duplicate_points: AtomicU64::new(0),
                invalid_points: AtomicU64::new(0),
                out_of_order_points: AtomicU64::new(0),
                flush_errors: AtomicU64::new(0),
                bytes_ingested: AtomicU64::new(0),
                last_flush_time: RwLock::new(Instant::now()),
            }),
            shutdown: Arc::new(AtomicBool::new(false)),
            flush_semaphore: Arc::new(Semaphore::new(config.max_concurrent_flushes)),
            dedup_cache: Arc::new(DashMap::new()),
            worker_handle: None,
        };

        let worker = pipeline.start_worker(receiver).await;
        
        Ok(Self {
            worker_handle: Some(worker),
            ..pipeline
        })
    }

    pub async fn ingest(&self, point: IngestionPoint) -> Result<(), Error> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::SystemShuttingDown);
        }

        if self.config.enable_validation && !self.validate_point(&point) {
            self.stats.invalid_points.fetch_add(1, Ordering::Relaxed);
            return Err(Error::ValidationFailed);
        }

        if self.config.enable_deduplication && self.is_duplicate(&point) {
            self.stats.duplicate_points.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        if self.config.enable_backpressure {
            let pending = self.sender.capacity() - self.sender.max_capacity();
            if pending > self.config.backpressure_threshold {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        self.sender.send(point).await
            .map_err(|_| Error::ChannelClosed)?;

        self.stats.total_points.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn ingest_batch(&self, points: Vec<IngestionPoint>) -> Result<(), Error> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::SystemShuttingDown);
        }

        let mut valid_points = Vec::with_capacity(points.len());
        
        for point in points {
            if self.config.enable_validation && !self.validate_point(&point) {
                self.stats.invalid_points.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            if self.config.enable_deduplication && self.is_duplicate(&point) {
                self.stats.duplicate_points.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            valid_points.push(point);
        }

        for point in valid_points {
            self.sender.send(point).await
                .map_err(|_| Error::ChannelClosed)?;
            self.stats.total_points.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn start_worker(&self, mut receiver: mpsc::Receiver<IngestionPoint>) -> tokio::task::JoinHandle<()> {
        let batches = self.batches.clone();
        let engine = self.engine.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let shutdown = self.shutdown.clone();
        let flush_semaphore = self.flush_semaphore.clone();

        tokio::spawn(async move {
            let mut flush_interval = interval(config.flush_interval);
            
            loop {
                tokio::select! {
                    Some(point) = receiver.recv() => {
                        Self::process_point(point, &batches, &config);
                    }
                    _ = flush_interval.tick() => {
                        Self::flush_all_batches(
                            &batches,
                            &engine,
                            &config,
                            &stats,
                            &flush_semaphore
                        ).await;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        shutdown.store(true, Ordering::Release);
                        break;
                    }
                }

                if shutdown.load(Ordering::Acquire) {
                    break;
                }
            }

            Self::flush_all_batches(&batches, &engine, &config, &stats, &flush_semaphore).await;
        })
    }

    fn process_point(
        point: IngestionPoint,
        batches: &Arc<DashMap<String, Arc<Mutex<SeriesBatch>>>>,
        config: &IngestionConfig,
    ) {
        let series_id = point.series_id.clone();
        let data_point = point.to_data_point();

        let batch = batches.entry(series_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(SeriesBatch::new(series_id))));

        let mut batch_guard = batch.lock();
        batch_guard.add_point(data_point);

        if batch_guard.should_flush(config) {
            let points = batch_guard.points.clone();
            let series_id = batch_guard.series_id.clone();
            batch_guard.clear();
            drop(batch_guard);

            tokio::spawn(async move {
                // Flush would happen here
            });
        }
    }

    async fn flush_all_batches(
        batches: &Arc<DashMap<String, Arc<Mutex<SeriesBatch>>>>,
        engine: &Arc<TimeSeriesEngine>,
        config: &IngestionConfig,
        stats: &Arc<IngestionStatistics>,
        flush_semaphore: &Arc<Semaphore>,
    ) {
        let mut flush_tasks = Vec::new();

        for entry in batches.iter() {
            let batch = entry.value().clone();
            let mut batch_guard = batch.lock();
            
            if !batch_guard.points.is_empty() {
                let points = batch_guard.points.clone();
                let series_id = batch_guard.series_id.clone();
                batch_guard.clear();
                drop(batch_guard);

                let engine = engine.clone();
                let stats = stats.clone();
                let semaphore = flush_semaphore.clone();

                let task = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    if let Err(_) = engine.insert_batch(&series_id, points).await {
                        stats.flush_errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.total_batches.fetch_add(1, Ordering::Relaxed);
                    }
                });

                flush_tasks.push(task);
            }
        }

        for task in flush_tasks {
            let _ = task.await;
        }

        *stats.last_flush_time.write() = Instant::now();
    }

    fn validate_point(&self, point: &IngestionPoint) -> bool {
        if point.series_id.is_empty() {
            return false;
        }

        if point.value.is_nan() || point.value.is_infinite() {
            return false;
        }

        if point.timestamp == 0 {
            return false;
        }

        true
    }

    fn is_duplicate(&self, point: &IngestionPoint) -> bool {
        let key = (point.series_id.clone(), point.timestamp);
        
        if let Some(last_seen) = self.dedup_cache.get(&key) {
            if last_seen.elapsed() < Duration::from_secs(60) {
                return true;
            }
        }

        self.dedup_cache.insert(key, Instant::now());
        
        if self.dedup_cache.len() > 100000 {
            self.cleanup_dedup_cache();
        }

        false
    }

    fn cleanup_dedup_cache(&self) {
        let cutoff = Instant::now() - Duration::from_secs(300);
        self.dedup_cache.retain(|_, v| *v > cutoff);
    }

    pub async fn flush(&self) -> Result<(), Error> {
        Self::flush_all_batches(
            &self.batches,
            &self.engine,
            &self.config,
            &self.stats,
            &self.flush_semaphore
        ).await;
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.shutdown.store(true, Ordering::Release);
        
        if let Some(handle) = self.worker_handle.take() {
            handle.await.map_err(|_| Error::TaskJoinError)?;
        }

        self.flush().await?;
        Ok(())
    }

    pub fn get_statistics(&self) -> IngestionStats {
        IngestionStats {
            total_points: self.stats.total_points.load(Ordering::Relaxed),
            total_batches: self.stats.total_batches.load(Ordering::Relaxed),
            duplicate_points: self.stats.duplicate_points.load(Ordering::Relaxed),
            invalid_points: self.stats.invalid_points.load(Ordering::Relaxed),
            out_of_order_points: self.stats.out_of_order_points.load(Ordering::Relaxed),
            flush_errors: self.stats.flush_errors.load(Ordering::Relaxed),
            bytes_ingested: self.stats.bytes_ingested.load(Ordering::Relaxed),
            last_flush_time: *self.stats.last_flush_time.read(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestionStats {
    pub total_points: u64,
    pub total_batches: u64,
    pub duplicate_points: u64,
    pub invalid_points: u64,
    pub out_of_order_points: u64,
    pub flush_errors: u64,
    pub bytes_ingested: u64,
    pub last_flush_time: Instant,
}

pub struct BatchIngester {
    config: Arc<IngestionConfig>,
    engine: Arc<TimeSeriesEngine>,
    buffer: Arc<ArrayQueue<IngestionPoint>>,
    workers: Vec<tokio::task::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl BatchIngester {
    pub async fn new(
        config: IngestionConfig,
        engine: Arc<TimeSeriesEngine>,
        worker_count: usize,
    ) -> Result<Self, Error> {
        let buffer = Arc::new(ArrayQueue::new(config.channel_buffer_size));
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let buffer = buffer.clone();
            let engine = engine.clone();
            let config = config.clone();
            let shutdown = shutdown.clone();

            let worker = tokio::spawn(async move {
                Self::worker_loop(buffer, engine, config, shutdown).await;
            });

            workers.push(worker);
        }

        Ok(Self {
            config: Arc::new(config),
            engine,
            buffer,
            workers,
            shutdown,
        })
    }

    pub fn ingest(&self, point: IngestionPoint) -> Result<(), Error> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::SystemShuttingDown);
        }

        self.buffer.push(point)
            .map_err(|_| Error::BufferFull)
    }

    async fn worker_loop(
        buffer: Arc<ArrayQueue<IngestionPoint>>,
        engine: Arc<TimeSeriesEngine>,
        config: IngestionConfig,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut batch = HashMap::new();
        let mut last_flush = Instant::now();

        while !shutdown.load(Ordering::Acquire) {
            while let Some(point) = buffer.pop() {
                let series_id = point.series_id.clone();
                let data_point = point.to_data_point();

                batch.entry(series_id)
                    .or_insert_with(Vec::new)
                    .push(data_point);

                if batch.values().map(|v| v.len()).sum::<usize>() >= config.batch_size {
                    Self::flush_batch(&mut batch, &engine).await;
                    last_flush = Instant::now();
                }
            }

            if last_flush.elapsed() >= config.flush_interval && !batch.is_empty() {
                Self::flush_batch(&mut batch, &engine).await;
                last_flush = Instant::now();
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        if !batch.is_empty() {
            Self::flush_batch(&mut batch, &engine).await;
        }
    }

    async fn flush_batch(
        batch: &mut HashMap<String, Vec<DataPoint>>,
        engine: &Arc<TimeSeriesEngine>,
    ) {
        for (series_id, points) in batch.drain() {
            let _ = engine.insert_batch(&series_id, points).await;
        }
    }

    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.shutdown.store(true, Ordering::Release);
        
        for worker in self.workers {
            worker.await.map_err(|_| Error::TaskJoinError)?;
        }

        Ok(())
    }
}
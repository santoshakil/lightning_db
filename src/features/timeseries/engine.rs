use std::sync::Arc;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use parking_lot::{RwLock, Mutex};
use bytes::Bytes;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use crate::core::storage::{PageManagerAsync, Page};
use dashmap::DashMap;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::{Semaphore, mpsc};
use ahash::AHashMap;

const BLOCK_SIZE: usize = 4096;
const POINTS_PER_BLOCK: usize = 128;
const COMPRESSION_THRESHOLD: usize = 1024;
const HOT_CACHE_SIZE: usize = 10000;
const SHARD_COUNT: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesConfig {
    pub retention_days: u32,
    pub block_size: usize,
    pub compression_enabled: bool,
    pub hot_cache_size: usize,
    pub shard_count: usize,
    pub auto_downsample: bool,
    pub downsample_intervals: Vec<Duration>,
    pub flush_interval: Duration,
    pub max_batch_size: usize,
    pub enable_wal: bool,
}

impl Default for TimeSeriesConfig {
    fn default() -> Self {
        Self {
            retention_days: 30,
            block_size: BLOCK_SIZE,
            compression_enabled: true,
            hot_cache_size: HOT_CACHE_SIZE,
            shard_count: SHARD_COUNT,
            auto_downsample: true,
            downsample_intervals: vec![
                Duration::from_secs(60),
                Duration::from_secs(300),
                Duration::from_secs(3600),
            ],
            flush_interval: Duration::from_secs(10),
            max_batch_size: 10000,
            enable_wal: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DataPoint {
    pub timestamp: u64,
    pub value: f64,
    pub tags: u64,
}

impl DataPoint {
    pub fn new(timestamp: u64, value: f64) -> Self {
        Self {
            timestamp,
            value,
            tags: 0,
        }
    }

    pub fn with_tags(timestamp: u64, value: f64, tags: u64) -> Self {
        Self {
            timestamp,
            value,
            tags,
        }
    }
}

#[derive(Debug)]
struct TimeSeriesBlock {
    start_time: u64,
    end_time: u64,
    points: Vec<DataPoint>,
    compressed: Option<Bytes>,
    statistics: BlockStatistics,
}

#[derive(Debug, Clone)]
struct BlockStatistics {
    count: usize,
    min: f64,
    max: f64,
    sum: f64,
    sum_squares: f64,
}

impl BlockStatistics {
    fn new() -> Self {
        Self {
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.0,
            sum_squares: 0.0,
        }
    }

    fn update(&mut self, value: f64) {
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.sum_squares += value * value;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            let mean = self.mean();
            (self.sum_squares / self.count as f64) - (mean * mean)
        }
    }

    fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }
}

pub struct TimeSeries {
    id: String,
    metadata: TimeSeriesMetadata,
    blocks: Arc<RwLock<BTreeMap<u64, Arc<TimeSeriesBlock>>>>,
    hot_buffer: Arc<RwLock<Vec<DataPoint>>>,
    last_flush: Arc<AtomicU64>,
    dirty: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimeSeriesMetadata {
    created_at: u64,
    updated_at: u64,
    total_points: u64,
    first_timestamp: Option<u64>,
    last_timestamp: Option<u64>,
    tags: HashMap<String, String>,
    unit: Option<String>,
    description: Option<String>,
}

struct TimeSeriesShard {
    series: DashMap<String, Arc<TimeSeries>>,
    index: Arc<SkipMap<u64, Vec<String>>>,
    stats: Arc<ShardStatistics>,
}

struct ShardStatistics {
    series_count: AtomicU64,
    total_points: AtomicU64,
    total_blocks: AtomicU64,
    compressed_blocks: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

pub struct TimeSeriesEngine {
    config: Arc<TimeSeriesConfig>,
    shards: Vec<Arc<TimeSeriesShard>>,
    storage: Arc<dyn PageManagerAsync>,
    hot_cache: Arc<DashMap<String, Arc<TimeSeries>>>,
    flush_semaphore: Arc<Semaphore>,
    shutdown: Arc<AtomicBool>,
    background_handle: Option<tokio::task::JoinHandle<()>>,
}

impl TimeSeriesEngine {
    pub async fn new(
        config: TimeSeriesConfig,
        storage: Arc<dyn PageManagerAsync>,
    ) -> Result<Self, Error> {
        let mut shards = Vec::with_capacity(config.shard_count);
        
        for _ in 0..config.shard_count {
            shards.push(Arc::new(TimeSeriesShard {
                series: DashMap::new(),
                index: Arc::new(SkipMap::new()),
                stats: Arc::new(ShardStatistics {
                    series_count: AtomicU64::new(0),
                    total_points: AtomicU64::new(0),
                    total_blocks: AtomicU64::new(0),
                    compressed_blocks: AtomicU64::new(0),
                    cache_hits: AtomicU64::new(0),
                    cache_misses: AtomicU64::new(0),
                }),
            }));
        }

        let engine = Self {
            config: Arc::new(config),
            shards,
            storage,
            hot_cache: Arc::new(DashMap::new()),
            flush_semaphore: Arc::new(Semaphore::new(1)),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_handle: None,
        };

        Ok(engine)
    }

    pub async fn create_series(&self, id: String, tags: HashMap<String, String>) -> Result<(), Error> {
        let shard = self.get_shard(&id);
        
        if shard.series.contains_key(&id) {
            return Err(Error::KeyAlreadyExists);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = TimeSeriesMetadata {
            created_at: now,
            updated_at: now,
            total_points: 0,
            first_timestamp: None,
            last_timestamp: None,
            tags,
            unit: None,
            description: None,
        };

        let series = Arc::new(TimeSeries {
            id: id.clone(),
            metadata,
            blocks: Arc::new(RwLock::new(BTreeMap::new())),
            hot_buffer: Arc::new(RwLock::new(Vec::with_capacity(self.config.block_size))),
            last_flush: Arc::new(AtomicU64::new(now)),
            dirty: Arc::new(AtomicBool::new(false)),
        });

        shard.series.insert(id.clone(), series.clone());
        self.hot_cache.insert(id, series);
        shard.stats.series_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub async fn insert(&self, series_id: &str, point: DataPoint) -> Result<(), Error> {
        let series = self.get_or_create_series(series_id).await?;
        
        {
            let mut buffer = series.hot_buffer.write();
            buffer.push(point);
            series.dirty.store(true, Ordering::Release);
        }

        if self.should_flush(&series) {
            self.flush_series(&series).await?;
        }

        Ok(())
    }

    pub async fn insert_batch(&self, series_id: &str, points: Vec<DataPoint>) -> Result<(), Error> {
        if points.is_empty() {
            return Ok(());
        }

        let series = self.get_or_create_series(series_id).await?;
        
        {
            let mut buffer = series.hot_buffer.write();
            buffer.extend(points);
            series.dirty.store(true, Ordering::Release);
        }

        if self.should_flush(&series) {
            self.flush_series(&series).await?;
        }

        Ok(())
    }

    pub async fn query(
        &self,
        series_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<DataPoint>, Error> {
        let series = self.get_series(series_id)?;
        let mut results = Vec::new();

        {
            let blocks = series.blocks.read();
            for (block_time, block) in blocks.range(start_time..=end_time) {
                if block.end_time >= start_time && block.start_time <= end_time {
                    for point in &block.points {
                        if point.timestamp >= start_time && point.timestamp <= end_time {
                            results.push(*point);
                        }
                    }
                }
            }
        }

        {
            let buffer = series.hot_buffer.read();
            for point in buffer.iter() {
                if point.timestamp >= start_time && point.timestamp <= end_time {
                    results.push(*point);
                }
            }
        }

        results.sort_by_key(|p| p.timestamp);
        Ok(results)
    }

    pub async fn aggregate(
        &self,
        series_id: &str,
        start_time: u64,
        end_time: u64,
        interval: Duration,
        agg_type: AggregationType,
    ) -> Result<Vec<DataPoint>, Error> {
        let points = self.query(series_id, start_time, end_time).await?;
        
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let interval_ms = interval.as_millis() as u64;
        let mut aggregated = Vec::new();
        let mut current_window = start_time;
        let mut window_points = Vec::new();

        for point in points {
            while point.timestamp >= current_window + interval_ms {
                if !window_points.is_empty() {
                    let agg_value = self.compute_aggregation(&window_points, agg_type);
                    aggregated.push(DataPoint::new(current_window, agg_value));
                    window_points.clear();
                }
                current_window += interval_ms;
            }
            window_points.push(point);
        }

        if !window_points.is_empty() {
            let agg_value = self.compute_aggregation(&window_points, agg_type);
            aggregated.push(DataPoint::new(current_window, agg_value));
        }

        Ok(aggregated)
    }

    async fn get_or_create_series(&self, series_id: &str) -> Result<Arc<TimeSeries>, Error> {
        if let Some(series) = self.hot_cache.get(series_id) {
            return Ok(series.clone());
        }

        let shard = self.get_shard(series_id);
        
        if let Some(series) = shard.series.get(series_id) {
            self.hot_cache.insert(series_id.to_string(), series.clone());
            return Ok(series.clone());
        }

        self.create_series(series_id.to_string(), HashMap::new()).await?;
        
        shard.series.get(series_id)
            .map(|s| s.clone())
            .ok_or(Error::KeyNotFound)
    }

    fn get_series(&self, series_id: &str) -> Result<Arc<TimeSeries>, Error> {
        if let Some(series) = self.hot_cache.get(series_id) {
            return Ok(series.clone());
        }

        let shard = self.get_shard(series_id);
        shard.series.get(series_id)
            .map(|s| s.clone())
            .ok_or(Error::KeyNotFound)
    }

    fn get_shard(&self, series_id: &str) -> &Arc<TimeSeriesShard> {
        let hash = ahash::RandomState::new().hash_one(series_id);
        let shard_idx = (hash as usize) % self.config.shard_count;
        &self.shards[shard_idx]
    }

    fn should_flush(&self, series: &TimeSeries) -> bool {
        let buffer_size = series.hot_buffer.read().len();
        buffer_size >= self.config.block_size
    }

    async fn flush_series(&self, series: &TimeSeries) -> Result<(), Error> {
        let _permit = self.flush_semaphore.acquire().await.unwrap();
        
        let points = {
            let mut buffer = series.hot_buffer.write();
            if buffer.is_empty() {
                return Ok(());
            }
            let points = buffer.clone();
            buffer.clear();
            points
        };

        if points.is_empty() {
            return Ok(());
        }

        let mut statistics = BlockStatistics::new();
        for point in &points {
            statistics.update(point.value);
        }

        let start_time = points.first().unwrap().timestamp;
        let end_time = points.last().unwrap().timestamp;

        let block = Arc::new(TimeSeriesBlock {
            start_time,
            end_time,
            points,
            compressed: None,
            statistics,
        });

        {
            let mut blocks = series.blocks.write();
            blocks.insert(start_time, block);
        }

        series.dirty.store(false, Ordering::Release);
        series.last_flush.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Ordering::Release
        );

        Ok(())
    }

    fn compute_aggregation(&self, points: &[DataPoint], agg_type: AggregationType) -> f64 {
        match agg_type {
            AggregationType::Sum => points.iter().map(|p| p.value).sum(),
            AggregationType::Mean => {
                if points.is_empty() {
                    0.0
                } else {
                    points.iter().map(|p| p.value).sum::<f64>() / points.len() as f64
                }
            }
            AggregationType::Min => points.iter().map(|p| p.value).fold(f64::MAX, f64::min),
            AggregationType::Max => points.iter().map(|p| p.value).fold(f64::MIN, f64::max),
            AggregationType::Count => points.len() as f64,
            AggregationType::First => points.first().map(|p| p.value).unwrap_or(0.0),
            AggregationType::Last => points.last().map(|p| p.value).unwrap_or(0.0),
        }
    }

    pub async fn delete_series(&self, series_id: &str) -> Result<(), Error> {
        let shard = self.get_shard(series_id);
        
        if let Some((_, _)) = shard.series.remove(series_id) {
            self.hot_cache.remove(series_id);
            shard.stats.series_count.fetch_sub(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    pub async fn get_statistics(&self, series_id: &str) -> Result<SeriesStatistics, Error> {
        let series = self.get_series(series_id)?;
        
        let mut total_points = 0;
        let mut min_value = f64::MAX;
        let mut max_value = f64::MIN;
        let mut sum = 0.0;
        
        {
            let blocks = series.blocks.read();
            for block in blocks.values() {
                total_points += block.statistics.count;
                min_value = min_value.min(block.statistics.min);
                max_value = max_value.max(block.statistics.max);
                sum += block.statistics.sum;
            }
        }

        {
            let buffer = series.hot_buffer.read();
            for point in buffer.iter() {
                total_points += 1;
                min_value = min_value.min(point.value);
                max_value = max_value.max(point.value);
                sum += point.value;
            }
        }

        Ok(SeriesStatistics {
            series_id: series_id.to_string(),
            total_points,
            min_value: if min_value == f64::MAX { 0.0 } else { min_value },
            max_value: if max_value == f64::MIN { 0.0 } else { max_value },
            mean: if total_points > 0 { sum / total_points as f64 } else { 0.0 },
            sum,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AggregationType {
    Sum,
    Mean,
    Min,
    Max,
    Count,
    First,
    Last,
}

#[derive(Debug, Clone)]
pub struct SeriesStatistics {
    pub series_id: String,
    pub total_points: usize,
    pub min_value: f64,
    pub max_value: f64,
    pub mean: f64,
    pub sum: f64,
}
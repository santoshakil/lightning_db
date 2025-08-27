use std::sync::Arc;
use std::time::Duration;
use std::collections::{HashMap, BTreeMap};
use crate::core::error::Error;
use super::engine::{DataPoint, TimeSeriesEngine, AggregationType};
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use dashmap::DashMap;

const MAX_CONCURRENT_AGGREGATIONS: usize = 4;

#[derive(Debug, Clone)]
pub enum DownsamplingStrategy {
    Average,
    Sum,
    Min,
    Max,
    First,
    Last,
    MinMaxAvg,
    Percentile(f64),
}

#[derive(Debug, Clone)]
pub struct DownsamplingConfig {
    pub source_interval: Duration,
    pub target_interval: Duration,
    pub strategy: DownsamplingStrategy,
    pub retain_raw_data: bool,
    pub materialized_views: bool,
}

pub struct Aggregator {
    engine: Arc<TimeSeriesEngine>,
    semaphore: Arc<Semaphore>,
    cache: Arc<AggregationCache>,
}

impl Aggregator {
    pub fn new(engine: Arc<TimeSeriesEngine>) -> Self {
        Self {
            engine,
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_AGGREGATIONS)),
            cache: Arc::new(AggregationCache::new()),
        }
    }

    pub async fn downsample(
        &self,
        series_id: &str,
        start_time: u64,
        end_time: u64,
        config: DownsamplingConfig,
    ) -> Result<Vec<DataPoint>, Error> {
        let _permit = self.semaphore.acquire().await.unwrap();
        
        let cache_key = format!(
            "{}_{}_{}_{:?}_{}",
            series_id,
            start_time,
            end_time,
            config.strategy,
            config.target_interval.as_secs()
        );

        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached);
        }

        let raw_points = self.engine.query(series_id, start_time, end_time).await?;
        
        if raw_points.is_empty() {
            return Ok(Vec::new());
        }

        let downsampled = match config.strategy {
            DownsamplingStrategy::Average => {
                self.downsample_average(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::Sum => {
                self.downsample_sum(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::Min => {
                self.downsample_min(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::Max => {
                self.downsample_max(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::First => {
                self.downsample_first(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::Last => {
                self.downsample_last(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::MinMaxAvg => {
                self.downsample_min_max_avg(&raw_points, config.target_interval)
            },
            DownsamplingStrategy::Percentile(p) => {
                self.downsample_percentile(&raw_points, config.target_interval, p)
            },
        };

        self.cache.put(cache_key, downsampled.clone());
        
        Ok(downsampled)
    }

    fn downsample_average(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            if window.is_empty() {
                0.0
            } else {
                window.iter().map(|p| p.value).sum::<f64>() / window.len() as f64
            }
        })
    }

    fn downsample_sum(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            window.iter().map(|p| p.value).sum()
        })
    }

    fn downsample_min(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            window.iter().map(|p| p.value).fold(f64::MAX, f64::min)
        })
    }

    fn downsample_max(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            window.iter().map(|p| p.value).fold(f64::MIN, f64::max)
        })
    }

    fn downsample_first(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            window.first().map(|p| p.value).unwrap_or(0.0)
        })
    }

    fn downsample_last(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        self.downsample_with_function(points, interval, |window| {
            window.last().map(|p| p.value).unwrap_or(0.0)
        })
    }

    fn downsample_min_max_avg(&self, points: &[DataPoint], interval: Duration) -> Vec<DataPoint> {
        let interval_ms = interval.as_millis() as u64;
        let mut result = Vec::new();
        
        if points.is_empty() {
            return result;
        }

        let start = (points[0].timestamp / interval_ms) * interval_ms;
        let end = points.last().unwrap().timestamp;
        
        let mut current_window = start;
        let mut window_points = Vec::new();

        for point in points {
            while point.timestamp >= current_window + interval_ms {
                if !window_points.is_empty() {
                    let min = window_points.iter().map(|p: &DataPoint| p.value).fold(f64::MAX, f64::min);
                    let max = window_points.iter().map(|p: &DataPoint| p.value).fold(f64::MIN, f64::max);
                    let avg = window_points.iter().map(|p: &DataPoint| p.value).sum::<f64>() / window_points.len() as f64;
                    
                    result.push(DataPoint::new(current_window, min));
                    result.push(DataPoint::new(current_window + interval_ms / 3, max));
                    result.push(DataPoint::new(current_window + 2 * interval_ms / 3, avg));
                    
                    window_points.clear();
                }
                current_window += interval_ms;
            }
            window_points.push(*point);
        }

        if !window_points.is_empty() {
            let min = window_points.iter().map(|p| p.value).fold(f64::MAX, f64::min);
            let max = window_points.iter().map(|p| p.value).fold(f64::MIN, f64::max);
            let avg = window_points.iter().map(|p| p.value).sum::<f64>() / window_points.len() as f64;
            
            result.push(DataPoint::new(current_window, min));
            result.push(DataPoint::new(current_window + interval_ms / 3, max));
            result.push(DataPoint::new(current_window + 2 * interval_ms / 3, avg));
        }

        result
    }

    fn downsample_percentile(&self, points: &[DataPoint], interval: Duration, percentile: f64) -> Vec<DataPoint> {
        let interval_ms = interval.as_millis() as u64;
        let mut result = Vec::new();
        
        if points.is_empty() {
            return result;
        }

        let start = (points[0].timestamp / interval_ms) * interval_ms;
        let end = points.last().unwrap().timestamp;
        
        let mut current_window = start;
        let mut window_points = Vec::new();

        for point in points {
            while point.timestamp >= current_window + interval_ms {
                if !window_points.is_empty() {
                    let mut values: Vec<f64> = window_points.iter().map(|p: &DataPoint| p.value).collect();
                    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                    
                    let index = ((percentile / 100.0) * (values.len() - 1) as f64) as usize;
                    let value = values[index.min(values.len() - 1)];
                    
                    result.push(DataPoint::new(current_window, value));
                    window_points.clear();
                }
                current_window += interval_ms;
            }
            window_points.push(*point);
        }

        if !window_points.is_empty() {
            let mut values: Vec<f64> = window_points.iter().map(|p| p.value).collect();
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            let index = ((percentile / 100.0) * (values.len() - 1) as f64) as usize;
            let value = values[index.min(values.len() - 1)];
            
            result.push(DataPoint::new(current_window, value));
        }

        result
    }

    fn downsample_with_function<F>(
        &self,
        points: &[DataPoint],
        interval: Duration,
        func: F,
    ) -> Vec<DataPoint>
    where
        F: Fn(&[DataPoint]) -> f64,
    {
        let interval_ms = interval.as_millis() as u64;
        let mut result = Vec::new();
        
        if points.is_empty() {
            return result;
        }

        let start = (points[0].timestamp / interval_ms) * interval_ms;
        let end = points.last().unwrap().timestamp;
        
        let mut current_window = start;
        let mut window_points = Vec::new();

        for point in points {
            while point.timestamp >= current_window + interval_ms {
                if !window_points.is_empty() {
                    let value = func(&window_points);
                    result.push(DataPoint::new(current_window, value));
                    window_points.clear();
                }
                current_window += interval_ms;
            }
            window_points.push(*point);
        }

        if !window_points.is_empty() {
            let value = func(&window_points);
            result.push(DataPoint::new(current_window, value));
        }

        result
    }

    pub async fn compute_rolling_window(
        &self,
        series_id: &str,
        start_time: u64,
        end_time: u64,
        window_size: Duration,
        function: AggregationType,
    ) -> Result<Vec<DataPoint>, Error> {
        let points = self.engine.query(series_id, start_time, end_time).await?;
        
        if points.is_empty() {
            return Ok(Vec::new());
        }

        let window_ms = window_size.as_millis() as u64;
        let mut result = Vec::new();

        for i in 0..points.len() {
            let window_start = points[i].timestamp.saturating_sub(window_ms / 2);
            let window_end = points[i].timestamp + window_ms / 2;
            
            let window_points: Vec<_> = points.iter()
                .filter(|p| p.timestamp >= window_start && p.timestamp <= window_end)
                .cloned()
                .collect();

            let value = self.compute_aggregation(&window_points, function);
            result.push(DataPoint::new(points[i].timestamp, value));
        }

        Ok(result)
    }

    fn compute_aggregation(&self, points: &[DataPoint], function: AggregationType) -> f64 {
        match function {
            AggregationType::Sum => points.iter().map(|p| p.value).sum(),
            AggregationType::Mean => {
                if points.is_empty() {
                    0.0
                } else {
                    points.iter().map(|p| p.value).sum::<f64>() / points.len() as f64
                }
            },
            AggregationType::Min => points.iter().map(|p| p.value).fold(f64::MAX, f64::min),
            AggregationType::Max => points.iter().map(|p| p.value).fold(f64::MIN, f64::max),
            AggregationType::Count => points.len() as f64,
            AggregationType::First => points.first().map(|p| p.value).unwrap_or(0.0),
            AggregationType::Last => points.last().map(|p| p.value).unwrap_or(0.0),
        }
    }

    pub async fn compute_statistics(
        &self,
        series_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<AggregationStatistics, Error> {
        let points = self.engine.query(series_id, start_time, end_time).await?;
        
        if points.is_empty() {
            return Ok(AggregationStatistics::default());
        }

        let count = points.len();
        let sum: f64 = points.iter().map(|p| p.value).sum();
        let mean = sum / count as f64;
        
        let min = points.iter().map(|p| p.value).fold(f64::MAX, f64::min);
        let max = points.iter().map(|p| p.value).fold(f64::MIN, f64::max);
        
        let variance = points.iter()
            .map(|p| (p.value - mean).powi(2))
            .sum::<f64>() / count as f64;
        let std_dev = variance.sqrt();

        let mut sorted_values: Vec<f64> = points.iter().map(|p| p.value).collect();
        sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let median = if count % 2 == 0 {
            (sorted_values[count / 2 - 1] + sorted_values[count / 2]) / 2.0
        } else {
            sorted_values[count / 2]
        };

        let p25_idx = (count as f64 * 0.25) as usize;
        let p75_idx = (count as f64 * 0.75) as usize;
        let p95_idx = (count as f64 * 0.95) as usize;
        let p99_idx = (count as f64 * 0.99) as usize;

        Ok(AggregationStatistics {
            count,
            sum,
            mean,
            min,
            max,
            std_dev,
            variance,
            median,
            p25: sorted_values[p25_idx.min(count - 1)],
            p75: sorted_values[p75_idx.min(count - 1)],
            p95: sorted_values[p95_idx.min(count - 1)],
            p99: sorted_values[p99_idx.min(count - 1)],
        })
    }

    pub async fn compute_correlation(
        &self,
        series1_id: &str,
        series2_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<f64, Error> {
        let points1 = self.engine.query(series1_id, start_time, end_time).await?;
        let points2 = self.engine.query(series2_id, start_time, end_time).await?;
        
        if points1.is_empty() || points2.is_empty() {
            return Ok(0.0);
        }

        let aligned = self.align_series(&points1, &points2);
        
        if aligned.is_empty() {
            return Ok(0.0);
        }

        let n = aligned.len() as f64;
        let sum_x: f64 = aligned.iter().map(|(x, _)| x).sum();
        let sum_y: f64 = aligned.iter().map(|(_, y)| y).sum();
        let sum_xy: f64 = aligned.iter().map(|(x, y)| x * y).sum();
        let sum_x2: f64 = aligned.iter().map(|(x, _)| x * x).sum();
        let sum_y2: f64 = aligned.iter().map(|(_, y)| y * y).sum();

        let numerator = n * sum_xy - sum_x * sum_y;
        let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();

        if denominator == 0.0 {
            Ok(0.0)
        } else {
            Ok(numerator / denominator)
        }
    }

    fn align_series(&self, series1: &[DataPoint], series2: &[DataPoint]) -> Vec<(f64, f64)> {
        let mut aligned = Vec::new();
        let mut i = 0;
        let mut j = 0;

        while i < series1.len() && j < series2.len() {
            if series1[i].timestamp == series2[j].timestamp {
                aligned.push((series1[i].value, series2[j].value));
                i += 1;
                j += 1;
            } else if series1[i].timestamp < series2[j].timestamp {
                i += 1;
            } else {
                j += 1;
            }
        }

        aligned
    }
}

#[derive(Debug, Clone, Default)]
pub struct AggregationStatistics {
    pub count: usize,
    pub sum: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub std_dev: f64,
    pub variance: f64,
    pub median: f64,
    pub p25: f64,
    pub p75: f64,
    pub p95: f64,
    pub p99: f64,
}

struct AggregationCache {
    cache: Arc<DashMap<String, Vec<DataPoint>>>,
    max_entries: usize,
}

impl AggregationCache {
    fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            max_entries: 1000,
        }
    }

    fn get(&self, key: &str) -> Option<Vec<DataPoint>> {
        self.cache.get(key).map(|entry| entry.clone())
    }

    fn put(&self, key: String, points: Vec<DataPoint>) {
        if self.cache.len() >= self.max_entries {
            if let Some(entry) = self.cache.iter().next() {
                self.cache.remove(entry.key());
            }
        }
        self.cache.insert(key, points);
    }
}

pub struct MaterializedView {
    id: String,
    source_series: String,
    config: DownsamplingConfig,
    last_update: RwLock<u64>,
    data: Arc<RwLock<BTreeMap<u64, DataPoint>>>,
}

impl MaterializedView {
    pub fn new(
        id: String,
        source_series: String,
        config: DownsamplingConfig,
    ) -> Self {
        Self {
            id,
            source_series,
            config,
            last_update: RwLock::new(0),
            data: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn refresh(&self, engine: &TimeSeriesEngine) -> Result<(), Error> {
        let last_update = *self.last_update.read();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let points = engine.query(&self.source_series, last_update, now).await?;
        
        if points.is_empty() {
            return Ok(());
        }

        let aggregator = Aggregator::new(Arc::new(engine.clone()));
        let downsampled = aggregator.downsample(
            &self.source_series,
            last_update,
            now,
            self.config.clone(),
        ).await?;

        {
            let mut data = self.data.write();
            for point in downsampled {
                data.insert(point.timestamp, point);
            }
        }

        *self.last_update.write() = now;
        
        Ok(())
    }

    pub fn query(&self, start_time: u64, end_time: u64) -> Vec<DataPoint> {
        let data = self.data.read();
        data.range(start_time..=end_time)
            .map(|(_, point)| *point)
            .collect()
    }
}
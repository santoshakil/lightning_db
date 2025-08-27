use std::sync::Arc;
use std::time::Duration;
use std::collections::{HashMap, BTreeMap};
use crate::core::error::Error;
use super::engine::{DataPoint, TimeSeriesEngine, AggregationType};
use serde::{Serialize, Deserialize};
use parking_lot::RwLock;
use dashmap::DashMap;

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start: u64,
    pub end: u64,
}

impl TimeRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn last_hours(hours: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            start: now - (hours * 3600),
            end: now,
        }
    }

    pub fn last_days(days: u64) -> Self {
        Self::last_hours(days * 24)
    }

    pub fn contains(&self, timestamp: u64) -> bool {
        timestamp >= self.start && timestamp <= self.end
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.end - self.start)
    }
}

#[derive(Debug, Clone)]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    In,
    NotIn,
    Between,
    Regex,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: FilterValue,
}

#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Number(f64),
    StringList(Vec<String>),
    NumberList(Vec<f64>),
    Range(f64, f64),
}

#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    pub series_ids: Vec<String>,
    pub time_range: TimeRange,
    pub filters: Vec<Filter>,
    pub aggregation: Option<QueryAggregation>,
    pub group_by: Vec<String>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub fill_strategy: Option<FillStrategy>,
}

#[derive(Debug, Clone)]
pub struct QueryAggregation {
    pub function: AggregationType,
    pub interval: Duration,
    pub align_to_calendar: bool,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum FillStrategy {
    None,
    Previous,
    Linear,
    Constant(f64),
}

pub struct QueryBuilder {
    query: TimeSeriesQuery,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            query: TimeSeriesQuery {
                series_ids: Vec::new(),
                time_range: TimeRange::last_hours(1),
                filters: Vec::new(),
                aggregation: None,
                group_by: Vec::new(),
                order_by: None,
                limit: None,
                fill_strategy: None,
            },
        }
    }

    pub fn series(mut self, series_id: impl Into<String>) -> Self {
        self.query.series_ids.push(series_id.into());
        self
    }

    pub fn series_list(mut self, series_ids: Vec<String>) -> Self {
        self.query.series_ids = series_ids;
        self
    }

    pub fn time_range(mut self, start: u64, end: u64) -> Self {
        self.query.time_range = TimeRange::new(start, end);
        self
    }

    pub fn last_hours(mut self, hours: u64) -> Self {
        self.query.time_range = TimeRange::last_hours(hours);
        self
    }

    pub fn last_days(mut self, days: u64) -> Self {
        self.query.time_range = TimeRange::last_days(days);
        self
    }

    pub fn filter(mut self, field: impl Into<String>, operator: FilterOperator, value: FilterValue) -> Self {
        self.query.filters.push(Filter {
            field: field.into(),
            operator,
            value,
        });
        self
    }

    pub fn aggregate(mut self, function: AggregationType, interval: Duration) -> Self {
        self.query.aggregation = Some(QueryAggregation {
            function,
            interval,
            align_to_calendar: false,
        });
        self
    }

    pub fn aggregate_aligned(mut self, function: AggregationType, interval: Duration) -> Self {
        self.query.aggregation = Some(QueryAggregation {
            function,
            interval,
            align_to_calendar: true,
        });
        self
    }

    pub fn group_by(mut self, fields: Vec<String>) -> Self {
        self.query.group_by = fields;
        self
    }

    pub fn order_by(mut self, field: impl Into<String>, descending: bool) -> Self {
        self.query.order_by = Some(OrderBy {
            field: field.into(),
            descending,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    pub fn fill(mut self, strategy: FillStrategy) -> Self {
        self.query.fill_strategy = Some(strategy);
        self
    }

    pub fn build(self) -> TimeSeriesQuery {
        self.query
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub series_id: String,
    pub points: Vec<DataPoint>,
    pub metadata: QueryMetadata,
}

#[derive(Debug, Clone)]
pub struct QueryMetadata {
    pub total_points: usize,
    pub time_range: TimeRange,
    pub execution_time: Duration,
    pub compressed_size: Option<usize>,
    pub cache_hit: bool,
}

pub struct QueryExecutor {
    engine: Arc<TimeSeriesEngine>,
    cache: Arc<QueryCache>,
    parallel_threshold: usize,
}

impl QueryExecutor {
    pub fn new(engine: Arc<TimeSeriesEngine>) -> Self {
        Self {
            engine,
            cache: Arc::new(QueryCache::new(1000)),
            parallel_threshold: 10,
        }
    }

    pub async fn execute(&self, query: TimeSeriesQuery) -> Result<Vec<QueryResult>, Error> {
        let start_time = std::time::Instant::now();
        
        let cache_key = self.generate_cache_key(&query);
        if let Some(cached) = self.cache.get(&cache_key) {
            return Ok(cached);
        }

        let mut results = Vec::new();
        
        if query.series_ids.len() > self.parallel_threshold {
            let tasks: Vec<_> = query.series_ids.iter().map(|series_id| {
                let engine = self.engine.clone();
                let series_id = series_id.clone();
                let time_range = query.time_range.clone();
                let aggregation = query.aggregation.clone();
                
                tokio::spawn(async move {
                    Self::execute_single_series(engine, series_id, time_range, aggregation).await
                })
            }).collect();

            for task in tasks {
                match task.await {
                    Ok(Ok(result)) => results.push(result),
                    Ok(Err(e)) => return Err(e),
                    Err(_) => return Err(Error::TaskJoinError),
                }
            }
        } else {
            for series_id in &query.series_ids {
                let result = Self::execute_single_series(
                    self.engine.clone(),
                    series_id.clone(),
                    query.time_range.clone(),
                    query.aggregation.clone(),
                ).await?;
                results.push(result);
            }
        }

        if let Some(fill_strategy) = &query.fill_strategy {
            for result in &mut results {
                self.apply_fill_strategy(&mut result.points, fill_strategy, &query.aggregation);
            }
        }

        if let Some(order_by) = &query.order_by {
            self.apply_ordering(&mut results, order_by);
        }

        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        let execution_time = start_time.elapsed();
        for result in &mut results {
            result.metadata.execution_time = execution_time;
        }

        self.cache.put(cache_key, results.clone());
        
        Ok(results)
    }

    async fn execute_single_series(
        engine: Arc<TimeSeriesEngine>,
        series_id: String,
        time_range: TimeRange,
        aggregation: Option<QueryAggregation>,
    ) -> Result<QueryResult, Error> {
        let points = if let Some(agg) = aggregation {
            engine.aggregate(
                &series_id,
                time_range.start,
                time_range.end,
                agg.interval,
                agg.function,
            ).await?
        } else {
            engine.query(&series_id, time_range.start, time_range.end).await?
        };

        Ok(QueryResult {
            series_id,
            points,
            metadata: QueryMetadata {
                total_points: points.len(),
                time_range,
                execution_time: Duration::from_secs(0),
                compressed_size: None,
                cache_hit: false,
            },
        })
    }

    fn apply_fill_strategy(
        &self,
        points: &mut Vec<DataPoint>,
        strategy: &FillStrategy,
        aggregation: &Option<QueryAggregation>,
    ) {
        if points.is_empty() {
            return;
        }

        match strategy {
            FillStrategy::None => {},
            FillStrategy::Previous => {
                self.fill_with_previous(points, aggregation);
            },
            FillStrategy::Linear => {
                self.fill_with_linear_interpolation(points, aggregation);
            },
            FillStrategy::Constant(value) => {
                self.fill_with_constant(points, *value, aggregation);
            },
        }
    }

    fn fill_with_previous(&self, points: &mut Vec<DataPoint>, aggregation: &Option<QueryAggregation>) {
        if let Some(agg) = aggregation {
            let interval_ms = agg.interval.as_millis() as u64;
            let mut filled = Vec::new();
            let mut last_value = points[0].value;
            
            let start = points[0].timestamp;
            let end = points.last().unwrap().timestamp;
            let mut current = start;
            let mut point_iter = points.iter();
            let mut next_point = point_iter.next();

            while current <= end {
                if let Some(point) = next_point {
                    if point.timestamp == current {
                        filled.push(*point);
                        last_value = point.value;
                        next_point = point_iter.next();
                    } else {
                        filled.push(DataPoint::new(current, last_value));
                    }
                } else {
                    filled.push(DataPoint::new(current, last_value));
                }
                current += interval_ms;
            }

            *points = filled;
        }
    }

    fn fill_with_linear_interpolation(&self, points: &mut Vec<DataPoint>, aggregation: &Option<QueryAggregation>) {
        if points.len() < 2 {
            return;
        }

        if let Some(agg) = aggregation {
            let interval_ms = agg.interval.as_millis() as u64;
            let mut filled = Vec::new();
            
            for window in points.windows(2) {
                let p1 = window[0];
                let p2 = window[1];
                
                filled.push(p1);
                
                let mut current = p1.timestamp + interval_ms;
                while current < p2.timestamp {
                    let ratio = (current - p1.timestamp) as f64 / (p2.timestamp - p1.timestamp) as f64;
                    let interpolated_value = p1.value + (p2.value - p1.value) * ratio;
                    filled.push(DataPoint::new(current, interpolated_value));
                    current += interval_ms;
                }
            }
            
            filled.push(*points.last().unwrap());
            *points = filled;
        }
    }

    fn fill_with_constant(&self, points: &mut Vec<DataPoint>, value: f64, aggregation: &Option<QueryAggregation>) {
        if let Some(agg) = aggregation {
            let interval_ms = agg.interval.as_millis() as u64;
            let mut filled = Vec::new();
            
            let start = points[0].timestamp;
            let end = points.last().unwrap().timestamp;
            let mut current = start;
            let mut point_iter = points.iter();
            let mut next_point = point_iter.next();

            while current <= end {
                if let Some(point) = next_point {
                    if point.timestamp == current {
                        filled.push(*point);
                        next_point = point_iter.next();
                    } else {
                        filled.push(DataPoint::new(current, value));
                    }
                } else {
                    filled.push(DataPoint::new(current, value));
                }
                current += interval_ms;
            }

            *points = filled;
        }
    }

    fn apply_ordering(&self, results: &mut Vec<QueryResult>, order_by: &OrderBy) {
        match order_by.field.as_str() {
            "series_id" => {
                results.sort_by(|a, b| {
                    if order_by.descending {
                        b.series_id.cmp(&a.series_id)
                    } else {
                        a.series_id.cmp(&b.series_id)
                    }
                });
            },
            "total_points" => {
                results.sort_by_key(|r| {
                    if order_by.descending {
                        std::usize::MAX - r.metadata.total_points
                    } else {
                        r.metadata.total_points
                    }
                });
            },
            _ => {},
        }
    }

    fn generate_cache_key(&self, query: &TimeSeriesQuery) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        for series_id in &query.series_ids {
            series_id.hash(&mut hasher);
        }
        query.time_range.start.hash(&mut hasher);
        query.time_range.end.hash(&mut hasher);
        
        if let Some(agg) = &query.aggregation {
            (agg.function as u32).hash(&mut hasher);
            agg.interval.as_secs().hash(&mut hasher);
        }
        
        format!("query_{:x}", hasher.finish())
    }
}

struct QueryCache {
    cache: Arc<DashMap<String, Vec<QueryResult>>>,
    max_size: usize,
}

impl QueryCache {
    fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            max_size,
        }
    }

    fn get(&self, key: &str) -> Option<Vec<QueryResult>> {
        self.cache.get(key).map(|entry| {
            let mut results = entry.clone();
            for result in &mut results {
                result.metadata.cache_hit = true;
            }
            results
        })
    }

    fn put(&self, key: String, results: Vec<QueryResult>) {
        if self.cache.len() >= self.max_size {
            if let Some(oldest) = self.cache.iter().next() {
                self.cache.remove(oldest.key());
            }
        }
        self.cache.insert(key, results);
    }
}

pub struct QueryOptimizer {
    engine: Arc<TimeSeriesEngine>,
}

impl QueryOptimizer {
    pub fn new(engine: Arc<TimeSeriesEngine>) -> Self {
        Self { engine }
    }

    pub fn optimize(&self, query: &mut TimeSeriesQuery) {
        self.optimize_time_range(query);
        self.optimize_aggregation(query);
        self.optimize_filters(query);
    }

    fn optimize_time_range(&self, query: &mut TimeSeriesQuery) {
        let duration = query.time_range.duration();
        
        if duration > Duration::from_days(365) {
            if query.aggregation.is_none() {
                query.aggregation = Some(QueryAggregation {
                    function: AggregationType::Mean,
                    interval: Duration::from_hours(24),
                    align_to_calendar: true,
                });
            }
        }
    }

    fn optimize_aggregation(&self, query: &mut TimeSeriesQuery) {
        if let Some(agg) = &mut query.aggregation {
            let duration = query.time_range.duration();
            let interval = agg.interval;
            
            let expected_points = duration.as_secs() / interval.as_secs();
            if expected_points > 10000 {
                agg.interval = Duration::from_secs(duration.as_secs() / 1000);
            }
        }
    }

    fn optimize_filters(&self, query: &mut TimeSeriesQuery) {
        query.filters.sort_by_key(|f| {
            match f.operator {
                FilterOperator::Equals => 0,
                FilterOperator::In => 1,
                FilterOperator::Between => 2,
                _ => 3,
            }
        });
    }
}

trait DurationExt {
    fn from_days(days: u64) -> Duration;
    fn from_hours(hours: u64) -> Duration;
}

impl DurationExt for Duration {
    fn from_days(days: u64) -> Duration {
        Duration::from_secs(days * 24 * 3600)
    }

    fn from_hours(hours: u64) -> Duration {
        Duration::from_secs(hours * 3600)
    }
}
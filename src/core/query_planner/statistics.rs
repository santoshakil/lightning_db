use crate::core::error::Error;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const HISTOGRAM_BUCKETS: usize = 256;
const SAMPLE_SIZE: usize = 10000;
const NULL_FRACTION_DEFAULT: f64 = 0.05;

#[derive(Debug, Clone)]
pub struct TableStatistics {
    tables: Arc<RwLock<HashMap<String, TableStats>>>,
    columns: Arc<RwLock<HashMap<String, HashMap<String, ColumnStatistics>>>>,
    indexes: Arc<RwLock<HashMap<String, IndexStatistics>>>,
    last_update: Arc<RwLock<std::time::SystemTime>>,
}

impl Default for TableStatistics {
    fn default() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            columns: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
            last_update: Arc::new(RwLock::new(std::time::SystemTime::now())),
        }
    }
}

impl TableStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_table(&self, table_name: &str) -> Option<TableStats> {
        self.tables.read().get(table_name).cloned()
    }

    pub fn get_column(&self, table_name: &str, column_name: &str) -> Option<ColumnStatistics> {
        self.columns
            .read()
            .get(table_name)
            .and_then(|cols| cols.get(column_name).cloned())
    }

    pub fn get_index(&self, index_name: &str) -> Option<IndexStatistics> {
        self.indexes.read().get(index_name).cloned()
    }

    pub fn update_table_stats(&self, table_name: String, stats: TableStats) {
        self.tables.write().insert(table_name, stats);
        *self.last_update.write() = std::time::SystemTime::now();
    }

    pub fn update_column_stats(
        &self,
        table_name: String,
        column_name: String,
        stats: ColumnStatistics,
    ) {
        self.columns
            .write()
            .entry(table_name)
            .or_default()
            .insert(column_name, stats);
    }

    pub fn update_index_stats(&self, index_name: String, stats: IndexStatistics) {
        self.indexes.write().insert(index_name, stats);
    }

    pub fn estimate_selectivity(
        &self,
        table_name: &str,
        column_name: &str,
        operator: &SelectivityOperator,
    ) -> f64 {
        if let Some(col_stats) = self.get_column(table_name, column_name) {
            match operator {
                SelectivityOperator::Equals(value) => {
                    if let Some(histogram) = &col_stats.histogram {
                        histogram.estimate_equals_selectivity(value)
                    } else if let Some(distinct) = col_stats.distinct_count {
                        1.0 / distinct as f64
                    } else {
                        0.1
                    }
                }
                SelectivityOperator::Range(low, high) => {
                    if let Some(histogram) = &col_stats.histogram {
                        histogram.estimate_range_selectivity(low, high)
                    } else {
                        0.3
                    }
                }
                SelectivityOperator::GreaterThan(value) => {
                    if let Some(histogram) = &col_stats.histogram {
                        histogram.estimate_greater_than_selectivity(value)
                    } else {
                        0.3
                    }
                }
                SelectivityOperator::LessThan(value) => {
                    if let Some(histogram) = &col_stats.histogram {
                        histogram.estimate_less_than_selectivity(value)
                    } else {
                        0.3
                    }
                }
                SelectivityOperator::In(values) => {
                    let single_selectivity = if let Some(distinct) = col_stats.distinct_count {
                        1.0 / distinct as f64
                    } else {
                        0.1
                    };
                    (single_selectivity * values.len() as f64).min(1.0)
                }
                SelectivityOperator::Like(pattern) => self.estimate_like_selectivity(pattern),
                SelectivityOperator::IsNull => {
                    col_stats.null_fraction.unwrap_or(NULL_FRACTION_DEFAULT)
                }
                SelectivityOperator::IsNotNull => {
                    1.0 - col_stats.null_fraction.unwrap_or(NULL_FRACTION_DEFAULT)
                }
            }
        } else {
            0.5
        }
    }

    fn estimate_like_selectivity(&self, pattern: &str) -> f64 {
        if pattern.starts_with('%') && pattern.ends_with('%') {
            0.5
        } else if pattern.starts_with('%') {
            0.25
        } else if pattern.ends_with('%') {
            0.15
        } else {
            0.05
        }
    }

    pub fn estimate_join_cardinality(
        &self,
        left_table: &str,
        right_table: &str,
        join_keys: &[(String, String)],
    ) -> usize {
        let left_stats = self.get_table(left_table);
        let right_stats = self.get_table(right_table);

        if let (Some(left), Some(right)) = (left_stats, right_stats) {
            let left_rows = left.row_count;
            let right_rows = right.row_count;

            let selectivity = if !join_keys.is_empty() {
                let mut combined_selectivity = 1.0;

                for (left_col, right_col) in join_keys {
                    let left_distinct = self
                        .get_column(left_table, left_col)
                        .and_then(|s| s.distinct_count)
                        .unwrap_or(left_rows);

                    let right_distinct = self
                        .get_column(right_table, right_col)
                        .and_then(|s| s.distinct_count)
                        .unwrap_or(right_rows);

                    let selectivity = 1.0 / left_distinct.max(right_distinct) as f64;
                    combined_selectivity *= selectivity;
                }

                combined_selectivity
            } else {
                1.0
            };

            (left_rows as f64 * right_rows as f64 * selectivity) as usize
        } else {
            1_000_000
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub row_count: usize,
    pub total_pages: usize,
    pub index_pages: Option<usize>,
    pub data_size: usize,
    pub avg_row_size: usize,
    pub last_analyzed: std::time::SystemTime,
    pub table_type: TableType,
    pub partitions: Option<Vec<PartitionStats>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TableType {
    Regular,
    Partitioned,
    Materialized,
    External,
    Temporary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    pub partition_id: String,
    pub row_count: usize,
    pub data_size: usize,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub null_count: Option<usize>,
    pub distinct_count: Option<usize>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub avg_length: Option<usize>,
    pub null_fraction: Option<f64>,
    pub correlation: Option<f64>,
    pub histogram: Option<Histogram>,
    pub most_common_values: Option<Vec<(Vec<u8>, f64)>>,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    String,
    Binary,
    Boolean,
    Date,
    Timestamp,
    Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    pub bucket_boundaries: Vec<Vec<u8>>,
    pub bucket_counts: Vec<usize>,
    pub bucket_distinct: Vec<usize>,
    pub total_count: usize,
    pub histogram_type: HistogramType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HistogramType {
    EquiWidth,
    EquiHeight,
    MaxDiff,
    VOptimal,
}

impl Histogram {
    pub fn new(values: &[Vec<u8>], histogram_type: HistogramType) -> Self {
        let bucket_count = HISTOGRAM_BUCKETS.min(values.len() / 10).max(1);

        match histogram_type {
            HistogramType::EquiHeight => Self::build_equi_height_histogram(values, bucket_count),
            HistogramType::EquiWidth => Self::build_equi_width_histogram(values, bucket_count),
            _ => Self::build_equi_height_histogram(values, bucket_count),
        }
    }

    fn build_equi_height_histogram(values: &[Vec<u8>], bucket_count: usize) -> Self {
        let mut sorted_values = values.to_vec();
        sorted_values.sort();

        let total_count = sorted_values.len();
        let bucket_size = total_count / bucket_count;

        let mut bucket_boundaries = Vec::new();
        let mut bucket_counts = Vec::new();
        let mut bucket_distinct = Vec::new();

        for i in 0..bucket_count {
            let start = i * bucket_size;
            let end = if i == bucket_count - 1 {
                total_count
            } else {
                (i + 1) * bucket_size
            };

            if start < sorted_values.len() {
                bucket_boundaries.push(sorted_values[end.min(sorted_values.len()) - 1].clone());
                bucket_counts.push(end - start);

                let distinct: HashSet<_> = sorted_values[start..end].iter().collect();
                bucket_distinct.push(distinct.len());
            }
        }

        Self {
            bucket_boundaries,
            bucket_counts,
            bucket_distinct,
            total_count,
            histogram_type: HistogramType::EquiHeight,
        }
    }

    fn build_equi_width_histogram(values: &[Vec<u8>], bucket_count: usize) -> Self {
        if values.is_empty() {
            return Self {
                bucket_boundaries: Vec::new(),
                bucket_counts: Vec::new(),
                bucket_distinct: Vec::new(),
                total_count: 0,
                histogram_type: HistogramType::EquiWidth,
            };
        }

        // Safe after empty check above, but use if-let for defensive coding
        let (Some(_min_val), Some(max_val)) = (values.iter().min(), values.iter().max()) else {
            return Self {
                bucket_boundaries: Vec::new(),
                bucket_counts: Vec::new(),
                bucket_distinct: Vec::new(),
                total_count: 0,
                histogram_type: HistogramType::EquiWidth,
            };
        };

        let mut bucket_boundaries = Vec::new();
        let mut bucket_counts = vec![0; bucket_count];
        let mut bucket_values: Vec<HashSet<Vec<u8>>> = vec![HashSet::new(); bucket_count];

        for _ in 0..bucket_count {
            bucket_boundaries.push(max_val.clone());
        }

        for value in values {
            let bucket_idx = Self::find_bucket_index(value, &bucket_boundaries);
            if bucket_idx < bucket_count {
                bucket_counts[bucket_idx] += 1;
                bucket_values[bucket_idx].insert(value.clone());
            }
        }

        let bucket_distinct: Vec<usize> = bucket_values.iter().map(|s| s.len()).collect();

        Self {
            bucket_boundaries,
            bucket_counts,
            bucket_distinct,
            total_count: values.len(),
            histogram_type: HistogramType::EquiWidth,
        }
    }

    fn find_bucket_index(value: &[u8], boundaries: &[Vec<u8>]) -> usize {
        for (i, boundary) in boundaries.iter().enumerate() {
            if value <= boundary.as_slice() {
                return i;
            }
        }
        boundaries.len() - 1
    }

    pub fn estimate_equals_selectivity(&self, value: &[u8]) -> f64 {
        let bucket_idx = Self::find_bucket_index(value, &self.bucket_boundaries);

        if bucket_idx >= self.bucket_counts.len() {
            return 0.0;
        }

        let bucket_count = self.bucket_counts[bucket_idx] as f64;
        let bucket_distinct = self.bucket_distinct[bucket_idx] as f64;

        if bucket_distinct > 0.0 && self.total_count > 0 {
            (bucket_count / bucket_distinct) / self.total_count as f64
        } else {
            0.0
        }
    }

    pub fn estimate_range_selectivity(&self, low: &[u8], high: &[u8]) -> f64 {
        if self.bucket_counts.is_empty() {
            return 0.0;
        }

        let low_bucket = Self::find_bucket_index(low, &self.bucket_boundaries);
        let high_bucket = Self::find_bucket_index(high, &self.bucket_boundaries);

        let mut count = 0;
        let max_idx = self.bucket_counts.len().saturating_sub(1);
        for i in low_bucket.min(max_idx)..=high_bucket.min(max_idx) {
            count += self.bucket_counts[i];
        }

        if self.total_count > 0 {
            count as f64 / self.total_count as f64
        } else {
            0.0
        }
    }

    pub fn estimate_greater_than_selectivity(&self, value: &[u8]) -> f64 {
        if self.bucket_counts.is_empty() {
            return 0.0;
        }

        let bucket_idx = Self::find_bucket_index(value, &self.bucket_boundaries);
        let start = bucket_idx.min(self.bucket_counts.len());

        let mut count = 0;
        for i in start..self.bucket_counts.len() {
            count += self.bucket_counts[i];
        }

        if self.total_count > 0 {
            count as f64 / self.total_count as f64
        } else {
            0.0
        }
    }

    pub fn estimate_less_than_selectivity(&self, value: &[u8]) -> f64 {
        if self.bucket_counts.is_empty() {
            return 0.0;
        }

        let bucket_idx = Self::find_bucket_index(value, &self.bucket_boundaries);
        let max_idx = self.bucket_counts.len().saturating_sub(1);

        let mut count = 0;
        for i in 0..=bucket_idx.min(max_idx) {
            count += self.bucket_counts[i];
        }

        if self.total_count > 0 {
            count as f64 / self.total_count as f64
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
    pub height: usize,
    pub leaf_pages: usize,
    pub internal_pages: usize,
    pub distinct_keys: usize,
    pub avg_leaf_entries: usize,
    pub clustering_factor: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Bitmap,
    GiST,
    GIN,
    BRIN,
}

pub enum SelectivityOperator {
    Equals(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    GreaterThan(Vec<u8>),
    LessThan(Vec<u8>),
    In(Vec<Vec<u8>>),
    Like(String),
    IsNull,
    IsNotNull,
}

pub struct StatisticsCollector {
    max_sample_size: usize,
}

impl Default for StatisticsCollector {
    fn default() -> Self {
        Self {
            max_sample_size: SAMPLE_SIZE,
        }
    }
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn analyze_table(&self, table_name: &str) -> Result<TableStats, Error> {
        let row_count = self.estimate_row_count(table_name).await?;
        let avg_row_size = self.estimate_avg_row_size(table_name).await?;
        let total_pages = (row_count * avg_row_size) / 8192;

        Ok(TableStats {
            row_count,
            total_pages,
            index_pages: None,
            data_size: row_count * avg_row_size,
            avg_row_size,
            last_analyzed: std::time::SystemTime::now(),
            table_type: TableType::Regular,
            partitions: None,
        })
    }

    pub async fn analyze_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<ColumnStatistics, Error> {
        let samples = self.sample_column_values(table_name, column_name).await?;

        let null_count = samples.iter().filter(|v| v.is_empty()).count();
        let non_null_samples: Vec<_> = samples.iter().filter(|v| !v.is_empty()).cloned().collect();

        let distinct: HashSet<_> = non_null_samples.iter().collect();
        let distinct_count = distinct.len();

        let min_value = non_null_samples.iter().min().cloned();
        let max_value = non_null_samples.iter().max().cloned();

        let histogram = if non_null_samples.len() > 100 {
            Some(Histogram::new(&non_null_samples, HistogramType::EquiHeight))
        } else {
            None
        };

        Ok(ColumnStatistics {
            null_count: Some(null_count),
            distinct_count: Some(distinct_count),
            min_value,
            max_value,
            avg_length: Some(
                non_null_samples.iter().map(|v| v.len()).sum::<usize>()
                    / non_null_samples.len().max(1),
            ),
            null_fraction: Some(null_count as f64 / samples.len() as f64),
            correlation: None,
            histogram,
            most_common_values: None,
            data_type: DataType::Binary,
        })
    }

    async fn estimate_row_count(&self, _table_name: &str) -> Result<usize, Error> {
        Ok(1_000_000)
    }

    async fn estimate_avg_row_size(&self, _table_name: &str) -> Result<usize, Error> {
        Ok(100)
    }

    async fn sample_column_values(
        &self,
        _table_name: &str,
        _column_name: &str,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut samples = Vec::new();
        for i in 0..self.max_sample_size {
            samples.push(i.to_le_bytes().to_vec());
        }
        Ok(samples)
    }

    pub fn estimate_correlation(&self, values: &[f64], positions: &[usize]) -> f64 {
        if values.len() != positions.len() || values.len() < 2 {
            return 0.0;
        }

        let n = values.len() as f64;
        let mean_val = values.iter().sum::<f64>() / n;
        let mean_pos = positions.iter().map(|&p| p as f64).sum::<f64>() / n;

        let mut cov = 0.0;
        let mut var_val = 0.0;
        let mut var_pos = 0.0;

        for i in 0..values.len() {
            let val_diff = values[i] - mean_val;
            let pos_diff = positions[i] as f64 - mean_pos;

            cov += val_diff * pos_diff;
            var_val += val_diff * val_diff;
            var_pos += pos_diff * pos_diff;
        }

        if var_val == 0.0 || var_pos == 0.0 {
            return 0.0;
        }

        cov / (var_val * var_pos).sqrt()
    }
}

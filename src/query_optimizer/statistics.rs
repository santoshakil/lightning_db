//! Statistics Collection and Management
//!
//! This module provides comprehensive statistics collection for the cost-based optimizer.
//! It maintains detailed information about data distribution, cardinality, and access patterns.

use crate::Result;
use super::{OptimizerConfig, Value};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// Statistics manager for collecting and maintaining table/column statistics
#[derive(Debug)]
pub struct StatisticsManager {
    /// Table statistics by table name
    table_stats: Arc<RwLock<HashMap<String, TableStatistics>>>,
    /// Column statistics by table.column
    column_stats: Arc<RwLock<HashMap<String, ColumnStatistics>>>,
    /// Index statistics by index name
    index_stats: Arc<RwLock<HashMap<String, IndexStatistics>>>,
    /// Configuration
    config: OptimizerConfig,
}

/// Comprehensive table statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Table name
    pub table_name: String,
    /// Total number of rows
    pub row_count: u64,
    /// Number of pages/blocks
    pub page_count: u64,
    /// Average row size in bytes
    pub avg_row_size: f64,
    /// Total table size in bytes
    pub table_size: u64,
    /// Last update timestamp
    pub last_updated: u64,
    /// Data modification counter since last stats update
    pub modifications_since_update: u64,
    /// Access patterns
    pub access_patterns: AccessPatterns,
    /// Data distribution characteristics
    pub data_distribution: DataDistribution,
}

/// Column-level statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Table and column name
    pub table_name: String,
    pub column_name: String,
    /// Data type
    pub data_type: String,
    /// Number of distinct values (cardinality)
    pub distinct_count: u64,
    /// Number of null values
    pub null_count: u64,
    /// Minimum value
    pub min_value: Option<Value>,
    /// Maximum value
    pub max_value: Option<Value>,
    /// Average value (for numeric types)
    pub avg_value: Option<f64>,
    /// Most common values with frequencies
    pub most_common_values: Vec<(Value, f64)>,
    /// Histogram for data distribution
    pub histogram: Histogram,
    /// Column correlation with other columns
    pub correlations: HashMap<String, f64>,
}

/// Index statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    /// Index name
    pub index_name: String,
    /// Table name
    pub table_name: String,
    /// Indexed columns
    pub columns: Vec<String>,
    /// Index type
    pub index_type: String,
    /// Index size in bytes
    pub index_size: u64,
    /// Number of index pages
    pub page_count: u64,
    /// Index selectivity (0.0 to 1.0)
    pub selectivity: f64,
    /// Usage statistics
    pub usage_stats: IndexUsageStats,
    /// Index clustering factor
    pub clustering_factor: f64,
}

/// Data access patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPatterns {
    /// Sequential scan frequency
    pub sequential_scans: u64,
    /// Index scan frequency
    pub index_scans: u64,
    /// Random access frequency
    pub random_accesses: u64,
    /// Range scan frequency
    pub range_scans: u64,
    /// Join frequency with other tables
    pub join_frequency: HashMap<String, u64>,
    /// Most frequently accessed columns
    pub hot_columns: Vec<String>,
}

/// Data distribution characteristics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataDistribution {
    /// Data skewness measure
    pub skewness: f64,
    /// Data clustering measure
    pub clustering: f64,
    /// Sorted data percentage
    pub sorted_percentage: f64,
    /// Null distribution pattern
    pub null_distribution: NullDistribution,
}

/// Null value distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NullDistribution {
    /// Nulls distributed randomly
    Random,
    /// Nulls clustered at beginning
    Beginning,
    /// Nulls clustered at end
    End,
    /// No nulls
    None,
}

/// Histogram for value distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Histogram {
    /// Histogram type
    pub histogram_type: HistogramType,
    /// Bucket boundaries
    pub boundaries: Vec<Value>,
    /// Frequency counts per bucket
    pub frequencies: Vec<u64>,
    /// Bucket densities
    pub densities: Vec<f64>,
}

/// Histogram types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HistogramType {
    /// Equal-width buckets
    EqualWidth,
    /// Equal-frequency buckets
    EqualFrequency,
    /// Adaptive buckets based on data distribution
    Adaptive,
}

/// Index usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsageStats {
    /// Number of times index was used
    pub usage_count: u64,
    /// Last used timestamp
    pub last_used: u64,
    /// Average rows returned per lookup
    pub avg_rows_returned: f64,
    /// Index effectiveness score
    pub effectiveness: f64,
}

impl StatisticsManager {
    /// Create new statistics manager
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            table_stats: Arc::new(RwLock::new(HashMap::new())),
            column_stats: Arc::new(RwLock::new(HashMap::new())),
            index_stats: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Update statistics for a table
    pub fn update_table_statistics(&self, table_name: &str) -> Result<()> {
        // In a real implementation, this would:
        // 1. Scan the table to collect statistics
        // 2. Analyze data distribution
        // 3. Update histograms
        // 4. Calculate correlations
        
        let table_stats = self.collect_table_statistics(table_name)?;
        let column_stats = self.collect_column_statistics(table_name)?;
        
        // Store the statistics
        {
            let mut stats = self.table_stats.write().unwrap();
            stats.insert(table_name.to_string(), table_stats);
        }
        
        for (column_key, column_stat) in column_stats {
            let mut stats = self.column_stats.write().unwrap();
            stats.insert(column_key, column_stat);
        }
        
        Ok(())
    }

    /// Collect table-level statistics
    fn collect_table_statistics(&self, table_name: &str) -> Result<TableStatistics> {
        // Simulate statistics collection
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(TableStatistics {
            table_name: table_name.to_string(),
            row_count: 100_000, // Would be actual count
            page_count: 2500,   // Assuming 4KB pages, ~40 rows per page
            avg_row_size: 160.0, // Average row size
            table_size: 16_000_000, // 16MB
            last_updated: timestamp,
            modifications_since_update: 0,
            access_patterns: AccessPatterns {
                sequential_scans: 50,
                index_scans: 200,
                random_accesses: 30,
                range_scans: 150,
                join_frequency: HashMap::new(),
                hot_columns: vec!["id".to_string(), "status".to_string()],
            },
            data_distribution: DataDistribution {
                skewness: 0.1,
                clustering: 0.8,
                sorted_percentage: 0.6,
                null_distribution: NullDistribution::Random,
            },
        })
    }

    /// Collect column-level statistics
    fn collect_column_statistics(&self, table_name: &str) -> Result<HashMap<String, ColumnStatistics>> {
        let mut column_stats = HashMap::new();
        
        // Example statistics for common columns
        let columns = vec!["id", "name", "age", "status", "created_at"];
        
        for column in columns {
            let key = format!("{}.{}", table_name, column);
            let stats = self.create_column_statistics(table_name, column)?;
            column_stats.insert(key, stats);
        }
        
        Ok(column_stats)
    }

    /// Create statistics for a specific column
    fn create_column_statistics(&self, table_name: &str, column_name: &str) -> Result<ColumnStatistics> {
        let histogram = match column_name {
            "id" => self.create_uniform_histogram(1, 100_000, 100),
            "age" => self.create_normal_histogram(18, 80, 50),
            "status" => self.create_categorical_histogram(vec!["active", "inactive", "pending"]),
            _ => self.create_default_histogram(),
        };

        let (min_val, max_val, avg_val) = match column_name {
            "id" => (Some(Value::Integer(1)), Some(Value::Integer(100_000)), Some(50_000.0)),
            "age" => (Some(Value::Integer(18)), Some(Value::Integer(80)), Some(35.0)),
            "name" => (None, None, None),
            "status" => (None, None, None),
            "created_at" => (Some(Value::Integer(1609459200)), Some(Value::Integer(1672531200)), None), // Unix timestamps
            _ => (None, None, None),
        };

        Ok(ColumnStatistics {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            data_type: self.infer_data_type(column_name),
            distinct_count: self.estimate_distinct_count(column_name),
            null_count: self.estimate_null_count(column_name),
            min_value: min_val,
            max_value: max_val,
            avg_value: avg_val,
            most_common_values: self.get_most_common_values(column_name),
            histogram,
            correlations: HashMap::new(),
        })
    }

    /// Create uniform distribution histogram
    fn create_uniform_histogram(&self, min: i64, max: i64, buckets: usize) -> Histogram {
        let range = max - min;
        let bucket_size = range / buckets as i64;
        let mut boundaries = Vec::new();
        let mut frequencies = Vec::new();
        let frequency_per_bucket = 100_000 / buckets as u64; // Assume 100k rows

        for i in 0..=buckets {
            boundaries.push(Value::Integer(min + i as i64 * bucket_size));
        }

        for _ in 0..buckets {
            frequencies.push(frequency_per_bucket);
        }

        let densities = frequencies.iter().map(|&f| f as f64 / 100_000.0).collect();

        Histogram {
            histogram_type: HistogramType::EqualWidth,
            boundaries,
            frequencies,
            densities,
        }
    }

    /// Create normal distribution histogram
    fn create_normal_histogram(&self, min: i64, max: i64, buckets: usize) -> Histogram {
        // Simplified normal distribution
        let range = max - min;
        let bucket_size = range / buckets as i64;
        let mut boundaries = Vec::new();
        let mut frequencies = Vec::new();

        for i in 0..=buckets {
            boundaries.push(Value::Integer(min + i as i64 * bucket_size));
        }

        // Create bell curve-like distribution
        let center = buckets / 2;
        for i in 0..buckets {
            let distance_from_center = (i as i32 - center as i32).abs();
            let frequency = 20_000 / (1 + distance_from_center as u64 * distance_from_center as u64);
            frequencies.push(frequency);
        }

        let total: u64 = frequencies.iter().sum();
        let densities = frequencies.iter().map(|&f| f as f64 / total as f64).collect();

        Histogram {
            histogram_type: HistogramType::Adaptive,
            boundaries,
            frequencies,
            densities,
        }
    }

    /// Create categorical histogram
    fn create_categorical_histogram(&self, categories: Vec<&str>) -> Histogram {
        let mut boundaries = Vec::new();
        let mut frequencies = Vec::new();

        for (i, category) in categories.iter().enumerate() {
            boundaries.push(Value::String(category.to_string()));
            // Simulate different frequencies for categories
            let frequency = match *category {
                "active" => 60_000,
                "inactive" => 30_000,
                "pending" => 10_000,
                _ => 5_000,
            };
            frequencies.push(frequency);
        }

        let total: u64 = frequencies.iter().sum();
        let densities = frequencies.iter().map(|&f| f as f64 / total as f64).collect();

        Histogram {
            histogram_type: HistogramType::EqualFrequency,
            boundaries,
            frequencies,
            densities,
        }
    }

    /// Create default histogram
    fn create_default_histogram(&self) -> Histogram {
        Histogram {
            histogram_type: HistogramType::EqualWidth,
            boundaries: vec![Value::Integer(0), Value::Integer(100)],
            frequencies: vec![50_000, 50_000],
            densities: vec![0.5, 0.5],
        }
    }

    /// Infer data type from column name
    fn infer_data_type(&self, column_name: &str) -> String {
        match column_name {
            "id" => "INTEGER".to_string(),
            "age" => "INTEGER".to_string(),
            "name" => "VARCHAR".to_string(),
            "status" => "VARCHAR".to_string(),
            "created_at" => "TIMESTAMP".to_string(),
            _ => "VARCHAR".to_string(),
        }
    }

    /// Estimate distinct count
    fn estimate_distinct_count(&self, column_name: &str) -> u64 {
        match column_name {
            "id" => 100_000, // Primary key
            "age" => 62,     // Age range 18-80
            "name" => 85_000, // High cardinality
            "status" => 3,   // Low cardinality
            "created_at" => 50_000, // Medium cardinality
            _ => 10_000,
        }
    }

    /// Estimate null count
    fn estimate_null_count(&self, column_name: &str) -> u64 {
        match column_name {
            "id" => 0,      // Primary key, no nulls
            "age" => 1_000, // Some missing ages
            "name" => 500,  // Few missing names
            "status" => 0,  // Required field
            "created_at" => 0, // System generated
            _ => 2_000,
        }
    }

    /// Get most common values
    fn get_most_common_values(&self, column_name: &str) -> Vec<(Value, f64)> {
        match column_name {
            "status" => vec![
                (Value::String("active".to_string()), 0.6),
                (Value::String("inactive".to_string()), 0.3),
                (Value::String("pending".to_string()), 0.1),
            ],
            "age" => vec![
                (Value::Integer(25), 0.15),
                (Value::Integer(30), 0.12),
                (Value::Integer(35), 0.10),
            ],
            _ => Vec::new(),
        }
    }

    /// Get table statistics
    pub fn get_table_statistics(&self, table_name: &str) -> Option<TableStatistics> {
        let stats = self.table_stats.read().unwrap();
        stats.get(table_name).cloned()
    }

    /// Get column statistics
    pub fn get_column_statistics(&self, table_name: &str, column_name: &str) -> Option<ColumnStatistics> {
        let key = format!("{}.{}", table_name, column_name);
        let stats = self.column_stats.read().unwrap();
        stats.get(&key).cloned()
    }

    /// Get index statistics
    pub fn get_index_statistics(&self, index_name: &str) -> Option<IndexStatistics> {
        let stats = self.index_stats.read().unwrap();
        stats.get(index_name).cloned()
    }

    /// Update index usage statistics
    pub fn update_index_usage(&self, index_name: &str, rows_returned: u64) -> Result<()> {
        let mut stats = self.index_stats.write().unwrap();
        
        if let Some(index_stat) = stats.get_mut(index_name) {
            index_stat.usage_stats.usage_count += 1;
            index_stat.usage_stats.last_used = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            // Update average rows returned (exponential moving average)
            let alpha = 0.1;
            index_stat.usage_stats.avg_rows_returned = 
                alpha * rows_returned as f64 + 
                (1.0 - alpha) * index_stat.usage_stats.avg_rows_returned;
        }
        
        Ok(())
    }

    /// Get number of tables with statistics
    pub fn get_table_count(&self) -> usize {
        let stats = self.table_stats.read().unwrap();
        stats.len()
    }

    /// Estimate selectivity for a predicate
    pub fn estimate_selectivity(&self, table_name: &str, column_name: &str, operator: &str, value: &Value) -> f64 {
        if let Some(column_stats) = self.get_column_statistics(table_name, column_name) {
            match operator {
                "=" => {
                    // For equality, selectivity = 1 / distinct_count
                    if column_stats.distinct_count > 0 {
                        1.0 / column_stats.distinct_count as f64
                    } else {
                        0.1 // Default estimate
                    }
                }
                ">" | "<" | ">=" | "<=" => {
                    // For range queries, estimate based on histogram
                    self.estimate_range_selectivity(&column_stats.histogram, operator, value)
                }
                "IN" => {
                    // For IN predicates, multiply by number of values
                    if let Value::Integer(count) = value {
                        (*count as f64) / column_stats.distinct_count as f64
                    } else {
                        0.1
                    }
                }
                _ => 0.1, // Default selectivity
            }
        } else {
            0.1 // Default selectivity when no statistics available
        }
    }

    /// Estimate range selectivity using histogram
    fn estimate_range_selectivity(&self, histogram: &Histogram, operator: &str, value: &Value) -> f64 {
        // Simplified implementation
        // In practice, this would analyze the histogram buckets
        // to estimate what fraction of data satisfies the condition
        match operator {
            ">" | ">=" => 0.3, // Assume 30% of data is greater
            "<" | "<=" => 0.3, // Assume 30% of data is less
            _ => 0.1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_manager_creation() {
        let config = OptimizerConfig::default();
        let stats_mgr = StatisticsManager::new(config);
        assert_eq!(stats_mgr.get_table_count(), 0);
    }

    #[test]
    fn test_table_statistics_update() {
        let config = OptimizerConfig::default();
        let stats_mgr = StatisticsManager::new(config);
        
        stats_mgr.update_table_statistics("users").unwrap();
        assert_eq!(stats_mgr.get_table_count(), 1);
        
        let table_stats = stats_mgr.get_table_statistics("users").unwrap();
        assert_eq!(table_stats.table_name, "users");
        assert!(table_stats.row_count > 0);
    }

    #[test]
    fn test_column_statistics() {
        let config = OptimizerConfig::default();
        let stats_mgr = StatisticsManager::new(config);
        
        stats_mgr.update_table_statistics("users").unwrap();
        
        let column_stats = stats_mgr.get_column_statistics("users", "id").unwrap();
        assert_eq!(column_stats.column_name, "id");
        assert_eq!(column_stats.data_type, "INTEGER");
        assert!(column_stats.distinct_count > 0);
    }

    #[test]
    fn test_selectivity_estimation() {
        let config = OptimizerConfig::default();
        let stats_mgr = StatisticsManager::new(config);
        
        stats_mgr.update_table_statistics("users").unwrap();
        
        let selectivity = stats_mgr.estimate_selectivity(
            "users", 
            "id", 
            "=", 
            &Value::Integer(1000)
        );
        assert!(selectivity > 0.0 && selectivity <= 1.0);
    }

    #[test]
    fn test_histogram_creation() {
        let config = OptimizerConfig::default();
        let stats_mgr = StatisticsManager::new(config);
        
        let histogram = stats_mgr.create_uniform_histogram(1, 100, 10);
        assert_eq!(histogram.boundaries.len(), 11); // 10 buckets + 1
        assert_eq!(histogram.frequencies.len(), 10);
        assert!(matches!(histogram.histogram_type, HistogramType::EqualWidth));
    }
}
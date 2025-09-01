use std::collections::{HashMap, HashSet};
use crate::core::error::Error;
use super::column::ColumnData;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub total_bytes: usize,
    pub mean: Option<f64>,
    pub variance: Option<f64>,
    pub skewness: Option<f64>,
    pub kurtosis: Option<f64>,
}

pub struct StatisticsCollector {
    stats: HashMap<String, ColumnStatistics>,
    sample_rate: f64,
}

impl Default for StatisticsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
            sample_rate: 1.0,
        }
    }

    pub fn with_sample_rate(sample_rate: f64) -> Self {
        Self {
            stats: HashMap::new(),
            sample_rate: sample_rate.clamp(0.01, 1.0),
        }
    }

    pub fn collect(&mut self, column_name: String, data: &ColumnData) -> Result<(), Error> {
        let stats = match data {
            ColumnData::Bool(values) => self.collect_bool_stats(values)?,
            ColumnData::Int8(values) => self.collect_int8_stats(values)?,
            ColumnData::Int16(values) => self.collect_int16_stats(values)?,
            ColumnData::Int32(values) => self.collect_int32_stats(values)?,
            ColumnData::Int64(values) => self.collect_int64_stats(values)?,
            ColumnData::UInt8(values) => self.collect_uint8_stats(values)?,
            ColumnData::UInt16(values) => self.collect_uint16_stats(values)?,
            ColumnData::UInt32(values) => self.collect_uint32_stats(values)?,
            ColumnData::UInt64(values) => self.collect_uint64_stats(values)?,
            ColumnData::Float32(values) => self.collect_float32_stats(values)?,
            ColumnData::Float64(values) => self.collect_float64_stats(values)?,
            ColumnData::String(values) => self.collect_string_stats(values)?,
            ColumnData::Binary(values) => self.collect_binary_stats(values)?,
            ColumnData::Null(count) => self.collect_null_stats(*count)?,
        };

        self.stats.insert(column_name, stats);
        Ok(())
    }

    fn collect_bool_stats(&self, values: &[bool]) -> Result<ColumnStatistics, Error> {
        let mut true_count = 0;
        let mut false_count = 0;

        for &value in values {
            if value {
                true_count += 1;
            } else {
                false_count += 1;
            }
        }

        Ok(ColumnStatistics {
            null_count: 0,
            distinct_count: Some(if true_count > 0 && false_count > 0 { 2 } else { 1 }),
            min_value: Some(vec![0u8]), // false
            max_value: Some(vec![1u8]), // true
            total_bytes: values.len(),
            mean: Some(true_count as f64 / values.len() as f64),
            variance: None,
            skewness: None,
            kurtosis: None,
        })
    }

    fn collect_int32_stats(&self, values: &[i32]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() {
            return Ok(self.empty_stats());
        }

        let mut min = i32::MAX;
        let mut max = i32::MIN;
        let mut sum = 0i64;
        let mut sum_squares = 0f64;
        let mut distinct_values = HashSet::new();

        let sample_size = ((values.len() as f64) * self.sample_rate) as usize;
        let step = if sample_size == 0 { 1 } else { values.len() / sample_size };

        for (i, &value) in values.iter().enumerate() {
            min = min.min(value);
            max = max.max(value);
            sum += value as i64;
            
            if i % step == 0 {
                distinct_values.insert(value);
            }
        }

        let mean = sum as f64 / values.len() as f64;
        
        for &value in values {
            let diff = (value as f64) - mean;
            sum_squares += diff * diff;
        }
        
        let variance = if values.len() > 1 {
            Some(sum_squares / (values.len() - 1) as f64)
        } else {
            None
        };

        Ok(ColumnStatistics {
            null_count: 0,
            distinct_count: Some(distinct_values.len()),
            min_value: Some(min.to_le_bytes().to_vec()),
            max_value: Some(max.to_le_bytes().to_vec()),
            total_bytes: values.len() * 4,
            mean: Some(mean),
            variance,
            skewness: None, // Implementation pending skewness calculation
            kurtosis: None, // Implementation pending kurtosis calculation
        })
    }

    fn collect_int64_stats(&self, values: &[i64]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() {
            return Ok(self.empty_stats());
        }

        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut sum = 0i128;
        let mut sum_squares = 0f64;
        let mut distinct_values = HashSet::new();

        let sample_size = ((values.len() as f64) * self.sample_rate) as usize;
        let step = if sample_size == 0 { 1 } else { values.len() / sample_size };

        for (i, &value) in values.iter().enumerate() {
            min = min.min(value);
            max = max.max(value);
            sum += value as i128;
            
            if i % step == 0 {
                distinct_values.insert(value);
            }
        }

        let mean = sum as f64 / values.len() as f64;
        
        for &value in values {
            let diff = (value as f64) - mean;
            sum_squares += diff * diff;
        }
        
        let variance = if values.len() > 1 {
            Some(sum_squares / (values.len() - 1) as f64)
        } else {
            None
        };

        Ok(ColumnStatistics {
            null_count: 0,
            distinct_count: Some(distinct_values.len()),
            min_value: Some(min.to_le_bytes().to_vec()),
            max_value: Some(max.to_le_bytes().to_vec()),
            total_bytes: values.len() * 8,
            mean: Some(mean),
            variance,
            skewness: None,
            kurtosis: None,
        })
    }

    fn collect_float64_stats(&self, values: &[f64]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() {
            return Ok(self.empty_stats());
        }

        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        let mut sum = 0.0;
        let mut sum_squares = 0.0;
        let mut valid_count = 0;
        let mut distinct_values = HashSet::new();

        let sample_size = ((values.len() as f64) * self.sample_rate) as usize;
        let step = if sample_size == 0 { 1 } else { values.len() / sample_size };

        for (i, &value) in values.iter().enumerate() {
            if value.is_finite() {
                min = min.min(value);
                max = max.max(value);
                sum += value;
                valid_count += 1;
                
                if i % step == 0 {
                    distinct_values.insert(value.to_bits());
                }
            }
        }

        if valid_count == 0 {
            return Ok(self.empty_stats());
        }

        let mean = sum / valid_count as f64;
        
        for &value in values {
            if value.is_finite() {
                let diff = value - mean;
                sum_squares += diff * diff;
            }
        }
        
        let variance = if valid_count > 1 {
            Some(sum_squares / (valid_count - 1) as f64)
        } else {
            None
        };

        Ok(ColumnStatistics {
            null_count: values.len() - valid_count,
            distinct_count: Some(distinct_values.len()),
            min_value: Some(min.to_le_bytes().to_vec()),
            max_value: Some(max.to_le_bytes().to_vec()),
            total_bytes: values.len() * 8,
            mean: Some(mean),
            variance,
            skewness: None,
            kurtosis: None,
        })
    }

    fn collect_string_stats(&self, values: &[String]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() {
            return Ok(self.empty_stats());
        }

        let mut min_len = usize::MAX;
        let mut max_len = 0;
        let mut total_bytes = 0;
        let mut distinct_values = HashSet::new();

        let sample_size = ((values.len() as f64) * self.sample_rate) as usize;
        let step = if sample_size == 0 { 1 } else { values.len() / sample_size };

        for (i, value) in values.iter().enumerate() {
            min_len = min_len.min(value.len());
            max_len = max_len.max(value.len());
            total_bytes += value.len();
            
            if i % step == 0 {
                distinct_values.insert(value.clone());
            }
        }

        let min_value = values.iter().min().map(|s| s.as_bytes().to_vec());
        let max_value = values.iter().max().map(|s| s.as_bytes().to_vec());

        Ok(ColumnStatistics {
            null_count: 0,
            distinct_count: Some(distinct_values.len()),
            min_value,
            max_value,
            total_bytes,
            mean: Some(total_bytes as f64 / values.len() as f64),
            variance: None,
            skewness: None,
            kurtosis: None,
        })
    }

    // Implement other type-specific stats collectors
    fn collect_int8_stats(&self, values: &[i8]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let int32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        let mut stats = self.collect_int32_stats(&int32_values)?;
        stats.total_bytes = values.len();
        Ok(stats)
    }

    fn collect_int16_stats(&self, values: &[i16]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let int32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        let mut stats = self.collect_int32_stats(&int32_values)?;
        stats.total_bytes = values.len() * 2;
        Ok(stats)
    }

    fn collect_uint8_stats(&self, values: &[u8]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let int32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        let mut stats = self.collect_int32_stats(&int32_values)?;
        stats.total_bytes = values.len();
        Ok(stats)
    }

    fn collect_uint16_stats(&self, values: &[u16]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let int32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
        let mut stats = self.collect_int32_stats(&int32_values)?;
        stats.total_bytes = values.len() * 2;
        Ok(stats)
    }

    fn collect_uint32_stats(&self, values: &[u32]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let int64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
        let mut stats = self.collect_int64_stats(&int64_values)?;
        stats.total_bytes = values.len() * 4;
        Ok(stats)
    }

    fn collect_uint64_stats(&self, values: &[u64]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let float_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
        let mut stats = self.collect_float64_stats(&float_values)?;
        stats.total_bytes = values.len() * 8;
        Ok(stats)
    }

    fn collect_float32_stats(&self, values: &[f32]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() { return Ok(self.empty_stats()); }
        let float64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
        let mut stats = self.collect_float64_stats(&float64_values)?;
        stats.total_bytes = values.len() * 4;
        Ok(stats)
    }

    fn collect_binary_stats(&self, values: &[Vec<u8>]) -> Result<ColumnStatistics, Error> {
        if values.is_empty() {
            return Ok(self.empty_stats());
        }

        let mut total_bytes = 0;
        let mut distinct_values = HashSet::new();

        let sample_size = ((values.len() as f64) * self.sample_rate) as usize;
        let step = if sample_size == 0 { 1 } else { values.len() / sample_size };

        for (i, value) in values.iter().enumerate() {
            total_bytes += value.len();
            
            if i % step == 0 {
                distinct_values.insert(value.clone());
            }
        }

        let min_value = values.iter().min().cloned();
        let max_value = values.iter().max().cloned();

        Ok(ColumnStatistics {
            null_count: 0,
            distinct_count: Some(distinct_values.len()),
            min_value,
            max_value,
            total_bytes,
            mean: Some(total_bytes as f64 / values.len() as f64),
            variance: None,
            skewness: None,
            kurtosis: None,
        })
    }

    fn collect_null_stats(&self, count: usize) -> Result<ColumnStatistics, Error> {
        Ok(ColumnStatistics {
            null_count: count,
            distinct_count: Some(0),
            min_value: None,
            max_value: None,
            total_bytes: 0,
            mean: None,
            variance: None,
            skewness: None,
            kurtosis: None,
        })
    }

    fn empty_stats(&self) -> ColumnStatistics {
        ColumnStatistics {
            null_count: 0,
            distinct_count: Some(0),
            min_value: None,
            max_value: None,
            total_bytes: 0,
            mean: None,
            variance: None,
            skewness: None,
            kurtosis: None,
        }
    }

    pub fn get_statistics(&self, column_name: &str) -> Option<&ColumnStatistics> {
        self.stats.get(column_name)
    }

    pub fn merge_statistics(&mut self, other: &StatisticsCollector) -> Result<(), Error> {
        for (column_name, other_stats) in &other.stats {
            if let Some(existing_stats) = self.stats.get_mut(column_name) {
                // Merge statistics - simplified implementation
                existing_stats.null_count += other_stats.null_count;
                existing_stats.total_bytes += other_stats.total_bytes;
                
                // For distinct count, we'd need more sophisticated merging
                if let (Some(existing), Some(other)) = (existing_stats.distinct_count, other_stats.distinct_count) {
                    existing_stats.distinct_count = Some(existing + other); // Approximation
                }
            } else {
                self.stats.insert(column_name.clone(), other_stats.clone());
            }
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.stats.clear();
    }
}
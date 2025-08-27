use std::collections::HashMap;
use crate::core::error::Error;
use super::column::ColumnData;

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub total_bytes: usize,
    pub mean: Option<f64>,
    pub variance: Option<f64>,
}

pub struct StatisticsCollector {
    stats: HashMap<String, ColumnStatistics>,
}

impl StatisticsCollector {
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
        }
    }

    pub fn collect(&mut self, column_name: String, data: &ColumnData) -> Result<(), Error> {
        let stats = match data {
            ColumnData::Int32(values) => {
                let mut min = i32::MAX;
                let mut max = i32::MIN;
                let mut sum = 0i64;
                
                for &v in values {
                    min = min.min(v);
                    max = max.max(v);
                    sum += v as i64;
                }
                
                ColumnStatistics {
                    null_count: 0,
                    distinct_count: None,
                    min_value: Some(min.to_le_bytes().to_vec()),
                    max_value: Some(max.to_le_bytes().to_vec()),
                    total_bytes: values.len() * 4,
                    mean: Some(sum as f64 / values.len() as f64),
                    variance: None,
                }
            },
            _ => ColumnStatistics {
                null_count: 0,
                distinct_count: None,
                min_value: None,
                max_value: None,
                total_bytes: 0,
                mean: None,
                variance: None,
            },
        };

        self.stats.insert(column_name, stats);
        Ok(())
    }

    pub fn get_statistics(&self, column_name: &str) -> Option<&ColumnStatistics> {
        self.stats.get(column_name)
    }
}
//! Cardinality Estimation for Query Optimization
//!
//! This module provides accurate cardinality (row count) estimation for query operations.
//! It uses statistics, histograms, and sampling to predict result sizes.

use crate::{Result, Error};
use super::{
    StatisticsManager, Histogram,
    PhysicalOperator, Predicate, ComparisonOp, Value, JoinCondition, JoinType, OptimizerConfig
};
use std::collections::HashMap;
use std::sync::Arc;

/// Cardinality estimation engine
#[derive(Debug)]
pub struct CardinalityEstimator {
    /// Statistics manager for accessing data distribution information
    statistics: Arc<StatisticsManager>,
    /// Estimation cache to avoid redundant calculations
    estimation_cache: HashMap<String, u64>,
    /// Configuration
    config: OptimizerConfig,
}

/// Selectivity estimation result
#[derive(Debug, Clone)]
pub struct SelectivityEstimate {
    /// Estimated selectivity (0.0 to 1.0)
    pub selectivity: f64,
    /// Confidence level in the estimate (0.0 to 1.0)
    pub confidence: f64,
    /// Method used for estimation
    pub method: EstimationMethod,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Cardinality estimation result
#[derive(Debug, Clone)]
pub struct CardinalityEstimate {
    /// Estimated number of rows
    pub cardinality: u64,
    /// Confidence level in the estimate
    pub confidence: f64,
    /// Lower bound estimate
    pub lower_bound: u64,
    /// Upper bound estimate
    pub upper_bound: u64,
    /// Estimation method used
    pub method: EstimationMethod,
}

/// Estimation methods
#[derive(Debug, Clone, Copy)]
pub enum EstimationMethod {
    /// Histogram-based estimation
    Histogram,
    /// Statistics-based estimation
    Statistics,
    /// Sampling-based estimation
    Sampling,
    /// Machine learning model
    MLModel,
    /// Heuristic-based estimation
    Heuristic,
    /// Exact count (when available)
    Exact,
}

/// Join cardinality estimation strategy
#[derive(Debug, Clone, Copy)]
pub enum JoinCardinalityStrategy {
    /// Independence assumption
    Independence,
    /// Inclusion principle
    Inclusion,
    /// Histogram-based
    Histogram,
    /// Sampling-based
    Sampling,
}

impl CardinalityEstimator {
    /// Create new cardinality estimator
    pub fn new(statistics: Arc<StatisticsManager>) -> Self {
        Self {
            statistics,
            estimation_cache: HashMap::new(),
            config: OptimizerConfig::default(),
        }
    }

    /// Estimate cardinality for a physical operator
    pub fn estimate_cardinality(&self, operator: &PhysicalOperator) -> Result<CardinalityEstimate> {
        match operator {
            PhysicalOperator::TableScan { table, predicate, .. } => {
                self.estimate_table_scan_cardinality(table, predicate.as_ref())
            }
            PhysicalOperator::IndexScan { table, predicate, .. } => {
                self.estimate_index_scan_cardinality(table, predicate.as_ref())
            }
            PhysicalOperator::NestedLoopJoin { left, right, condition, join_type } => {
                self.estimate_join_cardinality(left, right, condition, *join_type)
            }
            PhysicalOperator::HashJoin { left, right, condition, join_type, .. } => {
                self.estimate_join_cardinality(left, right, condition, *join_type)
            }
            PhysicalOperator::SortMergeJoin { left, right, condition, join_type } => {
                self.estimate_join_cardinality(left, right, condition, *join_type)
            }
            PhysicalOperator::Aggregation { input, group_by, .. } => {
                self.estimate_aggregation_cardinality(input, group_by)
            }
            PhysicalOperator::Sort { input, .. } => {
                // Sort doesn't change cardinality
                self.estimate_cardinality(input)
            }
            PhysicalOperator::Projection { input, .. } => {
                // Projection doesn't change cardinality
                self.estimate_cardinality(input)
            }
            PhysicalOperator::Filter { input, predicate, .. } => {
                self.estimate_filter_cardinality(input, predicate)
            }
            PhysicalOperator::Limit { input, count, .. } => {
                self.estimate_limit_cardinality(input, *count)
            }
        }
    }

    /// Estimate table scan cardinality
    fn estimate_table_scan_cardinality(&self, table: &str, predicate: Option<&Predicate>) -> Result<CardinalityEstimate> {
        let table_stats = self.statistics.get_table_statistics(table)
            .ok_or_else(|| Error::Generic(format!("No statistics for table {}", table)))?;

        let base_cardinality = table_stats.row_count;
        
        if let Some(pred) = predicate {
            let selectivity = self.estimate_predicate_selectivity(table, pred)?;
            let estimated_cardinality = (base_cardinality as f64 * selectivity.selectivity) as u64;
            
            Ok(CardinalityEstimate {
                cardinality: estimated_cardinality,
                confidence: selectivity.confidence,
                lower_bound: (estimated_cardinality as f64 * 0.5) as u64,
                upper_bound: (estimated_cardinality as f64 * 1.5) as u64,
                method: selectivity.method,
            })
        } else {
            Ok(CardinalityEstimate {
                cardinality: base_cardinality,
                confidence: 1.0,
                lower_bound: base_cardinality,
                upper_bound: base_cardinality,
                method: EstimationMethod::Exact,
            })
        }
    }

    /// Estimate index scan cardinality
    fn estimate_index_scan_cardinality(&self, table: &str, predicate: Option<&Predicate>) -> Result<CardinalityEstimate> {
        // For index scans, typically more selective than table scans
        let mut estimate = self.estimate_table_scan_cardinality(table, predicate)?;
        
        // Index scans are typically more selective
        estimate.cardinality = (estimate.cardinality as f64 * 0.1) as u64; // 10x more selective
        estimate.upper_bound = (estimate.upper_bound as f64 * 0.2) as u64;
        estimate.method = EstimationMethod::Heuristic;
        
        Ok(estimate)
    }

    /// Estimate join cardinality
    fn estimate_join_cardinality(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        condition: &JoinCondition,
        join_type: JoinType,
    ) -> Result<CardinalityEstimate> {
        let left_estimate = self.estimate_cardinality(left)?;
        let right_estimate = self.estimate_cardinality(right)?;
        
        let strategy = self.choose_join_cardinality_strategy(condition);
        
        let result_cardinality = match strategy {
            JoinCardinalityStrategy::Independence => {
                self.estimate_join_independence(left_estimate.cardinality, right_estimate.cardinality, condition)?
            }
            JoinCardinalityStrategy::Inclusion => {
                self.estimate_join_inclusion(left_estimate.cardinality, right_estimate.cardinality, condition)?
            }
            JoinCardinalityStrategy::Histogram => {
                self.estimate_join_histogram(left, right, condition)?
            }
            JoinCardinalityStrategy::Sampling => {
                self.estimate_join_sampling(left_estimate.cardinality, right_estimate.cardinality)?
            }
        };

        // Apply join type multiplier
        let final_cardinality = self.apply_join_type_factor(result_cardinality, join_type);
        
        let confidence = (left_estimate.confidence + right_estimate.confidence) / 2.0 * 0.8; // Lower confidence for joins
        
        Ok(CardinalityEstimate {
            cardinality: final_cardinality,
            confidence,
            lower_bound: (final_cardinality as f64 * 0.3) as u64,
            upper_bound: (final_cardinality as f64 * 2.0) as u64,
            method: EstimationMethod::Statistics,
        })
    }

    /// Estimate aggregation cardinality
    fn estimate_aggregation_cardinality(&self, input: &PhysicalOperator, group_by: &[String]) -> Result<CardinalityEstimate> {
        let input_estimate = self.estimate_cardinality(input)?;
        
        let result_cardinality = if group_by.is_empty() {
            // Single aggregate result
            1
        } else {
            // Estimate number of groups
            let estimated_groups = self.estimate_group_count(input, group_by)?;
            estimated_groups.min(input_estimate.cardinality) // Can't have more groups than input rows
        };
        
        Ok(CardinalityEstimate {
            cardinality: result_cardinality,
            confidence: input_estimate.confidence * 0.9,
            lower_bound: if group_by.is_empty() { 1 } else { (result_cardinality as f64 * 0.5) as u64 },
            upper_bound: if group_by.is_empty() { 1 } else { (result_cardinality as f64 * 1.5) as u64 },
            method: EstimationMethod::Statistics,
        })
    }

    /// Estimate filter cardinality
    fn estimate_filter_cardinality(&self, input: &PhysicalOperator, predicate: &Predicate) -> Result<CardinalityEstimate> {
        let input_estimate = self.estimate_cardinality(input)?;
        
        // For filters, we need to estimate based on the input operator type
        let table_name = self.extract_table_name(input);
        let selectivity = if let Some(table) = table_name {
            self.estimate_predicate_selectivity(&table, predicate)?
        } else {
            // Default selectivity for complex inputs
            SelectivityEstimate {
                selectivity: 0.1,
                confidence: 0.5,
                method: EstimationMethod::Heuristic,
                metadata: HashMap::new(),
            }
        };
        
        let result_cardinality = (input_estimate.cardinality as f64 * selectivity.selectivity) as u64;
        
        Ok(CardinalityEstimate {
            cardinality: result_cardinality,
            confidence: input_estimate.confidence * selectivity.confidence,
            lower_bound: (result_cardinality as f64 * 0.1) as u64,
            upper_bound: (result_cardinality as f64 * 2.0) as u64,
            method: selectivity.method,
        })
    }

    /// Estimate limit cardinality
    fn estimate_limit_cardinality(&self, input: &PhysicalOperator, count: usize) -> Result<CardinalityEstimate> {
        let input_estimate = self.estimate_cardinality(input)?;
        let result_cardinality = input_estimate.cardinality.min(count as u64);
        
        Ok(CardinalityEstimate {
            cardinality: result_cardinality,
            confidence: input_estimate.confidence,
            lower_bound: result_cardinality,
            upper_bound: result_cardinality,
            method: EstimationMethod::Exact,
        })
    }

    /// Estimate predicate selectivity
    fn estimate_predicate_selectivity(&self, table: &str, predicate: &Predicate) -> Result<SelectivityEstimate> {
        match predicate {
            Predicate::Comparison { column, operator, value } => {
                self.estimate_comparison_selectivity(table, column, operator, value)
            }
            Predicate::And(left, right) => {
                let left_sel = self.estimate_predicate_selectivity(table, left)?;
                let right_sel = self.estimate_predicate_selectivity(table, right)?;
                
                // Independence assumption for AND
                Ok(SelectivityEstimate {
                    selectivity: left_sel.selectivity * right_sel.selectivity,
                    confidence: (left_sel.confidence + right_sel.confidence) / 2.0 * 0.9,
                    method: EstimationMethod::Statistics,
                    metadata: HashMap::new(),
                })
            }
            Predicate::Or(left, right) => {
                let left_sel = self.estimate_predicate_selectivity(table, left)?;
                let right_sel = self.estimate_predicate_selectivity(table, right)?;
                
                // Independence assumption for OR
                let combined_selectivity = left_sel.selectivity + right_sel.selectivity - 
                                         (left_sel.selectivity * right_sel.selectivity);
                
                Ok(SelectivityEstimate {
                    selectivity: combined_selectivity.min(1.0),
                    confidence: (left_sel.confidence + right_sel.confidence) / 2.0 * 0.9,
                    method: EstimationMethod::Statistics,
                    metadata: HashMap::new(),
                })
            }
            Predicate::Not(inner) => {
                let inner_sel = self.estimate_predicate_selectivity(table, inner)?;
                
                Ok(SelectivityEstimate {
                    selectivity: 1.0 - inner_sel.selectivity,
                    confidence: inner_sel.confidence * 0.9,
                    method: inner_sel.method,
                    metadata: HashMap::new(),
                })
            }
            Predicate::In { column, values } => {
                self.estimate_in_selectivity(table, column, values)
            }
            Predicate::Between { column, min, max } => {
                self.estimate_range_selectivity(table, column, min, max)
            }
            Predicate::IsNull(column) => {
                self.estimate_null_selectivity(table, column, true)
            }
            Predicate::IsNotNull(column) => {
                self.estimate_null_selectivity(table, column, false)
            }
        }
    }

    /// Estimate comparison selectivity
    fn estimate_comparison_selectivity(
        &self,
        table: &str,
        column: &str,
        operator: &ComparisonOp,
        value: &Value,
    ) -> Result<SelectivityEstimate> {
        if let Some(column_stats) = self.statistics.get_column_statistics(table, column) {
            let selectivity = match operator {
                ComparisonOp::Equal => {
                    if column_stats.distinct_count > 0 {
                        1.0 / column_stats.distinct_count as f64
                    } else {
                        0.1 // Default estimate
                    }
                }
                ComparisonOp::NotEqual => {
                    let eq_selectivity = if column_stats.distinct_count > 0 {
                        1.0 / column_stats.distinct_count as f64
                    } else {
                        0.1
                    };
                    1.0 - eq_selectivity
                }
                ComparisonOp::LessThan | ComparisonOp::LessThanOrEqual => {
                    self.estimate_range_selectivity_with_histogram(&column_stats.histogram, operator, value)
                }
                ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => {
                    self.estimate_range_selectivity_with_histogram(&column_stats.histogram, operator, value)
                }
                ComparisonOp::Like => {
                    self.estimate_like_selectivity(value)
                }
                ComparisonOp::NotLike => {
                    1.0 - self.estimate_like_selectivity(value)
                }
            };
            
            Ok(SelectivityEstimate {
                selectivity: selectivity.max(0.0).min(1.0),
                confidence: 0.8,
                method: EstimationMethod::Statistics,
                metadata: HashMap::new(),
            })
        } else {
            // No statistics available, use heuristics
            let default_selectivity = match operator {
                ComparisonOp::Equal => 0.1,
                ComparisonOp::NotEqual => 0.9,
                ComparisonOp::LessThan | ComparisonOp::LessThanOrEqual => 0.3,
                ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => 0.3,
                ComparisonOp::Like => 0.2,
                ComparisonOp::NotLike => 0.8,
            };
            
            Ok(SelectivityEstimate {
                selectivity: default_selectivity,
                confidence: 0.3,
                method: EstimationMethod::Heuristic,
                metadata: HashMap::new(),
            })
        }
    }

    /// Estimate range selectivity using histogram
    fn estimate_range_selectivity_with_histogram(&self, histogram: &Histogram, operator: &ComparisonOp, value: &Value) -> f64 {
        // Simplified histogram analysis
        // In a real implementation, this would analyze bucket boundaries and densities
        match operator {
            ComparisonOp::LessThan | ComparisonOp::LessThanOrEqual => 0.3,
            ComparisonOp::GreaterThan | ComparisonOp::GreaterThanOrEqual => 0.3,
            _ => 0.1,
        }
    }

    /// Estimate LIKE selectivity
    fn estimate_like_selectivity(&self, pattern: &Value) -> f64 {
        if let Value::String(pattern_str) = pattern {
            if pattern_str.starts_with('%') && pattern_str.ends_with('%') {
                // %pattern% - substring search
                0.3
            } else if pattern_str.starts_with('%') {
                // %pattern - suffix search
                0.2
            } else if pattern_str.ends_with('%') {
                // pattern% - prefix search
                0.1
            } else {
                // exact pattern
                0.05
            }
        } else {
            0.1 // Default
        }
    }

    /// Estimate IN selectivity
    fn estimate_in_selectivity(&self, table: &str, column: &str, values: &[Value]) -> Result<SelectivityEstimate> {
        if let Some(column_stats) = self.statistics.get_column_statistics(table, column) {
            let values_count = values.len() as f64;
            let distinct_count = column_stats.distinct_count as f64;
            
            let selectivity = if distinct_count > 0.0 {
                (values_count / distinct_count).min(1.0)
            } else {
                0.1 * values_count
            };
            
            Ok(SelectivityEstimate {
                selectivity: selectivity.min(1.0),
                confidence: 0.8,
                method: EstimationMethod::Statistics,
                metadata: HashMap::new(),
            })
        } else {
            Ok(SelectivityEstimate {
                selectivity: (0.1 * values.len() as f64).min(1.0),
                confidence: 0.3,
                method: EstimationMethod::Heuristic,
                metadata: HashMap::new(),
            })
        }
    }

    /// Estimate range selectivity
    fn estimate_range_selectivity(&self, table: &str, column: &str, min: &Value, max: &Value) -> Result<SelectivityEstimate> {
        if let Some(column_stats) = self.statistics.get_column_statistics(table, column) {
            // Simplified range estimation
            // In a real implementation, this would use histogram analysis
            let selectivity = if column_stats.min_value.is_some() && column_stats.max_value.is_some() {
                0.2 // Assume 20% for range queries
            } else {
                0.3 // Default
            };
            
            Ok(SelectivityEstimate {
                selectivity,
                confidence: 0.7,
                method: EstimationMethod::Statistics,
                metadata: HashMap::new(),
            })
        } else {
            Ok(SelectivityEstimate {
                selectivity: 0.3,
                confidence: 0.3,
                method: EstimationMethod::Heuristic,
                metadata: HashMap::new(),
            })
        }
    }

    /// Estimate null selectivity
    fn estimate_null_selectivity(&self, table: &str, column: &str, is_null: bool) -> Result<SelectivityEstimate> {
        if let Some(column_stats) = self.statistics.get_column_statistics(table, column) {
            if let Some(table_stats) = self.statistics.get_table_statistics(table) {
                let null_ratio = column_stats.null_count as f64 / table_stats.row_count as f64;
                
                let selectivity = if is_null {
                    null_ratio
                } else {
                    1.0 - null_ratio
                };
                
                return Ok(SelectivityEstimate {
                    selectivity,
                    confidence: 0.9,
                    method: EstimationMethod::Statistics,
                    metadata: HashMap::new(),
                });
            }
        }
        
        // Default estimates
        let default_selectivity = if is_null { 0.1 } else { 0.9 };
        
        Ok(SelectivityEstimate {
            selectivity: default_selectivity,
            confidence: 0.3,
            method: EstimationMethod::Heuristic,
            metadata: HashMap::new(),
        })
    }

    /// Choose join cardinality estimation strategy
    fn choose_join_cardinality_strategy(&self, condition: &JoinCondition) -> JoinCardinalityStrategy {
        match condition {
            JoinCondition::Equi { .. } => JoinCardinalityStrategy::Histogram,
            JoinCondition::Complex(_) => JoinCardinalityStrategy::Sampling,
        }
    }

    /// Estimate join cardinality using independence assumption
    fn estimate_join_independence(&self, left_card: u64, right_card: u64, condition: &JoinCondition) -> Result<u64> {
        match condition {
            JoinCondition::Equi { .. } => {
                // For equi-joins, estimate based on foreign key relationships
                // Simplified: assume each left row matches with 1 right row on average
                Ok(left_card.min(right_card))
            }
            JoinCondition::Complex(_) => {
                // For complex conditions, use cross product with high selectivity
                Ok((left_card as f64 * right_card as f64 * 0.01) as u64)
            }
        }
    }

    /// Estimate join cardinality using inclusion principle
    fn estimate_join_inclusion(&self, left_card: u64, right_card: u64, condition: &JoinCondition) -> Result<u64> {
        // Conservative estimate assuming good overlap
        Ok((left_card + right_card) / 3)
    }

    /// Estimate join cardinality using histograms
    fn estimate_join_histogram(&self, left: &PhysicalOperator, right: &PhysicalOperator, condition: &JoinCondition) -> Result<u64> {
        // Simplified histogram-based estimation
        // In a real implementation, this would analyze column histograms
        let left_card = self.estimate_cardinality(left)?.cardinality;
        let right_card = self.estimate_cardinality(right)?.cardinality;
        
        Ok((left_card + right_card) / 2)
    }

    /// Estimate join cardinality using sampling
    fn estimate_join_sampling(&self, left_card: u64, right_card: u64) -> Result<u64> {
        // Simplified sampling-based estimation
        // In a real implementation, this would perform actual sampling
        Ok((left_card as f64 * right_card as f64).sqrt() as u64)
    }

    /// Apply join type factor to cardinality
    fn apply_join_type_factor(&self, base_cardinality: u64, join_type: JoinType) -> u64 {
        let factor = match join_type {
            JoinType::Inner => 1.0,
            JoinType::Left => 1.2,   // May have some unmatched left rows
            JoinType::Right => 1.2,  // May have some unmatched right rows
            JoinType::Full => 1.5,   // May have unmatched rows from both sides
            JoinType::Semi => 0.8,   // Typically fewer results
            JoinType::Anti => 0.3,   // Usually much fewer results
        };
        
        (base_cardinality as f64 * factor) as u64
    }

    /// Estimate number of groups for aggregation
    fn estimate_group_count(&self, input: &PhysicalOperator, group_by: &[String]) -> Result<u64> {
        if group_by.is_empty() {
            return Ok(1);
        }
        
        let input_cardinality = self.estimate_cardinality(input)?.cardinality;
        
        // Estimate based on group by columns
        let estimated_groups = if group_by.len() == 1 {
            // Single column grouping
            (input_cardinality as f64 * 0.1) as u64 // Assume 10% unique groups
        } else {
            // Multiple column grouping - typically more groups
            (input_cardinality as f64 * 0.3) as u64
        };
        
        Ok(estimated_groups.min(input_cardinality))
    }

    /// Extract table name from operator (for statistics lookup)
    fn extract_table_name(&self, operator: &PhysicalOperator) -> Option<String> {
        match operator {
            PhysicalOperator::TableScan { table, .. } => Some(table.clone()),
            PhysicalOperator::IndexScan { table, .. } => Some(table.clone()),
            _ => None,
        }
    }

    /// Get estimation statistics
    pub fn get_estimation_stats(&self) -> EstimationStatistics {
        EstimationStatistics {
            cache_size: self.estimation_cache.len(),
            cache_hit_rate: 0.0, // Would be tracked in real implementation
            avg_estimation_time_us: 0.0,
            accuracy_score: 0.0,
        }
    }

    /// Clear estimation cache
    pub fn clear_cache(&mut self) {
        self.estimation_cache.clear();
    }
}

/// Estimation statistics for monitoring
#[derive(Debug, Clone)]
pub struct EstimationStatistics {
    pub cache_size: usize,
    pub cache_hit_rate: f64,
    pub avg_estimation_time_us: f64,
    pub accuracy_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_estimator_creation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config));
        let estimator = CardinalityEstimator::new(statistics);
        
        assert_eq!(estimator.estimation_cache.len(), 0);
    }

    #[test]
    fn test_selectivity_estimation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config));
        let estimator = CardinalityEstimator::new(statistics);

        // Test equality predicate
        let predicate = Predicate::Comparison {
            column: "id".to_string(),
            operator: ComparisonOp::Equal,
            value: Value::Integer(1),
        };

        let selectivity = estimator.estimate_predicate_selectivity("users", &predicate).unwrap();
        assert!(selectivity.selectivity > 0.0 && selectivity.selectivity <= 1.0);
        assert!(selectivity.confidence > 0.0 && selectivity.confidence <= 1.0);
    }

    #[test]
    fn test_table_scan_cardinality() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config));
        let estimator = CardinalityEstimator::new(statistics);

        let operator = PhysicalOperator::TableScan {
            table: "users".to_string(),
            predicate: None,
            columns: vec!["id".to_string(), "name".to_string()],
        };

        // This will fail because we don't have statistics loaded, but tests the interface
        let result = estimator.estimate_cardinality(&operator);
        assert!(result.is_err()); // Expected since no statistics are loaded
    }

    #[test]
    fn test_join_type_factors() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config));
        let estimator = CardinalityEstimator::new(statistics);

        let base_cardinality = 1000;

        let inner_result = estimator.apply_join_type_factor(base_cardinality, JoinType::Inner);
        let left_result = estimator.apply_join_type_factor(base_cardinality, JoinType::Left);
        let full_result = estimator.apply_join_type_factor(base_cardinality, JoinType::Full);
        let anti_result = estimator.apply_join_type_factor(base_cardinality, JoinType::Anti);

        assert_eq!(inner_result, base_cardinality);
        assert!(left_result >= base_cardinality);
        assert!(full_result > left_result);
        assert!(anti_result < base_cardinality);
    }

    #[test]
    fn test_like_selectivity() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config));
        let estimator = CardinalityEstimator::new(statistics);

        let prefix_pattern = Value::String("test%".to_string());
        let suffix_pattern = Value::String("%test".to_string());
        let substring_pattern = Value::String("%test%".to_string());

        let prefix_sel = estimator.estimate_like_selectivity(&prefix_pattern);
        let suffix_sel = estimator.estimate_like_selectivity(&suffix_pattern);
        let substring_sel = estimator.estimate_like_selectivity(&substring_pattern);

        assert!(prefix_sel < suffix_sel);
        assert!(suffix_sel < substring_sel);
    }
}
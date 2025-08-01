//! Query Optimizer for Vectorized Execution
//!
//! This module provides cost-based query optimization specifically designed
//! for vectorized execution engines, including SIMD-aware optimization.

use crate::{Result, Error};
use super::{
    QueryOperation, ExecutionPlan, FilterExpression, AggregateFunction, SortColumn,
    JoinType, JoinCondition, ColumnarTable, TableStatistics, ColumnStatistics,
    DataType, ComparisonOperator, Value, SIMDSupport, VECTOR_BATCH_SIZE
};
use std::collections::HashMap;

/// Query optimizer for vectorized execution
pub struct VectorizedOptimizer {
    /// SIMD capabilities
    simd_support: SIMDSupport,
    /// Optimization statistics
    stats: OptimizerStatistics,
    /// Cost model parameters
    cost_model: CostModel,
}

/// Cost model for vectorized operations
#[derive(Debug, Clone)]
pub struct CostModel {
    /// Cost per row for scanning
    scan_cost_per_row: f64,
    /// Cost per row for filtering
    filter_cost_per_row: f64,
    /// Cost per row for sorting
    sort_cost_per_row: f64,
    /// Cost per group for aggregation
    aggregate_cost_per_group: f64,
    /// Cost per row for joins
    join_cost_per_row: f64,
    /// SIMD speedup factor
    simd_speedup_factor: f64,
    /// Memory access cost
    memory_access_cost: f64,
    /// Cache miss penalty
    cache_miss_penalty: f64,
}

/// Optimizer statistics
#[derive(Debug, Clone, Default)]
pub struct OptimizerStatistics {
    /// Number of plans generated
    pub plans_generated: u64,
    /// Number of optimizations performed
    pub optimizations_performed: u64,
    /// Total optimization time in microseconds
    pub optimization_time_us: u64,
    /// Number of SIMD optimizations applied
    pub simd_optimizations: u64,
}

/// Optimization hints
#[derive(Debug, Clone)]
pub struct OptimizationHints {
    /// Prefer SIMD operations
    pub prefer_simd: bool,
    /// Expected result size
    pub expected_result_size: Option<usize>,
    /// Memory limit for query execution
    pub memory_limit: Option<usize>,
    /// Time limit for query execution
    pub time_limit_ms: Option<u64>,
    /// Parallelism level
    pub parallelism: Option<usize>,
}

/// Execution cost estimate
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// Total estimated cost
    pub total_cost: f64,
    /// CPU cost
    pub cpu_cost: f64,
    /// Memory cost
    pub memory_cost: f64,
    /// I/O cost
    pub io_cost: f64,
    /// Estimated execution time in microseconds
    pub estimated_time_us: u64,
    /// Estimated memory usage in bytes
    pub estimated_memory_bytes: u64,
    /// SIMD utilization factor (0.0 to 1.0)
    pub simd_utilization: f64,
}

impl VectorizedOptimizer {
    /// Create new optimizer
    pub fn new() -> Self {
        Self {
            simd_support: SIMDSupport::detect(),
            stats: OptimizerStatistics::default(),
            cost_model: CostModel::default(),
        }
    }
    
    /// Create optimizer with custom cost model
    pub fn with_cost_model(cost_model: CostModel) -> Self {
        Self {
            simd_support: SIMDSupport::detect(),
            stats: OptimizerStatistics::default(),
            cost_model,
        }
    }
    
    /// Optimize query plan
    pub fn optimize(&mut self, plan: ExecutionPlan, table_stats: &TableStatistics, hints: Option<OptimizationHints>) -> Result<ExecutionPlan> {
        let start_time = std::time::Instant::now();
        
        let mut optimized_plan = plan;
        
        // Apply various optimization passes
        optimized_plan = self.apply_filter_pushdown(optimized_plan)?;
        optimized_plan = self.apply_projection_pushdown(optimized_plan)?;
        optimized_plan = self.apply_simd_optimization(optimized_plan, table_stats)?;
        optimized_plan = self.apply_join_reordering(optimized_plan, table_stats)?;
        optimized_plan = self.apply_aggregation_optimization(optimized_plan)?;
        
        // Calculate final cost estimate
        let cost_estimate = self.estimate_cost(&optimized_plan.root, table_stats)?;
        optimized_plan.estimated_cost = cost_estimate.total_cost;
        optimized_plan.memory_required = cost_estimate.estimated_memory_bytes;
        
        // Update statistics
        self.stats.optimizations_performed += 1;
        self.stats.optimization_time_us += start_time.elapsed().as_micros() as u64;
        
        Ok(optimized_plan)
    }
    
    /// Apply filter pushdown optimization
    fn apply_filter_pushdown(&mut self, plan: ExecutionPlan) -> Result<ExecutionPlan> {
        // Push filters as close to the data source as possible
        let optimized_root = self.pushdown_filters(plan.root)?;
        Ok(ExecutionPlan {
            root: optimized_root,
            ..plan
        })
    }
    
    fn pushdown_filters(&mut self, operation: QueryOperation) -> Result<QueryOperation> {
        match operation {
            QueryOperation::Filter { expression } => {
                // Try to push this filter down
                Ok(QueryOperation::Filter { expression })
            }
            
            QueryOperation::Project { columns } => {
                // Filters can often be pushed through projections
                Ok(QueryOperation::Project { columns })
            }
            
            QueryOperation::Join { left, right, join_type, condition } => {
                let optimized_left = Box::new(self.pushdown_filters(*left)?);
                let optimized_right = Box::new(self.pushdown_filters(*right)?);
                Ok(QueryOperation::Join {
                    left: optimized_left,
                    right: optimized_right,
                    join_type,
                    condition,
                })
            }
            
            // Other operations remain unchanged for now
            _ => Ok(operation),
        }
    }
    
    /// Apply projection pushdown optimization
    fn apply_projection_pushdown(&mut self, plan: ExecutionPlan) -> Result<ExecutionPlan> {
        // Push projections down to reduce data movement
        let optimized_root = self.pushdown_projections(plan.root, None)?;
        Ok(ExecutionPlan {
            root: optimized_root,
            ..plan
        })
    }
    
    fn pushdown_projections(&mut self, operation: QueryOperation, required_columns: Option<&[String]>) -> Result<QueryOperation> {
        match operation {
            QueryOperation::Project { columns } => {
                // Merge with parent projection requirements
                let final_columns = if let Some(required) = required_columns {
                    columns.into_iter()
                        .filter(|col| required.contains(col))
                        .collect()
                } else {
                    columns
                };
                Ok(QueryOperation::Project { columns: final_columns })
            }
            
            QueryOperation::Aggregate { functions, group_by } => {
                // Aggregations define their own column requirements
                Ok(QueryOperation::Aggregate { functions, group_by })
            }
            
            // For other operations, continue pushdown
            _ => Ok(operation),
        }
    }
    
    /// Apply SIMD-specific optimizations
    fn apply_simd_optimization(&mut self, mut plan: ExecutionPlan, table_stats: &TableStatistics) -> Result<ExecutionPlan> {
        if !self.simd_support.avx2 && !self.simd_support.sse4_1 {
            return Ok(plan);
        }
        
        // Mark plan as vectorized if it can benefit from SIMD
        if self.can_vectorize_operation(&plan.root, table_stats) {
            plan.vectorized = true;
            self.stats.simd_optimizations += 1;
        }
        
        // Optimize operation order for better SIMD utilization
        plan.root = self.optimize_for_simd(plan.root)?;
        
        Ok(plan)
    }
    
    fn can_vectorize_operation(&self, operation: &QueryOperation, table_stats: &TableStatistics) -> bool {
        match operation {
            QueryOperation::Scan { column, .. } => {
                // Check if column data type supports SIMD
                if let Some(col_stats) = table_stats.column_statistics.get(column) {
                    // Assume we can get data type from statistics (simplified)
                    true // Most numeric types support SIMD
                } else {
                    false
                }
            }
            
            QueryOperation::Filter { expression } => {
                self.can_vectorize_filter(expression, table_stats)
            }
            
            QueryOperation::Aggregate { functions, .. } => {
                // Most aggregate functions can be vectorized
                functions.iter().all(|func| {
                    matches!(func, 
                        AggregateFunction::Count |
                        AggregateFunction::Sum(_) |
                        AggregateFunction::Min(_) |
                        AggregateFunction::Max(_)
                    )
                })
            }
            
            QueryOperation::Project { .. } => true, // Projection is just memory movement
            QueryOperation::Limit { .. } => true,
            
            QueryOperation::Sort { .. } => false, // Sorting is complex to vectorize
            QueryOperation::Join { .. } => false, // Joins are complex to vectorize
        }
    }
    
    fn can_vectorize_filter(&self, expression: &FilterExpression, table_stats: &TableStatistics) -> bool {
        match expression {
            FilterExpression::Column { name, operator, value } => {
                // Check if column and operation support SIMD
                matches!(operator, 
                    ComparisonOperator::Equal |
                    ComparisonOperator::NotEqual |
                    ComparisonOperator::LessThan |
                    ComparisonOperator::LessThanOrEqual |
                    ComparisonOperator::GreaterThan |
                    ComparisonOperator::GreaterThanOrEqual
                ) && matches!(value.data_type(),
                    DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
                )
            }
            
            FilterExpression::And(left, right) => {
                self.can_vectorize_filter(left, table_stats) && 
                self.can_vectorize_filter(right, table_stats)
            }
            
            FilterExpression::Or(left, right) => {
                self.can_vectorize_filter(left, table_stats) &&
                self.can_vectorize_filter(right, table_stats)
            }
            
            FilterExpression::Not(expr) => {
                self.can_vectorize_filter(expr, table_stats)
            }
            
            FilterExpression::In { .. } => true,
            FilterExpression::Between { .. } => true,
            FilterExpression::IsNull(_) => true,
            FilterExpression::IsNotNull(_) => true,
        }
    }
    
    fn optimize_for_simd(&mut self, operation: QueryOperation) -> Result<QueryOperation> {
        // Reorder operations to maximize SIMD efficiency
        match operation {
            QueryOperation::Filter { expression } => {
                // Optimize filter expression tree for SIMD
                let optimized_expr = self.optimize_filter_for_simd(expression)?;
                Ok(QueryOperation::Filter { expression: optimized_expr })
            }
            
            QueryOperation::Join { left, right, join_type, condition } => {
                let optimized_left = Box::new(self.optimize_for_simd(*left)?);
                let optimized_right = Box::new(self.optimize_for_simd(*right)?);
                Ok(QueryOperation::Join {
                    left: optimized_left,
                    right: optimized_right,
                    join_type,
                    condition,
                })
            }
            
            // Other operations remain unchanged
            _ => Ok(operation),
        }
    }
    
    fn optimize_filter_for_simd(&mut self, expression: FilterExpression) -> Result<FilterExpression> {
        match expression {
            FilterExpression::And(left, right) => {
                // Reorder AND conditions to put more selective filters first
                let left_opt = Box::new(self.optimize_filter_for_simd(*left)?);
                let right_opt = Box::new(self.optimize_filter_for_simd(*right)?);
                Ok(FilterExpression::And(left_opt, right_opt))
            }
            
            FilterExpression::Or(left, right) => {
                let left_opt = Box::new(self.optimize_filter_for_simd(*left)?);
                let right_opt = Box::new(self.optimize_filter_for_simd(*right)?);
                Ok(FilterExpression::Or(left_opt, right_opt))
            }
            
            FilterExpression::Not(expr) => {
                let expr_opt = Box::new(self.optimize_filter_for_simd(*expr)?);
                Ok(FilterExpression::Not(expr_opt))
            }
            
            // Leaf conditions remain unchanged
            _ => Ok(expression),
        }
    }
    
    /// Apply join reordering optimization
    fn apply_join_reordering(&mut self, plan: ExecutionPlan, _table_stats: &TableStatistics) -> Result<ExecutionPlan> {
        // Join reordering is complex and depends on cardinality estimates
        // For now, return the plan unchanged
        Ok(plan)
    }
    
    /// Apply aggregation optimizations
    fn apply_aggregation_optimization(&mut self, plan: ExecutionPlan) -> Result<ExecutionPlan> {
        // Optimize aggregation operations for vectorization
        let optimized_root = self.optimize_aggregation(plan.root)?;
        Ok(ExecutionPlan {
            root: optimized_root,
            ..plan
        })
    }
    
    fn optimize_aggregation(&mut self, operation: QueryOperation) -> Result<QueryOperation> {
        match operation {
            QueryOperation::Aggregate { functions, group_by } => {
                // Reorder aggregate functions for better SIMD utilization
                let mut optimized_functions = functions;
                optimized_functions.sort_by_key(|func| {
                    match func {
                        AggregateFunction::Count => 0,  // Fastest
                        AggregateFunction::Sum(_) => 1,
                        AggregateFunction::Min(_) => 2,
                        AggregateFunction::Max(_) => 3,
                        AggregateFunction::Avg(_) => 4,
                        _ => 5,  // Slowest
                    }
                });
                
                Ok(QueryOperation::Aggregate {
                    functions: optimized_functions,
                    group_by,
                })
            }
            
            // Recursively optimize other operations
            QueryOperation::Join { left, right, join_type, condition } => {
                let optimized_left = Box::new(self.optimize_aggregation(*left)?);
                let optimized_right = Box::new(self.optimize_aggregation(*right)?);
                Ok(QueryOperation::Join {
                    left: optimized_left,
                    right: optimized_right,
                    join_type,
                    condition,
                })
            }
            
            _ => Ok(operation),
        }
    }
    
    /// Estimate execution cost for an operation
    pub fn estimate_cost(&self, operation: &QueryOperation, table_stats: &TableStatistics) -> Result<CostEstimate> {
        match operation {
            QueryOperation::Scan { column, filter } => {
                self.estimate_scan_cost(column, filter.as_ref(), table_stats)
            }
            
            QueryOperation::Filter { expression } => {
                self.estimate_filter_cost(expression, table_stats)
            }
            
            QueryOperation::Aggregate { functions, group_by } => {
                self.estimate_aggregate_cost(functions, group_by, table_stats)
            }
            
            QueryOperation::Sort { columns } => {
                self.estimate_sort_cost(columns, table_stats)
            }
            
            QueryOperation::Project { columns } => {
                self.estimate_project_cost(columns, table_stats)
            }
            
            QueryOperation::Limit { count, offset } => {
                self.estimate_limit_cost(*count, *offset, table_stats)
            }
            
            QueryOperation::Join { left, right, join_type, condition } => {
                self.estimate_join_cost(left, right, *join_type, condition, table_stats)
            }
        }
    }
    
    fn estimate_scan_cost(&self, column: &str, filter: Option<&FilterExpression>, table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        let base_cost = row_count * self.cost_model.scan_cost_per_row;
        
        let filter_cost = if let Some(filter_expr) = filter {
            let filter_estimate = self.estimate_filter_cost(filter_expr, table_stats)?;
            filter_estimate.total_cost
        } else {
            0.0
        };
        
        let simd_speedup = if self.simd_support.avx2 {
            self.cost_model.simd_speedup_factor
        } else {
            1.0
        };
        
        let total_cost = (base_cost + filter_cost) / simd_speedup;
        
        Ok(CostEstimate {
            total_cost,
            cpu_cost: total_cost * 0.8,
            memory_cost: total_cost * 0.1,
            io_cost: total_cost * 0.1,
            estimated_time_us: (total_cost * 1000.0) as u64,
            estimated_memory_bytes: (row_count * 8.0) as u64, // Rough estimate
            simd_utilization: if self.simd_support.avx2 { 0.8 } else { 0.0 },
        })
    }
    
    fn estimate_filter_cost(&self, _expression: &FilterExpression, table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        let base_cost = row_count * self.cost_model.filter_cost_per_row;
        
        let simd_speedup = if self.simd_support.avx2 {
            self.cost_model.simd_speedup_factor
        } else {
            1.0
        };
        
        let total_cost = base_cost / simd_speedup;
        
        Ok(CostEstimate {
            total_cost,
            cpu_cost: total_cost,
            memory_cost: 0.0,
            io_cost: 0.0,
            estimated_time_us: (total_cost * 100.0) as u64,
            estimated_memory_bytes: (row_count / 8.0) as u64, // Bit mask
            simd_utilization: if self.simd_support.avx2 { 0.9 } else { 0.0 },
        })
    }
    
    fn estimate_aggregate_cost(&self, functions: &[AggregateFunction], group_by: &[String], table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        let group_count = if group_by.is_empty() {
            1.0
        } else {
            // Rough estimate of number of groups
            (row_count / 10.0).max(1.0)
        };
        
        let base_cost = group_count * self.cost_model.aggregate_cost_per_group * functions.len() as f64;
        
        let simd_speedup = if self.simd_support.avx2 {
            self.cost_model.simd_speedup_factor * 0.7 // Aggregation benefits less from SIMD
        } else {
            1.0
        };
        
        let total_cost = base_cost / simd_speedup;
        
        Ok(CostEstimate {
            total_cost,
            cpu_cost: total_cost * 0.9,
            memory_cost: total_cost * 0.1,
            io_cost: 0.0,
            estimated_time_us: (total_cost * 500.0) as u64,
            estimated_memory_bytes: (group_count * functions.len() as f64 * 8.0) as u64,
            simd_utilization: if self.simd_support.avx2 { 0.6 } else { 0.0 },
        })
    }
    
    fn estimate_sort_cost(&self, _columns: &[SortColumn], table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        // Sorting is typically O(n log n)
        let base_cost = row_count * row_count.log2() * self.cost_model.sort_cost_per_row;
        
        Ok(CostEstimate {
            total_cost: base_cost,
            cpu_cost: base_cost * 0.8,
            memory_cost: base_cost * 0.2,
            io_cost: 0.0,
            estimated_time_us: (base_cost * 1000.0) as u64,
            estimated_memory_bytes: (row_count * 8.0) as u64,
            simd_utilization: 0.2, // Limited SIMD benefit for sorting
        })
    }
    
    fn estimate_project_cost(&self, _columns: &[String], table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        let base_cost = row_count * 0.1; // Very cheap operation
        
        Ok(CostEstimate {
            total_cost: base_cost,
            cpu_cost: base_cost * 0.2,
            memory_cost: base_cost * 0.8,
            io_cost: 0.0,
            estimated_time_us: (base_cost * 10.0) as u64,
            estimated_memory_bytes: (row_count * 4.0) as u64, // Pointer manipulation
            simd_utilization: 0.0, // No SIMD benefit
        })
    }
    
    fn estimate_limit_cost(&self, _count: usize, _offset: usize, _table_stats: &TableStatistics) -> Result<CostEstimate> {
        // Limit is essentially free
        Ok(CostEstimate {
            total_cost: 1.0,
            cpu_cost: 1.0,
            memory_cost: 0.0,
            io_cost: 0.0,
            estimated_time_us: 1,
            estimated_memory_bytes: 0,
            simd_utilization: 0.0,
        })
    }
    
    fn estimate_join_cost(&self, _left: &QueryOperation, _right: &QueryOperation, _join_type: JoinType, _condition: &JoinCondition, table_stats: &TableStatistics) -> Result<CostEstimate> {
        let row_count = table_stats.row_count as f64;
        // Simplified join cost model
        let base_cost = row_count * row_count * self.cost_model.join_cost_per_row;
        
        Ok(CostEstimate {
            total_cost: base_cost,
            cpu_cost: base_cost * 0.7,
            memory_cost: base_cost * 0.2,
            io_cost: base_cost * 0.1,
            estimated_time_us: (base_cost * 2000.0) as u64,
            estimated_memory_bytes: (row_count * row_count * 4.0) as u64,
            simd_utilization: 0.3, // Some SIMD benefit for hash operations
        })
    }
    
    /// Get optimizer statistics
    pub fn get_stats(&self) -> &OptimizerStatistics {
        &self.stats
    }
    
    /// Reset optimizer statistics
    pub fn reset_stats(&mut self) {
        self.stats = OptimizerStatistics::default();
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            scan_cost_per_row: 1.0,
            filter_cost_per_row: 0.5,
            sort_cost_per_row: 2.0,
            aggregate_cost_per_group: 10.0,
            join_cost_per_row: 3.0,
            simd_speedup_factor: 4.0, // 4x speedup with AVX2
            memory_access_cost: 1.0,
            cache_miss_penalty: 100.0,
        }
    }
}

impl Default for OptimizationHints {
    fn default() -> Self {
        Self {
            prefer_simd: true,
            expected_result_size: None,
            memory_limit: None,
            time_limit_ms: None,
            parallelism: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{ColumnDefinition};
    
    #[test]
    fn test_optimizer_creation() {
        let optimizer = VectorizedOptimizer::new();
        assert_eq!(optimizer.stats.plans_generated, 0);
        assert_eq!(optimizer.stats.optimizations_performed, 0);
    }
    
    #[test]
    fn test_cost_estimation() {
        let optimizer = VectorizedOptimizer::new();
        
        let table_stats = TableStatistics {
            row_count: 1000,
            column_count: 2,
            total_size_bytes: 8000,
            column_statistics: HashMap::new(),
        };
        
        let scan_op = QueryOperation::Scan {
            column: "test".to_string(),
            filter: None,
        };
        
        let cost = optimizer.estimate_cost(&scan_op, &table_stats).unwrap();
        assert!(cost.total_cost > 0.0);
        assert!(cost.estimated_time_us > 0);
    }
    
    #[test]
    fn test_simd_vectorization_check() {
        let optimizer = VectorizedOptimizer::new();
        
        let table_stats = TableStatistics {
            row_count: 1000,
            column_count: 1,
            total_size_bytes: 4000,
            column_statistics: HashMap::new(),
        };
        
        let filter_expr = FilterExpression::Column {
            name: "values".to_string(),
            operator: ComparisonOperator::GreaterThan,
            value: Value::Int32(50),
        };
        
        let can_vectorize = optimizer.can_vectorize_filter(&filter_expr, &table_stats);
        assert!(can_vectorize); // Integer comparisons should be vectorizable
    }
    
    #[test]
    fn test_plan_optimization() {
        let mut optimizer = VectorizedOptimizer::new();
        
        let table_stats = TableStatistics {
            row_count: 1000,
            column_count: 2,
            total_size_bytes: 8000,
            column_statistics: HashMap::new(),
        };
        
        let operation = QueryOperation::Filter {
            expression: FilterExpression::Column {
                name: "values".to_string(),
                operator: ComparisonOperator::Equal,
                value: Value::Int32(42),
            },
        };
        
        let plan = ExecutionPlan::new(operation);
        let optimized_plan = optimizer.optimize(plan, &table_stats, None).unwrap();
        
        assert!(optimized_plan.estimated_cost > 0.0);
        assert_eq!(optimizer.stats.optimizations_performed, 1);
    }
}
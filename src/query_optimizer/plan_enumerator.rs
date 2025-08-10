//! Plan Enumeration for Query Optimization
//!
//! This module generates alternative physical execution plans for logical queries.
//! It explores different join orders, access methods, and algorithm choices.

use crate::{Result, Error};
use super::{
    LogicalQuery, PhysicalPlan, PhysicalOperator, OptimizationMetadata,
    CostModel, CardinalityEstimator, OptimizerConfig, JoinType, JoinCondition,
    Predicate, IndexUsage, StatisticsManager, ComparisonOp, Value, LogicalJoin
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Plan enumeration engine for generating alternative query plans
#[derive(Debug)]
pub struct PlanEnumerator {
    /// Cost model for plan evaluation
    cost_model: Arc<CostModel>,
    /// Cardinality estimator for result size estimation
    cardinality_estimator: Arc<CardinalityEstimator>,
    /// Configuration
    config: OptimizerConfig,
    /// Generated plans cache
    plan_cache: HashMap<String, Vec<PhysicalPlan>>,
}

/// Plan enumeration strategy
#[derive(Debug, Clone, Copy)]
pub enum EnumerationStrategy {
    /// Exhaustive enumeration (for small queries)
    Exhaustive,
    /// Heuristic-based enumeration
    Heuristic,
    /// Dynamic programming approach
    DynamicProgramming,
    /// Greedy approach for large queries
    Greedy,
}

/// Join enumeration result
#[derive(Debug, Clone)]
pub struct JoinPlan {
    /// Join order (table names)
    pub join_order: Vec<String>,
    /// Physical operators for each step
    pub operations: Vec<PhysicalOperator>,
    /// Estimated cost
    pub total_cost: f64,
    /// Estimated cardinality
    pub estimated_rows: u64,
}

impl PlanEnumerator {
    /// Create new plan enumerator
    pub fn new(
        cost_model: Arc<CostModel>,
        cardinality_estimator: Arc<CardinalityEstimator>,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            cost_model,
            cardinality_estimator,
            config,
            plan_cache: HashMap::new(),
        }
    }

    /// Enumerate alternative physical plans for a logical query
    pub fn enumerate_plans(
        &self,
        logical_query: &LogicalQuery,
        metadata: &mut OptimizationMetadata,
    ) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Choose enumeration strategy based on query complexity
        let strategy = self.choose_enumeration_strategy(logical_query);
        
        match strategy {
            EnumerationStrategy::Exhaustive => {
                plans.extend(self.enumerate_exhaustive(logical_query)?);
            }
            EnumerationStrategy::Heuristic => {
                plans.extend(self.enumerate_heuristic(logical_query)?);
            }
            EnumerationStrategy::DynamicProgramming => {
                plans.extend(self.enumerate_dynamic_programming(logical_query)?);
            }
            EnumerationStrategy::Greedy => {
                plans.extend(self.enumerate_greedy(logical_query)?);
            }
        }
        
        // Generate single-table access plans
        for table in &logical_query.tables {
            plans.extend(self.enumerate_table_access_plans(table, logical_query)?);
        }
        
        // Generate join plans if multiple tables
        if logical_query.tables.len() > 1 {
            plans.extend(self.enumerate_join_plans(logical_query)?);
        }
        
        // Add aggregation plans if needed
        if !logical_query.aggregates.is_empty() {
            plans = self.add_aggregation_plans(plans, logical_query)?;
        }
        
        // Add sorting plans if needed
        if !logical_query.order_by.is_empty() {
            plans = self.add_sorting_plans(plans, logical_query)?;
        }
        
        // Add limit plans if needed
        if logical_query.limit.is_some() {
            plans = self.add_limit_plans(plans, logical_query)?;
        }
        
        // Limit number of plans considered
        plans.truncate(self.config.max_plans);
        
        metadata.plans_considered = plans.len();
        
        Ok(plans)
    }

    /// Choose optimal enumeration strategy
    fn choose_enumeration_strategy(&self, query: &LogicalQuery) -> EnumerationStrategy {
        let table_count = query.tables.len();
        let join_count = query.joins.len();
        let predicate_count = query.predicates.len();
        
        // Complexity score
        let complexity = table_count + join_count * 2 + predicate_count;
        
        match complexity {
            0..=5 => EnumerationStrategy::Exhaustive,
            6..=15 => EnumerationStrategy::DynamicProgramming,
            16..=30 => EnumerationStrategy::Heuristic,
            _ => EnumerationStrategy::Greedy,
        }
    }

    /// Exhaustive enumeration for small queries
    fn enumerate_exhaustive(&self, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Generate all possible join orders
        let join_orders = self.generate_all_join_orders(&query.tables);
        
        for join_order in join_orders {
            if let Ok(plan) = self.build_plan_for_join_order(query, &join_order) {
                plans.push(plan);
            }
        }
        
        Ok(plans)
    }

    /// Heuristic-based enumeration
    fn enumerate_heuristic(&self, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Apply common heuristics
        let heuristic_orders = vec![
            self.generate_selectivity_based_order(query)?,
            self.generate_size_based_order(query)?,
            self.generate_index_based_order(query)?,
        ];
        
        for join_order in heuristic_orders {
            if let Ok(plan) = self.build_plan_for_join_order(query, &join_order) {
                plans.push(plan);
            }
        }
        
        Ok(plans)
    }

    /// Dynamic programming enumeration
    fn enumerate_dynamic_programming(&self, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Build optimal subplans bottom-up
        let table_count = query.tables.len();
        let mut dp: HashMap<Vec<String>, PhysicalPlan> = HashMap::new();
        
        // Base case: single tables
        for table in &query.tables {
            let single_table_plans = self.enumerate_table_access_plans(table, query)?;
            if let Some(best_plan) = single_table_plans.into_iter().min_by(|a, b| {
                a.cost.partial_cmp(&b.cost).unwrap_or(std::cmp::Ordering::Equal)
            }) {
                dp.insert(vec![table.clone()], best_plan);
            }
        }
        
        // Build larger subsets
        for size in 2..=table_count {
            let subsets = self.generate_table_subsets(&query.tables, size);
            
            for subset in subsets {
                let mut best_plan: Option<PhysicalPlan> = None;
                let mut best_cost = f64::INFINITY;
                
                // Try all possible splits of this subset
                for split_size in 1..size {
                    let splits = self.generate_subset_splits(&subset, split_size);
                    
                    for (left_subset, right_subset) in splits {
                        if let (Some(left_plan), Some(right_plan)) = 
                            (dp.get(&left_subset), dp.get(&right_subset)) {
                            
                            // Create join plan
                            if let Ok(join_plan) = self.create_join_plan(
                                left_plan.clone(),
                                right_plan.clone(),
                                query
                            ) {
                                if join_plan.cost < best_cost {
                                    best_cost = join_plan.cost;
                                    best_plan = Some(join_plan);
                                }
                            }
                        }
                    }
                }
                
                if let Some(plan) = best_plan {
                    dp.insert(subset, plan);
                }
            }
        }
        
        // Extract final plans
        if let Some(final_plan) = dp.get(&query.tables) {
            plans.push(final_plan.clone());
        }
        
        Ok(plans)
    }

    /// Greedy enumeration for large queries
    fn enumerate_greedy(&self, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Use greedy heuristics to quickly find good plans
        let greedy_order = self.generate_greedy_join_order(query)?;
        
        if let Ok(plan) = self.build_plan_for_join_order(query, &greedy_order) {
            plans.push(plan);
        }
        
        Ok(plans)
    }

    /// Generate table access plans
    fn enumerate_table_access_plans(&self, table: &str, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Table scan plan
        let table_scan = PhysicalOperator::TableScan {
            table: table.to_string(),
            predicate: self.extract_table_predicates(table, &query.predicates),
            columns: self.extract_projected_columns(table, &query.projections),
        };
        
        let scan_plan = PhysicalPlan {
            operation: table_scan,
            cost: 100.0, // Would be calculated by cost model
            cardinality: 1000, // Would be estimated
            estimated_time_us: 1000,
            memory_required: 1024 * 1024,
            vectorized: self.config.enable_vectorization,
            indexes_used: Vec::new(),
            join_order: vec![table.to_string()],
        };
        
        plans.push(scan_plan);
        
        // Index scan plans (if indexes available)
        if self.config.enable_index_usage {
            plans.extend(self.enumerate_index_scans(table, query)?);
        }
        
        Ok(plans)
    }

    /// Generate index scan plans
    fn enumerate_index_scans(&self, table: &str, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // For demonstration, assume some common indexes exist
        let potential_indexes = vec![
            format!("{}_id_idx", table),
            format!("{}_name_idx", table),
            format!("{}_date_idx", table),
        ];
        
        for index in potential_indexes {
            let index_scan = PhysicalOperator::IndexScan {
                table: table.to_string(),
                index: index.clone(),
                key_conditions: Vec::new(), // Would extract from predicates
                predicate: self.extract_table_predicates(table, &query.predicates),
            };
            
            let index_plan = PhysicalPlan {
                operation: index_scan,
                cost: 50.0, // Typically cheaper than table scan
                cardinality: 100, // Typically more selective
                estimated_time_us: 500,
                memory_required: 512 * 1024,
                vectorized: false, // Index scans typically not vectorized
                indexes_used: vec![IndexUsage {
                    table: table.to_string(),
                    index: index.clone(),
                    columns: vec!["id".to_string()], // Simplified
                    selectivity: 0.1,
                    cost_reduction: 50.0,
                }],
                join_order: vec![table.to_string()],
            };
            
            plans.push(index_plan);
        }
        
        Ok(plans)
    }

    /// Generate join plans
    fn enumerate_join_plans(&self, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut plans = Vec::new();
        
        // Generate different join orders
        let join_orders = self.generate_promising_join_orders(query)?;
        
        for join_order in join_orders {
            // For each join order, try different join algorithms
            if let Ok(plan) = self.build_plan_for_join_order(query, &join_order) {
                // Also try with different join algorithms
                plans.extend(self.generate_join_algorithm_variants(&plan, query)?);
                plans.push(plan);
            }
        }
        
        Ok(plans)
    }

    /// Generate promising join orders
    fn generate_promising_join_orders(&self, query: &LogicalQuery) -> Result<Vec<Vec<String>>> {
        let mut orders = Vec::new();
        
        // Original order
        orders.push(query.tables.clone());
        
        // Selectivity-based order
        orders.push(self.generate_selectivity_based_order(query)?);
        
        // Size-based order (smallest first)
        orders.push(self.generate_size_based_order(query)?);
        
        // Index-based order
        orders.push(self.generate_index_based_order(query)?);
        
        Ok(orders)
    }

    /// Generate selectivity-based join order
    fn generate_selectivity_based_order(&self, query: &LogicalQuery) -> Result<Vec<String>> {
        let mut tables = query.tables.clone();
        
        // Sort by estimated selectivity (most selective first)
        tables.sort_by(|a, b| {
            let a_selectivity = self.estimate_table_selectivity(a, query);
            let b_selectivity = self.estimate_table_selectivity(b, query);
            a_selectivity.partial_cmp(&b_selectivity).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(tables)
    }

    /// Generate size-based join order
    fn generate_size_based_order(&self, query: &LogicalQuery) -> Result<Vec<String>> {
        let mut tables = query.tables.clone();
        
        // Sort by estimated table size (smallest first)
        tables.sort_by(|a, b| {
            let a_size = self.estimate_table_size(a);
            let b_size = self.estimate_table_size(b);
            a_size.cmp(&b_size)
        });
        
        Ok(tables)
    }

    /// Generate index-based join order
    fn generate_index_based_order(&self, query: &LogicalQuery) -> Result<Vec<String>> {
        let mut tables = query.tables.clone();
        
        // Sort by index availability (tables with useful indexes first)
        tables.sort_by(|a, b| {
            let a_index_score = self.calculate_index_score(a, query);
            let b_index_score = self.calculate_index_score(b, query);
            b_index_score.partial_cmp(&a_index_score).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(tables)
    }

    /// Generate greedy join order
    fn generate_greedy_join_order(&self, query: &LogicalQuery) -> Result<Vec<String>> {
        if query.tables.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut remaining_tables: HashSet<String> = query.tables.iter().cloned().collect();
        let mut join_order = Vec::new();
        
        // Start with the most selective table
        let first_table = self.find_most_selective_table(&remaining_tables, query);
        join_order.push(first_table.clone());
        remaining_tables.remove(&first_table);
        
        // Greedily add remaining tables
        while !remaining_tables.is_empty() {
            let next_table = self.find_best_next_table(&join_order, &remaining_tables, query);
            join_order.push(next_table.clone());
            remaining_tables.remove(&next_table);
        }
        
        Ok(join_order)
    }

    /// Build plan for specific join order
    fn build_plan_for_join_order(&self, query: &LogicalQuery, join_order: &[String]) -> Result<PhysicalPlan> {
        if join_order.is_empty() {
            return Err(Error::Generic("Empty join order".to_string()));
        }
        
        if join_order.len() == 1 {
            // Single table - use table access plan
            let plans = self.enumerate_table_access_plans(&join_order[0], query)?;
            return Ok(plans.into_iter().next().unwrap_or_else(|| {
                PhysicalPlan {
                    operation: PhysicalOperator::TableScan {
                        table: join_order[0].clone(),
                        predicate: None,
                        columns: Vec::new(),
                    },
                    cost: 100.0,
                    cardinality: 1000,
                    estimated_time_us: 1000,
                    memory_required: 1024 * 1024,
                    vectorized: false,
                    indexes_used: Vec::new(),
                    join_order: join_order.to_vec(),
                }
            }));
        }
        
        // Multi-table join
        let mut current_plan = self.build_plan_for_join_order(query, &join_order[0..1])?;
        
        for i in 1..join_order.len() {
            let right_plan = self.build_plan_for_join_order(query, &join_order[i..i+1])?;
            current_plan = self.create_join_plan(current_plan, right_plan, query)?;
        }
        
        Ok(current_plan)
    }

    /// Create join plan between two subplans
    fn create_join_plan(&self, left: PhysicalPlan, right: PhysicalPlan, query: &LogicalQuery) -> Result<PhysicalPlan> {
        // Find appropriate join condition
        let join_condition = self.find_join_condition(&left.join_order, &right.join_order, query)?;
        
        // Choose join algorithm
        let join_algorithm = self.choose_join_algorithm(&left, &right);
        
        let operation = match join_algorithm {
            JoinAlgorithmChoice::Hash => PhysicalOperator::HashJoin {
                left: Box::new(left.operation.clone()),
                right: Box::new(right.operation.clone()),
                condition: join_condition,
                join_type: JoinType::Inner, // Simplified
                build_side: if left.cardinality <= right.cardinality {
                    super::BuildSide::Left
                } else {
                    super::BuildSide::Right
                },
            },
            JoinAlgorithmChoice::SortMerge => PhysicalOperator::SortMergeJoin {
                left: Box::new(left.operation.clone()),
                right: Box::new(right.operation.clone()),
                condition: join_condition,
                join_type: JoinType::Inner,
            },
            JoinAlgorithmChoice::NestedLoop => PhysicalOperator::NestedLoopJoin {
                left: Box::new(left.operation.clone()),
                right: Box::new(right.operation.clone()),
                condition: join_condition,
                join_type: JoinType::Inner,
            },
        };
        
        let mut join_order = left.join_order.clone();
        join_order.extend(right.join_order);
        
        Ok(PhysicalPlan {
            operation,
            cost: left.cost + right.cost + 10.0, // Simplified join cost
            cardinality: (left.cardinality + right.cardinality) / 2, // Simplified
            estimated_time_us: left.estimated_time_us + right.estimated_time_us + 1000,
            memory_required: left.memory_required + right.memory_required,
            vectorized: left.vectorized && right.vectorized,
            indexes_used: {
                let mut indexes = left.indexes_used;
                indexes.extend(right.indexes_used);
                indexes
            },
            join_order,
        })
    }

    /// Choose join algorithm
    fn choose_join_algorithm(&self, left: &PhysicalPlan, right: &PhysicalPlan) -> JoinAlgorithmChoice {
        let left_size = left.cardinality;
        let right_size = right.cardinality;
        
        // Hash join for size difference
        if left_size * 10 < right_size || right_size * 10 < left_size {
            return JoinAlgorithmChoice::Hash;
        }
        
        // Sort-merge for large similar-sized tables
        if left_size > 10000 && right_size > 10000 {
            return JoinAlgorithmChoice::SortMerge;
        }
        
        // Nested loop for small tables
        if left_size < 1000 && right_size < 1000 {
            return JoinAlgorithmChoice::NestedLoop;
        }
        
        // Default to hash join
        JoinAlgorithmChoice::Hash
    }

    /// Add aggregation plans
    fn add_aggregation_plans(&self, plans: Vec<PhysicalPlan>, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut result = Vec::new();
        
        for plan in plans {
            let agg_operation = PhysicalOperator::Aggregation {
                input: Box::new(plan.operation.clone()),
                group_by: Vec::new(), // Would extract from query
                aggregates: query.aggregates.clone(),
                vectorized: self.config.enable_vectorization,
            };
            
            let agg_plan = PhysicalPlan {
                operation: agg_operation,
                cost: plan.cost + 5.0, // Aggregation cost
                cardinality: plan.cardinality / 10, // Aggregation typically reduces rows
                estimated_time_us: plan.estimated_time_us + 500,
                memory_required: plan.memory_required + 1024 * 1024,
                vectorized: plan.vectorized,
                indexes_used: plan.indexes_used,
                join_order: plan.join_order,
            };
            
            result.push(agg_plan);
        }
        
        Ok(result)
    }

    /// Add sorting plans
    fn add_sorting_plans(&self, plans: Vec<PhysicalPlan>, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut result = Vec::new();
        
        for plan in plans {
            let sort_operation = PhysicalOperator::Sort {
                input: Box::new(plan.operation.clone()),
                sort_keys: query.order_by.clone(),
                limit: query.limit,
            };
            
            let sort_plan = PhysicalPlan {
                operation: sort_operation,
                cost: plan.cost + (plan.cardinality as f64 * plan.cardinality as f64).log2() * 0.01,
                cardinality: plan.cardinality,
                estimated_time_us: plan.estimated_time_us + (plan.cardinality / 1000) as u64,
                memory_required: plan.memory_required + plan.cardinality * 64,
                vectorized: plan.vectorized,
                indexes_used: plan.indexes_used,
                join_order: plan.join_order,
            };
            
            result.push(sort_plan);
        }
        
        Ok(result)
    }

    /// Add limit plans
    fn add_limit_plans(&self, plans: Vec<PhysicalPlan>, query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        let mut result = Vec::new();
        
        if let Some(limit_count) = query.limit {
            for plan in plans {
                let limit_operation = PhysicalOperator::Limit {
                    input: Box::new(plan.operation.clone()),
                    count: limit_count,
                    offset: 0, // Would extract from query
                };
                
                let limit_plan = PhysicalPlan {
                    operation: limit_operation,
                    cost: plan.cost, // Limit is essentially free
                    cardinality: plan.cardinality.min(limit_count as u64),
                    estimated_time_us: plan.estimated_time_us,
                    memory_required: plan.memory_required,
                    vectorized: plan.vectorized,
                    indexes_used: plan.indexes_used,
                    join_order: plan.join_order,
                };
                
                result.push(limit_plan);
            }
        } else {
            result = plans;
        }
        
        Ok(result)
    }

    // Helper methods

    fn generate_all_join_orders(&self, tables: &[String]) -> Vec<Vec<String>> {
        if tables.len() <= 1 {
            return vec![tables.to_vec()];
        }
        
        let mut orders = Vec::new();
        self.permute_tables(tables, 0, &mut orders);
        orders
    }

    fn permute_tables(&self, tables: &[String], start: usize, result: &mut Vec<Vec<String>>) {
        if start >= tables.len() {
            result.push(tables.to_vec());
            return;
        }
        
        let mut tables = tables.to_vec();
        for i in start..tables.len() {
            tables.swap(start, i);
            self.permute_tables(&tables, start + 1, result);
            tables.swap(start, i);
        }
    }

    fn generate_table_subsets(&self, tables: &[String], size: usize) -> Vec<Vec<String>> {
        let mut subsets = Vec::new();
        let mut current = Vec::new();
        self.generate_subsets_recursive(tables, size, 0, &mut current, &mut subsets);
        subsets
    }

    fn generate_subsets_recursive(
        &self,
        tables: &[String],
        target_size: usize,
        start: usize,
        current: &mut Vec<String>,
        result: &mut Vec<Vec<String>>,
    ) {
        if current.len() == target_size {
            result.push(current.clone());
            return;
        }
        
        for i in start..tables.len() {
            current.push(tables[i].clone());
            self.generate_subsets_recursive(tables, target_size, i + 1, current, result);
            current.pop();
        }
    }

    fn generate_subset_splits(&self, subset: &[String], left_size: usize) -> Vec<(Vec<String>, Vec<String>)> {
        let mut splits = Vec::new();
        let left_subsets = self.generate_table_subsets(subset, left_size);
        
        for left_subset in left_subsets {
            let right_subset: Vec<String> = subset
                .iter()
                .filter(|table| !left_subset.contains(table))
                .cloned()
                .collect();
            
            if !right_subset.is_empty() {
                splits.push((left_subset, right_subset));
            }
        }
        
        splits
    }

    fn generate_join_algorithm_variants(&self, base_plan: &PhysicalPlan, _query: &LogicalQuery) -> Result<Vec<PhysicalPlan>> {
        // For now, just return the base plan
        // In a real implementation, this would generate variants with different join algorithms
        Ok(vec![base_plan.clone()])
    }

    fn extract_table_predicates(&self, table: &str, predicates: &[Predicate]) -> Option<Predicate> {
        // Find predicates that apply to this table
        // For simplicity, return None
        // In a real implementation, this would analyze the predicates
        None
    }

    fn extract_projected_columns(&self, table: &str, projections: &[String]) -> Vec<String> {
        // Extract columns for this table from projections
        // For simplicity, return common columns
        vec!["id".to_string(), "name".to_string()]
    }

    fn estimate_table_selectivity(&self, table: &str, query: &LogicalQuery) -> f64 {
        // Estimate how selective predicates are for this table
        0.1 // Default 10% selectivity
    }

    fn estimate_table_size(&self, table: &str) -> u64 {
        // Estimate table size (would use statistics in real implementation)
        match table {
            t if t.contains("user") => 100_000,
            t if t.contains("order") => 1_000_000,
            t if t.contains("product") => 10_000,
            _ => 50_000,
        }
    }

    fn calculate_index_score(&self, table: &str, query: &LogicalQuery) -> f64 {
        // Calculate how useful indexes are for this table
        1.0 // Default score
    }

    fn find_most_selective_table(&self, tables: &HashSet<String>, query: &LogicalQuery) -> String {
        tables
            .iter()
            .min_by(|a, b| {
                let a_selectivity = self.estimate_table_selectivity(a, query);
                let b_selectivity = self.estimate_table_selectivity(b, query);
                a_selectivity.partial_cmp(&b_selectivity).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .unwrap_or_else(|| tables.iter().next().unwrap().clone())
    }

    fn find_best_next_table(&self, current_order: &[String], remaining: &HashSet<String>, query: &LogicalQuery) -> String {
        // Find the best next table to join based on various criteria
        remaining
            .iter()
            .max_by(|a, b| {
                let a_score = self.calculate_join_score(current_order, a, query);
                let b_score = self.calculate_join_score(current_order, b, query);
                a_score.partial_cmp(&b_score).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .unwrap_or_else(|| remaining.iter().next().unwrap().clone())
    }

    fn calculate_join_score(&self, current_order: &[String], candidate: &str, query: &LogicalQuery) -> f64 {
        // Calculate score for joining this candidate table
        // Higher score = better candidate
        1.0 // Default score
    }

    fn find_join_condition(&self, left_tables: &[String], right_tables: &[String], query: &LogicalQuery) -> Result<JoinCondition> {
        // Find appropriate join condition between table sets
        // For simplicity, return a default condition
        Ok(JoinCondition::Equi {
            left_column: "id".to_string(),
            right_column: "foreign_id".to_string(),
        })
    }
}

/// Join algorithm selection
#[derive(Debug, Clone, Copy)]
enum JoinAlgorithmChoice {
    Hash,
    SortMerge,
    NestedLoop,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plan_enumerator_creation() {
        let config = OptimizerConfig::default();
        let cost_model = Arc::new(CostModel::new(config.clone()));
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        
        let enumerator = PlanEnumerator::new(cost_model, cardinality_estimator, config);
        assert!(enumerator.plan_cache.is_empty());
    }

    #[test]
    fn test_enumeration_strategy_selection() {
        let config = OptimizerConfig::default();
        let cost_model = Arc::new(CostModel::new(config.clone()));
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let enumerator = PlanEnumerator::new(cost_model, cardinality_estimator, config);

        // Small query should use exhaustive
        let small_query = LogicalQuery {
            tables: vec!["users".to_string()],
            projections: vec!["id".to_string()],
            predicates: vec![],
            joins: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
        };
        
        let strategy = enumerator.choose_enumeration_strategy(&small_query);
        assert!(matches!(strategy, EnumerationStrategy::Exhaustive));

        // Large query should use greedy
        let large_query = LogicalQuery {
            tables: vec!["t1".to_string(), "t2".to_string(), "t3".to_string(), "t4".to_string()],
            projections: vec!["col1".to_string(), "col2".to_string()],
            predicates: vec![
                Predicate::Comparison {
                    column: "id".to_string(),
                    operator: ComparisonOp::Equal,
                    value: Value::Integer(1),
                },
                Predicate::Comparison {
                    column: "name".to_string(),
                    operator: ComparisonOp::Like,
                    value: Value::String("test%".to_string()),
                },
            ],
            joins: vec![
                LogicalJoin {
                    left_table: "t1".to_string(),
                    right_table: "t2".to_string(),
                    condition: JoinCondition::Equi {
                        left_column: "id".to_string(),
                        right_column: "t1_id".to_string(),
                    },
                    join_type: JoinType::Inner,
                },
            ],
            aggregates: vec![AggregateFunction::Count],
            order_by: vec![SortKey {
                column: "id".to_string(),
                direction: super::SortDirection::Ascending,
                nulls_first: false,
            }],
            limit: Some(100),
        };

        let strategy = enumerator.choose_enumeration_strategy(&large_query);
        assert!(matches!(strategy, EnumerationStrategy::Greedy));
    }

    #[test]
    fn test_table_access_plan_generation() {
        let config = OptimizerConfig::default();
        let cost_model = Arc::new(CostModel::new(config.clone()));
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let enumerator = PlanEnumerator::new(cost_model, cardinality_estimator, config);

        let query = LogicalQuery {
            tables: vec!["users".to_string()],
            projections: vec!["id".to_string(), "name".to_string()],
            predicates: vec![],
            joins: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
        };

        let plans = enumerator.enumerate_table_access_plans("users", &query).unwrap();
        assert!(!plans.is_empty());
        
        // Should have at least table scan plan
        let has_table_scan = plans.iter().any(|p| {
            matches!(p.operation, PhysicalOperator::TableScan { .. })
        });
        assert!(has_table_scan);
    }
}
//! Join Optimization for Multi-Table Queries
//!
//! This module optimizes join order and algorithm selection for complex queries.
//! It uses dynamic programming, heuristics, and cost-based analysis.

use crate::{Result, Error};
use super::{
    CardinalityEstimator, CostModel, LogicalQuery, LogicalJoin, PhysicalOperator,
    JoinCondition, JoinType, Predicate, TableStatistics, ColumnStatistics, IndexStatistics,
    OptimizerConfig, StatisticsManager, Value
};
use std::collections::{HashMap, HashSet, BTreeSet, VecDeque};
use std::sync::Arc;

/// Join optimizer for multi-table query optimization
#[derive(Debug)]
pub struct JoinOptimizer {
    /// Cardinality estimator for result size estimation
    cardinality_estimator: Arc<CardinalityEstimator>,
    /// Cost model for join cost calculation
    cost_model: Arc<CostModel>,
    /// Join order enumeration cache
    order_cache: HashMap<String, JoinOrderPlan>,
    /// Configuration
    config: OptimizerConfig,
}

/// Join order optimization plan
#[derive(Debug, Clone)]
pub struct JoinOrderPlan {
    /// Optimal join order (table names)
    pub join_order: Vec<String>,
    /// Join tree representing the plan
    pub join_tree: JoinTree,
    /// Total estimated cost
    pub total_cost: f64,
    /// Estimated result cardinality
    pub result_cardinality: u64,
    /// Join algorithm choices
    pub join_algorithms: Vec<JoinAlgorithmChoice>,
    /// Optimization metadata
    pub metadata: JoinOptimizationMetadata,
}

/// Join tree structure for representing join plans
#[derive(Debug, Clone)]
pub enum JoinTree {
    /// Leaf node (table)
    Table {
        name: String,
        estimated_cardinality: u64,
        access_method: TableAccessMethod,
    },
    /// Join node
    Join {
        left: Box<JoinTree>,
        right: Box<JoinTree>,
        condition: JoinCondition,
        join_type: JoinType,
        algorithm: JoinAlgorithmChoice,
        estimated_cost: f64,
        estimated_cardinality: u64,
    },
}

/// Table access methods
#[derive(Debug, Clone)]
pub enum TableAccessMethod {
    /// Full table scan
    TableScan,
    /// Index scan
    IndexScan {
        index_name: String,
        selectivity: f64,
    },
    /// Index-only scan (covering index)
    IndexOnlyScan {
        index_name: String,
        selectivity: f64,
    },
}

/// Join algorithm choices
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgorithmChoice {
    /// Hash join
    Hash,
    /// Sort-merge join
    SortMerge,
    /// Nested loop join
    NestedLoop,
    /// Index nested loop join
    IndexNestedLoop,
    /// Vectorized hash join
    VectorizedHash,
}

/// Join optimization metadata
#[derive(Debug, Clone)]
pub struct JoinOptimizationMetadata {
    /// Optimization time in microseconds
    pub optimization_time_us: u64,
    /// Number of join orders considered
    pub orders_considered: usize,
    /// Enumeration strategy used
    pub strategy_used: JoinEnumerationStrategy,
    /// Statistics availability
    pub statistics_coverage: f64,
    /// Confidence in the plan
    pub plan_confidence: f64,
}

/// Join enumeration strategies
#[derive(Debug, Clone, Copy)]
pub enum JoinEnumerationStrategy {
    /// Dynamic programming (optimal for small queries)
    DynamicProgramming,
    /// Greedy heuristic
    Greedy,
    /// Left-deep enumeration
    LeftDeep,
    /// Bushy tree enumeration
    Bushy,
    /// Genetic algorithm
    Genetic,
}

/// Join selectivity estimation
#[derive(Debug, Clone)]
pub struct JoinSelectivity {
    /// Estimated selectivity factor
    pub selectivity: f64,
    /// Confidence in the estimate
    pub confidence: f64,
    /// Method used for estimation
    pub estimation_method: SelectivityMethod,
}

/// Selectivity estimation methods
#[derive(Debug, Clone, Copy)]
pub enum SelectivityMethod {
    /// Foreign key relationships
    ForeignKey,
    /// Histogram overlap analysis
    Histogram,
    /// Sampling-based estimation
    Sampling,
    /// Independence assumption
    Independence,
    /// Heuristic estimation
    Heuristic,
}

/// Join graph for dependency analysis
#[derive(Debug)]
pub struct JoinGraph {
    /// Tables in the query
    pub tables: HashSet<String>,
    /// Join edges with conditions
    pub edges: Vec<JoinEdge>,
    /// Table dependencies
    pub dependencies: HashMap<String, HashSet<String>>,
}

/// Join edge in the join graph
#[derive(Debug, Clone)]
pub struct JoinEdge {
    /// Left table
    pub left_table: String,
    /// Right table
    pub right_table: String,
    /// Join condition
    pub condition: JoinCondition,
    /// Join type
    pub join_type: JoinType,
    /// Estimated selectivity
    pub selectivity: f64,
    /// Join cost without index
    pub base_cost: f64,
}

impl JoinOptimizer {
    /// Create new join optimizer
    pub fn new(
        cardinality_estimator: Arc<CardinalityEstimator>,
        cost_model: Arc<CostModel>,
    ) -> Self {
        Self {
            cardinality_estimator,
            cost_model,
            order_cache: HashMap::new(),
            config: OptimizerConfig::default(),
        }
    }

    /// Optimize join order for a logical query
    pub fn optimize_join_order(&self, mut query: LogicalQuery) -> Result<LogicalQuery> {
        if query.tables.len() <= 1 || query.joins.is_empty() {
            return Ok(query); // No joins to optimize
        }

        // Build join graph
        let join_graph = self.build_join_graph(&query)?;

        // Choose optimization strategy
        let strategy = self.choose_optimization_strategy(&query);

        // Generate optimal join order
        let join_plan = match strategy {
            JoinEnumerationStrategy::DynamicProgramming => {
                self.optimize_with_dynamic_programming(&query, &join_graph)?
            }
            JoinEnumerationStrategy::Greedy => {
                self.optimize_with_greedy(&query, &join_graph)?
            }
            JoinEnumerationStrategy::LeftDeep => {
                self.optimize_left_deep(&query, &join_graph)?
            }
            JoinEnumerationStrategy::Bushy => {
                self.optimize_bushy(&query, &join_graph)?
            }
            JoinEnumerationStrategy::Genetic => {
                self.optimize_with_genetic(&query, &join_graph)?
            }
        };

        // Apply the optimized plan to the query
        query.tables = join_plan.join_order.clone();
        query.joins = self.reconstruct_joins_from_plan(&join_plan, &join_graph)?;

        Ok(query)
    }

    /// Build join graph from query
    fn build_join_graph(&self, query: &LogicalQuery) -> Result<JoinGraph> {
        let mut graph = JoinGraph {
            tables: query.tables.iter().cloned().collect(),
            edges: Vec::new(),
            dependencies: HashMap::new(),
        };

        // Add explicit join edges
        for join in &query.joins {
            let selectivity = self.estimate_join_selectivity(&join.condition)?;
            
            let edge = JoinEdge {
                left_table: join.left_table.clone(),
                right_table: join.right_table.clone(),
                condition: join.condition.clone(),
                join_type: join.join_type,
                selectivity: selectivity.selectivity,
                base_cost: 100.0, // Would be calculated by cost model
            };
            
            graph.edges.push(edge);
            
            // Add dependencies
            graph.dependencies
                .entry(join.left_table.clone())
                .or_default()
                .insert(join.right_table.clone());
        }

        // Analyze implicit joins from WHERE clause predicates
        self.analyze_implicit_joins(query, &mut graph)?;

        Ok(graph)
    }

    /// Analyze implicit joins from WHERE predicates
    fn analyze_implicit_joins(&self, query: &LogicalQuery, graph: &mut JoinGraph) -> Result<()> {
        // Look for predicates that compare columns from different tables
        // This would identify potential join conditions not explicitly specified
        for predicate in &query.predicates {
            self.extract_cross_table_predicates(predicate, graph)?;
        }
        
        Ok(())
    }

    /// Extract cross-table predicates that could be join conditions
    fn extract_cross_table_predicates(&self, predicate: &Predicate, graph: &mut JoinGraph) -> Result<()> {
        match predicate {
            Predicate::Comparison { column, .. } => {
                // Would analyze if this comparison involves multiple tables
                // For now, simplified implementation
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.extract_cross_table_predicates(left, graph)?;
                self.extract_cross_table_predicates(right, graph)?;
            }
            Predicate::Not(inner) => {
                self.extract_cross_table_predicates(inner, graph)?;
            }
            _ => {}
        }
        
        Ok(())
    }

    /// Choose optimization strategy based on query characteristics
    fn choose_optimization_strategy(&self, query: &LogicalQuery) -> JoinEnumerationStrategy {
        let table_count = query.tables.len();
        let join_count = query.joins.len();
        
        match (table_count, join_count) {
            (0..=4, _) => JoinEnumerationStrategy::DynamicProgramming,
            (5..=8, _) => JoinEnumerationStrategy::Greedy,
            (9..=15, _) => JoinEnumerationStrategy::LeftDeep,
            (16..=25, _) => JoinEnumerationStrategy::Bushy,
            _ => JoinEnumerationStrategy::Genetic,
        }
    }

    /// Optimize using dynamic programming
    fn optimize_with_dynamic_programming(&self, query: &LogicalQuery, graph: &JoinGraph) -> Result<JoinOrderPlan> {
        let tables = &query.tables;
        let n = tables.len();
        
        // DP table: dp[mask] = best plan for subset represented by mask
        let mut dp: HashMap<u32, JoinTree> = HashMap::new();
        
        // Base case: single tables
        for (i, table) in tables.iter().enumerate() {
            let mask = 1u32 << i;
            let cardinality = self.estimate_table_cardinality(table)?;
            
            dp.insert(mask, JoinTree::Table {
                name: table.clone(),
                estimated_cardinality: cardinality,
                access_method: TableAccessMethod::TableScan,
            });
        }
        
        // Fill DP table for larger subsets
        for subset_size in 2..=n {
            for mask in 1u32..(1u32 << n) {
                if mask.count_ones() as usize != subset_size {
                    continue;
                }
                
                let mut best_plan: Option<JoinTree> = None;
                let mut best_cost = f64::INFINITY;
                
                // Try all possible splits
                for left_mask in 1u32..mask {
                    if (left_mask & mask) != left_mask {
                        continue;
                    }
                    
                    let right_mask = mask ^ left_mask;
                    if right_mask == 0 {
                        continue;
                    }
                    
                    if let (Some(left_tree), Some(right_tree)) = (dp.get(&left_mask), dp.get(&right_mask)) {
                        if let Some(join_edge) = self.find_join_edge(left_mask, right_mask, tables, graph) {
                            let join_tree = self.create_join_tree(
                                left_tree.clone(),
                                right_tree.clone(),
                                &join_edge,
                            )?;
                            
                            let cost = self.calculate_join_tree_cost(&join_tree)?;
                            if cost < best_cost {
                                best_cost = cost;
                                best_plan = Some(join_tree);
                            }
                        }
                    }
                }
                
                if let Some(plan) = best_plan {
                    dp.insert(mask, plan);
                }
            }
        }
        
        // Extract final plan
        let final_mask = (1u32 << n) - 1;
        if let Some(final_tree) = dp.get(&final_mask) {
            Ok(JoinOrderPlan {
                join_order: self.extract_join_order(final_tree),
                join_tree: final_tree.clone(),
                total_cost: self.calculate_join_tree_cost(final_tree)?,
                result_cardinality: self.get_tree_cardinality(final_tree),
                join_algorithms: self.extract_join_algorithms(final_tree),
                metadata: JoinOptimizationMetadata {
                    optimization_time_us: 1000, // Would be measured
                    orders_considered: dp.len(),
                    strategy_used: JoinEnumerationStrategy::DynamicProgramming,
                    statistics_coverage: 0.8,
                    plan_confidence: 0.9,
                },
            })
        } else {
            Err(Error::Generic("No valid join plan found".to_string()))
        }
    }

    /// Optimize using greedy heuristic
    fn optimize_with_greedy(&self, query: &LogicalQuery, graph: &JoinGraph) -> Result<JoinOrderPlan> {
        let mut remaining_tables: HashSet<String> = query.tables.iter().cloned().collect();
        let mut join_order = Vec::new();
        let mut current_tree: Option<JoinTree> = None;
        
        // Start with the most selective table
        let first_table = self.find_most_selective_table(&remaining_tables, graph)?;
        join_order.push(first_table.clone());
        remaining_tables.remove(&first_table);
        
        let cardinality = self.estimate_table_cardinality(&first_table)?;
        current_tree = Some(JoinTree::Table {
            name: first_table,
            estimated_cardinality: cardinality,
            access_method: TableAccessMethod::TableScan,
        });
        
        // Greedily add remaining tables
        while !remaining_tables.is_empty() {
            let (next_table, best_edge) = self.find_best_next_join(&join_order, &remaining_tables, graph)?;
            
            let next_cardinality = self.estimate_table_cardinality(&next_table)?;
            let right_tree = JoinTree::Table {
                name: next_table.clone(),
                estimated_cardinality: next_cardinality,
                access_method: TableAccessMethod::TableScan,
            };
            
            if let Some(left_tree) = current_tree {
                current_tree = Some(self.create_join_tree(left_tree, right_tree, &best_edge)?);
            }
            
            join_order.push(next_table.clone());
            remaining_tables.remove(&next_table);
        }
        
        if let Some(final_tree) = current_tree {
            Ok(JoinOrderPlan {
                join_order,
                join_tree: final_tree.clone(),
                total_cost: self.calculate_join_tree_cost(&final_tree)?,
                result_cardinality: self.get_tree_cardinality(&final_tree),
                join_algorithms: self.extract_join_algorithms(&final_tree),
                metadata: JoinOptimizationMetadata {
                    optimization_time_us: 500,
                    orders_considered: query.tables.len(),
                    strategy_used: JoinEnumerationStrategy::Greedy,
                    statistics_coverage: 0.7,
                    plan_confidence: 0.7,
                },
            })
        } else {
            Err(Error::Generic("Failed to build join plan".to_string()))
        }
    }

    /// Optimize using left-deep trees
    fn optimize_left_deep(&self, query: &LogicalQuery, graph: &JoinGraph) -> Result<JoinOrderPlan> {
        // Generate left-deep join order based on selectivity
        let mut tables = query.tables.clone();
        tables.sort_by(|a, b| {
            let a_selectivity = self.estimate_table_selectivity(a, graph);
            let b_selectivity = self.estimate_table_selectivity(b, graph);
            a_selectivity.partial_cmp(&b_selectivity).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Build left-deep join tree
        let mut join_tree = None;
        
        for (i, table) in tables.iter().enumerate() {
            let cardinality = self.estimate_table_cardinality(table)?;
            let table_tree = JoinTree::Table {
                name: table.clone(),
                estimated_cardinality: cardinality,
                access_method: TableAccessMethod::TableScan,
            };
            
            if i == 0 {
                join_tree = Some(table_tree);
            } else if let Some(left_tree) = join_tree.take() {
                // Find join edge
                if let Some(edge) = self.find_table_join_edge(&tables[0..i], table, graph) {
                    join_tree = Some(self.create_join_tree(left_tree, table_tree, &edge)?);
                } else {
                    // If no edge found, put the left_tree back
                    join_tree = Some(left_tree);
                }
            }
        }
        
        if let Some(final_tree) = join_tree {
            Ok(JoinOrderPlan {
                join_order: tables,
                join_tree: final_tree.clone(),
                total_cost: self.calculate_join_tree_cost(&final_tree)?,
                result_cardinality: self.get_tree_cardinality(&final_tree),
                join_algorithms: self.extract_join_algorithms(&final_tree),
                metadata: JoinOptimizationMetadata {
                    optimization_time_us: 300,
                    orders_considered: 1,
                    strategy_used: JoinEnumerationStrategy::LeftDeep,
                    statistics_coverage: 0.6,
                    plan_confidence: 0.6,
                },
            })
        } else {
            Err(Error::Generic("Failed to build left-deep plan".to_string()))
        }
    }

    /// Optimize using bushy trees
    fn optimize_bushy(&self, query: &LogicalQuery, graph: &JoinGraph) -> Result<JoinOrderPlan> {
        // For bushy trees, we would implement a more sophisticated algorithm
        // For now, fall back to greedy
        self.optimize_with_greedy(query, graph)
    }

    /// Optimize using genetic algorithm
    fn optimize_with_genetic(&self, query: &LogicalQuery, graph: &JoinGraph) -> Result<JoinOrderPlan> {
        // Genetic algorithm would evolve join orders over generations
        // For now, fall back to greedy
        self.optimize_with_greedy(query, graph)
    }

    /// Estimate join selectivity
    fn estimate_join_selectivity(&self, condition: &JoinCondition) -> Result<JoinSelectivity> {
        match condition {
            JoinCondition::Equi { left_column, right_column } => {
                // Estimate based on foreign key relationships and cardinalities
                Ok(JoinSelectivity {
                    selectivity: 0.1, // Default 10% selectivity
                    confidence: 0.7,
                    estimation_method: SelectivityMethod::Independence,
                })
            }
            JoinCondition::Complex(_) => {
                // Complex conditions are typically less selective
                Ok(JoinSelectivity {
                    selectivity: 0.05,
                    confidence: 0.5,
                    estimation_method: SelectivityMethod::Heuristic,
                })
            }
        }
    }

    /// Find join edge between table subsets
    fn find_join_edge(&self, left_mask: u32, right_mask: u32, tables: &[String], graph: &JoinGraph) -> Option<JoinEdge> {
        let left_tables: Vec<&String> = tables.iter()
            .enumerate()
            .filter(|(i, _)| (left_mask & (1u32 << i)) != 0)
            .map(|(_, table)| table)
            .collect();
        
        let right_tables: Vec<&String> = tables.iter()
            .enumerate()
            .filter(|(i, _)| (right_mask & (1u32 << i)) != 0)
            .map(|(_, table)| table)
            .collect();
        
        // Find edge connecting left and right subsets
        for edge in &graph.edges {
            if left_tables.contains(&&edge.left_table) && right_tables.contains(&&edge.right_table) {
                return Some(edge.clone());
            }
            if left_tables.contains(&&edge.right_table) && right_tables.contains(&&edge.left_table) {
                return Some(edge.clone());
            }
        }
        
        None
    }

    /// Create join tree node
    fn create_join_tree(&self, left: JoinTree, right: JoinTree, edge: &JoinEdge) -> Result<JoinTree> {
        let left_cardinality = self.get_tree_cardinality(&left);
        let right_cardinality = self.get_tree_cardinality(&right);
        
        // Choose join algorithm
        let algorithm = self.choose_join_algorithm(left_cardinality, right_cardinality, &edge.condition);
        
        // Estimate join cost and cardinality
        let join_cost = self.estimate_join_cost(left_cardinality, right_cardinality, algorithm, edge)?;
        let result_cardinality = self.estimate_join_result_cardinality(left_cardinality, right_cardinality, edge.selectivity);
        
        Ok(JoinTree::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition: edge.condition.clone(),
            join_type: edge.join_type,
            algorithm,
            estimated_cost: join_cost,
            estimated_cardinality: result_cardinality,
        })
    }

    /// Choose join algorithm based on characteristics
    fn choose_join_algorithm(&self, left_cardinality: u64, right_cardinality: u64, condition: &JoinCondition) -> JoinAlgorithmChoice {
        match condition {
            JoinCondition::Equi { .. } => {
                // For equi-joins, choose based on size difference
                let size_ratio = if left_cardinality > 0 && right_cardinality > 0 {
                    (left_cardinality as f64 / right_cardinality as f64).max(right_cardinality as f64 / left_cardinality as f64)
                } else {
                    1.0
                };
                
                if size_ratio > 10.0 {
                    JoinAlgorithmChoice::Hash
                } else if left_cardinality > 100_000 && right_cardinality > 100_000 {
                    JoinAlgorithmChoice::SortMerge
                } else if left_cardinality < 1000 || right_cardinality < 1000 {
                    JoinAlgorithmChoice::NestedLoop
                } else {
                    JoinAlgorithmChoice::Hash
                }
            }
            JoinCondition::Complex(_) => {
                // Complex conditions typically need nested loop
                if left_cardinality < 10_000 && right_cardinality < 10_000 {
                    JoinAlgorithmChoice::NestedLoop
                } else {
                    JoinAlgorithmChoice::Hash
                }
            }
        }
    }

    /// Estimate join cost
    fn estimate_join_cost(&self, left_cardinality: u64, right_cardinality: u64, algorithm: JoinAlgorithmChoice, edge: &JoinEdge) -> Result<f64> {
        let base_cost = match algorithm {
            JoinAlgorithmChoice::Hash => {
                // Hash join: O(M + N) where M, N are input sizes
                (left_cardinality + right_cardinality) as f64 * 0.01
            }
            JoinAlgorithmChoice::SortMerge => {
                // Sort-merge: O(M log M + N log N)
                left_cardinality as f64 * (left_cardinality as f64).log2() * 0.001 +
                right_cardinality as f64 * (right_cardinality as f64).log2() * 0.001
            }
            JoinAlgorithmChoice::NestedLoop => {
                // Nested loop: O(M * N)
                left_cardinality as f64 * right_cardinality as f64 * 0.000001
            }
            JoinAlgorithmChoice::IndexNestedLoop => {
                // Index nested loop: O(M * log N)
                left_cardinality as f64 * (right_cardinality as f64).log2() * 0.001
            }
            JoinAlgorithmChoice::VectorizedHash => {
                // Vectorized hash join: faster than regular hash join
                (left_cardinality + right_cardinality) as f64 * 0.005
            }
        };
        
        Ok(base_cost)
    }

    /// Estimate join result cardinality
    fn estimate_join_result_cardinality(&self, left_cardinality: u64, right_cardinality: u64, selectivity: f64) -> u64 {
        // Simplified cardinality estimation
        ((left_cardinality as f64 * right_cardinality as f64).sqrt() * selectivity) as u64
    }

    // Helper methods

    fn estimate_table_cardinality(&self, table: &str) -> Result<u64> {
        // Would use statistics in real implementation
        Ok(match table {
            t if t.contains("user") => 100_000,
            t if t.contains("order") => 1_000_000,
            t if t.contains("product") => 10_000,
            _ => 50_000,
        })
    }

    fn estimate_table_selectivity(&self, table: &str, graph: &JoinGraph) -> f64 {
        // Estimate how selective this table is based on predicates
        0.1 // Default selectivity
    }

    fn find_most_selective_table(&self, tables: &HashSet<String>, graph: &JoinGraph) -> Result<String> {
        tables.iter()
            .min_by(|a, b| {
                let a_sel = self.estimate_table_selectivity(a, graph);
                let b_sel = self.estimate_table_selectivity(b, graph);
                a_sel.partial_cmp(&b_sel).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .ok_or_else(|| Error::Generic("No tables found".to_string()))
    }

    fn find_best_next_join(&self, current_tables: &[String], remaining: &HashSet<String>, graph: &JoinGraph) -> Result<(String, JoinEdge)> {
        let mut best_table = None;
        let mut best_edge = None;
        let mut best_score = f64::INFINITY;
        
        for table in remaining {
            if let Some(edge) = self.find_table_join_edge(current_tables, table, graph) {
                let score = self.calculate_join_score(&edge);
                if score < best_score {
                    best_score = score;
                    best_table = Some(table.clone());
                    best_edge = Some(edge);
                }
            }
        }
        
        if let (Some(table), Some(edge)) = (best_table, best_edge) {
            Ok((table, edge))
        } else {
            Err(Error::Generic("No valid join found".to_string()))
        }
    }

    fn find_table_join_edge(&self, current_tables: &[String], new_table: &str, graph: &JoinGraph) -> Option<JoinEdge> {
        for edge in &graph.edges {
            if current_tables.contains(&edge.left_table) && edge.right_table == new_table {
                return Some(edge.clone());
            }
            if current_tables.contains(&edge.right_table) && edge.left_table == new_table {
                return Some(edge.clone());
            }
        }
        None
    }

    fn calculate_join_score(&self, edge: &JoinEdge) -> f64 {
        // Lower score = better join
        edge.base_cost * (1.0 - edge.selectivity)
    }

    fn calculate_join_tree_cost(&self, tree: &JoinTree) -> Result<f64> {
        match tree {
            JoinTree::Table { .. } => Ok(1.0), // Base table access cost
            JoinTree::Join { left, right, estimated_cost, .. } => {
                let left_cost = self.calculate_join_tree_cost(left)?;
                let right_cost = self.calculate_join_tree_cost(right)?;
                Ok(left_cost + right_cost + estimated_cost)
            }
        }
    }

    fn get_tree_cardinality(&self, tree: &JoinTree) -> u64 {
        match tree {
            JoinTree::Table { estimated_cardinality, .. } => *estimated_cardinality,
            JoinTree::Join { estimated_cardinality, .. } => *estimated_cardinality,
        }
    }

    fn extract_join_order(&self, tree: &JoinTree) -> Vec<String> {
        match tree {
            JoinTree::Table { name, .. } => vec![name.clone()],
            JoinTree::Join { left, right, .. } => {
                let mut order = self.extract_join_order(left);
                order.extend(self.extract_join_order(right));
                order
            }
        }
    }

    fn extract_join_algorithms(&self, tree: &JoinTree) -> Vec<JoinAlgorithmChoice> {
        match tree {
            JoinTree::Table { .. } => Vec::new(),
            JoinTree::Join { left, right, algorithm, .. } => {
                let mut algorithms = self.extract_join_algorithms(left);
                algorithms.extend(self.extract_join_algorithms(right));
                algorithms.push(*algorithm);
                algorithms
            }
        }
    }

    fn reconstruct_joins_from_plan(&self, plan: &JoinOrderPlan, graph: &JoinGraph) -> Result<Vec<LogicalJoin>> {
        // Reconstruct the joins based on the optimized plan
        let mut joins = Vec::new();
        
        // Extract joins from the join tree
        self.extract_joins_from_tree(&plan.join_tree, &mut joins);
        
        Ok(joins)
    }

    fn extract_joins_from_tree(&self, tree: &JoinTree, joins: &mut Vec<LogicalJoin>) {
        match tree {
            JoinTree::Table { .. } => {
                // Leaf node, no joins
            }
            JoinTree::Join { left, right, condition, join_type, .. } => {
                // Extract table names from left and right subtrees
                let left_tables = self.extract_join_order(left);
                let right_tables = self.extract_join_order(right);
                
                // Create logical join (simplified)
                if let (Some(left_table), Some(right_table)) = (left_tables.first(), right_tables.first()) {
                    joins.push(LogicalJoin {
                        left_table: left_table.clone(),
                        right_table: right_table.clone(),
                        condition: condition.clone(),
                        join_type: *join_type,
                    });
                }
                
                // Recursively extract from subtrees
                self.extract_joins_from_tree(left, joins);
                self.extract_joins_from_tree(right, joins);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_optimizer_creation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(super::StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let cost_model = Arc::new(CostModel::new(config));
        
        let optimizer = JoinOptimizer::new(cardinality_estimator, cost_model);
        assert_eq!(optimizer.order_cache.len(), 0);
    }

    #[test]
    fn test_join_algorithm_selection() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(super::StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let cost_model = Arc::new(CostModel::new(config));
        let optimizer = JoinOptimizer::new(cardinality_estimator, cost_model);

        let equi_condition = JoinCondition::Equi {
            left_column: "id".to_string(),
            right_column: "user_id".to_string(),
        };

        // Small tables should use nested loop
        let small_algo = optimizer.choose_join_algorithm(100, 200, &equi_condition);
        assert_eq!(small_algo, JoinAlgorithmChoice::NestedLoop);

        // Large size difference should use hash join
        let hash_algo = optimizer.choose_join_algorithm(10_000, 100_000, &equi_condition);
        assert_eq!(hash_algo, JoinAlgorithmChoice::Hash);

        // Large similar-sized tables should use sort-merge
        let sort_algo = optimizer.choose_join_algorithm(200_000, 250_000, &equi_condition);
        assert_eq!(sort_algo, JoinAlgorithmChoice::SortMerge);
    }

    #[test]
    fn test_join_graph_construction() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(super::StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let cost_model = Arc::new(CostModel::new(config));
        let optimizer = JoinOptimizer::new(cardinality_estimator, cost_model);

        let query = LogicalQuery {
            tables: vec!["users".to_string(), "orders".to_string()],
            projections: vec!["user_id".to_string(), "order_id".to_string()],
            predicates: vec![],
            joins: vec![LogicalJoin {
                left_table: "users".to_string(),
                right_table: "orders".to_string(),
                condition: JoinCondition::Equi {
                    left_column: "id".to_string(),
                    right_column: "user_id".to_string(),
                },
                join_type: JoinType::Inner,
            }],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
        };

        let graph = optimizer.build_join_graph(&query).unwrap();
        assert_eq!(graph.tables.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert!(graph.tables.contains("users"));
        assert!(graph.tables.contains("orders"));
    }

    #[test]
    fn test_optimization_strategy_selection() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(super::StatisticsManager::new(config.clone()));
        let cardinality_estimator = Arc::new(CardinalityEstimator::new(statistics));
        let cost_model = Arc::new(CostModel::new(config));
        let optimizer = JoinOptimizer::new(cardinality_estimator, cost_model);

        // Small query should use dynamic programming
        let small_query = LogicalQuery {
            tables: vec!["t1".to_string(), "t2".to_string(), "t3".to_string()],
            projections: vec![],
            predicates: vec![],
            joins: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
        };

        let strategy = optimizer.choose_optimization_strategy(&small_query);
        assert!(matches!(strategy, JoinEnumerationStrategy::DynamicProgramming));

        // Large query should use genetic algorithm
        let large_query = LogicalQuery {
            tables: (0..30).map(|i| format!("table_{}", i)).collect(),
            projections: vec![],
            predicates: vec![],
            joins: vec![],
            aggregates: vec![],
            order_by: vec![],
            limit: None,
        };

        let strategy = optimizer.choose_optimization_strategy(&large_query);
        assert!(matches!(strategy, JoinEnumerationStrategy::Genetic));
    }
}
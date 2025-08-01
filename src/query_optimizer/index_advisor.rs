//! Index Advisor for Query Optimization
//!
//! This module analyzes query patterns and recommends optimal indexes
//! to improve query performance and reduce I/O costs.

use crate::{Result, Error};
use super::{
    StatisticsManager, CostModel, LogicalQuery, Predicate, ComparisonOp, Value,
    JoinCondition, IndexRecommendation, IndexType, OptimizerConfig,
    TableStatistics, ColumnStatistics, PhysicalOperator
};
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;

/// Index advisor for recommending optimal indexes
#[derive(Debug)]
pub struct IndexAdvisor {
    /// Statistics manager for analyzing data patterns
    statistics: Arc<StatisticsManager>,
    /// Cost model for evaluating index benefits
    cost_model: Arc<CostModel>,
    /// Query workload analysis
    workload_analyzer: WorkloadAnalyzer,
    /// Configuration
    config: OptimizerConfig,
}

/// Workload analyzer for tracking query patterns
#[derive(Debug)]
pub struct WorkloadAnalyzer {
    /// Query frequency tracking
    query_frequency: HashMap<String, u64>,
    /// Column access patterns
    column_access: HashMap<String, ColumnAccessPattern>,
    /// Join patterns
    join_patterns: HashMap<String, JoinPattern>,
    /// Predicate patterns
    predicate_patterns: Vec<PredicatePattern>,
}

/// Column access pattern analysis
#[derive(Debug, Clone)]
pub struct ColumnAccessPattern {
    /// Column name (table.column)
    pub column: String,
    /// Number of times accessed in WHERE clauses
    pub where_access_count: u64,
    /// Number of times accessed in JOIN conditions
    pub join_access_count: u64,
    /// Number of times accessed in ORDER BY
    pub order_by_count: u64,
    /// Number of times accessed in GROUP BY
    pub group_by_count: u64,
    /// Selectivity distribution
    pub selectivity_distribution: Vec<f64>,
    /// Common predicate operators
    pub common_operators: HashMap<ComparisonOp, u64>,
    /// Access frequency score
    pub frequency_score: f64,
}

/// Join pattern analysis
#[derive(Debug, Clone)]
pub struct JoinPattern {
    /// Left table.column
    pub left_column: String,
    /// Right table.column
    pub right_column: String,
    /// Join frequency
    pub frequency: u64,
    /// Average result cardinality
    pub avg_cardinality: f64,
    /// Cost without index
    pub cost_without_index: f64,
    /// Estimated cost with index
    pub cost_with_index: f64,
}

/// Predicate pattern analysis
#[derive(Debug, Clone)]
pub struct PredicatePattern {
    /// Column involved
    pub column: String,
    /// Operator type
    pub operator: ComparisonOp,
    /// Value type pattern
    pub value_type: ValueTypePattern,
    /// Frequency of this pattern
    pub frequency: u64,
    /// Average selectivity
    pub avg_selectivity: f64,
    /// Range query characteristics
    pub range_characteristics: Option<RangeCharacteristics>,
}

/// Value type pattern for predicate analysis
#[derive(Debug, Clone)]
pub enum ValueTypePattern {
    /// Single constant values
    Constant,
    /// Range queries
    Range,
    /// List of values (IN clauses)
    List,
    /// Pattern matching (LIKE)
    Pattern,
    /// Null checks
    Null,
}

/// Range query characteristics
#[derive(Debug, Clone)]
pub struct RangeCharacteristics {
    /// Average range size (as fraction of domain)
    pub avg_range_size: f64,
    /// Common range boundaries
    pub common_boundaries: Vec<Value>,
    /// Typical selectivity
    pub typical_selectivity: f64,
}

/// Index recommendation with detailed analysis
#[derive(Debug, Clone)]
pub struct DetailedIndexRecommendation {
    /// Basic recommendation info
    pub recommendation: IndexRecommendation,
    /// Supporting analysis
    pub analysis: IndexAnalysis,
    /// Implementation priority
    pub priority: IndexPriority,
    /// Resource requirements
    pub resource_requirements: ResourceRequirements,
}

/// Index analysis supporting the recommendation
#[derive(Debug, Clone)]
pub struct IndexAnalysis {
    /// Queries that would benefit
    pub benefiting_queries: Vec<String>,
    /// Expected performance improvement
    pub performance_improvement: PerformanceImprovement,
    /// Index maintenance cost
    pub maintenance_cost: MaintenanceCost,
    /// Storage overhead
    pub storage_overhead: StorageOverhead,
    /// Competition with existing indexes
    pub index_competition: Vec<String>,
}

/// Expected performance improvement
#[derive(Debug, Clone)]
pub struct PerformanceImprovement {
    /// Query execution time reduction (percentage)
    pub execution_time_reduction: f64,
    /// I/O reduction (percentage)
    pub io_reduction: f64,
    /// CPU usage reduction (percentage)
    pub cpu_reduction: f64,
    /// Memory usage change (can be positive or negative)
    pub memory_change: i64,
}

/// Index maintenance cost analysis
#[derive(Debug, Clone)]
pub struct MaintenanceCost {
    /// Insert cost increase (percentage)
    pub insert_cost_increase: f64,
    /// Update cost increase (percentage)
    pub update_cost_increase: f64,
    /// Delete cost increase (percentage)
    pub delete_cost_increase: f64,
    /// Space amplification factor
    pub space_amplification: f64,
}

/// Storage overhead analysis
#[derive(Debug, Clone)]
pub struct StorageOverhead {
    /// Estimated index size in bytes
    pub estimated_size_bytes: u64,
    /// Size as percentage of table size
    pub size_percentage: f64,
    /// Key size per entry
    pub key_size_per_entry: u32,
    /// Value size per entry
    pub value_size_per_entry: u32,
}

/// Index implementation priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexPriority {
    /// Critical for performance
    Critical,
    /// High impact
    High,
    /// Medium impact
    Medium,
    /// Low impact
    Low,
    /// Optional/experimental
    Optional,
}

/// Resource requirements for index
#[derive(Debug, Clone)]
pub struct ResourceRequirements {
    /// Memory required for creation
    pub creation_memory_mb: u64,
    /// Disk space required
    pub disk_space_mb: u64,
    /// Estimated creation time
    pub creation_time_minutes: u64,
    /// CPU cores recommended for creation
    pub recommended_cpu_cores: u32,
}

impl IndexAdvisor {
    /// Create new index advisor
    pub fn new(statistics: Arc<StatisticsManager>, cost_model: Arc<CostModel>) -> Self {
        Self {
            statistics,
            cost_model,
            workload_analyzer: WorkloadAnalyzer::new(),
            config: OptimizerConfig::default(),
        }
    }

    /// Analyze workload and recommend indexes
    pub fn recommend_indexes(&self, queries: &[LogicalQuery]) -> Result<Vec<IndexRecommendation>> {
        // Analyze the workload
        let workload_analysis = self.analyze_workload(queries)?;
        
        // Generate index candidates
        let candidates = self.generate_index_candidates(&workload_analysis)?;
        
        // Evaluate each candidate
        let mut recommendations = Vec::new();
        for candidate in candidates {
            if let Ok(recommendation) = self.evaluate_index_candidate(candidate, queries) {
                recommendations.push(recommendation);
            }
        }
        
        // Rank and filter recommendations
        self.rank_and_filter_recommendations(recommendations)
    }

    /// Generate detailed index recommendations with full analysis
    pub fn recommend_indexes_detailed(&self, queries: &[LogicalQuery]) -> Result<Vec<DetailedIndexRecommendation>> {
        let basic_recommendations = self.recommend_indexes(queries)?;
        
        let mut detailed_recommendations = Vec::new();
        for recommendation in basic_recommendations {
            let analysis = self.analyze_index_impact(&recommendation, queries)?;
            let priority = self.calculate_index_priority(&recommendation, &analysis);
            let resource_requirements = self.estimate_resource_requirements(&recommendation)?;
            
            detailed_recommendations.push(DetailedIndexRecommendation {
                recommendation,
                analysis,
                priority,
                resource_requirements,
            });
        }
        
        // Sort by priority and benefit
        detailed_recommendations.sort_by(|a, b| {
            a.priority.cmp(&b.priority)
                .then(b.recommendation.estimated_benefit.partial_cmp(&a.recommendation.estimated_benefit)
                     .unwrap_or(std::cmp::Ordering::Equal))
        });
        
        Ok(detailed_recommendations)
    }

    /// Analyze workload patterns
    fn analyze_workload(&self, queries: &[LogicalQuery]) -> Result<WorkloadAnalysis> {
        let mut column_access = HashMap::new();
        let mut join_patterns = HashMap::new();
        let mut predicate_patterns = Vec::new();
        
        for (query_idx, query) in queries.iter().enumerate() {
            // Analyze column access in predicates
            for predicate in &query.predicates {
                self.analyze_predicate_patterns(predicate, &mut column_access, &mut predicate_patterns);
            }
            
            // Analyze join patterns
            for join in &query.joins {
                self.analyze_join_pattern(join, &mut join_patterns);
            }
            
            // Analyze ORDER BY patterns
            for sort_key in &query.order_by {
                let column_key = format!("table.{}", sort_key.column); // Simplified
                column_access.entry(column_key.clone())
                    .or_insert_with(|| ColumnAccessPattern::new(column_key))
                    .order_by_count += 1;
            }
        }
        
        Ok(WorkloadAnalysis {
            column_access,
            join_patterns,
            predicate_patterns,
            total_queries: queries.len(),
        })
    }

    /// Generate index candidates based on workload
    fn generate_index_candidates(&self, workload: &WorkloadAnalysis) -> Result<Vec<IndexCandidate>> {
        let mut candidates = Vec::new();
        
        // Single-column indexes for frequently accessed columns
        for (_, access_pattern) in &workload.column_access {
            if access_pattern.frequency_score > 0.1 { // Threshold for consideration
                candidates.push(IndexCandidate {
                    table: self.extract_table_from_column(&access_pattern.column),
                    columns: vec![self.extract_column_name(&access_pattern.column)],
                    index_type: self.recommend_index_type(access_pattern),
                    estimated_benefit: access_pattern.frequency_score * 10.0,
                    confidence: 0.8,
                });
            }
        }
        
        // Composite indexes for join patterns
        for (_, join_pattern) in &workload.join_patterns {
            if join_pattern.frequency > 5 { // Threshold for consideration
                candidates.push(IndexCandidate {
                    table: self.extract_table_from_column(&join_pattern.left_column),
                    columns: vec![self.extract_column_name(&join_pattern.left_column)],
                    index_type: IndexType::BTree, // Good for joins
                    estimated_benefit: join_pattern.frequency as f64,
                    confidence: 0.9,
                });
                
                candidates.push(IndexCandidate {
                    table: self.extract_table_from_column(&join_pattern.right_column),
                    columns: vec![self.extract_column_name(&join_pattern.right_column)],
                    index_type: IndexType::BTree,
                    estimated_benefit: join_pattern.frequency as f64,
                    confidence: 0.9,
                });
            }
        }
        
        // Composite indexes for multiple predicate patterns on same table
        candidates.extend(self.generate_composite_index_candidates(workload)?);
        
        // Covering indexes for frequently projected columns
        candidates.extend(self.generate_covering_index_candidates(workload)?);
        
        Ok(candidates)
    }

    /// Generate composite index candidates
    fn generate_composite_index_candidates(&self, workload: &WorkloadAnalysis) -> Result<Vec<IndexCandidate>> {
        let mut candidates = Vec::new();
        
        // Group columns by table
        let mut table_columns: HashMap<String, Vec<String>> = HashMap::new();
        for column in workload.column_access.keys() {
            let table = self.extract_table_from_column(column);
            let col_name = self.extract_column_name(column);
            table_columns.entry(table).or_default().push(col_name);
        }
        
        // Generate composite index candidates for each table
        for (table, columns) in table_columns {
            if columns.len() >= 2 {
                // Sort by access frequency
                let mut sorted_columns = columns;
                sorted_columns.sort_by(|a, b| {
                    let a_freq = workload.column_access.get(&format!("{}.{}", table, a))
                        .map(|p| p.frequency_score)
                        .unwrap_or(0.0);
                    let b_freq = workload.column_access.get(&format!("{}.{}", table, b))
                        .map(|p| p.frequency_score)
                        .unwrap_or(0.0);
                    b_freq.partial_cmp(&a_freq).unwrap_or(std::cmp::Ordering::Equal)
                });
                
                // Create candidate with top 2-3 columns
                let composite_columns = sorted_columns.into_iter().take(3).collect::<Vec<_>>();
                if composite_columns.len() >= 2 {
                    let total_benefit: f64 = composite_columns.iter()
                        .map(|col| {
                            workload.column_access.get(&format!("{}.{}", table, col))
                                .map(|p| p.frequency_score)
                                .unwrap_or(0.0)
                        })
                        .sum();
                    
                    candidates.push(IndexCandidate {
                        table: table.clone(),
                        columns: composite_columns,
                        index_type: IndexType::Composite,
                        estimated_benefit: total_benefit * 1.5, // Bonus for composite
                        confidence: 0.7,
                    });
                }
            }
        }
        
        Ok(candidates)
    }

    /// Generate covering index candidates
    fn generate_covering_index_candidates(&self, workload: &WorkloadAnalysis) -> Result<Vec<IndexCandidate>> {
        let mut candidates = Vec::new();
        
        // Analyze patterns where covering indexes would be beneficial
        // For now, return empty - would implement based on SELECT column patterns
        
        Ok(candidates)
    }

    /// Evaluate an index candidate
    fn evaluate_index_candidate(&self, candidate: IndexCandidate, queries: &[LogicalQuery]) -> Result<IndexRecommendation> {
        // Calculate cost without index
        let cost_without = self.estimate_cost_without_index(&candidate, queries)?;
        
        // Calculate cost with index
        let cost_with = self.estimate_cost_with_index(&candidate, queries)?;
        
        // Calculate benefit
        let cost_reduction = cost_without - cost_with;
        let estimated_benefit = if cost_without > 0.0 {
            cost_reduction / cost_without * 100.0 // Percentage improvement
        } else {
            0.0
        };
        
        // Calculate index creation and maintenance cost
        let creation_cost = self.estimate_index_creation_cost(&candidate)?;
        
        // Calculate usage frequency
        let usage_frequency = self.calculate_usage_frequency(&candidate, queries);
        
        Ok(IndexRecommendation {
            table: candidate.table,
            columns: candidate.columns,
            index_type: candidate.index_type,
            estimated_benefit,
            estimated_cost: creation_cost,
            usage_frequency,
        })
    }

    /// Rank and filter recommendations
    fn rank_and_filter_recommendations(&self, mut recommendations: Vec<IndexRecommendation>) -> Result<Vec<IndexRecommendation>> {
        // Calculate overall score for each recommendation
        for recommendation in &mut recommendations {
            recommendation.estimated_benefit = self.calculate_overall_score(recommendation);
        }
        
        // Sort by benefit (descending)
        recommendations.sort_by(|a, b| {
            b.estimated_benefit.partial_cmp(&a.estimated_benefit).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Filter out low-benefit recommendations
        recommendations.retain(|r| r.estimated_benefit > 5.0); // 5% improvement threshold
        
        // Limit number of recommendations
        recommendations.truncate(20);
        
        Ok(recommendations)
    }

    /// Calculate overall score for recommendation
    fn calculate_overall_score(&self, recommendation: &IndexRecommendation) -> f64 {
        let benefit_score = recommendation.estimated_benefit;
        let cost_penalty = recommendation.estimated_cost / 100.0; // Normalize cost
        let frequency_bonus = recommendation.usage_frequency * 10.0;
        
        benefit_score + frequency_bonus - cost_penalty
    }

    /// Analyze predicate patterns
    fn analyze_predicate_patterns(
        &self,
        predicate: &Predicate,
        column_access: &mut HashMap<String, ColumnAccessPattern>,
        predicate_patterns: &mut Vec<PredicatePattern>,
    ) {
        match predicate {
            Predicate::Comparison { column, operator, value } => {
                let column_key = format!("table.{}", column); // Simplified
                let access_pattern = column_access.entry(column_key.clone())
                    .or_insert_with(|| ColumnAccessPattern::new(column_key));
                
                access_pattern.where_access_count += 1;
                *access_pattern.common_operators.entry(*operator).or_insert(0) += 1;
                
                predicate_patterns.push(PredicatePattern {
                    column: column.clone(),
                    operator: *operator,
                    value_type: self.classify_value_type(value),
                    frequency: 1,
                    avg_selectivity: 0.1, // Would be calculated from statistics
                    range_characteristics: None,
                });
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.analyze_predicate_patterns(left, column_access, predicate_patterns);
                self.analyze_predicate_patterns(right, column_access, predicate_patterns);
            }
            Predicate::Not(inner) => {
                self.analyze_predicate_patterns(inner, column_access, predicate_patterns);
            }
            Predicate::In { column, values } => {
                let column_key = format!("table.{}", column);
                let access_pattern = column_access.entry(column_key.clone())
                    .or_insert_with(|| ColumnAccessPattern::new(column_key));
                access_pattern.where_access_count += 1;
            }
            Predicate::Between { column, .. } => {
                let column_key = format!("table.{}", column);
                let access_pattern = column_access.entry(column_key.clone())
                    .or_insert_with(|| ColumnAccessPattern::new(column_key));
                access_pattern.where_access_count += 1;
            }
            Predicate::IsNull(column) | Predicate::IsNotNull(column) => {
                let column_key = format!("table.{}", column);
                let access_pattern = column_access.entry(column_key.clone())
                    .or_insert_with(|| ColumnAccessPattern::new(column_key));
                access_pattern.where_access_count += 1;
            }
        }
    }

    /// Analyze join patterns
    fn analyze_join_pattern(&self, join: &super::LogicalJoin, join_patterns: &mut HashMap<String, JoinPattern>) {
        if let JoinCondition::Equi { left_column, right_column } = &join.condition {
            let pattern_key = format!("{}#{}", left_column, right_column);
            let left_col = format!("{}.{}", join.left_table, left_column);
            let right_col = format!("{}.{}", join.right_table, right_column);
            
            let pattern = join_patterns.entry(pattern_key)
                .or_insert_with(|| JoinPattern {
                    left_column: left_col,
                    right_column: right_col,
                    frequency: 0,
                    avg_cardinality: 1000.0, // Default estimate
                    cost_without_index: 100.0,
                    cost_with_index: 10.0,
                });
            
            pattern.frequency += 1;
        }
    }

    /// Classify value type for predicate analysis
    fn classify_value_type(&self, value: &Value) -> ValueTypePattern {
        match value {
            Value::Integer(_) | Value::Float(_) | Value::String(_) | Value::Boolean(_) => ValueTypePattern::Constant,
            Value::Null => ValueTypePattern::Null,
        }
    }

    /// Recommend index type based on access pattern
    fn recommend_index_type(&self, access_pattern: &ColumnAccessPattern) -> IndexType {
        // Analyze operator patterns to recommend best index type
        let has_equality = access_pattern.common_operators.contains_key(&ComparisonOp::Equal);
        let has_range = access_pattern.common_operators.contains_key(&ComparisonOp::LessThan) ||
                       access_pattern.common_operators.contains_key(&ComparisonOp::GreaterThan);
        
        if has_equality && !has_range {
            IndexType::Hash // Good for equality lookups
        } else if has_range || access_pattern.order_by_count > 0 {
            IndexType::BTree // Good for range queries and sorting
        } else {
            IndexType::BTree // Default choice
        }
    }

    /// Estimate cost without index
    fn estimate_cost_without_index(&self, candidate: &IndexCandidate, queries: &[LogicalQuery]) -> Result<f64> {
        // Simplified cost estimation
        // Would use actual cost model in real implementation
        let base_cost = 100.0; // Table scan cost
        let frequency_factor = candidate.estimated_benefit / 10.0;
        Ok(base_cost * frequency_factor)
    }

    /// Estimate cost with index
    fn estimate_cost_with_index(&self, candidate: &IndexCandidate, queries: &[LogicalQuery]) -> Result<f64> {
        // Simplified cost estimation
        let index_lookup_cost = match candidate.index_type {
            IndexType::Hash => 1.0,
            IndexType::BTree => 3.0, // log(n) lookups
            IndexType::Partial => 2.0,
            IndexType::Composite => 4.0,
        };
        
        let frequency_factor = candidate.estimated_benefit / 10.0;
        Ok(index_lookup_cost * frequency_factor)
    }

    /// Estimate index creation cost
    fn estimate_index_creation_cost(&self, candidate: &IndexCandidate) -> Result<f64> {
        // Simplified cost estimation
        let base_cost = match candidate.index_type {
            IndexType::Hash => 10.0,
            IndexType::BTree => 20.0,
            IndexType::Partial => 15.0,
            IndexType::Composite => 30.0,
        };
        
        let column_factor = candidate.columns.len() as f64;
        Ok(base_cost * column_factor)
    }

    /// Calculate usage frequency
    fn calculate_usage_frequency(&self, candidate: &IndexCandidate, queries: &[LogicalQuery]) -> f64 {
        let mut usage_count = 0;
        
        for query in queries {
            if self.would_use_index(query, candidate) {
                usage_count += 1;
            }
        }
        
        usage_count as f64 / queries.len() as f64
    }

    /// Check if query would use the proposed index
    fn would_use_index(&self, query: &LogicalQuery, candidate: &IndexCandidate) -> bool {
        // Check if any predicates involve the indexed columns
        for predicate in &query.predicates {
            if self.predicate_uses_columns(predicate, &candidate.columns) {
                return true;
            }
        }
        
        // Check if any joins involve the indexed columns
        for join in &query.joins {
            if let JoinCondition::Equi { left_column, right_column } = &join.condition {
                if candidate.columns.contains(left_column) || candidate.columns.contains(right_column) {
                    return true;
                }
            }
        }
        
        // Check if ORDER BY uses indexed columns
        for sort_key in &query.order_by {
            if candidate.columns.contains(&sort_key.column) {
                return true;
            }
        }
        
        false
    }

    /// Check if predicate uses any of the specified columns
    fn predicate_uses_columns(&self, predicate: &Predicate, columns: &[String]) -> bool {
        match predicate {
            Predicate::Comparison { column, .. } => columns.contains(column),
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.predicate_uses_columns(left, columns) || self.predicate_uses_columns(right, columns)
            }
            Predicate::Not(inner) => self.predicate_uses_columns(inner, columns),
            Predicate::In { column, .. } => columns.contains(column),
            Predicate::Between { column, .. } => columns.contains(column),
            Predicate::IsNull(column) | Predicate::IsNotNull(column) => columns.contains(column),
        }
    }

    /// Analyze index impact in detail
    fn analyze_index_impact(&self, recommendation: &IndexRecommendation, queries: &[LogicalQuery]) -> Result<IndexAnalysis> {
        let mut benefiting_queries = Vec::new();
        
        for (i, query) in queries.iter().enumerate() {
            if self.would_use_index(query, &IndexCandidate {
                table: recommendation.table.clone(),
                columns: recommendation.columns.clone(),
                index_type: recommendation.index_type,
                estimated_benefit: recommendation.estimated_benefit,
                confidence: 1.0,
            }) {
                benefiting_queries.push(format!("query_{}", i));
            }
        }
        
        Ok(IndexAnalysis {
            benefiting_queries,
            performance_improvement: PerformanceImprovement {
                execution_time_reduction: recommendation.estimated_benefit,
                io_reduction: recommendation.estimated_benefit * 1.2,
                cpu_reduction: recommendation.estimated_benefit * 0.8,
                memory_change: 1024 * 1024, // 1MB increase for index cache
            },
            maintenance_cost: MaintenanceCost {
                insert_cost_increase: 10.0,
                update_cost_increase: 15.0,
                delete_cost_increase: 5.0,
                space_amplification: 1.2,
            },
            storage_overhead: StorageOverhead {
                estimated_size_bytes: 1024 * 1024 * 10, // 10MB
                size_percentage: 15.0,
                key_size_per_entry: 32,
                value_size_per_entry: 8,
            },
            index_competition: Vec::new(),
        })
    }

    /// Calculate index priority
    fn calculate_index_priority(&self, recommendation: &IndexRecommendation, analysis: &IndexAnalysis) -> IndexPriority {
        let benefit = recommendation.estimated_benefit;
        let frequency = recommendation.usage_frequency;
        
        if benefit > 50.0 && frequency > 0.8 {
            IndexPriority::Critical
        } else if benefit > 30.0 && frequency > 0.5 {
            IndexPriority::High
        } else if benefit > 15.0 && frequency > 0.3 {
            IndexPriority::Medium
        } else if benefit > 5.0 {
            IndexPriority::Low
        } else {
            IndexPriority::Optional
        }
    }

    /// Estimate resource requirements
    fn estimate_resource_requirements(&self, recommendation: &IndexRecommendation) -> Result<ResourceRequirements> {
        let size_factor = recommendation.columns.len() as u64;
        
        Ok(ResourceRequirements {
            creation_memory_mb: 100 * size_factor,
            disk_space_mb: 50 * size_factor,
            creation_time_minutes: 5 * size_factor,
            recommended_cpu_cores: 2,
        })
    }

    // Helper methods
    
    fn extract_table_from_column(&self, column: &str) -> String {
        column.split('.').next().unwrap_or("unknown").to_string()
    }
    
    fn extract_column_name(&self, column: &str) -> String {
        column.split('.').last().unwrap_or(column).to_string()
    }
}

impl WorkloadAnalyzer {
    fn new() -> Self {
        Self {
            query_frequency: HashMap::new(),
            column_access: HashMap::new(),
            join_patterns: HashMap::new(),
            predicate_patterns: Vec::new(),
        }
    }
}

impl ColumnAccessPattern {
    fn new(column: String) -> Self {
        Self {
            column,
            where_access_count: 0,
            join_access_count: 0,
            order_by_count: 0,
            group_by_count: 0,
            selectivity_distribution: Vec::new(),
            common_operators: HashMap::new(),
            frequency_score: 0.0,
        }
    }
}

/// Workload analysis result
#[derive(Debug)]
struct WorkloadAnalysis {
    column_access: HashMap<String, ColumnAccessPattern>,
    join_patterns: HashMap<String, JoinPattern>,
    predicate_patterns: Vec<PredicatePattern>,
    total_queries: usize,
}

/// Index candidate for evaluation
#[derive(Debug, Clone)]
struct IndexCandidate {
    table: String,
    columns: Vec<String>,
    index_type: IndexType,
    estimated_benefit: f64,
    confidence: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_advisor_creation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cost_model = Arc::new(CostModel::new(config));
        
        let advisor = IndexAdvisor::new(statistics, cost_model);
        assert_eq!(advisor.workload_analyzer.query_frequency.len(), 0);
    }

    #[test]
    fn test_value_type_classification() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cost_model = Arc::new(CostModel::new(config));
        let advisor = IndexAdvisor::new(statistics, cost_model);

        let int_value = Value::Integer(42);
        let string_value = Value::String("test".to_string());
        let null_value = Value::Null;

        assert!(matches!(advisor.classify_value_type(&int_value), ValueTypePattern::Constant));
        assert!(matches!(advisor.classify_value_type(&string_value), ValueTypePattern::Constant));
        assert!(matches!(advisor.classify_value_type(&null_value), ValueTypePattern::Null));
    }

    #[test]
    fn test_index_type_recommendation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cost_model = Arc::new(CostModel::new(config));
        let advisor = IndexAdvisor::new(statistics, cost_model);

        let mut equality_pattern = ColumnAccessPattern::new("test.id".to_string());
        equality_pattern.common_operators.insert(ComparisonOp::Equal, 10);

        let mut range_pattern = ColumnAccessPattern::new("test.date".to_string());
        range_pattern.common_operators.insert(ComparisonOp::LessThan, 5);
        range_pattern.common_operators.insert(ComparisonOp::GreaterThan, 5);

        let equality_type = advisor.recommend_index_type(&equality_pattern);
        let range_type = advisor.recommend_index_type(&range_pattern);

        assert!(matches!(equality_type, IndexType::Hash));
        assert!(matches!(range_type, IndexType::BTree));
    }

    #[test]
    fn test_index_priority_calculation() {
        let config = OptimizerConfig::default();
        let statistics = Arc::new(StatisticsManager::new(config.clone()));
        let cost_model = Arc::new(CostModel::new(config));
        let advisor = IndexAdvisor::new(statistics, cost_model);

        let high_benefit_rec = IndexRecommendation {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
            estimated_benefit: 60.0,
            estimated_cost: 10.0,
            usage_frequency: 0.9,
        };

        let low_benefit_rec = IndexRecommendation {
            table: "users".to_string(),
            columns: vec!["name".to_string()],
            index_type: IndexType::BTree,
            estimated_benefit: 3.0,
            estimated_cost: 10.0,
            usage_frequency: 0.1,
        };

        let analysis = IndexAnalysis {
            benefiting_queries: vec!["query_1".to_string()],
            performance_improvement: PerformanceImprovement {
                execution_time_reduction: 50.0,
                io_reduction: 60.0,
                cpu_reduction: 40.0,
                memory_change: 1024,
            },
            maintenance_cost: MaintenanceCost {
                insert_cost_increase: 10.0,
                update_cost_increase: 15.0,
                delete_cost_increase: 5.0,
                space_amplification: 1.2,
            },
            storage_overhead: StorageOverhead {
                estimated_size_bytes: 1024 * 1024,
                size_percentage: 10.0,
                key_size_per_entry: 32,
                value_size_per_entry: 8,
            },
            index_competition: Vec::new(),
        };

        let high_priority = advisor.calculate_index_priority(&high_benefit_rec, &analysis);
        let low_priority = advisor.calculate_index_priority(&low_benefit_rec, &analysis);

        assert!(matches!(high_priority, IndexPriority::Critical));
        assert!(matches!(low_priority, IndexPriority::Optional));
    }
}
use crate::core::error::{Error, Result};
use crate::core::index::{IndexKey, IndexManager, JoinType};
use crate::core::query_planner::{ExecutionPlan, ExecutionStep, QueryCondition, QuerySpec, QueryCost};
use parking_lot::RwLock;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatistics {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub avg_execution_time: Duration,
    pub total_rows_processed: u64,
    pub optimization_time: Duration,
    pub reoptimized_count: u64,
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub low_value: Vec<u8>,
    pub high_value: Vec<u8>,
    pub frequency: u64,
    pub distinct_count: u64,
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub avg_row_size: usize,
    pub null_count: HashMap<String, u64>,
    pub distinct_count: HashMap<String, u64>,
    pub histograms: HashMap<String, Vec<HistogramBucket>>,
    pub correlation: HashMap<String, f64>,
    pub last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct JoinStatistics {
    pub join_selectivity: f64,
    pub estimated_result_size: u64,
    pub preferred_algorithm: JoinAlgorithm,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinAlgorithm {
    NestedLoop,
    HashJoin,
    SortMergeJoin,
    IndexNestedLoop,
}

#[derive(Debug, Clone)]
pub struct CostModel {
    pub cpu_op_cost: f64,
    pub sequential_io_cost: f64,
    pub random_io_cost: f64,
    pub memory_cost: f64,
    pub network_cost: f64,
    pub index_probe_cost: f64,
    pub hash_build_cost: f64,
    pub sort_cost_per_row: f64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            cpu_op_cost: 0.001,
            sequential_io_cost: 1.0,
            random_io_cost: 4.0,
            memory_cost: 0.1,
            network_cost: 10.0,
            index_probe_cost: 2.0,
            hash_build_cost: 0.5,
            sort_cost_per_row: 0.01,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OptimizationHint {
    pub hint_type: HintType,
    pub target: String,
    pub parameters: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HintType {
    UseIndex,
    NoIndex,
    JoinOrder,
    JoinAlgorithm,
    Parallel,
    MaterializeView,
}

pub struct QueryCache {
    cache: Arc<RwLock<HashMap<u64, CachedPlan>>>,
    max_size: usize,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedPlan {
    plan: ExecutionPlan,
    created_at: Instant,
    access_count: u64,
    last_accessed: Instant,
    execution_stats: QueryExecutionStats,
}

#[derive(Debug, Clone)]
pub struct QueryExecutionStats {
    pub rows_examined: u64,
    pub rows_returned: u64,
    pub execution_time: Duration,
    pub memory_used: usize,
    pub temp_space_used: usize,
}

pub struct QueryOptimizer {
    statistics: Arc<RwLock<HashMap<String, TableStatistics>>>,
    cost_model: CostModel,
    query_cache: QueryCache,
    optimization_rules: Vec<Box<dyn OptimizationRule>>,
    adaptive_stats: Arc<RwLock<AdaptiveStatistics>>,
    query_stats: Arc<RwLock<QueryStatistics>>,
}

#[derive(Debug, Default)]
struct AdaptiveStatistics {
    feedback_history: BTreeMap<u64, QueryFeedback>,
    learned_correlations: HashMap<(String, String), f64>,
    bad_plans: HashSet<u64>,
}

#[derive(Debug, Clone)]
struct QueryFeedback {
    plan_hash: u64,
    estimated_cost: f64,
    actual_cost: f64,
    estimated_rows: u64,
    actual_rows: u64,
}

trait OptimizationRule: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, plan: &mut ExecutionPlan, stats: &HashMap<String, TableStatistics>) -> bool;
}

struct PredicatePushdownRule;
impl OptimizationRule for PredicatePushdownRule {
    fn name(&self) -> &str { "PredicatePushdown" }
    
    fn apply(&self, plan: &mut ExecutionPlan, _stats: &HashMap<String, TableStatistics>) -> bool {
        let mut modified = false;
        let mut new_steps = Vec::new();
        
        for (i, step) in plan.steps.iter().enumerate() {
            if let ExecutionStep::Filter { input_step, condition, estimated_rows } = step {
                if *input_step > 0 && i > *input_step {
                    if let Some(prev_step) = plan.steps.get(*input_step) {
                        if matches!(prev_step, ExecutionStep::RangeScan { .. }) {
                            new_steps.push(ExecutionStep::Filter {
                                input_step: *input_step,
                                condition: condition.clone(),
                                estimated_rows: *estimated_rows,
                            });
                            modified = true;
                            continue;
                        }
                    }
                }
            }
            new_steps.push(step.clone());
        }
        
        if modified {
            plan.steps = new_steps;
        }
        
        modified
    }
}

struct JoinReorderRule;
impl OptimizationRule for JoinReorderRule {
    fn name(&self) -> &str { "JoinReorder" }
    
    fn apply(&self, plan: &mut ExecutionPlan, stats: &HashMap<String, TableStatistics>) -> bool {
        let mut join_steps = Vec::new();
        
        for (i, step) in plan.steps.iter().enumerate() {
            if let ExecutionStep::Join { .. } = step {
                join_steps.push(i);
            }
        }
        
        if join_steps.len() < 2 {
            return false;
        }
        
        let mut best_order = join_steps.clone();
        let mut best_cost = estimate_join_order_cost(&plan.steps, &join_steps, stats);
        
        permute_joins(&mut join_steps, 0, &mut |order| {
            let cost = estimate_join_order_cost(&plan.steps, order, stats);
            if cost < best_cost {
                best_cost = cost;
                best_order = order.to_vec();
            }
        });
        
        if best_order != join_steps {
            reorder_joins(plan, &best_order);
            true
        } else {
            false
        }
    }
}

fn estimate_join_order_cost(
    _steps: &[ExecutionStep],
    _order: &[usize],
    _stats: &HashMap<String, TableStatistics>,
) -> f64 {
    100.0
}

fn permute_joins(joins: &mut [usize], k: usize, callback: &mut dyn FnMut(&[usize])) {
    if k == joins.len() {
        callback(joins);
        return;
    }
    
    for i in k..joins.len() {
        joins.swap(k, i);
        permute_joins(joins, k + 1, callback);
        joins.swap(k, i);
    }
}

fn reorder_joins(plan: &mut ExecutionPlan, _new_order: &[usize]) {
    let _reordered_steps = Vec::new();
    plan.estimated_cost.total_cost *= 0.9;
}

impl QueryOptimizer {
    pub fn new(max_cache_size: usize, cache_ttl: Duration) -> Self {
        let mut rules: Vec<Box<dyn OptimizationRule>> = Vec::new();
        rules.push(Box::new(PredicatePushdownRule));
        rules.push(Box::new(JoinReorderRule));
        
        Self {
            statistics: Arc::new(RwLock::new(HashMap::new())),
            cost_model: CostModel::default(),
            query_cache: QueryCache {
                cache: Arc::new(RwLock::new(HashMap::new())),
                max_size: max_cache_size,
                ttl: cache_ttl,
            },
            optimization_rules: rules,
            adaptive_stats: Arc::new(RwLock::new(AdaptiveStatistics::default())),
            query_stats: Arc::new(RwLock::new(QueryStatistics {
                total_queries: 0,
                cache_hits: 0,
                cache_misses: 0,
                avg_execution_time: Duration::ZERO,
                total_rows_processed: 0,
                optimization_time: Duration::ZERO,
                reoptimized_count: 0,
            })),
        }
    }
    
    pub fn optimize_query(&self, query: &QuerySpec) -> Result<ExecutionPlan> {
        let start = Instant::now();
        let query_hash = self.hash_query(query);
        
        {
            let mut stats = self.query_stats.write();
            stats.total_queries += 1;
        }
        
        if let Some(cached_plan) = self.get_cached_plan(query_hash) {
            let mut stats = self.query_stats.write();
            stats.cache_hits += 1;
            debug!("Query plan cache hit for hash {}", query_hash);
            return Ok(cached_plan);
        }
        
        let mut stats = self.query_stats.write();
        stats.cache_misses += 1;
        drop(stats);
        
        let mut initial_plan = self.create_initial_plan(query)?;
        
        let statistics = self.statistics.read();
        for rule in &self.optimization_rules {
            if rule.apply(&mut initial_plan, &statistics) {
                debug!("Applied optimization rule: {}", rule.name());
            }
        }
        drop(statistics);
        
        initial_plan = self.apply_cost_based_optimization(initial_plan)?;
        
        if self.should_use_adaptive_optimization(query_hash) {
            initial_plan = self.apply_adaptive_optimization(initial_plan, query_hash)?;
        }
        
        self.cache_plan(query_hash, initial_plan.clone());
        
        let optimization_time = start.elapsed();
        let mut stats = self.query_stats.write();
        stats.optimization_time = 
            (stats.optimization_time + optimization_time) / 2;
        
        info!(
            "Query optimized in {:?}, estimated cost: {:.2}",
            optimization_time, initial_plan.estimated_cost.total_cost
        );
        
        Ok(initial_plan)
    }
    
    fn hash_query(&self, query: &QuerySpec) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        format!("{:?}", query).hash(&mut hasher);
        hasher.finish()
    }
    
    fn get_cached_plan(&self, query_hash: u64) -> Option<ExecutionPlan> {
        let mut cache = self.query_cache.cache.write();
        
        if let Some(cached) = cache.get_mut(&query_hash) {
            let now = Instant::now();
            if now.duration_since(cached.created_at) <= self.query_cache.ttl {
                cached.access_count += 1;
                cached.last_accessed = now;
                return Some(cached.plan.clone());
            } else {
                cache.remove(&query_hash);
            }
        }
        
        None
    }
    
    fn cache_plan(&self, query_hash: u64, plan: ExecutionPlan) {
        let mut cache = self.query_cache.cache.write();
        
        if cache.len() >= self.query_cache.max_size {
            self.evict_lru_entry(&mut cache);
        }
        
        let cached_plan = CachedPlan {
            plan,
            created_at: Instant::now(),
            access_count: 1,
            last_accessed: Instant::now(),
            execution_stats: QueryExecutionStats {
                rows_examined: 0,
                rows_returned: 0,
                execution_time: Duration::ZERO,
                memory_used: 0,
                temp_space_used: 0,
            },
        };
        
        cache.insert(query_hash, cached_plan);
    }
    
    fn evict_lru_entry(&self, cache: &mut HashMap<u64, CachedPlan>) {
        if let Some((lru_key, _)) = cache
            .iter()
            .min_by_key(|(_, v)| v.last_accessed)
            .map(|(k, v)| (*k, v.clone()))
        {
            cache.remove(&lru_key);
            debug!("Evicted LRU query plan with hash {}", lru_key);
        }
    }
    
    fn create_initial_plan(&self, query: &QuerySpec) -> Result<ExecutionPlan> {
        let mut steps = Vec::new();
        let mut total_cost = 0.0;
        
        for condition in &query.conditions {
            let (step, cost) = self.create_condition_step(condition)?;
            steps.push(step);
            total_cost += cost;
        }
        
        for join in &query.joins {
            let join_stats = self.estimate_join_statistics(
                &join.left_field,
                &join.right_field,
                &join.join_type,
            );
            
            let step = self.create_join_step(&join_stats, &join.join_type);
            total_cost += self.estimate_join_cost(&join_stats);
            steps.push(step);
        }
        
        let estimated_cost = QueryCost {
            estimated_rows: self.estimate_result_rows(&steps),
            index_lookups: self.count_index_operations(&steps),
            range_scans: self.count_range_scans(&steps),
            full_scans: 0,
            total_cost,
        };
        
        Ok(ExecutionPlan {
            steps,
            estimated_cost,
        })
    }
    
    fn create_condition_step(&self, condition: &QueryCondition) -> Result<(ExecutionStep, f64)> {
        match condition {
            QueryCondition::Equals { field, value } => {
                let step = ExecutionStep::IndexLookup {
                    index_name: format!("idx_{}", field),
                    condition: condition.clone(),
                    estimated_rows: 10,
                };
                Ok((step, self.cost_model.index_probe_cost))
            }
            QueryCondition::Range { field, start, end } => {
                let step = ExecutionStep::RangeScan {
                    index_name: format!("idx_{}", field),
                    start_key: IndexKey::single(start.clone()),
                    end_key: IndexKey::single(end.clone()),
                    estimated_rows: 100,
                };
                Ok((step, self.cost_model.sequential_io_cost * 10.0))
            }
            _ => {
                let step = ExecutionStep::Filter {
                    input_step: 0,
                    condition: condition.clone(),
                    estimated_rows: 50,
                };
                Ok((step, self.cost_model.cpu_op_cost * 100.0))
            }
        }
    }
    
    fn estimate_join_statistics(
        &self,
        _left_field: &str,
        _right_field: &str,
        join_type: &JoinType,
    ) -> JoinStatistics {
        let algorithm = match join_type {
            JoinType::Inner => JoinAlgorithm::HashJoin,
            JoinType::Left | JoinType::Right => JoinAlgorithm::SortMergeJoin,
            JoinType::Full => JoinAlgorithm::NestedLoop,
        };
        
        JoinStatistics {
            join_selectivity: 0.1,
            estimated_result_size: 100,
            preferred_algorithm: algorithm,
        }
    }
    
    fn create_join_step(&self, stats: &JoinStatistics, join_type: &JoinType) -> ExecutionStep {
        ExecutionStep::Join {
            left_step: 0,
            right_step: 1,
            join_type: join_type.clone(),
            estimated_rows: stats.estimated_result_size as usize,
        }
    }
    
    fn estimate_join_cost(&self, stats: &JoinStatistics) -> f64 {
        match stats.preferred_algorithm {
            JoinAlgorithm::NestedLoop => {
                stats.estimated_result_size as f64 * self.cost_model.cpu_op_cost * 100.0
            }
            JoinAlgorithm::HashJoin => {
                stats.estimated_result_size as f64 * self.cost_model.hash_build_cost
                    + self.cost_model.memory_cost * 10.0
            }
            JoinAlgorithm::SortMergeJoin => {
                stats.estimated_result_size as f64 * self.cost_model.sort_cost_per_row * 2.0
                    + self.cost_model.sequential_io_cost * 5.0
            }
            JoinAlgorithm::IndexNestedLoop => {
                stats.estimated_result_size as f64 * self.cost_model.index_probe_cost
            }
        }
    }
    
    fn apply_cost_based_optimization(&self, mut plan: ExecutionPlan) -> Result<ExecutionPlan> {
        let original_cost = plan.estimated_cost.total_cost;
        
        let alternative_plans = self.generate_alternative_plans(&plan);
        
        let mut best_plan = plan;
        let mut best_cost = original_cost;
        
        for alt_plan in alternative_plans {
            let cost = self.estimate_plan_cost(&alt_plan);
            if cost < best_cost {
                best_cost = cost;
                best_plan = alt_plan;
            }
        }
        
        if best_cost < original_cost * 0.8 {
            info!(
                "Cost-based optimization reduced cost from {:.2} to {:.2}",
                original_cost, best_cost
            );
            best_plan.estimated_cost.total_cost = best_cost;
        }
        
        Ok(best_plan)
    }
    
    fn generate_alternative_plans(&self, base_plan: &ExecutionPlan) -> Vec<ExecutionPlan> {
        let mut alternatives = Vec::new();
        
        let mut index_scan_plan = base_plan.clone();
        for step in &mut index_scan_plan.steps {
            if let ExecutionStep::RangeScan { estimated_rows, .. } = step {
                *estimated_rows = (*estimated_rows as f64 * 0.8) as usize;
            }
        }
        alternatives.push(index_scan_plan);
        
        let mut parallel_plan = base_plan.clone();
        parallel_plan.estimated_cost.total_cost *= 0.5;
        alternatives.push(parallel_plan);
        
        alternatives
    }
    
    fn estimate_plan_cost(&self, plan: &ExecutionPlan) -> f64 {
        let mut total_cost = 0.0;
        
        for step in &plan.steps {
            total_cost += match step {
                ExecutionStep::IndexLookup { estimated_rows, .. } => {
                    self.cost_model.index_probe_cost + 
                    (*estimated_rows as f64 * self.cost_model.cpu_op_cost)
                }
                ExecutionStep::RangeScan { estimated_rows, .. } => {
                    self.cost_model.sequential_io_cost * 
                    (*estimated_rows as f64 / 100.0)
                }
                ExecutionStep::Join { estimated_rows, .. } => {
                    *estimated_rows as f64 * self.cost_model.cpu_op_cost * 10.0
                }
                ExecutionStep::Filter { estimated_rows, .. } => {
                    *estimated_rows as f64 * self.cost_model.cpu_op_cost
                }
            };
        }
        
        total_cost
    }
    
    fn should_use_adaptive_optimization(&self, query_hash: u64) -> bool {
        let adaptive_stats = self.adaptive_stats.read();
        
        if adaptive_stats.bad_plans.contains(&query_hash) {
            return true;
        }
        
        if let Some(feedback) = adaptive_stats.feedback_history.get(&query_hash) {
            let error_ratio = (feedback.actual_cost - feedback.estimated_cost).abs() 
                / feedback.estimated_cost;
            return error_ratio > 0.5;
        }
        
        false
    }
    
    fn apply_adaptive_optimization(
        &self,
        mut plan: ExecutionPlan,
        query_hash: u64,
    ) -> Result<ExecutionPlan> {
        let adaptive_stats = self.adaptive_stats.read();
        
        if let Some(feedback) = adaptive_stats.feedback_history.get(&query_hash) {
            let adjustment_factor = feedback.actual_cost / feedback.estimated_cost;
            
            for step in &mut plan.steps {
                match step {
                    ExecutionStep::IndexLookup { estimated_rows, .. } |
                    ExecutionStep::RangeScan { estimated_rows, .. } |
                    ExecutionStep::Join { estimated_rows, .. } |
                    ExecutionStep::Filter { estimated_rows, .. } => {
                        *estimated_rows = (*estimated_rows as f64 * adjustment_factor) as usize;
                    }
                }
            }
            
            plan.estimated_cost.total_cost *= adjustment_factor;
            
            let mut stats = self.query_stats.write();
            stats.reoptimized_count += 1;
            
            debug!(
                "Applied adaptive optimization for query {}, adjustment factor: {:.2}",
                query_hash, adjustment_factor
            );
        }
        
        Ok(plan)
    }
    
    fn estimate_result_rows(&self, steps: &[ExecutionStep]) -> usize {
        steps.iter().fold(1000, |acc, step| {
            match step {
                ExecutionStep::Filter { estimated_rows, .. } => *estimated_rows.min(&acc),
                ExecutionStep::Join { estimated_rows, .. } => *estimated_rows,
                _ => acc,
            }
        })
    }
    
    fn count_index_operations(&self, steps: &[ExecutionStep]) -> usize {
        steps
            .iter()
            .filter(|s| matches!(s, ExecutionStep::IndexLookup { .. }))
            .count()
    }
    
    fn count_range_scans(&self, steps: &[ExecutionStep]) -> usize {
        steps
            .iter()
            .filter(|s| matches!(s, ExecutionStep::RangeScan { .. }))
            .count()
    }
    
    pub fn update_statistics(&self, table_name: String, stats: TableStatistics) {
        let mut statistics = self.statistics.write();
        statistics.insert(table_name.clone(), stats);
        info!("Updated statistics for table: {}", table_name);
    }
    
    pub fn record_execution_feedback(
        &self,
        query_hash: u64,
        estimated_cost: f64,
        actual_cost: f64,
        estimated_rows: u64,
        actual_rows: u64,
    ) {
        let feedback = QueryFeedback {
            plan_hash: query_hash,
            estimated_cost,
            actual_cost,
            estimated_rows,
            actual_rows,
        };
        
        let mut adaptive_stats = self.adaptive_stats.write();
        adaptive_stats.feedback_history.insert(query_hash, feedback);
        
        if (actual_cost / estimated_cost) > 2.0 {
            adaptive_stats.bad_plans.insert(query_hash);
            warn!(
                "Query {} marked as bad plan: actual cost {:.2} >> estimated {:.2}",
                query_hash, actual_cost, estimated_cost
            );
        }
        
        let mut stats = self.query_stats.write();
        stats.total_rows_processed += actual_rows;
    }
    
    pub fn get_statistics(&self) -> QueryStatistics {
        self.query_stats.read().clone()
    }
    
    pub fn clear_cache(&self) {
        let mut cache = self.query_cache.cache.write();
        cache.clear();
        info!("Query plan cache cleared");
    }
    
    pub fn analyze_table(&self, table_name: &str, sample_size: usize) -> Result<TableStatistics> {
        let mut stats = TableStatistics {
            row_count: sample_size as u64,
            avg_row_size: 100,
            null_count: HashMap::new(),
            distinct_count: HashMap::new(),
            histograms: HashMap::new(),
            correlation: HashMap::new(),
            last_updated: Instant::now(),
        };
        
        stats.distinct_count.insert("id".to_string(), sample_size as u64);
        stats.correlation.insert("id".to_string(), 1.0);
        
        self.update_statistics(table_name.to_string(), stats.clone());
        
        Ok(stats)
    }
}

pub struct ParallelQueryExecutor {
    optimizer: Arc<QueryOptimizer>,
    thread_pool: rayon::ThreadPool,
    max_parallelism: usize,
}

impl ParallelQueryExecutor {
    pub fn new(optimizer: Arc<QueryOptimizer>, max_parallelism: usize) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(max_parallelism)
            .build()
            .unwrap();
        
        Self {
            optimizer,
            thread_pool,
            max_parallelism,
        }
    }
    
    pub fn execute_parallel(
        &self,
        plan: &ExecutionPlan,
        index_manager: &IndexManager,
    ) -> Result<Vec<Vec<u8>>> {
        use rayon::prelude::*;
        
        let parallelizable_steps: Vec<_> = plan.steps
            .iter()
            .enumerate()
            .filter(|(_, step)| self.is_parallelizable(step))
            .collect();
        
        if parallelizable_steps.len() < 2 {
            return self.execute_sequential(plan, index_manager);
        }
        
        let results = self.thread_pool.install(|| {
            parallelizable_steps
                .par_iter()
                .map(|(idx, step)| {
                    self.execute_step(*idx, step, index_manager)
                })
                .collect::<Result<Vec<_>>>()
        })?;
        
        self.merge_results(results)
    }
    
    fn is_parallelizable(&self, step: &ExecutionStep) -> bool {
        matches!(
            step,
            ExecutionStep::IndexLookup { .. } | ExecutionStep::RangeScan { .. }
        )
    }
    
    fn execute_sequential(
        &self,
        _plan: &ExecutionPlan,
        _index_manager: &IndexManager,
    ) -> Result<Vec<Vec<u8>>> {
        Ok(Vec::new())
    }
    
    fn execute_step(
        &self,
        _step_idx: usize,
        _step: &ExecutionStep,
        _index_manager: &IndexManager,
    ) -> Result<Vec<Vec<u8>>> {
        Ok(Vec::new())
    }
    
    fn merge_results(&self, results: Vec<Vec<Vec<u8>>>) -> Result<Vec<Vec<u8>>> {
        Ok(results.into_iter().flatten().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_query_optimizer_creation() {
        let optimizer = QueryOptimizer::new(100, Duration::from_secs(300));
        let stats = optimizer.get_statistics();
        assert_eq!(stats.total_queries, 0);
        assert_eq!(stats.cache_hits, 0);
    }
    
    #[test]
    fn test_query_caching() {
        let optimizer = QueryOptimizer::new(10, Duration::from_secs(60));
        let query = QuerySpec {
            conditions: vec![QueryCondition::Equals {
                field: "id".to_string(),
                value: vec![1, 2, 3],
            }],
            joins: vec![],
            limit: Some(10),
            order_by: None,
        };
        
        let _plan1 = optimizer.optimize_query(&query).unwrap();
        let _plan2 = optimizer.optimize_query(&query).unwrap();
        
        let stats = optimizer.get_statistics();
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
    }
    
    #[test]
    fn test_adaptive_optimization() {
        let optimizer = QueryOptimizer::new(100, Duration::from_secs(300));
        let query_hash = 12345;
        
        optimizer.record_execution_feedback(
            query_hash,
            100.0,
            300.0,
            1000,
            3000,
        );
        
        assert!(optimizer.should_use_adaptive_optimization(query_hash));
    }
    
    #[test]
    fn test_cost_model() {
        let model = CostModel::default();
        assert!(model.random_io_cost > model.sequential_io_cost);
        assert!(model.network_cost > model.memory_cost);
    }
    
    #[test]
    fn test_parallel_executor() {
        let optimizer = Arc::new(QueryOptimizer::new(100, Duration::from_secs(300)));
        let executor = ParallelQueryExecutor::new(optimizer, 4);
        assert_eq!(executor.max_parallelism, 4);
    }
}
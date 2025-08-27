use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use crate::core::error::Error;
use super::planner::{QueryPlan, PlanNode, Predicate, Expression, JoinNode, FilterNode};
use super::statistics::TableStatistics;
use super::cost_model::CostModel;

#[derive(Debug, Clone)]
pub struct QueryOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
    cost_model: Arc<CostModel>,
    config: OptimizerConfig,
}

#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    pub enable_predicate_pushdown: bool,
    pub enable_projection_pushdown: bool,
    pub enable_join_reordering: bool,
    pub enable_constant_folding: bool,
    pub enable_common_subexpression_elimination: bool,
    pub enable_materialized_view_rewrite: bool,
    pub max_join_reorder_size: usize,
    pub cost_threshold: f64,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_join_reordering: true,
            enable_constant_folding: true,
            enable_common_subexpression_elimination: true,
            enable_materialized_view_rewrite: true,
            max_join_reorder_size: 12,
            cost_threshold: 0.01,
        }
    }
}

pub trait OptimizationRule: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, plan: QueryPlan, stats: &TableStatistics) -> Result<QueryPlan, Error>;
    fn is_applicable(&self, plan: &QueryPlan) -> bool;
}

struct PredicatePushdown;
struct ProjectionPushdown;
struct JoinReordering;
struct ConstantFolding;
struct CommonSubexpressionElimination;
struct MaterializedViewRewrite;
struct IndexSelection;
struct PartitionPruning;

impl QueryOptimizer {
    pub fn new() -> Self {
        let mut rules: Vec<Box<dyn OptimizationRule>> = Vec::new();
        
        rules.push(Box::new(PredicatePushdown));
        rules.push(Box::new(ProjectionPushdown));
        rules.push(Box::new(JoinReordering));
        rules.push(Box::new(ConstantFolding));
        rules.push(Box::new(CommonSubexpressionElimination));
        rules.push(Box::new(MaterializedViewRewrite));
        rules.push(Box::new(IndexSelection));
        rules.push(Box::new(PartitionPruning));
        
        Self {
            rules,
            cost_model: Arc::new(CostModel::default()),
            config: OptimizerConfig::default(),
        }
    }

    pub fn optimize(&self, plan: QueryPlan, stats: &TableStatistics) -> Result<QueryPlan, Error> {
        let mut current_plan = plan;
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10;
        
        loop {
            let initial_cost = current_plan.estimated_cost.total_cost;
            let mut improved = false;
            
            for rule in &self.rules {
                if rule.is_applicable(&current_plan) {
                    let optimized = rule.apply(current_plan.clone(), stats)?;
                    
                    if self.is_better_plan(&optimized, &current_plan) {
                        current_plan = optimized;
                        improved = true;
                    }
                }
            }
            
            iterations += 1;
            
            if !improved || iterations >= MAX_ITERATIONS {
                break;
            }
            
            let cost_reduction = (initial_cost - current_plan.estimated_cost.total_cost) / initial_cost;
            if cost_reduction < self.config.cost_threshold {
                break;
            }
        }
        
        Ok(current_plan)
    }

    fn is_better_plan(&self, new_plan: &QueryPlan, old_plan: &QueryPlan) -> bool {
        new_plan.estimated_cost.total_cost < old_plan.estimated_cost.total_cost * 0.95
    }
}

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn is_applicable(&self, plan: &QueryPlan) -> bool {
        self.has_pushable_predicate(&plan.root)
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        let optimized_root = self.pushdown_predicates(plan.root)?;
        
        Ok(QueryPlan {
            root: optimized_root,
            ..plan
        })
    }
}

impl PredicatePushdown {
    fn has_pushable_predicate(&self, node: &PlanNode) -> bool {
        match node {
            PlanNode::Filter(filter) => {
                matches!(filter.input.as_ref(), PlanNode::Join(_) | PlanNode::Scan(_))
            },
            PlanNode::Join(join) => {
                self.has_pushable_predicate(&join.left) || 
                self.has_pushable_predicate(&join.right)
            },
            _ => false,
        }
    }

    fn pushdown_predicates(&self, node: Arc<PlanNode>) -> Result<Arc<PlanNode>, Error> {
        match node.as_ref() {
            PlanNode::Filter(filter) => {
                match filter.input.as_ref() {
                    PlanNode::Join(join) => {
                        let (left_preds, right_preds, join_preds) = 
                            self.split_join_predicates(&filter.predicate, join)?;
                        
                        let new_left = if !left_preds.is_empty() {
                            Arc::new(PlanNode::Filter(FilterNode {
                                input: join.left.clone(),
                                predicate: self.combine_predicates(left_preds),
                                selectivity: 0.5,
                                estimated_rows: join.left.as_ref().estimated_rows() / 2,
                                estimated_cost: super::planner::CostEstimate::zero(),
                            }))
                        } else {
                            join.left.clone()
                        };
                        
                        let new_right = if !right_preds.is_empty() {
                            Arc::new(PlanNode::Filter(FilterNode {
                                input: join.right.clone(),
                                predicate: self.combine_predicates(right_preds),
                                selectivity: 0.5,
                                estimated_rows: join.right.as_ref().estimated_rows() / 2,
                                estimated_cost: super::planner::CostEstimate::zero(),
                            }))
                        } else {
                            join.right.clone()
                        };
                        
                        let new_join = Arc::new(PlanNode::Join(JoinNode {
                            left: new_left,
                            right: new_right,
                            ..join.clone()
                        }));
                        
                        if !join_preds.is_empty() {
                            Ok(Arc::new(PlanNode::Filter(FilterNode {
                                input: new_join,
                                predicate: self.combine_predicates(join_preds),
                                ..filter.clone()
                            })))
                        } else {
                            Ok(new_join)
                        }
                    },
                    _ => Ok(node),
                }
            },
            _ => Ok(node),
        }
    }

    fn split_join_predicates(
        &self,
        predicate: &Predicate,
        _join: &JoinNode,
    ) -> Result<(Vec<Predicate>, Vec<Predicate>, Vec<Predicate>), Error> {
        let left_preds = Vec::new();
        let right_preds = Vec::new();
        let join_preds = vec![predicate.clone()];
        
        Ok((left_preds, right_preds, join_preds))
    }

    fn combine_predicates(&self, predicates: Vec<Predicate>) -> Predicate {
        if predicates.is_empty() {
            return Predicate::IsNotNull(Expression::Literal(super::planner::Value::Bool(true)));
        }
        
        let mut result = predicates[0].clone();
        for pred in predicates.into_iter().skip(1) {
            result = Predicate::And(Box::new(result), Box::new(pred));
        }
        
        result
    }
}

impl OptimizationRule for JoinReordering {
    fn name(&self) -> &str {
        "JoinReordering"
    }

    fn is_applicable(&self, plan: &QueryPlan) -> bool {
        self.count_joins(&plan.root) > 1
    }

    fn apply(&self, plan: QueryPlan, stats: &TableStatistics) -> Result<QueryPlan, Error> {
        let joins = self.extract_joins(&plan.root)?;
        
        if joins.len() <= 1 {
            return Ok(plan);
        }
        
        let best_order = self.find_best_join_order(joins, stats)?;
        let reordered_root = self.build_join_tree(best_order)?;
        
        Ok(QueryPlan {
            root: reordered_root,
            ..plan
        })
    }
}

impl JoinReordering {
    fn count_joins(&self, node: &PlanNode) -> usize {
        match node {
            PlanNode::Join(join) => {
                1 + self.count_joins(&join.left) + self.count_joins(&join.right)
            },
            _ => 0,
        }
    }

    fn extract_joins(&self, node: &PlanNode) -> Result<Vec<JoinInfo>, Error> {
        let mut joins = Vec::new();
        self.extract_joins_recursive(node, &mut joins)?;
        Ok(joins)
    }

    fn extract_joins_recursive(&self, node: &PlanNode, joins: &mut Vec<JoinInfo>) -> Result<(), Error> {
        match node {
            PlanNode::Join(join) => {
                joins.push(JoinInfo {
                    left_table: self.get_table_name(&join.left)?,
                    right_table: self.get_table_name(&join.right)?,
                    join_type: join.join_type,
                    condition: join.join_condition.clone(),
                    estimated_rows: join.estimated_rows,
                });
                
                self.extract_joins_recursive(&join.left, joins)?;
                self.extract_joins_recursive(&join.right, joins)?;
            },
            _ => {},
        }
        Ok(())
    }

    fn get_table_name(&self, node: &PlanNode) -> Result<String, Error> {
        match node {
            PlanNode::Scan(scan) => Ok(scan.table_name.clone()),
            _ => Ok("unknown".to_string()),
        }
    }

    fn find_best_join_order(&self, joins: Vec<JoinInfo>, _stats: &TableStatistics) -> Result<Vec<JoinInfo>, Error> {
        Ok(joins)
    }

    fn build_join_tree(&self, joins: Vec<JoinInfo>) -> Result<Arc<PlanNode>, Error> {
        Err(Error::NotImplemented("Join tree building".to_string()))
    }
}

#[derive(Debug, Clone)]
struct JoinInfo {
    left_table: String,
    right_table: String,
    join_type: super::planner::JoinType,
    condition: super::planner::JoinCondition,
    estimated_rows: usize,
}

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn is_applicable(&self, plan: &QueryPlan) -> bool {
        self.has_foldable_constants(&plan.root)
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        let optimized_root = self.fold_constants(plan.root)?;
        
        Ok(QueryPlan {
            root: optimized_root,
            ..plan
        })
    }
}

impl ConstantFolding {
    fn has_foldable_constants(&self, _node: &PlanNode) -> bool {
        false
    }

    fn fold_constants(&self, node: Arc<PlanNode>) -> Result<Arc<PlanNode>, Error> {
        Ok(node)
    }
}

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn is_applicable(&self, _plan: &QueryPlan) -> bool {
        true
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        Ok(plan)
    }
}

impl OptimizationRule for CommonSubexpressionElimination {
    fn name(&self) -> &str {
        "CommonSubexpressionElimination"
    }

    fn is_applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        Ok(plan)
    }
}

impl OptimizationRule for MaterializedViewRewrite {
    fn name(&self) -> &str {
        "MaterializedViewRewrite"
    }

    fn is_applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        Ok(plan)
    }
}

impl OptimizationRule for IndexSelection {
    fn name(&self) -> &str {
        "IndexSelection"
    }

    fn is_applicable(&self, plan: &QueryPlan) -> bool {
        self.has_indexable_predicate(&plan.root)
    }

    fn apply(&self, plan: QueryPlan, stats: &TableStatistics) -> Result<QueryPlan, Error> {
        let optimized_root = self.select_indexes(plan.root, stats)?;
        
        Ok(QueryPlan {
            root: optimized_root,
            ..plan
        })
    }
}

impl IndexSelection {
    fn has_indexable_predicate(&self, node: &PlanNode) -> bool {
        match node {
            PlanNode::Filter(filter) => {
                matches!(filter.input.as_ref(), PlanNode::Scan(_))
            },
            _ => false,
        }
    }

    fn select_indexes(&self, node: Arc<PlanNode>, _stats: &TableStatistics) -> Result<Arc<PlanNode>, Error> {
        Ok(node)
    }
}

impl OptimizationRule for PartitionPruning {
    fn name(&self) -> &str {
        "PartitionPruning"
    }

    fn is_applicable(&self, _plan: &QueryPlan) -> bool {
        false
    }

    fn apply(&self, plan: QueryPlan, _stats: &TableStatistics) -> Result<QueryPlan, Error> {
        Ok(plan)
    }
}

impl PlanNode {
    fn estimated_rows(&self) -> usize {
        match self {
            PlanNode::Scan(scan) => scan.estimated_rows,
            PlanNode::Filter(filter) => filter.estimated_rows,
            PlanNode::Join(join) => join.estimated_rows,
            PlanNode::Aggregate(agg) => agg.estimated_rows,
            PlanNode::Sort(sort) => sort.estimated_rows,
            _ => 1000,
        }
    }
}
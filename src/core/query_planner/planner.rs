use crate::core::error::Error;
use crate::core::index::IndexKey;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ExecutionStep {
    IndexLookup {
        index_name: String,
        condition: QueryCondition,
        estimated_rows: usize,
    },
    RangeScan {
        index_name: String,
        start_key: IndexKey,
        end_key: IndexKey,
        estimated_rows: usize,
    },
    Join {
        left_step: usize,
        right_step: usize,
        join_type: JoinType,
        estimated_rows: usize,
    },
    Filter {
        input_step: usize,
        condition: QueryCondition,
        estimated_rows: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub conditions: Vec<QueryCondition>,
    pub joins: Vec<QueryJoin>,
    pub projections: Vec<String>,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryCondition {
    Equals {
        field: String,
        value: Vec<u8>,
    },
    GreaterThan {
        field: String,
        value: Vec<u8>,
    },
    LessThan {
        field: String,
        value: Vec<u8>,
    },
    GreaterOrEqual {
        field: String,
        value: Vec<u8>,
    },
    LessOrEqual {
        field: String,
        value: Vec<u8>,
    },
    NotEquals {
        field: String,
        value: Vec<u8>,
    },
    In {
        field: String,
        values: Vec<Vec<u8>>,
    },
    Between {
        field: String,
        low: Vec<u8>,
        high: Vec<u8>,
    },
    Range {
        field: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    And(Box<QueryCondition>, Box<QueryCondition>),
    Or(Box<QueryCondition>, Box<QueryCondition>),
    Not(Box<QueryCondition>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryJoin {
    pub join_type: JoinType,
    pub left_table: String,
    pub right_table: String,
    pub left_field: String,
    pub right_field: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByClause {
    pub field: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub root_node: PlanNode,
    pub steps: Vec<ExecutionStep>,
    pub estimated_cost: QueryCost,
    pub estimated_rows: usize,
    pub optimization_hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCost {
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub network_cost: f64,
    pub memory_cost: f64,
    pub total_cost: f64,
}

#[derive(Debug, Clone)]
pub struct QueryPlanner {
    optimizer: Arc<super::optimizer::QueryOptimizer>,
    statistics: Arc<RwLock<super::statistics::TableStatistics>>,
    config: PlannerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannerConfig {
    pub enable_cost_based_optimization: bool,
    pub enable_join_reordering: bool,
    pub enable_predicate_pushdown: bool,
    pub enable_projection_pushdown: bool,
    pub enable_index_selection: bool,
    pub parallel_threshold: usize,
    pub memory_limit: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            enable_cost_based_optimization: true,
            enable_join_reordering: true,
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_index_selection: true,
            parallel_threshold: 10000,
            memory_limit: 1024 * 1024 * 256,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    pub root: Box<PlanNode>,
    pub estimated_cost: CostEstimate,
    pub estimated_rows: usize,
    pub required_memory: usize,
    pub parallel_degree: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanNode {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Join(JoinNode),
    Aggregate(AggregateNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Index(IndexNode),
    MaterializedView(MaterializedViewNode),
    HashAggregate(HashAggregateNode),
    WindowFunction(WindowFunctionNode),
    Exchange(ExchangeNode),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanNode {
    pub table_name: String,
    pub columns: Vec<String>,
    pub predicate: Option<Predicate>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
    pub scan_type: ScanType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ScanType {
    FullTable,
    Index,
    Range,
    Bitmap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterNode {
    pub input: Box<PlanNode>,
    pub predicate: Predicate,
    pub selectivity: f64,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectNode {
    pub input: Box<PlanNode>,
    pub expressions: Vec<Expression>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinNode {
    pub left: Box<PlanNode>,
    pub right: Box<PlanNode>,
    pub join_type: JoinType,
    pub join_condition: JoinCondition,
    pub strategy: JoinStrategy,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum JoinStrategy {
    NestedLoop,
    HashJoin,
    MergeJoin,
    IndexJoin,
    BroadcastJoin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub additional_predicate: Option<Predicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateNode {
    pub input: Box<PlanNode>,
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateFunction>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortNode {
    pub input: Box<PlanNode>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expression: Expression,
    pub descending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitNode {
    pub input: Box<PlanNode>,
    pub limit: usize,
    pub offset: Option<usize>,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnionNode {
    pub inputs: Vec<Box<PlanNode>>,
    pub union_type: UnionType,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnionType {
    All,
    Distinct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexNode {
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub predicate: Option<Predicate>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewNode {
    pub view_name: String,
    pub columns: Vec<String>,
    pub predicate: Option<Predicate>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashAggregateNode {
    pub input: Box<PlanNode>,
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateFunction>,
    pub estimated_groups: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFunctionNode {
    pub input: Box<PlanNode>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpr>,
    pub functions: Vec<WindowFunction>,
    pub estimated_rows: usize,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeNode {
    pub input: Box<PlanNode>,
    pub exchange_type: ExchangeType,
    pub partition_keys: Vec<Expression>,
    pub estimated_cost: CostEstimate,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ExchangeType {
    Gather,
    Repartition,
    Broadcast,
    RoundRobin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Predicate {
    Equals(Expression, Expression),
    NotEquals(Expression, Expression),
    GreaterThan(Expression, Expression),
    LessThan(Expression, Expression),
    GreaterThanOrEqual(Expression, Expression),
    LessThanOrEqual(Expression, Expression),
    In(Expression, Vec<Expression>),
    Between(Expression, Expression, Expression),
    Like(Expression, String),
    IsNull(Expression),
    IsNotNull(Expression),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Column(String),
    Literal(Value),
    BinaryOp(BinaryOperator, Box<Expression>, Box<Expression>),
    UnaryOp(UnaryOperator, Box<Expression>),
    Function(String, Vec<Expression>),
    Cast(Box<Expression>, DataType),
    Case(Vec<CaseWhen>, Option<Box<Expression>>),
    Subquery(Box<QueryPlan>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseWhen {
    pub condition: Predicate,
    pub result: Expression,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Concat,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Negate,
    BitwiseNot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count(Option<Expression>),
    Sum(Expression),
    Avg(Expression),
    Min(Expression),
    Max(Expression),
    GroupConcat(Expression, Option<String>),
    StdDev(Expression),
    Variance(Expression),
    Percentile(Expression, f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunction {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Lag(Expression, Option<i32>, Option<Expression>),
    Lead(Expression, Option<i32>, Option<Expression>),
    FirstValue(Expression),
    LastValue(Expression),
    NthValue(Expression, i32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Date(i32),
    Timestamp(i64),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    Timestamp,
    Decimal(u8, u8),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CostEstimate {
    pub cpu_cost: f64,
    pub io_cost: f64,
    pub network_cost: f64,
    pub memory_cost: f64,
    pub total_cost: f64,
}

impl CostEstimate {
    pub fn new(cpu: f64, io: f64, network: f64, memory: f64) -> Self {
        Self {
            cpu_cost: cpu,
            io_cost: io,
            network_cost: network,
            memory_cost: memory,
            total_cost: cpu + io + network + memory,
        }
    }

    pub fn zero() -> Self {
        Self::new(0.0, 0.0, 0.0, 0.0)
    }

    pub fn add(&self, other: &CostEstimate) -> CostEstimate {
        Self::new(
            self.cpu_cost + other.cpu_cost,
            self.io_cost + other.io_cost,
            self.network_cost + other.network_cost,
            self.memory_cost + other.memory_cost,
        )
    }

    pub fn scale(&self, factor: f64) -> CostEstimate {
        Self::new(
            self.cpu_cost * factor,
            self.io_cost * factor,
            self.network_cost * factor,
            self.memory_cost * factor,
        )
    }
}

impl QueryPlanner {
    pub fn new(config: PlannerConfig) -> Self {
        Self {
            optimizer: Arc::new(super::optimizer::QueryOptimizer::new()),
            statistics: Arc::new(RwLock::new(super::statistics::TableStatistics::new())),
            config,
        }
    }

    pub fn plan_query(&self, query_spec: &QuerySpec) -> Result<ExecutionPlan, Error> {
        let query = self.convert_spec_to_query(query_spec)?;
        let logical_plan = self.build_logical_plan(query)?;

        let optimized_plan = if self.config.enable_cost_based_optimization {
            self.optimizer
                .optimize(logical_plan, &self.statistics.read())?
        } else {
            logical_plan
        };

        let physical_plan = self.generate_physical_plan(optimized_plan)?;

        let exec_plan = ExecutionPlan {
            root_node: physical_plan.root.as_ref().clone(),
            steps: Vec::new(),
            estimated_cost: QueryCost {
                cpu_cost: physical_plan.estimated_cost.cpu_cost,
                io_cost: physical_plan.estimated_cost.io_cost,
                network_cost: physical_plan.estimated_cost.network_cost,
                memory_cost: physical_plan.estimated_cost.memory_cost,
                total_cost: physical_plan.estimated_cost.total_cost,
            },
            estimated_rows: physical_plan.estimated_rows,
            optimization_hints: Vec::new(),
        };

        Ok(exec_plan)
    }

    fn convert_spec_to_query(&self, spec: &QuerySpec) -> Result<Query, Error> {
        let table_name = if !spec.joins.is_empty() {
            spec.joins[0].left_table.clone()
        } else {
            "default_table".to_string()
        };

        let mut query = Query {
            select_columns: spec.projections.clone(),
            from_table: table_name,
            joins: Vec::new(),
            where_clause: self.convert_conditions_to_predicate(&spec.conditions)?,
            group_by: Vec::new(),
            having: None,
            order_by: spec
                .order_by
                .iter()
                .map(|o| OrderByExpr {
                    expression: Expression::Column(o.field.clone()),
                    descending: !o.ascending,
                    nulls_first: false,
                })
                .collect(),
            limit: spec.limit,
            offset: spec.offset,
            aggregates: Vec::new(),
        };

        for join in &spec.joins {
            query.joins.push(JoinClause {
                join_type: join.join_type,
                table: join.right_table.clone(),
                condition: JoinCondition {
                    left_keys: vec![join.left_field.clone()],
                    right_keys: vec![join.right_field.clone()],
                    additional_predicate: None,
                },
            });
        }

        Ok(query)
    }

    fn convert_conditions_to_predicate(
        &self,
        conditions: &[QueryCondition],
    ) -> Result<Option<Predicate>, Error> {
        if conditions.is_empty() {
            return Ok(None);
        }

        let mut predicates = Vec::new();
        for condition in conditions {
            let pred = match condition {
                QueryCondition::Equals { field, value } => Predicate::Equals(
                    Expression::Column(field.clone()),
                    Expression::Literal(Value::Binary(value.clone())),
                ),
                QueryCondition::GreaterThan { field, value } => Predicate::GreaterThan(
                    Expression::Column(field.clone()),
                    Expression::Literal(Value::Binary(value.clone())),
                ),
                QueryCondition::LessThan { field, value } => Predicate::LessThan(
                    Expression::Column(field.clone()),
                    Expression::Literal(Value::Binary(value.clone())),
                ),
                _ => continue,
            };
            predicates.push(pred);
        }

        if predicates.is_empty() {
            Ok(None)
        } else if predicates.len() == 1 {
            Ok(Some(predicates.into_iter().next().unwrap()))
        } else {
            let mut result = predicates[0].clone();
            for pred in predicates.into_iter().skip(1) {
                result = Predicate::And(Box::new(result), Box::new(pred));
            }
            Ok(Some(result))
        }
    }

    fn build_logical_plan(&self, query: Query) -> Result<QueryPlan, Error> {
        let mut plan = self.build_scan_nodes(&query)?;

        if let Some(predicate) = query.where_clause {
            plan = self.add_filter_node(plan, predicate)?;
        }

        if !query.group_by.is_empty() {
            plan = self.add_aggregate_node(plan, query.group_by, query.aggregates)?;
        }

        if !query.order_by.is_empty() {
            plan = self.add_sort_node(plan, query.order_by)?;
        }

        if let Some(limit) = query.limit {
            plan = self.add_limit_node(plan, limit, query.offset)?;
        }

        Ok(plan)
    }

    fn build_scan_nodes(&self, query: &Query) -> Result<QueryPlan, Error> {
        let scan_node = PlanNode::Scan(ScanNode {
            table_name: query.from_table.clone(),
            columns: query.select_columns.clone(),
            predicate: None,
            estimated_rows: 1_000_000,
            estimated_cost: CostEstimate::new(100.0, 1000.0, 0.0, 100.0),
            scan_type: ScanType::FullTable,
        });

        Ok(QueryPlan {
            root: Box::new(scan_node),
            estimated_cost: CostEstimate::new(100.0, 1000.0, 0.0, 100.0),
            estimated_rows: 1_000_000,
            required_memory: 1024 * 1024,
            parallel_degree: 1,
        })
    }

    fn add_filter_node(&self, input: QueryPlan, predicate: Predicate) -> Result<QueryPlan, Error> {
        let selectivity = self.estimate_selectivity(&predicate)?;
        let estimated_rows = (input.estimated_rows as f64 * selectivity) as usize;

        let filter_node = PlanNode::Filter(FilterNode {
            input: input.root,
            predicate,
            selectivity,
            estimated_rows,
            estimated_cost: input.estimated_cost.scale(selectivity),
        });

        Ok(QueryPlan {
            root: Box::new(filter_node),
            estimated_cost: input.estimated_cost.scale(selectivity),
            estimated_rows,
            required_memory: input.required_memory,
            parallel_degree: input.parallel_degree,
        })
    }

    fn add_aggregate_node(
        &self,
        input: QueryPlan,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateFunction>,
    ) -> Result<QueryPlan, Error> {
        let estimated_groups = self.estimate_distinct_values(&group_by, input.estimated_rows)?;

        let agg_node = PlanNode::HashAggregate(HashAggregateNode {
            input: input.root,
            group_by,
            aggregates,
            estimated_groups,
            estimated_cost: CostEstimate::new(
                input.estimated_rows as f64 * 0.1,
                0.0,
                0.0,
                estimated_groups as f64 * 100.0,
            ),
        });

        Ok(QueryPlan {
            root: Box::new(agg_node),
            estimated_cost: input.estimated_cost.add(&CostEstimate::new(
                input.estimated_rows as f64 * 0.1,
                0.0,
                0.0,
                estimated_groups as f64 * 100.0,
            )),
            estimated_rows: estimated_groups,
            required_memory: estimated_groups * 100,
            parallel_degree: input.parallel_degree,
        })
    }

    fn add_sort_node(
        &self,
        input: QueryPlan,
        order_by: Vec<OrderByExpr>,
    ) -> Result<QueryPlan, Error> {
        let sort_cost = CostEstimate::new(
            input.estimated_rows as f64 * (input.estimated_rows as f64).log2(),
            0.0,
            0.0,
            input.estimated_rows as f64 * 8.0,
        );

        let sort_node = PlanNode::Sort(SortNode {
            input: input.root,
            order_by,
            limit: None,
            estimated_rows: input.estimated_rows,
            estimated_cost: sort_cost,
        });

        Ok(QueryPlan {
            root: Box::new(sort_node),
            estimated_cost: input.estimated_cost.add(&sort_cost),
            estimated_rows: input.estimated_rows,
            required_memory: input.estimated_rows * 100,
            parallel_degree: input.parallel_degree,
        })
    }

    fn add_limit_node(
        &self,
        input: QueryPlan,
        limit: usize,
        offset: Option<usize>,
    ) -> Result<QueryPlan, Error> {
        let limit_node = PlanNode::Limit(LimitNode {
            input: input.root,
            limit,
            offset,
            estimated_cost: CostEstimate::zero(),
        });

        Ok(QueryPlan {
            root: Box::new(limit_node),
            estimated_cost: input.estimated_cost,
            estimated_rows: limit.min(input.estimated_rows),
            required_memory: limit * 100,
            parallel_degree: input.parallel_degree,
        })
    }

    fn generate_physical_plan(&self, logical_plan: QueryPlan) -> Result<QueryPlan, Error> {
        if logical_plan.estimated_rows > self.config.parallel_threshold {
            self.parallelize_plan(logical_plan)
        } else {
            Ok(logical_plan)
        }
    }

    fn parallelize_plan(&self, plan: QueryPlan) -> Result<QueryPlan, Error> {
        let parallel_degree = self.determine_parallel_degree(&plan)?;

        let exchange_node = PlanNode::Exchange(ExchangeNode {
            input: plan.root,
            exchange_type: ExchangeType::Gather,
            partition_keys: Vec::new(),
            estimated_cost: CostEstimate::new(0.0, 0.0, 100.0, 0.0),
        });

        Ok(QueryPlan {
            root: Box::new(exchange_node),
            estimated_cost: plan.estimated_cost.scale(1.0 / parallel_degree as f64),
            estimated_rows: plan.estimated_rows,
            required_memory: plan.required_memory,
            parallel_degree,
        })
    }

    fn determine_parallel_degree(&self, plan: &QueryPlan) -> Result<usize, Error> {
        let cpu_count = num_cpus::get();
        let data_size = plan.estimated_rows * 100;
        let chunk_size = 1024 * 1024;

        let ideal_parallelism = (data_size / chunk_size).max(1).min(cpu_count);
        Ok(ideal_parallelism)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn estimate_selectivity(&self, predicate: &Predicate) -> Result<f64, Error> {
        match predicate {
            Predicate::Equals(_, _) => Ok(0.1),
            Predicate::GreaterThan(_, _) | Predicate::LessThan(_, _) => Ok(0.3),
            Predicate::Between(_, _, _) => Ok(0.2),
            Predicate::In(_, ref values) => Ok(values.len() as f64 * 0.1),
            Predicate::Like(_, _) => Ok(0.5),
            Predicate::IsNull(_) => Ok(0.05),
            Predicate::IsNotNull(_) => Ok(0.95),
            Predicate::And(left, right) => {
                let left_sel = self.estimate_selectivity(left)?;
                let right_sel = self.estimate_selectivity(right)?;
                Ok(left_sel * right_sel)
            }
            Predicate::Or(left, right) => {
                let left_sel = self.estimate_selectivity(left)?;
                let right_sel = self.estimate_selectivity(right)?;
                Ok(left_sel + right_sel - left_sel * right_sel)
            }
            Predicate::Not(inner) => {
                let inner_sel = self.estimate_selectivity(inner)?;
                Ok(1.0 - inner_sel)
            }
            _ => Ok(0.5),
        }
    }

    fn estimate_distinct_values(
        &self,
        exprs: &[Expression],
        total_rows: usize,
    ) -> Result<usize, Error> {
        if exprs.is_empty() {
            return Ok(1);
        }

        let estimated = (total_rows as f64 * 0.1).max(1.0).min(total_rows as f64) as usize;
        Ok(estimated)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub select_columns: Vec<String>,
    pub from_table: String,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Predicate>,
    pub group_by: Vec<Expression>,
    pub having: Option<Predicate>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub aggregates: Vec<AggregateFunction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub condition: JoinCondition,
}

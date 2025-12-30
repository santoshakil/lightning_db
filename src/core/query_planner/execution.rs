use super::planner::{Expression, PlanNode, Predicate, QueryPlan, Value};
use crate::core::error::Error;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

const DEFAULT_BATCH_SIZE: usize = 1024;
const MAX_MEMORY_PER_OPERATOR: usize = 100 * 1024 * 1024;
const PIPELINE_BUFFER_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct ExecutionContext {
    pub batch_size: usize,
    pub memory_limit: usize,
    pub parallel_degree: usize,
    pub enable_pipelining: bool,
    pub enable_vectorization: bool,
    pub timeout: Option<std::time::Duration>,
    pub session_variables: HashMap<String, Value>,
    pub statistics_collector: Arc<RwLock<ExecutionStatistics>>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            memory_limit: MAX_MEMORY_PER_OPERATOR,
            parallel_degree: num_cpus::get(),
            enable_pipelining: true,
            enable_vectorization: true,
            timeout: Some(std::time::Duration::from_secs(300)),
            session_variables: HashMap::new(),
            statistics_collector: Arc::new(RwLock::new(ExecutionStatistics::new())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionStatistics {
    pub rows_processed: usize,
    pub bytes_processed: usize,
    pub operators_executed: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub spills_to_disk: usize,
    pub peak_memory_usage: usize,
    pub execution_time: std::time::Duration,
}

impl ExecutionStatistics {
    fn new() -> Self {
        Self {
            rows_processed: 0,
            bytes_processed: 0,
            operators_executed: 0,
            cache_hits: 0,
            cache_misses: 0,
            spills_to_disk: 0,
            peak_memory_usage: 0,
            execution_time: std::time::Duration::from_secs(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub columns: HashMap<String, ColumnVector>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub enum ColumnVector {
    Bool(Vec<bool>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Null(usize),
}

impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Bool(v) => v.len(),
            ColumnVector::Int32(v) => v.len(),
            ColumnVector::Int64(v) => v.len(),
            ColumnVector::Float32(v) => v.len(),
            ColumnVector::Float64(v) => v.len(),
            ColumnVector::String(v) => v.len(),
            ColumnVector::Binary(v) => v.len(),
            ColumnVector::Null(n) => *n,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ColumnVector::Bool(v) => v.len(),
            ColumnVector::Int32(v) => v.len() * 4,
            ColumnVector::Int64(v) => v.len() * 8,
            ColumnVector::Float32(v) => v.len() * 4,
            ColumnVector::Float64(v) => v.len() * 8,
            ColumnVector::String(v) => v.iter().map(|s| s.len()).sum(),
            ColumnVector::Binary(v) => v.iter().map(|b| b.len()).sum(),
            ColumnVector::Null(_) => 0,
        }
    }
}

#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error>;
    fn estimated_memory(&self) -> usize;
    fn estimated_rows(&self) -> usize;
    fn name(&self) -> &str;
}

pub struct RecordBatchStream {
    receiver: mpsc::Receiver<Result<RecordBatch, Error>>,
}

impl RecordBatchStream {
    pub fn new(receiver: mpsc::Receiver<Result<RecordBatch, Error>>) -> Self {
        Self { receiver }
    }

    pub async fn next(&mut self) -> Option<Result<RecordBatch, Error>> {
        self.receiver.recv().await
    }

    pub async fn collect(mut self) -> Result<Vec<RecordBatch>, Error> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next().await {
            batches.push(batch?);
        }
        Ok(batches)
    }
}

pub struct QueryExecutor {
    _operators: HashMap<String, Arc<dyn PhysicalOperator>>,
    _memory_manager: Arc<MemoryManager>,
    _scheduler: Arc<TaskScheduler>,
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self {
            _operators: HashMap::new(),
            _memory_manager: Arc::new(MemoryManager::new(1024 * 1024 * 1024)),
            _scheduler: Arc::new(TaskScheduler::new(num_cpus::get())),
        }
    }
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn execute_plan(
        &self,
        plan: QueryPlan,
        context: ExecutionContext,
    ) -> Result<QueryResult, Error> {
        let start_time = std::time::Instant::now();

        let physical_plan = self.create_physical_plan(plan)?;

        let mut stream = physical_plan.execute(&context).await?;
        let mut all_batches = Vec::new();
        let mut total_rows = 0;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            total_rows += batch.row_count;
            all_batches.push(batch);
        }

        let execution_time = start_time.elapsed();

        {
            let mut stats = context.statistics_collector.write();
            stats.execution_time = execution_time;
            stats.rows_processed = total_rows;
        }

        Ok(QueryResult {
            batches: all_batches,
            total_rows,
            execution_time,
            statistics: context.statistics_collector.read().clone(),
        })
    }

    fn create_physical_plan(&self, plan: QueryPlan) -> Result<Arc<dyn PhysicalOperator>, Error> {
        self.plan_to_operator(plan.root)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn plan_to_operator(&self, node: Box<PlanNode>) -> Result<Arc<dyn PhysicalOperator>, Error> {
        match node.as_ref() {
            PlanNode::Scan(scan) => Ok(Arc::new(ScanOperator {
                table_name: scan.table_name.clone(),
                columns: scan.columns.clone(),
                _predicate: scan.predicate.clone(),
                estimated_rows: scan.estimated_rows,
            })),
            PlanNode::Filter(filter) => {
                let input = self.plan_to_operator(filter.input.clone())?;
                Ok(Arc::new(FilterOperator {
                    input,
                    predicate: filter.predicate.clone(),
                    selectivity: filter.selectivity,
                }))
            }
            PlanNode::Join(join) => {
                let left = self.plan_to_operator(join.left.clone())?;
                let right = self.plan_to_operator(join.right.clone())?;

                match join.strategy {
                    super::planner::JoinStrategy::HashJoin => Ok(Arc::new(HashJoinOperator {
                        left,
                        right,
                        join_type: join.join_type,
                        left_keys: join.join_condition.left_keys.clone(),
                        right_keys: join.join_condition.right_keys.clone(),
                    })),
                    _ => Err(Error::UnsupportedFeature {
                        feature: format!("Join strategy {:?}", join.strategy),
                    }),
                }
            }
            PlanNode::HashAggregate(agg) => {
                let input = self.plan_to_operator(agg.input.clone())?;
                Ok(Arc::new(HashAggregateOperator {
                    input,
                    group_by: agg.group_by.clone(),
                    aggregates: agg.aggregates.clone(),
                    estimated_groups: agg.estimated_groups,
                }))
            }
            PlanNode::Sort(sort) => {
                let input = self.plan_to_operator(sort.input.clone())?;
                Ok(Arc::new(SortOperator {
                    input,
                    _order_by: sort.order_by.clone(),
                    limit: sort.limit,
                }))
            }
            PlanNode::Limit(limit) => {
                let input = self.plan_to_operator(limit.input.clone())?;
                Ok(Arc::new(LimitOperator {
                    input,
                    limit: limit.limit,
                    offset: limit.offset,
                }))
            }
            _ => Err(Error::UnsupportedFeature {
                feature: "Plan node type".to_string(),
            }),
        }
    }
}

struct ScanOperator {
    table_name: String,
    columns: Vec<String>,
    _predicate: Option<Predicate>,
    estimated_rows: usize,
}

#[async_trait]
impl PhysicalOperator for ScanOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let (sender, receiver) = mpsc::channel(PIPELINE_BUFFER_SIZE);

        let _table_name = self.table_name.clone();
        let columns = self.columns.clone();
        let batch_size = context.batch_size;

        tokio::spawn(async move {
            let mut row_count = 0;

            while row_count < 10000 {
                let mut batch_columns = HashMap::new();

                for col in &columns {
                    let values = (0..batch_size.min(10000 - row_count))
                        .map(|i| (row_count + i) as i64)
                        .collect();
                    batch_columns.insert(col.clone(), ColumnVector::Int64(values));
                }

                let batch = RecordBatch {
                    columns: batch_columns,
                    row_count: batch_size.min(10000 - row_count),
                };

                row_count += batch.row_count;

                if sender.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.estimated_rows * 100
    }

    fn estimated_rows(&self) -> usize {
        self.estimated_rows
    }

    fn name(&self) -> &str {
        "Scan"
    }
}

struct FilterOperator {
    input: Arc<dyn PhysicalOperator>,
    predicate: Predicate,
    selectivity: f64,
}

#[async_trait]
impl PhysicalOperator for FilterOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let mut input_stream = self.input.execute(context).await?;
        let (sender, receiver) = mpsc::channel(PIPELINE_BUFFER_SIZE);

        let predicate = self.predicate.clone();

        tokio::spawn(async move {
            while let Some(batch) = input_stream.next().await {
                match batch {
                    Ok(batch) => {
                        let filtered = Self::filter_batch(batch, &predicate);
                        if filtered.row_count > 0 && sender.send(Ok(filtered)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.input.estimated_memory()
    }

    fn estimated_rows(&self) -> usize {
        (self.input.estimated_rows() as f64 * self.selectivity) as usize
    }

    fn name(&self) -> &str {
        "Filter"
    }
}

impl FilterOperator {
    fn filter_batch(batch: RecordBatch, _predicate: &Predicate) -> RecordBatch {
        batch
    }
}

struct HashJoinOperator {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    join_type: super::planner::JoinType,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
}

#[async_trait]
impl PhysicalOperator for HashJoinOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let build_side = self.right.execute(context).await?;
        let hash_table = self.build_hash_table(build_side).await?;

        let mut probe_side = self.left.execute(context).await?;
        let (sender, receiver) = mpsc::channel(PIPELINE_BUFFER_SIZE);

        let left_keys = self.left_keys.clone();
        let right_keys = self.right_keys.clone();
        let join_type = self.join_type;

        tokio::spawn(async move {
            while let Some(batch) = probe_side.next().await {
                match batch {
                    Ok(batch) => {
                        let joined = Self::probe_hash_table(
                            batch,
                            &hash_table,
                            &left_keys,
                            &right_keys,
                            join_type,
                        );
                        if joined.row_count > 0 && sender.send(Ok(joined)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.right.estimated_rows() * 200
    }

    fn estimated_rows(&self) -> usize {
        self.left.estimated_rows()
    }

    fn name(&self) -> &str {
        "HashJoin"
    }
}

impl HashJoinOperator {
    async fn build_hash_table(&self, mut stream: RecordBatchStream) -> Result<HashTable, Error> {
        let mut table = HashTable::new();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            table.insert_batch(batch, &self.right_keys)?;
        }

        Ok(table)
    }

    fn probe_hash_table(
        batch: RecordBatch,
        _hash_table: &HashTable,
        _left_keys: &[String],
        _right_keys: &[String],
        _join_type: super::planner::JoinType,
    ) -> RecordBatch {
        batch
    }
}

struct HashTable {
    buckets: HashMap<u64, Vec<RecordBatch>>,
}

impl HashTable {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    fn insert_batch(&mut self, batch: RecordBatch, _keys: &[String]) -> Result<(), Error> {
        self.buckets.entry(0).or_default().push(batch);
        Ok(())
    }
}

struct HashAggregateOperator {
    input: Arc<dyn PhysicalOperator>,
    group_by: Vec<Expression>,
    aggregates: Vec<super::planner::AggregateFunction>,
    estimated_groups: usize,
}

#[async_trait]
impl PhysicalOperator for HashAggregateOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let mut input_stream = self.input.execute(context).await?;
        let mut accumulator = AggregateAccumulator::new(&self.group_by, &self.aggregates);

        while let Some(batch) = input_stream.next().await {
            let batch = batch?;
            accumulator.update(batch)?;
        }

        let result = accumulator.finalize()?;
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            let _ = sender.send(Ok(result)).await;
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.estimated_groups * 100
    }

    fn estimated_rows(&self) -> usize {
        self.estimated_groups
    }

    fn name(&self) -> &str {
        "HashAggregate"
    }
}

struct AggregateAccumulator {
    _groups: HashMap<Vec<Value>, AggregateState>,
}

impl AggregateAccumulator {
    fn new(_group_by: &[Expression], _aggregates: &[super::planner::AggregateFunction]) -> Self {
        Self {
            _groups: HashMap::new(),
        }
    }

    fn update(&mut self, _batch: RecordBatch) -> Result<(), Error> {
        Ok(())
    }

    fn finalize(self) -> Result<RecordBatch, Error> {
        Ok(RecordBatch {
            columns: HashMap::new(),
            row_count: 0,
        })
    }
}

struct AggregateState {
    _count: usize,
    _sum: f64,
    _min: Option<Value>,
    _max: Option<Value>,
}

struct SortOperator {
    input: Arc<dyn PhysicalOperator>,
    _order_by: Vec<super::planner::OrderByExpr>,
    limit: Option<usize>,
}

#[async_trait]
impl PhysicalOperator for SortOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let mut input_stream = self.input.execute(context).await?;
        let mut all_batches = Vec::new();

        while let Some(batch) = input_stream.next().await {
            all_batches.push(batch?);
        }

        let sorted = self.sort_batches(all_batches)?;
        let (sender, receiver) = mpsc::channel(1);

        tokio::spawn(async move {
            let _ = sender.send(Ok(sorted)).await;
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.input.estimated_rows() * 100
    }

    fn estimated_rows(&self) -> usize {
        self.limit.unwrap_or(self.input.estimated_rows())
    }

    fn name(&self) -> &str {
        "Sort"
    }
}

impl SortOperator {
    fn sort_batches(&self, batches: Vec<RecordBatch>) -> Result<RecordBatch, Error> {
        // Return first batch or empty batch if none
        match batches.into_iter().next() {
            Some(batch) => Ok(batch),
            None => Ok(RecordBatch {
                columns: HashMap::new(),
                row_count: 0,
            }),
        }
    }
}

struct LimitOperator {
    input: Arc<dyn PhysicalOperator>,
    limit: usize,
    offset: Option<usize>,
}

#[async_trait]
impl PhysicalOperator for LimitOperator {
    async fn execute(&self, context: &ExecutionContext) -> Result<RecordBatchStream, Error> {
        let mut input_stream = self.input.execute(context).await?;
        let (sender, receiver) = mpsc::channel(PIPELINE_BUFFER_SIZE);

        let limit = self.limit;
        let offset = self.offset.unwrap_or(0);

        tokio::spawn(async move {
            let mut skipped = 0;
            let mut taken = 0;

            while let Some(batch) = input_stream.next().await {
                match batch {
                    Ok(mut batch) => {
                        if skipped < offset {
                            let to_skip = offset - skipped;
                            if to_skip >= batch.row_count {
                                skipped += batch.row_count;
                                continue;
                            }
                            batch = Self::skip_rows(batch, to_skip);
                            skipped = offset;
                        }

                        if taken >= limit {
                            break;
                        }

                        let to_take = limit - taken;
                        if to_take < batch.row_count {
                            batch = Self::take_rows(batch, to_take);
                        }

                        taken += batch.row_count;

                        if sender.send(Ok(batch)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(RecordBatchStream::new(receiver))
    }

    fn estimated_memory(&self) -> usize {
        self.limit * 100
    }

    fn estimated_rows(&self) -> usize {
        self.limit
    }

    fn name(&self) -> &str {
        "Limit"
    }
}

impl LimitOperator {
    fn skip_rows(batch: RecordBatch, _n: usize) -> RecordBatch {
        batch
    }

    fn take_rows(batch: RecordBatch, _n: usize) -> RecordBatch {
        batch
    }
}

struct MemoryManager {
    _total_memory: usize,
    _used_memory: Arc<RwLock<usize>>,
    _allocations: Arc<RwLock<HashMap<String, usize>>>,
}

impl MemoryManager {
    fn new(total_memory: usize) -> Self {
        Self {
            _total_memory: total_memory,
            _used_memory: Arc::new(RwLock::new(0)),
            _allocations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn _allocate(&self, _operator: &str, _size: usize) -> Result<_MemoryAllocation<'_>, Error> {
        let mut used = self._used_memory.write();

        if *used + _size > self._total_memory {
            return Err(Error::ResourceExhausted {
                resource: format!(
                    "Memory: requested {}, available {}",
                    _size,
                    self._total_memory - *used
                ),
            });
        }

        *used += _size;
        self._allocations.write().insert(_operator.to_string(), _size);

        Ok(_MemoryAllocation {
            manager: self,
            size: _size,
            operator: _operator.to_string(),
        })
    }
}

struct _MemoryAllocation<'a> {
    manager: &'a MemoryManager,
    size: usize,
    operator: String,
}

impl Drop for _MemoryAllocation<'_> {
    fn drop(&mut self) {
        *self.manager._used_memory.write() -= self.size;
        self.manager._allocations.write().remove(&self.operator);
    }
}

struct TaskScheduler {
    _semaphore: Arc<Semaphore>,
    _task_queue: Arc<RwLock<VecDeque<Task>>>,
}

impl TaskScheduler {
    fn new(parallelism: usize) -> Self {
        Self {
            _semaphore: Arc::new(Semaphore::new(parallelism)),
            _task_queue: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    async fn _schedule<F, R>(&self, task: F) -> Result<R, Error>
    where
        F: Future<Output = Result<R, Error>> + Send + 'static,
        R: Send + 'static,
    {
        let _permit = self._semaphore.acquire().await.map_err(|_| Error::InvalidOperation {
            reason: "Scheduler semaphore closed".to_string(),
        })?;
        task.await
    }
}

struct Task {
    _id: String,
    _priority: u32,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub batches: Vec<RecordBatch>,
    pub total_rows: usize,
    pub execution_time: std::time::Duration,
    pub statistics: ExecutionStatistics,
}

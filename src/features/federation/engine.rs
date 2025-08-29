use crate::core::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, Semaphore};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedQuery {
    pub id: Uuid,
    pub sql: String,
    pub parameters: Vec<QueryParameter>,
    pub options: QueryOptions,
    pub metadata: QueryMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParameter {
    pub name: String,
    pub value: ParameterValue,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParameterValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<ParameterValue>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    DateTime,
    Timestamp,
    Array(Box<DataType>),
    Struct(Vec<FieldType>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldType {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryOptions {
    pub timeout: Option<Duration>,
    pub max_rows: Option<usize>,
    pub fetch_size: usize,
    pub read_consistency: ConsistencyLevel,
    pub enable_cache: bool,
    pub parallel_execution: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsistencyLevel {
    Strong,
    BoundedStaleness(Duration),
    SessionConsistency,
    EventualConsistency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    pub request_id: Uuid,
    pub client_id: String,
    pub submitted_at: DateTime<Utc>,
    pub tags: HashMap<String, String>,
}

pub struct FederationEngine {
    query_router: Arc<super::query_router::QueryRouter>,
    schema_mapper: Arc<super::schema_mapper::SchemaMapper>,
    data_sources: Arc<DataSourceRegistry>,
    query_optimizer: Arc<super::query_optimizer::FederatedOptimizer>,
    result_merger: Arc<super::result_merger::ResultMerger>,
    metadata_manager: Arc<super::metadata_manager::MetadataManager>,
    execution_coordinator: Arc<ExecutionCoordinator>,
    cache_manager: Arc<super::cache::FederationCache>,
    metrics: Arc<FederationMetrics>,
    config: Arc<FederationConfig>,
}

struct DataSourceRegistry {
    sources: Arc<DashMap<String, Arc<dyn DataSourceConnector>>>,
    source_metadata: Arc<DashMap<String, SourceMetadata>>,
    health_checker: Arc<HealthChecker>,
}

#[derive(Debug, Clone)]
struct SourceMetadata {
    name: String,
    source_type: DataSourceType,
    connection_string: String,
    capabilities: SourceCapabilities,
    statistics: SourceStatistics,
    last_accessed: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataSourceType {
    PostgreSQL,
    MySQL,
    MongoDB,
    Cassandra,
    Redis,
    Elasticsearch,
    S3,
    Kafka,
    HTTP,
    Custom(String),
}

#[derive(Debug, Clone)]
struct SourceCapabilities {
    supports_sql: bool,
    supports_transactions: bool,
    supports_joins: bool,
    supports_aggregations: bool,
    supports_window_functions: bool,
    supports_cte: bool,
    supports_subqueries: bool,
    supports_prepared_statements: bool,
    max_connection_limit: usize,
}

#[derive(Debug, Clone)]
struct SourceStatistics {
    total_queries: u64,
    failed_queries: u64,
    avg_response_time_ms: f64,
    last_error: Option<String>,
    uptime_percentage: f64,
}

#[async_trait]
pub trait DataSourceConnector: Send + Sync {
    async fn connect(&self) -> Result<()>;
    async fn disconnect(&self) -> Result<()>;
    async fn execute(&self, query: &SubQuery) -> Result<QueryResult>;
    async fn get_schema(&self, table: &str) -> Result<TableSchema>;
    async fn get_statistics(&self, table: &str) -> Result<TableStatistics>;
    async fn health_check(&self) -> Result<HealthStatus>;
}

#[derive(Debug, Clone)]
pub struct SubQuery {
    pub id: Uuid,
    pub parent_id: Uuid,
    pub source_name: String,
    pub sql: String,
    pub parameters: Vec<QueryParameter>,
    pub pushdown_predicates: Vec<Predicate>,
    pub projected_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Predicate {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: ParameterValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    In,
    Between,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub schema: ResultSchema,
    pub rows: Vec<Row>,
    pub execution_stats: ExecutionStats,
}

#[derive(Debug, Clone)]
pub struct ResultSchema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Date(chrono::NaiveDate),
    DateTime(DateTime<Utc>),
    Array(Vec<Value>),
}

#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub rows_scanned: u64,
    pub rows_returned: u64,
    pub execution_time_ms: u64,
    pub cpu_time_ms: u64,
    pub memory_used_bytes: u64,
    pub network_bytes_sent: u64,
    pub network_bytes_received: u64,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<IndexSchema>,
    pub partitions: Option<PartitionSchema>,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_unique: bool,
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub is_unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
    Bitmap,
    FullText,
    Spatial,
}

#[derive(Debug, Clone)]
pub struct PartitionSchema {
    pub partition_type: PartitionType,
    pub partition_columns: Vec<String>,
    pub partition_count: usize,
}

#[derive(Debug, Clone)]
pub enum PartitionType {
    Range,
    Hash,
    List,
    Composite,
}

#[derive(Debug, Clone)]
pub struct TableStatistics {
    pub row_count: u64,
    pub size_bytes: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
    pub last_analyzed: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub distinct_values: u64,
    pub null_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub avg_size_bytes: f64,
    pub histogram: Option<Histogram>,
}

#[derive(Debug, Clone)]
pub struct Histogram {
    pub buckets: Vec<HistogramBucket>,
    pub total_count: u64,
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub lower_bound: String,
    pub upper_bound: String,
    pub count: u64,
    pub distinct_count: u64,
}

#[derive(Debug, Clone)]
pub enum HealthStatus {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

struct HealthChecker {
    check_interval: Duration,
    timeout: Duration,
    max_retries: u32,
}

struct ExecutionCoordinator {
    execution_pool: Arc<ExecutionPool>,
    query_tracker: Arc<QueryTracker>,
    resource_manager: Arc<ResourceManager>,
    failure_handler: Arc<FailureHandler>,
}

struct ExecutionPool {
    thread_pool: Arc<tokio::runtime::Runtime>,
    semaphore: Arc<Semaphore>,
    max_concurrent_queries: usize,
}

struct QueryTracker {
    active_queries: Arc<DashMap<Uuid, ActiveQuery>>,
    completed_queries: Arc<RwLock<VecDeque<CompletedQuery>>>,
    query_history_limit: usize,
}

#[derive(Debug, Clone)]
struct ActiveQuery {
    query_id: Uuid,
    started_at: DateTime<Utc>,
    status: QueryStatus,
    sub_queries: Vec<Uuid>,
    resource_usage: ResourceUsage,
}

#[derive(Debug, Clone)]
enum QueryStatus {
    Planning,
    Executing,
    Merging,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, Clone)]
struct CompletedQuery {
    query_id: Uuid,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    status: QueryStatus,
    execution_stats: ExecutionStats,
}

#[derive(Debug, Clone)]
struct ResourceUsage {
    cpu_percentage: f64,
    memory_bytes: u64,
    network_bytes: u64,
    disk_io_bytes: u64,
}

struct ResourceManager {
    resource_limits: ResourceLimits,
    current_usage: Arc<RwLock<ResourceUsage>>,
    quota_manager: Arc<QuotaManager>,
}

#[derive(Debug, Clone)]
struct ResourceLimits {
    max_cpu_percentage: f64,
    max_memory_bytes: u64,
    max_network_bandwidth_mbps: f64,
    max_concurrent_connections: usize,
}

struct QuotaManager {
    quotas: Arc<DashMap<String, ClientQuota>>,
}

#[derive(Debug, Clone)]
struct ClientQuota {
    client_id: String,
    max_queries_per_minute: u32,
    max_data_scanned_gb: f64,
    max_result_size_mb: f64,
    current_usage: QuotaUsage,
}

#[derive(Debug, Clone)]
struct QuotaUsage {
    queries_count: u32,
    data_scanned_bytes: u64,
    last_reset: DateTime<Utc>,
}

struct FailureHandler {
    retry_policy: RetryPolicy,
    circuit_breakers: Arc<DashMap<String, CircuitBreaker>>,
    fallback_strategies: Arc<RwLock<Vec<FallbackStrategy>>>,
}

#[derive(Debug, Clone)]
struct RetryPolicy {
    max_retries: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    exponential_backoff: bool,
}

#[derive(Debug, Clone)]
struct CircuitBreaker {
    source_name: String,
    state: CircuitState,
    failure_count: u32,
    last_failure: Option<DateTime<Utc>>,
    success_count: u32,
    last_success: Option<DateTime<Utc>>,
    threshold: u32,
    timeout: Duration,
}

#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone)]
enum FallbackStrategy {
    UseCache,
    UseStaleData(Duration),
    ReturnPartialResults,
    RouteToAlternativeSource(String),
}

struct FederationMetrics {
    queries_total: Arc<RwLock<u64>>,
    queries_by_source: Arc<DashMap<String, u64>>,
    query_latency: Arc<RwLock<Vec<u64>>>,
    cache_hits: Arc<RwLock<u64>>,
    cache_misses: Arc<RwLock<u64>>,
    errors_total: Arc<RwLock<u64>>,
    data_transferred_bytes: Arc<RwLock<u64>>,
}

impl FederationEngine {
    pub async fn new(config: FederationConfig) -> Result<Self> {
        let data_sources = Arc::new(DataSourceRegistry::new());
        let metadata_manager = Arc::new(super::metadata_manager::MetadataManager::new(
            config.metadata_config.clone()
        ).await?);
        
        Ok(Self {
            query_router: Arc::new(super::query_router::QueryRouter::new(
                metadata_manager.clone()
            )),
            schema_mapper: Arc::new(super::schema_mapper::SchemaMapper::new()),
            data_sources,
            query_optimizer: Arc::new(super::query_optimizer::FederatedOptimizer::new(
                config.optimizer_config.clone()
            )),
            result_merger: Arc::new(super::result_merger::ResultMerger::new()),
            metadata_manager,
            execution_coordinator: Arc::new(ExecutionCoordinator::new(
                config.execution_config.clone()
            )),
            cache_manager: Arc::new(super::cache::FederationCache::new(
                config.cache_config.clone()
            )),
            metrics: Arc::new(FederationMetrics::new()),
            config: Arc::new(config),
        })
    }

    pub async fn execute_query(&self, query: FederatedQuery) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        self.metrics.record_query_start().await;
        
        if self.config.enable_cache && query.options.enable_cache {
            if let Some(cached) = self.cache_manager.get(&query).await? {
                self.metrics.record_cache_hit().await;
                return Ok(cached);
            }
        }
        
        let execution_plan = self.create_execution_plan(&query).await?;
        
        let optimized_plan = self.query_optimizer.optimize(execution_plan).await?;
        
        let active_query = self.execution_coordinator.register_query(&query).await?;
        
        let sub_results = self.execute_sub_queries(&optimized_plan).await?;
        
        let merged_result = self.result_merger.merge(sub_results, &optimized_plan).await?;
        
        if self.config.enable_cache && query.options.enable_cache {
            self.cache_manager.put(&query, &merged_result).await?;
        }
        
        let duration = start.elapsed().as_millis() as u64;
        self.metrics.record_query_complete(duration).await;
        
        self.execution_coordinator.complete_query(active_query.query_id).await?;
        
        Ok(merged_result)
    }

    async fn create_execution_plan(&self, query: &FederatedQuery) -> Result<ExecutionPlan> {
        let parsed_query = self.parse_query(&query.sql)?;
        
        let involved_sources = self.query_router.identify_sources(&parsed_query).await?;
        
        let schema_mappings = self.schema_mapper.map_schemas(&involved_sources).await?;
        
        let sub_queries = self.query_router.route_query(
            &parsed_query,
            &involved_sources,
            &schema_mappings
        ).await?;
        
        Ok(ExecutionPlan {
            query_id: query.id,
            parsed_query,
            sub_queries,
            merge_strategy: MergeStrategy::default(),
            estimated_cost: 0.0,
        })
    }

    async fn execute_sub_queries(&self, plan: &ExecutionPlan) -> Result<Vec<QueryResult>> {
        let mut results = Vec::new();
        let mut handles = Vec::new();
        
        for sub_query in &plan.sub_queries {
            let source = self.data_sources.get_source(&sub_query.source_name).await?;
            let sub_query = sub_query.clone();
            
            let handle = tokio::spawn(async move {
                source.execute(&sub_query).await
            });
            
            handles.push(handle);
        }
        
        for handle in handles {
            let result = handle.await??;
            results.push(result);
        }
        
        Ok(results)
    }

    pub async fn register_data_source(&self, name: String, connector: Arc<dyn DataSourceConnector>) -> Result<()> {
        connector.connect().await?;
        
        let health = connector.health_check().await?;
        if !matches!(health, HealthStatus::Healthy) {
            return Err("Data source is not healthy".into());
        }
        
        self.data_sources.register(name, connector).await?;
        
        self.metadata_manager.refresh_metadata().await?;
        
        Ok(())
    }

    pub async fn unregister_data_source(&self, name: &str) -> Result<()> {
        if let Some(source) = self.data_sources.get_source(name).await.ok() {
            source.disconnect().await?;
        }
        
        self.data_sources.unregister(name).await?;
        
        self.metadata_manager.refresh_metadata().await?;
        
        Ok(())
    }

    pub async fn get_federated_schema(&self) -> Result<FederatedSchema> {
        self.metadata_manager.get_federated_schema().await
    }

    pub async fn explain_query(&self, query: &FederatedQuery) -> Result<ExecutionPlan> {
        let plan = self.create_execution_plan(query).await?;
        let optimized = self.query_optimizer.optimize(plan).await?;
        Ok(optimized)
    }

    pub async fn get_statistics(&self) -> FederationStatistics {
        FederationStatistics {
            total_queries: *self.metrics.queries_total.read().await,
            queries_by_source: self.metrics.queries_by_source
                .iter()
                .map(|e| (e.key().clone(), *e.value()))
                .collect(),
            avg_latency_ms: self.metrics.calculate_avg_latency().await,
            cache_hit_rate: self.metrics.calculate_cache_hit_rate().await,
            error_rate: self.metrics.calculate_error_rate().await,
            data_transferred_gb: (*self.metrics.data_transferred_bytes.read().await as f64) / 1_073_741_824.0,
        }
    }

    pub async fn health_check(&self) -> Result<Vec<SourceHealth>> {
        let mut health_statuses = Vec::new();
        
        for source in self.data_sources.sources.iter() {
            let health = source.value().health_check().await?;
            health_statuses.push(SourceHealth {
                name: source.key().clone(),
                status: health,
                last_checked: Utc::now(),
            });
        }
        
        Ok(health_statuses)
    }

    fn parse_query(&self, sql: &str) -> Result<ParsedQuery> {
        Ok(ParsedQuery {
            original_sql: sql.to_string(),
            ast: QueryAST::default(),
            tables: Vec::new(),
            columns: Vec::new(),
            predicates: Vec::new(),
            joins: Vec::new(),
            aggregations: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        })
    }
}

impl DataSourceRegistry {
    fn new() -> Self {
        Self {
            sources: Arc::new(DashMap::new()),
            source_metadata: Arc::new(DashMap::new()),
            health_checker: Arc::new(HealthChecker {
                check_interval: Duration::minutes(1),
                timeout: Duration::seconds(30),
                max_retries: 3,
            }),
        }
    }

    async fn register(&self, name: String, connector: Arc<dyn DataSourceConnector>) -> Result<()> {
        self.sources.insert(name.clone(), connector);
        
        let metadata = SourceMetadata {
            name: name.clone(),
            source_type: DataSourceType::Custom(name.clone()),
            connection_string: String::new(),
            capabilities: SourceCapabilities::default(),
            statistics: SourceStatistics::default(),
            last_accessed: Utc::now(),
        };
        
        self.source_metadata.insert(name, metadata);
        Ok(())
    }

    async fn unregister(&self, name: &str) -> Result<()> {
        self.sources.remove(name);
        self.source_metadata.remove(name);
        Ok(())
    }

    async fn get_source(&self, name: &str) -> Result<Arc<dyn DataSourceConnector>> {
        self.sources.get(name)
            .map(|s| s.clone())
            .ok_or_else(|| format!("Data source '{}' not found", name).into())
    }
}

impl ExecutionCoordinator {
    fn new(config: ExecutionConfig) -> Self {
        Self {
            execution_pool: Arc::new(ExecutionPool {
                thread_pool: Arc::new(tokio::runtime::Runtime::new().unwrap()),
                semaphore: Arc::new(Semaphore::new(config.max_concurrent_queries)),
                max_concurrent_queries: config.max_concurrent_queries,
            }),
            query_tracker: Arc::new(QueryTracker {
                active_queries: Arc::new(DashMap::new()),
                completed_queries: Arc::new(RwLock::new(VecDeque::new())),
                query_history_limit: config.query_history_limit,
            }),
            resource_manager: Arc::new(ResourceManager {
                resource_limits: config.resource_limits,
                current_usage: Arc::new(RwLock::new(ResourceUsage::default())),
                quota_manager: Arc::new(QuotaManager {
                    quotas: Arc::new(DashMap::new()),
                }),
            }),
            failure_handler: Arc::new(FailureHandler {
                retry_policy: config.retry_policy,
                circuit_breakers: Arc::new(DashMap::new()),
                fallback_strategies: Arc::new(RwLock::new(Vec::new())),
            }),
        }
    }

    async fn register_query(&self, query: &FederatedQuery) -> Result<ActiveQuery> {
        let active = ActiveQuery {
            query_id: query.id,
            started_at: Utc::now(),
            status: QueryStatus::Planning,
            sub_queries: Vec::new(),
            resource_usage: ResourceUsage::default(),
        };
        
        self.query_tracker.active_queries.insert(query.id, active.clone());
        Ok(active)
    }

    async fn complete_query(&self, query_id: Uuid) -> Result<()> {
        if let Some((_, active)) = self.query_tracker.active_queries.remove(&query_id) {
            let completed = CompletedQuery {
                query_id,
                started_at: active.started_at,
                completed_at: Utc::now(),
                status: QueryStatus::Completed,
                execution_stats: ExecutionStats::default(),
            };
            
            self.query_tracker.completed_queries.write().await.push_back(completed);
        }
        Ok(())
    }
}

impl FederationMetrics {
    fn new() -> Self {
        Self {
            queries_total: Arc::new(RwLock::new(0)),
            queries_by_source: Arc::new(DashMap::new()),
            query_latency: Arc::new(RwLock::new(Vec::new())),
            cache_hits: Arc::new(RwLock::new(0)),
            cache_misses: Arc::new(RwLock::new(0)),
            errors_total: Arc::new(RwLock::new(0)),
            data_transferred_bytes: Arc::new(RwLock::new(0)),
        }
    }

    async fn record_query_start(&self) {
        *self.queries_total.write().await += 1;
    }

    async fn record_query_complete(&self, latency_ms: u64) {
        self.query_latency.write().await.push(latency_ms);
    }

    async fn record_cache_hit(&self) {
        *self.cache_hits.write().await += 1;
    }

    async fn calculate_avg_latency(&self) -> f64 {
        let latencies = self.query_latency.read().await;
        if latencies.is_empty() {
            return 0.0;
        }
        latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
    }

    async fn calculate_cache_hit_rate(&self) -> f64 {
        let hits = *self.cache_hits.read().await;
        let misses = *self.cache_misses.read().await;
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }

    async fn calculate_error_rate(&self) -> f64 {
        let errors = *self.errors_total.read().await;
        let total = *self.queries_total.read().await;
        if total == 0 {
            return 0.0;
        }
        errors as f64 / total as f64
    }
}

impl Default for SourceCapabilities {
    fn default() -> Self {
        Self {
            supports_sql: true,
            supports_transactions: false,
            supports_joins: false,
            supports_aggregations: true,
            supports_window_functions: false,
            supports_cte: false,
            supports_subqueries: false,
            supports_prepared_statements: true,
            max_connection_limit: 100,
        }
    }
}

impl Default for SourceStatistics {
    fn default() -> Self {
        Self {
            total_queries: 0,
            failed_queries: 0,
            avg_response_time_ms: 0.0,
            last_error: None,
            uptime_percentage: 100.0,
        }
    }
}

impl Default for ResourceUsage {
    fn default() -> Self {
        Self {
            cpu_percentage: 0.0,
            memory_bytes: 0,
            network_bytes: 0,
            disk_io_bytes: 0,
        }
    }
}

impl Default for ExecutionStats {
    fn default() -> Self {
        Self {
            rows_scanned: 0,
            rows_returned: 0,
            execution_time_ms: 0,
            cpu_time_ms: 0,
            memory_used_bytes: 0,
            network_bytes_sent: 0,
            network_bytes_received: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub query_id: Uuid,
    pub parsed_query: ParsedQuery,
    pub sub_queries: Vec<SubQuery>,
    pub merge_strategy: MergeStrategy,
    pub estimated_cost: f64,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub original_sql: String,
    pub ast: QueryAST,
    pub tables: Vec<TableReference>,
    pub columns: Vec<ColumnReference>,
    pub predicates: Vec<Predicate>,
    pub joins: Vec<JoinClause>,
    pub aggregations: Vec<AggregationFunction>,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryAST {
}

#[derive(Debug, Clone)]
pub struct TableReference {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnReference {
    pub table: Option<String>,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub left_table: String,
    pub right_table: String,
    pub condition: String,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct AggregationFunction {
    pub function: String,
    pub column: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Default)]
pub struct MergeStrategy {
    pub merge_type: MergeType,
    pub sort_columns: Vec<String>,
    pub deduplication: bool,
}

#[derive(Debug, Clone)]
pub enum MergeType {
    Union,
    UnionAll,
    Intersect,
    Except,
    Join,
}

impl Default for MergeType {
    fn default() -> Self {
        MergeType::Union
    }
}

#[derive(Debug, Clone)]
pub struct FederatedSchema {
    pub sources: Vec<SourceSchema>,
    pub global_tables: Vec<GlobalTable>,
    pub relationships: Vec<CrossSourceRelationship>,
}

#[derive(Debug, Clone)]
pub struct SourceSchema {
    pub name: String,
    pub tables: Vec<TableSchema>,
}

#[derive(Debug, Clone)]
pub struct GlobalTable {
    pub name: String,
    pub source_mappings: Vec<SourceTableMapping>,
}

#[derive(Debug, Clone)]
pub struct SourceTableMapping {
    pub source_name: String,
    pub table_name: String,
    pub column_mappings: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CrossSourceRelationship {
    pub name: String,
    pub left_source: String,
    pub left_table: String,
    pub left_column: String,
    pub right_source: String,
    pub right_table: String,
    pub right_column: String,
    pub relationship_type: RelationshipType,
}

#[derive(Debug, Clone)]
pub enum RelationshipType {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Debug, Clone)]
pub struct FederationStatistics {
    pub total_queries: u64,
    pub queries_by_source: HashMap<String, u64>,
    pub avg_latency_ms: f64,
    pub cache_hit_rate: f64,
    pub error_rate: f64,
    pub data_transferred_gb: f64,
}

#[derive(Debug, Clone)]
pub struct SourceHealth {
    pub name: String,
    pub status: HealthStatus,
    pub last_checked: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationConfig {
    pub enable_cache: bool,
    pub max_concurrent_queries: usize,
    pub query_timeout: Duration,
    pub metadata_config: MetadataConfig,
    pub optimizer_config: OptimizerConfig,
    pub cache_config: CacheConfig,
    pub execution_config: ExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    pub refresh_interval: Duration,
    pub schema_cache_ttl: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerConfig {
    pub enable_pushdown: bool,
    pub enable_join_reordering: bool,
    pub cost_model: CostModel,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CostModel {
    Simple,
    Statistics,
    MachineLearning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub max_size_mb: usize,
    pub ttl: Duration,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionPolicy {
    LRU,
    LFU,
    FIFO,
    TTL,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub max_concurrent_queries: usize,
    pub query_history_limit: usize,
    pub resource_limits: ResourceLimits,
    pub retry_policy: RetryPolicy,
}
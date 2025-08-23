use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use warp::{Filter, Reply};

#[derive(Debug, Clone)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

#[derive(Debug, Clone)]
pub struct MetricMetadata {
    pub name: String,
    pub metric_type: MetricType,
    pub help: String,
    pub labels: Vec<String>,
}

pub trait MetricValue: Send + Sync {
    fn as_prometheus_string(&self, metadata: &MetricMetadata) -> String;
    fn reset(&self);
}

pub struct Counter {
    value: Arc<AtomicU64>,
    metadata: MetricMetadata,
}

impl Counter {
    pub fn new(name: String, help: String, labels: Vec<String>) -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
            metadata: MetricMetadata {
                name,
                metric_type: MetricType::Counter,
                help,
                labels,
            },
        }
    }
    
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn inc_by(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }
    
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl MetricValue for Counter {
    fn as_prometheus_string(&self, metadata: &MetricMetadata) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            metadata.name,
            metadata.help,
            metadata.name,
            metadata.name,
            self.get()
        )
    }
    
    fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

pub struct Gauge {
    value: Arc<AtomicI64>,
    metadata: MetricMetadata,
}

impl Gauge {
    pub fn new(name: String, help: String, labels: Vec<String>) -> Self {
        Self {
            value: Arc::new(AtomicI64::new(0)),
            metadata: MetricMetadata {
                name,
                metric_type: MetricType::Gauge,
                help,
                labels,
            },
        }
    }
    
    pub fn set(&self, v: i64) {
        self.value.store(v, Ordering::Relaxed);
    }
    
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }
    
    pub fn add(&self, n: i64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }
    
    pub fn sub(&self, n: i64) {
        self.value.fetch_sub(n, Ordering::Relaxed);
    }
    
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl MetricValue for Gauge {
    fn as_prometheus_string(&self, metadata: &MetricMetadata) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {}\n",
            metadata.name,
            metadata.help,
            metadata.name,
            metadata.name,
            self.get()
        )
    }
    
    fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

pub struct Histogram {
    buckets: Arc<RwLock<Vec<(f64, u64)>>>,
    sum: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    metadata: MetricMetadata,
}

impl Histogram {
    pub fn new(name: String, help: String, labels: Vec<String>, buckets: Vec<f64>) -> Self {
        let mut sorted_buckets = buckets;
        sorted_buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let bucket_pairs: Vec<(f64, u64)> = sorted_buckets
            .into_iter()
            .map(|b| (b, 0))
            .collect();
        
        Self {
            buckets: Arc::new(RwLock::new(bucket_pairs)),
            sum: Arc::new(AtomicU64::new(0)),
            count: Arc::new(AtomicU64::new(0)),
            metadata: MetricMetadata {
                name,
                metric_type: MetricType::Histogram,
                help,
                labels,
            },
        }
    }
    
    pub async fn observe(&self, v: f64) {
        let mut buckets = self.buckets.write().await;
        
        for (threshold, count) in buckets.iter_mut() {
            if v <= *threshold {
                *count += 1;
            }
        }
        
        self.sum.fetch_add((v * 1000000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub async fn get_buckets(&self) -> Vec<(f64, u64)> {
        self.buckets.read().await.clone()
    }
    
    pub fn get_sum(&self) -> f64 {
        self.sum.load(Ordering::Relaxed) as f64 / 1000000.0
    }
    
    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

impl MetricValue for Histogram {
    fn as_prometheus_string(&self, metadata: &MetricMetadata) -> String {
        let buckets = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(self.get_buckets())
        });
        
        let mut output = format!(
            "# HELP {} {}\n# TYPE {} histogram\n",
            metadata.name, metadata.help, metadata.name
        );
        
        for (threshold, count) in &buckets {
            output.push_str(&format!(
                "{}_bucket{{le=\"{}\"}} {}\n",
                metadata.name, threshold, count
            ));
        }
        
        output.push_str(&format!(
            "{}_bucket{{le=\"+Inf\"}} {}\n",
            metadata.name,
            self.get_count()
        ));
        
        output.push_str(&format!("{}_sum {}\n", metadata.name, self.get_sum()));
        output.push_str(&format!("{}_count {}\n", metadata.name, self.get_count()));
        
        output
    }
    
    fn reset(&self) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut buckets = self.buckets.write().await;
                for (_, count) in buckets.iter_mut() {
                    *count = 0;
                }
            });
        });
        
        self.sum.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
    }
}

pub struct Summary {
    quantiles: Arc<RwLock<Vec<(f64, f64)>>>,
    observations: Arc<RwLock<Vec<f64>>>,
    metadata: MetricMetadata,
    max_observations: usize,
}

impl Summary {
    pub fn new(
        name: String,
        help: String,
        labels: Vec<String>,
        quantiles: Vec<f64>,
        max_observations: usize,
    ) -> Self {
        let quantile_pairs: Vec<(f64, f64)> = quantiles
            .into_iter()
            .map(|q| (q, 0.0))
            .collect();
        
        Self {
            quantiles: Arc::new(RwLock::new(quantile_pairs)),
            observations: Arc::new(RwLock::new(Vec::with_capacity(max_observations))),
            metadata: MetricMetadata {
                name,
                metric_type: MetricType::Summary,
                help,
                labels,
            },
            max_observations,
        }
    }
    
    pub async fn observe(&self, v: f64) {
        let mut observations = self.observations.write().await;
        
        if observations.len() >= self.max_observations {
            observations.remove(0);
        }
        
        observations.push(v);
        
        let mut sorted = observations.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let mut quantiles = self.quantiles.write().await;
        for (quantile, value) in quantiles.iter_mut() {
            let index = ((sorted.len() as f64 - 1.0) * *quantile) as usize;
            *value = sorted[index];
        }
    }
    
    pub async fn get_quantiles(&self) -> Vec<(f64, f64)> {
        self.quantiles.read().await.clone()
    }
    
    pub async fn get_sum(&self) -> f64 {
        self.observations.read().await.iter().sum()
    }
    
    pub async fn get_count(&self) -> usize {
        self.observations.read().await.len()
    }
}

impl MetricValue for Summary {
    fn as_prometheus_string(&self, metadata: &MetricMetadata) -> String {
        let quantiles = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(self.get_quantiles())
        });
        
        let sum = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(self.get_sum())
        });
        
        let count = tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(self.get_count())
        });
        
        let mut output = format!(
            "# HELP {} {}\n# TYPE {} summary\n",
            metadata.name, metadata.help, metadata.name
        );
        
        for (quantile, value) in &quantiles {
            output.push_str(&format!(
                "{}{{quantile=\"{}\"}} {}\n",
                metadata.name, quantile, value
            ));
        }
        
        output.push_str(&format!("{}_sum {}\n", metadata.name, sum));
        output.push_str(&format!("{}_count {}\n", metadata.name, count));
        
        output
    }
    
    fn reset(&self) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                self.observations.write().await.clear();
                let mut quantiles = self.quantiles.write().await;
                for (_, value) in quantiles.iter_mut() {
                    *value = 0.0;
                }
            });
        });
    }
}

pub struct MetricsRegistry {
    metrics: Arc<RwLock<HashMap<String, Arc<dyn MetricValue>>>>,
    metadata: Arc<RwLock<HashMap<String, MetricMetadata>>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn register_counter(&self, name: String, help: String, labels: Vec<String>) -> Arc<Counter> {
        let counter = Arc::new(Counter::new(name.clone(), help.clone(), labels.clone()));
        
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.clone(), counter.clone() as Arc<dyn MetricValue>);
        
        let mut metadata = self.metadata.write().await;
        metadata.insert(name.clone(), counter.metadata.clone());
        
        counter
    }
    
    pub async fn register_gauge(&self, name: String, help: String, labels: Vec<String>) -> Arc<Gauge> {
        let gauge = Arc::new(Gauge::new(name.clone(), help.clone(), labels.clone()));
        
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.clone(), gauge.clone() as Arc<dyn MetricValue>);
        
        let mut metadata = self.metadata.write().await;
        metadata.insert(name.clone(), gauge.metadata.clone());
        
        gauge
    }
    
    pub async fn register_histogram(
        &self,
        name: String,
        help: String,
        labels: Vec<String>,
        buckets: Vec<f64>,
    ) -> Arc<Histogram> {
        let histogram = Arc::new(Histogram::new(name.clone(), help.clone(), labels.clone(), buckets));
        
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.clone(), histogram.clone() as Arc<dyn MetricValue>);
        
        let mut metadata = self.metadata.write().await;
        metadata.insert(name.clone(), histogram.metadata.clone());
        
        histogram
    }
    
    pub async fn register_summary(
        &self,
        name: String,
        help: String,
        labels: Vec<String>,
        quantiles: Vec<f64>,
        max_observations: usize,
    ) -> Arc<Summary> {
        let summary = Arc::new(Summary::new(name.clone(), help.clone(), labels.clone(), quantiles, max_observations));
        
        let mut metrics = self.metrics.write().await;
        metrics.insert(name.clone(), summary.clone() as Arc<dyn MetricValue>);
        
        let mut metadata = self.metadata.write().await;
        metadata.insert(name.clone(), summary.metadata.clone());
        
        summary
    }
    
    pub async fn gather(&self) -> String {
        let metrics = self.metrics.read().await;
        let metadata = self.metadata.read().await;
        
        let mut output = String::new();
        
        for (name, metric) in metrics.iter() {
            if let Some(meta) = metadata.get(name) {
                output.push_str(&metric.as_prometheus_string(meta));
            }
        }
        
        output
    }
    
    pub async fn reset_all(&self) {
        let metrics = self.metrics.read().await;
        for metric in metrics.values() {
            metric.reset();
        }
    }
}

pub struct PrometheusExporter {
    registry: Arc<MetricsRegistry>,
    port: u16,
}

impl PrometheusExporter {
    pub fn new(registry: Arc<MetricsRegistry>, port: u16) -> Self {
        Self { registry, port }
    }
    
    pub async fn start(self) {
        let registry = self.registry.clone();
        
        let metrics_route = warp::path("metrics")
            .and(warp::get())
            .and_then(move || {
                let registry = registry.clone();
                async move {
                    let metrics = registry.gather().await;
                    Ok::<_, warp::Rejection>(
                        warp::reply::with_header(
                            metrics,
                            "Content-Type",
                            "text/plain; version=0.0.4"
                        )
                    )
                }
            });
        
        let health_route = warp::path("health")
            .and(warp::get())
            .map(|| warp::reply::with_status("OK", warp::http::StatusCode::OK));
        
        let routes = metrics_route.or(health_route);
        
        println!("Prometheus metrics exporter listening on :{}", self.port);
        
        warp::serve(routes)
            .run(([0, 0, 0, 0], self.port))
            .await;
    }
}

pub struct LightningDbMetrics {
    pub operations_total: Arc<Counter>,
    pub operations_duration: Arc<Histogram>,
    pub active_connections: Arc<Gauge>,
    pub cache_hits: Arc<Counter>,
    pub cache_misses: Arc<Counter>,
    pub memory_usage: Arc<Gauge>,
    pub disk_usage: Arc<Gauge>,
    pub error_total: Arc<Counter>,
    pub query_duration: Arc<Histogram>,
    pub index_operations: Arc<Counter>,
    pub compaction_duration: Arc<Histogram>,
    pub replication_lag: Arc<Gauge>,
}

impl LightningDbMetrics {
    pub async fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self {
            operations_total: registry.register_counter(
                "lightningdb_operations_total".to_string(),
                "Total number of database operations".to_string(),
                vec!["operation".to_string()]
            ).await,
            
            operations_duration: registry.register_histogram(
                "lightningdb_operation_duration_seconds".to_string(),
                "Database operation duration in seconds".to_string(),
                vec!["operation".to_string()],
                vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            ).await,
            
            active_connections: registry.register_gauge(
                "lightningdb_active_connections".to_string(),
                "Number of active database connections".to_string(),
                vec![]
            ).await,
            
            cache_hits: registry.register_counter(
                "lightningdb_cache_hits_total".to_string(),
                "Total number of cache hits".to_string(),
                vec![]
            ).await,
            
            cache_misses: registry.register_counter(
                "lightningdb_cache_misses_total".to_string(),
                "Total number of cache misses".to_string(),
                vec![]
            ).await,
            
            memory_usage: registry.register_gauge(
                "lightningdb_memory_usage_bytes".to_string(),
                "Current memory usage in bytes".to_string(),
                vec![]
            ).await,
            
            disk_usage: registry.register_gauge(
                "lightningdb_disk_usage_bytes".to_string(),
                "Current disk usage in bytes".to_string(),
                vec![]
            ).await,
            
            error_total: registry.register_counter(
                "lightningdb_errors_total".to_string(),
                "Total number of errors".to_string(),
                vec!["error_type".to_string()]
            ).await,
            
            query_duration: registry.register_histogram(
                "lightningdb_query_duration_seconds".to_string(),
                "Query execution duration in seconds".to_string(),
                vec!["query_type".to_string()],
                vec![0.001, 0.01, 0.1, 1.0, 10.0]
            ).await,
            
            index_operations: registry.register_counter(
                "lightningdb_index_operations_total".to_string(),
                "Total number of index operations".to_string(),
                vec!["index_type".to_string(), "operation".to_string()]
            ).await,
            
            compaction_duration: registry.register_histogram(
                "lightningdb_compaction_duration_seconds".to_string(),
                "Compaction duration in seconds".to_string(),
                vec![],
                vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
            ).await,
            
            replication_lag: registry.register_gauge(
                "lightningdb_replication_lag_seconds".to_string(),
                "Replication lag in seconds".to_string(),
                vec!["replica".to_string()]
            ).await,
        }
    }
}
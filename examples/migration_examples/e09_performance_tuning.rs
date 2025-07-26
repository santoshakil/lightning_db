//! Example 9: Performance Tuning Migration
//!
//! This example demonstrates database performance optimization techniques:
//! - Query optimization and execution plan analysis
//! - Cache configuration and warming strategies
//! - Connection pooling optimization
//! - Batch processing improvements
//! - Resource utilization monitoring
//! - Performance regression detection

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        PerformanceConfig, CacheStrategy, QueryOptimization,
    },
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use super::{MigrationExampleRunner, validation};

/// Performance tuning migration
pub struct PerformanceTuningMigration {
    baseline_metrics: Arc<RwLock<PerformanceBaseline>>,
    optimization_engine: Arc<OptimizationEngine>,
}

impl PerformanceTuningMigration {
    pub fn new() -> Self {
        Self {
            baseline_metrics: Arc::new(RwLock::new(PerformanceBaseline::default())),
            optimization_engine: Arc::new(OptimizationEngine::new()),
        }
    }
}

impl Migration for PerformanceTuningMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 8)
    }

    fn description(&self) -> &str {
        "Performance optimization and tuning for production workloads"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Step 1: Capture current performance baseline
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    capture_performance_baseline(database)
                }),
                rollback: Box::new(|_database| Ok(())),
                description: "Capture current performance metrics".to_string(),
            },
            
            // Step 2: Optimize cache configuration
            MigrationStep::UpdateConfiguration {
                config_key: "cache_strategy".to_string(),
                new_value: serde_json::json!({
                    "type": "adaptive_replacement_cache",
                    "size_mb": 2048,
                    "segments": 16,
                    "ttl_seconds": 3600,
                    "warming_enabled": true,
                    "prefetch_enabled": true,
                    "compression": "lz4"
                }),
                old_value: None,
            },
            
            // Step 3: Create materialized views for common queries
            MigrationStep::CreateMaterializedView {
                name: "product_stats_mv".to_string(),
                definition: r#"
                    SELECT 
                        p.category_id,
                        COUNT(*) as product_count,
                        AVG(CAST(p.price AS REAL)) as avg_price,
                        MIN(CAST(p.price AS REAL)) as min_price,
                        MAX(CAST(p.price AS REAL)) as max_price,
                        SUM(p.inventory_count) as total_inventory
                    FROM products p
                    WHERE p.active = true
                    GROUP BY p.category_id
                "#.to_string(),
                refresh_interval_seconds: Some(300), // Refresh every 5 minutes
            },
            
            MigrationStep::CreateMaterializedView {
                name: "user_order_summary_mv".to_string(),
                definition: r#"
                    SELECT 
                        o.user_id,
                        COUNT(*) as order_count,
                        SUM(CAST(o.total_amount AS REAL)) as total_spent,
                        MAX(o.created_at) as last_order_date,
                        COUNT(DISTINCT DATE(o.created_at)) as order_days
                    FROM orders o
                    WHERE o.status IN ('completed', 'delivered')
                    GROUP BY o.user_id
                "#.to_string(),
                refresh_interval_seconds: Some(900), // Refresh every 15 minutes
            },
            
            // Step 4: Optimize connection pooling
            MigrationStep::UpdateConfiguration {
                config_key: "connection_pool".to_string(),
                new_value: serde_json::json!({
                    "min_connections": 10,
                    "max_connections": 100,
                    "connection_timeout_ms": 5000,
                    "idle_timeout_seconds": 300,
                    "max_lifetime_seconds": 3600,
                    "queue_strategy": "fair",
                    "health_check_interval_seconds": 30
                }),
                old_value: None,
            },
            
            // Step 5: Enable query optimization features
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    enable_query_optimizations(database)
                }),
                rollback: Box::new(|database| {
                    disable_query_optimizations(database)
                }),
                description: "Enable advanced query optimizations".to_string(),
            },
            
            // Step 6: Configure batch processing
            MigrationStep::UpdateConfiguration {
                config_key: "batch_processing".to_string(),
                new_value: serde_json::json!({
                    "enabled": true,
                    "batch_size": 1000,
                    "parallel_workers": 4,
                    "queue_size": 10000,
                    "flush_interval_ms": 100,
                    "compression_threshold": 1024
                }),
                old_value: None,
            },
            
            // Step 7: Set up performance monitoring
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    setup_performance_monitoring(database)
                }),
                rollback: Box::new(|database| {
                    remove_performance_monitoring(database)
                }),
                description: "Configure performance monitoring and alerting".to_string(),
            },
            
            // Step 8: Warm up caches
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    warm_up_caches(database)
                }),
                rollback: Box::new(|_database| Ok(())),
                description: "Pre-warm caches with frequently accessed data".to_string(),
            },
            
            // Step 9: Validate performance improvements
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    validate_performance_improvements(database)
                }),
                rollback: Box::new(|_database| Ok(())),
                description: "Validate performance improvements against baseline".to_string(),
            },
        ]
    }

    fn target_schema(&self) -> Schema {
        let mut schema = Schema {
            version: self.version(),
            tables: BTreeMap::new(),
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            metadata: BTreeMap::new(),
        };

        // Add performance metadata
        schema.metadata.insert("performance_optimized".to_string(), "true".to_string());
        schema.metadata.insert("cache_strategy".to_string(), "arc_lz4".to_string());
        schema.metadata.insert("query_optimizer".to_string(), "cost_based".to_string());
        schema.metadata.insert("batch_processing".to_string(), "enabled".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating performance tuning preconditions...");
        
        // Check current system resources
        let resources = check_system_resources()?;
        if resources.available_memory_mb < 4096 {
            println!("⚠️  Warning: Limited memory available ({} MB)", resources.available_memory_mb);
        }
        
        // Check data volume
        let total_records = count_total_records(database)?;
        println!("📊 Total records in database: {}", total_records);
        
        if total_records < 10000 {
            println!("⚠️  Warning: Limited data for meaningful performance testing");
        }
        
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating performance improvements...");
        
        // Check that configurations were applied
        validation::validate_configuration_exists(database, "cache_strategy")?;
        validation::validate_configuration_exists(database, "connection_pool")?;
        validation::validate_configuration_exists(database, "batch_processing")?;
        
        // Verify materialized views
        validation::validate_view_exists(database, "product_stats_mv")?;
        validation::validate_view_exists(database, "user_order_summary_mv")?;
        
        // Compare performance metrics
        let baseline = self.baseline_metrics.read();
        let current = capture_current_metrics(database)?;
        
        let improvement = calculate_improvement(&baseline, &current);
        println!("🚀 Performance improvement: {:.1}%", improvement * 100.0);
        
        if improvement < -0.05 {
            return Err(lightning_db::Error::Validation(
                format!("Performance regression detected: {:.1}% degradation", improvement.abs() * 100.0)
            ));
        }
        
        println!("✅ Performance tuning completed successfully");
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct PerformanceBaseline {
    read_latency_us: f64,
    write_latency_us: f64,
    scan_throughput_mbs: f64,
    cache_hit_rate: f64,
    memory_usage_mb: u64,
    cpu_usage_percent: f64,
}

/// Optimization engine for query and resource optimization
struct OptimizationEngine {
    query_cache: RwLock<BTreeMap<String, QueryPlan>>,
    statistics: RwLock<DatabaseStatistics>,
}

impl OptimizationEngine {
    fn new() -> Self {
        Self {
            query_cache: RwLock::new(BTreeMap::new()),
            statistics: RwLock::new(DatabaseStatistics::default()),
        }
    }
    
    fn optimize_query(&self, query: &str) -> QueryPlan {
        // Check cache first
        if let Some(plan) = self.query_cache.read().get(query) {
            return plan.clone();
        }
        
        // Generate optimized query plan
        let plan = self.generate_query_plan(query);
        
        // Cache the plan
        self.query_cache.write().insert(query.to_string(), plan.clone());
        
        plan
    }
    
    fn generate_query_plan(&self, query: &str) -> QueryPlan {
        let stats = self.statistics.read();
        
        // Simulate query optimization based on statistics
        QueryPlan {
            query: query.to_string(),
            estimated_cost: 100.0,
            estimated_rows: 1000,
            access_method: if query.contains("WHERE") {
                "index_scan".to_string()
            } else {
                "sequential_scan".to_string()
            },
            join_order: Vec::new(),
            parallelism: if stats.table_sizes.values().any(|&size| size > 100000) {
                4
            } else {
                1
            },
        }
    }
    
    fn update_statistics(&self, database: &Database) -> Result<()> {
        let mut stats = self.statistics.write();
        
        // Count records in each table
        let tables = vec!["products", "orders", "users", "order_items"];
        for table in tables {
            let count = validation::count_records_with_prefix(database, &format!("{}_", table))?;
            stats.table_sizes.insert(table.to_string(), count);
        }
        
        // Analyze data distribution
        stats.selectivity_estimates.insert("products.active".to_string(), 0.9);
        stats.selectivity_estimates.insert("orders.status".to_string(), 0.25);
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct QueryPlan {
    query: String,
    estimated_cost: f64,
    estimated_rows: usize,
    access_method: String,
    join_order: Vec<String>,
    parallelism: usize,
}

#[derive(Debug, Default)]
struct DatabaseStatistics {
    table_sizes: BTreeMap<String, usize>,
    selectivity_estimates: BTreeMap<String, f64>,
    index_statistics: BTreeMap<String, IndexStats>,
}

#[derive(Debug)]
struct IndexStats {
    size_mb: f64,
    depth: usize,
    leaf_pages: usize,
    clustering_factor: f64,
}

#[derive(Debug)]
struct SystemResources {
    available_memory_mb: u64,
    cpu_cores: usize,
    disk_iops: u32,
    network_bandwidth_mbps: u32,
}

/// Capture performance baseline
fn capture_performance_baseline(database: &Database) -> Result<()> {
    println!("📊 Capturing performance baseline...");
    
    let mut baseline = PerformanceBaseline::default();
    
    // Measure read latency
    let start = Instant::now();
    for i in 1..=1000 {
        let key = format!("products_{:03}", i % 100 + 1);
        database.get(key.as_bytes())?;
    }
    baseline.read_latency_us = start.elapsed().as_micros() as f64 / 1000.0;
    
    // Measure write latency
    let start = Instant::now();
    for i in 1..=100 {
        let key = format!("perf_test_{}", i);
        let value = format!("test_value_{}", i);
        database.put(key.as_bytes(), value.as_bytes())?;
    }
    baseline.write_latency_us = start.elapsed().as_micros() as f64 / 100.0;
    
    // Measure scan throughput
    let start = Instant::now();
    let scan = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    let mut bytes_scanned = 0;
    for item in scan {
        let (_key, value) = item?;
        bytes_scanned += value.len();
    }
    let duration_s = start.elapsed().as_secs_f64();
    baseline.scan_throughput_mbs = (bytes_scanned as f64 / 1_048_576.0) / duration_s;
    
    // Simulate cache hit rate
    baseline.cache_hit_rate = 0.75;
    baseline.memory_usage_mb = 512;
    baseline.cpu_usage_percent = 45.0;
    
    // Store baseline
    database.put(
        b"_performance_baseline",
        serde_json::to_string(&baseline)?.as_bytes(),
    )?;
    
    println!("✅ Baseline captured: read={:.1}μs, write={:.1}μs, scan={:.1}MB/s",
             baseline.read_latency_us, baseline.write_latency_us, baseline.scan_throughput_mbs);
    
    Ok(())
}

/// Enable query optimizations
fn enable_query_optimizations(database: &Database) -> Result<()> {
    println!("⚡ Enabling query optimizations...");
    
    let optimizations = serde_json::json!({
        "enabled": true,
        "features": {
            "predicate_pushdown": true,
            "join_reordering": true,
            "index_selection": "cost_based",
            "parallel_execution": true,
            "result_caching": true,
            "adaptive_execution": true
        },
        "thresholds": {
            "parallel_scan_rows": 10000,
            "index_scan_selectivity": 0.1,
            "join_reorder_tables": 4
        }
    });
    
    database.put(
        b"_query_optimizations",
        optimizations.to_string().as_bytes(),
    )?;
    
    Ok(())
}

/// Disable query optimizations
fn disable_query_optimizations(database: &Database) -> Result<()> {
    database.delete(b"_query_optimizations")?;
    Ok(())
}

/// Set up performance monitoring
fn setup_performance_monitoring(database: &Database) -> Result<()> {
    println!("📈 Setting up performance monitoring...");
    
    let monitoring_config = serde_json::json!({
        "enabled": true,
        "metrics": {
            "operation_latencies": {
                "enabled": true,
                "histogram_buckets": [10, 50, 100, 500, 1000, 5000, 10000],
                "percentiles": [0.5, 0.9, 0.95, 0.99]
            },
            "cache_statistics": {
                "enabled": true,
                "interval_seconds": 60
            },
            "resource_usage": {
                "enabled": true,
                "cpu_sampling_interval_ms": 1000,
                "memory_sampling_interval_ms": 5000
            },
            "slow_query_log": {
                "enabled": true,
                "threshold_ms": 100
            }
        },
        "alerts": [
            {
                "name": "high_latency",
                "condition": "p99_latency_ms > 100",
                "severity": "warning"
            },
            {
                "name": "low_cache_hit_rate",
                "condition": "cache_hit_rate < 0.8",
                "severity": "info"
            },
            {
                "name": "high_memory_usage",
                "condition": "memory_usage_percent > 90",
                "severity": "critical"
            }
        ]
    });
    
    database.put(
        b"_performance_monitoring",
        monitoring_config.to_string().as_bytes(),
    )?;
    
    Ok(())
}

/// Remove performance monitoring
fn remove_performance_monitoring(database: &Database) -> Result<()> {
    database.delete(b"_performance_monitoring")?;
    Ok(())
}

/// Warm up caches with frequently accessed data
fn warm_up_caches(database: &Database) -> Result<()> {
    println!("🔥 Warming up caches...");
    
    let start = Instant::now();
    let mut warmed_count = 0;
    
    // Warm up product data (most frequently accessed)
    let product_scan = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    
    for item in product_scan.take(1000) {
        let (key, _value) = item?;
        // Access triggers cache loading
        database.get(&key)?;
        warmed_count += 1;
    }
    
    // Warm up recent orders
    let order_scan = database.scan(
        Some(b"orders_".to_vec()),
        Some(b"orders~".to_vec()),
    )?;
    
    for item in order_scan.take(500) {
        let (key, _value) = item?;
        database.get(&key)?;
        warmed_count += 1;
    }
    
    // Warm up user data
    let user_scan = database.scan(
        Some(b"users_".to_vec()),
        Some(b"users~".to_vec()),
    )?;
    
    for item in user_scan.take(200) {
        let (key, _value) = item?;
        database.get(&key)?;
        warmed_count += 1;
    }
    
    let duration = start.elapsed();
    println!("✅ Warmed {} cache entries in {:.2}ms", warmed_count, duration.as_millis());
    
    Ok(())
}

/// Validate performance improvements
fn validate_performance_improvements(database: &Database) -> Result<()> {
    println!("🏁 Validating performance improvements...");
    
    // Load baseline
    let baseline_data = database.get(b"_performance_baseline")?;
    let baseline: PerformanceBaseline = serde_json::from_slice(&baseline_data)?;
    
    // Capture current metrics
    let current = capture_current_metrics(database)?;
    
    // Calculate improvements
    let read_improvement = (baseline.read_latency_us - current.read_latency_us) / baseline.read_latency_us;
    let write_improvement = (baseline.write_latency_us - current.write_latency_us) / baseline.write_latency_us;
    let scan_improvement = (current.scan_throughput_mbs - baseline.scan_throughput_mbs) / baseline.scan_throughput_mbs;
    let cache_improvement = current.cache_hit_rate - baseline.cache_hit_rate;
    
    println!("📊 Performance Improvements:");
    println!("  Read latency: {:.1}% faster", read_improvement * 100.0);
    println!("  Write latency: {:.1}% faster", write_improvement * 100.0);
    println!("  Scan throughput: {:.1}% higher", scan_improvement * 100.0);
    println!("  Cache hit rate: +{:.1}%", cache_improvement * 100.0);
    
    let overall_improvement = (read_improvement + write_improvement + scan_improvement + cache_improvement) / 4.0;
    
    if overall_improvement < 0.1 {
        println!("⚠️  Warning: Limited performance improvement ({:.1}%)", overall_improvement * 100.0);
    } else {
        println!("✅ Overall performance improved by {:.1}%", overall_improvement * 100.0);
    }
    
    Ok(())
}

/// Check system resources
fn check_system_resources() -> Result<SystemResources> {
    // Simulated resource check
    Ok(SystemResources {
        available_memory_mb: 8192,
        cpu_cores: 8,
        disk_iops: 50000,
        network_bandwidth_mbps: 1000,
    })
}

/// Count total records in database
fn count_total_records(database: &Database) -> Result<usize> {
    let mut total = 0;
    let prefixes = vec!["products_", "orders_", "users_", "order_items_"];
    
    for prefix in prefixes {
        total += validation::count_records_with_prefix(database, prefix)?;
    }
    
    Ok(total)
}

/// Capture current performance metrics
fn capture_current_metrics(database: &Database) -> Result<PerformanceBaseline> {
    let mut metrics = PerformanceBaseline::default();
    
    // Measure optimized read latency
    let start = Instant::now();
    for i in 1..=1000 {
        let key = format!("products_{:03}", i % 100 + 1);
        database.get(key.as_bytes())?;
    }
    metrics.read_latency_us = start.elapsed().as_micros() as f64 / 1000.0;
    
    // Measure optimized write latency  
    let start = Instant::now();
    for i in 1..=100 {
        let key = format!("perf_test_opt_{}", i);
        let value = format!("optimized_value_{}", i);
        database.put(key.as_bytes(), value.as_bytes())?;
    }
    metrics.write_latency_us = start.elapsed().as_micros() as f64 / 100.0;
    
    // Measure optimized scan
    let start = Instant::now();
    let scan = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    let mut bytes_scanned = 0;
    for item in scan {
        let (_key, value) = item?;
        bytes_scanned += value.len();
    }
    let duration_s = start.elapsed().as_secs_f64();
    metrics.scan_throughput_mbs = (bytes_scanned as f64 / 1_048_576.0) / duration_s;
    
    // Simulated improved metrics
    metrics.cache_hit_rate = 0.92; // Improved from 0.75
    metrics.memory_usage_mb = 768; // Slightly higher but more efficient
    metrics.cpu_usage_percent = 38.0; // Lower CPU usage
    
    Ok(metrics)
}

/// Calculate overall improvement
fn calculate_improvement(baseline: &PerformanceBaseline, current: &PerformanceBaseline) -> f64 {
    let latency_improvement = (baseline.read_latency_us + baseline.write_latency_us - 
                              current.read_latency_us - current.write_latency_us) / 
                              (baseline.read_latency_us + baseline.write_latency_us);
    
    let throughput_improvement = (current.scan_throughput_mbs - baseline.scan_throughput_mbs) / 
                                baseline.scan_throughput_mbs;
    
    let cache_improvement = current.cache_hit_rate - baseline.cache_hit_rate;
    
    let cpu_improvement = (baseline.cpu_usage_percent - current.cpu_usage_percent) / 
                         baseline.cpu_usage_percent;
    
    // Weighted average of improvements
    (latency_improvement * 0.4 + throughput_improvement * 0.3 + 
     cache_improvement * 0.2 + cpu_improvement * 0.1)
}

/// Performance analyzer for detailed analysis
pub struct PerformanceAnalyzer<'a> {
    database: &'a Database,
    optimization_engine: Arc<OptimizationEngine>,
}

impl<'a> PerformanceAnalyzer<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self {
            database,
            optimization_engine: Arc::new(OptimizationEngine::new()),
        }
    }
    
    /// Analyze query performance
    pub fn analyze_query_performance(&self, queries: Vec<&str>) -> Result<QueryAnalysisReport> {
        println!("🔍 Analyzing query performance...");
        
        // Update statistics first
        self.optimization_engine.update_statistics(self.database)?;
        
        let mut results = Vec::new();
        
        for query in queries {
            let plan = self.optimization_engine.optimize_query(query);
            
            // Execute query and measure
            let start = Instant::now();
            let rows = self.execute_simulated_query(query)?;
            let duration = start.elapsed();
            
            results.push(QueryAnalysisResult {
                query: query.to_string(),
                plan: plan.clone(),
                actual_rows: rows,
                execution_time: duration,
                efficiency: if plan.estimated_rows > 0 {
                    rows as f64 / plan.estimated_rows as f64
                } else {
                    1.0
                },
            });
        }
        
        Ok(QueryAnalysisReport {
            total_queries: results.len(),
            results,
            recommendations: self.generate_recommendations(),
        })
    }
    
    /// Analyze cache effectiveness
    pub fn analyze_cache_performance(&self) -> Result<CacheAnalysisReport> {
        println!("📊 Analyzing cache performance...");
        
        // Simulate cache analysis
        Ok(CacheAnalysisReport {
            total_size_mb: 2048,
            used_size_mb: 1536,
            hit_rate: 0.92,
            miss_rate: 0.08,
            eviction_rate: 0.02,
            hot_keys: vec![
                "products_001".to_string(),
                "products_002".to_string(),
                "orders_latest".to_string(),
            ],
            cold_keys: vec![
                "orders_old_2022".to_string(),
                "users_inactive".to_string(),
            ],
            recommendations: vec![
                "Consider increasing cache size for better hit rate".to_string(),
                "Enable cache warming for cold start scenarios".to_string(),
            ],
        })
    }
    
    fn execute_simulated_query(&self, query: &str) -> Result<usize> {
        // Simulate query execution
        if query.contains("products") {
            Ok(validation::count_records_with_prefix(self.database, "products_")?)
        } else if query.contains("orders") {
            Ok(validation::count_records_with_prefix(self.database, "orders_")?)
        } else {
            Ok(100) // Default
        }
    }
    
    fn generate_recommendations(&self) -> Vec<String> {
        vec![
            "Create composite index on (user_id, created_at) for order queries".to_string(),
            "Consider partitioning orders table by date for better performance".to_string(),
            "Enable parallel query execution for large scans".to_string(),
        ]
    }
}

#[derive(Debug)]
pub struct QueryAnalysisReport {
    pub total_queries: usize,
    pub results: Vec<QueryAnalysisResult>,
    pub recommendations: Vec<String>,
}

#[derive(Debug)]
pub struct QueryAnalysisResult {
    pub query: String,
    pub plan: QueryPlan,
    pub actual_rows: usize,
    pub execution_time: Duration,
    pub efficiency: f64,
}

#[derive(Debug)]
pub struct CacheAnalysisReport {
    pub total_size_mb: u64,
    pub used_size_mb: u64,
    pub hit_rate: f64,
    pub miss_rate: f64,
    pub eviction_rate: f64,
    pub hot_keys: Vec<String>,
    pub cold_keys: Vec<String>,
    pub recommendations: Vec<String>,
}

impl QueryAnalysisReport {
    pub fn print_report(&self) {
        println!("\n📈 Query Performance Analysis:");
        println!("=".repeat(60));
        
        for result in &self.results {
            println!("\n🔍 Query: {}", result.query);
            println!("  Plan: {} (parallelism: {})", result.plan.access_method, result.plan.parallelism);
            println!("  Estimated cost: {:.2}, Estimated rows: {}", 
                    result.plan.estimated_cost, result.plan.estimated_rows);
            println!("  Actual rows: {}, Execution time: {:.2}ms", 
                    result.actual_rows, result.execution_time.as_millis());
            println!("  Efficiency: {:.1}%", result.efficiency * 100.0);
        }
        
        if !self.recommendations.is_empty() {
            println!("\n💡 Recommendations:");
            for rec in &self.recommendations {
                println!("  • {}", rec);
            }
        }
    }
}

impl CacheAnalysisReport {
    pub fn print_report(&self) {
        println!("\n📋 Cache Performance Analysis:");
        println!("=".repeat(50));
        println!("📦 Total size: {} MB", self.total_size_mb);
        println!("📦 Used size: {} MB ({:.1}%)", 
                self.used_size_mb, 
                (self.used_size_mb as f64 / self.total_size_mb as f64) * 100.0);
        println!("🎯 Hit rate: {:.1}%", self.hit_rate * 100.0);
        println!("❌ Miss rate: {:.1}%", self.miss_rate * 100.0);
        println!("🔄 Eviction rate: {:.1}%", self.eviction_rate * 100.0);
        
        if !self.hot_keys.is_empty() {
            println!("\n🔥 Hot keys:");
            for key in &self.hot_keys {
                println!("  • {}", key);
            }
        }
        
        if !self.recommendations.is_empty() {
            println!("\n💡 Recommendations:");
            for rec in &self.recommendations {
                println!("  • {}", rec);
            }
        }
    }
}

/// Run the performance tuning example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 9: Performance Tuning Migration");
    println!("=========================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e09_performance_tuning.db")?;
    
    // Create test data
    println!("📝 Creating performance test dataset...");
    create_performance_test_data(&runner)?;
    
    // Run the performance tuning migration
    let migration = PerformanceTuningMigration::new();
    runner.run_migration_with_validation(
        Box::new(migration),
        Box::new(|db| validate_performance_tuning(db)),
    )?;
    
    // Analyze performance improvements
    let analyzer = PerformanceAnalyzer::new(&runner.database);
    
    // Query analysis
    let queries = vec![
        "SELECT * FROM products WHERE category_id = 5 AND active = true",
        "SELECT * FROM orders WHERE user_id = 123 ORDER BY created_at DESC",
        "SELECT COUNT(*) FROM order_items GROUP BY product_id",
    ];
    
    let query_report = analyzer.analyze_query_performance(queries)?;
    query_report.print_report();
    
    // Cache analysis
    let cache_report = analyzer.analyze_cache_performance()?;
    cache_report.print_report();
    
    // Show migration history
    runner.show_history(Some(5))?;
    
    println!("\n🎉 Example 9 completed successfully!");
    println!("Next: Run example 10 to see production rollback strategies");
    
    Ok(())
}

fn create_performance_test_data(runner: &MigrationExampleRunner) -> Result<()> {
    // Create a larger dataset for meaningful performance testing
    let mut created = 0;
    
    // Create 10,000 products
    for i in 1..=10000 {
        let key = format!("products_{:05}", i);
        let value = serde_json::json!({
            "id": i,
            "name": format!("Product {}", i),
            "description": format!("Description for product {} with searchable text", i),
            "category_id": (i % 20) + 1,
            "price": format!("{:.2}", 10.0 + (i as f64 * 0.5)),
            "inventory_count": 100 + (i % 500),
            "active": i % 10 != 0,
            "created_at": chrono::Utc::now().timestamp() - (i * 60),
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
        created += 1;
        
        if created % 1000 == 0 {
            println!("  Created {} products...", created);
        }
    }
    
    // Create 5,000 orders
    for i in 1..=5000 {
        let key = format!("orders_{:05}", i);
        let value = serde_json::json!({
            "id": i,
            "user_id": (i % 1000) + 1,
            "order_number": format!("ORD-{:08}", i),
            "status": ["pending", "processing", "completed", "delivered"][i % 4],
            "total_amount": format!("{:.2}", 50.0 + (i as f64 * 2.5)),
            "created_at": chrono::Utc::now().timestamp() - (i * 300),
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created 10,000 products and 5,000 orders for performance testing");
    Ok(())
}

fn validate_performance_tuning(database: &Database) -> Result<()> {
    // Verify performance configurations are in place
    validation::validate_configuration_exists(database, "cache_strategy")?;
    validation::validate_configuration_exists(database, "connection_pool")?;
    
    println!("✅ Performance tuning validation successful");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_performance_tuning_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = PerformanceTuningMigration::new();
        
        // Test migration metadata
        assert_eq!(migration.version(), SchemaVersion::new(1, 8));
        
        // Test that steps include all optimizations
        let steps = migration.steps();
        assert!(steps.len() >= 9); // At least 9 optimization steps
    }

    #[test]
    fn test_optimization_engine() {
        let engine = OptimizationEngine::new();
        
        // Test query optimization
        let plan1 = engine.optimize_query("SELECT * FROM products WHERE id = 123");
        assert_eq!(plan1.access_method, "index_scan");
        
        let plan2 = engine.optimize_query("SELECT * FROM products");
        assert_eq!(plan2.access_method, "sequential_scan");
        
        // Test caching
        let plan1_cached = engine.optimize_query("SELECT * FROM products WHERE id = 123");
        assert_eq!(plan1.query, plan1_cached.query);
    }

    #[test]
    fn test_performance_metrics() {
        let baseline = PerformanceBaseline {
            read_latency_us: 100.0,
            write_latency_us: 200.0,
            scan_throughput_mbs: 50.0,
            cache_hit_rate: 0.75,
            memory_usage_mb: 512,
            cpu_usage_percent: 45.0,
        };
        
        let current = PerformanceBaseline {
            read_latency_us: 80.0,
            write_latency_us: 150.0,
            scan_throughput_mbs: 75.0,
            cache_hit_rate: 0.90,
            memory_usage_mb: 768,
            cpu_usage_percent: 35.0,
        };
        
        let improvement = calculate_improvement(&baseline, &current);
        assert!(improvement > 0.0); // Should show improvement
    }
}
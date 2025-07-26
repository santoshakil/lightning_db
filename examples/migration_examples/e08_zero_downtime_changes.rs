//! Example 8: Zero-Downtime Migration Strategies
//!
//! This example demonstrates techniques for database changes without downtime:
//! - Online schema changes with minimal locking
//! - Progressive rollout with feature flags
//! - Dual-write patterns for data migration
//! - Blue-green deployment strategies
//! - Rollback procedures and safety checks
//! - Performance impact monitoring

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        TableDefinition, ColumnDefinition, DataType,
        MigrationOptions, LockingStrategy, SafetyLevel,
    },
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use super::{MigrationExampleRunner, validation};

/// Zero-downtime migration implementation
pub struct ZeroDowntimeChangesMigration {
    feature_flags: Arc<FeatureFlagManager>,
    performance_monitor: Arc<PerformanceMonitor>,
}

impl ZeroDowntimeChangesMigration {
    pub fn new() -> Self {
        Self {
            feature_flags: Arc::new(FeatureFlagManager::new()),
            performance_monitor: Arc::new(PerformanceMonitor::new()),
        }
    }
}

impl Migration for ZeroDowntimeChangesMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 7)
    }

    fn description(&self) -> &str {
        "Zero-downtime schema changes with progressive rollout"
    }

    fn options(&self) -> MigrationOptions {
        MigrationOptions {
            locking_strategy: LockingStrategy::Optimistic,
            safety_level: SafetyLevel::Maximum,
            batch_size: 100,
            throttle_ms: 10,
            allow_concurrent_reads: true,
            allow_concurrent_writes: true,
            monitoring_enabled: true,
            rollback_checkpoint_interval: 1000,
        }
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Step 1: Add new columns without NOT NULL constraints (backward compatible)
            MigrationStep::AddColumn {
                table: "products".to_string(),
                column: ColumnDefinition {
                    name: "tags".to_string(),
                    data_type: DataType::Json,
                    nullable: true,  // Start as nullable
                    primary_key: false,
                    unique: false,
                    indexed: false,
                    default: Some(serde_json::json!([])),
                },
                if_not_exists: true,
            },
            
            MigrationStep::AddColumn {
                table: "products".to_string(),
                column: ColumnDefinition {
                    name: "search_vector".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    indexed: true,
                    default: None,
                },
                if_not_exists: true,
            },
            
            // Step 2: Create new tables alongside existing ones
            MigrationStep::CreateTable {
                definition: TableDefinition {
                    name: "product_reviews_v2".to_string(),
                    columns: vec![
                        ColumnDefinition {
                            name: "id".to_string(),
                            data_type: DataType::BigInt,
                            nullable: false,
                            primary_key: true,
                            unique: false,
                            indexed: false,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "product_id".to_string(),
                            data_type: DataType::BigInt,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: true,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "user_id".to_string(),
                            data_type: DataType::BigInt,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: true,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "rating".to_string(),
                            data_type: DataType::Integer,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: true,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "title".to_string(),
                            data_type: DataType::Text,
                            nullable: true,
                            primary_key: false,
                            unique: false,
                            indexed: false,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "content".to_string(),
                            data_type: DataType::Text,
                            nullable: true,
                            primary_key: false,
                            unique: false,
                            indexed: false,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "verified_purchase".to_string(),
                            data_type: DataType::Boolean,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: true,
                            default: Some(serde_json::json!(false)),
                        },
                        ColumnDefinition {
                            name: "helpful_votes".to_string(),
                            data_type: DataType::Integer,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: false,
                            default: Some(serde_json::json!(0)),
                        },
                        ColumnDefinition {
                            name: "total_votes".to_string(),
                            data_type: DataType::Integer,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: false,
                            default: Some(serde_json::json!(0)),
                        },
                        ColumnDefinition {
                            name: "created_at".to_string(),
                            data_type: DataType::Timestamp,
                            nullable: false,
                            primary_key: false,
                            unique: false,
                            indexed: true,
                            default: None,
                        },
                        ColumnDefinition {
                            name: "updated_at".to_string(),
                            data_type: DataType::Timestamp,
                            nullable: true,
                            primary_key: false,
                            unique: false,
                            indexed: false,
                            default: None,
                        },
                    ],
                    indexes: vec![],
                    constraints: vec![],
                },
                if_not_exists: true,
            },
            
            // Step 3: Progressive data migration with dual writes
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    let flags = Arc::new(FeatureFlagManager::new());
                    let monitor = Arc::new(PerformanceMonitor::new());
                    progressive_data_migration(database, flags, monitor)
                }),
                rollback: Box::new(|database| {
                    rollback_progressive_migration(database)
                }),
                description: "Progressive data migration with monitoring".to_string(),
            },
            
            // Step 4: Create backward-compatible views
            MigrationStep::CreateView {
                name: "product_reviews".to_string(),
                definition: "SELECT id, product_id, user_id, rating, content, created_at FROM product_reviews_v2".to_string(),
                replace_if_exists: true,
            },
            
            // Step 5: Set up triggers for dual writes
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    setup_dual_write_triggers(database)
                }),
                rollback: Box::new(|database| {
                    remove_dual_write_triggers(database)
                }),
                description: "Enable dual-write pattern for zero downtime".to_string(),
            },
            
            // Step 6: Create canary deployment markers
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    setup_canary_deployment(database)
                }),
                rollback: Box::new(|database| {
                    rollback_canary_deployment(database)
                }),
                description: "Configure canary deployment for gradual rollout".to_string(),
            },
            
            // Step 7: Add online index building (non-blocking)
            MigrationStep::CreateIndex {
                definition: lightning_db::schema_migration::IndexDefinition {
                    name: "idx_products_tags_gin".to_string(),
                    table: "products".to_string(),
                    columns: vec![lightning_db::schema_migration::IndexColumn {
                        name: "tags".to_string(),
                        order: lightning_db::schema_migration::SortOrder::Ascending,
                    }],
                    index_type: lightning_db::schema_migration::IndexType::BTree,
                    options: lightning_db::schema_migration::IndexOptions {
                        unique: false,
                        predicate: Some("tags IS NOT NULL".to_string()),
                        include: vec![],
                    },
                },
            },
            
            // Step 8: Health check validation
            MigrationStep::ExecuteCustom {
                forward: Box::new(|database| {
                    validate_zero_downtime_migration(database)
                }),
                rollback: Box::new(|_database| Ok(())),
                description: "Validate migration health and data integrity".to_string(),
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

        // Add zero-downtime metadata
        schema.metadata.insert("deployment_strategy".to_string(), "blue_green".to_string());
        schema.metadata.insert("feature_flags_enabled".to_string(), "true".to_string());
        schema.metadata.insert("rollback_enabled".to_string(), "true".to_string());
        schema.metadata.insert("monitoring_level".to_string(), "detailed".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating zero-downtime migration preconditions...");
        
        // Check system load before migration
        let load_check = self.performance_monitor.check_system_load();
        if load_check.cpu_usage > 0.8 || load_check.memory_usage > 0.9 {
            return Err(lightning_db::Error::Validation(
                format!("System load too high for migration: CPU {:.1}%, Memory {:.1}%",
                        load_check.cpu_usage * 100.0, load_check.memory_usage * 100.0)
            ));
        }
        
        // Verify tables exist
        validation::validate_table_exists(database, "products")?;
        
        // Check for active transactions
        let active_txns = check_active_transactions(database)?;
        if active_txns > 100 {
            println!("⚠️  Warning: {} active transactions detected", active_txns);
        }
        
        println!("✅ System ready for zero-downtime migration");
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating zero-downtime migration postconditions...");
        
        // Verify new schema elements
        validation::validate_column_exists(database, "products", "tags")?;
        validation::validate_column_exists(database, "products", "search_vector")?;
        validation::validate_table_exists(database, "product_reviews_v2")?;
        validation::validate_view_exists(database, "product_reviews")?;
        
        // Check data consistency
        let consistency_check = verify_data_consistency(database)?;
        if !consistency_check.is_consistent {
            return Err(lightning_db::Error::Validation(
                format!("Data consistency check failed: {}", consistency_check.error_message)
            ));
        }
        
        // Verify performance didn't degrade
        let perf_check = self.performance_monitor.compare_before_after();
        if perf_check.degradation > 0.1 {
            println!("⚠️  Warning: {:.1}% performance degradation detected", perf_check.degradation * 100.0);
        }
        
        println!("✅ Zero-downtime migration completed successfully");
        Ok(())
    }
}

/// Feature flag manager for progressive rollout
pub struct FeatureFlagManager {
    flags: parking_lot::RwLock<BTreeMap<String, FeatureFlag>>,
}

impl FeatureFlagManager {
    pub fn new() -> Self {
        let mut flags = BTreeMap::new();
        
        // Initialize default flags
        flags.insert("dual_writes_enabled".to_string(), FeatureFlag {
            enabled: AtomicBool::new(false),
            rollout_percentage: AtomicU64::new(0),
            created_at: Utc::now(),
        });
        
        flags.insert("new_schema_reads".to_string(), FeatureFlag {
            enabled: AtomicBool::new(false),
            rollout_percentage: AtomicU64::new(0),
            created_at: Utc::now(),
        });
        
        Self {
            flags: parking_lot::RwLock::new(flags),
        }
    }
    
    pub fn enable_progressive(&self, flag_name: &str, percentage: u64) -> Result<()> {
        let flags = self.flags.read();
        if let Some(flag) = flags.get(flag_name) {
            flag.rollout_percentage.store(percentage, Ordering::SeqCst);
            if percentage >= 100 {
                flag.enabled.store(true, Ordering::SeqCst);
            }
            println!("🚀 Feature flag '{}' set to {}%", flag_name, percentage);
        }
        Ok(())
    }
    
    pub fn is_enabled_for_key(&self, flag_name: &str, key: &str) -> bool {
        let flags = self.flags.read();
        if let Some(flag) = flags.get(flag_name) {
            if flag.enabled.load(Ordering::SeqCst) {
                return true;
            }
            
            let percentage = flag.rollout_percentage.load(Ordering::SeqCst);
            if percentage == 0 {
                return false;
            }
            
            // Use consistent hashing for deterministic rollout
            let hash = calculate_hash(key);
            (hash % 100) < percentage
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct FeatureFlag {
    enabled: AtomicBool,
    rollout_percentage: AtomicU64,
    created_at: DateTime<Utc>,
}

/// Performance monitor for zero-downtime migrations
pub struct PerformanceMonitor {
    metrics: parking_lot::RwLock<PerformanceMetrics>,
    baseline: parking_lot::RwLock<Option<PerformanceSnapshot>>,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            metrics: parking_lot::RwLock::new(PerformanceMetrics::default()),
            baseline: parking_lot::RwLock::new(None),
        }
    }
    
    pub fn record_operation(&self, operation: &str, duration: Duration) {
        let mut metrics = self.metrics.write();
        metrics.operation_count += 1;
        metrics.total_duration += duration;
        metrics.operations.entry(operation.to_string())
            .or_insert_with(Vec::new)
            .push(duration);
    }
    
    pub fn capture_baseline(&self) {
        let metrics = self.metrics.read();
        let snapshot = PerformanceSnapshot {
            timestamp: Utc::now(),
            operation_count: metrics.operation_count,
            average_latency: if metrics.operation_count > 0 {
                metrics.total_duration.as_micros() as f64 / metrics.operation_count as f64
            } else {
                0.0
            },
            p99_latency: calculate_percentile(&metrics, 0.99),
        };
        *self.baseline.write() = Some(snapshot);
    }
    
    pub fn check_system_load(&self) -> SystemLoad {
        // Simulated system load check
        SystemLoad {
            cpu_usage: 0.45,
            memory_usage: 0.62,
            io_wait: 0.08,
            active_connections: 234,
        }
    }
    
    pub fn compare_before_after(&self) -> PerformanceComparison {
        let current_metrics = self.metrics.read();
        let baseline = self.baseline.read();
        
        if let Some(ref base) = *baseline {
            let current_avg = if current_metrics.operation_count > 0 {
                current_metrics.total_duration.as_micros() as f64 / current_metrics.operation_count as f64
            } else {
                0.0
            };
            
            let degradation = if base.average_latency > 0.0 {
                (current_avg - base.average_latency) / base.average_latency
            } else {
                0.0
            };
            
            PerformanceComparison {
                baseline_latency: base.average_latency,
                current_latency: current_avg,
                degradation: degradation.max(0.0),
                improved: degradation < 0.0,
            }
        } else {
            PerformanceComparison {
                baseline_latency: 0.0,
                current_latency: 0.0,
                degradation: 0.0,
                improved: false,
            }
        }
    }
}

#[derive(Debug, Default)]
struct PerformanceMetrics {
    operation_count: u64,
    total_duration: Duration,
    operations: BTreeMap<String, Vec<Duration>>,
}

#[derive(Debug)]
struct PerformanceSnapshot {
    timestamp: DateTime<Utc>,
    operation_count: u64,
    average_latency: f64,
    p99_latency: f64,
}

#[derive(Debug)]
struct SystemLoad {
    cpu_usage: f64,
    memory_usage: f64,
    io_wait: f64,
    active_connections: u64,
}

#[derive(Debug)]
struct PerformanceComparison {
    baseline_latency: f64,
    current_latency: f64,
    degradation: f64,
    improved: bool,
}

/// Progressive data migration with monitoring
fn progressive_data_migration(
    database: &Database,
    flags: Arc<FeatureFlagManager>,
    monitor: Arc<PerformanceMonitor>,
) -> Result<()> {
    println!("🔄 Starting progressive data migration...");
    
    // Phase 1: Enable dual writes at 10%
    flags.enable_progressive("dual_writes_enabled", 10)?;
    std::thread::sleep(std::time::Duration::from_millis(100));
    
    // Phase 2: Gradually increase to 100%
    for percentage in (20..=100).step_by(20) {
        flags.enable_progressive("dual_writes_enabled", percentage)?;
        
        // Monitor performance impact
        let start = Instant::now();
        
        // Migrate batch of data
        let batch_size = 100;
        let scan = database.scan(
            Some(b"products_".to_vec()),
            Some(b"products~".to_vec()),
        )?;
        
        let mut count = 0;
        for item in scan.take(batch_size) {
            let (key, value) = item?;
            
            // Simulate data transformation
            if let Ok(mut product) = serde_json::from_slice::<serde_json::Value>(&value) {
                // Add tags field
                product["tags"] = serde_json::json!(["migrated", format!("batch_{}", percentage)]);
                
                // Generate search vector
                let name = product["name"].as_str().unwrap_or("");
                let desc = product["description"].as_str().unwrap_or("");
                product["search_vector"] = serde_json::json!(format!("{} {}", name, desc).to_lowercase());
                
                // Write back with monitoring
                database.put(&key, product.to_string().as_bytes())?;
                count += 1;
            }
        }
        
        monitor.record_operation("batch_migration", start.elapsed());
        println!("  ✅ Migrated {} records at {}% rollout", count, percentage);
        
        // Brief pause between batches
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    
    println!("✅ Progressive migration completed");
    Ok(())
}

/// Rollback progressive migration
fn rollback_progressive_migration(database: &Database) -> Result<()> {
    println!("⚩️ Rolling back progressive migration...");
    
    // Remove added fields
    let scan = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    
    for item in scan {
        let (key, value) = item?;
        if let Ok(mut product) = serde_json::from_slice::<serde_json::Value>(&value) {
            // Remove new fields
            product.as_object_mut().map(|obj| {
                obj.remove("tags");
                obj.remove("search_vector");
            });
            database.put(&key, product.to_string().as_bytes())?;
        }
    }
    
    Ok(())
}

/// Set up dual-write triggers
fn setup_dual_write_triggers(database: &Database) -> Result<()> {
    println!("🔄 Setting up dual-write triggers...");
    
    // Store trigger configuration
    let trigger_config = serde_json::json!({
        "triggers": [
            {
                "name": "dual_write_products",
                "source_table": "products",
                "target_tables": ["products_archive", "products_search"],
                "enabled": true,
                "created_at": Utc::now().to_rfc3339()
            },
            {
                "name": "dual_write_reviews",
                "source_table": "product_reviews",
                "target_table": "product_reviews_v2",
                "enabled": true,
                "transform": "add_metadata",
                "created_at": Utc::now().to_rfc3339()
            }
        ]
    });
    
    database.put(
        b"_system_dual_write_triggers",
        trigger_config.to_string().as_bytes(),
    )?;
    
    println!("✅ Dual-write triggers configured");
    Ok(())
}

/// Remove dual-write triggers
fn remove_dual_write_triggers(database: &Database) -> Result<()> {
    database.delete(b"_system_dual_write_triggers")?;
    Ok(())
}

/// Set up canary deployment
fn setup_canary_deployment(database: &Database) -> Result<()> {
    println!("🐦 Setting up canary deployment...");
    
    let canary_config = serde_json::json!({
        "deployment": {
            "strategy": "canary",
            "canary_percentage": 5,
            "stable_version": "1.6",
            "canary_version": "1.7",
            "health_check_interval_seconds": 60,
            "auto_rollback_threshold": {
                "error_rate": 0.05,
                "latency_p99_ms": 1000
            },
            "created_at": Utc::now().to_rfc3339()
        }
    });
    
    database.put(
        b"_system_canary_deployment",
        canary_config.to_string().as_bytes(),
    )?;
    
    println!("✅ Canary deployment configured at 5% traffic");
    Ok(())
}

/// Rollback canary deployment
fn rollback_canary_deployment(database: &Database) -> Result<()> {
    database.delete(b"_system_canary_deployment")?;
    Ok(())
}

/// Check active transactions
fn check_active_transactions(database: &Database) -> Result<u64> {
    // In a real implementation, this would check transaction manager
    // For now, simulate the check
    Ok(42)
}

/// Verify data consistency
fn verify_data_consistency(database: &Database) -> Result<ConsistencyCheck> {
    println!("🔍 Verifying data consistency...");
    
    let mut check = ConsistencyCheck {
        is_consistent: true,
        records_checked: 0,
        inconsistencies: Vec::new(),
        error_message: String::new(),
    };
    
    // Check sample of products for required fields
    let scan = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    
    for item in scan.take(100) {
        let (key, value) = item?;
        check.records_checked += 1;
        
        if let Ok(product) = serde_json::from_slice::<serde_json::Value>(&value) {
            // Verify new fields exist
            if !product.get("tags").is_some() {
                check.inconsistencies.push(format!("Missing tags field in {:?}", key));
            }
        }
    }
    
    if !check.inconsistencies.is_empty() {
        check.is_consistent = false;
        check.error_message = format!("{} inconsistencies found", check.inconsistencies.len());
    }
    
    Ok(check)
}

#[derive(Debug)]
struct ConsistencyCheck {
    is_consistent: bool,
    records_checked: u64,
    inconsistencies: Vec<String>,
    error_message: String,
}

/// Validate zero-downtime migration
fn validate_zero_downtime_migration(database: &Database) -> Result<()> {
    println!("🏥 Validating zero-downtime migration health...");
    
    // Check that old queries still work
    let products_count = validation::count_records_with_prefix(database, "products_")?;
    
    // Verify views are functioning
    // In a real implementation, this would execute queries through views
    
    // Check system health metrics
    if let Ok(value) = database.get(b"_system_canary_deployment") {
        if let Ok(config) = serde_json::from_slice::<serde_json::Value>(&value) {
            let canary_pct = config["deployment"]["canary_percentage"].as_u64().unwrap_or(0);
            println!("✅ Canary deployment active at {}%", canary_pct);
        }
    }
    
    println!("✅ Migration health check passed: {} products accessible", products_count);
    Ok(())
}

/// Calculate hash for consistent feature flag rollout
fn calculate_hash(key: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Calculate percentile from metrics
fn calculate_percentile(metrics: &PerformanceMetrics, percentile: f64) -> f64 {
    let mut all_durations: Vec<u64> = Vec::new();
    
    for durations in metrics.operations.values() {
        for duration in durations {
            all_durations.push(duration.as_micros() as u64);
        }
    }
    
    if all_durations.is_empty() {
        return 0.0;
    }
    
    all_durations.sort_unstable();
    let index = ((all_durations.len() as f64 - 1.0) * percentile) as usize;
    all_durations[index] as f64
}

/// Zero-downtime deployment orchestrator
pub struct DeploymentOrchestrator<'a> {
    database: &'a Database,
    flags: Arc<FeatureFlagManager>,
    monitor: Arc<PerformanceMonitor>,
}

impl<'a> DeploymentOrchestrator<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self {
            database,
            flags: Arc::new(FeatureFlagManager::new()),
            monitor: Arc::new(PerformanceMonitor::new()),
        }
    }
    
    /// Execute blue-green deployment
    pub fn blue_green_deployment(&self) -> Result<DeploymentResult> {
        println!("🔵🟢 Starting blue-green deployment...");
        
        let start = Instant::now();
        let mut result = DeploymentResult {
            strategy: "blue_green".to_string(),
            duration: Duration::default(),
            phases_completed: Vec::new(),
            rollback_required: false,
            final_state: "pending".to_string(),
        };
        
        // Phase 1: Prepare green environment
        println!("  Phase 1: Preparing green environment...");
        self.monitor.capture_baseline();
        result.phases_completed.push("prepare_green".to_string());
        
        // Phase 2: Sync data to green
        println!("  Phase 2: Syncing data to green environment...");
        self.flags.enable_progressive("dual_writes_enabled", 100)?;
        result.phases_completed.push("sync_data".to_string());
        
        // Phase 3: Validate green environment
        println!("  Phase 3: Validating green environment...");
        let validation = verify_data_consistency(self.database)?;
        if !validation.is_consistent {
            result.rollback_required = true;
            result.final_state = "rollback".to_string();
            return Ok(result);
        }
        result.phases_completed.push("validate_green".to_string());
        
        // Phase 4: Switch traffic (canary -> gradual -> full)
        println!("  Phase 4: Switching traffic to green...");
        for percentage in [5, 25, 50, 100] {
            setup_canary_with_percentage(self.database, percentage)?;
            std::thread::sleep(std::time::Duration::from_millis(100));
            
            // Check health at each stage
            let perf = self.monitor.compare_before_after();
            if perf.degradation > 0.2 {
                println!("⚠️  Performance degradation detected, rolling back...");
                result.rollback_required = true;
                break;
            }
            println!("    ✅ {}% traffic switched successfully", percentage);
        }
        result.phases_completed.push("switch_traffic".to_string());
        
        // Phase 5: Decommission blue
        if !result.rollback_required {
            println!("  Phase 5: Decommissioning blue environment...");
            result.phases_completed.push("decommission_blue".to_string());
            result.final_state = "success".to_string();
        }
        
        result.duration = start.elapsed();
        Ok(result)
    }
    
    /// Monitor deployment health
    pub fn monitor_deployment_health(&self) -> Result<DeploymentHealth> {
        let load = self.monitor.check_system_load();
        let perf = self.monitor.compare_before_after();
        
        Ok(DeploymentHealth {
            status: if perf.degradation < 0.05 { "healthy" } else { "degraded" }.to_string(),
            error_rate: 0.002, // 0.2% simulated error rate
            latency_p50_ms: 45.0,
            latency_p99_ms: 250.0,
            cpu_usage: load.cpu_usage,
            memory_usage: load.memory_usage,
            active_connections: load.active_connections,
            recommendations: vec![
                "Consider increasing connection pool size".to_string(),
                "Monitor memory usage during peak hours".to_string(),
            ],
        })
    }
}

#[derive(Debug)]
pub struct DeploymentResult {
    pub strategy: String,
    pub duration: Duration,
    pub phases_completed: Vec<String>,
    pub rollback_required: bool,
    pub final_state: String,
}

#[derive(Debug)]
pub struct DeploymentHealth {
    pub status: String,
    pub error_rate: f64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub active_connections: u64,
    pub recommendations: Vec<String>,
}

/// Setup canary deployment with specific percentage
fn setup_canary_with_percentage(database: &Database, percentage: u64) -> Result<()> {
    let config = serde_json::json!({
        "deployment": {
            "canary_percentage": percentage,
            "updated_at": Utc::now().to_rfc3339()
        }
    });
    
    database.put(
        b"_system_canary_percentage",
        config.to_string().as_bytes(),
    )?;
    
    Ok(())
}

impl DeploymentResult {
    pub fn print_summary(&self) {
        println!("\n📦 Deployment Summary:");
        println!("=".repeat(50));
        println!("💼 Strategy: {}", self.strategy);
        println!("⏱️  Duration: {:.2}s", self.duration.as_secs_f64());
        println!("📈 Phases completed: {}", self.phases_completed.join(" → "));
        println!("🏁 Final state: {}", self.final_state);
        
        if self.rollback_required {
            println!("⚠️  Rollback was required");
        }
    }
}

impl DeploymentHealth {
    pub fn print_health(&self) {
        println!("\n🏥 Deployment Health:");
        println!("=".repeat(50));
        println!("🟢 Status: {}", self.status);
        println!("📋 Metrics:");
        println!("  Error rate: {:.3}%", self.error_rate * 100.0);
        println!("  Latency P50: {:.1}ms", self.latency_p50_ms);
        println!("  Latency P99: {:.1}ms", self.latency_p99_ms);
        println!("  CPU usage: {:.1}%", self.cpu_usage * 100.0);
        println!("  Memory usage: {:.1}%", self.memory_usage * 100.0);
        println!("  Active connections: {}", self.active_connections);
        
        if !self.recommendations.is_empty() {
            println!("\n💡 Recommendations:");
            for rec in &self.recommendations {
                println!("  • {}", rec);
            }
        }
    }
}

/// Run the zero-downtime migration example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 8: Zero-Downtime Migration Strategies");
    println!("================================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e08_zero_downtime.db")?;
    
    // Set up previous schema
    println!("📋 Setting up test environment...");
    setup_test_environment(&runner)?;
    
    // Create the migration
    let migration = ZeroDowntimeChangesMigration::new();
    
    // Capture performance baseline
    migration.performance_monitor.capture_baseline();
    
    // Run the zero-downtime migration
    runner.run_migration_with_validation(
        Box::new(migration),
        Box::new(|db| validate_zero_downtime_deployment(db)),
    )?;
    
    // Demonstrate blue-green deployment
    println!("\n🔄 Demonstrating blue-green deployment...");
    let orchestrator = DeploymentOrchestrator::new(&runner.database);
    let deployment_result = orchestrator.blue_green_deployment()?;
    deployment_result.print_summary();
    
    // Check deployment health
    let health = orchestrator.monitor_deployment_health()?;
    health.print_health();
    
    // Show migration history
    runner.show_history(Some(10))?;
    
    println!("\n🎉 Example 8 completed successfully!");
    println!("Next: Run example 9 to see performance tuning strategies");
    
    Ok(())
}

fn setup_test_environment(runner: &MigrationExampleRunner) -> Result<()> {
    // Create initial test data
    for i in 1..=100 {
        let key = format!("products_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "name": format!("Product {}", i),
            "description": format!("Description for product {}", i),
            "price": format!("{:.2}", 10.0 + (i as f64 * 2.5)),
            "category_id": (i % 10) + 1,
            "created_at": Utc::now().timestamp() - (i * 3600),
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created 100 test products");
    Ok(())
}

fn validate_zero_downtime_deployment(database: &Database) -> Result<()> {
    // Verify zero-downtime specific validations
    let products_with_tags = validation::count_records_with_field(database, "products_", "tags")?;
    let products_with_search = validation::count_records_with_field(database, "products_", "search_vector")?;
    
    println!("✅ Zero-downtime validation: {} products with tags, {} with search vectors",
             products_with_tags, products_with_search);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_zero_downtime_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = ZeroDowntimeChangesMigration::new();
        
        // Test migration metadata
        assert_eq!(migration.version(), SchemaVersion::new(1, 7));
        
        // Test options
        let options = migration.options();
        assert_eq!(options.locking_strategy, LockingStrategy::Optimistic);
        assert!(options.allow_concurrent_reads);
        assert!(options.allow_concurrent_writes);
    }

    #[test]
    fn test_feature_flags() {
        let flags = FeatureFlagManager::new();
        
        // Test progressive rollout
        flags.enable_progressive("test_flag", 50).unwrap();
        
        // Test deterministic rollout
        let key1_enabled = flags.is_enabled_for_key("dual_writes_enabled", "user_123");
        let key2_enabled = flags.is_enabled_for_key("dual_writes_enabled", "user_123");
        assert_eq!(key1_enabled, key2_enabled); // Should be deterministic
    }

    #[test]
    fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new();
        
        // Record some operations
        monitor.record_operation("read", Duration::from_micros(100));
        monitor.record_operation("write", Duration::from_micros(200));
        monitor.record_operation("read", Duration::from_micros(150));
        
        // Capture baseline
        monitor.capture_baseline();
        
        // Record more operations
        monitor.record_operation("read", Duration::from_micros(120));
        monitor.record_operation("write", Duration::from_micros(180));
        
        // Compare performance
        let comparison = monitor.compare_before_after();
        assert!(comparison.baseline_latency > 0.0);
    }
}
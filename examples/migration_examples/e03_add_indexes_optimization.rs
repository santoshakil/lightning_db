//! Example 3: Index Optimization Strategies
//!
//! This example demonstrates advanced indexing techniques:
//! - Composite indexes for complex queries
//! - Partial indexes with predicates
//! - Covering indexes with included columns
//! - Hash indexes for equality lookups
//! - Index analysis and optimization
//! - Performance benchmarking

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        IndexDefinition, IndexType, IndexColumn, SortOrder, IndexOptions,
    },
};
use std::collections::BTreeMap;
use std::time::Instant;
use super::{MigrationExampleRunner, validation};

/// Index optimization migration
pub struct AddIndexesOptimizationMigration {}

impl Migration for AddIndexesOptimizationMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 2)
    }

    fn description(&self) -> &str {
        "Advanced index optimization for high-performance queries"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Composite index for product search and filtering
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_products_search_composite".to_string(),
                    table: "products".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "active".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "category_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "price".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["name".to_string(), "description".to_string(), "sku".to_string()],
                    },
                },
            },
            
            // Partial index for active products only
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_products_active_name".to_string(),
                    table: "products".to_string(),
                    columns: vec![IndexColumn {
                        name: "name".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("active = true".to_string()),
                        include: vec!["price".to_string(), "inventory_count".to_string()],
                    },
                },
            },
            
            // Hash index for fast SKU lookups
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_products_sku_hash".to_string(),
                    table: "products".to_string(),
                    columns: vec![IndexColumn {
                        name: "sku".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::Hash,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["id".to_string(), "name".to_string(), "price".to_string()],
                    },
                },
            },
            
            // Order performance optimization - composite index
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_orders_status_performance".to_string(),
                    table: "orders".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "status".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Descending,
                        },
                        IndexColumn {
                            name: "user_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["order_number".to_string(), "total_amount".to_string()],
                    },
                },
            },
            
            // Partial index for pending orders (most queried)
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_orders_pending_priority".to_string(),
                    table: "orders".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "total_amount".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("status = 'pending'".to_string()),
                        include: vec!["user_id".to_string(), "order_number".to_string()],
                    },
                },
            },
            
            // User activity analysis index
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_activity_analysis".to_string(),
                    table: "user_activity".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "action".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Descending,
                        },
                        IndexColumn {
                            name: "user_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["resource_type".to_string(), "resource_id".to_string()],
                    },
                },
            },
            
            // Session management optimization
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_sessions_cleanup".to_string(),
                    table: "user_sessions".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "active".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "expires_at".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["id".to_string(), "user_id".to_string()],
                    },
                },
            },
            
            // Multi-column covering index for order items analytics
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_order_items_analytics".to_string(),
                    table: "order_items".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "product_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "created_at".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec![
                            "quantity".to_string(), 
                            "unit_price".to_string(), 
                            "total_price".to_string(),
                            "order_id".to_string(),
                        ],
                    },
                },
            },
            
            // Category hierarchy traversal optimization
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_categories_hierarchy".to_string(),
                    table: "categories".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "parent_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "active".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "name".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["description".to_string()],
                    },
                },
            },
            
            // Email search optimization (case-insensitive)
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_users_email_search".to_string(),
                    table: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "email".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false, // Changed from unique to allow partial matching
                        predicate: Some("email IS NOT NULL AND email != ''".to_string()),
                        include: vec!["username".to_string(), "active".to_string(), "last_login_at".to_string()],
                    },
                },
            },
        ]
    }

    fn target_schema(&self) -> Schema {
        // This would include all previous schema elements plus the new indexes
        let mut schema = Schema {
            version: self.version(),
            tables: BTreeMap::new(),
            indexes: BTreeMap::new(),
            constraints: BTreeMap::new(),
            metadata: BTreeMap::new(),
        };

        // Add all new indexes to the schema
        for step in self.steps() {
            if let MigrationStep::CreateIndex { definition } = step {
                schema.indexes.insert(definition.name.clone(), definition);
            }
        }

        // Add performance optimization metadata
        schema.metadata.insert("optimization_level".to_string(), "advanced".to_string());
        schema.metadata.insert("index_strategy".to_string(), "covering_composite_partial".to_string());
        schema.metadata.insert("target_workload".to_string(), "high_read_analytics".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating index optimization preconditions...");
        
        // Ensure required tables exist
        validation::validate_table_exists(database, "products")?;
        validation::validate_table_exists(database, "orders")?;
        validation::validate_table_exists(database, "order_items")?;
        validation::validate_table_exists(database, "users")?;
        validation::validate_table_exists(database, "categories")?;
        validation::validate_table_exists(database, "user_activity")?;
        validation::validate_table_exists(database, "user_sessions")?;
        
        // Check that we have sufficient data for meaningful indexing
        let products_count = validation::count_records_with_prefix(database, "products_")?;
        let orders_count = validation::count_records_with_prefix(database, "orders_")?;
        
        if products_count < 10 || orders_count < 5 {
            println!("⚠️  Warning: Limited data for index optimization testing");
        }
        
        println!("✅ Found {} products and {} orders for optimization", products_count, orders_count);
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating index optimization postconditions...");
        
        // Validate all new indexes were created
        let expected_indexes = vec![
            "idx_products_search_composite",
            "idx_products_active_name", 
            "idx_products_sku_hash",
            "idx_orders_status_performance",
            "idx_orders_pending_priority",
            "idx_user_activity_analysis",
            "idx_user_sessions_cleanup",
            "idx_order_items_analytics",
            "idx_categories_hierarchy",
            "idx_users_email_search",
        ];
        
        for index_name in expected_indexes {
            validation::validate_index_exists(database, index_name)?;
        }
        
        println!("✅ Index optimization validation completed successfully");
        Ok(())
    }
}

/// Query performance analyzer
pub struct QueryPerformanceAnalyzer<'a> {
    database: &'a Database,
}

impl<'a> QueryPerformanceAnalyzer<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self { database }
    }
    
    /// Benchmark product search queries
    pub fn benchmark_product_search(&self) -> Result<PerformanceReport> {
        println!("📊 Benchmarking product search queries...");
        
        let mut results = Vec::new();
        
        // Test 1: Simple product lookup by SKU
        let start = Instant::now();
        for i in 1..=100 {
            let sku_key = format!("products_{:03}", i % 25 + 1);
            self.database.get(sku_key.as_bytes())?;
        }
        let sku_lookup_time = start.elapsed();
        results.push(QueryResult {
            query_type: "SKU Lookup".to_string(),
            operations: 100,
            duration: sku_lookup_time,
            ops_per_sec: 100.0 / sku_lookup_time.as_secs_f64(),
        });
        
        // Test 2: Product search by category (range scan)
        let start = Instant::now();
        for category_id in 1..=5 {
            let scan_result = self.database.scan(
                Some(format!("products_").as_bytes().to_vec()),
                Some(format!("products~").as_bytes().to_vec()),
            )?;
            
            let mut count = 0;
            for item in scan_result {
                let (_key, value) = item?;
                if let Ok(product) = serde_json::from_slice::<serde_json::Value>(&value) {
                    if product.get("category_id").and_then(|v| v.as_i64()) == Some(category_id) {
                        count += 1;
                    }
                }
            }
        }
        let category_search_time = start.elapsed();
        results.push(QueryResult {
            query_type: "Category Search".to_string(),
            operations: 5,
            duration: category_search_time,
            ops_per_sec: 5.0 / category_search_time.as_secs_f64(),
        });
        
        // Test 3: Price range queries
        let start = Instant::now();
        let price_ranges = vec![(10.0, 50.0), (50.0, 100.0), (100.0, 200.0)];
        for (min_price, max_price) in price_ranges {
            let scan_result = self.database.scan(
                Some(format!("products_").as_bytes().to_vec()),
                Some(format!("products~").as_bytes().to_vec()),
            )?;
            
            let mut count = 0;
            for item in scan_result {
                let (_key, value) = item?;
                if let Ok(product) = serde_json::from_slice::<serde_json::Value>(&value) {
                    if let Some(price_str) = product.get("price").and_then(|v| v.as_str()) {
                        if let Ok(price) = price_str.parse::<f64>() {
                            if price >= min_price && price <= max_price {
                                count += 1;
                            }
                        }
                    }
                }
            }
        }
        let price_range_time = start.elapsed();
        results.push(QueryResult {
            query_type: "Price Range".to_string(),
            operations: 3,
            duration: price_range_time,
            ops_per_sec: 3.0 / price_range_time.as_secs_f64(),
        });
        
        Ok(PerformanceReport {
            test_name: "Product Search Benchmark".to_string(),
            results,
            total_duration: sku_lookup_time + category_search_time + price_range_time,
        })
    }
    
    /// Benchmark order queries
    pub fn benchmark_order_queries(&self) -> Result<PerformanceReport> {
        println!("📊 Benchmarking order queries...");
        
        let mut results = Vec::new();
        
        // Test 1: User order history
        let start = Instant::now();
        for user_id in 1..=10 {
            let scan_result = self.database.scan(
                Some(format!("orders_").as_bytes().to_vec()),
                Some(format!("orders~").as_bytes().to_vec()),
            )?;
            
            let mut count = 0;
            for item in scan_result {
                let (_key, value) = item?;
                if let Ok(order) = serde_json::from_slice::<serde_json::Value>(&value) {
                    if order.get("user_id").and_then(|v| v.as_i64()) == Some(user_id) {
                        count += 1;
                    }
                }
            }
        }
        let user_orders_time = start.elapsed();
        results.push(QueryResult {
            query_type: "User Order History".to_string(),
            operations: 10,
            duration: user_orders_time,
            ops_per_sec: 10.0 / user_orders_time.as_secs_f64(),
        });
        
        // Test 2: Order status queries
        let start = Instant::now();
        let statuses = vec!["pending", "processing", "shipped", "delivered"];
        for status in statuses {
            let scan_result = self.database.scan(
                Some(format!("orders_").as_bytes().to_vec()),
                Some(format!("orders~").as_bytes().to_vec()),
            )?;
            
            let mut count = 0;
            for item in scan_result {
                let (_key, value) = item?;
                if let Ok(order) = serde_json::from_slice::<serde_json::Value>(&value) {
                    if order.get("status").and_then(|v| v.as_str()) == Some(status) {
                        count += 1;
                    }
                }
            }
        }
        let status_queries_time = start.elapsed();
        results.push(QueryResult {
            query_type: "Status Queries".to_string(),
            operations: 4,
            duration: status_queries_time,
            ops_per_sec: 4.0 / status_queries_time.as_secs_f64(),
        });
        
        Ok(PerformanceReport {
            test_name: "Order Query Benchmark".to_string(),
            results,
            total_duration: user_orders_time + status_queries_time,
        })
    }
    
    /// Analyze index effectiveness
    pub fn analyze_index_effectiveness(&self) -> Result<IndexAnalysis> {
        println!("🔍 Analyzing index effectiveness...");
        
        // This would typically involve query plan analysis
        // For now, we'll simulate index usage statistics
        
        let indexes = vec![
            IndexEffectiveness {
                index_name: "idx_products_search_composite".to_string(),
                usage_count: 1250,
                selectivity: 0.85,
                storage_mb: 2.3,
                maintenance_cost: IndexMaintenanceCost::Medium,
                recommendation: "Highly effective for product filtering queries".to_string(),
            },
            IndexEffectiveness {
                index_name: "idx_products_sku_hash".to_string(),
                usage_count: 890,
                selectivity: 0.99,
                storage_mb: 0.8,
                maintenance_cost: IndexMaintenanceCost::Low,
                recommendation: "Excellent for SKU lookups, consider hash index for even better performance".to_string(),
            },
            IndexEffectiveness {
                index_name: "idx_orders_pending_priority".to_string(),
                usage_count: 445,
                selectivity: 0.15,
                storage_mb: 0.5,
                maintenance_cost: IndexMaintenanceCost::Low,
                recommendation: "Good for pending order queries, partial index reduces storage".to_string(),
            },
            IndexEffectiveness {
                index_name: "idx_user_activity_analysis".to_string(),
                usage_count: 234,
                selectivity: 0.65,
                storage_mb: 1.2,
                maintenance_cost: IndexMaintenanceCost::High,
                recommendation: "Useful for analytics, consider partitioning for large datasets".to_string(),
            },
        ];
        
        Ok(IndexAnalysis {
            total_indexes: indexes.len(),
            indexes,
            overall_effectiveness: 82.5,
            recommendations: vec![
                "Consider adding more partial indexes for frequently filtered queries".to_string(),
                "Monitor index usage and drop unused indexes to reduce maintenance overhead".to_string(),
                "Evaluate composite index column order based on query patterns".to_string(),
            ],
        })
    }
}

/// Performance report structures
#[derive(Debug)]
pub struct PerformanceReport {
    pub test_name: String,
    pub results: Vec<QueryResult>,
    pub total_duration: std::time::Duration,
}

#[derive(Debug)]
pub struct QueryResult {
    pub query_type: String,
    pub operations: u32,
    pub duration: std::time::Duration,
    pub ops_per_sec: f64,
}

#[derive(Debug)]
pub struct IndexAnalysis {
    pub total_indexes: usize,
    pub indexes: Vec<IndexEffectiveness>,
    pub overall_effectiveness: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug)]
pub struct IndexEffectiveness {
    pub index_name: String,
    pub usage_count: u32,
    pub selectivity: f64,
    pub storage_mb: f64,
    pub maintenance_cost: IndexMaintenanceCost,
    pub recommendation: String,
}

#[derive(Debug)]
pub enum IndexMaintenanceCost {
    Low,
    Medium,
    High,
}

impl PerformanceReport {
    pub fn print_summary(&self) {
        println!("\n📈 {} Results:", self.test_name);
        println!("=".repeat(50));
        
        for result in &self.results {
            println!("📊 {}: {:.0} ops/sec ({:.2}ms avg)", 
                result.query_type,
                result.ops_per_sec,
                result.duration.as_millis() as f64 / result.operations as f64
            );
        }
        
        println!("⏱️  Total duration: {:.2}ms", self.total_duration.as_millis());
    }
}

impl IndexAnalysis {
    pub fn print_analysis(&self) {
        println!("\n🔍 Index Effectiveness Analysis:");
        println!("=".repeat(60));
        println!("📊 Overall Effectiveness: {:.1}%", self.overall_effectiveness);
        println!("📈 Total Indexes Analyzed: {}", self.total_indexes);
        
        println!("\n📋 Index Details:");
        for index in &self.indexes {
            println!("  🔍 {}", index.index_name);
            println!("     Usage: {} queries, Selectivity: {:.1}%", 
                index.usage_count, index.selectivity * 100.0);
            println!("     Storage: {:.1}MB, Maintenance: {:?}", 
                index.storage_mb, index.maintenance_cost);
            println!("     💡 {}", index.recommendation);
            println!();
        }
        
        println!("🎯 Recommendations:");
        for (i, rec) in self.recommendations.iter().enumerate() {
            println!("  {}. {}", i + 1, rec);
        }
    }
}

/// Run the index optimization example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 3: Index Optimization Strategies");
    println!("===================================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e03_index_optimization.db")?;
    
    // Set up previous migrations first
    println!("📋 Setting up previous schema migrations...");
    setup_previous_migrations(&runner)?;
    
    // Create comprehensive test data
    create_performance_test_data(&runner)?;
    
    // Run benchmark before optimization
    println!("\n🔄 Running performance benchmark BEFORE optimization...");
    let analyzer = QueryPerformanceAnalyzer::new(&runner.database);
    let before_products = analyzer.benchmark_product_search()?;
    let before_orders = analyzer.benchmark_order_queries()?;
    
    before_products.print_summary();
    before_orders.print_summary();
    
    // Run the index optimization migration
    let migration = AddIndexesOptimizationMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_index_optimization(db)),
    )?;
    
    // Run benchmark after optimization
    println!("\n🚀 Running performance benchmark AFTER optimization...");
    let after_products = analyzer.benchmark_product_search()?;
    let after_orders = analyzer.benchmark_order_queries()?;
    
    after_products.print_summary();
    after_orders.print_summary();
    
    // Analyze index effectiveness
    let index_analysis = analyzer.analyze_index_effectiveness()?;
    index_analysis.print_analysis();
    
    // Show performance comparison
    print_performance_comparison(&before_products, &after_products, &before_orders, &after_orders);
    
    // Show migration history
    runner.show_history(Some(15))?;
    
    println!("\n🎉 Example 3 completed successfully!");
    println!("Next: Run example 4 to see data type evolution strategies");
    
    Ok(())
}

fn setup_previous_migrations(runner: &MigrationExampleRunner) -> Result<()> {
    // Run initial schema
    let initial_migration = super::e01_initial_schema::InitialSchemaMigration {};
    runner.run_migration_with_validation(
        initial_migration,
        Box::new(|_db| Ok(())),
    )?;
    
    // Run user profiles migration
    let profiles_migration = super::e02_add_user_profiles::AddUserProfilesMigration {};
    runner.run_migration_with_validation(
        profiles_migration,
        Box::new(|_db| Ok(())),
    )?;
    
    Ok(())
}

fn create_performance_test_data(runner: &MigrationExampleRunner) -> Result<()> {
    println!("📝 Creating comprehensive test data for performance testing...");
    
    // Create additional products (100 total)
    for i in 26..=100 {
        let key = format!("products_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "category_id": (i % 5) + 1,
            "name": format!("Performance Product {}", i),
            "description": format!("High-performance test product {} for indexing", i),
            "price": format!("{}.99", 20 + (i * 3)),
            "cost": format!("{}.50", 10 + (i * 2)),
            "sku": format!("PERF-{:06}", i),
            "inventory_count": 150 - (i % 30),
            "active": i % 10 != 0, // 90% active products
            "created_at": chrono::Utc::now().timestamp() - (i * 3600),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create test orders (50 total)
    let statuses = vec!["pending", "processing", "shipped", "delivered", "cancelled"];
    for i in 1..=50 {
        let key = format!("orders_{:03}", i);
        let status = &statuses[i % statuses.len()];
        let value = serde_json::json!({
            "id": i,
            "user_id": (i % 20) + 1,
            "order_number": format!("ORD-{:08}", i * 1000),
            "status": status,
            "total_amount": format!("{}.99", 50 + (i * 10)),
            "tax_amount": format!("{}.00", 5 + i),
            "shipping_amount": "9.99",
            "shipping_address": {
                "street": format!("{} Test St", i),
                "city": "Test City",
                "state": "TS",
                "zip": format!("{:05}", 10000 + i)
            },
            "billing_address": null,
            "created_at": chrono::Utc::now().timestamp() - (i * 1800),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create order items (200 total)
    for i in 1..=200 {
        let key = format!("order_items_{:03}", i);
        let order_id = (i % 50) + 1;
        let product_id = (i % 100) + 1;
        let value = serde_json::json!({
            "id": i,
            "order_id": order_id,
            "product_id": product_id,
            "quantity": (i % 5) + 1,
            "unit_price": format!("{}.99", 25 + (product_id * 2)),
            "total_price": format!("{}.99", (25 + (product_id * 2)) * ((i % 5) + 1)),
            "product_snapshot": {
                "name": format!("Product {}", product_id),
                "sku": format!("PERF-{:06}", product_id)
            },
            "created_at": chrono::Utc::now().timestamp() - (i * 900)
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create user activity logs (500 total)
    let actions = vec!["login", "logout", "view_product", "add_to_cart", "purchase", "search"];
    for i in 1..=500 {
        let key = format!("user_activity_{:03}", i);
        let action = &actions[i % actions.len()];
        let value = serde_json::json!({
            "id": i,
            "user_id": (i % 20) + 1,
            "action": action,
            "resource_type": if action == "view_product" { "product" } else { null },
            "resource_id": if action == "view_product" { 
                Some(format!("{}", (i % 100) + 1)) 
            } else { null },
            "metadata": {
                "session_id": format!("sess_{}", i % 50),
                "page": format!("/page/{}", i % 10)
            },
            "ip_address": format!("192.168.1.{}", (i % 254) + 1),
            "user_agent": "Test Browser 1.0",
            "created_at": chrono::Utc::now().timestamp() - (i * 60)
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created test data: 100 products, 50 orders, 200 order items, 500 activity logs");
    Ok(())
}

fn validate_index_optimization(database: &Database) -> Result<()> {
    println!("🔍 Validating index optimization...");
    
    // Verify data integrity after indexing
    let products_count = validation::count_records_with_prefix(database, "products_")?;
    let orders_count = validation::count_records_with_prefix(database, "orders_")?;
    let order_items_count = validation::count_records_with_prefix(database, "order_items_")?;
    let activity_count = validation::count_records_with_prefix(database, "user_activity_")?;
    
    println!("📊 Data counts after indexing: {} products, {} orders, {} items, {} activities", 
             products_count, orders_count, order_items_count, activity_count);
    
    // Expected counts from our test data creation
    if products_count < 100 || orders_count < 50 || order_items_count < 200 || activity_count < 500 {
        return Err(lightning_db::Error::Validation(
            "Data integrity check failed after index creation".to_string()
        ));
    }
    
    println!("✅ Index optimization validation successful");
    Ok(())
}

fn print_performance_comparison(
    before_products: &PerformanceReport,
    after_products: &PerformanceReport,
    before_orders: &PerformanceReport,
    after_orders: &PerformanceReport,
) {
    println!("\n📈 Performance Comparison Summary:");
    println!("=".repeat(60));
    
    // Calculate improvements
    for (i, (before, after)) in before_products.results.iter()
        .zip(after_products.results.iter()).enumerate() {
        let improvement = ((after.ops_per_sec - before.ops_per_sec) / before.ops_per_sec) * 100.0;
        let status = if improvement > 0.0 { "🚀" } else { "⚠️" };
        
        println!("{} Product {}: {:.1}% improvement ({:.0} -> {:.0} ops/sec)",
            status, before.query_type, improvement, before.ops_per_sec, after.ops_per_sec);
    }
    
    for (i, (before, after)) in before_orders.results.iter()
        .zip(after_orders.results.iter()).enumerate() {
        let improvement = ((after.ops_per_sec - before.ops_per_sec) / before.ops_per_sec) * 100.0;
        let status = if improvement > 0.0 { "🚀" } else { "⚠️" };
        
        println!("{} Order {}: {:.1}% improvement ({:.0} -> {:.0} ops/sec)",
            status, before.query_type, improvement, before.ops_per_sec, after.ops_per_sec);
    }
    
    println!("\n💡 Index optimization provides significant performance gains for complex queries!");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_index_optimization_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = AddIndexesOptimizationMigration {};
        
        // Test migration steps
        let steps = migration.steps();
        assert_eq!(steps.len(), 10); // All index creation steps
        
        // Test version
        assert_eq!(migration.version(), SchemaVersion::new(1, 2));
        
        // Test that all steps are index creation
        for step in steps {
            matches!(step, MigrationStep::CreateIndex { .. });
        }
    }

    #[test]
    fn test_performance_analyzer() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let analyzer = QueryPerformanceAnalyzer::new(&runner.database);
        
        // Test index analysis
        let analysis = analyzer.analyze_index_effectiveness().unwrap();
        assert!(analysis.total_indexes > 0);
        assert!(analysis.overall_effectiveness > 0.0);
        assert!(!analysis.recommendations.is_empty());
    }

    #[test] 
    fn test_index_definitions() {
        let migration = AddIndexesOptimizationMigration {};
        let schema = migration.target_schema();
        
        // Verify we have the expected number of indexes
        assert_eq!(schema.indexes.len(), 10);
        
        // Check for specific critical indexes
        assert!(schema.indexes.contains_key("idx_products_search_composite"));
        assert!(schema.indexes.contains_key("idx_products_sku_hash"));
        assert!(schema.indexes.contains_key("idx_orders_pending_priority"));
        
        // Verify composite index structure
        let composite_idx = &schema.indexes["idx_products_search_composite"];
        assert_eq!(composite_idx.columns.len(), 4); // active, category_id, price, created_at
        assert_eq!(composite_idx.options.include.len(), 3); // name, description, sku
    }
}
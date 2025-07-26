//! Example 6: Complex Data Migration
//!
//! This example demonstrates advanced data migration patterns:
//! - Multi-step data transformations
//! - Cross-table data consolidation
//! - Complex business logic during migration
//! - Data validation and cleansing
//! - Batch processing with error handling
//! - Transactional migration operations
//! - Data format standardization

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        TableDefinition, ColumnDefinition, DataType, IndexDefinition,
        IndexType, IndexColumn, SortOrder, IndexOptions, TableOptions,
        ConstraintDefinition, ConstraintType,
    },
};
use std::collections::{BTreeMap, HashMap};
use serde_json::Value;
use super::{MigrationExampleRunner, validation};

/// Complex data migration with advanced transformation logic
pub struct ComplexDataMigrationMigration {}

impl Migration for ComplexDataMigrationMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 5)
    }

    fn description(&self) -> &str {
        "Complex data migration with transformations, consolidation, and validation"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Create consolidated customer data table
            MigrationStep::CreateTable {
                name: "customer_profiles".to_string(),
                definition: create_customer_profiles_table(),
            },
            
            // Create product catalog with enriched data
            MigrationStep::CreateTable {
                name: "product_catalog".to_string(),
                definition: create_product_catalog_table(),
            },
            
            // Create order analytics table
            MigrationStep::CreateTable {
                name: "order_analytics".to_string(),
                definition: create_order_analytics_table(),
            },
            
            // Create data quality reports table
            MigrationStep::CreateTable {
                name: "data_quality_reports".to_string(),
                definition: create_data_quality_reports_table(),
            },
            
            // Create migration logs table
            MigrationStep::CreateTable {
                name: "migration_logs".to_string(),
                definition: create_migration_logs_table(),
            },
            
            // Create business metrics table
            MigrationStep::CreateTable {
                name: "business_metrics".to_string(),
                definition: create_business_metrics_table(),
            },
            
            // Create standardized addresses table (enhanced)
            MigrationStep::CreateTable {
                name: "standardized_addresses".to_string(),
                definition: create_standardized_addresses_table(),
            },
            
            // Create customer segments table
            MigrationStep::CreateTable {
                name: "customer_segments".to_string(),
                definition: create_customer_segments_table(),
            },
            
            // Create comprehensive indexes for performance
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_customer_profiles_email_phone".to_string(),
                    table: "customer_profiles".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "email_normalized".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "phone_normalized".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("email_normalized IS NOT NULL OR phone_normalized IS NOT NULL".to_string()),
                        include: vec!["customer_id".to_string(), "full_name".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_catalog_search".to_string(),
                    table: "product_catalog".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "search_keywords".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "category_path".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("active = true".to_string()),
                        include: vec!["product_id".to_string(), "name".to_string(), "price_cents".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_order_analytics_customer_date".to_string(),
                    table: "order_analytics".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "customer_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "order_date".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["total_amount_cents".to_string(), "profit_margin".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_business_metrics_date_metric".to_string(),
                    table: "business_metrics".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "metric_date".to_string(),
                            order: SortOrder::Descending,
                        },
                        IndexColumn {
                            name: "metric_name".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["metric_value".to_string(), "metric_type".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_customer_segments_segment".to_string(),
                    table: "customer_segments".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "segment_name".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "segment_score".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["customer_id".to_string(), "assigned_date".to_string()],
                    },
                },
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

        // Add all new consolidated tables
        schema.tables.insert("customer_profiles".to_string(), create_customer_profiles_table());
        schema.tables.insert("product_catalog".to_string(), create_product_catalog_table());
        schema.tables.insert("order_analytics".to_string(), create_order_analytics_table());
        schema.tables.insert("data_quality_reports".to_string(), create_data_quality_reports_table());
        schema.tables.insert("migration_logs".to_string(), create_migration_logs_table());
        schema.tables.insert("business_metrics".to_string(), create_business_metrics_table());
        schema.tables.insert("standardized_addresses".to_string(), create_standardized_addresses_table());
        schema.tables.insert("customer_segments".to_string(), create_customer_segments_table());

        // Add all indexes
        for step in self.steps() {
            if let MigrationStep::CreateIndex { definition } = step {
                schema.indexes.insert(definition.name.clone(), definition);
            }
        }

        // Add migration metadata
        schema.metadata.insert("migration_type".to_string(), "complex_data_transformation".to_string());
        schema.metadata.insert("consolidation_strategy".to_string(), "multi_table_merge".to_string());
        schema.metadata.insert("data_quality_level".to_string(), "enterprise".to_string());
        schema.metadata.insert("business_logic_complexity".to_string(), "high".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating complex data migration preconditions...");
        
        // Ensure all source tables and data exist
        validation::validate_table_exists(database, "users")?;
        validation::validate_table_exists(database, "user_profiles")?;
        validation::validate_table_exists(database, "products")?;
        validation::validate_table_exists(database, "orders")?;
        validation::validate_table_exists(database, "order_items")?;
        validation::validate_table_exists(database, "addresses")?;
        
        // Check data volumes and quality
        let users_count = validation::count_records_with_prefix(database, "users_")?;
        let products_count = validation::count_records_with_prefix(database, "products_")?;
        let orders_count = validation::count_records_with_prefix(database, "orders_")?;
        
        println!("📊 Data volumes for migration: {} users, {} products, {} orders", 
                users_count, products_count, orders_count);
        
        if users_count < 5 || products_count < 10 || orders_count < 5 {
            return Err(lightning_db::Error::Validation(
                "Insufficient data for meaningful complex migration".to_string()
            ));
        }
        
        // Validate data quality before migration
        validate_source_data_quality(database)?;
        
        println!("✅ Complex data migration preconditions validated");
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating complex data migration postconditions...");
        
        // Validate all target tables were created
        let target_tables = vec![
            "customer_profiles", "product_catalog", "order_analytics",
            "data_quality_reports", "migration_logs", "business_metrics",
            "standardized_addresses", "customer_segments"
        ];
        
        for table_name in target_tables {
            validation::validate_table_exists(database, table_name)?;
        }
        
        // Validate critical indexes
        let critical_indexes = vec![
            "idx_customer_profiles_email_phone",
            "idx_product_catalog_search",
            "idx_order_analytics_customer_date",
        ];
        
        for index_name in critical_indexes {
            validation::validate_index_exists(database, index_name)?;
        }
        
        // Validate data transformation results
        validate_migration_completeness(database)?;
        
        println!("✅ Complex data migration postconditions validated");
        Ok(())
    }
}

/// Advanced data migration engine with complex transformation capabilities
pub struct ComplexDataMigrationEngine<'a> {
    database: &'a Database,
    batch_size: usize,
    validation_enabled: bool,
    transaction_mode: bool,
}

impl<'a> ComplexDataMigrationEngine<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self {
            database,
            batch_size: 50, // Smaller batches for complex operations
            validation_enabled: true,
            transaction_mode: true,
        }
    }
    
    /// Consolidate customer data from multiple sources
    pub fn migrate_customer_profiles(&self) -> Result<ComplexMigrationStats> {
        println!("👥 Consolidating customer profiles from multiple sources...");
        
        let mut stats = ComplexMigrationStats::new("customer_profile_consolidation");
        let mut deduplication_map: HashMap<String, i64> = HashMap::new();
        
        // Step 1: Load and consolidate user data
        let users_scan = self.database.scan(Some(b"users_".to_vec()), Some(b"users~".to_vec()))?;
        let mut user_profiles: HashMap<i64, Value> = HashMap::new();
        
        // Load user profiles data
        let profiles_scan = self.database.scan(Some(b"user_profiles_".to_vec()), Some(b"user_profiles~".to_vec()))?;
        for item in profiles_scan {
            let (_key, value) = item?;
            if let Ok(profile) = serde_json::from_slice::<Value>(&value) {
                if let Some(user_id) = profile.get("user_id").and_then(|v| v.as_i64()) {
                    user_profiles.insert(user_id, profile);
                }
            }
        }
        
        // Load addresses data
        let addresses_scan = self.database.scan(Some(b"addresses_".to_vec()), Some(b"addresses~".to_vec()))?;
        let mut user_addresses: HashMap<i64, Vec<Value>> = HashMap::new();
        for item in addresses_scan {
            let (_key, value) = item?;
            if let Ok(address) = serde_json::from_slice::<Value>(&value) {
                if let Some(user_id) = address.get("user_id").and_then(|v| v.as_i64()) {
                    user_addresses.entry(user_id).or_insert_with(Vec::new).push(address);
                }
            }
        }
        
        // Process each user
        for item in users_scan {
            let (_key, value) = item?;
            let user: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(user_id) = user.get("id").and_then(|v| v.as_i64()) {
                // Check for duplicates based on email normalization
                if let Some(email) = user.get("email").and_then(|v| v.as_str()) {
                    let normalized_email = normalize_email(email);
                    
                    if let Some(&existing_id) = deduplication_map.get(&normalized_email) {
                        stats.duplicates_found += 1;
                        stats.warnings.push(format!("Duplicate email {} found for users {} and {}", 
                            normalized_email, existing_id, user_id));
                        
                        // Merge duplicate customer data
                        self.merge_duplicate_customers(existing_id, user_id, &user, &user_profiles, &user_addresses)?;
                        continue;
                    }
                    
                    deduplication_map.insert(normalized_email.clone(), user_id);
                }
                
                // Create consolidated customer profile
                match self.create_consolidated_customer_profile(
                    user_id, 
                    &user, 
                    user_profiles.get(&user_id),
                    user_addresses.get(&user_id)
                ) {
                    Ok(()) => {
                        stats.successful_transformations += 1;
                        
                        // Calculate customer lifetime value
                        let ltv = self.calculate_customer_lifetime_value(user_id)?;
                        if ltv > 1000.0 {
                            stats.high_value_customers += 1;
                        }
                    }
                    Err(e) => {
                        stats.failed_transformations += 1;
                        stats.errors.push(format!("Customer profile creation failed for {}: {}", user_id, e));
                    }
                }
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} customers...", stats.total_records);
            }
        }
        
        stats.end_operation();
        self.log_migration_operation("customer_profile_consolidation", &stats)?;
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create enriched product catalog with computed fields
    pub fn migrate_product_catalog(&self) -> Result<ComplexMigrationStats> {
        println!("📦 Creating enriched product catalog with analytics...");
        
        let mut stats = ComplexMigrationStats::new("product_catalog_enrichment");
        
        // Load related data for enrichment
        let categories_map = self.load_categories_map()?;
        let price_history_map = self.load_price_history_map()?;
        let sales_data_map = self.load_product_sales_data()?;
        
        let products_scan = self.database.scan(Some(b"products_".to_vec()), Some(b"products~".to_vec()))?;
        
        for item in products_scan {
            let (_key, value) = item?;
            let product: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(product_id) = product.get("id").and_then(|v| v.as_i64()) {
                match self.create_enriched_product_catalog_entry(
                    product_id,
                    &product,
                    &categories_map,
                    &price_history_map,
                    &sales_data_map,
                ) {
                    Ok(()) => {
                        stats.successful_transformations += 1;
                        
                        // Track product performance metrics
                        if let Some(sales_data) = sales_data_map.get(&product_id) {
                            let total_sold = sales_data.get("total_quantity").and_then(|v| v.as_i64()).unwrap_or(0);
                            if total_sold > 100 {
                                stats.high_performance_items += 1;
                            }
                        }
                    }
                    Err(e) => {
                        stats.failed_transformations += 1;
                        stats.errors.push(format!("Product catalog entry creation failed for {}: {}", product_id, e));
                    }
                }
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} products...", stats.total_records);
            }
        }
        
        stats.end_operation();
        self.log_migration_operation("product_catalog_enrichment", &stats)?;
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create comprehensive order analytics
    pub fn migrate_order_analytics(&self) -> Result<ComplexMigrationStats> {
        println!("📊 Creating comprehensive order analytics...");
        
        let mut stats = ComplexMigrationStats::new("order_analytics_creation");
        
        let orders_scan = self.database.scan(Some(b"orders_".to_vec()), Some(b"orders~".to_vec()))?;
        
        // Load order items for detailed analysis
        let order_items_map = self.load_order_items_map()?;
        let product_costs_map = self.load_product_costs_map()?;
        
        for item in orders_scan {
            let (_key, value) = item?;
            let order: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(order_id) = order.get("id").and_then(|v| v.as_i64()) {
                match self.create_order_analytics_record(
                    order_id,
                    &order,
                    &order_items_map,
                    &product_costs_map,
                ) {
                    Ok(analytics) => {
                        stats.successful_transformations += 1;
                        
                        // Track high-value orders
                        if let Some(total_cents) = analytics.get("total_amount_cents").and_then(|v| v.as_i64()) {
                            if total_cents > 50000 { // $500+
                                stats.high_value_orders += 1;
                            }
                        }
                        
                        // Track profitable orders
                        if let Some(margin) = analytics.get("profit_margin").and_then(|v| v.as_f64()) {
                            if margin > 0.3 { // 30%+ margin
                                stats.profitable_orders += 1;
                            }
                        }
                    }
                    Err(e) => {
                        stats.failed_transformations += 1;
                        stats.errors.push(format!("Order analytics creation failed for {}: {}", order_id, e));
                    }
                }
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} orders...", stats.total_records);
            }
        }
        
        stats.end_operation();
        self.log_migration_operation("order_analytics_creation", &stats)?;
        stats.print_summary();
        Ok(stats)
    }
    
    /// Generate business metrics from consolidated data
    pub fn generate_business_metrics(&self) -> Result<ComplexMigrationStats> {
        println!("📈 Generating business metrics from consolidated data...");
        
        let mut stats = ComplexMigrationStats::new("business_metrics_generation");
        
        // Calculate various business metrics
        let metrics_to_calculate = vec![
            ("customer_acquisition_cost", self.calculate_customer_acquisition_cost()?),
            ("average_order_value", self.calculate_average_order_value()?),
            ("customer_lifetime_value_avg", self.calculate_avg_customer_lifetime_value()?),
            ("monthly_recurring_revenue", self.calculate_monthly_recurring_revenue()?),
            ("churn_rate", self.calculate_churn_rate()?),
            ("inventory_turnover", self.calculate_inventory_turnover()?),
            ("profit_margin_avg", self.calculate_average_profit_margin()?),
            ("customer_satisfaction_score", self.calculate_customer_satisfaction_score()?),
        ];
        
        for (metric_name, metric_value) in metrics_to_calculate {
            match self.create_business_metric_record(metric_name, metric_value) {
                Ok(()) => {
                    stats.successful_transformations += 1;
                    println!("   Generated metric: {} = {:.2}", metric_name, metric_value);
                }
                Err(e) => {
                    stats.failed_transformations += 1;
                    stats.errors.push(format!("Business metric creation failed for {}: {}", metric_name, e));
                }
            }
            stats.total_records += 1;
        }
        
        stats.end_operation();
        self.log_migration_operation("business_metrics_generation", &stats)?;
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create customer segments based on behavior and value
    pub fn create_customer_segments(&self) -> Result<ComplexMigrationStats> {
        println!("🎯 Creating customer segments based on behavior analysis...");
        
        let mut stats = ComplexMigrationStats::new("customer_segmentation");
        
        // Load customer data for segmentation
        let customer_profiles_scan = self.database.scan(
            Some(b"customer_profiles_".to_vec()), 
            Some(b"customer_profiles~".to_vec())
        )?;
        
        for item in customer_profiles_scan {
            let (_key, value) = item?;
            let customer: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(customer_id) = customer.get("customer_id").and_then(|v| v.as_i64()) {
                // Calculate customer scores and segment
                let recency_score = self.calculate_recency_score(customer_id)?;
                let frequency_score = self.calculate_frequency_score(customer_id)?;
                let monetary_score = self.calculate_monetary_score(customer_id)?;
                
                let segment = self.determine_customer_segment(recency_score, frequency_score, monetary_score);
                
                match self.assign_customer_segment(customer_id, &segment, recency_score + frequency_score + monetary_score) {
                    Ok(()) => {
                        stats.successful_transformations += 1;
                        
                        // Track segment distribution
                        match segment.as_str() {
                            "champions" => stats.premium_customers += 1,
                            "loyal_customers" => stats.loyal_customers += 1,
                            "at_risk" => stats.at_risk_customers += 1,
                            _ => {}
                        }
                    }
                    Err(e) => {
                        stats.failed_transformations += 1;
                        stats.errors.push(format!("Customer segmentation failed for {}: {}", customer_id, e));
                    }
                }
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} customers for segmentation...", stats.total_records);
            }
        }
        
        stats.end_operation();
        self.log_migration_operation("customer_segmentation", &stats)?;
        stats.print_summary();
        Ok(stats)
    }
    
    /// Generate comprehensive data quality report
    pub fn generate_data_quality_report(&self) -> Result<()> {
        println!("📋 Generating comprehensive data quality report...");
        
        let mut quality_issues = Vec::new();
        let mut total_records = 0;
        let mut clean_records = 0;
        
        // Analyze customer data quality
        let customer_scan = self.database.scan(
            Some(b"customer_profiles_".to_vec()),
            Some(b"customer_profiles~".to_vec())
        )?;
        
        for item in customer_scan {
            let (_key, value) = item?;
            total_records += 1;
            
            if let Ok(customer) = serde_json::from_slice::<Value>(&value) {
                let mut record_issues = Vec::new();
                
                // Check email format
                if let Some(email) = customer.get("email_normalized").and_then(|v| v.as_str()) {
                    if !is_valid_email(email) {
                        record_issues.push("Invalid email format".to_string());
                    }
                } else {
                    record_issues.push("Missing email".to_string());
                }
                
                // Check phone format
                if let Some(phone) = customer.get("phone_normalized").and_then(|v| v.as_str()) {
                    if !is_valid_phone(phone) {
                        record_issues.push("Invalid phone format".to_string());
                    }
                }
                
                // Check required fields
                if customer.get("full_name").and_then(|v| v.as_str()).map_or(true, |s| s.trim().is_empty()) {
                    record_issues.push("Missing or empty full name".to_string());
                }
                
                if record_issues.is_empty() {
                    clean_records += 1;
                } else {
                    quality_issues.extend(record_issues);
                }
            }
        }
        
        // Create quality report
        let quality_score = if total_records > 0 {
            (clean_records as f64 / total_records as f64) * 100.0
        } else {
            0.0
        };
        
        let report_id = chrono::Utc::now().timestamp_micros();
        let report_key = format!("data_quality_reports_{}", report_id);
        let report = serde_json::json!({
            "id": format!("{}", report_id),
            "report_date": chrono::Utc::now().timestamp(),
            "total_records_analyzed": total_records,
            "clean_records": clean_records,
            "quality_score_percentage": quality_score,
            "issues_found": quality_issues.len(),
            "issue_summary": {
                "email_issues": quality_issues.iter().filter(|i| i.contains("email")).count(),
                "phone_issues": quality_issues.iter().filter(|i| i.contains("phone")).count(),
                "name_issues": quality_issues.iter().filter(|i| i.contains("name")).count(),
            },
            "recommendations": [
                "Implement email validation at data entry",
                "Standardize phone number formats",
                "Require full name completion in user registration",
                "Add data cleansing pipeline for existing records",
            ],
            "next_review_date": chrono::Utc::now().timestamp() + (30 * 24 * 3600), // 30 days
        });
        
        self.database.put(report_key.as_bytes(), report.to_string().as_bytes())?;
        
        println!("📊 Data Quality Report Generated:");
        println!("   Total records: {}", total_records);
        println!("   Clean records: {} ({:.1}%)", clean_records, quality_score);
        println!("   Issues found: {}", quality_issues.len());
        
        Ok(())
    }
    
    // Helper methods for complex data operations
    
    fn create_consolidated_customer_profile(
        &self,
        user_id: i64,
        user: &Value,
        profile: Option<&Value>,
        addresses: Option<&Vec<Value>>,
    ) -> Result<()> {
        let customer_key = format!("customer_profiles_{:06}", user_id);
        
        // Normalize and clean data
        let email = user.get("email").and_then(|v| v.as_str()).unwrap_or("");
        let normalized_email = normalize_email(email);
        
        let phone = profile
            .and_then(|p| p.get("phone_number"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let normalized_phone = normalize_phone(phone);
        
        let full_name = format!("{} {}",
            user.get("first_name").and_then(|v| v.as_str()).unwrap_or(""),
            user.get("last_name").and_then(|v| v.as_str()).unwrap_or("")
        ).trim().to_string();
        
        // Calculate derived fields
        let account_age_days = if let Some(created_at) = user.get("created_at").and_then(|v| v.as_i64()) {
            (chrono::Utc::now().timestamp() - created_at) / 86400
        } else {
            0
        };
        
        let primary_address = addresses
            .and_then(|addrs| addrs.iter().find(|a| 
                a.get("is_default").and_then(|v| v.as_bool()).unwrap_or(false)
            ))
            .or_else(|| addresses.and_then(|addrs| addrs.first()));
        
        let customer_profile = serde_json::json!({
            "id": user_id,
            "customer_id": user_id,
            "email_original": email,
            "email_normalized": normalized_email,
            "phone_original": phone,
            "phone_normalized": normalized_phone,
            "full_name": full_name,
            "first_name": user.get("first_name").unwrap_or(&Value::Null),
            "last_name": user.get("last_name").unwrap_or(&Value::Null),
            "username": user.get("username").unwrap_or(&Value::Null),
            "account_status": if user.get("active").and_then(|v| v.as_bool()).unwrap_or(false) { "active" } else { "inactive" },
            "email_verified": user.get("email_verified").and_then(|v| v.as_bool()).unwrap_or(false),
            "account_created_at": user.get("created_at").unwrap_or(&Value::Null),
            "last_login_at": user.get("last_login_at").unwrap_or(&Value::Null),
            "account_age_days": account_age_days,
            "timezone": user.get("timezone").and_then(|v| v.as_str()).unwrap_or("UTC"),
            "primary_address": primary_address.unwrap_or(&Value::Null),
            "total_addresses": addresses.map(|a| a.len()).unwrap_or(0),
            "profile_completion_score": self.calculate_profile_completion_score(user, profile),
            "data_quality_score": self.calculate_data_quality_score(email, phone, &full_name),
            "customer_since": user.get("created_at").unwrap_or(&Value::Null),
            "last_updated": chrono::Utc::now().timestamp(),
            "migration_metadata": {
                "migrated_at": chrono::Utc::now().timestamp(),
                "source_tables": ["users", "user_profiles", "addresses"],
                "data_version": "1.5"
            }
        });
        
        self.database.put(customer_key.as_bytes(), customer_profile.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_enriched_product_catalog_entry(
        &self,
        product_id: i64,
        product: &Value,
        categories_map: &HashMap<i64, Value>,
        price_history_map: &HashMap<i64, Vec<Value>>,
        sales_data_map: &HashMap<i64, Value>,
    ) -> Result<()> {
        let catalog_key = format!("product_catalog_{:06}", product_id);
        
        // Build category path
        let category_path = if let Some(category_id) = product.get("category_id").and_then(|v| v.as_i64()) {
            self.build_category_path(category_id, categories_map)
        } else {
            "Uncategorized".to_string()
        };
        
        // Generate search keywords
        let search_keywords = self.generate_search_keywords(product, &category_path);
        
        // Calculate price statistics
        let (min_price, max_price, avg_price) = if let Some(price_history) = price_history_map.get(&product_id) {
            self.calculate_price_statistics(price_history)
        } else {
            let current_price = product.get("price_v2").and_then(|v| v.as_i64()).unwrap_or(0);
            (current_price, current_price, current_price as f64)
        };
        
        // Get sales performance data
        let sales_data = sales_data_map.get(&product_id);
        let total_sold = sales_data.and_then(|s| s.get("total_quantity")).and_then(|v| v.as_i64()).unwrap_or(0);
        let revenue_generated = sales_data.and_then(|s| s.get("total_revenue")).and_then(|v| v.as_i64()).unwrap_or(0);
        
        let catalog_entry = serde_json::json!({
            "id": product_id,
            "product_id": product_id,
            "name": product.get("name").unwrap_or(&Value::Null),
            "description": product.get("description").unwrap_or(&Value::Null),
            "sku": product.get("sku").unwrap_or(&Value::Null),
            "category_id": product.get("category_id").unwrap_or(&Value::Null),
            "category_path": category_path,
            "search_keywords": search_keywords,
            "price_cents": product.get("price_v2").unwrap_or(&Value::Null),
            "cost_cents": product.get("cost_v2").unwrap_or(&Value::Null),
            "price_history": {
                "min_price_cents": min_price,
                "max_price_cents": max_price,
                "avg_price_cents": avg_price as i64,
                "price_volatility": self.calculate_price_volatility(price_history_map.get(&product_id))
            },
            "inventory_status": {
                "current_stock": product.get("inventory_count").unwrap_or(&Value::Number(serde_json::Number::from(0))),
                "stock_status": self.determine_stock_status(product.get("inventory_count").and_then(|v| v.as_i64()).unwrap_or(0)),
                "last_restocked": null
            },
            "sales_performance": {
                "total_units_sold": total_sold,
                "total_revenue_cents": revenue_generated,
                "performance_tier": self.determine_performance_tier(total_sold, revenue_generated),
                "days_since_last_sale": null
            },
            "active": product.get("active").unwrap_or(&Value::Bool(true)),
            "created_at": product.get("created_at").unwrap_or(&Value::Null),
            "updated_at": product.get("updated_at").unwrap_or(&Value::Null),
            "catalog_score": self.calculate_catalog_score(product, total_sold, revenue_generated),
            "enrichment_metadata": {
                "enriched_at": chrono::Utc::now().timestamp(),
                "data_sources": ["products", "categories", "price_history", "order_items"],
                "enrichment_version": "1.5"
            }
        });
        
        self.database.put(catalog_key.as_bytes(), catalog_entry.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_order_analytics_record(
        &self,
        order_id: i64,
        order: &Value,
        order_items_map: &HashMap<i64, Vec<Value>>,
        product_costs_map: &HashMap<i64, i64>,
    ) -> Result<Value> {
        let analytics_key = format!("order_analytics_{:06}", order_id);
        
        // Calculate order metrics
        let order_items = order_items_map.get(&order_id).unwrap_or(&Vec::new());
        let total_items = order_items.len();
        let total_quantity: i64 = order_items.iter()
            .filter_map(|item| item.get("quantity").and_then(|v| v.as_i64()))
            .sum();
        
        // Calculate costs and profit
        let total_cost_cents: i64 = order_items.iter()
            .filter_map(|item| {
                let product_id = item.get("product_id").and_then(|v| v.as_i64())?;
                let quantity = item.get("quantity").and_then(|v| v.as_i64())?;
                let cost_per_unit = product_costs_map.get(&product_id)?;
                Some(cost_per_unit * quantity)
            })
            .sum();
        
        let total_amount_cents = order.get("total_amount")
            .and_then(|v| v.as_str())
            .and_then(|s| s.replace("$", "").replace(",", "").parse::<f64>().ok())
            .map(|f| (f * 100.0) as i64)
            .unwrap_or(0);
        
        let profit_cents = total_amount_cents - total_cost_cents;
        let profit_margin = if total_amount_cents > 0 {
            profit_cents as f64 / total_amount_cents as f64
        } else {
            0.0
        };
        
        // Determine order category
        let order_category = self.categorize_order(total_amount_cents, total_items, &order_items);
        
        let analytics_record = serde_json::json!({
            "id": order_id,
            "order_id": order_id,
            "customer_id": order.get("user_id").unwrap_or(&Value::Null),
            "order_number": order.get("order_number").unwrap_or(&Value::Null),
            "order_date": order.get("created_at").unwrap_or(&Value::Null),
            "status": order.get("status").unwrap_or(&Value::Null),
            "total_amount_cents": total_amount_cents,
            "total_cost_cents": total_cost_cents,
            "profit_cents": profit_cents,
            "profit_margin": profit_margin,
            "item_count": total_items,
            "total_quantity": total_quantity,
            "average_item_value_cents": if total_items > 0 { total_amount_cents / total_items as i64 } else { 0 },
            "order_category": order_category,
            "shipping_amount_cents": self.parse_currency_to_cents(order.get("shipping_amount").and_then(|v| v.as_str()).unwrap_or("0")),
            "tax_amount_cents": self.parse_currency_to_cents(order.get("tax_amount").and_then(|v| v.as_str()).unwrap_or("0")),
            "payment_method": null, // Would be derived from payment data if available
            "fulfillment_time_hours": null, // Would be calculated from status history
            "created_at": chrono::Utc::now().timestamp(),
            "analytics_metadata": {
                "calculated_at": chrono::Utc::now().timestamp(),
                "data_sources": ["orders", "order_items", "products"],
                "calculation_version": "1.5"
            }
        });
        
        self.database.put(analytics_key.as_bytes(), analytics_record.to_string().as_bytes())?;
        Ok(analytics_record)
    }
    
    // Business metrics calculation methods
    
    fn calculate_customer_acquisition_cost(&self) -> Result<f64> {
        // Simplified calculation - in real scenario would use marketing spend data
        let total_customers = validation::count_records_with_prefix(self.database, "customer_profiles_")?;
        let estimated_marketing_spend = 50000.0; // Example value
        Ok(if total_customers > 0 { estimated_marketing_spend / total_customers as f64 } else { 0.0 })
    }
    
    fn calculate_average_order_value(&self) -> Result<f64> {
        let mut total_value = 0i64;
        let mut order_count = 0;
        
        let scan_result = self.database.scan(Some(b"order_analytics_".to_vec()), Some(b"order_analytics~".to_vec()))?;
        for item in scan_result {
            let (_key, value) = item?;
            if let Ok(analytics) = serde_json::from_slice::<Value>(&value) {
                if let Some(amount) = analytics.get("total_amount_cents").and_then(|v| v.as_i64()) {
                    total_value += amount;
                    order_count += 1;
                }
            }
        }
        
        Ok(if order_count > 0 { total_value as f64 / 100.0 / order_count as f64 } else { 0.0 })
    }
    
    fn calculate_customer_lifetime_value(&self, customer_id: i64) -> Result<f64> {
        let mut total_spent = 0i64;
        let mut order_count = 0;
        
        let scan_result = self.database.scan(Some(b"order_analytics_".to_vec()), Some(b"order_analytics~".to_vec()))?;
        for item in scan_result {
            let (_key, value) = item?;
            if let Ok(analytics) = serde_json::from_slice::<Value>(&value) {
                if analytics.get("customer_id").and_then(|v| v.as_i64()) == Some(customer_id) {
                    if let Some(amount) = analytics.get("total_amount_cents").and_then(|v| v.as_i64()) {
                        total_spent += amount;
                        order_count += 1;
                    }
                }
            }
        }
        
        // Simple LTV calculation: average order value * estimated lifetime orders
        let avg_order_value = if order_count > 0 { total_spent as f64 / order_count as f64 } else { 0.0 };
        let estimated_lifetime_orders = 12.0; // Estimated orders over customer lifetime
        
        Ok((avg_order_value * estimated_lifetime_orders) / 100.0) // Convert cents to dollars
    }
    
    // Additional helper methods (simplified for brevity)
    
    fn log_migration_operation(&self, operation_name: &str, stats: &ComplexMigrationStats) -> Result<()> {
        let log_id = chrono::Utc::now().timestamp_micros();
        let log_key = format!("migration_logs_{}", log_id);
        let log_record = serde_json::json!({
            "id": format!("{}", log_id),
            "operation_name": operation_name,
            "started_at": stats.start_time,
            "completed_at": stats.end_time.unwrap_or(chrono::Utc::now().timestamp()),
            "duration_seconds": stats.duration.as_secs(),
            "total_records": stats.total_records,
            "successful_transformations": stats.successful_transformations,
            "failed_transformations": stats.failed_transformations,
            "warnings_count": stats.warnings.len(),
            "errors_count": stats.errors.len(),
            "status": "completed"
        });
        
        self.database.put(log_key.as_bytes(), log_record.to_string().as_bytes())?;
        Ok(())
    }
    
    // Placeholder implementations for complex calculations
    fn load_categories_map(&self) -> Result<HashMap<i64, Value>> { Ok(HashMap::new()) }
    fn load_price_history_map(&self) -> Result<HashMap<i64, Vec<Value>>> { Ok(HashMap::new()) }
    fn load_product_sales_data(&self) -> Result<HashMap<i64, Value>> { Ok(HashMap::new()) }
    fn load_order_items_map(&self) -> Result<HashMap<i64, Vec<Value>>> { Ok(HashMap::new()) }
    fn load_product_costs_map(&self) -> Result<HashMap<i64, i64>> { Ok(HashMap::new()) }
    fn calculate_avg_customer_lifetime_value(&self) -> Result<f64> { Ok(850.0) }
    fn calculate_monthly_recurring_revenue(&self) -> Result<f64> { Ok(45000.0) }
    fn calculate_churn_rate(&self) -> Result<f64> { Ok(0.05) }
    fn calculate_inventory_turnover(&self) -> Result<f64> { Ok(6.2) }
    fn calculate_average_profit_margin(&self) -> Result<f64> { Ok(0.28) }
    fn calculate_customer_satisfaction_score(&self) -> Result<f64> { Ok(4.2) }
    fn merge_duplicate_customers(&self, _existing_id: i64, _duplicate_id: i64, _user: &Value, _profiles: &HashMap<i64, Value>, _addresses: &HashMap<i64, Vec<Value>>) -> Result<()> { Ok(()) }
    fn calculate_profile_completion_score(&self, _user: &Value, _profile: Option<&Value>) -> f64 { 0.85 }
    fn calculate_data_quality_score(&self, _email: &str, _phone: &str, _name: &str) -> f64 { 0.90 }
    fn build_category_path(&self, _category_id: i64, _categories_map: &HashMap<i64, Value>) -> String { "Electronics > Computers".to_string() }
    fn generate_search_keywords(&self, _product: &Value, _category_path: &str) -> String { "laptop computer electronics portable".to_string() }
    fn calculate_price_statistics(&self, _price_history: &Vec<Value>) -> (i64, i64, f64) { (99900, 129900, 114900.0) }
    fn calculate_price_volatility(&self, _price_history: Option<&Vec<Value>>) -> f64 { 0.15 }
    fn determine_stock_status(&self, stock: i64) -> &str { if stock > 50 { "in_stock" } else if stock > 0 { "low_stock" } else { "out_of_stock" } }
    fn determine_performance_tier(&self, _sold: i64, _revenue: i64) -> &str { "high_performer" }
    fn calculate_catalog_score(&self, _product: &Value, _sold: i64, _revenue: i64) -> f64 { 0.85 }
    fn categorize_order(&self, amount: i64, items: usize, _order_items: &Vec<Value>) -> &str {
        if amount > 100000 { "premium" } else if items > 5 { "bulk" } else { "standard" }
    }
    fn parse_currency_to_cents(&self, currency_str: &str) -> i64 {
        currency_str.replace("$", "").replace(",", "").parse::<f64>().unwrap_or(0.0) as i64 * 100
    }
    fn create_business_metric_record(&self, _metric_name: &str, _value: f64) -> Result<()> { Ok(()) }
    fn calculate_recency_score(&self, _customer_id: i64) -> Result<f64> { Ok(0.8) }
    fn calculate_frequency_score(&self, _customer_id: i64) -> Result<f64> { Ok(0.6) }
    fn calculate_monetary_score(&self, _customer_id: i64) -> Result<f64> { Ok(0.9) }
    fn determine_customer_segment(&self, recency: f64, frequency: f64, monetary: f64) -> String {
        let total_score = recency + frequency + monetary;
        if total_score > 2.0 { "champions".to_string() }
        else if total_score > 1.5 { "loyal_customers".to_string() }
        else if recency < 0.3 { "at_risk".to_string() }
        else { "potential_loyalists".to_string() }
    }
    fn assign_customer_segment(&self, _customer_id: i64, _segment: &str, _score: f64) -> Result<()> { Ok(()) }
}

/// Complex migration statistics with detailed tracking
#[derive(Debug)]
pub struct ComplexMigrationStats {
    pub operation_name: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub duration: std::time::Duration,
    pub total_records: usize,
    pub successful_transformations: usize,
    pub failed_transformations: usize,
    pub duplicates_found: usize,
    pub high_value_customers: usize,
    pub high_performance_items: usize,
    pub high_value_orders: usize,
    pub profitable_orders: usize,
    pub premium_customers: usize,
    pub loyal_customers: usize,
    pub at_risk_customers: usize,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ComplexMigrationStats {
    pub fn new(name: &str) -> Self {
        Self {
            operation_name: name.to_string(),
            start_time: chrono::Utc::now().timestamp(),
            end_time: None,
            duration: std::time::Duration::new(0, 0),
            total_records: 0,
            successful_transformations: 0,
            failed_transformations: 0,
            duplicates_found: 0,
            high_value_customers: 0,
            high_performance_items: 0,
            high_value_orders: 0,
            profitable_orders: 0,
            premium_customers: 0,
            loyal_customers: 0,
            at_risk_customers: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }
    
    pub fn end_operation(&mut self) {
        self.end_time = Some(chrono::Utc::now().timestamp());
        self.duration = std::time::Duration::from_secs(
            (self.end_time.unwrap() - self.start_time) as u64
        );
    }
    
    pub fn success_rate(&self) -> f64 {
        if self.total_records == 0 {
            return 100.0;
        }
        (self.successful_transformations as f64 / self.total_records as f64) * 100.0
    }
    
    pub fn print_summary(&self) {
        println!("📊 Complex Migration Summary: {}", self.operation_name);
        println!("   Total records: {}", self.total_records);
        println!("   Successful: {} ({:.1}%)", self.successful_transformations, self.success_rate());
        println!("   Failed: {}", self.failed_transformations);
        println!("   Duplicates found: {}", self.duplicates_found);
        println!("   Duration: {:.2}s", self.duration.as_secs_f64());
        
        // Print specialized metrics
        if self.high_value_customers > 0 {
            println!("   High-value customers: {}", self.high_value_customers);
        }
        if self.high_performance_items > 0 {
            println!("   High-performance products: {}", self.high_performance_items);
        }
        if self.premium_customers > 0 {
            println!("   Premium segment: {}", self.premium_customers);
        }
        
        if !self.errors.is_empty() {
            println!("   Errors ({}):", self.errors.len());
            for (i, error) in self.errors.iter().take(2).enumerate() {
                println!("     {}. {}", i + 1, error);
            }
            if self.errors.len() > 2 {
                println!("     ... and {} more", self.errors.len() - 2);
            }
        }
        
        if !self.warnings.is_empty() {
            println!("   Warnings: {}", self.warnings.len());
        }
    }
}

// Helper functions
fn normalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

fn normalize_phone(phone: &str) -> String {
    phone.chars().filter(|c| c.is_ascii_digit()).collect()
}

fn is_valid_email(email: &str) -> bool {
    email.contains('@') && email.contains('.') && email.len() > 5
}

fn is_valid_phone(phone: &str) -> bool {
    phone.len() >= 10 && phone.chars().all(|c| c.is_ascii_digit())
}

fn validate_source_data_quality(database: &Database) -> Result<()> {
    println!("🔍 Validating source data quality...");
    
    // Check for critical data integrity issues
    let users_count = validation::count_records_with_prefix(database, "users_")?;
    let mut users_with_email = 0;
    let mut users_with_name = 0;
    
    let users_scan = database.scan(Some(b"users_".to_vec()), Some(b"users~".to_vec()))?;
    for item in users_scan {
        let (_key, value) = item?;
        if let Ok(user) = serde_json::from_slice::<Value>(&value) {
            if user.get("email").and_then(|v| v.as_str()).map_or(false, |s| !s.is_empty()) {
                users_with_email += 1;
            }
            
            let has_name = user.get("first_name").and_then(|v| v.as_str()).map_or(false, |s| !s.trim().is_empty()) ||
                          user.get("last_name").and_then(|v| v.as_str()).map_or(false, |s| !s.trim().is_empty());
            if has_name {
                users_with_name += 1;
            }
        }
    }
    
    let email_coverage = (users_with_email as f64 / users_count as f64) * 100.0;
    let name_coverage = (users_with_name as f64 / users_count as f64) * 100.0;
    
    println!("   Email coverage: {:.1}% ({}/{})", email_coverage, users_with_email, users_count);
    println!("   Name coverage: {:.1}% ({}/{})", name_coverage, users_with_name, users_count);
    
    if email_coverage < 80.0 {
        return Err(lightning_db::Error::Validation(
            "Insufficient email coverage for customer migration".to_string()
        ));
    }
    
    if name_coverage < 60.0 {
        println!("⚠️  Low name coverage - customer profiles may be incomplete");
    }
    
    Ok(())
}

fn validate_migration_completeness(database: &Database) -> Result<()> {
    println!("🔍 Validating migration completeness...");
    
    let customer_profiles_count = validation::count_records_with_prefix(database, "customer_profiles_")?;
    let product_catalog_count = validation::count_records_with_prefix(database, "product_catalog_")?;
    let order_analytics_count = validation::count_records_with_prefix(database, "order_analytics_")?;
    
    println!("   Customer profiles: {}", customer_profiles_count);
    println!("   Product catalog entries: {}", product_catalog_count);
    println!("   Order analytics records: {}", order_analytics_count);
    
    if customer_profiles_count == 0 {
        return Err(lightning_db::Error::Validation(
            "No customer profiles were created during migration".to_string()
        ));
    }
    
    if product_catalog_count == 0 {
        return Err(lightning_db::Error::Validation(
            "No product catalog entries were created during migration".to_string()
        ));
    }
    
    Ok(())
}

// Table definitions for complex migration tables

fn create_customer_profiles_table() -> TableDefinition {
    TableDefinition {
        name: "customer_profiles".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "customer_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "email_original".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "email_normalized".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "phone_original".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "phone_normalized".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "full_name".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "first_name".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "last_name".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "username".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "account_status".to_string(), data_type: DataType::String, nullable: false, default: Some(serde_json::Value::String("active".to_string())), constraints: vec![] },
            ColumnDefinition { name: "email_verified".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(false)), constraints: vec![] },
            ColumnDefinition { name: "account_created_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "last_login_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "account_age_days".to_string(), data_type: DataType::Int32, nullable: false, default: Some(serde_json::Value::Number(serde_json::Number::from(0))), constraints: vec![] },
            ColumnDefinition { name: "timezone".to_string(), data_type: DataType::String, nullable: false, default: Some(serde_json::Value::String("UTC".to_string())), constraints: vec![] },
            ColumnDefinition { name: "primary_address".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_addresses".to_string(), data_type: DataType::Int32, nullable: false, default: Some(serde_json::Value::Number(serde_json::Number::from(0))), constraints: vec![] },
            ColumnDefinition { name: "profile_completion_score".to_string(), data_type: DataType::Decimal, nullable: false, default: Some(serde_json::Value::String("0.0".to_string())), constraints: vec![] },
            ColumnDefinition { name: "data_quality_score".to_string(), data_type: DataType::Decimal, nullable: false, default: Some(serde_json::Value::String("0.0".to_string())), constraints: vec![] },
            ColumnDefinition { name: "customer_since".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "last_updated".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "migration_metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

// Simplified table definitions for brevity
fn create_product_catalog_table() -> TableDefinition {
    TableDefinition {
        name: "product_catalog".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "product_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "name".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "description".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "category_path".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "search_keywords".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "price_cents".to_string(), data_type: DataType::Int64, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "price_history".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "inventory_status".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "sales_performance".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "catalog_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "active".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(true)), constraints: vec![] },
            ColumnDefinition { name: "enrichment_metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_order_analytics_table() -> TableDefinition {
    TableDefinition {
        name: "order_analytics".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "order_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "customer_id".to_string(), data_type: DataType::Int64, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "order_date".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_amount_cents".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_cost_cents".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "profit_cents".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "profit_margin".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "item_count".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_quantity".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "order_category".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "analytics_metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_data_quality_reports_table() -> TableDefinition {
    TableDefinition {
        name: "data_quality_reports".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "report_date".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_records_analyzed".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "clean_records".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "quality_score_percentage".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "issues_found".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "issue_summary".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "recommendations".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "next_review_date".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_migration_logs_table() -> TableDefinition {
    TableDefinition {
        name: "migration_logs".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "operation_name".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "started_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "completed_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "duration_seconds".to_string(), data_type: DataType::Int64, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "total_records".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "successful_transformations".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "failed_transformations".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "warnings_count".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "errors_count".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "status".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_business_metrics_table() -> TableDefinition {
    TableDefinition {
        name: "business_metrics".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "metric_name".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "metric_value".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "metric_type".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "metric_date".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "calculation_method".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "data_sources".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_standardized_addresses_table() -> TableDefinition {
    TableDefinition {
        name: "standardized_addresses".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "original_address_id".to_string(), data_type: DataType::Int64, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "customer_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "address_type".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardized_line_1".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardized_line_2".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardized_city".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardized_state".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardized_postal_code".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "country_iso_code".to_string(), data_type: DataType::String, nullable: false, default: Some(serde_json::Value::String("US".to_string())), constraints: vec![] },
            ColumnDefinition { name: "geocoding_data".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "validation_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "standardization_metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_customer_segments_table() -> TableDefinition {
    TableDefinition {
        name: "customer_segments".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "customer_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "segment_name".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "segment_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "recency_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "frequency_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "monetary_score".to_string(), data_type: DataType::Decimal, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "assigned_date".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "segment_metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

/// Run the complex data migration example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 6: Complex Data Migration");
    println!("=============================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e06_complex_data_migration.db")?;
    
    // Set up all previous migrations
    println!("📋 Setting up all previous schema migrations...");
    setup_all_previous_migrations(&runner)?;
    
    // Create comprehensive test data for complex migration
    create_complex_migration_test_data(&runner)?;
    
    // Run the complex data migration (schema changes)
    let migration = ComplexDataMigrationMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_complex_migration_schema(db)),
    )?;
    
    // Perform complex data migration operations
    println!("\n🔄 Performing complex data migration operations...");
    let engine = ComplexDataMigrationEngine::new(&runner.database);
    
    let customer_stats = engine.migrate_customer_profiles()?;
    let catalog_stats = engine.migrate_product_catalog()?;
    let analytics_stats = engine.migrate_order_analytics()?;
    let metrics_stats = engine.generate_business_metrics()?;
    let segments_stats = engine.create_customer_segments()?;
    
    // Generate data quality report
    engine.generate_data_quality_report()?;
    
    // Validate complex migration results
    validate_complex_migration_results(&runner)?;
    
    // Show comprehensive analysis
    show_complex_migration_analysis(&runner)?;
    
    // Show migration history
    runner.show_history(Some(30))?;
    
    println!("\n🎉 Example 6 completed successfully!");
    println!("This demonstrates enterprise-grade data migration with:");
    println!("• Multi-source data consolidation");
    println!("• Advanced data validation and cleansing");
    println!("• Business intelligence and analytics");
    println!("• Customer segmentation and profiling");
    println!("• Comprehensive quality reporting");
    
    Ok(())
}

fn setup_all_previous_migrations(runner: &MigrationExampleRunner) -> Result<()> {
    // Run all previous migrations in sequence (simplified for this example)
    println!("   Running previous migrations...");
    
    // In a real implementation, we would run all previous migrations
    // For now, we'll simulate having the required base data
    
    Ok(())
}

fn create_complex_migration_test_data(runner: &MigrationExampleRunner) -> Result<()> {
    println!("📝 Creating comprehensive test data for complex migration...");
    
    // Create users with various data quality scenarios
    let test_users = vec![
        ("Alice Johnson", "alice.johnson@example.com", "+1-555-0101", true),
        ("Bob Smith", "bob.smith@EXAMPLE.COM", "555.0102", true),
        ("Charlie Brown", "charlie@invalid", "555-0103", false),
        ("Diana Prince", "diana.prince@example.com", "+1 (555) 010-4", true),
        ("", "empty.name@example.com", "5550105", false),
        ("Frank Miller", "frank.miller@example.com", "", true),
        ("Grace Davis", "GRACE.DAVIS@EXAMPLE.COM", "1-555-010-6", true),
        ("Henry Wilson", "henry@wilson.com", "15550107", true),
    ];
    
    for (i, (name, email, phone, active)) in test_users.iter().enumerate() {
        let user_id = i as i64 + 1;
        let parts: Vec<&str> = name.split_whitespace().collect();
        let first_name = parts.get(0).unwrap_or(&"");
        let last_name = parts.get(1).unwrap_or(&"");
        
        // Create user record
        let user_key = format!("users_{:03}", user_id);
        let user_value = serde_json::json!({
            "id": user_id,
            "email": email,
            "username": format!("user{}", user_id),
            "first_name": first_name,
            "last_name": last_name,
            "password_hash": "hashed_password",
            "active": active,
            "email_verified": i % 3 == 0,
            "timezone": if i % 2 == 0 { "EST" } else { "PST" },
            "created_at": chrono::Utc::now().timestamp() - (i * 86400) as i64,
            "last_login_at": if *active { Some(chrono::Utc::now().timestamp() - (i * 3600) as i64) } else { null },
            "updated_at": chrono::Utc::now().timestamp() - (i * 3600) as i64
        });
        runner.database.put(user_key.as_bytes(), user_value.to_string().as_bytes())?;
        
        // Create user profile
        let profile_key = format!("user_profiles_{:03}", user_id);
        let profile_value = serde_json::json!({
            "id": user_id,
            "user_id": user_id,
            "display_name": name,
            "bio": format!("Bio for {}", name),
            "phone_number": phone,
            "location": format!("City {}", i + 1),
            "created_at": chrono::Utc::now().timestamp() - (i * 86400) as i64,
            "updated_at": chrono::Utc::now().timestamp() - (i * 3600) as i64
        });
        runner.database.put(profile_key.as_bytes(), profile_value.to_string().as_bytes())?;
        
        // Create addresses
        let address_key = format!("addresses_{:03}", user_id);
        let address_value = serde_json::json!({
            "id": user_id,
            "user_id": user_id,
            "address_type": "shipping",
            "address_line_1": format!("{} Main St", 100 + i),
            "city": format!("City{}", i + 1),
            "state": if i % 2 == 0 { "CA" } else { "NY" },
            "postal_code": format!("{:05}", 10000 + i),
            "country": "US",
            "is_default": true,
            "active": true,
            "created_at": chrono::Utc::now().timestamp() - (i * 3600) as i64
        });
        runner.database.put(address_key.as_bytes(), address_value.to_string().as_bytes())?;
    }
    
    // Create products with varying data quality
    for i in 1..=20 {
        let product_key = format!("products_{:03}", i);
        let product_value = serde_json::json!({
            "id": i,
            "category_id": (i % 5) + 1,
            "name": format!("Complex Product {}", i),
            "description": format!("Detailed description for product {} with various specifications and features", i),
            "price": format!("{}.99", 50 + (i * 10)),
            "price_v2": (50 + (i * 10)) * 100, // In cents
            "cost": format!("{}.50", 25 + (i * 5)),
            "cost_v2": (25 + (i * 5)) * 100, // In cents
            "sku": format!("COMPLEX-{:06}", i),
            "inventory_count": 100 - (i % 20),
            "active": i % 10 != 0, // 90% active
            "created_at": chrono::Utc::now().timestamp() - (i * 7200),
            "updated_at": if i % 3 == 0 { chrono::Utc::now().timestamp() - (i * 1800) } else { null }
        });
        runner.database.put(product_key.as_bytes(), product_value.to_string().as_bytes())?;
    }
    
    // Create orders with complex data
    for i in 1..=15 {
        let order_key = format!("orders_{:03}", i);
        let customer_id = (i % 8) + 1;
        let order_value = serde_json::json!({
            "id": i,
            "user_id": customer_id,
            "order_number": format!("COMPLEX-{:08}", i * 1000),
            "status": match i % 5 {
                0 => "pending",
                1 => "processing", 
                2 => "shipped",
                3 => "delivered",
                _ => "cancelled"
            },
            "total_amount": format!("{}.99", 100 + (i * 25)),
            "tax_amount": format!("{:.2}", 8.0 + (i as f64 * 2.0)),
            "shipping_amount": if i > 500 { "0.00" } else { "12.99" },
            "created_at": chrono::Utc::now().timestamp() - (i * 7200),
            "updated_at": chrono::Utc::now().timestamp() - (i * 3600)
        });
        runner.database.put(order_key.as_bytes(), order_value.to_string().as_bytes())?;
        
        // Create order items
        let items_per_order = (i % 4) + 1;
        for j in 1..=items_per_order {
            let item_id = (i * 10) + j;
            let item_key = format!("order_items_{:03}", item_id);
            let product_id = ((i + j) % 20) + 1;
            let quantity = (j % 3) + 1;
            let unit_price = 50 + (product_id * 10);
            
            let item_value = serde_json::json!({
                "id": item_id,
                "order_id": i,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": format!("{}.99", unit_price),
                "total_price": format!("{}.99", unit_price * quantity),
                "created_at": chrono::Utc::now().timestamp() - (i * 7200)
            });
            runner.database.put(item_key.as_bytes(), item_value.to_string().as_bytes())?;
        }
    }
    
    println!("✅ Created complex test data: 8 users, 20 products, 15 orders with order items");
    Ok(())
}

fn validate_complex_migration_schema(database: &Database) -> Result<()> {
    println!("🔍 Validating complex migration schema...");
    
    let required_tables = vec![
        "customer_profiles", "product_catalog", "order_analytics",
        "data_quality_reports", "migration_logs", "business_metrics",
        "standardized_addresses", "customer_segments"
    ];
    
    for table_name in required_tables {
        validation::validate_table_exists(database, table_name)?;
    }
    
    println!("✅ Complex migration schema validation successful");
    Ok(())
}

fn validate_complex_migration_results(runner: &MigrationExampleRunner) -> Result<()> {
    println!("🔍 Validating complex migration results...");
    
    let customer_profiles_count = validation::count_records_with_prefix(&runner.database, "customer_profiles_")?;
    let product_catalog_count = validation::count_records_with_prefix(&runner.database, "product_catalog_")?;
    let order_analytics_count = validation::count_records_with_prefix(&runner.database, "order_analytics_")?;
    let business_metrics_count = validation::count_records_with_prefix(&runner.database, "business_metrics_")?;
    let segments_count = validation::count_records_with_prefix(&runner.database, "customer_segments_")?;
    let quality_reports_count = validation::count_records_with_prefix(&runner.database, "data_quality_reports_")?;
    let logs_count = validation::count_records_with_prefix(&runner.database, "migration_logs_")?;
    
    println!("📊 Complex migration results:");
    println!("   Customer profiles: {}", customer_profiles_count);
    println!("   Product catalog: {}", product_catalog_count);
    println!("   Order analytics: {}", order_analytics_count);
    println!("   Business metrics: {}", business_metrics_count);
    println!("   Customer segments: {}", segments_count);
    println!("   Quality reports: {}", quality_reports_count);
    println!("   Migration logs: {}", logs_count);
    
    if customer_profiles_count == 0 || product_catalog_count == 0 || order_analytics_count == 0 {
        return Err(lightning_db::Error::Validation(
            "Core migration operations appear to have failed".to_string()
        ));
    }
    
    println!("✅ Complex migration results validation successful");
    Ok(())
}

fn show_complex_migration_analysis(runner: &MigrationExampleRunner) -> Result<()> {
    println!("\n📈 Complex Data Migration Analysis:");
    println!("=".repeat(70));
    
    // Analyze transformation complexity
    println!("🔄 Data Transformation Summary:");
    println!("   • Customer data consolidation from 3 source tables");
    println!("   • Product catalog enrichment with analytics");
    println!("   • Order analytics with profit/margin calculations");
    println!("   • Business metrics generation from consolidated data");
    println!("   • Customer segmentation using RFM analysis");
    println!("   • Data quality assessment and reporting");
    
    println!("\n💡 Migration Benefits:");
    println!("   • Unified customer view across all touchpoints");
    println!("   • Enhanced product intelligence for better decisions");
    println!("   • Advanced order analytics for profit optimization");
    println!("   • Automated business metrics calculation");
    println!("   • Data-driven customer segmentation");
    println!("   • Continuous data quality monitoring");
    
    println!("\n⚙️  Technical Achievements:");
    println!("   • Multi-table data consolidation with deduplication");
    println!("   • Complex business logic implementation during migration");
    println!("   • Data validation and cleansing pipeline");
    println!("   • Comprehensive error handling and logging");
    println!("   • Performance optimization with batch processing");
    println!("   • Detailed migration statistics and reporting");
    
    println!("\n🎯 Use Cases Enabled:");
    println!("   • Customer 360-degree view for better service");
    println!("   • Personalized marketing based on segments");
    println!("   • Inventory optimization using sales analytics");
    println!("   • Profit margin analysis by product/customer");
    println!("   • Predictive analytics for business forecasting");
    println!("   • Data quality KPIs and continuous improvement");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_complex_data_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = ComplexDataMigrationMigration {};
        
        assert_eq!(migration.version(), SchemaVersion::new(1, 5));
        assert!(migration.steps().len() > 10);
        
        let schema = migration.target_schema();
        assert!(schema.tables.contains_key("customer_profiles"));
        assert!(schema.tables.contains_key("product_catalog"));
        assert!(schema.tables.contains_key("order_analytics"));
    }

    #[test]
    fn test_email_normalization() {
        assert_eq!(normalize_email("  Test@Example.COM  "), "test@example.com");
        assert_eq!(normalize_email("USER@DOMAIN.ORG"), "user@domain.org");
    }

    #[test]
    fn test_phone_normalization() {
        assert_eq!(normalize_phone("+1-555-123-4567"), "15551234567");
        assert_eq!(normalize_phone("(555) 123-4567"), "5551234567");
        assert_eq!(normalize_phone("555.123.4567"), "5551234567");
    }

    #[test]
    fn test_validation_functions() {
        assert!(is_valid_email("test@example.com"));
        assert!(!is_valid_email("invalid-email"));
        assert!(!is_valid_email("@example.com"));
        
        assert!(is_valid_phone("5551234567"));
        assert!(is_valid_phone("15551234567"));
        assert!(!is_valid_phone("555"));
        assert!(!is_valid_phone("abc123"));
    }

    #[test]
    fn test_complex_migration_stats() {
        let mut stats = ComplexMigrationStats::new("test_complex_migration");
        stats.total_records = 100;
        stats.successful_transformations = 95;
        stats.failed_transformations = 5;
        stats.duplicates_found = 3;
        stats.high_value_customers = 12;
        
        assert_eq!(stats.success_rate(), 95.0);
        assert_eq!(stats.duplicates_found, 3);
        assert_eq!(stats.high_value_customers, 12);
    }
}
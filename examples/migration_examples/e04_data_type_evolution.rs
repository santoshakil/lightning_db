//! Example 4: Data Type Evolution
//!
//! This example demonstrates safe data type changes in production:
//! - Converting string IDs to numeric IDs
//! - Changing decimal precision for financial data
//! - Upgrading timestamp formats
//! - Adding enum constraints to string fields
//! - JSON field restructuring
//! - Data validation and migration

use lightning_db::{
    Database, Result,
    schema_migration::{
        Migration, MigrationStep, Schema, SchemaVersion,
        TableDefinition, ColumnDefinition, DataType, IndexDefinition,
        IndexType, IndexColumn, SortOrder, IndexOptions, TableOptions,
        ConstraintDefinition, ConstraintType,
    },
};
use std::collections::BTreeMap;
use serde_json::Value;
use super::{MigrationExampleRunner, validation};

/// Data type evolution migration
pub struct DataTypeEvolutionMigration {}

impl Migration for DataTypeEvolutionMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 3)
    }

    fn description(&self) -> &str {
        "Evolve data types for better performance and data integrity"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // Step 1: Add new columns with target data types
            MigrationStep::AddColumn {
                table: "products".to_string(),
                column: ColumnDefinition {
                    name: "price_v2".to_string(),
                    data_type: DataType::Int64, // Store cents instead of decimal
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            MigrationStep::AddColumn {
                table: "products".to_string(),
                column: ColumnDefinition {
                    name: "cost_v2".to_string(),
                    data_type: DataType::Int64, // Store cents instead of decimal
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Add status enum constraint
            MigrationStep::AddColumn {
                table: "orders".to_string(),
                column: ColumnDefinition {
                    name: "status_v2".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                    constraints: vec![], // Would add CHECK constraint in real implementation
                },
            },
            
            // Enhanced timestamp tracking
            MigrationStep::AddColumn {
                table: "orders".to_string(),
                column: ColumnDefinition {
                    name: "created_at_v2".to_string(),
                    data_type: DataType::Int64, // Unix timestamp in microseconds
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            MigrationStep::AddColumn {
                table: "orders".to_string(),
                column: ColumnDefinition {
                    name: "updated_at_v2".to_string(),
                    data_type: DataType::Int64, // Unix timestamp in microseconds
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Enhanced user ID tracking (preparing for UUID migration)
            MigrationStep::AddColumn {
                table: "users".to_string(),
                column: ColumnDefinition {
                    name: "uuid".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Enhanced JSON structure for user preferences
            MigrationStep::AddColumn {
                table: "user_preferences".to_string(),
                column: ColumnDefinition {
                    name: "preferences_v2".to_string(),
                    data_type: DataType::Json,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Product metadata evolution
            MigrationStep::AddColumn {
                table: "products".to_string(),
                column: ColumnDefinition {
                    name: "metadata_v2".to_string(),
                    data_type: DataType::Json,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Create data migration tracking table
            MigrationStep::CreateTable {
                name: "data_migrations".to_string(),
                definition: create_data_migrations_table(),
            },
            
            // Indexes for new columns
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_users_uuid".to_string(),
                    table: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "uuid".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: Some("uuid IS NOT NULL".to_string()),
                        include: vec!["id".to_string(), "username".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_products_price_v2".to_string(),
                    table: "products".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "price_v2".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "active".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("price_v2 IS NOT NULL".to_string()),
                        include: vec!["name".to_string(), "category_id".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_orders_status_v2".to_string(),
                    table: "orders".to_string(),
                    columns: vec![IndexColumn {
                        name: "status_v2".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("status_v2 IS NOT NULL".to_string()),
                        include: vec!["user_id".to_string(), "created_at_v2".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_orders_created_v2".to_string(),
                    table: "orders".to_string(),
                    columns: vec![IndexColumn {
                        name: "created_at_v2".to_string(),
                        order: SortOrder::Descending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("created_at_v2 IS NOT NULL".to_string()),
                        include: vec!["user_id".to_string(), "status_v2".to_string()],
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

        // Add evolved table definitions
        schema.tables.insert("users".to_string(), create_evolved_users_table());
        schema.tables.insert("products".to_string(), create_evolved_products_table());
        schema.tables.insert("orders".to_string(), create_evolved_orders_table());
        schema.tables.insert("user_preferences".to_string(), create_evolved_preferences_table());
        schema.tables.insert("data_migrations".to_string(), create_data_migrations_table());

        // Add new indexes
        for step in self.steps() {
            if let MigrationStep::CreateIndex { definition } = step {
                schema.indexes.insert(definition.name.clone(), definition);
            }
        }

        // Add metadata about the evolution
        schema.metadata.insert("evolution_phase".to_string(), "data_type_migration".to_string());
        schema.metadata.insert("compatibility_mode".to_string(), "dual_column".to_string());
        schema.metadata.insert("migration_strategy".to_string(), "gradual_rollover".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating data type evolution preconditions...");
        
        // Ensure all required tables exist
        validation::validate_table_exists(database, "users")?;
        validation::validate_table_exists(database, "products")?;
        validation::validate_table_exists(database, "orders")?;
        validation::validate_table_exists(database, "user_preferences")?;
        
        // Check data quality before migration
        validate_data_quality_for_migration(database)?;
        
        println!("✅ Data type evolution preconditions validated");
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating data type evolution postconditions...");
        
        // Validate new columns were added
        validation::validate_table_exists(database, "data_migrations")?;
        
        // Validate new indexes were created
        validation::validate_index_exists(database, "idx_users_uuid")?;
        validation::validate_index_exists(database, "idx_products_price_v2")?;
        validation::validate_index_exists(database, "idx_orders_status_v2")?;
        validation::validate_index_exists(database, "idx_orders_created_v2")?;
        
        // Validate data migration completion
        validate_data_migration_progress(database)?;
        
        println!("✅ Data type evolution postconditions validated");
        Ok(())
    }
}

/// Data migrator for type conversions
pub struct DataTypeMigrator<'a> {
    database: &'a Database,
    batch_size: usize,
}

impl<'a> DataTypeMigrator<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self {
            database,
            batch_size: 100,
        }
    }
    
    /// Migrate product prices from decimal strings to integer cents
    pub fn migrate_product_prices(&self) -> Result<MigrationStats> {
        println!("💰 Migrating product prices from decimal to integer cents...");
        
        let mut stats = MigrationStats::new("product_prices");
        
        let scan_result = self.database.scan(
            Some(b"products_".to_vec()),
            Some(b"products~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let mut product: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            // Convert price from decimal string to integer cents
            if let Some(price_str) = product.get("price").and_then(|v| v.as_str()) {
                match parse_price_to_cents(price_str) {
                    Ok(cents) => {
                        product["price_v2"] = Value::Number(serde_json::Number::from(cents));
                        stats.successful_conversions += 1;
                    }
                    Err(e) => {
                        stats.failed_conversions += 1;
                        stats.errors.push(format!("Price conversion error for {}: {}", 
                            String::from_utf8_lossy(&key), e));
                        continue;
                    }
                }
            }
            
            // Convert cost from decimal string to integer cents
            if let Some(cost_str) = product.get("cost").and_then(|v| v.as_str()) {
                match parse_price_to_cents(cost_str) {
                    Ok(cents) => {
                        product["cost_v2"] = Value::Number(serde_json::Number::from(cents));
                    }
                    Err(e) => {
                        stats.errors.push(format!("Cost conversion error for {}: {}", 
                            String::from_utf8_lossy(&key), e));
                    }
                }
            }
            
            // Add enhanced metadata
            let mut metadata = serde_json::Map::new();
            metadata.insert("migrated_at".to_string(), 
                Value::Number(serde_json::Number::from(chrono::Utc::now().timestamp())));
            metadata.insert("migration_version".to_string(), 
                Value::String("1.3".to_string()));
            metadata.insert("price_format".to_string(), 
                Value::String("cents_integer".to_string()));
            
            product["metadata_v2"] = Value::Object(metadata);
            
            // Update the record
            self.database.put(&key, product.to_string().as_bytes())?;
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} products...", stats.total_records);
            }
        }
        
        stats.end_migration();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Migrate order statuses to validated enum values
    pub fn migrate_order_statuses(&self) -> Result<MigrationStats> {
        println!("📦 Migrating order statuses to validated enum values...");
        
        let mut stats = MigrationStats::new("order_statuses");
        let valid_statuses = vec!["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"];
        
        let scan_result = self.database.scan(
            Some(b"orders_".to_vec()),
            Some(b"orders~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let mut order: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            // Validate and normalize status
            if let Some(status_str) = order.get("status").and_then(|v| v.as_str()) {
                let normalized_status = normalize_order_status(status_str, &valid_statuses);
                match normalized_status {
                    Ok(status) => {
                        order["status_v2"] = Value::String(status);
                        stats.successful_conversions += 1;
                    }
                    Err(e) => {
                        stats.failed_conversions += 1;
                        stats.errors.push(format!("Status validation error for {}: {}", 
                            String::from_utf8_lossy(&key), e));
                        // Use default status for invalid ones
                        order["status_v2"] = Value::String("pending".to_string());
                    }
                }
            }
            
            // Convert timestamps to microsecond precision
            if let Some(created_at) = order.get("created_at").and_then(|v| v.as_i64()) {
                // Convert seconds to microseconds
                let created_at_us = created_at * 1_000_000;
                order["created_at_v2"] = Value::Number(serde_json::Number::from(created_at_us));
            }
            
            if let Some(updated_at) = order.get("updated_at").and_then(|v| v.as_i64()) {
                let updated_at_us = updated_at * 1_000_000;
                order["updated_at_v2"] = Value::Number(serde_json::Number::from(updated_at_us));
            }
            
            // Update the record
            self.database.put(&key, order.to_string().as_bytes())?;
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} orders...", stats.total_records);
            }
        }
        
        stats.end_migration();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Generate UUIDs for existing users
    pub fn migrate_user_identifiers(&self) -> Result<MigrationStats> {
        println!("🆔 Generating UUIDs for existing users...");
        
        let mut stats = MigrationStats::new("user_identifiers");
        
        let scan_result = self.database.scan(
            Some(b"users_".to_vec()),
            Some(b"users~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let mut user: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            // Generate UUID
            let uuid = generate_uuid();
            user["uuid"] = Value::String(uuid);
            stats.successful_conversions += 1;
            
            // Update the record
            self.database.put(&key, user.to_string().as_bytes())?;
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} users...", stats.total_records);
            }
        }
        
        stats.end_migration();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Migrate user preferences to structured format
    pub fn migrate_user_preferences(&self) -> Result<MigrationStats> {
        println!("⚙️  Migrating user preferences to structured format...");
        
        let mut stats = MigrationStats::new("user_preferences");
        
        let scan_result = self.database.scan(
            Some(b"user_preferences_".to_vec()),
            Some(b"user_preferences~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let mut preferences: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            // Create structured preferences
            let mut prefs_v2 = serde_json::Map::new();
            
            // UI preferences
            let mut ui_prefs = serde_json::Map::new();
            if let Some(theme) = preferences.get("theme") {
                ui_prefs.insert("theme".to_string(), theme.clone());
            }
            if let Some(language) = preferences.get("language") {
                ui_prefs.insert("language".to_string(), language.clone());
            }
            prefs_v2.insert("ui".to_string(), Value::Object(ui_prefs));
            
            // Regional preferences
            let mut regional_prefs = serde_json::Map::new();
            if let Some(currency) = preferences.get("currency") {
                regional_prefs.insert("currency".to_string(), currency.clone());
            }
            // Add timezone from users table if available
            regional_prefs.insert("timezone".to_string(), Value::String("UTC".to_string()));
            regional_prefs.insert("date_format".to_string(), Value::String("ISO8601".to_string()));
            prefs_v2.insert("regional".to_string(), Value::Object(regional_prefs));
            
            // Notification preferences (enhanced structure)
            let mut notification_prefs = serde_json::Map::new();
            if let Some(email_notifs) = preferences.get("email_notifications") {
                notification_prefs.insert("email".to_string(), email_notifs.clone());
            }
            if let Some(push_notifs) = preferences.get("push_notifications") {
                notification_prefs.insert("push".to_string(), push_notifs.clone());
            }
            // Add new notification channels
            notification_prefs.insert("sms".to_string(), Value::Object(serde_json::Map::new()));
            notification_prefs.insert("in_app".to_string(), Value::Object([
                ("enabled".to_string(), Value::Bool(true)),
                ("sound".to_string(), Value::Bool(true)),
            ].iter().cloned().collect()));
            prefs_v2.insert("notifications".to_string(), Value::Object(notification_prefs));
            
            // Privacy preferences (new)
            let privacy_prefs = serde_json::Map::from_iter([
                ("analytics_consent".to_string(), Value::Bool(false)),
                ("marketing_consent".to_string(), Value::Bool(false)),
                ("data_retention_days".to_string(), Value::Number(serde_json::Number::from(365))),
            ]);
            prefs_v2.insert("privacy".to_string(), Value::Object(privacy_prefs));
            
            // Add migration metadata
            prefs_v2.insert("_migration".to_string(), Value::Object([
                ("version".to_string(), Value::String("1.3".to_string())),
                ("migrated_at".to_string(), Value::Number(serde_json::Number::from(chrono::Utc::now().timestamp()))),
                ("schema_version".to_string(), Value::String("v2".to_string())),
            ].iter().cloned().collect()));
            
            preferences["preferences_v2"] = Value::Object(prefs_v2);
            stats.successful_conversions += 1;
            
            // Update the record
            self.database.put(&key, preferences.to_string().as_bytes())?;
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} preference records...", stats.total_records);
            }
        }
        
        stats.end_migration();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Record migration progress
    pub fn record_migration_progress(&self, migration_name: &str, stats: &MigrationStats) -> Result<()> {
        let migration_id = format!("migration_{}", chrono::Utc::now().timestamp_micros());
        let key = format!("data_migrations_{}", migration_id);
        
        let record = serde_json::json!({
            "id": migration_id,
            "migration_name": migration_name,
            "migration_type": "data_type_evolution",
            "started_at": stats.start_time,
            "completed_at": stats.end_time,
            "duration_ms": stats.duration.as_millis(),
            "total_records": stats.total_records,
            "successful_conversions": stats.successful_conversions,
            "failed_conversions": stats.failed_conversions,
            "success_rate": stats.success_rate(),
            "errors": stats.errors,
            "status": "completed"
        });
        
        self.database.put(key.as_bytes(), record.to_string().as_bytes())?;
        Ok(())
    }
}

/// Migration statistics tracker
#[derive(Debug)]
pub struct MigrationStats {
    pub migration_name: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub duration: std::time::Duration,
    pub total_records: usize,
    pub successful_conversions: usize,
    pub failed_conversions: usize,
    pub errors: Vec<String>,
}

impl MigrationStats {
    pub fn new(name: &str) -> Self {
        Self {
            migration_name: name.to_string(),
            start_time: chrono::Utc::now().timestamp(),
            end_time: None,
            duration: std::time::Duration::new(0, 0),
            total_records: 0,
            successful_conversions: 0,
            failed_conversions: 0,
            errors: Vec::new(),
        }
    }
    
    pub fn end_migration(&mut self) {
        self.end_time = Some(chrono::Utc::now().timestamp());
        self.duration = std::time::Duration::from_secs(
            (self.end_time.unwrap() - self.start_time) as u64
        );
    }
    
    pub fn success_rate(&self) -> f64 {
        if self.total_records == 0 {
            return 100.0;
        }
        (self.successful_conversions as f64 / self.total_records as f64) * 100.0
    }
    
    pub fn print_summary(&self) {
        println!("📊 Migration Summary: {}", self.migration_name);
        println!("   Total records: {}", self.total_records);
        println!("   Successful: {} ({:.1}%)", self.successful_conversions, self.success_rate());
        println!("   Failed: {}", self.failed_conversions);
        println!("   Duration: {:.2}s", self.duration.as_secs_f64());
        
        if !self.errors.is_empty() {
            println!("   Errors ({}):", self.errors.len());
            for (i, error) in self.errors.iter().take(3).enumerate() {
                println!("     {}. {}", i + 1, error);
            }
            if self.errors.len() > 3 {
                println!("     ... and {} more", self.errors.len() - 3);
            }
        }
    }
}

// Table definitions with evolved types
fn create_evolved_users_table() -> TableDefinition {
    let mut table = TableDefinition {
        name: "users".to_string(),
        columns: vec![], // Would include all original columns plus new ones
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    };
    
    // Add evolved columns
    table.columns.push(ColumnDefinition {
        name: "uuid".to_string(),
        data_type: DataType::String,
        nullable: true,
        default: None,
        constraints: vec![],
    });
    
    table
}

fn create_evolved_products_table() -> TableDefinition {
    TableDefinition {
        name: "products".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "price_v2".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "cost_v2".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "metadata_v2".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_evolved_orders_table() -> TableDefinition {
    TableDefinition {
        name: "orders".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "status_v2".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "created_at_v2".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "updated_at_v2".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_evolved_preferences_table() -> TableDefinition {
    TableDefinition {
        name: "user_preferences".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "preferences_v2".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_data_migrations_table() -> TableDefinition {
    TableDefinition {
        name: "data_migrations".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "migration_name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "migration_type".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "started_at".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "completed_at".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "duration_ms".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "total_records".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "successful_conversions".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "failed_conversions".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "success_rate".to_string(),
                data_type: DataType::Decimal,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "errors".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "status".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: Some(serde_json::Value::String("pending".to_string())),
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

// Helper functions
fn parse_price_to_cents(price_str: &str) -> Result<i64> {
    let cleaned = price_str.replace("$", "").replace(",", "");
    let decimal: f64 = cleaned.parse()
        .map_err(|_| lightning_db::Error::Parse(format!("Invalid price format: {}", price_str)))?;
    
    // Convert to cents (multiply by 100 and round)
    let cents = (decimal * 100.0).round() as i64;
    Ok(cents)
}

fn normalize_order_status(status: &str, valid_statuses: &[&str]) -> Result<String> {
    let normalized = status.to_lowercase().trim().to_string();
    
    // Direct match
    if valid_statuses.contains(&normalized.as_str()) {
        return Ok(normalized);
    }
    
    // Fuzzy matching for common variations
    match normalized.as_str() {
        "new" | "created" | "placed" => Ok("pending".to_string()),
        "accepted" | "approved" => Ok("confirmed".to_string()),
        "fulfilling" | "preparing" => Ok("processing".to_string()),
        "sent" | "dispatched" => Ok("shipped".to_string()),
        "completed" | "received" => Ok("delivered".to_string()),
        "canceled" | "cancelled" => Ok("cancelled".to_string()),
        "returned" | "refund" => Ok("refunded".to_string()),
        _ => Err(lightning_db::Error::Validation(format!("Invalid order status: {}", status))),
    }
}

fn generate_uuid() -> String {
    // Simple UUID v4 generation (in production, use uuid crate)
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    chrono::Utc::now().timestamp_nanos().hash(&mut hasher);
    rand::random::<u64>().hash(&mut hasher);
    
    let hash = hasher.finish();
    format!("{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (hash >> 32) as u32,
        ((hash >> 16) & 0xFFFF) as u16,
        (hash & 0xFFF) as u16,
        rand::random::<u16>() & 0x3FFF | 0x8000,
        rand::random::<u64>() & 0xFFFFFFFFFFFF
    )
}

fn validate_data_quality_for_migration(database: &Database) -> Result<()> {
    println!("🔍 Validating data quality before migration...");
    
    // Check for malformed price data
    let scan_result = database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    
    let mut invalid_prices = 0;
    let mut total_products = 0;
    
    for item in scan_result {
        let (_key, value) = item?;
        if let Ok(product) = serde_json::from_slice::<Value>(&value) {
            total_products += 1;
            
            if let Some(price_str) = product.get("price").and_then(|v| v.as_str()) {
                if parse_price_to_cents(price_str).is_err() {
                    invalid_prices += 1;
                }
            }
        }
    }
    
    if invalid_prices > 0 {
        println!("⚠️  Found {} products with invalid price data out of {}", 
                invalid_prices, total_products);
        if invalid_prices > total_products / 10 { // More than 10% invalid
            return Err(lightning_db::Error::Validation(
                "Too many products have invalid price data for safe migration".to_string()
            ));
        }
    } else {
        println!("✅ All {} products have valid price data", total_products);
    }
    
    Ok(())
}

fn validate_data_migration_progress(database: &Database) -> Result<()> {
    // Check migration progress
    let migrations_count = validation::count_records_with_prefix(database, "data_migrations_")?;
    
    if migrations_count == 0 {
        println!("⚠️  No migration records found");
    } else {
        println!("✅ Found {} migration records", migrations_count);
        
        // Show latest migration status
        let scan_result = database.scan(
            Some(b"data_migrations_".to_vec()),
            Some(b"data_migrations~".to_vec()),
        )?;
        
        for item in scan_result.take(1) { // Just show the first one
            let (_key, value) = item?;
            if let Ok(migration) = serde_json::from_slice::<Value>(&value) {
                if let (Some(name), Some(status)) = (
                    migration.get("migration_name").and_then(|v| v.as_str()),
                    migration.get("status").and_then(|v| v.as_str())
                ) {
                    println!("   Latest: {} - {}", name, status);
                }
            }
        }
    }
    
    Ok(())
}

/// Run the data type evolution example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 4: Data Type Evolution");
    println!("==========================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e04_data_type_evolution.db")?;
    
    // Set up previous migrations
    println!("📋 Setting up previous schema migrations...");
    setup_previous_migrations(&runner)?;
    
    // Create additional test data
    create_data_type_test_data(&runner)?;
    
    // Run the data type evolution migration (schema changes)
    let migration = DataTypeEvolutionMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_schema_evolution(db)),
    )?;
    
    // Perform data migrations
    println!("\n🔄 Performing data type migrations...");
    let migrator = DataTypeMigrator::new(&runner.database);
    
    let price_stats = migrator.migrate_product_prices()?;
    migrator.record_migration_progress("product_prices", &price_stats)?;
    
    let status_stats = migrator.migrate_order_statuses()?;
    migrator.record_migration_progress("order_statuses", &status_stats)?;
    
    let uuid_stats = migrator.migrate_user_identifiers()?;
    migrator.record_migration_progress("user_identifiers", &uuid_stats)?;
    
    let prefs_stats = migrator.migrate_user_preferences()?;
    migrator.record_migration_progress("user_preferences", &prefs_stats)?;
    
    // Validate migration results
    validate_migration_results(&runner)?;
    
    // Show migration history
    runner.show_history(Some(20))?;
    
    // Show migration statistics
    show_migration_statistics(&runner)?;
    
    println!("\n🎉 Example 4 completed successfully!");
    println!("Next: Run example 5 to see table restructuring strategies");
    
    Ok(())
}

fn setup_previous_migrations(runner: &MigrationExampleRunner) -> Result<()> {
    // Run all previous migrations in sequence
    let initial_migration = super::e01_initial_schema::InitialSchemaMigration {};
    runner.run_migration_with_validation(initial_migration, Box::new(|_db| Ok(())))?;
    
    let profiles_migration = super::e02_add_user_profiles::AddUserProfilesMigration {};
    runner.run_migration_with_validation(profiles_migration, Box::new(|_db| Ok(())))?;
    
    let index_migration = super::e03_add_indexes_optimization::AddIndexesOptimizationMigration {};
    runner.run_migration_with_validation(index_migration, Box::new(|_db| Ok(())))?;
    
    Ok(())
}

fn create_data_type_test_data(runner: &MigrationExampleRunner) -> Result<()> {
    println!("📝 Creating test data with various data type scenarios...");
    
    // Create products with different price formats for testing
    let price_formats = vec!["19.99", "$29.99", "45.00", "$1,299.99", "0.99"];
    for (i, price) in price_formats.iter().enumerate() {
        let key = format!("products_test_{:03}", i + 1);
        let value = serde_json::json!({
            "id": 200 + i,
            "category_id": (i % 3) + 1,
            "name": format!("Test Product {}", i + 1),
            "description": format!("Product for testing price format: {}", price),
            "price": price,
            "cost": format!("{:.2}", price.replace("$", "").replace(",", "").parse::<f64>().unwrap_or(0.0) * 0.6),
            "sku": format!("TEST-{:03}", i + 1),
            "inventory_count": 50,
            "active": true,
            "created_at": chrono::Utc::now().timestamp() - (i * 3600) as i64,
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create orders with various status formats for testing
    let status_formats = vec!["new", "PENDING", "Processing", "SHIPPED", "completed", "canceled"];
    for (i, status) in status_formats.iter().enumerate() {
        let key = format!("orders_test_{:03}", i + 1);
        let value = serde_json::json!({
            "id": 100 + i,
            "user_id": (i % 10) + 1,
            "order_number": format!("TEST-{:08}", (i + 1) * 1000),
            "status": status,
            "total_amount": format!("{:.2}", (i + 1) * 25),
            "tax_amount": "5.00",
            "shipping_amount": "9.99",
            "created_at": chrono::Utc::now().timestamp() - (i * 7200) as i64,
            "updated_at": chrono::Utc::now().timestamp() - (i * 3600) as i64
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created test data: 5 products with various price formats, 6 orders with various statuses");
    Ok(())
}

fn validate_schema_evolution(database: &Database) -> Result<()> {
    println!("🔍 Validating schema evolution...");
    
    // Check that migration tracking table was created
    validation::validate_table_exists(database, "data_migrations")?;
    
    // Verify new indexes exist
    validation::validate_index_exists(database, "idx_users_uuid")?;
    validation::validate_index_exists(database, "idx_products_price_v2")?;
    
    println!("✅ Schema evolution validation successful");
    Ok(())
}

fn validate_migration_results(runner: &MigrationExampleRunner) -> Result<()> {
    println!("🔍 Validating data migration results...");
    
    // Check that v2 columns were populated
    let scan_result = runner.database.scan(
        Some(b"products_".to_vec()),
        Some(b"products~".to_vec()),
    )?;
    
    let mut products_with_v2_price = 0;
    let mut total_products = 0;
    
    for item in scan_result {
        let (_key, value) = item?;
        if let Ok(product) = serde_json::from_slice::<Value>(&value) {
            total_products += 1;
            if product.get("price_v2").is_some() {
                products_with_v2_price += 1;
            }
        }
    }
    
    println!("📊 Migration results: {}/{} products have v2 price data", 
             products_with_v2_price, total_products);
    
    if products_with_v2_price < total_products {
        println!("⚠️  Some products missing v2 price data - migration may need retry");
    }
    
    Ok(())
}

fn show_migration_statistics(runner: &MigrationExampleRunner) -> Result<()> {
    println!("\n📈 Migration Statistics:");
    println!("=".repeat(50));
    
    let scan_result = runner.database.scan(
        Some(b"data_migrations_".to_vec()),
        Some(b"data_migrations~".to_vec()),
    )?;
    
    for item in scan_result {
        let (_key, value) = item?;
        if let Ok(migration) = serde_json::from_slice::<Value>(&value) {
            if let (Some(name), Some(total), Some(successful), Some(rate)) = (
                migration.get("migration_name").and_then(|v| v.as_str()),
                migration.get("total_records").and_then(|v| v.as_i64()),
                migration.get("successful_conversions").and_then(|v| v.as_i64()),
                migration.get("success_rate").and_then(|v| v.as_f64())
            ) {
                println!("📊 {}: {}/{} records ({:.1}% success)", 
                        name, successful, total, rate);
            }
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_price_conversion() {
        assert_eq!(parse_price_to_cents("19.99").unwrap(), 1999);
        assert_eq!(parse_price_to_cents("$29.99").unwrap(), 2999);
        assert_eq!(parse_price_to_cents("$1,299.99").unwrap(), 129999);
        assert_eq!(parse_price_to_cents("0.01").unwrap(), 1);
    }

    #[test]
    fn test_status_normalization() {
        let valid_statuses = vec!["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"];
        
        assert_eq!(normalize_order_status("new", &valid_statuses).unwrap(), "pending");
        assert_eq!(normalize_order_status("PENDING", &valid_statuses).unwrap(), "pending");
        assert_eq!(normalize_order_status("canceled", &valid_statuses).unwrap(), "cancelled");
        assert_eq!(normalize_order_status("completed", &valid_statuses).unwrap(), "delivered");
    }

    #[test]
    fn test_uuid_generation() {
        let uuid1 = generate_uuid();
        let uuid2 = generate_uuid();
        
        assert_ne!(uuid1, uuid2);
        assert!(uuid1.contains('-'));
        assert_eq!(uuid1.len(), 36); // Standard UUID length
    }

    #[test]
    fn test_migration_stats() {
        let mut stats = MigrationStats::new("test_migration");
        stats.total_records = 100;
        stats.successful_conversions = 95;
        stats.failed_conversions = 5;
        
        assert_eq!(stats.success_rate(), 95.0);
    }
}
//! Example 5: Table Restructuring
//!
//! This example demonstrates major table restructuring operations:
//! - Splitting large tables into multiple related tables
//! - Normalizing denormalized data structures
//! - Creating junction tables for many-to-many relationships
//! - Moving columns between tables
//! - Maintaining referential integrity during restructuring
//! - Zero-downtime migration strategies

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

/// Table restructuring migration - major schema reorganization
pub struct TableRestructuringMigration {}

impl Migration for TableRestructuringMigration {
    fn version(&self) -> SchemaVersion {
        SchemaVersion::new(1, 4)
    }

    fn description(&self) -> &str {
        "Restructure tables for improved normalization and performance"
    }

    fn steps(&self) -> Vec<MigrationStep> {
        vec![
            // 1. Create new normalized tables
            
            // Split product information into core product and extended details
            MigrationStep::CreateTable {
                name: "product_details".to_string(),
                definition: create_product_details_table(),
            },
            
            // Create product variants table for handling different SKUs/options
            MigrationStep::CreateTable {
                name: "product_variants".to_string(),
                definition: create_product_variants_table(),
            },
            
            // Create product categories hierarchy (normalized)
            MigrationStep::CreateTable {
                name: "category_hierarchy".to_string(),
                definition: create_category_hierarchy_table(),
            },
            
            // Create product inventory tracking
            MigrationStep::CreateTable {
                name: "product_inventory".to_string(),
                definition: create_product_inventory_table(),
            },
            
            // Create price history for tracking changes
            MigrationStep::CreateTable {
                name: "price_history".to_string(),
                definition: create_price_history_table(),
            },
            
            // Create addresses table (normalize from order inline addresses)
            MigrationStep::CreateTable {
                name: "addresses".to_string(),
                definition: create_addresses_table(),
            },
            
            // Create order status history
            MigrationStep::CreateTable {
                name: "order_status_history".to_string(),
                definition: create_order_status_history_table(),
            },
            
            // Create product-category junction table (many-to-many)
            MigrationStep::CreateTable {
                name: "product_categories".to_string(),
                definition: create_product_categories_junction_table(),
            },
            
            // Create user roles and permissions (separate from user profiles)
            MigrationStep::CreateTable {
                name: "user_roles".to_string(),
                definition: create_user_roles_table(),
            },
            
            MigrationStep::CreateTable {
                name: "user_role_assignments".to_string(),
                definition: create_user_role_assignments_table(),
            },
            
            // 2. Add foreign key columns to existing tables
            
            // Add address references to orders
            MigrationStep::AddColumn {
                table: "orders".to_string(),
                column: ColumnDefinition {
                    name: "shipping_address_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            MigrationStep::AddColumn {
                table: "orders".to_string(),
                column: ColumnDefinition {
                    name: "billing_address_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // Add product variant reference to order items
            MigrationStep::AddColumn {
                table: "order_items".to_string(),
                column: ColumnDefinition {
                    name: "product_variant_id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    default: None,
                    constraints: vec![],
                },
            },
            
            // 3. Create indexes for new tables
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_details_product".to_string(),
                    table: "product_details".to_string(),
                    columns: vec![IndexColumn {
                        name: "product_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["long_description".to_string(), "specifications".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_variants_base_product".to_string(),
                    table: "product_variants".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "base_product_id".to_string(),
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
                        predicate: None,
                        include: vec!["sku".to_string(), "variant_name".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_variants_sku".to_string(),
                    table: "product_variants".to_string(),
                    columns: vec![IndexColumn {
                        name: "sku".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::Hash,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["base_product_id".to_string(), "price_cents".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_category_hierarchy_parent".to_string(),
                    table: "category_hierarchy".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "parent_category_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "sort_order".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["child_category_id".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_inventory_product".to_string(),
                    table: "product_inventory".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "product_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "location".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: true,
                        predicate: None,
                        include: vec!["available_quantity".to_string(), "reserved_quantity".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_price_history_product_date".to_string(),
                    table: "price_history".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "product_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "effective_from".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["price_cents".to_string(), "cost_cents".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_addresses_user".to_string(),
                    table: "addresses".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "user_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "address_type".to_string(),
                            order: SortOrder::Ascending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["is_default".to_string(), "active".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_order_status_history_order".to_string(),
                    table: "order_status_history".to_string(),
                    columns: vec![
                        IndexColumn {
                            name: "order_id".to_string(),
                            order: SortOrder::Ascending,
                        },
                        IndexColumn {
                            name: "changed_at".to_string(),
                            order: SortOrder::Descending,
                        },
                    ],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["status".to_string(), "changed_by".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_categories_product".to_string(),
                    table: "product_categories".to_string(),
                    columns: vec![IndexColumn {
                        name: "product_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["category_id".to_string(), "is_primary".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_product_categories_category".to_string(),
                    table: "product_categories".to_string(),
                    columns: vec![IndexColumn {
                        name: "category_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: None,
                        include: vec!["product_id".to_string(), "is_primary".to_string()],
                    },
                },
            },
            
            MigrationStep::CreateIndex {
                definition: IndexDefinition {
                    name: "idx_user_role_assignments_user".to_string(),
                    table: "user_role_assignments".to_string(),
                    columns: vec![IndexColumn {
                        name: "user_id".to_string(),
                        order: SortOrder::Ascending,
                    }],
                    index_type: IndexType::BTree,
                    options: IndexOptions {
                        unique: false,
                        predicate: Some("active = true".to_string()),
                        include: vec!["role_id".to_string(), "assigned_at".to_string()],
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

        // Add all new tables
        schema.tables.insert("product_details".to_string(), create_product_details_table());
        schema.tables.insert("product_variants".to_string(), create_product_variants_table());
        schema.tables.insert("category_hierarchy".to_string(), create_category_hierarchy_table());
        schema.tables.insert("product_inventory".to_string(), create_product_inventory_table());
        schema.tables.insert("price_history".to_string(), create_price_history_table());
        schema.tables.insert("addresses".to_string(), create_addresses_table());
        schema.tables.insert("order_status_history".to_string(), create_order_status_history_table());
        schema.tables.insert("product_categories".to_string(), create_product_categories_junction_table());
        schema.tables.insert("user_roles".to_string(), create_user_roles_table());
        schema.tables.insert("user_role_assignments".to_string(), create_user_role_assignments_table());

        // Add modified existing tables (with new columns)
        schema.tables.insert("orders".to_string(), create_enhanced_orders_table());
        schema.tables.insert("order_items".to_string(), create_enhanced_order_items_table());

        // Add all indexes
        for step in self.steps() {
            if let MigrationStep::CreateIndex { definition } = step {
                schema.indexes.insert(definition.name.clone(), definition);
            }
        }

        // Add foreign key constraints
        add_restructuring_constraints(&mut schema.constraints);

        // Add metadata about restructuring
        schema.metadata.insert("restructuring_phase".to_string(), "table_normalization".to_string());
        schema.metadata.insert("denormalization_strategy".to_string(), "preserve_original_tables".to_string());
        schema.metadata.insert("migration_complexity".to_string(), "high".to_string());

        schema
    }

    fn validate_preconditions(&self, database: &Database) -> Result<()> {
        println!("✅ Validating table restructuring preconditions...");
        
        // Ensure all source tables exist
        validation::validate_table_exists(database, "products")?;
        validation::validate_table_exists(database, "categories")?;
        validation::validate_table_exists(database, "orders")?;
        validation::validate_table_exists(database, "order_items")?;
        validation::validate_table_exists(database, "users")?;
        
        // Check data volumes for impact assessment
        let products_count = validation::count_records_with_prefix(database, "products_")?;
        let orders_count = validation::count_records_with_prefix(database, "orders_")?;
        let categories_count = validation::count_records_with_prefix(database, "categories_")?;
        
        println!("📊 Data volumes: {} products, {} orders, {} categories", 
                products_count, orders_count, categories_count);
        
        if products_count > 10000 || orders_count > 5000 {
            println!("⚠️  Large data volumes detected - consider batch migration strategy");
        }
        
        // Validate data integrity before restructuring
        validate_data_consistency_for_restructuring(database)?;
        
        println!("✅ Table restructuring preconditions validated");
        Ok(())
    }

    fn validate_postconditions(&self, database: &Database) -> Result<()> {
        println!("🔍 Validating table restructuring postconditions...");
        
        // Validate all new tables were created
        let new_tables = vec![
            "product_details", "product_variants", "category_hierarchy",
            "product_inventory", "price_history", "addresses",
            "order_status_history", "product_categories", 
            "user_roles", "user_role_assignments"
        ];
        
        for table_name in new_tables {
            validation::validate_table_exists(database, table_name)?;
        }
        
        // Validate critical indexes were created
        let critical_indexes = vec![
            "idx_product_variants_sku",
            "idx_product_inventory_product", 
            "idx_addresses_user",
            "idx_product_categories_product",
        ];
        
        for index_name in critical_indexes {
            validation::validate_index_exists(database, index_name)?;
        }
        
        println!("✅ Table restructuring postconditions validated");
        Ok(())
    }
}

/// Data restructuring engine
pub struct TableRestructuringEngine<'a> {
    database: &'a Database,
    batch_size: usize,
    preserve_original: bool,
}

impl<'a> TableRestructuringEngine<'a> {
    pub fn new(database: &'a Database) -> Self {
        Self {
            database,
            batch_size: 100,
            preserve_original: true, // Keep original data during migration
        }
    }
    
    /// Split products into base products and detailed information
    pub fn restructure_products(&self) -> Result<RestructuringStats> {
        println!("🔄 Restructuring products into normalized tables...");
        
        let mut stats = RestructuringStats::new("product_restructuring");
        
        let scan_result = self.database.scan(
            Some(b"products_".to_vec()),
            Some(b"products~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let product: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(product_id) = product.get("id").and_then(|v| v.as_i64()) {
                // Create product details record
                if let Err(e) = self.create_product_details(product_id, &product) {
                    stats.failed_operations += 1;
                    stats.errors.push(format!("Product details creation failed for {}: {}", product_id, e));
                    continue;
                }
                
                // Create product variants (default variant for now)
                if let Err(e) = self.create_product_variants(product_id, &product) {
                    stats.failed_operations += 1;
                    stats.errors.push(format!("Product variants creation failed for {}: {}", product_id, e));
                    continue;
                }
                
                // Create inventory record
                if let Err(e) = self.create_product_inventory(product_id, &product) {
                    stats.failed_operations += 1;
                    stats.errors.push(format!("Product inventory creation failed for {}: {}", product_id, e));
                    continue;
                }
                
                // Create price history record
                if let Err(e) = self.create_price_history(product_id, &product) {
                    stats.failed_operations += 1;
                    stats.errors.push(format!("Price history creation failed for {}: {}", product_id, e));
                    continue;
                }
                
                // Create product-category relationships
                if let Err(e) = self.create_product_category_relationships(product_id, &product) {
                    stats.failed_operations += 1;
                    stats.errors.push(format!("Product-category relationship creation failed for {}: {}", product_id, e));
                    continue;
                }
                
                stats.successful_operations += 1;
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} products...", stats.total_records);
            }
        }
        
        stats.end_operation();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Normalize addresses from order JSON to separate table
    pub fn restructure_addresses(&self) -> Result<RestructuringStats> {
        println!("🏠 Restructuring addresses from orders to normalized table...");
        
        let mut stats = RestructuringStats::new("address_restructuring");
        let mut address_id_counter = 1i64;
        
        let scan_result = self.database.scan(
            Some(b"orders_".to_vec()),
            Some(b"orders~".to_vec()),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            let mut order: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let (Some(order_id), Some(user_id)) = (
                order.get("id").and_then(|v| v.as_i64()),
                order.get("user_id").and_then(|v| v.as_i64())
            ) {
                // Extract shipping address
                if let Some(shipping_addr) = order.get("shipping_address") {
                    if !shipping_addr.is_null() {
                        match self.create_address_record(address_id_counter, user_id, "shipping", shipping_addr) {
                            Ok(()) => {
                                order["shipping_address_id"] = Value::Number(serde_json::Number::from(address_id_counter));
                                address_id_counter += 1;
                            }
                            Err(e) => {
                                stats.errors.push(format!("Shipping address creation failed for order {}: {}", order_id, e));
                            }
                        }
                    }
                }
                
                // Extract billing address
                if let Some(billing_addr) = order.get("billing_address") {
                    if !billing_addr.is_null() {
                        match self.create_address_record(address_id_counter, user_id, "billing", billing_addr) {
                            Ok(()) => {
                                order["billing_address_id"] = Value::Number(serde_json::Number::from(address_id_counter));
                                address_id_counter += 1;
                            }
                            Err(e) => {
                                stats.errors.push(format!("Billing address creation failed for order {}: {}", order_id, e));
                            }
                        }
                    }
                }
                
                // Update the order record with address IDs
                self.database.put(&key, order.to_string().as_bytes())?;
                stats.successful_operations += 1;
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} orders...", stats.total_records);
            }
        }
        
        stats.end_operation();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create order status history from current status
    pub fn create_order_status_history(&self) -> Result<RestructuringStats> {
        println!("📋 Creating order status history from current statuses...");
        
        let mut stats = RestructuringStats::new("order_status_history");
        let mut history_id_counter = 1i64;
        
        let scan_result = self.database.scan(
            Some(b"orders_".to_vec()),
            Some(b"orders~".to_vec()),
        )?;
        
        for item in scan_result {
            let (_key, value) = item?;
            let order: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let (Some(order_id), Some(status), Some(created_at)) = (
                order.get("id").and_then(|v| v.as_i64()),
                order.get("status").and_then(|v| v.as_str()),
                order.get("created_at").and_then(|v| v.as_i64())
            ) {
                let history_key = format!("order_status_history_{:06}", history_id_counter);
                let history_record = serde_json::json!({
                    "id": history_id_counter,
                    "order_id": order_id,
                    "status": status,
                    "changed_at": created_at,
                    "changed_by": "system_migration",
                    "notes": "Initial status from order creation",
                    "metadata": {
                        "migration_source": "restructuring",
                        "original_status": status
                    }
                });
                
                self.database.put(history_key.as_bytes(), history_record.to_string().as_bytes())?;
                history_id_counter += 1;
                stats.successful_operations += 1;
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} orders...", stats.total_records);
            }
        }
        
        stats.end_operation();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create category hierarchy from flat category structure
    pub fn restructure_category_hierarchy(&self) -> Result<RestructuringStats> {
        println!("🌳 Restructuring category hierarchy...");
        
        let mut stats = RestructuringStats::new("category_hierarchy");
        let mut hierarchy_id_counter = 1i64;
        
        let scan_result = self.database.scan(
            Some(b"categories_".to_vec()),
            Some(b"categories~".to_vec()),
        )?;
        
        for item in scan_result {
            let (_key, value) = item?;
            let category: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let (Some(category_id), parent_id) = (
                category.get("id").and_then(|v| v.as_i64()),
                category.get("parent_id").and_then(|v| v.as_i64())
            ) {
                // Create hierarchy record for categories with parents
                if let Some(parent_id) = parent_id {
                    let hierarchy_key = format!("category_hierarchy_{:06}", hierarchy_id_counter);
                    let hierarchy_record = serde_json::json!({
                        "id": hierarchy_id_counter,
                        "parent_category_id": parent_id,
                        "child_category_id": category_id,
                        "depth_level": 1, // Would calculate actual depth in real implementation
                        "sort_order": category_id, // Simple ordering by ID
                        "created_at": chrono::Utc::now().timestamp()
                    });
                    
                    self.database.put(hierarchy_key.as_bytes(), hierarchy_record.to_string().as_bytes())?;
                    hierarchy_id_counter += 1;
                }
                
                stats.successful_operations += 1;
            }
            
            if stats.total_records % self.batch_size == 0 {
                println!("   Processed {} categories...", stats.total_records);
            }
        }
        
        stats.end_operation();
        stats.print_summary();
        Ok(stats)
    }
    
    /// Create basic user roles
    pub fn create_user_roles(&self) -> Result<RestructuringStats> {
        println!("👥 Creating user roles and assignments...");
        
        let mut stats = RestructuringStats::new("user_roles");
        
        // Create standard roles
        let roles = vec![
            ("admin", "Administrator with full access"),
            ("customer", "Regular customer account"),
            ("staff", "Staff member with limited admin access"),
            ("vendor", "Product vendor/supplier"),
        ];
        
        for (i, (role_name, description)) in roles.iter().enumerate() {
            let role_id = i as i64 + 1;
            let role_key = format!("user_roles_{:03}", role_id);
            let role_record = serde_json::json!({
                "id": role_id,
                "role_name": role_name,
                "description": description,
                "permissions": self.get_default_permissions(role_name),
                "active": true,
                "created_at": chrono::Utc::now().timestamp()
            });
            
            self.database.put(role_key.as_bytes(), role_record.to_string().as_bytes())?;
            stats.successful_operations += 1;
        }
        
        // Assign default customer role to all existing users
        let scan_result = self.database.scan(
            Some(b"users_".to_vec()),
            Some(b"users~".to_vec()),
        )?;
        
        let mut assignment_id_counter = 1i64;
        for item in scan_result {
            let (_key, value) = item?;
            let user: Value = serde_json::from_slice(&value)?;
            
            stats.total_records += 1;
            
            if let Some(user_id) = user.get("id").and_then(|v| v.as_i64()) {
                let assignment_key = format!("user_role_assignments_{:06}", assignment_id_counter);
                let assignment_record = serde_json::json!({
                    "id": assignment_id_counter,
                    "user_id": user_id,
                    "role_id": 2, // Customer role
                    "assigned_at": chrono::Utc::now().timestamp(),
                    "assigned_by": "system_migration",
                    "active": true,
                    "expires_at": null
                });
                
                self.database.put(assignment_key.as_bytes(), assignment_record.to_string().as_bytes())?;
                assignment_id_counter += 1;
            }
        }
        
        stats.end_operation();
        stats.print_summary();
        Ok(stats)
    }
    
    // Helper methods for creating normalized records
    
    fn create_product_details(&self, product_id: i64, product: &Value) -> Result<()> {
        let details_key = format!("product_details_{:06}", product_id);
        let details_record = serde_json::json!({
            "id": product_id,
            "product_id": product_id,
            "long_description": product.get("description").unwrap_or(&Value::Null),
            "specifications": {
                "weight": null,
                "dimensions": null,
                "materials": null,
                "care_instructions": null
            },
            "seo_title": product.get("name").unwrap_or(&Value::Null),
            "seo_description": product.get("description").unwrap_or(&Value::Null),
            "keywords": [],
            "additional_images": [],
            "videos": [],
            "documents": [],
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        
        self.database.put(details_key.as_bytes(), details_record.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_product_variants(&self, product_id: i64, product: &Value) -> Result<()> {
        let variant_key = format!("product_variants_{:06}", product_id);
        let variant_record = serde_json::json!({
            "id": product_id,
            "base_product_id": product_id,
            "sku": product.get("sku").unwrap_or(&Value::String("".to_string())),
            "variant_name": "Default",
            "variant_options": {},
            "price_cents": product.get("price_v2").unwrap_or(&Value::Number(serde_json::Number::from(0))),
            "cost_cents": product.get("cost_v2").unwrap_or(&Value::Number(serde_json::Number::from(0))),
            "weight_grams": null,
            "dimensions": null,
            "active": product.get("active").unwrap_or(&Value::Bool(true)),
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        
        self.database.put(variant_key.as_bytes(), variant_record.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_product_inventory(&self, product_id: i64, product: &Value) -> Result<()> {
        let inventory_key = format!("product_inventory_{:06}", product_id);
        let inventory_record = serde_json::json!({
            "id": product_id,
            "product_id": product_id,
            "location": "default_warehouse",
            "available_quantity": product.get("inventory_count").unwrap_or(&Value::Number(serde_json::Number::from(0))),
            "reserved_quantity": 0,
            "reorder_level": 10,
            "reorder_quantity": 50,
            "last_restocked_at": null,
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        
        self.database.put(inventory_key.as_bytes(), inventory_record.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_price_history(&self, product_id: i64, product: &Value) -> Result<()> {
        let price_key = format!("price_history_{:06}", product_id);
        let price_record = serde_json::json!({
            "id": product_id,
            "product_id": product_id,
            "price_cents": product.get("price_v2").unwrap_or(&Value::Number(serde_json::Number::from(0))),
            "cost_cents": product.get("cost_v2").unwrap_or(&Value::Number(serde_json::Number::from(0))),
            "effective_from": product.get("created_at").unwrap_or(&Value::Number(serde_json::Number::from(chrono::Utc::now().timestamp()))),
            "effective_to": null,
            "currency": "USD",
            "reason": "initial_price",
            "changed_by": "system_migration",
            "created_at": chrono::Utc::now().timestamp()
        });
        
        self.database.put(price_key.as_bytes(), price_record.to_string().as_bytes())?;
        Ok(())
    }
    
    fn create_product_category_relationships(&self, product_id: i64, product: &Value) -> Result<()> {
        if let Some(category_id) = product.get("category_id").and_then(|v| v.as_i64()) {
            let relationship_key = format!("product_categories_{}_{}", product_id, category_id);
            let relationship_record = serde_json::json!({
                "id": format!("{}{:03}", product_id, category_id),
                "product_id": product_id,
                "category_id": category_id,
                "is_primary": true,
                "sort_order": 0,
                "created_at": chrono::Utc::now().timestamp()
            });
            
            self.database.put(relationship_key.as_bytes(), relationship_record.to_string().as_bytes())?;
        }
        Ok(())
    }
    
    fn create_address_record(&self, address_id: i64, user_id: i64, address_type: &str, address_data: &Value) -> Result<()> {
        let address_key = format!("addresses_{:06}", address_id);
        let address_record = serde_json::json!({
            "id": address_id,
            "user_id": user_id,
            "address_type": address_type,
            "address_line_1": address_data.get("street").unwrap_or(&Value::String("".to_string())),
            "address_line_2": null,
            "city": address_data.get("city").unwrap_or(&Value::String("".to_string())),
            "state": address_data.get("state").unwrap_or(&Value::String("".to_string())),
            "postal_code": address_data.get("zip").unwrap_or(&Value::String("".to_string())),
            "country": "US",
            "is_default": false,
            "active": true,
            "created_at": chrono::Utc::now().timestamp(),
            "updated_at": null
        });
        
        self.database.put(address_key.as_bytes(), address_record.to_string().as_bytes())?;
        Ok(())
    }
    
    fn get_default_permissions(&self, role_name: &str) -> Value {
        match role_name {
            "admin" => serde_json::json!([
                "users.read", "users.write", "users.delete",
                "products.read", "products.write", "products.delete",
                "orders.read", "orders.write", "orders.delete",
                "reports.read", "settings.write"
            ]),
            "staff" => serde_json::json!([
                "products.read", "products.write",
                "orders.read", "orders.write",
                "users.read"
            ]),
            "vendor" => serde_json::json!([
                "products.read", "products.write",
                "orders.read"
            ]),
            "customer" => serde_json::json!([
                "orders.read", "profile.write"
            ]),
            _ => serde_json::json!([]),
        }
    }
}

/// Restructuring statistics tracker
#[derive(Debug)]
pub struct RestructuringStats {
    pub operation_name: String,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub duration: std::time::Duration,
    pub total_records: usize,
    pub successful_operations: usize,
    pub failed_operations: usize,
    pub errors: Vec<String>,
}

impl RestructuringStats {
    pub fn new(name: &str) -> Self {
        Self {
            operation_name: name.to_string(),
            start_time: chrono::Utc::now().timestamp(),
            end_time: None,
            duration: std::time::Duration::new(0, 0),
            total_records: 0,
            successful_operations: 0,
            failed_operations: 0,
            errors: Vec::new(),
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
        (self.successful_operations as f64 / self.total_records as f64) * 100.0
    }
    
    pub fn print_summary(&self) {
        println!("📊 Restructuring Summary: {}", self.operation_name);
        println!("   Total records: {}", self.total_records);
        println!("   Successful: {} ({:.1}%)", self.successful_operations, self.success_rate());
        println!("   Failed: {}", self.failed_operations);
        println!("   Duration: {:.2}s", self.duration.as_secs_f64());
        
        if !self.errors.is_empty() {
            println!("   Errors ({}):", self.errors.len());
            for (i, error) in self.errors.iter().take(2).enumerate() {
                println!("     {}. {}", i + 1, error);
            }
            if self.errors.len() > 2 {
                println!("     ... and {} more", self.errors.len() - 2);
            }
        }
    }
}

// Table definitions for new normalized structures

fn create_product_details_table() -> TableDefinition {
    TableDefinition {
        name: "product_details".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "product_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "long_description".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "specifications".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "seo_title".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "seo_description".to_string(),
                data_type: DataType::String,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "keywords".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "additional_images".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "videos".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "documents".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "created_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_product_variants_table() -> TableDefinition {
    TableDefinition {
        name: "product_variants".to_string(),
        columns: vec![
            ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "base_product_id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "sku".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "variant_name".to_string(),
                data_type: DataType::String,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "variant_options".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "price_cents".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "cost_cents".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "weight_grams".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "dimensions".to_string(),
                data_type: DataType::Json,
                nullable: true,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "active".to_string(),
                data_type: DataType::Boolean,
                nullable: false,
                default: Some(serde_json::Value::Bool(true)),
                constraints: vec![],
            },
            ColumnDefinition {
                name: "created_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: false,
                default: None,
                constraints: vec![],
            },
            ColumnDefinition {
                name: "updated_at".to_string(),
                data_type: DataType::Timestamp,
                nullable: true,
                default: None,
                constraints: vec![],
            },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

// Additional table definitions (simplified for brevity)
fn create_category_hierarchy_table() -> TableDefinition {
    TableDefinition {
        name: "category_hierarchy".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "parent_category_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "child_category_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "depth_level".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "sort_order".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_product_inventory_table() -> TableDefinition {
    TableDefinition {
        name: "product_inventory".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "product_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "location".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "available_quantity".to_string(), data_type: DataType::Int32, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "reserved_quantity".to_string(), data_type: DataType::Int32, nullable: false, default: Some(serde_json::Value::Number(serde_json::Number::from(0))), constraints: vec![] },
            ColumnDefinition { name: "reorder_level".to_string(), data_type: DataType::Int32, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "reorder_quantity".to_string(), data_type: DataType::Int32, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "last_restocked_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "updated_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_price_history_table() -> TableDefinition {
    TableDefinition {
        name: "price_history".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "product_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "price_cents".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "cost_cents".to_string(), data_type: DataType::Int64, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "effective_from".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "effective_to".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "currency".to_string(), data_type: DataType::String, nullable: false, default: Some(serde_json::Value::String("USD".to_string())), constraints: vec![] },
            ColumnDefinition { name: "reason".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "changed_by".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_addresses_table() -> TableDefinition {
    TableDefinition {
        name: "addresses".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "address_type".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "address_line_1".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "address_line_2".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "city".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "state".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "postal_code".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "country".to_string(), data_type: DataType::String, nullable: false, default: Some(serde_json::Value::String("US".to_string())), constraints: vec![] },
            ColumnDefinition { name: "is_default".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(false)), constraints: vec![] },
            ColumnDefinition { name: "active".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(true)), constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "updated_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_order_status_history_table() -> TableDefinition {
    TableDefinition {
        name: "order_status_history".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "order_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "status".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "changed_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "changed_by".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "notes".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "metadata".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_product_categories_junction_table() -> TableDefinition {
    TableDefinition {
        name: "product_categories".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "product_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "category_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "is_primary".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(false)), constraints: vec![] },
            ColumnDefinition { name: "sort_order".to_string(), data_type: DataType::Int32, nullable: false, default: Some(serde_json::Value::Number(serde_json::Number::from(0))), constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_user_roles_table() -> TableDefinition {
    TableDefinition {
        name: "user_roles".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "role_name".to_string(), data_type: DataType::String, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "description".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "permissions".to_string(), data_type: DataType::Json, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "active".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(true)), constraints: vec![] },
            ColumnDefinition { name: "created_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_user_role_assignments_table() -> TableDefinition {
    TableDefinition {
        name: "user_role_assignments".to_string(),
        columns: vec![
            ColumnDefinition { name: "id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "user_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "role_id".to_string(), data_type: DataType::Int64, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "assigned_at".to_string(), data_type: DataType::Timestamp, nullable: false, default: None, constraints: vec![] },
            ColumnDefinition { name: "assigned_by".to_string(), data_type: DataType::String, nullable: true, default: None, constraints: vec![] },
            ColumnDefinition { name: "active".to_string(), data_type: DataType::Boolean, nullable: false, default: Some(serde_json::Value::Bool(true)), constraints: vec![] },
            ColumnDefinition { name: "expires_at".to_string(), data_type: DataType::Timestamp, nullable: true, default: None, constraints: vec![] },
        ],
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    }
}

fn create_enhanced_orders_table() -> TableDefinition {
    let mut table = TableDefinition {
        name: "orders".to_string(),
        columns: vec![], // Would include all original columns
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    };
    
    // Add new foreign key columns
    table.columns.extend(vec![
        ColumnDefinition {
            name: "shipping_address_id".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            default: None,
            constraints: vec![],
        },
        ColumnDefinition {
            name: "billing_address_id".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            default: None,
            constraints: vec![],
        },
    ]);
    
    table
}

fn create_enhanced_order_items_table() -> TableDefinition {
    let mut table = TableDefinition {
        name: "order_items".to_string(),
        columns: vec![], // Would include all original columns
        primary_key: vec!["id".to_string()],
        options: TableOptions::default(),
    };
    
    // Add product variant reference
    table.columns.push(ColumnDefinition {
        name: "product_variant_id".to_string(),
        data_type: DataType::Int64,
        nullable: true,
        default: None,
        constraints: vec![],
    });
    
    table
}

fn add_restructuring_constraints(constraints: &mut BTreeMap<String, ConstraintDefinition>) {
    constraints.insert("fk_product_details_product".to_string(), ConstraintDefinition {
        name: "fk_product_details_product".to_string(),
        constraint_type: ConstraintType::ForeignKey {
            table: "product_details".to_string(),
            columns: vec!["product_id".to_string()],
            referenced_table: "products".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        },
    });
    
    constraints.insert("fk_product_variants_product".to_string(), ConstraintDefinition {
        name: "fk_product_variants_product".to_string(),
        constraint_type: ConstraintType::ForeignKey {
            table: "product_variants".to_string(),
            columns: vec!["base_product_id".to_string()],
            referenced_table: "products".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        },
    });
    
    constraints.insert("fk_addresses_user".to_string(), ConstraintDefinition {
        name: "fk_addresses_user".to_string(),
        constraint_type: ConstraintType::ForeignKey {
            table: "addresses".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: Some("CASCADE".to_string()),
            on_update: None,
        },
    });
    
    // Add more constraints...
}

fn validate_data_consistency_for_restructuring(database: &Database) -> Result<()> {
    println!("🔍 Validating data consistency before restructuring...");
    
    // Check for orphaned product categories
    let products_scan = database.scan(Some(b"products_".to_vec()), Some(b"products~".to_vec()))?;
    let mut invalid_categories = 0;
    let mut total_products = 0;
    
    for item in products_scan {
        let (_key, value) = item?;
        if let Ok(product) = serde_json::from_slice::<Value>(&value) {
            total_products += 1;
            
            if let Some(category_id) = product.get("category_id").and_then(|v| v.as_i64()) {
                let category_key = format!("categories_{:03}", category_id);
                if database.get(category_key.as_bytes())?.is_none() {
                    invalid_categories += 1;
                }
            }
        }
    }
    
    if invalid_categories > 0 {
        println!("⚠️  Found {} products referencing non-existent categories", invalid_categories);
        if invalid_categories > total_products / 20 { // More than 5% invalid
            return Err(lightning_db::Error::Validation(
                "Too many orphaned category references for safe restructuring".to_string()
            ));
        }
    }
    
    println!("✅ Data consistency validation passed ({} products checked)", total_products);
    Ok(())
}

/// Run the table restructuring example
pub fn run_example() -> Result<()> {
    println!("🚀 Running Example 5: Table Restructuring");
    println!("==========================================\n");
    
    let runner = MigrationExampleRunner::new("examples/databases/e05_table_restructuring.db")?;
    
    // Set up all previous migrations first
    println!("📋 Setting up previous schema migrations...");
    setup_all_previous_migrations(&runner)?;
    
    // Create comprehensive test data for restructuring
    create_restructuring_test_data(&runner)?;
    
    // Run the table restructuring migration (schema changes)
    let migration = TableRestructuringMigration {};
    runner.run_migration_with_validation(
        migration,
        Box::new(|db| validate_restructuring_schema(db)),
    )?;
    
    // Perform data restructuring operations
    println!("\n🔄 Performing table restructuring operations...");
    let engine = TableRestructuringEngine::new(&runner.database);
    
    let product_stats = engine.restructure_products()?;
    let address_stats = engine.restructure_addresses()?;
    let status_stats = engine.create_order_status_history()?;
    let hierarchy_stats = engine.restructure_category_hierarchy()?;
    let roles_stats = engine.create_user_roles()?;
    
    // Validate restructuring results
    validate_restructuring_results(&runner)?;
    
    // Show comprehensive analysis
    show_restructuring_analysis(&runner)?;
    
    // Show migration history
    runner.show_history(Some(25))?;
    
    println!("\n🎉 Example 5 completed successfully!");
    println!("Next: Run example 6 to see complex data migration patterns");
    
    Ok(())
}

fn setup_all_previous_migrations(runner: &MigrationExampleRunner) -> Result<()> {
    // Run all previous migrations in sequence
    let migrations: Vec<Box<dyn Migration>> = vec![
        Box::new(super::e01_initial_schema::InitialSchemaMigration {}),
        Box::new(super::e02_add_user_profiles::AddUserProfilesMigration {}),
        Box::new(super::e03_add_indexes_optimization::AddIndexesOptimizationMigration {}),
        Box::new(super::e04_data_type_evolution::DataTypeEvolutionMigration {}),
    ];
    
    for migration in migrations {
        runner.run_migration_with_validation(migration, Box::new(|_db| Ok(())))?;
    }
    
    Ok(())
}

fn create_restructuring_test_data(runner: &MigrationExampleRunner) -> Result<()> {
    println!("📝 Creating comprehensive test data for restructuring...");
    
    // Create additional categories with hierarchy
    for i in 10..=15 {
        let key = format!("categories_{:03}", i);
        let parent_id = if i <= 12 { Some(1) } else { Some(10) }; // Create hierarchy
        let value = serde_json::json!({
            "id": i,
            "name": format!("Subcategory {}", i),
            "description": format!("Nested category under parent"),
            "parent_id": parent_id,
            "active": true,
            "created_at": chrono::Utc::now().timestamp()
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create products with nested categories
    for i in 101..=120 {
        let key = format!("products_{:03}", i);
        let category_id = 10 + (i % 6); // Use nested categories
        let value = serde_json::json!({
            "id": i,
            "category_id": category_id,
            "name": format!("Restructure Product {}", i),
            "description": format!("Product {} for testing table restructuring with long descriptions that will be moved to product_details table", i),
            "price": format!("{}.99", 25 + (i * 2)),
            "price_v2": (25 + (i * 2)) * 100, // In cents
            "cost": format!("{}.50", 15 + i),
            "cost_v2": (15 + i) * 100, // In cents
            "sku": format!("RESTR-{:06}", i),
            "inventory_count": 75 + (i % 50),
            "active": true,
            "created_at": chrono::Utc::now().timestamp() - (i * 1800),
            "updated_at": null
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    // Create orders with detailed address information
    for i in 51..=70 {
        let key = format!("orders_{:03}", i);
        let value = serde_json::json!({
            "id": i,
            "user_id": (i % 20) + 1,
            "order_number": format!("RESTR-{:08}", i * 1000),
            "status": if i % 3 == 0 { "pending" } else if i % 3 == 1 { "processing" } else { "shipped" },
            "total_amount": format!("{}.99", 75 + (i * 8)),
            "tax_amount": "8.99",
            "shipping_amount": "12.99",
            "shipping_address": {
                "street": format!("{} Restructure St", i),
                "city": "Migration City",
                "state": "MC",
                "zip": format!("{:05}", 20000 + i)
            },
            "billing_address": {
                "street": format!("{} Billing Ave", i + 100),
                "city": "Payment City", 
                "state": "PC",
                "zip": format!("{:05}", 30000 + i)
            },
            "created_at": chrono::Utc::now().timestamp() - (i * 3600),
            "updated_at": chrono::Utc::now().timestamp() - (i * 1800)
        });
        runner.database.put(key.as_bytes(), value.to_string().as_bytes())?;
    }
    
    println!("✅ Created restructuring test data: 6 nested categories, 20 products, 20 orders with addresses");
    Ok(())
}

fn validate_restructuring_schema(database: &Database) -> Result<()> {
    println!("🔍 Validating table restructuring schema...");
    
    // Check that all new normalized tables were created
    let new_tables = vec![
        "product_details", "product_variants", "category_hierarchy", 
        "product_inventory", "price_history", "addresses",
        "order_status_history", "product_categories", 
        "user_roles", "user_role_assignments"
    ];
    
    for table_name in new_tables {
        validation::validate_table_exists(database, table_name)?;
    }
    
    println!("✅ Table restructuring schema validation successful");
    Ok(())
}

fn validate_restructuring_results(runner: &MigrationExampleRunner) -> Result<()> {
    println!("🔍 Validating table restructuring results...");
    
    // Check that data was properly moved to normalized tables
    let product_details_count = validation::count_records_with_prefix(&runner.database, "product_details_")?;
    let product_variants_count = validation::count_records_with_prefix(&runner.database, "product_variants_")?;
    let addresses_count = validation::count_records_with_prefix(&runner.database, "addresses_")?;
    let status_history_count = validation::count_records_with_prefix(&runner.database, "order_status_history_")?;
    let roles_count = validation::count_records_with_prefix(&runner.database, "user_roles_")?;
    
    println!("📊 Restructuring results:");
    println!("   Product details: {}", product_details_count);
    println!("   Product variants: {}", product_variants_count);
    println!("   Addresses: {}", addresses_count);
    println!("   Status history: {}", status_history_count);
    println!("   User roles: {}", roles_count);
    
    if product_details_count == 0 || product_variants_count == 0 {
        return Err(lightning_db::Error::Validation(
            "Product restructuring appears to have failed".to_string()
        ));
    }
    
    if addresses_count == 0 {
        println!("⚠️  No addresses were normalized - check address extraction logic");
    }
    
    println!("✅ Table restructuring results validation successful");
    Ok(())
}

fn show_restructuring_analysis(runner: &MigrationExampleRunner) -> Result<()> {
    println!("\n📈 Table Restructuring Analysis:");
    println!("=".repeat(60));
    
    // Analyze the impact of restructuring
    let original_tables = vec!["products", "categories", "orders", "order_items", "users"];
    let new_tables = vec![
        "product_details", "product_variants", "category_hierarchy",
        "product_inventory", "price_history", "addresses", 
        "order_status_history", "product_categories",
        "user_roles", "user_role_assignments"
    ];
    
    println!("📊 Schema Evolution Summary:");
    println!("   Original tables: {}", original_tables.len());
    println!("   New normalized tables: {}", new_tables.len());
    println!("   Total tables: {}", original_tables.len() + new_tables.len());
    
    // Calculate storage distribution
    let mut total_records = 0;
    for table_prefix in &["products_", "orders_", "users_", "categories_"] {
        let count = validation::count_records_with_prefix(&runner.database, table_prefix)?;
        total_records += count;
        println!("   {}: {} records", table_prefix.trim_end_matches('_'), count);
    }
    
    let mut normalized_records = 0;
    for table_prefix in &["product_details_", "addresses_", "user_roles_", "order_status_history_"] {
        let count = validation::count_records_with_prefix(&runner.database, table_prefix)?;
        normalized_records += count;
    }
    
    println!("\n💡 Restructuring Benefits:");
    println!("   • Improved data normalization reduces redundancy");
    println!("   • Better referential integrity with foreign keys");
    println!("   • Enhanced query performance for specific use cases");
    println!("   • Flexible schema for future extensions");
    println!("   • Cleaner separation of concerns");
    
    println!("\n⚠️  Migration Considerations:");
    println!("   • Increased query complexity for some operations");
    println!("   • More joins required for comprehensive data retrieval");
    println!("   • Application code needs updates for new schema");
    println!("   • Temporary dual-schema period during migration");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_table_restructuring_migration() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let migration = TableRestructuringMigration {};
        
        // Test migration steps
        let steps = migration.steps();
        assert!(steps.len() > 20); // Many table creations, column additions, and indexes
        
        // Test version
        assert_eq!(migration.version(), SchemaVersion::new(1, 4));
        
        // Test schema contains new tables
        let schema = migration.target_schema();
        assert!(schema.tables.contains_key("product_details"));
        assert!(schema.tables.contains_key("product_variants"));
        assert!(schema.tables.contains_key("addresses"));
        assert!(schema.tables.contains_key("user_roles"));
    }

    #[test]
    fn test_restructuring_engine() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let runner = MigrationExampleRunner::new(db_path.to_str().unwrap()).unwrap();
        let engine = TableRestructuringEngine::new(&runner.database);
        
        // Test engine configuration
        assert_eq!(engine.batch_size, 100);
        assert_eq!(engine.preserve_original, true);
    }

    #[test]
    fn test_restructuring_stats() {
        let mut stats = RestructuringStats::new("test_restructuring");
        stats.total_records = 100;
        stats.successful_operations = 90;
        stats.failed_operations = 10;
        
        assert_eq!(stats.success_rate(), 90.0);
        assert_eq!(stats.operation_name, "test_restructuring");
    }

    #[test]
    fn test_table_definitions() {
        let product_details = create_product_details_table();
        assert_eq!(product_details.name, "product_details");
        assert_eq!(product_details.columns.len(), 12);
        
        let addresses = create_addresses_table();
        assert_eq!(addresses.name, "addresses");
        assert!(addresses.columns.len() >= 10);
        
        let user_roles = create_user_roles_table();
        assert_eq!(user_roles.name, "user_roles");
        assert_eq!(user_roles.primary_key, vec!["id".to_string()]);
    }
}
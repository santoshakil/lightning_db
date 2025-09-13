// This example demonstrates the new simplified Lightning DB API
// Run with: cargo run --example new_api_demo

use lightning_db::{Database, DatabaseBuilder, Result};

fn main() -> Result<()> {
    println!("🚀 Lightning DB - New Simplified API Demo");
    
    // === 1. SIMPLE DATABASE CREATION ===
    println!("\n1. Creating database with builder pattern:");
    let db = DatabaseBuilder::new()
        .cache_size(50 * 1024 * 1024)        // 50MB cache
        .compression_enabled(true)
        .compression_level(3)
        .max_active_transactions(1000)
        .create("./demo_db")?;
    
    println!("   ✅ Database created with optimized settings");
    
    // === 2. CORE OPERATIONS (24 total methods) ===
    println!("\n2. Core CRUD operations:");
    
    // Basic operations
    db.put(b"user:1", b"John Doe")?;
    db.put(b"user:2", b"Jane Smith")?;
    
    if let Some(value) = db.get(b"user:1")? {
        println!("   ✅ Retrieved: {}", String::from_utf8_lossy(&value));
    }
    
    // Transaction operations  
    let tx_id = db.begin_transaction()?;
    db.put_tx(tx_id, b"temp:key", b"temp:value")?;
    db.commit_transaction(tx_id)?;
    println!("   ✅ Transaction completed successfully");
    
    // === 3. BATCH OPERATIONS (via BatchManager) ===
    println!("\n3. Batch operations:");
    let batch_mgr = db.batch_manager();
    
    let batch_data = vec![
        (b"batch:1".to_vec(), b"value1".to_vec()),
        (b"batch:2".to_vec(), b"value2".to_vec()),
        (b"batch:3".to_vec(), b"value3".to_vec()),
    ];
    
    batch_mgr.put(&batch_data)?;
    println!("   ✅ Batch insert of {} records", batch_data.len());
    
    // === 4. INDEX OPERATIONS (via IndexManager) ===
    println!("\n4. Index operations:");
    let index_mgr = db.index_manager();
    
    index_mgr.create("user_id", vec!["id".to_string()])?;
    println!("   ✅ Created user_id index");
    
    let indexes = index_mgr.list();
    println!("   📋 Available indexes: {:?}", indexes);
    
    // === 5. ADVANCED QUERIES (via QueryEngine) ===
    println!("\n5. Advanced queries:");
    let query_engine = db.query_engine();
    
    // Scan all users
    let user_results = query_engine.scan_prefix(b"user:")?;
    println!("   🔍 Found {} user records", user_results.count());
    
    // === 6. MONITORING (via MonitoringManager) ===
    println!("\n6. Performance monitoring:");
    let monitor = db.monitoring_manager();
    
    let stats = monitor.stats();
    println!("   📊 Database stats:");
    println!("      - Keys: {}", stats.num_keys);
    println!("      - Size: {} bytes", stats.total_size);
    println!("      - Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
    
    // === 7. ADVANCED TRANSACTIONS (via TransactionManager) ===
    println!("\n7. Advanced transaction features:");
    let tx_mgr = db.transaction_manager();
    
    let active_txs = tx_mgr.get_active_transactions();
    println!("   🔄 Active transactions: {}", active_txs.len());
    
    // === 8. MAINTENANCE (via MaintenanceManager) ===
    println!("\n8. Database maintenance:");
    let maintenance = db.maintenance_manager();
    
    maintenance.compact()?;
    println!("   🧹 Database compaction completed");
    
    // === CLEANUP ===
    db.close()?;
    println!("\n✅ Database closed successfully");
    
    println!("\n🎉 Demo completed! The new API provides:");
    println!("   • 24 core methods (vs 134 in old API)");
    println!("   • 7 specialized managers for advanced features");
    println!("   • Clear separation between basic and advanced functionality");
    println!("   • Builder pattern for configuration");
    println!("   • Type-safe, discoverable interface");
    
    Ok(())
}

// Comparison with old API
#[allow(dead_code)]
fn old_api_comparison() {
    println!("=== OLD API (134 methods) ===");
    println!("db.create(path, config)?;                    // 8 different create methods");
    println!("db.batch_put(pairs)?;                        // 5 different batch methods");  
    println!("db.create_index_with_config(config)?;        // 11 index methods");
    println!("db.scan_prefix_with_limit_reverse(..)?;      // 18 query methods");
    println!("db.begin_transaction_with_isolation(..)?;    // 28 transaction methods");
    println!("db.get_performance_stats_detailed(..)?;      // 22 monitoring methods");
    println!("db.compact_incremental_with_options(..)?;    // 15 maintenance methods");
    println!();
    
    println!("=== NEW API (24 core + managers) ===");
    println!("DatabaseBuilder::new().create(path)?;        // Clean builder pattern");
    println!("db.batch_manager().put(pairs)?;              // Organized by function");
    println!("db.index_manager().create_with_config(config)?;");
    println!("db.query_engine().scan_prefix(prefix)?;      // Intuitive method names");  
    println!("db.transaction_manager().begin_with_isolation(level)?;");
    println!("db.monitoring_manager().get_performance_stats()?;");
    println!("db.maintenance_manager().compact()?;         // Simple, clear methods");
}
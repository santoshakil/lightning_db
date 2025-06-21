use lightning_db::{
    Database, LightningDbConfig, IndexConfig, IndexType, IndexKey, 
    JoinQuery, JoinType, SimpleRecord, IndexableRecord,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Join Operations and Multi-Index Queries");
    
    let db_path = "/tmp/test_joins.db";
    std::fs::remove_dir_all(db_path).ok(); // Clean up
    
    // Create database
    let mut config = LightningDbConfig::default();
    config.compression_enabled = false; // Disable LSM for simplicity
    let db = Database::create(&db_path, config)?;
    
    println!("Database created, setting up indexes...");
    
    // Create indexes
    db.create_index(IndexConfig {
        name: "user_name_idx".to_string(),
        columns: vec!["name".to_string()],
        unique: false,
        index_type: IndexType::BTree,
    })?;
    
    db.create_index(IndexConfig {
        name: "department_idx".to_string(),
        columns: vec!["department".to_string()],
        unique: false,
        index_type: IndexType::BTree,
    })?;
    
    db.create_index(IndexConfig {
        name: "age_idx".to_string(),
        columns: vec!["age".to_string()],
        unique: false,
        index_type: IndexType::BTree,
    })?;
    
    println!("Indexes created, inserting test data...");
    
    // Insert test data
    let users = vec![
        ("user1", "Alice", "Engineering", "25"),
        ("user2", "Bob", "Engineering", "30"),
        ("user3", "Carol", "Marketing", "28"),
        ("user4", "Dave", "Engineering", "35"),
        ("user5", "Eve", "Marketing", "29"),
    ];
    
    for (user_id, name, dept, age) in &users {
        let mut record = SimpleRecord::new();
        record.set_field("name".to_string(), name.as_bytes().to_vec());
        record.set_field("department".to_string(), dept.as_bytes().to_vec());
        record.set_field("age".to_string(), age.as_bytes().to_vec());
        
        let value = serde_json::to_vec(&record)?;
        db.put_indexed(user_id.as_bytes(), &value, &record)?;
        println!("Inserted: {} -> {} ({}, age {})", user_id, name, dept, age);
    }
    
    println!("\nTesting join operations:");
    
    // Test 1: Inner join on department index
    println!("\n1. Looking for users in Engineering department:");
    let eng_key = IndexKey::single(b"Engineering".to_vec());
    let marketing_key = IndexKey::single(b"Marketing".to_vec());
    
    // Simple join demonstration - find overlap between department queries
    let join = JoinQuery::new(
        "department_idx".to_string(),
        "department_idx".to_string(),
        JoinType::Inner
    )
    .left_key(eng_key.clone())
    .right_key(eng_key.clone());
    
    let join_results = db.join_indexes(join)?;
    println!("Self-join on Engineering department found {} matches", join_results.len());
    
    // Test 2: Range-based operations
    println!("\n2. Range operations on age index:");
    let age_25 = IndexKey::single(b"25".to_vec());
    let age_30 = IndexKey::single(b"30".to_vec());
    let age_35 = IndexKey::single(b"35".to_vec());
    
    let range_join = JoinQuery::new(
        "age_idx".to_string(),
        "age_idx".to_string(),
        JoinType::Inner
    )
    .left_range(age_25, age_30.clone())
    .right_range(age_30, age_35);
    
    let range_results = db.join_indexes(range_join)?;
    println!("Age range join found {} matches", range_results.len());
    
    // Test 3: Direct index queries for comparison
    println!("\n3. Direct index queries:");
    
    let eng_query = lightning_db::IndexQuery::new("department_idx".to_string())
        .key(IndexKey::single(b"Engineering".to_vec()));
    let eng_users = db.query_index(eng_query)?;
    println!("Engineering department has {} users", eng_users.len());
    
    let marketing_query = lightning_db::IndexQuery::new("department_idx".to_string())
        .key(IndexKey::single(b"Marketing".to_vec()));
    let marketing_users = db.query_index(marketing_query)?;
    println!("Marketing department has {} users", marketing_users.len());
    
    println!("\n✅ Join operations and multi-index queries test completed!");
    
    Ok(())
}
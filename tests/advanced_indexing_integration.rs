use lightning_db::features::advanced_indexing::{
    AdvancedIndexManager, AdvancedIndexType, IndexQuery, QueryOperator,
    SpatialQuery, BoundingBox, TextQuery, BloomFilterConfig
};
use std::collections::HashMap;

#[test]
fn test_bitmap_index_operations() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "status_idx".to_string(),
        AdvancedIndexType::Bitmap,
        vec!["status".to_string()]
    ).unwrap();
    
    manager.insert("status_idx", b"active", b"doc1").unwrap();
    manager.insert("status_idx", b"active", b"doc2").unwrap();
    manager.insert("status_idx", b"inactive", b"doc3").unwrap();
    manager.insert("status_idx", b"active", b"doc4").unwrap();
    
    let query = IndexQuery::Equality {
        column: "status".to_string(),
        value: b"active".to_vec(),
    };
    
    let results = manager.search("status_idx", &query).unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.contains(&b"doc1".to_vec()));
    assert!(results.contains(&b"doc2".to_vec()));
    assert!(results.contains(&b"doc4".to_vec()));
}

#[test]
fn test_hash_index_operations() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "user_id_idx".to_string(),
        AdvancedIndexType::Hash,
        vec!["user_id".to_string()]
    ).unwrap();
    
    for i in 0..1000 {
        let key = format!("user_{}", i);
        let value = format!("data_{}", i);
        manager.insert("user_id_idx", key.as_bytes(), value.as_bytes()).unwrap();
    }
    
    let query = IndexQuery::Equality {
        column: "user_id".to_string(),
        value: b"user_500".to_vec(),
    };
    
    let results = manager.search("user_id_idx", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], b"data_500");
    
    let stats = manager.get_index_stats("user_id_idx").unwrap();
    assert_eq!(stats.entries, 1000);
}

#[test]
fn test_spatial_index_operations() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "location_idx".to_string(),
        AdvancedIndexType::Spatial,
        vec!["latitude".to_string(), "longitude".to_string()]
    ).unwrap();
    
    let locations = vec![
        (40.7128, -74.0060, "NYC"),
        (34.0522, -118.2437, "LA"),
        (41.8781, -87.6298, "Chicago"),
        (29.7604, -95.3698, "Houston"),
        (33.4484, -112.0740, "Phoenix"),
    ];
    
    for (lat, lon, city) in locations {
        let key = format!("{},{}", lat, lon);
        manager.insert("location_idx", key.as_bytes(), city.as_bytes()).unwrap();
    }
    
    let query = IndexQuery::Spatial(SpatialQuery::BoundingBox(BoundingBox {
        min_x: 40.0,
        min_y: -75.0,
        max_x: 42.0,
        max_y: -73.0,
    }));
    
    let results = manager.search("location_idx", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], b"NYC");
}

#[test]
fn test_fulltext_index_operations() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "content_idx".to_string(),
        AdvancedIndexType::FullText,
        vec!["content".to_string()]
    ).unwrap();
    
    let documents = vec![
        ("doc1", "The quick brown fox jumps over the lazy dog"),
        ("doc2", "Lightning DB is a high-performance database"),
        ("doc3", "Full-text search enables powerful queries"),
        ("doc4", "The database supports multiple index types"),
    ];
    
    for (id, content) in documents {
        manager.insert("content_idx", content.as_bytes(), id.as_bytes()).unwrap();
    }
    
    let query = IndexQuery::Text(TextQuery::Contains("database".to_string()));
    
    let results = manager.search("content_idx", &query).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.contains(&b"doc2".to_vec()));
    assert!(results.contains(&b"doc4".to_vec()));
    
    let phrase_query = IndexQuery::Text(TextQuery::Phrase("high-performance database".to_string()));
    let phrase_results = manager.search("content_idx", &phrase_query).unwrap();
    assert_eq!(phrase_results.len(), 1);
    assert_eq!(phrase_results[0], b"doc2");
}

#[test]
fn test_bloom_filter_index() {
    let mut manager = AdvancedIndexManager::new();
    
    let config = BloomFilterConfig {
        expected_items: 10000,
        false_positive_rate: 0.01,
    };
    
    manager.create_bloom_filter_index(
        "bloom_idx".to_string(),
        vec!["email".to_string()],
        config
    ).unwrap();
    
    for i in 0..1000 {
        let email = format!("user{}@example.com", i);
        manager.insert("bloom_idx", email.as_bytes(), b"exists").unwrap();
    }
    
    let existing_query = IndexQuery::Membership {
        column: "email".to_string(),
        value: b"user500@example.com".to_vec(),
    };
    
    let exists = manager.check_membership("bloom_idx", &existing_query).unwrap();
    assert!(exists);
    
    let non_existing_query = IndexQuery::Membership {
        column: "email".to_string(),
        value: b"nonexistent@example.com".to_vec(),
    };
    
    let not_exists = manager.check_membership("bloom_idx", &non_existing_query).unwrap();
    assert!(!not_exists);
}

#[test]
fn test_covering_index() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_covering_index(
        "user_covering_idx".to_string(),
        vec!["user_id".to_string()],
        vec!["name".to_string(), "email".to_string(), "created_at".to_string()]
    ).unwrap();
    
    let user_data = HashMap::from([
        ("name", "John Doe"),
        ("email", "john@example.com"),
        ("created_at", "2024-01-01"),
    ]);
    
    manager.insert_with_data(
        "user_covering_idx",
        b"user_123",
        user_data
    ).unwrap();
    
    let query = IndexQuery::Equality {
        column: "user_id".to_string(),
        value: b"user_123".to_vec(),
    };
    
    let results = manager.search_with_data("user_covering_idx", &query).unwrap();
    assert_eq!(results.len(), 1);
    
    let (_, data) = &results[0];
    assert_eq!(data.get("name"), Some(&"John Doe".to_string()));
    assert_eq!(data.get("email"), Some(&"john@example.com".to_string()));
}

#[test]
fn test_partial_index() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_partial_index(
        "active_users_idx".to_string(),
        vec!["user_id".to_string()],
        "status = 'active'".to_string()
    ).unwrap();
    
    manager.insert_conditional("active_users_idx", b"user1", b"data1", true).unwrap();
    manager.insert_conditional("active_users_idx", b"user2", b"data2", false).unwrap();
    manager.insert_conditional("active_users_idx", b"user3", b"data3", true).unwrap();
    
    let stats = manager.get_index_stats("active_users_idx").unwrap();
    assert_eq!(stats.entries, 2);
}

#[test]
fn test_expression_index() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_expression_index(
        "lower_email_idx".to_string(),
        "LOWER(email)".to_string()
    ).unwrap();
    
    manager.insert("lower_email_idx", b"john@example.com", b"user1").unwrap();
    manager.insert("lower_email_idx", b"jane@example.com", b"user2").unwrap();
    manager.insert("lower_email_idx", b"bob@example.com", b"user3").unwrap();
    
    let query = IndexQuery::Equality {
        column: "lower_email".to_string(),
        value: b"john@example.com".to_vec(),
    };
    
    let results = manager.search("lower_email_idx", &query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], b"user1");
}

#[test]
fn test_composite_index() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "composite_idx".to_string(),
        AdvancedIndexType::Composite,
        vec!["category".to_string(), "subcategory".to_string(), "price".to_string()]
    ).unwrap();
    
    let products = vec![
        (("electronics", "phones", "999"), "iPhone"),
        (("electronics", "phones", "699"), "Samsung"),
        (("electronics", "laptops", "1299"), "MacBook"),
        (("clothing", "shirts", "49"), "T-Shirt"),
    ];
    
    for ((cat, subcat, price), product) in products {
        let key = format!("{}:{}:{}", cat, subcat, price);
        manager.insert("composite_idx", key.as_bytes(), product.as_bytes()).unwrap();
    }
    
    let query = IndexQuery::Composite {
        conditions: vec![
            ("category".to_string(), QueryOperator::Eq, b"electronics".to_vec()),
            ("subcategory".to_string(), QueryOperator::Eq, b"phones".to_vec()),
        ],
    };
    
    let results = manager.search("composite_idx", &query).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_index_intersection() {
    let mut manager = AdvancedIndexManager::new();
    
    manager.create_index(
        "category_idx".to_string(),
        AdvancedIndexType::Hash,
        vec!["category".to_string()]
    ).unwrap();
    
    manager.create_index(
        "price_idx".to_string(),
        AdvancedIndexType::BTree,
        vec!["price".to_string()]
    ).unwrap();
    
    for i in 0..100 {
        let category = if i % 2 == 0 { "electronics" } else { "clothing" };
        let price = i * 10;
        
        manager.insert("category_idx", category.as_bytes(), format!("product_{}", i).as_bytes()).unwrap();
        manager.insert("price_idx", price.to_string().as_bytes(), format!("product_{}", i).as_bytes()).unwrap();
    }
    
    let category_results = manager.search("category_idx", &IndexQuery::Equality {
        column: "category".to_string(),
        value: b"electronics".to_vec(),
    }).unwrap();
    
    let price_results = manager.search("price_idx", &IndexQuery::Range {
        column: "price".to_string(),
        min: Some(b"200".to_vec()),
        max: Some(b"500".to_vec()),
    }).unwrap();
    
    let intersection: Vec<Vec<u8>> = category_results
        .iter()
        .filter(|item| price_results.contains(item))
        .cloned()
        .collect();
    
    assert!(intersection.len() > 0);
    assert!(intersection.len() < category_results.len());
    assert!(intersection.len() < price_results.len());
}
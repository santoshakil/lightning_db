#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bytes::Bytes;
    use std::collections::HashMap;
    use crate::core::storage::PageManagerAsync;
    use crate::core::storage::Page;
    use async_trait::async_trait;
    use crate::core::error::Error;

    // Mock PageManager for testing
    struct MockPageManager;

    impl MockPageManager {
        fn new() -> Self {
            MockPageManager
        }
    }

    #[async_trait]
    impl PageManagerAsync for MockPageManager {
        async fn get_page(&self, _page_id: u64) -> Result<Page, Error> {
            Ok(Page::new(0, vec![0; 4096]))
        }

        async fn put_page(&self, _page: Page) -> Result<(), Error> {
            Ok(())
        }

        async fn allocate_page(&self) -> Result<u64, Error> {
            Ok(1)
        }

        async fn deallocate_page(&self, _page_id: u64) -> Result<(), Error> {
            Ok(())
        }

        async fn sync(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn create_test_engine() -> ColumnarEngine {
        let config = ColumnarConfig::default();
        let storage = Arc::new(MockPageManager::new());
        
        futures::executor::block_on(async {
            ColumnarEngine::new(config, storage).await.unwrap()
        })
    }

    fn create_test_schema() -> Schema {
        Schema {
            name: "test_table".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    compression: Some(CompressionType::Snappy),
                    encoding: Some(EncodingType::Plain),
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    compression: Some(CompressionType::Dictionary),
                    encoding: Some(EncodingType::Dictionary),
                },
                ColumnDefinition {
                    name: "value".to_string(),
                    data_type: DataType::Float64,
                    nullable: false,
                    compression: Some(CompressionType::Lz4),
                    encoding: Some(EncodingType::Delta),
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
        }
    }

    #[tokio::test]
    async fn test_create_table() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        let result = engine.create_table(schema).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_insert_batch() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![
            Value::Int32(1), Value::Int32(2), Value::Int32(3)
        ]);
        columns.insert("name".to_string(), vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
            Value::String("Charlie".to_string()),
        ]);
        columns.insert("value".to_string(), vec![
            Value::Float64(10.5), Value::Float64(20.3), Value::Float64(30.7)
        ]);
        
        let result = engine.insert_batch("test_table", columns).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query_all_columns() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![
            Value::Int32(1), Value::Int32(2), Value::Int32(3)
        ]);
        columns.insert("name".to_string(), vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
            Value::String("Charlie".to_string()),
        ]);
        columns.insert("value".to_string(), vec![
            Value::Float64(10.5), Value::Float64(20.3), Value::Float64(30.7)
        ]);
        
        engine.insert_batch("test_table", columns).await.unwrap();
        
        let result = engine.query(
            "test_table",
            vec!["id".to_string(), "name".to_string(), "value".to_string()],
            None,
        ).await;
        
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.row_count, 3);
        assert!(query_result.columns.contains_key("id"));
        assert!(query_result.columns.contains_key("name"));
        assert!(query_result.columns.contains_key("value"));
    }

    #[tokio::test]
    async fn test_query_with_predicate() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![
            Value::Int32(1), Value::Int32(2), Value::Int32(3)
        ]);
        columns.insert("name".to_string(), vec![
            Value::String("Alice".to_string()),
            Value::String("Bob".to_string()),
            Value::String("Charlie".to_string()),
        ]);
        columns.insert("value".to_string(), vec![
            Value::Float64(10.5), Value::Float64(20.3), Value::Float64(30.7)
        ]);
        
        engine.insert_batch("test_table", columns).await.unwrap();
        
        let predicate = Predicate::GreaterThan("id".to_string(), Value::Int32(1));
        let result = engine.query(
            "test_table",
            vec!["id".to_string(), "name".to_string()],
            Some(predicate),
        ).await;
        
        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.row_count, 2); // Should filter out id=1
    }

    #[tokio::test]
    async fn test_compression_integration() {
        use super::compression::{Compressor, CompressionStrategy, ColumnCompression};
        
        let compressor = Compressor::new(CompressionStrategy::Balanced);
        let original_data = Bytes::from(vec![1u8; 1000]); // Highly compressible data
        
        let compressed = compressor.compress(&original_data, ColumnCompression::Snappy).unwrap();
        let decompressed = compressor.decompress(&compressed, ColumnCompression::Snappy).unwrap();
        
        assert_eq!(original_data, decompressed);
        assert!(compressed.len() < original_data.len()); // Should be compressed
    }

    #[tokio::test]
    async fn test_encoding_integration() {
        use super::encoding::{Encoder, EncodingType};
        
        let encoder = Encoder::new(EncodingType::RunLength);
        let original_data = Bytes::from(vec![5u8; 100]); // Highly repetitive data
        
        let (encoded, dictionary) = encoder.encode(&original_data).unwrap();
        let decoded = encoder.decode(&encoded, dictionary.as_ref()).unwrap();
        
        assert_eq!(original_data, decoded);
        assert!(encoded.len() < original_data.len()); // Should be encoded efficiently
    }

    #[tokio::test]
    async fn test_dictionary_encoding() {
        use super::encoding::{Encoder, EncodingType, Dictionary};
        use bytes::{BytesMut, BufMut};
        
        // Create test data with repeated strings
        let mut buffer = BytesMut::new();
        
        // Simulate string data: "hello", "world", "hello", "test", "world"
        for s in &["hello", "world", "hello", "test", "world"] {
            buffer.put_u32_le(s.len() as u32);
            buffer.put(s.as_bytes());
        }
        
        let encoder = Encoder::new(EncodingType::Dictionary);
        let (encoded, dictionary) = encoder.encode(&buffer.freeze()).unwrap();
        
        assert!(dictionary.is_some());
        let dict = dictionary.unwrap();
        assert_eq!(dict.size(), 3); // Should have 3 unique values
        
        let decoded = encoder.decode(&encoded, Some(&dict)).unwrap();
        assert_eq!(buffer.freeze(), decoded);
    }

    #[tokio::test]
    async fn test_delta_encoding() {
        use super::encoding::{Encoder, EncodingType};
        use bytes::{BytesMut, BufMut};
        
        // Create sequential integer data
        let mut buffer = BytesMut::new();
        for i in 1000..1010i32 {
            buffer.put_i32_le(i);
        }
        
        let encoder = Encoder::new(EncodingType::Delta);
        let (encoded, _) = encoder.encode(&buffer.freeze()).unwrap();
        let decoded = encoder.decode(&encoded, None).unwrap();
        
        assert_eq!(buffer.freeze(), decoded);
        assert!(encoded.len() < buffer.len()); // Should compress sequential data
    }

    #[tokio::test]
    async fn test_statistics_collection() {
        use super::statistics::StatisticsCollector;
        use super::column::ColumnData;
        
        let mut collector = StatisticsCollector::new();
        
        let int_data = ColumnData::Int32(vec![1, 2, 3, 4, 5, 1, 2, 3]);
        collector.collect("test_column".to_string(), &int_data).unwrap();
        
        let stats = collector.get_statistics("test_column").unwrap();
        assert_eq!(stats.null_count, 0);
        assert!(stats.distinct_count.is_some());
        assert!(stats.min_value.is_some());
        assert!(stats.max_value.is_some());
        assert!(stats.mean.is_some());
        assert!(stats.variance.is_some());
    }

    #[tokio::test]
    async fn test_null_handling() {
        use super::statistics::StatisticsCollector;
        use super::column::ColumnData;
        
        let mut collector = StatisticsCollector::new();
        
        let null_data = ColumnData::Null(100);
        collector.collect("null_column".to_string(), &null_data).unwrap();
        
        let stats = collector.get_statistics("null_column").unwrap();
        assert_eq!(stats.null_count, 100);
        assert_eq!(stats.distinct_count, Some(0));
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[tokio::test]
    async fn test_string_statistics() {
        use super::statistics::StatisticsCollector;
        use super::column::ColumnData;
        
        let mut collector = StatisticsCollector::new();
        
        let string_data = ColumnData::String(vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
            "apple".to_string(),
        ]);
        
        collector.collect("string_column".to_string(), &string_data).unwrap();
        
        let stats = collector.get_statistics("string_column").unwrap();
        assert_eq!(stats.null_count, 0);
        assert!(stats.distinct_count.is_some());
        assert!(stats.min_value.is_some());
        assert!(stats.max_value.is_some());
        assert!(stats.total_bytes > 0);
    }

    #[tokio::test]
    async fn test_indexing() {
        use super::indexing::{IndexBuilder, IndexType};
        
        let values = vec![
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(30),
            Value::Int32(20), // Duplicate
        ];
        
        let builder = IndexBuilder::new(IndexType::Hash);
        let index = builder.build(&values).unwrap();
        
        // Test lookup
        let key = 20i32.to_le_bytes();
        let positions = index.lookup(&key);
        assert_eq!(positions, vec![1, 3]); // Should find both occurrences
    }

    #[tokio::test]
    async fn test_btree_indexing() {
        use super::indexing::{IndexBuilder, IndexType};
        
        let values = vec![
            Value::Int32(30),
            Value::Int32(10),
            Value::Int32(20),
            Value::Int32(40),
        ];
        
        let builder = IndexBuilder::new(IndexType::BTree);
        let index = builder.build(&values).unwrap();
        
        // Test lookup
        let key = 20i32.to_le_bytes();
        let positions = index.lookup(&key);
        assert_eq!(positions, vec![2]); // Should find the correct position
    }

    #[tokio::test]
    async fn test_bitmap_indexing() {
        use super::indexing::{IndexBuilder, IndexType};
        
        let values = vec![
            Value::Int32(1),
            Value::Int32(0),
            Value::Int32(1),
            Value::Int32(1),
            Value::Int32(0),
        ];
        
        let builder = IndexBuilder::new(IndexType::Bitmap);
        let index = builder.build(&values).unwrap();
        
        // Test lookup for value 1
        let key = 1i32.to_le_bytes();
        let positions = index.lookup(&key);
        assert_eq!(positions.len(), 3); // Should find 3 occurrences of 1
    }

    #[tokio::test]
    async fn test_schema_validation() {
        let engine = create_test_engine();
        
        // Test empty schema name
        let invalid_schema = Schema {
            name: "".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    compression: None,
                    encoding: None,
                }
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        
        let result = engine.validate_schema(&invalid_schema);
        assert!(result.is_err());
        
        // Test duplicate column names
        let invalid_schema = Schema {
            name: "test".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    compression: None,
                    encoding: None,
                },
                ColumnDefinition {
                    name: "id".to_string(), // Duplicate
                    data_type: DataType::String,
                    nullable: false,
                    compression: None,
                    encoding: None,
                }
            ],
            primary_key: vec![],
            indexes: vec![],
        };
        
        let result = engine.validate_schema(&invalid_schema);
        assert!(result.is_err());
        
        // Test invalid primary key reference
        let invalid_schema = Schema {
            name: "test".to_string(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    compression: None,
                    encoding: None,
                }
            ],
            primary_key: vec!["missing_column".to_string()],
            indexes: vec![],
        };
        
        let result = engine.validate_schema(&invalid_schema);
        assert!(result.is_err());
        
        // Test valid schema
        let valid_schema = create_test_schema();
        let result = engine.validate_schema(&valid_schema);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_usage_stats() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![Value::Int32(1), Value::Int32(2)]);
        columns.insert("name".to_string(), vec![
            Value::String("Test".to_string()), 
            Value::String("Data".to_string())
        ]);
        columns.insert("value".to_string(), vec![Value::Float64(1.0), Value::Float64(2.0)]);
        
        engine.insert_batch("test_table", columns).await.unwrap();
        
        let stats = engine.get_memory_usage().await.unwrap();
        assert_eq!(stats.cache_entries, 0); // Nothing cached yet
        assert!(stats.table_bytes >= 0);
        assert!(stats.partition_count >= 0);
    }

    #[tokio::test]
    async fn test_table_compaction() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        // Insert small batches to create multiple partitions
        for i in 0..5 {
            let mut columns = HashMap::new();
            columns.insert("id".to_string(), vec![Value::Int32(i)]);
            columns.insert("name".to_string(), vec![
                Value::String(format!("name_{}", i))
            ]);
            columns.insert("value".to_string(), vec![Value::Float64(i as f64)]);
            
            engine.insert_batch("test_table", columns).await.unwrap();
        }
        
        let result = engine.compact_table("test_table").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_predicate_evaluation() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![
            Value::Int32(1), Value::Int32(2), Value::Int32(3), Value::Int32(4)
        ]);
        columns.insert("name".to_string(), vec![
            Value::String("A".to_string()),
            Value::String("B".to_string()),
            Value::String("C".to_string()),
            Value::String("D".to_string()),
        ]);
        columns.insert("value".to_string(), vec![
            Value::Float64(10.0), Value::Float64(20.0), 
            Value::Float64(30.0), Value::Float64(40.0)
        ]);
        
        engine.insert_batch("test_table", columns).await.unwrap();
        
        // Test equals predicate
        let predicate = Predicate::Equals("id".to_string(), Value::Int32(2));
        let result = engine.query("test_table", vec!["id".to_string()], Some(predicate)).await.unwrap();
        assert_eq!(result.row_count, 1);
        
        // Test greater than predicate
        let predicate = Predicate::GreaterThan("id".to_string(), Value::Int32(2));
        let result = engine.query("test_table", vec!["id".to_string()], Some(predicate)).await.unwrap();
        assert_eq!(result.row_count, 2);
        
        // Test compound predicate (AND)
        let predicate = Predicate::And(
            Box::new(Predicate::GreaterThan("id".to_string(), Value::Int32(1))),
            Box::new(Predicate::GreaterThan("value".to_string(), Value::Float64(25.0)))
        );
        let result = engine.query("test_table", vec!["id".to_string()], Some(predicate)).await.unwrap();
        assert_eq!(result.row_count, 2); // Should match id 3 and 4
    }

    #[tokio::test]
    async fn test_data_type_handling() {
        use super::column::{Column, ColumnData, DataType};
        
        // Test different data types
        let bool_column = Column {
            name: "bool_col".to_string(),
            data_type: DataType::Bool,
            column_type: super::column::ColumnType::Variable,
            nullable: false,
            data: ColumnData::Bool(vec![true, false, true]),
            statistics: None,
        };
        
        assert_eq!(bool_column.len(), 3);
        assert_eq!(bool_column.memory_size(), 3);
        
        let string_column = Column {
            name: "str_col".to_string(),
            data_type: DataType::String,
            column_type: super::column::ColumnType::Variable,
            nullable: true,
            data: ColumnData::String(vec!["hello".to_string(), "world".to_string()]),
            statistics: None,
        };
        
        assert_eq!(string_column.len(), 2);
        assert_eq!(string_column.memory_size(), 10); // "hello" + "world" = 10 bytes
    }

    #[tokio::test]
    async fn test_error_handling() {
        let engine = create_test_engine();
        
        // Test querying non-existent table
        let result = engine.query("non_existent", vec!["col".to_string()], None).await;
        assert!(result.is_err());
        
        // Test inserting into non-existent table
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![Value::Int32(1)]);
        let result = engine.insert_batch("non_existent", columns).await;
        assert!(result.is_err());
        
        // Test getting statistics for non-existent table
        let result = engine.get_statistics("non_existent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_large_batch_insert() {
        let engine = create_test_engine();
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        // Create large batch
        let batch_size = 10000;
        let mut columns = HashMap::new();
        
        let ids: Vec<Value> = (0..batch_size).map(|i| Value::Int32(i)).collect();
        let names: Vec<Value> = (0..batch_size)
            .map(|i| Value::String(format!("name_{}", i)))
            .collect();
        let values: Vec<Value> = (0..batch_size)
            .map(|i| Value::Float64(i as f64))
            .collect();
        
        columns.insert("id".to_string(), ids);
        columns.insert("name".to_string(), names);
        columns.insert("value".to_string(), values);
        
        let result = engine.insert_batch("test_table", columns).await;
        assert!(result.is_ok());
        
        // Verify data was inserted
        let query_result = engine.query(
            "test_table", 
            vec!["id".to_string()], 
            None
        ).await.unwrap();
        assert_eq!(query_result.row_count, batch_size as usize);
    }

    #[tokio::test]
    async fn test_concurrent_queries() {
        let engine = Arc::new(create_test_engine());
        let schema = create_test_schema();
        
        engine.create_table(schema).await.unwrap();
        
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), vec![
            Value::Int32(1), Value::Int32(2), Value::Int32(3)
        ]);
        columns.insert("name".to_string(), vec![
            Value::String("A".to_string()),
            Value::String("B".to_string()),
            Value::String("C".to_string()),
        ]);
        columns.insert("value".to_string(), vec![
            Value::Float64(1.0), Value::Float64(2.0), Value::Float64(3.0)
        ]);
        
        engine.insert_batch("test_table", columns).await.unwrap();
        
        // Run multiple concurrent queries
        let mut handles = vec![];
        for i in 0..10 {
            let engine_clone = engine.clone();
            let handle = tokio::spawn(async move {
                let predicate = Predicate::GreaterThan("id".to_string(), Value::Int32(i % 3));
                engine_clone.query(
                    "test_table",
                    vec!["id".to_string(), "name".to_string()],
                    Some(predicate),
                ).await
            });
            handles.push(handle);
        }
        
        // Wait for all queries to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }
}
//! Comprehensive unit tests for Lightning DB
//! 
//! This consolidated module contains all unit tests that were previously
//! scattered across small individual files including:
//! - Basic functionality unit tests
//! - Component-specific unit tests  
//! - Simple validation tests
//! - Checksum and utility function tests
//! - B-tree basic operations
//! - Page manager operations
//! - Transaction basic operations

use lightning_db::utils::integrity::calculate_checksum;
use lightning_db::core::btree::{BPlusTree, BTreeLeafIterator};
use lightning_db::core::storage::PageManager;
use lightning_db::core::transaction::{UnifiedTransactionManager as TransactionManager, UnifiedVersionStore as VersionStore};
use parking_lot::RwLock;
use std::sync::Arc;
use tempfile::tempdir;

/// Unit test configuration
#[derive(Debug, Clone)]
pub struct UnitTestConfig {
    pub enable_checksum_tests: bool,
    pub enable_btree_tests: bool,
    pub enable_page_manager_tests: bool,
    pub enable_transaction_tests: bool,
    pub test_data_size: usize,
    pub iteration_count: usize,
}

impl Default for UnitTestConfig {
    fn default() -> Self {
        Self {
            enable_checksum_tests: true,
            enable_btree_tests: true,
            enable_page_manager_tests: true,
            enable_transaction_tests: true,
            test_data_size: 1024,
            iteration_count: 100,
        }
    }
}

/// Comprehensive unit test framework
pub struct UnitTestFramework {
    config: UnitTestConfig,
}

impl UnitTestFramework {
    pub fn new(config: UnitTestConfig) -> Self {
        Self { config }
    }

    /// Run all unit tests
    pub fn run_all_unit_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running comprehensive unit tests...");

        if self.config.enable_checksum_tests {
            self.run_checksum_tests()?;
        }

        if self.config.enable_btree_tests {
            self.run_btree_tests()?;
        }

        if self.config.enable_page_manager_tests {
            self.run_page_manager_tests()?;
        }

        if self.config.enable_transaction_tests {
            self.run_transaction_tests()?;
        }

        println!("All unit tests completed successfully!");
        Ok(())
    }

    /// Checksum and utility function tests
    fn run_checksum_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running checksum unit tests...");

        self.test_simple_checksum()?;
        self.test_checksum_consistency()?;
        self.test_checksum_edge_cases()?;

        Ok(())
    }

    fn test_simple_checksum(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Test empty data
        let empty_data = &[];
        let empty_checksum = calculate_checksum(empty_data);
        assert_eq!(empty_checksum, 0); // CRC32 of empty data should be 0
        
        // Test known data
        let test_data = b"Hello, World!";
        let test_checksum = calculate_checksum(test_data);
        
        // Same data should produce same checksum
        let duplicate_checksum = calculate_checksum(test_data);
        assert_eq!(test_checksum, duplicate_checksum);

        println!("Simple checksum test passed");
        Ok(())
    }

    fn test_checksum_consistency(&self) -> Result<(), Box<dyn std::error::Error>> {
        for i in 0..self.config.iteration_count {
            let data = format!("test_data_{}", i);
            let checksum1 = calculate_checksum(data.as_bytes());
            let checksum2 = calculate_checksum(data.as_bytes());
            
            assert_eq!(checksum1, checksum2, "Checksum inconsistency at iteration {}", i);
        }

        println!("Checksum consistency test passed");
        Ok(())
    }

    fn test_checksum_edge_cases(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Single byte
        let single_byte = &[0x42];
        let single_checksum = calculate_checksum(single_byte);
        assert!(single_checksum != 0);

        // Large data
        let large_data = vec![0xAA; self.config.test_data_size];
        let large_checksum = calculate_checksum(&large_data);
        assert!(large_checksum != 0);

        // Binary data
        let binary_data: Vec<u8> = (0..=255).collect();
        let binary_checksum = calculate_checksum(&binary_data);
        assert!(binary_checksum != 0);

        println!("Checksum edge cases test passed");
        Ok(())
    }

    /// B-tree unit tests
    fn run_btree_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running B-tree unit tests...");

        self.test_btree_simple_insert_get()?;
        self.test_btree_create_iterator()?;
        self.test_btree_multiple_operations()?;
        self.test_btree_edge_cases()?;

        Ok(())
    }

    fn test_btree_simple_insert_get(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?
        ));
        let mut btree = BPlusTree::new(page_manager)?;

        // Test simple insert and get
        println!("Testing simple insert and get...");

        btree.insert(b"key1", b"value1")?;
        println!("Inserted key1");

        let result = btree.get(b"key1")?;
        println!("Got result: {:?}", result);
        assert_eq!(result, Some(b"value1".to_vec()));

        println!("Simple B-tree insert/get test passed!");
        Ok(())
    }

    fn test_btree_create_iterator(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?
        ));
        let mut btree = BPlusTree::new(page_manager)?;

        // Insert one key
        btree.insert(b"key1", b"value1")?;
        println!("Inserted key1");

        // Try to create an iterator
        println!("Creating iterator...");
        let iter = BTreeLeafIterator::<256>::new(&btree, None, None, true);
        assert!(iter.is_ok(), "Failed to create iterator: {:?}", iter.err());
        println!("Iterator created successfully");

        // Try to get one item from iterator
        println!("Getting first item from iterator...");
        let mut iter = iter?;
        let first = iter.next();
        println!("First item: {:?}", first);

        match first {
            Some(Ok((key, value))) => {
                assert_eq!(key, b"key1".to_vec());
                assert_eq!(value, b"value1".to_vec());
            }
            Some(Err(e)) => panic!("Iterator returned error: {:?}", e),
            None => panic!("Iterator returned None when should have item"),
        }

        println!("B-tree iterator test passed!");
        Ok(())
    }

    fn test_btree_multiple_operations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?
        ));
        let mut btree = BPlusTree::new(page_manager)?;

        // Insert multiple keys
        for i in 0..10 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            btree.insert(key.as_bytes(), value.as_bytes())?;
        }

        // Verify all keys
        for i in 0..10 {
            let key = format!("key_{}", i);
            let expected_value = format!("value_{}", i);
            let result = btree.get(key.as_bytes())?;
            assert_eq!(result, Some(expected_value.as_bytes().to_vec()));
        }

        println!("B-tree multiple operations test passed");
        Ok(())
    }

    fn test_btree_edge_cases(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?
        ));
        let mut btree = BPlusTree::new(page_manager)?;

        // Test empty key
        btree.insert(b"", b"empty_key_value")?;
        let result = btree.get(b"")?;
        assert_eq!(result, Some(b"empty_key_value".to_vec()));

        // Test large key/value
        let large_key = vec![b'k'; 1000];
        let large_value = vec![b'v'; 2000];
        btree.insert(&large_key, &large_value)?;
        let result = btree.get(&large_key)?;
        assert_eq!(result, Some(large_value));

        // Test non-existent key
        let result = btree.get(b"non_existent")?;
        assert_eq!(result, None);

        println!("B-tree edge cases test passed");
        Ok(())
    }

    /// Page manager unit tests
    fn run_page_manager_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running page manager unit tests...");

        self.test_page_manager_create()?;
        self.test_page_manager_with_rwlock()?;
        self.test_page_manager_multiple_allocations()?;

        Ok(())
    }

    fn test_page_manager_create(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting page manager test...");

        let dir = tempdir()?;
        println!("Created temp dir: {:?}", dir.path());

        let page_manager = PageManager::create(&dir.path().join("test.db"), 1024 * 1024);
        assert!(
            page_manager.is_ok(),
            "Failed to create page manager: {:?}",
            page_manager.err()
        );
        println!("Successfully created page manager");

        let mut page_manager = page_manager?;
        println!("Page manager created with initial state");

        // Try to allocate a page
        let page_id = page_manager.allocate_page();
        assert!(
            page_id.is_ok(),
            "Failed to allocate page: {:?}",
            page_id.err()
        );
        println!("Successfully allocated page: {}", page_id?);

        println!("Page manager create test completed successfully!");
        Ok(())
    }

    fn test_page_manager_with_rwlock(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting page manager with RwLock test...");

        let dir = tempdir()?;
        println!("Created temp dir: {:?}", dir.path());

        let page_manager = Arc::new(RwLock::new(
            PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?
        ));
        println!("Created Arc<RwLock<PageManager>>");

        // Try to get a write lock and allocate a page
        {
            println!("Acquiring write lock...");
            let mut pm = page_manager.write();
            println!("Acquired write lock");

            let page_id = pm.allocate_page();
            assert!(
                page_id.is_ok(),
                "Failed to allocate page: {:?}",
                page_id.err()
            );
            println!("Successfully allocated page: {}", page_id?);
        }
        println!("Released write lock");

        println!("Page manager RwLock test completed successfully!");
        Ok(())
    }

    fn test_page_manager_multiple_allocations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let mut page_manager = PageManager::create(&dir.path().join("test.db"), 1024 * 1024)?;

        let mut allocated_pages = Vec::new();

        // Allocate multiple pages
        for i in 0..10 {
            let page_id = page_manager.allocate_page()?;
            allocated_pages.push(page_id);
            println!("Allocated page {} with ID: {}", i, page_id);
        }

        // Verify all pages are unique
        let mut unique_pages = allocated_pages.clone();
        unique_pages.sort();
        unique_pages.dedup();
        assert_eq!(unique_pages.len(), allocated_pages.len(), "Duplicate page IDs allocated");

        println!("Page manager multiple allocations test passed");
        Ok(())
    }

    /// Transaction unit tests
    fn run_transaction_tests(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running transaction unit tests...");

        self.test_simple_sequential_transactions()?;
        self.test_simple_conflicting_transactions()?;
        self.test_transaction_rollback()?;

        Ok(())
    }

    fn test_simple_sequential_transactions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100);

        // First transaction
        let tx1_id = tx_mgr.begin()?;
        let tx1_arc = tx_mgr.get_transaction(tx1_id)?;
        tx1_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);

        // Commit first transaction
        let result1 = tx_mgr.commit(tx1_id);
        println!("First commit result: {:?}", result1);
        assert!(result1.is_ok());

        // Second transaction on different key
        let tx2_id = tx_mgr.begin()?;
        let tx2_arc = tx_mgr.get_transaction(tx2_id)?;
        tx2_arc
            .write()
            .add_write(b"key2".to_vec(), Some(b"value2".to_vec()), 0);

        // Commit second transaction
        let result2 = tx_mgr.commit(tx2_id);
        println!("Second commit result: {:?}", result2);
        assert!(result2.is_ok());

        println!("Sequential transactions test passed");
        Ok(())
    }

    fn test_simple_conflicting_transactions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100);

        // Start both transactions
        let tx1_id = tx_mgr.begin()?;
        let tx2_id = tx_mgr.begin()?;

        // Both write to same key
        let tx1_arc = tx_mgr.get_transaction(tx1_id)?;
        tx1_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"value1".to_vec()), 0);

        let tx2_arc = tx_mgr.get_transaction(tx2_id)?;
        tx2_arc
            .write()
            .add_write(b"key1".to_vec(), Some(b"value2".to_vec()), 0);

        // First commit should succeed
        let result1 = tx_mgr.commit(tx1_id);
        println!("First commit result: {:?}", result1);
        assert!(result1.is_ok());

        // Second commit should fail due to conflict
        let result2 = tx_mgr.commit(tx2_id);
        println!("Second commit result: {:?}", result2);
        assert!(result2.is_err());

        println!("Conflicting transactions test passed");
        Ok(())
    }

    fn test_transaction_rollback(&self) -> Result<(), Box<dyn std::error::Error>> {
        let version_store = Arc::new(VersionStore::new());
        let tx_mgr = TransactionManager::new(100);

        // Start transaction
        let tx_id = tx_mgr.begin()?;
        let tx_arc = tx_mgr.get_transaction(tx_id)?;
        tx_arc
            .write()
            .add_write(b"rollback_key".to_vec(), Some(b"rollback_value".to_vec()), 0);

        // Rollback transaction
        let result = tx_mgr.abort(tx_id);
        println!("Rollback result: {:?}", result);
        assert!(result.is_ok());

        // Verify transaction is no longer accessible
        let tx_result = tx_mgr.get_transaction(tx_id);
        assert!(tx_result.is_err(), "Transaction should not be accessible after abort");

        println!("Transaction rollback test passed");
        Ok(())
    }
}

// Integration tests for the unit test framework
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_framework_creation() {
        let config = UnitTestConfig::default();
        let framework = UnitTestFramework::new(config);
        
        assert!(framework.config.enable_checksum_tests);
        assert!(framework.config.enable_btree_tests);
        assert_eq!(framework.config.iteration_count, 100);
    }

    #[test]
    fn test_checksum_unit_tests() {
        let config = UnitTestConfig {
            enable_checksum_tests: true,
            enable_btree_tests: false,
            enable_page_manager_tests: false,
            enable_transaction_tests: false,
            test_data_size: 512,
            iteration_count: 10,
        };
        
        let framework = UnitTestFramework::new(config);
        let result = framework.run_checksum_tests();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_btree_unit_tests() {
        let config = UnitTestConfig {
            enable_checksum_tests: false,
            enable_btree_tests: true,
            enable_page_manager_tests: false,
            enable_transaction_tests: false,
            test_data_size: 256,
            iteration_count: 5,
        };
        
        let framework = UnitTestFramework::new(config);
        let result = framework.run_btree_tests();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_page_manager_unit_tests() {
        let config = UnitTestConfig {
            enable_checksum_tests: false,
            enable_btree_tests: false,
            enable_page_manager_tests: true,
            enable_transaction_tests: false,
            test_data_size: 256,
            iteration_count: 5,
        };
        
        let framework = UnitTestFramework::new(config);
        let result = framework.run_page_manager_tests();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_unit_tests() {
        let config = UnitTestConfig {
            enable_checksum_tests: false,
            enable_btree_tests: false,
            enable_page_manager_tests: false,
            enable_transaction_tests: true,
            test_data_size: 256,
            iteration_count: 5,
        };
        
        let framework = UnitTestFramework::new(config);
        let result = framework.run_transaction_tests();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_comprehensive_unit_tests() {
        let config = UnitTestConfig {
            test_data_size: 128,
            iteration_count: 5,
            ..Default::default()
        };
        
        let framework = UnitTestFramework::new(config);
        let result = framework.run_all_unit_tests();
        
        assert!(result.is_ok());
    }
}
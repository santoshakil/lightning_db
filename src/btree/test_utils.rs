//! Test utilities for B+Tree testing
//! 
//! This module provides test helpers that can be used to test the B+Tree
//! implementation in isolation.

#[cfg(test)]
pub mod tests {
    use crate::btree::{BPlusTree, MIN_KEYS_PER_NODE, MAX_KEYS_PER_NODE};
    use crate::storage::{PageManager, PageManagerWrapper};
    use crate::error::Result;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use tempfile::tempdir;

    /// Create a test B+Tree with a temporary storage backend
    pub fn create_test_btree() -> Result<(BPlusTree, tempfile::TempDir)> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test_btree.db");
        
        let page_manager = Arc::new(RwLock::new(PageManager::create(&db_path, 4096 * 16)?));
        let wrapper = PageManagerWrapper::standard(page_manager);
        let btree = BPlusTree::new_with_wrapper(wrapper)?;
        
        Ok((btree, dir))
    }

    #[test]
    fn test_btree_basic_operations() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;
        
        // Test insert
        btree.insert(b"key1", b"value1")?;
        assert_eq!(btree.get(b"key1")?, Some(b"value1".to_vec()));
        
        // Test update
        btree.insert(b"key1", b"updated")?;
        assert_eq!(btree.get(b"key1")?, Some(b"updated".to_vec()));
        
        // Test delete
        btree.delete(b"key1")?;
        assert_eq!(btree.get(b"key1")?, None);
        
        // Test non-existent key
        assert_eq!(btree.get(b"nonexistent")?, None);
        
        Ok(())
    }

    #[test]
    fn test_btree_edge_cases() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;
        
        // Empty value
        btree.insert(b"empty", b"")?;
        assert_eq!(btree.get(b"empty")?, Some(b"".to_vec()));
        
        // Single byte key
        btree.insert(b"x", b"single")?;
        assert_eq!(btree.get(b"x")?, Some(b"single".to_vec()));
        
        // Large key (near limit)
        let large_key = vec![b'L'; 4095];
        let large_val = vec![b'V'; 1000];
        btree.insert(&large_key, &large_val)?;
        assert_eq!(btree.get(&large_key)?, Some(large_val));
        
        // Empty key should fail
        assert!(btree.insert(b"", b"value").is_err());
        
        // Oversized key should fail
        let oversized_key = vec![b'O'; 4097];
        assert!(btree.insert(&oversized_key, b"value").is_err());
        
        Ok(())
    }

    #[test]
    fn test_btree_node_splits() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;
        
        // Insert enough keys to force multiple splits
        const NUM_KEYS: usize = 500;
        
        // Sequential insertion
        for i in 0..NUM_KEYS {
            let key = format!("seq_{:06}", i);
            let val = format!("val_{}", i);
            btree.insert(key.as_bytes(), val.as_bytes())?;
        }
        
        // Verify all keys
        for i in 0..NUM_KEYS {
            let key = format!("seq_{:06}", i);
            let expected = format!("val_{}", i);
            assert_eq!(btree.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        // Reverse order insertion
        for i in 0..NUM_KEYS {
            let key = format!("rev_{:06}", NUM_KEYS - i - 1);
            let val = format!("rev_{}", i);
            btree.insert(key.as_bytes(), val.as_bytes())?;
        }
        
        // Verify reverse keys
        for i in 0..NUM_KEYS {
            let key = format!("rev_{:06}", i);
            let expected = format!("rev_{}", NUM_KEYS - i - 1);
            assert_eq!(btree.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        Ok(())
    }

    #[test]
    fn test_btree_deletion_patterns() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;
        
        const NUM_KEYS: usize = 200;
        
        // Insert test data
        for i in 0..NUM_KEYS {
            let key = format!("del_{:04}", i);
            let val = format!("val_{}", i);
            btree.insert(key.as_bytes(), val.as_bytes())?;
        }
        
        // Delete every other key
        for i in (0..NUM_KEYS).step_by(2) {
            let key = format!("del_{:04}", i);
            assert!(btree.delete(key.as_bytes())?);
        }
        
        // Verify deletions
        for i in 0..NUM_KEYS {
            let key = format!("del_{:04}", i);
            if i % 2 == 0 {
                assert_eq!(btree.get(key.as_bytes())?, None);
            } else {
                assert!(btree.get(key.as_bytes())?.is_some());
            }
        }
        
        // Delete remaining keys
        for i in (1..NUM_KEYS).step_by(2) {
            let key = format!("del_{:04}", i);
            assert!(btree.delete(key.as_bytes())?);
        }
        
        // Verify all deleted
        for i in 0..NUM_KEYS {
            let key = format!("del_{:04}", i);
            assert_eq!(btree.get(key.as_bytes())?, None);
        }
        
        // Double delete should return false
        assert!(!btree.delete(b"del_0000")?);
        
        Ok(())
    }

    #[test]
    fn test_btree_persistence() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("persist_btree.db");
        
        // Phase 1: Create and populate
        {
            let page_manager = Arc::new(RwLock::new(PageManager::create(&db_path, 4096 * 16)?));
            let wrapper = PageManagerWrapper::standard(page_manager.clone());
            let mut btree = BPlusTree::new_with_wrapper(wrapper)?;
            
            for i in 0..50 {
                let key = format!("persist_{:02}", i);
                let val = format!("value_{}", i);
                btree.insert(key.as_bytes(), val.as_bytes())?;
            }
            
            // Update some
            for i in (0..50).step_by(5) {
                let key = format!("persist_{:02}", i);
                let val = format!("updated_{}", i);
                btree.insert(key.as_bytes(), val.as_bytes())?;
            }
            
            // Delete some
            for i in (0..50).step_by(10) {
                let key = format!("persist_{:02}", i);
                btree.delete(key.as_bytes())?;
            }
            
            // Sync to disk
            page_manager.write().sync()?;
        }
        
        // Phase 2: Reopen and verify
        {
            let page_manager = Arc::new(RwLock::new(PageManager::open(&db_path)?));
            let wrapper = PageManagerWrapper::standard(page_manager);
            let btree = BPlusTree::from_existing_with_wrapper(wrapper, 1, 1);
            
            for i in 0..50 {
                let key = format!("persist_{:02}", i);
                
                if i % 10 == 0 {
                    // Should be deleted
                    assert_eq!(btree.get(key.as_bytes())?, None);
                } else if i % 5 == 0 {
                    // Should be updated
                    let expected = format!("updated_{}", i);
                    assert_eq!(btree.get(key.as_bytes())?, Some(expected.into_bytes()));
                } else {
                    // Should have original value
                    let expected = format!("value_{}", i);
                    assert_eq!(btree.get(key.as_bytes())?, Some(expected.into_bytes()));
                }
            }
        }
        
        Ok(())
    }

    #[test]
    fn test_btree_concurrent_safety() -> Result<()> {
        use std::thread;
        use std::sync::Arc;
        
        let (btree, _dir) = create_test_btree()?;
        let btree = Arc::new(RwLock::new(btree));
        
        const NUM_THREADS: usize = 4;
        const OPS_PER_THREAD: usize = 250;
        
        let mut handles = vec![];
        
        // Spawn threads
        for thread_id in 0..NUM_THREADS {
            let btree_clone = Arc::clone(&btree);
            
            let handle = thread::spawn(move || -> Result<()> {
                for i in 0..OPS_PER_THREAD {
                    let key = format!("t{}_k{:03}", thread_id, i);
                    let val = format!("t{}_v{}", thread_id, i);
                    
                    // Write operation
                    btree_clone.write().insert(key.as_bytes(), val.as_bytes())?;
                    
                    // Read operation
                    let retrieved = btree_clone.read().get(key.as_bytes())?;
                    assert_eq!(retrieved, Some(val.into_bytes()));
                    
                    // Occasional delete
                    if i % 10 == 0 && i > 0 {
                        let del_key = format!("t{}_k{:03}", thread_id, i - 5);
                        btree_clone.write().delete(del_key.as_bytes())?;
                    }
                }
                Ok(())
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads
        for handle in handles {
            handle.join().unwrap()?;
        }
        
        // Verify final state
        let btree = btree.read();
        for thread_id in 0..NUM_THREADS {
            for i in 0..OPS_PER_THREAD {
                let key = format!("t{}_k{:03}", thread_id, i);
                
                if i % 10 == 5 && i < OPS_PER_THREAD - 5 {
                    // Should be deleted
                    assert_eq!(btree.get(key.as_bytes())?, None);
                } else {
                    // Should exist
                    assert!(btree.get(key.as_bytes())?.is_some());
                }
            }
        }
        
        Ok(())
    }

    #[test]
    fn test_btree_boundary_conditions() -> Result<()> {
        let (mut btree, _dir) = create_test_btree()?;
        
        // Test at node capacity boundaries
        println!("Testing at MIN_KEYS={}, MAX_KEYS={} boundaries", MIN_KEYS_PER_NODE, MAX_KEYS_PER_NODE);
        
        // Fill exactly to MAX_KEYS_PER_NODE
        for i in 0..MAX_KEYS_PER_NODE {
            let key = format!("boundary_{:03}", i);
            let val = format!("bval_{}", i);
            btree.insert(key.as_bytes(), val.as_bytes())?;
        }
        
        // One more should trigger split
        btree.insert(b"trigger_split", b"split_value")?;
        
        // Verify all keys still accessible
        for i in 0..MAX_KEYS_PER_NODE {
            let key = format!("boundary_{:03}", i);
            assert!(btree.get(key.as_bytes())?.is_some());
        }
        assert_eq!(btree.get(b"trigger_split")?, Some(b"split_value".to_vec()));
        
        // Delete down to MIN_KEYS_PER_NODE
        for i in 0..(MAX_KEYS_PER_NODE - MIN_KEYS_PER_NODE) {
            let key = format!("boundary_{:03}", i);
            assert!(btree.delete(key.as_bytes())?);
        }
        
        // Verify remaining keys
        for i in (MAX_KEYS_PER_NODE - MIN_KEYS_PER_NODE)..MAX_KEYS_PER_NODE {
            let key = format!("boundary_{:03}", i);
            assert!(btree.get(key.as_bytes())?.is_some());
        }
        
        Ok(())
    }
}
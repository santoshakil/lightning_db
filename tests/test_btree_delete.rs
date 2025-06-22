use lightning_db::Database;
use lightning_db::LightningDbConfig;
use tempfile::tempdir;

#[test]
fn test_btree_delete_operations() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert test data
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Verify all keys exist
    for i in 0..100 {
        let key = format!("key{:03}", i);
        assert!(db.get(key.as_bytes()).unwrap().is_some());
    }

    // Delete every other key
    for i in (0..100).step_by(2) {
        let key = format!("key{:03}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Verify deletion
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let result = db.get(key.as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(result.is_none(), "Key {} should be deleted", key);
        } else {
            assert!(result.is_some(), "Key {} should exist", key);
        }
    }
}

#[test]
fn test_btree_delete_with_reopen() {
    let dir = tempdir().unwrap();
    
    // Create and populate database
    {
        let mut config = LightningDbConfig::default();
        config.compression_enabled = false; // Use B+Tree only for this test
        let db = Database::create(dir.path(), config).unwrap();
        
        for i in 0..50 {
            let key = format!("key{:02}", i);
            let value = format!("value{:02}", i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Delete some keys
        for i in (10..40).step_by(3) {
            let key = format!("key{:02}", i);
            db.delete(key.as_bytes()).unwrap();
        }
        
        // Ensure data is synced before closing
        db.sync().unwrap();
    }
    
    // Reopen and verify
    {
        let mut config = LightningDbConfig::default();
        config.compression_enabled = false; // Use B+Tree only for this test
        let db = Database::open(dir.path(), config).unwrap();
        
        for i in 0..50 {
            let key = format!("key{:02}", i);
            let result = db.get(key.as_bytes()).unwrap();
            
            if i >= 10 && i < 40 && (i - 10) % 3 == 0 {
                assert!(result.is_none(), "Key {} should be deleted", key);
            } else {
                assert!(result.is_some(), "Key {} should exist", key);
            }
        }
    }
}

#[test]
fn test_btree_delete_all_and_reinsert() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert, delete all, then reinsert
    for round in 0..3 {
        // Insert
        for i in 0..20 {
            let key = format!("round{}_key{:02}", round, i);
            let value = format!("round{}_value{:02}", round, i);
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        // Verify
        for i in 0..20 {
            let key = format!("round{}_key{:02}", round, i);
            assert!(db.get(key.as_bytes()).unwrap().is_some());
        }
        
        // Delete all
        for i in 0..20 {
            let key = format!("round{}_key{:02}", round, i);
            db.delete(key.as_bytes()).unwrap();
        }
        
        // Verify deletion
        for i in 0..20 {
            let key = format!("round{}_key{:02}", round, i);
            assert!(db.get(key.as_bytes()).unwrap().is_none());
        }
    }
}

#[test]
fn test_btree_delete_nonexistent() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Try to delete non-existent key
    db.delete(b"nonexistent").unwrap();
    
    // Insert and delete
    db.put(b"exists", b"value").unwrap();
    db.delete(b"exists").unwrap();
    
    // Try to delete again
    db.delete(b"exists").unwrap();
}

#[test]
fn test_btree_mass_delete_for_underflow() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();

    // Insert many keys to create a multi-level tree
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        let value = format!("value{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Delete most keys to trigger node merging
    for i in 0..900 {
        let key = format!("key{:04}", i);
        db.delete(key.as_bytes()).unwrap();
    }

    // Verify remaining keys
    for i in 900..1000 {
        let key = format!("key{:04}", i);
        let result = db.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "Key {} should exist", key);
    }

    // Verify deleted keys
    for i in 0..900 {
        let key = format!("key{:04}", i);
        let result = db.get(key.as_bytes()).unwrap();
        assert!(result.is_none(), "Key {} should be deleted", key);
    }
}
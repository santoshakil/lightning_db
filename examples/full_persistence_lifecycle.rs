use lightning_db::{Database, LightningDbConfig};
use std::fs;
use tempfile::tempdir;

fn get_file_sizes(db_path: &std::path::Path) -> (u64, u64) {
    let btree_size = fs::metadata(db_path.join("btree.db"))
        .map(|m| m.len())
        .unwrap_or(0);
    
    let wal_size = fs::read_dir(db_path.join("wal"))
        .ok()
        .and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .max()
        })
        .unwrap_or(0);
    
    (btree_size, wal_size)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB Full Persistence Lifecycle ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();
    
    // Step 1: Initial write
    println!("Step 1: Writing initial data...");
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;
        
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        let (btree, wal) = get_file_sizes(db_path);
        println!("  After writes: btree.db={} bytes, WAL={} bytes", btree, wal);
        
        db.checkpoint()?;
        let (btree, wal) = get_file_sizes(db_path);
        println!("  After checkpoint: btree.db={} bytes, WAL={} bytes", btree, wal);
    }
    
    // Step 2: Reopen and verify
    println!("\nStep 2: Reopening and verifying data...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        let mut found = 0;
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }
        println!("  ✓ Found {}/10 entries after reopen", found);
    }
    
    // Step 3: Write more data
    println!("\nStep 3: Writing more data...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        for i in 10..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}_with_padding_{}", i, "x".repeat(100));
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        
        let (btree, wal) = get_file_sizes(db_path);
        println!("  After 90 more writes: btree.db={} bytes, WAL={} bytes", btree, wal);
    }
    
    // Step 4: Delete operations
    println!("\nStep 4: Testing deletes...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Delete every other key
        for i in (0..100).step_by(2) {
            let key = format!("key_{:03}", i);
            db.delete(key.as_bytes())?;
        }
        
        db.checkpoint()?;
        
        // Verify deletes
        let mut remaining = 0;
        let mut deleted = 0;
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            match db.get(key.as_bytes())? {
                Some(_) => remaining += 1,
                None => deleted += 1,
            }
        }
        
        println!("  ✓ Deleted: {}, Remaining: {}", deleted, remaining);
    }
    
    // Step 5: Final verification after complete restart
    println!("\nStep 5: Final verification after complete restart...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Check that odd-numbered keys exist and even ones don't
        let mut correct = 0;
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let exists = db.get(key.as_bytes())?.is_some();
            
            if (i % 2 == 1 && exists) || (i % 2 == 0 && !exists) {
                correct += 1;
            }
        }
        
        println!("  ✓ {}/100 keys in correct state", correct);
        
        if correct == 100 {
            println!("\n✅ PERSISTENCE FULLY VERIFIED!");
            println!("   - Data survives database restarts");
            println!("   - Deletes are persistent");
            println!("   - WAL recovery works correctly");
        }
    }
    
    Ok(())
}
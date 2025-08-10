use lightning_db::{Database, Options, Result, Transaction, WriteBatch};
use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<()> {
    println!("🧪 Lightning DB CRUD Verification Test");
    println!("=====================================\n");

    // Create temporary directory for test
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("crud_test.db");
    
    println!("📁 Database path: {}", db_path.display());
    
    // Test 1: Basic Create operations
    println!("\n✅ Test 1: CREATE Operations");
    {
        let opts = Options::builder()
            .create_if_missing(true)
            .build();
        
        let db = Database::open(&db_path, opts)?;
        let start = Instant::now();
        
        // Insert various data types
        db.put(b"string_key", b"Hello, World!")?;
        db.put(b"number_key", &42u64.to_le_bytes())?;
        db.put(b"binary_key", &[0xFF, 0xDE, 0xAD, 0xBE, 0xEF])?;
        db.put(b"empty_value", b"")?;
        db.put(b"large_value", &vec![0xAB; 1024])?; // 1KB value
        
        // Insert with batch for atomicity
        let mut batch = WriteBatch::new();
        for i in 0..10 {
            batch.put(format!("batch_key_{}", i).as_bytes(), format!("batch_value_{}", i).as_bytes());
        }
        db.write(batch)?;
        
        db.flush()?;
        let elapsed = start.elapsed();
        
        println!("  ✓ Inserted 15 items in {:?}", elapsed);
        println!("  ✓ Data flushed to disk");
        
        // Verify immediately
        assert_eq!(db.get(b"string_key")?.unwrap(), b"Hello, World!");
        assert_eq!(db.get(b"number_key")?.unwrap(), &42u64.to_le_bytes());
        assert_eq!(db.get(b"empty_value")?.unwrap(), b"");
        
        // Check database size
        let metadata = db.get_metadata()?;
        println!("  ✓ Database size: {} bytes", metadata.size_on_disk);
        println!("  ✓ Total keys: {}", metadata.total_keys);
    }
    
    // Verify persistence by reopening
    println!("\n🔄 Verifying Persistence (reopening database)");
    {
        let db = Database::open(&db_path, Options::default())?;
        
        // Verify all data is still there
        assert_eq!(db.get(b"string_key")?.unwrap(), b"Hello, World!");
        assert_eq!(db.get(b"number_key")?.unwrap(), &42u64.to_le_bytes());
        assert_eq!(db.get(b"binary_key")?.unwrap(), &[0xFF, 0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(db.get(b"empty_value")?.unwrap(), b"");
        assert_eq!(db.get(b"large_value")?.unwrap().len(), 1024);
        
        // Verify batch items
        for i in 0..10 {
            let key = format!("batch_key_{}", i);
            let expected = format!("batch_value_{}", i);
            assert_eq!(db.get(key.as_bytes())?.unwrap(), expected.as_bytes());
        }
        
        println!("  ✓ All 15 items verified after reopen");
    }
    
    // Test 2: READ Operations
    println!("\n✅ Test 2: READ Operations");
    {
        let db = Database::open(&db_path, Options::default())?;
        let start = Instant::now();
        
        // Sequential reads
        let mut found = 0;
        for i in 0..10 {
            let key = format!("batch_key_{}", i);
            if db.get(key.as_bytes())?.is_some() {
                found += 1;
            }
        }
        
        // Non-existent key
        assert!(db.get(b"non_existent_key")?.is_none());
        
        // Range scan
        let mut count = 0;
        let iter = db.iterator();
        for item in iter {
            let (_, _) = item?;
            count += 1;
        }
        
        let elapsed = start.elapsed();
        println!("  ✓ Sequential reads: {} items in {:?}", found, elapsed);
        println!("  ✓ Range scan: {} total items", count);
        println!("  ✓ Non-existent key returns None correctly");
    }
    
    // Test 3: UPDATE Operations
    println!("\n✅ Test 3: UPDATE Operations");
    {
        let db = Database::open(&db_path, Options::default())?;
        let start = Instant::now();
        
        // Update existing values
        db.put(b"string_key", b"Updated: Hello, Lightning DB!")?;
        db.put(b"number_key", &100u64.to_le_bytes())?;
        
        // Transactional update
        let txn = db.begin_transaction()?;
        txn.put(b"transactional_update", b"initial_value")?;
        txn.put(b"transactional_update", b"updated_value")?;
        txn.commit()?;
        
        db.flush()?;
        let elapsed = start.elapsed();
        
        println!("  ✓ Updated 3 items in {:?}", elapsed);
        
        // Verify updates
        assert_eq!(db.get(b"string_key")?.unwrap(), b"Updated: Hello, Lightning DB!");
        assert_eq!(db.get(b"number_key")?.unwrap(), &100u64.to_le_bytes());
        assert_eq!(db.get(b"transactional_update")?.unwrap(), b"updated_value");
        
        println!("  ✓ All updates verified");
    }
    
    // Test 4: DELETE Operations
    println!("\n✅ Test 4: DELETE Operations");
    {
        let db = Database::open(&db_path, Options::default())?;
        let start = Instant::now();
        
        // Delete single item
        db.delete(b"empty_value")?;
        assert!(db.get(b"empty_value")?.is_none());
        
        // Batch delete
        let mut batch = WriteBatch::new();
        for i in 0..5 {
            batch.delete(format!("batch_key_{}", i).as_bytes());
        }
        db.write(batch)?;
        
        // Transactional delete
        let txn = db.begin_transaction()?;
        txn.delete(b"transactional_update")?;
        txn.commit()?;
        
        db.flush()?;
        let elapsed = start.elapsed();
        
        println!("  ✓ Deleted 7 items in {:?}", elapsed);
        
        // Verify deletions
        assert!(db.get(b"empty_value")?.is_none());
        for i in 0..5 {
            assert!(db.get(format!("batch_key_{}", i).as_bytes())?.is_none());
        }
        assert!(db.get(b"transactional_update")?.is_none());
        
        // Verify remaining items still exist
        assert!(db.get(b"string_key")?.is_some());
        for i in 5..10 {
            assert!(db.get(format!("batch_key_{}", i).as_bytes())?.is_some());
        }
        
        println!("  ✓ Deletions verified, remaining items intact");
    }
    
    // Test 5: File System Verification
    println!("\n🔍 Test 5: File System Verification");
    {
        // Check actual files on disk
        let db_files: Vec<_> = fs::read_dir(temp_dir.path())?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();
        
        println!("  📁 Database files on disk:");
        for file in &db_files {
            let file_path = temp_dir.path().join(file);
            let size = fs::metadata(&file_path)?.len();
            println!("    - {}: {} bytes", file, size);
        }
        
        // Verify at least some data files exist
        assert!(!db_files.is_empty(), "No database files found!");
        
        // Check total size
        let total_size: u64 = db_files.iter()
            .map(|f| fs::metadata(temp_dir.path().join(f)).map(|m| m.len()).unwrap_or(0))
            .sum();
        
        println!("  ✓ Total disk usage: {} bytes", total_size);
        assert!(total_size > 0, "Database has no data on disk!");
    }
    
    // Test 6: Complex Operations
    println!("\n✅ Test 6: Complex Mixed Operations");
    {
        let db = Database::open(&db_path, Options::default())?;
        let start = Instant::now();
        
        // Mixed operations in a transaction
        let txn = db.begin_transaction()?;
        
        // Add new items
        txn.put(b"txn_new_1", b"new_value_1")?;
        txn.put(b"txn_new_2", b"new_value_2")?;
        
        // Update existing
        txn.put(b"string_key", b"Final update")?;
        
        // Delete some
        txn.delete(b"large_value")?;
        
        // Read within transaction
        let read_value = txn.get(b"string_key")?;
        assert_eq!(read_value.unwrap(), b"Final update");
        
        txn.commit()?;
        db.flush()?;
        
        let elapsed = start.elapsed();
        println!("  ✓ Complex transaction completed in {:?}", elapsed);
        
        // Final verification
        assert_eq!(db.get(b"string_key")?.unwrap(), b"Final update");
        assert_eq!(db.get(b"txn_new_1")?.unwrap(), b"new_value_1");
        assert!(db.get(b"large_value")?.is_none());
        
        // Count final items
        let final_count = db.iterator().count();
        println!("  ✓ Final database contains {} items", final_count);
    }
    
    // Test 7: Crash and Recovery Simulation
    println!("\n💥 Test 7: Crash Simulation and Recovery");
    {
        // Add some data before "crash"
        let db = Database::open(&db_path, Options::default())?;
        db.put(b"before_crash", b"important_data")?;
        db.flush()?;
        drop(db); // Simulate crash
        
        // Recover
        let db = Database::open(&db_path, Options::default())?;
        assert_eq!(db.get(b"before_crash")?.unwrap(), b"important_data");
        println!("  ✓ Database recovered successfully after simulated crash");
        
        // Verify all previous data is still intact
        assert_eq!(db.get(b"string_key")?.unwrap(), b"Final update");
        assert_eq!(db.get(b"txn_new_1")?.unwrap(), b"new_value_1");
        println!("  ✓ All data integrity maintained");
    }
    
    println!("\n🎉 All CRUD verification tests passed!");
    println!("✅ Database operations are working correctly with proper persistence");
    
    Ok(())
}
use lightning_db::{Database, LightningDbConfig, WalSyncMode};
use tempfile::tempdir;

fn main() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    
    println!("Testing crash recovery at: {:?}", path);
    
    // Phase 1: Write data with sync mode
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        // Disable compression for simplicity
        config.compression_enabled = false;
        
        println!("Phase 1: Opening database and writing data...");
        let db = Database::open(&path, config).unwrap();
        
        db.put(b"key1", b"value1").unwrap();
        println!("Put key1=value1");
        
        db.put(b"key2", b"value2").unwrap();
        println!("Put key2=value2");
        
        // Verify data is written
        let val1 = db.get(b"key1").unwrap();
        let val2 = db.get(b"key2").unwrap();
        println!("Before drop - key1: {:?}, key2: {:?}", val1, val2);
        
        // Simulate crash by dropping without explicit close
        drop(db);
        println!("Database dropped (simulating crash)");
    }
    
    // Phase 2: Reopen and verify
    {
        let mut config = LightningDbConfig::default();
        config.wal_sync_mode = WalSyncMode::Sync;
        // Disable compression for simplicity
        config.compression_enabled = false;
        
        println!("\nPhase 2: Reopening database...");
        let db = Database::open(&path, config).unwrap();
        
        let val1 = db.get(b"key1").unwrap();
        let val2 = db.get(b"key2").unwrap();
        
        println!("After reopen - key1: {:?}, key2: {:?}", val1, val2);
        
        if val1.is_none() || val2.is_none() {
            println!("ERROR: Data not recovered!");
            std::process::exit(1);
        } else {
            println!("SUCCESS: Data recovered correctly!");
        }
    }
}
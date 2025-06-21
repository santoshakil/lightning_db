use lightning_db::wal_improved::ImprovedWriteAheadLog;
use lightning_db::wal::WALOperation;
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing WAL sync performance...\n");
    
    let dir = tempdir()?;
    
    // Test 1: With sync
    {
        let wal_path = dir.path().join("wal_sync");
        let wal = ImprovedWriteAheadLog::create_with_config(&wal_path, true, false)?;
        
        println!("Test 1: WAL with sync_on_commit=true");
        let start = Instant::now();
        for i in 0..100 {
            wal.append(WALOperation::Put {
                key: format!("key_{}", i).into_bytes(),
                value: b"value".to_vec(),
            })?;
        }
        let duration = start.elapsed();
        let ops_per_sec = 100.0 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
    }
    
    // Test 2: Without sync
    {
        let wal_path = dir.path().join("wal_nosync");
        let wal = ImprovedWriteAheadLog::create_with_config(&wal_path, false, false)?;
        
        println!("\nTest 2: WAL with sync_on_commit=false");
        let start = Instant::now();
        for i in 0..10000 {
            wal.append(WALOperation::Put {
                key: format!("key_{}", i).into_bytes(),
                value: b"value".to_vec(),
            })?;
        }
        let duration = start.elapsed();
        let ops_per_sec = 10000.0 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
        println!("  • Improvement: {:.0}x", ops_per_sec / 300.0);
    }
    
    // Test 3: With group commit
    {
        let wal_path = dir.path().join("wal_group");
        let wal = ImprovedWriteAheadLog::create_with_config(&wal_path, true, true)?;
        
        println!("\nTest 3: WAL with group_commit=true");
        let start = Instant::now();
        for i in 0..1000 {
            wal.append(WALOperation::Put {
                key: format!("key_{}", i).into_bytes(),
                value: b"value".to_vec(),
            })?;
        }
        let duration = start.elapsed();
        let ops_per_sec = 1000.0 / duration.as_secs_f64();
        println!("  • {:.0} ops/sec", ops_per_sec);
    }
    
    Ok(())
}
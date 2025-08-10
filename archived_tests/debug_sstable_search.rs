use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Lightning DB SSTable Search Debug Test");
    println!("=======================================\n");
    
    let dir = tempdir()?;
    let db_path = dir.path().to_path_buf();
    
    println!("Phase 1: Write data in batches");
    println!("-------------------------------");
    
    {
        let config = LightningDbConfig::default();
        let db = Arc::new(Database::create(&db_path, config)?);
        
        // Write data in batches to create multiple SSTables
        for batch in 0..3 {
            println!("Writing batch {}...", batch);
            for i in 0..10000 {
                let key = format!("key_{:06}", batch * 10000 + i);
                let value = format!("value_{:06}", batch * 10000 + i);
                db.put(key.as_bytes(), value.as_bytes())?;
            }
            
            // Force flush after each batch
            db.sync()?;
            
            // Check stats
            if let Some(stats) = db.lsm_stats() {
                println!("  After batch {} - L0 files: {}, Memtable: {} bytes", 
                        batch, 
                        stats.levels.get(0).map(|l| l.num_files).unwrap_or(0),
                        stats.memtable_size);
            }
        }
        
        println!("\nTotal written: 30000 entries");
        
        // Check how many are readable
        let mut readable = 0;
        for i in 0..30000 {
            let key = format!("key_{:06}", i);
            if db.get(key.as_bytes())?.is_some() {
                readable += 1;
            }
        }
        println!("Readable before close: {}/30000", readable);
        
        // Force final sync
        db.sync()?;
    }
    
    println!("\nPhase 2: Reopen and check recovery");
    println!("------------------------------------");
    
    // Count SSTable files
    let lsm_dir = db_path.join("lsm");
    if lsm_dir.exists() {
        let mut sst_count = 0;
        let mut total_size = 0u64;
        for entry in std::fs::read_dir(&lsm_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "sst" {
                    let metadata = std::fs::metadata(&path)?;
                    sst_count += 1;
                    total_size += metadata.len();
                    println!("  SSTable: {} ({} bytes)", 
                            path.file_name().unwrap().to_string_lossy(),
                            metadata.len());
                }
            }
        }
        println!("Total: {} SSTable files, {} bytes", sst_count, total_size);
    }
    
    // Reopen database
    let db = Database::open(&db_path, LightningDbConfig::default())?;
    
    // Check stats after recovery
    if let Some(stats) = db.lsm_stats() {
        println!("\nAfter recovery:");
        println!("  L0 files: {}", stats.levels.get(0).map(|l| l.num_files).unwrap_or(0));
        println!("  Memtable: {} bytes", stats.memtable_size);
        println!("  Cache hit rate: {:.1}%", stats.cache_hit_rate);
    }
    
    // Count recovered entries
    let mut recovered = 0;
    let mut first_missing = None;
    
    for i in 0..30000 {
        let key = format!("key_{:06}", i);
        if db.get(key.as_bytes())?.is_some() {
            recovered += 1;
        } else if first_missing.is_none() {
            first_missing = Some(i);
        }
    }
    
    println!("\nRecovered {}/30000 entries", recovered);
    if let Some(idx) = first_missing {
        println!("First missing entry at index: {}", idx);
    }
    
    // Try to find pattern in missing entries
    if recovered < 30000 {
        println!("\nAnalyzing missing entries pattern:");
        let mut missing_ranges = Vec::new();
        let mut range_start = None;
        
        for i in 0..30000 {
            let key = format!("key_{:06}", i);
            let exists = db.get(key.as_bytes())?.is_some();
            
            match (exists, range_start) {
                (false, None) => range_start = Some(i),
                (true, Some(start)) => {
                    missing_ranges.push((start, i - 1));
                    range_start = None;
                }
                _ => {}
            }
        }
        
        if let Some(start) = range_start {
            missing_ranges.push((start, 29999));
        }
        
        println!("Missing ranges (first 5):");
        for (start, end) in missing_ranges.iter().take(5) {
            println!("  {} - {} ({} entries)", start, end, end - start + 1);
        }
    }
    
    // Summary
    println!("\n=== SUMMARY ===");
    if recovered == 30000 {
        println!("✅ SUCCESS: All entries recovered from SSTables");
    } else {
        println!("❌ FAILURE: Lost {} entries", 30000 - recovered);
        println!("The SSTables exist but entries aren't being found");
    }
    
    Ok(())
}
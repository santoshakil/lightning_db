use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::io::{Read, Seek};
use tempfile::tempdir;

fn find_in_buffer(buffer: &[u8], pattern: &[u8]) -> Option<usize> {
    buffer.windows(pattern.len())
        .position(|window| window == pattern)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB File Content Verification ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();
    
    println!("Step 1: Creating database and writing test data...");
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;
        
        // Write some recognizable data
        db.put(b"test_key_001", b"HELLO_WORLD_12345")?;
        db.put(b"test_key_002", b"LIGHTNING_DB_ROCKS")?;
        db.put(b"test_key_003", b"DATA_PERSISTENCE_TEST")?;
        
        // Write with unicode
        db.put("key_🔥".as_bytes(), "value_🌟".as_bytes())?;
        
        // Force to disk
        db.checkpoint()?;
        println!("  ✓ Data written and checkpoint completed");
    }
    
    println!("\nStep 2: Database closed, examining raw file contents...");
    
    // Read the btree.db file
    let btree_file = db_path.join("btree.db");
    if btree_file.exists() {
        let mut file = fs::File::open(&btree_file)?;
        let file_size = file.metadata()?.len();
        
        println!("  File: {:?}", btree_file);
        println!("  Size: {} bytes", file_size);
        
        // Read entire file to search for our data
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        println!("\nStep 3: Searching for our data in the raw file...");
        
        // Look for our test strings in the file
        let test_strings = vec![
            (b"test_key_001".as_ref(), "test_key_001"),
            (b"test_key_002".as_ref(), "test_key_002"), 
            (b"test_key_003".as_ref(), "test_key_003"),
            (b"HELLO_WORLD_12345".as_ref(), "HELLO_WORLD_12345"),
            (b"LIGHTNING_DB_ROCKS".as_ref(), "LIGHTNING_DB_ROCKS"),
            (b"DATA_PERSISTENCE_TEST".as_ref(), "DATA_PERSISTENCE_TEST"),
        ];
        
        let mut found_count = 0;
        for (test_bytes, test_str) in &test_strings {
            if let Some(pos) = find_in_buffer(&buffer, test_bytes) {
                println!("  ✓ Found '{}' at byte offset {}", test_str, pos);
                found_count += 1;
                
                // Show some context (safely)
                let start = pos.saturating_sub(10);
                let end = (pos + test_bytes.len() + 10).min(buffer.len());
                let context = &buffer[start..end];
                
                // Create a safe string representation
                let safe_context: String = context.iter()
                    .map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' })
                    .collect();
                
                println!("    Context: ...{}...", safe_context);
            } else {
                println!("  ❌ NOT FOUND: '{}'", test_str);
            }
        }
        
        println!("\n  Found {}/{} test strings in raw file", found_count, test_strings.len());
        
        // Show a hexdump of the first 512 bytes
        println!("\n  First 512 bytes of file (hexdump):");
        for (i, chunk) in buffer.iter().take(512).enumerate() {
            if i % 16 == 0 {
                print!("\n  {:04x}: ", i);
            }
            print!("{:02x} ", chunk);
        }
        println!("\n");
        
        // Also check WAL directory
        let wal_dir = db_path.join("wal");
        if wal_dir.exists() {
            println!("\n  WAL directory exists:");
            for entry in fs::read_dir(&wal_dir)? {
                let entry = entry?;
                println!("    - {}: {} bytes", 
                         entry.file_name().to_string_lossy(),
                         entry.metadata()?.len());
            }
        }
    } else {
        println!("  ❌ btree.db file not found!");
    }
    
    println!("\nStep 4: Reopening database to verify data is readable...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Try to read our data back
        let tests = vec![
            (b"test_key_001".as_ref(), b"HELLO_WORLD_12345".as_ref()),
            (b"test_key_002".as_ref(), b"LIGHTNING_DB_ROCKS".as_ref()),
            (b"test_key_003".as_ref(), b"DATA_PERSISTENCE_TEST".as_ref()),
            ("key_🔥".as_bytes(), "value_🌟".as_bytes()),
        ];
        
        let mut verified = 0;
        for (key, expected_value) in &tests {
            match db.get(key)? {
                Some(value) => {
                    if value == *expected_value {
                        println!("  ✓ Key '{}' = '{}'", 
                                 String::from_utf8_lossy(key),
                                 String::from_utf8_lossy(&value));
                        verified += 1;
                    } else {
                        println!("  ❌ Key '{}' has wrong value!", String::from_utf8_lossy(key));
                    }
                }
                None => {
                    println!("  ❌ Key '{}' not found!", String::from_utf8_lossy(key));
                }
            }
        }
        
        println!("\n  Verified {}/{} entries", verified, tests.len());
    }
    
    println!("\nStep 5: Testing delete persistence...");
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        // Delete a key
        db.delete(b"test_key_002")?;
        println!("  Deleted 'test_key_002'");
        
        // Verify it's gone
        if db.get(b"test_key_002")?.is_none() {
            println!("  ✓ Key successfully deleted from current session");
        }
        
        // Checkpoint to ensure delete is persisted
        db.checkpoint()?;
    }
    
    // Reopen and verify delete persisted
    {
        let db = Database::open(db_path, LightningDbConfig::default())?;
        
        if db.get(b"test_key_002")?.is_none() {
            println!("  ✓ Delete persisted - key still gone after reopen");
        } else {
            println!("  ❌ Delete NOT persisted - key reappeared!");
        }
        
        // Check other keys still exist
        if db.get(b"test_key_001")?.is_some() && db.get(b"test_key_003")?.is_some() {
            println!("  ✓ Other keys still present");
        }
    }
    
    println!("\n=== CONCLUSION ===");
    println!("If we found the test strings in the raw file AND could read them back,");
    println!("then persistence is DEFINITELY working at the file system level.");
    
    Ok(())
}
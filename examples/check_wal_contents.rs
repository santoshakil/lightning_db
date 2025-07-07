use lightning_db::{Database, LightningDbConfig};
use std::fs;
use std::io::Read;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Lightning DB WAL Content Verification ===\n");

    let dir = tempdir()?;
    let db_path = dir.path();

    // Write test data
    {
        let db = Database::create(db_path, LightningDbConfig::default())?;
        db.put(b"KEY1", b"VALUE1")?;
        db.put(b"KEY2", b"VALUE2")?;
        db.checkpoint()?;
    }

    // Check WAL file
    let wal_dir = db_path.join("wal");
    if let Ok(entries) = fs::read_dir(&wal_dir) {
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("seg") {
                println!("WAL File: {:?}", path);

                let mut file = fs::File::open(&path)?;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;

                println!("Size: {} bytes", buffer.len());

                // Look for our data
                if buffer.windows(4).any(|w| w == b"KEY1") {
                    println!("✓ Found KEY1 in WAL!");
                }
                if buffer.windows(6).any(|w| w == b"VALUE1") {
                    println!("✓ Found VALUE1 in WAL!");
                }

                // Show readable parts
                println!("\nReadable content in WAL:");
                let readable: String = buffer
                    .iter()
                    .map(|&b| {
                        if b.is_ascii_graphic() || b == b' ' {
                            b as char
                        } else {
                            '.'
                        }
                    })
                    .collect();
                println!("{}", readable);
            }
        }
    }

    Ok(())
}

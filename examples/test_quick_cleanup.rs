use lightning_db::{Database, LightningDbConfig};
use std::time::Instant;
use tempfile::tempdir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Quick Cleanup Test");
    println!("====================\n");
    
    for i in 0..3 {
        println!("Test {}: ", i + 1);
        
        let dir = tempdir()?;
        
        // Create database
        let start = Instant::now();
        let db = Database::create(dir.path().join("test.db"), LightningDbConfig::default())?;
        println!("  Created in: {:.3}s", start.elapsed().as_secs_f64());
        
        // Do some work
        let start = Instant::now();
        db.put(b"key", b"value")?;
        let _result = db.get(b"key")?;
        println!("  Work in: {:.3}s", start.elapsed().as_secs_f64());
        
        // Drop and measure
        let start = Instant::now();
        drop(db);
        let drop_time = start.elapsed().as_secs_f64();
        println!("  Dropped in: {:.3}s", drop_time);
        
        if drop_time > 1.0 {
            println!("  ⚠️  Still slow!");
        } else {
            println!("  ✅ Fast cleanup!");
        }
        println!();
    }
    
    println!("🏁 Quick cleanup test complete");
    Ok(())
}
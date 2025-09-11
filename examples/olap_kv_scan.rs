use lightning_db::Database;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: usize = std::env::var("OLAP_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_00);
    let value_size: usize = std::env::var("OLAP_VALUE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);

    let db = Database::create_temp()?;

    let mut v = vec![0u8; value_size];
    for i in 0..value_size {
        v[i] = (i % 251) as u8;
    }

    let t0 = Instant::now();
    for i in 0..rows {
        let key = format!("scan_{:010}", i);
        db.put(key.as_bytes(), &v)?;
    }
    let load_ms = t0.elapsed().as_millis();

    let start_key = b"scan_0000000000".to_vec();
    let end_key = format!("scan_{:010}", rows).into_bytes();

    let t1 = Instant::now();
    let mut bytes = 0usize;
    let mut cnt = 0usize;
    for (k, val) in db.range(Some(&start_key), Some(&end_key))? {
        bytes += k.len() + val.len();
        cnt += 1;
    }
    let scan_ms = t1.elapsed().as_millis();

    println!("OLAP KV baseline:");
    println!("  rows: {}", rows);
    println!("  value_bytes: {}", value_size);
    println!("  load_ms: {}", load_ms);
    println!("  scan_ms: {}", scan_ms);
    println!("  scanned_rows: {}", cnt);
    println!("  scanned_bytes: {}", bytes);
    if scan_ms > 0 {
        println!(
            "  mb_per_s: {:.2}",
            (bytes as f64 / (1024.0 * 1024.0)) / (scan_ms as f64 / 1000.0)
        );
    }

    Ok(())
}
